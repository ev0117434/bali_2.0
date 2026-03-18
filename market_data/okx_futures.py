#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/okx_futures.py — OKX FUTURES (SWAP/Perp) best bid/ask collector via WebSocket.

WS URL  : wss://ws.okx.com:8443/ws/v5/public
Channel : tickers (instId=BTC-USDT-SWAP)
Subscribe: up to 300 instId per connection
Ping    : string "ping" every 25 s

Symbol conversion:
  BTCUSDT  ->  BTC-USDT-SWAP   (for OKX SWAP API)
  instId BTC-USDT-SWAP  ->  BTCUSDT  (normalized)
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import websockets
from websockets.exceptions import ConnectionClosed

sys.path.insert(0, str(Path(__file__).parent))
from common import (
    Stats, ConnectionStats, LogManager, ChunkManager, SnapshotLogger,
    create_redis, load_symbols, chunk_list,
    HISTORY_FLUSH_INTERVAL, TICKER_FLUSH_INTERVAL,
)

EXCHANGE   = "okx"
MARKET     = "futures"
WS_URL     = "wss://ws.okx.com:8443/ws/v5/public"
CHUNK_SIZE = 300
PING_INTERVAL = 25
MAX_RECONNECT_DELAY = 60

_ticker_buf:  Dict[str, Tuple[str, str, float]] = {}
_history_buf: Dict[str, List[Tuple[float, str, str]]] = {}


def _to_okx_swap(symbol: str) -> Optional[str]:
    """BTCUSDT → BTC-USDT-SWAP"""
    for quote in ("USDC", "USDT"):
        if symbol.endswith(quote):
            base = symbol[: -len(quote)]
            return f"{base}-{quote}-SWAP"
    return None


def _from_okx_swap(inst_id: str) -> str:
    """BTC-USDT-SWAP → BTCUSDT"""
    return inst_id.replace("-SWAP", "").replace("-", "")


async def _ws_worker(
    idx: int, symbols: List[str],
    conn_stats: ConnectionStats, stats: Stats,
) -> None:
    inst_map: Dict[str, str] = {}
    for sym in symbols:
        okx_id = _to_okx_swap(sym)
        if okx_id:
            inst_map[okx_id] = sym

    conn_stats.url = WS_URL
    delay = 1

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                close_timeout=5,
                max_size=2**21,
                open_timeout=15,
            ) as ws:
                conn_stats.active = True
                delay = 1

                args = [{"channel": "tickers", "instId": iid} for iid in inst_map]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))

                async def _ping_loop():
                    while True:
                        await asyncio.sleep(PING_INTERVAL)
                        try:
                            await ws.send("ping")
                        except Exception:
                            break

                ping_task = asyncio.create_task(_ping_loop())

                try:
                    async for raw in ws:
                        recv_ts = time.time()
                        if raw == "pong":
                            continue
                        try:
                            obj = json.loads(raw)
                            if "event" in obj:
                                continue
                            data_list = obj.get("data", [])
                            if not data_list:
                                continue

                            for item in data_list:
                                inst_id = item.get("instId", "")
                                sym     = inst_map.get(inst_id) or _from_okx_swap(inst_id)
                                if not sym:
                                    continue

                                bid = item.get("bidPx", "")
                                ask = item.get("askPx", "")
                                bq  = item.get("bidSz", "0")
                                aq  = item.get("askSz", "0")
                                if not bid or not ask:
                                    continue

                                ex_ts_ms   = int(item.get("ts", 0))
                                latency_ms = (recv_ts * 1000 - ex_ts_ms) if ex_ts_ms else 0.0

                                stats.record_message(rtt_ms=latency_ms, symbol=sym)
                                conn_stats.msgs_total  += 1
                                conn_stats.msgs_window += 1
                                conn_stats.last_msg_ts  = recv_ts

                                _ticker_buf[sym] = (bid, ask, recv_ts)
                                if sym not in _history_buf:
                                    _history_buf[sym] = []
                                _history_buf[sym].append((recv_ts, bid, ask))

                        except Exception:
                            pass
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except (ConnectionClosed, OSError, Exception) as exc:
            conn_stats.active = False
            err = repr(exc)[:120]
            conn_stats.last_error    = err
            conn_stats.last_error_ts = time.time()
            conn_stats.reconnects   += 1
            stats.record_error(err)

        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_RECONNECT_DELAY)


async def _ticker_flusher(redis_client, stats: Stats, chunk_manager: ChunkManager):
    while True:
        await asyncio.sleep(TICKER_FLUSH_INTERVAL)
        if not _ticker_buf:
            continue
        batch = dict(_ticker_buf)
        _ticker_buf.clear()
        try:
            pipe = redis_client.pipeline(transaction=False)
            for sym, (bid, ask, ts) in batch.items():
                key = chunk_manager.get_ticker_key(sym)
                pipe.hset(key, mapping={
                    "bid": bid,
                    "ask": ask,
                    "ts":  f"{ts:.3f}",
                })
            await pipe.execute()
            write_ts = __import__("time").time()
            for _, (_, _, recv_ts) in batch.items():
                stats.record_proc_latency((write_ts - recv_ts) * 1000)
            stats.record_ticker_write(len(batch))
        except Exception:
            pass


async def _history_flusher(redis_client, stats: Stats, chunk_manager: ChunkManager):
    while True:
        await asyncio.sleep(HISTORY_FLUSH_INTERVAL)
        if chunk_manager.needs_rotation():
            await chunk_manager.rotate()
        if not _history_buf:
            continue
        snapshot: Dict[str, List[Tuple[float, str, str]]] = {}
        for sym in list(_history_buf.keys()):
            entries = _history_buf.pop(sym, [])
            if entries:
                snapshot[sym] = entries
        if not snapshot:
            continue
        total = 0
        try:
            pipe = redis_client.pipeline(transaction=False)
            for sym, entries in snapshot.items():
                key = chunk_manager.get_history_key(sym)
                for ts, bid, ask in entries:
                    pipe.rpush(key, f"{ts:.3f},{bid},{ask}")
                total += len(entries)
            await pipe.execute()
            stats.record_history_entry(total)
        except Exception:
            pass


async def main():
    script_name = f"{EXCHANGE}_{MARKET}"
    log_mgr = LogManager(script_name)
    log_mgr.initialize()
    logger  = log_mgr.get_logger()
    logger.info(f"[{script_name}] Starting...")

    symbols = load_symbols(EXCHANGE, MARKET)
    logger.info(f"[{script_name}] Loaded {len(symbols)} symbols")

    redis_client  = await create_redis()
    stats         = Stats()
    stats.symbols_tracked = len(symbols)
    chunk_manager = ChunkManager(EXCHANGE, MARKET, redis_client)
    await chunk_manager.initialize()

    sym_chunks = chunk_list(symbols, CHUNK_SIZE)
    for _ in sym_chunks:
        stats.connections.append(ConnectionStats())

    logger.info(f"[{script_name}] Starting {len(sym_chunks)} WS connections ({WS_URL})...")

    snap_logger = SnapshotLogger(script_name, log_mgr, stats, chunk_manager)
    tasks = []
    for i, (chunk, conn_st) in enumerate(zip(sym_chunks, stats.connections)):
        tasks.append(asyncio.create_task(_ws_worker(i, chunk, conn_st, stats)))
    tasks.append(asyncio.create_task(_ticker_flusher(redis_client, stats, chunk_manager)))
    tasks.append(asyncio.create_task(_history_flusher(redis_client, stats, chunk_manager)))
    tasks.append(asyncio.create_task(snap_logger.run()))

    logger.info(f"[{script_name}] All tasks started.")
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info(f"[{script_name}] Shutting down.")
    finally:
        snap_logger.stop()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
