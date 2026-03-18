#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/gate_futures.py — Gate.io FUTURES (USDT-Margined) best bid/ask collector via WebSocket.

WS URL  : wss://fx-ws.gateio.ws/v4/ws/usdt
Channel : futures.book_ticker
Subscribe: batches of 100 symbols per payload
Ping    : {"channel":"futures.ping","time":<ts>} every 25 s

Symbol format:
  Subscribe file: BTCUSDT
  Gate.io WS:    BTC_USDT
  Redis:         BTCUSDT

Message fields:
  result.b  = best bid price
  result.a  = best ask price
  result.t  = exchange timestamp (ms)
  result.s  = symbol (BTC_USDT)
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

EXCHANGE   = "gate"
MARKET     = "futures"
WS_URL     = "wss://fx-ws.gateio.ws/v4/ws/usdt"
CHUNK_SIZE = 500
SUB_BATCH  = 100
PING_INTERVAL = 25
MAX_RECONNECT_DELAY = 60

_ticker_buf:  Dict[str, Tuple[str, str, float]] = {}
_history_buf: Dict[str, List[Tuple[float, str, str]]] = {}


def _to_gate(symbol: str) -> Optional[str]:
    """BTCUSDT → BTC_USDT"""
    for quote in ("USDC", "USDT"):
        if symbol.endswith(quote):
            base = symbol[: -len(quote)]
            return f"{base}_{quote}"
    return None


def _from_gate(gate_sym: str) -> str:
    """BTC_USDT → BTCUSDT"""
    return gate_sym.replace("_", "")


async def _ws_worker(
    idx: int, symbols: List[str],
    conn_stats: ConnectionStats, stats: Stats,
) -> None:
    sym_map: Dict[str, str] = {}
    gate_syms: List[str] = []
    for sym in symbols:
        gs = _to_gate(sym)
        if gs:
            sym_map[gs] = sym
            gate_syms.append(gs)

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

                for batch in chunk_list(gate_syms, SUB_BATCH):
                    msg = {
                        "time":    int(time.time()),
                        "channel": "futures.book_ticker",
                        "event":   "subscribe",
                        "payload": batch,
                    }
                    await ws.send(json.dumps(msg))
                    await asyncio.sleep(0.05)

                async def _ping_loop():
                    while True:
                        await asyncio.sleep(PING_INTERVAL)
                        try:
                            await ws.send(json.dumps({
                                "channel": "futures.ping",
                                "time":    int(time.time()),
                            }))
                        except Exception:
                            break

                ping_task = asyncio.create_task(_ping_loop())

                try:
                    async for raw in ws:
                        recv_ts = time.time()
                        try:
                            obj = json.loads(raw)
                            channel = obj.get("channel", "")
                            event   = obj.get("event", "")

                            if event in ("subscribe", "unsubscribe") or channel == "futures.pong":
                                continue
                            if event != "update":
                                continue

                            result = obj.get("result", {})
                            if not result:
                                continue

                            gate_sym = result.get("s", "")
                            sym      = sym_map.get(gate_sym) or _from_gate(gate_sym)
                            bid      = str(result.get("b", ""))
                            ask      = str(result.get("a", ""))
                            bq       = str(result.get("B", "0"))
                            aq       = str(result.get("A", "0"))
                            if not sym or not bid or not ask:
                                continue

                            ex_ts_ms   = result.get("t", 0)
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
