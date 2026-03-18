#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/binance_spot.py — Binance SPOT best bid/ask collector via WebSocket.

WS URL : wss://stream.binance.com:9443/stream?streams=...
Channel: {symbol}@bookTicker  (fastest best bid/ask updates)
Chunk  : up to 300 streams per connection
Ping   : built-in websockets ping_interval
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

# ─── Exchange config ──────────────────────────────────────────────────────────
EXCHANGE   = "binance"
MARKET     = "spot"
WS_BASE    = "wss://stream.binance.com:9443/stream?streams="
CHUNK_SIZE = 300       # до 300 потоков на WS-соединение
MAX_RECONNECT_DELAY = 60

# ─── Shared state (written by ws-workers, read by flushers) ──────────────────
# symbol -> (bid, ask, local_ts)
_ticker_buf: Dict[str, Tuple[str, str, float]] = {}
# symbol -> [(local_ts, bid, ask), ...]
_history_buf: Dict[str, List[Tuple[float, str, str]]] = {}


def _build_url(symbols: List[str]) -> str:
    streams = "/".join(f"{s.lower()}@bookTicker" for s in symbols)
    return WS_BASE + streams


# ══════════════════════════════════════════════════════════════════════════════
# WS connection worker
# ══════════════════════════════════════════════════════════════════════════════

async def _ws_worker(
    idx:        int,
    symbols:    List[str],
    conn_stats: ConnectionStats,
    stats:      Stats,
) -> None:
    """
    One WS connection for a symbol chunk.
    Reconnects automatically on any error.
    """
    url   = _build_url(symbols)
    conn_stats.url = url[:80]
    delay = 1

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                max_size=2**20,
                open_timeout=15,
            ) as ws:
                conn_stats.active = True
                delay = 1

                async for raw in ws:
                    recv_ts = time.time()
                    try:
                        obj  = json.loads(raw)
                        data = obj.get("data", obj)
                        sym  = data.get("s", "").upper()
                        bid  = data.get("b", "")
                        ask  = data.get("a", "")
                        bq   = data.get("B", "")
                        aq   = data.get("A", "")
                        if not sym or not bid or not ask:
                            continue

                        # Binance bookTicker has no exchange timestamp => RTT unavailable
                        stats.record_message(rtt_ms=None, symbol=sym)
                        conn_stats.msgs_total  += 1
                        conn_stats.msgs_window += 1
                        conn_stats.last_msg_ts  = recv_ts

                        _ticker_buf[sym] = (bid, ask, recv_ts)

                        if sym not in _history_buf:
                            _history_buf[sym] = []
                        _history_buf[sym].append((recv_ts, bid, ask))

                    except Exception:
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


# ══════════════════════════════════════════════════════════════════════════════
# Background flushers
# ══════════════════════════════════════════════════════════════════════════════

async def _ticker_flusher(redis_client, stats: Stats, chunk_manager: ChunkManager):
    """Every TICKER_FLUSH_INTERVAL seconds flush accumulated tickers to Redis via pipeline."""
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
    """Every second flush history buffer to Redis; rotate chunk if needed."""
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


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    script_name = f"{EXCHANGE}_{MARKET}"

    log_mgr = LogManager(script_name)
    log_mgr.initialize()
    logger  = log_mgr.get_logger()
    logger.info(f"[{script_name}] Starting...")

    symbols = load_symbols(EXCHANGE, MARKET)
    logger.info(f"[{script_name}] Loaded {len(symbols)} symbols")

    redis_client  = await create_redis()
    logger.info(f"[{script_name}] Redis connected: {redis_client.connection_pool.connection_kwargs}")

    stats         = Stats()
    stats.symbols_tracked = len(symbols)
    chunk_manager = ChunkManager(EXCHANGE, MARKET, redis_client)
    await chunk_manager.initialize()

    sym_chunks = chunk_list(symbols, CHUNK_SIZE)
    for _ in sym_chunks:
        stats.connections.append(ConnectionStats())

    logger.info(
        f"[{script_name}] Starting {len(sym_chunks)} WS connections "
        f"({CHUNK_SIZE} symbols each)..."
    )

    snap_logger = SnapshotLogger(script_name, log_mgr, stats, chunk_manager)

    tasks = []
    for i, (chunk, conn_st) in enumerate(zip(sym_chunks, stats.connections)):
        tasks.append(asyncio.create_task(_ws_worker(i, chunk, conn_st, stats)))

    tasks.append(asyncio.create_task(_ticker_flusher(redis_client, stats, chunk_manager)))
    tasks.append(asyncio.create_task(_history_flusher(redis_client, stats, chunk_manager)))
    tasks.append(asyncio.create_task(snap_logger.run()))

    logger.info(f"[{script_name}] All tasks started. Symbols: {len(symbols)}.")
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info(f"[{script_name}] Shutting down.")
    finally:
        snap_logger.stop()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
