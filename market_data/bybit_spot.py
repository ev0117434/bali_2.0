#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/bybit_spot.py — Сборщик best bid/ask с Bybit SPOT через WebSocket.

WS URL  : wss://stream.bybit.com/v5/public/spot
Канал   : orderbook.1.{SYMBOL}   (L1 — только лучший бид/аск)
Подписка: до 10 символов в одном subscribe-сообщении
Пинг    : {"op":"ping"} каждые 20 сек

Формат топика ответа: orderbook.1.BTCUSDT
data.b[0][0] = best bid price
data.a[0][0] = best ask price
ts            = exchange timestamp (мс)
"""

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import websockets
from websockets.exceptions import ConnectionClosed

sys.path.insert(0, str(Path(__file__).parent))
from common import (
    Stats, ConnectionStats, LogManager, ChunkManager, SnapshotLogger,
    create_redis, load_symbols, chunk_list,
    HISTORY_FLUSH_INTERVAL, TICKER_FLUSH_INTERVAL,
)

EXCHANGE   = "bybit"
MARKET     = "spot"
WS_URL     = "wss://stream.bybit.com/v5/public/spot"
CHUNK_SIZE = 200   # символов на одно WS-соединение
SUB_BATCH  = 10    # символов в одном subscribe-сообщении (рекомендация Bybit Spot)
PING_INTERVAL   = 20
MAX_RECONNECT_DELAY = 60

_ticker_buf:  Dict[str, Tuple[str, str, str, str, float, float]] = {}
_history_buf: Dict[str, List[Tuple[float, str, str]]] = {}


async def _ws_worker(
    idx: int, symbols: List[str],
    conn_stats: ConnectionStats, stats: Stats,
) -> None:
    conn_stats.url = WS_URL
    delay = 1

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,   # Собственный пинг
                close_timeout=5,
                max_size=2**20,
                open_timeout=15,
            ) as ws:
                conn_stats.active = True
                delay = 1

                # Подписываемся батчами по SUB_BATCH
                for batch in chunk_list(symbols, SUB_BATCH):
                    args = [f"orderbook.1.{s}" for s in batch]
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))

                last_ping = time.time()

                async def _ping_loop():
                    nonlocal last_ping
                    while True:
                        await asyncio.sleep(PING_INTERVAL)
                        try:
                            await ws.send(json.dumps({"op": "ping"}))
                            last_ping = time.time()
                        except Exception:
                            break

                ping_task = asyncio.create_task(_ping_loop())

                try:
                    async for raw in ws:
                        recv_ts = time.time()
                        try:
                            obj = json.loads(raw)
                            # Фильтруем служебные сообщения
                            if obj.get("op") in ("subscribe", "ping", "pong"):
                                continue
                            topic = obj.get("topic", "")
                            if not topic.startswith("orderbook.1."):
                                continue

                            data = obj.get("data", {})
                            sym  = data.get("s", "").upper()
                            bids = data.get("b", [])
                            asks = data.get("a", [])
                            if not sym or not bids or not asks:
                                continue

                            bid = str(bids[0][0])
                            ask = str(asks[0][0])
                            bq  = str(bids[0][1]) if len(bids[0]) > 1 else "0"
                            aq  = str(asks[0][1]) if len(asks[0]) > 1 else "0"

                            # Exchange timestamp (мс)
                            ex_ts_ms = obj.get("ts", 0)
                            latency_ms = (recv_ts * 1000 - ex_ts_ms) if ex_ts_ms else 0.0

                            stats.record_message(latency_ms=latency_ms, symbol=sym)
                            conn_stats.msgs_total  += 1
                            conn_stats.msgs_window += 1
                            conn_stats.last_msg_ts  = recv_ts

                            _ticker_buf[sym] = (bid, ask, bq, aq, recv_ts, latency_ms)
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
            for sym, (bid, ask, bq, aq, ts, lat) in batch.items():
                key = chunk_manager.get_ticker_key(sym)
                pipe.hset(key, mapping={
                    "bid": bid, "ask": ask,
                    "bid_qty": bq, "ask_qty": aq,
                    "ts": f"{ts:.3f}", "latency_ms": f"{lat:.2f}",
                    "exchange": EXCHANGE, "market": MARKET,
                })
            await pipe.execute()
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
    logger.info(f"[{script_name}] Запуск...")

    symbols = load_symbols(EXCHANGE, MARKET)
    logger.info(f"[{script_name}] Загружено {len(symbols)} символов")

    redis_client  = await create_redis()
    stats         = Stats()
    stats.symbols_tracked = len(symbols)
    chunk_manager = ChunkManager(EXCHANGE, MARKET, redis_client)
    await chunk_manager.initialize()

    sym_chunks = chunk_list(symbols, CHUNK_SIZE)
    for _ in sym_chunks:
        stats.connections.append(ConnectionStats())

    logger.info(f"[{script_name}] Запускаю {len(sym_chunks)} WS-соединений ({WS_URL})...")

    snap_logger = SnapshotLogger(script_name, log_mgr, stats, chunk_manager)
    tasks = []
    for i, (chunk, conn_st) in enumerate(zip(sym_chunks, stats.connections)):
        tasks.append(asyncio.create_task(_ws_worker(i, chunk, conn_st, stats)))
    tasks.append(asyncio.create_task(_ticker_flusher(redis_client, stats, chunk_manager)))
    tasks.append(asyncio.create_task(_history_flusher(redis_client, stats, chunk_manager)))
    tasks.append(asyncio.create_task(snap_logger.run()))

    logger.info(f"[{script_name}] Все задачи запущены.")
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info(f"[{script_name}] Завершение.")
    finally:
        snap_logger.stop()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
