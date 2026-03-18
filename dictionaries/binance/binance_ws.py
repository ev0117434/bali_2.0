#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
binance_ws.py - Валидация торговых пар Binance через WebSocket.

Подключается к Binance WS (Spot и Futures USD-M) по потокам bookTicker.
В течение DURATION_SECONDS фиксирует пары, по которым пришёл хотя бы 1 ответ.
Сохраняет активные пары в binance/data/.
"""

import asyncio
import json
import time
from pathlib import Path
from typing import List, Set, Tuple

import websockets
from websockets.exceptions import ConnectionClosed

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_WS_BASE = "wss://stream.binance.com:9443/stream?streams="
FUTURES_WS_BASE = "wss://fstream.binance.com/stream?streams="

CHUNK_SIZE = 300        # кол-во стримов на одно WS-соединение
DURATION_SECONDS = 60   # окно проверки (секунд)

SPOT_ACTIVE_FILE = DATA_DIR / "binance_spot_active.txt"
FUTURES_ACTIVE_FILE = DATA_DIR / "binance_futures_active.txt"


def _build_url(base: str, symbols: List[str]) -> str:
    streams = "/".join(f"{s.lower()}@bookTicker" for s in symbols)
    return base + streams


def _chunk(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


async def _consumer(url: str, received: Set[str], stop_at: float) -> None:
    """Одно WS-соединение: читает bookTicker, записывает символы в received."""
    try:
        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_queue=4096,
        ) as ws:
            while True:
                now = time.time()
                if now >= stop_at:
                    break
                timeout = max(0.05, stop_at - now)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    continue
                try:
                    obj = json.loads(raw)
                    data = obj.get("data", obj)
                    sym = data.get("s")
                    if sym:
                        received.add(sym.upper())
                except Exception:
                    pass
    except (ConnectionClosed, OSError, Exception):
        pass


async def _validate_market(
    base_url: str,
    symbols: List[str],
    duration: int,
) -> Set[str]:
    """Запускает несколько WS-соединений (чанки по CHUNK_SIZE) параллельно."""
    received: Set[str] = set()
    stop_at = time.time() + duration
    chunks = _chunk(symbols, CHUNK_SIZE)
    tasks = [
        asyncio.create_task(_consumer(_build_url(base_url, chunk), received, stop_at))
        for chunk in chunks
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    return received


def _save(path: Path, pairs: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


async def _run(
    spot: List[str],
    futures: List[str],
    duration: int,
) -> Tuple[List[str], List[str]]:
    spot_task = asyncio.create_task(_validate_market(SPOT_WS_BASE, spot, duration))
    futures_task = asyncio.create_task(_validate_market(FUTURES_WS_BASE, futures, duration))
    spot_received, futures_received = await asyncio.gather(spot_task, futures_task)

    spot_active = [s for s in spot if s in spot_received]
    futures_active = [s for s in futures if s in futures_received]

    _save(SPOT_ACTIVE_FILE, spot_active)
    _save(FUTURES_ACTIVE_FILE, futures_active)

    return spot_active, futures_active


def validate_pairs(
    spot: List[str],
    futures: List[str],
    duration: int = DURATION_SECONDS,
) -> Tuple[List[str], List[str]]:
    """
    Валидирует пары через WS.
    Возвращает (active_spot, active_futures) — пары, по которым пришёл ответ.
    """
    return asyncio.run(_run(spot, futures, duration))


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(BASE_DIR.parent))
    from binance.binance_pairs import fetch_pairs

    spot, futures = fetch_pairs()
    print(f"Binance: загружено Spot={len(spot)}, Futures={len(futures)}")
    print(f"Запускаю WS-валидацию ({DURATION_SECONDS} сек)...")
    active_spot, active_futures = validate_pairs(spot, futures)
    print(f"Spot active:    {len(active_spot)}/{len(spot)}  -> {SPOT_ACTIVE_FILE}")
    print(f"Futures active: {len(active_futures)}/{len(futures)}  -> {FUTURES_ACTIVE_FILE}")
