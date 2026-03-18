#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gate_ws.py - Валидация торговых пар Gate.io через WebSocket.

Подключается к Gate.io WS V4 (Spot и Futures USDT).
Канал: spot.book_ticker / futures.book_ticker
В течение DURATION_SECONDS фиксирует пары, по которым пришёл хотя бы 1 ответ.
Возвращает нормализованные (BTCUSDT) активные пары.
Сохраняет результаты в gate/data/.

Особенности протокола Gate.io:
- Подписка: {"time": ts, "channel": "spot.book_ticker", "event": "subscribe", "payload": [...]}
- Пинг:     {"time": ts, "channel": "spot.ping"}
- Ответ содержит result.s = "BTC_USDT"
- Несколько subscribe-сообщений на одном соединении (батчами по BATCH_SIZE)
"""

import asyncio
import json
import time
from pathlib import Path
from typing import List, Set, Tuple

import websockets

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_WS_URL    = "wss://api.gateio.ws/ws/v4/"
FUTURES_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"

BATCH_SIZE       = 100    # символов на одно subscribe-сообщение
DURATION_SECONDS = 60     # окно проверки
PING_INTERVAL    = 20     # интервал пинга (сек)

SPOT_ACTIVE_FILE    = DATA_DIR / "gate_spot_active.txt"
FUTURES_ACTIVE_FILE = DATA_DIR / "gate_futures_active.txt"


def _normalize(sym: str) -> str:
    """BTC_USDT → BTCUSDT."""
    return sym.replace("_", "")


def _chunk(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


def _ts() -> int:
    return int(time.time())


async def _ping_loop(ws, channel_prefix: str, stop_evt: asyncio.Event) -> None:
    """Кастомный пинг Gate.io: {"time": ts, "channel": "spot.ping"}."""
    try:
        while not stop_evt.is_set():
            await asyncio.sleep(PING_INTERVAL)
            if stop_evt.is_set():
                break
            await ws.send(json.dumps({"time": _ts(), "channel": f"{channel_prefix}.ping"}))
    except Exception:
        pass


async def _validate_market(
    url: str,
    channel: str,
    native_symbols: List[str],
    duration: int,
) -> Set[str]:
    """
    Одно WS-соединение для рынка.
    Отправляет несколько subscribe-сообщений батчами.
    Возвращает нормализованные символы, по которым пришёл ответ.
    """
    responded: Set[str] = set()
    stop_evt = asyncio.Event()
    channel_prefix = channel.split(".")[0]  # "spot" или "futures"

    async def _recv_loop(ws):
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if msg.get("channel") != channel:
                    continue
                if msg.get("event") != "update":
                    continue
                result = msg.get("result", {})
                sym = result.get("s", "")
                if sym:
                    responded.add(_normalize(sym))
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    try:
        async with websockets.connect(
            url,
            ping_interval=None,
            max_queue=2048,
            close_timeout=3,
        ) as ws:
            # Подписка батчами
            for batch in _chunk(native_symbols, BATCH_SIZE):
                await ws.send(json.dumps({
                    "time":    _ts(),
                    "channel": channel,
                    "event":   "subscribe",
                    "payload": batch,
                }))
                await asyncio.sleep(0.02)

            recv_task = asyncio.create_task(_recv_loop(ws))
            ping_task = asyncio.create_task(_ping_loop(ws, channel_prefix, stop_evt))

            try:
                await asyncio.sleep(duration)
            finally:
                stop_evt.set()
                recv_task.cancel()
                ping_task.cancel()
                try:
                    await ws.close()
                except Exception:
                    pass
                await asyncio.gather(recv_task, ping_task, return_exceptions=True)

    except Exception:
        pass

    return responded


def _save(path: Path, pairs: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


async def _run(
    spot_native: List[str],
    futures_native: List[str],
    spot_norm: List[str],
    futures_norm: List[str],
    duration: int,
) -> Tuple[List[str], List[str]]:
    spot_task = asyncio.create_task(
        _validate_market(SPOT_WS_URL, "spot.book_ticker", spot_native, duration)
    )
    futures_task = asyncio.create_task(
        _validate_market(FUTURES_WS_URL, "futures.book_ticker", futures_native, duration)
    )
    spot_responded, futures_responded = await asyncio.gather(spot_task, futures_task)

    spot_active    = [s for s in spot_norm    if s in spot_responded]
    futures_active = [s for s in futures_norm if s in futures_responded]

    _save(SPOT_ACTIVE_FILE,    spot_active)
    _save(FUTURES_ACTIVE_FILE, futures_active)

    return spot_active, futures_active


def validate_pairs(
    spot_native: List[str],
    futures_native: List[str],
    spot_norm: List[str],
    futures_norm: List[str],
    duration: int = DURATION_SECONDS,
) -> Tuple[List[str], List[str]]:
    """
    Валидирует пары Gate.io через WS.
    Принимает нативные (BTC_USDT) для подписки и нормализованные (BTCUSDT) для фильтра.
    Возвращает (active_spot_norm, active_futures_norm).
    """
    return asyncio.run(_run(spot_native, futures_native, spot_norm, futures_norm, duration))


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(BASE_DIR.parent))
    from gate.gate_pairs import fetch_pairs, load_native

    spot_norm, futures_norm = fetch_pairs()
    spot_native, futures_native = load_native()
    print(f"Gate.io: загружено Spot={len(spot_norm)}, Futures={len(futures_norm)}")
    print(f"Запускаю WS-валидацию ({DURATION_SECONDS} сек)...")
    active_spot, active_futures = validate_pairs(spot_native, futures_native, spot_norm, futures_norm)
    print(f"Spot active:    {len(active_spot)}/{len(spot_norm)}  -> {SPOT_ACTIVE_FILE}")
    print(f"Futures active: {len(active_futures)}/{len(futures_norm)}  -> {FUTURES_ACTIVE_FILE}")
