#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bybit_ws.py - Валидация торговых пар Bybit через WebSocket.

Подключается к Bybit WS (Spot и Linear/Futures) по каналу orderbook.1.
В течение DURATION_SECONDS фиксирует пары, по которым пришёл хотя бы 1 ответ.
Сохраняет активные пары в bybit/data/.

Особенности протокола Bybit:
- Кастомный пинг: {"op": "ping"}
- Подписка батчами (spot: 10, futures: 200) из-за ограничений размера frame
"""

import asyncio
import json
import time
from pathlib import Path
from typing import List, Set, Tuple

import websockets

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
FUTURES_WS_URL = "wss://stream.bybit.com/v5/public/linear"

SPOT_BATCH = 10       # символов на один subscribe-запрос (spot)
FUTURES_BATCH = 200   # символов на один subscribe-запрос (futures)
MAX_CHARS_PER_REQUEST = 20000  # ограничение размера payload

DURATION_SECONDS = 60   # окно проверки (секунд)
PING_INTERVAL = 20      # интервал кастомного пинга (секунд)

SPOT_ACTIVE_FILE = DATA_DIR / "bybit_spot_active.txt"
FUTURES_ACTIVE_FILE = DATA_DIR / "bybit_futures_active.txt"


def _make_batches(symbols: List[str], max_items: int) -> List[List[str]]:
    """Разбивает символы на батчи с учётом max_items и MAX_CHARS_PER_REQUEST."""
    batches: List[List[str]] = []
    cur: List[str] = []
    cur_chars = 0
    for sym in symbols:
        topic = f"orderbook.1.{sym}"
        add_chars = len(topic) + 3  # запятая + кавычки
        if cur and (len(cur) >= max_items or (cur_chars + add_chars) > MAX_CHARS_PER_REQUEST):
            batches.append(cur)
            cur = []
            cur_chars = 0
        cur.append(sym)
        cur_chars += add_chars
    if cur:
        batches.append(cur)
    return batches


async def _ping_loop(ws, stop_evt: asyncio.Event) -> None:
    """Отправляет кастомный пинг Bybit каждые PING_INTERVAL секунд."""
    try:
        while not stop_evt.is_set():
            await asyncio.sleep(PING_INTERVAL)
            if stop_evt.is_set():
                break
            await ws.send(json.dumps({"op": "ping"}))
    except Exception:
        pass


async def _validate_market(
    url: str,
    symbols: List[str],
    batch_size: int,
    duration: int,
    market_name: str,
) -> Set[str]:
    """
    Одно WS-соединение для рынка.
    Подписывается на orderbook.1.{SYMBOL} батчами.
    Возвращает множество символов, по которым пришёл ответ.
    """
    responded: Set[str] = set()
    stop_evt = asyncio.Event()

    async def _recv_loop(ws):
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                # Игнорируем ошибки подписки
                if msg.get("op") == "subscribe" and not msg.get("success", True):
                    continue
                topic = msg.get("topic", "")
                if topic.startswith("orderbook.1."):
                    data = msg.get("data", {})
                    sym = data.get("s") if isinstance(data, dict) else None
                    if not sym:
                        sym = topic.split(".")[-1]
                    if sym:
                        responded.add(sym.upper())
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    try:
        async with websockets.connect(
            url,
            ping_interval=None,  # используем кастомный пинг
            max_queue=2048,
            close_timeout=3,
        ) as ws:
            # Отправляем подписки батчами
            for batch in _make_batches(symbols, batch_size):
                args = [f"orderbook.1.{s}" for s in batch]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                await asyncio.sleep(0.02)

            recv_task = asyncio.create_task(_recv_loop(ws))
            ping_task = asyncio.create_task(_ping_loop(ws, stop_evt))

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
    spot: List[str],
    futures: List[str],
    duration: int,
) -> Tuple[List[str], List[str]]:
    spot_task = asyncio.create_task(
        _validate_market(SPOT_WS_URL, spot, SPOT_BATCH, duration, "SPOT")
    )
    futures_task = asyncio.create_task(
        _validate_market(FUTURES_WS_URL, futures, FUTURES_BATCH, duration, "FUTURES")
    )
    spot_responded, futures_responded = await asyncio.gather(spot_task, futures_task)

    spot_active = [s for s in spot if s in spot_responded]
    futures_active = [s for s in futures if s in futures_responded]

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
    from bybit.bybit_pairs import fetch_pairs

    spot, futures = fetch_pairs()
    print(f"Bybit: загружено Spot={len(spot)}, Futures={len(futures)}")
    print(f"Запускаю WS-валидацию ({DURATION_SECONDS} сек)...")
    active_spot, active_futures = validate_pairs(spot, futures)
    print(f"Spot active:    {len(active_spot)}/{len(spot)}  -> {SPOT_ACTIVE_FILE}")
    print(f"Futures active: {len(active_futures)}/{len(futures)}  -> {FUTURES_ACTIVE_FILE}")
