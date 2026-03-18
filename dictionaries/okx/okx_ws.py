#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
okx_ws.py - Валидация торговых пар OKX через WebSocket.

Подключается к OKX WS V5 Public API, подписывается на канал tickers.
В течение DURATION_SECONDS фиксирует пары, по которым пришёл хотя бы 1 ответ.
Возвращает нормализованные (BTCUSDT) активные пары.
Сохраняет их в okx/data/.

Особенности протокола OKX:
- Подписка: {"op": "subscribe", "args": [{"channel": "tickers", "instId": "BTC-USDT"}]}
- Пинг: текстовая строка "ping", ответ "pong"
- Чанки по 300 instId на одно соединение
- Задержка 0.15 с между открытием соединений
"""

import asyncio
import json
import time
from pathlib import Path
from typing import List, Set, Tuple

import websockets

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_WS_URL    = "wss://ws.okx.com:8443/ws/v5/public"
FUTURES_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

CHUNK_SIZE = 300          # instId на одно WS-соединение
CONNECT_DELAY = 0.15      # пауза между открытием соединений (сек)
DURATION_SECONDS = 60     # окно проверки
PING_INTERVAL = 25        # интервал пинга (сек)

SPOT_ACTIVE_FILE    = DATA_DIR / "okx_spot_active.txt"
FUTURES_ACTIVE_FILE = DATA_DIR / "okx_futures_active.txt"


def _normalize(okx_sym: str) -> str:
    """BTC-USDT → BTCUSDT, BTC-USDT-SWAP → BTCUSDT."""
    s = okx_sym
    if s.endswith("-SWAP"):
        s = s[:-5]
    return s.replace("-", "")


def _chunk(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


async def _ping_loop(ws, stop_evt: asyncio.Event) -> None:
    try:
        while not stop_evt.is_set():
            await asyncio.sleep(PING_INTERVAL)
            if stop_evt.is_set():
                break
            await ws.send("ping")
    except Exception:
        pass


async def _consumer(
    url: str,
    inst_ids: List[str],
    responded: Set[str],
    stop_at: float,
    delay: float,
) -> None:
    """Одно WS-соединение: подписывается на chunk instId, собирает ответы."""
    if delay:
        await asyncio.sleep(delay)

    stop_evt = asyncio.Event()

    async def _recv_loop(ws):
        try:
            async for raw in ws:
                if raw == "pong":
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                data_list = msg.get("data", [])
                for item in data_list:
                    inst_id = item.get("instId", "")
                    if inst_id:
                        responded.add(_normalize(inst_id))
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
            # Одним запросом подписываемся на все instId чанка
            args = [{"channel": "tickers", "instId": iid} for iid in inst_ids]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))

            recv_task = asyncio.create_task(_recv_loop(ws))
            ping_task = asyncio.create_task(_ping_loop(ws, stop_evt))

            remaining = max(0, stop_at - time.time())
            try:
                await asyncio.sleep(remaining)
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


async def _validate_market(
    url: str,
    native_symbols: List[str],
    duration: int,
    market_label: str,
) -> Set[str]:
    """
    Открывает несколько WS-соединений параллельно (чанки по CHUNK_SIZE).
    Возвращает множество нормализованных символов, по которым пришёл ответ.
    """
    responded: Set[str] = set()
    stop_at = time.time() + duration
    chunks = _chunk(native_symbols, CHUNK_SIZE)
    tasks = [
        asyncio.create_task(
            _consumer(url, chunk, responded, stop_at, idx * CONNECT_DELAY)
        )
        for idx, chunk in enumerate(chunks)
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
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
        _validate_market(SPOT_WS_URL, spot_native, duration, "SPOT")
    )
    futures_task = asyncio.create_task(
        _validate_market(FUTURES_WS_URL, futures_native, duration, "FUTURES")
    )
    spot_responded, futures_responded = await asyncio.gather(spot_task, futures_task)

    # Фильтруем, сохраняя исходный порядок из нормализованного списка
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
    Валидирует пары OKX через WS.
    Принимает нативные (BTC-USDT) символы для подписки и нормализованные
    (BTCUSDT) для фильтрации результата.
    Возвращает (active_spot_norm, active_futures_norm).
    """
    return asyncio.run(_run(spot_native, futures_native, spot_norm, futures_norm, duration))


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(BASE_DIR.parent))
    from okx.okx_pairs import fetch_pairs, load_native

    spot_norm, futures_norm = fetch_pairs()
    spot_native, futures_native = load_native()
    print(f"OKX: загружено Spot={len(spot_norm)}, Futures={len(futures_norm)}")
    print(f"Запускаю WS-валидацию ({DURATION_SECONDS} сек)...")
    active_spot, active_futures = validate_pairs(spot_native, futures_native, spot_norm, futures_norm)
    print(f"Spot active:    {len(active_spot)}/{len(spot_norm)}  -> {SPOT_ACTIVE_FILE}")
    print(f"Futures active: {len(active_futures)}/{len(futures_norm)}  -> {FUTURES_ACTIVE_FILE}")
