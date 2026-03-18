#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
okx_pairs.py - Получение всех торговых пар OKX через REST API.

Загружает пары с SPOT и SWAP (бессрочные фьючерсы) рынков.
Фильтр Spot:  quoteCcy   = USDT или USDC, state = live
Фильтр Swap:  settleCcy  = USDT или USDC, state = live

Символы хранятся в нативном формате OKX (BTC-USDT, BTC-USDT-SWAP).
fetch_pairs() возвращает нормализованные пары (BTCUSDT), но сохраняет
и нативные списки в data/ — они нужны okx_ws.py для подписки.
"""

import json
from pathlib import Path
from typing import List, Tuple
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_URL = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
SWAP_URL = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"

# Нативные (OKX-формат) файлы для WS-подписки
SPOT_NATIVE_FILE   = DATA_DIR / "okx_spot_native.txt"
FUTURES_NATIVE_FILE = DATA_DIR / "okx_futures_native.txt"

# Нормализованные файлы (BTCUSDT) для пересечений
SPOT_FILE    = DATA_DIR / "okx_spot.txt"
FUTURES_FILE = DATA_DIR / "okx_futures.txt"

QUOTE_ASSETS = ("USDT", "USDC")


def _fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _normalize(okx_sym: str) -> str:
    """BTC-USDT → BTCUSDT, BTC-USDT-SWAP → BTCUSDT."""
    s = okx_sym
    if s.endswith("-SWAP"):
        s = s[:-5]
    return s.replace("-", "")


def _extract_spot(instruments: list) -> Tuple[List[str], List[str]]:
    """Возвращает (native, normalized) для spot."""
    native, normalized = [], []
    for s in instruments:
        if s.get("state") != "live":
            continue
        if s.get("quoteCcy") not in QUOTE_ASSETS:
            continue
        inst_id = s["instId"]
        native.append(inst_id)
        normalized.append(_normalize(inst_id))
    return (
        sorted(native),
        sorted(normalized),
    )


def _extract_swap(instruments: list) -> Tuple[List[str], List[str]]:
    """Возвращает (native, normalized) для swap/futures."""
    native, normalized = [], []
    for s in instruments:
        if s.get("state") != "live":
            continue
        if s.get("settleCcy") not in QUOTE_ASSETS:
            continue
        inst_id = s["instId"]
        native.append(inst_id)
        normalized.append(_normalize(inst_id))
    return (
        sorted(native),
        sorted(normalized),
    )


def _save(path: Path, pairs: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


def fetch_pairs() -> Tuple[List[str], List[str]]:
    """
    Загружает пары OKX.
    Сохраняет нативные (BTC-USDT-SWAP) и нормализованные (BTCUSDT) списки.
    Возвращает нормализованные (spot, futures) — как у Binance/Bybit.
    """
    spot_data = _fetch_json(SPOT_URL)
    spot_native, spot_norm = _extract_spot(spot_data.get("data", []))

    swap_data = _fetch_json(SWAP_URL)
    fut_native, fut_norm = _extract_swap(swap_data.get("data", []))

    _save(SPOT_NATIVE_FILE,    spot_native)
    _save(FUTURES_NATIVE_FILE, fut_native)
    _save(SPOT_FILE,           spot_norm)
    _save(FUTURES_FILE,        fut_norm)

    return spot_norm, fut_norm


def load_native() -> Tuple[List[str], List[str]]:
    """Читает нативные OKX-символы из файлов (нужно для okx_ws.py)."""
    spot = [l.strip() for l in SPOT_NATIVE_FILE.read_text(encoding="utf-8").splitlines() if l.strip()]
    fut  = [l.strip() for l in FUTURES_NATIVE_FILE.read_text(encoding="utf-8").splitlines() if l.strip()]
    return spot, fut


if __name__ == "__main__":
    spot, futures = fetch_pairs()
    print(f"OKX Spot:    {len(spot)} пар  -> {SPOT_FILE}")
    print(f"OKX Futures: {len(futures)} пар  -> {FUTURES_FILE}")
