#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
binance_pairs.py - Получение всех торговых пар Binance через REST API.

Загружает пары с Spot и Futures (USD-M) рынков.
Фильтр: quoteAsset = USDT или USDC, status = TRADING.
Сохраняет результаты в binance/data/.
"""

import json
from pathlib import Path
from typing import List, Tuple
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_URL = "https://api.binance.com/api/v3/exchangeInfo"
FUTURES_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

SPOT_FILE = DATA_DIR / "binance_spot.txt"
FUTURES_FILE = DATA_DIR / "binance_futures.txt"

QUOTE_ASSETS = ("USDT", "USDC")


def _fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _extract_symbols(exchange_info: dict) -> List[str]:
    pairs = []
    for s in exchange_info.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") in QUOTE_ASSETS:
            pairs.append(s["symbol"])
    return sorted(pairs)


def _save(path: Path, pairs: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


def fetch_pairs() -> Tuple[List[str], List[str]]:
    """Загружает и возвращает (spot_pairs, futures_pairs) для Binance."""
    spot_info = _fetch_json(SPOT_URL)
    spot = _extract_symbols(spot_info)

    futures_info = _fetch_json(FUTURES_URL)
    futures = _extract_symbols(futures_info)

    _save(SPOT_FILE, spot)
    _save(FUTURES_FILE, futures)

    return spot, futures


if __name__ == "__main__":
    spot, futures = fetch_pairs()
    print(f"Binance Spot:    {len(spot)} пар  -> {SPOT_FILE}")
    print(f"Binance Futures: {len(futures)} пар  -> {FUTURES_FILE}")
