#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bybit_pairs.py - Получение всех торговых пар Bybit через REST API.

Загружает пары с Spot и Linear (Futures) рынков.
Фильтр: quoteCoin = USDT или USDC, status = Trading.
Поддерживает пагинацию (cursor-based).
Сохраняет результаты в bybit/data/.
"""

import json
from pathlib import Path
from typing import List, Tuple
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_URL = "https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000"
FUTURES_URL = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"

SPOT_FILE = DATA_DIR / "bybit_spot.txt"
FUTURES_FILE = DATA_DIR / "bybit_futures.txt"

QUOTE_ASSETS = ("USDT", "USDC")


def _fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fetch_all_pages(base_url: str) -> List[dict]:
    """Обходит все страницы Bybit API через cursor-пагинацию."""
    instruments = []
    cursor = ""
    while True:
        url = base_url + (f"&cursor={cursor}" if cursor else "")
        data = _fetch_json(url)
        result = data.get("result", {})
        instruments.extend(result.get("list", []))
        cursor = result.get("nextPageCursor", "")
        if not cursor:
            break
    return instruments


def _extract_symbols(instruments: List[dict]) -> List[str]:
    pairs = []
    for s in instruments:
        if s.get("status") != "Trading":
            continue
        if s.get("quoteCoin") in QUOTE_ASSETS:
            pairs.append(s["symbol"])
    return sorted(pairs)


def _save(path: Path, pairs: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


def fetch_pairs() -> Tuple[List[str], List[str]]:
    """Загружает и возвращает (spot_pairs, futures_pairs) для Bybit."""
    spot_instruments = _fetch_all_pages(SPOT_URL)
    spot = _extract_symbols(spot_instruments)

    futures_instruments = _fetch_all_pages(FUTURES_URL)
    futures = _extract_symbols(futures_instruments)

    _save(SPOT_FILE, spot)
    _save(FUTURES_FILE, futures)

    return spot, futures


if __name__ == "__main__":
    spot, futures = fetch_pairs()
    print(f"Bybit Spot:    {len(spot)} пар  -> {SPOT_FILE}")
    print(f"Bybit Futures: {len(futures)} пар  -> {FUTURES_FILE}")
