#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gate_pairs.py - Получение всех торговых пар Gate.io через REST API.

Загружает пары с Spot и Futures (USDT-маржинальные бессрочные контракты).
Фильтр Spot:    quote = USDT или USDC, trade_status = "tradable"
Фильтр Futures: settle = USDT, in_delisting = false

Gate.io использует формат BTC_USDT (с подчёркиванием).
fetch_pairs() нормализует символы к BTCUSDT для совместимости с остальными биржами.
Нативные символы сохраняются отдельно — они нужны gate_ws.py для подписки.
"""

import json
from pathlib import Path
from typing import List, Tuple
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_URL    = "https://api.gateio.ws/api/v4/spot/currency_pairs"
FUTURES_URL = "https://api.gateio.ws/api/v4/futures/usdt/contracts"

SPOT_NATIVE_FILE    = DATA_DIR / "gate_spot_native.txt"
FUTURES_NATIVE_FILE = DATA_DIR / "gate_futures_native.txt"
SPOT_FILE           = DATA_DIR / "gate_spot.txt"
FUTURES_FILE        = DATA_DIR / "gate_futures.txt"

QUOTE_ASSETS = ("USDT", "USDC")


def _fetch_json(url: str) -> list:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _normalize(sym: str) -> str:
    """BTC_USDT → BTCUSDT."""
    return sym.replace("_", "")


def _extract_spot(data: list) -> Tuple[List[str], List[str]]:
    native, normalized = [], []
    for s in data:
        if s.get("trade_status") != "tradable":
            continue
        if s.get("quote") not in QUOTE_ASSETS:
            continue
        sym = s["id"]
        native.append(sym)
        normalized.append(_normalize(sym))
    return sorted(native), sorted(normalized)


def _extract_futures(data: list) -> Tuple[List[str], List[str]]:
    native, normalized = [], []
    for s in data:
        if s.get("in_delisting", False):
            continue
        name = s.get("name", "")
        # Gate.io USDT-futures: BTC_USDT, ETH_USDT, etc.
        quote = name.split("_")[-1] if "_" in name else ""
        if quote not in QUOTE_ASSETS:
            continue
        native.append(name)
        normalized.append(_normalize(name))
    return sorted(native), sorted(normalized)


def _save(path: Path, pairs: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


def fetch_pairs() -> Tuple[List[str], List[str]]:
    """
    Загружает пары Gate.io.
    Сохраняет нативные (BTC_USDT) и нормализованные (BTCUSDT) списки.
    Возвращает нормализованные (spot, futures).
    """
    spot_data = _fetch_json(SPOT_URL)
    spot_native, spot_norm = _extract_spot(spot_data)

    fut_data = _fetch_json(FUTURES_URL)
    fut_native, fut_norm = _extract_futures(fut_data)

    _save(SPOT_NATIVE_FILE,    spot_native)
    _save(FUTURES_NATIVE_FILE, fut_native)
    _save(SPOT_FILE,           spot_norm)
    _save(FUTURES_FILE,        fut_norm)

    return spot_norm, fut_norm


def load_native() -> Tuple[List[str], List[str]]:
    """Читает нативные Gate.io-символы из файлов (нужно для gate_ws.py)."""
    spot = [l.strip() for l in SPOT_NATIVE_FILE.read_text(encoding="utf-8").splitlines() if l.strip()]
    fut  = [l.strip() for l in FUTURES_NATIVE_FILE.read_text(encoding="utf-8").splitlines() if l.strip()]
    return spot, fut


if __name__ == "__main__":
    spot, futures = fetch_pairs()
    print(f"Gate.io Spot:    {len(spot)} пар  -> {SPOT_FILE}")
    print(f"Gate.io Futures: {len(futures)} пар  -> {FUTURES_FILE}")
