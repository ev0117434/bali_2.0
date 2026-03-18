#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dictionaries/combination/generate.py — Генерация файлов пересечений символов.

Для каждой пары (spot_exch × fut_exch), где spot_exch ≠ fut_exch,
находит общие символы и записывает в файл:
    {spot_exch}_spot_{fut_exch}_futures.txt

Использование:
    python3 dictionaries/combination/generate.py
"""

from pathlib import Path

SUBSCRIBE_DIR   = Path(__file__).resolve().parent.parent / "subscribe"
COMBINATION_DIR = Path(__file__).resolve().parent

EXCHANGES = ["binance", "bybit", "gate", "okx"]


def load_symbols(exchange: str, market: str) -> set:
    path = SUBSCRIBE_DIR / exchange / f"{exchange}_{market}.txt"
    if not path.exists():
        print(f"  [warn] не найден: {path}")
        return set()
    return {
        line.strip().upper()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }


def main():
    COMBINATION_DIR.mkdir(parents=True, exist_ok=True)

    generated = 0
    for spot_exch in EXCHANGES:
        spot_syms = load_symbols(spot_exch, "spot")
        if not spot_syms:
            continue
        for fut_exch in EXCHANGES:
            if spot_exch == fut_exch:
                continue
            fut_syms = load_symbols(fut_exch, "futures")
            if not fut_syms:
                continue
            common = sorted(spot_syms & fut_syms)
            if not common:
                print(f"  [skip] {spot_exch}_spot_{fut_exch}_futures — нет общих символов")
                continue
            fname = COMBINATION_DIR / f"{spot_exch}_spot_{fut_exch}_futures.txt"
            fname.write_text("\n".join(common) + "\n", encoding="utf-8")
            print(f"  {fname.name}: {len(common)} символов")
            generated += 1

    print(f"\nГотово: сгенерировано {generated} файлов направлений.")


if __name__ == "__main__":
    main()
