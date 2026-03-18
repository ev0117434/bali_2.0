#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dictionaries/main.py — Оркестратор обновления торговых пар.
=============================================================

Назначение
----------
Модуль собирает активные торговые пары с 4 бирж (Binance, Bybit, OKX, Gate.io),
строит пересечения (combination) и файлы подписки (subscribe) для системы
market-data.

Архитектура выполнения
----------------------
Выполнение разбито на 4 фазы:

  Фаза 1 — Параллельный REST-запрос (ThreadPoolExecutor, 4 воркера):
    Все 4 биржи опрашиваются одновременно. Каждый воркер вызывает
    <exchange>_pairs.fetch_pairs() и сохраняет результаты на диск.
    Итог: сырые списки пар (spot + futures) по каждой бирже.

  Фаза 2 — Параллельная WS-валидация (один event loop, asyncio.gather):
    Все 4 биржи валидируются одновременно в одном asyncio event loop.
    Каждая биржа открывает WebSocket-соединения, 60 секунд слушает рынок
    и фиксирует пары, по которым пришёл хотя бы один ответ.
    Итог: списки *активных* пар по каждой бирже.
    Суммарное время фазы: ~60 с вместо ~240 с при последовательном запуске.

  Фаза 3 — Построение пересечений (combination/):
    По всем комбинациям «spot_A ∩ futures_B» строятся .txt-файлы.

  Фаза 4 — Построение файлов подписки (subscribe/):
    Из combination-файлов агрегируются subscribe-файлы без дублей.

  Фаза 5 — Итоговый отчёт в stdout.

Структура файлов на диске
-------------------------
dictionaries/
├── combination/          # пересечения активных пар
│   ├── binance_spot_bybit_futures.txt
│   ├── bybit_spot_binance_futures.txt
│   ├── binance_spot_okx_futures.txt
│   ├── okx_spot_binance_futures.txt
│   ├── bybit_spot_okx_futures.txt
│   ├── okx_spot_bybit_futures.txt
│   ├── binance_spot_gate_futures.txt
│   ├── gate_spot_binance_futures.txt
│   ├── bybit_spot_gate_futures.txt
│   ├── gate_spot_bybit_futures.txt
│   ├── okx_spot_gate_futures.txt
│   └── gate_spot_okx_futures.txt
└── subscribe/            # файлы подписки для market-data
    ├── binance/
    │   ├── binance_spot.txt
    │   └── binance_futures.txt
    ├── bybit/
    │   ├── bybit_spot.txt
    │   └── bybit_futures.txt
    ├── okx/
    │   ├── okx_spot.txt
    │   └── okx_futures.txt
    └── gate/
        ├── gate_spot.txt
        └── gate_futures.txt

Логика combination-файлов
--------------------------
Каждый файл — отсортированное пересечение двух множеств активных пар:
  binance_spot_bybit_futures.txt  = binance_spot  ∩ bybit_futures
  bybit_spot_binance_futures.txt  = bybit_spot    ∩ binance_futures
  binance_spot_okx_futures.txt    = binance_spot  ∩ okx_futures
  okx_spot_binance_futures.txt    = okx_spot      ∩ binance_futures
  bybit_spot_okx_futures.txt      = bybit_spot    ∩ okx_futures
  okx_spot_bybit_futures.txt      = okx_spot      ∩ bybit_futures
  binance_spot_gate_futures.txt   = binance_spot  ∩ gate_futures
  gate_spot_binance_futures.txt   = gate_spot     ∩ binance_futures
  bybit_spot_gate_futures.txt     = bybit_spot    ∩ gate_futures
  gate_spot_bybit_futures.txt     = gate_spot     ∩ bybit_futures
  okx_spot_gate_futures.txt       = okx_spot      ∩ gate_futures
  gate_spot_okx_futures.txt       = gate_spot     ∩ okx_futures

Логика subscribe-файлов
------------------------
Каждый subscribe-файл объединяет все пары данного рынка из ВСЕХ combination-
файлов, в названии которых присутствует ключевое слово рынка.
Пример:  subscribe/binance/binance_spot.txt содержит объединение пар из
  binance_spot_bybit_futures.txt  +  binance_spot_okx_futures.txt
  + binance_spot_gate_futures.txt (без дублей, отсортировано).

Нормализация символов
---------------------
Все биржи используют единый нормализованный формат BTCUSDT:
  Binance : BTCUSDT       (нативный формат)
  Bybit   : BTCUSDT       (нативный формат)
  OKX     : BTC-USDT      → нормализуется в BTCUSDT
            BTC-USDT-SWAP → нормализуется в BTCUSDT
  Gate.io : BTC_USDT      → нормализуется в BTCUSDT

Публичный API модуля
--------------------
  main() -> None
      Запускает полный цикл обновления и печатает отчёт.

Зависимости
-----------
  Стандартная библиотека: asyncio, concurrent.futures, time, sys, pathlib
  Внутренние модули:
    binance.binance_pairs  — fetch_pairs()
    binance.binance_ws     — _run() (async)
    bybit.bybit_pairs      — fetch_pairs()
    bybit.bybit_ws         — _run() (async)
    okx.okx_pairs          — fetch_pairs(), load_native()
    okx.okx_ws             — _run() (async)
    gate.gate_pairs        — fetch_pairs(), load_native()
    gate.gate_ws           — _run() (async)

Использование
-------------
  python -m dictionaries.main
  # или напрямую:
  python dictionaries/main.py

Примерное время выполнения
--------------------------
  Фаза 1 (REST, параллельно)  : ~3–5 сек
  Фаза 2 (WS, параллельно)    : ~60 сек
  Фазы 3–5                    : <1 сек
  Итого                       : ~65–70 сек (vs ~245 сек при последовательном)
"""

import asyncio
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

COMBINATION_DIR = BASE_DIR / "combination"
SUBSCRIBE_DIR   = BASE_DIR / "subscribe"

# ── Таблица пересечений ────────────────────────────────────────────────────────
# Каждый кортеж: (ключ_A, ключ_B, имя_файла)
COMBINATIONS = [
    ("binance_spot", "bybit_futures",   "binance_spot_bybit_futures.txt"),
    ("bybit_spot",   "binance_futures", "bybit_spot_binance_futures.txt"),
    ("binance_spot", "okx_futures",     "binance_spot_okx_futures.txt"),
    ("okx_spot",     "binance_futures", "okx_spot_binance_futures.txt"),
    ("bybit_spot",   "okx_futures",     "bybit_spot_okx_futures.txt"),
    ("okx_spot",     "bybit_futures",   "okx_spot_bybit_futures.txt"),
    ("binance_spot", "gate_futures",    "binance_spot_gate_futures.txt"),
    ("gate_spot",    "binance_futures", "gate_spot_binance_futures.txt"),
    ("bybit_spot",   "gate_futures",    "bybit_spot_gate_futures.txt"),
    ("gate_spot",    "bybit_futures",   "gate_spot_bybit_futures.txt"),
    ("okx_spot",     "gate_futures",    "okx_spot_gate_futures.txt"),
    ("gate_spot",    "okx_futures",     "gate_spot_okx_futures.txt"),
]

# ── Маппинг ключевых слов → файлы подписки ────────────────────────────────────
SUBSCRIBE_MAP = {
    "binance_spot":    SUBSCRIBE_DIR / "binance" / "binance_spot.txt",
    "binance_futures": SUBSCRIBE_DIR / "binance" / "binance_futures.txt",
    "bybit_spot":      SUBSCRIBE_DIR / "bybit"   / "bybit_spot.txt",
    "bybit_futures":   SUBSCRIBE_DIR / "bybit"   / "bybit_futures.txt",
    "okx_spot":        SUBSCRIBE_DIR / "okx"     / "okx_spot.txt",
    "okx_futures":     SUBSCRIBE_DIR / "okx"     / "okx_futures.txt",
    "gate_spot":       SUBSCRIBE_DIR / "gate"    / "gate_spot.txt",
    "gate_futures":    SUBSCRIBE_DIR / "gate"    / "gate_futures.txt",
}

# ── Константа длительности WS-валидации ───────────────────────────────────────
DURATION_SECONDS = 60


# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции
# ──────────────────────────────────────────────────────────────────────────────

def _save(path: Path, pairs: list) -> None:
    """Сохраняет список пар в файл (создаёт директории при необходимости)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


def _make_combinations(active: dict) -> dict:
    """
    Строит все combination-файлы как пересечения активных пар.

    Parameters
    ----------
    active : dict
        Словарь вида::

            {
                "binance_spot":    [...],
                "binance_futures": [...],
                "bybit_spot":      [...],
                ...
            }

    Returns
    -------
    dict
        Словарь ``{filename: count}`` — количество пар в каждом файле.
    """
    COMBINATION_DIR.mkdir(parents=True, exist_ok=True)
    results = {}
    for key_a, key_b, filename in COMBINATIONS:
        set_a = set(active.get(key_a, []))
        set_b = set(active.get(key_b, []))
        intersection = sorted(set_a & set_b)
        _save(COMBINATION_DIR / filename, intersection)
        results[filename] = len(intersection)
    return results


def _make_subscribe_files() -> dict:
    """
    Агрегирует subscribe-файлы из всех combination/*.txt.

    Для каждого ключевого слова из ``SUBSCRIBE_MAP`` ищет combination-файлы,
    содержащие это слово в имени, и объединяет их пары (без дублей).

    Returns
    -------
    dict
        Словарь ``{keyword: count}`` — количество уникальных пар в каждом
        subscribe-файле.
    """
    buckets: dict = {key: set() for key in SUBSCRIBE_MAP}

    for comb_file in COMBINATION_DIR.glob("*.txt"):
        name = comb_file.name
        lines = [
            line.strip()
            for line in comb_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        for key in SUBSCRIBE_MAP:
            if key in name:
                buckets[key].update(lines)

    counts = {}
    for key, path in SUBSCRIBE_MAP.items():
        pairs = sorted(buckets[key])
        _save(path, pairs)
        counts[key] = len(pairs)
    return counts


# ──────────────────────────────────────────────────────────────────────────────
# Фаза 1 — Параллельный REST-сбор
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_all_exchanges() -> dict:
    """
    Запускает REST-запросы ко всем 4 биржам параллельно.

    Использует ``ThreadPoolExecutor`` (4 воркера), так как запросы —
    блокирующий I/O (urllib).

    Returns
    -------
    dict
        Словарь с результатами по каждой бирже::

            {
                "bn":   (spot_list, futures_list),     # Binance
                "bb":   (spot_list, futures_list),     # Bybit
                "okx":  (spot_norm, futures_norm),     # OKX (нормализованные)
                "gate": (spot_norm, futures_norm),     # Gate.io (нормализованные)
            }

    Raises
    ------
    Exception
        Пробрасывает исключение если хотя бы один REST-запрос завершился
        с ошибкой.
    """
    from binance.binance_pairs import fetch_pairs as bn_fetch
    from bybit.bybit_pairs     import fetch_pairs as bb_fetch
    from okx.okx_pairs         import fetch_pairs as okx_fetch
    from gate.gate_pairs       import fetch_pairs as gate_fetch

    tasks = {
        "bn":   bn_fetch,
        "bb":   bb_fetch,
        "okx":  okx_fetch,
        "gate": gate_fetch,
    }

    results = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_key = {executor.submit(fn): key for key, fn in tasks.items()}
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            results[key] = future.result()  # пробросит исключение при ошибке

    return results


# ──────────────────────────────────────────────────────────────────────────────
# Фаза 2 — Параллельная WS-валидация
# ──────────────────────────────────────────────────────────────────────────────

async def _validate_all_exchanges(
    bn_spot: list,
    bn_fut: list,
    bb_spot: list,
    bb_fut: list,
    okx_spot_native: list,
    okx_fut_native: list,
    okx_spot_norm: list,
    okx_fut_norm: list,
    gate_spot_native: list,
    gate_fut_native: list,
    gate_spot_norm: list,
    gate_fut_norm: list,
    duration: int,
) -> tuple:
    """
    Запускает WS-валидацию всех 4 бирж параллельно в одном event loop.

    Все биржи стартуют одновременно через ``asyncio.gather``. Поскольку
    каждая биржа ждёт ``duration`` секунд, суммарное время равно одному
    окну валидации (~60 с), а не четырём (~240 с).

    Parameters
    ----------
    bn_spot : list
        Список пар Binance Spot для валидации.
    bn_fut : list
        Список пар Binance Futures для валидации.
    bb_spot : list
        Список пар Bybit Spot для валидации.
    bb_fut : list
        Список пар Bybit Futures для валидации.
    okx_spot_native : list
        Нативные символы OKX Spot (``BTC-USDT``) для WS-подписки.
    okx_fut_native : list
        Нативные символы OKX Futures/Swap (``BTC-USDT-SWAP``) для WS-подписки.
    okx_spot_norm : list
        Нормализованные символы OKX Spot (``BTCUSDT``) для фильтрации результата.
    okx_fut_norm : list
        Нормализованные символы OKX Futures (``BTCUSDT``) для фильтрации результата.
    gate_spot_native : list
        Нативные символы Gate.io Spot (``BTC_USDT``) для WS-подписки.
    gate_fut_native : list
        Нативные символы Gate.io Futures (``BTC_USDT``) для WS-подписки.
    gate_spot_norm : list
        Нормализованные символы Gate.io Spot (``BTCUSDT``) для фильтрации.
    gate_fut_norm : list
        Нормализованные символы Gate.io Futures (``BTCUSDT``) для фильтрации.
    duration : int
        Длительность WS-окна наблюдения в секундах.

    Returns
    -------
    tuple
        8-элементный кортеж активных списков::

            (
                abn_spot,  abn_fut,    # Binance
                abb_spot,  abb_fut,    # Bybit
                aokx_spot, aokx_fut,   # OKX
                agate_spot, agate_fut, # Gate.io
            )
    """
    from binance.binance_ws import _run as bn_run
    from bybit.bybit_ws     import _run as bb_run
    from okx.okx_ws         import _run as okx_run
    from gate.gate_ws       import _run as gate_run

    (
        (abn_spot, abn_fut),
        (abb_spot, abb_fut),
        (aokx_spot, aokx_fut),
        (agate_spot, agate_fut),
    ) = await asyncio.gather(
        bn_run(bn_spot, bn_fut, duration),
        bb_run(bb_spot, bb_fut, duration),
        okx_run(okx_spot_native, okx_fut_native, okx_spot_norm, okx_fut_norm, duration),
        gate_run(gate_spot_native, gate_fut_native, gate_spot_norm, gate_fut_norm, duration),
    )

    return abn_spot, abn_fut, abb_spot, abb_fut, aokx_spot, aokx_fut, agate_spot, agate_fut


# ──────────────────────────────────────────────────────────────────────────────
# Отчёт
# ──────────────────────────────────────────────────────────────────────────────

def _print_report(r: dict, comb_counts: dict, sub_counts: dict, total_time: float) -> None:
    """
    Печатает итоговый отчёт в stdout.

    Parameters
    ----------
    r : dict
        Словарь метрик вида ``{bn_spot_total: int, bn_spot_active: int, ...}``.
    comb_counts : dict
        Словарь ``{filename: count}`` из ``_make_combinations()``.
    sub_counts : dict
        Словарь ``{keyword: count}`` из ``_make_subscribe_files()``.
    total_time : float
        Общее время выполнения в секундах.
    """
    sep = "=" * 70
    print(f"\n{sep}")
    print("ИТОГОВЫЙ ОТЧЁТ")
    print(sep)

    for exch, label in [("bn", "BINANCE"), ("bb", "BYBIT"), ("okx", "OKX"), ("gate", "GATE.IO")]:
        st = r[f"{exch}_spot_total"]
        ft = r[f"{exch}_fut_total"]
        sa = r[f"{exch}_spot_active"]
        fa = r[f"{exch}_fut_active"]
        pct_s = sa / st * 100 if st else 0
        pct_f = fa / ft * 100 if ft else 0
        print(f"\n[{label}] REST API:  Spot={st}, Futures={ft}")
        print(f"[{label}] WS active: Spot={sa}/{st} ({pct_s:.1f}%), Futures={fa}/{ft} ({pct_f:.1f}%)")

    print(f"\n[КОМБИНАЦИИ] combination/")
    for _, _, fname in COMBINATIONS:
        cnt = comb_counts.get(fname, 0)
        print(f"   {cnt:4d} пар  {fname}")

    print(f"\n[ПОДПИСКА] subscribe/")
    for key, path in SUBSCRIBE_MAP.items():
        cnt = sub_counts.get(key, 0)
        print(f"   {cnt:4d} пар  {path.relative_to(BASE_DIR)}")

    print(f"\nОбщее время: {total_time:.2f} сек")
    print(sep + "\n")


# ──────────────────────────────────────────────────────────────────────────────
# Главная функция
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """
    Запускает полный цикл обновления торговых пар.

    Порядок выполнения
    ------------------
    1. **Фаза 1 — Параллельный REST** (все 4 биржи одновременно):
       Получение сырых списков пар через публичные REST API.
       Результаты сохраняются в ``<exchange>/data/``.

    2. **Фаза 2 — Параллельная WS-валидация** (все 4 биржи одновременно):
       Открытие WebSocket-соединений и 60-секундное наблюдение.
       Активными считаются пары, по которым пришёл хотя бы 1 тик.
       Результаты сохраняются в ``<exchange>/data/<exchange>_*_active.txt``.

    3. **Фаза 3 — Пересечения** (combination/):
       Построение файлов по схеме ``spot_A ∩ futures_B``.

    4. **Фаза 4 — Файлы подписки** (subscribe/):
       Агрегация пар по рынкам без дублей.

    5. **Фаза 5 — Отчёт**:
       Вывод статистики в stdout.
    """
    from okx.okx_pairs   import load_native as okx_load_native
    from gate.gate_pairs import load_native as gate_load_native

    r: dict = {}
    t0 = time.time()

    # ── Фаза 1: Параллельный REST ─────────────────────────────────────────────
    print("[Фаза 1/4] REST API: получение пар со всех бирж параллельно...", flush=True)
    t_rest = time.time()

    rest_results = _fetch_all_exchanges()

    bn_spot,       bn_fut       = rest_results["bn"]
    bb_spot,       bb_fut       = rest_results["bb"]
    okx_spot_norm, okx_fut_norm = rest_results["okx"]
    gate_spot_norm,gate_fut_norm= rest_results["gate"]

    r["bn_spot_total"]   = len(bn_spot)
    r["bn_fut_total"]    = len(bn_fut)
    r["bb_spot_total"]   = len(bb_spot)
    r["bb_fut_total"]    = len(bb_fut)
    r["okx_spot_total"]  = len(okx_spot_norm)
    r["okx_fut_total"]   = len(okx_fut_norm)
    r["gate_spot_total"] = len(gate_spot_norm)
    r["gate_fut_total"]  = len(gate_fut_norm)

    print(f"       Binance  — Spot: {len(bn_spot)}, Futures: {len(bn_fut)}")
    print(f"       Bybit    — Spot: {len(bb_spot)}, Futures: {len(bb_fut)}")
    print(f"       OKX      — Spot: {len(okx_spot_norm)}, Futures: {len(okx_fut_norm)}")
    print(f"       Gate.io  — Spot: {len(gate_spot_norm)}, Futures: {len(gate_fut_norm)}")
    print(f"       Время REST: {time.time() - t_rest:.1f} сек")

    # Нативные символы OKX и Gate.io нужны для WS-подписки
    okx_spot_native,  okx_fut_native  = okx_load_native()
    gate_spot_native, gate_fut_native = gate_load_native()

    # ── Фаза 2: Параллельная WS-валидация ────────────────────────────────────
    print(f"\n[Фаза 2/4] WS-валидация: все биржи параллельно ({DURATION_SECONDS} сек)...", flush=True)
    t_ws = time.time()

    (
        abn_spot,  abn_fut,
        abb_spot,  abb_fut,
        aokx_spot, aokx_fut,
        agate_spot,agate_fut,
    ) = asyncio.run(
        _validate_all_exchanges(
            bn_spot,          bn_fut,
            bb_spot,          bb_fut,
            okx_spot_native,  okx_fut_native,  okx_spot_norm,  okx_fut_norm,
            gate_spot_native, gate_fut_native, gate_spot_norm, gate_fut_norm,
            DURATION_SECONDS,
        )
    )

    r["bn_spot_active"]   = len(abn_spot)
    r["bn_fut_active"]    = len(abn_fut)
    r["bb_spot_active"]   = len(abb_spot)
    r["bb_fut_active"]    = len(abb_fut)
    r["okx_spot_active"]  = len(aokx_spot)
    r["okx_fut_active"]   = len(aokx_fut)
    r["gate_spot_active"] = len(agate_spot)
    r["gate_fut_active"]  = len(agate_fut)

    print(f"       Binance  — Spot: {len(abn_spot)}/{len(bn_spot)}, Futures: {len(abn_fut)}/{len(bn_fut)}")
    print(f"       Bybit    — Spot: {len(abb_spot)}/{len(bb_spot)}, Futures: {len(abb_fut)}/{len(bb_fut)}")
    print(f"       OKX      — Spot: {len(aokx_spot)}/{len(okx_spot_norm)}, Futures: {len(aokx_fut)}/{len(okx_fut_norm)}")
    print(f"       Gate.io  — Spot: {len(agate_spot)}/{len(gate_spot_norm)}, Futures: {len(agate_fut)}/{len(gate_fut_norm)}")
    print(f"       Время WS: {time.time() - t_ws:.0f} сек")

    # ── Фаза 3: Пересечения ───────────────────────────────────────────────────
    print("\n[Фаза 3/4] Создание пересечений (combination/)...", flush=True)
    active = {
        "binance_spot":    abn_spot,
        "binance_futures": abn_fut,
        "bybit_spot":      abb_spot,
        "bybit_futures":   abb_fut,
        "okx_spot":        aokx_spot,
        "okx_futures":     aokx_fut,
        "gate_spot":       agate_spot,
        "gate_futures":    agate_fut,
    }
    comb_counts = _make_combinations(active)
    for fname, cnt in comb_counts.items():
        print(f"       {cnt:4d} пар  {fname}")

    # ── Фаза 4: Файлы подписки ────────────────────────────────────────────────
    print("\n[Фаза 4/4] Создание файлов подписки (subscribe/)...", flush=True)
    sub_counts = _make_subscribe_files()
    for key, cnt in sub_counts.items():
        print(f"        {cnt:4d} пар  {key}")

    # ── Фаза 5: Отчёт ─────────────────────────────────────────────────────────
    _print_report(r, comb_counts, sub_counts, time.time() - t0)


if __name__ == "__main__":
    main()
