#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/tests/load_test.py — Нагрузочные тесты Redis-слоя.

Запуск:
    python3 market_data/tests/load_test.py

Переменные окружения (те же, что и у сборщиков):
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

Тесты:
    1. Pipeline write       — запись N тикеров через pipeline (HSET)
    2. Pipeline read        — чтение N тикеров через pipeline (HGETALL)
    3. History RPUSH        — пакетная запись в List (история)
    4. SCAN throughput      — сканирование md:test:* ключей
    5. Concurrent writers   — параллельная запись из K корутин
    6. Chunk rotation       — запись + ротация чанков (реальный паттерн)
"""

import asyncio
import os
import sys
import time
from pathlib import Path
from typing import List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common import create_redis

# ─── Конфигурация тестов ──────────────────────────────────────────────────────

TEST_PREFIX    = "md:test:"           # Все ключи теста пишутся сюда
SYMBOLS_SMALL  = 100                  # Малая нагрузка
SYMBOLS_MEDIUM = 500                  # Средняя
SYMBOLS_LARGE  = 2000                 # Большая
HISTORY_BATCH  = 1000                 # Записей истории за раз
CONCURRENT_K   = 8                    # Параллельных writer-корутин
ITERATIONS     = 5                    # Повторов каждого теста (берём медиану)

# ─── Утилиты ──────────────────────────────────────────────────────────────────

def _make_symbols(n: int) -> List[str]:
    """Генерирует N символов: SYM0001USDT, SYM0002USDT, ..."""
    return [f"SYM{i:04d}USDT" for i in range(1, n + 1)]


def _percentile(data: List[float], p: float) -> float:
    if not data:
        return 0.0
    data = sorted(data)
    idx = int(len(data) * p / 100)
    return data[min(idx, len(data) - 1)]


def _fmt(ms: float) -> str:
    return f"{ms:.2f}ms"


async def _cleanup(redis, prefix: str = TEST_PREFIX) -> int:
    """Удаляет все ключи с префиксом. Возвращает кол-во удалённых."""
    cursor, count = b"0", 0
    while True:
        cursor, keys = await redis.scan(cursor, match=f"{prefix}*", count=500)
        if keys:
            await redis.delete(*keys)
            count += len(keys)
        if cursor in (b"0", 0):
            break
    return count


def _print_result(name: str, times_ms: List[float], extra: str = "") -> None:
    mn  = min(times_ms)
    avg = sum(times_ms) / len(times_ms)
    p95 = _percentile(times_ms, 95)
    mx  = max(times_ms)
    flag = "  ⚠" if avg > 20 else ""
    print(
        f"  {name:<40} "
        f"min={_fmt(mn):>10}  avg={_fmt(avg):>10}  "
        f"p95={_fmt(p95):>10}  max={_fmt(mx):>10}"
        f"{flag}  {extra}"
    )


# ─── Тест 1: Pipeline write (HSET) ───────────────────────────────────────────

async def test_pipeline_write(redis, n_symbols: int) -> List[float]:
    """Запись N тикеров одним pipeline за ITERATIONS итераций."""
    symbols = _make_symbols(n_symbols)
    times = []

    for _ in range(ITERATIONS):
        ts = f"{time.time():.3f}"
        pipe = redis.pipeline(transaction=False)
        for sym in symbols:
            key = f"{TEST_PREFIX}ticker:{sym}"
            pipe.hset(key, mapping={"bid": "45000.12", "ask": "45000.13", "ts": ts})
        t0 = time.perf_counter()
        await pipe.execute()
        times.append((time.perf_counter() - t0) * 1000)

    return times


# ─── Тест 2: Pipeline read (HGETALL) ─────────────────────────────────────────

async def test_pipeline_read(redis, n_symbols: int) -> List[float]:
    """Чтение N тикеров через pipeline (HGETALL)."""
    symbols = _make_symbols(n_symbols)
    # Убедиться, что ключи существуют
    pipe = redis.pipeline(transaction=False)
    for sym in symbols:
        key = f"{TEST_PREFIX}ticker:{sym}"
        pipe.hset(key, mapping={"bid": "45000.12", "ask": "45000.13", "ts": "1234567890.000"})
    await pipe.execute()

    times = []
    for _ in range(ITERATIONS):
        pipe = redis.pipeline(transaction=False)
        for sym in symbols:
            pipe.hgetall(f"{TEST_PREFIX}ticker:{sym}")
        t0 = time.perf_counter()
        results = await pipe.execute()
        times.append((time.perf_counter() - t0) * 1000)

    return times


# ─── Тест 3: History RPUSH ────────────────────────────────────────────────────

async def test_history_rpush(redis, n_entries: int) -> List[float]:
    """Пакетная запись N записей истории в один List."""
    key = f"{TEST_PREFIX}hist:binance:spot:BTCUSDT:1"
    entries = [f"{time.time() + i:.3f},45000.12,45000.13" for i in range(n_entries)]
    times = []

    for _ in range(ITERATIONS):
        await redis.delete(key)
        pipe = redis.pipeline(transaction=False)
        for entry in entries:
            pipe.rpush(key, entry)
        t0 = time.perf_counter()
        await pipe.execute()
        times.append((time.perf_counter() - t0) * 1000)

    return times


# ─── Тест 4: SCAN throughput ─────────────────────────────────────────────────

async def test_scan(redis, n_symbols: int) -> List[float]:
    """Сканирование md:test:* ключей после создания N тикеров."""
    symbols = _make_symbols(n_symbols)
    pipe = redis.pipeline(transaction=False)
    for sym in symbols:
        pipe.hset(f"{TEST_PREFIX}ticker:{sym}", mapping={"bid": "1", "ask": "1", "ts": "1"})
    await pipe.execute()

    times = []
    for _ in range(ITERATIONS):
        cursor = b"0"
        found  = 0
        t0 = time.perf_counter()
        while True:
            cursor, keys = await redis.scan(cursor, match=f"{TEST_PREFIX}*", count=500)
            found += len(keys)
            if cursor in (b"0", 0):
                break
        times.append((time.perf_counter() - t0) * 1000)

    return times


# ─── Тест 5: Concurrent writers ───────────────────────────────────────────────

async def _writer(redis, symbols: List[str], iterations: int) -> List[float]:
    times = []
    ts = f"{time.time():.3f}"
    for _ in range(iterations):
        pipe = redis.pipeline(transaction=False)
        for sym in symbols:
            pipe.hset(f"{TEST_PREFIX}concurrent:{sym}", mapping={"bid": "1", "ask": "2", "ts": ts})
        t0 = time.perf_counter()
        await pipe.execute()
        times.append((time.perf_counter() - t0) * 1000)
    return times


async def test_concurrent_write(redis, n_symbols: int, k_writers: int) -> List[float]:
    """K параллельных writer-корутин, каждая пишет n_symbols/K тикеров."""
    chunk = n_symbols // k_writers
    symbols_chunks = [_make_symbols(n_symbols)[i * chunk:(i + 1) * chunk] for i in range(k_writers)]
    tasks = [asyncio.create_task(_writer(redis, chunk_sym, ITERATIONS)) for chunk_sym in symbols_chunks]
    results = await asyncio.gather(*tasks)

    # Объединяем времена всех воркеров (показываем p95 по всем батчам)
    all_times: List[float] = []
    for r in results:
        all_times.extend(r)
    return all_times


# ─── Тест 6: Chunk rotation pattern ─────────────────────────────────────────

async def test_chunk_rotation(redis, n_symbols: int) -> List[float]:
    """
    Имитация паттерна ротации чанков:
    - Запись тикеров (ticker flusher, каждые 50ms эмулируем 5 итераций)
    - Запись истории (history flusher, каждые 1s)
    - Удаление старого чанка
    - Создание нового
    """
    symbols = _make_symbols(n_symbols)
    chunk_id = 1
    times = []

    for iteration in range(ITERATIONS):
        t0 = time.perf_counter()

        # 1. Ticker write (как в _ticker_flusher)
        pipe = redis.pipeline(transaction=False)
        ts   = f"{time.time():.3f}"
        for sym in symbols:
            pipe.hset(f"{TEST_PREFIX}ticker:{sym}", mapping={"bid": "45000.12", "ask": "45000.13", "ts": ts})
        await pipe.execute()

        # 2. History write (как в _history_flusher)
        pipe = redis.pipeline(transaction=False)
        entry = f"{time.time():.3f},45000.12,45000.13"
        for sym in symbols:
            pipe.rpush(f"{TEST_PREFIX}hist:{sym}:{chunk_id}", entry)
        await pipe.execute()

        # 3. Chunk rotation: удалить старый, обновить метаданные
        if iteration > 0:
            old_chunk = chunk_id - 1
            pipe = redis.pipeline(transaction=False)
            for sym in symbols:
                pipe.delete(f"{TEST_PREFIX}hist:{sym}:{old_chunk}")
            pipe.hset(f"{TEST_PREFIX}chunks:config", "test:spot", f'{{"current_chunk_id":{chunk_id}}}')
            await pipe.execute()

        chunk_id += 1
        times.append((time.perf_counter() - t0) * 1000)

    return times


# ─── Вывод заголовков ─────────────────────────────────────────────────────────

def _header(title: str) -> None:
    print(f"\n{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    print("=" * 70)
    print("  market_data — Redis Load Tests")
    print(f"  REDIS: {os.getenv('REDIS_HOST','127.0.0.1')}:{os.getenv('REDIS_PORT','6379')}  "
          f"DB={os.getenv('REDIS_DB','0')}")
    print(f"  Iterations per test: {ITERATIONS}")
    print("=" * 70)

    redis = await create_redis()

    # Чистим тестовые ключи перед стартом
    n_cleaned = await _cleanup(redis)
    if n_cleaned:
        print(f"\n[setup] Очищено {n_cleaned} тестовых ключей из предыдущего запуска.")

    try:
        # ── Тест 1: Pipeline write ────────────────────────────────────────────
        _header("1. Pipeline write (HSET) — запись тикеров")
        for n in (SYMBOLS_SMALL, SYMBOLS_MEDIUM, SYMBOLS_LARGE):
            times = await test_pipeline_write(redis, n)
            _print_result(f"write {n:>5} symbols", times, f"≈ {n * 3} cmds/batch")

        # ── Тест 2: Pipeline read ─────────────────────────────────────────────
        _header("2. Pipeline read (HGETALL) — чтение тикеров")
        for n in (SYMBOLS_SMALL, SYMBOLS_MEDIUM, SYMBOLS_LARGE):
            times = await test_pipeline_read(redis, n)
            _print_result(f"read  {n:>5} symbols", times)

        # ── Тест 3: History RPUSH ─────────────────────────────────────────────
        _header("3. History RPUSH — запись в List")
        for n in (100, HISTORY_BATCH, HISTORY_BATCH * 5):
            times = await test_history_rpush(redis, n)
            _print_result(f"rpush {n:>5} entries", times)

        # ── Тест 4: SCAN ──────────────────────────────────────────────────────
        _header("4. SCAN throughput — сканирование md:test:* ключей")
        for n in (SYMBOLS_SMALL, SYMBOLS_MEDIUM, SYMBOLS_LARGE):
            times = await test_scan(redis, n)
            _print_result(f"scan  {n:>5} keys", times)

        # ── Тест 5: Concurrent writers ────────────────────────────────────────
        _header(f"5. Concurrent writers — {CONCURRENT_K} параллельных coroutine")
        for n in (SYMBOLS_MEDIUM, SYMBOLS_LARGE):
            times = await test_concurrent_write(redis, n, CONCURRENT_K)
            _print_result(
                f"concurrent {CONCURRENT_K}x{n//CONCURRENT_K} symbols",
                times,
                f"total={n} symbols"
            )

        # ── Тест 6: Chunk rotation pattern ────────────────────────────────────
        _header("6. Chunk rotation pattern — реальный паттерн flush + rotate")
        for n in (SYMBOLS_SMALL, SYMBOLS_MEDIUM):
            times = await test_chunk_rotation(redis, n)
            _print_result(f"rotate {n:>5} symbols", times, "ticker+hist+delete")

        # ── Итог ─────────────────────────────────────────────────────────────
        print("\n" + "=" * 70)
        print("  Пороговые значения:")
        print("    Pipeline write 500 sym avg < 5ms  — норма")
        print("    Pipeline write 500 sym avg > 20ms — Redis перегружен")
        print("    SCAN 2000 keys  avg < 50ms         — норма")
        print("    SCAN 2000 keys  avg > 200ms        — проблема с Redis")
        print("=" * 70)

    finally:
        n_cleaned = await _cleanup(redis)
        print(f"\n[cleanup] Удалено {n_cleaned} тестовых ключей.")
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
