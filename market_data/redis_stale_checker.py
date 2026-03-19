#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/redis_stale_checker.py — Проверяет устаревшие (stale) тикерные ключи в Redis.

Проверяет только ключи формата:
  md:{exchange}:{market}:{symbol}

Примеры:
  md:binance:spot:BTCUSDT     md:binance:futures:BTCUSDT
  md:bybit:spot:ADAUSDT       md:bybit:futures:SOLUSDT
  md:okx:spot:BTCUSDT         md:okx:futures:ETHUSDT
  md:gate:spot:LTCUSDT        md:gate:futures:DOTUSDT

Ключ считается устаревшим, если поле `ts` (Unix timestamp последнего тика)
не обновлялось дольше STALE_THRESHOLD_S секунд.

Логи пишутся в JSON Lines формате (один JSON-объект на строку).

Конфигурация (переменные окружения):
  REDIS_HOST            — хост Redis               (по умолчанию: 127.0.0.1)
  REDIS_PORT            — порт Redis               (по умолчанию: 6379)
  REDIS_DB              — номер БД                 (по умолчанию: 0)
  REDIS_PASSWORD        — пароль                   (по умолчанию: нет)
  STALE_THRESHOLD_S     — порог устаревания, сек   (по умолчанию: 30)
  CHECK_INTERVAL_S      — интервал проверки, сек   (по умолчанию: 5)
  SNAPSHOT_INTERVAL_S   — интервал снапшотов, сек  (по умолчанию: 60)
  LOG_STALE_ONLY        — 1=только при stale ключах, 0=каждый цикл (по умолчанию: 0)
  STALE_LOG_DIR         — папка для логов          (по умолчанию: logs/redis_stale_checker)
  LOG_CHUNK_SECONDS     — размер лог-чанка, сек    (по умолчанию: 86400 = 24ч)
  LOG_MAX_CHUNKS        — кол-во хранимых чанков   (по умолчанию: 2)

Типы записей в логе:
  start       — запуск скрипта с конфигурацией
  connected   — успешное подключение к Redis
  check       — результат одного цикла проверки
  snapshot    — периодическая сводная статистика
  error       — ошибка соединения или Redis
  stopped     — остановка скрипта

Запуск:
  python market_data/redis_stale_checker.py
  STALE_THRESHOLD_S=10 CHECK_INTERVAL_S=2 python market_data/redis_stale_checker.py
  LOG_STALE_ONLY=1 STALE_THRESHOLD_S=60 python market_data/redis_stale_checker.py
"""

import asyncio
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import redis.asyncio as aioredis

# ─── Конфигурация из env ───────────────────────────────────────────────────────

REDIS_HOST     = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

STALE_THRESHOLD_S   = float(os.getenv("STALE_THRESHOLD_S",   "30"))
CHECK_INTERVAL_S    = float(os.getenv("CHECK_INTERVAL_S",    "5"))
SNAPSHOT_INTERVAL_S = float(os.getenv("SNAPSHOT_INTERVAL_S", "60"))
LOG_STALE_ONLY      = os.getenv("LOG_STALE_ONLY", "0") == "1"

LOG_DIR           = Path(os.getenv("STALE_LOG_DIR",
                         str(_PROJECT_ROOT / "logs" / "redis_stale_checker")))
LOG_CHUNK_SECONDS = int(os.getenv("LOG_CHUNK_SECONDS", str(86400)))
LOG_MAX_CHUNKS    = int(os.getenv("LOG_MAX_CHUNKS", "2"))


# ══════════════════════════════════════════════════════════════════════════════
# LogManager — ротирующий лог-файл (JSON Lines)
# ══════════════════════════════════════════════════════════════════════════════

class LogManager:
    """
    Лог-файл с ротацией по времени.

    Структура:
      logs/redis_stale_checker/
        20240319_120000-ongoing/stale_checker.log   ← текущий чанк
        20240318_120000-20240319_120000/             ← завершённый чанк
    """

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._chunk_start: float = time.time()
        self._chunk_dir: Optional[Path] = None
        self._file = None
        self._open_chunk()
        self._cleanup_old_chunks()

    @staticmethod
    def _ts() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    def _open_chunk(self) -> None:
        name = f"{self._ts()}-ongoing"
        self._chunk_dir = self.base_dir / name
        self._chunk_dir.mkdir(parents=True, exist_ok=True)
        self._file = open(self._chunk_dir / "stale_checker.log", "a", encoding="utf-8")
        self._chunk_start = time.time()

    def _close_chunk(self) -> None:
        if self._file:
            self._file.close()
            self._file = None
        if self._chunk_dir and self._chunk_dir.exists():
            new_name = self._chunk_dir.name.replace("-ongoing", f"-{self._ts()}")
            try:
                self._chunk_dir.rename(self._chunk_dir.parent / new_name)
                self._chunk_dir = self._chunk_dir.parent / new_name
            except Exception:
                pass

    def _cleanup_old_chunks(self) -> None:
        chunks = sorted(
            [d for d in self.base_dir.iterdir()
             if d.is_dir() and "-ongoing" not in d.name],
            key=lambda d: d.stat().st_mtime,
        )
        while len(chunks) > LOG_MAX_CHUNKS:
            shutil.rmtree(chunks.pop(0), ignore_errors=True)

    def write(self, line: str) -> None:
        if time.time() - self._chunk_start >= LOG_CHUNK_SECONDS:
            self._close_chunk()
            self._open_chunk()
            self._cleanup_old_chunks()
        if self._file:
            self._file.write(line + "\n")
            self._file.flush()

    def close(self) -> None:
        self._close_chunk()


# ══════════════════════════════════════════════════════════════════════════════
# Redis
# ══════════════════════════════════════════════════════════════════════════════

async def connect_redis() -> aioredis.Redis:
    """Создаёт async Redis-клиент с ретраями (5 попыток, exponential backoff)."""
    for attempt in range(5):
        try:
            client = aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30,
                decode_responses=True,
            )
            await client.ping()
            return client
        except Exception as exc:
            if attempt < 4:
                await asyncio.sleep(2 ** attempt)
            else:
                raise RuntimeError(
                    f"Не удалось подключиться к Redis после 5 попыток: {exc}"
                ) from exc


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    """Текущее время в ISO 8601 с миллисекундами (UTC)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _jdump(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _is_ticker_key(key: str) -> bool:
    """
    True если ключ является тикерным: md:{exchange}:{market}:{symbol}
    (ровно 4 части при разбивке по ':').

    Исключает:
      md:hist:...          — ключи истории (6+ частей)
      md:chunks:config     — метаданные чанков (3 части)
    """
    parts = key.split(":")
    return len(parts) == 4 and parts[0] == "md"


# ══════════════════════════════════════════════════════════════════════════════
# Сканирование и проверка
# ══════════════════════════════════════════════════════════════════════════════

async def get_ticker_keys(client: aioredis.Redis) -> list[str]:
    """
    Возвращает все тикерные ключи md:{exchange}:{market}:{symbol}
    через SCAN (неблокирующий итератор по всему keyspace).
    """
    keys: list[str] = []
    cursor = 0
    while True:
        cursor, batch = await client.scan(cursor, match="md:*", count=500)
        for k in batch:
            if _is_ticker_key(k):
                keys.append(k)
        if cursor == 0:
            break
    return keys


async def check_stale_keys(
    client: aioredis.Redis,
    keys: list[str],
    threshold_s: float,
    now: float,
) -> tuple[list[dict], list[dict]]:
    """
    Проверяет список тикерных ключей на устаревание.

    Читает поле `ts` из каждого Hash через pipeline (одним round-trip),
    сравнивает с текущим временем.

    Возвращает (stale_list, fresh_list).
    Каждый элемент: {key, exchange, market, symbol, last_ts, age_s, reason?}
    """
    if not keys:
        return [], []

    # Batch-чтение поля ts через pipeline
    pipe = client.pipeline(transaction=False)
    for k in keys:
        pipe.hget(k, "ts")
    ts_values: list = await pipe.execute()

    stale: list[dict] = []
    fresh: list[dict] = []

    for key, ts_raw in zip(keys, ts_values):
        parts = key.split(":")  # ['md', exchange, market, symbol]
        entry = {
            "key":      key,
            "exchange": parts[1],
            "market":   parts[2],
            "symbol":   parts[3],
        }

        if ts_raw is None:
            # Ключ существует, но поле ts отсутствует
            entry["last_ts"] = None
            entry["age_s"]   = None
            entry["reason"]  = "no_ts_field"
            stale.append(entry)
            continue

        try:
            ts_val = float(ts_raw)
        except (ValueError, TypeError):
            entry["last_ts"] = ts_raw
            entry["age_s"]   = None
            entry["reason"]  = "invalid_ts"
            stale.append(entry)
            continue

        age_s = now - ts_val
        entry["last_ts"] = round(ts_val, 3)
        entry["age_s"]   = round(age_s, 3)

        if age_s > threshold_s:
            entry["reason"] = "expired"
            stale.append(entry)
        else:
            fresh.append(entry)

    return stale, fresh


# ══════════════════════════════════════════════════════════════════════════════
# Главный цикл
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    log = LogManager(LOG_DIR)

    start_rec = _jdump({
        "type":               "start",
        "ts":                 _now_iso(),
        "host":               f"{REDIS_HOST}:{REDIS_PORT}",
        "db":                 REDIS_DB,
        "stale_threshold_s":  STALE_THRESHOLD_S,
        "check_interval_s":   CHECK_INTERVAL_S,
        "snapshot_interval_s": SNAPSHOT_INTERVAL_S,
        "log_stale_only":     LOG_STALE_ONLY,
        "log_dir":            str(LOG_DIR),
    })
    log.write(start_rec)
    print(start_rec)

    client: Optional[aioredis.Redis] = None
    consecutive_errors  = 0
    start_ts            = time.time()
    checks_total        = 0
    stale_events_total  = 0   # число циклов, где найден хотя бы один stale ключ
    last_snapshot_ts    = time.time()

    try:
        while True:
            loop_start = time.monotonic()

            try:
                # ── Соединение ────────────────────────────────────────────────
                if client is None:
                    client = await connect_redis()
                    conn_rec = _jdump({
                        "type": "connected",
                        "ts":   _now_iso(),
                        "host": f"{REDIS_HOST}:{REDIS_PORT}",
                        "db":   REDIS_DB,
                    })
                    log.write(conn_rec)
                    print(conn_rec)
                    consecutive_errors = 0

                # ── Сканирование и проверка ────────────────────────────────────
                t0  = time.perf_counter()
                now = time.time()

                ticker_keys        = await get_ticker_keys(client)
                stale_list, fresh_list = await check_stale_keys(
                    client, ticker_keys, STALE_THRESHOLD_S, now
                )

                elapsed_ms  = round((time.perf_counter() - t0) * 1000, 2)
                checks_total += 1
                has_stale    = len(stale_list) > 0

                if has_stale:
                    stale_events_total += 1

                # ── Запись результата цикла ────────────────────────────────────
                if not LOG_STALE_ONLY or has_stale:
                    check_rec = _jdump({
                        "type":          "check",
                        "ts":            _now_iso(),
                        "elapsed_ms":    elapsed_ms,
                        "total_keys":    len(ticker_keys),
                        "fresh":         len(fresh_list),
                        "stale":         len(stale_list),
                        "stale_details": stale_list,
                    })
                    log.write(check_rec)
                    print(check_rec)

                # ── Периодический снапшот ──────────────────────────────────────
                now_t = time.time()
                if now_t - last_snapshot_ts >= SNAPSHOT_INTERVAL_S:
                    snap_rec = _jdump({
                        "type":               "snapshot",
                        "ts":                 _now_iso(),
                        "uptime_s":           round(now_t - start_ts, 1),
                        "checks_total":       checks_total,
                        "stale_events_total": stale_events_total,
                        "config": {
                            "stale_threshold_s":  STALE_THRESHOLD_S,
                            "check_interval_s":   CHECK_INTERVAL_S,
                            "log_stale_only":     LOG_STALE_ONLY,
                        },
                    })
                    log.write(snap_rec)
                    print(snap_rec)
                    last_snapshot_ts = now_t

            except (ConnectionError, OSError) as exc:
                consecutive_errors += 1
                err_rec = _jdump({
                    "type":       "error",
                    "ts":         _now_iso(),
                    "attempt":    consecutive_errors,
                    "error_type": type(exc).__name__,
                    "message":    str(exc),
                })
                print(err_rec, file=sys.stderr)
                log.write(err_rec)
                try:
                    if client:
                        await client.aclose()
                except Exception:
                    pass
                client  = None
                backoff = min(2 ** (consecutive_errors - 1), 30)
                await asyncio.sleep(backoff)
                continue

            except aioredis.RedisError as exc:
                consecutive_errors += 1
                err_rec = _jdump({
                    "type":       "error",
                    "ts":         _now_iso(),
                    "attempt":    consecutive_errors,
                    "error_type": type(exc).__name__,
                    "message":    str(exc),
                })
                print(err_rec, file=sys.stderr)
                log.write(err_rec)
                await asyncio.sleep(min(2 ** (consecutive_errors - 1), 30))
                continue

            # ── Ждём следующего цикла ─────────────────────────────────────────
            elapsed = time.monotonic() - loop_start
            await asyncio.sleep(max(0.0, CHECK_INTERVAL_S - elapsed))

    except (asyncio.CancelledError, KeyboardInterrupt):
        pass

    finally:
        stop_rec = _jdump({
            "type":               "stopped",
            "ts":                 _now_iso(),
            "uptime_s":           round(time.time() - start_ts, 1),
            "checks_total":       checks_total,
            "stale_events_total": stale_events_total,
        })
        log.write(stop_rec)
        print(f"\n{stop_rec}")
        log.close()
        if client:
            try:
                await client.aclose()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
