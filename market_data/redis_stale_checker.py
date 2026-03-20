#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/redis_stale_checker.py — Проверяет устаревшие (stale) тикерные ключи в Redis.

Проверяет только ключи формата:
  md:{exchange}:{market}:{symbol}

Ключ считается устаревшим, если поле `ts` не обновлялось дольше STALE_THRESHOLD_S секунд.

══════════════════════════════════════════════════════════════
ФАЙЛЫ КОРЗИН (BASKET FILES)
══════════════════════════════════════════════════════════════
Каждые CHECK_INTERVAL_S (5 сек) скрипт группирует все stale-ключи
по текущему возрасту (age_s) и пишет в stale_baskets/:

  stale_1-5s.txt      — ключи с age_s ∈ [1, 5)
  stale_5-10s.txt     — ключи с age_s ∈ [5, 10)
  stale_10-15s.txt    — ключи с age_s ∈ [10, 15)
  ...
  stale_355-360s.txt  — ключи с age_s ∈ [355, 360)
  stale_360+s.txt     — ключи с age_s ≥ 360

Формат содержимого файла (по одному на строку, sorted):
  exchange:market:symbol

Файлы перезаписываются каждый цикл. Пустые файлы удаляются.
══════════════════════════════════════════════════════════════

Конфигурация (переменные окружения):
  REDIS_HOST            — хост Redis               (по умолчанию: 127.0.0.1)
  REDIS_PORT            — порт Redis               (по умолчанию: 6379)
  REDIS_DB              — номер БД                 (по умолчанию: 0)
  REDIS_PASSWORD        — пароль                   (по умолчанию: нет)
  STALE_THRESHOLD_S     — порог устаревания, сек   (по умолчанию: 30)
  CHECK_INTERVAL_S      — интервал проверки, сек   (по умолчанию: 5)
  SNAPSHOT_INTERVAL_S   — интервал снапшотов, сек  (по умолчанию: 60)
  LOG_STALE_ONLY        — 1=только при stale, 0=каждый цикл (по умолчанию: 0)
  STALE_LOG_DIR         — папка для логов          (по умолчанию: logs/redis_stale_checker)
  LOG_CHUNK_SECONDS     — размер лог-чанка, сек    (по умолчанию: 86400 = 24ч)
  LOG_MAX_CHUNKS        — кол-во хранимых чанков   (по умолчанию: 2)
  WRITE_BASKETS         — 1=писать корзины, 0=нет  (по умолчанию: 1)
  BASKET_MIN_AGE_S      — мин. возраст для корзин  (по умолчанию: 1)
  BASKET_STEP_S         — шаг диапазонов, сек      (по умолчанию: 5)
  BASKET_MAX_S          — макс. граница диапазонов (по умолчанию: 360)
  BASKETS_DIR           — папка для файлов корзин  (по умолчанию: stale_baskets/)

Типы записей в логе:
  start       — запуск скрипта с конфигурацией
  connected   — успешное подключение к Redis
  check       — результат одного цикла проверки
  snapshot    — периодическая сводная статистика
  error       — ошибка соединения или Redis
  stopped     — остановка скрипта
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

# ─── Корзины ──────────────────────────────────────────────────────────────────

WRITE_BASKETS    = os.getenv("WRITE_BASKETS",   "1") == "1"
BASKET_MIN_AGE_S = float(os.getenv("BASKET_MIN_AGE_S", "1"))   # мин. возраст stale
BASKET_STEP_S    = int(os.getenv("BASKET_STEP_S",   "5"))       # шаг диапазона
BASKET_MAX_S     = int(os.getenv("BASKET_MAX_S",  "360"))       # граница последнего диапазона
BASKETS_DIR      = Path(os.getenv("BASKETS_DIR",
                        str(_PROJECT_ROOT / "stale_baskets")))


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _jdump(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _is_ticker_key(key: str) -> bool:
    """True для тикерных ключей md:{exchange}:{market}:{symbol} (ровно 4 части)."""
    parts = key.split(":")
    return len(parts) == 4 and parts[0] == "md"


def _bucket_name(age_s: float) -> str:
    """
    Возвращает имя диапазона для заданного возраста stale.

    age_s ∈ [1, 5)   → "1-5s"
    age_s ∈ [5, 10)  → "5-10s"
    ...
    age_s ∈ [355, 360) → "355-360s"
    age_s ≥ 360        → "360+s"
    """
    if age_s >= BASKET_MAX_S:
        return f"{BASKET_MAX_S}+s"
    step_idx = int(age_s / BASKET_STEP_S)
    lo = step_idx * BASKET_STEP_S
    hi = lo + BASKET_STEP_S
    # Первый диапазон начинается с BASKET_MIN_AGE_S, а не с 0
    if lo < BASKET_MIN_AGE_S:
        lo = int(BASKET_MIN_AGE_S)
    return f"{lo}-{hi}s"


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
# Сканирование и получение возрастов тикерных ключей
# ══════════════════════════════════════════════════════════════════════════════

async def get_ticker_keys(client: aioredis.Redis) -> list[str]:
    """SCAN md:* → только тикерные ключи с ровно 4 частями."""
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


async def fetch_key_ages(
    client: aioredis.Redis,
    keys: list[str],
    now: float,
) -> list[dict]:
    """
    Batch-чтение поля `ts` через pipeline (один round-trip).

    Возвращает список dict для каждого ключа:
      {key, exchange, market, symbol, age_s, last_ts}
      age_s = None если ts отсутствует или невалидно (+ поле reason)
    """
    if not keys:
        return []

    pipe = client.pipeline(transaction=False)
    for k in keys:
        pipe.hget(k, "ts")
    ts_values: list = await pipe.execute()

    result: list[dict] = []
    for key, ts_raw in zip(keys, ts_values):
        parts = key.split(":")
        entry: dict = {
            "key":      key,
            "exchange": parts[1],
            "market":   parts[2],
            "symbol":   parts[3],
        }

        if ts_raw is None:
            entry["last_ts"] = None
            entry["age_s"]   = None
            entry["reason"]  = "no_ts_field"
            result.append(entry)
            continue

        try:
            ts_val = float(ts_raw)
        except (ValueError, TypeError):
            entry["last_ts"] = ts_raw
            entry["age_s"]   = None
            entry["reason"]  = "invalid_ts"
            result.append(entry)
            continue

        entry["last_ts"] = round(ts_val, 3)
        entry["age_s"]   = round(now - ts_val, 3)
        result.append(entry)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Запись файлов корзин
# ══════════════════════════════════════════════════════════════════════════════

def write_bucket_files(entries: list[dict]) -> None:
    """
    Группирует stale-ключи по диапазонам age_s и пишет файлы в BASKETS_DIR.

    Алгоритм:
      1. Удалить существующие stale_*.txt (устаревшее состояние)
      2. Сгруппировать записи по _bucket_name(age_s)
      3. Записать непустые файлы: stale_{lo}-{hi}s.txt
      4. Формат: exchange:market:symbol (по одному на строку, sorted)
    """
    BASKETS_DIR.mkdir(parents=True, exist_ok=True)

    # Очистить прошлый цикл
    for old in BASKETS_DIR.glob("stale_*.txt"):
        try:
            old.unlink()
        except OSError:
            pass

    # Сгруппировать по диапазону
    buckets: dict[str, list[str]] = {}
    for e in entries:
        age_s = e.get("age_s")
        if age_s is None or age_s < BASKET_MIN_AGE_S:
            continue
        bname = _bucket_name(age_s)
        coin  = f"{e['exchange']}:{e['market']}:{e['symbol']}"
        buckets.setdefault(bname, []).append(coin)

    # Записать файлы (только непустые)
    for bname, coins in buckets.items():
        fpath = BASKETS_DIR / f"stale_{bname}.txt"
        fpath.write_text(
            "\n".join(sorted(set(coins))) + "\n",
            encoding="utf-8",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Главный цикл
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    log = LogManager(LOG_DIR)

    start_rec = _jdump({
        "type":                "start",
        "ts":                  _now_iso(),
        "host":                f"{REDIS_HOST}:{REDIS_PORT}",
        "db":                  REDIS_DB,
        "stale_threshold_s":   STALE_THRESHOLD_S,
        "check_interval_s":    CHECK_INTERVAL_S,
        "snapshot_interval_s": SNAPSHOT_INTERVAL_S,
        "log_stale_only":      LOG_STALE_ONLY,
        "write_baskets":       WRITE_BASKETS,
        "basket_min_age_s":    BASKET_MIN_AGE_S if WRITE_BASKETS else None,
        "basket_step_s":       BASKET_STEP_S    if WRITE_BASKETS else None,
        "basket_max_s":        BASKET_MAX_S     if WRITE_BASKETS else None,
        "baskets_dir":         str(BASKETS_DIR) if WRITE_BASKETS else None,
        "log_dir":             str(LOG_DIR),
    })
    log.write(start_rec)
    print(start_rec)

    client: Optional[aioredis.Redis] = None
    consecutive_errors  = 0
    start_ts            = time.time()
    checks_total        = 0
    stale_events_total  = 0
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

                # ── Сканирование и получение возрастов ────────────────────────
                t0  = time.perf_counter()
                now = time.time()

                ticker_keys = await get_ticker_keys(client)
                all_data    = await fetch_key_ages(client, ticker_keys, now)

                elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
                checks_total += 1

                # Разделить на stale / fresh по основному порогу (для лога)
                stale_list = [
                    e for e in all_data
                    if e.get("age_s") is None or e["age_s"] > STALE_THRESHOLD_S
                ]
                fresh_list = [
                    e for e in all_data
                    if e.get("age_s") is not None and e["age_s"] <= STALE_THRESHOLD_S
                ]
                has_stale = len(stale_list) > 0
                if has_stale:
                    stale_events_total += 1

                # ── Лог цикла ─────────────────────────────────────────────────
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

                # ── Файлы корзин ──────────────────────────────────────────────
                if WRITE_BASKETS:
                    # Все ключи с age_s >= BASKET_MIN_AGE_S попадают в корзины
                    bucket_entries = [
                        e for e in all_data
                        if e.get("age_s") is not None and e["age_s"] >= BASKET_MIN_AGE_S
                    ]
                    write_bucket_files(bucket_entries)

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
                            "stale_threshold_s": STALE_THRESHOLD_S,
                            "check_interval_s":  CHECK_INTERVAL_S,
                            "log_stale_only":    LOG_STALE_ONLY,
                            "write_baskets":     WRITE_BASKETS,
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
