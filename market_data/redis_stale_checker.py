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

Ключ считается устаревшим, если поле `ts` не обновлялось дольше STALE_THRESHOLD_S секунд.

══════════════════════════════════════════════════════════════
КОРЗИНЫ STALE (BASKET LISTS)
══════════════════════════════════════════════════════════════
Каждые CHECK_INTERVAL_S (5 сек) скрипт проверяет все тикерные ключи.
Монета попадает в корзину, если за последние BASKET_WINDOW_S секунд (30 мин)
она была stale не менее BASKET_STEP раз (или кратно BASKET_STEP):

  basket_5:  ≥ 5  stale-событий за 30 мин
  basket_10: ≥ 10 stale-событий за 30 мин
  basket_15: ≥ 15 stale-событий за 30 мин
  basket_20: ≥ 20 stale-событий за 30 мин
  ...

Свойства корзин:
  - Аддитивность: монета только добавляется, никогда не удаляется из корзины
  - Без дублей: каждая монета присутствует один раз
  - Частота stale: `stale_count_30m` и `stale_rate_pct` обновляются в реальном времени

Файл состояния: stale_baskets/stale_baskets.json (перезаписывается каждый цикл)
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
  BASKET_WINDOW_S       — окно для корзин, сек     (по умолчанию: 1800 = 30 мин)
  BASKET_STEP           — шаг порогов корзин       (по умолчанию: 5)
  BASKETS_DIR           — папка для файла корзин   (по умолчанию: stale_baskets/)

Типы записей в логе:
  start          — запуск скрипта с конфигурацией
  connected      — успешное подключение к Redis
  check          — результат одного цикла проверки
  basket_added   — монета добавлена в корзину
  snapshot       — периодическая сводная статистика
  error          — ошибка соединения или Redis
  stopped        — остановка скрипта

Запуск:
  python market_data/redis_stale_checker.py
  STALE_THRESHOLD_S=10 CHECK_INTERVAL_S=2 python market_data/redis_stale_checker.py
  WRITE_BASKETS=0 python market_data/redis_stale_checker.py
"""

import asyncio
import json
import os
import shutil
import sys
import time
from collections import deque
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

WRITE_BASKETS   = os.getenv("WRITE_BASKETS",  "1") == "1"
BASKET_WINDOW_S = float(os.getenv("BASKET_WINDOW_S", "1800"))   # 30 мин
BASKET_STEP     = int(os.getenv("BASKET_STEP", "5"))
BASKETS_DIR     = Path(os.getenv("BASKETS_DIR",
                        str(_PROJECT_ROOT / "stale_baskets")))


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _ts_to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"


def _jdump(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _is_ticker_key(key: str) -> bool:
    """
    True для тикерных ключей md:{exchange}:{market}:{symbol} (ровно 4 части).
    Исключает: md:hist:* (история), md:chunks:config (метаданные).
    """
    parts = key.split(":")
    return len(parts) == 4 and parts[0] == "md"


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
# BasketTracker — корзины монет с повторяющимися stale-событиями
# ══════════════════════════════════════════════════════════════════════════════

class BasketTracker:
    """
    Отслеживает монеты с повторяющимися stale-событиями в скользящем окне.

    Алгоритм:
      1. Каждый цикл (5 сек) получает множество stale-ключей → пишет в deque
      2. Удаляет из deque записи старше BASKET_WINDOW_S (30 мин)
      3. Поддерживает инкрементальный счётчик stale-событий на ключ
      4. Как только ключ достигает нового кратного BASKET_STEP порога —
         добавляет его в соответствующую корзину (аддитивно, без дублей)

    Пример: ключ stale 7 раз → в basket_5
            ключ stale 12 раз → в basket_5 и basket_10
            ключ stale 8 раз  → остаётся в basket_5 и basket_10 (аддитивно)
    """

    def __init__(
        self,
        window_s: float = 1800.0,
        step: int = 5,
        check_interval_s: float = 5.0,
    ) -> None:
        self.window_s         = window_s
        self.step             = step
        self.check_interval_s = check_interval_s
        # Теоретический максимум проверок в окне (для расчёта rate)
        self.max_checks       = max(1, int(window_s / check_interval_s))

        # Скользящее окно: (timestamp, frozenset_of_stale_keys)
        self._window: deque[tuple[float, frozenset]] = deque()

        # Текущий счётчик stale-событий на ключ (в окне)
        self._counts: dict[str, int] = {}

        # Метаданные ключей: exchange, market, symbol, first_seen_ts, last_stale_ts
        self._key_info: dict[str, dict] = {}

        # Максимальный порог, которого уже достиг ключ (оптимизация: не перебирать все пороги)
        self._key_max_threshold: dict[str, int] = {}

        # Корзины: порог (int) → список записей (аддитивный, без дублей)
        self._basket_lists: dict[int, list[dict]] = {}
        # Корзины: порог → set ключей (для быстрой проверки дублей)
        self._basket_sets: dict[int, set[str]] = {}

    # ── Запись нового цикла ──────────────────────────────────────────────────

    def record(self, ts: float, stale_entries: list[dict]) -> list[dict]:
        """
        Зафиксировать stale-события текущего цикла.

        stale_entries: список dict с полями key, exchange, market, symbol
        Возвращает: список новых добавлений в корзины — [{threshold, entry}, ...]
        """
        key_set = frozenset(e["key"] for e in stale_entries)

        # Обновить метаданные ключей
        for e in stale_entries:
            k = e["key"]
            if k not in self._key_info:
                self._key_info[k] = {
                    "exchange":      e["exchange"],
                    "market":        e["market"],
                    "symbol":        e["symbol"],
                    "first_seen_ts": ts,
                }
            self._key_info[k]["last_stale_ts"] = ts

        # Добавить в окно и обновить счётчики
        self._window.append((ts, key_set))
        for k in key_set:
            self._counts[k] = self._counts.get(k, 0) + 1

        # Вытолкнуть устаревшие записи (старше window_s)
        cutoff = ts - self.window_s
        while self._window and self._window[0][0] < cutoff:
            _, old_set = self._window.popleft()
            for k in old_set:
                if k in self._counts:
                    self._counts[k] -= 1
                    if self._counts[k] <= 0:
                        del self._counts[k]

        return self._update_baskets(ts)

    def _update_baskets(self, ts: float) -> list[dict]:
        """
        Добавить ключи, достигшие нового порога, в соответствующие корзины.
        Возвращает список новых добавлений: [{threshold, entry}, ...]
        """
        additions = []
        for key, count in self._counts.items():
            # Максимальный кратный порог, который даёт текущий счёт
            top_threshold = (count // self.step) * self.step
            if top_threshold < self.step:
                continue

            prev_max = self._key_max_threshold.get(key, 0)
            if top_threshold <= prev_max:
                continue  # уже добавлен во все нужные корзины

            # Добавить во все новые пороги от (prev_max + step) до top_threshold
            for thr in range(prev_max + self.step, top_threshold + 1, self.step):
                if thr not in self._basket_sets:
                    self._basket_sets[thr]  = set()
                    self._basket_lists[thr] = []

                if key not in self._basket_sets[thr]:
                    self._basket_sets[thr].add(key)
                    info  = self._key_info.get(key, {})
                    entry = {
                        "key":                key,
                        "exchange":           info.get("exchange", ""),
                        "market":             info.get("market", ""),
                        "symbol":             info.get("symbol", ""),
                        "stale_count_30m":    count,
                        "stale_rate_pct":     round(count / self.max_checks * 100, 2),
                        "added_to_basket_at": _ts_to_iso(ts),
                        "last_stale_at":      _ts_to_iso(
                            info.get("last_stale_ts", ts)
                        ),
                    }
                    self._basket_lists[thr].append(entry)
                    additions.append({"threshold": thr, "entry": entry})

            self._key_max_threshold[key] = top_threshold

        return additions

    # ── Состояние корзин ──────────────────────────────────────────────────────

    def get_state(self) -> dict:
        """
        Текущее состояние всех корзин с актуальными счётчиками.
        Возвращает dict для записи в stale_baskets.json.
        """
        baskets: dict[str, dict] = {}
        for thr in sorted(self._basket_lists.keys()):
            entries = []
            for entry in self._basket_lists[thr]:
                key   = entry["key"]
                count = self._counts.get(key, 0)
                info  = self._key_info.get(key, {})
                entries.append({
                    "key":                entry["key"],
                    "exchange":           entry["exchange"],
                    "market":             entry["market"],
                    "symbol":             entry["symbol"],
                    "stale_count_30m":    count,
                    "stale_rate_pct":     round(count / self.max_checks * 100, 2),
                    "added_to_basket_at": entry["added_to_basket_at"],
                    "last_stale_at":      _ts_to_iso(
                        info.get("last_stale_ts", 0)
                    ),
                })
            baskets[str(thr)] = {
                "threshold":     thr,
                "description":   (
                    f"stale >= {thr} times in last "
                    f"{int(self.window_s // 60)} min"
                ),
                "total_entries": len(entries),
                "entries":       entries,
            }

        return {
            "updated_at": _now_iso(),
            "config": {
                "window_minutes":       int(self.window_s / 60),
                "check_interval_s":     self.check_interval_s,
                "stale_threshold_s":    STALE_THRESHOLD_S,
                "basket_step":          self.step,
                "max_checks_in_window": self.max_checks,
            },
            "baskets": baskets,
        }

    def summary(self) -> dict:
        """Краткая статистика для снапшота."""
        return {
            "active_keys_in_window": len(self._counts),
            "baskets":               {str(t): len(lst)
                                      for t, lst in self._basket_lists.items()},
        }


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
# Сканирование и проверка тикерных ключей
# ══════════════════════════════════════════════════════════════════════════════

async def get_ticker_keys(client: aioredis.Redis) -> list[str]:
    """SCAN md:* → фильтр до ровно 4-частных ключей (ticker keys)."""
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
    Batch-проверка через pipeline (один round-trip).
    Возвращает (stale_list, fresh_list).
    """
    if not keys:
        return [], []

    pipe = client.pipeline(transaction=False)
    for k in keys:
        pipe.hget(k, "ts")
    ts_values: list = await pipe.execute()

    stale: list[dict] = []
    fresh: list[dict] = []

    for key, ts_raw in zip(keys, ts_values):
        parts = key.split(":")
        entry = {
            "key":      key,
            "exchange": parts[1],
            "market":   parts[2],
            "symbol":   parts[3],
        }

        if ts_raw is None:
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

        age_s            = now - ts_val
        entry["last_ts"] = round(ts_val, 3)
        entry["age_s"]   = round(age_s, 3)

        if age_s > threshold_s:
            entry["reason"] = "expired"
            stale.append(entry)
        else:
            fresh.append(entry)

    return stale, fresh


# ══════════════════════════════════════════════════════════════════════════════
# Запись файла корзин
# ══════════════════════════════════════════════════════════════════════════════

def write_baskets_file(state: dict) -> None:
    """
    Атомарная запись stale_baskets/stale_baskets.json.
    Сначала пишем во временный файл, затем rename (atomic on POSIX).
    """
    BASKETS_DIR.mkdir(parents=True, exist_ok=True)
    target = BASKETS_DIR / "stale_baskets.json"
    tmp    = BASKETS_DIR / "stale_baskets.json.tmp"
    try:
        tmp.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(target)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Главный цикл
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    log = LogManager(LOG_DIR)

    basket = BasketTracker(
        window_s=BASKET_WINDOW_S,
        step=BASKET_STEP,
        check_interval_s=CHECK_INTERVAL_S,
    ) if WRITE_BASKETS else None

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
        "basket_window_s":     BASKET_WINDOW_S if WRITE_BASKETS else None,
        "basket_step":         BASKET_STEP     if WRITE_BASKETS else None,
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

                # ── Сканирование и проверка ────────────────────────────────────
                t0  = time.perf_counter()
                now = time.time()

                ticker_keys             = await get_ticker_keys(client)
                stale_list, fresh_list  = await check_stale_keys(
                    client, ticker_keys, STALE_THRESHOLD_S, now
                )

                elapsed_ms   = round((time.perf_counter() - t0) * 1000, 2)
                checks_total += 1
                has_stale    = len(stale_list) > 0

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

                # ── Корзины ───────────────────────────────────────────────────
                if basket is not None:
                    additions = basket.record(now, stale_list)

                    # Лог каждого нового добавления в корзину
                    for item in additions:
                        added_rec = _jdump({
                            "type":      "basket_added",
                            "ts":        _now_iso(),
                            "threshold": item["threshold"],
                            **item["entry"],
                        })
                        log.write(added_rec)
                        print(added_rec)

                    # Запись файла состояния корзин
                    write_baskets_file(basket.get_state())

                # ── Периодический снапшот ──────────────────────────────────────
                now_t = time.time()
                if now_t - last_snapshot_ts >= SNAPSHOT_INTERVAL_S:
                    snap: dict = {
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
                    }
                    if basket is not None:
                        snap["baskets"] = basket.summary()
                    snap_rec = _jdump(snap)
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
