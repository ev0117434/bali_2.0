#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/redis_monitor.py — Мониторинг Redis для системы сбора рыночных данных.

Проверяет каждые MONITOR_INTERVAL секунд:
  - Связность Redis (ping-латентность)
  - Использование памяти и системные метрики (INFO)
  - Количество ключей: тикеры md:*, история md:hist:*, md:chunks:config
  - Актуальность тикеров по каждой бирже/рынку (возраст последнего обновления)
  - Состояние history-чанков (из md:chunks:config)
  - Пропускную способность Redis (cmd/s, keyspace hit-ratio)

Запуск:
    python3 market_data/redis_monitor.py

Переменные окружения:
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD — параметры подключения к Redis
    MONITOR_INTERVAL    — интервал между снапшотами (по умолчанию 10 секунд)
    STALE_THRESHOLD_S   — порог устаревания тикера в секундах (по умолчанию 60)
"""

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import redis.asyncio as aioredis

# ─── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    HISTORY_CHUNK_SECONDS,
    SNAPSHOT_INTERVAL,
    LogManager,
    create_redis,
)

# ─── Constants ─────────────────────────────────────────────────────────────────
MONITOR_INTERVAL  = int(float(os.getenv("MONITOR_INTERVAL", str(SNAPSHOT_INTERVAL))))
STALE_THRESHOLD_S = float(os.getenv("STALE_THRESHOLD_S", "60"))

EXCHANGES   = ["binance", "bybit", "okx", "gate"]
MARKETS     = ["spot", "futures"]
SCRIPT_NAME = "redis_monitor"


# ══════════════════════════════════════════════════════════════════════════════
# Data structures
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class RedisHealthStats:
    """Метрики здоровья Redis из INFO."""
    connected:                bool  = False
    ping_ms:                  float = 0.0
    redis_version:            str   = ""
    uptime_seconds:           int   = 0
    used_memory_human:        str   = ""
    used_memory_bytes:        int   = 0
    used_memory_peak_bytes:   int   = 0
    mem_fragmentation_ratio:  float = 0.0
    connected_clients:        int   = 0
    blocked_clients:          int   = 0
    rejected_connections:     int   = 0
    total_commands_processed: int   = 0
    instantaneous_ops_per_sec: int  = 0
    keyspace_hits:            int   = 0
    keyspace_misses:          int   = 0
    total_net_input_bytes:    int   = 0
    total_net_output_bytes:   int   = 0
    rdb_last_bgsave_status:   str   = ""
    aof_enabled:              bool  = False
    error:                    str   = ""


@dataclass
class TickerMarketStats:
    """Статистика тикеров для одной биржи+рынка."""
    exchange:    str   = ""
    market:      str   = ""
    key_count:   int   = 0
    newest_age_s: float = -1.0   # возраст самого свежего тикера, сек (-1 = нет данных)
    oldest_age_s: float = -1.0   # возраст самого старого тикера, сек
    stale_count: int   = 0       # тикеров старше STALE_THRESHOLD_S


@dataclass
class ChunkStats:
    """Состояние history-чанков для одной биржи+рынка."""
    exchange:         str       = ""
    market:           str       = ""
    current_chunk_id: int       = 0
    active_chunks:    List[int] = field(default_factory=list)
    elapsed_s:        float     = 0.0
    remaining_s:      float     = 0.0
    total_hist_keys:  int       = 0


@dataclass
class KeyspaceStats:
    """Статистика ключей Redis."""
    total_keys:           int  = 0
    ticker_keys:          int  = 0
    hist_keys:            int  = 0
    chunks_config_exists: bool = False


# ══════════════════════════════════════════════════════════════════════════════
# RedisMonitor
# ══════════════════════════════════════════════════════════════════════════════

class RedisMonitor:
    """Асинхронный монитор Redis для системы bali_2.0."""

    def __init__(self, redis_client: aioredis.Redis, log_manager: LogManager):
        self.redis       = redis_client
        self.log_manager = log_manager
        self._start_ts   = time.time()

        # Для расчёта delta-метрик между снапшотами
        self._prev_commands: int   = 0
        self._prev_hits:     int   = 0
        self._prev_misses:   int   = 0
        self._prev_ts:       float = time.time()

    # ── Collecting metrics ─────────────────────────────────────────────────────

    async def _get_health(self) -> RedisHealthStats:
        stats = RedisHealthStats()
        try:
            t0 = time.perf_counter()
            await self.redis.ping()
            stats.ping_ms   = (time.perf_counter() - t0) * 1000
            stats.connected = True

            info = await self.redis.info()

            stats.redis_version              = info.get("redis_version", "")
            stats.uptime_seconds             = info.get("uptime_in_seconds", 0)
            stats.used_memory_human          = info.get("used_memory_human", "")
            stats.used_memory_bytes          = info.get("used_memory", 0)
            stats.used_memory_peak_bytes     = info.get("used_memory_peak", 0)
            stats.mem_fragmentation_ratio    = info.get("mem_fragmentation_ratio", 0.0)
            stats.connected_clients          = info.get("connected_clients", 0)
            stats.blocked_clients            = info.get("blocked_clients", 0)
            stats.rejected_connections       = info.get("rejected_connections", 0)
            stats.total_commands_processed   = info.get("total_commands_processed", 0)
            stats.instantaneous_ops_per_sec  = info.get("instantaneous_ops_per_sec", 0)
            stats.keyspace_hits              = info.get("keyspace_hits", 0)
            stats.keyspace_misses            = info.get("keyspace_misses", 0)
            stats.total_net_input_bytes      = info.get("total_net_input_bytes", 0)
            stats.total_net_output_bytes     = info.get("total_net_output_bytes", 0)
            stats.rdb_last_bgsave_status     = info.get("rdb_last_bgsave_status", "")
            stats.aof_enabled                = bool(info.get("aof_enabled", 0))

        except Exception as e:
            stats.connected = False
            stats.error     = str(e)[:200]
            self.log_manager.log(f"[redis_monitor] Redis недоступен: {e}", logging.ERROR)

        return stats

    async def _get_keyspace_stats(self) -> KeyspaceStats:
        stats = KeyspaceStats()
        try:
            cursor = b"0"
            while True:
                cursor, keys = await self.redis.scan(cursor, match="md:*", count=1000)
                for key in keys:
                    stats.total_keys += 1
                    if key.startswith("md:hist:"):
                        stats.hist_keys += 1
                    elif key == "md:chunks:config":
                        stats.chunks_config_exists = True
                    else:
                        stats.ticker_keys += 1
                if cursor in (b"0", 0):
                    break
        except Exception as e:
            self.log_manager.log(f"[redis_monitor] Ошибка сканирования ключей: {e}", logging.ERROR)
        return stats

    async def _get_ticker_stats(self, exchange: str, market: str) -> TickerMarketStats:
        stats = TickerMarketStats(exchange=exchange, market=market)
        try:
            cursor = b"0"
            keys: List[str] = []
            while True:
                cursor, batch = await self.redis.scan(
                    cursor, match=f"md:{exchange}:{market}:*", count=500
                )
                keys.extend(batch)
                if cursor in (b"0", 0):
                    break

            stats.key_count = len(keys)
            if not keys:
                return stats

            now  = time.time()
            pipe = self.redis.pipeline(transaction=False)
            for key in keys:
                pipe.hget(key, "ts")
            results = await pipe.execute()

            ages: List[float] = []
            for ts_str in results:
                if ts_str is not None:
                    try:
                        age = now - float(ts_str)
                        ages.append(age)
                        if age > STALE_THRESHOLD_S:
                            stats.stale_count += 1
                    except (ValueError, TypeError):
                        pass

            if ages:
                stats.newest_age_s = min(ages)
                stats.oldest_age_s = max(ages)

        except Exception as e:
            self.log_manager.log(
                f"[redis_monitor] Ошибка тикеров {exchange}/{market}: {e}", logging.ERROR
            )
        return stats

    async def _get_chunk_stats(self, exchange: str, market: str) -> Optional[ChunkStats]:
        try:
            raw = await self.redis.hget("md:chunks:config", f"{exchange}:{market}")
            if not raw:
                return None

            cfg   = json.loads(raw)
            stats = ChunkStats(
                exchange=exchange,
                market=market,
                current_chunk_id=cfg.get("current_chunk_id", 0),
                active_chunks=cfg.get("active_chunks", []),
            )

            # Elapsed / remaining для текущего чанка
            chunks_meta  = cfg.get("chunks", {})
            current_meta = chunks_meta.get(str(stats.current_chunk_id), {})
            start_ts     = current_meta.get("start_ts", 0.0)
            if start_ts:
                stats.elapsed_s   = time.time() - start_ts
                stats.remaining_s = max(0.0, HISTORY_CHUNK_SECONDS - stats.elapsed_s)

            # Количество history-ключей для этой биржи/рынка
            cursor = b"0"
            while True:
                cursor, keys = await self.redis.scan(
                    cursor, match=f"md:hist:{exchange}:{market}:*", count=500
                )
                stats.total_hist_keys += len(keys)
                if cursor in (b"0", 0):
                    break

            return stats

        except Exception as e:
            self.log_manager.log(
                f"[redis_monitor] Ошибка чанков {exchange}/{market}: {e}", logging.ERROR
            )
            return None

    # ── Snapshot building ──────────────────────────────────────────────────────

    @staticmethod
    def _fmt_uptime(seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        return f"{h}h {m:02d}m {s:02d}s"

    @staticmethod
    def _fmt_bytes(b: int) -> str:
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if b < 1024:
                return f"{b:.1f} {unit}"
            b //= 1024
        return f"{b:.1f} PB"

    def _build_snapshot(
        self,
        health:      RedisHealthStats,
        keyspace:    KeyspaceStats,
        ticker_list: List[TickerMarketStats],
        chunk_list:  List[Optional[ChunkStats]],
        elapsed:     float,
    ) -> str:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"

        lines = [
            "=" * 66,
            f"SNAPSHOT  {SCRIPT_NAME:<20}  {now}",
            "=" * 66,
            f"Uptime монитора: {self._fmt_uptime(time.time() - self._start_ts)}",
            "",
        ]

        # ── Redis Connection ───────────────────────────────────────────────────
        status = "✓ ONLINE" if health.connected else "✗ OFFLINE"
        lines.append(f"REDIS  [{status}]")
        lines.append(f"  Адрес:          {REDIS_HOST}:{REDIS_PORT}  db={REDIS_DB}")
        if health.connected:
            lines += [
                f"  Версия:         Redis {health.redis_version}",
                f"  Uptime Redis:   {self._fmt_uptime(health.uptime_seconds)}",
                f"  Ping:           {health.ping_ms:.2f} ms",
            ]
        else:
            lines.append(f"  Ошибка:         {health.error}")
        lines.append("")

        # ── Memory ────────────────────────────────────────────────────────────
        if health.connected:
            peak_pct = (
                health.used_memory_bytes / health.used_memory_peak_bytes * 100
                if health.used_memory_peak_bytes > 0 else 0.0
            )
            lines += [
                "ПАМЯТЬ",
                f"  Используется:   {health.used_memory_human}  "
                f"(пик: {self._fmt_bytes(health.used_memory_peak_bytes)}, {peak_pct:.1f}%)",
                f"  Фрагментация:   {health.mem_fragmentation_ratio:.2f}x",
                "",
            ]

        # ── Clients ───────────────────────────────────────────────────────────
        if health.connected:
            lines += [
                "КЛИЕНТЫ",
                f"  Подключено:     {health.connected_clients}",
                f"  Заблокировано:  {health.blocked_clients}",
                f"  Отклонено всего:{health.rejected_connections:,}",
                "",
            ]

        # ── Throughput / ops ──────────────────────────────────────────────────
        if health.connected:
            dt         = max(elapsed, 0.001)
            cmd_delta  = health.total_commands_processed - self._prev_commands
            hits_delta = health.keyspace_hits   - self._prev_hits
            miss_delta = health.keyspace_misses - self._prev_misses
            cmd_rate   = cmd_delta / dt
            total_hm   = hits_delta + miss_delta
            hit_ratio  = hits_delta / total_hm * 100 if total_hm > 0 else 0.0

            self._prev_commands = health.total_commands_processed
            self._prev_hits     = health.keyspace_hits
            self._prev_misses   = health.keyspace_misses
            self._prev_ts       = time.time()

            net_in  = self._fmt_bytes(health.total_net_input_bytes)
            net_out = self._fmt_bytes(health.total_net_output_bytes)
            lines += [
                "ПРОИЗВОДИТЕЛЬНОСТЬ",
                f"  Команд всего:   {health.total_commands_processed:,}  "
                f"({cmd_rate:.0f} cmd/s за {elapsed:.0f}s)",
                f"  ops/sec (live): {health.instantaneous_ops_per_sec:,}",
                f"  Keyspace hit%:  {hit_ratio:.1f}%  "
                f"(+{hits_delta:,} hits  +{miss_delta:,} misses  за {elapsed:.0f}s)",
                f"  Сеть:           in={net_in}  out={net_out}  (всего с запуска Redis)",
                f"  RDB:            {health.rdb_last_bgsave_status or '—'}  "
                f"  AOF: {'вкл' if health.aof_enabled else 'выкл'}",
                "",
            ]

        # ── Keyspace ──────────────────────────────────────────────────────────
        lines += [
            "КЛЮЧИ В REDIS",
            f"  Всего md:*:            {keyspace.total_keys:,}",
            f"  Тикеры  md:ex:mkt:sym: {keyspace.ticker_keys:,}",
            f"  История md:hist:*:      {keyspace.hist_keys:,}",
            f"  md:chunks:config:       {'✓ есть' if keyspace.chunks_config_exists else '✗ нет'}",
            "",
        ]

        # ── Ticker freshness ──────────────────────────────────────────────────
        lines.append("АКТУАЛЬНОСТЬ ТИКЕРОВ")
        lines.append(
            f"  {'Биржа / Рынок':<22} {'Ключей':>7}  {'Свежий':>9}  {'Старый':>9}  {'Устар.':>6}"
        )
        lines.append("  " + "-" * 61)
        any_stale = False
        for ts in ticker_list:
            label  = f"{ts.exchange}/{ts.market}"
            newest = f"{ts.newest_age_s:.1f}s" if ts.newest_age_s >= 0 else "—"
            oldest = f"{ts.oldest_age_s:.1f}s" if ts.oldest_age_s >= 0 else "—"
            stale  = str(ts.stale_count)
            flag   = " ⚠" if ts.stale_count > 0 else ""
            if ts.stale_count > 0:
                any_stale = True
            lines.append(
                f"  {label:<22} {ts.key_count:>7,}  {newest:>9}  {oldest:>9}  {stale:>6}{flag}"
            )
        if any_stale:
            lines.append(f"  ⚠ Тикер устаревший, если старше {STALE_THRESHOLD_S:.0f}s")
        lines.append("")

        # ── History chunks ────────────────────────────────────────────────────
        lines.append("HISTORY ЧАНКИ")
        has_chunks = False
        for cs in chunk_list:
            if cs is None:
                continue
            has_chunks = True
            label = f"{cs.exchange}/{cs.market}"
            lines.append(
                f"  {label:<22}  чанк #{cs.current_chunk_id}  "
                f"активных: {cs.active_chunks}  hist-ключей: {cs.total_hist_keys}"
            )
            lines.append(
                f"  {'':22}  прошло: {cs.elapsed_s:.0f}s  осталось: {cs.remaining_s:.0f}s"
            )
        if not has_chunks:
            lines.append("  Нет данных (md:chunks:config отсутствует или пуст)")
        lines.append("")

        lines.append("=" * 66)
        return "\n".join(lines)

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def run(self):
        self.log_manager.log(
            f"[redis_monitor] Запуск мониторинга. "
            f"Redis={REDIS_HOST}:{REDIS_PORT} db={REDIS_DB}  "
            f"Интервал={MONITOR_INTERVAL}s  "
            f"Порог устаревания={STALE_THRESHOLD_S}s"
        )
        last_ts = time.time()

        while True:
            await asyncio.sleep(MONITOR_INTERVAL)
            elapsed = time.time() - last_ts
            last_ts = time.time()

            try:
                n_pairs = len(EXCHANGES) * len(MARKETS)

                results = await asyncio.gather(
                    self._get_health(),
                    self._get_keyspace_stats(),
                    *[
                        self._get_ticker_stats(ex, mkt)
                        for ex in EXCHANGES for mkt in MARKETS
                    ],
                    *[
                        self._get_chunk_stats(ex, mkt)
                        for ex in EXCHANGES for mkt in MARKETS
                    ],
                )

                health      = results[0]
                keyspace    = results[1]
                ticker_list = list(results[2:2 + n_pairs])
                chunk_list_ = list(results[2 + n_pairs:])

                snapshot = self._build_snapshot(
                    health, keyspace, ticker_list, chunk_list_, elapsed
                )
                self.log_manager.log(snapshot)

            except Exception as e:
                self.log_manager.log(
                    f"[redis_monitor] Ошибка в основном цикле: {e}", logging.ERROR
                )


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log_manager = LogManager(SCRIPT_NAME)
    log_manager.initialize()
    log_manager.log(
        f"[redis_monitor] Инициализация. "
        f"Redis={REDIS_HOST}:{REDIS_PORT} db={REDIS_DB}  "
        f"Интервал={MONITOR_INTERVAL}s  "
        f"Порог устаревания тикера={STALE_THRESHOLD_S}s"
    )

    try:
        redis_client = await create_redis()
    except RuntimeError as e:
        log_manager.log(f"[redis_monitor] FATAL: {e}", logging.CRITICAL)
        sys.exit(1)

    monitor = RedisMonitor(redis_client, log_manager)
    try:
        await monitor.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        log_manager.log("[redis_monitor] Остановлен.")
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
