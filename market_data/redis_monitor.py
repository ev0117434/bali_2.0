#!/usr/bin/env python3
"""
Redis Monitor — непрерывный сбор метрик Redis с записью в лог-файлы.

Метрики:
  • PING-латентность (min / avg / p95 / max) — 5 измерений за цикл
  • Встроенный LATENCY LATEST — события задержек самого Redis
  • Память: used / peak / rss / fragmentation ratio / maxmemory
  • Соединения: clients / blocked / rejected / total
  • Ops/sec: instantaneous_ops_per_sec, input/output KB/s
  • Keyspace: всего ключей в DB, ключей md:* проекта
  • Slowlog: длина журнала медленных команд
  • Сервер: версия, uptime, роль, slave-узлы

Конфигурация (переменные окружения):
  REDIS_HOST          — хост Redis            (по умолчанию: 127.0.0.1)
  REDIS_PORT          — порт Redis            (по умолчанию: 6379)
  REDIS_DB            — номер БД              (по умолчанию: 0)
  REDIS_PASSWORD      — пароль                (по умолчанию: нет)
  MONITOR_INTERVAL    — интервал сбора, сек   (по умолчанию: 5)
  MONITOR_LOG_DIR     — папка для логов       (по умолчанию: logs/redis_monitor)
  MONITOR_LOG_JSON    — 1 = JSON Lines, 0 = читаемый текст (по умолчанию: 0)
  LOG_CHUNK_SECONDS   — размер одного лог-чанка сек (по умолчанию: 86400 = 24ч)
  LOG_MAX_CHUNKS      — кол-во хранимых чанков     (по умолчанию: 2)

Запуск:
  python redis_monitor.py
  MONITOR_INTERVAL=10 MONITOR_LOG_JSON=1 python redis_monitor.py
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

# ─── Конфигурация из env (совместимо с common.py) ─────────────────────────────

REDIS_HOST     = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

INTERVAL       = float(os.getenv("MONITOR_INTERVAL", "5"))       # секунд между циклами
LOG_DIR        = Path(os.getenv("MONITOR_LOG_DIR", "logs/redis_monitor"))
LOG_JSON       = os.getenv("MONITOR_LOG_JSON", "0") == "1"       # 1 = JSON Lines

LOG_CHUNK_SECONDS = int(os.getenv("LOG_CHUNK_SECONDS", str(86400)))
LOG_MAX_CHUNKS    = int(os.getenv("LOG_MAX_CHUNKS", "2"))

# ─── Ротирующий лог-файл ──────────────────────────────────────────────────────

class LogManager:
    """
    Лог-файл с ротацией по времени (аналог common.py LogManager).

    Структура:
      logs/redis_monitor/
        20240318_120000-ongoing/monitor.log   ← текущий чанк
        20240317_120000-20240318_120000/      ← завершённый чанк
    """

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._chunk_start: float = time.time()
        self._chunk_dir: Optional[Path] = None
        self._file = None
        self._open_chunk()
        self._cleanup_old_chunks()

    # ── внутренние методы ──────────────────────────────────────────────────────

    @staticmethod
    def _ts() -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def _open_chunk(self) -> None:
        name = f"{self._ts()}-ongoing"
        self._chunk_dir = self.base_dir / name
        self._chunk_dir.mkdir(parents=True, exist_ok=True)
        self._file = open(self._chunk_dir / "monitor.log", "a", encoding="utf-8")
        self._chunk_start = time.time()

    def _close_chunk(self) -> None:
        if self._file:
            self._file.close()
            self._file = None
        if self._chunk_dir and self._chunk_dir.exists():
            new_name = self._chunk_dir.name.replace("-ongoing", f"-{self._ts()}")
            self._chunk_dir.rename(self._chunk_dir.parent / new_name)
            self._chunk_dir = self._chunk_dir.parent / new_name

    def _cleanup_old_chunks(self) -> None:
        chunks = sorted(
            [d for d in self.base_dir.iterdir()
             if d.is_dir() and "-ongoing" not in d.name],
            key=lambda d: d.stat().st_mtime,
        )
        while len(chunks) > LOG_MAX_CHUNKS:
            shutil.rmtree(chunks.pop(0), ignore_errors=True)

    # ── публичный API ──────────────────────────────────────────────────────────

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


# ─── Подключение к Redis ───────────────────────────────────────────────────────

async def connect_redis() -> aioredis.Redis:
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


# ─── Сбор метрик ──────────────────────────────────────────────────────────────

async def measure_ping_latency(client: aioredis.Redis, samples: int = 5) -> dict:
    """Измеряет RTT через PING (мс). Возвращает min/avg/p95/max."""
    times: list[float] = []
    for _ in range(samples):
        t0 = time.perf_counter()
        await client.ping()
        times.append((time.perf_counter() - t0) * 1000)
        await asyncio.sleep(0.005)
    times.sort()
    n = len(times)
    return {
        "min_ms": round(times[0], 3),
        "avg_ms": round(sum(times) / n, 3),
        "p95_ms": round(times[int(n * 0.95)], 3),
        "max_ms": round(times[-1], 3),
        "samples": n,
    }


async def get_latency_events(client: aioredis.Redis) -> dict:
    """
    Читает встроенный журнал задержек Redis (LATENCY LATEST).
    Redis логирует события, которые превысили пороговое время (latency-monitor-threshold).
    Возвращает словарь: {event_name: {last_ms, max_ms, event_count}}.
    """
    result: dict = {}
    try:
        # LATENCY LATEST → список [event, last_timestamp, last_latency, max_latency]
        raw = await client.execute_command("LATENCY LATEST")
        if raw:
            for entry in raw:
                if isinstance(entry, (list, tuple)) and len(entry) >= 4:
                    event_name = entry[0]
                    last_ms    = int(entry[2])   # мс
                    max_ms     = int(entry[3])   # мс
                    result[event_name] = {"last_ms": last_ms, "max_ms": max_ms}
    except Exception:
        pass
    return result


async def count_md_keys(client: aioredis.Redis) -> int:
    """Подсчитывает ключи md:* (данные проекта) через SCAN (без блокировки)."""
    count = 0
    cursor = 0
    try:
        while True:
            cursor, keys = await client.scan(cursor, match="md:*", count=500)
            count += len(keys)
            if cursor == 0:
                break
    except Exception:
        pass
    return count


async def collect_metrics(client: aioredis.Redis) -> dict:
    """Собирает все метрики за один проход."""

    ts_now = datetime.now(timezone.utc).isoformat()

    # Измерения параллельно (латентность + INFO + LATENCY LATEST + md:* keys)
    ping_task     = asyncio.create_task(measure_ping_latency(client))
    lat_ev_task   = asyncio.create_task(get_latency_events(client))
    md_keys_task  = asyncio.create_task(count_md_keys(client))

    # INFO all — основной источник данных
    info: dict = await client.info("all")

    # Ждём параллельные задачи
    ping_result, latency_events, md_keys = await asyncio.gather(
        ping_task, lat_ev_task, md_keys_task
    )

    # Slowlog
    slowlog_len = 0
    try:
        slowlog_len = await client.slowlog_len()
    except Exception:
        pass

    # ── Вспомогательная функция для безопасного чтения int/float из info ──────
    def gi(key: str, default=0):
        v = info.get(key, default)
        try:
            return int(v)
        except (ValueError, TypeError):
            try:
                return float(v)
            except (ValueError, TypeError):
                return v

    # ── Keyspace: кол-во ключей в нужной DB ───────────────────────────────────
    db_key_name = f"db{REDIS_DB}"
    ks_val = info.get(db_key_name)
    db_total_keys = 0
    if isinstance(ks_val, dict):
        db_total_keys = ks_val.get("keys", 0)
    elif isinstance(ks_val, str) and "keys=" in ks_val:
        try:
            db_total_keys = int(ks_val.split("keys=")[1].split(",")[0])
        except Exception:
            pass

    # ── Сборка результата ──────────────────────────────────────────────────────
    metrics = {
        "timestamp": ts_now,
        "connected": True,

        # Задержки (PING RTT)
        "latency_ping": ping_result,

        # Встроенные события задержек Redis
        "latency_events": latency_events,

        # Память
        "memory": {
            "used_bytes":          gi("used_memory"),
            "used_human":          info.get("used_memory_human"),
            "peak_bytes":          gi("used_memory_peak"),
            "peak_human":          info.get("used_memory_peak_human"),
            "rss_bytes":           gi("used_memory_rss"),
            "rss_human":           info.get("used_memory_rss_human"),
            "fragmentation_ratio": round(float(info.get("mem_fragmentation_ratio", 0)), 3),
            "maxmemory_bytes":     gi("maxmemory"),
            "maxmemory_human":     info.get("maxmemory_human", "0B"),
            "maxmemory_policy":    info.get("maxmemory_policy", "noeviction"),
        },

        # Соединения
        "connections": {
            "connected_clients":           gi("connected_clients"),
            "blocked_clients":             gi("blocked_clients"),
            "tracking_clients":            gi("tracking_clients", 0),
            "total_connections_received":  gi("total_connections_received"),
            "rejected_connections":        gi("rejected_connections", 0),
        },

        # Операции
        "ops": {
            "instantaneous_ops_per_sec":   gi("instantaneous_ops_per_sec"),
            "total_commands_processed":    gi("total_commands_processed"),
            "instantaneous_input_kbps":    round(float(info.get("instantaneous_input_kbps", 0)), 2),
            "instantaneous_output_kbps":   round(float(info.get("instantaneous_output_kbps", 0)), 2),
            "total_net_input_bytes":       gi("total_net_input_bytes"),
            "total_net_output_bytes":      gi("total_net_output_bytes"),
        },

        # Ошибки
        "errors": {
            "rejected_connections":        gi("rejected_connections", 0),
            "evicted_keys":                gi("evicted_keys", 0),
            "keyspace_hits":               gi("keyspace_hits", 0),
            "keyspace_misses":             gi("keyspace_misses", 0),
        },

        # Медленные команды
        "slowlog_len": slowlog_len,

        # Keyspace
        "keyspace": {
            "db_total_keys": db_total_keys,
            "md_keys":       md_keys,
        },

        # Постоянность (persistence)
        "persistence": {
            "rdb_changes_since_last_save": gi("rdb_changes_since_last_save", 0),
            "rdb_last_bgsave_status":      info.get("rdb_last_bgsave_status", "unknown"),
            "aof_enabled":                 gi("aof_enabled", 0) == 1,
        },

        # Сервер
        "server": {
            "redis_version":  info.get("redis_version"),
            "uptime_seconds": gi("uptime_in_seconds"),
            "hz":             gi("hz"),
            "role":           info.get("role"),
            "os":             info.get("os"),
            "tcp_port":       gi("tcp_port"),
        },

        # Репликация
        "replication": {
            "role":             info.get("role"),
            "connected_slaves": gi("connected_slaves", 0),
            "master_replid":    info.get("master_replid"),
        },
    }

    return metrics


# ─── Форматирование вывода ─────────────────────────────────────────────────────

_WARN_PING_P95_MS    = 10.0   # предупреждение если p95 > 10 мс
_WARN_FRAG_RATIO     = 1.5    # предупреждение если фрагментация > 1.5
_WARN_REJECTED       = 1      # предупреждение если есть отклонённые соединения
_WARN_SLOWLOG        = 10     # предупреждение если slowlog > 10 записей


def _warn_flags(m: dict) -> list[str]:
    flags: list[str] = []
    p95 = m["latency_ping"]["p95_ms"]
    if p95 > _WARN_PING_P95_MS:
        flags.append(f"⚠ PING p95={p95}ms > {_WARN_PING_P95_MS}ms")
    if m["memory"]["fragmentation_ratio"] > _WARN_FRAG_RATIO:
        flags.append(f"⚠ fragmentation={m['memory']['fragmentation_ratio']} > {_WARN_FRAG_RATIO}")
    if m["connections"]["rejected_connections"] >= _WARN_REJECTED:
        flags.append(f"⚠ rejected_connections={m['connections']['rejected_connections']}")
    if m["slowlog_len"] >= _WARN_SLOWLOG:
        flags.append(f"⚠ slowlog_len={m['slowlog_len']}")
    if m["latency_events"]:
        for ev, v in m["latency_events"].items():
            flags.append(f"⚠ latency_event[{ev}] last={v['last_ms']}ms max={v['max_ms']}ms")
    return flags


def format_human(m: dict) -> str:
    lat  = m["latency_ping"]
    mem  = m["memory"]
    con  = m["connections"]
    ops  = m["ops"]
    ks   = m["keyspace"]
    srv  = m["server"]
    err  = m["errors"]

    warns = _warn_flags(m)
    warn_block = ""
    if warns:
        warn_block = "\n" + "\n".join(f"  {w}" for w in warns) + "\n"

    # Hit rate
    hits   = err["keyspace_hits"]
    misses = err["keyspace_misses"]
    hit_rate = f"{hits/(hits+misses)*100:.1f}%" if (hits + misses) > 0 else "n/a"

    return (
        f"\n{'─'*64}\n"
        f"  {m['timestamp']}  Redis {srv['redis_version']}  uptime {srv['uptime_seconds']}s\n"
        f"{'─'*64}\n"
        f"  PING latency    min={lat['min_ms']}ms  avg={lat['avg_ms']}ms  "
        f"p95={lat['p95_ms']}ms  max={lat['max_ms']}ms\n"
        f"  memory          {mem['used_human']} used  |  peak {mem['peak_human']}"
        f"  |  rss {mem['rss_human']}  |  frag={mem['fragmentation_ratio']}\n"
        f"  connections     clients={con['connected_clients']}"
        f"  blocked={con['blocked_clients']}"
        f"  rejected={con['rejected_connections']}"
        f"  total={con['total_connections_received']}\n"
        f"  ops/sec         {ops['instantaneous_ops_per_sec']} ops/s"
        f"  |  in={ops['instantaneous_input_kbps']} KB/s"
        f"  out={ops['instantaneous_output_kbps']} KB/s"
        f"  |  total_cmds={ops['total_commands_processed']}\n"
        f"  keyspace        db{REDIS_DB}={ks['db_total_keys']} keys"
        f"  |  md:* keys={ks['md_keys']}"
        f"  |  hit_rate={hit_rate}"
        f"  evicted={err['evicted_keys']}\n"
        f"  slowlog         {m['slowlog_len']} entries"
        f"  |  rdb_changes={m['persistence']['rdb_changes_since_last_save']}"
        f"  |  hz={srv['hz']}\n"
        f"{warn_block}"
        f"{'─'*64}"
    )


def format_json(m: dict) -> str:
    return json.dumps(m, ensure_ascii=False)


def format_error(exc: Exception, attempt: int) -> str:
    ts = datetime.now(timezone.utc).isoformat()
    return f"[{ts}] ERROR(attempt={attempt}) {type(exc).__name__}: {exc}"


# ─── Главный цикл ─────────────────────────────────────────────────────────────

async def main() -> None:
    log = LogManager(LOG_DIR)

    header = (
        f"[redis_monitor] start  host={REDIS_HOST}:{REDIS_PORT}  db={REDIS_DB}"
        f"  interval={INTERVAL}s  json={LOG_JSON}  log_dir={LOG_DIR}"
    )
    print(header)
    log.write(header)

    client: Optional[aioredis.Redis] = None
    consecutive_errors = 0

    try:
        while True:
            loop_start = time.monotonic()

            try:
                # ── Соединение ────────────────────────────────────────────────
                if client is None:
                    client = await connect_redis()
                    conn_msg = (
                        f"[redis_monitor] connected  {REDIS_HOST}:{REDIS_PORT}"
                    )
                    print(conn_msg)
                    log.write(conn_msg)

                # ── Сбор метрик ───────────────────────────────────────────────
                metrics = await collect_metrics(client)
                consecutive_errors = 0

                # ── Вывод ─────────────────────────────────────────────────────
                line = format_json(metrics) if LOG_JSON else format_human(metrics)
                print(line)
                log.write(line)

            except (ConnectionError, OSError) as exc:
                consecutive_errors += 1
                err = format_error(exc, consecutive_errors)
                print(err, file=sys.stderr)
                log.write(err)
                try:
                    if client:
                        await client.aclose()
                except Exception:
                    pass
                client = None
                backoff = min(2 ** (consecutive_errors - 1), 30)
                await asyncio.sleep(backoff)
                continue

            except aioredis.RedisError as exc:
                consecutive_errors += 1
                err = format_error(exc, consecutive_errors)
                print(err, file=sys.stderr)
                log.write(err)
                await asyncio.sleep(min(2 ** (consecutive_errors - 1), 30))
                continue

            # ── Ждём следующего цикла ─────────────────────────────────────────
            elapsed = time.monotonic() - loop_start
            await asyncio.sleep(max(0.0, INTERVAL - elapsed))

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        stop_msg = "[redis_monitor] stopped"
        print(f"\n{stop_msg}")
        log.write(stop_msg)
        log.close()
        if client:
            try:
                await client.aclose()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
