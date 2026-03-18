#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/common.py — Общие утилиты для сборщиков рыночных данных.

Содержит:
- Stats / ConnectionStats  : статистика для снапшотов
- LogManager               : ротация лог-файлов (24ч-чанки, хранить 2)
- ChunkManager             : 20-мин чанки истории цен в Redis (хранить 5)
- SnapshotLogger           : снапшот каждые 10 секунд
- create_redis()           : async Redis-клиент
- load_symbols()           : чтение списка символов из subscribe-файла
- chunk_list()             : разбивка списка на чанки
"""

import asyncio
import json
import logging
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import redis.asyncio as aioredis

# Принудительно выставляем UTF-8 для stdout/stderr (актуально при запуске
# через run_all.py или в окружениях без локали UTF-8)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR     = PROJECT_ROOT / "logs"
SUBSCRIBE_DIR = PROJECT_ROOT / "dictionaries" / "subscribe"

# ─── Redis ────────────────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# ─── Timings ──────────────────────────────────────────────────────────────────
HISTORY_CHUNK_SECONDS  = 20 * 60   # 20 минут — один чанк истории
HISTORY_MAX_CHUNKS     = 5         # Хранить последние 5 чанков

LOG_CHUNK_SECONDS      = 24 * 60 * 60  # 24 часа — один лог-чанк
LOG_MAX_CHUNKS         = 2             # Хранить последние 2 завершённых чанка

SNAPSHOT_INTERVAL      = 10    # секунд между снапшотами
TICKER_FLUSH_INTERVAL  = 0.05  # секунд между пакетными записями тикеров (50ms)
HISTORY_FLUSH_INTERVAL = 1.0   # секунд между пакетными записями истории


# ══════════════════════════════════════════════════════════════════════════════
# Statistics
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ConnectionStats:
    url:          str   = ""
    active:       bool  = False
    msgs_total:   int   = 0
    msgs_window:  int   = 0
    last_msg_ts:  float = 0.0
    reconnects:   int   = 0
    last_error:   str   = ""
    last_error_ts: float = 0.0


@dataclass
class Stats:
    start_ts: float = field(default_factory=time.time)
    connections: List[ConnectionStats] = field(default_factory=list)

    msgs_total:   int = 0
    msgs_window:  int = 0

    ticker_writes_total:  int = 0
    ticker_writes_window: int = 0

    history_entries_total:  int = 0
    history_entries_window: int = 0

    # RTT: exchange_timestamp → recv_ts  (сетевая задержка биржа→скрипт)
    # Недоступна для Binance bookTicker (нет exchange timestamp в потоке)
    rtt_latencies_window:  List[float] = field(default_factory=list)

    # Internal: recv_ts → redis write complete  (время в буфере + pipeline)
    proc_latencies_window: List[float] = field(default_factory=list)

    symbols_active_window: Set[str] = field(default_factory=set)
    symbols_tracked: int = 0

    reconnects_total: int   = 0
    last_error:       str   = ""
    last_error_ts:    float = 0.0

    def reset_window(self):
        self.msgs_window              = 0
        self.ticker_writes_window     = 0
        self.history_entries_window   = 0
        self.rtt_latencies_window.clear()
        self.proc_latencies_window.clear()
        self.symbols_active_window.clear()
        for c in self.connections:
            c.msgs_window = 0

    def record_message(self, rtt_ms: Optional[float] = None, symbol: str = ""):
        self.msgs_total  += 1
        self.msgs_window += 1
        if rtt_ms is not None and rtt_ms > 0:
            self.rtt_latencies_window.append(rtt_ms)
        if symbol:
            self.symbols_active_window.add(symbol)

    def record_proc_latency(self, ms: float):
        """Вызывается из ticker_flusher после записи pipeline в Redis."""
        if ms > 0:
            self.proc_latencies_window.append(ms)

    def record_ticker_write(self, n: int = 1):
        self.ticker_writes_total  += n
        self.ticker_writes_window += n

    def record_history_entry(self, n: int = 1):
        self.history_entries_total  += n
        self.history_entries_window += n

    def record_error(self, err: str):
        self.last_error    = err[:200]
        self.last_error_ts = time.time()
        self.reconnects_total += 1


# ══════════════════════════════════════════════════════════════════════════════
# LogManager
# ══════════════════════════════════════════════════════════════════════════════

class LogManager:
    """
    Ротация лог-файлов.
    - Один чанк = 24 ч; папка формата  YYYYMMDD_HHMMSS-YYYYMMDD_HHMMSS
    - Хранить LOG_MAX_CHUNKS завершённых чанков; при открытии нового
      (N+1)-го чанка удалять самый старый.
    """

    def __init__(self, script_name: str):
        self.script_name  = script_name
        self.log_dir_root = LOGS_DIR / script_name
        self.log_dir_root.mkdir(parents=True, exist_ok=True)

        self._current_dir:  Optional[Path]           = None
        self._current_start_ts: float                = 0.0
        self._logger:       Optional[logging.Logger] = None
        self._completed_dirs: List[Path]             = []

    # ── internal ─────────────────────────────────────────────────────────────

    @staticmethod
    def _dt_str(ts: float) -> str:
        return datetime.utcfromtimestamp(ts).strftime("%Y%m%d_%H%M%S")

    def _scan_completed(self):
        """Найти и отсортировать папки завершённых чанков."""
        dirs = sorted(
            [d for d in self.log_dir_root.iterdir()
             if d.is_dir() and "-" in d.name and "ongoing" not in d.name],
            key=lambda d: d.name,
        )
        self._completed_dirs = dirs

    def _close_current(self):
        """Закрыть текущий чанк: сбросить хендлер и переименовать папку."""
        if self._logger:
            for h in self._logger.handlers[:]:
                try:
                    h.flush()
                    h.close()
                except Exception:
                    pass
                self._logger.removeHandler(h)

        if self._current_dir and self._current_dir.exists():
            end_str   = self._dt_str(time.time())
            start_str = self._dt_str(self._current_start_ts)
            final_dir = self.log_dir_root / f"{start_str}-{end_str}"
            try:
                self._current_dir.rename(final_dir)
                self._completed_dirs.append(final_dir)
            except Exception:
                pass

    def _open_new_chunk(self):
        now = time.time()
        self._current_start_ts = now
        chunk_name = f"{self._dt_str(now)}-ongoing"
        self._current_dir = self.log_dir_root / chunk_name
        self._current_dir.mkdir(parents=True, exist_ok=True)
        self._setup_logger()

    def _setup_logger(self):
        log_file = self._current_dir / f"{self.script_name}.log"
        logger   = logging.getLogger(f"mdata.{self.script_name}")
        logger.setLevel(logging.DEBUG)

        for h in logger.handlers[:]:
            try:
                h.close()
            except Exception:
                pass
            logger.removeHandler(h)

        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(fh)

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(ch)

        # Prevent propagation to root logger
        logger.propagate = False
        self._logger = logger

    def _purge_old_chunks(self):
        """Удалить самые старые чанки сверх лимита."""
        self._scan_completed()
        while len(self._completed_dirs) > LOG_MAX_CHUNKS:
            old = self._completed_dirs.pop(0)
            try:
                shutil.rmtree(old)
            except Exception:
                pass

    def _needs_rotation(self) -> bool:
        return (self._current_start_ts == 0.0 or
                time.time() - self._current_start_ts >= LOG_CHUNK_SECONDS)

    # ── public ───────────────────────────────────────────────────────────────

    def initialize(self):
        self._scan_completed()
        self._open_new_chunk()

    def rotate_if_needed(self):
        if not self._needs_rotation():
            return
        self._close_current()
        self._open_new_chunk()
        self._purge_old_chunks()

    def get_logger(self) -> logging.Logger:
        self.rotate_if_needed()
        return self._logger

    def log(self, msg: str, level: int = logging.INFO):
        self.rotate_if_needed()
        if self._logger:
            self._logger.log(level, msg)


# ══════════════════════════════════════════════════════════════════════════════
# ChunkManager  (Redis history chunks)
# ══════════════════════════════════════════════════════════════════════════════

class ChunkManager:
    """
    Управляет 20-минутными чанками истории цен в Redis.

    Схема ключей:
      {prefix}:history:{symbol}:{chunk_id}   — Redis List, элементы "{ts},{bid},{ask}"
      {prefix}:chunks:config                 — JSON-строка с метаданными чанков

    Логика ротации:
      - Хранить HISTORY_MAX_CHUNKS = 5 чанков.
      - При создании 6-го удалять 1-й, при создании 7-го — 2-й и т.д.
    """

    def __init__(self, exchange: str, market: str, redis_client: aioredis.Redis):
        self.exchange = exchange
        self.market   = market
        self.redis    = redis_client
        self.field_key  = f"{exchange}:{market}"    # поле в глобальном Hash
        self.config_key = "md:chunks:config"          # один ключ на всю систему

        self.current_chunk_id: int          = 0
        self.chunk_start_ts:   float        = 0.0
        self.active_chunks:    List[int]    = []
        self.chunk_meta:       Dict[str, Any] = {}

    @staticmethod
    def _dt_str(ts: float) -> str:
        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")

    # ── internal ─────────────────────────────────────────────────────────────

    async def _save(self):
        """Сохраняет конфиг своего exchange:market в глобальный Hash md:chunks:config."""
        config = {
            "current_chunk_id": self.current_chunk_id,
            "active_chunks":    self.active_chunks,
            "chunks":           self.chunk_meta,
        }
        await self.redis.hset(
            self.config_key,
            self.field_key,
            json.dumps(config, ensure_ascii=False),
        )

    async def _delete_chunk_keys(self, chunk_id: int) -> int:
        pattern = f"md:hist:{self.exchange}:{self.market}:*:{chunk_id}"
        cursor, deleted = b"0", 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match=pattern, count=500)
            if keys:
                await self.redis.delete(*keys)
                deleted += len(keys)
            if cursor in (b"0", 0):
                break
        return deleted

    async def _new_first_chunk(self):
        now = time.time()
        self.current_chunk_id = 1
        self.chunk_start_ts   = now
        self.active_chunks    = [1]
        self.chunk_meta       = {"1": {"start_ts": now, "start_dt": self._dt_str(now)}}
        await self._save()

    # ── public ───────────────────────────────────────────────────────────────

    async def initialize(self):
        raw = await self.redis.hget(self.config_key, self.field_key)
        if raw:
            try:
                cfg = json.loads(raw)
                self.current_chunk_id = cfg["current_chunk_id"]
                self.active_chunks    = cfg["active_chunks"]
                self.chunk_meta       = {str(k): v for k, v in cfg["chunks"].items()}
                meta = self.chunk_meta.get(str(self.current_chunk_id), {})
                self.chunk_start_ts   = meta.get("start_ts", 0.0)
                # Если текущий чанк уже истёк — сразу ротируем
                if time.time() - self.chunk_start_ts >= HISTORY_CHUNK_SECONDS:
                    await self.rotate()
                return
            except Exception:
                pass
        await self._new_first_chunk()

    async def rotate(self):
        """
        Закрыть текущий чанк, открыть новый.
        Если активных чанков > HISTORY_MAX_CHUNKS — удалить самый старый.
        """
        now = time.time()
        cid = str(self.current_chunk_id)
        if cid in self.chunk_meta:
            self.chunk_meta[cid]["end_ts"] = now
            self.chunk_meta[cid]["end_dt"] = self._dt_str(now)

        self.current_chunk_id += 1
        self.chunk_start_ts    = now
        new_cid = str(self.current_chunk_id)
        self.active_chunks.append(self.current_chunk_id)
        self.chunk_meta[new_cid] = {"start_ts": now, "start_dt": self._dt_str(now)}

        # Удалить самый старый, если превысили лимит
        if len(self.active_chunks) > HISTORY_MAX_CHUNKS:
            old_id = self.active_chunks.pop(0)
            self.chunk_meta.pop(str(old_id), None)
            await self._delete_chunk_keys(old_id)

        await self._save()

    def needs_rotation(self) -> bool:
        return time.time() - self.chunk_start_ts >= HISTORY_CHUNK_SECONDS

    def get_history_key(self, symbol: str) -> str:
        return f"md:hist:{self.exchange}:{self.market}:{symbol}:{self.current_chunk_id}"

    def get_ticker_key(self, symbol: str) -> str:
        return f"md:{self.exchange}:{self.market}:{symbol}"

    def elapsed(self) -> float:
        return time.time() - self.chunk_start_ts

    def remaining(self) -> float:
        return max(0.0, HISTORY_CHUNK_SECONDS - self.elapsed())

    def config_summary(self) -> str:
        lines = [
            f"  Chunk #{self.current_chunk_id}: запущен {self.elapsed():.0f}s назад, "
            f"осталось ~{self.remaining():.0f}s",
            f"  Активные чанки: {self.active_chunks}",
        ]
        for cid_str, meta in sorted(self.chunk_meta.items(), key=lambda x: int(x[0])):
            start = meta.get("start_dt", "?")
            end   = meta.get("end_dt", "(текущий)")
            lines.append(f"    Чанк {cid_str}: {start} → {end}")
        return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# SnapshotLogger
# ══════════════════════════════════════════════════════════════════════════════

class SnapshotLogger:
    """Выводит снапшот статистики каждые SNAPSHOT_INTERVAL секунд."""

    def __init__(
        self,
        script_name:   str,
        log_manager:   LogManager,
        stats:         Stats,
        chunk_manager: Optional[ChunkManager] = None,
    ):
        self.script_name   = script_name
        self.log_manager   = log_manager
        self.stats         = stats
        self.chunk_manager = chunk_manager
        self._last_ts      = time.time()
        self._running      = False

    @staticmethod
    def _fmt_uptime(seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        return f"{h}h {m:02d}m {s:02d}s"

    @staticmethod
    def _percentile(data: List[float], pct: float) -> float:
        if not data:
            return 0.0
        sd  = sorted(data)
        idx = min(int(len(sd) * pct / 100), len(sd) - 1)
        return sd[idx]

    def _build(self, elapsed: float) -> str:
        s   = self.stats
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"

        active_conn = sum(1 for c in s.connections if c.active)
        total_conn  = len(s.connections)

        rate_msg  = s.msgs_window  / elapsed if elapsed > 0 else 0.0
        rate_tick = s.ticker_writes_window / elapsed if elapsed > 0 else 0.0
        rate_hist = s.history_entries_window / elapsed if elapsed > 0 else 0.0

        rtt  = s.rtt_latencies_window
        proc = s.proc_latencies_window

        lines = [
            "=" * 66,
            f"SNAPSHOT  {self.script_name:<20}  {now}",
            "=" * 66,
            f"Uptime: {self._fmt_uptime(time.time() - s.start_ts)}",
            "",
            f"СОЕДИНЕНИЯ ({active_conn}/{total_conn} активных)",
        ]
        for i, c in enumerate(s.connections, 1):
            status = "✓ ALIVE" if c.active else "✗ DEAD "
            url_short = (c.url[:52] + "…") if len(c.url) > 53 else c.url
            lines.append(
                f"  Conn-{i:02d}: [{status}]  msgs={c.msgs_total:,}  "
                f"last={c.msgs_window} msg/{SNAPSHOT_INTERVAL}s  url={url_short}"
            )
            if c.last_error:
                err_dt = datetime.utcfromtimestamp(c.last_error_ts).strftime("%H:%M:%S")
                lines.append(f"          ↳ err: {c.last_error[:80]}  @ {err_dt}")

        lines += [
            "",
            "ПРОПУСКНАЯ СПОСОБНОСТЬ",
            f"  Сообщений:      {s.msgs_total:>12,}  |  {rate_msg:>8.1f} msg/s",
            f"  Ticker writes:  {s.ticker_writes_total:>12,}  |  {rate_tick:>8.1f} wr/s",
            f"  History ent.:   {s.history_entries_total:>12,}  |  {rate_hist:>8.1f} ent/s",
            "",
            "СИМВОЛЫ",
            f"  Отслеживается:  {s.symbols_tracked}",
            f"  Активных за {SNAPSHOT_INTERVAL}s: {len(s.symbols_active_window)}",
            "",
            "ЗАДЕРЖКИ",
        ]
        # RTT (биржа → скрипт)
        if rtt:
            lines.append(
                f"  RTT (exchange→recv)  [{len(rtt):>5} сэмплов]:  "
                f"min={min(rtt):.2f}  avg={sum(rtt)/len(rtt):.2f}  "
                f"p95={self._percentile(rtt,95):.2f}  max={max(rtt):.2f} ms"
            )
        else:
            lines.append("  RTT (exchange→recv)  — нет данных (биржа не даёт timestamp)")
        # Internal (recv → redis write)
        if proc:
            lines.append(
                f"  Internal (recv→Redis)[{len(proc):>5} сэмплов]:  "
                f"min={min(proc):.2f}  avg={sum(proc)/len(proc):.2f}  "
                f"p95={self._percentile(proc,95):.2f}  max={max(proc):.2f} ms"
            )
        else:
            lines.append("  Internal (recv→Redis) — нет данных")

        if self.chunk_manager:
            lines += ["", "REDIS HISTORY ЧАНКИ"]
            lines.append(self.chunk_manager.config_summary())

        if s.reconnects_total:
            lines.append("")
            lines.append(f"РЕКОННЕКТЫ: {s.reconnects_total} всего")
        if s.last_error:
            err_dt = datetime.utcfromtimestamp(s.last_error_ts).strftime("%Y-%m-%d %H:%M:%S UTC")
            lines.append(f"Последняя ошибка: {s.last_error}  @ {err_dt}")

        lines.append("=" * 66)
        return "\n".join(lines)

    async def run(self):
        self._running = True
        while self._running:
            await asyncio.sleep(SNAPSHOT_INTERVAL)
            now     = time.time()
            elapsed = now - self._last_ts
            try:
                self.log_manager.log(self._build(elapsed))
            except Exception:
                pass
            self.stats.reset_window()
            self._last_ts = now

    def stop(self):
        self._running = False


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

async def create_redis() -> aioredis.Redis:
    """Создаёт async Redis-клиент с ретраями."""
    for attempt in range(5):
        try:
            client = aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30,
            )
            await client.ping()
            return client
        except Exception as e:
            if attempt < 4:
                await asyncio.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"Не удалось подключиться к Redis: {e}") from e


def load_symbols(exchange: str, market: str) -> List[str]:
    """Загрузить список символов из dictionaries/subscribe/{exchange}/{exchange}_{market}.txt"""
    path = SUBSCRIBE_DIR / exchange / f"{exchange}_{market}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Subscribe-файл не найден: {path}")
    return [
        line.strip().upper()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]


def chunk_list(lst: list, n: int) -> List[list]:
    """Разбить список на подсписки по n элементов."""
    return [lst[i: i + n] for i in range(0, len(lst), n)]
