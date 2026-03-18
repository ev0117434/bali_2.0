#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/common.py — Shared utilities for market data collectors.

Contains:
- Stats / ConnectionStats  : per-window statistics for snapshots
- LogManager               : rotating log files (24h chunks, keep 2)
- ChunkManager             : 20-min price-history chunks in Redis (keep 5)
- SnapshotLogger           : JSON-Lines snapshot every 10 seconds
- create_redis()           : async Redis client with retry
- load_symbols()           : load symbol list from subscribe file
- chunk_list()             : split list into equal chunks
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

# Force UTF-8 on stdout/stderr (needed when launched via run_all.py subprocess
# or in environments without a UTF-8 locale)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ══════════════════════════════════════════════════════════════════════════════
# JSON-Lines log formatter
# ══════════════════════════════════════════════════════════════════════════════

class _JsonFormatter(logging.Formatter):
    """
    Formats every log record as a single JSON object (JSON Lines).

    Regular messages:
        {"ts":"2026-03-18T16:33:07.886Z","script":"binance_spot","level":"INFO","msg":"Starting..."}

    Snapshot entries produced by SnapshotLogger are already complete JSON
    objects; they are passed through unchanged so the log file stays valid
    JSON Lines throughout.
    """

    def __init__(self, script_name: str):
        super().__init__()
        self._script = script_name

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        # SnapshotLogger emits a pre-built JSON object — pass it through as-is
        if msg.startswith("{") and msg.endswith("}"):
            return msg
        ts = (
            datetime.utcfromtimestamp(record.created).strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{int(record.msecs):03d}Z"
        )
        return json.dumps(
            {"ts": ts, "script": self._script, "level": record.levelname, "msg": msg},
            ensure_ascii=False,
        )


# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT  = Path(__file__).resolve().parent.parent
LOGS_DIR      = PROJECT_ROOT / "logs"
SUBSCRIBE_DIR = PROJECT_ROOT / "dictionaries" / "subscribe"

# ─── Redis ────────────────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# ─── Timings ──────────────────────────────────────────────────────────────────
HISTORY_CHUNK_SECONDS  = 20 * 60        # 20 minutes per history chunk
HISTORY_MAX_CHUNKS     = 5             # keep last 5 chunks

LOG_CHUNK_SECONDS      = 24 * 60 * 60  # 24 hours per log chunk
LOG_MAX_CHUNKS         = 2             # keep last 2 completed chunks

SNAPSHOT_INTERVAL      = 10    # seconds between snapshots
TICKER_FLUSH_INTERVAL  = 0.05  # seconds between ticker batch writes (50 ms)
HISTORY_FLUSH_INTERVAL = 1.0   # seconds between history batch writes


# ══════════════════════════════════════════════════════════════════════════════
# Statistics
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ConnectionStats:
    url:           str   = ""
    active:        bool  = False
    msgs_total:    int   = 0
    msgs_window:   int   = 0
    last_msg_ts:   float = 0.0
    reconnects:    int   = 0
    last_error:    str   = ""
    last_error_ts: float = 0.0


@dataclass
class Stats:
    start_ts:    float = field(default_factory=time.time)
    connections: List[ConnectionStats] = field(default_factory=list)

    msgs_total:   int = 0
    msgs_window:  int = 0

    ticker_writes_total:  int = 0
    ticker_writes_window: int = 0

    history_entries_total:  int = 0
    history_entries_window: int = 0

    # RTT: exchange_timestamp → recv_ts  (network latency exchange→script)
    # Not available for Binance bookTicker (no exchange timestamp in stream)
    rtt_latencies_window:  List[float] = field(default_factory=list)

    # Internal: recv_ts → redis write complete  (buffer time + pipeline)
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
        """Called from ticker_flusher after pipeline write completes."""
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
    Rotating log files — JSON Lines format.
    - One chunk = 24 h; directory name: YYYYMMDD_HHMMSS-YYYYMMDD_HHMMSS
    - Keep LOG_MAX_CHUNKS completed chunks; delete oldest when (N+1)-th opens.
    """

    def __init__(self, script_name: str):
        self.script_name  = script_name
        self.log_dir_root = LOGS_DIR / script_name
        self.log_dir_root.mkdir(parents=True, exist_ok=True)

        self._current_dir:      Optional[Path]           = None
        self._current_start_ts: float                    = 0.0
        self._logger:           Optional[logging.Logger] = None
        self._completed_dirs:   List[Path]               = []

    # ── internal ─────────────────────────────────────────────────────────────

    @staticmethod
    def _dt_str(ts: float) -> str:
        return datetime.utcfromtimestamp(ts).strftime("%Y%m%d_%H%M%S")

    def _scan_completed(self):
        """Find and sort directories of completed chunks."""
        dirs = sorted(
            [d for d in self.log_dir_root.iterdir()
             if d.is_dir() and "-" in d.name and "ongoing" not in d.name],
            key=lambda d: d.name,
        )
        self._completed_dirs = dirs

    def _close_current(self):
        """Close current chunk: flush handlers and rename directory."""
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

        fmt = _JsonFormatter(self.script_name)

        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

        # Prevent propagation to root logger
        logger.propagate = False
        self._logger = logger

    def _purge_old_chunks(self):
        """Delete oldest chunks that exceed LOG_MAX_CHUNKS."""
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
    Manages 20-minute price-history chunks in Redis.

    Key schema:
      md:hist:{exchange}:{market}:{symbol}:{chunk_id}  — Redis List, items "{ts},{bid},{ask}"
      md:chunks:config                                 — Hash, field per exchange:market

    Rotation logic:
      Keep HISTORY_MAX_CHUNKS = 5 chunks.
      When chunk N+1 is created, chunk N-4 is deleted.
    """

    def __init__(self, exchange: str, market: str, redis_client: aioredis.Redis):
        self.exchange = exchange
        self.market   = market
        self.redis    = redis_client
        self.field_key  = f"{exchange}:{market}"   # field in global Hash
        self.config_key = "md:chunks:config"        # single key for the whole system

        self.current_chunk_id: int          = 0
        self.chunk_start_ts:   float        = 0.0
        self.active_chunks:    List[int]    = []
        self.chunk_meta:       Dict[str, Any] = {}

    @staticmethod
    def _dt_str(ts: float) -> str:
        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")

    # ── internal ─────────────────────────────────────────────────────────────

    async def _save(self):
        """Persist this exchange:market config into global Hash md:chunks:config."""
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
                # If current chunk has already expired — rotate immediately
                if time.time() - self.chunk_start_ts >= HISTORY_CHUNK_SECONDS:
                    await self.rotate()
                return
            except Exception:
                pass
        await self._new_first_chunk()

    async def rotate(self):
        """
        Close current chunk, open a new one.
        If active chunks exceed HISTORY_MAX_CHUNKS — delete the oldest.
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

        # Delete oldest chunk if over limit
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
        """Human-readable chunk status (used in debugging)."""
        lines = [
            f"  Chunk #{self.current_chunk_id}: started {self.elapsed():.0f}s ago, "
            f"~{self.remaining():.0f}s remaining",
            f"  Active chunks: {self.active_chunks}",
        ]
        for cid_str, meta in sorted(self.chunk_meta.items(), key=lambda x: int(x[0])):
            start = meta.get("start_dt", "?")
            end   = meta.get("end_dt", "(current)")
            lines.append(f"    Chunk {cid_str}: {start} -> {end}")
        return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# SnapshotLogger
# ══════════════════════════════════════════════════════════════════════════════

class SnapshotLogger:
    """Emits a JSON-Lines snapshot every SNAPSHOT_INTERVAL seconds."""

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
    def _percentile(data: List[float], pct: float) -> float:
        if not data:
            return 0.0
        sd  = sorted(data)
        idx = min(int(len(sd) * pct / 100), len(sd) - 1)
        return sd[idx]

    def _build(self, elapsed: float) -> str:
        """Return a single JSON-Lines string representing the current snapshot."""
        s   = self.stats
        now = time.time()
        ts  = (
            datetime.utcfromtimestamp(now).strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{int((now % 1) * 1000):03d}Z"
        )

        rate_msg  = s.msgs_window  / elapsed if elapsed > 0 else 0.0
        rate_tick = s.ticker_writes_window / elapsed if elapsed > 0 else 0.0
        rate_hist = s.history_entries_window / elapsed if elapsed > 0 else 0.0

        def _lat(data: List[float]) -> Dict[str, Any]:
            if not data:
                return {"samples": 0, "min_ms": None, "avg_ms": None,
                        "p95_ms": None, "max_ms": None}
            return {
                "samples": len(data),
                "min_ms":  round(min(data), 2),
                "avg_ms":  round(sum(data) / len(data), 2),
                "p95_ms":  round(self._percentile(data, 95), 2),
                "max_ms":  round(max(data), 2),
            }

        obj: Dict[str, Any] = {
            "ts":       ts,
            "script":   self.script_name,
            "type":     "snapshot",
            "uptime_s": int(now - s.start_ts),
            "connections": [
                {
                    "id":          i + 1,
                    "status":      "alive" if c.active else "dead",
                    "msgs_total":  c.msgs_total,
                    "msgs_window": c.msgs_window,
                    "reconnects":  c.reconnects,
                    "last_error":  c.last_error or None,
                    "url":         c.url,
                }
                for i, c in enumerate(s.connections)
            ],
            "throughput": {
                "msgs_total":             s.msgs_total,
                "msgs_per_s":             round(rate_msg, 1),
                "ticker_writes_total":    s.ticker_writes_total,
                "ticker_writes_per_s":    round(rate_tick, 1),
                "history_entries_total":  s.history_entries_total,
                "history_entries_per_s":  round(rate_hist, 1),
            },
            "symbols": {
                "tracked":       s.symbols_tracked,
                "active_window": len(s.symbols_active_window),
            },
            "latency": {
                "rtt":  _lat(s.rtt_latencies_window),
                "proc": _lat(s.proc_latencies_window),
            },
            "reconnects_total": s.reconnects_total,
            "last_error":       s.last_error or None,
        }

        if self.chunk_manager:
            cm = self.chunk_manager
            obj["history_chunk"] = {
                "chunk_id":      cm.current_chunk_id,
                "elapsed_s":     int(cm.elapsed()),
                "remaining_s":   int(cm.remaining()),
                "active_chunks": cm.active_chunks,
            }

        return json.dumps(obj, ensure_ascii=False)

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
    """Create async Redis client with exponential-backoff retry (5 attempts)."""
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
                raise RuntimeError(f"Failed to connect to Redis: {e}") from e


def load_symbols(exchange: str, market: str) -> List[str]:
    """Load symbol list from dictionaries/subscribe/{exchange}/{exchange}_{market}.txt"""
    path = SUBSCRIBE_DIR / exchange / f"{exchange}_{market}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Subscribe file not found: {path}")
    return [
        line.strip().upper()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]


def chunk_list(lst: list, n: int) -> List[list]:
    """Split list into sublists of at most n elements."""
    return [lst[i: i + n] for i in range(0, len(lst), n)]
