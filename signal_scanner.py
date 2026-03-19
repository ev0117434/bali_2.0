#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
signal_scanner.py — Сканер арбитражных сигналов spot → futures.

Читает списки символов из dictionaries/combination/,
каждые SCAN_INTERVAL секунд опрашивает Redis через pipeline,
вычисляет спред (bid_futures - ask_spot) / ask_spot * 100
и записывает сигналы в signal/signals.csv.

Формат строки сигнала (CSV):
    spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
    binance,bybit,BTCUSDT,45000.1,45451.5,1.0023,1741234567890

Лог-файлы (JSON Lines) ротируются каждые 24ч, хранится 2 последних чанка:
    logs/signal_scanner/YYYYMMDD_HHMMSS-ongoing/signal_scanner.log

Каждая запись лога — отдельный JSON объект (тип: snapshot / signal / event).

Задержки, которые измеряются:
    pipeline_ms  — время выполнения redis pipeline (мс)
    data_age_ms  — разница между now и ts цены в Redis (насколько старые данные)

Cooldown: один сигнал по одному направлению+символу раз в SIGNAL_COOLDOWN секунд.

Переменные окружения:
    REDIS_HOST        (default: 127.0.0.1)
    REDIS_PORT        (default: 6379)
    REDIS_DB          (default: 0)
    REDIS_PASSWORD    (default: пусто)
    SCAN_INTERVAL     (default: 0.2)    — секунд между опросами Redis
    SIGNAL_COOLDOWN   (default: 3600)   — секунд между повторными сигналами
    MIN_SPREAD_PCT    (default: 0.1)    — минимальный спред для сигнала, %

Использование:
    python3 signal_scanner.py
"""

import asyncio
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis

# ─── Пути ────────────────────────────────────────────────────────────────────
PROJECT_ROOT    = Path(__file__).resolve().parent
COMBINATION_DIR = PROJECT_ROOT / "dictionaries" / "combination"
SIGNAL_DIR      = PROJECT_ROOT / "signal"
SIGNAL_FILE     = SIGNAL_DIR / "signals.csv"
LOGS_DIR        = PROJECT_ROOT / "logs"

# ─── Redis ────────────────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# ─── Настройки сканера ────────────────────────────────────────────────────────
SCAN_INTERVAL   = float(os.getenv("SCAN_INTERVAL",   "0.2"))    # секунд
SIGNAL_COOLDOWN = float(os.getenv("SIGNAL_COOLDOWN", "3600"))   # секунд
MIN_SPREAD_PCT  = float(os.getenv("MIN_SPREAD_PCT",  "0.1"))    # %

SNAPSHOT_INTERVAL = 10   # секунд между снапшотами в лог
SCRIPT_NAME       = "signal_scanner"

LOG_CHUNK_SECONDS = 24 * 60 * 60   # 24ч на чанк
LOG_MAX_CHUNKS    = 2               # хранить 2 завершённых чанка


# ══════════════════════════════════════════════════════════════════════════════
# LogManager — JSON Lines с ротацией (аналог redis_monitor.py)
# ══════════════════════════════════════════════════════════════════════════════

class LogManager:
    """
    Ротирующий лог-файл, пишет JSON Lines.

    Структура:
        logs/signal_scanner/
          20240318_120000-ongoing/signal_scanner.log  ← текущий чанк
          20240317_120000-20240318_120000/             ← завершённый чанк
    """

    def __init__(self):
        self._base = LOGS_DIR / SCRIPT_NAME
        self._base.mkdir(parents=True, exist_ok=True)
        self._chunk_start: float = 0.0
        self._chunk_dir: Optional[Path] = None
        self._fh = None
        self._open_chunk()
        self._cleanup()

    @staticmethod
    def _ts_str() -> str:
        return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    def _open_chunk(self) -> None:
        name = f"{self._ts_str()}-ongoing"
        self._chunk_dir = self._base / name
        self._chunk_dir.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._chunk_dir / f"{SCRIPT_NAME}.log", "a", encoding="utf-8")
        self._chunk_start = time.time()

    def _close_chunk(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None
        if self._chunk_dir and self._chunk_dir.exists():
            new_name = self._chunk_dir.name.replace("-ongoing", f"-{self._ts_str()}")
            try:
                self._chunk_dir.rename(self._chunk_dir.parent / new_name)
            except Exception:
                pass

    def _cleanup(self) -> None:
        chunks = sorted(
            [d for d in self._base.iterdir()
             if d.is_dir() and "-ongoing" not in d.name],
            key=lambda d: d.name,
        )
        while len(chunks) > LOG_MAX_CHUNKS:
            shutil.rmtree(chunks.pop(0), ignore_errors=True)

    def _rotate_if_needed(self) -> None:
        if time.time() - self._chunk_start >= LOG_CHUNK_SECONDS:
            self._close_chunk()
            self._open_chunk()
            self._cleanup()

    def write(self, obj: Dict[str, Any]) -> None:
        """Сериализует obj в JSON и пишет одну строку в лог-файл."""
        self._rotate_if_needed()
        if self._fh:
            self._fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
            self._fh.flush()

    def close(self) -> None:
        self._close_chunk()


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _percentile(data: List[float], pct: float) -> float:
    if not data:
        return 0.0
    sd  = sorted(data)
    idx = min(int(len(sd) * pct / 100), len(sd) - 1)
    return sd[idx]


def _latency_stats(samples: List[float]) -> Dict[str, float]:
    if not samples:
        return {"min": 0.0, "avg": 0.0, "p95": 0.0, "max": 0.0, "samples": 0}
    return {
        "min":     round(min(samples), 3),
        "avg":     round(sum(samples) / len(samples), 3),
        "p95":     round(_percentile(samples, 95), 3),
        "max":     round(max(samples), 3),
        "samples": len(samples),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Загрузка направлений
# ══════════════════════════════════════════════════════════════════════════════

def load_directions() -> List[Tuple[str, str, List[str]]]:
    """
    Читает все файлы {spot_exch}_spot_{fut_exch}_futures.txt из COMBINATION_DIR.
    Возвращает список (spot_exch, fut_exch, [symbols]).
    """
    result = []
    if not COMBINATION_DIR.exists():
        return result
    for fpath in sorted(COMBINATION_DIR.glob("*_spot_*_futures.txt")):
        stem  = fpath.stem   # e.g. "binance_spot_bybit_futures"
        parts = stem.split("_spot_")
        if len(parts) != 2:
            continue
        spot_exch = parts[0]
        fut_exch  = parts[1].replace("_futures", "")
        symbols = [
            line.strip().upper()
            for line in fpath.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        ]
        if symbols:
            result.append((spot_exch, fut_exch, symbols))
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Redis
# ══════════════════════════════════════════════════════════════════════════════

async def create_redis() -> aioredis.Redis:
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
        except Exception as exc:
            if attempt < 4:
                await asyncio.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"Redis connection failed: {exc}") from exc


# ══════════════════════════════════════════════════════════════════════════════
# Запись сигналов в CSV
# ══════════════════════════════════════════════════════════════════════════════

def _append_signal_csv(
    spot_exch: str, fut_exch: str, symbol: str,
    ask_spot: float, bid_futures: float,
    spread_pct: float, ts_ms: int,
) -> None:
    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    line = (
        f"{spot_exch},{fut_exch},{symbol},"
        f"{ask_spot},{bid_futures},"
        f"{spread_pct:.4f},{ts_ms}\n"
    )
    with open(SIGNAL_FILE, "a", encoding="utf-8") as fh:
        fh.write(line)


# ══════════════════════════════════════════════════════════════════════════════
# Статистика окна
# ══════════════════════════════════════════════════════════════════════════════

class WindowStats:
    def __init__(self):
        self.start_ts = time.time()

        # Счётчики (total накапливается, window сбрасывается каждые 10с)
        self.scans_total    = 0
        self.scans_window   = 0
        self.signals_total  = 0
        self.signals_window = 0
        self.errors_total   = 0
        self.errors_window  = 0

        # Задержки за окно (мс)
        self.pipeline_ms_window: List[float] = []   # время выполнения pipe.execute()
        self.data_age_ms_window: List[float]  = []  # now - ts_redis (по каждой цене)
        self.signal_age_ms_window: List[float] = [] # data_age в момент сигнала

        # Последние события
        self.last_signal:  Optional[Dict] = None
        self.last_error:   str            = ""
        self.last_error_ts: float         = 0.0

        # Cooldown skipped
        self.cooldown_skipped_total  = 0
        self.cooldown_skipped_window = 0

    def record_scan(self, pipeline_ms: float):
        self.scans_total   += 1
        self.scans_window  += 1
        self.pipeline_ms_window.append(pipeline_ms)

    def record_data_age(self, age_ms: float):
        self.data_age_ms_window.append(age_ms)

    def record_signal(self, signal_obj: Dict, age_ms: float):
        self.signals_total   += 1
        self.signals_window  += 1
        self.signal_age_ms_window.append(age_ms)
        self.last_signal = signal_obj

    def record_error(self, err: str):
        self.errors_total   += 1
        self.errors_window  += 1
        self.last_error     = err[:300]
        self.last_error_ts  = time.time()

    def record_cooldown_skip(self, n: int):
        self.cooldown_skipped_total  += n
        self.cooldown_skipped_window += n

    def reset_window(self):
        self.scans_window            = 0
        self.signals_window          = 0
        self.errors_window           = 0
        self.cooldown_skipped_window = 0
        self.pipeline_ms_window.clear()
        self.data_age_ms_window.clear()
        self.signal_age_ms_window.clear()


# ══════════════════════════════════════════════════════════════════════════════
# Snapshot-логгер
# ══════════════════════════════════════════════════════════════════════════════

class SnapshotLogger:
    def __init__(self, log: LogManager, stats: WindowStats,
                 directions: int, pairs: int):
        self._log       = log
        self._stats     = stats
        self._directions = directions
        self._pairs      = pairs
        self._last_ts    = time.time()
        self._running    = False

    def _build(self, elapsed: float) -> Dict[str, Any]:
        s   = self._stats
        now = time.time()

        rate_scans   = s.scans_window   / elapsed if elapsed > 0 else 0.0
        rate_signals = s.signals_window / elapsed if elapsed > 0 else 0.0

        return {
            "type":     "snapshot",
            "ts":       _now_iso(),
            "uptime_s": round(now - s.start_ts, 1),

            "config": {
                "directions":      self._directions,
                "pairs":           self._pairs,
                "scan_interval_s": SCAN_INTERVAL,
                "cooldown_s":      SIGNAL_COOLDOWN,
                "min_spread_pct":  MIN_SPREAD_PCT,
                "signal_file":     str(SIGNAL_FILE),
            },

            "scans": {
                "total":       s.scans_total,
                "window":      s.scans_window,
                "rate_per_s":  round(rate_scans, 2),
            },

            "signals": {
                "total":       s.signals_total,
                "window":      s.signals_window,
                "rate_per_s":  round(rate_signals, 4),
            },

            "errors": {
                "total":   s.errors_total,
                "window":  s.errors_window,
            },

            "cooldown_skipped": {
                "total":  s.cooldown_skipped_total,
                "window": s.cooldown_skipped_window,
            },

            # Задержка выполнения redis pipeline (мс)
            "pipeline_latency_ms": _latency_stats(s.pipeline_ms_window),

            # Возраст данных в Redis (now - ts_поля) по всем прочитанным ценам (мс)
            "data_age_ms": _latency_stats(s.data_age_ms_window),

            # Возраст данных именно в момент сигнала (мс)
            "signal_data_age_ms": _latency_stats(s.signal_age_ms_window),

            "last_signal": s.last_signal,
            "last_error":  s.last_error or None,
        }

    async def run(self):
        self._running = True
        while self._running:
            await asyncio.sleep(SNAPSHOT_INTERVAL)
            now     = time.time()
            elapsed = now - self._last_ts
            try:
                snap = self._build(elapsed)
                self._log.write(snap)
                # Краткий вывод в stdout
                s = self._stats
                print(
                    f"[snapshot] scans={s.scans_total:,}({s.scans_window}/win) "
                    f"rate={snap['scans']['rate_per_s']:.1f}/s | "
                    f"signals={s.signals_total}({s.signals_window}/win) | "
                    f"pipe={snap['pipeline_latency_ms']['avg']:.2f}ms(avg) "
                    f"p95={snap['pipeline_latency_ms']['p95']:.2f}ms | "
                    f"data_age={snap['data_age_ms']['avg']:.0f}ms(avg) "
                    f"p95={snap['data_age_ms']['p95']:.0f}ms",
                    flush=True,
                )
            except Exception:
                pass
            self._stats.reset_window()
            self._last_ts = now

    def stop(self):
        self._running = False


# ══════════════════════════════════════════════════════════════════════════════
# Основной цикл сканирования
# ══════════════════════════════════════════════════════════════════════════════

async def scan_loop(
    redis_client: aioredis.Redis,
    directions:   List[Tuple[str, str, List[str]]],
    log:          LogManager,
    stats:        WindowStats,
) -> None:
    cooldown: Dict[str, float] = {}   # ckey → timestamp последнего сигнала

    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        now   = time.time()
        ts_ms = int(now * 1000)

        # ── Строим pipeline для пар без активного cooldown ────────────────────
        pipe     = redis_client.pipeline(transaction=False)
        requests: List[Tuple[str, str, str, str]] = []   # (spot, fut, sym, ckey)
        skipped  = 0

        for spot_exch, fut_exch, symbols in directions:
            for sym in symbols:
                ckey = f"{spot_exch}:{fut_exch}:{sym}"
                if now - cooldown.get(ckey, 0.0) < SIGNAL_COOLDOWN:
                    skipped += 1
                    continue
                # hmget возвращает [ask, ts] и [bid, ts]
                pipe.hmget(f"md:{spot_exch}:spot:{sym}",    "ask", "ts")
                pipe.hmget(f"md:{fut_exch}:futures:{sym}",  "bid", "ts")
                requests.append((spot_exch, fut_exch, sym, ckey))

        if skipped:
            stats.record_cooldown_skip(skipped)

        if not requests:
            continue

        # ── Выполняем pipeline, замеряем время ───────────────────────────────
        pipe_t0 = time.perf_counter()
        try:
            results = await pipe.execute()
        except Exception as exc:
            err = repr(exc)[:200]
            stats.record_error(err)
            log.write({"type": "error", "ts": _now_iso(), "error": err})
            continue
        pipeline_ms = (time.perf_counter() - pipe_t0) * 1000
        stats.record_scan(pipeline_ms)

        # ── Обрабатываем результаты ───────────────────────────────────────────
        for i, (spot_exch, fut_exch, sym, ckey) in enumerate(requests):
            spot_row = results[i * 2]      # [ask, ts]
            fut_row  = results[i * 2 + 1]  # [bid, ts]

            if not spot_row or spot_row[0] is None:
                continue
            if not fut_row  or fut_row[0]  is None:
                continue

            try:
                ask_spot    = float(spot_row[0])
                bid_futures = float(fut_row[0])
            except (TypeError, ValueError):
                continue

            if ask_spot <= 0:
                continue

            # Возраст данных: берём минимальный ts из двух цен
            try:
                spot_ts  = float(spot_row[1]) if spot_row[1] else now
                fut_ts   = float(fut_row[1])  if fut_row[1]  else now
                older_ts = min(spot_ts, fut_ts)
                age_ms   = (now - older_ts) * 1000
            except (TypeError, ValueError):
                age_ms = 0.0

            stats.record_data_age(age_ms)

            # Спот всегда дешевле фьючерса
            spread_pct = (bid_futures - ask_spot) / ask_spot * 100

            if spread_pct < MIN_SPREAD_PCT:
                continue

            # ── Сигнал ───────────────────────────────────────────────────────
            cooldown[ckey] = now

            signal_obj = {
                "type":          "signal",
                "ts":            _now_iso(),
                "ts_ms":         ts_ms,
                "spot_exch":     spot_exch,
                "fut_exch":      fut_exch,
                "symbol":        sym,
                "ask_spot":      ask_spot,
                "bid_futures":   bid_futures,
                "spread_pct":    round(spread_pct, 4),
                "data_age_spot_ms": round((now - float(spot_row[1] or now)) * 1000, 1),
                "data_age_fut_ms":  round((now - float(fut_row[1]  or now)) * 1000, 1),
            }

            _append_signal_csv(
                spot_exch, fut_exch, sym,
                ask_spot, bid_futures, spread_pct, ts_ms,
            )
            log.write(signal_obj)
            stats.record_signal(signal_obj, age_ms)

            print(
                f"[signal] {spot_exch}→{fut_exch} {sym} "
                f"ask={ask_spot} bid={bid_futures} "
                f"spread={spread_pct:.4f}% "
                f"age={age_ms:.0f}ms",
                flush=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# Точка входа
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    log = LogManager()

    log.write({
        "type":   "start",
        "ts":     _now_iso(),
        "config": {
            "redis":            f"{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
            "scan_interval_s":  SCAN_INTERVAL,
            "signal_cooldown_s": SIGNAL_COOLDOWN,
            "min_spread_pct":   MIN_SPREAD_PCT,
            "signal_file":      str(SIGNAL_FILE),
            "combination_dir":  str(COMBINATION_DIR),
        },
    })
    print(f"[{SCRIPT_NAME}] Запуск...", flush=True)

    directions = load_directions()
    if not directions:
        msg = f"Не найдено файлов направлений в {COMBINATION_DIR}"
        log.write({"type": "fatal", "ts": _now_iso(), "error": msg})
        print(f"[{SCRIPT_NAME}] FATAL: {msg}", flush=True)
        sys.exit(1)

    total_pairs = sum(len(s) for _, _, s in directions)
    log.write({
        "type":       "directions_loaded",
        "ts":         _now_iso(),
        "directions": len(directions),
        "pairs":      total_pairs,
    })
    print(
        f"[{SCRIPT_NAME}] Направлений: {len(directions)}, пар: {total_pairs}",
        flush=True,
    )

    redis_client = await create_redis()
    log.write({
        "type":  "redis_connected",
        "ts":    _now_iso(),
        "redis": f"{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
    })
    print(f"[{SCRIPT_NAME}] Redis: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}", flush=True)

    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)

    stats       = WindowStats()
    snap_logger = SnapshotLogger(log, stats, len(directions), total_pairs)

    snap_task = asyncio.create_task(snap_logger.run())
    scan_task = asyncio.create_task(
        scan_loop(redis_client, directions, log, stats)
    )

    try:
        await asyncio.gather(snap_task, scan_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        snap_logger.stop()
        snap_task.cancel()
        scan_task.cancel()
        log.write({"type": "stop", "ts": _now_iso(),
                   "scans_total": stats.scans_total,
                   "signals_total": stats.signals_total})
        print(f"\n[{SCRIPT_NAME}] Остановлен.", flush=True)
        log.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
