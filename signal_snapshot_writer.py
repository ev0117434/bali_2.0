#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
signal_snapshot_writer.py — Запись снапшотов активных сигналов каждые 0.3 секунды.

Каждые SNAPSHOT_WRITE_INTERVAL секунд опрашивает Redis по всем парам из
dictionaries/combination/. Когда спред пары впервые >= MIN_SPREAD_PCT —
запускается окно записи длиной SNAPSHOT_DURATION секунд. В течение этого
окна строки пишутся каждые 0.3 с независимо от дальнейшего поведения спреда.
По истечении окна запись для этой пары останавливается навсегда (до рестарта).

Структура выходных файлов:
    signal_snapshots/
    └── YYYY-MM-DD/                        ← папка дня (UTC)
        └── HH/                            ← папка часа (UTC, 00–23)
            └── {spot}_s_{fut}_f_{sym}.csv ← файл пары (только активные сигналы)

Пример имени файла:
    binance_s_bybit_f_BTCUSDT.csv

Формат строки CSV (первая строка файла — заголовок):
    spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
    binance,bybit,BTCUSDT,45000.10,45676.35,1.5023,1741234567890

Логи (JSON Lines) — logs/signal_snapshot_writer/:
    type: "start"     — запуск скрипта
    type: "snapshot"  — каждые 10 с: метрики производительности и задержек
    type: "warning"   — превышение целевого интервала цикла (overrun)
    type: "error"     — ошибка Redis pipeline или записи в файл
    type: "stop"      — остановка скрипта

Переменные окружения:
    REDIS_HOST               (default: 127.0.0.1)
    REDIS_PORT               (default: 6379)
    REDIS_DB                 (default: 0)
    REDIS_PASSWORD           (default: пусто)
    SNAPSHOT_WRITE_INTERVAL  (default: 0.3)    — секунд между записями
    MIN_SPREAD_PCT           (default: 1.5)    — минимальный спред для запуска окна, %
    SNAPSHOT_DURATION        (default: 3500)   — секунд записи после первого сигнала

Использование:
    python3 signal_snapshot_writer.py
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
SNAPSHOTS_DIR   = PROJECT_ROOT / "signal_snapshots"
LOGS_DIR        = PROJECT_ROOT / "logs"

# ─── Redis ────────────────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# ─── Настройки ────────────────────────────────────────────────────────────────
WRITE_INTERVAL     = float(os.getenv("SNAPSHOT_WRITE_INTERVAL", "0.3"))  # секунд
MIN_SPREAD_PCT     = float(os.getenv("MIN_SPREAD_PCT", "1.5"))            # %
SNAPSHOT_DURATION  = float(os.getenv("SNAPSHOT_DURATION", "3500"))        # секунд
LOG_SNAPSHOT_EVERY = 10    # секунд между JSON-снапшотами в лог
OVERRUN_THRESHOLD  = 2.0   # лог warning если цикл занял > WRITE_INTERVAL * это число
SCRIPT_NAME        = "signal_snapshot_writer"

LOG_CHUNK_SECONDS = 24 * 60 * 60   # 24ч на чанк
LOG_MAX_CHUNKS    = 2               # хранить 2 завершённых чанка


# ══════════════════════════════════════════════════════════════════════════════
# LogManager — JSON Lines с ротацией 24ч
# ══════════════════════════════════════════════════════════════════════════════

class LogManager:
    """
    Ротирующий лог-файл, пишет JSON Lines.

    Структура:
        logs/signal_snapshot_writer/
          20240318_120000-ongoing/signal_snapshot_writer.log  ← текущий чанк
          20240317_120000-20240318_120000/                    ← завершённый
    """

    def __init__(self) -> None:
        self._base = LOGS_DIR / SCRIPT_NAME
        self._base.mkdir(parents=True, exist_ok=True)
        self._chunk_start: float       = 0.0
        self._chunk_dir: Optional[Path] = None
        self._fh                        = None
        self._open_chunk()
        self._cleanup()

    @staticmethod
    def _ts_str() -> str:
        return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    def _open_chunk(self) -> None:
        name = f"{self._ts_str()}-ongoing"
        self._chunk_dir = self._base / name
        self._chunk_dir.mkdir(parents=True, exist_ok=True)
        self._fh = open(
            self._chunk_dir / f"{SCRIPT_NAME}.log", "a", encoding="utf-8"
        )
        self._chunk_start = time.time()

    def _close_chunk(self) -> None:
        if self._fh:
            try:
                self._fh.flush()
                self._fh.close()
            except Exception:
                pass
            self._fh = None
        if self._chunk_dir and self._chunk_dir.exists():
            new_name = self._chunk_dir.name.replace(
                "-ongoing", f"-{self._ts_str()}"
            )
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
            try:
                self._fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
                self._fh.flush()
            except Exception:
                pass

    def close(self) -> None:
        self._close_chunk()


# ══════════════════════════════════════════════════════════════════════════════
# FileHandleManager — открытые CSV-дескрипторы с ротацией по часу
# ══════════════════════════════════════════════════════════════════════════════

class FileHandleManager:
    """
    Кэширует открытые файловые дескрипторы CSV по парам.

    При смене UTC-часа закрывает все дескрипторы и создаёт новый слой
    папок YYYY-MM-DD/HH/. Новые дескрипторы открываются лениво при
    первом обращении к get().

    Проверку смены часа нужно вызывать один раз в начале каждого цикла
    записи методом check_rotate(), а не внутри get(), чтобы не допустить
    рассогласования дескрипторов внутри одной итерации.
    """

    def __init__(self) -> None:
        # (spot_exch, fut_exch, symbol) → file object
        self._handles: Dict[Tuple[str, str, str], Any] = {}
        self._current_hour_key: str = ""   # "YYYY-MM-DD/HH"

    @staticmethod
    def _hour_key() -> str:
        now = datetime.utcnow()
        return f"{now.strftime('%Y-%m-%d')}/{now.strftime('%H')}"

    def check_rotate(self) -> bool:
        """
        Проверяет, сменился ли UTC-час. Если да — закрывает все
        дескрипторы и обновляет текущий ключ часа.
        Возвращает True при ротации.
        """
        hk = self._hour_key()
        if hk != self._current_hour_key:
            self._close_all()
            self._current_hour_key = hk
            return True
        return False

    def _close_all(self) -> None:
        for fh in self._handles.values():
            try:
                fh.flush()
                fh.close()
            except Exception:
                pass
        self._handles.clear()

    def get(self, spot_exch: str, fut_exch: str, symbol: str) -> Any:
        """
        Возвращает открытый файловый дескриптор для пары.
        Если файл ещё не открыт — создаёт директорию, файл и дескриптор.
        При создании нового файла записывает строку заголовка CSV.
        """
        key = (spot_exch, fut_exch, symbol)
        if key not in self._handles:
            day_str, hour_str = self._current_hour_key.split("/")
            dir_path  = SNAPSHOTS_DIR / day_str / hour_str
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"{spot_exch}_s_{fut_exch}_f_{symbol}.csv"

            is_new = not file_path.exists() or file_path.stat().st_size == 0
            fh = open(file_path, "a", encoding="utf-8")
            if is_new:
                fh.write("spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts\n")
            self._handles[key] = fh

        return self._handles[key]

    def flush_all(self) -> None:
        """Принудительно сбрасывает буферы всех открытых файлов."""
        for fh in self._handles.values():
            try:
                fh.flush()
            except Exception:
                pass

    def close(self) -> None:
        self._close_all()


# ══════════════════════════════════════════════════════════════════════════════
# Stats — счётчики для JSON-снапшотов
# ══════════════════════════════════════════════════════════════════════════════

class Stats:
    def __init__(self) -> None:
        self.start_ts = time.time()

        self.cycles_total   = 0
        self.cycles_window  = 0

        self.writes_total   = 0   # строк записано
        self.writes_window  = 0

        self.skipped_total  = 0   # пар пропущено (нет данных в Redis)
        self.skipped_window = 0

        self.below_total    = 0   # пар пропущено (спред ниже порога, окно не открыто)
        self.below_window   = 0

        self.activated_total = 0  # пар, у которых открылось окно записи
        self.expired_total   = 0  # пар, у которых окно закрылось по таймауту
        self.active_now      = 0  # пар с открытым окном прямо сейчас

        self.errors_total   = 0   # ошибки Redis или файловые ошибки
        self.errors_window  = 0

        self.overruns_total  = 0  # циклов с превышением целевого интервала
        self.overruns_window = 0

        # Задержки за окно (мс)
        self.pipeline_ms_window: List[float] = []   # время выполнения pipeline
        self.write_ms_window:    List[float] = []   # время записи в файлы
        self.cycle_ms_window:    List[float] = []   # полное время цикла

        self.last_error:    str   = ""
        self.last_error_ts: float = 0.0

    def record_cycle(
        self,
        pipeline_ms: float,
        write_ms:    float,
        cycle_ms:    float,
        written:     int,
        skipped:     int,
        below:       int,
        activated:   int,
        expired:     int,
        active_now:  int,
        overrun:     bool,
    ) -> None:
        self.cycles_total  += 1
        self.cycles_window += 1
        self.writes_total  += written
        self.writes_window += written
        self.skipped_total  += skipped
        self.skipped_window += skipped
        self.below_total    += below
        self.below_window   += below
        self.activated_total += activated
        self.expired_total   += expired
        self.active_now       = active_now
        self.pipeline_ms_window.append(pipeline_ms)
        self.write_ms_window.append(write_ms)
        self.cycle_ms_window.append(cycle_ms)
        if overrun:
            self.overruns_total  += 1
            self.overruns_window += 1

    def record_error(self, err: str) -> None:
        self.errors_total  += 1
        self.errors_window += 1
        self.last_error    = err[:300]
        self.last_error_ts = time.time()

    def reset_window(self) -> None:
        self.cycles_window   = 0
        self.writes_window   = 0
        self.skipped_window  = 0
        self.below_window    = 0
        self.errors_window   = 0
        self.overruns_window = 0
        # activated/expired/active_now — не сбрасываем (total-метрики)
        self.pipeline_ms_window.clear()
        self.write_ms_window.clear()
        self.cycle_ms_window.clear()


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _float_ts_to_iso(ts: float) -> str:
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _percentile(data: List[float], pct: float) -> float:
    if not data:
        return 0.0
    sd  = sorted(data)
    idx = min(int(len(sd) * pct / 100), len(sd) - 1)
    return sd[idx]


def _latency_stats(samples: List[float]) -> Dict[str, Any]:
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
# Загрузка направлений из combination/
# ══════════════════════════════════════════════════════════════════════════════

def load_directions() -> List[Tuple[str, str, List[str]]]:
    """
    Читает файлы {spot_exch}_spot_{fut_exch}_futures.txt из COMBINATION_DIR.
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
# Основной цикл записи
# ══════════════════════════════════════════════════════════════════════════════

async def write_loop(
    redis_client: aioredis.Redis,
    pairs:        List[Tuple[str, str, str]],   # [(spot_exch, fut_exch, symbol)]
    log:          LogManager,
    stats:        Stats,
    fhm:          FileHandleManager,
) -> None:
    """
    Каждые WRITE_INTERVAL секунд:
      1. Ротация файловых дескрипторов при смене часа.
      2. Redis pipeline — читаем ask_spot и bid_futures для всех пар.
      3. Для каждой пары:
           - нет данных → skipped
           - спред >= MIN_SPREAD_PCT и пара ещё не в active_windows → открываем окно
           - пара в active_windows и elapsed >= SNAPSHOT_DURATION → закрываем окно (навсегда)
           - пара в active_windows и elapsed < SNAPSHOT_DURATION → пишем строку
           - иначе → below_threshold
      4. Сброс буферов файлов.
      5. Ожидание до следующего цикла.

    active_windows: Dict[(spot, fut, sym) → start_ts]
      Пара попадает сюда при первом пересечении порога и остаётся
      до истечения SNAPSHOT_DURATION секунд, после чего удаляется навсегда.
    """
    # (spot_exch, fut_exch, symbol) → timestamp первого сигнала
    active_windows: Dict[Tuple[str, str, str], float] = {}
    # пары, у которых окно уже истекло — больше не запускаем
    expired_pairs:  set = set()

    while True:
        cycle_t0 = time.perf_counter()
        now      = time.time()
        ts_ms    = int(now * 1000)

        # ── 1. Ротация часа ───────────────────────────────────────────────────
        fhm.check_rotate()

        # ── 2. Redis pipeline ─────────────────────────────────────────────────
        pipe = redis_client.pipeline(transaction=False)
        for spot_exch, fut_exch, sym in pairs:
            pipe.hmget(f"md:{spot_exch}:spot:{sym}",   "ask", "ts")
            pipe.hmget(f"md:{fut_exch}:futures:{sym}", "bid", "ts")

        pipe_t0 = time.perf_counter()
        try:
            results = await pipe.execute()
        except Exception as exc:
            err = repr(exc)[:200]
            stats.record_error(err)
            log.write({
                "type":  "error",
                "ts":    _now_iso(),
                "phase": "redis_pipeline",
                "error": err,
            })
            await asyncio.sleep(WRITE_INTERVAL)
            continue
        pipeline_ms = (time.perf_counter() - pipe_t0) * 1000

        # ── 3. Обработка результатов ──────────────────────────────────────────
        write_t0  = time.perf_counter()
        written   = 0
        skipped   = 0   # нет данных в Redis
        below     = 0   # спред ниже порога, окно не открыто
        activated = 0   # пар, у которых открылось окно в этом цикле
        expired   = 0   # пар, у которых окно закрылось в этом цикле

        for i, (spot_exch, fut_exch, sym) in enumerate(pairs):
            key      = (spot_exch, fut_exch, sym)
            spot_row = results[i * 2]      # [ask, ts]
            fut_row  = results[i * 2 + 1]  # [bid, ts]

            # ── A. Нет данных в Redis ─────────────────────────────────────────
            if not spot_row or spot_row[0] is None:
                skipped += 1
                continue
            if not fut_row or fut_row[0] is None:
                skipped += 1
                continue
            try:
                ask_spot    = float(spot_row[0])
                bid_futures = float(fut_row[0])
            except (TypeError, ValueError):
                skipped += 1
                continue
            if ask_spot <= 0:
                skipped += 1
                continue

            spread_pct = (bid_futures - ask_spot) / ask_spot * 100

            # ── B. Окно уже истекло — пара заблокирована навсегда ─────────────
            if key in expired_pairs:
                continue

            # ── C. Окно активно — проверяем таймаут или пишем ────────────────
            if key in active_windows:
                elapsed = now - active_windows[key]
                if elapsed >= SNAPSHOT_DURATION:
                    # Окно закрылось — переносим в expired, логируем
                    del active_windows[key]
                    expired_pairs.add(key)
                    expired += 1
                    log.write({
                        "type":        "window_expired",
                        "ts":          _now_iso(),
                        "spot_exch":   spot_exch,
                        "fut_exch":    fut_exch,
                        "symbol":      sym,
                        "duration_s":  round(elapsed, 1),
                        "spread_pct":  round(spread_pct, 4),
                    })
                    continue
                # Окно ещё активно — пишем строку
                line = (
                    f"{spot_exch},{fut_exch},{sym},"
                    f"{ask_spot},{bid_futures},"
                    f"{spread_pct:.4f},{ts_ms}\n"
                )
                try:
                    fhm.get(spot_exch, fut_exch, sym).write(line)
                    written += 1
                except Exception as exc:
                    err = repr(exc)[:200]
                    stats.record_error(err)
                    log.write({
                        "type":  "error",
                        "ts":    _now_iso(),
                        "phase": "file_write",
                        "pair":  f"{spot_exch}_s_{fut_exch}_f_{sym}",
                        "error": err,
                    })
                continue

            # ── D. Окно не открыто — проверяем, достиг ли порога ─────────────
            if spread_pct >= MIN_SPREAD_PCT:
                # Открываем окно записи
                active_windows[key] = now
                activated += 1
                log.write({
                    "type":       "window_opened",
                    "ts":         _now_iso(),
                    "spot_exch":  spot_exch,
                    "fut_exch":   fut_exch,
                    "symbol":     sym,
                    "spread_pct": round(spread_pct, 4),
                    "duration_s": SNAPSHOT_DURATION,
                })
                # Сразу пишем первую строку
                line = (
                    f"{spot_exch},{fut_exch},{sym},"
                    f"{ask_spot},{bid_futures},"
                    f"{spread_pct:.4f},{ts_ms}\n"
                )
                try:
                    fhm.get(spot_exch, fut_exch, sym).write(line)
                    written += 1
                except Exception as exc:
                    err = repr(exc)[:200]
                    stats.record_error(err)
                    log.write({
                        "type":  "error",
                        "ts":    _now_iso(),
                        "phase": "file_write",
                        "pair":  f"{spot_exch}_s_{fut_exch}_f_{sym}",
                        "error": err,
                    })
            else:
                below += 1

        # ── 4. Сброс буферов ──────────────────────────────────────────────────
        fhm.flush_all()
        write_ms = (time.perf_counter() - write_t0) * 1000

        # ── 5. Учёт статистики ────────────────────────────────────────────────
        cycle_ms = (time.perf_counter() - cycle_t0) * 1000
        overrun  = cycle_ms > WRITE_INTERVAL * 1000 * OVERRUN_THRESHOLD
        stats.record_cycle(
            pipeline_ms, write_ms, cycle_ms,
            written, skipped, below, activated, expired,
            len(active_windows), overrun,
        )

        if overrun:
            log.write({
                "type":            "warning",
                "ts":              _now_iso(),
                "msg":             "cycle_overrun",
                "cycle_ms":        round(cycle_ms, 2),
                "target_ms":       round(WRITE_INTERVAL * 1000, 1),
                "pipeline_ms":     round(pipeline_ms, 2),
                "write_ms":        round(write_ms, 2),
                "written":         written,
                "skipped":         skipped,
                "below_threshold": below,
                "active_windows":  len(active_windows),
            })

        # ── 6. Сон до следующего цикла ────────────────────────────────────────
        sleep_time = max(0.0, WRITE_INTERVAL - cycle_ms / 1000)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


# ══════════════════════════════════════════════════════════════════════════════
# Цикл снапшотов в лог
# ══════════════════════════════════════════════════════════════════════════════

async def snapshot_loop(
    log:         LogManager,
    stats:       Stats,
    total_pairs: int,
) -> None:
    """Каждые LOG_SNAPSHOT_EVERY секунд пишет JSON-снапшот в лог и stdout."""
    last_ts = time.time()

    while True:
        await asyncio.sleep(LOG_SNAPSHOT_EVERY)
        now     = time.time()
        elapsed = now - last_ts

        rate_cycles = stats.cycles_window / elapsed if elapsed > 0 else 0.0
        rate_writes = stats.writes_window / elapsed if elapsed > 0 else 0.0

        snap: Dict[str, Any] = {
            "type":     "snapshot",
            "ts":       _now_iso(),
            "uptime_s": round(now - stats.start_ts, 1),

            "config": {
                "write_interval_s": WRITE_INTERVAL,
                "min_spread_pct":   MIN_SPREAD_PCT,
                "snapshot_duration_s": SNAPSHOT_DURATION,
                "total_pairs":      total_pairs,
                "snapshots_dir":    str(SNAPSHOTS_DIR),
            },

            "cycles": {
                "total":      stats.cycles_total,
                "window":     stats.cycles_window,
                "rate_per_s": round(rate_cycles, 2),
            },

            "writes": {
                "total":      stats.writes_total,
                "window":     stats.writes_window,
                "rate_per_s": round(rate_writes, 1),
            },

            "skipped": {
                "total":  stats.skipped_total,
                "window": stats.skipped_window,
                "reason": "no_redis_data",
            },

            "below_threshold": {
                "total":     stats.below_total,
                "window":    stats.below_window,
                "threshold": MIN_SPREAD_PCT,
            },

            "windows": {
                "active_now":      stats.active_now,
                "activated_total": stats.activated_total,
                "expired_total":   stats.expired_total,
                "duration_s":      SNAPSHOT_DURATION,
            },

            "overruns": {
                "total":     stats.overruns_total,
                "window":    stats.overruns_window,
                "threshold": f">{OVERRUN_THRESHOLD}x interval",
            },

            "errors": {
                "total":   stats.errors_total,
                "window":  stats.errors_window,
                "last":    stats.last_error or None,
                "last_ts": (
                    _float_ts_to_iso(stats.last_error_ts)
                    if stats.last_error_ts else None
                ),
            },

            # Задержки (мс)
            "pipeline_latency_ms": _latency_stats(stats.pipeline_ms_window),
            "write_latency_ms":    _latency_stats(stats.write_ms_window),
            "cycle_latency_ms":    _latency_stats(stats.cycle_ms_window),
        }

        log.write(snap)

        print(
            f"[{SCRIPT_NAME}] "
            f"cycles={stats.cycles_total:,}({stats.cycles_window}/win) "
            f"writes={stats.writes_total:,} rate={rate_writes:.0f}/s | "
            f"active={stats.active_now} activated={stats.activated_total} "
            f"expired={stats.expired_total} | "
            f"pipe={snap['pipeline_latency_ms']['avg']:.2f}ms "
            f"write={snap['write_latency_ms']['avg']:.2f}ms | "
            f"err={stats.errors_total}",
            flush=True,
        )

        stats.reset_window()
        last_ts = now


# ══════════════════════════════════════════════════════════════════════════════
# Точка входа
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    log = LogManager()

    log.write({
        "type":   "start",
        "ts":     _now_iso(),
        "config": {
            "redis":               f"{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
            "write_interval_s":    WRITE_INTERVAL,
            "min_spread_pct":      MIN_SPREAD_PCT,
            "snapshot_duration_s": SNAPSHOT_DURATION,
            "snapshots_dir":       str(SNAPSHOTS_DIR),
            "combination_dir":     str(COMBINATION_DIR),
        },
    })
    print(f"[{SCRIPT_NAME}] Запуск...", flush=True)

    directions = load_directions()
    if not directions:
        msg = f"Не найдено файлов направлений в {COMBINATION_DIR}"
        log.write({"type": "fatal", "ts": _now_iso(), "error": msg})
        print(f"[{SCRIPT_NAME}] FATAL: {msg}", flush=True)
        sys.exit(1)

    # Флаттенизируем пары один раз — это единственный рабочий список
    pairs: List[Tuple[str, str, str]] = [
        (spot_exch, fut_exch, sym)
        for spot_exch, fut_exch, symbols in directions
        for sym in symbols
    ]

    log.write({
        "type":       "directions_loaded",
        "ts":         _now_iso(),
        "directions": len(directions),
        "pairs":      len(pairs),
    })
    print(
        f"[{SCRIPT_NAME}] Направлений: {len(directions)}, пар: {len(pairs)}",
        flush=True,
    )

    redis_client = await create_redis()
    log.write({
        "type":  "redis_connected",
        "ts":    _now_iso(),
        "redis": f"{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
    })
    print(
        f"[{SCRIPT_NAME}] Redis: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
        flush=True,
    )

    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    stats = Stats()
    fhm   = FileHandleManager()

    write_task = asyncio.create_task(
        write_loop(redis_client, pairs, log, stats, fhm)
    )
    snap_task = asyncio.create_task(
        snapshot_loop(log, stats, len(pairs))
    )

    try:
        await asyncio.gather(write_task, snap_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        write_task.cancel()
        snap_task.cancel()
        fhm.close()
        log.write({
            "type":         "stop",
            "ts":           _now_iso(),
            "cycles_total": stats.cycles_total,
            "writes_total": stats.writes_total,
            "errors_total": stats.errors_total,
        })
        print(f"\n[{SCRIPT_NAME}] Остановлен.", flush=True)
        log.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
