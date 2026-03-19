#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
signal_scanner.py — Сканер арбитражных сигналов spot → futures.

Читает списки символов из dictionaries/combination/,
каждые SCAN_INTERVAL секунд опрашивает Redis,
вычисляет спред (bid_futures - ask_spot) / ask_spot * 100
и записывает сигналы в signal/signals.csv.

Формат строки сигнала:
    spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
    binance,bybit,BTCUSDT,45000.1,45451.5,1.0023,1741234567890

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
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

# ─── Пути и общие утилиты ─────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "market_data"))

from common import (  # noqa: E402
    LogManager,
    SnapshotLogger,
    Stats,
    ConnectionStats,
    create_redis,
    SNAPSHOT_INTERVAL,
)

import redis.asyncio as aioredis  # noqa: E402

# ─── Пути ────────────────────────────────────────────────────────────────────
COMBINATION_DIR = PROJECT_ROOT / "dictionaries" / "combination"
SIGNAL_DIR      = PROJECT_ROOT / "signal"
SIGNAL_FILE     = SIGNAL_DIR / "signals.csv"

# ─── Настройки сканера ────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

SCAN_INTERVAL   = float(os.getenv("SCAN_INTERVAL",   "0.2"))    # секунд
SIGNAL_COOLDOWN = float(os.getenv("SIGNAL_COOLDOWN", "3600"))   # секунд
MIN_SPREAD_PCT  = float(os.getenv("MIN_SPREAD_PCT",  "0.1"))    # %

SCRIPT_NAME = "signal_scanner"


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
# Запись сигналов
# ══════════════════════════════════════════════════════════════════════════════

def _write_signal(
    spot_exch:   str,
    fut_exch:    str,
    symbol:      str,
    ask_spot:    float,
    bid_futures: float,
    spread_pct:  float,
    ts_ms:       int,
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
# Snapshot-логгер для signal_scanner
# ══════════════════════════════════════════════════════════════════════════════

class SignalSnapshotLogger:
    """Выводит статистику сканера каждые SNAPSHOT_INTERVAL секунд."""

    def __init__(self, log_manager: LogManager, stats: "ScannerStats"):
        self._lm       = log_manager
        self._stats    = stats
        self._last_ts  = time.time()
        self._running  = False

    @staticmethod
    def _fmt_uptime(seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        return f"{h}h {m:02d}m {s:02d}s"

    def _build(self, elapsed: float) -> str:
        s   = self._stats
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
        rate_scans   = s.scans_window   / elapsed if elapsed > 0 else 0.0
        rate_signals = s.signals_window / elapsed if elapsed > 0 else 0.0

        lines = [
            "=" * 66,
            f"SNAPSHOT  {SCRIPT_NAME:<20}  {now}",
            "=" * 66,
            f"Uptime:           {self._fmt_uptime(time.time() - s.start_ts)}",
            "",
            "ПРОИЗВОДИТЕЛЬНОСТЬ",
            f"  Опросов Redis:  {s.scans_total:>12,}  |  {rate_scans:>8.1f} scan/s",
            f"  Сигналов:       {s.signals_total:>12,}  |  {rate_signals:>8.2f} sig/s",
            "",
            "КОНФИГУРАЦИЯ",
            f"  Направлений:    {s.directions}",
            f"  Пар символов:   {s.pairs}",
            f"  Интервал опроса:{SCAN_INTERVAL}s  cooldown:{SIGNAL_COOLDOWN}s  "
            f"мин. спред:{MIN_SPREAD_PCT}%",
            f"  Файл сигналов:  {SIGNAL_FILE}",
        ]
        if s.last_signal:
            lines += ["", f"  Последний:      {s.last_signal}"]
        if s.last_error:
            lines.append(f"  Последняя ошибка: {s.last_error}")
        lines.append("=" * 66)
        return "\n".join(lines)

    async def run(self):
        self._running = True
        while self._running:
            await asyncio.sleep(SNAPSHOT_INTERVAL)
            now     = time.time()
            elapsed = now - self._last_ts
            try:
                self._lm.log(self._build(elapsed))
            except Exception:
                pass
            self._stats.reset_window()
            self._last_ts = now

    def stop(self):
        self._running = False


# ══════════════════════════════════════════════════════════════════════════════
# Статистика сканера
# ══════════════════════════════════════════════════════════════════════════════

class ScannerStats:
    def __init__(self, directions: int, pairs: int):
        self.start_ts      = time.time()
        self.directions    = directions
        self.pairs         = pairs

        self.scans_total   = 0
        self.scans_window  = 0
        self.signals_total = 0
        self.signals_window = 0

        self.last_signal: str = ""
        self.last_error:  str = ""

    def record_scan(self):
        self.scans_total  += 1
        self.scans_window += 1

    def record_signal(self, line: str):
        self.signals_total  += 1
        self.signals_window += 1
        self.last_signal = line[:120]

    def record_error(self, err: str):
        self.last_error = err[:200]

    def reset_window(self):
        self.scans_window   = 0
        self.signals_window = 0


# ══════════════════════════════════════════════════════════════════════════════
# Основной цикл сканирования
# ══════════════════════════════════════════════════════════════════════════════

async def scan_loop(
    redis_client: aioredis.Redis,
    directions:   List[Tuple[str, str, List[str]]],
    log_manager:  LogManager,
    stats:        ScannerStats,
) -> None:
    # cooldown_key -> timestamp последнего сигнала
    cooldown: Dict[str, float] = {}

    logger = log_manager.get_logger()
    logger.info(
        f"[{SCRIPT_NAME}] Запуск сканирования: "
        f"{stats.directions} направлений, {stats.pairs} пар, "
        f"интервал={SCAN_INTERVAL}s, cooldown={SIGNAL_COOLDOWN}s, "
        f"мин.спред={MIN_SPREAD_PCT}%"
    )

    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        log_manager.rotate_if_needed()

        now   = time.time()
        ts_ms = int(now * 1000)

        # Собираем запросы только для пар без активного cooldown
        pipe     = redis_client.pipeline(transaction=False)
        requests: List[Tuple[str, str, str, str]] = []  # (spot, fut, sym, ckey)

        for spot_exch, fut_exch, symbols in directions:
            for sym in symbols:
                ckey = f"{spot_exch}:{fut_exch}:{sym}"
                if now - cooldown.get(ckey, 0.0) < SIGNAL_COOLDOWN:
                    continue
                pipe.hmget(f"md:{spot_exch}:spot:{sym}",    "ask", "ts")
                pipe.hmget(f"md:{fut_exch}:futures:{sym}",  "bid", "ts")
                requests.append((spot_exch, fut_exch, sym, ckey))

        stats.record_scan()

        if not requests:
            continue

        try:
            results = await pipe.execute()
        except Exception as exc:
            err = repr(exc)[:200]
            stats.record_error(err)
            log_manager.get_logger().error(f"[{SCRIPT_NAME}] Redis error: {err}")
            continue

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

            # Спот всегда дешевле фьючерса → спред всегда > 0
            spread_pct = (bid_futures - ask_spot) / ask_spot * 100

            if spread_pct < MIN_SPREAD_PCT:
                continue

            # Фиксируем cooldown и пишем сигнал
            cooldown[ckey] = now
            signal_str = (
                f"{spot_exch},{fut_exch},{sym},"
                f"{ask_spot},{bid_futures},"
                f"{spread_pct:.4f},{ts_ms}"
            )
            _write_signal(spot_exch, fut_exch, sym, ask_spot, bid_futures, spread_pct, ts_ms)
            stats.record_signal(signal_str)
            log_manager.get_logger().info(f"[signal] {signal_str}")


# ══════════════════════════════════════════════════════════════════════════════
# Точка входа
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    log_mgr = LogManager(SCRIPT_NAME)
    log_mgr.initialize()
    logger  = log_mgr.get_logger()

    logger.info(f"[{SCRIPT_NAME}] Запуск...")

    directions = load_directions()
    if not directions:
        logger.error(
            f"[{SCRIPT_NAME}] Не найдено файлов направлений в {COMBINATION_DIR}"
        )
        sys.exit(1)

    total_pairs = sum(len(s) for _, _, s in directions)
    logger.info(
        f"[{SCRIPT_NAME}] Загружено {len(directions)} направлений, "
        f"{total_pairs} пар символов"
    )

    redis_client = await create_redis()
    logger.info(f"[{SCRIPT_NAME}] Redis: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"[{SCRIPT_NAME}] Файл сигналов: {SIGNAL_FILE}")

    stats      = ScannerStats(len(directions), total_pairs)
    snap_logger = SignalSnapshotLogger(log_mgr, stats)

    snap_task = asyncio.create_task(snap_logger.run())
    scan_task = asyncio.create_task(
        scan_loop(redis_client, directions, log_mgr, stats)
    )

    try:
        await asyncio.gather(snap_task, scan_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info(f"[{SCRIPT_NAME}] Завершение по запросу.")
    finally:
        snap_logger.stop()
        snap_task.cancel()
        scan_task.cancel()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
