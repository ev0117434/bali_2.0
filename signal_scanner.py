#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
signal_scanner.py — Сканер арбитражных сигналов spot → futures.

Читает списки символов из dictionaries/combination/,
каждые SCAN_INTERVAL секунд опрашивает Redis,
вычисляет спред и записывает сигналы в signal/signals_YYYYMMDD.csv.

Формат строки сигнала:
    spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
    binance,bybit,BTCUSDT,45000.1,45451.5,1.0023,1741234567890

Cooldown: один сигнал по одному направлению+символу раз в SIGNAL_COOLDOWN секунд.

Переменные окружения:
    REDIS_HOST        (default: 127.0.0.1)
    REDIS_PORT        (default: 6379)
    REDIS_DB          (default: 0)
    REDIS_PASSWORD    (default: пусто)
    SCAN_INTERVAL     (default: 1.0)   — секунд между опросами Redis
    SIGNAL_COOLDOWN   (default: 3600)  — секунд между повторными сигналами
    MIN_SPREAD_PCT    (default: 0.1)   — минимальный спред для сигнала, %

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

import redis.asyncio as aioredis

# ─── Пути ────────────────────────────────────────────────────────────────────
PROJECT_ROOT    = Path(__file__).resolve().parent
COMBINATION_DIR = PROJECT_ROOT / "dictionaries" / "combination"
SIGNAL_DIR      = PROJECT_ROOT / "signal"

# ─── Redis ────────────────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# ─── Настройки сканера ────────────────────────────────────────────────────────
SCAN_INTERVAL   = float(os.getenv("SCAN_INTERVAL",   "1.0"))    # секунд
SIGNAL_COOLDOWN = float(os.getenv("SIGNAL_COOLDOWN", "3600"))   # секунд
MIN_SPREAD_PCT  = float(os.getenv("MIN_SPREAD_PCT",  "0.1"))    # %


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
        stem = fpath.stem  # e.g. "binance_spot_bybit_futures"
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
# Запись сигналов
# ══════════════════════════════════════════════════════════════════════════════

def _signal_path() -> Path:
    SIGNAL_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    return SIGNAL_DIR / f"signals_{date_str}.csv"


def _write_signal(
    spot_exch: str,
    fut_exch: str,
    symbol: str,
    ask_spot: float,
    bid_futures: float,
    spread_pct: float,
    ts_ms: int,
) -> None:
    line = (
        f"{spot_exch},{fut_exch},{symbol},"
        f"{ask_spot},{bid_futures},"
        f"{spread_pct:.4f},{ts_ms}\n"
    )
    path = _signal_path()
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(line)
    print(f"[signal] {line.rstrip()}", flush=True)


# ══════════════════════════════════════════════════════════════════════════════
# Основной цикл сканирования
# ══════════════════════════════════════════════════════════════════════════════

async def scan_loop(
    redis_client: aioredis.Redis,
    directions: List[Tuple[str, str, List[str]]],
) -> None:
    # cooldown_key -> timestamp последнего сигнала
    cooldown: Dict[str, float] = {}

    total_pairs = sum(len(syms) for _, _, syms in directions)
    print(
        f"[scanner] Направлений: {len(directions)}, "
        f"пар символов: {total_pairs}\n"
        f"[scanner] Интервал опроса: {SCAN_INTERVAL}s, "
        f"cooldown: {SIGNAL_COOLDOWN}s, "
        f"мин. спред: {MIN_SPREAD_PCT}%",
        flush=True,
    )

    while True:
        await asyncio.sleep(SCAN_INTERVAL)
        now   = time.time()
        ts_ms = int(now * 1000)

        # Собираем запросы для тех пар, у которых не активен cooldown
        # Каждая пара → 2 hmget: spot ask + futures bid
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

        if not requests:
            continue

        try:
            results = await pipe.execute()
        except Exception as exc:
            print(f"[scanner] Redis error: {exc}", flush=True)
            continue

        for i, (spot_exch, fut_exch, sym, ckey) in enumerate(requests):
            spot_row = results[i * 2]      # [ask, ts]  или [None, None]
            fut_row  = results[i * 2 + 1]  # [bid, ts]  или [None, None]

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

            spread_pct = (bid_futures - ask_spot) / ask_spot * 100

            if spread_pct < MIN_SPREAD_PCT:
                continue

            # Фиксируем cooldown и пишем сигнал
            cooldown[ckey] = now
            _write_signal(
                spot_exch, fut_exch, sym,
                ask_spot, bid_futures,
                spread_pct, ts_ms,
            )


# ══════════════════════════════════════════════════════════════════════════════
# Точка входа
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    directions = load_directions()
    if not directions:
        print(
            f"[scanner] Не найдено файлов направлений в {COMBINATION_DIR}\n"
            f"          Запустите: python3 dictionaries/combination/generate.py",
            flush=True,
        )
        sys.exit(1)

    redis_client = await create_redis()
    print(f"[scanner] Redis: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}", flush=True)

    try:
        await scan_loop(redis_client, directions)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n[scanner] Остановлен.", flush=True)
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
