#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run.py — Запуск всей системы: 8 сборщиков рыночных данных + signal_scanner.

Запускает каждый скрипт как отдельный subprocess, следит за ними
и автоматически перезапускает упавшие процессы с экспоненциальной задержкой.

Снапшот состояния всех процессов — каждые 10 секунд.

Использование:
    python3 run.py

Переменные окружения (передаются дочерним процессам):
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
    SCAN_INTERVAL, SIGNAL_COOLDOWN, MIN_SPREAD_PCT
"""

import asyncio
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import redis.asyncio as aioredis

# ─── Пути ────────────────────────────────────────────────────────────────────
PROJECT_ROOT  = Path(__file__).resolve().parent
MARKET_DIR    = PROJECT_ROOT / "market_data"
LOGS_DIR      = PROJECT_ROOT / "logs"

REDIS_HOST     = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

SNAPSHOT_INTERVAL   = 10    # секунд между выводом статуса
MAX_RESTART_DELAY   = 120   # максимальная задержка перед рестартом (сек)
RESTART_RESET_AFTER = 300   # если процесс жил дольше — сбросить задержку (сек)

# ─── Список скриптов ──────────────────────────────────────────────────────────
# (script_path, cwd)
SCRIPTS: List[tuple] = [
    # 8 сборщиков рыночных данных
    (MARKET_DIR / "binance_spot.py",    MARKET_DIR),
    (MARKET_DIR / "binance_futures.py", MARKET_DIR),
    (MARKET_DIR / "bybit_spot.py",      MARKET_DIR),
    (MARKET_DIR / "bybit_futures.py",   MARKET_DIR),
    (MARKET_DIR / "okx_spot.py",        MARKET_DIR),
    (MARKET_DIR / "okx_futures.py",     MARKET_DIR),
    (MARKET_DIR / "gate_spot.py",       MARKET_DIR),
    (MARKET_DIR / "gate_futures.py",    MARKET_DIR),
    # Сканер сигналов
    (PROJECT_ROOT / "signal_scanner.py", PROJECT_ROOT),
]


# ─── Предстартовая очистка ────────────────────────────────────────────────────

async def flush_redis() -> int:
    """Удаляет все ключи md:* из Redis."""
    client = aioredis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
        password=REDIS_PASSWORD, decode_responses=True,
        socket_connect_timeout=5,
    )
    try:
        await client.ping()
        cursor, deleted = b"0", 0
        while True:
            cursor, keys = await client.scan(cursor, match="md:*", count=500)
            if keys:
                await client.delete(*keys)
                deleted += len(keys)
            if cursor in (b"0", 0):
                break
        return deleted
    finally:
        await client.aclose()


def ask_clear_logs() -> bool:
    try:
        ans = input("Очистить папку logs/? [y/N]: ").strip().lower()
        return ans in ("y", "yes", "д", "да")
    except (EOFError, KeyboardInterrupt):
        return False


def clear_logs() -> int:
    if not LOGS_DIR.exists():
        return 0
    count = 0
    for item in LOGS_DIR.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
        count += 1
    return count


# ─── Dataclass процесса ───────────────────────────────────────────────────────

@dataclass
class ProcInfo:
    name:          str
    script:        Path
    cwd:           Path
    proc:          Optional[asyncio.subprocess.Process] = None
    pid:           int   = 0
    start_ts:      float = 0.0
    restarts:      int   = 0
    restart_delay: float = 1.0
    last_exit_code: Optional[int] = None
    last_exit_ts:   float = 0.0
    status:        str   = "stopped"   # running | stopped | restarting


# ─── Запуск / сторожевой цикл ────────────────────────────────────────────────

async def launch(info: ProcInfo) -> None:
    info.proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(info.script),
        env={**os.environ},
        cwd=str(info.cwd),
    )
    info.pid      = info.proc.pid
    info.start_ts = time.time()
    info.status   = "running"


async def watch(info: ProcInfo, stop_event: asyncio.Event) -> None:
    await launch(info)
    print(f"[run] ▶  {info.name:<25} PID={info.pid}", flush=True)

    while not stop_event.is_set():
        try:
            rc = await info.proc.wait()
        except asyncio.CancelledError:
            break

        info.last_exit_code = rc
        info.last_exit_ts   = time.time()
        info.status         = "restarting"
        uptime = time.time() - info.start_ts

        print(
            f"[run] ✗  {info.name:<25} exit={rc}  "
            f"uptime={uptime:.0f}s  retry_in={info.restart_delay:.0f}s",
            flush=True,
        )

        if uptime >= RESTART_RESET_AFTER:
            info.restart_delay = 1.0

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=info.restart_delay)
            break
        except asyncio.TimeoutError:
            pass

        if stop_event.is_set():
            break

        info.restarts     += 1
        info.restart_delay = min(info.restart_delay * 2, MAX_RESTART_DELAY)

        try:
            await launch(info)
            print(
                f"[run] ↺  {info.name:<25} PID={info.pid}  restart #{info.restarts}",
                flush=True,
            )
        except Exception as exc:
            print(f"[run] ✗  {info.name:<25} failed to start: {exc}", flush=True)
            info.status = "stopped"

    info.status = "stopped"


# ─── Снапшот ─────────────────────────────────────────────────────────────────

def _fmt_uptime(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}h {m:02d}m {s:02d}s"


def print_snapshot(procs: List[ProcInfo], manager_start: float) -> None:
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    uptime  = _fmt_uptime(time.time() - manager_start)
    running = sum(1 for p in procs if p.status == "running")

    lines = [
        "",
        "=" * 68,
        f"SNAPSHOT  run.py  {now_str}",
        f"Manager uptime: {uptime}  |  Processes: {running}/{len(procs)} running",
        "=" * 68,
    ]

    # Сборщики и сканер отдельными секциями
    collectors = [p for p in procs if p.cwd == MARKET_DIR]
    scanner    = [p for p in procs if p.cwd == PROJECT_ROOT]

    for section_name, section in [("СБОРЩИКИ", collectors), ("СКАНЕР", scanner)]:
        lines.append(f"  ── {section_name} " + "─" * (50 - len(section_name)))
        for p in section:
            if p.status == "running":
                detail = f"PID={p.pid:<7} uptime={_fmt_uptime(time.time() - p.start_ts)}"
                icon   = "✓"
            elif p.status == "restarting":
                detail = f"restarting in ~{p.restart_delay:.0f}s  restarts={p.restarts}"
                icon   = "↺"
            else:
                detail = f"stopped  exit={p.last_exit_code}  restarts={p.restarts}"
                icon   = "✗"
            lines.append(f"  {icon} {p.name:<28} {detail}")

    lines.append("=" * 68)
    print("\n".join(lines), flush=True)


# ─── Graceful shutdown ────────────────────────────────────────────────────────

async def _terminate_all(procs: List[ProcInfo]) -> None:
    print("\n[run] Завершение всех процессов...", flush=True)
    for p in procs:
        if p.proc and p.proc.returncode is None:
            try:
                p.proc.terminate()
            except ProcessLookupError:
                pass

    deadline = time.time() + 5.0
    for p in procs:
        if p.proc and p.proc.returncode is None:
            remaining = max(0.0, deadline - time.time())
            try:
                await asyncio.wait_for(p.proc.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                try:
                    p.proc.kill()
                except ProcessLookupError:
                    pass

    print("[run] Все процессы завершены.", flush=True)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main() -> None:

    # ── 1. Очистка Redis ─────────────────────────────────────────────────────
    print("[run] Очищаю Redis (md:*)...", end=" ", flush=True)
    try:
        n = await flush_redis()
        print(f"удалено {n} ключей.")
    except Exception as exc:
        print(f"\n[run] ОШИБКА подключения к Redis: {exc}")
        sys.exit(1)

    # ── 2. Очистка логов ─────────────────────────────────────────────────────
    if ask_clear_logs():
        n = clear_logs()
        print(f"[run] Логи очищены ({n} объектов удалено).")
    else:
        print("[run] Логи оставлены без изменений.")

    print()

    # ── 3. Формируем список процессов ────────────────────────────────────────
    procs = [
        ProcInfo(name=script.stem, script=script, cwd=cwd)
        for script, cwd in SCRIPTS
    ]

    manager_start = time.time()
    stop_event    = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    print(f"[run] Запуск {len(procs)} процессов...", flush=True)

    watch_tasks = [
        asyncio.create_task(watch(p, stop_event))
        for p in procs
    ]

    async def _snapshot_loop():
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=SNAPSHOT_INTERVAL)
            except asyncio.TimeoutError:
                pass
            if not stop_event.is_set():
                print_snapshot(procs, manager_start)

    snap_task = asyncio.create_task(_snapshot_loop())

    await stop_event.wait()

    snap_task.cancel()
    for t in watch_tasks:
        t.cancel()

    await _terminate_all(procs)
    print_snapshot(procs, manager_start)


if __name__ == "__main__":
    asyncio.run(main())
