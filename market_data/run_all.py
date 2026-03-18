#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/run_all.py — Запуск всех 8 сборщиков рыночных данных.

Запускает каждый скрипт как отдельный subprocess, следит за ними
и автоматически перезапускает упавшие процессы с экспоненциальной задержкой.

Снапшот состояния всех процессов — каждые 10 секунд.

Использование:
    python3 market_data/run_all.py

Переменные окружения (передаются дочерним процессам):
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
"""

import asyncio
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# ─── Список скриптов ──────────────────────────────────────────────────────────
SCRIPTS_DIR = Path(__file__).parent
SCRIPTS = [
    "binance_spot.py",
    "binance_futures.py",
    "bybit_spot.py",
    "bybit_futures.py",
    "okx_spot.py",
    "okx_futures.py",
    "gate_spot.py",
    "gate_futures.py",
]

SNAPSHOT_INTERVAL    = 10   # секунд между выводом статуса
MAX_RESTART_DELAY    = 120  # максимальная задержка перед рестартом (сек)
RESTART_RESET_AFTER  = 300  # если процесс жил дольше — сбросить задержку (сек)


# ─── Dataclass для отслеживания одного процесса ───────────────────────────────
@dataclass
class ProcInfo:
    name: str
    script: str
    proc:   Optional[asyncio.subprocess.Process] = None
    pid:    int   = 0
    start_ts: float = 0.0
    restarts: int = 0
    restart_delay: float = 1.0
    last_exit_code: Optional[int] = None
    last_exit_ts:   float = 0.0
    status: str = "stopped"   # running | stopped | restarting


# ─── Запуск / перезапуск процесса ────────────────────────────────────────────
async def launch(info: ProcInfo) -> None:
    """Запустить subprocess для одного скрипта."""
    info.proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(SCRIPTS_DIR / info.script),
        env={**os.environ},          # передать все переменные окружения
        cwd=str(SCRIPTS_DIR),
    )
    info.pid      = info.proc.pid
    info.start_ts = time.time()
    info.status   = "running"


async def watch(info: ProcInfo, stop_event: asyncio.Event) -> None:
    """
    Сторожевой цикл для одного скрипта.
    При падении — ждёт restart_delay и перезапускает.
    """
    await launch(info)
    print(f"[run_all] ▶ {info.name:<20} PID={info.pid}")

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
            f"[run_all] ✗ {info.name:<20} exited code={rc}  "
            f"uptime={uptime:.0f}s  restart_delay={info.restart_delay:.0f}s"
        )

        # Сбросить задержку если процесс был живой достаточно долго
        if uptime >= RESTART_RESET_AFTER:
            info.restart_delay = 1.0

        # Ждём перед рестартом, но не дольше MAX_RESTART_DELAY
        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=info.restart_delay
            )
            break  # stop_event сработал — выходим
        except asyncio.TimeoutError:
            pass

        if stop_event.is_set():
            break

        info.restarts     += 1
        info.restart_delay = min(info.restart_delay * 2, MAX_RESTART_DELAY)

        try:
            await launch(info)
            print(f"[run_all] ↺ {info.name:<20} PID={info.pid}  restart #{info.restarts}")
        except Exception as exc:
            print(f"[run_all] ✗ {info.name:<20} failed to start: {exc}")
            info.status = "stopped"

    info.status = "stopped"


# ─── Снапшот состояния ────────────────────────────────────────────────────────
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
        "=" * 64,
        f"SNAPSHOT  run_all  {now_str}",
        f"Manager uptime: {uptime}  |  Processes: {running}/{len(procs)} running",
        "=" * 64,
    ]
    for p in procs:
        if p.status == "running":
            ptime = _fmt_uptime(time.time() - p.start_ts)
            icon  = "✓"
            detail = f"PID={p.pid:<7} uptime={ptime}"
        elif p.status == "restarting":
            icon  = "↺"
            delay = p.restart_delay
            detail = f"restarting in ~{delay:.0f}s  restarts={p.restarts}"
        else:
            icon  = "✗"
            detail = f"stopped  exit={p.last_exit_code}  restarts={p.restarts}"

        lines.append(f"  {icon} {p.name:<22} {detail}")

    lines.append("=" * 64)
    print("\n".join(lines), flush=True)


# ─── Graceful shutdown ────────────────────────────────────────────────────────
async def _terminate_all(procs: List[ProcInfo]) -> None:
    print("\n[run_all] Завершение всех процессов...")
    for p in procs:
        if p.proc and p.proc.returncode is None:
            try:
                p.proc.terminate()
            except ProcessLookupError:
                pass

    # Ждём до 5 секунд, потом kill
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

    print("[run_all] Все процессы завершены.")


# ─── Main ─────────────────────────────────────────────────────────────────────
async def main() -> None:
    manager_start = time.time()
    stop_event    = asyncio.Event()

    # Регистрация сигналов
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    procs = [
        ProcInfo(name=s.replace(".py", ""), script=s)
        for s in SCRIPTS
    ]

    print(f"[run_all] Запуск {len(procs)} сборщиков...")

    # Запускаем все сторожевые задачи
    watch_tasks = [
        asyncio.create_task(watch(p, stop_event))
        for p in procs
    ]

    # Периодический снапшот
    async def _snapshot_loop():
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=SNAPSHOT_INTERVAL)
            except asyncio.TimeoutError:
                pass
            if not stop_event.is_set():
                print_snapshot(procs, manager_start)

    snap_task = asyncio.create_task(_snapshot_loop())

    # Ждём сигнала остановки
    await stop_event.wait()

    # Останавливаем
    snap_task.cancel()
    for t in watch_tasks:
        t.cancel()

    await _terminate_all(procs)
    print_snapshot(procs, manager_start)


if __name__ == "__main__":
    asyncio.run(main())
