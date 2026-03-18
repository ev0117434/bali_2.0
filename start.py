#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
start.py — Единая точка входа для запуска всей системы bali_2.0.

Запускает два долгоживущих процесса:
  1. market_data/run_all.py       — менеджер 8 сборщиков рыночных данных
  2. market_data/redis_monitor.py — мониторинг Redis

Последовательность старта:
  1. run_all.py стартует немедленно (у него интерактивный ввод: очистить логи?)
  2. redis_monitor.py стартует через MONITOR_START_DELAY секунд (по умолчанию 3s)
     чтобы не мешать интерактивному диалогу run_all.py

Если любой из процессов завершится — он будет перезапущен с экспоненциальной
задержкой (1s → 2s → 4s → … → MAX_RESTART_DELAY).

Использование:
    python3 start.py

Остановка:
    Ctrl+C (SIGINT) или kill <PID> (SIGTERM) — graceful shutdown обоих процессов
    (SIGTERM → ждать 5 секунд → SIGKILL если не остановились).

Переменные окружения (передаются дочерним процессам):
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
    MONITOR_INTERVAL, STALE_THRESHOLD_S
"""

import asyncio
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# ─── Пути ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
RUN_ALL_SCRIPT    = PROJECT_ROOT / "market_data" / "run_all.py"
MONITOR_SCRIPT    = PROJECT_ROOT / "market_data" / "redis_monitor.py"

# ─── Константы ─────────────────────────────────────────────────────────────────
MONITOR_START_DELAY = 3    # секунд: задержка перед запуском redis_monitor.py
MAX_RESTART_DELAY   = 60   # секунд: потолок задержки между рестартами
RESTART_RESET_AFTER = 120  # секунд: если процесс жил дольше — сброс задержки до 1s
SNAPSHOT_INTERVAL   = 30   # секунд между выводом статуса процессов


# ══════════════════════════════════════════════════════════════════════════════
# ManagedProcess
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ManagedProcess:
    """Отслеживает один дочерний процесс."""
    name:          str
    script:        Path
    proc:          Optional[asyncio.subprocess.Process] = None
    pid:           int   = 0
    start_ts:      float = 0.0
    restarts:      int   = 0
    restart_delay: float = 1.0
    status:        str   = "pending"   # pending | running | restarting | stopped


# ── Запуск процесса ────────────────────────────────────────────────────────────

async def _launch(mp: ManagedProcess, stdin=None) -> None:
    """Запускает subprocess, сохраняет PID и время старта."""
    mp.proc     = await asyncio.create_subprocess_exec(
        sys.executable,
        str(mp.script),
        stdin=stdin,
        env={**os.environ},
        cwd=str(PROJECT_ROOT),
    )
    mp.pid      = mp.proc.pid
    mp.start_ts = time.time()
    mp.status   = "running"


# ── Сторожевой цикл ───────────────────────────────────────────────────────────

async def _watch(
    mp:         ManagedProcess,
    stop_event: asyncio.Event,
    delay:      float = 0.0,
    stdin=None,
) -> None:
    """
    Сторожевой цикл для одного процесса.
      - delay      : подождать N секунд перед первым запуском
      - stop_event : сигнал завершения (устанавливается при Ctrl+C / SIGTERM)
    При падении процесса ждёт restart_delay и перезапускает.
    """
    # Начальная задержка (для redis_monitor.py)
    if delay > 0:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
            return
        except asyncio.TimeoutError:
            pass

    if stop_event.is_set():
        return

    # Первый запуск
    await _launch(mp, stdin=stdin)
    print(f"[start] ▶  {mp.name:<30} PID={mp.pid}")

    # Основной цикл слежения
    while not stop_event.is_set():
        try:
            rc = await mp.proc.wait()
        except asyncio.CancelledError:
            break

        uptime    = time.time() - mp.start_ts
        mp.status = "restarting"

        print(
            f"[start] ✗  {mp.name:<30} завершился code={rc}  "
            f"uptime={uptime:.0f}s  следующий рестарт через {mp.restart_delay:.0f}s"
        )

        # Если процесс прожил долго — сбросить экспоненциальную задержку
        if uptime >= RESTART_RESET_AFTER:
            mp.restart_delay = 1.0

        # Ждём перед рестартом
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=mp.restart_delay)
            break
        except asyncio.TimeoutError:
            pass

        if stop_event.is_set():
            break

        mp.restarts      += 1
        mp.restart_delay  = min(mp.restart_delay * 2, MAX_RESTART_DELAY)

        try:
            await _launch(mp, stdin=stdin)
            print(f"[start] ↺  {mp.name:<30} PID={mp.pid}  рестарт #{mp.restarts}")
        except Exception as e:
            print(f"[start] ✗  {mp.name:<30} не удалось запустить: {e}")
            mp.status = "stopped"

    mp.status = "stopped"


# ── Снапшот состояния ─────────────────────────────────────────────────────────

def _fmt_uptime(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}h {m:02d}m {s:02d}s"


def _print_snapshot(procs: list, manager_start: float) -> None:
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    uptime  = _fmt_uptime(time.time() - manager_start)
    running = sum(1 for p in procs if p.status == "running")

    lines = [
        "",
        "=" * 62,
        f"SNAPSHOT  start.py          {now_str}",
        f"Uptime: {uptime}  |  Активных: {running}/{len(procs)}",
        "=" * 62,
    ]

    for p in procs:
        if p.status == "running":
            icon   = "✓"
            detail = f"PID={p.pid:<7}  uptime={_fmt_uptime(time.time() - p.start_ts)}"
        elif p.status == "restarting":
            icon   = "↺"
            detail = f"рестарт через ~{p.restart_delay:.0f}s  всего рестартов: {p.restarts}"
        elif p.status == "pending":
            icon   = "…"
            detail = f"ожидание запуска ({MONITOR_START_DELAY}s задержка)"
        else:
            icon   = "✗"
            detail = f"остановлен  всего рестартов: {p.restarts}"

        lines.append(f"  {icon}  {p.name:<30}  {detail}")

    lines.append("=" * 62)
    print("\n".join(lines), flush=True)


# ── Graceful shutdown ──────────────────────────────────────────────────────────

async def _terminate_all(procs: list) -> None:
    """SIGTERM → ждать 5 секунд → SIGKILL."""
    print("\n[start] Завершение всех процессов...", flush=True)

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

    print("[start] Все процессы завершены.", flush=True)


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    manager_start = time.time()
    stop_event    = asyncio.Event()

    # Обработка сигналов завершения
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    # Проверить наличие скриптов
    for script in (RUN_ALL_SCRIPT, MONITOR_SCRIPT):
        if not script.exists():
            print(f"[start] ОШИБКА: скрипт не найден: {script}", file=sys.stderr)
            sys.exit(1)

    procs = [
        ManagedProcess(name="market_data/run_all.py",       script=RUN_ALL_SCRIPT),
        ManagedProcess(name="market_data/redis_monitor.py", script=MONITOR_SCRIPT),
    ]

    print("=" * 62)
    print("  bali_2.0 — система сбора рыночных данных")
    print("=" * 62)
    print(f"  run_all.py      — запуск немедленно (интерактивный диалог ниже)")
    print(f"  redis_monitor   — запуск через {MONITOR_START_DELAY}s (после диалога)")
    print(f"  Остановка:        Ctrl+C")
    print("=" * 62)
    print()

    # run_all.py запускается с унаследованным stdin (для интерактивного вопроса)
    # redis_monitor.py запускается с задержкой и без stdin
    watch_tasks = [
        asyncio.create_task(
            _watch(procs[0], stop_event, delay=0.0, stdin=None)
        ),
        asyncio.create_task(
            _watch(procs[1], stop_event, delay=MONITOR_START_DELAY,
                   stdin=asyncio.subprocess.DEVNULL)
        ),
    ]

    # Периодический снапшот состояния
    async def _snapshot_loop():
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=SNAPSHOT_INTERVAL)
            except asyncio.TimeoutError:
                pass
            if not stop_event.is_set():
                _print_snapshot(procs, manager_start)

    snap_task = asyncio.create_task(_snapshot_loop())

    # Ожидаем сигнала остановки
    await stop_event.wait()

    # Завершение
    snap_task.cancel()
    for t in watch_tasks:
        t.cancel()

    await _terminate_all(procs)
    _print_snapshot(procs, manager_start)


if __name__ == "__main__":
    asyncio.run(main())
