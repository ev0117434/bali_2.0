#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_data/run_all.py — Launch all 8 market data collectors.

Starts each script as a separate subprocess, monitors them,
and automatically restarts crashed processes with exponential backoff.

Process status snapshot printed every 10 seconds.

Usage:
    python3 market_data/run_all.py

Environment variables (passed to child processes):
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
"""

import asyncio
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent))
from common import create_redis, LOGS_DIR

# ─── Pre-start cleanup ────────────────────────────────────────────────────────

async def flush_redis() -> int:
    """Delete all md:* keys from Redis. Returns count of deleted keys."""
    client = await create_redis()
    cursor, deleted = b"0", 0
    while True:
        cursor, keys = await client.scan(cursor, match="md:*", count=500)
        if keys:
            await client.delete(*keys)
            deleted += len(keys)
        if cursor in (b"0", 0):
            break
    await client.aclose()
    return deleted


def ask_clear_logs() -> bool:
    """Ask user whether to clear logs/ directory. Returns True if yes."""
    try:
        ans = input("Clear logs/ directory? [y/N]: ").strip().lower()
        return ans in ("y", "yes", "д", "да")
    except (EOFError, KeyboardInterrupt):
        return False


def clear_logs() -> int:
    """Delete contents of logs/ directory. Returns count of removed items."""
    if not LOGS_DIR.exists():
        return 0
    count = 0
    for item in LOGS_DIR.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
            count += 1
        else:
            item.unlink()
    return count


# ─── Script list ──────────────────────────────────────────────────────────────
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

SNAPSHOT_INTERVAL    = 10   # seconds between status snapshots
MAX_RESTART_DELAY    = 120  # maximum delay before restart (s)
RESTART_RESET_AFTER  = 300  # reset delay if process lived longer than this (s)


# ─── Process tracking dataclass ───────────────────────────────────────────────
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
    status: str = "stopped"    # running | stopped | restarting


# ─── Launch / restart process ─────────────────────────────────────────────────
async def launch(info: ProcInfo) -> None:
    """Start a subprocess for one script."""
    info.proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(SCRIPTS_DIR / info.script),
        env={**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"},
        cwd=str(SCRIPTS_DIR),
    )
    info.pid      = info.proc.pid
    info.start_ts = time.time()
    info.status   = "running"


async def watch(info: ProcInfo, stop_event: asyncio.Event) -> None:
    """
    Guard loop for one script.
    On exit — waits restart_delay then restarts.
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

        # Reset delay if process was alive long enough
        if uptime >= RESTART_RESET_AFTER:
            info.restart_delay = 1.0

        # Wait before restart, capped at MAX_RESTART_DELAY
        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=info.restart_delay
            )
            break  # stop_event fired — exit
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


# ─── Status snapshot ──────────────────────────────────────────────────────────
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
    print("\n[run_all] Shutting down all processes...")
    for p in procs:
        if p.proc and p.proc.returncode is None:
            try:
                p.proc.terminate()
            except ProcessLookupError:
                pass

    # Wait up to 5 seconds, then kill
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

    print("[run_all] All processes terminated.")


# ─── Main ─────────────────────────────────────────────────────────────────────
async def main() -> None:

    # ── 1. Flush Redis (always) ───────────────────────────────────────────────
    print("[run_all] Flushing Redis (md:*)...", end=" ", flush=True)
    try:
        n = await flush_redis()
        print(f"deleted {n} keys.")
    except Exception as exc:
        print(f"\n[run_all] ERROR: Redis connection failed: {exc}")
        sys.exit(1)

    # ── 2. Clear logs (optional) ──────────────────────────────────────────────
    if ask_clear_logs():
        n = clear_logs()
        print(f"[run_all] Logs cleared ({n} directories removed).")
    else:
        print("[run_all] Logs kept unchanged.")

    print()

    # ── 3. Start ──────────────────────────────────────────────────────────────
    manager_start = time.time()
    stop_event    = asyncio.Event()

    # Register signals
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    procs = [
        ProcInfo(name=s.replace(".py", ""), script=s)
        for s in SCRIPTS
    ]

    print(f"[run_all] Starting {len(procs)} collectors...")

    # Start all watch tasks
    watch_tasks = [
        asyncio.create_task(watch(p, stop_event))
        for p in procs
    ]

    # Periodic snapshot loop
    async def _snapshot_loop():
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=SNAPSHOT_INTERVAL)
            except asyncio.TimeoutError:
                pass
            if not stop_event.is_set():
                print_snapshot(procs, manager_start)

    snap_task = asyncio.create_task(_snapshot_loop())

    # Wait for stop signal
    await stop_event.wait()

    # Cancel tasks
    snap_task.cancel()
    for t in watch_tasks:
        t.cancel()

    await _terminate_all(procs)
    print_snapshot(procs, manager_start)


if __name__ == "__main__":
    asyncio.run(main())
