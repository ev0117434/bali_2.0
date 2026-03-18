# Logging System

## Log Location

```
{PROJECT_ROOT}/logs/
├── binance_spot/
│   ├── 20240301_120000-20240302_120000/   ← completed chunk
│   │   └── binance_spot.log
│   └── 20240302_120000-ongoing/           ← current (active)
│       └── binance_spot.log
├── binance_futures/
│   └── ...
... (8 directories, one per script)
```

`PROJECT_ROOT` = one level above `market_data/`, i.e. the project root.

---

## Log Format — JSON Lines (NDJSON)

Every line in every `.log` file is a standalone, valid JSON object.
This makes logs trivially parseable with `jq`, Python, or any structured log tool.

### Regular messages

```json
{"ts":"2026-03-18T16:33:07.123Z","script":"binance_spot","level":"INFO","msg":"[binance_spot] Starting..."}
{"ts":"2026-03-18T16:33:07.200Z","script":"binance_spot","level":"INFO","msg":"[binance_spot] Loaded 150 symbols"}
{"ts":"2026-03-18T16:33:07.350Z","script":"binance_spot","level":"INFO","msg":"[binance_spot] Redis connected: {'host': '127.0.0.1', 'port': 6379, 'db': 0}"}
{"ts":"2026-03-18T16:33:07.400Z","script":"binance_spot","level":"INFO","msg":"[binance_spot] Starting 1 WS connections (300 symbols each)..."}
{"ts":"2026-03-18T16:33:07.450Z","script":"binance_spot","level":"INFO","msg":"[binance_spot] All tasks started. Symbols: 150."}
```

Fields:

| Field    | Type   | Description                                     |
|----------|--------|-------------------------------------------------|
| `ts`     | string | UTC timestamp, ISO-8601 with milliseconds       |
| `script` | string | Script name (e.g. `binance_spot`)               |
| `level`  | string | `DEBUG` / `INFO` / `WARNING` / `ERROR`          |
| `msg`    | string | Log message text                                |

### Snapshot entries (every 10 seconds)

Snapshots are emitted by `SnapshotLogger` as complete JSON objects on a single line.
They contain a `"type": "snapshot"` field instead of `"msg"`.

```json
{
  "ts": "2026-03-18T16:33:17.451Z",
  "script": "binance_futures",
  "type": "snapshot",
  "uptime_s": 10,
  "connections": [
    {
      "id": 1,
      "status": "alive",
      "msgs_total": 41541,
      "msgs_window": 41541,
      "reconnects": 0,
      "last_error": null,
      "url": "wss://fstream.binance.com/stream?streams=0gusdt@book..."
    },
    {
      "id": 2,
      "status": "alive",
      "msgs_total": 22155,
      "msgs_window": 22155,
      "reconnects": 0,
      "last_error": null,
      "url": "wss://fstream.binance.com/stream?streams=monusdt@boo..."
    }
  ],
  "throughput": {
    "msgs_total": 63696,
    "msgs_per_s": 6366.1,
    "ticker_writes_total": 13357,
    "ticker_writes_per_s": 1335.0,
    "history_entries_total": 59346,
    "history_entries_per_s": 5931.4
  },
  "symbols": {
    "tracked": 528,
    "active_window": 522
  },
  "latency": {
    "rtt":  {"samples": 0,     "min_ms": null,  "avg_ms": null,  "p95_ms": null,  "max_ms": null},
    "proc": {"samples": 13357, "min_ms": 0.85,  "avg_ms": 26.90, "p95_ms": 53.71, "max_ms": 99.98}
  },
  "reconnects_total": 0,
  "last_error": null,
  "history_chunk": {
    "chunk_id": 1,
    "elapsed_s": 10,
    "remaining_s": 1190,
    "active_chunks": [1]
  }
}
```

> In the actual log file this entire object appears on **one line** (no pretty-printing).

---

## Snapshot Fields Reference

### `connections[]`

| Field         | Description                                     |
|---------------|-------------------------------------------------|
| `id`          | Connection index (1-based)                      |
| `status`      | `"alive"` or `"dead"`                           |
| `msgs_total`  | Total messages received since start             |
| `msgs_window` | Messages in last 10-second window               |
| `reconnects`  | Number of reconnections                         |
| `last_error`  | Last error string, or `null`                    |
| `url`         | WebSocket URL (truncated to 80 chars)           |

### `throughput`

| Field                    | Description                          |
|--------------------------|--------------------------------------|
| `msgs_total`             | Total messages since start           |
| `msgs_per_s`             | Rate in current 10s window           |
| `ticker_writes_total`    | Total Redis ticker writes            |
| `ticker_writes_per_s`    | Write rate in current window         |
| `history_entries_total`  | Total history entries written        |
| `history_entries_per_s`  | History entry rate                   |

### `latency`

Two sub-objects: `rtt` and `proc`, each with the same shape:

| Field     | Description                                           |
|-----------|-------------------------------------------------------|
| `samples` | Number of measurements in the 10s window             |
| `min_ms`  | Minimum latency (ms), or `null` if no data           |
| `avg_ms`  | Average latency (ms)                                 |
| `p95_ms`  | 95th percentile latency (ms)                         |
| `max_ms`  | Maximum latency (ms)                                 |

- **`rtt`** — Exchange timestamp → script receive time (network RTT). Available for: Bybit, OKX, Gate. **Not available** for Binance (no exchange timestamp in bookTicker).
- **`proc`** — Script receive → Redis pipeline write complete (includes buffer time).

### `history_chunk`

| Field           | Description                                      |
|-----------------|--------------------------------------------------|
| `chunk_id`      | Current chunk number (starts at 1)               |
| `elapsed_s`     | Seconds elapsed since chunk started              |
| `remaining_s`   | Seconds until next rotation (~1200s = 20 min)    |
| `active_chunks` | List of currently stored chunk IDs               |

---

## Log Rotation

| Parameter           | Value                                        |
|---------------------|----------------------------------------------|
| `LOG_CHUNK_SECONDS` | `86400` (24 hours per chunk)                 |
| `LOG_MAX_CHUNKS`    | `2` (keep last 2 completed chunks)           |
| Active dir name     | `YYYYMMDD_HHMMSS-ongoing`                    |
| Closed dir name     | `YYYYMMDD_HHMMSS-YYYYMMDD_HHMMSS`           |

When rotation triggers:
1. Directory `...-ongoing` is renamed to `...-<end_time>`
2. New `<new_time>-ongoing` directory is created
3. If completed chunks exceed `LOG_MAX_CHUNKS` — oldest is deleted

Total retention: ~48 hours.

---

## Querying Logs with jq

### Last 5 snapshots for a script

```bash
grep '"type":"snapshot"' logs/binance_futures/$(ls -t logs/binance_futures/ | head -1)/binance_futures.log \
  | tail -5 | jq .
```

### Average proc latency over last 10 minutes

```bash
grep '"type":"snapshot"' logs/bybit_spot/$(ls -t logs/bybit_spot/ | head -1)/bybit_spot.log \
  | tail -60 | jq '[.latency.proc.avg_ms // 0] | add / length'
```

### All errors across all scripts

```bash
grep -r '"level":"ERROR"' logs/*/$(date +%Y%m%d)*/*.log | jq .msg
```

### Symbols active in last snapshot

```bash
tail -1 <(grep '"type":"snapshot"' logs/okx_spot/*/okx_spot.log) \
  | jq '{tracked: .symbols.tracked, active: .symbols.active_window}'
```

### Watch throughput live

```bash
tail -f logs/gate_futures/$(ls -t logs/gate_futures/ | head -1)/gate_futures.log \
  | grep --line-buffered '"type":"snapshot"' \
  | jq --unbuffered '{msgs_s: .throughput.msgs_per_s, writes_s: .throughput.ticker_writes_per_s}'
```

---

## Log Cleanup

### At startup (interactive, via run_all.py)

```
[run_all] Flushing Redis (md:*)... deleted 12450 keys.
Clear logs/ directory? [y/N]: y
[run_all] Logs cleared (8 directories removed).
```

Accepted answers: `y`, `yes`, `д`, `да`

### Manually

```bash
rm -rf logs/
mkdir logs/
```

---

## Disk Space Estimate

One snapshot line ≈ 600–900 bytes (JSON).
At 8 scripts × 6 snapshots/min × 60 min × 24 h × 2 chunks × 750 bytes ≈ **~100 MB** per 48h.
