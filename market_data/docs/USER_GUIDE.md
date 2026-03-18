# User Guide — Скрипты сборщиков

## Общие принципы

Каждый скрипт автономен: подключается к одной бирже по WebSocket, собирает best bid/ask по всем символам из своего файла подписки и пишет в Redis. Все скрипты имеют одинаковую структуру запуска и идентичное поведение при ошибках.

---

## Запуск

### Все 8 скриптов сразу (рекомендуется)

```bash
python3 market_data/run_all.py
```

При запуске:
1. Автоматически удаляются все `md:*` ключи из Redis
2. Предлагается очистить папку `logs/`
3. Запускаются все 8 скриптов как отдельные процессы с автоперезапуском

Остановка: `Ctrl+C` (SIGINT) → graceful shutdown всех процессов в течение 5 секунд.

### Один скрипт отдельно (для отладки)

```bash
python3 market_data/binance_spot.py
python3 market_data/bybit_futures.py
# и т.д.
```

---

## Файлы подписки (списки символов)

Каждый скрипт загружает символы из:
```
dictionaries/subscribe/{exchange}/{exchange}_{market}.txt
```

| Скрипт            | Файл подписки                                  |
|-------------------|------------------------------------------------|
| binance_spot      | `dictionaries/subscribe/binance/binance_spot.txt`    |
| binance_futures   | `dictionaries/subscribe/binance/binance_futures.txt` |
| bybit_spot        | `dictionaries/subscribe/bybit/bybit_spot.txt`        |
| bybit_futures     | `dictionaries/subscribe/bybit/bybit_futures.txt`     |
| okx_spot          | `dictionaries/subscribe/okx/okx_spot.txt`            |
| okx_futures       | `dictionaries/subscribe/okx/okx_futures.txt`         |
| gate_spot         | `dictionaries/subscribe/gate/gate_spot.txt`          |
| gate_futures      | `dictionaries/subscribe/gate/gate_futures.txt`       |

**Формат файла:** один символ на строку, uppercase. Строки начинающиеся с `#` — комментарии.

```
# Bitcoin
BTCUSDT
# Ethereum
ETHUSDT
ADAUSDT
SOLUSDT
```

**Добавить символ:** просто добавить строку в файл и перезапустить скрипт.

---

## Reading Output — JSON Lines Snapshots (every 10s)

Every 10 seconds each script writes a snapshot line to stdout and to its log file.
Use `jq` for pretty-printing:

```bash
tail -f logs/binance_spot/$(ls -t logs/binance_spot/ | head -1)/binance_spot.log \
  | grep --line-buffered '"type":"snapshot"' | jq .
```

Example (pretty-printed):

```json
{
  "ts": "2026-03-18T16:33:17.451Z",
  "script": "binance_spot",
  "type": "snapshot",
  "uptime_s": 10,
  "connections": [
    {"id": 1, "status": "alive", "msgs_total": 1523, "msgs_window": 152,
     "reconnects": 0, "last_error": null, "url": "wss://stream.binance.com:9443/..."}
  ],
  "throughput": {
    "msgs_total": 1523,    "msgs_per_s": 15.2,
    "ticker_writes_total": 148, "ticker_writes_per_s": 14.8,
    "history_entries_total": 148, "history_entries_per_s": 14.8
  },
  "symbols": {"tracked": 150, "active_window": 148},
  "latency": {
    "rtt":  {"samples": 0,   "min_ms": null, "avg_ms": null, "p95_ms": null, "max_ms": null},
    "proc": {"samples": 148, "min_ms": 0.3,  "avg_ms": 0.8,  "p95_ms": 1.9,  "max_ms": 3.1}
  },
  "reconnects_total": 0,
  "last_error": null,
  "history_chunk": {"chunk_id": 1, "elapsed_s": 10, "remaining_s": 1190, "active_chunks": [1]}
}
```

| Field                          | Meaning                                         |
|--------------------------------|-------------------------------------------------|
| `throughput.msgs_per_s`        | WS messages/s in the last 10-second window      |
| `throughput.ticker_writes_per_s` | Redis ticker writes/s                         |
| `throughput.history_entries_per_s` | History RPUSH entries/s                   |
| `latency.rtt`                  | Exchange timestamp → script receive (RTT)       |
| `latency.proc`                 | Script receive → Redis write complete           |
| `history_chunk.chunk_id`       | Current 20-minute history chunk number          |
| `history_chunk.remaining_s`    | Seconds until next chunk rotation               |

> **Note:** `latency.rtt` samples = 0 for Binance — no exchange timestamp in bookTicker.

---

## Error Behavior

### WebSocket connection dropped

Script reconnects automatically with exponential backoff:

```
Connection dropped -> wait 1s -> reconnect
Dropped again      -> wait 2s -> reconnect
Dropped again      -> wait 4s -> reconnect
...
Maximum            -> 60s between attempts
```

If the connection was alive ≥ 60s — delay resets to 1s.

Reconnect counter visible in snapshot: `connections[].reconnects`.

### Whole script crash (run_all.py only)

`run_all.py` monitors processes and restarts with the same logic:

```
Process exited -> wait 1s -> restart
...
Maximum        -> 120s between attempts
```

If process lived ≥ 300s — delay resets to 1s.

### Redis unavailable at startup

Script retries 5 times with exponential backoff (1s → 2s → 4s → 8s → 16s), then exits with error.

---

## Verifying Data is Flowing

### Check a ticker

```bash
redis-cli HGETALL md:binance:spot:BTCUSDT
# 1) "bid"
# 2) "45000.12"
# 3) "ask"
# 4) "45000.13"
# 5) "ts"
# 6) "1710000000.123"
```

### Check history

```bash
redis-cli LRANGE md:hist:binance:spot:BTCUSDT:1 0 4
# 1) "1710000000.123,45000.12,45000.13"
# 2) "1710000001.456,45000.14,45000.15"
# ...
```

### Count keys in Redis

```bash
redis-cli --scan --pattern 'md:*' | wc -l
```

### Confirm data is updating

```bash
watch -n 1 'redis-cli HGET md:bybit:spot:BTCUSDT ts'
```

---

## Per-Script Details

---

### binance_spot.py

**WebSocket:** `wss://stream.binance.com:9443/stream?streams=...`
**Channel:** `{symbol}@bookTicker` — fastest best bid/ask updates
**Limit:** 300 symbols per connection
**RTT:** not available (Binance bookTicker has no exchange timestamp)

Subscription is embedded directly in the URL:
```
wss://...?streams=btcusdt@bookTicker/ethusdt@bookTicker/...
```

---

### binance_futures.py

**WebSocket:** `wss://fstream.binance.com/stream?streams=...`
**Channel:** `{symbol}@bookTicker`
Identical to `binance_spot.py`, differs only in host.

---

### bybit_spot.py

**WebSocket:** `wss://stream.bybit.com/v5/public/spot`
**Channel:** `orderbook.1.{SYMBOL}` — L1 orderbook (best level only)
**Limit:** 200 symbols per connection, 10 symbols per subscribe message
**Ping:** `{"op":"ping"}` every 20s
**RTT:** available (field `ts` in milliseconds)

Subscribe messages sent after connection is established:
```json
{"op":"subscribe","args":["orderbook.1.BTCUSDT","orderbook.1.ETHUSDT",...]}
```

---

### bybit_futures.py

**WebSocket:** `wss://stream.bybit.com/v5/public/linear`
**Channel:** `orderbook.1.{SYMBOL}`
Identical to `bybit_spot.py`, differs in URL and batch size (200 instead of 10).

---

### okx_spot.py

**WebSocket:** `wss://ws.okx.com:8443/ws/v5/public`
**Channel:** `tickers` with `instId: "BTC-USDT"` (hyphen format)
**Limit:** 300 symbols per connection
**Ping:** string `"ping"` every 25s, response `"pong"`
**RTT:** available (field `ts` — string in ms)

Symbol conversion: `BTCUSDT` → `BTC-USDT` (subscribe) → `BTCUSDT` (Redis key).

```json
{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"},...]}
```

---

### okx_futures.py

**WebSocket:** `wss://ws.okx.com:8443/ws/v5/public`
**Channel:** `tickers` with `instId: "BTC-USDT-SWAP"`
Conversion: `BTCUSDT` → `BTC-USDT-SWAP` → `BTCUSDT`.

```json
{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT-SWAP"},...]}
```

---

### gate_spot.py

**WebSocket:** `wss://api.gateio.ws/ws/v4/`
**Channel:** `spot.book_ticker`
**Limit:** 500 symbols per connection, batches of 100 with 50ms delay between batches
**Ping:** `{"channel":"spot.ping","time":<unix_ts>}` every 25s
**RTT:** available (field `result.t` in milliseconds)

Symbol conversion: `BTCUSDT` → `BTC_USDT` → `BTCUSDT`.

```json
{
  "time": 1710000000,
  "channel": "spot.book_ticker",
  "event": "subscribe",
  "payload": ["BTC_USDT","ETH_USDT",...]
}
```

---

### gate_futures.py

**WebSocket:** `wss://fx-ws.gateio.ws/v4/ws/usdt`
**Channel:** `futures.book_ticker`
**Ping:** `{"channel":"futures.ping","time":<unix_ts>}` every 25s
Identical to `gate_spot.py`, differs in URL and channel names.

---

## Debugging

### Run a single script with output to terminal

```bash
REDIS_HOST=127.0.0.1 python3 market_data/binance_spot.py
```

### Force-flush Redis and run a single script

```bash
redis-cli --scan --pattern 'md:*' | xargs redis-cli DEL
python3 market_data/binance_spot.py
```

### Check connected WebSocket processes

```bash
ss -tp | grep python
```

### Watch latency in real time (via logs)

```bash
tail -f logs/bybit_spot/$(ls -t logs/bybit_spot/ | head -1)/bybit_spot.log \
  | grep --line-buffered '"type":"snapshot"' \
  | jq --unbuffered '{rtt: .latency.rtt.avg_ms, proc: .latency.proc.avg_ms}'
```
