# Система логирования

## Где хранятся логи

```
{PROJECT_ROOT}/logs/
│
├── binance_spot/
│   ├── 20240301_120000-20240302_120000/   ← завершённый чанк
│   │   └── binance_spot.log
│   └── 20240302_120000-ongoing/           ← текущий (активный)
│       └── binance_spot.log
│
├── binance_futures/  bybit_spot/  bybit_futures/
├── okx_spot/         okx_futures/
├── gate_spot/        gate_futures/
│   └── ...                                ← аналогичная структура
│
├── signal_scanner/
│   └── 20240302_120000-ongoing/
│       └── signal_scanner.log
│
└── redis_monitor/
    └── 20240302_120000-ongoing/
        └── monitor.log
```

`PROJECT_ROOT` = корень проекта (на уровень выше `market_data/`).

---

## Ротация логов

| Параметр              | Значение                             |
|-----------------------|--------------------------------------|
| `LOG_CHUNK_SECONDS`   | `86400` (24 часа на один чанк)       |
| `LOG_MAX_CHUNKS`      | `2` (хранятся последние 2 чанка)     |
| Имя папки (активная)  | `YYYYMMDD_HHMMSS-ongoing`            |
| Имя папки (закрытая)  | `YYYYMMDD_HHMMSS-YYYYMMDD_HHMMSS`   |

При ротации:
1. Папка `...-ongoing` переименовывается в `...-<end_time>`
2. Создаётся новая папка `<new_time>-ongoing`
3. Если закрытых чанков стало > `LOG_MAX_CHUNKS` — самый старый удаляется

Итого: ~48 часов логов на каждый скрипт.

---

## Форматы записей

### Коллекторы (8 скриптов)

Записи — plain-text, одна строка = одно событие.

**Стартовые события:**
```
[binance_spot] Запуск...
[binance_spot] Загружено 150 символов
[binance_spot] Redis: {'host': '127.0.0.1', 'port': 6379, 'db': 0}
[binance_spot] Запускаю 1 WS-соединений (по 300 символов каждое)...
```

**Снапшот (каждые 10с):**
```
================================================================
SNAPSHOT  binance_spot       2024-03-01 12:00:10.451 UTC
================================================================
Uptime: 0h 00m 10s  |  Symbols: 150  |  Reconnects: 0

 Connections:
  [0] wss://stream.binance.com:9443/...  ACTIVE
      msgs_total=1523  msgs/win=152  last_msg=0.1s ago  reconnects=0

 Throughput (last 10s):
  messages:  152  (15.2/s)
  t.writes:  148  (14.8/s)
  h.entries: 148  (14.8/s)

 Latency:
  RTT (exchange→recv):   n/a
  Proc (recv→redis):     min=0.3ms  avg=0.8ms  p95=1.9ms  max=3.1ms

 History chunk:
  chunk_id=1  elapsed=10s / 1200s  remaining=1190s

 Errors: none
================================================================
```

**Ошибки WebSocket:**
```
[binance_spot][conn=0] Ошибка: ConnectionClosedError: received 1006 (connection reset)
[binance_spot][conn=0] Переподключение через 1s...
[binance_spot][conn=0] Переподключение через 2s...
```

**Ротация чанков истории:**
```
[binance_spot] Ротация history chunk: 1 → 2  (удалён чанк 0)
```

---

### signal_scanner.py

Записи в JSON Lines (один JSON-объект на строку). Тип записи определяется полем `"type"`.

**Снапшот (каждые 10с):**
```json
{
  "type": "snapshot",
  "timestamp": "2024-03-01T12:00:10.451000+00:00",
  "uptime_s": 600,
  "pairs_loaded": 320,
  "min_spread_pct": 1.5,
  "signal_cooldown_s": 3600,
  "scans_total": 3000,
  "scans_window": 50,
  "signals_total": 7,
  "signals_window": 1,
  "pipeline_ms": {"min": 0.8, "avg": 1.2, "p95": 2.1, "max": 3.4, "samples": 50},
  "data_age_ms":  {"min": 12,  "avg": 35,  "p95": 90,  "max": 210, "samples": 50}
}
```

**Сигнал:**
```json
{
  "type": "signal",
  "timestamp": "2024-03-01T12:00:08.123000+00:00",
  "spot_exch":    "binance",
  "fut_exch":     "bybit",
  "symbol":       "BTCUSDT",
  "ask_spot":     45000.10,
  "bid_futures":  45676.35,
  "spread_pct":   1.5023,
  "ts_ms":        1741234567890,
  "data_age_ms":  42
}
```

**Системные события:**
```json
{"type": "start",   "timestamp": "...", "pairs_loaded": 320, "min_spread_pct": 1.5}
{"type": "stopped", "timestamp": "..."}
```

---

### redis_monitor.py

JSON Lines по умолчанию (`MONITOR_LOG_JSON=1`). Один JSON-объект = один замер (каждые 5с).

**Замер метрик:**
```json
{
  "timestamp": "2024-03-01T12:00:10.000000+00:00",
  "connected": true,
  "latency_ping": {"min_ms": 0.12, "avg_ms": 0.18, "p95_ms": 0.31, "max_ms": 0.45, "samples": 5},
  "latency_events": {},
  "memory": {
    "used_bytes": 47382528, "used_human": "45.2M",
    "peak_bytes": 50462720, "peak_human": "48.1M",
    "fragmentation_ratio": 1.05,
    "maxmemory_bytes": 0, "maxmemory_policy": "noeviction"
  },
  "connections": {
    "connected_clients": 11,
    "blocked_clients": 0,
    "rejected_connections": 0,
    "total_connections_received": 45
  },
  "ops": {
    "instantaneous_ops_per_sec": 2340,
    "instantaneous_input_kbps": 180.5,
    "instantaneous_output_kbps": 95.2,
    "total_commands_processed": 1456789
  },
  "keyspace": {"db_total_keys": 12450, "md_keys": 12430},
  "slowlog_len": 0,
  "server": {"redis_version": "7.2.0", "uptime_seconds": 86400, "role": "master"}
}
```

**Предупреждения** (`⚠`) выводятся в stdout при текстовом режиме (`MONITOR_LOG_JSON=0`):
- PING p95 > 10ms
- fragmentation ratio > 1.5
- rejected_connections >= 1
- slowlog_len >= 10

**Системные события:**
```json
{"timestamp": "...", "event": "start",     "host": "127.0.0.1:6379", "interval_s": 5}
{"timestamp": "...", "event": "connected", "host": "127.0.0.1:6379"}
{"timestamp": "...", "event": "stopped"}
{"timestamp": "...", "event": "error",     "error_type": "ConnectionError", "message": "..."}
```

---

## Как читать логи

### Следить за активным скриптом в реальном времени

```bash
# Коллектор
tail -f logs/binance_spot/$(ls -t logs/binance_spot/ | head -1)/binance_spot.log

# Сканер сигналов
tail -f logs/signal_scanner/$(ls -t logs/signal_scanner/ | head -1)/signal_scanner.log

# Redis монитор
tail -f logs/redis_monitor/$(ls -t logs/redis_monitor/ | head -1)/monitor.log
```

### Посмотреть последние ошибки по всем скриптам

```bash
grep -r "Ошибка\|Error\|error" logs/*/$(date +%Y%m%d)*/
```

### Все снапшоты за последние N минут (коллектор)

```bash
grep "SNAPSHOT" logs/binance_spot/$(ls -t logs/binance_spot/ | head -1)/binance_spot.log | tail -20
```

### Сравнить задержки между биржами

```bash
grep "Proc (recv→redis)" logs/*/$(ls -t logs/binance_spot/ | head -1)/*.log
```

### Посмотреть все сигналы из лога

```bash
grep '"type":"signal"' logs/signal_scanner/$(ls -t logs/signal_scanner/ | head -1)/signal_scanner.log | python3 -m json.tool
```

### Извлечь ops/sec из Redis-монитора

```bash
grep -o '"instantaneous_ops_per_sec":[0-9]*' \
  logs/redis_monitor/$(ls -t logs/redis_monitor/ | head -1)/monitor.log | tail -10
```

---

## Классы, управляющие логами (common.py)

### `LogManager`

```python
lm = LogManager("binance_spot")
# инициализируется при создании, сразу открывает файл

lm.write("строка сообщения")     # запись + auto-ротация при необходимости
lm.close()                        # закрыть файл при завершении
```

| Метод      | Описание                                                  |
|------------|-----------------------------------------------------------|
| `write(s)` | Пишет строку + `\n`, проверяет необходимость ротации      |
| `close()`  | Закрывает файл и переименовывает папку (убирает `-ongoing`)|

### `SnapshotLogger`

Запускается как фоновая asyncio-задача, каждые `SNAPSHOT_INTERVAL = 10` секунд пишет снапшот в stdout **и** в лог-файл.

```python
snap = SnapshotLogger("binance_spot", log_manager, stats, chunk_manager)
task = asyncio.create_task(snap.run())
# ...
snap.stop()
```

---

## Очистка логов

### При запуске run.py / run_all.py (интерактивно)

```
python3 run.py

[run] Очищаю Redis (md:*)... удалено 12450 ключей.
Очистить папку logs/? [y/N]: y
[run] Логи очищены (10 объектов удалено).
```

Принимаемые ответы: `y`, `yes`, `д`, `да`

### Вручную

```bash
rm -rf logs/
mkdir logs/
```

---

## Оценка дискового пространства

Один снапшот коллектора ≈ 40–60 строк × 50 байт ≈ 2–3 KB каждые 10 секунд.

| Компонент         | Объём за 48 часов (2 чанка)             |
|-------------------|-----------------------------------------|
| 8 коллекторов     | 8 × 48ч × 3600 / 10 × 2.5KB ≈ **350 MB** |
| signal_scanner    | ≈ **50–100 MB** (зависит от кол-ва сигналов) |
| redis_monitor     | ≈ **5–15 MB** (1 JSON-строка / 5с ≈ 200 байт) |

Для уменьшения объёма — снизить `LOG_MAX_CHUNKS = 1` в `common.py` (и в `redis_monitor.py`).
