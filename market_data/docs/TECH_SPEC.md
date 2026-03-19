# Техническая спецификация

## 1. Константы (common.py)

### Тайминги

| Константа               | Значение    | Описание                                              |
|-------------------------|------------:|-------------------------------------------------------|
| `TICKER_FLUSH_INTERVAL` | `0.05`s     | Интервал сброса буфера тикеров в Redis (50ms)         |
| `HISTORY_FLUSH_INTERVAL`| `1.0`s      | Интервал сброса буфера истории в Redis                |
| `SNAPSHOT_INTERVAL`     | `10`s       | Интервал вывода статистики (stdout + лог)             |
| `HISTORY_CHUNK_SECONDS` | `1200`s     | Длина одного чанка истории (20 минут)                 |
| `HISTORY_MAX_CHUNKS`    | `5`         | Максимум хранимых чанков ≈ ~1.5–2 часа истории       |
| `LOG_CHUNK_SECONDS`     | `86400`s    | Длина одного лог-файла (24 часа)                      |
| `LOG_MAX_CHUNKS`        | `2`         | Максимум хранимых лог-файлов ≈ ~48 часов             |

### Redis (из переменных окружения)

| Переменная        | Дефолт       |
|-------------------|:------------:|
| `REDIS_HOST`      | `127.0.0.1`  |
| `REDIS_PORT`      | `6379`       |
| `REDIS_DB`        | `0`          |
| `REDIS_PASSWORD`  | `None`       |

Параметры соединения (`create_redis`):
```python
socket_connect_timeout  = 5     # секунд на открытие TCP
socket_keepalive        = True  # TCP keepalive
health_check_interval   = 30    # секунд между health-check пингами
decode_responses        = True  # ключи/значения как str, не bytes
```

Redis retry при старте: 5 попыток, backoff 1s → 2s → 4s → 8s → 16s.

---

## 2. Константы (exchange-скрипты)

### Параметры WebSocket соединений

| Скрипт            | `CHUNK_SIZE` | `SUB_BATCH` | `PING_INTERVAL` | `MAX_RECONNECT_DELAY` |
|-------------------|------------:|------------:|----------------:|----------------------:|
| binance_spot      | 300         | —           | 20s (auto)      | 60s                   |
| binance_futures   | 300         | —           | 20s (auto)      | 60s                   |
| bybit_spot        | 200         | 10          | 20s (manual)    | 60s                   |
| bybit_futures     | 200         | 200         | 20s (manual)    | 60s                   |
| okx_spot          | 300         | —           | 25s (manual)    | 60s                   |
| okx_futures       | 300         | —           | 25s (manual)    | 60s                   |
| gate_spot         | 500         | 100         | 25s (manual)    | 60s                   |
| gate_futures      | 500         | 100         | 25s (manual)    | 60s                   |

`CHUNK_SIZE` — символов на одно WS-соединение
`SUB_BATCH` — символов в одном subscribe-сообщении
`PING_INTERVAL` — интервал keepalive пинга
`MAX_RECONNECT_DELAY` — максимальная пауза перед переподключением

### websockets параметры (все скрипты)

```python
ping_interval  = <PING_INTERVAL или None>
ping_timeout   = 10    # секунд
close_timeout  = 5     # секунд
max_size       = 2**20 # 1MB (OKX: 2**21 = 2MB)
open_timeout   = 15    # секунд на handshake
```

### Задержки переподключения (WS-воркеры)

```
Попытка 1: wait = 1s
Попытка 2: wait = 2s
Попытка 3: wait = 4s
...
Попытка N: wait = min(prev × 2, MAX_RECONNECT_DELAY)
```

Сброс задержки: если WS держался ≥ 60s — delay сбрасывается в 1s.

---

## 3. Константы (signal_scanner.py)

| Переменная        | Дефолт   | Описание                                      |
|-------------------|:--------:|-----------------------------------------------|
| `SCAN_INTERVAL`   | `0.2`s   | Секунд между опросами Redis                   |
| `SIGNAL_COOLDOWN` | `3600`s  | Секунд между повторными сигналами по одной паре |
| `MIN_SPREAD_PCT`  | `1.5`    | Минимальный спред для генерации сигнала, %    |
| `SNAPSHOT_INTERVAL` | `10`s  | Интервал снапшота в лог                       |

Формула:
```
spread_pct = (bid_futures - ask_spot) / ask_spot × 100
```

### Источники пар

Читает все 12 файлов из `dictionaries/combination/`:
```
binance_spot_bybit_futures.txt   → проверяем bid(bybit futures) vs ask(binance spot)
bybit_spot_binance_futures.txt
... (12 комбинаций)
```

### Выходной формат (signal/signals.csv)

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,45000.10,45676.35,1.5023,1741234567890
```

---

## 4. Константы (redis_monitor.py)

| Переменная         | Дефолт              | Описание                              |
|--------------------|:-------------------:|---------------------------------------|
| `MONITOR_INTERVAL` | `5`s                | Интервал сбора метрик                 |
| `MONITOR_LOG_DIR`  | `logs/redis_monitor`| Папка для логов                       |
| `MONITOR_LOG_JSON` | `1`                 | `1` = JSON Lines, `0` = читаемый текст |
| `LOG_CHUNK_SECONDS`| `86400`s            | Размер лог-чанка                      |
| `LOG_MAX_CHUNKS`   | `2`                 | Сколько чанков хранить                |

Собираемые метрики: PING-латентность (5 сэмплов), LATENCY LATEST, память, соединения, ops/sec, keyspace (total + md:* ключи), slowlog, конфигурация сервера.

---

## 5. Redis ключи

### Текущие тикеры

```
Ключ:   md:{exchange}:{market}:{symbol}
Тип:    Hash
Поля:
  bid  →  "цена"            строка, float без trailing zeros
  ask  →  "цена"            строка
  ts   →  "timestamp.mss"   строка, unix timestamp с 3 знаками (например "1710000000.123")
TTL:    нет (перезаписывается при каждом обновлении)
```

Примеры ключей:
```
md:binance:spot:BTCUSDT
md:binance:futures:BTCUSDT
md:bybit:spot:ADAUSDT
md:bybit:futures:SOLUSDT
md:okx:spot:BTCUSDT
md:okx:futures:ETHUSDT
md:gate:spot:LTCUSDT
md:gate:futures:DOTUSDT
```

### История (чанки)

```
Ключ:   md:hist:{exchange}:{market}:{symbol}:{chunk_id}
Тип:    List (RPUSH → правый конец, хронологический порядок)
Элемент: "{ts:.3f},{bid},{ask}"
Пример элемента: "1710000010.456,45000.12,45000.13"
TTL:    нет
```

`chunk_id` — целое число, начинается с 1, увеличивается при каждой ротации.
Активных чанков: `HISTORY_MAX_CHUNKS = 5`. При создании 6-го — первый удаляется.

Примеры:
```
md:hist:binance:spot:BTCUSDT:1
md:hist:binance:spot:BTCUSDT:2
...
md:hist:gate:futures:ETHUSDT:5
```

### Метаданные чанков

```
Ключ:   md:chunks:config
Тип:    Hash
Поле:   {exchange}:{market}
Значение: JSON (строка)
```

JSON-структура:
```json
{
  "current_chunk_id": 3,
  "active_chunks": [1, 2, 3],
  "chunks": {
    "1": {
      "start_ts":  1710000000.0,
      "start_dt":  "2024-03-10 00:00:00 UTC",
      "end_ts":    1710001200.0,
      "end_dt":    "2024-03-10 00:20:00 UTC"
    },
    "3": {
      "start_ts":  1710001200.0,
      "start_dt":  "2024-03-10 00:20:00 UTC",
      "end_ts":    null,
      "end_dt":    null
    }
  }
}
```

Поле хранится под ключом `"binance:spot"`, `"bybit:futures"`, и т.д.

---

## 6. Структуры данных (common.py)

### `ConnectionStats`

```python
@dataclass
class ConnectionStats:
    url:           str   = ""     # WebSocket URL (обрезан до 80 символов)
    active:        bool  = False  # True если соединение живо
    msgs_total:    int   = 0      # Всего сообщений с момента старта
    msgs_window:   int   = 0      # Сообщений за текущее 10с окно
    last_msg_ts:   float = 0.0    # unix timestamp последнего сообщения
    reconnects:    int   = 0      # Число переподключений
    last_error:    str   = ""     # Последняя ошибка (макс 120 символов)
    last_error_ts: float = 0.0    # Когда была последняя ошибка
```

### `Stats`

```python
@dataclass
class Stats:
    start_ts:               float       # Время старта скрипта
    connections:            List[ConnectionStats]

    msgs_total:             int         # Всего сообщений
    msgs_window:            int         # Сообщений за окно

    ticker_writes_total:    int         # Всего записей тикеров в Redis
    ticker_writes_window:   int         # За окно

    history_entries_total:  int
    history_entries_window: int

    rtt_latencies_window:   List[float] # RTT в мс (exchange ts → recv)
    proc_latencies_window:  List[float] # Processing в мс (recv → redis write)

    symbols_active_window:  Set[str]    # Символы, получившие update за окно
    symbols_tracked:        int         # Всего символов

    reconnects_total:       int
    last_error:             str
    last_error_ts:          float
```

### Буферы (глобальные в каждом скрипте)

```python
_ticker_buf: Dict[str, Tuple[str, str, float]]
# symbol → (bid, ask, local_ts)
# Перезаписывается при каждом новом тике (только последнее значение)

_history_buf: Dict[str, List[Tuple[float, str, str]]]
# symbol → [(ts, bid, ask), ...]
# Накапливается, сбрасывается раз в 1с
```

---

## 7. Конвертация символов

Биржи используют разные форматы. Конвертация выполняется в каждом скрипте:

| Биржа    | Тип     | Входной (subscribe файл) | WS формат        | Redis ключ    |
|----------|---------|:------------------------:|:----------------:|:-------------:|
| Binance  | spot    | `BTCUSDT`                | `btcusdt`        | `BTCUSDT`     |
| Binance  | futures | `BTCUSDT`                | `btcusdt`        | `BTCUSDT`     |
| Bybit    | spot    | `BTCUSDT`                | `BTCUSDT`        | `BTCUSDT`     |
| Bybit    | futures | `BTCUSDT`                | `BTCUSDT`        | `BTCUSDT`     |
| OKX      | spot    | `BTCUSDT`                | `BTC-USDT`       | `BTCUSDT`     |
| OKX      | futures | `BTCUSDT`                | `BTC-USDT-SWAP`  | `BTCUSDT`     |
| Gate     | spot    | `BTCUSDT`                | `BTC_USDT`       | `BTCUSDT`     |
| Gate     | futures | `BTCUSDT`                | `BTC_USDT`       | `BTCUSDT`     |

Поддерживаемые quote-активы: `USDT`, `USDC`. Прочие — пропускаются молча.

---

## 8. Форматы WebSocket сообщений (входящие)

### Binance (spot и futures)
```json
{
  "stream": "btcusdt@bookTicker",
  "data": {
    "s": "BTCUSDT",
    "b": "45000.12",
    "a": "45000.13"
  }
}
```
Нет exchange timestamp → RTT недоступен.

### Bybit (spot и futures)
```json
{
  "topic": "orderbook.1.BTCUSDT",
  "ts": 1710000010123,
  "data": {
    "s": "BTCUSDT",
    "b": [["45000.12", "1.500"]],
    "a": [["45000.13", "2.000"]]
  }
}
```
`ts` — миллисекунды, integer.

### OKX (spot и futures)
```json
{
  "arg": {"channel": "tickers", "instId": "BTC-USDT"},
  "data": [{
    "instId": "BTC-USDT",
    "bidPx":  "45000.12",
    "askPx":  "45000.13",
    "ts":     "1710000010123"
  }]
}
```
`ts` — строка миллисекунды.

### Gate (spot и futures)
```json
{
  "channel": "spot.book_ticker",
  "event":   "update",
  "result": {
    "t": 1710000010123,
    "s": "BTC_USDT",
    "b": "45000.12",
    "a": "45000.13"
  }
}
```
`result.t` — миллисекунды, integer. Futures: структура идентична.

---

## 9. Форматы Ping/Pong

| Биржа    | Ping (отправляем)                              | Pong (ожидаем)                                 | Тип   |
|----------|------------------------------------------------|------------------------------------------------|-------|
| Binance  | auto (websockets lib)                          | auto                                           | frame |
| Bybit    | `{"op":"ping"}`                                | `{"op":"pong",...}`                            | JSON  |
| OKX      | `"ping"`                                       | `"pong"`                                       | text  |
| Gate     | `{"channel":"spot.ping","time":1710000000}`    | `{"channel":"spot.pong","time":1710000000}`    | JSON  |
| Gate fut | `{"channel":"futures.ping","time":1710000000}` | `{"channel":"futures.pong","time":1710000000}` | JSON  |

---

## 10. Технические детали run.py (главный запускатор)

### ProcInfo dataclass

```python
@dataclass
class ProcInfo:
    name:           str                # Имя без .py ("binance_spot")
    script:         Path               # Путь к файлу
    cwd:            Path               # Рабочая директория
    proc:           Process | None     # asyncio subprocess
    pid:            int                # PID
    start_ts:       float              # unix timestamp старта
    restarts:       int                # Число рестартов
    restart_delay:  float              # Текущая задержка (1.0 → 120.0)
    last_exit_code: int | None         # Код выхода последнего процесса
    last_exit_ts:   float              # Когда упал
    status:         str                # "running" | "stopped" | "restarting"
```

### Список запускаемых процессов (10 штук)

```
market_data/binance_spot.py      cwd=market_data/
market_data/binance_futures.py   cwd=market_data/
market_data/bybit_spot.py        cwd=market_data/
market_data/bybit_futures.py     cwd=market_data/
market_data/okx_spot.py          cwd=market_data/
market_data/okx_futures.py       cwd=market_data/
market_data/gate_spot.py         cwd=market_data/
market_data/gate_futures.py      cwd=market_data/
signal_scanner.py                cwd=project_root/
market_data/redis_monitor.py     cwd=market_data/
```

### Логика автоперезапуска

```
RESTART_RESET_AFTER = 300s   ← если процесс прожил дольше — сброс в 1s
MAX_RESTART_DELAY   = 120s   ← потолок задержки

Алгоритм:
  1. Процесс упал
  2. uptime = now - start_ts
  3. if uptime >= 300: restart_delay = 1.0
  4. wait(restart_delay)
  5. restart_delay = min(restart_delay * 2, 120)
  6. Запустить процесс заново
```

### Секции снапшота

```
СБОРЩИКИ  — процессы с cwd=market_data/ кроме redis_monitor
СКАНЕР    — процессы с cwd=project_root/ (signal_scanner)
МОНИТОРИНГ — redis_monitor
```

### Graceful shutdown (SIGINT/SIGTERM)

```
1. stop_event.set()
2. Отправить SIGTERM каждому процессу
3. Ждать завершения (deadline = now + 5s)
4. Если не завершился — SIGKILL
5. Вывести итоговый снапшот
```
