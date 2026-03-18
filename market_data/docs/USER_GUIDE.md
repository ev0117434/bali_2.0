# Руководство пользователя — скрипты сборщиков

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

## Вывод — снапшоты в формате JSON Lines (каждые 10с)

Каждые 10 секунд каждый скрипт записывает строку-снапшот в stdout и в лог-файл.
Для удобного просмотра используйте `jq`:

```bash
tail -f logs/binance_spot/$(ls -t logs/binance_spot/ | head -1)/binance_spot.log \
  | grep --line-buffered '"type":"snapshot"' | jq .
```

Пример (форматированный):

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

| Поле                               | Описание                                             |
|------------------------------------|------------------------------------------------------|
| `throughput.msgs_per_s`            | WS-сообщений/с за последнее 10с окно                 |
| `throughput.ticker_writes_per_s`   | Записей тикеров в Redis/с                            |
| `throughput.history_entries_per_s` | Записей истории (RPUSH)/с                            |
| `latency.rtt`                      | Exchange timestamp → приём в скрипте (RTT)           |
| `latency.proc`                     | Приём в скрипте → завершение записи в Redis          |
| `history_chunk.chunk_id`           | Номер текущего 20-минутного чанка истории            |
| `history_chunk.remaining_s`        | Секунд до следующей ротации чанка                    |

> **Примечание:** `latency.rtt` samples = 0 для Binance — bookTicker не содержит timestamp биржи.

---

## Поведение при ошибках

### Обрыв WebSocket-соединения

Скрипт переподключается автоматически с экспоненциальной выдержкой:

```
Обрыв              -> ждать 1s -> переподключение
Повторный обрыв    -> ждать 2s -> переподключение
Ещё раз            -> ждать 4s -> переподключение
...
Максимум           -> 60s между попытками
```

Если соединение держалось ≥ 60с — задержка сбрасывается до 1s.

Счётчик переподключений виден в снапшоте: `connections[].reconnects`.

### Падение всего скрипта (только run_all.py)

`run_all.py` следит за процессами и перезапускает по той же логике:

```
Процесс упал -> ждать 1s -> перезапуск
...
Максимум     -> 120s между попытками
```

Если процесс прожил ≥ 300с — задержка сбрасывается до 1s.

### Redis недоступен при старте

Скрипт делает 5 попыток с экспоненциальной выдержкой (1s → 2s → 4s → 8s → 16s), затем завершается с ошибкой.

---

## Проверка работы

### Проверить тикер

```bash
redis-cli HGETALL md:binance:spot:BTCUSDT
# 1) "bid"
# 2) "45000.12"
# 3) "ask"
# 4) "45000.13"
# 5) "ts"
# 6) "1710000000.123"
```

### Проверить историю

```bash
redis-cli LRANGE md:hist:binance:spot:BTCUSDT:1 0 4
# 1) "1710000000.123,45000.12,45000.13"
# 2) "1710000001.456,45000.14,45000.15"
# ...
```

### Подсчитать ключи в Redis

```bash
redis-cli --scan --pattern 'md:*' | wc -l
```

### Убедиться, что данные обновляются

```bash
watch -n 1 'redis-cli HGET md:bybit:spot:BTCUSDT ts'
```

---

## Особенности отдельных скриптов

---

### binance_spot.py

**WebSocket:** `wss://stream.binance.com:9443/stream?streams=...`
**Канал:** `{symbol}@bookTicker` — самые быстрые обновления best bid/ask
**Лимит:** 300 символов на соединение
**RTT:** недоступен (Binance bookTicker не содержит timestamp биржи)

Подписка встроена прямо в URL:
```
wss://...?streams=btcusdt@bookTicker/ethusdt@bookTicker/...
```

---

### binance_futures.py

**WebSocket:** `wss://fstream.binance.com/stream?streams=...`
**Канал:** `{symbol}@bookTicker`
Идентичен `binance_spot.py`, отличается только хостом.

---

### bybit_spot.py

**WebSocket:** `wss://stream.bybit.com/v5/public/spot`
**Канал:** `orderbook.1.{SYMBOL}` — L1 стакан (только лучший уровень)
**Лимит:** 200 символов на соединение, 10 символов в одном subscribe-сообщении
**Ping:** `{"op":"ping"}` каждые 20с
**RTT:** доступен (поле `ts` в миллисекундах)

Subscribe-сообщения отправляются после установки соединения:
```json
{"op":"subscribe","args":["orderbook.1.BTCUSDT","orderbook.1.ETHUSDT",...]}
```

---

### bybit_futures.py

**WebSocket:** `wss://stream.bybit.com/v5/public/linear`
**Канал:** `orderbook.1.{SYMBOL}`
Идентичен `bybit_spot.py`, отличается URL и размером батча (200 вместо 10).

---

### okx_spot.py

**WebSocket:** `wss://ws.okx.com:8443/ws/v5/public`
**Канал:** `tickers` с `instId: "BTC-USDT"` (формат через дефис)
**Лимит:** 300 символов на соединение
**Ping:** строка `"ping"` каждые 25с, ответ `"pong"`
**RTT:** доступен (поле `ts` — строка в мс)

Конвертация символов: `BTCUSDT` → `BTC-USDT` (subscribe) → `BTCUSDT` (ключ Redis).

```json
{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"},...]}
```

---

### okx_futures.py

**WebSocket:** `wss://ws.okx.com:8443/ws/v5/public`
**Канал:** `tickers` с `instId: "BTC-USDT-SWAP"`
Конвертация: `BTCUSDT` → `BTC-USDT-SWAP` → `BTCUSDT`.

```json
{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT-SWAP"},...]}
```

---

### gate_spot.py

**WebSocket:** `wss://api.gateio.ws/ws/v4/`
**Канал:** `spot.book_ticker`
**Лимит:** 500 символов на соединение, батчи по 100 с паузой 50ms между батчами
**Ping:** `{"channel":"spot.ping","time":<unix_ts>}` каждые 25с
**RTT:** доступен (поле `result.t` в миллисекундах)

Конвертация символов: `BTCUSDT` → `BTC_USDT` → `BTCUSDT`.

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
**Канал:** `futures.book_ticker`
**Ping:** `{"channel":"futures.ping","time":<unix_ts>}` каждые 25с
Идентичен `gate_spot.py`, отличается URL и именами каналов.

---

## Отладка

### Запустить один скрипт с выводом в терминал

```bash
REDIS_HOST=127.0.0.1 python3 market_data/binance_spot.py
```

### Принудительно сбросить Redis и запустить один скрипт

```bash
redis-cli --scan --pattern 'md:*' | xargs redis-cli DEL
python3 market_data/binance_spot.py
```

### Проверить подключённые WebSocket-процессы

```bash
ss -tp | grep python
```

### Смотреть задержки в реальном времени (через логи)

```bash
tail -f logs/bybit_spot/$(ls -t logs/bybit_spot/ | head -1)/bybit_spot.log \
  | grep --line-buffered '"type":"snapshot"' \
  | jq --unbuffered '{rtt: .latency.rtt.avg_ms, proc: .latency.proc.avg_ms}'
```
