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

## Как читать вывод (снапшот каждые 10с)

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
  RTT (exchange→recv):   n/a         ← Binance не даёт timestamp
  Proc (recv→redis):     min=0.3ms  avg=0.8ms  p95=1.9ms  max=3.1ms

 History chunk:
  chunk_id=1  elapsed=10s / 1200s  remaining=1190s

 Errors: none
================================================================
```

| Поле            | Что означает                                      |
|-----------------|---------------------------------------------------|
| `messages`      | Кол-во WS-сообщений за последние 10 секунд        |
| `t.writes`      | Записей тикеров в Redis за 10с                    |
| `h.entries`     | Записей в историю за 10с                          |
| `RTT`           | Задержка биржа → скрипт (только где есть ts)      |
| `Proc`          | Задержка получение → запись в Redis               |
| `chunk_id`      | Номер текущего 20-минутного чанка истории         |
| `remaining`     | Сколько секунд до следующей ротации чанка         |

---

## Поведение при ошибках

### Разрыв WebSocket соединения

Скрипт автоматически переподключается с экспоненциальной задержкой:

```
Соединение упало → ждать 1s → переподключение
Снова упало     → ждать 2s → переподключение
Снова упало     → ждать 4s → переподключение
...
Максимум        → 60s между попытками
```

Если соединение держалось ≥ 60 секунд — задержка сбрасывается до 1s.

Счётчик переподключений виден в снапшоте (`reconnects=N`).

### Падение всего скрипта (только при run_all.py)

`run_all.py` отслеживает процессы и перезапускает с той же логикой:

```
Процесс упал → ждать 1s → перезапуск
...
Максимум     → 120s между попытками
```

Если процесс жил ≥ 300 секунд — задержка сбрасывается до 1s.

### Redis недоступен при старте

Скрипт делает 5 попыток с exponential backoff (1s → 2s → 4s → 8s → 16s), затем завершается с ошибкой.

---

## Как проверить что данные поступают

### Посмотреть тикер

```bash
redis-cli HGETALL md:binance:spot:BTCUSDT
# 1) "bid"
# 2) "45000.12"
# 3) "ask"
# 4) "45000.13"
# 5) "ts"
# 6) "1710000000.123"
```

### Посмотреть историю

```bash
redis-cli LRANGE md:hist:binance:spot:BTCUSDT:1 0 4
# 1) "1710000000.123,45000.12,45000.13"
# 2) "1710000001.456,45000.14,45000.15"
# ...
```

### Количество ключей в Redis

```bash
redis-cli --scan --pattern 'md:*' | wc -l
```

### Убедиться, что данные обновляются

```bash
watch -n 1 'redis-cli HGET md:bybit:spot:BTCUSDT ts'
```

---

## Отдельные скрипты — особенности

---

### binance_spot.py

**WebSocket:** `wss://stream.binance.com:9443/stream?streams=...`
**Канал:** `{symbol}@bookTicker` — самые быстрые обновления best bid/ask
**Лимит:** 300 символов на одно соединение
**RTT:** недоступен (Binance не включает timestamp в bookTicker)

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
**Лимит:** 200 символов на соединение, батчи по 10 символов в подписке
**Ping:** каждые 20 секунд `{"op":"ping"}`
**RTT:** доступен (поле `ts` в миллисекундах)

Подписка отправляется отдельными сообщениями после установки соединения:
```json
{"op":"subscribe","args":["orderbook.1.BTCUSDT","orderbook.1.ETHUSDT",...]}
```

---

### bybit_futures.py

**WebSocket:** `wss://stream.bybit.com/v5/public/linear`
**Канал:** `orderbook.1.{SYMBOL}`
Идентичен `bybit_spot.py`, отличается URL и лимитом батча (200 вместо 10).

---

### okx_spot.py

**WebSocket:** `wss://ws.okx.com:8443/ws/v5/public`
**Канал:** `tickers` с `instId: "BTC-USDT"` (формат с дефисом)
**Лимит:** 300 символов на соединение
**Ping:** строка `"ping"` каждые 25 секунд, ответ `"pong"`
**RTT:** доступен (поле `ts` — строка в мс)

Конвертация символов: `BTCUSDT` → `BTC-USDT` (при подписке) → `BTCUSDT` (при записи).

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
**Лимит:** 500 символов на соединение, батчи по 100 с задержкой 50ms между батчами
**Ping:** `{"channel":"spot.ping","time":<unix_ts>}` каждые 25с
**RTT:** доступен (поле `result.t` в миллисекундах)

Конвертация: `BTCUSDT` → `BTC_USDT` → `BTCUSDT`.

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

### Проверить подключённые WebSocket процессы

```bash
ss -tp | grep python
```

### Посмотреть задержки в реальном времени (через логи)

```bash
grep "Proc\|RTT" logs/bybit_spot/$(ls -t logs/bybit_spot/ | head -1)/bybit_spot.log | tail -5
```
