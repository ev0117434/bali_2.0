# User Guide — Запуск и эксплуатация системы

## Запуск

### Вся система (рекомендуется)

```bash
# Из корня проекта
python3 run.py
```

При запуске:
1. Удаляются все `md:*` ключи в Redis (чистый старт)
2. Предлагается очистить папку `logs/`
3. Запускаются 10 процессов: 8 коллекторов + signal_scanner + redis_monitor
4. Каждые 10 секунд — снапшот состояния всех процессов

**Остановка:** `Ctrl+C` → graceful shutdown в течение 5 секунд.

---

### Только коллекторы (без сканера и мониторинга)

```bash
python3 market_data/run_all.py
```

Запускает только 8 WebSocket-коллекторов. Полезно если сканер и мониторинг запускаются отдельно.

---

### Один скрипт отдельно (отладка)

```bash
python3 market_data/binance_spot.py
python3 market_data/bybit_futures.py
python3 signal_scanner.py
python3 market_data/redis_monitor.py
```

---

## Предварительное условие: списки торговых пар

Перед первым запуском нужно сгенерировать файлы подписки:

```bash
cd dictionaries
python3 main.py   # ~2 минуты
cd ..
```

Результат: файлы в `dictionaries/subscribe/` (читают коллекторы) и `dictionaries/combination/` (читает signal_scanner).

Обновлять пары рекомендуется раз в сутки — состав активных пар на биржах меняется.

---

## Файлы подписки коллекторов

Каждый коллектор загружает символы из:
```
dictionaries/subscribe/{exchange}/{exchange}_{market}.txt
```

| Скрипт            | Файл подписки                                              |
|-------------------|------------------------------------------------------------|
| binance_spot      | `dictionaries/subscribe/binance/binance_spot.txt`          |
| binance_futures   | `dictionaries/subscribe/binance/binance_futures.txt`       |
| bybit_spot        | `dictionaries/subscribe/bybit/bybit_spot.txt`              |
| bybit_futures     | `dictionaries/subscribe/bybit/bybit_futures.txt`           |
| okx_spot          | `dictionaries/subscribe/okx/okx_spot.txt`                  |
| okx_futures       | `dictionaries/subscribe/okx/okx_futures.txt`               |
| gate_spot         | `dictionaries/subscribe/gate/gate_spot.txt`                |
| gate_futures      | `dictionaries/subscribe/gate/gate_futures.txt`             |

**Формат файла:** один символ на строку, uppercase. Строки с `#` — комментарии.

```
# Bitcoin
BTCUSDT
ETHUSDT
SOLUSDT
```

**Добавить символ:** вписать в файл и перезапустить скрипт.

---

## Как читать снапшот коллектора (каждые 10с)

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

| Поле            | Что означает                                          |
|-----------------|-------------------------------------------------------|
| `messages`      | WS-сообщений за последние 10с                         |
| `t.writes`      | Записей тикеров в Redis за 10с                        |
| `h.entries`     | Записей в историю (List) за 10с                       |
| `RTT`           | Задержка биржа → скрипт (где есть exchange ts)        |
| `Proc`          | Задержка получение → запись в Redis                   |
| `chunk_id`      | Номер текущего 20-минутного чанка истории             |
| `remaining`     | Секунд до следующей ротации чанка                     |

---

## Как читать снапшот signal_scanner (каждые 10с)

Пишется в лог `logs/signal_scanner/.../signal_scanner.log` и в stdout.

```json
{
  "type": "snapshot",
  "timestamp": "2024-03-01T12:00:10.451Z",
  "uptime_s": 600,
  "pairs_loaded": 320,
  "min_spread_pct": 1.5,
  "scans_total": 3000,
  "scans_window": 50,
  "signals_total": 7,
  "signals_window": 1,
  "pipeline_ms": {"min": 0.8, "avg": 1.2, "p95": 2.1, "max": 3.4},
  "data_age_ms":  {"min": 12,  "avg": 35,  "p95": 90,  "max": 210}
}
```

| Поле             | Что означает                                          |
|------------------|-------------------------------------------------------|
| `pairs_loaded`   | Пар загружено из combination/ (по всем 12 файлам)    |
| `scans_total`    | Всего проходов сканирования                           |
| `signals_total`  | Всего сигналов с момента запуска                      |
| `pipeline_ms`    | Время выполнения Redis pipeline (мс)                  |
| `data_age_ms`    | Возраст данных в Redis (now − ts тикера)              |

---

## Как читать метрики redis_monitor (каждые 5с)

По умолчанию пишется в JSON Lines (`MONITOR_LOG_JSON=1`).

Пример одной строки лога:
```json
{
  "timestamp": "2024-03-01T12:00:10Z",
  "latency_ping": {"min_ms": 0.12, "avg_ms": 0.18, "p95_ms": 0.31, "max_ms": 0.45},
  "memory": {"used_human": "45.2M", "peak_human": "48.1M", "fragmentation_ratio": 1.05},
  "connections": {"connected_clients": 11, "rejected_connections": 0},
  "ops": {"instantaneous_ops_per_sec": 2340, "instantaneous_input_kbps": 180.5},
  "keyspace": {"db_total_keys": 12450, "md_keys": 12430},
  "slowlog_len": 0
}
```

---

## Поведение при ошибках

### Разрыв WebSocket соединения (внутри скрипта)

Автоматическое переподключение с экспоненциальной задержкой:

```
Упало → ждать 1s → реконнект
Снова → ждать 2s → реконнект
Снова → ждать 4s → реконнект
...
Максимум         → 60s
```

Если соединение держалось ≥ 60с — задержка сбрасывается до 1s. Счётчик переподключений виден в снапшоте (`reconnects=N`).

### Падение всего скрипта (через run.py / run_all.py)

```
Упал → ждать 1s → перезапуск
...
Максимум         → 120s
```

Если процесс жил ≥ 300с — задержка сбрасывается до 1s.

### Redis недоступен при старте

5 попыток с backoff 1s → 2s → 4s → 8s → 16s, затем выход с ошибкой.

---

## Проверка поступления данных

### Посмотреть тикер в Redis

```bash
redis-cli HGETALL md:binance:spot:BTCUSDT
# 1) "bid"
# 2) "45000.12"
# 3) "ask"
# 4) "45000.13"
# 5) "ts"
# 6) "1710000000.123"
```

### Убедиться, что данные обновляются

```bash
watch -n 1 'redis-cli HGET md:bybit:spot:BTCUSDT ts'
```

### Посмотреть историю

```bash
redis-cli LRANGE md:hist:binance:spot:BTCUSDT:1 0 4
# 1) "1710000000.123,45000.12,45000.13"
# 2) "1710000001.456,45000.14,45000.15"
```

### Количество активных тикеров

```bash
redis-cli --scan --pattern 'md:*' | wc -l
```

### Последние сигналы

```bash
tail -20 signal/signals.csv
```

---

## Особенности каждого коллектора

### binance_spot.py

- **WS:** `wss://stream.binance.com:9443/stream?streams=...`
- **Канал:** `{symbol}@bookTicker` — самые быстрые обновления
- **Лимит:** 300 символов / соединение
- **RTT:** недоступен (Binance не включает ts в bookTicker)
- Стримы встроены в URL соединения

### binance_futures.py

- **WS:** `wss://fstream.binance.com/stream?streams=...`
- Идентичен binance_spot, отличается только хостом

### bybit_spot.py

- **WS:** `wss://stream.bybit.com/v5/public/spot`
- **Канал:** `orderbook.1.{SYMBOL}` (L1 стакан)
- **Лимит:** 200 символов / соединение, батч подписки по 10
- **Пинг:** `{"op":"ping"}` каждые 20с
- **RTT:** доступен (поле `ts` в мс)

### bybit_futures.py

- **WS:** `wss://stream.bybit.com/v5/public/linear`
- Идентичен bybit_spot, батч подписки по 200

### okx_spot.py

- **WS:** `wss://ws.okx.com:8443/ws/v5/public`
- **Канал:** `tickers` с `instId: "BTC-USDT"`
- **Лимит:** 300 инструментов / соединение
- **Пинг:** строка `"ping"` каждые 25с
- **RTT:** доступен
- Конвертация: `BTCUSDT` → `BTC-USDT` (подписка) → `BTCUSDT` (Redis)

### okx_futures.py

- **WS:** `wss://ws.okx.com:8443/ws/v5/public`
- **Канал:** `tickers` с `instId: "BTC-USDT-SWAP"`
- Конвертация: `BTCUSDT` → `BTC-USDT-SWAP` → `BTCUSDT`

### gate_spot.py

- **WS:** `wss://api.gateio.ws/ws/v4/`
- **Канал:** `spot.book_ticker`
- **Лимит:** 500 символов / соединение, батч по 100 с паузой 50ms
- **Пинг:** `{"channel":"spot.ping","time":<ts>}` каждые 25с
- **RTT:** доступен
- Конвертация: `BTCUSDT` → `BTC_USDT` → `BTCUSDT`

### gate_futures.py

- **WS:** `wss://fx-ws.gateio.ws/v4/ws/usdt`
- **Канал:** `futures.book_ticker`
- Идентичен gate_spot, отличается URL и именами каналов

---

## Отладка

### Запустить скрипт с нестандартным Redis

```bash
REDIS_HOST=10.0.0.5 REDIS_PASSWORD=secret python3 market_data/binance_spot.py
```

### Принудительно сбросить Redis и запустить

```bash
redis-cli --scan --pattern 'md:*' | xargs redis-cli DEL
python3 market_data/binance_spot.py
```

### Проверить подключённые WebSocket процессы

```bash
ss -tp | grep python
```

### Следить за задержками в реальном времени

```bash
tail -f logs/bybit_spot/$(ls -t logs/bybit_spot/ | head -1)/bybit_spot.log | grep "Proc\|RTT"
```

### Посмотреть последние ошибки по всем скриптам

```bash
grep -r "Ошибка\|Error\|error" logs/*/$(ls -t logs/binance_spot/ | head -1)/
```

### Нагрузочный тест Redis

```bash
python3 market_data/tests/load_test.py
```
