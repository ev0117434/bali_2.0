# bali_2.0 — Система сбора рыночных данных

Асинхронная система real-time сбора лучших цен bid/ask (book ticker) с четырёх
криптовалютных бирж через WebSocket. Данные записываются в Redis и доступны
вышестоящим сервисам через несколько форматов ключей.

---

## Содержание

1. [Архитектура](#1-архитектура)
2. [Требования](#2-требования)
3. [Быстрый старт](#3-быстрый-старт)
4. [Структура проекта](#4-структура-проекта)
5. [Модуль dictionaries — генератор пар](#5-модуль-dictionaries--генератор-пар)
6. [Модуль market_data — сборщики данных](#6-модуль-market_data--сборщики-данных)
7. [Скрипт redis_monitor.py — мониторинг Redis](#7-скрипт-redis_monitorpy--мониторинг-redis)
8. [Схема данных Redis](#8-схема-данных-redis)
9. [Конфигурация — переменные окружения](#9-конфигурация--переменные-окружения)
10. [Система логирования](#10-система-логирования)
11. [Снапшоты и метрики](#11-снапшоты-и-метрики)
12. [Диагностика Redis вручную](#12-диагностика-redis-вручную)
13. [Производительность и ограничения](#13-производительность-и-ограничения)
14. [Устранение неисправностей](#14-устранение-неисправностей)
15. [Справочник по всем файлам](#15-справочник-по-всем-файлам)

---

## 1. Архитектура

### Высокоуровневая схема

```
python3 start.py
│
├── market_data/run_all.py        (менеджер процессов)
│   ├── binance_spot.py           WebSocket → Redis
│   ├── binance_futures.py        WebSocket → Redis
│   ├── bybit_spot.py             WebSocket → Redis
│   ├── bybit_futures.py          WebSocket → Redis
│   ├── okx_spot.py               WebSocket → Redis
│   ├── okx_futures.py            WebSocket → Redis
│   ├── gate_spot.py              WebSocket → Redis
│   └── gate_futures.py           WebSocket → Redis
│
└── market_data/redis_monitor.py  (мониторинг Redis)
```

### Поток данных внутри одного сборщика

```
Биржевой WebSocket
        │
        │  raw JSON  (best bid/ask обновление)
        ▼
    _ws_worker  (asyncio task, один на каждые N символов)
        │
        │  parse → normalize symbol → extract bid / ask / ts
        ▼
  _ticker_buf  [dict: symbol → (bid, ask, local_ts)]     ← перезаписывается
  _history_buf [dict: symbol → [(ts, bid, ask), ...]]    ← накапливается
        │                  │
        │  каждые 50ms      │  каждые 1s
        ▼                  ▼
  Redis Pipeline         Redis Pipeline
  HSET md:ex:mkt:sym     RPUSH md:hist:ex:mkt:sym:N
```

### Что хранится в Redis

```
md:{exchange}:{market}:{symbol}          Hash  bid/ask/ts  — текущий тикер
md:hist:{exchange}:{market}:{symbol}:{N} List  "ts,bid,ask" — история (чанк N)
md:chunks:config                         Hash  JSON          — метаданные чанков
```

---

## 2. Требования

| Компонент    | Версия  |
|--------------|---------|
| Python       | 3.11+   |
| Redis        | любая современная (5+) |
| redis-py     | ≥ 7.0.0 |
| websockets   | ≥ 12.0  |

Установка зависимостей:

```bash
pip install -r market_data/requirements.txt
```

Redis должен быть запущен и доступен до старта сборщиков.

---

## 3. Быстрый старт

### Шаг 1 — Обновить списки торговых пар (если нужно)

Делается один раз или перед каждым запуском при необходимости актуальных пар.

```bash
python3 dictionaries/main.py
```

Занимает ~65–70 секунд (60 сек — WebSocket-валидация на всех 4 биржах параллельно).
По завершении обновляются файлы в `dictionaries/subscribe/`.

> Если файлы `dictionaries/subscribe/` уже содержат нужные символы — этот шаг
> можно пропустить.

### Шаг 2 — Запустить систему

```bash
python3 start.py
```

При первом запуске run_all.py задаст вопрос:

```
[run_all] Очищаю Redis (md:*)... удалено 0 ключей.
Очистить папку logs/? [y/N]:
```

Ответьте `y` (очистить) или Enter (оставить).

После этого:
- Запустятся 8 сборщиков данных (каждый как отдельный процесс)
- Через 3 секунды запустится redis_monitor.py
- Снапшоты состояния будут выводиться каждые 10 секунд (от сборщиков) и каждые 30 секунд (от start.py)

### Остановка

```
Ctrl+C
```

Все процессы получат SIGTERM. Если не остановятся в течение 5 секунд — SIGKILL.

---

## 4. Структура проекта

```
bali_2.0/
│
├── start.py                       ← ТОЧКА ВХОДА: запускает всё
│
├── market_data/
│   ├── run_all.py                 ← Менеджер 8 сборщиков
│   ├── common.py                  ← Общие утилиты (Redis, логи, статистика)
│   │
│   ├── binance_spot.py            ← Сборщик: Binance SPOT
│   ├── binance_futures.py         ← Сборщик: Binance FUTURES
│   ├── bybit_spot.py              ← Сборщик: Bybit SPOT
│   ├── bybit_futures.py           ← Сборщик: Bybit FUTURES
│   ├── okx_spot.py                ← Сборщик: OKX SPOT
│   ├── okx_futures.py             ← Сборщик: OKX FUTURES
│   ├── gate_spot.py               ← Сборщик: Gate.io SPOT
│   ├── gate_futures.py            ← Сборщик: Gate.io FUTURES
│   │
│   ├── redis_monitor.py           ← Мониторинг Redis
│   ├── requirements.txt           ← Зависимости Python
│   │
│   ├── docs/
│   │   ├── README.md              ← Архитектура market_data
│   │   ├── TECH_SPEC.md           ← Технические детали
│   │   ├── LOGS.md                ← Система логов
│   │   ├── OVERVIEW.md            ← Боттлнеки и нагрузочные тесты
│   │   └── USER_GUIDE.md          ← Гайд по каждому скрипту
│   │
│   └── tests/
│       └── load_test.py           ← Нагрузочные тесты Redis
│
├── dictionaries/
│   ├── main.py                    ← Генератор списков торговых пар
│   ├── README.md                  ← Документация модуля
│   │
│   ├── binance/
│   │   ├── binance_pairs.py       ← REST API: получение пар
│   │   └── binance_ws.py          ← WS-валидация активности пар
│   ├── bybit/
│   │   ├── bybit_pairs.py
│   │   └── bybit_ws.py
│   ├── okx/
│   │   ├── okx_pairs.py
│   │   └── okx_ws.py
│   ├── gate/
│   │   ├── gate_pairs.py
│   │   └── gate_ws.py
│   │
│   ├── combination/               ← Генерируется: пересечения пар
│   │   ├── binance_spot_bybit_futures.txt
│   │   ├── bybit_spot_binance_futures.txt
│   │   └── ... (12 файлов)
│   │
│   └── subscribe/                 ← Генерируется: списки подписки
│       ├── binance/
│       │   ├── binance_spot.txt
│       │   └── binance_futures.txt
│       ├── bybit/
│       ├── okx/
│       └── gate/
│
└── logs/                          ← Создаётся при запуске
    ├── binance_spot/
    ├── binance_futures/
    ├── ...
    └── redis_monitor/
```

---

## 5. Модуль dictionaries — генератор пар

### Назначение

Собирает активные торговые пары с 4 бирж, валидирует их через WebSocket
(только те пары, по которым пришёл хотя бы один тик за 60 секунд) и генерирует
файлы подписки для сборщиков.

### Запуск

```bash
python3 dictionaries/main.py
```

**Время выполнения:** ~65–70 секунд.

### 5 фаз выполнения

| Фаза | Что происходит | Время |
|------|---------------|-------|
| 1 | REST API: получение всех пар с 4 бирж (параллельно, 4 потока) | ~3–5s |
| 2 | WebSocket: 60-секундная валидация активности (4 биржи параллельно) | ~60s |
| 3 | Построение пересечений spot_A ∩ futures_B (12 комбинаций) | <1s |
| 4 | Построение subscribe-файлов (объединение без дублей) | <1s |
| 5 | Итоговый отчёт в stdout | — |

### Логика subscribe-файлов

Каждый subscribe-файл содержит объединение пар из всех combination-файлов,
в имени которых есть ключевое слово данного рынка:

```
binance_spot_bybit_futures.txt  →  binance_spot.txt  +  bybit_futures.txt
binance_spot_okx_futures.txt    →  binance_spot.txt  +  okx_futures.txt
binance_spot_gate_futures.txt   →  binance_spot.txt  +  gate_futures.txt
...
```

Итого: 12 combination-файлов → 8 subscribe-файлов, без дублей.

### Формат файлов подписки

```
# Один символ на строку, uppercase, USDT/USDC-деноминированные пары
BTCUSDT
ETHUSDT
SOLUSDT
...
```

Строки, начинающиеся с `#` — комментарии, игнорируются.

### Нормализация символов

| Биржа | API-формат | Нормализованный |
|-------|-----------|----------------|
| Binance | `BTCUSDT` | `BTCUSDT` |
| Bybit | `BTCUSDT` | `BTCUSDT` |
| OKX | `BTC-USDT`, `BTC-USDT-SWAP` | `BTCUSDT` |
| Gate.io | `BTC_USDT` | `BTCUSDT` |

---

## 6. Модуль market_data — сборщики данных

### Запуск (рекомендуется через start.py)

```bash
python3 start.py
```

### Запуск только сборщиков (без redis_monitor)

```bash
python3 market_data/run_all.py
```

### Запуск одного сборщика (для отладки)

```bash
python3 market_data/binance_spot.py
python3 market_data/bybit_futures.py
# и т.д.
```

### Действия run_all.py при старте

1. **Flush Redis** — удаляет все ключи `md:*` (всегда, без вопросов)
2. **Вопрос о логах** — `Очистить папку logs/? [y/N]:`
   - `y`, `yes`, `д`, `да` → удаляет содержимое `logs/`
   - Любой другой ввод → логи сохраняются
3. **Запуск 8 subprocess** — по одному на каждый скрипт

### Автоперезапуск процессов (run_all.py)

Если процесс упал, run_all.py перезапустит его с экспоненциальной задержкой:

```
Упал → ждём 1s → рестарт
Снова упал → ждём 2s → рестарт
Снова упал → ждём 4s → рестарт
...
Максимум — 120s между попытками
```

Если процесс прожил ≥ 300 секунд — задержка сбрасывается до 1s при следующем падении.

### Параметры WebSocket-соединений

| Скрипт | WS URL | Символов/соединение | Формат канала |
|--------|--------|---------------------|---------------|
| binance_spot | `wss://stream.binance.com:9443` | 300 | `{sym}@bookTicker` |
| binance_futures | `wss://fstream.binance.com` | 300 | `{sym}@bookTicker` |
| bybit_spot | `wss://stream.bybit.com/v5/public/spot` | 200 | `orderbook.1.{SYM}` |
| bybit_futures | `wss://stream.bybit.com/v5/public/linear` | 200 | `orderbook.1.{SYM}` |
| okx_spot | `wss://ws.okx.com:8443/ws/v5/public` | 300 | `tickers` / `BTC-USDT` |
| okx_futures | `wss://ws.okx.com:8443/ws/v5/public` | 300 | `tickers` / `BTC-USDT-SWAP` |
| gate_spot | `wss://api.gateio.ws/ws/v4/` | 500 | `spot.book_ticker` |
| gate_futures | `wss://fx-ws.gateio.ws/v4/ws/usdt` | 500 | `futures.book_ticker` |

### Ping/Pong keepalive

| Биржа | Что отправляем | Ответ | Интервал |
|-------|---------------|-------|----------|
| Binance | (автоматически библиотекой) | — | 20s |
| Bybit | `{"op":"ping"}` | `{"op":"pong",...}` | 20s |
| OKX | строка `"ping"` | строка `"pong"` | 25s |
| Gate spot | `{"channel":"spot.ping","time":N}` | `{"channel":"spot.pong",...}` | 25s |
| Gate futures | `{"channel":"futures.ping","time":N}` | `{"channel":"futures.pong",...}` | 25s |

### Автоподключение WebSocket

При разрыве соединения скрипт автоматически переподключается:

```
Разрыв → ждём 1s → переподключение
Снова → ждём 2s → переподключение
...
Максимум — 60s между попытками
```

Если соединение держалось ≥ 60 секунд — задержка сбрасывается до 1s.

---

## 7. Скрипт redis_monitor.py — мониторинг Redis

### Назначение

Периодически опрашивает Redis и записывает снапшоты состояния в лог
и stdout. Не влияет на работу сборщиков.

### Запуск (автоматически через start.py)

```bash
python3 market_data/redis_monitor.py
```

### Что проверяется каждые MONITOR_INTERVAL секунд

| Блок снапшота | Содержимое |
|--------------|------------|
| **REDIS** | Статус подключения, ping-латентность (мс), версия, uptime |
| **ПАМЯТЬ** | `used_memory`, пик памяти, коэффициент фрагментации |
| **КЛИЕНТЫ** | Подключённых клиентов, заблокированных, отклонённых соединений |
| **ПРОИЗВОДИТЕЛЬНОСТЬ** | cmd/s, ops/sec (мгновенное), keyspace hit-ratio, сетевой трафик, RDB/AOF |
| **КЛЮЧИ В REDIS** | Тикеры `md:*`, история `md:hist:*`, наличие `md:chunks:config` |
| **АКТУАЛЬНОСТЬ ТИКЕРОВ** | По каждой бирже/рынку: кол-во ключей, возраст свежего/старого тикера, ⚠ устаревшие |
| **HISTORY ЧАНКИ** | Текущий чанк, активные чанки, прошло/осталось секунд, кол-во hist-ключей |

### Пример снапшота

```
==================================================================
SNAPSHOT  redis_monitor        2024-03-01 12:00:10.451 UTC
==================================================================
Uptime монитора: 0h 00m 10s

REDIS  [✓ ONLINE]
  Адрес:          127.0.0.1:6379  db=0
  Версия:         Redis 7.2.4
  Uptime Redis:   2h 15m 30s
  Ping:           0.18 ms

ПАМЯТЬ
  Используется:   24.5M  (пик: 25.1 MB, 97.6%)
  Фрагментация:   1.05x

КЛИЕНТЫ
  Подключено:     9
  Заблокировано:  0
  Отклонено всего:0

ПРОИЗВОДИТЕЛЬНОСТЬ
  Команд всего:   1,248,312  (850 cmd/s за 10s)
  ops/sec (live): 843
  Keyspace hit%:  99.2%  (+15,240 hits  +123 misses  за 10s)
  Сеть:           in=18.2 MB  out=142.7 MB  (всего с запуска Redis)
  RDB:            ok    AOF: выкл

КЛЮЧИ В REDIS
  Всего md:*:            1,524
  Тикеры  md:ex:mkt:sym:   24
  История md:hist:*:      1,499
  md:chunks:config:       ✓ есть

АКТУАЛЬНОСТЬ ТИКЕРОВ
  Биржа / Рынок          Ключей    Свежий    Старый  Устар.
  -------------------------------------------------------------
  binance/spot                3      0.1s      0.3s       0
  binance/futures             3      0.1s      0.2s       0
  bybit/spot                  3      0.2s      0.5s       0
  ...

HISTORY ЧАНКИ
  binance/spot            чанк #3  активных: [1, 2, 3]  hist-ключей: 9
                          прошло: 415s  осталось: 785s
  ...
==================================================================
```

### Переменные окружения redis_monitor.py

| Переменная | По умолчанию | Описание |
|-----------|-------------|---------|
| `MONITOR_INTERVAL` | `10` | Интервал между снапшотами (секунд) |
| `STALE_THRESHOLD_S` | `60` | Тикер считается устаревшим, если старше N секунд |

---

## 8. Схема данных Redis

### Текущие тикеры

```
Ключ:   md:{exchange}:{market}:{symbol}
Тип:    Hash
Поля:
  bid  →  строка-цена, без trailing zeros  (пример: "45000.12")
  ask  →  строка-цена                      (пример: "45000.13")
  ts   →  unix timestamp с 3 знаками       (пример: "1710000000.123")
TTL:    нет (перезаписывается при каждом обновлении)
```

Примеры ключей:
```
md:binance:spot:BTCUSDT
md:binance:futures:ETHUSDT
md:bybit:spot:SOLUSDT
md:bybit:futures:ADAUSDT
md:okx:spot:BTCUSDT
md:okx:futures:ETHUSDT
md:gate:spot:LTCUSDT
md:gate:futures:DOTUSDT
```

Чтение тикера:
```bash
redis-cli HGETALL md:binance:spot:BTCUSDT
# 1) "bid"
# 2) "45000.12"
# 3) "ask"
# 4) "45000.13"
# 5) "ts"
# 6) "1710000000.123"
```

### История (20-минутные чанки)

```
Ключ:   md:hist:{exchange}:{market}:{symbol}:{chunk_id}
Тип:    List (RPUSH → правый конец, хронологический порядок)
Элемент: строка "{ts:.3f},{bid},{ask}"
Пример: "1710000010.456,45000.12,45000.13"
TTL:    нет
```

- `chunk_id` — целое, начинается с 1, увеличивается при каждой ротации
- Хранится последние 5 чанков ≈ ~1.5–2 часа истории
- При создании 6-го чанка — 1-й удаляется

Чтение истории:
```bash
redis-cli LRANGE md:hist:binance:spot:BTCUSDT:1 0 9
# 1) "1710000000.123,45000.12,45000.13"
# 2) "1710000001.456,45000.14,45000.15"
# ...

redis-cli LLEN md:hist:binance:spot:BTCUSDT:1
# (integer) 1523
```

### Метаданные чанков

```
Ключ:   md:chunks:config
Тип:    Hash
Поля:   {exchange}:{market}  →  JSON-строка
```

Пример поля `"binance:spot"`:
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
    "2": {
      "start_ts":  1710001200.0,
      "start_dt":  "2024-03-10 00:20:00 UTC",
      "end_ts":    1710002400.0,
      "end_dt":    "2024-03-10 00:40:00 UTC"
    },
    "3": {
      "start_ts":  1710002400.0,
      "start_dt":  "2024-03-10 00:40:00 UTC",
      "end_ts":    null,
      "end_dt":    null
    }
  }
}
```

Чтение конфига:
```bash
redis-cli HGETALL md:chunks:config
redis-cli HGET md:chunks:config binance:spot
```

---

## 9. Конфигурация — переменные окружения

### Redis

| Переменная | По умолчанию | Описание |
|-----------|-------------|---------|
| `REDIS_HOST` | `127.0.0.1` | Хост Redis |
| `REDIS_PORT` | `6379` | Порт Redis |
| `REDIS_DB` | `0` | Номер базы данных |
| `REDIS_PASSWORD` | — | Пароль (если не задан — без аутентификации) |

Пример с нестандартными параметрами:
```bash
REDIS_HOST=10.0.0.5 REDIS_PORT=6380 python3 start.py
```

Или через `.env` (если используете dotenv):
```bash
export REDIS_HOST=10.0.0.5
export REDIS_PORT=6380
python3 start.py
```

### redis_monitor.py

| Переменная | По умолчанию | Описание |
|-----------|-------------|---------|
| `MONITOR_INTERVAL` | `10` | Интервал снапшотов в секундах |
| `STALE_THRESHOLD_S` | `60` | Порог "устаревшего" тикера в секундах |

### Параметры соединения с Redis (в коде, common.py)

```python
socket_connect_timeout  = 5     # секунд на открытие TCP
socket_keepalive        = True  # TCP keepalive
health_check_interval   = 30    # секунд между health-check пингами
decode_responses        = True  # ключи/значения как str, не bytes
```

Retry при старте: 5 попыток, backoff `1s → 2s → 4s → 8s → 16s`.

---

## 10. Система логирования

### Где хранятся логи

```
logs/
├── binance_spot/
│   ├── 20240301_120000-20240302_120000/   ← завершённый чанк (24ч)
│   │   └── binance_spot.log
│   └── 20240302_120000-ongoing/           ← текущий активный лог
│       └── binance_spot.log
├── binance_futures/
├── bybit_spot/
├── bybit_futures/
├── okx_spot/
├── okx_futures/
├── gate_spot/
├── gate_futures/
└── redis_monitor/
    └── 20240302_120000-ongoing/
        └── redis_monitor.log
```

### Ротация логов

| Параметр | Значение |
|---------|---------|
| Длина чанка | 24 часа (`LOG_CHUNK_SECONDS = 86400`) |
| Хранить чанков | 2 последних (`LOG_MAX_CHUNKS = 2`) |
| Итого хранится | ~48 часов |

При ротации:
1. Папка `...-ongoing` переименовывается в `...-<end_time>`
2. Создаётся новая `<new_time>-ongoing`
3. Если чанков стало > 2 — самый старый удаляется

### Размер логов

```
Один снапшот ≈ 40–60 строк × 50 байт ≈ 2–3 KB каждые 10 секунд
8 скриптов × 2 чанка × 24ч × 8640 снапшотов × ~2.5 KB ≈ ~350 MB
```

Если диск ограничен — уменьшить `LOG_MAX_CHUNKS` в `common.py`.

### Чтение логов в реальном времени

```bash
# Следить за конкретным сборщиком
tail -f logs/binance_spot/$(ls -t logs/binance_spot/ | head -1)/binance_spot.log

# Следить за мониторингом Redis
tail -f logs/redis_monitor/$(ls -t logs/redis_monitor/ | head -1)/redis_monitor.log

# Все ошибки за сегодня
grep -r "Ошибка\|Error\|DEAD\|error" logs/

# Снапшоты задержек по всем биржам
grep "Internal\|proc" logs/*/$(ls -t logs/binance_spot/ | head -1)/*.log
```

### Очистка логов вручную

```bash
rm -rf logs/
mkdir logs/
```

---

## 11. Снапшоты и метрики

### Снапшоты сборщиков (каждые 10 секунд)

Каждый из 8 скриптов выводит свой снапшот в stdout и лог:

```
==================================================================
SNAPSHOT  binance_spot         2024-03-01 12:00:10.451 UTC
==================================================================
Uptime: 0h 00m 10s

СОЕДИНЕНИЯ (1/1 активных)
  Conn-01: [✓ ALIVE]  msgs=1,523  last=152 msg/10s  url=wss://stream.binance.com:9443/...

ПРОПУСКНАЯ СПОСОБНОСТЬ
  Сообщений:           1,523  |     152.3 msg/s
  Ticker writes:       1,480  |     148.0 wr/s
  History ent.:        1,480  |     148.0 ent/s

СИМВОЛЫ
  Отслеживается:  3
  Активных за 10s: 3

ЗАДЕРЖКИ
  RTT (exchange→recv)  — нет данных (биржа не даёт timestamp)
  Internal (recv→Redis)[  152 сэмплов]:  min=0.30  avg=0.82  p95=1.90  max=3.10 ms

REDIS HISTORY ЧАНКИ
  Chunk #1: запущен 10s назад, осталось ~1190s
  Активные чанки: [1]
    Чанк 1: 2024-03-01 00:00:00 UTC → (текущий)
==================================================================
```

### Снапшот run_all.py (каждые 10 секунд)

```
================================================================
SNAPSHOT  run_all  2024-03-01 12:00:10 UTC
Manager uptime: 1h 23m 45s  |  Processes: 8/8 running
================================================================
  ✓ binance_spot           PID=12345  uptime=1h 20m 30s
  ✓ binance_futures        PID=12346  uptime=1h 20m 30s
  ...
================================================================
```

### Снапшот start.py (каждые 30 секунд)

```
==============================================================
SNAPSHOT  start.py          2024-03-01 12:00:40 UTC
Uptime: 0h 00m 40s  |  Активных: 2/2
==============================================================
  ✓  market_data/run_all.py        PID=12344   uptime=0h 00m 40s
  ✓  market_data/redis_monitor.py  PID=12350   uptime=0h 00m 37s
==============================================================
```

### Расшифровка метрик сборщика

| Метрика | Описание | Норма |
|--------|---------|-------|
| `msg/s` | Сообщений от биржи в секунду | зависит от числа символов |
| `wr/s` | Записей тикеров в Redis в секунду | ≈ msg/s |
| `ent/s` | Записей в историю в секунду | ≈ msg/s |
| `RTT` | Задержка биржа→скрипт (только Bybit, OKX, Gate) | <50ms |
| `Internal p95` | Задержка получение→запись в Redis | <5ms |
| `Активных за 10s` | Символов, получивших update | = символов_всего |

---

## 12. Диагностика Redis вручную

### Проверить что данные поступают

```bash
# Текущий тикер
redis-cli HGETALL md:binance:spot:BTCUSDT

# Следить за обновлениями (timestamp должен меняться)
watch -n 1 'redis-cli HGET md:binance:spot:BTCUSDT ts'

# Посмотреть историю
redis-cli LRANGE md:hist:binance:spot:BTCUSDT:1 -5 -1

# Длина истории
redis-cli LLEN md:hist:binance:spot:BTCUSDT:1
```

### Посчитать ключи

```bash
# Все md:* ключи
redis-cli --scan --pattern 'md:*' | wc -l

# Только тикеры (без истории и конфига)
redis-cli --scan --pattern 'md:*' | grep -v 'hist\|chunks' | wc -l

# Только история
redis-cli --scan --pattern 'md:hist:*' | wc -l
```

### Состояние чанков

```bash
# Все чанки
redis-cli HGETALL md:chunks:config

# Конкретная биржа
redis-cli HGET md:chunks:config binance:spot
```

### Проверить производительность Redis

```bash
# Пинг-латентность
redis-cli --latency

# Статистика сервера
redis-cli INFO stats

# Использование памяти
redis-cli INFO memory

# Общий INFO
redis-cli INFO
```

### Нагрузочные тесты

```bash
python3 market_data/tests/load_test.py
```

Нормативы:

| Тест | Норма | Проблема |
|------|-------|---------|
| Pipeline write (500 символов) | < 5ms | > 20ms |
| Read HGETALL (500 символов) | < 10ms | > 50ms |
| History RPUSH (1000 записей) | < 10ms | > 30ms |
| SCAN md:* (10k ключей) | < 50ms | > 200ms |
| Concurrent write (8×500) | < 15ms/batch | > 50ms/batch |

---

## 13. Производительность и ограничения

### Ожидаемые показатели

| Параметр | Значение |
|---------|---------|
| Скриптов | 8 (4 биржи × 2 рынка) |
| Суммарный throughput | 500–3000 msg/s |
| Задержка recv→Redis (p95) | 0.3–5ms |
| Flush тикеров | каждые 50ms (батч HSET) |
| Flush истории | каждые 1s (батч RPUSH) |
| История на символ | ~1.5–2 часа (5 чанков × 20 минут) |
| Хранение логов | ~48 часов (2 чанка × 24 часа) |

### Известные боттлнеки

#### 1. Redis pipeline при высоком числе символов

**Симптом:** `proc latency p95 > 10ms` в снапшоте, `t.writes/s` < `messages/s`.

**Причина:** Каждые 50ms выполняется pipeline с N командами HSET. При 500+ символах
и высоком tick rate пайплайн может занимать > 50ms.

**Решение:**
- Перенести Redis на localhost (если сейчас сетевой)
- Проверить: `redis-cli --latency`
- Увеличить `TICKER_FLUSH_INTERVAL` в `common.py` (например до 100ms)

#### 2. Реконнект Bybit/Gate — задержка переподписки

**Симптом:** `reconnects > 0`, пропуски в истории `md:hist:bybit:*`.

**Причина:** При реконнекте Bybit и Gate заново отправляют сотни subscribe-сообщений
батчами. Пока идёт подписка — данные не поступают.

**Решение:** Мониторить `reconnects_total` в снапшоте. При систематических
дисконнектах — проверить стабильность сети до биржи.

#### 3. Конвертация символов OKX/Gate — молчаливый пропуск

**Симптом:** `symbols_tracked` < числа строк в subscribe-файле.

**Причина:** OKX и Gate пропускают символы не на USDT/USDC (нет конвертации).

**Решение:** Держать в subscribe-файлах только USDT/USDC пары.

#### 4. Binance — нет RTT-метрики

**Причина:** Binance `bookTicker` не включает exchange timestamp.

**Решение:** Для мониторинга сетевой задержки использовать внешний ping до
`stream.binance.com`.

#### 5. Gate.io — медленный первый старт

**Причина:** Gate требует 50ms паузы между батчами subscribe. При 500 символах
(5 батчей × 50ms = 250ms) первые данные появляются позже других бирж.

**Это нормальное поведение**, документировано.

---

## 14. Устранение неисправностей

### Redis не запускается / недоступен

```
[run_all] ОШИБКА подключения к Redis: ...
```

**Проверить:**
```bash
redis-cli ping           # должен ответить PONG
systemctl status redis   # статус сервиса (Linux)
redis-server --version   # версия
```

**Параметры по умолчанию:** `127.0.0.1:6379` db=0.
Если Redis на другом хосте/порту — задать переменные окружения:
```bash
REDIS_HOST=10.0.0.5 REDIS_PORT=6380 python3 start.py
```

### Файл подписки не найден

```
FileNotFoundError: Subscribe-файл не найден: dictionaries/subscribe/binance/binance_spot.txt
```

**Решение:** Запустить генератор пар:
```bash
python3 dictionaries/main.py
```

### Сборщик постоянно реконнектится

В снапшоте: `Conn-01: [✗ DEAD]  reconnects=15`

**Проверить:**
1. Доступность биржи: `ping stream.binance.com`
2. Блокировку по IP (редко, но бывает): попробовать VPN
3. Правильность symbols в subscribe-файле

### Данные устаревшие (redis_monitor показывает ⚠)

В снапшоте redis_monitor:
```
binance/spot       3    120.5s    140.2s      3 ⚠
```

**Причина:** Сборщик не работает или соединение разорвано.

**Проверить:**
```bash
# Состояние процессов
ps aux | grep python

# Логи сборщика
tail -50 logs/binance_spot/$(ls -t logs/binance_spot/ | head -1)/binance_spot.log
```

### Высокое потребление памяти Python-процессом

```bash
ps aux | grep market_data | awk '{print $6, $11}'
```

**Норма:** < 100MB на процесс.

**Если > 200MB:** возможно зависание history flusher (Redis недоступен → буфер растёт).
Проверить `h.entries/s` в снапшоте — должно быть ≈ `messages/s`.

### Логи не создаются

**Проверить:** права на запись в папку `logs/`:
```bash
ls -la logs/
mkdir -p logs/ && chmod 755 logs/
```

### Один из скриптов не запускается (синтаксис / импорт)

```bash
python3 market_data/binance_spot.py  # запустить вручную для просмотра ошибки
```

### Проверить что история корректная (нет дыр)

```bash
# Получить последние 5 записей истории
redis-cli LRANGE md:hist:binance:spot:BTCUSDT:1 -5 -1

# Timestamps должны идти строго по возрастанию с интервалом ~1 tick
# Пример нормальной истории:
# "1710000010.123,45000.12,45000.13"
# "1710000010.456,45000.14,45000.15"
# "1710000010.789,45000.12,45000.13"
```

---

## 15. Справочник по всем файлам

### Корневой уровень

| Файл | Назначение |
|------|-----------|
| `start.py` | **Точка входа.** Запускает run_all.py и redis_monitor.py, следит за ними, перезапускает при падении |

### market_data/

| Файл | Назначение |
|------|-----------|
| `run_all.py` | Запускает 8 сборщиков как subprocess, следит, автоперезапуск. Flush Redis при старте |
| `common.py` | Общие утилиты: `create_redis()`, `LogManager`, `ChunkManager`, `SnapshotLogger`, `Stats`, `ConnectionStats` |
| `redis_monitor.py` | Мониторинг Redis: health, память, клиенты, ops/s, актуальность тикеров, чанки |
| `binance_spot.py` | Сборщик Binance SPOT через `{sym}@bookTicker` |
| `binance_futures.py` | Сборщик Binance FUTURES через `{sym}@bookTicker` |
| `bybit_spot.py` | Сборщик Bybit SPOT через `orderbook.1.{SYM}` |
| `bybit_futures.py` | Сборщик Bybit FUTURES через `orderbook.1.{SYM}` |
| `okx_spot.py` | Сборщик OKX SPOT через `tickers` (инструменты `BTC-USDT`) |
| `okx_futures.py` | Сборщик OKX FUTURES через `tickers` (инструменты `BTC-USDT-SWAP`) |
| `gate_spot.py` | Сборщик Gate.io SPOT через `spot.book_ticker` |
| `gate_futures.py` | Сборщик Gate.io FUTURES через `futures.book_ticker` |
| `requirements.txt` | `redis>=7.0.0`, `websockets>=12.0` |
| `tests/load_test.py` | Нагрузочные тесты производительности Redis |

### market_data/docs/

| Файл | Содержимое |
|------|-----------|
| `README.md` | Архитектура market_data, схема потока данных |
| `TECH_SPEC.md` | Все константы, Redis-ключи, форматы WS-сообщений, структуры данных |
| `LOGS.md` | Детальное описание системы логирования |
| `OVERVIEW.md` | Боттлнеки, нагрузочные тесты, мониторинг в продакшне |
| `USER_GUIDE.md` | Гайд по каждому скрипту, параметры, особенности |

### dictionaries/

| Файл | Назначение |
|------|-----------|
| `main.py` | Оркестратор: REST + WS-валидация → combination → subscribe |
| `README.md` | Документация модуля dictionaries |
| `binance/binance_pairs.py` | REST API Binance: получение spot + futures пар |
| `binance/binance_ws.py` | WS-валидация активности пар Binance |
| `bybit/bybit_pairs.py` | REST API Bybit (cursor-пагинация) |
| `bybit/bybit_ws.py` | WS-валидация Bybit (кастомный ping) |
| `okx/okx_pairs.py` | REST API OKX + нормализация `BTC-USDT` → `BTCUSDT` |
| `okx/okx_ws.py` | WS-валидация OKX (300 instId/соединение) |
| `gate/gate_pairs.py` | REST API Gate.io spot + futures USDT |
| `gate/gate_ws.py` | WS-валидация Gate.io (батч 100, ping JSON) |
| `combination/*.txt` | Генерируется: 12 пересечений spot_A ∩ futures_B |
| `subscribe/{ex}/{ex}_{mkt}.txt` | Генерируется: 8 файлов подписки для сборщиков |

### logs/

| Папка | Содержимое |
|------|-----------|
| `logs/{script_name}/{range}-ongoing/` | Активный лог (текущий чанк 24ч) |
| `logs/{script_name}/{range1}-{range2}/` | Завершённый лог |

Имена диапазонов: `YYYYMMDD_HHMMSS`.
