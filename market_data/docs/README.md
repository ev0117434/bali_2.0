# Market Data — Архитектура

## Назначение

Модуль `market_data` — набор из восьми асинхронных WebSocket-коллекторов best bid/ask с четырёх криптобирж в режиме реального времени. Данные складываются в Redis и читаются `signal_scanner.py`.

---

## Структура файлов

```
market_data/
├── common.py              # Общие классы и утилиты (Stats, LogManager, ChunkManager, ...)
├── run_all.py             # Менеджер 8 коллекторов (без сканера и мониторинга)
├── redis_monitor.py       # Мониторинг метрик Redis
│
├── binance_spot.py        # Binance SPOT      wss://stream.binance.com:9443
├── binance_futures.py     # Binance FUTURES   wss://fstream.binance.com
├── bybit_spot.py          # Bybit SPOT        wss://stream.bybit.com/v5/public/spot
├── bybit_futures.py       # Bybit FUTURES     wss://stream.bybit.com/v5/public/linear
├── okx_spot.py            # OKX SPOT          wss://ws.okx.com:8443/ws/v5/public
├── okx_futures.py         # OKX FUTURES       wss://ws.okx.com:8443/ws/v5/public
├── gate_spot.py           # Gate SPOT         wss://api.gateio.ws/ws/v4/
├── gate_futures.py        # Gate FUTURES      wss://fx-ws.gateio.ws/v4/ws/usdt
│
├── requirements.txt       # redis>=7.0.0, websockets>=12.0
│
├── docs/
│   ├── README.md          # (этот файл) Архитектура
│   ├── USER_GUIDE.md      # Руководство пользователя
│   ├── TECH_SPEC.md       # Технические детали: константы, ключи, форматы
│   ├── LOGS.md            # Система логирования
│   └── OVERVIEW.md        # Обзор, боттлнеки, нагрузочные тесты
│
└── tests/
    └── load_test.py       # Нагрузочные тесты Redis
```

---

## Высокоуровневая схема

```
┌─────────────────────────────────────────────────────────────┐
│                          run.py                             │
│   (10 subprocess: 8 коллекторов + scanner + monitor)        │
└──────────┬──────────────────────────────────────────────────┘
           │ spawn subprocess ×10
           ▼
┌─────────────────────────────────────────────────────────────┐
│  Exchange Script (e.g. binance_spot.py)                     │
│                                                             │
│  ┌─────────────┐   ┌────────────────┐   ┌───────────────┐  │
│  │  WS Worker  │──▶│ _ticker_buf    │──▶│  Redis HSET   │  │
│  │  (×N conn.) │   │ (flush 50ms)   │   │  md:...       │  │
│  │             │──▶│ _history_buf   │──▶│  Redis RPUSH  │  │
│  └─────────────┘   │ (flush 1s)     │   │  md:hist:...  │  │
│                    └────────────────┘   └───────────────┘  │
│  ┌─────────────┐                                            │
│  │  Snapshot   │  → stdout + log (каждые 10с)              │
│  └─────────────┘                                           │
└─────────────────────────────────────────────────────────────┘
           │
           ▼
┌────────────────────────┐    ┌──────────────────────────────┐
│        Redis           │    │   signal_scanner.py          │
│                        │◀───│   (читает тикеры через       │
│  md:{ex}:{mk}:{sym}    │    │    pipeline, пишет сигналы   │
│  md:hist:...:N         │    │    в signal/signals.csv)     │
│  md:chunks:config      │    └──────────────────────────────┘
└────────────────────────┘
           ▲
           │ метрики
┌──────────────────────────────┐
│   redis_monitor.py           │
│   (PING, INFO, LATENCY,      │
│    KEYSPACE → logs/)         │
└──────────────────────────────┘
```

---

## Поток данных

```
Exchange WS
    │  raw JSON
    ▼
_ws_worker (asyncio task)
    │  parse → normalize symbol → extract bid/ask/ts
    ▼
_ticker_buf  [symbol → (bid, ask, local_ts)]       ← только последнее значение
_history_buf [symbol → [(ts, bid, ask), ...]]      ← накапливается
    │
    │  каждые 50ms (ticker) / 1s (history)
    ▼
Redis Pipeline
    │  HSET  md:{exchange}:{market}:{symbol}  bid ask ts
    │  RPUSH md:hist:{exchange}:{market}:{symbol}:{chunk_id}
    ▼
Redis
```

---

## Паттерны соединений

| Биржа           | Символов/соединение | Соединений (est.) | Канал                  |
|-----------------|--------------------:|------------------:|------------------------|
| Binance Spot    | 300                 | 1–2               | `{sym}@bookTicker`     |
| Binance Futures | 300                 | 1–2               | `{sym}@bookTicker`     |
| Bybit Spot      | 200                 | 2–3               | `orderbook.1.{SYM}`    |
| Bybit Futures   | 200                 | 2–3               | `orderbook.1.{SYM}`    |
| OKX Spot        | 300                 | 1–2               | `tickers` (instId)     |
| OKX Futures     | 300                 | 1–2               | `tickers` (SWAP)       |
| Gate Spot       | 500                 | 1                 | `spot.book_ticker`     |
| Gate Futures    | 500                 | 1                 | `futures.book_ticker`  |

---

## Хранение в Redis

### Текущий тикер
```
Ключ:   md:{exchange}:{market}:{symbol}
Тип:    Hash
Поля:   bid, ask, ts
Пример: md:binance:spot:BTCUSDT → {bid:"45000.12", ask:"45000.13", ts:"1710000000.123"}
```

### История (20-минутные чанки)
```
Ключ:   md:hist:{exchange}:{market}:{symbol}:{chunk_id}
Тип:    List
Элемент: "{ts:.3f},{bid},{ask}"
Пример: md:hist:bybit:futures:ETHUSDT:3 → ["1710000010.100,3100.50,3100.55", ...]
Хранится: последние 5 чанков ≈ ~1.5–2 часа истории
```

### Метаданные чанков
```
Ключ:   md:chunks:config
Тип:    Hash
Поле:   {exchange}:{market}  → JSON
Пример: "binance:spot" → {"current_chunk_id":3,"active_chunks":[1,2,3],...}
```

---

## Жизненный цикл запуска

```
python3 run.py
     │
     ├─ 1. Flush Redis: удалить все md:* ключи
     ├─ 2. Спросить: очистить logs/? [y/N]
     ├─ 3. Запустить 10 subprocess
     │
     │  Каждый коллектор:
     │     ├─ Загрузить список символов из dictionaries/subscribe/
     │     ├─ Подключиться к Redis
     │     ├─ Инициализировать ChunkManager
     │     ├─ Запустить N WS-воркеров
     │     ├─ Запустить ticker/history flusher
     │     └─ Запустить SnapshotLogger (каждые 10с)
     │
     │  signal_scanner:
     │     ├─ Загрузить пары из dictionaries/combination/
     │     ├─ Подключиться к Redis
     │     └─ Каждые 0.2с: pipeline-опрос → проверка спреда → CSV
     │
     │  redis_monitor:
     │     └─ Каждые 5с: INFO + LATENCY + SCAN → JSON лог
     │
     └─ При SIGINT/SIGTERM: graceful shutdown (5с таймаут + SIGKILL)
```

---

## Зависимости

```
redis>=7.0.0      # asyncio-клиент Redis
websockets>=12.0  # WebSocket клиент
```

```bash
pip install -r market_data/requirements.txt
```

---

## Переменные окружения

| Переменная      | По умолчанию | Описание                  |
|-----------------|:------------:|---------------------------|
| `REDIS_HOST`    | `127.0.0.1`  | Хост Redis                |
| `REDIS_PORT`    | `6379`       | Порт Redis                |
| `REDIS_DB`      | `0`          | Номер базы данных Redis   |
| `REDIS_PASSWORD`| не задан     | Пароль Redis (если нужен) |

---

## Быстрый старт

```bash
# Вся система (рекомендуется)
python3 run.py

# Только 8 коллекторов
python3 market_data/run_all.py

# Один скрипт (отладка)
python3 market_data/binance_spot.py
```

---

## Документация

| Файл                | Содержимое                                        |
|---------------------|---------------------------------------------------|
| `USER_GUIDE.md`     | Запуск, интерпретация снапшотов, отладка          |
| `TECH_SPEC.md`      | Константы, Redis ключи, форматы WS-сообщений      |
| `LOGS.md`           | Структура логов, форматы записей, команды чтения  |
| `OVERVIEW.md`       | Боттлнеки, нагрузочные тесты, мониторинг          |
