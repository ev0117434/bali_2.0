# Market Data — Общая архитектура

## Назначение

Модуль `market_data` — это набор из восьми асинхронных WebSocket-сборщиков рыночных данных (best bid/ask) с четырёх криптобирж в режиме реального времени. Данные складываются в Redis и используются вышестоящими сервисами.

---

## Структура файлов

```
market_data/
├── common.py              # Общие классы и утилиты
├── run_all.py             # Менеджер процессов (запуск всех 8 скриптов)
│
├── binance_spot.py        # Binance SPOT    wss://stream.binance.com:9443
├── binance_futures.py     # Binance FUTURES wss://fstream.binance.com
├── bybit_spot.py          # Bybit SPOT      wss://stream.bybit.com/v5/public/spot
├── bybit_futures.py       # Bybit FUTURES   wss://stream.bybit.com/v5/public/linear
├── okx_spot.py            # OKX SPOT        wss://ws.okx.com:8443/ws/v5/public
├── okx_futures.py         # OKX FUTURES     wss://ws.okx.com:8443/ws/v5/public
├── gate_spot.py           # Gate SPOT       wss://api.gateio.ws/ws/v4/
├── gate_futures.py        # Gate FUTURES    wss://fx-ws.gateio.ws/v4/ws/usdt
│
├── docs/                  # Документация
│   ├── README.md          # (этот файл) Архитектура
│   ├── USER_GUIDE.md      # Гайд по каждому скрипту
│   ├── TECH_SPEC.md       # Технические детали: ключи, задержки, переменные
│   ├── LOGS.md            # Система логирования
│   └── OVERVIEW.md        # Краткий обзор + боттлнеки + нагрузочные тесты
│
└── tests/
    └── load_test.py       # Нагрузочные тесты Redis
```

---

## Высокоуровневая схема

```
┌────────────────────────────────────────────────────────┐
│                        run_all.py                      │
│   (менеджер процессов, 8 subprocess + автоперезапуск)  │
└──────────┬─────────────────────────────────────────────┘
           │ spawn subprocess ×8
           ▼
┌────────────────────────────────────────────────────────┐
│  Exchange Script (e.g. binance_spot.py)                │
│                                                        │
│  ┌─────────────┐   ┌────────────────┐  ┌───────────┐  │
│  │  WS Worker  │──▶│ _ticker_buf    │──▶│  Redis    │  │
│  │  (×N conn.) │   │ (50ms flush)   │  │  HSET     │  │
│  │             │   ├────────────────┤  │  md:...   │  │
│  │             │──▶│ _history_buf   │──▶│  RPUSH    │  │
│  └─────────────┘   │ (1s flush)     │  │  md:hist  │  │
│                    └────────────────┘  └───────────┘  │
│  ┌─────────────┐                                       │
│  │  Snapshot   │  → stdout + log файл (каждые 10с)    │
│  └─────────────┘                                       │
└────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────┐
│        Redis         │
│                      │
│  md:{ex}:{mk}:{sym}  │  ← текущие тикеры (Hash)
│  md:hist:...:N       │  ← история 20-мин чанки (List)
│  md:chunks:config    │  ← метаданные чанков (Hash)
└──────────────────────┘
```

---

## Поток данных

```
Exchange WS
    │
    │  raw JSON message
    ▼
_ws_worker (asyncio task)
    │
    │  parse → normalize symbol
    │  extract bid/ask/ts
    ▼
_ticker_buf  [dict: symbol → (bid, ask, local_ts)]
_history_buf [dict: symbol → [(ts, bid, ask), ...]]
    │
    │  каждые 50ms (ticker) / 1s (history)
    ▼
Redis Pipeline
    │  HSET md:{exchange}:{market}:{symbol} bid ask ts
    │  RPUSH md:hist:{exchange}:{market}:{symbol}:{chunk_id}
    ▼
Redis
```

---

## Паттерны соединений

| Биржа           | Символов/соединение | Соединений (est.) | Канал              |
|-----------------|--------------------:|------------------:|--------------------|
| Binance Spot    | 300                 | 1–2               | `{sym}@bookTicker` |
| Binance Futures | 300                 | 1–2               | `{sym}@bookTicker` |
| Bybit Spot      | 200                 | 2–3               | `orderbook.1.{SYM}`|
| Bybit Futures   | 200                 | 2–3               | `orderbook.1.{SYM}`|
| OKX Spot        | 300                 | 1–2               | `tickers`          |
| OKX Futures     | 300                 | 1–2               | `tickers` (SWAP)   |
| Gate Spot       | 500                 | 1                 | `spot.book_ticker` |
| Gate Futures    | 500                 | 1                 | `futures.book_ticker`|

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
Пример поля: "binance:spot" → {"current_chunk_id":3,"active_chunks":[1,2,3],...}
```

---

## Жизненный цикл запуска

```
python3 market_data/run_all.py
     │
     ├─ 1. Flush Redis: удалить все md:* ключи (всегда)
     ├─ 2. Спросить: очистить logs/? [y/N]
     ├─ 3. Запустить 8 subprocess (по одному на скрипт)
     │
     │  Каждый subprocess:
     │     ├─ Загрузить список символов из dictionaries/subscribe/
     │     ├─ Подключиться к Redis
     │     ├─ Инициализировать ChunkManager
     │     ├─ Запустить N WS-воркеров
     │     ├─ Запустить ticker/history flusher
     │     └─ Запустить SnapshotLogger (каждые 10с)
     │
     └─ При SIGINT/SIGTERM: graceful shutdown всех процессов
```

---

## Зависимости

```
redis>=7.0.0      # asyncio-клиент Redis
websockets>=12.0  # WebSocket клиент
```

Установка:
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
# Из корня проекта
python3 market_data/run_all.py

# Один скрипт отдельно (для отладки)
python3 market_data/binance_spot.py
```
