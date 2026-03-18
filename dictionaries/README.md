# dictionaries

Модуль генерации и хранения списков торговых пар для 4 бирж.
Запускается вручную — результат записывается в `subscribe/` и `combination/`, откуда коллекторы `market-data/` читают символы при старте.

## Содержание

- [Структура](#структура)
- [Быстрый старт](#быстрый-старт)
- [Как работает](#как-работает)
- [Биржи](#биржи)
- [Пересечения (combination)](#пересечения-combination)
- [Файлы подписки (subscribe)](#файлы-подписки-subscribe)
- [Зависимости](#зависимости)

---

## Структура

```
dictionaries/
├── main.py                        # Оркестратор — запускает всё и печатает отчёт
│
├── binance/
│   ├── binance_pairs.py           # REST API → spot + futures пары
│   └── binance_ws.py              # WS-валидация активности пар (bookTicker)
│
├── bybit/
│   ├── bybit_pairs.py             # REST API → spot + futures пары (cursor-пагинация)
│   └── bybit_ws.py                # WS-валидация (orderbook.1, кастомный пинг)
│
├── okx/
│   ├── okx_pairs.py               # REST API → SPOT + SWAP пары, нормализация BTC-USDT→BTCUSDT
│   └── okx_ws.py                  # WS-валидация (tickers, 300 instId/соединение)
│
├── gate/
│   ├── gate_pairs.py              # REST API → spot + futures USDT-маржинальные
│   └── gate_ws.py                 # WS-валидация (spot/futures.book_ticker, батч 100)
│
├── combination/                   # Генерируется автоматически
│   ├── binance_spot_bybit_futures.txt
│   ├── bybit_spot_binance_futures.txt
│   ├── binance_spot_okx_futures.txt
│   ├── okx_spot_binance_futures.txt
│   ├── bybit_spot_okx_futures.txt
│   ├── okx_spot_bybit_futures.txt
│   ├── binance_spot_gate_futures.txt
│   ├── gate_spot_binance_futures.txt
│   ├── bybit_spot_gate_futures.txt
│   ├── gate_spot_bybit_futures.txt
│   ├── okx_spot_gate_futures.txt
│   └── gate_spot_okx_futures.txt
│
└── subscribe/                     # Генерируется автоматически — читают коллекторы
    ├── binance/
    │   ├── binance_spot.txt
    │   └── binance_futures.txt
    ├── bybit/
    │   ├── bybit_spot.txt
    │   └── bybit_futures.txt
    ├── okx/
    │   ├── okx_spot.txt
    │   └── okx_futures.txt
    └── gate/
        ├── gate_spot.txt
        └── gate_futures.txt
```

---

## Быстрый старт

```bash
# Из корня репозитория
cd dictionaries
pip install websockets
python3 main.py
```

Скрипт работает ~10 минут (60 сек WS-валидация × 4 биржи параллельно = ~2 мин).
По завершении печатает итоговый отчёт и обновляет все файлы в `combination/` и `subscribe/`.

---

## Как работает

```
Для каждой биржи:
  1. REST API → полный список USDT/USDC пар (spot + futures)
  2. WebSocket → подписка на все пары, 60 сек наблюдение
  3. Только пары, ответившие хоть раз → список активных пар

После всех бирж:
  4. Пересечения (12 комбинаций) → combination/*.txt
  5. Subscribe-файлы → subscribe/{exchange}/*.txt

Логика subscribe:
  Каждое ключевое слово в имени combination-файла → свой subscribe-файл.
  binance_spot_gate_futures.txt → binance_spot.txt + gate_futures.txt
  Дубли убираются через set().
```

### Промежуточные данные

Каждый модуль сохраняет промежуточные результаты в свою папку `data/`:

| Файл | Содержимое |
|------|-----------|
| `binance/data/binance_spot.txt` | Все пары от REST API |
| `binance/data/binance_spot_active.txt` | Только прошедшие WS-валидацию |
| `okx/data/okx_spot_native.txt` | Нативный OKX-формат (BTC-USDT) для WS-подписки |
| `okx/data/okx_spot.txt` | Нормализованный формат (BTCUSDT) для пересечений |

---

## Биржи

### Binance

| Параметр | Spot | Futures |
|----------|------|---------|
| REST API | `api.binance.com/api/v3/exchangeInfo` | `fapi.binance.com/fapi/v1/exchangeInfo` |
| Фильтр | `status=TRADING`, `quoteAsset∈{USDT,USDC}` | — |
| WS URL | `wss://stream.binance.com:9443/stream` | `wss://fstream.binance.com/stream` |
| WS канал | `{symbol}@bookTicker` | `{symbol}@bookTicker` |
| Чанк WS | 300 символов/соединение | 300 символов/соединение |

### Bybit

| Параметр | Spot | Futures |
|----------|------|---------|
| REST API | `api.bybit.com/v5/market/instruments-info?category=spot` | `?category=linear` |
| Фильтр | `status=Trading`, `quoteCoin∈{USDT,USDC}` | — |
| WS URL | `wss://stream.bybit.com/v5/public/spot` | `wss://stream.bybit.com/v5/public/linear` |
| WS канал | `orderbook.1.{SYMBOL}` | `orderbook.1.{SYMBOL}` |
| Батч подписки | 10 символов/запрос | 200 символов/запрос |
| Пинг | `{"op":"ping"}` | `{"op":"ping"}` |

### OKX

| Параметр | Spot | Futures |
|----------|------|---------|
| REST API | `okx.com/api/v5/public/instruments?instType=SPOT` | `?instType=SWAP` |
| Фильтр | `state=live`, `quoteCcy∈{USDT,USDC}` | `state=live`, `settleCcy∈{USDT,USDC}` |
| Формат символов | `BTC-USDT` → нормализуется в `BTCUSDT` | `BTC-USDT-SWAP` → `BTCUSDT` |
| WS URL | `wss://ws.okx.com:8443/ws/v5/public` | — (тот же) |
| WS канал | `tickers` с `instId=BTC-USDT` | `tickers` с `instId=BTC-USDT-SWAP` |
| Чанк WS | 300 instId/соединение | — |
| Пинг | строка `"ping"` | — |

### Gate.io

| Параметр | Spot | Futures |
|----------|------|---------|
| REST API | `api.gateio.ws/api/v4/spot/currency_pairs` | `/api/v4/futures/usdt/contracts` |
| Фильтр | `trade_status=tradable`, `quote∈{USDT,USDC}` | `in_delisting=false`, `quote∈{USDT,USDC}` |
| Формат символов | `BTC_USDT` → `BTCUSDT` | `BTC_USDT` → `BTCUSDT` |
| WS URL | `wss://api.gateio.ws/ws/v4/` | `wss://fx-ws.gateio.ws/v4/ws/usdt` |
| WS канал | `spot.book_ticker` | `futures.book_ticker` |
| Батч подписки | 100 символов/запрос | 100 символов/запрос |
| Пинг | `{"channel":"spot.ping"}` | `{"channel":"futures.ping"}` |

---

## Пересечения (combination)

12 файлов — все возможные пары из 4 бирж × 2 рынка:

```
Обозначение:  A_spot_B_futures = пары, торгующиеся на A-Spot И B-Futures

binance_spot ∩ bybit_futures    →  binance_spot_bybit_futures.txt
bybit_spot   ∩ binance_futures  →  bybit_spot_binance_futures.txt
binance_spot ∩ okx_futures      →  binance_spot_okx_futures.txt
okx_spot     ∩ binance_futures  →  okx_spot_binance_futures.txt
bybit_spot   ∩ okx_futures      →  bybit_spot_okx_futures.txt
okx_spot     ∩ bybit_futures    →  okx_spot_bybit_futures.txt
binance_spot ∩ gate_futures     →  binance_spot_gate_futures.txt
gate_spot    ∩ binance_futures  →  gate_spot_binance_futures.txt
bybit_spot   ∩ gate_futures     →  bybit_spot_gate_futures.txt
gate_spot    ∩ bybit_futures    →  gate_spot_bybit_futures.txt
okx_spot     ∩ gate_futures     →  okx_spot_gate_futures.txt
gate_spot    ∩ okx_futures      →  gate_spot_okx_futures.txt
```

Каждый файл — список символов (один на строку, BTCUSDT-формат), отсортированный по алфавиту.

---

## Файлы подписки (subscribe)

8 файлов — по одному на каждый рынок каждой биржи.

Формируются из combination-файлов по ключевым словам в имени:

```
Ключевое слово в имени файла  →  subscribe-файл
─────────────────────────────────────────────────────────────
binance_spot     →  subscribe/binance/binance_spot.txt
binance_futures  →  subscribe/binance/binance_futures.txt
bybit_spot       →  subscribe/bybit/bybit_spot.txt
bybit_futures    →  subscribe/bybit/bybit_futures.txt
okx_spot         →  subscribe/okx/okx_spot.txt
okx_futures      →  subscribe/okx/okx_futures.txt
gate_spot        →  subscribe/gate/gate_spot.txt
gate_futures     →  subscribe/gate/gate_futures.txt
```

Один combination-файл вносит пары сразу в два subscribe-файла. Например,
`binance_spot_gate_futures.txt` → `binance_spot.txt` + `gate_futures.txt`.
Дубли между файлами устраняются автоматически.

Эти файлы читают коллекторы `market-data/` при запуске:

```bash
# Проверить сколько пар загружено
wc -l subscribe/binance/binance_spot.txt
wc -l subscribe/bybit/bybit_futures.txt
```

---

## Зависимости

```bash
pip install websockets
```

Стандартная библиотека Python используется для REST API (`urllib.request`).
`websockets` — для WS-валидации.
Python 3.11+.
