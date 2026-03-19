# dictionaries — Генерация списков торговых пар

Модуль собирает активные торговые пары с 4 бирж через REST API и валидирует их через WebSocket. Результат записывается в `subscribe/` и `combination/` — эти файлы читает система при старте.

Запускается **вручную или по расписанию** (рекомендуется раз в сутки, состав активных пар меняется).

---

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
│
├── main.py                        # Оркестратор — запускает всё и печатает отчёт
│
├── binance/
│   ├── binance_pairs.py           # REST API → spot + futures пары
│   ├── binance_ws.py              # WS-валидация активности пар (bookTicker)
│   └── data/                      # Промежуточные данные (авто)
│       ├── binance_spot.txt       # Все пары от REST API
│       └── binance_spot_active.txt # Прошедшие WS-валидацию
│
├── bybit/
│   ├── bybit_pairs.py             # REST API → spot + futures пары (cursor-пагинация)
│   ├── bybit_ws.py                # WS-валидация (orderbook.1, кастомный пинг)
│   └── data/
│
├── okx/
│   ├── okx_pairs.py               # REST API → SPOT + SWAP пары
│   ├── okx_ws.py                  # WS-валидация (tickers, 300 instId/соединение)
│   └── data/
│       ├── okx_spot_native.txt    # Нативный формат OKX (BTC-USDT)
│       └── okx_spot.txt           # Нормализованный (BTCUSDT)
│
├── gate/
│   ├── gate_pairs.py              # REST API → spot + futures USDT-маржинальные
│   ├── gate_ws.py                 # WS-валидация (spot/futures.book_ticker, батч 100)
│   └── data/
│
├── combination/                   # 12 файлов пересечений (авто, читает signal_scanner)
│   ├── binance_spot_bybit_futures.txt
│   ├── bybit_spot_binance_futures.txt
│   └── ...
│
└── subscribe/                     # 8 файлов подписки (авто, читают коллекторы)
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
pip install websockets
python3 dictionaries/main.py
```

Скрипт работает ~2 минуты (60с WS-валидация × 4 биржи параллельно ≈ ~60с суммарно + REST API). По завершении печатает отчёт и обновляет все файлы в `combination/` и `subscribe/`.

После этого можно запускать всю систему:
```bash
python3 run.py
```

---

## Как работает

Выполнение разбито на 5 фаз:

```
Фаза 1 — REST API (параллельно, 4 потока):
  Binance, Bybit, OKX, Gate → полные списки USDT/USDC пар

Фаза 2 — WS-валидация (параллельно, asyncio.gather):
  60 секунд слушаем WebSocket каждой биржи.
  Фиксируем пары, по которым пришёл хотя бы 1 ответ.
  Итог: списки *активных* пар.

Фаза 3 — Построение пересечений (combination/):
  Все комбинации «spot_A ∩ futures_B» → 12 .txt-файлов

Фаза 4 — Построение файлов подписки (subscribe/):
  Из combination-файлов агрегируются subscribe-файлы без дублей

Фаза 5 — Отчёт в stdout
```

### Промежуточные данные

Каждый модуль сохраняет промежуточные результаты в `data/`:

| Файл | Содержимое |
|------|-----------|
| `binance/data/binance_spot.txt` | Все пары от REST API |
| `binance/data/binance_spot_active.txt` | Прошедшие WS-валидацию |
| `okx/data/okx_spot_native.txt` | Нативный OKX-формат (BTC-USDT) для WS-подписки |
| `okx/data/okx_spot.txt` | Нормализованный (BTCUSDT) для пересечений |

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
| Пагинация | cursor-based | cursor-based |

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

12 файлов — все возможные пары «spot биржи A × futures биржи B»:

```
Обозначение: A_spot_B_futures = пары, торгующиеся и на A-Spot, и на B-Futures

binance_spot ∩ bybit_futures    →  combination/binance_spot_bybit_futures.txt
bybit_spot   ∩ binance_futures  →  combination/bybit_spot_binance_futures.txt
binance_spot ∩ okx_futures      →  combination/binance_spot_okx_futures.txt
okx_spot     ∩ binance_futures  →  combination/okx_spot_binance_futures.txt
bybit_spot   ∩ okx_futures      →  combination/bybit_spot_okx_futures.txt
okx_spot     ∩ bybit_futures    →  combination/okx_spot_bybit_futures.txt
binance_spot ∩ gate_futures     →  combination/binance_spot_gate_futures.txt
gate_spot    ∩ binance_futures  →  combination/gate_spot_binance_futures.txt
bybit_spot   ∩ gate_futures     →  combination/bybit_spot_gate_futures.txt
gate_spot    ∩ bybit_futures    →  combination/gate_spot_bybit_futures.txt
okx_spot     ∩ gate_futures     →  combination/okx_spot_gate_futures.txt
gate_spot    ∩ okx_futures      →  combination/gate_spot_okx_futures.txt
```

Формат файла: один символ на строку (BTCUSDT), отсортировано по алфавиту.

Эти файлы читает `signal_scanner.py` для определения какие пары проверять.

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

Один combination-файл вносит пары сразу в два subscribe-файла. Например, `binance_spot_gate_futures.txt` → `binance_spot.txt` + `gate_futures.txt`. Дубли устраняются автоматически.

Эти файлы читают коллекторы `market_data/` при запуске:

```bash
# Проверить сколько пар загружено
wc -l dictionaries/subscribe/binance/binance_spot.txt
wc -l dictionaries/subscribe/bybit/bybit_futures.txt
```

---

## Зависимости

```bash
pip install websockets
```

Стандартная библиотека Python используется для REST API (`urllib.request`). `websockets` — для WS-валидации. Python 3.11+.
