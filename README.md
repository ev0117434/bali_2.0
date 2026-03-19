# bali_2.0 — Сканер арбитража spot → futures

Система сбора рыночных данных и поиска арбитражных возможностей между спотовыми и фьючерсными рынками на четырёх криптобиржах в реальном времени.

---

## Содержание

- [Как работает](#как-работает)
- [Компоненты системы](#компоненты-системы)
- [Зависимости и установка](#зависимости-и-установка)
- [Быстрый старт](#быстрый-старт)
- [Переменные окружения](#переменные-окружения)
- [Структура проекта](#структура-проекта)
- [Структура сигналов](#структура-сигналов)
- [Логи](#логи)
- [Проверка работы](#проверка-работы)
- [Управление](#управление)

---

## Как работает

```
dictionaries/main.py          (разово или по расписанию)
        │
        │  REST API + WS-валидация → списки активных пар
        ▼
dictionaries/subscribe/       ← 8 файлов подписки (по бирже × рынку)
dictionaries/combination/     ← 12 файлов пересечений (spot_A × futures_B)
        │
        │  коллекторы читают subscribe/ при старте
        ▼
run.py  ←  запускает 11 процессов
  ├── binance_spot.py    ─┐
  ├── binance_futures.py  │
  ├── bybit_spot.py       │  WebSocket → Redis
  ├── bybit_futures.py    │  (best bid/ask каждые ~50ms)
  ├── okx_spot.py         │
  ├── okx_futures.py      │
  ├── gate_spot.py        │
  ├── gate_futures.py    ─┘
  │
  ├── signal_scanner.py          Redis → signal/signals.csv
  │                              (спред ≥ 1.5% → сигнал)
  ├── signal_snapshot_writer.py  Redis → signal_snapshots/YYYY-MM-DD/HH/*.csv
  │                              (все пары каждые 0.3 с)
  └── redis_monitor.py           Redis метрики → logs/redis_monitor/
```

**Формула арбитражного спреда:**
```
spread = (bid_futures - ask_spot) / ask_spot × 100%
```
Если `spread ≥ MIN_SPREAD_PCT` — записывается сигнал.

---

## Компоненты системы

### 1. `dictionaries/` — Генерация списков торговых пар

Запускается **вручную** или по расписанию (рекомендуется раз в сутки). Обращается к REST API бирж, затем 60 секунд валидирует пары через WebSocket. Результат — файлы в `subscribe/` и `combination/`.

Подробнее: [`dictionaries/README.md`](dictionaries/README.md)

### 2. `market_data/` — Сборщики рыночных данных

8 асинхронных WebSocket-скриптов. Каждый обслуживает одну биржу + рынок. Записывают best bid/ask в Redis с интервалом ~50ms, историю цен — каждые 1s.

Подробнее: [`market_data/docs/README.md`](market_data/docs/README.md)

### 3. `signal_scanner.py` — Сканер арбитражных сигналов

Каждые 0.2с читает тикеры из Redis по всем парам из `combination/`, вычисляет спред и при превышении порога записывает строку в `signal/signals.csv`.

### 4. `signal_snapshot_writer.py` — Запись снапшотов спредов

Каждые 0.3с читает тикеры из Redis **по всем парам** (независимо от порога), вычисляет спред и записывает строку в CSV-файл. Файлы организованы по дням и часам:

```
signal_snapshots/
└── YYYY-MM-DD/
    └── HH/
        ├── binance__bybit__BTCUSDT.csv
        ├── binance__bybit__ETHUSDT.csv
        └── ...
```

Подробнее: [`docs/SIGNAL_SNAPSHOTS.md`](docs/SIGNAL_SNAPSHOTS.md)

### 5. `redis_monitor.py` — Мониторинг Redis

Каждые 5с собирает метрики Redis (латентность, память, ops/sec, keyspace) и пишет в `logs/redis_monitor/`.

### 7. `run.py` — Главный запускатор

Запускает все 11 процессов, следит за ними, перезапускает упавшие с экспоненциальной задержкой. Снапшот состояния — каждые 10 секунд.

---

## Зависимости и установка

**Требования:**
- Python 3.11+
- Redis (локальный или удалённый)

**Установка зависимостей:**
```bash
pip install redis>=7.0.0 websockets>=12.0
```

Или через файл:
```bash
pip install -r market_data/requirements.txt
```

---

## Быстрый старт

### Шаг 1 — Сгенерировать списки пар (один раз)

```bash
cd dictionaries
python3 main.py
cd ..
```

Скрипт работает ~2 минуты. По завершении обновятся все файлы в `subscribe/` и `combination/`.

### Шаг 2 — Запустить всю систему

```bash
python3 run.py
```

При старте:
1. Очищаются все `md:*` ключи в Redis
2. Задаётся вопрос об очистке логов
3. Запускаются все 10 процессов

**Остановка:** `Ctrl+C` — graceful shutdown всех процессов в течение 5 секунд.

---

## Переменные окружения

Все переменные передаются дочерним процессам автоматически через `os.environ`.

### Redis (общие для всех компонентов)

| Переменная       | По умолчанию | Описание                   |
|------------------|:------------:|----------------------------|
| `REDIS_HOST`     | `127.0.0.1`  | Хост Redis                 |
| `REDIS_PORT`     | `6379`       | Порт Redis                 |
| `REDIS_DB`       | `0`          | Номер базы данных          |
| `REDIS_PASSWORD` | не задан     | Пароль (если нужен)        |

### signal_scanner.py

| Переменная        | По умолчанию | Описание                                      |
|-------------------|:------------:|-----------------------------------------------|
| `SCAN_INTERVAL`   | `0.2`        | Секунд между опросами Redis                   |
| `SIGNAL_COOLDOWN` | `3600`       | Секунд между повторными сигналами (по паре)   |
| `MIN_SPREAD_PCT`  | `1.5`        | Минимальный спред для генерации сигнала, %    |

### signal_snapshot_writer.py

| Переменная                | По умолчанию | Описание                                     |
|---------------------------|:------------:|----------------------------------------------|
| `SNAPSHOT_WRITE_INTERVAL` | `0.3`        | Секунд между записями снапшотов              |

### redis_monitor.py

| Переменная         | По умолчанию          | Описание                        |
|--------------------|:---------------------:|---------------------------------|
| `MONITOR_INTERVAL` | `5`                   | Секунд между замерами           |
| `MONITOR_LOG_DIR`  | `logs/redis_monitor`  | Папка для лог-файлов            |
| `MONITOR_LOG_JSON` | `1`                   | `1` = JSON Lines, `0` = текст   |
| `LOG_CHUNK_SECONDS`| `86400`               | Размер лог-чанка (сек)          |
| `LOG_MAX_CHUNKS`   | `2`                   | Сколько чанков хранить          |

### Пример запуска с нестандартными настройками

```bash
REDIS_HOST=10.0.0.5 REDIS_PASSWORD=secret MIN_SPREAD_PCT=2.0 python3 run.py
```

---

## Структура проекта

```
bali_2.0/
│
├── run.py                          # Главный запускатор (11 процессов)
├── signal_scanner.py               # Сканер арбитражных сигналов
├── signal_snapshot_writer.py       # Запись снапшотов спредов (каждые 0.3 с)
│
├── signal/
│   └── signals.csv                 # Обнаруженные сигналы (CSV, спред ≥ порога)
│
├── signal_snapshots/               # Снапшоты всех пар (создаётся при запуске)
│   └── YYYY-MM-DD/                 # папка дня (UTC)
│       └── HH/                     # папка часа (UTC, 00–23)
│           └── {spot}__{fut}__{symbol}.csv  # файл пары
│
├── docs/
│   └── SIGNAL_SNAPSHOTS.md         # Документация signal_snapshot_writer
│
├── dictionaries/                   # Генерация списков торговых пар
│   ├── main.py                     # Запускатор (REST + WS-валидация)
│   ├── README.md                   # Документация модуля
│   ├── binance/
│   │   ├── binance_pairs.py        # REST API → список пар
│   │   └── binance_ws.py           # WS-валидация активных пар
│   ├── bybit/  okx/  gate/         # Аналогичная структура
│   ├── combination/                # 12 файлов пересечений (авто)
│   │   ├── binance_spot_bybit_futures.txt
│   │   └── ...
│   └── subscribe/                  # 8 файлов подписки (авто)
│       ├── binance/binance_spot.txt
│       └── ...
│
├── market_data/                    # Сборщики рыночных данных
│   ├── common.py                   # Общие утилиты (Stats, LogManager, ...)
│   ├── run_all.py                  # Запуск только 8 коллекторов
│   ├── redis_monitor.py            # Мониторинг Redis
│   ├── binance_spot.py             # Binance SPOT  → Redis
│   ├── binance_futures.py          # Binance FUTURES → Redis
│   ├── bybit_spot.py               # Bybit SPOT → Redis
│   ├── bybit_futures.py            # Bybit FUTURES → Redis
│   ├── okx_spot.py                 # OKX SPOT → Redis
│   ├── okx_futures.py              # OKX FUTURES → Redis
│   ├── gate_spot.py                # Gate SPOT → Redis
│   ├── gate_futures.py             # Gate FUTURES → Redis
│   ├── requirements.txt            # redis>=7.0.0, websockets>=12.0
│   ├── docs/
│   │   ├── README.md               # Архитектура market_data
│   │   ├── USER_GUIDE.md           # Руководство пользователя
│   │   ├── TECH_SPEC.md            # Техническая спецификация
│   │   ├── LOGS.md                 # Система логирования
│   │   └── OVERVIEW.md             # Обзор, боттлнеки, нагрузочные тесты
│   └── tests/
│       └── load_test.py            # Нагрузочные тесты Redis
│
└── logs/                           # Лог-файлы (не в git)
    ├── binance_spot/
    ├── binance_futures/
    ├── bybit_spot/  bybit_futures/
    ├── okx_spot/    okx_futures/
    ├── gate_spot/   gate_futures/
    ├── signal_scanner/
    ├── signal_snapshot_writer/
    └── redis_monitor/
```

---

## Структура сигналов

Файл: `signal/signals.csv`

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,45000.10,45676.35,1.5023,1741234567890
okx,gate,ETHUSDT,2800.00,2844.10,1.5750,1741234570012
```

| Поле          | Описание                                         |
|---------------|--------------------------------------------------|
| `spot_exch`   | Биржа, где покупаем (спот)                       |
| `fut_exch`    | Биржа, где продаём (фьючерс)                     |
| `symbol`      | Торговая пара (BTCUSDT)                          |
| `ask_spot`    | Цена покупки на споте                            |
| `bid_futures` | Цена продажи на фьючерсе                         |
| `spread_pct`  | Спред в процентах                                |
| `ts`          | Временная метка сигнала (unix ms)                |

Cooldown: один сигнал по одной паре (`spot_exch+fut_exch+symbol`) раз в `SIGNAL_COOLDOWN` секунд (по умолчанию 3600).

---

## Логи

```
logs/
├── binance_spot/
│   ├── 20240301_120000-20240302_120000/   ← завершённый чанк
│   │   └── binance_spot.log
│   └── 20240302_120000-ongoing/           ← текущий
│       └── binance_spot.log
├── ...  (аналогично для остальных 7 коллекторов)
├── signal_scanner/
│   └── YYYYMMDD_HHMMSS-ongoing/
│       └── signal_scanner.log
└── redis_monitor/
    └── YYYYMMDD_HHMMSS-ongoing/
        └── monitor.log
```

Ротация: 1 чанк = 24 часа, хранятся последние 2 чанка ≈ ~48 часов.

### Следить за логами в реальном времени

```bash
# Коллектор
tail -f logs/binance_spot/$(ls -t logs/binance_spot/ | head -1)/binance_spot.log

# Сканер сигналов
tail -f logs/signal_scanner/$(ls -t logs/signal_scanner/ | head -1)/signal_scanner.log

# Записчик снапшотов
tail -f logs/signal_snapshot_writer/$(ls -t logs/signal_snapshot_writer/ | head -1)/signal_snapshot_writer.log

# Redis монитор
tail -f logs/redis_monitor/$(ls -t logs/redis_monitor/ | head -1)/monitor.log
```

---

## Проверка работы

### Redis — есть ли данные

```bash
# Количество активных тикеров
redis-cli --scan --pattern 'md:*:*:*' | grep -v hist | grep -v chunks | wc -l

# Посмотреть конкретный тикер
redis-cli HGETALL md:binance:spot:BTCUSDT

# Убедиться, что данные обновляются в реальном времени
watch -n 1 'redis-cli HGET md:bybit:spot:BTCUSDT ts'
```

### Сигналы

```bash
# Последние 10 сигналов (порог MIN_SPREAD_PCT)
tail -10 signal/signals.csv

# Количество сигналов за сегодня
grep $(date +%Y) signal/signals.csv | wc -l
```

### Снапшоты спредов

```bash
# Посмотреть последние записи для конкретной пары (текущий час)
tail -20 signal_snapshots/$(date -u +%Y-%m-%d)/$(date -u +%H)/binance__bybit__BTCUSDT.csv

# Сколько строк записано за текущий час по всем парам
wc -l signal_snapshots/$(date -u +%Y-%m-%d)/$(date -u +%H)/*.csv | tail -1

# Список папок (дней)
ls signal_snapshots/
```

### Состояние процессов

`run.py` выводит снапшот каждые 10 секунд:

```
====================================================================
SNAPSHOT  run.py  2024-03-01 12:00:00 UTC
Manager uptime: 0h 05m 00s  |  Processes: 10/10 running
====================================================================
  ── СБОРЩИКИ ──────────────────────────────────────────────────────
  ✓ binance_spot              PID=1001    uptime=0h 04m 58s
  ✓ binance_futures           PID=1002    uptime=0h 04m 58s
  ✓ bybit_spot                PID=1003    uptime=0h 04m 58s
  ✓ bybit_futures             PID=1004    uptime=0h 04m 58s
  ✓ okx_spot                  PID=1005    uptime=0h 04m 58s
  ✓ okx_futures               PID=1006    uptime=0h 04m 58s
  ✓ gate_spot                 PID=1007    uptime=0h 04m 58s
  ✓ gate_futures              PID=1008    uptime=0h 04m 58s
  ── СКАНЕР ────────────────────────────────────────────────────────
  ✓ signal_scanner            PID=1009    uptime=0h 04m 58s
  ── МОНИТОРИНГ ────────────────────────────────────────────────────
  ✓ redis_monitor             PID=1010    uptime=0h 04m 58s
====================================================================
```

---

## Управление

### Остановка

```bash
Ctrl+C   # в терминале с run.py
# или
kill -SIGTERM <PID run.py>
```

### Обновить списки торговых пар

```bash
# Остановить run.py, обновить пары, запустить снова
Ctrl+C
cd dictionaries && python3 main.py && cd ..
python3 run.py
```

### Запустить только коллекторы (без сканера и мониторинга)

```bash
python3 market_data/run_all.py
```

### Запустить один скрипт отдельно (отладка)

```bash
python3 market_data/binance_spot.py
python3 signal_scanner.py
```

### Проверить нагрузку Redis

```bash
python3 market_data/tests/load_test.py
```

### Очистить Redis вручную

```bash
redis-cli --scan --pattern 'md:*' | xargs redis-cli DEL
```
