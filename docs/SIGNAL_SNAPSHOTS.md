# signal_snapshot_writer.py — Документация

## Назначение

`signal_snapshot_writer.py` непрерывно записывает снапшоты спредов **по всем торговым парам** из `dictionaries/combination/` с интервалом **0.3 секунды**.

В отличие от `signal_scanner.py` (который записывает только сигналы выше порога `MIN_SPREAD_PCT`), этот модуль сохраняет историю спреда **для каждой пары** независимо от его значения. Это позволяет:

- восстановить точную динамику спреда в любой момент времени;
- найти пары, которые почти достигли порога (потенциальные будущие сигналы);
- проводить ретроспективный анализ без потери данных.

---

## Структура выходных файлов

```
signal_snapshots/
└── YYYY-MM-DD/             ← папка дня (UTC)
    └── HH/                 ← папка часа (UTC, 00–23)
        ├── binance__bybit__BTCUSDT.csv
        ├── binance__bybit__ETHUSDT.csv
        ├── bybit__binance__BTCUSDT.csv
        └── ...             ← по одному файлу на каждую пару направления
```

**Именование файлов:** `{spot_exch}__{fut_exch}__{symbol}.csv`

- `spot_exch` — биржа покупки (спот)
- `fut_exch`  — биржа продажи (фьючерс)
- `symbol`    — торговая пара

**Ротация:** при смене UTC-часа создаётся новая папка `HH/`. Старые папки **не удаляются автоматически** (в отличие от лог-файлов) — управление хранилищем остаётся на усмотрение оператора.

---

## Формат CSV

Первая строка файла — заголовок (записывается один раз при создании файла):

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
```

Каждая следующая строка — снапшот данного момента:

```
binance,bybit,BTCUSDT,45000.10,45676.35,1.5023,1741234567890
```

| Поле          | Тип    | Описание                                              |
|---------------|--------|-------------------------------------------------------|
| `spot_exch`   | string | Биржа спотового рынка (binance / bybit / okx / gate)  |
| `fut_exch`    | string | Биржа фьючерсного рынка                               |
| `symbol`      | string | Торговая пара (BTCUSDT, ETHUSDT, …)                   |
| `ask_spot`    | float  | Лучший ask на споте в момент снапшота                 |
| `bid_futures` | float  | Лучший bid на фьючерсе в момент снапшота              |
| `spread_pct`  | float  | Спред, % = (bid_futures − ask_spot) / ask_spot × 100  |
| `ts`          | int    | Unix-время в миллисекундах (UTC)                      |

**Пример файла:**
```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,65412.30,65743.10,0.5056,1741234567890
binance,bybit,BTCUSDT,65415.00,65740.50,0.4965,1741234568190
binance,bybit,BTCUSDT,65411.80,65741.20,0.5031,1741234568490
```

---

## Архитектура

```
dictionaries/combination/
  *.txt (12 файлов пересечений)
        │
        │  load_directions() — загрузка при старте
        ▼
  pairs: [(spot_exch, fut_exch, symbol), ...]   ← плоский список всех пар
        │
        │  каждые 0.3 с (SNAPSHOT_WRITE_INTERVAL)
        ▼
  Redis pipeline
  ├── HMGET md:{spot_exch}:spot:{symbol}    → ask, ts
  └── HMGET md:{fut_exch}:futures:{symbol}  → bid, ts
        │
        │  для каждой пары с валидными данными
        ▼
  spread_pct = (bid − ask) / ask × 100
        │
        ▼
  FileHandleManager.get(spot, fut, sym)
  └── signal_snapshots/YYYY-MM-DD/HH/{spot}__{fut}__{sym}.csv
        │
        ▼
  fh.write(line)  →  fhm.flush_all() в конце каждого цикла
```

### FileHandleManager

Кэширует открытые файловые дескрипторы по ключу `(spot_exch, fut_exch, symbol)`. При смене UTC-часа:

1. Сбрасывает (`flush`) и закрывает все открытые дескрипторы.
2. Обновляет текущий ключ часа.
3. Новые дескрипторы открываются лениво при первом обращении `get()`.

Ротация проверяется **один раз в начале каждого цикла** (`check_rotate()`), а не внутри `get()`, чтобы все пары одного цикла гарантированно записывались в одну и ту же папку часа.

---

## Конфигурация

| Переменная окружения      | По умолчанию | Описание                              |
|---------------------------|:------------:|---------------------------------------|
| `SNAPSHOT_WRITE_INTERVAL` | `0.3`        | Секунд между циклами записи           |
| `REDIS_HOST`              | `127.0.0.1`  | Хост Redis                            |
| `REDIS_PORT`              | `6379`       | Порт Redis                            |
| `REDIS_DB`                | `0`          | Номер базы данных Redis               |
| `REDIS_PASSWORD`          | —            | Пароль Redis (если нужен)             |

---

## Логирование

Логи хранятся в `logs/signal_snapshot_writer/` в формате **JSON Lines** (один JSON-объект на строку).

### Ротация логов

- Один чанк = **24 часа**.
- Хранится не более **2 завершённых чанков** (≈ 48 часов).
- При открытии нового чанка самый старый удаляется автоматически.

```
logs/signal_snapshot_writer/
├── 20240318_120000-20240319_120000/   ← завершённый
│   └── signal_snapshot_writer.log
└── 20240319_120000-ongoing/           ← текущий
    └── signal_snapshot_writer.log
```

### Типы записей

#### `start` — запуск скрипта

```json
{
  "type": "start",
  "ts": "2024-03-19T12:00:00.000Z",
  "config": {
    "redis": "127.0.0.1:6379/0",
    "write_interval_s": 0.3,
    "snapshots_dir": "/path/to/signal_snapshots",
    "combination_dir": "/path/to/dictionaries/combination"
  }
}
```

#### `directions_loaded` — пары загружены

```json
{
  "type": "directions_loaded",
  "ts": "2024-03-19T12:00:00.050Z",
  "directions": 12,
  "pairs": 1440
}
```

#### `redis_connected` — подключение к Redis

```json
{
  "type": "redis_connected",
  "ts": "2024-03-19T12:00:00.080Z",
  "redis": "127.0.0.1:6379/0"
}
```

#### `snapshot` — периодический снапшот (каждые 10 с)

```json
{
  "type": "snapshot",
  "ts": "2024-03-19T12:00:10.000Z",
  "uptime_s": 10.0,
  "config": {
    "write_interval_s": 0.3,
    "total_pairs": 1440,
    "snapshots_dir": "/path/to/signal_snapshots"
  },
  "cycles": {
    "total": 33,
    "window": 33,
    "rate_per_s": 3.3
  },
  "writes": {
    "total": 47520,
    "window": 47520,
    "rate_per_s": 4752.0
  },
  "skipped": {
    "total": 0,
    "window": 0,
    "reason": "no_redis_data"
  },
  "overruns": {
    "total": 0,
    "window": 0,
    "threshold": ">2.0x interval"
  },
  "errors": {
    "total": 0,
    "window": 0,
    "last": null,
    "last_ts": null
  },
  "pipeline_latency_ms": {
    "min": 1.2,
    "avg": 2.1,
    "p95": 3.8,
    "max": 5.4,
    "samples": 33
  },
  "write_latency_ms": {
    "min": 0.8,
    "avg": 1.5,
    "p95": 2.9,
    "max": 4.1,
    "samples": 33
  },
  "cycle_latency_ms": {
    "min": 2.1,
    "avg": 3.8,
    "p95": 6.2,
    "max": 9.0,
    "samples": 33
  }
}
```

**Поля снапшота:**

| Поле | Описание |
|------|----------|
| `cycles` | Счётчик полных итераций основного цикла |
| `writes` | Строк, записанных в CSV-файлы |
| `skipped` | Пар, пропущенных из-за отсутствия данных в Redis |
| `overruns` | Циклов, превысивших `2 × WRITE_INTERVAL` по времени |
| `pipeline_latency_ms` | Время выполнения Redis pipeline (мс) |
| `write_latency_ms` | Время записи и flush всех файлов (мс) |
| `cycle_latency_ms` | Полное время цикла (pipeline + write + overhead) |

#### `warning` — превышение целевого интервала

Записывается, если цикл занял более `2 × WRITE_INTERVAL` секунд:

```json
{
  "type": "warning",
  "ts": "2024-03-19T12:00:15.320Z",
  "msg": "cycle_overrun",
  "cycle_ms": 812.4,
  "target_ms": 300.0,
  "pipeline_ms": 750.1,
  "write_ms": 55.3,
  "written": 1438,
  "skipped": 2
}
```

#### `error` — ошибка Redis или записи в файл

```json
{
  "type": "error",
  "ts": "2024-03-19T12:01:00.000Z",
  "phase": "redis_pipeline",
  "error": "ConnectionError: ..."
}
```

```json
{
  "type": "error",
  "ts": "2024-03-19T12:01:00.100Z",
  "phase": "file_write",
  "pair": "binance__bybit__BTCUSDT",
  "error": "OSError: [Errno 28] No space left on device"
}
```

Поле `phase`:
- `redis_pipeline` — ошибка при выполнении `pipe.execute()`
- `file_write` — ошибка при записи в конкретный CSV-файл

#### `stop` — остановка скрипта

```json
{
  "type": "stop",
  "ts": "2024-03-19T18:00:00.000Z",
  "cycles_total": 216000,
  "writes_total": 311040000,
  "errors_total": 0
}
```

---

## Производительность

| Параметр | Значение |
|----------|----------|
| Интервал цикла | 0.3 с (≈ 3.3 цикла/с) |
| Redis pipeline | 1 pipeline на цикл, 2 команды на пару |
| Ожидаемая задержка pipeline | 1–5 мс |
| Записей в секунду | ≈ `total_pairs × 3.3` |
| Размер одной строки | ≈ 60–80 байт |
| Дисковая нагрузка при 1000 парах | ≈ 200–250 КБ/с |
| Объём за 1 час при 1000 парах | ≈ 720–900 МБ/ч |

> **Совет:** при большом количестве пар рекомендуется мониторить переменную `overruns` в снапшотах. Если цикл систематически занимает более 0.6 с (2 × интервал), увеличьте `SNAPSHOT_WRITE_INTERVAL` или убедитесь в производительности Redis и диска.

---

## Мониторинг

```bash
# Следить за логами в реальном времени
tail -f logs/signal_snapshot_writer/$(ls -t logs/signal_snapshot_writer/ | head -1)/signal_snapshot_writer.log

# Посмотреть последние снапшоты (только type=snapshot)
grep '"type": "snapshot"' \
  logs/signal_snapshot_writer/$(ls -t logs/signal_snapshot_writer/ | head -1)/signal_snapshot_writer.log \
  | tail -3 | python3 -m json.tool

# Посмотреть ошибки
grep '"type": "error"' \
  logs/signal_snapshot_writer/$(ls -t logs/signal_snapshot_writer/ | head -1)/signal_snapshot_writer.log

# Количество строк в файле пары за текущий час
wc -l signal_snapshots/$(date -u +%Y-%m-%d)/$(date -u +%H)/binance__bybit__BTCUSDT.csv

# Последние 5 снапшотов пары
tail -5 signal_snapshots/$(date -u +%Y-%m-%d)/$(date -u +%H)/binance__bybit__BTCUSDT.csv

# Общее количество файлов за сегодня
find signal_snapshots/$(date -u +%Y-%m-%d)/ -name "*.csv" | wc -l

# Общий объём данных за сегодня
du -sh signal_snapshots/$(date -u +%Y-%m-%d)/
```

---

## Запуск отдельно (отладка)

```bash
# Запустить только signal_snapshot_writer
python3 signal_snapshot_writer.py

# С нестандартным интервалом
SNAPSHOT_WRITE_INTERVAL=1.0 python3 signal_snapshot_writer.py

# С внешним Redis
REDIS_HOST=10.0.0.5 REDIS_PASSWORD=secret python3 signal_snapshot_writer.py
```

---

## Управление хранилищем

Файлы снапшотов **не удаляются автоматически**. Рекомендуемые подходы:

```bash
# Удалить данные старше 7 дней
find signal_snapshots/ -type d -name "????-??-??" -mtime +7 -exec rm -rf {} +

# Удалить все данные кроме последних 2 дней (пример cron)
# 0 3 * * * find /path/to/signal_snapshots/ -maxdepth 1 -type d -mtime +2 -exec rm -rf {} +
```
