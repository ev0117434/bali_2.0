# signal_snapshot_writer.py — Документация

## Назначение

`signal_snapshot_writer.py` записывает снапшоты сигналов по следующей логике:

1. Каждые 0.3 с опрашивает Redis по всем парам из `combination/`.
2. Когда спред пары **впервые** >= `MIN_SPREAD_PCT` — открывается **окно записи** на `SNAPSHOT_DURATION` секунд (по умолчанию 3500).
3. В течение окна строки пишутся каждые 0.3 с **независимо** от дальнейшего поведения спреда.
4. По истечении `SNAPSHOT_DURATION` секунд запись для этой пары **останавливается навсегда** (до следующего запуска скрипта).

Отличие от `signal_scanner.py`:
- `signal_scanner.py` — записывает **один раз** при первом пересечении порога (cooldown 1 час), результат в `signal/signals.csv`.
- `signal_snapshot_writer.py` — пишет **каждые 0.3 с в течение 3500 с** после первого сигнала, результат в `signal_snapshots/`.

---

## Структура выходных файлов

```
signal_snapshots/
└── YYYY-MM-DD/                        ← папка дня (UTC)
    └── HH/                            ← папка часа (UTC, 00–23)
        ├── binance_s_bybit_f_BTCUSDT.csv
        ├── binance_s_bybit_f_ETHUSDT.csv
        ├── bybit_s_binance_f_BTCUSDT.csv
        └── ...
```

**Именование файлов:** `{spot_exch}_s_{fut_exch}_f_{symbol}.csv`

- `_s_` — разделитель после спотовой биржи (spot)
- `_f_` — разделитель после фьючерсной биржи (futures)

Примеры: `binance_s_bybit_f_BTCUSDT.csv`, `okx_s_gate_f_ETHUSDT.csv`

**Ротация:** при смене UTC-часа создаётся новая папка `HH/`. Файлы **не удаляются автоматически** — управление хранилищем на усмотрение оператора.

---

## Формат CSV

Первая строка файла — заголовок (записывается один раз при создании):

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
```

Каждая следующая строка — снапшот в момент, когда спред был активен:

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

**Пример файла** `binance_s_bybit_f_BTCUSDT.csv`:
```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,65412.30,65743.10,0.5056,1741234567890
binance,bybit,BTCUSDT,65415.00,65740.50,0.4965,1741234568190
```

> Строки появляются только пока `spread_pct >= MIN_SPREAD_PCT`. Если спред упал ниже — строки перестают записываться (но файл остаётся открытым для следующего периода активности в том же часе).

---

## Архитектура

```
dictionaries/combination/
  *.txt (12 файлов пересечений)
        │
        │  load_directions() — загрузка при старте
        ▼
  pairs: [(spot_exch, fut_exch, symbol), ...]   ← плоский список всех пар

        каждые 0.3 с (SNAPSHOT_WRITE_INTERVAL)
        │
        ▼
  Redis pipeline
  ├── HMGET md:{spot_exch}:spot:{symbol}    → ask, ts
  └── HMGET md:{fut_exch}:futures:{symbol}  → bid, ts
        │
        ▼
  для каждой пары:
  ┌─ нет данных в Redis → skipped (пропустить)
  ├─ пара в expired_pairs → пропустить навсегда
  ├─ пара в active_windows:
  │    elapsed >= SNAPSHOT_DURATION → window_expired, перенести в expired_pairs
  │    elapsed  < SNAPSHOT_DURATION → записать строку в CSV
  └─ пара НЕ в active_windows:
       spread >= MIN_SPREAD_PCT → window_opened, добавить в active_windows, записать строку
       spread  < MIN_SPREAD_PCT → below_threshold (пропустить)
        │
        ▼
  FileHandleManager.get(spot, fut, sym)
  └── signal_snapshots/YYYY-MM-DD/HH/{spot}_s_{fut}_f_{sym}.csv
        │
        ▼
  fh.write(line)  →  fhm.flush_all() в конце цикла
```

### Жизненный цикл окна пары

```
спред < 1.5%      спред ≥ 1.5%          (3500 с спустя)
─────────────── ● ──────────────────────── ● ─────────────
              window_opened             window_expired
               (начало записи)          (запись навсегда остановлена)
```

### FileHandleManager

Кэширует открытые файловые дескрипторы по ключу `(spot_exch, fut_exch, symbol)`. При смене UTC-часа:

1. Сбрасывает (`flush`) и закрывает все открытые дескрипторы.
2. Обновляет текущий ключ часа.
3. Новые дескрипторы открываются лениво при первом `get()`.

Ротация проверяется **один раз в начале каждого цикла** (`check_rotate()`), чтобы все пары одного цикла гарантированно писались в одну папку часа.

---

## Конфигурация

| Переменная окружения      | По умолчанию | Описание                                        |
|---------------------------|:------------:|-------------------------------------------------|
| `SNAPSHOT_WRITE_INTERVAL` | `0.3`        | Секунд между циклами записи                     |
| `MIN_SPREAD_PCT`          | `1.5`        | Порог спреда для открытия окна записи, %        |
| `SNAPSHOT_DURATION`       | `3500`       | Длина окна записи после первого сигнала, секунд |
| `REDIS_HOST`              | `127.0.0.1`  | Хост Redis                                      |
| `REDIS_PORT`              | `6379`       | Порт Redis                                      |
| `REDIS_DB`                | `0`          | Номер базы данных Redis                         |
| `REDIS_PASSWORD`          | —            | Пароль Redis (если нужен)                       |

> `MIN_SPREAD_PCT` должна совпадать с одноимённой переменной в `signal_scanner.py`.

---

## Логирование

Логи в `logs/signal_snapshot_writer/` в формате **JSON Lines**.

### Ротация логов

- Один чанк = **24 часа**, хранится **2 завершённых чанка** (≈ 48 ч).

```
logs/signal_snapshot_writer/
├── 20240318_120000-20240319_120000/
│   └── signal_snapshot_writer.log
└── 20240319_120000-ongoing/
    └── signal_snapshot_writer.log
```

### Типы записей

#### `start` — запуск

```json
{
  "type": "start",
  "ts": "2024-03-19T12:00:00.000Z",
  "config": {
    "redis": "127.0.0.1:6379/0",
    "write_interval_s": 0.3,
    "min_spread_pct": 1.5,
    "snapshots_dir": "/path/to/signal_snapshots",
    "combination_dir": "/path/to/dictionaries/combination"
  }
}
```

#### `snapshot` — каждые 10 с

```json
{
  "type": "snapshot",
  "ts": "2024-03-19T12:00:10.000Z",
  "uptime_s": 10.0,
  "config": {
    "write_interval_s": 0.3,
    "min_spread_pct": 1.5,
    "snapshot_duration_s": 3500,
    "total_pairs": 1440,
    "snapshots_dir": "/path/to/signal_snapshots"
  },
  "cycles": { "total": 33, "window": 33, "rate_per_s": 3.3 },
  "writes": { "total": 12, "window": 12, "rate_per_s": 1.2 },
  "skipped": { "total": 0, "window": 0, "reason": "no_redis_data" },
  "below_threshold": { "total": 47496, "window": 47496, "threshold": 1.5 },
  "windows": { "active_now": 4, "activated_total": 4, "expired_total": 0, "duration_s": 3500 },
  "overruns": { "total": 0, "window": 0, "threshold": ">2.0x interval" },
  "errors": { "total": 0, "window": 0, "last": null, "last_ts": null },
  "pipeline_latency_ms": { "min": 1.2, "avg": 2.1, "p95": 3.8, "max": 5.4, "samples": 33 },
  "write_latency_ms":    { "min": 0.01, "avg": 0.05, "p95": 0.1, "max": 0.3, "samples": 33 },
  "cycle_latency_ms":    { "min": 2.1, "avg": 3.8, "p95": 6.2, "max": 9.0, "samples": 33 }
}
```

**Ключевые поля:**

| Поле | Описание |
|------|----------|
| `writes` | Строк записано в CSV |
| `skipped` | Пар без данных в Redis |
| `below_threshold` | Пар ниже порога, окно не открыто |
| `windows.active_now` | Пар с открытым окном прямо сейчас |
| `windows.activated_total` | Пар, у которых окно когда-либо открывалось |
| `windows.expired_total` | Пар, у которых окно истекло по таймауту |
| `pipeline_latency_ms` | Время выполнения Redis pipeline |
| `write_latency_ms` | Время записи строк + flush файлов |
| `cycle_latency_ms` | Полное время цикла |

#### `window_opened` — открытие окна записи

```json
{
  "type": "window_opened",
  "ts": "2024-03-19T14:05:32.100Z",
  "spot_exch": "binance",
  "fut_exch": "bybit",
  "symbol": "BTCUSDT",
  "spread_pct": 1.6234,
  "duration_s": 3500
}
```

#### `window_expired` — истечение окна (запись остановлена)

```json
{
  "type": "window_expired",
  "ts": "2024-03-19T15:03:52.100Z",
  "spot_exch": "binance",
  "fut_exch": "bybit",
  "symbol": "BTCUSDT",
  "duration_s": 3500.1,
  "spread_pct": 0.8012
}
```

#### `warning` — overrun (цикл > 2× интервала)

```json
{
  "type": "warning",
  "ts": "2024-03-19T12:00:15.320Z",
  "msg": "cycle_overrun",
  "cycle_ms": 812.4,
  "target_ms": 300.0,
  "pipeline_ms": 750.1,
  "write_ms": 0.3,
  "written": 4,
  "skipped": 0,
  "below_threshold": 1436
}
```

#### `error` — ошибка

```json
{ "type": "error", "ts": "...", "phase": "redis_pipeline", "error": "ConnectionError: ..." }
{ "type": "error", "ts": "...", "phase": "file_write", "pair": "binance_s_bybit_f_BTCUSDT", "error": "OSError: ..." }
```

#### `stop` — остановка

```json
{ "type": "stop", "ts": "...", "cycles_total": 216000, "writes_total": 840, "errors_total": 0 }
```

---

## Мониторинг

```bash
# Следить за логами в реальном времени
tail -f logs/signal_snapshot_writer/$(ls -t logs/signal_snapshot_writer/ | head -1)/signal_snapshot_writer.log

# Только снапшоты (type=snapshot)
grep '"type": "snapshot"' \
  logs/signal_snapshot_writer/$(ls -t logs/signal_snapshot_writer/ | head -1)/signal_snapshot_writer.log \
  | tail -1 | python3 -m json.tool

# Только ошибки
grep '"type": "error"' \
  logs/signal_snapshot_writer/$(ls -t logs/signal_snapshot_writer/ | head -1)/signal_snapshot_writer.log

# Последние строки снапшота для пары (текущий час)
tail -10 signal_snapshots/$(date -u +%Y-%m-%d)/$(date -u +%H)/binance_s_bybit_f_BTCUSDT.csv

# Список файлов в текущем часе
ls signal_snapshots/$(date -u +%Y-%m-%d)/$(date -u +%H)/

# Общий объём за сегодня
du -sh signal_snapshots/$(date -u +%Y-%m-%d)/
```

---

## Запуск отдельно

```bash
python3 signal_snapshot_writer.py

# С другим порогом
MIN_SPREAD_PCT=2.0 python3 signal_snapshot_writer.py

# С нестандартным интервалом
SNAPSHOT_WRITE_INTERVAL=0.5 MIN_SPREAD_PCT=1.5 python3 signal_snapshot_writer.py
```

---

## Управление хранилищем

Файлы снапшотов не удаляются автоматически:

```bash
# Удалить данные старше 7 дней
find signal_snapshots/ -maxdepth 1 -type d -name "????-??-??" -mtime +7 -exec rm -rf {} +
```
