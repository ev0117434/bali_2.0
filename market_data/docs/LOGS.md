# Система логирования

## Где хранятся логи

```
{PROJECT_ROOT}/logs/
├── binance_spot/
│   ├── 20240301_120000-20240302_120000/   ← завершённый чанк
│   │   └── binance_spot.log
│   └── 20240302_120000-ongoing/           ← текущий (активный)
│       └── binance_spot.log
├── binance_futures/
│   └── ...
├── bybit_spot/
│   └── ...
... (8 папок — по одной на скрипт)
```

`PROJECT_ROOT` = директория на уровень выше `market_data/`, то есть корень проекта.

---

## Ротация логов

| Параметр              | Значение                             |
|-----------------------|--------------------------------------|
| `LOG_CHUNK_SECONDS`   | `86400` (24 часа на один чанк)       |
| `LOG_MAX_CHUNKS`      | `2` (хранятся последние 2 чанка)     |
| Имя папки (активная)  | `YYYYMMDD_HHMMSS-ongoing`            |
| Имя папки (закрытая)  | `YYYYMMDD_HHMMSS-YYYYMMDD_HHMMSS`   |

Когда наступает ротация:
1. Папка `...-ongoing` переименовывается в `...-<end_time>`
2. Создаётся новая папка `<new_time>-ongoing`
3. Если закрытых чанков стало больше `LOG_MAX_CHUNKS` — самый старый удаляется

Итого: хранится ~48 часов логов.

---

## Формат записей

Каждый скрипт пишет в свой `{script_name}.log`. Записи — plain-text, одна строка = одно событие. Временна́я метка включена в текст сообщения.

### Стартовые события

```
[binance_spot] Запуск...
[binance_spot] Загружено 150 символов
[binance_spot] Redis: {'host': '127.0.0.1', 'port': 6379, 'db': 0}
[binance_spot] Запускаю 1 WS-соединений (по 300 символов каждое)...
```

### Снапшот (каждые 10 секунд)

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
  RTT (exchange→recv):   n/a
  Proc (recv→redis):     min=0.3ms  avg=0.8ms  p95=1.9ms  max=3.1ms

 History chunk:
  chunk_id=1  elapsed=10s / 1200s  remaining=1190s

 Errors: none
================================================================
```

### Ошибки WebSocket

```
[binance_spot][conn=0] Ошибка: ConnectionClosedError: received 1006 (connection reset)
[binance_spot][conn=0] Переподключение через 1s...
[binance_spot][conn=0] Переподключение через 2s...
[binance_spot][conn=0] Переподключение через 4s...
```

### Ротация чанков истории

```
[binance_spot] Ротация history chunk: 1 → 2  (удалён чанк 0)
```

---

## Как читать логи

### Следить за активным скриптом в реальном времени

```bash
tail -f logs/binance_spot/$(ls -t logs/binance_spot/ | head -1)/binance_spot.log
```

### Посмотреть последние ошибки по всем скриптам

```bash
grep -r "Ошибка\|Error\|error" logs/*/$(date +%Y%m%d)*/
```

### Все снапшоты за последние 10 минут

```bash
grep "SNAPSHOT" logs/binance_spot/$(ls -t logs/binance_spot/ | head -1)/binance_spot.log | tail -20
```

### Сравнить задержки между биржами

```bash
grep "Proc (recv→redis)" logs/*/$(ls -t logs/binance_spot/ | head -1)/*.log
```

---

## Классы, управляющие логами (common.py)

### `LogManager`

```python
lm = LogManager("binance_spot")
await lm.initialize()

logger = lm.get_logger()
logger.info("[binance_spot] Сообщение")

# В фоновом цикле вызывается:
await lm.rotate_if_needed()   # проверяет 24-часовой дедлайн
```

| Метод                | Описание                                              |
|----------------------|-------------------------------------------------------|
| `initialize()`       | Создаёт первую папку лога `YYYYMMDD_HHMMSS-ongoing`   |
| `get_logger()`       | Возвращает текущий `logging.Logger`                   |
| `rotate_if_needed()` | Ротирует если прошло ≥ 86400 секунд с начала чанка    |
| `log(msg, level)`    | Удобный враппер над `logger.log()`                    |

### `SnapshotLogger`

```python
snap = SnapshotLogger("binance_spot", log_manager, stats, chunk_manager)
task = asyncio.create_task(snap.run())
# ...
snap.stop()
```

Запускается как фоновая задача, печатает снапшот каждые `SNAPSHOT_INTERVAL = 10` секунд в stdout **и** в лог-файл.

---

## Очистка логов

### При запуске run_all.py (интерактивно)

```
python3 market_data/run_all.py

[run_all] Очищаю Redis (md:*)... удалено 12450 ключей.
Очистить папку logs/? [y/N]: y
[run_all] Логи очищены (8 папок удалено).
```

Принимаемые ответы: `y`, `yes`, `д`, `да`

### Вручную

```bash
rm -rf logs/
mkdir logs/
```

---

## Влияние на дисковое пространство

Один снапшот ≈ 40–60 строк × 50 байт ≈ 2–3 KB каждые 10 секунд.

При 8 скриптах за 48 часов:
```
8 скриптов × 2 чанка × 24ч × 3600с/ч / 10с × ~2.5 KB ≈ ~350 MB
```

При необходимости уменьшить — снизьте `LOG_MAX_CHUNKS` в `common.py`.
