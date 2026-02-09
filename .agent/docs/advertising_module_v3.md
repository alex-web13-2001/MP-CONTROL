# История разработки MP-CONTROL

## 2026-02-07: Исправления модуля WB Advertising V3

### Исправлено:

1. **ClickHouse auth** — добавлен `users.d/default-user.xml` для Docker network access
2. **SQLAlchemy** — переименовано `metadata` → `event_metadata` (reserved word)
3. **WB API** — обновлено на `POST /v1/promotion/adverts` (новый эндпоинт Oct 2025)
4. **EventDetector** — парсинг `unitedParams` вместо `params`, добавлена валидация типов
5. **unhashable dict** — добавлена проверка `isinstance(nm, int)` в `_extract_items()`

### Результаты синхронизации:

- 1003 строк в ads_raw_history
- 37 кампаний, 40 товаров
- 122,754₽ расходов, 412 заказов
- 152 halo items (is_associated=1)

# Модуль сбора рекламных данных WB V3

## Обзор архитектуры

Модуль предназначен для накопления исторических данных рекламных кампаний Wildberries с детекцией событий изменений. В отличие от предыдущей версии (ReplacingMergeTree), новая архитектура **накапливает** данные, а не заменяет их.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   WB API V3     │────▶│  Celery Worker  │────▶│   ClickHouse    │
│  /v3/fullstats  │     │    + Redis      │     │ ads_raw_history │
│  /v2/adverts    │     │                 │     │   (MergeTree)   │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │   PostgreSQL    │
                        │   event_log     │
                        └─────────────────┘
```

---

## Компоненты системы

### 1. ClickHouse: `ads_raw_history`

**Путь**: `docker/clickhouse/init.sql`

**Engine**: `MergeTree` (НЕ ReplacingMergeTree!) — данные накапливаются.

**Ключевые поля**:
| Поле | Тип | Описание |
|------|-----|----------|
| `fetched_at` | DateTime | Момент забора (для внутридневных графиков) |
| `shop_id` | UInt32 | ID магазина |
| `advert_id` | UInt64 | ID кампании |
| `nm_id` | UInt64 | ID товара (артикул WB) |
| `vendor_code` | String | Артикул продавца |
| `campaign_type` | UInt8 | Тип кампании (1=search, 7=auto, 8=search+catalog) |
| `views, clicks, spend...` | — | Метрики из API |
| `cpm` | Decimal | Текущая ставка (для отслеживания изменений) |
| `is_associated` | UInt8 | 1 = товар не в официальном списке кампании (Halo) |

**Materialized Views**:

```sql
-- Дневные агрегаты для Карты продаж
ads_daily_mv: GROUP BY date, shop_id, advert_id, nm_id

-- Часовые агрегаты для детального таймлайна
ads_hourly_mv: GROUP BY hour, shop_id, advert_id, nm_id
```

---

### 2. PostgreSQL: `event_log`

**Путь**: `backend/app/models/event_log.py`

Хранит события изменений кампаний:

- `BID_CHANGE` — изменение ставки CPM/CPC
- `STATUS_CHANGE` — пауза/запуск кампании
- `ITEM_ADD` — товар добавлен в кампанию
- `ITEM_REMOVE` — товар удален из кампании
- `ITEM_INACTIVE` — товар перестал получать показы

---

### 3. Redis: State Manager

**Путь**: `backend/app/core/redis_state.py`

Хранит **Last State** кампаний для детекции изменений.

**Структура** (Redis Hash):

```
HSET ads:state:{shop_id}:{advert_id}
  cpm "500"
  status "9"
  items "[123, 456, 789]"
  campaign_type "8"
```

**Дополнительно**: Хранит `last_views` для ITEM_INACTIVE:

```
SET ads:state:views:{shop}:{advert}:{nm_id} "1234"
```

---

### 4. Event Detector

**Путь**: `backend/app/services/event_detector.py`

**Логика работы**:

1. Получает настройки кампании из API (`/adv/v2/adverts`)
2. Сравнивает с Last State в Redis
3. Генерирует события при обнаружении изменений
4. Обновляет Last State

**Debouncing (защита от мусора)**:

```python
# Событие BID_CHANGE генерируется ТОЛЬКО если:
# 1. raw_cpm не None
# 2. raw_cpm != ""
# 3. float(raw_cpm) > 0
```

---

### 5. Advertising Loader

**Путь**: `backend/app/services/wb_advertising_loader.py`

**Класс `AdsRawHistoryRow`**: Dataclass для строки истории.

**Методы**:
| Метод | Описание |
|-------|----------|
| `parse_stats_for_history()` | Парсит /v3/fullstats, добавляет is_associated |
| `insert_history()` | INSERT в ads_raw_history |
| `get_vendor_code_cache()` | Кэш артикулов из fact_finances |

**Защита от ZeroDivision**:

```python
cpc = (spend / clicks) if clicks > 0 else Decimal(0)
```

---

### 6. Celery Task

**Путь**: `backend/celery_app/tasks/tasks.py`

**Функция**: `sync_wb_advert_history(shop_id, api_key, days_back, accumulate_history)`

**Флаг `accumulate_history=True`**:

1. Получает настройки кампаний → детектирует события
2. Собирает campaign_items, cpm_values, campaign_types
3. Загружает vendor_code кэш
4. Получает /v3/fullstats → insert в ads_raw_history

---

## Флаг is_associated (Halo Effect)

**Проблема**: WB присваивает конверсии "покатушечным" товарам, которые пользователь купил после клика на другой товар.

**Решение**:

```python
# official_items = список nm_id из настроек кампании (/v2/adverts)
# stats_items = список nm_id из статистики (/v3/fullstats)

is_associated = 0 if nm_id in official_items else 1
```

- `is_associated=0` — товар официально в кампании (locomotive)
- `is_associated=1` — товар не в списке, но получил конверсию (halo/wagon)

---

## API Endpoints используемые

| Endpoint                      | Назначение             | Rate Limit |
| ----------------------------- | ---------------------- | ---------- |
| `GET /adv/v1/promotion/count` | Список кампаний        | —          |
| `GET /adv/v2/adverts`         | Настройки (CPM, items) | —          |
| `GET /adv/v3/fullstats`       | Полная статистика      | 1 req/min  |

---

## Запуск

```bash
# Синхронизация с накоплением истории
from celery_app.tasks.tasks import sync_wb_advert_history
sync_wb_advert_history.delay(
    shop_id=1,
    api_key="YOUR_KEY",
    days_back=30,
    accumulate_history=True
)
```

---

## Verification Query

```sql
-- Проверка данных за сегодня
SELECT
    toDate(fetched_at) as date,
    count() as rows,
    sum(spend) as total_spend,
    countIf(is_associated = 1) as halo_items
FROM mms_analytics.ads_raw_history
WHERE shop_id = 1
GROUP BY date
ORDER BY date DESC
LIMIT 7;

-- Сравнение с дневными агрегатами
SELECT *
FROM mms_analytics.ads_daily_mv FINAL
WHERE shop_id = 1
ORDER BY date DESC
LIMIT 10;
```
