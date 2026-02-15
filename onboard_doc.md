# Онбординг + Загрузка данных — История разработки

## Дата: 2026-02-15

---

## Phase 1: Onboarding Wizard

### Что сделано

**Backend:**

- Модель `Shop` (`backend/app/models/shop.py`) — расширена полями для Ozon Performance API:
  - `perf_client_id` (String) — Client-Id для Performance API
  - `perf_client_secret_encrypted` (LargeBinary) — зашифрованный секрет
  - `status` (String) — статус магазина (`active`, `syncing`, `disabled`)
  - `status_message` (Text) — сообщение об ошибке
- Endpoint `POST /shops/validate-key` — валидация API ключей перед созданием магазина
- Endpoint `POST /shops` — создание магазина с автоматическим запуском `load_historical_data.delay()`

**Frontend:**

- `OnboardingPage.tsx` — 4-шаговый wizard:
  1. Выбор маркетплейса (WB / Ozon)
  2. Ввод API-ключей (Seller API + Performance API для Ozon)
  3. Валидация ключей через backend
  4. Создание магазина → переход к загрузке данных
- `OnboardingGuard.tsx` — Guard компонент:
  - Если нет магазинов → редирект на `/onboarding`
  - Если магазин в статусе `syncing` → редирект на `/onboarding` (показ прогресса)
  - Return-visit: при возврате после закрытия браузера, OnboardingPage автоматически находит syncing магазин и показывает прогресс
- `authStore.ts` — добавлено поле `status` в интерфейс `Shop`

---

## Phase 2: Progress Bar + Polling

### Что сделано

**Backend:**

- Redis прогресс: ключ `sync_progress:{shop_id}` с TTL 24ч
  - Формат: `{status, current_step, total_steps, step_name, percent, error}`
- Endpoint `GET /shops/{id}/sync-status` — polling для frontend

**Frontend:**

- `StepSyncing` компонент в OnboardingPage:
  - Animated progress bar с процентами
  - Название текущего шага
  - Emoji индикатор (📦 → 🎉 / ⚠️)
  - Polling каждые 2 секунды
  - Кнопка «Перейти в Dashboard» при завершении
- API: `getSyncStatusApi()` в `frontend/src/api/shops.ts`

---

## Phase 3: Исправление загрузчиков (Аудит)

### Обнаруженная проблема

При тестировании выяснилось что **ни один загрузчик реально не выполнялся**. Все 5 subtasks падали с ошибкой:

```
ValueError: task_id must not be empty. Got None instead.
```

**Причина:** Subtasks объявлены как `@celery_app.task(bind=True)`, т.е. первый аргумент — `self` (Celery Task instance). Оркестратор `load_historical_data` вызывал их **напрямую** как обычные Python функции:

```python
# ❌ БЫЛО — прямой вызов, self.update_state() падает
sync_ozon_products(shop_id=shop_id, api_key=api_key, client_id=client_id)
```

При прямом вызове `self` получает аргументы не тех позиций, и `self.request.id` = `None` → `self.update_state()` выбрасывает ValueError.

Ошибка маскировалась через `except → continue`, и прогресс-бар доходил до 100% за 3 секунды.

### Исправление

Заменил прямые вызовы на `.apply()` — метод Celery, который создаёт полный task context (с `task_id`), но выполняется синхронно в том же процессе:

```python
# ✅ СТАЛО — .apply() с proper task context
def _run_subtask(task_ref, **kwargs):
    result = task_ref.apply(kwargs=kwargs)
    if result.failed():
        raise result.result
    return result.result
```

### Расширение pipeline: 5 → 11 шагов

Также сверил список загрузчиков с документацией `loaders_ozon.md` и добавил недостающие 6 модулей:

| #   | Шаг                             | Celery Task                   | API           |
| --- | ------------------------------- | ----------------------------- | ------------- |
| 1   | Каталог товаров                 | `sync_ozon_products`          | Seller        |
| 2   | Снимок данных (4-in-1)          | `sync_ozon_product_snapshots` | Seller        |
| 3   | Заказы (365 дней)               | `backfill_ozon_orders`        | Seller        |
| 4   | Финансы (12 месяцев)            | `backfill_ozon_finance`       | Seller        |
| 5   | Воронка продаж (365 дней)       | `backfill_ozon_funnel`        | Seller        |
| 6   | Возвраты (180 дней)             | `backfill_ozon_returns`       | Seller        |
| 7   | Остатки на складах              | `sync_ozon_warehouse_stocks`  | Seller        |
| 8   | Цены и комиссии                 | `sync_ozon_prices`            | Seller        |
| 9   | Рейтинг продавца                | `sync_ozon_seller_rating`     | Seller        |
| 10  | Рейтинг контента                | `sync_ozon_content_rating`    | Seller        |
| 11  | Рекламная статистика (180 дней) | `backfill_ozon_ads`           | Performance\* |

\*Шаг 11 добавляется только если заполнены Performance API credentials (`perf_client_id` + `perf_client_secret`).

### Аудит shop_id (мультитенантность)

Проверил все 9 Ozon сервисов и 14 ClickHouse таблиц — `shop_id` везде:

- Включён в INSERT VALUES каждого загрузчика
- Стоит **первым элементом** ORDER BY в каждой ClickHouse таблице
- PostgreSQL dim-таблицы используют UNIQUE constraint `(shop_id, product_id)`

### Результаты тестирования

Запущен `load_historical_data.delay(shop_id=1)` — все 11 шагов выполнились с реальными данными:

| Таблица ClickHouse           | Строк  | Диапазон дат            |
| ---------------------------- | ------ | ----------------------- |
| `fact_ozon_orders`           | 4 546  | 2025-08-19 → 2026-02-15 |
| `fact_ozon_transactions`     | 12 979 | 2025-05-17 → 2026-02-15 |
| `fact_ozon_funnel`           | 3 635  | 2025-11-17 → 2026-02-15 |
| `fact_ozon_returns`          | 229    | 2025-06-05 → 2026-02-12 |
| `fact_ozon_warehouse_stocks` | 265    | snapshot (сегодня)      |
| `fact_ozon_prices`           | 40     | snapshot (сегодня)      |
| `fact_ozon_seller_rating`    | 10     | snapshot (сегодня)      |
| `fact_ozon_commissions`      | 40     | snapshot (сегодня)      |
| `fact_ozon_content_rating`   | 40     | snapshot (сегодня)      |
| `fact_ozon_ad_daily`         | 2 438+ | 2025-08-19 → 2025-12-01 |

---

### Изменённые файлы

| Файл                                          | Что изменено                                                                                         |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `backend/celery_app/tasks/tasks.py`           | `load_historical_data`: `.apply()` вместо прямого вызова, 11 шагов, perf credentials, error tracking |
| `backend/app/models/shop.py`                  | Поля `perf_client_id`, `perf_client_secret_encrypted`, `status`, `status_message`                    |
| `frontend/src/pages/OnboardingPage.tsx`       | 4-step wizard + StepSyncing с progress bar + return-visit handling                                   |
| `frontend/src/components/OnboardingGuard.tsx` | Guard с проверкой syncing status                                                                     |
| `frontend/src/stores/authStore.ts`            | Поле `status` в интерфейсе `Shop`                                                                    |
| `frontend/src/api/shops.ts`                   | `getSyncStatusApi()`                                                                                 |

---

## Phase 4: Починка sync_ozon_content + мониторинг контента

### Проблема

Таблица `dim_ozon_product_content` в PostgreSQL была пустой — функция `sync_ozon_content` не имела декоратора `@celery_app.task` и не была включена в pipeline `load_historical_data`.

### Исправления

1. **Добавлен декоратор** `@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)` на `sync_ozon_content`
2. **Включена в pipeline** как шаг 11 (реклама сдвинулась на шаг 12)
3. **Исправлен** `POSTGRES_PASSWORD` в conn_params (было `mms`, стало `mms_secret` из env)

### Как работает мониторинг контента

Функция `sync_ozon_content` выполняет следующее:

1. Загружает список товаров через `/v3/product/list`
2. Загружает инфо о товарах (названия, фото) через `/v2/product/info/list`
3. Загружает описания **последовательно** через `/v1/product/info/description` (rate-limited)
4. Вычисляет MD5 хэши: `title_hash`, `description_hash`, `images_hash`
5. Сохраняет URL главного фото (`main_image_url`) и кол-во фото (`images_count`)
6. **Сравнивает с предыдущими хэшами** из `dim_ozon_product_content`
7. При обнаружении изменения генерирует events:
   - `OZON_PHOTO_CHANGE` — изменилось главное фото или галерея
   - `OZON_SEO_CHANGE` — изменилось название или описание

#### Триггеры мониторинга:

- **При первой загрузке** (onboarding) — сохраняет baseline хэши, events = 0
- **При ежедневном запуске** (cron/celery-beat) — сравнивает с baseline и фиксирует изменения в `event_log`

### Результаты тестирования

```
dim_ozon_product_content: 40 rows

product_id=1670726907  title_hash=9d9334c7..  desc_hash=36990202..
  main_image_url=https://cdn1.ozone.ru/s3/multimedia-1-p/7535969809.jpg
  images_hash=baee9088..  images_count=11

product_id=1670668065  title_hash=c4769a07..  desc_hash=976b7363..
  main_image_url=https://cdn1.ozone.ru/s3/multimedia-1-y/7769256118.jpg
  images_hash=bbd34d98..  images_count=10
```

### Pipeline итого: 12 шагов

| #   | Шаг                       | Celery Task                   | Хранилище  |
| --- | ------------------------- | ----------------------------- | ---------- |
| 1   | Каталог товаров           | `sync_ozon_products`          | PostgreSQL |
| 2   | Снимок данных (4-in-1)    | `sync_ozon_product_snapshots` | ClickHouse |
| 3   | Заказы (365 дней)         | `backfill_ozon_orders`        | ClickHouse |
| 4   | Финансы (12 месяцев)      | `backfill_ozon_finance`       | ClickHouse |
| 5   | Воронка продаж            | `backfill_ozon_funnel`        | ClickHouse |
| 6   | Возвраты (180 дней)       | `backfill_ozon_returns`       | ClickHouse |
| 7   | Остатки на складах        | `sync_ozon_warehouse_stocks`  | ClickHouse |
| 8   | Цены и комиссии           | `sync_ozon_prices`            | ClickHouse |
| 9   | Рейтинг продавца          | `sync_ozon_seller_rating`     | ClickHouse |
| 10  | Рейтинг контента          | `sync_ozon_content_rating`    | ClickHouse |
| 11  | Контент хэши (мониторинг) | `sync_ozon_content`           | PostgreSQL |
| 12  | Рекламная статистика\*    | `backfill_ozon_ads`           | ClickHouse |

\*Шаг 12 добавляется только при наличии Performance API credentials.

### Изменённые файлы

| Файл                                | Что изменено                                                                                                               |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `backend/celery_app/tasks/tasks.py` | Добавлен `@celery_app.task` декоратор на `sync_ozon_content`, включён в pipeline как шаг 11, исправлен `POSTGRES_PASSWORD` |

---

## Phase 5: Координаторы синхронизации (автоматический сбор данных)

### Проблема

12 запланированных задач в `celery.py` были **закомментированы** — каждая хардкодила `shop_id` и `api_key`. Данные собирались только при ручной загрузке через onboarding.

### Решение: 3 координатора

Вместо хардкода credentials — координаторы-задачи, которые:

1. Читают все магазины `status=active` из PostgreSQL
2. Расшифровывают API ключи
3. Диспатчат `.delay()` для каждого магазина

#### `sync_all_daily` — crontab(3:00 UTC)

Для каждого **Ozon** магазина запускает 8 задач:
`sync_ozon_products`, `sync_ozon_product_snapshots`, `sync_ozon_finance`, `sync_ozon_funnel`, `sync_ozon_returns`, `sync_ozon_seller_rating`, `sync_ozon_content_rating`, `sync_ozon_content`

Для каждого **WB** магазина запускает 2 задачи:
`sync_warehouses`, `sync_product_content`

#### `sync_all_frequent` — каждые 30 минут

Для **Ozon**: `sync_ozon_orders`, `sync_ozon_warehouse_stocks`, `sync_ozon_prices`
Для **WB**: `sync_orders`, `sync_commercial_data`, `sync_sales_funnel`

#### `sync_all_ads` — каждые 60 минут

Для **Ozon** (с perf credentials): `sync_ozon_ad_stats`, `monitor_ozon_bids`
Для **WB**: `sync_wb_advert_history`

### Результаты тестирования

**`sync_all_frequent`** — нашёл 1 active shop (shop 2), dispatched 3 задачи:

- `sync_ozon_orders`: 201 rows (FBO=201, 14 дней)
- `sync_ozon_warehouse_stocks`: 265 rows (38 SKUs, 23 склада)
- `sync_ozon_prices`: 40 rows

**`sync_all_daily`** — dispatched 8 задач для shop 2:

- `sync_ozon_products`: 40 products (2.0s)
- `sync_ozon_product_snapshots`: 40 products (2.1s)
- `sync_ozon_finance`: 88 rows (0.6s)
- `sync_ozon_funnel`: 120 rows (0.6s)
- `sync_ozon_returns`: 229 rows (0.6s)
- `sync_ozon_seller_rating`: 10 metrics (0.4s)
- `sync_ozon_content_rating`: 40 SKUs (2.5s)
- `sync_ozon_content`: 40 products, 0 events (20.6s)

### Изменённые файлы

| Файл                                | Что изменено                                                                            |
| ----------------------------------- | --------------------------------------------------------------------------------------- |
| `backend/celery_app/tasks/tasks.py` | Добавлены 3 координатора: `sync_all_daily`, `sync_all_frequent`, `sync_all_ads`         |
| `backend/celery_app/celery.py`      | Удалены 12 закомментированных задач, добавлены 3 координатора в beat_schedule + routing |

---

## Phase 6: Расширение WB pipeline + проверка прав ключа

### WB Pipeline: 4 → 7 шагов

| #   | Шаг                           | Задача                           |
| --- | ----------------------------- | -------------------------------- |
| 1   | Контент товаров               | `sync_product_content`           |
| 2   | Заказы (90 дней)              | `backfill_orders`                |
| 3   | **Воронка продаж (365 дней)** | `backfill_sales_funnel` ← НОВЫЙ  |
| 4   | Финансовые отчёты             | `sync_wb_finance_history`        |
| 5   | **Рекламная история**         | `sync_wb_advert_history` ← НОВЫЙ |
| 6   | **Цены + остатки**            | `sync_commercial_data` ← НОВЫЙ   |
| 7   | Склады                        | `sync_warehouses`                |

### WB ключ: проверка прав через /ping

Валидация теперь пингует 5 WB API доменов:

- `content-api` — контент товаров
- `statistics-api` — статистика, финансы
- `marketplace-api` — заказы, склады
- `advert-api` — реклама
- `discounts-prices-api` — цены, скидки

При неполном доступе возвращается список предупреждений `warnings`.

### Удалённый код

- Задача `download_wb_finance_reports` — мёртвый код (вызывала deprecated `sync_reports_for_period` → NotImplementedError)
- Эндпоинт `POST /finance-reports/download` — удалён
- Схема `DownloadReportsRequest` — удалена

### Изменённые файлы

| Файл                                    | Что изменено                                                 |
| --------------------------------------- | ------------------------------------------------------------ |
| `backend/celery_app/tasks/tasks.py`     | WB pipeline: 4→7 шагов; удалён `download_wb_finance_reports` |
| `backend/app/api/v1/shops.py`           | `_validate_wb_key` переписана с /ping на 5 доменах           |
| `backend/app/schemas/auth.py`           | Добавлено поле `warnings` в `ValidateKeyResponse`            |
| `backend/app/api/v1/finance_reports.py` | Удалены `/download` эндпоинт и импорт удалённой задачи       |
