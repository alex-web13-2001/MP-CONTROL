# WB Finance Reports API Integration

Документация по интеграции с API Wildberries для скачивания еженедельных финансовых отчетов о реализации.

## Обзор

Добавлен функционал для автоматического скачивания финансовых отчетов WB через Celery задачу и загрузки данных в ClickHouse.

## API Эндпоинты

| Метод    | Путь                                       | Описание                                  |
| -------- | ------------------------------------------ | ----------------------------------------- |
| POST     | `/api/v1/finance-reports/download`         | Скачать отчеты за период (только файлы)   |
| **POST** | **`/api/v1/finance-reports/sync`**         | **Полный цикл: скачать + загрузить в БД** |
| GET      | `/api/v1/finance-reports/status/{task_id}` | Проверить статус задачи                   |
| GET      | `/api/v1/finance-reports/list`             | Получить список скачанных файлов          |

## Использование

### 1. Полная синхронизация за 3 месяца (рекомендуется)

```bash
curl -X POST "http://localhost:8000/api/v1/finance-reports/sync" \
     -H "Content-Type: application/json" \
     -d '{
       "shop_id": 1,
       "api_key": "YOUR_WB_API_KEY",
       "months": 3
     }'
```

**Что происходит:**

1. Генерируются недельные диапазоны за последние N месяцев
2. Для каждой недели скачивается отчет с WB
3. CSV парсится и данные загружаются в `fact_finances`
4. Прогресс обновляется в реальном времени

**Время выполнения:** 30-120 минут для 3 месяцев

### 2. Только скачивание файлов

```bash
curl -X POST "http://localhost:8000/api/v1/finance-reports/download" \
     -H "Content-Type: application/json" \
     -d '{
       "shop_id": 1,
       "date_from": "2025-01-01",
       "date_to": "2025-01-31",
       "api_key": "YOUR_WB_API_KEY"
     }'
```

### 3. Проверка статуса

```bash
curl "http://localhost:8000/api/v1/finance-reports/status/{task_id}"
```

Ответ при синхронизации:

```json
{
  "task_id": "abc123...",
  "status": "PROGRESS",
  "progress": {
    "current_week": 5,
    "total_weeks": 13,
    "date_range": "2024-12-02 - 2024-12-08",
    "rows_inserted": 15420
  }
}
```

## Технические детали

### Алгоритм работы (sync)

1. **Генерация диапазонов** — разбивка периода на недели (пн-вс)
2. **Получение ID отчетов** через `statistics-api.wildberries.ru`
3. **Запрос генерации файла** через `common-api.wildberries.ru`
4. **Polling статуса** до получения ссылки
5. **Скачивание CSV**
6. **Парсинг и маппинг** на схему `fact_finances`
7. **Batch insert** в ClickHouse (по 1000 строк)

### Маппинг CSV → fact_finances

| fact_finances       | CSV поле                            | Описание         |
| ------------------- | ----------------------------------- | ---------------- |
| `event_date`        | rr_dt                               | Дата операции    |
| `order_id`          | srid                                | ID продажи       |
| `external_id`       | nm_id                               | ID товара WB     |
| `vendor_code`       | sa_name                             | Артикул продавца |
| `operation_type`    | supplier_oper_name                  | Тип операции     |
| `payout_amount`     | ppvz_for_pay                        | К выплате        |
| `logistics_total`   | delivery_rub + rebill_logistic_cost | Логистика        |
| `wb_storage_amount` | storage_fee                         | Хранение         |

### Созданные файлы

| Файл                                                                                 | Описание                    |
| ------------------------------------------------------------------------------------ | --------------------------- |
| [wb_finance_report_service.py](../backend/app/services/wb_finance_report_service.py) | Скачивание отчетов          |
| [wb_finance_loader.py](../backend/app/services/wb_finance_loader.py)                 | Парсинг CSV + загрузка в CH |
| [tasks.py](../backend/celery_app/tasks/tasks.py)                                     | Celery задачи               |
| [finance_reports.py](../backend/app/api/v1/finance_reports.py)                       | API эндпоинты               |

## Дата изменения

**2026-02-02** — Реализована интеграция с WB Financial Reports API и загрузчик в ClickHouse
