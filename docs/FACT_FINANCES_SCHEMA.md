# fact_finances — Схема данных (Финальная версия)

Центральная таблица для унифицированного хранения финансовых данных с WB, Ozon и файловых источников.

## Расположение

- **Таблица**: `mms_analytics.fact_finances`
- **View**: `mms_analytics.fact_finances_latest`
- **SQL**: [docker/clickhouse/init.sql](../docker/clickhouse/init.sql)

## Структура полей

### 1. CORE GROUP (Общие для всех)

| Поле             | Тип                     | Описание                                   |
| ---------------- | ----------------------- | ------------------------------------------ |
| `event_date`     | Date                    | Дата операции                              |
| `shop_id`        | UInt32                  | ID магазина                                |
| `marketplace`    | Enum8('wb'=1, 'ozon'=2) | Маркетплейс                                |
| `order_id`       | String                  | Номер заказа/отправления                   |
| `external_id`    | String                  | nmId (WB) / SKU (Ozon)                     |
| `vendor_code`    | String                  | Артикул продавца                           |
| `operation_type` | String                  | Продажа, Возврат, Логистика, Корректировка |
| `quantity`       | Int32                   | Количество                                 |
| `retail_amount`  | Decimal(18,2)           | Цена для клиента                           |
| `payout_amount`  | Decimal(18,2)           | Чистыми к выплате                          |

### 2. EXPENSES GROUP (Расходы)

| Поле                | Тип           | Описание                  |
| ------------------- | ------------- | ------------------------- |
| `commission_amount` | Decimal(18,2) | Комиссия МП               |
| `logistics_total`   | Decimal(18,2) | Общая логистика           |
| `ads_total`         | Decimal(18,2) | Реклама (если вычитается) |
| `penalty_total`     | Decimal(18,2) | Штрафы                    |

### 3. WB SPECIFIC GROUP

| Поле                | Тип           | Описание           |
| ------------------- | ------------- | ------------------ |
| `wb_gi_id`          | UInt64        | Номер поставки     |
| `wb_ppvz_for_pay`   | Decimal(18,2) | К перечислению ПВЗ |
| `wb_delivery_rub`   | Decimal(18,2) | Доставка в рублях  |
| `wb_storage_amount` | Decimal(18,2) | Хранение           |

### 4. OZON SPECIFIC GROUP

| Поле                      | Тип           | Описание             |
| ------------------------- | ------------- | -------------------- |
| `ozon_acquiring`          | Decimal(18,2) | Эквайринг            |
| `ozon_last_mile`          | Decimal(18,2) | Последняя миля       |
| `ozon_milestone`          | Decimal(18,2) | Магистраль           |
| `ozon_marketing_services` | Decimal(18,2) | Маркетинговые услуги |

### 5. SERVICE FIELDS

| Поле               | Тип      | Описание               |
| ------------------ | -------- | ---------------------- |
| `source_file_name` | String   | Название файла (аудит) |
| `raw_payload`      | String   | JSON строки (бэкап)    |
| `updated_at`       | DateTime | Время обновления       |

---

## Логика маппинга

### Wildberries (Еженедельный отчет)

| fact_finances       | WB поле                      | Правило |
| ------------------- | ---------------------------- | ------- |
| `order_id`          | Номер заказа                 | —       |
| `external_id`       | nmId                         | —       |
| `vendor_code`       | sa_name                      | —       |
| `retail_amount`     | Цена розничная               | —       |
| `payout_amount`     | К перечислению за товар      | —       |
| `logistics_total`   | Услуги по доставке + Возврат | Сумма   |
| `operation_type`    | Обоснование для оплаты       | Парсинг |
| `wb_ppvz_for_pay`   | ppvz_for_pay                 | —       |
| `wb_delivery_rub`   | delivery_rub                 | —       |
| `wb_storage_amount` | storage_fee                  | —       |

### Ozon (Отчет о реализации)

| fact_finances     | Ozon поле                  | Правило |
| ----------------- | -------------------------- | ------- |
| `order_id`        | Номер отправления          | —       |
| `external_id`     | SKU                        | —       |
| `vendor_code`     | offer_id                   | —       |
| `retail_amount`   | Цена реализации            | —       |
| `payout_amount`   | Итого к начислению         | —       |
| `logistics_total` | Магистраль + Миля + Сборка | Сумма   |
| `operation_type`  | Тип начисления             | —       |
| `ozon_acquiring`  | Эквайринг                  | —       |
| `ozon_last_mile`  | Последняя миля             | —       |
| `ozon_milestone`  | Магистраль                 | —       |

---

## Примеры запросов

```sql
-- Выручка по магазинам за месяц
SELECT
    shop_id,
    marketplace,
    sum(payout_amount) as revenue,
    sum(commission_amount) as commission,
    sum(logistics_total) as logistics
FROM fact_finances_latest
WHERE event_date >= '2026-01-01' AND event_date < '2026-02-01'
GROUP BY shop_id, marketplace;

-- Unit-экономика по артикулу (маржинальность)
SELECT
    vendor_code,
    sum(quantity) as units,
    sum(retail_amount) as gmv,
    sum(payout_amount) as revenue,
    sum(commission_amount + logistics_total + penalty_total) as costs,
    sum(payout_amount) - sum(commission_amount + logistics_total) as profit
FROM fact_finances_latest
WHERE shop_id = 1
GROUP BY vendor_code
ORDER BY profit DESC;
```

> [!TIP]
> Всегда используйте `fact_finances_latest` — это оптимизированный view без `FINAL`.
