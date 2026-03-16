## 2026-03-16 (v4)

### feat(warehouses): Ozon — ИИ-анализ географии продаж (Gemini 2.5 Flash)

**Backend** (`warehouses.py`):
- `POST /ozon/geography/ai-analysis` — endpoint ИИ-анализа для Ozon кластеров
  - Prompt `_AI_PROMPT_GEO_OZON`: 7 шагов сбора данных (кластеры+города, топ товары, per-cluster products, warehouse stocks, cross-delivery stats)
  - Маппинг `WAREHOUSE_TO_CLUSTER` для привязки стоков РФЦ к кластерам
  - Кеширование 6ч в Redis, force-refresh параметр
  - Gemini 2.5 Flash через kie.ai API

**Frontend** (`OzonGeographyPage.tsx`, `warehouses.ts`):
- Компонент `OzonGeographyAIInsight` (~400 строк):
  - Баннер: severity 🔴/🟡/🟢 + diagnosis + кнопка «Прочитать»
  - Модалка: 4 метрики (концентрация, кластеры, недообслуженные, заказы)
  - Секции: Концентрация продаж, Инсайты по товарам, Логистическое соответствие, Рекомендации
- API функция `getOzonGeographyAIAnalysis()`

**Bugfix**: `free_to_sell_amount` → `free_to_sell` в запросе к `fact_ozon_warehouse_stocks`

---

## 2026-03-16 (v3)

### feat(warehouses): Ozon — география продаж (кластеры → города → товары)

**Backend** (`warehouses.py`):
- `GET /ozon/geography` — основной endpoint: кластеры доставки + города + стабильность + топ товары
  - SQL: `fact_ozon_orders FINAL` GROUP BY `cluster_to`, `city`
  - Стабильность: `count(DISTINCT toDate(order_date)) / period * 100`
  - Топ товары per-cluster (top-5) и общие (top-50) с metadata из `dim_ozon_products`
  - Фильтр по массиву SKU
- `GET /ozon/geography/city-products` — drill-down товаров по конкретному городу
  - Стабильность по неделям: `uniqExact(toMonday(order_date)) / total_weeks`
- `GET /ozon/geography/products-search` — autocomplete для фильтра (по name, offer_id, sku)

**Frontend** (`OzonGeographyPage.tsx` ~530 строк, `warehouses.ts`):
- `OzonGeographyPage` — полная страница:
  - 4 KPI: заказы, выручка, ср. чек, охват (X кл. · Y гор.)
  - `ClustersTable` — 2-уровневая: кластеры → города → товары
  - `TopProductsTable` — сортируемая таблица с колонками: кластеры, города, доля
  - `OzonProductCombobox` — мульти-фильтр по SKU (autocomplete с debounce)
  - Period selector (7/14/30/60/90 дн), skeleton loading, анимации
- `WarehousesGeographyPage.tsx` — redirect `→ /warehouses/analytics` заменён на `<OzonGeographyPage />`
- API типы: `OzonGeographyResponse`, `OzonGeographyCluster`, `OzonGeographyProduct`

---

## 2026-03-16 (v2)

### fix(supply): синхронизация расчёта хранения между Поставками и Хранением

**Backend** (`warehouses.py`):
- При наличии фактических данных `fact_wb_paid_storage` для магазина, SKU/склад-комбинации без записей теперь получают `storage_per_day=0` вместо завышенной тарифной оценки
- Логика синхронизирована с аналитическим endpoint (раздел Хранение)
- Было: Поставки 31K₽ vs Хранение 15K₽ (2x расхождение из-за tariff fallback)
- Стало: Поставки ~46K₽ vs Хранение прогноз ~42K₽ (~10%, разница из-за разного окна данных 14д vs 30д)

### fix(overview): OOS диагностика — серверная агрегация по всем складам

**Backend** (`warehouses.py`):
- Новый блок «Out-of-stock aggregation» — серверный расчёт OOS из полных `wh_stocks` + `wh_sku_orders` (без лимита `[:50]` per warehouse)
- Суммирует stock/orders по nm_id **по всем складам**, фильтрует `stock/daily < 14`, возвращает top-10
- Новое поле `kpi.out_of_stock_skus` в ответе analytics API

**Frontend** (`WarehousesOverviewPage.tsx`, `warehouses.ts`):
- OOS карточка диагностики теперь берёт данные из `kpi.out_of_stock_skus` (бэкенд) вместо фронтенд-агрегации из обрезанных `w.skus[:50]`
- Добавлен тип `out_of_stock_skus` в `WBAnalyticsKpi`
- Было: диагностика показывала 5шт/1/день (только SKU попавшие в top-50 по заказам на складе)
- Стало: корректная агрегация 37шт/20/день (все склады)

## 2026-03-16

### feat(warehouses): реструктуризация раздела «Склады» — 5 подстраниц

**Frontend** (новые страницы + рефакторинг навигации):
- **`WarehousesOverviewPage`** (~1130 строк) — единый дашборд складов WB:
  - 4 KPI: оборачиваемость, заказы/день, остатки, SKU
  - Блок «Диагностика проблем»: кросс, хранение, out-of-stock, штрафы (ProblemCard)
  - Блок «Расходы за период»: горизонтальные бары (логистика, ↳кросс, хранение, возмещение, приёмка, удержания)
  - Таблица 20+ складов с сортировкой (статус, кросс%, оборач., хранение₽)
  - ИИ-анализ (AIDiagnosticsBlock): сценарии по SKU + перераспределение через Gemini
- **`WarehousesCrossPage`** (~980 строк) — кросс-логистика WB:
  - 4 KPI: кросс-стоимость (≈), средний кросс%, проблемных SKU, складов с кроссом
  - Топ-проблемные SKU с потерями и рекомендациями «куда довезти»
  - Кросс-карта: матрица «склад × округ» (зелёный=свой, красный=кросс)
  - Кросс-анализ по складам: раскрываемые строки → SKU детализация
  - SkuGeographyPanel: «где лежит» + «куда продаётся» при клике на SKU
- **`WarehousesStoragePage`** — хранение WB (WBWarehouseAnalyticsContent)
- **`WarehousesGeographyPage`** — география продаж WB с ИИ-анализом
- Sidebar: «Склады» → 5 подпунктов (Обзор, Кросс-логистика, Хранение, География, Поставки)
- Routing: `/warehouses/overview`, `/cross`, `/storage`, `/geography`, `/supply`

### fix(warehouses): унификация расчёта кросс-стоимости

**Frontend** (`WarehousesCrossPage.tsx`):
- Убрана фиктивная константа `CROSS_COST_PER_ORDER = 33₽` — выдуманная цифра
- Формула: `Σ(wh.logistics_cost × wh.cross_orders / wh.orders)` — из `fact_finances`
- Пометка `≈` — WB не разделяет логистику на кросс/обычную в отчётах
- Единый расчёт на Overview (↳ Кросс) и Cross (KPI) — цифры совпадают
- Обновлены: KPI, TopProblemSkus, SkuGeographyPanel, CrossWarehousesTable

### fix(warehouses): удалён блок «Нужна поставка» из Overview

**Frontend** (`WarehousesOverviewPage.tsx`):
- Удалена карточка ProblemCard key="supply" — дублировала «Скоро out-of-stock» и страницу Поставки
- Показывала бессмысленные «2 скл.» без объяснения что и зачем поставлять
- SKU с `daily_sales=0` получали `+0 шт` — вводило в заблуждение
- Очищены unused: import PackagePlus, переменная criticalWhs

### docs: обновление архитектурной документации

- `06_FRONTEND.md`: добавлены секции WarehousesOverviewPage и WarehousesCrossPage, обновлены Routing (5 складских маршрутов) и Sidebar (5 подпунктов)

---

## 2026-03-15

### feat(warehouses): ИИ-анализ географии продаж

**Backend** (`warehouses.py`):
- Новый endpoint `POST /warehouses/wb/geography/ai-analysis`
- Системный промпт `_AI_PROMPT_GEOGRAPHY` с JSON-схемой ответа
- Сбор данных: округа/регионы, стабильность, топ-товары, запасы по складам, кросс-доставки, реклама
- Вызов Gemini 2.5 Flash с группировкой всех данных в один промпт
- Кеширование ответов на 6 часов, force-refresh параметр

**Frontend** (`warehouses.ts`, `WarehousesGeographyPage.tsx`):
- Новые типы: `GeoAIAnalysis`, `GeoAIConcentration`, `GeoAIProductInsight`, `GeoAILogisticsMatch`
- API функция `getGeographyAIAnalysis()`
- Компонент `GeographyAIInsight` — 5 визуальных блоков:
  1. **Header**: severity 🔴/🟡/🟢 + диагноз
  2. **4 метрики**: концентрация %, регионы, недообслуженные округа, заказы
  3. **Концентрация продаж**: таблица топ-регионов + risk level + рекомендация
  4. **Инсайты по товарам**: артикул + тип (лидер/нестабильный/кросс-проблема) + действие + эффект в ₽
  5. **Логистическое соответствие**: округа с кросс-доставкой + склад + рекомендация
  6. **Общие рекомендации**: конкретные советы с числами
- Отображается между KPI и таблицей округов (только без SKU-фильтра)

---

## 2026-03-14 (v6)


### feat(warehouses): Полная таблица хранения SKU с поиском и сортировкой

**Backend** (`warehouses.py`):
- Убран лимит `[:20]` — таблица теперь отдаёт **все SKU** (не TOP-20)
- KPI и таблица показывают согласованные суммы

**Frontend** (`WBWarehouseAnalyticsContent.tsx`):
- Переименовано: «ТОП по стоимости хранения» → «Хранение по SKU»
- **Поиск** по названию, артикулу или nm_id с live-фильтрацией
- **Сортировка** по всем столбцам (клик на заголовок) с индикаторами ▲▼
- Артикул под названием выделен **жирным** шрифтом (text-[11px] font-bold)
- Полное название товара в **2 строки** (line-clamp-2, max-w-300px)
- Hover-эффект на строках таблицы
- Удалена колонка «Источник», убрана легенда Факт/Оценка
- Заголовок: «Хранение за 30д» (left) + «Прогноз 30д» (right)
- Итого пересчитывается с учётом фильтрации поиска

**Bugfix** (`warehouses.py`):
- Исправлена аномально большая сумма «Хранение за 30д» (435 844₽ вместо 4 540₽) для магазинов с фактическими данными paid storage
- Причина: tariff-fallback завышал оценку для SKU без записей в `fact_wb_paid_storage`, хотя магазин уже имел фактические данные
- Теперь при наличии actual paid storage, SKU без записей получают `est_cost_month=0` вместо тарифной оценки
- Исправлен `total_penalties`: «Списание за отзыв» (192K) выделено в отдельную категорию расходов, больше не маппится как «Штрафы». Native WB `operation_type='Штраф'` (4 540₽) — единственный источник для KPI штрафов

**feat(supply)**: Реальные данные paid storage в Поставках WB:
- `_build_wb_supply_data()` теперь загружает фактические данные из `fact_wb_paid_storage` (avg за 14 дней)
- Приоритет: реальная стоимость → тарифный fallback
- Добавлен `storage_source` (actual/tariff) для каждого склада
- Excel экспорт автоматически получает обновлённые данные через тот же data builder

---

## 2026-03-14 (v5)
### feat(warehouses): Прогноз хранения WB на 30 дней

**Backend** (`warehouses.py`):
- Запрос per-SKU daily cost из `fact_wb_paid_storage` (avg за последние 7 дней)
- Агрегация per-SKU orders и stock для расчёта daily_sales
- Формула прогноза: `Σ(cost_per_unit_per_day × max(0, stock − daily_sales × i))` для i=0..29
- Учитывает убывание стока: чем быстрее товар продаётся, тем меньше прогноз
- Новые поля: `forecast_30d`, `daily_sales`, `daily_cost`, `days_to_sell` для каждого SKU
- KPI: `forecast_30d` — суммарный прогноз по всем SKU

**Frontend** (`warehouses.ts`, `WBWarehouseAnalyticsContent.tsx`):
- KPI карточка «Прогноз 30д» со статусом (сравнение с текущим хранением)
- 3 новых колонки в StorageSkusTable: «Прод/д», «Дней», «Прогноз 30д»
- Цветовая индикация: дней до распродажи (🟢 <90, 🟡 <180, 🔴 >180, ∞ нет продаж)
- Суммарный прогноз в заголовке таблицы (amber) рядом с хранением/мес (red)

---

## 2026-03-14 (v4)

### feat(warehouses): WB аналитика складов — фактические данные хранения

**Backend** (`warehouses.py`):
- Запрос к `fact_wb_paid_storage` для фактической стоимости хранения per-SKU per-warehouse
- Fallback на тарифную оценку (storBase × vol × qty × коэф) если нет данных paid storage
- Per-warehouse `storage_cost_actual` и `storage_cost_month` в объекте склада
- KPI: `total_storage_actual` (факт из paid storage) + `has_actual_storage` флаг
- `storage_source: "actual" | "estimated"` для каждого SKU и склада

**Bugfix** (`warehouses.py`):
- Fix `UnboundLocalError: wh_list` в `/wb/ai-analysis` — переменная определялась только в ветке tariff-fallback, но использовалась безусловно

**Frontend** (`warehouses.ts`, `WBWarehouseAnalyticsContent.tsx`):
- KPI карточка «Хранение (факт)» с подписью «По отчётам WB • за Nд»
- Новая колонка «Хранение ₽» в таблице складов (фактическая стоимость per-warehouse)
- Бэйджи 📊 Факт / 📐 Оценка в таблице «ТОП по стоимости хранения»
- Легенда: «📊 Факт — из отчётов WB API | 📐 Оценка — по тарифам»

---

## 2026-03-14 (v3)

### feat(warehouses): WB Paid Storage API — фактические данные хранения по SKU

**ClickHouse** (`docker/clickhouse/migrations/004_add_wb_paid_storage.sql`):
- Новая таблица `fact_wb_paid_storage` — ежедневные данные хранения по SKU × склад × тип расчёта
- `shop_id` в ORDER BY — привязка к магазину как везде в системе
- `warehouse_price` — фактическая сумма (может быть отрицательной для скидок WB)
- `calc_type` — различает основное хранение, скидки на остаток/период поставки

**Backend** (`wb_paid_storage_service.py`):
- `WBPaidStorageService` — create→poll→download цикл для WB Report API
- Автоматическая разбивка по 7-дневным чанкам (лимит API = 8 дней)
- `prepare_ch_rows()` — конвертация API-ответа в CH rows с `shop_id`

**Celery** (`tasks.py`):
- `sync_wb_paid_storage` — ежедневная задача, 7 дней назад, queue=sync
- `backfill_wb_paid_storage` — одноразовый бэкфилл за N месяцев
- `sync_all_daily` обновлён — `sync_wb_paid_storage` включён для всех WB-магазинов

**ИИ-анализ** (`warehouses.py`):
- Гибридная логика `est_storage_month`: приоритетно фактические данные из `fact_wb_paid_storage`, fallback на тарифную оценку
- `storage_source: "actual" | "estimated"` — источник данных для каждого SKU
- `has_actual_storage` + `actual_storage_skus` — метаданные в AI response
- Prompt обновлён: «факт» vs «оценка» для прозрачности

---

## 2026-03-14 (v2)

### feat(warehouses): ИИ-анализ складов v2 — P&L + перераспределение + параллельные запросы

**Backend** (`warehouses.py`):
- Два специализированных Gemini-промпта вместо одного:
  - **Промпт 1 (SKU problems)**: сценарный анализ по каждому проблемному SKU с вариантами: `discount`, `launch_ads` (для оборачиваемости), `withdraw`, `do_nothing`, `reduce_supply`
  - **Промпт 2 (Redistribution)**: конкретные рекомендации по перемещению товаров между складами (`transfers[]` с `from_warehouse`, `destinations[]`, `keep_at_source`, `expected_effect`)
- **P&L обогащение**: для каждого SKU подтягиваются данные из `fact_finances`: revenue, payout, logistics, storage_fact, deductions, cogs → `net_profit` (чистая прибыль)
- **Параллельные Gemini API вызовы** через `asyncio.gather()` — ~2x быстрее
- **Исправлен ClickHouse SQL**: разбит сложный JOIN-запрос warehouse summary на 4 простых запроса (stock per warehouse, orders per warehouse, stock per nm_id, orders per nm_id) → мержатся в Python. Устранена ошибка `Correlated subqueries are not supported in JOINs yet`
- Кеширование с TTL 6ч для обоих промптов
- Новый формат ответа: `sku_actions[]` (с `net_profit_month`), `transfers[]` (с `destinations[]`), `general_tips[]`, `supply_tip`

**Frontend** (`warehouses.ts`, `WBWarehouseAnalyticsContent.tsx`):
- Новые TypeScript интерфейсы: `AITransfer`, `AITransferDestination`, обновлён `AISkuAction` (+`net_profit_month`), `AISkuOption` (`launch_ads` вместо `advertise`)
- Компонент `WarehouseAIInsight` переписан на 3 визуальных блока:
  1. **Проблемные товары**: expandable карточки SKU с P&L данными (хранение, чист. прибыль), grid сценариев (discount/ads/withdraw/do_nothing)
  2. **Перераспределение**: таблица трансферов (склад-источник → склады-получатели с qty, причиной, эффектом)
  3. **Supply tip**: рекомендация по поставке + кнопка-ссылка на раздел Поставки
- Увеличены шрифты: 13-14px основной текст, 14px заголовки сценариев
- Удалён `advertise` action (заменён на `launch_ads` — контекст оборачиваемости)
- 3 KPI карточки: Кросс-логистика, Избыточное хранение, Потенциал экономии

---

## 2026-03-14

### feat(warehouses): ИИ-анализ складов → сценарный формат по SKU


**Backend** (`warehouses.py`):
- Полностью переписан AI system prompt: вместо общих рекомендаций → детальный сценарный анализ по каждому проблемному SKU
- Новый формат ответа: `sku_actions[]` (vendor_code, problem, options[]), `warehouse_tips[]`, `supply_tip`
- Каждый SKU получает 2-3 опции: `discount`, `advertise`, `redistribute`, `withdraw`, `do_nothing`, `reduce_supply`
- Опции включают: label, detail (с расчётами DRR, маржи, прогнозом продаж), expected_savings, risk (low/medium/high)
- ИИ указывает `recommended_option` — индекс лучшего варианта
- Новый SQL запрос: per-warehouse stock distribution (nm_id × warehouse_name из fact_inventory_snapshot)
- Обогащённый SKU-контекст: цена, себестоимость, маржа (руб/%), estimated_storage_cost/month, warehouse_distribution{}
- Запрещены рекомендации по рекламе/ценам вне складского контекста

**Frontend** (`warehouses.ts`, `WBWarehouseAnalyticsContent.tsx`):
- Новые TypeScript интерфейсы: `AISkuAction`, `AISkuOption`, `AIWarehouseTip`
- Компонент `WarehouseAIInsight` переписан:
  - Expandable карточки SKU с vendor_code, problem, storage_cost/month
  - Grid сценариев внутри каждого SKU: иконка + label + detail + risk badge + savings
  - Рекомендуемый вариант выделен зелёной рамкой + бейджем «✓ Рекомендуем»
  - Блок «По складам» с конкретными советами по перераспределению
  - Supply tip с кнопкой-ссылкой на раздел Поставки (корректный shop_id)
  - Auto-expand первых 2 SKU карточек

---

## 2026-03-13

### feat(warehouses): WB — кросс-складской анализ расхода стока

**Backend** (`warehouses.py`):
- Новый маппинг `WAREHOUSE_TO_OKRUG` — 50+ складов WB → 8 федеральных округов (вкл. `:Питание` и `СГТ`)
- Новый SQL: фактический расход `warehouse_name × nm_id × oblast_okrug_name` из `fact_orders_raw`
- `wh_consumption` — маппинг {nm_id: {warehouse: {okrug: qty}}}
- `effective_days` — реальный запас с учётом кросс-нагрузки (Воронеж→ЮФО, Самара→Сибирь и т.д.)
- `cross_daily` / `cross_okrugs` — детализация паразитной нагрузки по округам
- Пересчёт статуса по `effective_days` (ok→attention→critical)
- Корректная работа с food-товарами: кросс-слив неизбежен (нет складов Питания в Сибири/ДВ)

**Excel экспорт**:
- Лист «Рекомендации по складам»: колонки «Реал.зап, дн» (col 8) и «Кросс» (col 9) — сдвинуты остальные на +2

---

## 2026-03-12 (v2)

### feat(warehouses): Ozon — кросс-кластерный анализ расхода стока

**Backend** (`warehouses.py`):
- Новый SQL: фактический расход по `warehouse_name × offer_id × cluster_to` из `fact_ozon_orders`
- `wh_consumption` — маппинг {offer_id: {source_cluster: {dest_cluster: qty}}} через `WAREHOUSE_TO_CLUSTER`
- `effective_days` — реальный запас: `stock / фактический_расход` (включая кросс-нагрузку от соседних пустых кластеров)
- `post_restock_days` — прогноз после закрытия дефицитов: `stock / собственный_расход`
- `cross_consumption` / `cross_clusters` — детализация паразитной нагрузки (какой кластер сколько шт/день)
- Пересчёт статуса товара по `effective_days` (attention → critical если реальный запас <14д)
- Сортировка по min(days_supply, effective_days)

**Excel экспорт**:
- Лист «Рекомендации»: колонки «Реал.зап» (col 15) и «Кросс» (col 16) — сдвинуты ПОСТАВИТЬ и далее на +2
- Лист «Методология»: секция «КРОСС-КЛАСТЕРНЫЙ АНАЛИЗ» — объяснение warehouse_name анализа

**Frontend** (`warehouses.ts`):
- `CrossClusterDrain` — новый тип
- `SupplyCluster` + `effective_days`, `post_restock_days`, `cross_consumption`, `cross_clusters`
- `SupplyItem` + `effective_days`

## 2026-03-12

### feat(warehouses): Ozon поставки — реальные стоки по РФЦ

**Backend** (`warehouses.py`):
- Маппинг `WAREHOUSE_TO_CLUSTER` (34 РФЦ → кластеры доставки) + обратный `CLUSTER_TO_WAREHOUSES_OZON`
- `_compute_supply` переписан: стоки берутся **по каждому складу** из `fact_ozon_warehouse_stocks` (`GROUP BY offer_id, warehouse_name`)
- Формула: `need = max(0, daily×target×safety − РЕАЛЬНЫЙ_сток_на_РФЦ)` вместо пропорциональной оценки
- Новые поля в JSON: `wh_stock`, `warehouses` (список РФЦ с остатками)
- Excel: колонка «Оц.стока» → «Сток РФЦ» + новая колонка «Склады» (Лист 1)
- Excel: Sheet 4 «Поставка по кластерам» + Sheet 5 «Объединённые кластеры» — добавлена колонка «Сток РФЦ»
- Excel: Методология обновлена — формула с real stock, описание маппинга складов

**Frontend** (`WarehouseSupplyPage.tsx`, `warehouses.ts`):
- `SupplyCluster` + `wh_stock`, `warehouses` поля
- `HubItem` + `wh_stock`
- Колонка «Оц. стока» → «Сток РФЦ»

### docs: обновление архитектурной документации

- `04_BACKEND_API.md`: Response Schema Ozon Supply обновлена (wh_stock, warehouses, warehouse), маппинг WAREHOUSE_TO_CLUSTER, формула per-warehouse stock, Excel описание обновлено
- `06_FRONTEND.md`: типы SupplyCluster/HubItem + колонка «Сток РФЦ»

## 2026-03-11 (v10)

### fix(warehouses): исключение FBS складов из плана кроссдокинга

**Backend** (`warehouses.py`):
- Crossdocking analysis теперь фильтрует заказы через FBO whitelist (`warehouse_type='fbo'`)
- FBS склады (ООО "ТЕЙЛОРД" и т.п.) больше не попадают в план распределения
- Ранее FBS заказы ошибочно показывали «поставить на свой же склад»

## 2026-03-11 (v9)

### feat(warehouses): Excel экспорт плана распределения

**Backend** (`warehouses.py`):
- Новый endpoint `GET /ozon/analytics/distribution-plan/excel`
- Лист 1 «Сводный план» — все SKU со складами, действиями, причинами, выгодой, городами спроса
- Лист 2 «Поставки по складам» — supply items сгруппированы по складу с merge cells и итогами
- Лист 3 «Перемещения» — transfer items сгруппированы по источнику, с окупаемостью и итогами

**Frontend** (`WarehouseAnalyticsPage.tsx`, `warehouses.ts`):
- `downloadDistributionPlanExcel()` в API
- Зеленая кнопка «Скачать Excel» на табе Кроссдокинг со стейтом загрузки

## 2026-03-11 (v8)

### feat(warehouses): кроссдокинг — контекст «почему» и «что даст»

**Backend** (`warehouses.py`):
- SQL запрос географии CD-заказов: `city`, `cluster_from` по SKU × warehouse из `fact_ozon_orders`
- Каждый distribution_plan item обогащён: `demand_cities` (top-5 городов спроса), `shipped_from` (откуда отгружается), `reason` (текстовое объяснение), `benefit` (ожидаемый эффект с окупаемостью)
- Warehouse-level: `top_demand_cities` (top-5 городов по всем SKU склада), `total_orders_cd`

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Warehouse header: строка «📍 Спрос: Москва (8), Домодедово (4)...» — top-4 города
- SKU sub-rows: контекстный блок с причиной (📍) и выгодой (💡) под каждым товаром
- `React.Fragment` для корректного рендера пар строк (данные + контекст)

**Types** (`warehouses.ts`):
- `DistributionPlanItem` + `demand_cities`, `shipped_from`, `reason`, `benefit`
- `DistributionPlanWarehouse` + `top_demand_cities`, `total_orders_cd`

## 2026-03-11 (v7)

### feat(warehouses): Кроссдокинг → План распределения по складам

**Backend** (`warehouses.py`):
- Новая агрегация `distribution_plan`: группировка crossdocking SKU по складу-получателю
- Для каждого склада: список SKU с action (`transfer`/`supply`), qty, source_warehouse, transfer_cost
- Алгоритм: ищет склад-донор с избытком → если есть, action=transfer (со стоимостью), иначе supply

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- `CrossdockingTable` заменён на `DistributionPlanTable` — складоцентричный вид
- 4 KPI-карточки: складов с CD, расход/мес, переместить, поставить
- Раскрываемые блоки складов с таблицей SKU: действие, кол-во, прод/д, CD/мес, источник
- Фильтры (Все/Переместить/Поставить) и поиск работают по distribution_plan

**Types** (`warehouses.ts`):
- `DistributionPlanItem`, `DistributionPlanWarehouse` + поле `distribution_plan` в response

## 2026-03-11 (v6)

### feat(warehouses): столбец Реклама + компактные бейджи + умные рекомендации

**Backend** (`warehouses.py`):
- Добавлен ClickHouse запрос к `fact_ozon_ad_daily` для SKU из зоны риска (spend_30d, orders_30d, has_active_ads)
- Рекомендации теперь ad-aware: учитывают наличие/отсутствие рекламы в каждом SKU
- Добавлено поле `ad_info` в `storage_risk_skus`

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Бейдж «Зона» стал компактным (без emoji, fixed-width, меньше padding)
- Колонка «Сверх лимита» заменена на «Реклама» (Да/Нет)
- Рекомендации различают ситуации: «Запустить рекламу + скидка» vs «Скидка 30-50% или вывоз»

**Types** (`warehouses.ts`):
- `StorageRiskSku` + `ad_info: { has_ads, spend_30d, orders_30d }`

## 2026-03-11 (v5)

### refactor(warehouses): per-SKU рекомендации вместо per-warehouse

**Backend** (`warehouses.py`):
- Удалены `paid_storage` рекомендации по складам (одинаковый текст для всех)
- Добавлено поле `recommendation` в `storage_risk_skus` — индивидуальная рекомендация для каждого SKU:
  - `critical`: «Вывезти или списать» (0 продаж, мёртвый сток)
  - `high`: «Скидка 30-50% или вывоз» (низкие продажи) / «Скидка 20-30% + промо» (оборач. >300д)
  - `medium`: «Ускорить продажи» (рекламой + акции) / «Снизить цену 15-20%» (warning)
  - `low`: «Не поставлять новые» (есть продажи, но избыточный запас)

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Карточки по складам → одна сводка-баннер (кол-во SKU + прогноз расходов + кол-во мёртвого стока)
- Колонка «Складов» → «Рекомендация» с action + reason для каждого SKU

**Types** (`warehouses.ts`):
- `StorageRiskSku` + `recommendation: { action, reason, severity }`

## 2026-03-11 (v4)

### fix(warehouses): читаемость рекомендаций + логика распределения по складам

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Шрифты увеличены (14px заголовки, 13px impact), контрастные `dark:`/light модификаторы
- `bg-orange-50`/`bg-blue-50` + `text-orange-800`/`text-blue-800` для light theme

**Backend** (`warehouses.py`):
- `move_stock` полностью переписан: распределение по **нескольким складам**
- Расчёт capacity каждого склада-получателя (90 дней запаса)
- Проекция оборачиваемости: `[склад: Xд → Yд запаса]` — видно что будет после перемещения
- Лимит: каждый склад получает только столько, сколько может продать за 90 дней

## 2026-03-11 (v3)

### fix(warehouses): фидбек — автокомплит, география продаж, рекомендации

**Backend** (`warehouses.py`):
- SQL запрос `sku_sales_geo_data` — продажи по SKU × cluster_to (fact_ozon_orders)
- `sales_clusters` в `sku_geography` — кластеры с заказами, шт, выручкой

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Кастомный автокомплит вместо нативного select (поиск по артикулу/название/SKU)
- **«Где лежит» + «Куда продаётся»** — два side-by-side блока в географии товара
- Inline рекомендации: добавлен impact, action items, потенциал экономии в ₽/мес

**API types** (`warehouses.ts`): добавлен `SalesCluster`

## 2026-03-11 (v2)

### feat(warehouses): редизайн «Аналитика складов» — табовый дашборд

**Backend** (`warehouses.py`):

- Автогенерация текстового `summary` — сводка проблем (платное хранение, перезатарка, кроссдокинг, потенциал экономии, скорость доставки)
- `sku_geography` — инвертированный вид warehouse→SKU для географии по конкретному товару

**Frontend** (`WarehouseAnalyticsPage.tsx`):

- **4 вкладки** вместо простыни ~5000px:
  - **Обзор**: текстовое саммари + KPI с бенчмарками/цветовой индикацией + расходы + top-3 проблемных SKU
  - **Хранение**: StorageRiskTable + фильтры (Платное/Скоро/Все) + поиск + inline рекомендации
  - **Кроссдокинг**: CrossdockingTable + фильтры (Переместить/Поставить/Все) + поиск + inline рекомендации
  - **Склады и география**: WarehouseTable + SKU geography selector + фильтры по статусу
- KPI карточки: цветовая индикация (🔴 Критично /🟡 Внимание / 🟢 Норма) + бенчмарки (160дн, 48ч)
- Кнопки навигации из саммари → конкретная вкладка
- Убран отдельный блок «Рекомендации» — встроены inline в контексте каждой вкладки

**API types** (`warehouses.ts`): добавлены `SkuGeography`, `SkuGeographyWarehouse`, обновлен `WarehouseAnalyticsResponse`

## 2026-03-11

### feat(finances): WB Excel финансовый отчёт (6 листов)

**Новый endpoint** `GET /api/v1/finances/wb/excel`:

- 6 листов Excel: Сводка, По дням, По неделям, По месяцам, По товарам, Расходы детально
- Полная ретроспектива по неделям и месяцам (вся история магазина)
- KPI waterfall: выручка → комиссия → payout → расходы → прибыль
- Форматирование: цветовые % (зелёный/красный), формат дат «2 март. — 8 март.»

**Ключевые исправления:**

- **Двойной учёт рекламы устранён:** `fact_advert_stats_v3` и `deduction` с «продвижение» — один и тот же расход. Оставлен только «ВБ Промо» (из удержаний), убрана дублирующая «Реклама (внешн.)» из сводки, недельного и месячного отчётов
- **penalty_total fix:** для `operation_type='Удержание'` поле `penalty_total` дублирует `deduction`. Теперь фильтруется `operation_type!='Удержание'`
- **SKU ad spend fix:** маппинг `nm_id → vendor_code` через `fact_finances` → `fact_advert_stats_v3` по `nm_id` (ранее join по vendor_code давал нули)
- **Формула прибыли:** `Payout − Логистика − Хранение − Приёмка − Удержания − ВБ Промо − Штрафы − COGS`
- **Расходы детально:** исключены Продажа/Возврат (выручка, не расходы)
- **Content-Disposition:** RFC 5987 для кириллических имён файлов

**Frontend:**

- Кнопка «Скачать → Excel отчёт» теперь работает для обоих маркетплейсов (Ozon и WB)
- `downloadWbExcelReport()` в `finances.ts`

### docs: обновление архитектурной документации

- `04_BACKEND_API.md`: секция Финансов — 8 endpoints (добавлены excel, weekly-report), описание 6 листов Excel, WB-специфика

## 2026-03-10 (v2)

### feat(warehouses): WB Supply Frontend — единый интерфейс поставок

**Frontend** (`WarehouseSupplyPage.tsx`):

- Единый `WarehouseSupplyPage` — авто-переключение Ozon/WB по `currentShop.marketplace`
- WB-компоненты: `WBContent`, `WBSupplyTable`, `WBWarehouseSummaryTable`, `WBSettingsPanel`, `WBStatusBadge`
- 4 KPI: Итого поставить, Критические SKU, Перезатарка, Хранение/мес (₽)
- Табы: «По товарам» (expandable → склады) / «По складам» (сводка)

**Backend** (`warehouses.py`):

- `GET /warehouses/wb/supply` — JSON endpoint (рекомендации по поставке WB)
- `_build_wb_supply_data()` — общий helper для JSON и Excel endpoints
- Fix: `decimal.Decimal / float` TypeError, `ModuleNotFoundError` для Redis

### fix(warehouses): исправлена логика хранения WB

- Хранение WB **платное с 1-го дня** (не «60 дней бесплатно»)
- 60/90 дней — срок фиксации коэффициентов поставки (60 — большинство, 90 — одежда/обувь)
- Overstock = `turnover_days > target_days` (настраиваемый), не `> 60`
- Acceptance: «Без коэфф.» вместо «Бесплатно»

### docs: обновление архитектурной документации

- `04_BACKEND_API.md`: секция WB Supply (endpoints, response schema, WB-специфика, источники данных)
- `06_FRONTEND.md`: единый `WarehouseSupplyPage` (Ozon + WB ветки, WB-компоненты)

## 2026-03-10

### feat(warehouses): WB Supply — поставки с учётом платного хранения

**Новый endpoint** `GET /api/v1/warehouses/wb/supply/xlsx`:

- 4 листа Excel: Рекомендации, Сводка, Тарифы складов, Риск хранения
- Данные из: `fact_inventory_snapshot`, `fact_orders_raw`, `dim_products`, `fact_wb_acceptance_tariffs`
- Оборачиваемость по складам, расчёт стоимости хранения (объём × тариф × коэффициент)
- Рекомендации: `need = max(0, ceil(daily × target_days × safety − stock))`

**Новая инфраструктура:**

- Таблица CH `fact_wb_acceptance_tariffs` — тарифы хранения/логистики по складам
- WB API `/api/tariffs/v1/acceptance/coefficients` — 144 склада, 14 дней
- `WBTariffsService` + Celery task `sync_wb_tariffs` (ежедневно)

### docs: обновление архитектурной документации

- `02_DATA_MODEL.md`: добавлена таблица `fact_wb_acceptance_tariffs` (14 колонок, ReplacingMergeTree)
- `03_CELERY_PIPELINE.md`: добавлена задача `sync_wb_tariffs`, обновлён `sync_all_daily` dispatch
- `04_BACKEND_API.md`: добавлен endpoint `GET /warehouses/wb/supply/xlsx` (4 листа Excel)
- `05_SERVICES.md`: добавлен `WBTariffsService` (22 сервиса), обновлена сводная таблица

### fix(warehouses): исправлены подписи Excel на русский язык

- Все заголовки столбцов переведены на русский (полные названия с единицами измерения)
- Приёмка: «Бесплатно» / «Платно x20» вместо «Free» / «0.0»
- Рекомендации: развёрнутые русскоязычные тексты вместо «CRIT», «OK»
- Отгрузка: «Да» / «Нет» вместо «OK» / «X»
- Оборачиваемость: «Нет продаж» вместо числа 999
- Исправлено: `product_name` → `name` в запросе к `dim_products`

### feat(warehouses): Excel лист «Анализ логистики» (Ozon) — доля влияния маршрутов

- 6-й лист в Excel-экспорте Ozon: «Анализ логистики» по объединённым группам
- Формула: `объём × часы` для маршрутов >29ч (методика Ozon)
- Автогенерация рекомендаций по сплиту поставки
- Цветовая индикация: красный ≥40% влияния, оранжевый ≥20%

## 2026-03-09 (v2)

### feat(finances): Понедельный финансовый отчёт (Ozon)

**Backend** (`backend/app/api/v1/finances.py`):

- Новый endpoint `GET /finances/ozon/weekly-report` — понедельная агрегация P&L за всю историю магазина
- SQL-запросы к `fact_ozon_transactions` с `GROUP BY toMonday()` + рекламные расходы из `fact_ozon_ad_daily`
- COGS-расчёт через `product_costs` (PG) + SKU→offer_id маппинг из `dim_ozon_products`
- 20 колонок: qty, продажи, возвраты, комиссия, компенсации, услуги, продвижение, прочие, ФБО, эквайринг, доставка, к перечислению, себестоимость, ВАЛ + 6 процентных

**Frontend**:

- Новый компонент `WeeklyReportTable.tsx` — Excel-style таблица с:
  - Sticky-колонки (год, неделя, период)
  - Процентные колонки с зелёным фоном
  - Сортировка по всем колонкам, строка «Итого»
  - Экспорт в CSV
- Табы **«P&L»** / **«Пон. отчёт»** на странице Финансов (для Ozon и WB магазинов)
- Lazy-загрузка данных при переключении на таб

### feat(finances): Понедельный финансовый отчёт (WB)

**Backend** (`backend/app/api/v1/finances.py`):

- Новый endpoint `GET /finances/wb/weekly-report` — понедельная агрегация P&L для WB
- SQL-запросы к `fact_finances FINAL` с `GROUP BY toMonday(event_date)` + рекламные расходы из `fact_advert_stats`
- COGS-расчёт через `product_costs` (PG) по `vendor_code`
- 18 колонок: qty, цена розн., реализовано, комиссия WB, возвраты (шт/сум), логистика, хранение, приёмка, удержания, компенсации, реклама, к перечислению, себестоимость, ВАЛ + 4 процентных

**Frontend**:

- `WeeklyReportTable.tsx` теперь поддерживает оба маркетплейса через prop `marketplace`
- Отдельные наборы колонок: OZON_VALUE_COLS / WB_VALUE_COLS
- Таб «Пон. отчёт» показывается и для WB магазинов

### fix(finances): Ozon Logistics split

- Исправлена разбивка Logistics по `operation_type` вместо `delivery_schema` (который пуст для bulk-операций)
- Добавлена колонка «Хранение» (category='Storage')
- Переименованы колонки: «Др. услуги» → «Логистика», «Усл. ФБО» → «ФБО/Поставки»

---

## 2026-03-09

### feat(warehouses): Консолидация поставок по приоритетным кластерам Ozon

**Backend** (`backend/app/api/v1/warehouses.py`):

- Матрица `DELIVERY_HOURS` 25×25 — нормативное время доставки между кластерами (из Ozon 01/2026)
- `_resolve_hub()` — определение оптимального склада отгрузки для кластера спроса
- `hub` / `hub_hours` поля в `SupplyCluster` response
- `hubs` — агрегация поставки по складам отгрузки (HubSummary)
- Excel-экспорт: новый 4-й лист «Поставка по кластерам»
- Excel-экспорт: новый 5-й лист **«Объединённые кластеры»** — 9 хабов вместо 25, сводная поставка
- Excel лист 1: добавлены колонки «Склад отгрузки» и «Доставка, ч» с цветовой индикацией
- `CONSOLIDATED_GROUPS` — 9 объединённых групп кластеров (Москва+Тверь+Ярославль+Беларусь и т.д.)

**Frontend**:

- Новые типы `HubItem`, `HubSummary` в `warehouses.ts`
- Табы «По SKU» / «По кластерам» — переключение между таблицами
- Компонент `HubTable` — collapsible список складов → SKU с need > 0
- Цветовая индикация времени доставки (28ч зелёный, 45ч жёлтый)

---

### feat(warehouses): Раздел «Поставки FBO» — рекомендации по SKU × кластер

**Backend** (`backend/app/api/v1/warehouses.py`):

- `GET /warehouses/ozon/supply` — JSON-рекомендации по SKU × кластер
  - 4 запроса ClickHouse: FBO stocks, sales × cluster, ad boost (7d vs prev 7d), ad metrics
  - Product info из PostgreSQL (dim_ozon_products)
  - Параметры: `sales_period` (14-90), `target_days` (14-90), `safety` (1.0-2.0), `use_ad_boost`
  - Формула: `daily × target_days × safety − FBO_stock × доля`
  - Ad boost: `min(рост_7д / prev_7д, 2.0)`, для новых SKU с рекламой = 1.3x
- `GET /warehouses/ozon/supply/export` — Excel (3 листа: Рекомендации, Сводка, Методология)
- Зарегистрирован `warehouses_router` в `router.py`

**Frontend**:

- `warehouses.ts` — API клиент + TypeScript типы (SupplyItem, SupplyCluster, SupplyResponse)
- `WarehouseSupplyPage.tsx` — полная страница:
  - 4 KPI карточки: Итого поставить, Критические SKU, На внимании, Средний запас
  - Панель настроек: горизонт (14/30/45/60/90), период продаж, safety (0-30%), ad boost toggle
  - Таблица SKU с сортировкой, статус-бейджами, раскрытием кластеров по клику
  - Кнопка «Скачать Excel» (blob download)
- Sidebar: «Склады» → collapsible с children «Поставки»
- App.tsx: роут `/warehouses/supply`

---

### feat: ИИ-анализ событий — Gemini 2.5 Flash (kie.ai)

**Backend** (`events_analysis.py`):

- `POST /events/analysis` — SSE streaming endpoint
- Собирает события из PostgreSQL + KPI из ClickHouse
- **Каталог товаров**: названия + артикулы продавца из `dim_products` / `dim_ozon_products`
- **Привязка событий к товарам**: каждое событие содержит название товара по nm_id
- **Per-product funnel**: показы, клики, корзины, заказы, CTR%, CR→корз%, CR→заказ%
- Формирует промпт с данными, отправляет в Gemini 2.5 Flash через kie.ai API
- Стримит ответ обратно клиенту как Server-Sent Events
- Поддержка Ozon и WB (таблицы заказов/рекламы per marketplace)

**Frontend** (`EventsGraphPage.tsx`, `events_graph.ts`):

- Кнопка «Запустить анализ» в блоке «ИИ-анализ событий»
- SSE-клиент на базе `fetch` + `ReadableStream`
- Стриминг текста с markdown-рендерингом (заголовки, списки, bold, таблицы)
- Loading state, кнопка «Остановить», перезапуск анализа
- **Сброс анализа при смене магазина**: abort stream + reset state в useEffect по `[shop, period, groupBy]`

**Конфигурация**:

- `KIE_AI_API_KEY` в `.env`
- `router.py`: подключён `events_analysis_router`

**Документация**:

- `04_BACKEND_API.md`: секция «ИИ-анализ событий — /api/v1/events/analysis» — endpoint, request, механизм, SSE response
- `06_FRONTEND.md`: секция EventsGraphPage — AI analysis card, SSE streaming, сброс при смене магазина

---

## 2026-03-08

### feat(wb): критические события полного отсутствия на складах

- `STOCK_OUT_FBO_TOTAL` — товар закончился на **всех** складах ФБО
- `STOCK_OUT_FBS_TOTAL` — товар закончился на **всех** складах ФБС
- Агрегация: суммирует остатки по всем складам каждого типа (FBO/FBS)
- Redis: `state:stock_total:{shop_id}:{nm_id}:{fbo|fbs}` — хранит предыдущий агрегат
- Фронтенд: ⚠️ AlertTriangle, красная рамка + ring, визуально отличается от обычного STOCK_OUT

### fix(wb-ads): названия кампаний + ставки в рублях

**Проблема 1**: Рекламные события показывали только ID кампании (напр. «Кампания #34293797»)
без названия.

**Решение**: `event_detector.py` V1/V2 — добавлен `campaign_title` из `campaign.name`
в `event_metadata` для всех `BID_CHANGE` и `STATUS_CHANGE` событий.

**Проблема 2**: Ставки WB хранятся в копейках (100000 = 1000₽), но отображались как
«100 000 ₽» (без деления на 100).

**Решение**: `events.py` — `_format_value` конвертирует kopecks ÷ 100 → «1000 ₽».

### feat(wb): Content-Length based photo change detection

**Проблема**: WB CDN не меняет URL при замене фото — файл перезаписывается по тому же пути.
`extract_photo_id` извлекал `vol/part/nmID/N` — идентичный до и после замены.
ETag и Last-Modified нестабильны (round-robin между CDN нодами).

**Решение**: `Content-Length` из HTTP HEAD запросов — единственный стабильный заголовок
между CDN нодами, меняется только при реальной замене файла.

- `wb_content_service.py`: async HEAD запросы к CDN (`_fetch_photo_fingerprints`)
  - 20 параллельных запросов, timeout 5с, fallback на path-based ID
  - `main_photo_id` = Content-Length главного фото
  - `photos_hash` = MD5 от JSON массива Content-Length всех фото
- `event_detector.py`: `elif` → `if` — main photo и gallery детектируются независимо
  - `CONTENT_PHOTO_ADDED` — фото добавлено (count↑)
  - `CONTENT_PHOTO_REMOVED` — фото удалено (count↓)
  - `CONTENT_PHOTO_ORDER_CHANGED` — фото заменено (count=, hash≠)
- `events.py`: категории, лейблы, детализация для новых типов
- `EventsPage.tsx`: иконки — зелёный Plus (добавлено), красный Minus (удалено)

---

## 2026-03-03

### feat: WB LTV — полный модуль анализа повторных покупок

**Открытие**: обнаружен buyer_id в поле `srid` заказов WB.  
Формула: `substring(splitByChar('.', srid)[1], 1, 8)` для числовых srid 16-19 символов.  
Точность: 97.1% (подтверждена на 2 магазинах, cross-validated с Ozon).

**Backend** (`backend/app/api/v1/wb_ltv.py`):

- `GET /sales/wb/ltv` — KPI, когортная матрица, SKU repeat table, time distribution
- `GET /sales/wb/ltv/chain` — цепочка покупок L1→L5 для конкретного nm_id
- Buyer ID extraction из srid, фильтр числовых srid 16-19 символов
- Зарегистрирован в `router.py`

**Frontend**:

- `frontend/src/api/wb_ltv.ts` — API модуль с типами
- `LtvPage.tsx` расширена для WB: автовыбор API (Ozon/WB) по marketplace магазина

### fix: WB LTV — артикулы и картинки из dim_products

- **Было**: искал в несуществующей `dim_wb_products` → числовые артикулы `01-0001034`
- **Стало**: `dim_products` (name, vendor_code) + CDN `wb_image_url(nm_id)` — как dashboard WB

### docs: архитектурная документация LTV

- `04_BACKEND_API.md`: секция «Клиентская аналитика (LTV)» — 4 endpoints, response schemas, логика Ozon/WB
- `06_FRONTEND.md`: секция `LtvPage` (~710 строк), routing, sidebar «КЛИЕНТЫ»
- Роутинг обновлён: 12 роутеров (добавлены ltv_router, wb_ltv_router)

---

## 2026-02-28

### fix(sales): sticky header/column + обрезка названий

- Первый столбец «Товар» закрепляется при горизонтальном скроле
- Строка заголовков закрепляется при вертикальном скроле (max-h 600px)
- Названия товаров обрезаны до 220px вместо полных длинных строк
- Непрозрачный фон sticky-ячеек через inline styles

## 2026-02-28

### ui(sales): полный редизайн ABC/XYZ

- Карточки: 2 группы (ABC + XYZ) бок о бок, градиенты, крупные цифры
- Матрица: увеличенные ячейки, эмодзи-индикаторы, подписи, hover-эффекты
- Таблица: +Маржа%, +МП расх., +Реклама — полная финансовая картина
- Склонение: товар/товара/товаров
- Названия товаров: шире (min-w-320px), zebra-striping

## 2026-02-28

### fix(sales): ABC/XYZ — чистая прибыль вместо валовой

- **Было**: `profit = revenue - cogs` (только себестоимость)
- **Стало**: `profit = revenue - commission - logistics - storage - acquiring - ad_spend - cogs`
- Источники: `fact_ozon_transactions`, `fact_ozon_ad_daily`, `product_costs`
- Bulk charges (Storage/Acquiring) распределяются пропорционально выручке

## 2026-02-28

### feat(sales): подменю «Продажи» + ABC/XYZ анализ

- **Sidebar рефакторинг**: «Продажи» → collapsible группа с подпунктами:
  - «Обзор продаж» (`/sales`) — прежняя страница
  - «ABC/XYZ анализ» (`/sales/abc-xyz`) — новая страница
- **Backend**: `GET /sales/ozon/abc-xyz` (period, use_profit)
  - ABC: кумулятивная доля выручки/прибыли (A≤80%, B≤95%, C>95%)
  - XYZ: CV понедельных продаж (X<10%, Y<25%, Z≥25%)
- **Frontend**: AbcXyzPage — 6 summary cards, матрица 3×3, сортируемая таблица
- **API**: `abc-xyz.ts` — TypeScript интерфейсы + fetch

### feat(sales): сортировка столбцов таблицы товаров

- SortKey/SortDir state с дефолтной сортировкой по выручке
- Клик по заголовку → ▲/▼ индикатор + сортировка ASC/DESC

### feat(sales): колонка «Цена» с дельтой периода

- Средняя цена товара + изменение в % относительно предыдущего периода

## 2026-02-26

### feat(finances): товарная P&L таблица — детализация по товарам

- `GET /finances/wb/products` — WB P&L по vendor_code (fact_finances + ads + COGS)
- `GET /finances/ozon/products` — Ozon P&L по offer_id (orders + transactions + ads + COGS)
- Фронтенд: `ProductFinanceTable` — сортировка, delta %, цвет маржи, sticky итого
- Колонки WB: Продажи | Выручка | Логистика | Хранение | Реклама | С/с | Прибыль | Маржа
- Колонки Ozon: Продажи | Выручка | Комиссия | Логистика | Реклама | С/с | Прибыль | Маржа

### fix(finances): Ozon — правильные таблицы и поля

- `fact_ozon_finances` (не существует) → `fact_ozon_transactions`
- `sku_id` → `sku`, `spend` → `money_spent` в fact_ozon_ad_daily
- Bulk charges (Логистика/Хранение/Эквайринг) — распределение пропорционально выручке

### fix(finances): WB — двойной учёт рекламы

- Исключены рекламные услуги ("ВБ.Продвижение") из поля `deduction` (удержания) в отчётах реализации, так как расходы на рекламу отдельно запрашиваются из `fact_advert_stats_v3`.

## 2026-02-24

### feat(wb): P&L водопад — новые столбцы

- `revenue_7d` = `retail_amount` (продажи для покупателя)
- `sales_amount` = `current_price × qty` (продажи по цене из админки)
- `payout` = `ppvz_for_pay` (к перечислению от WB)
- `avg_price` = `retail_amount / qty` (средняя цена покупателя)
- `profit` = `payout - mp_fees - COGS - ads`

### fix(frontend): баг таймзоны

- `toISOString().slice(0,10)` → локальное форматирование
- Даты больше не сдвигаются на -1 день при UTC+5

### ui(table): редизайн таблицы товаров

- 11→9 столбцов (убраны Ср. цена, Возвр., События)
- DRR: цветной текст вместо бейджа
- Остатки: одно число + тултип FBO/FBS
- Дельта: инлайн после штук
- Единый 2-строчный формат ячеек
- Фиксированные ширины (940px), без горизонтального скролла

### fix(wb): UnboundLocalError sales_amount — пустая таблица

- `sales_amount` определялся ПОСЛЕ использования в формуле DRR
- Python выбрасывал `UnboundLocalError`, API `/products/wb` возвращал 500
- Перемещён `current_price` + `sales_amount` перед расчётом DRR

### feat(tooltip): подробная разблюдовка удержаний Услуги МП

- Тултип при наведении: Комиссия / Логистика / Хранение / Прочее
- Каждая статья: сумма + % от продаж
- Нулевые статьи скрываются автоматически

### fix(drr): ДРР считается от продаж (не от выплаты)

- Было: ДРР = ad_spend / payout (ppvz_for_pay)
- Стало: ДРР = ad_spend / sales_amount (цена × шт)

### fix(ux): тултипы маркетплейс-нейтральные + баннер 7 дней

**Тултипы заголовков:**

- Убраны все упоминания «WB», «ppvz_for_pay», «СПП»
- Тексты универсальны для WB и Озон: «маркетплейс» вместо «WB»
- Понятный язык: «расчётный счёт» вместо «р/с (ppvz_for_pay)»

**Баннер 7 дней:**

- Amber-предупреждение перед таблицей
- «Данные за 7 дней могут быть неполными»
- Объясняет задержку финансового отчёта маркетплейса 1-3 дня
- Скрывается при 30 днях или пользовательских датах

### feat(ozon): avg_price — реальная цена покупателя после скидок Ozon

**Проблема:** Ср. цена для Озон = цена из ЛК (до скидок площадки). На самом деле покупатель платит меньше.

**Решение:** SQL-запрос к `fact_ozon_orders`:

- `buyer_revenue = (price - total_discount_value) × qty`
- `avg_price = buyer_revenue / orders`
- `sales_amount = avg_price × orders`

**Пример:** `АМ-СОБ-МЕЛ-ЯГ-1` — цена ЛК 953₽, реальная 634₽ (скидка ~34%)

### fix: С/с процент = доля себестоимости в цене

**Проблема:** Тултип говорил «изменение за период» — бессмыслица для ручного поля.
**Решение:** `margin_percent = cost / price × 100` (доля С/с в цене из ЛК).
Цвета: зелёный <30%, жёлтый 30-50%, красный >50%.

## 2026-02-25

### fix(wb): С/с% = cost/price, Прибыль% от цены продажи

- **С/с%**: было `grossProfitPct` (от выплаты, мог быть отрицательный) → `(cost + packaging) / price × 100` (всегда положительный)
- **Прибыль%**: было `profit / payout` → `profit / sales_amount` (от цены продажи)
- Backend WB: `margin = gross_profit / sales_amount` (ранее: / payout)

### fix(ozon): формула прибыли — revenue вместо txn_payout

- Было: `profit = txn_payout - COGS - ad_spend` (txn_payout не привязан к периоду заказов, за w08 = -33k!)
- Стало: `profit = revenue - COGS - mp_fees - ad_spend` (mp_fees = revenue - txn_payout)
- Результат: прибыль математически идентична, но привязана к дате заказа

### fix(ozon): totals — Выплата и Ср. выплата

- `totals.payout` добавлен в API (было 0₽)
- `totals.avg_price` = `payout / orders` (было пусто)
- Фронтенд: ячейка «Ср. выпл.» в итоговой строке (было пустой `<td/>`)

### docs: обновлена архитектурная документация

- `04_BACKEND_API.md`: формула прибыли Ozon, totals schema, margin_percent
- `06_FRONTEND.md`: столбцы таблицы, wbToOzon маппинг, тултипы С/с

## 2026-02-28 — WB Sales Overview (Обзор продаж для Wildberries)

### Backend

- `GET /api/v1/sales/wb` — KPI (заказы, выручка, ср. чек, отмены), дневной график, география (top-20 регионов), top-20 товаров с органической воронкой из `fact_sales_funnel`
- `GET /api/v1/sales/wb/product-daily` — дневная динамика по конкретным товарам (для оверлея на графике)

### Frontend

- `sales.ts`: добавлены `getWbSalesApi()` и `getWbProductDailyApi()`
- `SalesPage.tsx`: адаптирован для обоих маркетплейсов:
  - Автоопределение маркетплейса → вызов нужного API
  - KPI: «Отмены» (XCircle) для WB, «Возвраты» (RotateCcw) для Ozon
  - График: легенда показывает «Отмены» для WB
  - Таблица: «Воронка» для WB (органическая), «Рекл. воронка» для Ozon
  - Блок «Причины возвратов» скрыт для WB (нет данных)

### Исправление: Рекламная воронка WB (fix)

- Заменён источник данных воронки: `fact_sales_funnel` (органическая) → `fact_advert_stats_v3 FINAL` (рекламная)
- Теперь корректные значения: views (рекл. показы), clicks (клики), atbs (корзины), orders (заказы), spend (расходы)
- CTR = clicks/views, CR→корз. = atbs/clicks, CR→заказ = orders/atbs
- Столбцы «Клики» и «CTR» отображаются для WB (ранее ошибочно скрывались)

## 2026-02-28 — WB ABC/XYZ анализ

### Backend

- `GET /api/v1/sales/wb/abc-xyz` — полный ABC/XYZ анализ для товаров WB
  - Revenue/costs: `fact_finances FINAL` (per vendor_code/nm_id)
  - Ad spend: `fact_advert_stats_v3 FINAL` (per nm_id)
  - COGS: `product_costs` (PG, per vendor_code)
  - Product info: `dim_products` (PG, per nm_id)
  - Weekly: `fact_orders_raw FINAL` для XYZ (per nm_id)
  - Commission = revenue - payout (модель WB)

### Frontend

- `abc-xyz.ts`: добавлена `fetchWbAbcXyz()`
- `AbcXyzPage.tsx`: авто-определение маркетплейса → вызов нужного API

## 2026-02-28 — Раздел «Прогноз продаж» для Ozon

### Backend

- `GET /api/v1/sales/ozon/forecast` — трендовый прогноз + unit economics
  - History: дневная выручка/заказы из `fact_ozon_orders`
  - Forecast: линейная регрессия на Python (без numpy) + доверительный коридор ±σ
  - Per-product: CPO, CPC, CTR, CR, ROI из `fact_ozon_ad_daily` + `fact_ozon_transactions`
  - COGS из `product_costs`, info из `dim_ozon_products`

### Frontend

- `ForecastPage.tsx` — комбо-раздел:
  - Трендовый график (Recharts AreaChart + Line, история + прогноз с доверительным коридором)
  - 4 KPI карточки: прогноз выручки/заказов, тренд, направление
  - Симулятор «Что если?» — ползунки бюджета рекламы (×0.5—3.0) и цены (×0.8—1.2) per-product
  - Мгновенный JS-пересчёт заказов/выручки/прибыли при перемещении ползунков
- `forecast.ts` — API клиент с TypeScript типами
- Роут `/sales/forecast`, навигация в Sidebar (иконка Sparkles)

## 2026-02-28 — NeuralProphet вместо линейной регрессии

### Backend

- Заменена линейная регрессия на **NeuralProphet 0.9.0** (PyTorch)
  - Мультипликативная недельная сезонность
  - Авторегрессия AR(7) — последние 7 дней влияют на прогноз
  - `n_forecasts=horizon` + диагональное извлечение yhat
  - Доверительный интервал из исторической волатильности
  - Fallback на 7-дневное скользящее среднее при ошибке модели

### Зависимости

- `neuralprophet==0.9.0`, `torch==2.2.0` (CPU-only), `pandas<3.0`
- Dockerfile обновлён: `--index-url https://download.pytorch.org/whl/cpu`
- `.gitignore`: добавлен `backend/lightning_logs/`

### Устранённые проблемы

- pandas 3.0 → ошибка `Series.view()` (фикс: pandas<3.0)
- torch 2.10 → несовместимость API (фикс: torch==2.2.0)
- `n_forecasts` не был задан → только 1 день прогноза
- Диагональное извлечение: yhat1 для дня 1, yhat2 для дня 2, и т.д.

## 2026-02-28 — LightGBM per-SKU прогноз с воронкой

### Backend

- Добавлен **LightGBM** per-SKU прогноз с 21 lagged фичей (показы, клики, корзины, рекл. расход за 1-3 дня)
- Новый endpoint `GET /sales/ozon/forecast/sku` — прогноз заказов, выручки, прибыли по каждому SKU
- Feature importance — показывает какой фактор больше всего влияет на продажи каждого SKU
- Формула прибыли: выручка − комиссия − логистика − реклама − себестоимость
- Зависимости: `lightgbm>=4.0`, `scikit-learn`

### Frontend

- Новая секция «Прогноз по SKU (LightGBM)» на странице прогноза
- Кнопки выбора SKU (топ-5 по выручке), KPI карточки, график заказов с confidence band
- Панель «Влияние факторов» — визуальные бары feature importance
- Блок «Структура прибыли» — разбивка: выручка, комиссия, логистика, реклама, чистая прибыль

### Инфраструктура

- Nginx proxy timeout увеличен с 60с до 180с для ML-эндпоинтов
- Dockerfile: torch незафиксирован (2.2.0 удалён с CDN PyTorch)

## 2026-03-01 — Очистка ветки: удалён ML-код для деплоя

### Удалено из feature/sales-page

- NeuralProphet + LightGBM эндпоинты и хелперы из `sales.py` (−942 строки)
- `ForecastPage.tsx`, `forecast.ts` (удалены)
- ML-зависимости: `torch`, `neuralprophet`, `lightgbm`, `scikit-learn`, `pandas`
- Torch из Dockerfile / Dockerfile.prod
- Роут `/sales/forecast` из App.tsx и Sidebar.tsx
- Nginx timeout возвращён с 180с на 60с

### Сохранено

- Весь ML-код сохранён в ветке `feature/forecast-ml`

### Исправлено

- docker-compose.yml: nginx переключён с prod-конфига (SSL) на dev-конфиг
- Docker build теперь 60с вместо 5+ минут

## 2026-03-01 — Обновление архитектурной документации

### 04_BACKEND_API.md

- Добавлена секция «Продажи — /api/v1/sales» (6 endpoints)
- Добавлена секция «Финансы — /api/v1/finances» (4 endpoints)
- Роутинг обновлён: 10 роутеров вместо 6
- Удалены forecast endpoints (код в feature/forecast-ml)

### 06_FRONTEND.md

- Добавлены секции SalesPage (1019 строк) и AbcXyzPage (489 строк)
- Routing обновлён: 6 активных роутов
- Убран «Прогноз» из sidebar

### 07_INFRASTRUCTURE.md

- Документировано различие nginx.conf (dev) и nginx.prod.conf (prod)
- ML зависимости удалены, Docker build ~60с

## 2026-03-01

### fix(forecast): WB profit calculation — real mp_fees from fact_finances FINAL

- **Проблема**: forecast использовал `fact_finances_latest.retail_amount` для расчёта mp_fees,
  но у некоторых магазинов (shop_id=18) `retail_amount` всегда = 0.
  Fallback 5.5% давал нереально высокую прибыль (маржа 51% вместо 13.4%)
- **Решение**: перешли на `fact_finances FINAL` + `JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub')` —
  тот же подход что и раздел Финансов (`finances.py`)
- **Компоненты mp_fees**: Commission(rev-payout) + Logistics(wb_delivery_rub) + Storage + Acquiring + Penalties + Deductions
- **Fallback**: обновлён с 5.5% до 40%
- **Файл**: `backend/app/api/v1/sales.py` (endpoint `/sales/wb/forecast`)

## 2026-03-01

### ui(sales+finances): единый стиль таблиц — sticky + scroll

**SalesPage.tsx → TopProductsTable:**

- Обёрнут в `rounded-2xl` контейнер с title bar (убрана дублирующая Card)
- `max-h-[600px]` + `overflow-auto` — вертикальный скролл
- Sticky header (`sticky top-0 z-20`)
- Sticky первый столбец «Товар» (`stickyBase` + box-shadow)
- Zebra striping: `idx % 2 === 0 ? bg-card : bg-muted/0.06`
- Ячейки: `px-4 py-3.5`, `text-[13px]`, `font-semibold`
- Sort indicator: `text-[hsl(var(--primary))]` вместо opacity

**ProductFinanceTable.tsx:**

- `rounded-2xl` контейнер + title bar c счётчиком товаров
- `max-h-[600px]` + `overflow-auto`
- Sticky header (`sticky top-0 z-20`)
- Sticky footer «Итого» (`sticky bottom-0 z-20`) — всегда виден
- Sticky первый столбец с `stickyCol` + box-shadow
- Zebra striping, единые размеры ячеек

## 2026-03-01

### ui(sales+finances): unified table style — matching ABC/XYZ

**TopProductsTable** (SalesPage.tsx):

- Rounded-2xl card container с title bar
- `max-h-[600px]` scrollable area
- Sticky header (vertical z-20) + sticky first column 'Товар' (horizontal, box-shadow)
- Zebra striping (odd rows bg-muted/0.06)
- Убрана дублирующая Card-обёртка
- Единые padding/font-size ячеек

**ProductFinanceTable** (ProductFinanceTable.tsx):

- Тот же rounded-2xl card + title bar + счётчик товаров
- `max-h-[600px]` vertical scroll
- Sticky header (z-20) + sticky first column + **sticky ИТОГО footer** (bottom-0)
- Zebra striping, единые шрифты через HSL vars

## 2026-03-02

### docs: полный аудит и обновление архитектурной документации

**06_FRONTEND.md:**

- Добавлены секции ForecastPage (387 строк), WBProductsPage (644 строки), FinancesPage (1009 строк)
- Routing diagram: 7 активных + 4 placeholder маршрута (ранее неполный)
- Sidebar навигация: полная таблица с секциями, вложенным меню и статусами
- Unified Table Design: единый стиль таблиц (sticky header/column/footer, max-h-[600px], zebra)
- Компоненты: добавлены ProductFinanceTable, DateRangePicker, Badge
- API Layer: 9 модулей (добавлены forecast.ts, wb-products.ts)

**01_OVERVIEW.md:**

- Frontend routing diagram: 7 активных + 4 placeholder (ранее: 2 страницы)
- API Layer: 9 модулей с перечислением

**04_BACKEND_API.md:**

- Добавлена секция «Товары WB» — 4 endpoints (GET list, PATCH cost, POST bulk, GET template)
- Sales: +2 forecast endpoints (/ozon/forecast, /wb/forecast), итого 8
- forecast_engine.py — внутренняя утилита (не роутер)

## 2026-03-02

### feat: LTV клиентов Ozon — полный раздел анализа повторных покупок

**Backend (app/api/v1/ltv.py):**

- 2 API endpoints: `GET /sales/ozon/ltv` и `GET /sales/ozon/ltv/chain`
- KPI метрики: уникальные клиенты, повторные, retention rate, средний LTV, avg check
- Когортная retention матрица (месячные когорты с % удержания до +6 мес)
- Таблица SKU с повторными покупками (конверсии →2/→3, avg days between, LTV repeat)
- Распределение времени до повторной покупки (гистограмма по бакетам)
- Цепочка продаж L1→L5 (кросс-продажи после покупки конкретного SKU)
- ClickHouse-совместимые запросы (window functions, safe NaN handling)

**Frontend:**

- `ltv.ts` — API модуль с TypeScript типами
- `LtvPage.tsx` (673 строки) — KPI карточки, cohort heatmap, interactive chain visualization, SKU table с сортировкой/поиском, histogram
- Route `/customers/ltv` в App.tsx
- Sidebar: пункт «Клиенты → LTV анализ»
