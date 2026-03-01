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

