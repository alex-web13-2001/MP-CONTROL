# MP-CONTROL — Backend API

> REST API на FastAPI. Все endpoints начинаются с `/api/v1/`.  
> Файлы: `backend/app/api/v1/` (5 роутеров), `backend/app/schemas/auth.py`

---

## Роутинг

```python
# backend/app/api/v1/router.py
router.include_router(auth_router)        # /api/v1/auth/*
router.include_router(shops_router)       # /api/v1/shops/*
router.include_router(commercial_router)  # /api/v1/commercial/*
router.include_router(finance_router)     # /api/v1/finance-reports/*
router.include_router(advert_router)      # /api/v1/advertising/*
```

---

## Аутентификация — `/api/v1/auth`

### Endpoints

| Метод  | Path             | Описание                                 | Auth   |
| ------ | ---------------- | ---------------------------------------- | ------ |
| `POST` | `/auth/register` | Регистрация нового пользователя          | —      |
| `POST` | `/auth/login`    | Авторизация, возврат JWT                 | —      |
| `POST` | `/auth/refresh`  | Обновление access token                  | —      |
| `GET`  | `/auth/me`       | Профиль текущего пользователя + магазины | Bearer |

### Schemas

```
RegisterRequest { email: EmailStr, password: str[6-128], name: str[1-255] }
LoginRequest    { email: EmailStr, password: str }
RefreshRequest  { refresh_token: str }

TokenResponse {
    access_token: str
    refresh_token: str
    token_type: "bearer"
    user: UserResponse
}

UserResponse {
    id: str (UUID)
    email: str
    name: str
    is_active: bool
    shops: ShopResponse[]
}

ShopResponse {
    id: int, name: str, marketplace: str,
    is_active: bool, status: str
}
```

### Логика

- `register`: проверка уникальности email → bcrypt hash → создание User → JWT пара
- `login`: поиск по email → bcrypt verify → JWT пара (access 120 мин, refresh 7 дней)
- `refresh`: decode refresh token → проверка type="refresh" → новая JWT пара
- `me`: `Depends(get_current_user)` → UserResponse с shops

---

## Управление магазинами — `/api/v1/shops`

### Endpoints

| Метод    | Path                           | Описание                                 | Auth   |
| -------- | ------------------------------ | ---------------------------------------- | ------ |
| `GET`    | `/shops`                       | Список магазинов текущего пользователя   | Bearer |
| `POST`   | `/shops`                       | Добавить новый магазин (ключи шифруются) | Bearer |
| `POST`   | `/shops/validate-key`          | Валидация API ключа маркетплейса         | Bearer |
| `GET`    | `/shops/{shop_id}/sync-status` | Статус первичной синхронизации           | Bearer |
| `PATCH`  | `/shops/{shop_id}/keys`        | Обновить API ключи                       | Bearer |
| `DELETE` | `/shops/{shop_id}`             | Удалить магазин и все данные             | Bearer |

### Schemas

```
ShopCreate {
    name: str, marketplace: "wildberries"|"ozon",
    api_key: str,
    client_id?: str,           // Ozon Seller Client-Id
    perf_client_id?: str,      // Ozon Performance Client-Id
    perf_client_secret?: str   // Ozon Performance Client-Secret
}

ValidateKeyRequest {
    marketplace: "wildberries"|"ozon",
    api_key: str,
    client_id?: str,
    perf_client_id?: str,
    perf_client_secret?: str
}

ValidateKeyResponse {
    valid: bool,
    seller_valid?: bool,     // Ozon seller check
    perf_valid?: bool,       // Ozon performance check
    message: str,
    shop_name?: str,         // Auto-detected
    warnings?: str[]         // Missing WB permissions
}
```

### Ключевая логика

- **create_shop**: Fernet-шифрование api_key → `api_key_encrypted`, perf_secret → `perf_client_secret_encrypted`. После создания → `load_historical_data.delay(shop_id)`.
- **validate_key (WB)**: проверяет `/ping` на 7 доменах API → warnings для отсутствующих прав.
- **validate_key (Ozon)**: проверяет Seller API + Performance API OAuth2 раздельно.
- **get_sync_status**: читает Redis `sync_status:{shop_id}` → progress (step, total, eta). Fallback: PostgreSQL `shop.status`.
- **delete_shop**: удаляет данные из ClickHouse (6 таблиц) + PostgreSQL (каскад) + Redis state.

---

## Коммерческий мониторинг — `/api/v1/commercial`

### Endpoints

| Метод  | Path                           | Описание                         | Auth |
| ------ | ------------------------------ | -------------------------------- | ---- |
| `POST` | `/commercial/sync`             | Старт синхронизации цен+остатков | —    |
| `POST` | `/commercial/sync-warehouses`  | Синхронизация складов            | —    |
| `POST` | `/commercial/sync-content`     | Синхронизация контента карточек  | —    |
| `GET`  | `/commercial/status/{task_id}` | Статус задачи Celery             | —    |
| `POST` | `/commercial/turnover`         | Расчёт оборачиваемости on demand | —    |

### Turnover API

```
POST /commercial/turnover
{
    shop_id: 1,
    nm_ids: [123, 456],  // optional
    days: 30
}

→ {
    products: [
        { nm_id, current_quantity, avg_daily_sales, turnover_days }
    ],
    total_products: int
}
```

**Формула:** `turnover_days = current_quantity / avg_daily_sales`  
**Источники:** ClickHouse `fact_inventory_snapshot` (остатки) + `fact_finances` (продажи)

---

## Финансовые отчёты — `/api/v1/finance-reports`

| Метод  | Path                                | Описание                         | Auth |
| ------ | ----------------------------------- | -------------------------------- | ---- |
| `POST` | `/finance-reports/sync`             | Старт синхронизации за N месяцев | —    |
| `GET`  | `/finance-reports/status/{task_id}` | Статус задачи                    | —    |
| `GET`  | `/finance-reports/list`             | Список загруженных файлов        | —    |

---

## Реклама — `/api/v1/advertising`

| Метод  | Path                            | Описание                      | Auth |
| ------ | ------------------------------- | ----------------------------- | ---- |
| `POST` | `/advertising/sync`             | Старт рекламной синхронизации | —    |
| `GET`  | `/advertising/status/{task_id}` | Статус задачи                 | —    |

---

## Общий паттерн async task endpoints

Все sync-endpoints возвращают `task_id` → клиент полит через GET `/status/{task_id}`:

```
1. POST /sync → Celery .delay() → { task_id: "uuid", message: "Started..." }
2. GET /status/{task_id} → AsyncResult → { status, progress?, result?, error? }
   - PENDING → STARTED → PROGRESS { current, total } → SUCCESS { rows_inserted }
   - FAILURE { error: "..." }
```

---

## Dependency Injection

```python
get_db()            → AsyncSession (PostgreSQL)
get_current_user()  → User (JWT decode → SELECT user + shops)
```

`get_current_user` используется как `Depends()` в auth/shops endpoints. Commercial/finance endpoints пока не защищены Bearer (принимают api_key в body).
