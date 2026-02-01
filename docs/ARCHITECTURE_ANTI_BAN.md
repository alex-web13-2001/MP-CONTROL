# Архитектура защиты от банов маркетплейсов

> **Все запросы к WB/Ozon должны идти через `MarketplaceClient`!**

---

## Компоненты системы

```
                    ┌──────────────────┐
                    │  Celery Task     │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │ MarketplaceClient│
                    │  • Sticky proxy  │
                    │  • JA3 spoofing  │
                    └────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼───────┐   ┌────────▼───────┐   ┌───────▼───────┐
│ CircuitBreaker│   │ RedisRateLimiter│   │ ProxyProvider │
│ OPEN/CLOSED/  │   │ Sliding window │   │ Sticky +      │
│ HALF_OPEN     │   │ + Jitter       │   │ Quarantine    │
└───────┬───────┘   └────────┬───────┘   └───────┬───────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                      ┌──────▼──────┐
                      │    Redis    │
                      └─────────────┘
```

---

## Реализованные механизмы

### 1. Jitter (Thundering Herd)

При 429 ошибке добавляется **±10-30 сек** случайной задержки:

- 50 магазинов с 429 «просыпаются» в разное время
- Нагрузка размазывается равномерно

### 2. Circuit Breaker

| Состояние | Описание                           |
| --------- | ---------------------------------- |
| CLOSED    | Нормальная работа                  |
| OPEN      | 10+ ошибок auth → магазин отключен |
| HALF_OPEN | Через 1 час → пробуем восстановить |

**Логика:**

- 401 ошибки на разных прокси → Circuit открывается
- Shop.status → `auth_error`
- Пользователь обновляет ключ → Circuit сбрасывается

### 3. Sticky Sessions

Один прокси на один магазин во время задачи.

### 4. Карантин прокси

| Код | Время  | Причина        |
| --- | ------ | -------------- |
| 403 | 30 мин | IP забанен     |
| 429 | 15 мин | Rate limited   |
| 5xx | 5 мин  | Ошибка сервера |

---

## Использование

```python
# В Celery таске:
try:
    async with MarketplaceClient(db, shop_id=1) as client:
        response = await client.get("/orders")
except ShopDisabledError:
    # Магазин отключен — ключ невалиден
    logger.error("Shop disabled, needs API key update")
    return
```

---

## Мониторинг

```sql
-- Магазины с ошибками авторизации
SELECT * FROM shops WHERE status = 'auth_error';

-- Circuit Breaker состояние (Redis)
-- mms:circuit:{shop_id}:state
-- mms:circuit:{shop_id}:failures
```

---

## API для сброса

```python
# При обновлении API ключа пользователем:
from app.core.circuit_breaker import reset_shop_circuit
await reset_shop_circuit(shop_id, db)
```
