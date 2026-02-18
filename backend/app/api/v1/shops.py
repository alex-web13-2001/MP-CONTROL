"""
Shop management API endpoints.

GET    /shops                  — List user's shops
POST   /shops                  — Add a new shop (API keys encrypted)
POST   /shops/validate-key     — Validate marketplace API key
GET    /shops/{id}/sync-status  — Poll sync progress (Redis)
DELETE /shops/{id}              — Remove a shop
"""
import json
import logging
import os

import httpx
import redis
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.encryption import encrypt_api_key
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User
from app.schemas.auth import ShopCreate, ShopResponse, ShopUpdateKeys, ValidateKeyRequest, ValidateKeyResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/shops", tags=["shops"])


# ── Validation helpers ────────────────────────────────────────────

async def _validate_wb_key(api_key: str) -> tuple[bool, str, str | None, list[str]]:
    """
    Validate WB API key by pinging all required service domains.

    Checks /ping on each domain to verify the token has correct permissions.
    Returns: (valid, message, shop_name, warnings)
    """
    # Services we need for data collection
    WB_SERVICES = {
        "content-api":          "Контент (карточки товаров)",
        "statistics-api":       "Статистика (воронка продаж)",
        "marketplace-api":      "Маркетплейс (заказы, склады)",
        "advert-api":           "Реклама (кампании, ставки)",
        "discounts-prices-api": "Цены и скидки",
        "finance-api":          "Финансы (отчёты)",
    }

    accessible = {}
    warnings = []
    key_valid = False

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            for service, label in WB_SERVICES.items():
                try:
                    resp = await client.get(
                        f"https://{service}.wildberries.ru/ping",
                        headers={"Authorization": api_key},
                    )
                    if resp.status_code == 200:
                        accessible[service] = True
                    elif resp.status_code == 401:
                        accessible[service] = False
                        warnings.append(f"⚠️ Нет доступа: {label} ({service})")
                    else:
                        accessible[service] = False
                        warnings.append(f"⚠️ {label}: код {resp.status_code}")
                except Exception as e:
                    logger.error("WB ping %s failed: %s: %s", service, type(e).__name__, e)
                    accessible[service] = None
                    warnings.append(f"⚠️ Ошибка проверки: {label}")

        # Key is valid if at least one service responds 200
        ok_count = sum(1 for v in accessible.values() if v is True)
        total = len(WB_SERVICES)

        if ok_count == 0:
            return False, "Неверный API ключ. Проверьте ключ в личном кабинете WB.", None, warnings
        elif ok_count < total:
            key_valid = True
            msg = f"API ключ валиден ✅ ({ok_count}/{total} сервисов доступно)"
            return key_valid, msg, None, warnings
        else:
            return True, "API ключ Wildberries валиден ✅ (все сервисы доступны)", None, []

    except Exception as e:
        logger.error("WB key validation error: %s", e)
        return False, f"Ошибка соединения: {str(e)}", None, []



async def _validate_ozon_seller(client_id: str, api_key: str) -> tuple[bool, str, str | None]:
    """Test Ozon Seller API key."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api-seller.ozon.ru/v3/product/list",
                headers={
                    "Client-Id": client_id,
                    "Api-Key": api_key,
                    "Content-Type": "application/json",
                },
                json={"filter": {"visibility": "ALL"}, "limit": 1, "last_id": ""},
            )
        if resp.status_code == 200:
            return True, "Ozon Seller API валиден ✅", None
        elif resp.status_code in (401, 403):
            return False, "Неверный Client-Id или Api-Key Ozon.", None
        else:
            return False, f"Ошибка проверки Ozon Seller: код {resp.status_code}", None
    except Exception as e:
        logger.error("Ozon seller validation error: %s", e)
        return False, f"Ошибка соединения: {str(e)}", None


async def _validate_ozon_performance(client_id: str, client_secret: str) -> tuple[bool, str]:
    """Test Ozon Performance API credentials by fetching OAuth2 token."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api-performance.ozon.ru/api/client/token",
                json={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": "client_credentials",
                },
                headers={"Content-Type": "application/json"},
            )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("access_token"):
                return True, "Ozon Performance API валиден ✅"
        return False, "Неверные Performance API credentials. Проверьте Client-Id и Client-Secret."
    except Exception as e:
        logger.error("Ozon performance validation error: %s", e)
        return False, f"Ошибка соединения: {str(e)}"


# ── Endpoints ─────────────────────────────────────────────────────

@router.get("", response_model=list[ShopResponse])
async def list_shops(current_user: User = Depends(get_current_user)):
    """List all shops belonging to the current user."""
    return [ShopResponse.model_validate(shop) for shop in current_user.shops]


@router.post("/validate-key", response_model=ValidateKeyResponse)
async def validate_key(
    body: ValidateKeyRequest,
    current_user: User = Depends(get_current_user),
):
    """
    Validate marketplace API key with a test request.

    Tests the key against the actual marketplace API to confirm it works.
    For Ozon, validates both Seller and Performance credentials separately.
    """
    if body.marketplace == "wildberries":
        valid, message, shop_name, warnings = await _validate_wb_key(body.api_key)
        return ValidateKeyResponse(
            valid=valid, message=message, shop_name=shop_name,
            warnings=warnings if warnings else None,
        )

    elif body.marketplace == "ozon":
        if not body.client_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Для Ozon необходим Client-Id Seller API",
            )

        # Validate Seller API
        seller_valid, seller_msg, shop_name = await _validate_ozon_seller(
            body.client_id, body.api_key
        )

        # Validate Performance API (if credentials provided)
        perf_valid = None
        perf_msg = ""
        if body.perf_client_id and body.perf_client_secret:
            perf_valid, perf_msg = await _validate_ozon_performance(
                body.perf_client_id, body.perf_client_secret
            )

        overall_valid = seller_valid and (perf_valid is None or perf_valid)
        messages = [seller_msg]
        if perf_msg:
            messages.append(perf_msg)

        return ValidateKeyResponse(
            valid=overall_valid,
            seller_valid=seller_valid,
            perf_valid=perf_valid,
            message=" | ".join(messages),
            shop_name=shop_name,
        )


@router.post("", response_model=ShopResponse, status_code=status.HTTP_201_CREATED)
async def create_shop(
    body: ShopCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Add a new marketplace shop connection."""
    # Validate Ozon requires client_id
    if body.marketplace == "ozon" and not body.client_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Для Ozon необходим Client-Id",
        )

    # Encrypt credentials
    api_key_encrypted = encrypt_api_key(body.api_key)
    perf_secret_encrypted = None
    if body.perf_client_secret:
        perf_secret_encrypted = encrypt_api_key(body.perf_client_secret)

    shop = Shop(
        user_id=current_user.id,
        name=body.name,
        marketplace=body.marketplace,
        api_key_encrypted=api_key_encrypted,
        client_id=body.client_id,
        perf_client_id=body.perf_client_id,
        perf_client_secret_encrypted=perf_secret_encrypted,
        status="syncing",
    )
    db.add(shop)
    await db.flush()
    await db.refresh(shop)

    # Trigger background data loading
    try:
        from celery_app.tasks.tasks import load_historical_data
        load_historical_data.delay(shop_id=shop.id)
        logger.info("Triggered load_historical_data for shop %s", shop.id)
    except Exception as e:
        logger.error("Failed to trigger data loading for shop %s: %s", shop.id, e)

    return ShopResponse.model_validate(shop)


@router.get("/{shop_id}/sync-status")
async def get_sync_status(
    shop_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Poll sync progress for a shop.

    Returns progress stored in Redis by the ``load_historical_data`` task.
    Falls back to shop.status from PostgreSQL if Redis has no data.
    """
    # Verify ownership
    result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(status_code=404, detail="Магазин не найден")

    # Try Redis first
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    r = redis.from_url(redis_url)
    progress_key = f"sync_progress:{shop_id}"
    raw = r.get(progress_key)

    if raw:
        return json.loads(raw)

    # Fallback to DB status
    if shop.status == "active":
        return {
            "status": "done",
            "current_step": 0,
            "total_steps": 0,
            "step_name": "Готово!",
            "percent": 100,
            "error": None,
        }
    elif shop.status == "syncing":
        return {
            "status": "loading",
            "current_step": 0,
            "total_steps": 0,
            "step_name": "Ожидание начала загрузки...",
            "percent": 0,
            "error": None,
        }
    else:
        return {
            "status": shop.status or "unknown",
            "current_step": 0,
            "total_steps": 0,
            "step_name": shop.status_message or "",
            "percent": 0,
            "error": shop.status_message,
        }


@router.patch("/{shop_id}/keys", response_model=ShopResponse)
async def update_shop_keys(
    shop_id: int,
    body: ShopUpdateKeys,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update API keys for an existing shop."""
    result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = result.scalar_one_or_none()

    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Магазин не найден",
        )

    # ── Validate the new key ─────────────────────────────
    if shop.marketplace == "wildberries":
        valid, message, _, warnings = await _validate_wb_key(body.api_key)
        if not valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Невалидный API-ключ WB: {message}",
            )
    elif shop.marketplace == "ozon":
        client_id = body.client_id or shop.client_id
        if not client_id:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Для Ozon необходим Client-Id",
            )
        valid, message = await _validate_ozon_seller(client_id, body.api_key)
        if not valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Невалидный API-ключ Ozon: {message}",
            )

    # ── Encrypt and save ─────────────────────────────────
    shop.api_key_encrypted = encrypt_api_key(body.api_key)

    if body.client_id is not None:
        shop.client_id = body.client_id
    if body.perf_client_id is not None:
        shop.perf_client_id = body.perf_client_id
    if body.perf_client_secret:
        shop.perf_client_secret_encrypted = encrypt_api_key(body.perf_client_secret)

    # Reset error status if key was updated
    if shop.status in ("error", "suspended"):
        shop.status = "active"
        shop.status_message = None

    from sqlalchemy import text as sa_text
    shop.updated_at = (await db.execute(sa_text("SELECT NOW()"))).scalar()

    logger.info("API keys updated for shop %d (%s) by user %s", shop_id, shop.name, current_user.id)
    return ShopResponse.model_validate(shop)


@router.delete("/{shop_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_shop(
    shop_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a shop connection and all associated data."""
    result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = result.scalar_one_or_none()

    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Магазин не найден",
        )

    # ── 1. Clean up ClickHouse analytics data ────────────────────
    try:
        from app.core.clickhouse import get_clickhouse_client
        ch = get_clickhouse_client()

        ch_tables = [
            # Ozon tables
            "fact_ozon_ad_daily",
            "fact_ozon_orders",
            "fact_ozon_transactions",
            "fact_ozon_funnel",
            "fact_ozon_returns",
            "fact_ozon_prices",
            "fact_ozon_commissions",
            "fact_ozon_seller_rating",
            "fact_ozon_warehouse_stocks",
            "fact_ozon_content_rating",
            "fact_ozon_promotions",
            "fact_ozon_availability",
            "fact_ozon_inventory",
            "log_ozon_bids",
            # WB tables
            "fact_orders_raw",
            "fact_sales_funnel",
            "fact_inventory_snapshot",
            "fact_finances",
            "fact_advert_stats",
            "fact_advert_stats_v3",
            "ad_stats",
            "dim_advert_campaigns",
            "log_wb_bids",
            "ads_raw_history",
            "orders",
            "positions",
            "sales_daily",
        ]

        for table in ch_tables:
            try:
                ch.command(
                    f"ALTER TABLE {table} DELETE WHERE shop_id = {{shop_id:UInt32}}",
                    parameters={"shop_id": shop_id},
                )
            except Exception:
                pass  # Table may not exist or have no data

        ch.close()
        logger.info("ClickHouse cleanup done for shop %d (%d tables)", shop_id, len(ch_tables))

    except Exception as e:
        logger.warning("ClickHouse cleanup failed for shop %d: %s", shop_id, e)

    # ── 2. Clean up PostgreSQL related tables ────────────────────
    from sqlalchemy import text
    pg_tables = [
        "event_log",
        "dim_ozon_product_content",
        "dim_ozon_products",
        "dim_product_content",
        "dim_products",
        "autobidder_settings",
    ]
    for table in pg_tables:
        try:
            await db.execute(text(f"DELETE FROM {table} WHERE shop_id = :sid"), {"sid": shop_id})
        except Exception as e:
            logger.warning("PG cleanup skip %s for shop %d: %s", table, shop_id, e)
            await db.rollback()
    logger.info("PostgreSQL related data cleaned for shop %d", shop_id)

    # ── 3. Clean up Redis state/cache keys ───────────────────────
    try:
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        r = redis.from_url(redis_url)

        patterns = [
            f"ads:state:{shop_id}:*",
            f"ads:state:views:{shop_id}:*",
            f"ads:state:bid:{shop_id}:*",
            f"state:price:{shop_id}:*",
            f"state:stock:{shop_id}:*",
            f"state:image:{shop_id}:*",
            f"state:content:{shop_id}:*",
            f"ozon_ads:state:{shop_id}:*",
        ]
        deleted_keys = 0
        for pattern in patterns:
            for key in r.scan_iter(match=pattern, count=500):
                r.delete(key)
                deleted_keys += 1

        # Delete specific keys
        for key in [
            f"sync_progress:{shop_id}",
            f"lock:load_historical_data:{shop_id}",
        ]:
            r.delete(key)
            deleted_keys += 1

        r.close()
        logger.info("Redis cleanup done for shop %d (%d keys)", shop_id, deleted_keys)

    except Exception as e:
        logger.warning("Redis cleanup failed for shop %d: %s", shop_id, e)

    # ── 4. Delete the shop record itself ─────────────────────────
    await db.delete(shop)
    logger.info("Shop %d (%s) fully deleted by user %s", shop_id, shop.name, current_user.id)
