"""
Products API endpoints.

GET  /products/ozon?shop_id=X&page=1&per_page=25&sort=revenue_7d&order=desc&filter=all&search=
PATCH /products/ozon/cost  — update cost price for a product
"""
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/products", tags=["Products"])


# ── Request / Response schemas ────────────────────────────

class CostUpdateRequest(BaseModel):
    shop_id: int
    offer_id: str
    cost_price: float
    packaging_cost: float = 0


class CostUpdateResponse(BaseModel):
    ok: bool
    offer_id: str
    cost_price: float


# ── Ozon Products List ────────────────────────────────────

@router.get("/ozon")
async def get_ozon_products(
    shop_id: int = Query(...),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=10, le=100),
    sort: str = Query("revenue_7d"),
    order: str = Query("desc"),
    filter: str = Query("all"),
    search: str = Query(""),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Ozon products list with aggregated analytics from 8+ data sources."""

    # Verify shop ownership
    shop_result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = shop_result.scalar_one_or_none()
    if not shop or shop.marketplace != "ozon":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Shop not found")

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()
    except Exception as e:
        logger.error("ClickHouse connection error: %s", e)
        raise HTTPException(status_code=500, detail="Analytics unavailable")

    today = date.today()
    d7_start = today - timedelta(days=6)
    d7_prev_start = d7_start - timedelta(days=7)
    d7_prev_end = d7_start - timedelta(days=1)
    d30_start = today - timedelta(days=29)

    # ────────────────────────────────────────────────────
    # 1. Base catalog from PostgreSQL (dim_ozon_products)
    # ────────────────────────────────────────────────────
    pg_result = await db.execute(
        text("""
            SELECT p.product_id, p.offer_id, p.sku, p.name, p.barcode,
                   COALESCE(NULLIF(p.primary_image_url, ''), p.main_image_url, '') AS image_url,
                   p.price, p.old_price, p.min_price, p.marketing_price,
                   p.stocks_fbo, p.stocks_fbs,
                   p.price_index_color, p.price_index_value, p.competitor_min_price,
                   p.status, p.moderate_status, p.status_name,
                   p.is_archived, p.volume_weight, p.vat,
                   p.model_id, p.model_count,
                   COALESCE(c.images_count, 0) AS images_count,
                   COALESCE(c.title_hash, '') AS title_hash,
                   COALESCE(cost.cost_price, 0) AS cost_price,
                   COALESCE(cost.packaging_cost, 0) AS packaging_cost
            FROM dim_ozon_products p
            LEFT JOIN dim_ozon_product_content c
                ON c.shop_id = p.shop_id AND c.product_id = p.product_id
            LEFT JOIN product_costs cost
                ON cost.shop_id = p.shop_id AND cost.offer_id = p.offer_id
            WHERE p.shop_id = :shop_id
            ORDER BY p.name
        """),
        {"shop_id": shop_id},
    )
    rows = pg_result.fetchall()

    if not rows:
        return {
            "products": [],
            "total": 0,
            "page": page,
            "per_page": per_page,
            "cost_missing_count": 0,
        }

    # Build products dict keyed by offer_id
    products_map = {}
    all_offer_ids = []
    all_skus = []
    all_product_ids = []

    for r in rows:
        oid = r[1]  # offer_id
        products_map[oid] = {
            "product_id": r[0],
            "offer_id": oid,
            "sku": r[2],
            "name": r[3] or oid,
            "barcode": r[4],
            "image_url": r[5] or "",
            "price": float(r[6] or 0),
            "old_price": float(r[7] or 0),
            "min_price": float(r[8] or 0),
            "marketing_price": float(r[9] or 0),
            "stocks_fbo": r[10] or 0,
            "stocks_fbs": r[11] or 0,
            "price_index_color": r[12] or "",
            "price_index_value": float(r[13] or 0),
            "competitor_min_price": float(r[14] or 0),
            "status": r[15] or "",
            "moderate_status": r[16] or "",
            "status_name": r[17] or "",
            "is_archived": r[18] or False,
            "volume_weight": float(r[19] or 0),
            "vat": float(r[20] or 0),
            "model_id": r[21],
            "model_count": r[22] or 0,
            "images_count": r[23] or 0,
            "cost_price": float(r[25] or 0),
            "packaging_cost": float(r[26] or 0),
            # Will be filled from CH
            "orders_7d": 0,
            "revenue_7d": 0.0,
            "orders_prev_7d": 0,
            "revenue_delta": 0.0,
            "ad_spend_7d": 0.0,
            "drr": 0.0,
            "returns_30d": 0,
            "content_rating": 0.0,
            "commission_percent": 0.0,
            "fbo_logistics": 0.0,
            "margin": None,
            "margin_percent": None,
            "events": [],
            "promotions": [],
        }
        all_offer_ids.append(oid)
        if r[2]:
            all_skus.append(r[2])
        all_product_ids.append(r[0])

    # ────────────────────────────────────────────────────
    # 2. Orders 7d + prev 7d from ClickHouse
    # ────────────────────────────────────────────────────
    try:
        orders_result = ch.query("""
            SELECT offer_id,
                   sumIf(quantity, order_date >= {d7_start:Date} AND order_date <= {today:Date}) AS orders_7d,
                   sumIf(price * quantity, order_date >= {d7_start:Date} AND order_date <= {today:Date}) AS revenue_7d,
                   sumIf(quantity, order_date >= {d7_prev_start:Date} AND order_date <= {d7_prev_end:Date}) AS orders_prev
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND order_date >= {d7_prev_start:Date}
              AND status NOT IN ('cancelled', 'canceled')
            GROUP BY offer_id
        """, parameters={
            "shop_id": shop_id,
            "d7_start": d7_start,
            "today": today,
            "d7_prev_start": d7_prev_start,
            "d7_prev_end": d7_prev_end,
        })
        for r in orders_result.result_rows:
            oid = r[0]
            if oid in products_map:
                products_map[oid]["orders_7d"] = r[1]
                products_map[oid]["revenue_7d"] = float(r[2])
                prev = r[3]
                if prev > 0:
                    products_map[oid]["revenue_delta"] = round(
                        (r[1] - prev) / prev * 100, 1
                    )
                elif r[1] > 0:
                    products_map[oid]["revenue_delta"] = 100.0
    except Exception as e:
        logger.warning("CH orders query failed: %s", e)

    # ────────────────────────────────────────────────────
    # 3. Ads 7d from ClickHouse
    # ────────────────────────────────────────────────────
    try:
        ads_result = ch.query("""
            SELECT offer_id,
                   sum(spend) AS ad_spend
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d7_start:Date}
            GROUP BY offer_id
        """, parameters={"shop_id": shop_id, "d7_start": d7_start})
        for r in ads_result.result_rows:
            oid = r[0]
            if oid in products_map:
                products_map[oid]["ad_spend_7d"] = float(r[1])
                rev = products_map[oid]["revenue_7d"]
                if rev > 0:
                    products_map[oid]["drr"] = round(float(r[1]) / rev * 100, 1)
    except Exception as e:
        logger.warning("CH ads query failed: %s", e)

    # ────────────────────────────────────────────────────
    # 4. Returns 30d from ClickHouse
    # ────────────────────────────────────────────────────
    try:
        returns_result = ch.query("""
            SELECT offer_id,
                   sum(quantity) AS returns_count
            FROM mms_analytics.fact_ozon_returns FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d30_start:Date}
            GROUP BY offer_id
        """, parameters={"shop_id": shop_id, "d30_start": d30_start})
        for r in returns_result.result_rows:
            if r[0] in products_map:
                products_map[r[0]]["returns_30d"] = r[1]
    except Exception as e:
        logger.warning("CH returns query failed: %s", e)

    # ────────────────────────────────────────────────────
    # 5. Commissions (latest) from ClickHouse
    # ────────────────────────────────────────────────────
    try:
        comm_result = ch.query("""
            SELECT offer_id,
                   argMax(sales_percent, dt) AS sales_pct,
                   argMax(fbo_fulfillment_amount, dt) AS fbo_logistics
            FROM mms_analytics.fact_ozon_commissions FINAL
            WHERE shop_id = {shop_id:UInt32}
            GROUP BY offer_id
        """, parameters={"shop_id": shop_id})
        for r in comm_result.result_rows:
            if r[0] in products_map:
                products_map[r[0]]["commission_percent"] = float(r[1])
                products_map[r[0]]["fbo_logistics"] = float(r[2])
    except Exception as e:
        logger.warning("CH commissions query failed: %s", e)

    # ────────────────────────────────────────────────────
    # 6. Content rating (latest) from ClickHouse
    # ────────────────────────────────────────────────────
    try:
        rating_result = ch.query("""
            SELECT sku,
                   argMax(rating, dt) AS rating
            FROM mms_analytics.fact_ozon_content_rating FINAL
            WHERE shop_id = {shop_id:UInt32}
            GROUP BY sku
        """, parameters={"shop_id": shop_id})
        # Map sku → product
        sku_to_offer = {}
        for oid, p in products_map.items():
            if p["sku"]:
                sku_to_offer[p["sku"]] = oid
        for r in rating_result.result_rows:
            oid = sku_to_offer.get(r[0])
            if oid:
                products_map[oid]["content_rating"] = float(r[1])
    except Exception as e:
        logger.warning("CH content rating query failed: %s", e)

    # ────────────────────────────────────────────────────
    # 7. Active promotions from ClickHouse
    # ────────────────────────────────────────────────────
    try:
        promo_result = ch.query("""
            SELECT product_id, promo_type
            FROM mms_analytics.fact_ozon_promotions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND is_enabled = 1
              AND dt >= {d30_start:Date}
        """, parameters={"shop_id": shop_id, "d30_start": d30_start})
        pid_to_offer = {}
        for oid, p in products_map.items():
            pid_to_offer[p["product_id"]] = oid
        for r in promo_result.result_rows:
            oid = pid_to_offer.get(r[0])
            if oid:
                products_map[oid]["promotions"].append(r[1])
    except Exception as e:
        logger.warning("CH promotions query failed: %s", e)

    # ────────────────────────────────────────────────────
    # 8. Events from PostgreSQL event_log (last 30 days)
    # ────────────────────────────────────────────────────
    try:
        events_result = await db.execute(
            text("""
                SELECT nm_id, event_type, old_value, new_value, created_at
                FROM event_log
                WHERE shop_id = :shop_id
                  AND created_at >= :since
                ORDER BY created_at DESC
            """),
            {"shop_id": shop_id, "since": today - timedelta(days=30)},
        )
        for ev in events_result:
            # nm_id maps to product_id for Ozon events
            pid = ev[0]
            oid = pid_to_offer.get(pid) if pid else None
            if oid and oid in products_map:
                products_map[oid]["events"].append({
                    "type": ev[1],
                    "old_value": ev[2],
                    "new_value": ev[3],
                    "date": ev[4].isoformat() if ev[4] else None,
                })
    except Exception as e:
        logger.warning("PG events query failed: %s", e)

    # ────────────────────────────────────────────────────
    # 9. Price changes from ClickHouse (last 30d)
    # ────────────────────────────────────────────────────
    try:
        price_result = ch.query("""
            SELECT offer_id,
                   groupArray(price) AS prices,
                   groupArray(dt) AS dates
            FROM (
                SELECT offer_id, price, dt
                FROM mms_analytics.fact_ozon_prices FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND dt >= {d30_start:Date}
                ORDER BY dt
            )
            GROUP BY offer_id
        """, parameters={"shop_id": shop_id, "d30_start": d30_start})
        for r in price_result.result_rows:
            oid = r[0]
            if oid in products_map and len(r[1]) >= 2:
                prices = r[1]
                dates = r[2]
                for i in range(1, len(prices)):
                    if prices[i] != prices[i - 1]:
                        direction = "PRICE_UP" if prices[i] > prices[i - 1] else "PRICE_DOWN"
                        products_map[oid]["events"].append({
                            "type": direction,
                            "old_value": str(prices[i - 1]),
                            "new_value": str(prices[i]),
                            "date": str(dates[i]),
                        })
    except Exception as e:
        logger.warning("CH price changes query failed: %s", e)

    # ────────────────────────────────────────────────────
    # Calculate margin for products with cost_price
    # ────────────────────────────────────────────────────
    for p in products_map.values():
        cost = p["cost_price"] + p["packaging_cost"]
        if cost > 0 and p["price"] > 0:
            commission = p["price"] * p["commission_percent"] / 100
            logistics = p["fbo_logistics"]
            margin = p["price"] - cost - commission - logistics
            p["margin"] = round(margin, 2)
            p["margin_percent"] = round(margin / p["price"] * 100, 1)

    # ────────────────────────────────────────────────────
    # Apply filter
    # ────────────────────────────────────────────────────
    products_list = list(products_map.values())

    if filter == "in_stock":
        products_list = [p for p in products_list if p["stocks_fbo"] + p["stocks_fbs"] > 0]
    elif filter == "no_stock":
        products_list = [p for p in products_list if p["stocks_fbo"] + p["stocks_fbs"] == 0 and not p["is_archived"]]
    elif filter == "with_ads":
        products_list = [p for p in products_list if p["ad_spend_7d"] > 0]
    elif filter == "problems":
        products_list = [p for p in products_list if (
            p["drr"] > 20 or
            (p["stocks_fbo"] + p["stocks_fbs"] == 0 and not p["is_archived"]) or
            p["price_index_color"] in ("NON_PROFIT",)
        )]
    elif filter == "archived":
        products_list = [p for p in products_list if p["is_archived"]]

    # Apply search
    if search:
        q = search.lower()
        products_list = [
            p for p in products_list
            if q in (p["name"] or "").lower()
            or q in (p["offer_id"] or "").lower()
            or q in str(p["sku"] or "")
            or q in (p["barcode"] or "").lower()
        ]

    # Sort
    sort_key_map = {
        "revenue_7d": lambda p: p["revenue_7d"],
        "orders_7d": lambda p: p["orders_7d"],
        "stocks": lambda p: p["stocks_fbo"] + p["stocks_fbs"],
        "price": lambda p: p["price"],
        "margin": lambda p: p["margin"] if p["margin"] is not None else -999999,
        "drr": lambda p: p["drr"],
        "returns": lambda p: p["returns_30d"],
        "name": lambda p: (p["name"] or "").lower(),
        "content_rating": lambda p: p["content_rating"],
    }
    sort_fn = sort_key_map.get(sort, sort_key_map["revenue_7d"])
    products_list.sort(key=sort_fn, reverse=(order == "desc"))

    # Count cost missing
    cost_missing = sum(1 for p in products_map.values() if p["cost_price"] == 0 and not p["is_archived"])

    # Paginate
    total = len(products_list)
    start = (page - 1) * per_page
    end = start + per_page
    page_items = products_list[start:end]

    # Trim events to last 5 per product
    for p in page_items:
        p["events"] = sorted(p["events"], key=lambda e: e.get("date", ""), reverse=True)[:5]

    return {
        "products": page_items,
        "total": total,
        "page": page,
        "per_page": per_page,
        "cost_missing_count": cost_missing,
    }


# ── Update Cost Price ─────────────────────────────────────

@router.patch("/ozon/cost")
async def update_ozon_cost(
    body: CostUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update cost price for an Ozon product."""

    # Verify shop ownership
    shop_result = await db.execute(
        select(Shop).where(Shop.id == body.shop_id, Shop.user_id == current_user.id)
    )
    shop = shop_result.scalar_one_or_none()
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    # Upsert cost
    await db.execute(
        text("""
            INSERT INTO product_costs (shop_id, offer_id, cost_price, packaging_cost)
            VALUES (:shop_id, :offer_id, :cost_price, :packaging_cost)
            ON CONFLICT (shop_id, offer_id) DO UPDATE SET
                cost_price = EXCLUDED.cost_price,
                packaging_cost = EXCLUDED.packaging_cost,
                updated_at = NOW()
        """),
        {
            "shop_id": body.shop_id,
            "offer_id": body.offer_id,
            "cost_price": body.cost_price,
            "packaging_cost": body.packaging_cost,
        },
    )
    await db.commit()

    return CostUpdateResponse(
        ok=True,
        offer_id=body.offer_id,
        cost_price=body.cost_price,
    )
