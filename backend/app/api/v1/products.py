"""
Products API endpoints.

GET  /products/ozon?shop_id=X&page=1&per_page=25&sort=revenue_7d&order=desc&filter=all&search=
PATCH /products/ozon/cost  — update cost price for a product
POST /products/ozon/cost/bulk — bulk upload cost prices via Excel
"""
import io
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
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
    period: int = Query(7, ge=1, le=366),
    date_from: Optional[date] = Query(None, description="Custom range start (YYYY-MM-DD)"),
    date_to: Optional[date] = Query(None, description="Custom range end (YYYY-MM-DD)"),
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
    if date_from and date_to:
        d_start = date_from
        d_end = date_to
    else:
        d_end = today
        d_start = today - timedelta(days=period - 1)
    span = (d_end - d_start).days + 1
    d_prev_start = d_start - timedelta(days=span)
    d_prev_end = d_start - timedelta(days=1)
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
            "revenue_list": 0.0,
            "orders_prev_7d": 0,
            "revenue_delta": 0.0,
            "orders_30d": 0,
            "ad_spend_7d": 0.0,
            "drr": 0.0,
            "returns_30d": 0,
            "content_rating": 0.0,
            "commission_percent": 0.0,
            "fbo_logistics": 0.0,
            "margin": None,
            "margin_percent": None,
            "payout_period": 0.0,
            "payout_prev": 0.0,
            "gross_profit": None,
            "gross_profit_percent": None,
            "gross_profit_prev": None,
            "gross_profit_delta": None,
            "mp_fees": 0.0,
            "mp_fees_percent": 0.0,
            "mp_fees_storage": 0.0,
            "mp_fees_other": 0.0,
            "avg_price": 0.0,
            "sales_amount": 0.0,
            "period": period,
            "events": [],
            "promotions": [],
        }
        all_offer_ids.append(oid)
        if r[2]:
            all_skus.append(r[2])
        all_product_ids.append(r[0])

    # ────────────────────────────────────────────────────
    # 2. Orders (period) + prev period from ClickHouse
    # ────────────────────────────────────────────────────
    try:
        orders_result = ch.query("""
            SELECT offer_id,
                   sumIf(quantity, order_date >= {d_start:Date} AND order_date <= {d_end:Date}) AS orders_period,
                   sumIf(price * quantity, order_date >= {d_start:Date} AND order_date <= {d_end:Date}) AS revenue_list,
                   sumIf(payout, order_date >= {d_start:Date} AND order_date <= {d_end:Date}) AS payout_period,
                   sumIf(quantity, order_date >= {d_prev_start:Date} AND order_date <= {d_prev_end:Date}) AS orders_prev,
                   sumIf(payout, order_date >= {d_prev_start:Date} AND order_date <= {d_prev_end:Date}) AS payout_prev,
                   sumIf(quantity, order_date >= {d_start:Date} AND order_date <= {d_end:Date} AND payout > 0) AS payout_qty
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND order_date >= {d_prev_start:Date}
              AND status NOT IN ('cancelled', 'canceled')
            GROUP BY offer_id
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start,
            "d_end": d_end,
            "d_prev_start": d_prev_start,
            "d_prev_end": d_prev_end,
        })
        for r in orders_result.result_rows:
            oid = r[0]
            if oid in products_map:
                products_map[oid]["orders_7d"] = r[1]
                products_map[oid]["revenue_7d"] = float(r[2])       # price × qty (list price, like Ozon admin)
                products_map[oid]["payout_period"] = float(r[3])    # payout from orders (for profit calc)
                products_map[oid]["payout_prev"] = float(r[5])      # prev period payout
                # avg_price = payout per unit (real money received per piece)
                # NOTE: Ozon removed buyer price from Seller API (Nov 2025)
                # Use payout_qty (orders with payout > 0) to avoid dividing by undelivered orders
                payout_qty = r[6] if r[6] else 0
                if payout_qty > 0 and float(r[3]) > 0:
                    products_map[oid]["avg_price"] = round(float(r[3]) / payout_qty, 2)
                prev_orders = r[4]
                products_map[oid]["orders_prev_7d"] = prev_orders
                if prev_orders > 0:
                    products_map[oid]["revenue_delta"] = round(
                        (r[1] - prev_orders) / prev_orders * 100, 1
                    )
                elif r[1] > 0:
                    products_map[oid]["revenue_delta"] = 100.0
    except Exception as e:
        logger.warning("CH orders query failed: %s", e)

    # ────────────────────────────────────────────────────
    # 3. Ads 7d from ClickHouse (keyed by SKU, not offer_id)
    # ────────────────────────────────────────────────────
    # Build sku → offer_id mapping for ads lookup
    sku_to_offer = {}
    for oid, p in products_map.items():
        if p["sku"]:
            sku_to_offer[p["sku"]] = oid

    try:
        ads_result = ch.query("""
            SELECT sku,
                   sum(money_spent) AS ad_spend,
                   sum(views) AS views,
                   sum(clicks) AS clicks,
                   sum(orders) AS ad_orders,
                   sum(revenue) AS ad_revenue
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d_start:Date}
              AND dt <= {d_end:Date}
            GROUP BY sku
        """, parameters={"shop_id": shop_id, "d_start": d_start, "d_end": d_end})
        for r in ads_result.result_rows:
            oid = sku_to_offer.get(r[0])
            if oid and oid in products_map:
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

    # Also get 30d orders for return rate calculation
    try:
        orders30_result = ch.query("""
            SELECT offer_id,
                   sum(quantity) AS orders_30d
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND order_date >= {d30_start:Date}
              AND status NOT IN ('cancelled', 'canceled')
            GROUP BY offer_id
        """, parameters={"shop_id": shop_id, "d30_start": d30_start})
        for r in orders30_result.result_rows:
            if r[0] in products_map:
                products_map[r[0]]["orders_30d"] = r[1]
    except Exception as e:
        logger.warning("CH orders_30d query failed: %s", e)

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
        # Map sku → product (already built above for ads)
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
    # 10. Net payout from fact_ozon_transactions (current + prev period)
    #     This is the ACTUAL money received after ALL deductions
    #     (commission, logistics, storage, acquiring, returns).
    # ────────────────────────────────────────────────────
    try:
        txn_result = ch.query("""
            SELECT sku,
                   sum(CASE WHEN operation_date >= {d_start:Date} AND operation_date <= {d_end:Date} THEN amount ELSE 0 END) AS txn_cur,
                   sum(CASE WHEN operation_date >= {d_prev_start:Date} AND operation_date <= {d_prev_end:Date} THEN amount ELSE 0 END) AS txn_prev
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND operation_date >= {d_prev_start:Date}
              AND operation_date <= {d_end:Date}
              AND sku > 0
            GROUP BY sku
        """, parameters={
            "shop_id": shop_id, "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })
        for r in txn_result.result_rows:
            oid = sku_to_offer.get(r[0])
            if oid and oid in products_map:
                products_map[oid]["txn_payout"] = float(r[1])
                products_map[oid]["txn_payout_prev"] = float(r[2])
    except Exception as e:
        logger.warning("CH transactions query failed: %s", e)

    # ────────────────────────────────────────────────────
    # Calculate margin & gross profit
    # ────────────────────────────────────────────────────
    for p in products_map.values():
        cost = p["cost_price"] + p["packaging_cost"]
        if cost > 0 and p["price"] > 0:
            # margin_percent = cost as percentage of selling price (from admin)
            p["margin"] = round(cost, 2)
            p["margin_percent"] = round(cost / p["price"] * 100, 1)

        # ── Marketplace fees (compute FIRST, needed for profit) ──
        txn_payout = p.get("txn_payout", 0)
        txn_payout_prev = p.get("txn_payout_prev", 0)
        if p["revenue_7d"] > 0 or txn_payout != 0:
            total_fees = p["revenue_7d"] - txn_payout
            commission_part = p["revenue_7d"] - p["payout_period"]
            logistics_part = p["payout_period"] - txn_payout
            p["mp_fees"] = round(total_fees, 2)
            p["mp_fees_commission"] = round(commission_part, 2)
            p["mp_fees_logistics"] = round(logistics_part, 2)
            if p["revenue_7d"] > 0:
                p["mp_fees_percent"] = round(total_fees / p["revenue_7d"] * 100, 1)
            else:
                p["mp_fees_percent"] = 0.0

        # ── Gross profit = revenue - COGS - mp_fees - ad_spend ──
        # NOTE: previously used txn_payout - COGS - ad, but txn_payout is NOT tied
        # to order period (includes settlements for past orders, refunds, etc.)
        if cost > 0 and p["orders_7d"] > 0:
            cogs = cost * p["orders_7d"]
            mp_fees_val = p.get("mp_fees", 0)
            gp = p["revenue_7d"] - cogs - mp_fees_val - p["ad_spend_7d"]
            p["gross_profit"] = round(gp, 2)
            if p["revenue_7d"] > 0:
                p["gross_profit_percent"] = round(gp / p["revenue_7d"] * 100, 1)
            # Prev-period gross profit for delta
            if p.get("orders_prev_7d", 0) > 0:
                cogs_prev = cost * p["orders_prev_7d"]
                rev_prev = p.get("revenue_prev", 0)
                mp_pct = p.get("mp_fees_percent", 0) / 100
                mp_fees_prev = rev_prev * mp_pct
                gp_prev = rev_prev - cogs_prev - mp_fees_prev - p.get("ad_spend_prev", 0)
                p["gross_profit_prev"] = round(gp_prev, 2)
                if gp_prev != 0:
                    p["gross_profit_delta"] = round((gp - gp_prev) / abs(gp_prev) * 100, 1)
                elif gp > 0:
                    p["gross_profit_delta"] = 100.0

        # sales_amount = avg_price × orders (payout-based total)
        if p["avg_price"] > 0 and p["orders_7d"] > 0:
            p["sales_amount"] = round(p["avg_price"] * p["orders_7d"], 2)

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
    elif filter == "no_ads":
        products_list = [p for p in products_list if p["ad_spend_7d"] == 0 and not p["is_archived"]]
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

    # Sort — push null/zero to the end regardless of direction
    # Use (primary_key, offer_id) tuple for STABLE pagination across pages
    _DESC_NULL = float('-inf')  # for desc: nulls go last (smallest)
    _ASC_NULL = float('inf')   # for asc: nulls go last (largest)
    _null = _DESC_NULL if order == "desc" else _ASC_NULL

    sort_key_map = {
        "revenue_7d": lambda p: (p["revenue_7d"] if p["revenue_7d"] else _null, p["offer_id"]),
        "orders_7d": lambda p: (p["orders_7d"] if p["orders_7d"] else _null, p["offer_id"]),
        "stocks": lambda p: ((p["stocks_fbo"] + p["stocks_fbs"]) or _null, p["offer_id"]),
        "price": lambda p: (p["price"] if p["price"] else _null, p["offer_id"]),
        "margin": lambda p: (p["margin"] if p["margin"] is not None else _null, p["offer_id"]),
        "gross_profit": lambda p: (p["gross_profit"] if p["gross_profit"] is not None else _null, p["offer_id"]),
        "drr": lambda p: (p["drr"] if p["drr"] else _null, p["offer_id"]),
        "returns": lambda p: (p["returns_30d"] if p["returns_30d"] else _null, p["offer_id"]),
        "name": lambda p: ((p["name"] or "").lower(), p["offer_id"]),
        "content_rating": lambda p: (p["content_rating"] if p["content_rating"] else _null, p["offer_id"]),
    }
    sort_fn = sort_key_map.get(sort, sort_key_map["revenue_7d"])
    products_list.sort(key=sort_fn, reverse=(order == "desc"))

    # Count cost missing
    cost_missing = sum(1 for p in products_map.values() if p["cost_price"] == 0 and not p["is_archived"])

    # ── Compute totals across ALL filtered products (before pagination) ──
    t_stocks = 0
    t_orders = 0
    t_revenue = 0.0
    t_payout = 0.0
    t_ad_spend = 0.0
    t_mp_fees = 0.0
    t_mp_fees_commission = 0.0
    t_mp_fees_logistics = 0.0
    t_total_cogs = 0.0
    t_profit = 0.0
    t_profit_count = 0
    t_returns = 0
    t_orders_30d = 0
    for p in products_list:
        t_stocks += p["stocks_fbo"] + p["stocks_fbs"]
        t_orders += p["orders_7d"]
        t_revenue += p["revenue_7d"]
        t_payout += p.get("payout_period", 0)
        t_ad_spend += p["ad_spend_7d"]
        t_mp_fees += p["mp_fees"]
        t_mp_fees_commission += p.get("mp_fees_commission", 0)
        t_mp_fees_logistics += p.get("mp_fees_logistics", 0)
        t_returns += p["returns_30d"]
        t_orders_30d += p.get("orders_30d", 0)
        # COGS для итого-строки
        if p["cost_price"] > 0 and p["orders_7d"] > 0:
            t_total_cogs += (p["cost_price"] + p.get("packaging_cost", 0)) * p["orders_7d"]
        if p["gross_profit"] is not None:
            t_profit += p["gross_profit"]
            t_profit_count += 1

    totals = {
        "count": len(products_list),
        "stocks": t_stocks,
        "orders": t_orders,
        "revenue": round(t_revenue, 2),
        "payout": round(t_payout, 2),
        "avg_price": round(t_payout / t_orders, 2) if t_orders > 0 and t_payout > 0 else 0,
        "total_cogs": round(t_total_cogs, 2),
        "ad_spend": round(t_ad_spend, 2),
        "drr": round(t_ad_spend / t_revenue * 100, 1) if t_revenue > 0 else 0,
        "returns_pct": round(t_returns / t_orders_30d * 100, 1) if t_orders_30d > 0 else 0,
        "mp_fees": round(t_mp_fees, 2),
        "mp_fees_commission": round(t_mp_fees_commission, 2),
        "mp_fees_logistics": round(t_mp_fees_logistics, 2),
        "mp_fees_pct": round(t_mp_fees / t_revenue * 100, 1) if t_revenue > 0 else 0,
        "profit": round(t_profit, 2),
        "profit_pct": round(t_profit / t_revenue * 100, 1) if t_revenue > 0 and t_profit_count > 0 else 0,
        "profit_count": t_profit_count,
    }

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
        "period": period,
        "totals": totals,
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

    # Trim offer_id to prevent space duplicates
    offer_id = body.offer_id.strip()

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
            "offer_id": offer_id,
            "cost_price": body.cost_price,
            "packaging_cost": body.packaging_cost,
        },
    )
    await db.commit()

    # Check if cost > price for this product → warn
    warning = None
    price_result = await db.execute(
        text("SELECT price FROM dim_ozon_products WHERE shop_id = :sid AND offer_id = :oid LIMIT 1"),
        {"sid": body.shop_id, "oid": offer_id},
    )
    row = price_result.fetchone()
    if row and row[0] and body.cost_price > float(row[0]):
        warning = f"Себестоимость ({body.cost_price}₽) выше цены продажи ({float(row[0])}₽)"

    return CostUpdateResponse(
        ok=True,
        offer_id=offer_id,
        cost_price=body.cost_price,
    )


# ── Bulk Upload Cost Prices via Excel ─────────────────────

@router.post("/ozon/cost/bulk")
async def upload_cost_excel(
    shop_id: int = Query(...),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Bulk upload cost prices from Excel.
    Expected format: column A = offer_id (артикул), column B = cost_price.
    No headers required.
    """
    # Verify shop ownership
    shop_result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = shop_result.scalar_one_or_none()
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    # Validate file type
    if not file.filename or not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Файл должен быть в формате .xlsx")

    try:
        import openpyxl
        content = await file.read()
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        if ws is None:
            raise HTTPException(status_code=400, detail="Пустой Excel файл")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка чтения Excel: {e}")

    updated = 0
    errors = []
    total_rows = 0
    skipped_empty = 0

    for row_idx, row in enumerate(ws.iter_rows(min_col=1, max_col=2, values_only=True), start=1):
        total_rows += 1
        offer_id_raw, cost_raw = row
        if offer_id_raw is None or cost_raw is None:
            skipped_empty += 1
            continue

        offer_id = str(offer_id_raw).strip()
        if not offer_id:
            skipped_empty += 1
            continue

        try:
            # Handle Russian locale: non-breaking spaces as thousands separator, comma as decimal
            if isinstance(cost_raw, str):
                cost_str = cost_raw.replace('\xa0', '').replace(' ', '').replace(',', '.')
                cost_price = float(cost_str)
            else:
                cost_price = float(cost_raw)
            if cost_price < 0:
                errors.append(f"Строка {row_idx}: отрицательная цена ({cost_price})")
                continue
        except (ValueError, TypeError):
            errors.append(f"Строка {row_idx}: невалидная цена '{cost_raw}'")
            continue

        await db.execute(
            text("""
                INSERT INTO product_costs (shop_id, offer_id, cost_price)
                VALUES (:shop_id, :offer_id, :cost_price)
                ON CONFLICT (shop_id, offer_id) DO UPDATE SET
                    cost_price = EXCLUDED.cost_price,
                    updated_at = NOW()
            """),
            {"shop_id": shop_id, "offer_id": offer_id, "cost_price": cost_price},
        )
        updated += 1

    await db.commit()
    logger.info(
        "[cost-bulk] shop=%s: total_rows=%d updated=%d skipped_empty=%d errors=%d",
        shop_id, total_rows, updated, skipped_empty, len(errors),
    )

    # Check for cost > price warnings
    warnings = []
    if updated > 0:
        warn_result = await db.execute(
            text("""
                SELECT c.offer_id, c.cost_price, p.price
                FROM product_costs c
                JOIN dim_ozon_products p ON p.shop_id = c.shop_id AND p.offer_id = c.offer_id
                WHERE c.shop_id = :shop_id AND c.cost_price > p.price AND p.price > 0
            """),
            {"shop_id": shop_id},
        )
        for r in warn_result.fetchall():
            warnings.append(f"{r[0]}: с/с {float(r[1]):.0f}₽ > цена {float(r[2]):.0f}₽")

    return {
        "ok": True,
        "updated": updated,
        "errors": errors[:20],
        "warnings": warnings[:20],
    }


# ── Download Cost Template Excel ──────────────────────────

@router.get("/ozon/cost/template")
async def download_cost_template(
    shop_id: int = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Download Excel template with all offer_ids for cost price entry."""
    from fastapi.responses import StreamingResponse

    # Verify shop ownership
    shop_result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = shop_result.scalar_one_or_none()
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    # Get all products with current cost
    result = await db.execute(
        text("""
            SELECT p.offer_id,
                   COALESCE(cost.cost_price, 0) AS cost_price,
                   p.name
            FROM dim_ozon_products p
            LEFT JOIN product_costs cost
                ON cost.shop_id = p.shop_id AND cost.offer_id = p.offer_id
            WHERE p.shop_id = :shop_id
              AND NOT COALESCE(p.is_archived, false)
            ORDER BY p.name
        """),
        {"shop_id": shop_id},
    )
    rows = result.fetchall()

    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Себестоимость"

    # Headers
    ws.append(["Артикул", "Себестоимость", "Название (справочно)"])
    ws.column_dimensions['A'].width = 25
    ws.column_dimensions['B'].width = 18
    ws.column_dimensions['C'].width = 50

    for r in rows:
        ws.append([r[0], float(r[1]), r[2] or ""])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=cost_template_{shop_id}.xlsx"},
    )
