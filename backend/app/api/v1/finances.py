"""
Finances API endpoints.

GET /finances/ozon?shop_id=X&period=7&group_by=day  — Ozon P&L
GET /finances/wb?shop_id=X&period=7&group_by=day    — WB P&L
"""
import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/finances", tags=["Finances"])


def _safe_delta(current: float, previous: float) -> float:
    """Percentage change, safe for zero division."""
    if previous == 0:
        return 100.0 if current > 0 else (-100.0 if current < 0 else 0.0)
    return round((current - previous) / abs(previous) * 100, 1)


# ── Ozon Finances ─────────────────────────────────────────


@router.get("/ozon")
async def get_ozon_finances(
    shop_id: int = Query(..., description="Shop ID"),
    period: int = Query(7, ge=1, le=366, description="Period in days"),
    date_from: Optional[date] = Query(None, description="Custom range start"),
    date_to: Optional[date] = Query(None, description="Custom range end"),
    group_by: str = Query("day", description="Grouping: day, week, month"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Ozon P&L overview: KPI, expense breakdown, daily dynamics, period comparison.
    """

    # ── Verify shop ownership ──
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

    # ── Date ranges ──
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

    # ══════════════════════════════════════════════════════
    # 1. ORDERS: revenue (price × qty) current + prev period
    #    Using sumIf to split cur/prev entirely in SQL
    #    toDate(order_date) because order_date is DateTime
    # ══════════════════════════════════════════════════════
    revenue_cur = 0.0
    revenue_prev = 0.0
    orders_cur = 0
    orders_prev = 0

    try:
        orders_totals = ch.query("""
            SELECT
                sumIf(price * quantity, toDate(order_date) >= {d_start:Date} AND toDate(order_date) <= {d_end:Date}) AS rev_cur,
                sumIf(quantity, toDate(order_date) >= {d_start:Date} AND toDate(order_date) <= {d_end:Date}) AS ord_cur,
                sumIf(price * quantity, toDate(order_date) >= {d_prev_start:Date} AND toDate(order_date) <= {d_prev_end:Date}) AS rev_prev,
                sumIf(quantity, toDate(order_date) >= {d_prev_start:Date} AND toDate(order_date) <= {d_prev_end:Date}) AS ord_prev
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(order_date) >= {d_prev_start:Date}
              AND toDate(order_date) <= {d_end:Date}
              AND status NOT IN ('cancelled', 'canceled')
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })
        if orders_totals.result_rows:
            r = orders_totals.result_rows[0]
            revenue_cur = float(r[0] or 0)
            orders_cur = int(r[1] or 0)
            revenue_prev = float(r[2] or 0)
            orders_prev = int(r[3] or 0)
    except Exception as e:
        logger.warning("CH orders totals query failed: %s", e)

    # Daily orders for chart
    orders_daily = {}
    try:
        orders_daily_result = ch.query("""
            SELECT
                toDate(order_date) AS dt,
                sum(price * quantity) AS revenue,
                sum(quantity) AS orders_count
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(order_date) >= {d_start:Date}
              AND toDate(order_date) <= {d_end:Date}
              AND status NOT IN ('cancelled', 'canceled')
            GROUP BY dt
            ORDER BY dt
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
        })
        for r in orders_daily_result.result_rows:
            orders_daily[str(r[0])] = {"revenue": float(r[1] or 0), "orders": int(r[2] or 0)}
    except Exception as e:
        logger.warning("CH orders daily query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 2. TRANSACTIONS — using Ozon's built-in detail fields
    #
    #  Revenue transactions have per-order detail:
    #    accruals_for_sale  = gross sale amount (= Excel "Σ Продажи")
    #    sale_commission    = Ozon commission (negative)
    #    services_total     = per-order logistics + services (negative)
    #    amount             = accruals + commission + services (net)
    #
    #  Expense categories (Logistics sku=0, Storage, Acquiring, etc.)
    #    → bulk charges NOT embedded in Revenue per-order data
    #
    #  P&L formula:
    #    revenue      = accruals_for_sale (from Revenue txns)
    #    commission   = |sale_commission|
    #    services     = |services_total| (per-order logistics)
    #    bulk_charges = |Logistics| + |Storage| + |Acquiring| + |Refund| + ...
    #    mp_fees      = commission + services + bulk_charges
    #    payout       = sum(ALL txn amounts) = Excel "К перечислению"
    #    profit       = revenue - mp_fees - ads(ad_daily) - cogs
    # ══════════════════════════════════════════════════════

    # Revenue transaction fields
    accruals_cur = 0.0     # gross sale amount
    accruals_prev = 0.0
    commission_cur = 0.0   # |sale_commission|
    commission_prev = 0.0
    services_cur = 0.0     # |services_total| from Revenue
    services_prev = 0.0
    payout_cur = 0.0       # sum(ALL amounts)
    payout_prev = 0.0

    # Expense category breakdown (bulk charges)
    bulk_cur = {
        "logistics": 0.0,    # Crossdocking/supply (sku=0)
        "storage": 0.0,
        "acquiring": 0.0,
        "refunds": 0.0,
        "penalties": 0.0,
        "compensation": 0.0,
        "other": 0.0,
    }
    bulk_prev = {
        "logistics": 0.0,
        "storage": 0.0,
        "acquiring": 0.0,
        "refunds": 0.0,
        "penalties": 0.0,
        "compensation": 0.0,
        "other": 0.0,
    }
    txn_daily = {}

    CAT_MAP = {
        "Logistics": "logistics",
        "Storage": "storage",
        "Acquiring": "acquiring",
        "Refund": "refunds",
        "Penalty": "penalties",
        "Compensation": "compensation",
    }

    try:
        # 2a. Revenue transaction detail + total payout
        txn_totals = ch.query("""
            SELECT
                sumIf(accruals_for_sale,
                    toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                    AND category = 'Revenue') AS accruals_cur,
                sumIf(accruals_for_sale,
                    toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                    AND category = 'Revenue') AS accruals_prev,
                sumIf(sale_commission,
                    toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                    AND category = 'Revenue') AS comm_cur,
                sumIf(sale_commission,
                    toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                    AND category = 'Revenue') AS comm_prev,
                sumIf(services_total,
                    toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                    AND category = 'Revenue') AS svc_cur,
                sumIf(services_total,
                    toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                    AND category = 'Revenue') AS svc_prev,
                sumIf(amount,
                    toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                    ) AS pay_cur,
                sumIf(amount,
                    toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                    ) AS pay_prev
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(operation_date) >= {d_prev_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })
        if txn_totals.result_rows:
            r = txn_totals.result_rows[0]
            accruals_cur = float(r[0] or 0)
            accruals_prev = float(r[1] or 0)
            commission_cur = abs(float(r[2] or 0))
            commission_prev = abs(float(r[3] or 0))
            services_cur = abs(float(r[4] or 0))
            services_prev = abs(float(r[5] or 0))
            payout_cur = float(r[6] or 0)
            payout_prev = float(r[7] or 0)
    except Exception as e:
        logger.warning("CH txn totals query failed: %s", e)

    try:
        # 2b. Bulk expense categories (Logistics sku=0, Storage, Acquiring, etc.)
        cat_result = ch.query("""
            SELECT
                category,
                sumIf(amount, toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}) AS total_cur,
                sumIf(amount, toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}) AS total_prev
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(operation_date) >= {d_prev_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
              AND category NOT IN ('Revenue', 'Marketing')
            GROUP BY category
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })
        for r in cat_result.result_rows:
            key = CAT_MAP.get(r[0], "other")
            if key in bulk_cur:
                bulk_cur[key] = float(r[1] or 0)
                bulk_prev[key] = float(r[2] or 0)
            else:
                bulk_cur["other"] += float(r[1] or 0)
                bulk_prev["other"] += float(r[2] or 0)
    except Exception as e:
        logger.warning("CH bulk categories query failed: %s", e)

    try:
        # 2c. Daily payout (excl Marketing) for dynamics chart
        txn_daily_result = ch.query("""
            SELECT
                toDate(operation_date) AS dt,
                sum(amount) AS payout
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(operation_date) >= {d_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
              AND category != 'Marketing'
            GROUP BY dt
            ORDER BY dt
        """, parameters={
            "shop_id": shop_id, "d_start": d_start, "d_end": d_end,
        })
        for r in txn_daily_result.result_rows:
            txn_daily[str(r[0])] = float(r[1] or 0)
    except Exception as e:
        logger.warning("CH txn daily query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 3. ADS: ad_spend current + prev + daily
    #    fact_ozon_ad_daily.dt is Date, so no toDate needed
    # ══════════════════════════════════════════════════════
    ad_spend_cur = 0.0
    ad_spend_prev = 0.0
    ads_daily = {}

    try:
        ads_totals = ch.query("""
            SELECT
                sumIf(money_spent, dt >= {d_start:Date} AND dt <= {d_end:Date}) AS ads_cur,
                sumIf(money_spent, dt >= {d_prev_start:Date} AND dt <= {d_prev_end:Date}) AS ads_prev
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d_prev_start:Date}
              AND dt <= {d_end:Date}
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })
        if ads_totals.result_rows:
            ad_spend_cur = float(ads_totals.result_rows[0][0] or 0)
            ad_spend_prev = float(ads_totals.result_rows[0][1] or 0)
    except Exception as e:
        logger.warning("CH ads totals query failed: %s", e)

    try:
        ads_daily_result = ch.query("""
            SELECT dt, sum(money_spent) AS ad_spend
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d_start:Date}
              AND dt <= {d_end:Date}
            GROUP BY dt
            ORDER BY dt
        """, parameters={
            "shop_id": shop_id, "d_start": d_start, "d_end": d_end,
        })
        for r in ads_daily_result.result_rows:
            ads_daily[str(r[0])] = float(r[1] or 0)
    except Exception as e:
        logger.warning("CH ads daily query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 4. COGS: cost_price × qty from Revenue TRANSACTIONS
    #    (same items as revenue/commission/logistics)
    #
    #    Revenue txn has SKU → map to offer_id via dim_ozon_products
    #    → then get cost from product_costs
    #    This ensures COGS counts the same delivered items as revenue
    # ══════════════════════════════════════════════════════
    cogs_cur = 0.0
    cogs_prev = 0.0
    cogs_daily = {}

    try:
        # Get SKU→offer_id from dim_ozon_products
        sku_map_result = await db.execute(
            text("""
                SELECT sku, offer_id
                FROM dim_ozon_products
                WHERE shop_id = :shop_id AND sku > 0
            """),
            {"shop_id": shop_id},
        )
        sku_to_offer = {int(r[0]): r[1] for r in sku_map_result.fetchall()}

        # Get cost prices from product_costs
        cost_result = await db.execute(
            text("""
                SELECT offer_id, COALESCE(cost_price, 0) + COALESCE(packaging_cost, 0) AS total_cost
                FROM product_costs
                WHERE shop_id = :shop_id AND (cost_price > 0 OR packaging_cost > 0)
            """),
            {"shop_id": shop_id},
        )
        cost_map = {r[0]: float(r[1]) for r in cost_result.fetchall()}

        if cost_map and sku_to_offer:
            # Build SKU→cost map
            sku_cost_map = {}
            for sku, offer_id in sku_to_offer.items():
                cost = cost_map.get(offer_id, 0)
                if cost > 0:
                    sku_cost_map[sku] = cost

            if sku_cost_map:
                sku_list = list(sku_cost_map.keys())
                # Get qty per SKU per day from Revenue transactions
                cogs_ch = ch.query("""
                    SELECT
                        toDate(operation_date) AS dt,
                        sku,
                        count() AS qty
                    FROM mms_analytics.fact_ozon_transactions FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(operation_date) >= {d_prev_start:Date}
                      AND toDate(operation_date) <= {d_end:Date}
                      AND category = 'Revenue'
                      AND sku IN {skus:Array(UInt64)}
                    GROUP BY dt, sku
                """, parameters={
                    "shop_id": shop_id,
                    "d_prev_start": d_prev_start, "d_end": d_end,
                    "skus": sku_list,
                })
                for r in cogs_ch.result_rows:
                    row_date = r[0]
                    sku = int(r[1])
                    qty = int(r[2] or 0)
                    cost = sku_cost_map.get(sku, 0)
                    cogs_val = cost * qty
                    if d_start <= row_date <= d_end:
                        cogs_cur += cogs_val
                        key = str(row_date)
                        cogs_daily[key] = cogs_daily.get(key, 0) + cogs_val
                    elif d_prev_start <= row_date <= d_prev_end:
                        cogs_prev += cogs_val
    except Exception as e:
        logger.warning("COGS calculation failed: %s", e)

    # ══════════════════════════════════════════════════════
    # Compute derived metrics
    #
    # revenue      = accruals_for_sale (gross sale amount from txn)
    # commission   = |sale_commission| (from Revenue txns)
    # services     = |services_total| (per-order logistics+services)
    # bulk_charges = |Logistics| + |Storage| + |Acquiring| + ...
    # mp_fees      = commission + services + bulk_charges
    # payout       = revenue - mp_fees (before ads & COGS)
    # profit       = revenue - mp_fees - ads(ad_daily) - cogs
    # ══════════════════════════════════════════════════════
    # Use accruals_for_sale as revenue (= Excel "Σ Продажи")
    revenue_cur = accruals_cur if accruals_cur > 0 else revenue_cur
    revenue_prev = accruals_prev if accruals_prev > 0 else revenue_prev

    # Bulk charges (negative in CH → abs)
    bulk_charges_cur = sum(abs(v) for v in bulk_cur.values())
    bulk_charges_prev = sum(abs(v) for v in bulk_prev.values())

    # Total MP fees = commission + services + bulk
    mp_fees_cur = commission_cur + services_cur + bulk_charges_cur
    mp_fees_prev = commission_prev + services_prev + bulk_charges_prev

    # Payout = revenue - mp_fees (what Ozon transfers before ads & COGS)
    payout_cur = revenue_cur - mp_fees_cur
    payout_prev = revenue_prev - mp_fees_prev

    # Profit
    profit_cur = payout_cur - ad_spend_cur - cogs_cur
    profit_prev = payout_prev - ad_spend_prev - cogs_prev
    profit_pct = round(profit_cur / revenue_cur * 100, 1) if revenue_cur > 0 else 0.0

    # ── Build KPI ──
    kpi = {
        "revenue": round(revenue_cur, 2),
        "revenue_delta": _safe_delta(revenue_cur, revenue_prev),
        "payout": round(payout_cur, 2),
        "payout_delta": _safe_delta(payout_cur, payout_prev),
        "mp_fees": round(mp_fees_cur, 2),
        "mp_fees_delta": _safe_delta(mp_fees_cur, mp_fees_prev),
        "ad_spend": round(ad_spend_cur, 2),
        "ad_spend_delta": _safe_delta(ad_spend_cur, ad_spend_prev),
        "cogs": round(cogs_cur, 2),
        "cogs_delta": _safe_delta(cogs_cur, cogs_prev),
        "profit": round(profit_cur, 2),
        "profit_delta": _safe_delta(profit_cur, profit_prev),
        "profit_pct": profit_pct,
        "orders": orders_cur,
        "orders_delta": _safe_delta(orders_cur, orders_prev),
    }

    # ── Build breakdown ──
    breakdown_resp = {
        "revenue": round(revenue_cur, 2),
        "commission": round(commission_cur, 2),
        "logistics": round(services_cur + abs(bulk_cur.get("logistics", 0)), 2),
        "storage": round(abs(bulk_cur.get("storage", 0)), 2),
        "acquiring": round(abs(bulk_cur.get("acquiring", 0)), 2),
        "advertising": round(ad_spend_cur, 2),
        "refunds": round(abs(bulk_cur.get("refunds", 0)), 2),
        "penalties": round(abs(bulk_cur.get("penalties", 0)), 2),
        "compensation": round(abs(bulk_cur.get("compensation", 0)), 2),
        "cogs": round(cogs_cur, 2),
        "profit": round(profit_cur, 2),
    }

    # ── Build daily dynamics ──
    all_dates = set()
    d = d_start
    while d <= d_end:
        all_dates.add(str(d))
        d += timedelta(days=1)

    daily_raw = []
    for ds in sorted(all_dates):
        rev = orders_daily.get(ds, {}).get("revenue", 0)
        ords = orders_daily.get(ds, {}).get("orders", 0)
        txn_d = txn_daily.get(ds, 0)  # sum(txn excl Marketing) for day
        ads_d = ads_daily.get(ds, 0)
        cogs_d = cogs_daily.get(ds, 0)
        # mp_fees = rev - txn_d (since txn_d = rev_txn_net + expenses)
        # payout = rev - mp_fees = txn_d + (rev - rev) approximately
        # Simpler: use accruals-based approach for daily is complex,
        # so just derive: payout_d = txn_d, mp_d = rev - txn_d
        # NOTE: txn_d already excludes Marketing
        payout_d = txn_d
        mp_d = max(0, rev - txn_d) if rev > 0 else 0
        profit_d = payout_d - ads_d - cogs_d

        daily_raw.append({
            "date": ds,
            "revenue": round(rev, 2),
            "payout": round(payout_d, 2),
            "mp_fees": round(mp_d, 2),
            "ad_spend": round(ads_d, 2),
            "cogs": round(cogs_d, 2),
            "orders": ords,
            "profit": round(profit_d, 2),
        })

    # Apply grouping if week/month
    if group_by in ("week", "month"):
        from collections import defaultdict
        grouped = defaultdict(lambda: {
            "revenue": 0, "payout": 0, "mp_fees": 0, "ad_spend": 0,
            "cogs": 0, "orders": 0, "profit": 0,
        })
        for pt in daily_raw:
            d_obj = date.fromisoformat(pt["date"])
            if group_by == "week":
                key = str(d_obj - timedelta(days=d_obj.weekday()))
            else:
                key = str(d_obj.replace(day=1))
            for field in ("revenue", "payout", "mp_fees", "ad_spend", "cogs", "orders", "profit"):
                grouped[key][field] += pt[field]

        daily_final = []
        for k in sorted(grouped.keys()):
            entry = {"date": k}
            for field in ("revenue", "payout", "mp_fees", "ad_spend", "cogs", "orders", "profit"):
                val = grouped[k][field]
                entry[field] = round(val, 2) if isinstance(val, float) else val
            daily_final.append(entry)
    else:
        daily_final = daily_raw

    # ── Build comparison ──
    comparison = {
        "current": {
            "revenue": round(revenue_cur, 2),
            "payout": round(payout_cur, 2),
            "mp_fees": round(mp_fees_cur, 2),
            "commission": round(commission_cur, 2),
            "logistics": round(services_cur + abs(bulk_cur.get("logistics", 0)), 2),
            "storage": round(abs(bulk_cur.get("storage", 0)), 2),
            "acquiring": round(abs(bulk_cur.get("acquiring", 0)), 2),
            "advertising": round(ad_spend_cur, 2),
            "refunds": round(abs(bulk_cur.get("refunds", 0)), 2),
            "penalties": round(abs(bulk_cur.get("penalties", 0)), 2),
            "cogs": round(cogs_cur, 2),
            "profit": round(profit_cur, 2),
            "orders": orders_cur,
        },
        "previous": {
            "revenue": round(revenue_prev, 2),
            "payout": round(payout_prev, 2),
            "mp_fees": round(mp_fees_prev, 2),
            "commission": round(commission_prev, 2),
            "logistics": round(services_prev + abs(bulk_prev.get("logistics", 0)), 2),
            "storage": round(abs(bulk_prev.get("storage", 0)), 2),
            "acquiring": round(abs(bulk_prev.get("acquiring", 0)), 2),
            "advertising": round(ad_spend_prev, 2),
            "refunds": round(abs(bulk_prev.get("refunds", 0)), 2),
            "penalties": round(abs(bulk_prev.get("penalties", 0)), 2),
            "cogs": round(cogs_prev, 2),
            "profit": round(profit_prev, 2),
            "orders": orders_prev,
        },
    }

    # Add delta percentages
    delta_pct = {}
    for key in comparison["current"]:
        cur_val = comparison["current"][key]
        prev_val = comparison["previous"].get(key, 0)
        delta_pct[key] = _safe_delta(cur_val, prev_val)
    comparison["delta_pct"] = delta_pct

    return {
        "shop_id": shop_id,
        "period": period,
        "date_from": str(d_start),
        "date_to": str(d_end),
        "group_by": group_by,
        "kpi": kpi,
        "breakdown": breakdown_resp,
        "daily": daily_final,
        "comparison": comparison,
    }


# ── WB Finances ─────────────────────────────────────────


@router.get("/wb")
async def get_wb_finances(
    shop_id: int = Query(..., description="Shop ID"),
    period: int = Query(7, ge=1, le=366, description="Period in days"),
    date_from: Optional[date] = Query(None, description="Custom range start"),
    date_to: Optional[date] = Query(None, description="Custom range end"),
    group_by: str = Query("day", description="Grouping: day, week, month"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    WB P&L overview: KPI, expense breakdown, daily dynamics, period comparison.

    Single data source: fact_finances FINAL (WB realization report)
      - Revenue:    retail_amount   (operation_type = 'Продажа')
      - Payout:     payout_amount   (ppvz_for_pay)
      - Commission: commission_amount (ppvz_sales_commission)
      - Logistics:  logistics_total (delivery_rub + rebill_logistic_cost)
      - Storage:    storage_fee
      - Acquiring:  wb_acquiring    (acquiring_fee)
      - Penalties:  penalty_total
    Plus:
      - Advertising: fact_advert_stats_v3 (spend)
      - COGS:        product_costs (PG) × qty from fact_finances
    """

    # ── Verify shop ownership ──
    shop_result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = shop_result.scalar_one_or_none()
    if not shop or shop.marketplace != "wildberries":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Shop not found")

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()
    except Exception as e:
        logger.error("ClickHouse connection error: %s", e)
        raise HTTPException(status_code=500, detail="Analytics unavailable")

    # ── Date ranges ──
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

    # ══════════════════════════════════════════════════════
    # 1. UNIFIED P&L from fact_finances FINAL
    #    Single source of truth: WB realization report
    # ══════════════════════════════════════════════════════
    revenue_cur = 0.0
    revenue_prev = 0.0
    payout_cur = 0.0
    payout_prev = 0.0
    commission_cur = 0.0
    commission_prev = 0.0
    logistics_cur = 0.0
    logistics_prev = 0.0
    storage_cur = 0.0
    storage_prev = 0.0
    penalties_cur = 0.0
    penalties_prev = 0.0
    acquiring_cur = 0.0
    acquiring_prev = 0.0
    acceptance_cur = 0.0
    acceptance_prev = 0.0
    deductions_cur = 0.0
    deductions_prev = 0.0
    returns_cur = 0.0
    returns_prev = 0.0
    orders_cur = 0
    orders_prev = 0

    try:
        fin_totals = ch.query("""
            SELECT
                -- Revenue = retail_price_withdisc_rub (розничная цена = что платит покупатель)
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Продажа' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS rev_cur,
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Продажа' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS rev_prev,

                -- Payout = ppvz_for_pay (к перечислению продавцу)
                sumIf(payout_amount,
                    operation_type = 'Продажа' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS pay_cur,
                sumIf(payout_amount,
                    operation_type = 'Продажа' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS pay_prev,

                -- Logistics (all operation types)
                sumIf(logistics_total,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS log_cur,
                sumIf(logistics_total,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS log_prev,

                -- Storage
                sumIf(storage_fee,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS stor_cur,
                sumIf(storage_fee,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS stor_prev,

                -- Penalties (only actual penalties, not deductions)
                sumIf(penalty_total,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS pen_cur,
                sumIf(penalty_total,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS pen_prev,

                -- Acquiring (bank fee)
                sumIf(wb_acquiring,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS acq_cur,
                sumIf(wb_acquiring,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS acq_prev,

                -- Acceptance (платная приёмка)
                sumIf(acceptance_fee,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS acc_cur,
                sumIf(acceptance_fee,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS acc_prev,

                -- Deductions (удержания)
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS ded_cur,
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS ded_prev,

                -- Returns
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Возврат' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS ret_cur,
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Возврат' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS ret_prev,

                -- Orders count (quantity from Продажа with positive qty)
                sumIf(quantity,
                    operation_type = 'Продажа' AND quantity > 0 AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS ord_cur,
                sumIf(quantity,
                    operation_type = 'Продажа' AND quantity > 0 AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS ord_prev

            FROM mms_analytics.fact_finances FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND marketplace = 1
              AND event_date >= {d_prev_start:Date}
              AND event_date <= {d_end:Date}
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })
        if fin_totals.result_rows:
            r = fin_totals.result_rows[0]
            revenue_cur = float(r[0] or 0)
            revenue_prev = float(r[1] or 0)
            payout_cur = float(r[2] or 0)
            payout_prev = float(r[3] or 0)
            logistics_cur = abs(float(r[4] or 0))
            logistics_prev = abs(float(r[5] or 0))
            storage_cur = abs(float(r[6] or 0))
            storage_prev = abs(float(r[7] or 0))
            penalties_cur = abs(float(r[8] or 0))
            penalties_prev = abs(float(r[9] or 0))
            acquiring_cur = abs(float(r[10] or 0))
            acquiring_prev = abs(float(r[11] or 0))
            acceptance_cur = abs(float(r[12] or 0))
            acceptance_prev = abs(float(r[13] or 0))
            deductions_cur = abs(float(r[14] or 0))
            deductions_prev = abs(float(r[15] or 0))
            returns_cur = abs(float(r[16] or 0))
            returns_prev = abs(float(r[17] or 0))
            orders_cur = int(r[18] or 0)
            orders_prev = int(r[19] or 0)
            # Commission = Revenue - Payout (includes SPP discount + WB commission)
            commission_cur = max(revenue_cur - payout_cur, 0)
            commission_prev = max(revenue_prev - payout_prev, 0)
    except Exception as e:
        logger.warning("CH WB finances totals query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 2. DAILY breakdown from fact_finances FINAL
    # ══════════════════════════════════════════════════════
    daily_data = {}
    try:
        daily_result = ch.query("""
            SELECT
                event_date AS dt,
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Продажа') AS revenue,
                sumIf(payout_amount, operation_type = 'Продажа') AS payout,
                sum(logistics_total) AS logistics,
                sum(storage_fee) AS storage,
                sum(penalty_total) AS penalties,
                sum(wb_acquiring) AS acquiring,
                sum(acceptance_fee) AS acceptance,
                sum(JSONExtractFloat(raw_payload, 'deduction')) AS deductions,
                sumIf(quantity, operation_type = 'Продажа' AND quantity > 0) AS orders,
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Возврат') AS returns
            FROM mms_analytics.fact_finances FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND marketplace = 1
              AND event_date >= {d_start:Date}
              AND event_date <= {d_end:Date}
            GROUP BY dt
            ORDER BY dt
        """, parameters={
            "shop_id": shop_id, "d_start": d_start, "d_end": d_end,
        })
        for r in daily_result.result_rows:
            rev_d = float(r[1] or 0)
            pay_d = float(r[2] or 0)
            daily_data[str(r[0])] = {
                "revenue": rev_d,
                "payout": pay_d,
                "commission": max(rev_d - pay_d, 0),
                "logistics": abs(float(r[3] or 0)),
                "storage": abs(float(r[4] or 0)),
                "penalties": abs(float(r[5] or 0)),
                "acquiring": abs(float(r[6] or 0)),
                "acceptance": abs(float(r[7] or 0)),
                "deductions": abs(float(r[8] or 0)),
                "orders": int(r[9] or 0),
                "returns": abs(float(r[10] or 0)),
            }
    except Exception as e:
        logger.warning("CH WB daily query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 3. ADS: fact_advert_stats_v3
    # ══════════════════════════════════════════════════════
    ad_spend_cur = 0.0
    ad_spend_prev = 0.0
    ads_daily = {}

    try:
        ads_totals = ch.query("""
            SELECT
                sumIf(spend, date >= {d_start:Date} AND date <= {d_end:Date}) AS ads_cur,
                sumIf(spend, date >= {d_prev_start:Date} AND date <= {d_prev_end:Date}) AS ads_prev
            FROM mms_analytics.fact_advert_stats_v3
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {d_prev_start:Date}
              AND date <= {d_end:Date}
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })
        if ads_totals.result_rows:
            ad_spend_cur = float(ads_totals.result_rows[0][0] or 0)
            ad_spend_prev = float(ads_totals.result_rows[0][1] or 0)
    except Exception as e:
        logger.warning("CH WB ads totals query failed: %s", e)

    try:
        ads_daily_result = ch.query("""
            SELECT date, sum(spend) AS ad_spend
            FROM mms_analytics.fact_advert_stats_v3
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {d_start:Date}
              AND date <= {d_end:Date}
            GROUP BY date
            ORDER BY date
        """, parameters={
            "shop_id": shop_id, "d_start": d_start, "d_end": d_end,
        })
        for r in ads_daily_result.result_rows:
            ads_daily[str(r[0])] = float(r[1] or 0)
    except Exception as e:
        logger.warning("CH WB ads daily query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 4. COGS: vendor_code from fact_finances → product_costs
    # ══════════════════════════════════════════════════════
    cogs_cur = 0.0
    cogs_prev = 0.0
    cogs_daily = {}

    try:
        cost_result = await db.execute(
            text("""
                SELECT offer_id, COALESCE(cost_price, 0) + COALESCE(packaging_cost, 0) AS total_cost
                FROM product_costs
                WHERE shop_id = :shop_id AND (cost_price > 0 OR packaging_cost > 0)
            """),
            {"shop_id": shop_id},
        )
        cost_map = {r[0].lower(): float(r[1]) for r in cost_result.fetchall()}

        if cost_map:
            cogs_ch = ch.query("""
                SELECT
                    event_date AS dt,
                    vendor_code AS art,
                    sum(quantity) AS qty
                FROM mms_analytics.fact_finances FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND marketplace = 1
                  AND operation_type = 'Продажа'
                  AND quantity > 0
                  AND event_date >= {d_prev_start:Date}
                  AND event_date <= {d_end:Date}
                GROUP BY dt, art
            """, parameters={
                "shop_id": shop_id,
                "d_prev_start": d_prev_start, "d_end": d_end,
            })
            for r in cogs_ch.result_rows:
                row_date = r[0]
                art = str(r[1] or "")
                qty = int(r[2] or 0)
                # Normalize to lowercase in Python (CH lower() doesn't handle Cyrillic)
                cost = cost_map.get(art.lower(), 0)
                cogs_val = cost * qty
                if d_start <= row_date <= d_end:
                    cogs_cur += cogs_val
                    key = str(row_date)
                    cogs_daily[key] = cogs_daily.get(key, 0) + cogs_val
                elif d_prev_start <= row_date <= d_prev_end:
                    cogs_prev += cogs_val
    except Exception as e:
        logger.warning("WB COGS calculation failed: %s", e)

    # ══════════════════════════════════════════════════════
    # Compute derived metrics
    #
    # Waterfall:  Revenue → -Commission → Payout → -Expenses → -Ads → -COGS → Profit
    #
    # mp_fees (display) = commission + logistics + storage + penalties + acquiring + acceptance + deductions
    # operating_expenses = logistics + storage + penalties + acquiring + acceptance + deductions
    # profit = payout - operating_expenses - ads - cogs
    #
    # NOTE: commission is NOT subtracted from payout (it's already excluded!)
    # ══════════════════════════════════════════════════════
    operating_cur = logistics_cur + storage_cur + penalties_cur + acquiring_cur + acceptance_cur + deductions_cur
    operating_prev = logistics_prev + storage_prev + penalties_prev + acquiring_prev + acceptance_prev + deductions_prev

    mp_fees_cur = commission_cur + operating_cur
    mp_fees_prev = commission_prev + operating_prev

    profit_cur = payout_cur - operating_cur - ad_spend_cur - cogs_cur
    profit_prev = payout_prev - operating_prev - ad_spend_prev - cogs_prev
    profit_pct = round(profit_cur / revenue_cur * 100, 1) if revenue_cur > 0 else 0.0

    # ── Build KPI ──
    kpi = {
        "revenue": round(revenue_cur, 2),
        "revenue_delta": _safe_delta(revenue_cur, revenue_prev),
        "payout": round(payout_cur, 2),
        "payout_delta": _safe_delta(payout_cur, payout_prev),
        "mp_fees": round(mp_fees_cur, 2),
        "mp_fees_delta": _safe_delta(mp_fees_cur, mp_fees_prev),
        "ad_spend": round(ad_spend_cur, 2),
        "ad_spend_delta": _safe_delta(ad_spend_cur, ad_spend_prev),
        "cogs": round(cogs_cur, 2),
        "cogs_delta": _safe_delta(cogs_cur, cogs_prev),
        "profit": round(profit_cur, 2),
        "profit_delta": _safe_delta(profit_cur, profit_prev),
        "profit_pct": profit_pct,
        "orders": orders_cur,
        "orders_delta": _safe_delta(orders_cur, orders_prev),
    }

    # ── Build breakdown ──
    breakdown_resp = {
        "revenue": round(revenue_cur, 2),
        "commission": round(commission_cur, 2),
        "logistics": round(logistics_cur, 2),
        "storage": round(storage_cur, 2),
        "acquiring": round(acquiring_cur, 2),
        "advertising": round(ad_spend_cur, 2),
        "refunds": round(returns_cur, 2),
        "penalties": round(penalties_cur, 2),
        "deductions": round(deductions_cur, 2),
        "compensation": round(acceptance_cur, 2),
        "cogs": round(cogs_cur, 2),
        "profit": round(profit_cur, 2),
    }

    # ── Build daily dynamics ──
    all_dates = set()
    d = d_start
    while d <= d_end:
        all_dates.add(str(d))
        d += timedelta(days=1)

    daily_raw = []
    for ds in sorted(all_dates):
        dd = daily_data.get(ds, {})
        rev = dd.get("revenue", 0)
        pay = dd.get("payout", 0)
        comm_d = dd.get("commission", 0)
        log_d = dd.get("logistics", 0)
        stor_d = dd.get("storage", 0)
        pen_d = dd.get("penalties", 0)
        acq_d = dd.get("acquiring", 0)
        acc_d = dd.get("acceptance", 0)
        ded_d = dd.get("deductions", 0)
        ords = dd.get("orders", 0)
        ads_d = ads_daily.get(ds, 0)
        cogs_d = cogs_daily.get(ds, 0)
        op_d = log_d + stor_d + pen_d + acq_d + acc_d + ded_d
        mp_d = comm_d + op_d
        profit_d = pay - op_d - ads_d - cogs_d

        daily_raw.append({
            "date": ds,
            "revenue": round(rev, 2),
            "payout": round(pay, 2),
            "mp_fees": round(mp_d, 2),
            "ad_spend": round(ads_d, 2),
            "cogs": round(cogs_d, 2),
            "orders": ords,
            "profit": round(profit_d, 2),
        })

    # Apply grouping if week/month
    if group_by in ("week", "month"):
        from collections import defaultdict
        grouped = defaultdict(lambda: {
            "revenue": 0, "payout": 0, "mp_fees": 0, "ad_spend": 0,
            "cogs": 0, "orders": 0, "profit": 0,
        })
        for pt in daily_raw:
            d_obj = date.fromisoformat(pt["date"])
            if group_by == "week":
                key = str(d_obj - timedelta(days=d_obj.weekday()))
            else:
                key = str(d_obj.replace(day=1))
            for field in ("revenue", "payout", "mp_fees", "ad_spend", "cogs", "orders", "profit"):
                grouped[key][field] += pt[field]

        daily_final = []
        for k in sorted(grouped.keys()):
            entry = {"date": k}
            for field in ("revenue", "payout", "mp_fees", "ad_spend", "cogs", "orders", "profit"):
                val = grouped[k][field]
                entry[field] = round(val, 2) if isinstance(val, float) else val
            daily_final.append(entry)
    else:
        daily_final = daily_raw

    # ── Build comparison ──
    comparison = {
        "current": {
            "revenue": round(revenue_cur, 2),
            "payout": round(payout_cur, 2),
            "mp_fees": round(mp_fees_cur, 2),
            "commission": round(commission_cur, 2),
            "logistics": round(logistics_cur, 2),
            "storage": round(storage_cur, 2),
            "acquiring": round(acquiring_cur, 2),
            "advertising": round(ad_spend_cur, 2),
            "refunds": round(returns_cur, 2),
            "penalties": round(penalties_cur, 2),
            "deductions": round(deductions_cur, 2),
            "compensation": round(acceptance_cur, 2),
            "cogs": round(cogs_cur, 2),
            "profit": round(profit_cur, 2),
            "orders": orders_cur,
        },
        "previous": {
            "revenue": round(revenue_prev, 2),
            "payout": round(payout_prev, 2),
            "mp_fees": round(mp_fees_prev, 2),
            "commission": round(commission_prev, 2),
            "logistics": round(logistics_prev, 2),
            "storage": round(storage_prev, 2),
            "acquiring": round(acquiring_prev, 2),
            "advertising": round(ad_spend_prev, 2),
            "refunds": round(returns_prev, 2),
            "penalties": round(penalties_prev, 2),
            "deductions": round(deductions_prev, 2),
            "compensation": round(acceptance_prev, 2),
            "cogs": round(cogs_prev, 2),
            "profit": round(profit_prev, 2),
            "orders": orders_prev,
        },
    }

    # Add delta percentages
    delta_pct = {}
    for key in comparison["current"]:
        cur_val = comparison["current"][key]
        prev_val = comparison["previous"].get(key, 0)
        delta_pct[key] = _safe_delta(cur_val, prev_val)
    comparison["delta_pct"] = delta_pct

    return {
        "shop_id": shop_id,
        "period": period,
        "date_from": str(d_start),
        "date_to": str(d_end),
        "group_by": group_by,
        "kpi": kpi,
        "breakdown": breakdown_resp,
        "daily": daily_final,
        "comparison": comparison,
    }


# ══════════════════════════════════════════════════════════
# Product-level P&L Endpoints
# ══════════════════════════════════════════════════════════


@router.get("/wb/products")
async def get_wb_products_finance(
    shop_id: int = Query(..., description="Shop ID"),
    period: int = Query(7, ge=1, le=366, description="Period in days"),
    date_from: Optional[date] = Query(None, description="Custom range start"),
    date_to: Optional[date] = Query(None, description="Custom range end"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    WB Product-level P&L: per-product breakdown of revenue, logistics,
    storage, acquiring, ads, COGS, profit — with period comparison.

    Sources:
      - fact_finances FINAL: revenue, payout, logistics, storage, acquiring (by vendor_code)
      - fact_advert_stats_v3: ad spend (by nm_id → vendor_code mapping)
      - product_costs (PG): cost_price + packaging_cost
    """

    # ── Verify shop ──
    shop_result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = shop_result.scalar_one_or_none()
    if not shop or shop.marketplace != "wildberries":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Shop not found")

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()
    except Exception as e:
        logger.error("ClickHouse connection error: %s", e)
        raise HTTPException(status_code=500, detail="Analytics unavailable")

    # ── Date ranges ──
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

    # ══════════════════════════════════════════════════════
    # 1. Per-product financials from fact_finances
    # ══════════════════════════════════════════════════════
    products = {}  # vendor_code -> {...}

    try:
        fin_products = ch.query("""
            SELECT
                vendor_code,
                JSONExtractUInt(raw_payload, 'nm_id') AS nm_id,

                -- Current period
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Продажа' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS rev_cur,
                sumIf(payout_amount,
                    operation_type = 'Продажа' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS pay_cur,
                sumIf(logistics_total,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS log_cur,
                sumIf(storage_fee,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS stor_cur,
                sumIf(wb_acquiring,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS acq_cur,
                sumIf(penalty_total,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS pen_cur,
                sumIf(quantity,
                    operation_type = 'Продажа' AND quantity > 0
                    AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS sales_cur,
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Возврат' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS ret_cur,

                -- Previous period
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Продажа' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS rev_prev,
                sumIf(payout_amount,
                    operation_type = 'Продажа' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS pay_prev,
                sumIf(logistics_total,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS log_prev,
                sumIf(storage_fee,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS stor_prev,
                sumIf(wb_acquiring,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS acq_prev,
                sumIf(penalty_total,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS pen_prev,
                sumIf(quantity,
                    operation_type = 'Продажа' AND quantity > 0
                    AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS sales_prev,
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Возврат' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS ret_prev

            FROM mms_analytics.fact_finances FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND marketplace = 1
              AND event_date >= {d_prev_start:Date}
              AND event_date <= {d_end:Date}
            GROUP BY vendor_code, nm_id
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })

        for r in fin_products.result_rows:
            vc = str(r[0] or "").strip()
            if not vc:
                vc = "__unknown__"
            nm = int(r[1] or 0)
            rev_cur = float(r[2] or 0)
            pay_cur = float(r[3] or 0)
            log_cur = abs(float(r[4] or 0))
            stor_cur = abs(float(r[5] or 0))
            acq_cur = abs(float(r[6] or 0))
            pen_cur = abs(float(r[7] or 0))
            sales_cur = int(r[8] or 0)
            ret_cur = abs(float(r[9] or 0))
            rev_prev = float(r[10] or 0)
            pay_prev = float(r[11] or 0)
            log_prev = abs(float(r[12] or 0))
            stor_prev = abs(float(r[13] or 0))
            acq_prev = abs(float(r[14] or 0))
            pen_prev = abs(float(r[15] or 0))
            sales_prev = int(r[16] or 0)
            ret_prev = abs(float(r[17] or 0))

            if vc not in products:
                products[vc] = {
                    "vendor_code": vc,
                    "nm_id": nm,
                    "cur": {"revenue": 0, "payout": 0, "logistics": 0, "storage": 0,
                            "acquiring": 0, "penalties": 0, "sales": 0, "returns": 0,
                            "ad_spend": 0, "cogs": 0},
                    "prev": {"revenue": 0, "payout": 0, "logistics": 0, "storage": 0,
                             "acquiring": 0, "penalties": 0, "sales": 0, "returns": 0,
                             "ad_spend": 0, "cogs": 0},
                }
            p = products[vc]
            if nm and not p["nm_id"]:
                p["nm_id"] = nm
            p["cur"]["revenue"] += rev_cur
            p["cur"]["payout"] += pay_cur
            p["cur"]["logistics"] += log_cur
            p["cur"]["storage"] += stor_cur
            p["cur"]["acquiring"] += acq_cur
            p["cur"]["penalties"] += pen_cur
            p["cur"]["sales"] += sales_cur
            p["cur"]["returns"] += ret_cur
            p["prev"]["revenue"] += rev_prev
            p["prev"]["payout"] += pay_prev
            p["prev"]["logistics"] += log_prev
            p["prev"]["storage"] += stor_prev
            p["prev"]["acquiring"] += acq_prev
            p["prev"]["penalties"] += pen_prev
            p["prev"]["sales"] += sales_prev
            p["prev"]["returns"] += ret_prev
    except Exception as e:
        logger.warning("CH WB product finance query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 2. Ad spend per nm_id from fact_advert_stats_v3
    # ══════════════════════════════════════════════════════
    # Build nm_id → vendor_code map from products
    nm_to_vc = {}
    for vc, p in products.items():
        if p["nm_id"]:
            nm_to_vc[p["nm_id"]] = vc

    try:
        ads_by_nm = ch.query("""
            SELECT
                nm_id,
                sumIf(spend, date >= {d_start:Date} AND date <= {d_end:Date}) AS ads_cur,
                sumIf(spend, date >= {d_prev_start:Date} AND date <= {d_prev_end:Date}) AS ads_prev
            FROM mms_analytics.fact_advert_stats_v3
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {d_prev_start:Date}
              AND date <= {d_end:Date}
            GROUP BY nm_id
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })

        unmatched_ads_cur = 0.0
        unmatched_ads_prev = 0.0
        for r in ads_by_nm.result_rows:
            nm = int(r[0] or 0)
            ads_c = float(r[1] or 0)
            ads_p = float(r[2] or 0)
            vc = nm_to_vc.get(nm)
            if vc and vc in products:
                products[vc]["cur"]["ad_spend"] += ads_c
                products[vc]["prev"]["ad_spend"] += ads_p
            else:
                unmatched_ads_cur += ads_c
                unmatched_ads_prev += ads_p

        # Add unmatched ads to __unmatched_ads__ bucket
        if unmatched_ads_cur > 0 or unmatched_ads_prev > 0:
            if "__unmatched_ads__" not in products:
                products["__unmatched_ads__"] = {
                    "vendor_code": "__unmatched_ads__",
                    "nm_id": 0,
                    "cur": {"revenue": 0, "payout": 0, "logistics": 0, "storage": 0,
                            "acquiring": 0, "penalties": 0, "sales": 0, "returns": 0,
                            "ad_spend": 0, "cogs": 0},
                    "prev": {"revenue": 0, "payout": 0, "logistics": 0, "storage": 0,
                             "acquiring": 0, "penalties": 0, "sales": 0, "returns": 0,
                             "ad_spend": 0, "cogs": 0},
                }
            products["__unmatched_ads__"]["cur"]["ad_spend"] += unmatched_ads_cur
            products["__unmatched_ads__"]["prev"]["ad_spend"] += unmatched_ads_prev
    except Exception as e:
        logger.warning("CH WB ads per product query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 3. COGS from product_costs (PG)
    # ══════════════════════════════════════════════════════
    try:
        cost_result = await db.execute(
            text("""
                SELECT offer_id, COALESCE(cost_price, 0) + COALESCE(packaging_cost, 0) AS total_cost
                FROM product_costs
                WHERE shop_id = :shop_id AND (cost_price > 0 OR packaging_cost > 0)
            """),
            {"shop_id": shop_id},
        )
        cost_map = {r[0].lower(): float(r[1]) for r in cost_result.fetchall()}

        for vc, p in products.items():
            unit_cost = cost_map.get(vc.lower(), 0)
            if unit_cost > 0:
                p["cur"]["cogs"] = round(unit_cost * p["cur"]["sales"], 2)
                p["prev"]["cogs"] = round(unit_cost * p["prev"]["sales"], 2)
    except Exception as e:
        logger.warning("PG product_costs query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 4. Build response
    # ══════════════════════════════════════════════════════
    result_products = []
    for vc, p in products.items():
        cur = p["cur"]
        prev = p["prev"]

        # Profit = payout - logistics - storage - acquiring - penalties - ad_spend - cogs
        cur_profit = cur["payout"] - cur["logistics"] - cur["storage"] - cur["acquiring"] - cur["penalties"] - cur["ad_spend"] - cur["cogs"]
        prev_profit = prev["payout"] - prev["logistics"] - prev["storage"] - prev["acquiring"] - prev["penalties"] - prev["ad_spend"] - prev["cogs"]

        current = {
            "sales": cur["sales"],
            "revenue": round(cur["revenue"], 2),
            "payout": round(cur["payout"], 2),
            "logistics": round(cur["logistics"], 2),
            "storage": round(cur["storage"], 2),
            "acquiring": round(cur["acquiring"], 2),
            "penalties": round(cur["penalties"], 2),
            "returns": round(cur["returns"], 2),
            "ad_spend": round(cur["ad_spend"], 2),
            "cogs": round(cur["cogs"], 2),
            "profit": round(cur_profit, 2),
        }
        previous = {
            "sales": prev["sales"],
            "revenue": round(prev["revenue"], 2),
            "payout": round(prev["payout"], 2),
            "logistics": round(prev["logistics"], 2),
            "storage": round(prev["storage"], 2),
            "acquiring": round(prev["acquiring"], 2),
            "penalties": round(prev["penalties"], 2),
            "returns": round(prev["returns"], 2),
            "ad_spend": round(prev["ad_spend"], 2),
            "cogs": round(prev["cogs"], 2),
            "profit": round(prev_profit, 2),
        }

        delta_pct = {}
        for key in current:
            delta_pct[key] = _safe_delta(current[key], previous[key])

        pct_of_rev = {}
        rev = current["revenue"]
        if rev > 0:
            for key in ("logistics", "storage", "acquiring", "penalties", "ad_spend", "cogs", "profit", "returns"):
                pct_of_rev[key] = round(current[key] / rev * 100, 1)

        result_products.append({
            "vendor_code": vc,
            "nm_id": p["nm_id"],
            "current": current,
            "previous": previous,
            "delta_pct": delta_pct,
            "pct_of_revenue": pct_of_rev,
        })

    # Sort by current revenue descending
    result_products.sort(key=lambda x: x["current"]["revenue"], reverse=True)

    # Totals
    total_cur = {}
    total_prev = {}
    for key in ("sales", "revenue", "payout", "logistics", "storage", "acquiring",
                "penalties", "returns", "ad_spend", "cogs", "profit"):
        total_cur[key] = round(sum(p["current"][key] for p in result_products), 2)
        total_prev[key] = round(sum(p["previous"][key] for p in result_products), 2)

    total_delta = {}
    for key in total_cur:
        total_delta[key] = _safe_delta(total_cur[key], total_prev[key])

    return {
        "shop_id": shop_id,
        "date_from": str(d_start),
        "date_to": str(d_end),
        "products": result_products,
        "totals": {
            "current": total_cur,
            "previous": total_prev,
            "delta_pct": total_delta,
        },
    }


@router.get("/ozon/products")
async def get_ozon_products_finance(
    shop_id: int = Query(..., description="Shop ID"),
    period: int = Query(7, ge=1, le=366, description="Period in days"),
    date_from: Optional[date] = Query(None, description="Custom range start"),
    date_to: Optional[date] = Query(None, description="Custom range end"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Ozon Product-level P&L: per-product breakdown of revenue, commission,
    logistics, ads, COGS, profit — with period comparison.

    Sources:
      - fact_ozon_orders FINAL: revenue, quantity (by offer_id)
      - fact_ozon_finances: transactions breakdown (by offer_id/sku)
      - fact_ozon_ad_daily: ad spend (by sku_id → offer_id)
      - product_costs (PG): cost_price + packaging_cost
    """

    # ── Verify shop ──
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

    # ── Date ranges ──
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

    products = {}  # offer_id -> {...}

    # ══════════════════════════════════════════════════════
    # 1. Revenue from fact_ozon_orders
    # ══════════════════════════════════════════════════════
    try:
        orders_result = ch.query("""
            SELECT
                offer_id,
                sumIf(price * quantity, toDate(order_date) >= {d_start:Date} AND toDate(order_date) <= {d_end:Date}) AS rev_cur,
                sumIf(quantity, toDate(order_date) >= {d_start:Date} AND toDate(order_date) <= {d_end:Date}) AS sales_cur,
                sumIf(price * quantity, toDate(order_date) >= {d_prev_start:Date} AND toDate(order_date) <= {d_prev_end:Date}) AS rev_prev,
                sumIf(quantity, toDate(order_date) >= {d_prev_start:Date} AND toDate(order_date) <= {d_prev_end:Date}) AS sales_prev
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(order_date) >= {d_prev_start:Date}
              AND toDate(order_date) <= {d_end:Date}
              AND status NOT IN ('cancelled', 'canceled')
            GROUP BY offer_id
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })

        for r in orders_result.result_rows:
            oid = str(r[0] or "").strip()
            if not oid:
                oid = "__unknown__"
            products[oid] = {
                "offer_id": oid,
                "cur": {
                    "revenue": float(r[1] or 0), "sales": int(r[2] or 0),
                    "commission": 0, "logistics": 0, "storage": 0, "acquiring": 0,
                    "penalties": 0, "returns": 0, "ad_spend": 0, "cogs": 0,
                },
                "prev": {
                    "revenue": float(r[3] or 0), "sales": int(r[4] or 0),
                    "commission": 0, "logistics": 0, "storage": 0, "acquiring": 0,
                    "penalties": 0, "returns": 0, "ad_spend": 0, "cogs": 0,
                },
            }
    except Exception as e:
        logger.warning("CH Ozon orders per product query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 2. Transaction details from fact_ozon_transactions
    #    Revenue txns: per-order commission + services (by sku)
    #    Bulk charges: Logistics/Storage/etc — NOT per-product
    # ══════════════════════════════════════════════════════

    # Build sku → offer_id mapping
    sku_to_offer = {}
    try:
        sku_map = ch.query("""
            SELECT DISTINCT sku, offer_id
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        for r in sku_map.result_rows:
            sku_to_offer[int(r[0])] = str(r[1])
    except Exception:
        pass

    try:
        # 2a. Per-product commission + services from Revenue transactions
        txn_result = ch.query("""
            SELECT
                sku,
                sumIf(abs(sale_commission),
                    toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                ) AS comm_cur,
                sumIf(abs(services_total),
                    toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                ) AS svc_cur,
                sumIf(abs(sale_commission),
                    toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                ) AS comm_prev,
                sumIf(abs(services_total),
                    toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                ) AS svc_prev
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND category = 'Revenue'
              AND toDate(operation_date) >= {d_prev_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
            GROUP BY sku
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })

        for r in txn_result.result_rows:
            sku = int(r[0] or 0)
            oid = sku_to_offer.get(sku, str(sku))
            comm_cur = float(r[1] or 0)
            svc_cur = float(r[2] or 0)
            comm_prev = float(r[3] or 0)
            svc_prev = float(r[4] or 0)
            if oid in products:
                products[oid]["cur"]["commission"] += comm_cur
                products[oid]["cur"]["logistics"] += svc_cur  # services_total ≈ per-order logistics
                products[oid]["prev"]["commission"] += comm_prev
                products[oid]["prev"]["logistics"] += svc_prev
    except Exception as e:
        logger.warning("CH Ozon txn per product query failed: %s", e)

    # 2b. Bulk charges (Logistics, Storage, Acquiring)
    #     These are NOT per-product — distribute proportionally by revenue share
    CAT_MAP = {
        "Logistics": "logistics",
        "Storage": "storage",
        "Acquiring": "acquiring",
    }
    bulk_cur_total = {}
    bulk_prev_total = {}
    try:
        cat_result = ch.query("""
            SELECT
                category,
                sumIf(abs(amount), toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}) AS total_cur,
                sumIf(abs(amount), toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}) AS total_prev
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(operation_date) >= {d_prev_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
              AND category IN ('Logistics', 'Storage', 'Acquiring')
            GROUP BY category
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })

        for r in cat_result.result_rows:
            key = CAT_MAP.get(r[0], "other")
            bulk_cur_total[key] = float(r[1] or 0)
            bulk_prev_total[key] = float(r[2] or 0)

        # Distribute bulk charges proportionally by revenue
        total_rev_cur = sum(p["cur"]["revenue"] for p in products.values())
        total_rev_prev = sum(p["prev"]["revenue"] for p in products.values())

        for oid, p in products.items():
            for bkey in ("logistics", "storage", "acquiring"):
                if bkey in bulk_cur_total and total_rev_cur > 0:
                    share = p["cur"]["revenue"] / total_rev_cur
                    p["cur"][bkey] += round(bulk_cur_total[bkey] * share, 2)
                if bkey in bulk_prev_total and total_rev_prev > 0:
                    share = p["prev"]["revenue"] / total_rev_prev
                    p["prev"][bkey] += round(bulk_prev_total[bkey] * share, 2)
    except Exception as e:
        logger.warning("CH Ozon bulk charges query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 3. Ad spend from fact_ozon_ad_daily
    # ══════════════════════════════════════════════════════
    try:
        ads_result = ch.query("""
            SELECT
                sku_id,
                sumIf(spend, dt >= {d_start:Date} AND dt <= {d_end:Date}) AS ads_cur,
                sumIf(spend, dt >= {d_prev_start:Date} AND dt <= {d_prev_end:Date}) AS ads_prev
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d_prev_start:Date}
              AND dt <= {d_end:Date}
            GROUP BY sku_id
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })

        # sku_to_offer already built in section 2
        for r in ads_result.result_rows:
            sku = int(r[0] or 0)
            ads_c = float(r[1] or 0)
            ads_p = float(r[2] or 0)
            oid = sku_to_offer.get(sku, str(sku))
            if oid in products:
                products[oid]["cur"]["ad_spend"] += ads_c
                products[oid]["prev"]["ad_spend"] += ads_p
    except Exception as e:
        logger.warning("CH Ozon ads per product query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 4. COGS from product_costs (PG)
    # ══════════════════════════════════════════════════════
    try:
        cost_result = await db.execute(
            text("""
                SELECT offer_id, COALESCE(cost_price, 0) + COALESCE(packaging_cost, 0) AS total_cost
                FROM product_costs
                WHERE shop_id = :shop_id AND (cost_price > 0 OR packaging_cost > 0)
            """),
            {"shop_id": shop_id},
        )
        cost_map = {r[0].lower(): float(r[1]) for r in cost_result.fetchall()}

        for oid, p in products.items():
            unit_cost = cost_map.get(oid.lower(), 0)
            if unit_cost > 0:
                p["cur"]["cogs"] = round(unit_cost * p["cur"]["sales"], 2)
                p["prev"]["cogs"] = round(unit_cost * p["prev"]["sales"], 2)
    except Exception as e:
        logger.warning("PG product_costs query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 5. Build response
    # ══════════════════════════════════════════════════════
    result_products = []
    for oid, p in products.items():
        cur = p["cur"]
        prev = p["prev"]

        # Ozon profit = revenue - commission - logistics - acquiring - ad_spend - cogs
        cur_profit = cur["revenue"] - cur["commission"] - cur["logistics"] - cur["acquiring"] - cur["ad_spend"] - cur["cogs"]
        prev_profit = prev["revenue"] - prev["commission"] - prev["logistics"] - prev["acquiring"] - prev["ad_spend"] - prev["cogs"]

        current = {
            "sales": cur["sales"],
            "revenue": round(cur["revenue"], 2),
            "commission": round(cur["commission"], 2),
            "logistics": round(cur["logistics"], 2),
            "storage": round(cur["storage"], 2),
            "acquiring": round(cur["acquiring"], 2),
            "ad_spend": round(cur["ad_spend"], 2),
            "cogs": round(cur["cogs"], 2),
            "profit": round(cur_profit, 2),
        }
        previous = {
            "sales": prev["sales"],
            "revenue": round(prev["revenue"], 2),
            "commission": round(prev["commission"], 2),
            "logistics": round(prev["logistics"], 2),
            "storage": round(prev["storage"], 2),
            "acquiring": round(prev["acquiring"], 2),
            "ad_spend": round(prev["ad_spend"], 2),
            "cogs": round(prev["cogs"], 2),
            "profit": round(prev_profit, 2),
        }

        delta_pct = {}
        for key in current:
            delta_pct[key] = _safe_delta(current[key], previous[key])

        pct_of_rev = {}
        rev = current["revenue"]
        if rev > 0:
            for key in ("commission", "logistics", "storage", "acquiring", "ad_spend", "cogs", "profit"):
                pct_of_rev[key] = round(current[key] / rev * 100, 1)

        result_products.append({
            "vendor_code": oid,
            "current": current,
            "previous": previous,
            "delta_pct": delta_pct,
            "pct_of_revenue": pct_of_rev,
        })

    # Sort by current revenue descending
    result_products.sort(key=lambda x: x["current"]["revenue"], reverse=True)

    # Totals
    total_cur = {}
    total_prev = {}
    for key in ("sales", "revenue", "commission", "logistics", "storage", "acquiring", "ad_spend", "cogs", "profit"):
        total_cur[key] = round(sum(p["current"][key] for p in result_products), 2)
        total_prev[key] = round(sum(p["previous"][key] for p in result_products), 2)

    total_delta = {}
    for key in total_cur:
        total_delta[key] = _safe_delta(total_cur[key], total_prev[key])

    return {
        "shop_id": shop_id,
        "date_from": str(d_start),
        "date_to": str(d_end),
        "products": result_products,
        "totals": {
            "current": total_cur,
            "previous": total_prev,
            "delta_pct": total_delta,
        },
    }
