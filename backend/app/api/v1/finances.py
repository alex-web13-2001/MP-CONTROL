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

    Data sources:
      - Revenue/Commission:  fact_orders_raw_latest (finished_price, price_with_disc)
      - Expenses:            fact_finances_latest   (logistics, storage, penalties)
      - Advertising:         fact_advert_stats_v3   (spend)
      - COGS:                product_costs (PG) × qty from orders
    """

    # ── Verify shop ownership ──
    shop_result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = shop_result.scalar_one_or_none()
    if not shop or shop.marketplace != "wb":
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
    # 1. ORDERS: revenue + count
    #    finished_price = what buyer pays (after all discounts)
    #    NOTE: price_with_disc - finished_price = SPP discount
    #    (covered by WB, NOT a seller expense – don't include!)
    # ══════════════════════════════════════════════════════
    revenue_cur = 0.0
    revenue_prev = 0.0
    orders_cur = 0
    orders_prev = 0

    orders_daily = {}

    try:
        orders_totals = ch.query("""
            SELECT
                sumIf(finished_price, toDate(date) >= {d_start:Date} AND toDate(date) <= {d_end:Date}) AS rev_cur,
                countIf(toDate(date) >= {d_start:Date} AND toDate(date) <= {d_end:Date}) AS ord_cur,
                sumIf(finished_price, toDate(date) >= {d_prev_start:Date} AND toDate(date) <= {d_prev_end:Date}) AS rev_prev,
                countIf(toDate(date) >= {d_prev_start:Date} AND toDate(date) <= {d_prev_end:Date}) AS ord_prev
            FROM mms_analytics.fact_orders_raw_latest
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(date) >= {d_prev_start:Date}
              AND toDate(date) <= {d_end:Date}
              AND is_cancel = 0
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
        logger.warning("CH WB orders totals query failed: %s", e)

    try:
        orders_daily_result = ch.query("""
            SELECT
                toDate(date) AS dt,
                sum(finished_price) AS revenue,
                count() AS orders_count
            FROM mms_analytics.fact_orders_raw_latest
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(date) >= {d_start:Date}
              AND toDate(date) <= {d_end:Date}
              AND is_cancel = 0
            GROUP BY dt
            ORDER BY dt
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
        })
        for r in orders_daily_result.result_rows:
            orders_daily[str(r[0])] = {
                "revenue": float(r[1] or 0),
                "orders": int(r[2] or 0),
            }
    except Exception as e:
        logger.warning("CH WB orders daily query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 2. EXPENSES from fact_finances_latest
    #    Logistics, Storage, Penalties, Other
    # ══════════════════════════════════════════════════════
    logistics_cur = 0.0
    logistics_prev = 0.0
    storage_cur = 0.0
    storage_prev = 0.0
    penalties_cur = 0.0
    penalties_prev = 0.0
    other_expenses_cur = 0.0
    other_expenses_prev = 0.0
    expenses_daily = {}

    EXPENSE_MAP = {
        "Логистика": "logistics",
        "Хранение": "storage",
        "Штраф": "penalties",
    }

    try:
        exp_totals = ch.query("""
            SELECT
                operation_type,
                sumIf(logistics_total + storage_fee + penalty_total,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}) AS exp_cur,
                sumIf(logistics_total + storage_fee + penalty_total,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}) AS exp_prev
            FROM mms_analytics.fact_finances_latest
            WHERE shop_id = {shop_id:UInt32}
              AND event_date >= {d_prev_start:Date}
              AND event_date <= {d_end:Date}
            GROUP BY operation_type
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })
        for r in exp_totals.result_rows:
            op_type = r[0]
            val_cur = abs(float(r[1] or 0))
            val_prev = abs(float(r[2] or 0))
            cat = EXPENSE_MAP.get(op_type, "other")
            if cat == "logistics":
                logistics_cur += val_cur
                logistics_prev += val_prev
            elif cat == "storage":
                storage_cur += val_cur
                storage_prev += val_prev
            elif cat == "penalties":
                penalties_cur += val_cur
                penalties_prev += val_prev
            else:
                other_expenses_cur += val_cur
                other_expenses_prev += val_prev
    except Exception as e:
        logger.warning("CH WB expenses query failed: %s", e)

    try:
        exp_daily_result = ch.query("""
            SELECT
                event_date AS dt,
                sum(logistics_total) AS logistics,
                sum(storage_fee) AS storage,
                sum(penalty_total) AS penalties
            FROM mms_analytics.fact_finances_latest
            WHERE shop_id = {shop_id:UInt32}
              AND event_date >= {d_start:Date}
              AND event_date <= {d_end:Date}
            GROUP BY dt
            ORDER BY dt
        """, parameters={
            "shop_id": shop_id, "d_start": d_start, "d_end": d_end,
        })
        for r in exp_daily_result.result_rows:
            expenses_daily[str(r[0])] = {
                "logistics": abs(float(r[1] or 0)),
                "storage": abs(float(r[2] or 0)),
                "penalties": abs(float(r[3] or 0)),
            }
    except Exception as e:
        logger.warning("CH WB expenses daily query failed: %s", e)

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
    # 4. COGS: supplier_article = offer_id in product_costs
    #    Each order in fact_orders_raw = 1 item
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
            articles = list(cost_map.keys())
            cogs_ch = ch.query("""
                SELECT
                    toDate(date) AS dt,
                    supplier_article AS art,
                    count() AS qty
                FROM mms_analytics.fact_orders_raw_latest
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(date) >= {d_prev_start:Date}
                  AND toDate(date) <= {d_end:Date}
                  AND is_cancel = 0
                GROUP BY dt, art
            """, parameters={
                "shop_id": shop_id,
                "d_prev_start": d_prev_start, "d_end": d_end,
            })
            for r in cogs_ch.result_rows:
                row_date = r[0]
                art = r[1]
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
    # WB: NO commission in our data (ppvz_for_pay not available per-order)
    # mp_fees = logistics + storage + penalties + other (fact_finances only)
    # payout  = revenue - mp_fees (before ads & COGS)
    # profit  = payout - ads - cogs
    # ══════════════════════════════════════════════════════
    mp_fees_cur = logistics_cur + storage_cur + penalties_cur + other_expenses_cur
    mp_fees_prev = logistics_prev + storage_prev + penalties_prev + other_expenses_prev

    payout_cur = revenue_cur - mp_fees_cur
    payout_prev = revenue_prev - mp_fees_prev

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
        "commission": 0,
        "logistics": round(logistics_cur, 2),
        "storage": round(storage_cur, 2),
        "advertising": round(ad_spend_cur, 2),
        "penalties": round(penalties_cur, 2),
        "other": round(other_expenses_cur, 2),
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
        od = orders_daily.get(ds, {})
        ed = expenses_daily.get(ds, {})
        rev = od.get("revenue", 0)
        ords = od.get("orders", 0)
        logist_d = ed.get("logistics", 0)
        stor_d = ed.get("storage", 0)
        pen_d = ed.get("penalties", 0)
        ads_d = ads_daily.get(ds, 0)
        cogs_d = cogs_daily.get(ds, 0)
        mp_d = logist_d + stor_d + pen_d
        payout_d = rev - mp_d
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
            "commission": 0,
            "logistics": round(logistics_cur, 2),
            "storage": round(storage_cur, 2),
            "advertising": round(ad_spend_cur, 2),
            "penalties": round(penalties_cur, 2),
            "other": round(other_expenses_cur, 2),
            "cogs": round(cogs_cur, 2),
            "profit": round(profit_cur, 2),
            "orders": orders_cur,
        },
        "previous": {
            "revenue": round(revenue_prev, 2),
            "payout": round(payout_prev, 2),
            "mp_fees": round(mp_fees_prev, 2),
            "commission": 0,
            "logistics": round(logistics_prev, 2),
            "storage": round(storage_prev, 2),
            "advertising": round(ad_spend_prev, 2),
            "penalties": round(penalties_prev, 2),
            "other": round(other_expenses_prev, 2),
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
