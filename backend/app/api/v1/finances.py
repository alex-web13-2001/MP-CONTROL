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
    # 1. TRANSACTIONS — using Ozon's built-in detail fields
    #
    #  Revenue transactions have per-item detail (category = 'Revenue'):
    #    accruals_for_sale  = gross sale amount (= Excel "Σ Продажи")
    #    sale_commission    = Ozon commission (negative)
    #    services_total     = per-item logistics + services (negative)
    #    amount             = accruals + commission + services (net)
    #
    #  Expense categories (Logistics sku=0, Storage, Acquiring, etc.)
    #    → bulk charges NOT embedded in Revenue per-order data
    #
    #  P&L formula (Transaction-based):
    #    revenue      = accruals_for_sale (from Revenue txns)
    #    orders       = count() of Revenue txns
    #    commission   = |sale_commission|
    #    services     = |services_total| (per-order logistics)
    #    bulk_charges = |Logistics| + |Storage| + |Acquiring| + |Refund| + ...
    #    mp_fees      = commission + services + bulk_charges
    #    payout       = sum(ALL txn amounts) = Excel "К перечислению"
    #    profit       = revenue - mp_fees - ads(ad_daily) - cogs
    # ══════════════════════════════════════════════════════

    # Revenue transaction fields
    revenue_cur = 0.0      # sum(accruals_for_sale)
    revenue_prev = 0.0
    orders_cur = 0         # count() of Revenue txns
    orders_prev = 0
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
        "marketing": 0.0,
        "other": 0.0,
    }
    bulk_prev = {
        "logistics": 0.0,
        "storage": 0.0,
        "acquiring": 0.0,
        "refunds": 0.0,
        "penalties": 0.0,
        "compensation": 0.0,
        "marketing": 0.0,
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
        "Marketing": "marketing",
    }

    try:
        # 1a. Revenue transaction detail + total payout
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
                    ) AS pay_prev,
                countIf(
                    toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                    AND category = 'Revenue') AS ord_cur,
                countIf(
                    toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                    AND category = 'Revenue') AS ord_prev
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
            revenue_cur = float(r[0] or 0)
            revenue_prev = float(r[1] or 0)
            commission_cur = abs(float(r[2] or 0))
            commission_prev = abs(float(r[3] or 0))
            services_cur = abs(float(r[4] or 0))
            services_prev = abs(float(r[5] or 0))
            payout_cur = float(r[6] or 0)
            payout_prev = float(r[7] or 0)
            orders_cur = int(r[8] or 0)
            orders_prev = int(r[9] or 0)
    except Exception as e:
        logger.warning("CH txn totals query failed: %s", e)

    # Daily orders & revenue for chart
    orders_daily = {}
    try:
        orders_daily_result = ch.query("""
            SELECT
                toDate(operation_date) AS dt,
                sum(accruals_for_sale) AS revenue,
                count() AS orders_count
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(operation_date) >= {d_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
              AND category = 'Revenue'
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
              AND category NOT IN ('Revenue')
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
    # 3. ADS: taken directly from transactions (Marketing) to match balance
    # ══════════════════════════════════════════════════════
    ad_spend_cur = abs(bulk_cur.get("marketing", 0))
    ad_spend_prev = abs(bulk_prev.get("marketing", 0))
    ads_daily = {}

    try:
        # Get daily ad spend from transactions for the chart
        ads_daily_result = ch.query("""
            SELECT toDate(operation_date) AS dt, sum(amount) AS ad_spend
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(operation_date) >= {d_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
              AND category = 'Marketing'
            GROUP BY dt
            ORDER BY dt
        """, parameters={
            "shop_id": shop_id, "d_start": d_start, "d_end": d_end,
        })
        for r in ads_daily_result.result_rows:
            ads_daily[str(r[0])] = abs(float(r[1] or 0))
    except Exception as e:
        logger.warning("CH ads daily from txn query failed: %s", e)

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
    # bulk_charges = |Logistics| + |Storage| + |Acquiring| + |Refunds| + |Penalties| + |Compensation| + |Other| (excluding Marketing)
    # mp_fees      = commission + services + bulk_charges
    # payout       = sum of ALL txn amounts including Marketing (what Ozon actually transfers)
    # operating    = services + bulk_charges (for frontend to display as Расходы МП directly)
    # profit       = revenue - commission - operating - ad_spend - cogs
    # ══════════════════════════════════════════════════════

    # Bulk charges (excluding Marketing which is ads)
    bulk_charges_cur = sum(abs(v) for k, v in bulk_cur.items() if k != "marketing")
    bulk_charges_prev = sum(abs(v) for k, v in bulk_prev.items() if k != "marketing")

    # Total MP fees = commission + services + bulk
    mp_fees_cur = commission_cur + services_cur + bulk_charges_cur
    mp_fees_prev = commission_prev + services_prev + bulk_charges_prev
    
    # Operating expenses (everything except commission and ads)
    operating_cur = services_cur + bulk_charges_cur
    operating_prev = services_prev + bulk_charges_prev

    # Profit (based on revenue, subtracting all expenses)
    profit_cur = revenue_cur - mp_fees_cur - ad_spend_cur - cogs_cur
    profit_prev = revenue_prev - mp_fees_prev - ad_spend_prev - cogs_prev
    profit_pct = round(profit_cur / revenue_cur * 100, 1) if revenue_cur > 0 else 0.0

    # ── Build KPI ──
    kpi = {
        "revenue": round(revenue_cur, 2),
        "revenue_delta": _safe_delta(revenue_cur, revenue_prev),
        "payout": round(payout_cur, 2),  # This is the exact sum of transactions
        "payout_delta": _safe_delta(payout_cur, payout_prev),
        "mp_fees": round(mp_fees_cur, 2),
        "mp_fees_delta": _safe_delta(mp_fees_cur, mp_fees_prev),
        "operating": round(operating_cur, 2),
        "operating_delta": _safe_delta(operating_cur, operating_prev),
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
        # mp_d is operating + commission (mp_fees)
        mp_d = max(0, rev - txn_d - ads_d) if rev > 0 else 0
        payout_d = txn_d
        profit_d = rev - mp_d - ads_d - cogs_d

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
      - Logistics:  wb_delivery_rub (delivery_rub only, without rebill_logistic_cost)
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
                -- Revenue = retail_price_withdisc_rub (продажи минус возвраты)
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Продажа' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date})
                 - sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Возврат' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}) AS rev_cur,
                
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Продажа' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date})
                 - sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Возврат' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}) AS rev_prev,

                -- Payout = ppvz_for_pay (к перечислению продавцу)
                sumIf(payout_amount, operation_type = 'Продажа' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date})
                 - sumIf(payout_amount, operation_type = 'Возврат' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}) AS pay_cur,
                 
                sumIf(payout_amount, operation_type = 'Продажа' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date})
                 - sumIf(payout_amount, operation_type = 'Возврат' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}) AS pay_prev,

                -- Logistics (delivery_rub only, NOT rebill_logistic_cost)
                sumIf(wb_delivery_rub,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS log_cur,
                sumIf(wb_delivery_rub,
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

                -- Deductions (удержания, ИСКЛЮЧАЯ рекламу во избежание двойного учёта с fact_advert_stats)
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date} 
                    AND positionCaseInsensitiveUTF8(JSONExtractString(raw_payload, 'bonus_type_name'), 'продвижение') = 0
                ) AS ded_cur,
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                    AND positionCaseInsensitiveUTF8(JSONExtractString(raw_payload, 'bonus_type_name'), 'продвижение') = 0
                ) AS ded_prev,

                -- Returns
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Возврат' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}) AS returns_cur,
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Возврат' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}) AS returns_prev,
                sumIf(quantity, operation_type = 'Продажа' AND quantity > 0 AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}) AS orders_cur,
                sumIf(quantity, operation_type = 'Продажа' AND quantity > 0 AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}) AS orders_prev,
                sumIf(JSONExtractFloat(raw_payload, 'deduction'), event_date >= {d_start:Date} AND event_date <= {d_end:Date}) AS total_deductions_cur,
                sumIf(JSONExtractFloat(raw_payload, 'deduction'), event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}) AS total_deductions_prev
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
            total_deductions_cur = abs(float(r[20] or 0))
            total_deductions_prev = abs(float(r[21] or 0))

            # Commission = Revenue - Payout (includes SPP discount + WB commission) + Acquiring
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
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Продажа') 
                 - sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Возврат') AS revenue,
                sumIf(payout_amount, operation_type = 'Продажа') - sumIf(payout_amount, operation_type = 'Возврат') AS payout,
                sum(wb_delivery_rub) AS logistics,
                sum(storage_fee) AS storage,
                sum(penalty_total) AS penalties,
                sum(wb_acquiring) AS acquiring,
                sum(acceptance_fee) AS acceptance,
                sumIf(JSONExtractFloat(raw_payload, 'deduction'), positionCaseInsensitiveUTF8(JSONExtractString(raw_payload, 'bonus_type_name'), 'продвижение') = 0) AS deductions,
                sum(JSONExtractFloat(raw_payload, 'deduction')) AS total_deductions,
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
                "total_deductions": abs(float(r[9] or 0)),
                "orders": int(r[10] or 0),
                "returns": abs(float(r[11] or 0)),
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
                    sumIf(quantity, operation_type = 'Продажа') - sumIf(quantity, operation_type = 'Возврат') AS qty
                FROM mms_analytics.fact_finances FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND marketplace = 1
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
    # Compute derived metrics — Universal WB formula
    #
    # ppvz_for_pay (К перечислению) already includes deductions for:
    #   - commission (ppvz_reward)
    #   - acquiring (ppvz_kvw_prc)
    #   - refunds / returns
    #   - penalties
    #
    # External expenses (subtracted from ppvz_for_pay to get bank transfer):
    #   - logistics (wb_delivery_rub)
    #   - storage (storage_fee)
    #   - acceptance (acceptance_fee)
    #   - deductions (deduction field — ads, reviews, transit, etc.)
    #
    # Итого к оплате = ppvz_for_pay - logistics - storage - acceptance - deductions
    # ══════════════════════════════════════════════════════
    operating_cur = logistics_cur + storage_cur + acceptance_cur + total_deductions_cur
    operating_prev = logistics_prev + storage_prev + acceptance_prev + total_deductions_prev

    mp_fees_cur = commission_cur + operating_cur
    mp_fees_prev = commission_prev + operating_prev

    # Payout = ppvz_for_pay (к перечислению за товар, до вычета логистики/хранения/удержаний)

    # Profit (ads already inside total_deductions, NOT subtracted separately)
    profit_cur = revenue_cur - mp_fees_cur - cogs_cur
    profit_prev = revenue_prev - mp_fees_prev - cogs_prev
    profit_pct = round(profit_cur / revenue_cur * 100, 1) if revenue_cur > 0 else 0.0

    # ── Build KPI ──
    kpi = {
        "revenue": round(revenue_cur, 2),
        "revenue_delta": _safe_delta(revenue_cur, revenue_prev),
        "payout": round(payout_cur, 2),
        "payout_delta": _safe_delta(payout_cur, payout_prev),
        "mp_fees": round(mp_fees_cur, 2),
        "mp_fees_delta": _safe_delta(mp_fees_cur, mp_fees_prev),
        "operating": round(operating_cur, 2),
        "operating_delta": _safe_delta(operating_cur, operating_prev),
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
    # Split deductions: ads (ВБ Продвижение) vs other (transit, reviews, etc.)
    deductions_ads_cur = total_deductions_cur - deductions_cur  # ВБ Продвижение portion
    deductions_ads_prev = total_deductions_prev - deductions_prev
    breakdown_resp = {
        "revenue": round(revenue_cur, 2),
        "commission": round(commission_cur, 2),
        "logistics": round(logistics_cur, 2),
        "storage": round(storage_cur, 2),
        "acquiring": round(acquiring_cur, 2),
        "advertising": round(ad_spend_cur, 2),  # from fact_advert_stats (informational KPI)
        "refunds": round(returns_cur, 2),
        "penalties": 0,
        "deductions": round(total_deductions_cur, 2),  # Full total for _bank_transfer calc
        "deductions_ads": round(deductions_ads_cur, 2),  # ВБ Продвижение from deduction
        "deductions_other": round(deductions_cur, 2),  # non-ad: transit delivery, reviews, etc.
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
        pay = dd.get("payout", 0) # This is the payout_amount from WB report, not the final bank transfer
        comm_d = dd.get("commission", 0)
        log_d = dd.get("logistics", 0)
        stor_d = dd.get("storage", 0)
        pen_d = dd.get("penalties", 0)
        acq_d = dd.get("acquiring", 0)
        acc_d = dd.get("acceptance", 0)
        ded_d = dd.get("deductions", 0)
        tded_d = dd.get("total_deductions", 0)
        ads_d = ads_daily.get(ds, 0)
        cogs_d = cogs_daily.get(ds, 0)
        op_d = log_d + stor_d + acc_d + tded_d  # logistics + storage + acceptance + total_deductions
        mp_d = comm_d + op_d

        # Payout = ppvz_for_pay (к перечислению за товар)
        bank_trans_d = pay

        profit_d = rev - mp_d - cogs_d  # ads already in tded_d

        daily_raw.append({
            "date": ds,
            "revenue": rev,
            "payout": bank_trans_d,
            "mp_fees": mp_d,
            "operating": op_d,
            "ad_spend": ads_d,
            "cogs": cogs_d,
            "orders": dd.get("orders", 0),
            "profit": profit_d,
        })

    # Apply grouping if week/month
    if group_by in ("week", "month"):
        from collections import defaultdict
        grouped = defaultdict(lambda: {
            "revenue": 0, "payout": 0, "mp_fees": 0, "operating": 0, "ad_spend": 0,
            "cogs": 0, "orders": 0, "profit": 0,
        })
        for pt in daily_raw:
            d_obj = date.fromisoformat(pt["date"])
            if group_by == "week":
                key = str(d_obj - timedelta(days=d_obj.weekday()))
            else:
                key = str(d_obj.replace(day=1))
            # Recalculate payout and profit based on new definitions for grouped data
            rev = pt["revenue"]
            comm = max(rev - pt["payout"], 0) # Recalculate commission from daily_raw's payout
            oper = pt["mp_fees"] - comm # Recalculate operating from daily_raw's mp_fees and new comm
            mp_fees = comm + oper
            grouped[key]["revenue"] += rev
            grouped[key]["payout"] += pt["payout"]
            grouped[key]["mp_fees"] += mp_fees
            grouped[key]["operating"] += oper
            grouped[key]["ad_spend"] += pt["ad_spend"]
            grouped[key]["cogs"] += pt["cogs"]
            grouped[key]["orders"] += pt["orders"]
            grouped[key]["profit"] += pt["profit"]

        daily_final = []
        for k in sorted(grouped.keys()):
            entry = {"date": k}
            for field in ("revenue", "payout", "mp_fees", "operating", "ad_spend", "cogs", "orders", "profit"):
                val = grouped[k].get(field, 0)
                entry[field] = round(val, 2) if isinstance(val, (int, float)) else val
            daily_final.append(entry)
    else:
        daily_final = []
        for ds in sorted(daily_data.keys()):
            pt = daily_data[ds]
            rev = pt["revenue"]
            pay = pt["payout"] # This is the bank transfer
            comm = pt["commission"]
            oper = pt["logistics"] + pt["storage"] + pt["acceptance"] + pt.get("total_deductions", 0)
            mp_fees = comm + oper
            ads = ads_daily.get(ds, 0)
            cogs = cogs_daily.get(ds, 0)
            prof = rev - mp_fees - cogs  # ads already in total_deductions

            daily_final.append({
                "date": ds,
                "revenue": round(rev, 2),
                "payout": round(pay, 2),
                "mp_fees": round(mp_fees, 2),
                "operating": round(oper, 2),
                "ad_spend": round(ads, 2),
                "cogs": round(cogs, 2),
                "orders": pt["orders"],
                "profit": round(prof, 2),
            })

    # ── Build comparison ──
    comparison = {
        "current": {
            "revenue": round(revenue_cur, 2),
            "payout": round(payout_cur, 2),
            "mp_fees": round(mp_fees_cur, 2),
            "operating": round(operating_cur, 2),
            "commission": round(commission_cur, 2),
            "logistics": round(logistics_cur, 2),
            "storage": round(storage_cur, 2),
            "acquiring": round(acquiring_cur, 2),
            "advertising": round(ad_spend_cur, 2),
            "refunds": round(returns_cur, 2),
            "penalties": 0,
            "deductions": round(total_deductions_cur, 2),
            "deductions_ads": round(deductions_ads_cur, 2),
            "deductions_other": round(deductions_cur, 2),
            "compensation": round(acceptance_cur, 2),
            "cogs": round(cogs_cur, 2),
            "profit": round(profit_cur, 2),
            "orders": orders_cur,
        },
        "previous": {
            "revenue": round(revenue_prev, 2),
            "payout": round(payout_prev, 2),
            "mp_fees": round(mp_fees_prev, 2),
            "operating": round(operating_prev, 2),
            "commission": round(commission_prev, 2),
            "logistics": round(logistics_prev, 2),
            "storage": round(storage_prev, 2),
            "acquiring": round(acquiring_prev, 2),
            "advertising": round(ad_spend_prev, 2),
            "refunds": round(returns_prev, 2),
            "penalties": 0,
            "deductions": round(total_deductions_prev, 2),
            "deductions_ads": round(deductions_ads_prev, 2),
            "deductions_other": round(deductions_prev, 2),
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
                ) - sumIf(payout_amount,
                    operation_type = 'Возврат' AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS pay_cur,
                sumIf(wb_delivery_rub,
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
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS ded_cur,
                sumIf(acceptance_fee,
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS acc_cur,
                sumIf(quantity,
                    operation_type = 'Возврат'
                    AND event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS ret_qty_cur,

                -- Previous period
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Продажа' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS rev_prev,
                sumIf(payout_amount,
                    operation_type = 'Продажа' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) - sumIf(payout_amount,
                    operation_type = 'Возврат' AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS pay_prev,
                sumIf(wb_delivery_rub,
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
                ) AS ret_prev,
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS ded_prev,
                sumIf(acceptance_fee,
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS acc_prev,
                sumIf(quantity,
                    operation_type = 'Возврат'
                    AND event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS ret_qty_prev

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
            ded_cur_v = abs(float(r[10] or 0))
            acc_cur_v = abs(float(r[11] or 0))
            ret_qty_cur = abs(int(r[12] or 0))
            # Previous period
            rev_prev = float(r[13] or 0)
            pay_prev = float(r[14] or 0)
            log_prev = abs(float(r[15] or 0))
            stor_prev = abs(float(r[16] or 0))
            acq_prev = abs(float(r[17] or 0))
            pen_prev = abs(float(r[18] or 0))
            sales_prev = int(r[19] or 0)
            ret_prev = abs(float(r[20] or 0))
            ded_prev_v = abs(float(r[21] or 0))
            acc_prev_v = abs(float(r[22] or 0))
            ret_qty_prev = abs(int(r[23] or 0))

            if vc not in products:
                products[vc] = {
                    "vendor_code": vc,
                    "nm_id": nm,
                    "cur": {"revenue": 0, "payout": 0, "logistics": 0, "storage": 0,
                            "acquiring": 0, "penalties": 0, "sales": 0, "returns": 0,
                            "returns_qty": 0, "ad_spend": 0, "cogs": 0,
                            "deductions": 0, "acceptance": 0},
                    "prev": {"revenue": 0, "payout": 0, "logistics": 0, "storage": 0,
                             "acquiring": 0, "penalties": 0, "sales": 0, "returns": 0,
                             "returns_qty": 0, "ad_spend": 0, "cogs": 0,
                             "deductions": 0, "acceptance": 0},
                }
            p = products[vc]
            if nm and not p["nm_id"]:
                p["nm_id"] = nm
            p["cur"]["revenue"] += rev_cur - ret_cur
            p["cur"]["payout"] += pay_cur
            p["cur"]["logistics"] += log_cur
            p["cur"]["storage"] += stor_cur
            p["cur"]["acquiring"] += acq_cur
            p["cur"]["penalties"] += pen_cur
            p["cur"]["sales"] += sales_cur
            p["cur"]["returns"] += ret_cur
            p["cur"]["returns_qty"] += ret_qty_cur
            p["cur"]["deductions"] += ded_cur_v
            p["cur"]["acceptance"] += acc_cur_v
            p["prev"]["revenue"] += rev_prev - ret_prev
            p["prev"]["payout"] += pay_prev
            p["prev"]["logistics"] += log_prev
            p["prev"]["storage"] += stor_prev
            p["prev"]["acquiring"] += acq_prev
            p["prev"]["penalties"] += pen_prev
            p["prev"]["sales"] += sales_prev
            p["prev"]["returns"] += ret_prev
            p["prev"]["returns_qty"] += ret_qty_prev
            p["prev"]["deductions"] += ded_prev_v
            p["prev"]["acceptance"] += acc_prev_v
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
                            "returns_qty": 0, "ad_spend": 0, "cogs": 0,
                            "deductions": 0, "acceptance": 0},
                    "prev": {"revenue": 0, "payout": 0, "logistics": 0, "storage": 0,
                             "acquiring": 0, "penalties": 0, "sales": 0, "returns": 0,
                             "returns_qty": 0, "ad_spend": 0, "cogs": 0,
                             "deductions": 0, "acceptance": 0},
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
                # Net qty = sales - returns_qty (matching KPI COGS logic)
                net_qty_cur = max(p["cur"]["sales"] - p["cur"]["returns_qty"], 0)
                net_qty_prev = max(p["prev"]["sales"] - p["prev"]["returns_qty"], 0)
                p["cur"]["cogs"] = round(unit_cost * net_qty_cur, 2)
                p["prev"]["cogs"] = round(unit_cost * net_qty_prev, 2)
    except Exception as e:
        logger.warning("PG product_costs query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 3.5. Link deductions to products by nm_id from bonus_type_name
    #
    # WB deduction records have nm_id=0 and empty vendor_code, BUT
    # bonus_type_name often contains "товар NNNNNN" with the real nm_id.
    # We parse it with ClickHouse regex and link to products via nm_to_vc.
    #
    # Storage records have NO product ID at all → distribute proportionally.
    # ══════════════════════════════════════════════════════
    # Zero out deductions from main SQL (they were grouped into __unknown__)
    # — we'll recalculate properly below via parsed nm_id
    unknown_p = products.get("__unknown__")
    if unknown_p:
        unknown_p["cur"]["deductions"] = 0
        unknown_p["prev"]["deductions"] = 0
    try:
        ded_by_nm = ch.query("""
            SELECT
                toUInt64OrZero(extract(
                    JSONExtractString(raw_payload, 'bonus_type_name'),
                    'товар\\s+(\\d+)'
                )) AS parsed_nm_id,
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    event_date >= {d_start:Date} AND event_date <= {d_end:Date}
                ) AS ded_cur,
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    event_date >= {d_prev_start:Date} AND event_date <= {d_prev_end:Date}
                ) AS ded_prev
            FROM mms_analytics.fact_finances FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND marketplace = 1
              AND event_date >= {d_prev_start:Date}
              AND event_date <= {d_end:Date}
              AND JSONExtractFloat(raw_payload, 'deduction') != 0
            GROUP BY parsed_nm_id
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })

        unlinked_ded_cur = 0.0
        unlinked_ded_prev = 0.0
        for r in ded_by_nm.result_rows:
            nm = int(r[0] or 0)
            d_cur = abs(float(r[1] or 0))
            d_prev = abs(float(r[2] or 0))

            vc = nm_to_vc.get(nm) if nm else None
            if vc and vc in products:
                products[vc]["cur"]["deductions"] += d_cur
                products[vc]["prev"]["deductions"] += d_prev
            else:
                unlinked_ded_cur += d_cur
                unlinked_ded_prev += d_prev

    except Exception as e:
        logger.warning("CH deductions by nm_id query failed: %s", e)
        # Fallback: all deductions from __unknown__ stay as-is
        unknown_p = products.get("__unknown__")
        unlinked_ded_cur = unknown_p["cur"]["deductions"] if unknown_p else 0
        unlinked_ded_prev = unknown_p["prev"]["deductions"] if unknown_p else 0

    # ── Proportional distribution: ONLY storage + unlinked deductions ──
    # NOTE: acceptance has vendor_code in WB API → already grouped correctly
    unknown_p = products.get("__unknown__")
    if unknown_p:
        undist_storage_cur = unknown_p["cur"].get("storage", 0)
        undist_storage_prev = unknown_p["prev"].get("storage", 0)
    else:
        undist_storage_cur = undist_storage_prev = 0

    for period_key, u_stor, u_ded in [
        ("cur", undist_storage_cur, unlinked_ded_cur),
        ("prev", undist_storage_prev, unlinked_ded_prev),
    ]:
        total_undist = u_stor + u_ded
        if total_undist == 0:
            continue
        total_rev = sum(
            p[period_key]["revenue"]
            for vc, p in products.items()
            if not vc.startswith("__") and p[period_key]["revenue"] > 0
        )
        if total_rev <= 0:
            continue
        for vc, p in products.items():
            if vc.startswith("__"):
                continue
            rev = p[period_key]["revenue"]
            if rev <= 0:
                continue
            share = rev / total_rev
            p[period_key]["storage"] += round(u_stor * share, 2)
            p[period_key]["deductions"] += round(u_ded * share, 2)

    # Zero out __unknown__ storage/deductions (redistributed)
    if unknown_p:
        for pk in ("cur", "prev"):
            unknown_p[pk]["storage"] = 0
            unknown_p[pk]["deductions"] = 0
    # ══════════════════════════════════════════════════════
    # 4. Build response
    # ══════════════════════════════════════════════════════
    result_products = []
    for vc, p in products.items():
        cur = p["cur"]
        prev = p["prev"]

        # Universal WB profit formula (matches waterfall):
        # profit = payout - logistics - storage - deductions - acceptance - cogs
        cur_profit = cur["payout"] - cur["logistics"] - cur["storage"] - cur["deductions"] - cur["acceptance"] - cur["cogs"]
        prev_profit = prev["payout"] - prev["logistics"] - prev["storage"] - prev["deductions"] - prev["acceptance"] - prev["cogs"]

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
            "deductions": round(cur["deductions"], 2),
            "acceptance": round(cur["acceptance"], 2),
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
            "deductions": round(prev["deductions"], 2),
            "acceptance": round(prev["acceptance"], 2),
            "cogs": round(prev["cogs"], 2),
            "profit": round(prev_profit, 2),
        }

        delta_pct = {}
        for key in current:
            delta_pct[key] = _safe_delta(current[key], previous[key])

        pct_of_rev = {}
        rev = current["revenue"]
        if rev > 0:
            for key in ("logistics", "storage", "deductions", "ad_spend", "cogs", "profit"):
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
                "penalties", "returns", "ad_spend", "deductions", "acceptance", "cogs", "profit"):
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
    # 1. Product metrics from fact_ozon_transactions
    #    Revenue, Sales volume, Commission, Logistics (per-item)
    # ══════════════════════════════════════════════════════

    # Build sku → offer_id mapping from dim_ozon_products (PG)
    # Must use same source as KPI COGS to ensure offer_id consistency with product_costs
    sku_to_offer = {}
    try:
        sku_map_result = await db.execute(
            text("""
                SELECT sku, offer_id
                FROM dim_ozon_products
                WHERE shop_id = :shop_id AND sku > 0
            """),
            {"shop_id": shop_id},
        )
        for r in sku_map_result.fetchall():
            sku_to_offer[int(r[0])] = r[1]
    except Exception:
        pass

    try:
        txn_result = ch.query("""
            SELECT
                sku,
                sumIf(accruals_for_sale, toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}) AS rev_cur,
                countIf(toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}) AS sales_cur,
                sumIf(sale_commission, toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}) AS comm_cur,
                sumIf(services_total, toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}) AS svc_cur,
                
                sumIf(accruals_for_sale, toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}) AS rev_prev,
                countIf(toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}) AS sales_prev,
                sumIf(sale_commission, toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}) AS comm_prev,
                sumIf(services_total, toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}) AS svc_prev
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND category = 'Revenue'
              AND sku > 0
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
            
            if oid not in products:
                products[oid] = {
                    "offer_id": oid,
                    "cur": {
                        "revenue": 0.0, "sales": 0, "commission": 0.0, "logistics": 0.0, 
                        "storage": 0.0, "acquiring": 0.0, "penalties": 0.0, "returns": 0.0, "ad_spend": 0.0, "cogs": 0.0,
                    },
                    "prev": {
                        "revenue": 0.0, "sales": 0, "commission": 0.0, "logistics": 0.0, 
                        "storage": 0.0, "acquiring": 0.0, "penalties": 0.0, "returns": 0.0, "ad_spend": 0.0, "cogs": 0.0,
                    }
                }
            
            # Accumulate metrics (commission and logistics are negative in DB, so we take abs)
            products[oid]["cur"]["revenue"] += float(r[1] or 0)
            products[oid]["cur"]["sales"] += int(r[2] or 0)
            products[oid]["cur"]["commission"] += abs(float(r[3] or 0))
            products[oid]["cur"]["logistics"] += abs(float(r[4] or 0))

            products[oid]["prev"]["revenue"] += float(r[5] or 0)
            products[oid]["prev"]["sales"] += int(r[6] or 0)
            products[oid]["prev"]["commission"] += abs(float(r[7] or 0))
            products[oid]["prev"]["logistics"] += abs(float(r[8] or 0))

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
    #    Fields: sku (not sku_id!), money_spent (not spend!)
    # ══════════════════════════════════════════════════════
    try:
        ads_result = ch.query("""
            SELECT
                sku,
                sumIf(money_spent, dt >= {d_start:Date} AND dt <= {d_end:Date}) AS ads_cur,
                sumIf(money_spent, dt >= {d_prev_start:Date} AND dt <= {d_prev_end:Date}) AS ads_prev
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d_prev_start:Date}
              AND dt <= {d_end:Date}
            GROUP BY sku
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })

        # sku_to_offer already built in section 2
        unmatched_ads_cur = 0.0
        unmatched_ads_prev = 0.0
        for r in ads_result.result_rows:
            sku = int(r[0] or 0)
            ads_c = float(r[1] or 0)
            ads_p = float(r[2] or 0)
            oid = sku_to_offer.get(sku, str(sku))
            if oid in products:
                products[oid]["cur"]["ad_spend"] += ads_c
                products[oid]["prev"]["ad_spend"] += ads_p
            else:
                unmatched_ads_cur += ads_c
                unmatched_ads_prev += ads_p

        # Add unmatched ads to __no_product__ bucket
        if unmatched_ads_cur > 0 or unmatched_ads_prev > 0:
            no_prod_key = "__no_product__"
            if no_prod_key not in products:
                products[no_prod_key] = {
                    "offer_id": no_prod_key,
                    "cur": {"revenue": 0.0, "sales": 0, "commission": 0.0, "logistics": 0.0,
                            "storage": 0.0, "acquiring": 0.0, "penalties": 0.0, "returns": 0.0, "ad_spend": 0.0, "cogs": 0.0},
                    "prev": {"revenue": 0.0, "sales": 0, "commission": 0.0, "logistics": 0.0,
                             "storage": 0.0, "acquiring": 0.0, "penalties": 0.0, "returns": 0.0, "ad_spend": 0.0, "cogs": 0.0},
                }
            products[no_prod_key]["cur"]["ad_spend"] += unmatched_ads_cur
            products[no_prod_key]["prev"]["ad_spend"] += unmatched_ads_prev
    except Exception as e:
        logger.warning("CH Ozon ads per product query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 3.5. Refund + Other bulk charges (not tied to products)
    #      These are in the KPI mp_fees but not in per-product queries
    # ══════════════════════════════════════════════════════
    try:
        refund_other = ch.query("""
            SELECT
                sumIf(abs(amount), toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}) AS total_cur,
                sumIf(abs(amount), toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}) AS total_prev
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(operation_date) >= {d_prev_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
              AND category IN ('Refund', 'Other')
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
            "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
        })
        if refund_other.result_rows:
            oc = refund_other.result_rows[0]
            ref_cur = float(oc[0] or 0)
            ref_prev = float(oc[1] or 0)
            if ref_cur > 0 or ref_prev > 0:
                no_prod_key = "__no_product__"
                if no_prod_key not in products:
                    products[no_prod_key] = {
                        "offer_id": no_prod_key,
                        "cur": {"revenue": 0.0, "sales": 0, "commission": 0.0, "logistics": 0.0,
                                "storage": 0.0, "acquiring": 0.0, "penalties": 0.0, "returns": 0.0, "ad_spend": 0.0, "cogs": 0.0},
                        "prev": {"revenue": 0.0, "sales": 0, "commission": 0.0, "logistics": 0.0,
                                 "storage": 0.0, "acquiring": 0.0, "penalties": 0.0, "returns": 0.0, "ad_spend": 0.0, "cogs": 0.0},
                    }
                # These reduce profit (unattributed expenses go into commission bucket)
                products[no_prod_key]["cur"]["commission"] += ref_cur
                products[no_prod_key]["prev"]["commission"] += ref_prev
    except Exception as e:
        logger.warning("CH Ozon refund/other for product table failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 4. COGS from product_costs (PG) & Names from dim_ozon_products
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

    names_map = {}
    try:
        name_result = await db.execute(
            text("""
                SELECT offer_id, name
                FROM dim_ozon_products
                WHERE shop_id = :shop_id
            """),
            {"shop_id": shop_id},
        )
        for r in name_result.fetchall():
            if r[1]:
                names_map[r[0]] = r[1]
    except Exception as e:
        logger.warning("PG dim_ozon_products query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 5. Build response
    # ══════════════════════════════════════════════════════
    result_products = []
    for oid, p in products.items():
        cur = p["cur"]
        prev = p["prev"]

        # Ozon profit = revenue - commission - logistics - storage - acquiring - ad_spend - cogs
        cur_profit = cur["revenue"] - cur["commission"] - cur["logistics"] - cur["storage"] - cur["acquiring"] - cur["ad_spend"] - cur["cogs"]
        prev_profit = prev["revenue"] - prev["commission"] - prev["logistics"] - prev["storage"] - prev["acquiring"] - prev["ad_spend"] - prev["cogs"]

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
            "name": names_map.get(oid, ""),
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


# ══════════════════════════════════════════════════════════
# Weekly P&L Report (Понедельный отчёт) — Ozon
# ══════════════════════════════════════════════════════════


@router.get("/ozon/weekly-report")
async def get_ozon_weekly_report(
    shop_id: int = Query(..., description="Shop ID"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Ozon Weekly P&L Report — all weeks (Mon–Sun) from the earliest data.

    Columns match the user's Excel-style report:
      - Year, Week#, Period (date range)
      - Quantity, Sales, Returns, Commission
      - Compensations, Other services, Marketing, Other charges
      - FBO services, Agent services (Acquiring), Delivery services
      - Payout, COGS, Gross profit (VAL)
      - Percentage columns (% of Sales)

    Sources:
      - fact_ozon_transactions FINAL: all financial metrics
      - fact_ozon_ad_daily FINAL: ad spend per week
      - product_costs (PG): cost_price + packaging_cost for COGS
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

    # ══════════════════════════════════════════════════════
    # 1. Main weekly aggregation from fact_ozon_transactions
    # ══════════════════════════════════════════════════════
    weeks = {}  # week_start (str) -> {...}

    try:
        txn_weekly = ch.query("""
            SELECT
                toMonday(toDate(operation_date)) AS week_start,

                -- Quantity: count Revenue transactions
                countIf(category = 'Revenue') AS qty,

                -- Sales (Σ Продажи)
                sumIf(accruals_for_sale, category = 'Revenue') AS sales,

                -- Returns (Возврат)
                sumIf(abs(amount), category = 'Refund') AS returns,

                -- Commission (Σ Комиссия)
                sumIf(abs(sale_commission), category = 'Revenue') AS commission,

                -- Compensations (Компенсации Ozon)
                sumIf(amount, category = 'Compensation') AS compensations,

                -- Other services (Σ Другие услуги) — per-item services from Revenue txns
                sumIf(abs(services_total), category = 'Revenue') AS other_services,

                -- Marketing (Σ Продвижение) — from transactions only
                sumIf(abs(amount), category = 'Marketing') AS marketing_txn,

                -- Other charges (Σ Прочие начисления)
                sumIf(abs(amount), category IN ('Penalty', 'Other')) AS other_charges,

                -- FBO logistics (crossdocking, supply-related operations)
                sumIf(abs(amount), category = 'Logistics' AND operation_type IN (
                    'MarketplaceServiceItemCrossdocking',
                    'OperationMarketplaceSupplyAdditional',
                    'OperationMarketplaceSupplyExpirationDateProcessing',
                    'OperationMarketplaceServiceSupplyInboundCargoShortage',
                    'OperationMarketplaceServiceSupplyInboundSupplyShortage'
                )) AS fbo_services,

                -- Acquiring (Услуги агентов)
                sumIf(abs(amount), category = 'Acquiring') AS acquiring,

                -- Delivery logistics = ALL Logistics EXCEPT FBO types
                sumIf(abs(amount), category = 'Logistics' AND operation_type NOT IN (
                    'MarketplaceServiceItemCrossdocking',
                    'OperationMarketplaceSupplyAdditional',
                    'OperationMarketplaceSupplyExpirationDateProcessing',
                    'OperationMarketplaceServiceSupplyInboundCargoShortage',
                    'OperationMarketplaceServiceSupplyInboundSupplyShortage'
                )) AS delivery_services,

                -- Storage (Хранение)
                sumIf(abs(amount), category = 'Storage') AS storage,

                -- Payout (К перечислению) — net sum of ALL transactions
                sum(amount) AS payout

            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
            GROUP BY week_start
            ORDER BY week_start
        """, parameters={"shop_id": shop_id})

        for r in txn_weekly.result_rows:
            ws = str(r[0])
            weeks[ws] = {
                "week_start": ws,
                "qty": int(r[1] or 0),
                "sales": float(r[2] or 0),
                "returns": float(r[3] or 0),
                "commission": float(r[4] or 0),
                "compensations": float(r[5] or 0),
                "other_services": float(r[6] or 0),
                "marketing": float(r[7] or 0),
                "other_charges": float(r[8] or 0),
                "fbo_services": float(r[9] or 0),
                "acquiring": float(r[10] or 0),
                "delivery_services": float(r[11] or 0),
                "storage": float(r[12] or 0),
                "payout": float(r[13] or 0),
            }
    except Exception as e:
        logger.warning("CH Ozon weekly transactions query failed: %s", e)

    if not weeks:
        return {"shop_id": shop_id, "weeks": [], "totals": {}}

    # ══════════════════════════════════════════════════════
    # 2. Ad spend from fact_ozon_ad_daily (weekly)
    # ══════════════════════════════════════════════════════
    try:
        ads_weekly = ch.query("""
            SELECT
                toMonday(dt) AS week_start,
                sum(money_spent) AS ad_spend
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
            GROUP BY week_start
            ORDER BY week_start
        """, parameters={"shop_id": shop_id})

        for r in ads_weekly.result_rows:
            ws = str(r[0])
            ad_val = float(r[1] or 0)
            if ws in weeks:
                weeks[ws]["marketing"] += ad_val
            else:
                # Week with only ad spend (rare but possible)
                weeks[ws] = {
                    "week_start": ws,
                    "qty": 0, "sales": 0, "returns": 0,
                    "commission": 0, "compensations": 0,
                    "other_services": 0, "marketing": ad_val,
                    "other_charges": 0, "fbo_services": 0,
                    "acquiring": 0, "delivery_services": 0,
                    "storage": 0, "payout": 0,
                }
    except Exception as e:
        logger.warning("CH Ozon weekly ads query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 3. COGS from product_costs (PG) + weekly qty per SKU
    # ══════════════════════════════════════════════════════
    # Build sku → offer_id map
    sku_to_offer = {}
    try:
        sku_map_result = await db.execute(
            text("""
                SELECT sku, offer_id
                FROM dim_ozon_products
                WHERE shop_id = :shop_id AND sku > 0
            """),
            {"shop_id": shop_id},
        )
        for r in sku_map_result.fetchall():
            sku_to_offer[int(r[0])] = r[1]
    except Exception:
        pass

    cost_map = {}
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
    except Exception:
        pass

    if cost_map and sku_to_offer:
        try:
            cogs_weekly = ch.query("""
                SELECT
                    toMonday(toDate(operation_date)) AS week_start,
                    sku,
                    countIf(category = 'Revenue') AS qty
                FROM mms_analytics.fact_ozon_transactions FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND category = 'Revenue'
                  AND sku > 0
                GROUP BY week_start, sku
            """, parameters={"shop_id": shop_id})

            # Accumulate COGS per week
            cogs_by_week = {}
            for r in cogs_weekly.result_rows:
                ws = str(r[0])
                sku = int(r[1] or 0)
                qty = int(r[2] or 0)
                offer_id = sku_to_offer.get(sku, "")
                unit_cost = cost_map.get(offer_id.lower(), 0)
                if unit_cost > 0 and qty > 0:
                    cogs_by_week[ws] = cogs_by_week.get(ws, 0) + unit_cost * qty

            for ws, cogs_val in cogs_by_week.items():
                if ws in weeks:
                    weeks[ws]["cogs"] = round(cogs_val, 2)
        except Exception as e:
            logger.warning("CH Ozon weekly COGS query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 4. Build response rows
    # ══════════════════════════════════════════════════════
    result_weeks = []
    totals = {
        "qty": 0, "sales": 0, "returns": 0, "commission": 0,
        "compensations": 0, "other_services": 0, "marketing": 0,
        "other_charges": 0, "fbo_services": 0, "acquiring": 0,
        "delivery_services": 0, "storage": 0, "payout": 0,
        "cogs": 0, "gross_profit": 0,
    }

    for ws in sorted(weeks.keys()):
        w = weeks[ws]
        ws_date = date.fromisoformat(ws)
        we_date = ws_date + timedelta(days=6)  # Sunday

        cogs = w.get("cogs", 0)
        gross_profit = w["payout"] - cogs

        sales = w["sales"]
        row = {
            "year": ws_date.year,
            "week": ws_date.isocalendar()[1],
            "week_start": ws,
            "week_end": str(we_date),
            "qty": w["qty"],
            "sales": round(sales, 2),
            "returns": round(w["returns"], 2),
            "commission": round(w["commission"], 2),
            "compensations": round(w["compensations"], 2),
            "other_services": round(w["other_services"], 2),
            "marketing": round(w["marketing"], 2),
            "other_charges": round(w["other_charges"], 2),
            "fbo_services": round(w["fbo_services"], 2),
            "acquiring": round(w["acquiring"], 2),
            "delivery_services": round(w["delivery_services"], 2),
            "storage": round(w.get("storage", 0), 2),
            "payout": round(w["payout"], 2),
            "cogs": round(cogs, 2),
            "gross_profit": round(gross_profit, 2),
            # Percentage columns (% of sales)
            "commission_pct": round(w["commission"] / sales * 100, 1) if sales > 0 else 0,
            "marketing_pct": round(w["marketing"] / sales * 100, 1) if sales > 0 else 0,
            "fbo_pct": round(w["fbo_services"] / sales * 100, 1) if sales > 0 else 0,
            "delivery_pct": round(w["delivery_services"] / sales * 100, 1) if sales > 0 else 0,
            "cogs_pct": round(cogs / sales * 100, 1) if sales > 0 else 0,
            "gross_profit_pct": round(gross_profit / sales * 100, 1) if sales > 0 else 0,
        }
        result_weeks.append(row)

        # Accumulate totals
        for key in totals:
            if key == "gross_profit":
                totals[key] += gross_profit
            elif key in w:
                totals[key] += w[key]
            elif key == "cogs":
                totals[key] += cogs

    # Round totals
    for key in totals:
        totals[key] = round(totals[key], 2)

    # Total percentages
    total_sales = totals["sales"]
    totals["commission_pct"] = round(totals["commission"] / total_sales * 100, 1) if total_sales > 0 else 0
    totals["marketing_pct"] = round(totals["marketing"] / total_sales * 100, 1) if total_sales > 0 else 0
    totals["fbo_pct"] = round(totals["fbo_services"] / total_sales * 100, 1) if total_sales > 0 else 0
    totals["delivery_pct"] = round(totals["delivery_services"] / total_sales * 100, 1) if total_sales > 0 else 0
    totals["cogs_pct"] = round(totals["cogs"] / total_sales * 100, 1) if total_sales > 0 else 0
    totals["gross_profit_pct"] = round(totals["gross_profit"] / total_sales * 100, 1) if total_sales > 0 else 0

    return {
        "shop_id": shop_id,
        "weeks": result_weeks,
        "totals": totals,
    }


# ══════════════════════════════════════════════════════════════
# WB WEEKLY P&L REPORT
# ══════════════════════════════════════════════════════════════

@router.get("/wb/weekly-report")
async def get_wb_weekly_report(
    shop_id: int = Query(..., description="Shop ID"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    WB Weekly P&L Report — all weeks (Mon–Sun) from the earliest data.

    Sources:
      - fact_finances FINAL: all WB financial metrics
      - fact_advert_stats FINAL: ad spend per week
      - product_costs (PG): COGS
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

    # ══════════════════════════════════════════════════════
    # 1. Main weekly aggregation from fact_finances (WB)
    #    SAME FIELDS as P&L waterfall (get_wb_finances)
    # ══════════════════════════════════════════════════════
    weeks = {}

    try:
        txn_weekly = ch.query("""
            SELECT
                toMonday(event_date) AS week_start,

                -- Orders qty
                sumIf(quantity, operation_type = 'Продажа' AND quantity > 0) AS qty,

                -- Revenue = retail_price_withdisc_rub (Продажа - Возврат)
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Продажа')
                 - sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Возврат') AS revenue,

                -- Payout = payout_amount (Продажа - Возврат) = К перечислению
                sumIf(payout_amount, operation_type = 'Продажа')
                 - sumIf(payout_amount, operation_type = 'Возврат') AS payout,

                -- Returns amount
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'), operation_type = 'Возврат') AS returns_amount,
                countIf(operation_type = 'Возврат') AS returns_qty,

                -- Logistics (wb_delivery_rub, same as P&L)
                sum(wb_delivery_rub) AS logistics,

                -- Storage
                sum(storage_fee) AS storage,

                -- Acquiring
                sum(wb_acquiring) AS acquiring,

                -- Acceptance
                sum(acceptance_fee) AS acceptance,

                -- Deductions (from raw_payload, EXCLUDING 'продвижение' to avoid double-counting)
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    positionCaseInsensitiveUTF8(JSONExtractString(raw_payload, 'bonus_type_name'), 'продвижение') = 0
                ) AS deductions,

                -- WB Promotion (deductions where bonus_type_name contains 'продвижение')
                sumIf(JSONExtractFloat(raw_payload, 'deduction'),
                    positionCaseInsensitiveUTF8(JSONExtractString(raw_payload, 'bonus_type_name'), 'продвижение') > 0
                ) AS wb_promo,

                -- Total deductions (all)
                sum(JSONExtractFloat(raw_payload, 'deduction')) AS total_deductions

            FROM mms_analytics.fact_finances FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND marketplace = 1
            GROUP BY week_start
            ORDER BY week_start
        """, parameters={"shop_id": shop_id})

        for r in txn_weekly.result_rows:
            ws = str(r[0])
            revenue = float(r[2] or 0)
            payout = float(r[3] or 0)
            weeks[ws] = {
                "week_start": ws,
                "qty": int(r[1] or 0),
                "revenue": revenue,
                "payout": payout,
                "commission": max(revenue - payout, 0),  # Commission = Revenue - Payout
                "returns_amount": abs(float(r[4] or 0)),
                "returns_qty": int(r[5] or 0),
                "logistics": abs(float(r[6] or 0)),
                "storage": abs(float(r[7] or 0)),
                "acquiring": abs(float(r[8] or 0)),
                "acceptance": abs(float(r[9] or 0)),
                "deductions": abs(float(r[10] or 0)),
                "wb_promo": abs(float(r[11] or 0)),
                "marketing": 0,  # external ads from fact_advert_stats
                "cogs": 0,
            }
    except Exception as e:
        logger.warning("CH WB weekly transactions query failed: %s", e)

    if not weeks:
        return {"shop_id": shop_id, "weeks": [], "totals": {}}

    # ══════════════════════════════════════════════════════
    # 2. Ad spend from fact_advert_stats
    # ══════════════════════════════════════════════════════
    try:
        ad_weekly = ch.query("""
            SELECT
                toMonday(date) AS week_start,
                sum(spend) AS total_spend
            FROM mms_analytics.fact_advert_stats FINAL
            WHERE shop_id = {shop_id:UInt32}
            GROUP BY week_start
        """, parameters={"shop_id": shop_id})

        for r in ad_weekly.result_rows:
            ws = str(r[0])
            ad_val = float(r[1] or 0)
            if ws in weeks:
                weeks[ws]["marketing"] = ad_val
            else:
                weeks[ws] = {
                    "week_start": ws,
                    "qty": 0, "retail_amount": 0, "ppvz_for_pay": 0,
                    "commission": 0, "returns_qty": 0, "returns_amount": 0,
                    "logistics": 0, "storage": 0, "deductions": 0,
                    "acceptance": 0, "compensations": 0, "returns_compensation": 0,
                    "payout": 0, "marketing": ad_val, "cogs": 0,
                }
    except Exception as e:
        logger.warning("CH WB weekly ads query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 3. COGS from product_costs (PG) via vendor_code
    # ══════════════════════════════════════════════════════
    cost_map = {}
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
    except Exception:
        pass

    if cost_map:
        try:
            cogs_weekly = ch.query("""
                SELECT
                    toMonday(event_date) AS week_start,
                    vendor_code,
                    countIf(operation_type = 'Продажа') AS qty
                FROM mms_analytics.fact_finances FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND marketplace = 'wb'
                  AND operation_type = 'Продажа'
                  AND vendor_code != ''
                GROUP BY week_start, vendor_code
            """, parameters={"shop_id": shop_id})

            cogs_by_week = {}
            for r in cogs_weekly.result_rows:
                ws = str(r[0])
                vc = str(r[1] or "").lower()
                qty = int(r[2] or 0)
                unit_cost = cost_map.get(vc, 0)
                if unit_cost > 0 and qty > 0:
                    cogs_by_week[ws] = cogs_by_week.get(ws, 0) + unit_cost * qty

            for ws, cogs_val in cogs_by_week.items():
                if ws in weeks:
                    weeks[ws]["cogs"] = round(cogs_val, 2)
        except Exception as e:
            logger.warning("CH WB weekly COGS query failed: %s", e)

    # ══════════════════════════════════════════════════════
    # 4. Build response rows
    #    Profit = Payout - Logistics - Storage - Acceptance - Deductions - WBPromo - AdsExternal - COGS
    # ══════════════════════════════════════════════════════
    result_weeks = []
    totals = {
        "qty": 0, "revenue": 0, "payout": 0, "commission": 0,
        "returns_qty": 0, "returns_amount": 0,
        "logistics": 0, "storage": 0, "acquiring": 0,
        "acceptance": 0, "deductions": 0, "wb_promo": 0,
        "marketing": 0, "cogs": 0, "gross_profit": 0,
    }

    for ws in sorted(weeks.keys()):
        w = weeks[ws]
        ws_date = date.fromisoformat(ws)
        we_date = ws_date + timedelta(days=6)

        cogs = w.get("cogs", 0)
        # Profit = К перечислению − Логистика − Хранение − Приёмка − Удержания − WB Промо − Реклама − Себестоимость
        gross_profit = (
            w["payout"]
            - w["logistics"]
            - w["storage"]
            - w["acceptance"]
            - w["deductions"]
            - w["wb_promo"]
            - w["marketing"]
            - cogs
        )

        revenue = w["revenue"]
        row = {
            "year": ws_date.year,
            "week": ws_date.isocalendar()[1],
            "week_start": ws,
            "week_end": str(we_date),
            "qty": w["qty"],
            "revenue": round(revenue, 2),
            "payout": round(w["payout"], 2),
            "commission": round(w["commission"], 2),
            "returns_qty": w["returns_qty"],
            "returns_amount": round(w["returns_amount"], 2),
            "logistics": round(w["logistics"], 2),
            "storage": round(w["storage"], 2),
            "acquiring": round(w["acquiring"], 2),
            "acceptance": round(w["acceptance"], 2),
            "deductions": round(w["deductions"], 2),
            "wb_promo": round(w["wb_promo"], 2),
            "marketing": round(w["marketing"], 2),
            "cogs": round(cogs, 2),
            "gross_profit": round(gross_profit, 2),
            # Percentage columns (% of revenue)
            "commission_pct": round(w["commission"] / revenue * 100, 1) if revenue > 0 else 0,
            "logistics_pct": round(w["logistics"] / revenue * 100, 1) if revenue > 0 else 0,
            "cogs_pct": round(cogs / revenue * 100, 1) if revenue > 0 else 0,
            "gross_profit_pct": round(gross_profit / revenue * 100, 1) if revenue > 0 else 0,
        }
        result_weeks.append(row)

        # Accumulate totals
        for key in totals:
            if key == "gross_profit":
                totals[key] += gross_profit
            elif key in w:
                totals[key] += w[key]
            elif key == "cogs":
                totals[key] += cogs

    # Round totals
    for key in totals:
        totals[key] = round(totals[key], 2)

    # Total percentages
    total_revenue = totals["revenue"]
    totals["commission_pct"] = round(totals["commission"] / total_revenue * 100, 1) if total_revenue > 0 else 0
    totals["logistics_pct"] = round(totals["logistics"] / total_revenue * 100, 1) if total_revenue > 0 else 0
    totals["cogs_pct"] = round(totals["cogs"] / total_revenue * 100, 1) if total_revenue > 0 else 0
    totals["gross_profit_pct"] = round(totals["gross_profit"] / total_revenue * 100, 1) if total_revenue > 0 else 0

    return {
        "shop_id": shop_id,
        "weeks": result_weeks,
        "totals": totals,
    }
