"""
Shared Ozon Finance SQL queries and helpers.

Used by both finances.py (API) and finances_export.py (Excel export)
to ensure consistent data access and calculation logic.
"""
import logging
from datetime import date
from typing import Dict, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ── SKU → offer_id mapping ────────────────────────────────

async def get_sku_to_offer_map(db: AsyncSession, shop_id: int) -> Dict[int, str]:
    """
    Build SKU → offer_id mapping from dim_ozon_products.
    Used by COGS calculation and product-level queries.
    """
    sku_to_offer = {}
    try:
        result = await db.execute(
            text("""
                SELECT sku, offer_id
                FROM dim_ozon_products
                WHERE shop_id = :shop_id AND sku > 0
            """),
            {"shop_id": shop_id},
        )
        for r in result.fetchall():
            sku_to_offer[int(r[0])] = r[1]
    except Exception as e:
        logger.warning("Failed to load SKU→offer_id map: %s", e)
    return sku_to_offer


# ── Cost map from product_costs ────────────────────────────

async def get_cost_map(db: AsyncSession, shop_id: int) -> Dict[str, float]:
    """
    Load offer_id → unit cost from product_costs (PostgreSQL).
    Unit cost = cost_price + packaging_cost.
    """
    cost_map = {}
    try:
        result = await db.execute(
            text("""
                SELECT offer_id, COALESCE(cost_price, 0) + COALESCE(packaging_cost, 0) AS total_cost
                FROM product_costs
                WHERE shop_id = :shop_id AND (cost_price > 0 OR packaging_cost > 0)
            """),
            {"shop_id": shop_id},
        )
        cost_map = {r[0]: float(r[1]) for r in result.fetchall()}
    except Exception as e:
        logger.warning("Failed to load cost map: %s", e)
    return cost_map


# ── Build SKU → cost map ──────────────────────────────────

def build_sku_cost_map(
    sku_to_offer: Dict[int, str],
    cost_map: Dict[str, float],
) -> Dict[int, float]:
    """
    Combine SKU→offer_id and offer_id→cost into SKU→unit_cost.
    """
    sku_cost_map = {}
    for sku, offer_id in sku_to_offer.items():
        cost = cost_map.get(offer_id, cost_map.get(offer_id.lower() if isinstance(offer_id, str) else "", 0))
        if cost > 0:
            sku_cost_map[sku] = cost
    return sku_cost_map


# ── COGS calculation via CH ────────────────────────────────

def calc_cogs_from_ch(
    ch,
    shop_id: int,
    sku_cost_map: Dict[int, float],
    d_start: date,
    d_end: date,
    d_prev_start: date,
    d_prev_end: date,
) -> Tuple[float, float, Dict[str, float]]:
    """
    Calculate COGS from Revenue transactions in ClickHouse.

    Uses count() for quantity. After CH migration 007 is applied,
    can be switched to sum(items_count) for multi-item robustness.

    Returns:
        (cogs_cur, cogs_prev, cogs_daily)
    """
    cogs_cur = 0.0
    cogs_prev = 0.0
    cogs_daily: Dict[str, float] = {}

    if not sku_cost_map:
        return cogs_cur, cogs_prev, cogs_daily

    sku_list = list(sku_cost_map.keys())
    try:
        # TODO: switch to sum(items_count) after CH migration 007 is applied
        result = ch.query("""
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
        for r in result.result_rows:
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

    return cogs_cur, cogs_prev, cogs_daily


# ── Daily breakdown by category ──────────────────────────

def get_daily_category_breakdown(
    ch,
    shop_id: int,
    d_start: date,
    d_end: date,
) -> Dict[str, Dict[str, float]]:
    """
    Get daily breakdown of transaction amounts by category.

    Returns: {date_str: {revenue, commission, services, logistics, storage,
              acquiring, marketing, refunds, penalties, compensation, other}}
    """
    daily: Dict[str, Dict[str, float]] = {}

    CAT_KEYS = {
        "Revenue": "revenue",
        "Logistics": "logistics",
        "Storage": "storage",
        "Acquiring": "acquiring",
        "Marketing": "marketing",
        "Refund": "refunds",
        "Penalty": "penalties",
        "Compensation": "compensation",
    }

    try:
        result = ch.query("""
            SELECT
                toDate(operation_date) AS dt,
                category,
                sum(accruals_for_sale) AS accruals,
                sum(abs(sale_commission)) AS commission,
                sum(abs(services_total)) AS services,
                sum(amount) AS total_amount
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(operation_date) >= {d_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
            GROUP BY dt, category
            ORDER BY dt
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
        })

        for r in result.result_rows:
            ds = str(r[0])
            cat = r[1]
            accruals = float(r[2] or 0)
            commission = float(r[3] or 0)
            services = float(r[4] or 0)
            total_amount = float(r[5] or 0)

            if ds not in daily:
                daily[ds] = {
                    "revenue": 0, "commission": 0, "services": 0,
                    "logistics": 0, "storage": 0, "acquiring": 0,
                    "marketing": 0, "refunds": 0, "penalties": 0,
                    "compensation": 0, "other": 0, "payout": 0,
                }

            daily[ds]["payout"] += total_amount

            if cat == "Revenue":
                daily[ds]["revenue"] += accruals
                daily[ds]["commission"] += commission
                daily[ds]["services"] += services
            else:
                key = CAT_KEYS.get(cat, "other")
                daily[ds][key] += abs(total_amount)

    except Exception as e:
        logger.warning("Daily category breakdown query failed: %s", e)

    return daily


# ── Placement cost from fact_ozon_placement_cost ─────────

def get_placement_costs_by_sku(
    ch,
    shop_id: int,
    d_start: date,
    d_end: date,
) -> Dict[str, float]:
    """
    Get per-offer_id storage placement costs from fact_ozon_placement_cost.

    Uses dt BETWEEN d_start AND d_end to sum actual daily placement costs
    for the user-selected period. Falls back to latest period if no data
    exists in the requested range.

    IMPORTANT: fact_ozon_placement_cost has sku=0 for ~95% of rows.
    Ozon Placement Report binds data to offer_id, not sku.
    Therefore we group by offer_id (like warehouses.py /ozon/storage endpoint).

    Returns: {offer_id: placement_cost}
    """
    costs: Dict[str, float] = {}
    try:
        # Primary: filter by dt within user-selected period
        result = ch.query("""
            SELECT
                offer_id,
                round(sum(placement_cost), 2) AS total_cost
            FROM mms_analytics.fact_ozon_placement_cost FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d_start:Date}
              AND dt <= {d_end:Date}
            GROUP BY offer_id
            HAVING total_cost != 0
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start,
            "d_end": d_end,
        })
        for r in result.result_rows:
            oid = str(r[0] or "").strip()
            if oid:
                costs[oid] = float(r[1] or 0)

        # Fallback: if no data in date range, use latest period
        if not costs:
            result = ch.query("""
                SELECT
                    offer_id,
                    round(sum(placement_cost), 2) AS total_cost
                FROM mms_analytics.fact_ozon_placement_cost FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND period_to = (
                      SELECT max(period_to)
                      FROM mms_analytics.fact_ozon_placement_cost FINAL
                      WHERE shop_id = {shop_id:UInt32}
                  )
                GROUP BY offer_id
                HAVING total_cost != 0
            """, parameters={
                "shop_id": shop_id,
            })
            for r in result.result_rows:
                oid = str(r[0] or "").strip()
                if oid:
                    costs[oid] = float(r[1] or 0)
    except Exception as e:
        logger.warning("Placement cost query failed: %s", e)
    return costs


# ── SKU → offer_id mapping from ClickHouse ───────────────

def get_sku_to_offer_map_ch(
    ch,
    shop_id: int,
) -> Dict[int, str]:
    """
    Build SKU → offer_id mapping from fact_ozon_orders (ClickHouse).

    This is a FALLBACK for dim_ozon_products (PostgreSQL).
    fact_ozon_orders always has offer_id populated from Ozon API,
    whereas dim_ozon_products may be missing some products.

    Returns: {sku: offer_id}
    """
    sku_offer: Dict[int, str] = {}
    try:
        result = ch.query("""
            SELECT sku, any(offer_id) AS offer_id
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND sku > 0
              AND offer_id != ''
            GROUP BY sku
        """, parameters={"shop_id": shop_id})
        for r in result.result_rows:
            sku = int(r[0] or 0)
            oid = str(r[1] or "").strip()
            if sku > 0 and oid:
                sku_offer[sku] = oid
    except Exception as e:
        logger.warning("CH sku→offer_id mapping failed: %s", e)
    return sku_offer


# ── Ad spend from fact_ozon_ad_daily ─────────────────────

def get_ad_costs_by_sku(
    ch,
    shop_id: int,
    d_start: date,
    d_end: date,
) -> Dict[int, float]:
    """
    Get per-SKU advertising spend from fact_ozon_ad_daily.

    This is the correct source for per-SKU ad spend on Ozon.
    fact_ozon_transactions Marketing entries have sku=0 (Ozon API
    does not link ad spend to SKU in transaction stream).

    Returns: {sku: money_spent}
    """
    costs: Dict[int, float] = {}
    try:
        result = ch.query("""
            SELECT
                sku,
                sum(money_spent) AS total_spent
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d_start:Date}
              AND dt <= {d_end:Date}
              AND sku > 0
            GROUP BY sku
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start, "d_end": d_end,
        })
        for r in result.result_rows:
            sku = int(r[0] or 0)
            if sku > 0:
                costs[sku] = float(r[1] or 0)
    except Exception as e:
        logger.warning("Ad costs by SKU query failed: %s", e)
    return costs
