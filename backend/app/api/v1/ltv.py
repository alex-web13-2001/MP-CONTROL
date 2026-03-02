"""
LTV Analysis API endpoints (Ozon only).

GET /sales/ozon/ltv           — KPI + cohorts + SKU table + time distribution
GET /sales/ozon/ltv/chain     — Purchase chain for a specific SKU (L1→L5)
"""
import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sales", tags=["LTV Analysis"])


# ── Helpers ────────────────────────────────────────────────────

import math


def _sf(v) -> float:
    """Safe float: NaN/Inf -> 0.0 for JSON compliance."""
    try:
        f = float(v) if v is not None else 0.0
        return 0.0 if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return 0.0


def _ltv_dates(
    period: str,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> tuple[date, date]:
    """Return (start, end) dates for LTV analysis."""
    today = date.today()
    if date_from and date_to:
        return date_from, date_to
    mapping = {
        "30d": 30,
        "90d": 90,
        "6m": 180,
        "1y": 365,
        "all": 730,
    }
    days = mapping.get(period, 180)
    return today - timedelta(days=days), today


async def _verify_ozon_shop(db: AsyncSession, shop_id: int, user: User) -> Shop:
    """Verify shop exists and belongs to user."""
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == user.id,
            Shop.marketplace == "ozon",
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ozon магазин не найден",
        )
    return shop


# ══════════════════════════════════════════════════════════════
# Main LTV endpoint — KPI + Cohorts + SKU table + Distribution
# ══════════════════════════════════════════════════════════════


@router.get("/ozon/ltv")
async def get_ozon_ltv(
    shop_id: int = Query(..., description="Shop ID"),
    period: str = Query("6m", description="Period: 30d, 90d, 6m, 1y, all"),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Ozon customer LTV analysis.

    Returns KPI metrics, cohort retention matrix, SKU repeat purchase table,
    and time-between-purchases distribution.

    Source: fact_ozon_orders FINAL — client_id = splitByChar('-', posting_number)[1]
    """
    await _verify_ozon_shop(db, shop_id, current_user)
    start_date, end_date = _ltv_dates(period, date_from, date_to)

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()
        params = {"shop_id": shop_id, "start_date": start_date, "end_date": end_date}

        # ══════════════════════════════════════════════
        # 1. KPI — unique clients, repeat, avg LTV, etc
        # ══════════════════════════════════════════════
        kpi_result = ch.query("""
            WITH clients AS (
                SELECT
                    splitByChar('-', posting_number)[1] AS client_id,
                    count(DISTINCT order_number) AS orders,
                    sum(price * quantity) AS client_rev,
                    min(toDate(addHours(in_process_at, 3))) AS first_order_date,
                    max(toDate(addHours(in_process_at, 3))) AS last_order_date
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(in_process_at, 3)) >= {start_date:Date}
                  AND toDate(addHours(in_process_at, 3)) <= {end_date:Date}
                GROUP BY client_id
            )
            SELECT
                count() AS total_clients,
                countIf(orders >= 2) AS repeat_clients,
                round(avg(client_rev), 0) AS avg_ltv,
                round(sum(client_rev) / nullIf(sum(orders), 0), 0) AS avg_check,
                round(avg(orders), 2) AS avg_orders_per_client,
                sum(client_rev) AS total_revenue
            FROM clients
        """, parameters=params).first_row

        kpi = {
            "total_clients": int(kpi_result[0] or 0),
            "repeat_clients": int(kpi_result[1] or 0),
            "repeat_rate": round(int(kpi_result[1] or 0) / max(int(kpi_result[0] or 0), 1) * 100, 1),
            "avg_ltv": _sf(kpi_result[2]),
            "avg_check": _sf(kpi_result[3]),
            "avg_orders_per_client": _sf(kpi_result[4]),
            "total_revenue": _sf(kpi_result[5]),
        }

        # ══════════════════════════════════════════════
        # 2. Cohort Retention Matrix
        # ══════════════════════════════════════════════
        cohort_rows = ch.query("""
            WITH
                client_orders AS (
                    SELECT
                        splitByChar('-', posting_number)[1] AS client_id,
                        order_number,
                        min(toDate(addHours(in_process_at, 3))) AS order_date
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(in_process_at, 3)) >= {start_date:Date}
                      AND toDate(addHours(in_process_at, 3)) <= {end_date:Date}
                    GROUP BY client_id, order_number
                ),
                first_orders AS (
                    SELECT client_id, min(order_date) AS first_date
                    FROM client_orders
                    GROUP BY client_id
                ),
                cohort_data AS (
                    SELECT
                        fo.client_id,
                        toStartOfMonth(fo.first_date) AS cohort_month,
                        dateDiff('month', fo.first_date, co.order_date) AS month_offset
                    FROM first_orders fo
                    JOIN client_orders co ON fo.client_id = co.client_id
                )
            SELECT
                toString(cohort_month) AS cohort,
                month_offset,
                count(DISTINCT client_id) AS clients
            FROM cohort_data
            GROUP BY cohort_month, month_offset
            ORDER BY cohort_month, month_offset
        """, parameters=params).result_rows

        # Build cohort matrix
        cohorts: dict[str, dict] = {}
        for row in cohort_rows:
            cohort_key = row[0][:7]  # YYYY-MM
            offset = int(row[1])
            clients = int(row[2])
            if cohort_key not in cohorts:
                cohorts[cohort_key] = {"cohort": cohort_key, "size": 0, "months": {}}
            if offset == 0:
                cohorts[cohort_key]["size"] = clients
            cohorts[cohort_key]["months"][str(offset)] = clients

        # Calculate retention percentages
        cohort_matrix = []
        for key in sorted(cohorts.keys()):
            c = cohorts[key]
            size = c["size"]
            months_data = {}
            for offset_str, count in c["months"].items():
                months_data[offset_str] = {
                    "clients": count,
                    "rate": round(count / max(size, 1) * 100, 1),
                }
            cohort_matrix.append({
                "cohort": key,
                "size": size,
                "months": months_data,
            })

        # ══════════════════════════════════════════════
        # 3. SKU repeat purchase summary table
        # ══════════════════════════════════════════════
        sku_rows = ch.query("""
            WITH
                sku_clients AS (
                    SELECT
                        sku,
                        offer_id,
                        product_name,
                        splitByChar('-', posting_number)[1] AS client_id,
                        order_number,
                        toDate(addHours(in_process_at, 3)) AS order_date,
                        sum(price * quantity) AS order_revenue,
                        sum(quantity) AS qty
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(in_process_at, 3)) >= {start_date:Date}
                      AND toDate(addHours(in_process_at, 3)) <= {end_date:Date}
                    GROUP BY sku, offer_id, product_name, client_id, order_number, order_date
                ),
                sku_client_agg AS (
                    SELECT
                        sku,
                        argMax(offer_id, order_date) AS offer_id,
                        any(product_name) AS product_name,
                        client_id,
                        count() AS purchases,
                        sum(order_revenue) AS client_revenue,
                        sum(qty) AS client_qty,
                        min(order_date) AS first_buy,
                        max(order_date) AS last_buy
                    FROM sku_clients
                    GROUP BY sku, client_id
                )
            SELECT
                sku,
                argMax(offer_id, last_buy) AS offer_id,
                any(product_name) AS name,
                count() AS total_buyers,
                sum(client_qty) AS total_qty,
                sum(client_revenue) AS total_revenue,
                countIf(purchases >= 2) AS repeat_buyers,
                countIf(purchases >= 3) AS buyers_3plus,
                round(countIf(purchases >= 2) / nullIf(count(), 0) * 100, 1) AS conv_2,
                round(countIf(purchases >= 3) / nullIf(countIf(purchases >= 2), 0) * 100, 1) AS conv_3,
                round(avg(if(purchases >= 2, dateDiff('day', first_buy, last_buy) / (purchases - 1), 0)), 0) AS avg_days_between,
                round(avgIf(client_revenue, purchases >= 2), 0) AS avg_ltv_repeat
            FROM sku_client_agg
            GROUP BY sku
            HAVING total_buyers >= 1
            ORDER BY repeat_buyers DESC, total_revenue DESC
            LIMIT 100
        """, parameters=params).result_rows

        sku_table = []
        for r in sku_rows:
            sku_table.append({
                "sku": int(r[0]),
                "offer_id": str(r[1]),
                "name": str(r[2])[:80],
                "total_buyers": int(r[3] or 0),
                "total_qty": int(r[4] or 0),
                "total_revenue": _sf(r[5]),
                "repeat_buyers": int(r[6] or 0),
                "buyers_3plus": int(r[7] or 0),
                "conv_to_2": _sf(r[8]),
                "conv_to_3": _sf(r[9]),
                "avg_days_between": int(_sf(r[10])),
                "avg_ltv_repeat": _sf(r[11]),
                "image_url": "",
            })

        # ── Enrich with images from PostgreSQL ──
        if sku_table:
            sku_ids = [s["sku"] for s in sku_table]
            from sqlalchemy import text as sa_text
            img_result = await db.execute(
                sa_text(
                    "SELECT sku, main_image_url FROM dim_ozon_products "
                    "WHERE shop_id = :shop_id AND sku = ANY(:skus)"
                ),
                {"shop_id": shop_id, "skus": sku_ids},
            )
            img_map = {int(row[0]): (row[1] or "") for row in img_result.fetchall()}
            for item in sku_table:
                item["image_url"] = img_map.get(item["sku"], "")

        # ══════════════════════════════════════════════
        # 4. Time-to-repeat distribution (histogram data)
        # ══════════════════════════════════════════════
        dist_rows = ch.query("""
            WITH
                client_orders AS (
                    SELECT
                        splitByChar('-', posting_number)[1] AS client_id,
                        order_number,
                        min(toDate(addHours(in_process_at, 3))) AS order_date
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(in_process_at, 3)) >= {start_date:Date}
                      AND toDate(addHours(in_process_at, 3)) <= {end_date:Date}
                    GROUP BY client_id, order_number
                ),
                with_next AS (
                    SELECT
                        client_id,
                        order_date,
                        leadInFrame(order_date, 1, toDate('1970-01-01'))
                            OVER (PARTITION BY client_id ORDER BY order_date
                                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
                            AS next_date
                    FROM client_orders
                ),
                gaps AS (
                    SELECT dateDiff('day', order_date, next_date) AS days_gap
                    FROM with_next
                    WHERE next_date > toDate('1970-01-01')
                      AND next_date > order_date
                )
            SELECT
                multiIf(
                    days_gap <= 7, '0-7',
                    days_gap <= 14, '8-14',
                    days_gap <= 30, '15-30',
                    days_gap <= 60, '31-60',
                    days_gap <= 90, '61-90',
                    '90+'
                ) AS bucket,
                count() AS cnt,
                round(avg(days_gap), 1) AS avg_days
            FROM gaps
            GROUP BY bucket
            ORDER BY
                multiIf(
                    bucket = '0-7', 1,
                    bucket = '8-14', 2,
                    bucket = '15-30', 3,
                    bucket = '31-60', 4,
                    bucket = '61-90', 5,
                    6
                )
        """, parameters=params).result_rows

        distribution = [
            {"bucket": str(r[0]), "count": int(r[1]), "avg_days": float(r[2])}
            for r in dist_rows
        ]

        ch.close()

        return {
            "shop_id": shop_id,
            "period": period,
            "date_range": {"start": str(start_date), "end": str(end_date)},
            "kpi": kpi,
            "cohort_matrix": cohort_matrix,
            "sku_table": sku_table,
            "time_distribution": distribution,
        }

    except Exception as e:
        logger.error("LTV analysis error: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка анализа LTV: {str(e)}",
        )


# ══════════════════════════════════════════════════════════════
# Purchase Chain endpoint — L1→L5 for specific SKU
# ══════════════════════════════════════════════════════════════


@router.get("/ozon/ltv/chain")
async def get_ozon_purchase_chain(
    shop_id: int = Query(..., description="Shop ID"),
    sku: int = Query(..., description="SKU to analyze chain for"),
    period: str = Query("6m", description="Period: 30d, 90d, 6m, 1y, all"),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Purchase chain analysis for a specific SKU.

    Shows what customers buy in their 2nd, 3rd, 4th, 5th purchases
    after initially buying this SKU. Tracks cross-sell and up-sell patterns.
    """
    await _verify_ozon_shop(db, shop_id, current_user)
    start_date, end_date = _ltv_dates(period, date_from, date_to)

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()
        params = {
            "shop_id": shop_id,
            "target_sku": sku,
            "start_date": start_date,
            "end_date": end_date,
        }

        # ── Step 1: Get all client orders with numbered sequence ──
        chain_data = ch.query("""
            WITH
                -- All orders per client, one row per (client, order, date)
                all_orders AS (
                    SELECT
                        splitByChar('-', posting_number)[1] AS client_id,
                        order_number,
                        sku,
                        argMax(offer_id, toDate(addHours(in_process_at, 3))) AS offer_id,
                        argMax(product_name, toDate(addHours(in_process_at, 3))) AS product_name,
                        min(toDate(addHours(in_process_at, 3))) AS order_date,
                        sum(price * quantity) AS revenue,
                        sum(quantity) AS qty
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(in_process_at, 3)) >= {start_date:Date}
                      AND toDate(addHours(in_process_at, 3)) <= {end_date:Date}
                    GROUP BY client_id, order_number, sku
                ),
                -- Clients who bought the target SKU
                target_clients AS (
                    SELECT DISTINCT client_id
                    FROM all_orders
                    WHERE sku = {target_sku:UInt64}
                ),
                -- Number each client's orders chronologically
                client_numbered AS (
                    SELECT
                        ao.client_id,
                        ao.order_number,
                        ao.sku,
                        ao.offer_id,
                        ao.product_name,
                        ao.order_date,
                        ao.revenue,
                        ao.qty,
                        dense_rank() OVER (
                            PARTITION BY ao.client_id
                            ORDER BY ao.order_date, ao.order_number
                        ) AS purchase_num
                    FROM all_orders ao
                    JOIN target_clients tc ON ao.client_id = tc.client_id
                ),
                -- Find which purchase_num the target SKU was first bought
                first_target AS (
                    SELECT client_id, min(purchase_num) AS target_pnum
                    FROM client_numbered
                    WHERE sku = {target_sku:UInt64}
                    GROUP BY client_id
                ),
                -- Reindex: L1 = target purchase, L2 = next, etc
                reindexed AS (
                    SELECT
                        cn.client_id,
                        cn.sku,
                        cn.offer_id,
                        cn.product_name,
                        cn.order_date,
                        cn.revenue,
                        cn.qty,
                        cn.purchase_num - ft.target_pnum + 1 AS level
                    FROM client_numbered cn
                    JOIN first_target ft ON cn.client_id = ft.client_id
                    WHERE cn.purchase_num >= ft.target_pnum
                      AND cn.purchase_num < ft.target_pnum + 5
                )
            SELECT
                level,
                sku,
                argMax(offer_id, order_date) AS offer_id,
                argMax(product_name, order_date) AS name,
                count(DISTINCT client_id) AS buyers,
                sum(qty) AS total_qty,
                round(sum(revenue), 0) AS total_revenue,
                round(avg(revenue), 0) AS avg_revenue
            FROM reindexed
            GROUP BY level, sku
            ORDER BY level, buyers DESC
        """, parameters=params).result_rows

        # ── Step 2: Get L1 stats for context ──
        l1_stats = ch.query("""
            SELECT
                count(DISTINCT splitByChar('-', posting_number)[1]) AS total_buyers,
                sum(quantity) AS total_qty,
                round(sum(price * quantity), 0) AS total_revenue,
                round(avg(price), 0) AS avg_price,
                argMax(offer_id, toDate(addHours(in_process_at, 3))) AS offer_id,
                argMax(product_name, toDate(addHours(in_process_at, 3))) AS name
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND sku = {target_sku:UInt64}
              AND toDate(addHours(in_process_at, 3)) >= {start_date:Date}
              AND toDate(addHours(in_process_at, 3)) <= {end_date:Date}
        """, parameters=params).first_row

        # ── Step 3: Time between purchases ──
        time_between = ch.query("""
            WITH
                client_orders AS (
                    SELECT
                        splitByChar('-', posting_number)[1] AS client_id,
                        order_number,
                        min(toDate(addHours(in_process_at, 3))) AS order_date
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(in_process_at, 3)) >= {start_date:Date}
                      AND toDate(addHours(in_process_at, 3)) <= {end_date:Date}
                      AND splitByChar('-', posting_number)[1] IN (
                          SELECT DISTINCT splitByChar('-', posting_number)[1]
                          FROM mms_analytics.fact_ozon_orders FINAL
                          WHERE shop_id = {shop_id:UInt32} AND sku = {target_sku:UInt64}
                      )
                    GROUP BY client_id, order_number
                ),
                numbered AS (
                    SELECT
                        client_id,
                        order_date,
                        row_number() OVER (PARTITION BY client_id ORDER BY order_date) AS rn
                    FROM client_orders
                )
            SELECT
                round(avgIf(
                    dateDiff('day', n1.order_date, n2.order_date),
                    n1.rn = 1 AND n2.rn = 2
                ), 0) AS avg_days_1_to_2,
                round(avgIf(
                    dateDiff('day', n1.order_date, n2.order_date),
                    n1.rn = 2 AND n2.rn = 3
                ), 0) AS avg_days_2_to_3,
                round(avgIf(
                    dateDiff('day', n1.order_date, n2.order_date),
                    n1.rn = 3 AND n2.rn = 4
                ), 0) AS avg_days_3_to_4,
                round(avgIf(
                    dateDiff('day', n1.order_date, n2.order_date),
                    n1.rn = 4 AND n2.rn = 5
                ), 0) AS avg_days_4_to_5
            FROM numbered n1
            JOIN numbered n2 ON n1.client_id = n2.client_id
            WHERE n2.rn = n1.rn + 1
        """, parameters=params).first_row

        ch.close()

        # ── Build response ──
        # Group chain_data by level
        levels: dict[int, list] = {}
        for row in chain_data:
            lvl = int(row[0])
            if lvl not in levels:
                levels[lvl] = []
            levels[lvl].append({
                "sku": int(row[1]),
                "offer_id": str(row[2]),
                "name": str(row[3])[:80],
                "buyers": int(row[4] or 0),
                "total_qty": int(row[5] or 0),
                "total_revenue": _sf(row[6]),
                "avg_revenue": _sf(row[7]),
            })

        # Calculate conversions
        l1_total = int(l1_stats[0] or 0)
        chain_levels = []
        for lvl_num in range(1, 6):
            products = levels.get(lvl_num, [])
            lvl_total_buyers = sum(p["buyers"] for p in products)
            prev_total = l1_total if lvl_num == 1 else sum(
                p["buyers"] for p in levels.get(lvl_num - 1, [])
            )

            # Add percentage from L1 to each product
            for p in products:
                p["pct_of_l1"] = round(p["buyers"] / max(l1_total, 1) * 100, 1)
                p["pct_of_level"] = round(
                    p["buyers"] / max(lvl_total_buyers, 1) * 100, 1
                )

            chain_levels.append({
                "level": lvl_num,
                "total_buyers": lvl_total_buyers,
                "conversion_from_prev": round(
                    lvl_total_buyers / max(prev_total, 1) * 100, 1
                ) if lvl_num > 1 else 100.0,
                "conversion_from_l1": round(
                    lvl_total_buyers / max(l1_total, 1) * 100, 1
                ),
                "products": products[:10],  # Top 10 per level
            })

        return {
            "shop_id": shop_id,
            "target_sku": sku,
            "period": period,
            "date_range": {"start": str(start_date), "end": str(end_date)},
            "l1": {
                "sku": sku,
                "offer_id": str(l1_stats[4]),
                "name": str(l1_stats[5])[:80],
                "total_buyers": l1_total,
                "total_qty": int(l1_stats[1] or 0),
                "total_revenue": _sf(l1_stats[2]),
                "avg_price": _sf(l1_stats[3]),
            },
            "chain": chain_levels,
            "avg_days_between": {
                "l1_to_l2": int(_sf(time_between[0])),
                "l2_to_l3": int(_sf(time_between[1])),
                "l3_to_l4": int(_sf(time_between[2])),
                "l4_to_l5": int(_sf(time_between[3])),
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Purchase chain error: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка цепочки продаж: {str(e)}",
        )
