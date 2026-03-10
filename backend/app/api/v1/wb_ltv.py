"""
WB LTV Analysis API endpoints.

GET /sales/wb/ltv           — KPI + cohorts + SKU table + time distribution
GET /sales/wb/ltv/chain     — Purchase chain for a specific nm_id (L1→L5)

Buyer identification method:
    srid format: {buyer_hash}{sequential}.{item}.{variant}
    buyer_id = substring(splitByChar('.', srid)[1], 1, 8)
    Applies to numeric srid of length 16..19 (covers ~95% of orders).
    Validated: 97% single-region match across 2 shops.
"""
import logging
import math
import re
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.user import User
from app.models.shop import Shop
from app.api.v1.auth import get_current_user

router = APIRouter(prefix="/sales", tags=["WB LTV Analysis"])
logger = logging.getLogger(__name__)

# ── Buyer ID extraction for ClickHouse ────────────────────────
# Only numeric srid with length 16..19 (95% of orders).
# First 8 chars = stable buyer hash.
BUYER_ID_EXPR = "substring(splitByChar('.', srid)[1], 1, 8)"
BUYER_FILTER = (
    "length(splitByChar('.', srid)[1]) BETWEEN 16 AND 19 "
    "AND match(splitByChar('.', srid)[1], '^[0-9]+$')"
)


# ── Helpers ────────────────────────────────────────────────────

def _sf(v):
    """Safe float: NaN/Inf -> 0.0 for JSON compliance."""
    if v is None:
        return 0.0
    f = float(v)
    return 0.0 if (math.isnan(f) or math.isinf(f)) else round(f, 2)


def _ltv_dates(
    period: str,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
):
    """Return (start, end) dates for LTV analysis."""
    if date_from and date_to:
        return date_from, date_to
    end = date.today()
    mapping = {"30d": 30, "90d": 90, "6m": 180, "1y": 365, "all": 730}
    days = mapping.get(period, 180)
    return end - timedelta(days=days), end


async def _verify_wb_shop(db: AsyncSession, shop_id: int, user: User) -> Shop:
    """Verify shop exists, belongs to user, and is WB."""
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == user.id,
            Shop.marketplace == "wildberries",
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="WB магазин не найден",
        )
    return shop


# ══════════════════════════════════════════════════════════════
# Main WB LTV endpoint — KPI + Cohorts + SKU table + Distribution
# ══════════════════════════════════════════════════════════════

@router.get("/wb/ltv")
async def get_wb_ltv(
    shop_id: int = Query(..., description="Shop ID"),
    period: str = Query("6m", description="Period: 30d, 90d, 6m, 1y, all"),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    WB customer LTV analysis.

    Returns KPI metrics, cohort retention matrix, SKU repeat purchase table,
    and time-between-purchases distribution.

    Source: fact_orders_raw FINAL — buyer_id = substring(srid_base, 1, 8)
    """
    await _verify_wb_shop(db, shop_id, current_user)
    start_date, end_date = _ltv_dates(period, date_from, date_to)

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()
        params = {"shop_id": shop_id, "start_date": start_date, "end_date": end_date}

        # ══════════════════════════════════════════════
        # 1. KPI — unique buyers, repeat, avg LTV, etc
        # ══════════════════════════════════════════════
        kpi_result = ch.query(f"""
            WITH clients AS (
                SELECT
                    {BUYER_ID_EXPR} AS buyer_id,
                    count() AS orders,
                    sum(price_with_disc) AS client_rev
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND date >= {{start_date:Date}}
                  AND date <= {{end_date:Date}}
                  AND is_cancel = 0
                  AND {BUYER_FILTER}
                GROUP BY buyer_id
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
        cohort_rows = ch.query(f"""
            WITH
                buyer_orders AS (
                    SELECT
                        {BUYER_ID_EXPR} AS buyer_id,
                        toDate(date) AS order_date
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {{shop_id:UInt32}}
                      AND date >= {{start_date:Date}}
                      AND date <= {{end_date:Date}}
                      AND is_cancel = 0
                      AND {BUYER_FILTER}
                ),
                first_orders AS (
                    SELECT buyer_id, min(order_date) AS first_date
                    FROM buyer_orders
                    GROUP BY buyer_id
                ),
                cohort_data AS (
                    SELECT
                        fo.buyer_id,
                        toStartOfMonth(fo.first_date) AS cohort_month,
                        dateDiff('month', fo.first_date, bo.order_date) AS month_offset
                    FROM first_orders fo
                    JOIN buyer_orders bo ON fo.buyer_id = bo.buyer_id
                )
            SELECT
                toString(cohort_month) AS cohort,
                month_offset,
                count(DISTINCT buyer_id) AS clients
            FROM cohort_data
            GROUP BY cohort_month, month_offset
            ORDER BY cohort_month, month_offset
        """, parameters=params).result_rows

        # Build cohort matrix
        cohorts: dict[str, dict] = {}
        for row in cohort_rows:
            cohort_key = row[0][:7]
            offset = int(row[1])
            clients = int(row[2])
            if cohort_key not in cohorts:
                cohorts[cohort_key] = {"cohort": cohort_key, "size": 0, "months": {}}
            if offset == 0:
                cohorts[cohort_key]["size"] = clients
            cohorts[cohort_key]["months"][str(offset)] = clients

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
        sku_rows = ch.query(f"""
            WITH
                sku_clients AS (
                    SELECT
                        nm_id,
                        supplier_article,
                        subject,
                        brand,
                        {BUYER_ID_EXPR} AS buyer_id,
                        toDate(date) AS order_date,
                        price_with_disc AS revenue,
                        1 AS qty
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {{shop_id:UInt32}}
                      AND date >= {{start_date:Date}}
                      AND date <= {{end_date:Date}}
                      AND is_cancel = 0
                      AND {BUYER_FILTER}
                ),
                sku_buyer_agg AS (
                    SELECT
                        nm_id,
                        any(supplier_article) AS supplier_article,
                        any(subject) AS subject,
                        any(brand) AS brand,
                        buyer_id,
                        count() AS purchases,
                        sum(revenue) AS client_revenue,
                        sum(qty) AS client_qty,
                        min(order_date) AS first_buy,
                        max(order_date) AS last_buy
                    FROM sku_clients
                    GROUP BY nm_id, buyer_id
                )
            SELECT
                nm_id,
                any(supplier_article) AS article,
                any(subject) AS subject,
                any(brand) AS brand,
                count() AS total_buyers,
                sum(client_qty) AS total_qty,
                sum(client_revenue) AS total_revenue,
                countIf(purchases >= 2) AS repeat_buyers,
                countIf(purchases >= 3) AS buyers_3plus,
                round(countIf(purchases >= 2) / nullIf(count(), 0) * 100, 1) AS conv_2,
                round(countIf(purchases >= 3) / nullIf(countIf(purchases >= 2), 0) * 100, 1) AS conv_3,
                round(avg(if(purchases >= 2, dateDiff('day', first_buy, last_buy) / (purchases - 1), 0)), 0) AS avg_days,
                round(avgIf(client_revenue, purchases >= 2), 0) AS avg_ltv_repeat
            FROM sku_buyer_agg
            GROUP BY nm_id
            HAVING total_buyers >= 1
            ORDER BY repeat_buyers DESC, total_revenue DESC
            LIMIT 100
        """, parameters=params).result_rows

        sku_table = []
        for r in sku_rows:
            article = str(r[1])
            subject = str(r[2]) if r[2] else ""
            brand = str(r[3]) if r[3] else ""
            # Build readable name: use article if it has Cyrillic/letters,
            # otherwise use "brand subject" as fallback
            has_letters = bool(re.search(r'[а-яА-ЯёЁa-zA-Z]', article)) and not re.match(r'^\d{2}-\d+$', article)
            name = article if has_letters else f"{brand} {subject}".strip() or article
            sku_table.append({
                "sku": int(r[0]),
                "offer_id": article,
                "name": name[:80],
                "total_buyers": int(r[4] or 0),
                "total_qty": int(r[5] or 0),
                "total_revenue": _sf(r[6]),
                "repeat_buyers": int(r[7] or 0),
                "buyers_3plus": int(r[8] or 0),
                "conv_to_2": _sf(r[9]),
                "conv_to_3": _sf(r[10]),
                "avg_days_between": int(_sf(r[11])),
                "avg_ltv_repeat": _sf(r[12]),
                "image_url": "",
            })

        # ── Enrich with product names + images from PostgreSQL ──
        if sku_table:
            from app.api.v1.dashboard import wb_image_url
            nm_ids = [s["sku"] for s in sku_table]
            try:
                pg_result = await db.execute(
                    sa_text(
                        "SELECT nm_id, name, vendor_code FROM dim_products "
                        "WHERE shop_id = :shop_id AND nm_id = ANY(:nm_ids)"
                    ),
                    {"shop_id": shop_id, "nm_ids": nm_ids},
                )
                pg_map = {}
                for row in pg_result.fetchall():
                    pg_map[int(row[0])] = {
                        "name": row[1] or "",
                        "vendor_code": row[2] or "",
                    }
                for item in sku_table:
                    nm_id = item["sku"]
                    info = pg_map.get(nm_id, {})
                    # Use vendor_code as article, name as display name
                    if info.get("vendor_code"):
                        item["offer_id"] = info["vendor_code"]
                    if info.get("name"):
                        item["name"] = str(info["name"])[:80]
                    elif info.get("vendor_code"):
                        item["name"] = info["vendor_code"]
                    # Generate WB CDN image URL
                    item["image_url"] = wb_image_url(nm_id)
            except Exception as exc:
                logger.warning("PG enrichment failed: %s", exc)

        # ══════════════════════════════════════════════
        # 4. Time-to-repeat distribution (histogram)
        # ══════════════════════════════════════════════
        dist_rows = ch.query(f"""
            WITH
                buyer_orders AS (
                    SELECT
                        {BUYER_ID_EXPR} AS buyer_id,
                        toDate(date) AS order_date
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {{shop_id:UInt32}}
                      AND date >= {{start_date:Date}}
                      AND date <= {{end_date:Date}}
                      AND is_cancel = 0
                      AND {BUYER_FILTER}
                ),
                with_next AS (
                    SELECT
                        buyer_id,
                        order_date,
                        leadInFrame(order_date, 1, toDate('1970-01-01'))
                            OVER (PARTITION BY buyer_id ORDER BY order_date
                                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
                            AS next_date
                    FROM buyer_orders
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

        # ══════════════════════════════════════════════
        # 5. Monthly new vs repeat buyers
        # ══════════════════════════════════════════════
        monthly_rows = ch.query(f"""
            WITH
                buyer_orders AS (
                    SELECT
                        {BUYER_ID_EXPR} AS buyer_id,
                        toDate(date) AS order_date,
                        price_with_disc AS revenue
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {{shop_id:UInt32}}
                      AND date >= {{start_date:Date}}
                      AND date <= {{end_date:Date}}
                      AND is_cancel = 0
                      AND {BUYER_FILTER}
                ),
                first_purchase AS (
                    SELECT buyer_id, min(order_date) AS first_date
                    FROM buyer_orders
                    GROUP BY buyer_id
                ),
                monthly_agg AS (
                    SELECT
                        toStartOfMonth(bo.order_date) AS month,
                        bo.buyer_id,
                        fp.first_date,
                        sum(bo.revenue) AS revenue
                    FROM buyer_orders bo
                    JOIN first_purchase fp ON bo.buyer_id = fp.buyer_id
                    GROUP BY month, bo.buyer_id, fp.first_date
                )
            SELECT
                toString(month) AS m,
                count() AS total,
                countIf(toStartOfMonth(first_date) = month) AS new_buyers,
                countIf(toStartOfMonth(first_date) < month) AS repeat_buyers,
                round(sumIf(revenue, toStartOfMonth(first_date) = month), 0) AS new_revenue,
                round(sumIf(revenue, toStartOfMonth(first_date) < month), 0) AS repeat_revenue
            FROM monthly_agg
            GROUP BY month
            ORDER BY month
        """, parameters=params).result_rows

        monthly_buyers = [
            {
                "month": str(r[0])[:7],
                "total": int(r[1] or 0),
                "new_buyers": int(r[2] or 0),
                "repeat_buyers": int(r[3] or 0),
                "new_revenue": _sf(r[4]),
                "repeat_revenue": _sf(r[5]),
            }
            for r in monthly_rows
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
            "monthly_buyers": monthly_buyers,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("WB LTV analysis error: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка анализа WB LTV: {str(e)}",
        )


# ══════════════════════════════════════════════════════════════
# Purchase Chain endpoint — L1→L5 for specific nm_id
# ══════════════════════════════════════════════════════════════

@router.get("/wb/ltv/chain")
async def get_wb_purchase_chain(
    shop_id: int = Query(..., description="Shop ID"),
    sku: int = Query(..., description="nm_id to analyze chain for"),
    period: str = Query("6m", description="Period: 30d, 90d, 6m, 1y, all"),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    WB Purchase chain analysis for a specific nm_id.

    Shows what customers buy in their 2nd, 3rd, 4th, 5th purchases
    after initially buying this product.
    """
    await _verify_wb_shop(db, shop_id, current_user)
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

        # ── Chain data: what do buyers purchase after target nm_id ──
        chain_data = ch.query(f"""
            WITH
                all_orders AS (
                    SELECT
                        {BUYER_ID_EXPR} AS buyer_id,
                        nm_id,
                        supplier_article,
                        subject,
                        brand,
                        toDate(date) AS order_date,
                        price_with_disc AS revenue,
                        1 AS qty
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {{shop_id:UInt32}}
                      AND date >= {{start_date:Date}}
                      AND date <= {{end_date:Date}}
                      AND is_cancel = 0
                      AND {BUYER_FILTER}
                ),
                target_buyers AS (
                    SELECT DISTINCT buyer_id
                    FROM all_orders
                    WHERE nm_id = {{target_sku:UInt64}}
                ),
                numbered AS (
                    SELECT
                        ao.buyer_id,
                        ao.nm_id,
                        ao.supplier_article,
                        ao.subject,
                        ao.brand,
                        ao.order_date,
                        ao.revenue,
                        ao.qty,
                        dense_rank() OVER (
                            PARTITION BY ao.buyer_id
                            ORDER BY ao.order_date, ao.nm_id
                        ) AS purchase_num
                    FROM all_orders ao
                    JOIN target_buyers tb ON ao.buyer_id = tb.buyer_id
                ),
                first_target AS (
                    SELECT buyer_id, min(purchase_num) AS target_pnum
                    FROM numbered
                    WHERE nm_id = {{target_sku:UInt64}}
                    GROUP BY buyer_id
                ),
                reindexed AS (
                    SELECT
                        n.buyer_id,
                        n.nm_id,
                        n.supplier_article,
                        n.subject,
                        n.brand,
                        n.order_date,
                        n.revenue,
                        n.qty,
                        n.purchase_num - ft.target_pnum + 1 AS level
                    FROM numbered n
                    JOIN first_target ft ON n.buyer_id = ft.buyer_id
                    WHERE n.purchase_num >= ft.target_pnum
                      AND n.purchase_num < ft.target_pnum + 5
                )
            SELECT
                level,
                nm_id,
                any(supplier_article) AS article,
                any(subject) AS subject,
                any(brand) AS brand,
                count(DISTINCT buyer_id) AS buyers,
                sum(qty) AS total_qty,
                round(sum(revenue), 0) AS total_revenue,
                round(avg(revenue), 0) AS avg_revenue
            FROM reindexed
            GROUP BY level, nm_id
            ORDER BY level, buyers DESC
        """, parameters=params).result_rows

        # ── L1 stats ──
        l1_stats = ch.query(f"""
            SELECT
                count(DISTINCT {BUYER_ID_EXPR}) AS total_buyers,
                sum(1) AS total_qty,
                sum(price_with_disc) AS total_revenue,
                round(avg(price_with_disc), 0) AS avg_price,
                any(supplier_article) AS article,
                any(subject) AS subject,
                any(brand) AS brand
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {{shop_id:UInt32}}
              AND nm_id = {{target_sku:UInt64}}
              AND date >= {{start_date:Date}}
              AND date <= {{end_date:Date}}
              AND is_cancel = 0
              AND {BUYER_FILTER}
        """, parameters=params).first_row

        # ── Avg days between purchases ──
        time_between = ch.query(f"""
            WITH
                buyer_orders AS (
                    SELECT
                        {BUYER_ID_EXPR} AS buyer_id,
                        toDate(date) AS order_date,
                        dense_rank() OVER (
                            PARTITION BY {BUYER_ID_EXPR}
                            ORDER BY date
                        ) AS rn
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {{shop_id:UInt32}}
                      AND date >= {{start_date:Date}}
                      AND date <= {{end_date:Date}}
                      AND is_cancel = 0
                      AND {BUYER_FILTER}
                      AND {BUYER_ID_EXPR} IN (
                          SELECT DISTINCT {BUYER_ID_EXPR}
                          FROM mms_analytics.fact_orders_raw FINAL
                          WHERE shop_id = {{shop_id:UInt32}}
                            AND nm_id = {{target_sku:UInt64}}
                            AND is_cancel = 0
                            AND {BUYER_FILTER}
                      )
                ),
                numbered AS (
                    SELECT buyer_id, order_date, rn
                    FROM buyer_orders
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
            JOIN numbered n2 ON n1.buyer_id = n2.buyer_id
            WHERE n2.rn = n1.rn + 1
        """, parameters=params).first_row

        ch.close()

        # ── Build response ──
        from app.api.v1.dashboard import wb_image_url

        # Collect all nm_ids from chain + l1 for PG enrichment
        all_chain_nm_ids = set()
        for row in chain_data:
            all_chain_nm_ids.add(int(row[1]))
        all_chain_nm_ids.add(sku)  # target sku

        # Fetch names from dim_products
        pg_chain_map = {}
        try:
            pg_result = await db.execute(
                sa_text(
                    "SELECT nm_id, name, vendor_code FROM dim_products "
                    "WHERE shop_id = :shop_id AND nm_id = ANY(:nm_ids)"
                ),
                {"shop_id": shop_id, "nm_ids": list(all_chain_nm_ids)},
            )
            for row in pg_result.fetchall():
                pg_chain_map[int(row[0])] = {
                    "name": row[1] or "",
                    "vendor_code": row[2] or "",
                }
        except Exception as exc:
            logger.warning("PG chain enrichment failed: %s", exc)

        levels: dict[int, list] = {}
        for row in chain_data:
            lvl = int(row[0])
            nm_id = int(row[1])
            article = str(row[2])
            info = pg_chain_map.get(nm_id, {})
            offer_id = info.get("vendor_code") or article
            name = info.get("name") or info.get("vendor_code") or article
            if lvl not in levels:
                levels[lvl] = []
            levels[lvl].append({
                "sku": nm_id,
                "offer_id": offer_id,
                "name": name[:80],
                "buyers": int(row[5] or 0),
                "total_qty": int(row[6] or 0),
                "total_revenue": _sf(row[7]),
                "avg_revenue": _sf(row[8]),
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
                "products": products[:10],
            })

        l1_article = str(l1_stats[4]) if l1_stats[4] else ""
        l1_info = pg_chain_map.get(sku, {})
        l1_offer = l1_info.get("vendor_code") or l1_article
        l1_name = l1_info.get("name") or l1_info.get("vendor_code") or l1_article
        return {
            "shop_id": shop_id,
            "target_sku": sku,
            "period": period,
            "date_range": {"start": str(start_date), "end": str(end_date)},
            "l1": {
                "sku": sku,
                "offer_id": l1_offer,
                "name": l1_name[:80],
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
        logger.error("WB Purchase chain error: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка цепочки WB: {str(e)}",
        )
