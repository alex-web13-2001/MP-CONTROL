"""
Sales API endpoints.

GET /sales/ozon?shop_id=X&period=7  — Ozon sales analytics
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

router = APIRouter(prefix="/sales", tags=["Sales"])


# ── Helpers ────────────────────────────────────────────────────


def _safe_delta(current: float, previous: float) -> float:
    """Percentage change, safe for zero division."""
    if previous == 0:
        return 0.0 if current == 0 else 100.0
    return round((current - previous) / abs(previous) * 100, 1)


def _parse_dates(
    period: int,
    date_from: Optional[date],
    date_to: Optional[date],
) -> tuple[date, date, date, date]:
    """Return (cur_start, cur_end, prev_start, prev_end)."""
    today = date.today()
    if date_from and date_to:
        cur_start = date_from
        cur_end = date_to
        days = (cur_end - cur_start).days + 1
    else:
        days = period
        cur_end = today
        cur_start = today - timedelta(days=days - 1)
    prev_end = cur_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)
    return cur_start, cur_end, prev_start, prev_end


# ══════════════════════════════════════════════════════════════
# Ozon Sales
# ══════════════════════════════════════════════════════════════


@router.get("/ozon")
async def get_ozon_sales(
    shop_id: int = Query(..., description="Shop ID"),
    period: int = Query(7, ge=1, le=366, description="Period in days"),
    date_from: Optional[date] = Query(None, description="Custom range start"),
    date_to: Optional[date] = Query(None, description="Custom range end"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Ozon sales analytics: KPI, daily chart, geography, top products, returns.

    Sources:
      - fact_ozon_orders FINAL: orders, revenue, geography, top products
      - fact_ozon_returns FINAL: returns count, reasons
      - dim_ozon_products (PG): product names + images
    """
    # ── Verify shop ownership ──────────────────────────
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == current_user.id,
            Shop.marketplace == "ozon",
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ozon магазин не найден",
        )

    # ── Dates ──────────────────────────────────────────
    cur_start, cur_end, prev_start, prev_end = _parse_dates(period, date_from, date_to)

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()

        params = {
            "shop_id": shop_id,
            "cur_start": cur_start,
            "cur_end": cur_end,
            "prev_start": prev_start,
            "prev_end": prev_end,
        }

        # ══════════════════════════════════════════════
        # 1. KPI — Orders + Revenue (current vs previous)
        # ══════════════════════════════════════════════
        orders_kpi = ch.query("""
            SELECT
                period,
                count() AS orders_count,
                sum(price * quantity) AS revenue,
                sum(price * quantity) / nullIf(count(), 0) AS avg_check
            FROM (
                SELECT
                    CASE
                        WHEN toDate(addHours(in_process_at, 3)) >= {cur_start:Date} AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date} THEN 'current'
                        WHEN toDate(addHours(in_process_at, 3)) >= {prev_start:Date} AND toDate(addHours(in_process_at, 3)) <= {prev_end:Date} THEN 'previous'
                    END AS period,
                    price, quantity
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(in_process_at, 3)) >= {prev_start:Date}
                  AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
            )
            WHERE period != ''
            GROUP BY period
        """, parameters=params).result_rows

        orders_map = {
            row[0]: {"count": int(row[1]), "revenue": float(row[2]), "avg_check": float(row[3] or 0)}
            for row in orders_kpi
        }
        cur_o = orders_map.get("current", {"count": 0, "revenue": 0, "avg_check": 0})
        prev_o = orders_map.get("previous", {"count": 0, "revenue": 0, "avg_check": 0})

        # Returns KPI (current vs previous)
        returns_kpi = ch.query("""
            SELECT
                period,
                count() AS returns_count
            FROM (
                SELECT
                    CASE
                        WHEN dt >= {cur_start:Date} AND dt <= {cur_end:Date} THEN 'current'
                        WHEN dt >= {prev_start:Date} AND dt <= {prev_end:Date} THEN 'previous'
                    END AS period
                FROM mms_analytics.fact_ozon_returns FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND dt >= {prev_start:Date}
                  AND dt <= {cur_end:Date}
            )
            WHERE period != ''
            GROUP BY period
        """, parameters=params).result_rows

        returns_map = {row[0]: int(row[1]) for row in returns_kpi}
        cur_returns = returns_map.get("current", 0)
        prev_returns = returns_map.get("previous", 0)

        returns_pct = round(cur_returns / cur_o["count"] * 100, 1) if cur_o["count"] > 0 else 0

        kpi = {
            "orders_count": cur_o["count"],
            "orders_delta": _safe_delta(cur_o["count"], prev_o["count"]),
            "revenue": round(cur_o["revenue"]),
            "revenue_delta": _safe_delta(cur_o["revenue"], prev_o["revenue"]),
            "avg_check": round(cur_o["avg_check"]),
            "returns_count": cur_returns,
            "returns_delta": _safe_delta(cur_returns, prev_returns),
            "returns_pct": returns_pct,
        }

        # ══════════════════════════════════════════════
        # 2. Daily Chart — orders + revenue + returns per day
        # ══════════════════════════════════════════════
        daily_orders = ch.query("""
            SELECT
                toDate(addHours(in_process_at, 3)) AS dt,
                count() AS orders,
                sum(price * quantity) AS revenue
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(addHours(in_process_at, 3)) >= {cur_start:Date}
              AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
            GROUP BY dt
            ORDER BY dt
        """, parameters=params).result_rows

        daily_returns = ch.query("""
            SELECT
                dt,
                count() AS returns
            FROM mms_analytics.fact_ozon_returns FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {cur_start:Date}
              AND dt <= {cur_end:Date}
            GROUP BY dt
            ORDER BY dt
        """, parameters=params).result_rows

        returns_by_date = {str(row[0]): int(row[1]) for row in daily_returns}

        daily = []
        for row in daily_orders:
            dt_str = str(row[0])
            daily.append({
                "date": dt_str,
                "orders": int(row[1]),
                "revenue": round(float(row[2])),
                "returns": returns_by_date.get(dt_str, 0),
            })

        # ══════════════════════════════════════════════
        # 3. Geography — orders by region
        # ══════════════════════════════════════════════
        geo_rows = ch.query("""
            SELECT
                city,
                count() AS orders,
                sum(price * quantity) AS revenue,
                sum(price * quantity) / nullIf(count(), 0) AS avg_check
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(addHours(in_process_at, 3)) >= {cur_start:Date}
              AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
              AND city != ''
            GROUP BY city
            ORDER BY revenue DESC
            LIMIT 20
        """, parameters=params).result_rows

        total_orders_geo = sum(int(r[1]) for r in geo_rows)
        geo = []
        for row in geo_rows:
            orders_count = int(row[1])
            geo.append({
                "region": row[0],
                "orders": orders_count,
                "revenue": round(float(row[2])),
                "pct": round(orders_count / total_orders_geo * 100, 1) if total_orders_geo > 0 else 0,
                "avg_check": round(float(row[3] or 0)),
            })

        # ══════════════════════════════════════════════
        # 4. Top Products — by revenue with returns
        # ══════════════════════════════════════════════
        top_products_rows = ch.query("""
            SELECT
                sku,
                any(offer_id) AS offer_id,
                any(product_name) AS product_name,
                count() AS orders,
                sum(price * quantity) AS revenue,
                round(avg(price), 0) AS avg_price
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(addHours(in_process_at, 3)) >= {cur_start:Date}
              AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
            GROUP BY sku
            ORDER BY revenue DESC
            LIMIT 20
        """, parameters=params).result_rows

        # Get returns per SKU (current period)
        sku_list = [int(row[0]) for row in top_products_rows]
        returns_by_sku: dict[int, int] = {}
        if sku_list:
            sku_str = ",".join(str(s) for s in sku_list)
            returns_per_sku = ch.query(f"""
                SELECT sku, count() AS returns
                FROM mms_analytics.fact_ozon_returns FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND dt >= {{cur_start:Date}}
                  AND dt <= {{cur_end:Date}}
                  AND sku IN ({sku_str})
                GROUP BY sku
            """, parameters=params).result_rows
            returns_by_sku = {int(row[0]): int(row[1]) for row in returns_per_sku}

        # Get ad funnel per SKU (current period)
        funnel_by_sku: dict[int, dict] = {}
        if sku_list:
            sku_str = ",".join(str(s) for s in sku_list)
            funnel_rows = ch.query(f"""
                SELECT
                    sku,
                    sum(views) AS views,
                    sum(clicks) AS clicks,
                    sum(add_to_cart) AS atbs
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND dt >= {{cur_start:Date}}
                  AND dt <= {{cur_end:Date}}
                  AND sku IN ({sku_str})
                GROUP BY sku
            """, parameters=params).result_rows
            for fr in funnel_rows:
                funnel_by_sku[int(fr[0])] = {
                    "views": int(fr[1]),
                    "clicks": int(fr[2]),
                    "add_to_cart": int(fr[3]),
                }

        # ── Previous period data for deltas ──
        prev_orders_by_sku: dict[int, dict] = {}
        prev_returns_by_sku: dict[int, int] = {}
        prev_funnel_by_sku: dict[int, dict] = {}

        if sku_list:
            sku_str = ",".join(str(s) for s in sku_list)

            # Previous orders + revenue + avg_price
            prev_orders_rows = ch.query(f"""
                SELECT sku, count() AS orders, sum(price * quantity) AS revenue,
                       round(avg(price), 0) AS avg_price
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND toDate(addHours(in_process_at, 3)) >= {{prev_start:Date}}
                  AND toDate(addHours(in_process_at, 3)) <= {{prev_end:Date}}
                  AND sku IN ({sku_str})
                GROUP BY sku
            """, parameters=params).result_rows
            for pr in prev_orders_rows:
                prev_orders_by_sku[int(pr[0])] = {
                    "orders": int(pr[1]),
                    "revenue": round(float(pr[2])),
                    "avg_price": round(float(pr[3])),
                }

            # Previous returns
            prev_ret_rows = ch.query(f"""
                SELECT sku, count() AS returns
                FROM mms_analytics.fact_ozon_returns FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND dt >= {{prev_start:Date}}
                  AND dt <= {{prev_end:Date}}
                  AND sku IN ({sku_str})
                GROUP BY sku
            """, parameters=params).result_rows
            prev_returns_by_sku = {int(r[0]): int(r[1]) for r in prev_ret_rows}

            # Previous ad funnel
            prev_funnel_rows = ch.query(f"""
                SELECT sku, sum(views), sum(clicks), sum(add_to_cart)
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND dt >= {{prev_start:Date}}
                  AND dt <= {{prev_end:Date}}
                  AND sku IN ({sku_str})
                GROUP BY sku
            """, parameters=params).result_rows
            for pf in prev_funnel_rows:
                prev_funnel_by_sku[int(pf[0])] = {
                    "views": int(pf[1]),
                    "clicks": int(pf[2]),
                    "add_to_cart": int(pf[3]),
                }

        # Enrich with product images from PG
        sku_to_image: dict[int, str] = {}
        sku_to_name: dict[int, str] = {}
        if sku_list:
            placeholders = ",".join(f":sku_{i}" for i in range(len(sku_list)))
            bind_params = {f"sku_{i}": sku for i, sku in enumerate(sku_list)}
            bind_params["sid"] = shop_id
            pg_products = await db.execute(
                text(f"""
                    SELECT sku, name,
                           COALESCE(NULLIF(primary_image_url, ''), '') AS image_url
                    FROM dim_ozon_products
                    WHERE shop_id = :sid AND sku IN ({placeholders})
                """),
                bind_params,
            )
            for row in pg_products.fetchall():
                sku_to_image[row[0]] = row[2]
                sku_to_name[row[0]] = row[1]

        top_products = []
        for row in top_products_rows:
            sku = int(row[0])
            orders_count = int(row[3])
            revenue = round(float(row[4]))
            avg_price = round(float(row[5]))
            ret_count = returns_by_sku.get(sku, 0)
            funnel = funnel_by_sku.get(sku, {})
            views = funnel.get("views", 0)
            clicks = funnel.get("clicks", 0)
            add_to_cart = funnel.get("add_to_cart", 0)

            # Previous period
            prev = prev_orders_by_sku.get(sku, {})
            prev_orders = prev.get("orders", 0)
            prev_revenue = prev.get("revenue", 0)
            prev_avg_price = prev.get("avg_price", 0)
            prev_ret = prev_returns_by_sku.get(sku, 0)
            prev_f = prev_funnel_by_sku.get(sku, {})
            prev_views = prev_f.get("views", 0)
            prev_clicks = prev_f.get("clicks", 0)
            prev_atc = prev_f.get("add_to_cart", 0)

            # Current rates
            ctr = round(clicks / views * 100, 2) if views > 0 else 0
            cart_rate = round(add_to_cart / clicks * 100, 2) if clicks > 0 else 0
            order_rate = round(orders_count / add_to_cart * 100, 2) if add_to_cart > 0 else 0

            # Previous rates
            prev_ctr = round(prev_clicks / prev_views * 100, 2) if prev_views > 0 else 0
            prev_cart_rate = round(prev_atc / prev_clicks * 100, 2) if prev_clicks > 0 else 0
            prev_order_rate = round(prev_orders / prev_atc * 100, 2) if prev_atc > 0 else 0

            top_products.append({
                "sku": sku,
                "offer_id": row[1],
                "name": sku_to_name.get(sku, row[2] or ""),
                "image_url": sku_to_image.get(sku, ""),
                "orders": orders_count,
                "revenue": revenue,
                "avg_price": avg_price,
                "returns": ret_count,
                "return_pct": round(ret_count / orders_count * 100, 1) if orders_count > 0 else 0,
                # Deltas (sales)
                "orders_delta": _safe_delta(orders_count, prev_orders),
                "revenue_delta": _safe_delta(revenue, prev_revenue),
                "avg_price_delta": _safe_delta(avg_price, prev_avg_price),
                # Ad funnel metrics
                "ad_views": views,
                "ad_clicks": clicks,
                "ad_add_to_cart": add_to_cart,
                "ad_ctr": ctr,
                "ad_cart_rate": cart_rate,
                "ad_order_rate": order_rate,
                # Deltas (ad funnel)
                "ad_views_delta": _safe_delta(views, prev_views),
                "ad_clicks_delta": _safe_delta(clicks, prev_clicks),
                "ad_add_to_cart_delta": _safe_delta(add_to_cart, prev_atc),
                "ad_ctr_delta": round(ctr - prev_ctr, 2),
                "ad_cart_rate_delta": round(cart_rate - prev_cart_rate, 2),
                "ad_order_rate_delta": round(order_rate - prev_order_rate, 2),
            })

        # ══════════════════════════════════════════════
        # 5. Returns — by reason
        # ══════════════════════════════════════════════
        returns_reasons_rows = ch.query("""
            SELECT
                return_reason,
                count() AS cnt
            FROM mms_analytics.fact_ozon_returns FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {cur_start:Date}
              AND dt <= {cur_end:Date}
              AND return_reason != ''
            GROUP BY return_reason
            ORDER BY cnt DESC
            LIMIT 10
        """, parameters=params).result_rows

        total_reasons = sum(int(r[1]) for r in returns_reasons_rows)
        by_reason = []
        for row in returns_reasons_rows:
            cnt = int(row[1])
            by_reason.append({
                "reason": row[0],
                "count": cnt,
                "pct": round(cnt / total_reasons * 100, 1) if total_reasons > 0 else 0,
            })

        returns_data = {
            "total": cur_returns,
            "by_reason": by_reason,
        }

        return {
            "shop_id": shop_id,
            "date_from": str(cur_start),
            "date_to": str(cur_end),
            "kpi": kpi,
            "daily": daily,
            "geo": geo,
            "top_products": top_products,
            "returns": returns_data,
        }

    except Exception as e:
        logger.exception("Ozon sales error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка загрузки данных продаж: {e}",
        )


# ══════════════════════════════════════════════════════════════
# Per-Product Daily Dynamics
# ══════════════════════════════════════════════════════════════


@router.get("/ozon/product-daily")
async def get_ozon_product_daily(
    shop_id: int = Query(..., description="Shop ID"),
    skus: str = Query(..., description="Comma-separated SKU list"),
    period: int = Query(7, ge=1, le=366, description="Period in days"),
    date_from: Optional[date] = Query(None, description="Custom range start"),
    date_to: Optional[date] = Query(None, description="Custom range end"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Daily orders + revenue per product (by SKU list).
    Used to overlay individual product lines on the sales chart.
    """
    # Verify shop ownership
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == current_user.id,
            Shop.marketplace == "ozon",
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ozon магазин не найден",
        )

    # Parse SKUs
    try:
        sku_list = [int(s.strip()) for s in skus.split(",") if s.strip()]
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Некорректный формат SKU",
        )

    if not sku_list or len(sku_list) > 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Укажите от 1 до 10 SKU",
        )

    cur_start, cur_end, _, _ = _parse_dates(period, date_from, date_to)

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()

        sku_csv = ",".join(str(s) for s in sku_list)
        rows = ch.query(f"""
            SELECT
                sku,
                toDate(addHours(in_process_at, 3)) AS dt,
                count() AS orders,
                sum(price * quantity) AS revenue
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {{shop_id:UInt32}}
              AND toDate(addHours(in_process_at, 3)) >= {{cur_start:Date}}
              AND toDate(addHours(in_process_at, 3)) <= {{cur_end:Date}}
              AND sku IN ({sku_csv})
            GROUP BY sku, dt
            ORDER BY sku, dt
        """, parameters={
            "shop_id": shop_id,
            "cur_start": cur_start,
            "cur_end": cur_end,
        }).result_rows

        # Group by SKU
        products: dict[int, list] = {}
        for row in rows:
            sku = int(row[0])
            if sku not in products:
                products[sku] = []
            products[sku].append({
                "date": str(row[1]),
                "orders": int(row[2]),
                "revenue": round(float(row[3])),
            })

        return {
            "shop_id": shop_id,
            "date_from": str(cur_start),
            "date_to": str(cur_end),
            "products": products,
        }

    except Exception as e:
        logger.exception("Ozon product daily error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка загрузки динамики товаров: {e}",
        )


# ═══════════════════════════════════════════════════════════════
# ABC / XYZ Analysis
# ═══════════════════════════════════════════════════════════════

import math
import statistics


def _abc_group(cumulative_pct: float) -> str:
    if cumulative_pct <= 80:
        return "A"
    elif cumulative_pct <= 95:
        return "B"
    return "C"


def _xyz_group(cv: float) -> str:
    if cv < 10:
        return "X"
    elif cv < 25:
        return "Y"
    return "Z"


@router.get("/ozon/abc-xyz")
async def get_ozon_abc_xyz(
    shop_id: int = Query(...),
    period: int = Query(90, ge=14, le=365),
    use_profit: bool = Query(False),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """ABC/XYZ analysis for Ozon products.

    Net profit formula (same as finances):
        profit = revenue - commission - logistics - storage - acquiring - ad_spend - cogs
    """
    # Verify shop
    result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == user.id)
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(status_code=404, detail="Магазин не найден")

    try:
        from app.core.clickhouse import get_clickhouse_client

        ch = get_clickhouse_client()

        cur_end = date.today()
        cur_start = cur_end - timedelta(days=period)

        params = {
            "shop_id": shop_id,
            "cur_start": str(cur_start),
            "cur_end": str(cur_end),
        }

        # ── 0. SKU → offer_id mapping (from PG) ──
        sku_to_offer: dict[int, str] = {}
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

        # ── 1. Per-product metrics from fact_ozon_transactions ──
        #    Revenue, orders, commission, per-item logistics
        products: dict[str, dict] = {}

        txn_rows = ch.query("""
            SELECT
                sku,
                sum(accruals_for_sale) AS revenue,
                count() AS sales,
                sum(abs(sale_commission)) AS commission,
                sum(abs(services_total)) AS logistics
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND category = 'Revenue'
              AND sku > 0
              AND toDate(operation_date) >= {cur_start:Date}
              AND toDate(operation_date) <= {cur_end:Date}
            GROUP BY sku
        """, parameters=params).result_rows

        for r in txn_rows:
            sku = int(r[0] or 0)
            oid = sku_to_offer.get(sku, str(sku))
            products[oid] = {
                "sku": sku,
                "offer_id": oid,
                "revenue": float(r[1] or 0),
                "orders": int(r[2] or 0),
                "commission": float(r[3] or 0),
                "logistics": float(r[4] or 0),
                "storage": 0.0,
                "acquiring": 0.0,
                "ad_spend": 0.0,
                "cogs": 0.0,
            }

        if not products:
            return {"products": [], "summary": {}, "matrix": {}}

        # ── 2. Bulk charges (Logistics/Storage/Acquiring) ──
        #    Distributed proportionally by revenue share
        CAT_MAP = {
            "Logistics": "logistics",
            "Storage": "storage",
            "Acquiring": "acquiring",
        }
        try:
            bulk_result = ch.query("""
                SELECT
                    category,
                    sum(abs(amount)) AS total
                FROM mms_analytics.fact_ozon_transactions FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(operation_date) >= {cur_start:Date}
                  AND toDate(operation_date) <= {cur_end:Date}
                  AND category IN ('Logistics', 'Storage', 'Acquiring')
                GROUP BY category
            """, parameters=params)

            bulk_totals: dict[str, float] = {}
            for r in bulk_result.result_rows:
                key = CAT_MAP.get(r[0], "other")
                bulk_totals[key] = float(r[1] or 0)

            total_rev = sum(p["revenue"] for p in products.values())
            if total_rev > 0:
                for oid, p in products.items():
                    share = p["revenue"] / total_rev
                    for bkey in ("logistics", "storage", "acquiring"):
                        if bkey in bulk_totals:
                            p[bkey] += round(bulk_totals[bkey] * share, 2)
        except Exception as e:
            logger.warning("ABC/XYZ bulk charges error: %s", e)

        # ── 3. Ad spend from fact_ozon_ad_daily ──
        try:
            ads_result = ch.query("""
                SELECT
                    sku,
                    sum(money_spent) AS ads
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND dt >= {cur_start:Date}
                  AND dt <= {cur_end:Date}
                GROUP BY sku
            """, parameters=params)

            for r in ads_result.result_rows:
                sku = int(r[0] or 0)
                ads = float(r[1] or 0)
                oid = sku_to_offer.get(sku, str(sku))
                if oid in products:
                    products[oid]["ad_spend"] += ads
                else:
                    # Distribute unmatched ads proportionally
                    total_rev = sum(p["revenue"] for p in products.values())
                    if total_rev > 0:
                        for p in products.values():
                            p["ad_spend"] += round(ads * p["revenue"] / total_rev, 2)
        except Exception as e:
            logger.warning("ABC/XYZ ads error: %s", e)

        # ── 4. COGS from product_costs (PG) ──
        cost_map: dict[str, float] = {}
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
        except Exception as e:
            logger.warning("ABC/XYZ cogs error: %s", e)

        for oid, p in products.items():
            unit_cost = cost_map.get(oid.lower(), 0)
            if unit_cost > 0:
                p["cogs"] = round(unit_cost * p["orders"], 2)

        # ── 5. Product images & names from PG ──
        sku_to_image: dict[int, str] = {}
        sku_to_name: dict[int, str] = {}
        sku_list = [p["sku"] for p in products.values()]
        if sku_list:
            try:
                placeholders = ",".join(f":sku_{i}" for i in range(len(sku_list)))
                bind_params = {f"sku_{i}": sku for i, sku in enumerate(sku_list)}
                bind_params["sid"] = shop_id
                pg_products = await db.execute(
                    text(f"""
                        SELECT sku, name,
                               COALESCE(NULLIF(primary_image_url, ''), '') AS image_url
                        FROM dim_ozon_products
                        WHERE shop_id = :sid AND sku IN ({placeholders})
                    """),
                    bind_params,
                )
                for row in pg_products.fetchall():
                    sku_to_image[row[0]] = row[2]
                    sku_to_name[row[0]] = row[1]
            except Exception as e:
                logger.warning("ABC/XYZ product names error: %s", e)

        # ── 6. Weekly revenue per SKU (for XYZ) ──
        weekly_by_sku: dict[str, list[float]] = {oid: [] for oid in products}
        try:
            weekly_rows = ch.query("""
                SELECT
                    sku,
                    toStartOfWeek(toDate(addHours(in_process_at, 3)), 1) AS week,
                    sum(price * quantity) AS revenue
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(in_process_at, 3)) >= {cur_start:Date}
                  AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
                GROUP BY sku, week
                ORDER BY sku, week
            """, parameters=params).result_rows

            all_weeks: set[str] = set()
            for row in weekly_rows:
                sku = int(row[0])
                oid = sku_to_offer.get(sku, str(sku))
                w = str(row[1])
                all_weeks.add(w)
                if oid in weekly_by_sku:
                    weekly_by_sku[oid].append(float(row[2]))

            num_weeks = max(len(all_weeks), 1)
            for oid in weekly_by_sku:
                while len(weekly_by_sku[oid]) < num_weeks:
                    weekly_by_sku[oid].append(0.0)
        except Exception as e:
            logger.warning("ABC/XYZ weekly data error: %s", e)

        # ── 7. Build product list with NET profit ──
        product_list = []
        for oid, p in products.items():
            sku = p["sku"]
            revenue = round(p["revenue"])
            commission = round(p["commission"])
            logistics = round(p["logistics"])
            storage = round(p["storage"])
            acquiring = round(p["acquiring"])
            ad_spend = round(p["ad_spend"])
            cogs = round(p["cogs"])

            # NET profit = revenue - ALL expenses
            profit = revenue - commission - logistics - storage - acquiring - ad_spend - cogs
            mp_fees = commission + logistics + storage + acquiring

            avg_price = round(revenue / p["orders"]) if p["orders"] > 0 else 0

            product_list.append({
                "sku": sku,
                "offer_id": oid,
                "name": sku_to_name.get(sku, ""),
                "image_url": sku_to_image.get(sku, ""),
                "revenue": revenue,
                "profit": profit,
                "orders": p["orders"],
                "avg_price": avg_price,
                "cost_price": round(cost_map.get(oid.lower(), 0)),
                "commission": commission,
                "logistics": logistics,
                "storage": storage,
                "acquiring": acquiring,
                "mp_fees": mp_fees,
                "ad_spend": ad_spend,
                "cogs": cogs,
                "margin_pct": round(profit / revenue * 100, 1) if revenue > 0 else 0,
                "weekly_data": weekly_by_sku.get(oid, []),
            })

        # ── 8. Compute ABC ──
        metric_key = "profit" if use_profit else "revenue"
        product_list.sort(key=lambda p: p[metric_key], reverse=True)

        total_metric = sum(max(p[metric_key], 0) for p in product_list)

        cumulative = 0.0
        for p in product_list:
            share = (max(p[metric_key], 0) / total_metric * 100) if total_metric > 0 else 0
            cumulative += share
            p["abc_share"] = round(share, 1)
            p["abc_cumulative"] = round(cumulative, 1)
            p["abc_group"] = _abc_group(cumulative)

        # ── 9. Compute XYZ ──
        for p in product_list:
            weekly = p.get("weekly_data", [])
            if len(weekly) < 2:
                p["xyz_cv"] = 0
                p["xyz_group"] = "X"
                continue
            mean_val = statistics.mean(weekly)
            if mean_val == 0:
                p["xyz_cv"] = 100.0
                p["xyz_group"] = "Z"
                continue
            stdev = statistics.stdev(weekly)
            cv = round(stdev / mean_val * 100, 1)
            p["xyz_cv"] = cv
            p["xyz_group"] = _xyz_group(cv)

        # ── 10. Summary & Matrix ──
        summary: dict[str, dict] = {}
        for g in ["A", "B", "C", "X", "Y", "Z"]:
            summary[g] = {"count": 0, "revenue_share": 0}

        matrix: dict[str, int] = {}
        for a in ["A", "B", "C"]:
            for x in ["X", "Y", "Z"]:
                matrix[f"{a}{x}"] = 0

        for p in product_list:
            abc = p["abc_group"]
            xyz = p["xyz_group"]
            summary[abc]["count"] += 1
            summary[abc]["revenue_share"] += p["abc_share"]
            summary[xyz]["count"] += 1
            matrix[f"{abc}{xyz}"] += 1

        for g in summary.values():
            g["revenue_share"] = round(g["revenue_share"], 1)

        return {
            "shop_id": shop_id,
            "period": period,
            "use_profit": use_profit,
            "products": product_list,
            "summary": summary,
            "matrix": matrix,
        }

    except Exception as e:
        logger.exception("ABC/XYZ error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка ABC/XYZ анализа: {e}",
        )

