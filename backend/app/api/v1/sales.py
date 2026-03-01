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


# ══════════════════════════════════════════════════════════════
# WB Sales Overview
# ══════════════════════════════════════════════════════════════


@router.get("/wb")
async def get_wb_sales(
    shop_id: int = Query(..., description="Shop ID"),
    period: int = Query(7, ge=1, le=366, description="Period in days"),
    date_from: Optional[date] = Query(None, description="Custom range start"),
    date_to: Optional[date] = Query(None, description="Custom range end"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    WB sales analytics: KPI, daily chart, geography, top products.

    Sources:
      - fact_orders_raw FINAL: orders, revenue, geography, cancels
      - fact_sales_funnel: organic funnel (views, carts, orders)
      - dim_products (PG): product names + images
    """
    # ── Verify shop ownership ──────────────────────────
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == current_user.id,
            Shop.marketplace == "wildberries",
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="WB магазин не найден",
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
        # 1. KPI — Orders + Revenue + Cancels
        # ══════════════════════════════════════════════
        orders_kpi = ch.query("""
            SELECT
                period,
                count() AS orders_count,
                sum(price_with_disc) AS revenue,
                sum(price_with_disc) / nullIf(count(), 0) AS avg_check,
                countIf(is_cancel = 1) AS cancel_count
            FROM (
                SELECT
                    CASE
                        WHEN toDate(date) >= {cur_start:Date} AND toDate(date) <= {cur_end:Date} THEN 'current'
                        WHEN toDate(date) >= {prev_start:Date} AND toDate(date) <= {prev_end:Date} THEN 'previous'
                    END AS period,
                    price_with_disc, is_cancel
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(date) >= {prev_start:Date}
                  AND toDate(date) <= {cur_end:Date}
            )
            WHERE period != ''
            GROUP BY period
        """, parameters=params).result_rows

        orders_map = {
            row[0]: {
                "count": int(row[1]),
                "revenue": float(row[2]),
                "avg_check": float(row[3] or 0),
                "cancels": int(row[4]),
            }
            for row in orders_kpi
        }
        cur_o = orders_map.get("current", {"count": 0, "revenue": 0, "avg_check": 0, "cancels": 0})
        prev_o = orders_map.get("previous", {"count": 0, "revenue": 0, "avg_check": 0, "cancels": 0})

        cancel_pct = round(cur_o["cancels"] / cur_o["count"] * 100, 1) if cur_o["count"] > 0 else 0

        kpi = {
            "orders_count": cur_o["count"],
            "orders_delta": _safe_delta(cur_o["count"], prev_o["count"]),
            "revenue": round(cur_o["revenue"]),
            "revenue_delta": _safe_delta(cur_o["revenue"], prev_o["revenue"]),
            "avg_check": round(cur_o["avg_check"]),
            "cancels_count": cur_o["cancels"],
            "cancels_delta": _safe_delta(cur_o["cancels"], prev_o["cancels"]),
            "cancels_pct": cancel_pct,
        }

        # ══════════════════════════════════════════════
        # 2. Daily Chart — orders + revenue + cancels
        # ══════════════════════════════════════════════
        daily_rows = ch.query("""
            SELECT
                toDate(date) AS dt,
                count() AS orders,
                sum(price_with_disc) AS revenue,
                countIf(is_cancel = 1) AS cancels
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(date) >= {cur_start:Date}
              AND toDate(date) <= {cur_end:Date}
            GROUP BY dt
            ORDER BY dt
        """, parameters=params).result_rows

        daily = []
        for row in daily_rows:
            daily.append({
                "date": str(row[0]),
                "orders": int(row[1]),
                "revenue": round(float(row[2])),
                "returns": int(row[3]),  # "returns" key for frontend compatibility
            })

        # ══════════════════════════════════════════════
        # 3. Geography — orders by region
        # ══════════════════════════════════════════════
        geo_rows = ch.query("""
            SELECT
                region_name,
                count() AS orders,
                sum(price_with_disc) AS revenue,
                sum(price_with_disc) / nullIf(count(), 0) AS avg_check
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(date) >= {cur_start:Date}
              AND toDate(date) <= {cur_end:Date}
              AND region_name != ''
              AND is_cancel = 0
            GROUP BY region_name
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
        # 4. Top Products — by revenue
        # ══════════════════════════════════════════════
        top_products_rows = ch.query("""
            SELECT
                nm_id,
                any(supplier_article) AS supplier_article,
                count() AS orders,
                sum(price_with_disc) AS revenue,
                round(avg(price_with_disc), 0) AS avg_price,
                countIf(is_cancel = 1) AS cancels
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(date) >= {cur_start:Date}
              AND toDate(date) <= {cur_end:Date}
            GROUP BY nm_id
            ORDER BY revenue DESC
            LIMIT 20
        """, parameters=params).result_rows

        nm_list = [int(row[0]) for row in top_products_rows]

        # Ad funnel from fact_advert_stats_v3 (ReplacingMergeTree)
        funnel_by_nm: dict[int, dict] = {}
        if nm_list:
            nm_str = ",".join(str(n) for n in nm_list)
            funnel_rows = ch.query(f"""
                SELECT
                    nm_id,
                    sum(views) AS ad_views,
                    sum(clicks) AS ad_clicks,
                    sum(atbs) AS ad_carts,
                    sum(orders) AS ad_orders,
                    sum(spend) AS ad_spend
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND date >= {{cur_start:Date}}
                  AND date <= {{cur_end:Date}}
                  AND nm_id IN ({nm_str})
                GROUP BY nm_id
            """, parameters=params).result_rows
            for fr in funnel_rows:
                funnel_by_nm[int(fr[0])] = {
                    "views": int(fr[1]),
                    "clicks": int(fr[2]),
                    "carts": int(fr[3]),
                    "orders": int(fr[4]),
                    "spend": float(fr[5]),
                }

        # Previous period orders
        prev_orders_by_nm: dict[int, dict] = {}
        if nm_list:
            nm_str = ",".join(str(n) for n in nm_list)
            prev_rows = ch.query(f"""
                SELECT nm_id, count() AS orders, sum(price_with_disc) AS revenue,
                       round(avg(price_with_disc), 0) AS avg_price
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND toDate(date) >= {{prev_start:Date}}
                  AND toDate(date) <= {{prev_end:Date}}
                  AND nm_id IN ({nm_str})
                GROUP BY nm_id
            """, parameters=params).result_rows
            for pr in prev_rows:
                prev_orders_by_nm[int(pr[0])] = {
                    "orders": int(pr[1]),
                    "revenue": round(float(pr[2])),
                    "avg_price": round(float(pr[3])),
                }

        # Previous ad funnel
        prev_funnel_by_nm: dict[int, dict] = {}
        if nm_list:
            nm_str = ",".join(str(n) for n in nm_list)
            prev_funnel_rows = ch.query(f"""
                SELECT
                    nm_id,
                    sum(views) AS ad_views,
                    sum(clicks) AS ad_clicks,
                    sum(atbs) AS ad_carts,
                    sum(orders) AS ad_orders
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND date >= {{prev_start:Date}}
                  AND date <= {{prev_end:Date}}
                  AND nm_id IN ({nm_str})
                GROUP BY nm_id
            """, parameters=params).result_rows
            for pf in prev_funnel_rows:
                prev_funnel_by_nm[int(pf[0])] = {
                    "views": int(pf[1]),
                    "clicks": int(pf[2]),
                    "carts": int(pf[3]),
                    "orders": int(pf[4]),
                }

        # Product names + images from PG
        nm_to_name: dict[int, str] = {}
        nm_to_image: dict[int, str] = {}
        nm_to_vendor: dict[int, str] = {}
        if nm_list:
            placeholders = ",".join(f":nm_{i}" for i in range(len(nm_list)))
            bind_params = {f"nm_{i}": nm for i, nm in enumerate(nm_list)}
            bind_params["sid"] = shop_id
            pg_products = await db.execute(
                text(f"""
                    SELECT nm_id, COALESCE(name, ''), COALESCE(main_image_url, ''),
                           COALESCE(vendor_code, '')
                    FROM dim_products
                    WHERE shop_id = :sid AND nm_id IN ({placeholders})
                """),
                bind_params,
            )
            for row in pg_products.fetchall():
                nm_to_name[row[0]] = row[1]
                nm_to_image[row[0]] = row[2]
                nm_to_vendor[row[0]] = row[3]

        top_products = []
        for row in top_products_rows:
            nm_id = int(row[0])
            vendor = row[1] or nm_to_vendor.get(nm_id, "")
            orders_count = int(row[2])
            revenue = round(float(row[3]))
            avg_price = round(float(row[4]))
            cancels = int(row[5])

            funnel = funnel_by_nm.get(nm_id, {})
            ad_views = funnel.get("views", 0)
            ad_clicks = funnel.get("clicks", 0)
            ad_carts = funnel.get("carts", 0)

            prev = prev_orders_by_nm.get(nm_id, {})
            prev_orders = prev.get("orders", 0)
            prev_revenue = prev.get("revenue", 0)
            prev_avg_price = prev.get("avg_price", 0)

            prev_f = prev_funnel_by_nm.get(nm_id, {})
            prev_views = prev_f.get("views", 0)
            prev_clicks = prev_f.get("clicks", 0)
            prev_carts = prev_f.get("carts", 0)

            # Real ad rates
            ctr = round(ad_clicks / ad_views * 100, 2) if ad_views > 0 else 0
            cart_rate = round(ad_carts / ad_clicks * 100, 2) if ad_clicks > 0 else 0
            order_rate = round(orders_count / ad_carts * 100, 2) if ad_carts > 0 else 0

            prev_ctr = round(prev_clicks / prev_views * 100, 2) if prev_views > 0 else 0
            prev_cart_rate = round(prev_carts / prev_clicks * 100, 2) if prev_clicks > 0 else 0
            prev_order_rate = round(prev_orders / prev_carts * 100, 2) if prev_carts > 0 else 0

            top_products.append({
                "sku": nm_id,
                "offer_id": vendor,
                "name": nm_to_name.get(nm_id, ""),
                "image_url": nm_to_image.get(nm_id, ""),
                "orders": orders_count,
                "revenue": revenue,
                "avg_price": avg_price,
                "returns": cancels,  # "returns" for frontend compat
                "return_pct": round(cancels / orders_count * 100, 1) if orders_count > 0 else 0,
                # Deltas
                "orders_delta": _safe_delta(orders_count, prev_orders),
                "revenue_delta": _safe_delta(revenue, prev_revenue),
                "avg_price_delta": _safe_delta(avg_price, prev_avg_price),
                # Ad funnel
                "ad_views": ad_views,
                "ad_clicks": ad_clicks,
                "ad_add_to_cart": ad_carts,
                "ad_ctr": ctr,
                "ad_cart_rate": cart_rate,
                "ad_order_rate": order_rate,
                # Funnel deltas
                "ad_views_delta": _safe_delta(ad_views, prev_views),
                "ad_clicks_delta": _safe_delta(ad_clicks, prev_clicks),
                "ad_add_to_cart_delta": _safe_delta(ad_carts, prev_carts),
                "ad_ctr_delta": round(ctr - prev_ctr, 2),
                "ad_cart_rate_delta": round(cart_rate - prev_cart_rate, 2),
                "ad_order_rate_delta": round(order_rate - prev_order_rate, 2),
            })

        return {
            "shop_id": shop_id,
            "date_from": str(cur_start),
            "date_to": str(cur_end),
            "kpi": kpi,
            "daily": daily,
            "geo": geo,
            "top_products": top_products,
            "returns": {"total": cur_o["cancels"], "by_reason": []},
        }

    except Exception as e:
        logger.exception("WB sales error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка загрузки данных продаж WB: {e}",
        )


# ══════════════════════════════════════════════════════════════
# WB Per-Product Daily Dynamics
# ══════════════════════════════════════════════════════════════


@router.get("/wb/product-daily")
async def get_wb_product_daily(
    shop_id: int = Query(..., description="Shop ID"),
    skus: str = Query(..., description="Comma-separated nm_id list"),
    period: int = Query(7, ge=1, le=366, description="Period in days"),
    date_from: Optional[date] = Query(None, description="Custom range start"),
    date_to: Optional[date] = Query(None, description="Custom range end"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Daily orders + revenue per WB product (by nm_id list).
    Used to overlay individual product lines on the sales chart.
    """
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == current_user.id,
            Shop.marketplace == "wildberries",
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="WB магазин не найден",
        )

    try:
        nm_list = [int(s.strip()) for s in skus.split(",") if s.strip()]
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Некорректный формат nm_id",
        )

    if not nm_list or len(nm_list) > 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Укажите от 1 до 10 nm_id",
        )

    cur_start, cur_end, _, _ = _parse_dates(period, date_from, date_to)

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()
        nm_csv = ",".join(str(n) for n in nm_list)
        rows = ch.query(f"""
            SELECT
                nm_id,
                toDate(date) AS dt,
                count() AS orders,
                sum(price_with_disc) AS revenue
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {{shop_id:UInt32}}
              AND toDate(date) >= {{cur_start:Date}}
              AND toDate(date) <= {{cur_end:Date}}
              AND nm_id IN ({nm_csv})
            GROUP BY nm_id, dt
            ORDER BY nm_id, dt
        """, parameters={
            "shop_id": shop_id,
            "cur_start": cur_start,
            "cur_end": cur_end,
        }).result_rows

        products: dict[int, list] = {}
        for row in rows:
            nm_id = int(row[0])
            if nm_id not in products:
                products[nm_id] = []
            products[nm_id].append({
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
        logger.exception("WB product daily error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка загрузки динамики товаров WB: {e}",
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


# ═══════════════════════════════════════════════════════════════
# WB ABC / XYZ Analysis
# ═══════════════════════════════════════════════════════════════


@router.get("/wb/abc-xyz")
async def get_wb_abc_xyz(
    shop_id: int = Query(...),
    period: int = Query(90, ge=14, le=365),
    use_profit: bool = Query(False),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """ABC/XYZ analysis for WB products.

    Data sources:
      - fact_finances FINAL: revenue, logistics, storage, acquiring, penalties, commission (rev-payout)
      - fact_advert_stats_v3 FINAL: ad spend per nm_id
      - product_costs (PG): COGS per vendor_code
      - dim_products (PG): product names + images
      - fact_orders_raw FINAL: weekly revenue per nm_id (for XYZ)
    """
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

        # ── 1. Per-product financials from fact_finances FINAL ──
        products: dict[str, dict] = {}

        fin_rows = ch.query("""
            SELECT
                vendor_code,
                JSONExtractUInt(raw_payload, 'nm_id') AS nm_id,
                sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                    operation_type = 'Продажа') AS revenue,
                sumIf(payout_amount, operation_type = 'Продажа')
                    - sumIf(payout_amount, operation_type = 'Возврат') AS payout,
                sum(abs(wb_delivery_rub)) AS logistics,
                sum(abs(storage_fee)) AS storage,
                sum(abs(wb_acquiring)) AS acquiring,
                sum(abs(penalty_total)) AS penalties,
                sumIf(quantity, operation_type = 'Продажа' AND quantity > 0) AS sales
            FROM mms_analytics.fact_finances FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND marketplace = 1
              AND event_date >= {cur_start:Date}
              AND event_date <= {cur_end:Date}
            GROUP BY vendor_code, nm_id
        """, parameters=params).result_rows

        for r in fin_rows:
            vc = str(r[0] or "").strip()
            if not vc:
                continue
            nm_id = int(r[1] or 0)
            revenue = float(r[2] or 0)
            payout = float(r[3] or 0)
            logistics = abs(float(r[4] or 0))
            storage = abs(float(r[5] or 0))
            acquiring = abs(float(r[6] or 0))
            penalties = abs(float(r[7] or 0))
            sales = int(r[8] or 0)
            commission = max(revenue - payout, 0)

            if vc not in products:
                products[vc] = {
                    "sku": nm_id,
                    "offer_id": vc,
                    "revenue": 0.0,
                    "orders": 0,
                    "commission": 0.0,
                    "logistics": 0.0,
                    "storage": 0.0,
                    "acquiring": 0.0,
                    "penalties": 0.0,
                    "ad_spend": 0.0,
                    "cogs": 0.0,
                }
            p = products[vc]
            if nm_id and not p["sku"]:
                p["sku"] = nm_id
            p["revenue"] += revenue
            p["orders"] += sales
            p["commission"] += commission
            p["logistics"] += logistics
            p["storage"] += storage
            p["acquiring"] += acquiring
            p["penalties"] += penalties

        if not products:
            return {"shop_id": shop_id, "period": period, "use_profit": use_profit,
                    "products": [], "summary": {}, "matrix": {}}

        # ── 2. Ad spend from fact_advert_stats_v3 ──
        nm_to_vc: dict[int, str] = {}
        for vc, p in products.items():
            if p["sku"]:
                nm_to_vc[p["sku"]] = vc

        try:
            ad_rows = ch.query("""
                SELECT nm_id, sum(spend) AS ad_spend
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND date >= {cur_start:Date}
                  AND date <= {cur_end:Date}
                GROUP BY nm_id
            """, parameters=params).result_rows

            total_rev = sum(p["revenue"] for p in products.values())
            for r in ad_rows:
                nm_id = int(r[0] or 0)
                ads = float(r[1] or 0)
                vc = nm_to_vc.get(nm_id)
                if vc and vc in products:
                    products[vc]["ad_spend"] += ads
                elif total_rev > 0:
                    for p in products.values():
                        p["ad_spend"] += round(ads * p["revenue"] / total_rev, 2)
        except Exception as e:
            logger.warning("WB ABC/XYZ ads error: %s", e)

        # ── 3. COGS from product_costs (PG) ──
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
            logger.warning("WB ABC/XYZ cogs error: %s", e)

        for vc, p in products.items():
            unit_cost = cost_map.get(vc.lower(), 0)
            if unit_cost > 0:
                p["cogs"] = round(unit_cost * p["orders"], 2)

        # ── 4. Product images & names from dim_products (PG) ──
        nm_to_name: dict[int, str] = {}
        nm_to_image: dict[int, str] = {}
        nm_list = [p["sku"] for p in products.values() if p["sku"]]
        if nm_list:
            try:
                placeholders = ",".join(f":nm_{i}" for i in range(len(nm_list)))
                bind_params = {f"nm_{i}": nm for i, nm in enumerate(nm_list)}
                bind_params["sid"] = shop_id
                pg_products = await db.execute(
                    text(f"""
                        SELECT nm_id, COALESCE(name, ''), COALESCE(main_image_url, '')
                        FROM dim_products
                        WHERE shop_id = :sid AND nm_id IN ({placeholders})
                    """),
                    bind_params,
                )
                for row in pg_products.fetchall():
                    nm_to_name[row[0]] = row[1]
                    nm_to_image[row[0]] = row[2]
            except Exception as e:
                logger.warning("WB ABC/XYZ product names error: %s", e)

        # ── 5. Weekly revenue per nm_id (for XYZ) ──
        weekly_by_vc: dict[str, list[float]] = {vc: [] for vc in products}
        try:
            weekly_rows = ch.query("""
                SELECT
                    nm_id,
                    toStartOfWeek(toDate(date), 1) AS week,
                    sum(price_with_disc) AS revenue
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(date) >= {cur_start:Date}
                  AND toDate(date) <= {cur_end:Date}
                  AND is_cancel = 0
                GROUP BY nm_id, week
                ORDER BY nm_id, week
            """, parameters=params).result_rows

            all_weeks: set[str] = set()
            for row in weekly_rows:
                nm_id = int(row[0])
                vc = nm_to_vc.get(nm_id)
                w = str(row[1])
                all_weeks.add(w)
                if vc and vc in weekly_by_vc:
                    weekly_by_vc[vc].append(float(row[2]))

            num_weeks = max(len(all_weeks), 1)
            for vc in weekly_by_vc:
                while len(weekly_by_vc[vc]) < num_weeks:
                    weekly_by_vc[vc].append(0.0)
        except Exception as e:
            logger.warning("WB ABC/XYZ weekly data error: %s", e)

        # ── 6. Build product list with NET profit ──
        product_list = []
        for vc, p in products.items():
            sku = p["sku"]
            revenue = round(p["revenue"])
            commission = round(p["commission"])
            logistics = round(p["logistics"])
            storage = round(p["storage"])
            acquiring = round(p["acquiring"])
            penalties = round(p["penalties"])
            ad_spend = round(p["ad_spend"])
            cogs = round(p["cogs"])

            profit = revenue - commission - logistics - storage - acquiring - penalties - ad_spend - cogs
            mp_fees = commission + logistics + storage + acquiring + penalties

            avg_price = round(revenue / p["orders"]) if p["orders"] > 0 else 0

            product_list.append({
                "sku": sku,
                "offer_id": vc,
                "name": nm_to_name.get(sku, ""),
                "image_url": nm_to_image.get(sku, ""),
                "revenue": revenue,
                "profit": profit,
                "orders": p["orders"],
                "avg_price": avg_price,
                "cost_price": round(cost_map.get(vc.lower(), 0)),
                "commission": commission,
                "logistics": logistics,
                "storage": storage,
                "acquiring": acquiring,
                "mp_fees": mp_fees,
                "ad_spend": ad_spend,
                "cogs": cogs,
                "margin_pct": round(profit / revenue * 100, 1) if revenue > 0 else 0,
                "weekly_data": weekly_by_vc.get(vc, []),
            })

        # ── 7. Compute ABC ──
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

        # ── 8. Compute XYZ ──
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

        # ── 9. Summary & Matrix ──
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
        logger.exception("WB ABC/XYZ error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка ABC/XYZ анализа WB: {e}",
        )


# ═══════════════════════════════════════════════════════════════
# Ozon Sales Forecast + Simulator  (LightGBM)
# ═══════════════════════════════════════════════════════════════


def _lightgbm_forecast(
    dates: list[str],
    values: list[float],
    forecast_days: int,
    exog_daily: dict[str, dict[str, float]] | None = None,
) -> tuple[list[dict], dict, dict[str, float]]:
    """Run LightGBM forecast with lag features, rolling stats, seasonality,
    and exogenous variables (ad_spend, views, clicks, carts, active_sku).

    Args:
        dates: list of date strings
        values: target values (revenue or orders)
        forecast_days: how many days to forecast
        exog_daily: {date_str: {ad_spend, views, clicks, carts, active_sku}}

    Returns (forecast_points, trend_info, feature_importance).
    """
    import pandas as pd
    import numpy as np

    exog_daily = exog_daily or {}

    # ── Fallback for insufficient data ──
    if len(dates) < 14:
        mean_val = sum(values) / len(values) if values else 0
        last_dt = date.fromisoformat(dates[-1]) if dates else date.today()
        forecast_pts = []
        for d in range(1, forecast_days + 1):
            fd = last_dt + timedelta(days=d)
            forecast_pts.append({
                "date": str(fd),
                "value": round(mean_val),
                "value_low": round(mean_val * 0.7),
                "value_high": round(mean_val * 1.3),
            })
        return forecast_pts, {"slope_pct": 0, "direction": "flat"}, {}

    try:
        import lightgbm as lgb

        df = pd.DataFrame({"ds": pd.to_datetime(dates), "y": values})
        df = df.sort_values("ds").reset_index(drop=True)

        # ── Exogenous columns from daily data ──
        exog_cols = ["ad_spend", "views", "clicks", "carts", "active_sku"]
        for col in exog_cols:
            df[col] = df["ds"].apply(
                lambda d, c=col: exog_daily.get(str(d.date()), {}).get(c, 0)
            )

        # ── Feature engineering: target lags & rolling ──
        for lag in [1, 2, 3, 7, 14]:
            df[f"lag{lag}"] = df["y"].shift(lag)

        df["ma3"] = df["y"].rolling(3, min_periods=1).mean()
        df["ma7"] = df["y"].rolling(7, min_periods=1).mean()
        df["ma14"] = df["y"].rolling(14, min_periods=1).mean()
        df["std7"] = df["y"].rolling(7, min_periods=1).std().fillna(0)

        # ── Feature engineering: exogenous lags ──
        for col in exog_cols:
            df[f"{col}_lag1"] = df[col].shift(1)
            df[f"{col}_lag3"] = df[col].shift(3)
            df[f"{col}_ma7"] = df[col].rolling(7, min_periods=1).mean()

        # Derived: CTR, cart_rate
        df["ctr"] = (df["clicks"] / df["views"].replace(0, 1) * 100).fillna(0)
        df["cart_rate"] = (df["carts"] / df["clicks"].replace(0, 1) * 100).fillna(0)

        # Seasonality
        df["day_of_week"] = df["ds"].dt.dayofweek
        df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
        df["month"] = df["ds"].dt.month

        feature_cols = [
            # Target lags & rolling
            "lag1", "lag2", "lag3", "lag7", "lag14",
            "ma3", "ma7", "ma14", "std7",
            # Exogenous lags
            "ad_spend_lag1", "ad_spend_lag3", "ad_spend_ma7",
            "views_lag1", "views_lag3", "views_ma7",
            "clicks_lag1", "clicks_lag3", "clicks_ma7",
            "carts_lag1", "carts_lag3", "carts_ma7",
            "active_sku_lag1", "active_sku_lag3", "active_sku_ma7",
            # Derived
            "ctr", "cart_rate",
            # Seasonality
            "day_of_week", "is_weekend", "month",
        ]

        # Drop rows with NaN (from lags)
        df_train = df.dropna(subset=feature_cols).copy()

        if len(df_train) < 10:
            raise ValueError("Not enough training data after feature engineering")

        X = df_train[feature_cols].values
        y = df_train["y"].values

        model = lgb.LGBMRegressor(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            num_leaves=31,
            min_child_samples=5,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,
            reg_lambda=0.1,
            verbose=-1,
        )
        model.fit(X, y)

        # ── Feature importance ──
        importances = model.feature_importances_
        total_imp = sum(importances) or 1
        feat_importance = {
            col: round(imp / total_imp * 100, 1)
            for col, imp in sorted(
                zip(feature_cols, importances),
                key=lambda x: x[1],
                reverse=True,
            )
        }

        # ── Multi-step forecast ──
        last_dt = pd.Timestamp(dates[-1])
        recent_values = list(values[-14:])

        # Build recent exog arrays (last 14 days, index 0 = most recent)
        recent_exog: dict[str, list[float]] = {col: [] for col in exog_cols}
        for i in range(14):
            d_str = str(date.fromisoformat(dates[-1]) - timedelta(days=i))
            for col in exog_cols:
                recent_exog[col].append(exog_daily.get(d_str, {}).get(col, 0))

        forecast_pts = []
        residuals = y - model.predict(X)
        residual_std = float(np.std(residuals))

        for step in range(1, forecast_days + 1):
            fd = last_dt + pd.Timedelta(days=step)

            vals = recent_values
            features = {
                # Target lags
                "lag1": vals[-1] if len(vals) >= 1 else 0,
                "lag2": vals[-2] if len(vals) >= 2 else 0,
                "lag3": vals[-3] if len(vals) >= 3 else 0,
                "lag7": vals[-7] if len(vals) >= 7 else 0,
                "lag14": vals[-14] if len(vals) >= 14 else 0,
                "ma3": float(np.mean(vals[-3:])) if len(vals) >= 3 else float(np.mean(vals)),
                "ma7": float(np.mean(vals[-7:])) if len(vals) >= 7 else float(np.mean(vals)),
                "ma14": float(np.mean(vals[-14:])) if len(vals) >= 14 else float(np.mean(vals)),
                "std7": float(np.std(vals[-7:])) if len(vals) >= 7 else 0,
            }
            # Exogenous lags (use last known values for future)
            for col in exog_cols:
                arr = recent_exog[col]
                features[f"{col}_lag1"] = arr[0] if arr else 0
                features[f"{col}_lag3"] = arr[2] if len(arr) > 2 else (arr[0] if arr else 0)
                features[f"{col}_ma7"] = float(np.mean(arr[:7])) if arr else 0

            # Derived
            v = recent_exog["views"][0] if recent_exog["views"] else 1
            c = recent_exog["clicks"][0] if recent_exog["clicks"] else 0
            ct = recent_exog["carts"][0] if recent_exog["carts"] else 0
            features["ctr"] = round(c / max(v, 1) * 100, 2)
            features["cart_rate"] = round(ct / max(c, 1) * 100, 2)

            # Seasonality
            features["day_of_week"] = fd.dayofweek
            features["is_weekend"] = 1 if fd.dayofweek >= 5 else 0
            features["month"] = fd.month

            X_pred = np.array([[features[c] for c in feature_cols]])
            yhat = float(model.predict(X_pred)[0])
            yhat = max(yhat, 0)

            # Confidence interval widens with step
            confidence_mult = 1 + (step - 1) * 0.05
            band = residual_std * confidence_mult * 1.5

            forecast_pts.append({
                "date": str(fd.date()),
                "value": round(yhat),
                "value_low": round(max(yhat - band, 0)),
                "value_high": round(yhat + band),
            })

            # Update recent_values for next step (autoregressive)
            recent_values.append(yhat)

        # ── Trend ──
        if len(forecast_pts) >= 2:
            first_v = forecast_pts[0]["value"]
            last_v = forecast_pts[-1]["value"]
            mean_val = sum(values) / len(values) if values else 1
            slope_pct = round(
                (last_v - first_v) / max(mean_val, 1) / len(forecast_pts) * 100, 1
            )
        else:
            slope_pct = 0

        trend_info = {
            "slope_pct": slope_pct,
            "direction": "up" if slope_pct > 0.1 else "down" if slope_pct < -0.1 else "flat",
        }

        return forecast_pts, trend_info, feat_importance

    except Exception as e:
        logger.warning("LightGBM forecast failed, fallback to moving average: %s", e)
        window = values[-7:] if len(values) >= 7 else values
        mean_val = sum(window) / len(window) if window else 0
        last_dt = date.fromisoformat(dates[-1]) if dates else date.today()
        forecast_pts = []
        for d in range(1, forecast_days + 1):
            fd = last_dt + timedelta(days=d)
            forecast_pts.append({
                "date": str(fd),
                "value": round(mean_val),
                "value_low": round(mean_val * 0.7),
                "value_high": round(mean_val * 1.3),
            })
        return forecast_pts, {"slope_pct": 0, "direction": "flat"}, {}



@router.get("/ozon/forecast")
async def get_ozon_forecast(
    shop_id: int = Query(...),
    period: int = Query(120, ge=14, le=365),
    forecast_days: int = Query(30, ge=7, le=90),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Unified sales forecast for Ozon — bottom-up per-SKU approach.

    For each top-50 SKU:
    - LightGBM forecasts: revenue, orders, ad_spend
    - Full economics: commission, logistics, COGS, profit, margin, ROI
    - Rule-based recommendations

    Overall forecast = SUM(per-SKU forecasts).
    """
    result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == user.id)
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(status_code=404, detail="Магазин не найден")

    try:
        from app.core.clickhouse import get_clickhouse_client
        from app.api.v1.forecast_engine import generate_sku_recommendations
        import pandas as pd
        import numpy as np
        import asyncio

        ch = get_clickhouse_client()

        cur_end = date.today() - timedelta(days=1)  # exclude incomplete today
        cur_start = cur_end - timedelta(days=period - 1)

        params = {
            "shop_id": shop_id,
            "cur_start": str(cur_start),
            "cur_end": str(cur_end),
        }

        # ══════════════════════════════════════════════════════════
        # 1. COLLECT DATA
        # ══════════════════════════════════════════════════════════

        # ── 1a. Overall daily history (for chart) ──
        daily_rows = ch.query("""
            SELECT
                toDate(addHours(in_process_at, 3)) AS dt,
                sum(price * quantity) AS revenue,
                count() AS orders
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(addHours(in_process_at, 3)) >= {cur_start:Date}
              AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
            GROUP BY dt
            ORDER BY dt
        """, parameters=params).result_rows

        history = []
        for r in daily_rows:
            history.append({
                "date": str(r[0]),
                "revenue": round(float(r[1] or 0)),
                "orders": int(r[2] or 0),
            })

        # ── 1b. Top SKUs by revenue ──
        top_rows = ch.query("""
            SELECT sku, sum(price * quantity) AS revenue
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(addHours(in_process_at, 3)) >= {cur_start:Date}
              AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
            GROUP BY sku
            ORDER BY revenue DESC
            LIMIT 20
        """, parameters=params).result_rows
        sku_list = [int(r[0]) for r in top_rows]

        if not sku_list:
            return {
                "shop_id": shop_id, "period": period,
                "forecast_days": forecast_days,
                "history": history,
                "overall": {"forecast": [], "trend": {}, "totals": {}},
                "products": [],
            }

        sku_str = ",".join(str(s) for s in sku_list)

        # ── 1c. Daily orders per SKU ──
        orders_rows = ch.query(f"""
            SELECT
                sku,
                toDate(addHours(in_process_at, 3)) AS dt,
                sum(price * quantity) AS revenue,
                count() AS orders
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {{shop_id:UInt32}}
              AND toDate(addHours(in_process_at, 3)) >= {{cur_start:Date}}
              AND toDate(addHours(in_process_at, 3)) <= {{cur_end:Date}}
              AND sku IN ({sku_str})
            GROUP BY sku, dt
            ORDER BY sku, dt
        """, parameters=params).result_rows

        # ── 1d. Daily ad funnel per SKU ──
        funnel_rows = ch.query(f"""
            SELECT
                sku, dt,
                sum(views) AS views,
                sum(clicks) AS clicks,
                sum(add_to_cart) AS carts,
                sum(money_spent) AS ad_spend
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {{shop_id:UInt32}}
              AND dt >= {{cur_start:Date}}
              AND dt <= {{cur_end:Date}}
              AND sku IN ({sku_str})
            GROUP BY sku, dt
            ORDER BY sku, dt
        """, parameters=params).result_rows

        # ── 1e. Product info, COGS, commission rates, logistics ──
        sku_to_offer: dict[int, str] = {}
        sku_to_name: dict[int, str] = {}
        sku_to_image: dict[int, str] = {}
        cost_map: dict[str, float] = {}
        commission_rates: dict[int, float] = {}
        logistics_per_order: dict[int, float] = {}

        try:
            placeholders = ",".join(f":sku_{i}" for i in range(len(sku_list)))
            bind_params = {f"sku_{i}": s for i, s in enumerate(sku_list)}
            bind_params["sid"] = shop_id

            pg_products = await db.execute(
                text(f"""
                    SELECT sku, name, offer_id,
                           COALESCE(NULLIF(primary_image_url, ''), '') AS image_url
                    FROM dim_ozon_products
                    WHERE shop_id = :sid AND sku IN ({placeholders})
                """),
                bind_params,
            )
            for row in pg_products.fetchall():
                sku_to_name[row[0]] = row[1]
                sku_to_offer[row[0]] = row[2] or str(row[0])
                sku_to_image[row[0]] = row[3]

            cost_result = await db.execute(
                text("""
                    SELECT offer_id,
                           COALESCE(cost_price, 0) + COALESCE(packaging_cost, 0) AS total_cost
                    FROM product_costs
                    WHERE shop_id = :shop_id AND (cost_price > 0 OR packaging_cost > 0)
                """),
                {"shop_id": shop_id},
            )
            cost_map = {r[0].lower(): float(r[1]) for r in cost_result.fetchall()}
        except Exception as e:
            logger.warning("Forecast products/costs error: %s", e)

        # Commission rates per SKU from transactions
        try:
            comm_rows = ch.query(f"""
                SELECT
                    sku,
                    sum(abs(sale_commission)) / nullIf(sum(abs(accruals_for_sale)), 0) AS commission_rate
                FROM mms_analytics.fact_ozon_transactions FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND category = 'Revenue'
                  AND sku IN ({sku_str})
                  AND toDate(operation_date) >= {{cur_start:Date}}
                  AND toDate(operation_date) <= {{cur_end:Date}}
                GROUP BY sku
            """, parameters=params).result_rows
            for r in comm_rows:
                commission_rates[int(r[0])] = min(float(r[1] or 0), 1.0)
        except Exception:
            pass

        # Real logistics per order per SKU
        try:
            log_rows = ch.query(f"""
                SELECT
                    sku,
                    sum(abs(services_total)) AS total_logistics,
                    count() AS tx_count
                FROM mms_analytics.fact_ozon_transactions FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND category = 'Revenue'
                  AND sku IN ({sku_str})
                  AND toDate(operation_date) >= {{cur_start:Date}}
                  AND toDate(operation_date) <= {{cur_end:Date}}
                GROUP BY sku
            """, parameters=params).result_rows
            for r in log_rows:
                tx_count = int(r[2] or 1) or 1
                logistics_per_order[int(r[0])] = float(r[1] or 0) / tx_count
        except Exception:
            pass

        # ══════════════════════════════════════════════════════════
        # 2. BUILD PER-SKU DATA & RUN FORECASTS
        # ══════════════════════════════════════════════════════════

        all_dates = pd.date_range(cur_start, cur_end, freq="D")

        # Index data by SKU
        sku_orders: dict[int, dict[str, dict]] = {}
        for r in orders_rows:
            s = int(r[0])
            sku_orders.setdefault(s, {})[str(r[1])] = {
                "revenue": float(r[2] or 0), "orders": int(r[3] or 0),
            }

        sku_funnel: dict[int, dict[str, dict]] = {}
        for r in funnel_rows:
            s = int(r[0])
            sku_funnel.setdefault(s, {})[str(r[1])] = {
                "views": int(r[2] or 0), "clicks": int(r[3] or 0),
                "carts": int(r[4] or 0), "ad_spend": float(r[5] or 0),
            }

        products_list = []
        # For bottom-up aggregation: {date_str: {revenue, orders, profit, ad_spend}}
        overall_by_day: dict[str, dict[str, float]] = {}

        loop = asyncio.get_event_loop()

        for s in sku_list:
            oid = sku_to_offer.get(s, str(s))
            cogs_unit = cost_map.get(oid.lower(), 0)
            comm_rate = commission_rates.get(s, 0.15)
            log_per_ord = logistics_per_order.get(s, 100)

            # Build daily data for this SKU
            daily_data = []
            total_hist_rev = 0.0
            total_hist_orders = 0
            total_hist_ad_spend = 0.0
            total_hist_views = 0
            total_hist_clicks = 0
            total_hist_carts = 0

            for dt in all_dates:
                dt_str = str(dt.date())
                o = sku_orders.get(s, {}).get(dt_str, {"revenue": 0, "orders": 0})
                f = sku_funnel.get(s, {}).get(dt_str, {"views": 0, "clicks": 0, "carts": 0, "ad_spend": 0})
                daily_data.append({
                    "ds": dt_str,
                    "orders": o["orders"],
                    "revenue": o["revenue"],
                    "views": f["views"],
                    "clicks": f["clicks"],
                    "carts": f["carts"],
                    "ad_spend": f["ad_spend"],
                })
                total_hist_rev += o["revenue"]
                total_hist_orders += o["orders"]
                total_hist_ad_spend += f["ad_spend"]
                total_hist_views += f["views"]
                total_hist_clicks += f["clicks"]
                total_hist_carts += f["carts"]

            # Run per-SKU forecast (in thread)
            forecast_pts, trend_info, feat_imp = await loop.run_in_executor(
                None,
                lambda dd=daily_data, fd=forecast_days: _lightgbm_sku_forecast(dd, fd),
            )

            # ── Calculate full economics for each forecast day ──
            # Estimate future ad_spend from recent average
            recent_7d_ads = [
                sku_funnel.get(s, {}).get(str(d.date()), {}).get("ad_spend", 0)
                for d in all_dates[-7:]
            ]
            avg_daily_ad = float(np.mean(recent_7d_ads)) if recent_7d_ads else 0

            total_fc_rev = 0.0
            total_fc_orders = 0
            total_fc_profit = 0.0
            total_fc_ad = 0.0

            for pt in forecast_pts:
                rev = pt["revenue"]
                ords = pt["orders"]
                ad_est = round(avg_daily_ad)
                commission = round(rev * comm_rate)
                logistics = round(ords * log_per_ord)
                cogs = round(cogs_unit * ords)
                pft = round(rev - commission - logistics - ad_est - cogs)
                mgn = round(pft / rev * 100, 1) if rev > 0 else 0

                pt["ad_spend"] = ad_est
                pt["commission"] = commission
                pt["logistics"] = logistics
                pt["cogs"] = cogs
                pt["profit"] = pft
                pt["margin_pct"] = mgn

                total_fc_rev += rev
                total_fc_orders += ords
                total_fc_profit += pft
                total_fc_ad += ad_est

                # Bottom-up aggregation
                d = pt["date"]
                overall_by_day.setdefault(d, {"revenue": 0, "orders": 0, "profit": 0, "ad_spend": 0})
                overall_by_day[d]["revenue"] += rev
                overall_by_day[d]["orders"] += ords
                overall_by_day[d]["profit"] += pft
                overall_by_day[d]["ad_spend"] += ad_est

            # ── Historical economics for recommendations ──
            hist_commission = round(total_hist_rev * comm_rate)
            hist_logistics = round(total_hist_orders * log_per_ord)
            hist_cogs = round(cogs_unit * total_hist_orders)
            hist_profit = round(total_hist_rev - hist_commission - hist_logistics - total_hist_ad_spend - hist_cogs)
            hist_margin = round(hist_profit / total_hist_rev * 100, 1) if total_hist_rev > 0 else 0
            hist_roi = round((total_hist_rev - total_hist_ad_spend) / total_hist_ad_spend * 100, 1) if total_hist_ad_spend > 0 else 0
            hist_ctr = round(total_hist_clicks / max(total_hist_views, 1) * 100, 2)
            hist_cart_rate = round(total_hist_carts / max(total_hist_clicks, 1) * 100, 2)

            fc_margin = round(total_fc_profit / total_fc_rev * 100, 1) if total_fc_rev > 0 else 0

            # Revenue trend %
            rev_first_half = sum(d["revenue"] for d in daily_data[:len(daily_data)//2])
            rev_second_half = sum(d["revenue"] for d in daily_data[len(daily_data)//2:])
            rev_trend_pct = round((rev_second_half - rev_first_half) / max(rev_first_half, 1) * 100, 1)

            ord_first_half = sum(d["orders"] for d in daily_data[:len(daily_data)//2])
            ord_second_half = sum(d["orders"] for d in daily_data[len(daily_data)//2:])
            ord_trend_pct = round((ord_second_half - ord_first_half) / max(ord_first_half, 1) * 100, 1)

            # ── Generate recommendations ──
            avg_price = round(total_hist_rev / total_hist_orders) if total_hist_orders > 0 else 0

            analysis = generate_sku_recommendations(
                revenue=total_hist_rev,
                orders=total_hist_orders,
                ad_spend=total_hist_ad_spend,
                commission=hist_commission,
                logistics=hist_logistics,
                cogs=hist_cogs,
                profit=hist_profit,
                margin_pct=hist_margin,
                roi=hist_roi,
                ctr=hist_ctr,
                cart_rate=hist_cart_rate,
                avg_price=avg_price,
                forecast_revenue=total_fc_rev,
                forecast_orders=total_fc_orders,
                forecast_profit=total_fc_profit,
                forecast_margin_pct=fc_margin,
                forecast_ad_spend=total_fc_ad,
                revenue_trend_pct=rev_trend_pct,
                orders_trend_pct=ord_trend_pct,
                period_days=period,
                forecast_days=forecast_days,
            )

            products_list.append({
                "sku": s,
                "offer_id": oid,
                "name": sku_to_name.get(s, ""),
                "image_url": sku_to_image.get(s, ""),
                # Historical totals
                "history_totals": {
                    "revenue": round(total_hist_rev),
                    "orders": total_hist_orders,
                    "ad_spend": round(total_hist_ad_spend),
                    "profit": hist_profit,
                    "margin_pct": hist_margin,
                    "roi": hist_roi,
                    "avg_price": avg_price,
                    "ctr": hist_ctr,
                    "cart_rate": hist_cart_rate,
                },
                # Forecast
                "forecast": forecast_pts,
                "trend": trend_info,
                "forecast_totals": {
                    "revenue": round(total_fc_rev),
                    "orders": total_fc_orders,
                    "ad_spend": round(total_fc_ad),
                    "profit": round(total_fc_profit),
                    "margin_pct": fc_margin,
                },
                "analysis": analysis,
                "feature_importance": feat_imp,
            })

        # Sort by historical revenue desc
        products_list.sort(key=lambda x: x["history_totals"]["revenue"], reverse=True)

        # ══════════════════════════════════════════════════════════
        # 3. BOTTOM-UP OVERALL FORECAST
        # ══════════════════════════════════════════════════════════

        overall_forecast = []
        for d in sorted(overall_by_day.keys()):
            v = overall_by_day[d]
            overall_forecast.append({
                "date": d,
                "revenue": round(v["revenue"]),
                "orders": round(v["orders"]),
                "profit": round(v["profit"]),
                "ad_spend": round(v["ad_spend"]),
            })

        overall_totals = {
            "revenue": sum(f["revenue"] for f in overall_forecast),
            "orders": sum(f["orders"] for f in overall_forecast),
            "profit": sum(f["profit"] for f in overall_forecast),
            "ad_spend": sum(f["ad_spend"] for f in overall_forecast),
        }
        overall_totals["margin_pct"] = (
            round(overall_totals["profit"] / overall_totals["revenue"] * 100, 1)
            if overall_totals["revenue"] > 0 else 0
        )

        # Overall trend
        if len(overall_forecast) >= 2:
            first_rev = overall_forecast[0]["revenue"]
            last_rev = overall_forecast[-1]["revenue"]
            avg_rev = sum(f["revenue"] for f in overall_forecast) / len(overall_forecast) or 1
            slope = round((last_rev - first_rev) / avg_rev / len(overall_forecast) * 100, 1)
        else:
            slope = 0

        overall_trend = {
            "revenue_slope_pct": slope,
            "direction": "up" if slope > 0.1 else "down" if slope < -0.1 else "flat",
        }

        # Count severity by type
        rec_summary = {"critical": 0, "warning": 0, "opportunity": 0, "ok": 0}
        for p in products_list:
            sev = p.get("analysis", {}).get("severity", "ok")
            rec_summary[sev] = rec_summary.get(sev, 0) + 1

        return {
            "shop_id": shop_id,
            "period": period,
            "forecast_days": forecast_days,
            "history": history,
            "overall": {
                "forecast": overall_forecast,
                "trend": overall_trend,
                "totals": overall_totals,
            },
            "recommendation_summary": rec_summary,
            "products": products_list,
        }

    except Exception as e:
        logger.exception("Ozon forecast error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка прогноза: {e}",
        )


# ═══════════════════════════════════════════════════════════════
# LightGBM Per-SKU Forecast
# ═══════════════════════════════════════════════════════════════


def _build_sku_features(df):
    """Build lagged features from daily SKU data for LightGBM."""
    import pandas as pd

    df = df.sort_values("ds").copy()

    # Lag features — orders
    for lag in [1, 2, 3, 7]:
        df[f"orders_lag{lag}"] = df["orders"].shift(lag)
    # Moving averages
    df["orders_ma7"] = df["orders"].rolling(7, min_periods=1).mean()
    df["orders_ma3"] = df["orders"].rolling(3, min_periods=1).mean()

    # Lag features — revenue
    for lag in [1, 7]:
        df[f"revenue_lag{lag}"] = df["revenue"].shift(lag)

    # Lag features — funnel
    for lag in [1, 2, 3]:
        df[f"views_lag{lag}"] = df["views"].shift(lag)
    for lag in [1, 2]:
        df[f"clicks_lag{lag}"] = df["clicks"].shift(lag)
    for lag in [1, 2]:
        df[f"carts_lag{lag}"] = df["carts"].shift(lag)
    for lag in [1, 2]:
        df[f"ad_spend_lag{lag}"] = df["ad_spend"].shift(lag)

    # Calendar features
    df["day_of_week"] = df["ds"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    # Conversion ratios (lagged)
    df["ctr_lag1"] = (df["clicks_lag1"] / df["views_lag1"].replace(0, 1)).fillna(0)
    df["cart_rate_lag1"] = (df["carts_lag1"] / df["clicks_lag1"].replace(0, 1)).fillna(0)

    return df


FEATURE_COLS = [
    "orders_lag1", "orders_lag2", "orders_lag3", "orders_lag7",
    "orders_ma7", "orders_ma3",
    "revenue_lag1", "revenue_lag7",
    "views_lag1", "views_lag2", "views_lag3",
    "clicks_lag1", "clicks_lag2",
    "carts_lag1", "carts_lag2",
    "ad_spend_lag1", "ad_spend_lag2",
    "day_of_week", "is_weekend",
    "ctr_lag1", "cart_rate_lag1",
]


def _lightgbm_sku_forecast(
    daily_data: list[dict],
    forecast_days: int = 14,
) -> tuple[list[dict], dict, dict]:
    """Per-SKU forecast using LightGBM with funnel lagged features.

    Optimized: numpy-based prediction loop, reduced estimators.
    """
    import pandas as pd
    import numpy as np

    df = pd.DataFrame(daily_data)
    df["ds"] = pd.to_datetime(df["ds"])

    if len(df) < 14:
        mean_orders = df["orders"].mean() if len(df) > 0 else 0
        mean_revenue = df["revenue"].mean() if len(df) > 0 else 0
        last_dt = df["ds"].max() if len(df) > 0 else pd.Timestamp.today()
        pts = []
        for d in range(1, forecast_days + 1):
            fd = last_dt + pd.Timedelta(days=d)
            pts.append({
                "date": str(fd.date()),
                "orders": round(mean_orders),
                "revenue": round(mean_revenue),
                "orders_low": round(mean_orders * 0.7),
                "orders_high": round(mean_orders * 1.3),
            })
        return pts, {"slope_pct": 0, "direction": "flat"}, {}

    try:
        import lightgbm as lgb

        # Build features
        feat_df = _build_sku_features(df)
        train_df = feat_df.dropna(subset=["orders_lag7"]).copy()

        if len(train_df) < 7:
            raise ValueError("Not enough training data after lag creation")

        X_train = train_df[FEATURE_COLS].fillna(0).values
        y_orders = train_df["orders"].values

        # Train LightGBM — orders only (revenue derived from avg_price)
        model_ord = lgb.LGBMRegressor(
            objective="regression",
            n_estimators=50,
            num_leaves=31,
            learning_rate=0.08,
            min_child_samples=3,
            subsample=0.8,
            colsample_bytree=0.8,
            verbose=-1,
        )
        model_ord.fit(X_train, y_orders)

        # Feature importance
        importances = model_ord.feature_importances_
        feat_imp = {
            FEATURE_COLS[i]: round(float(importances[i]) / max(importances.sum(), 1) * 100, 1)
            for i in range(len(FEATURE_COLS))
            if importances[i] > 0
        }
        feat_imp = dict(sorted(feat_imp.items(), key=lambda x: -x[1]))

        # Average price per order (for revenue derivation)
        recent_mask = df["orders"] > 0
        if recent_mask.sum() > 0:
            avg_price_per_order = float(
                df.loc[recent_mask, "revenue"].tail(30).sum()
                / max(df.loc[recent_mask, "orders"].tail(30).sum(), 1)
            )
        else:
            avg_price_per_order = float(df["revenue"].sum() / max(df["orders"].sum(), 1))

        # ── Multi-step iterative forecast (numpy-based for speed) ──
        col_idx = {c: i for i, c in enumerate(FEATURE_COLS)}

        # Extract last row as numpy array
        last_feat = feat_df[FEATURE_COLS].fillna(0).iloc[-1].values.astype(np.float64)

        # Ring buffers for lag7 (need to track 7 past values)
        orders_history = df["orders"].tail(14).tolist()
        revenue_history = df["revenue"].tail(14).tolist()
        std_orders = float(np.std(orders_history)) if len(orders_history) > 1 else 1

        # Historical daily average (used as floor for predictions)
        avg_daily_orders = float(df["orders"].mean())

        # Stable averages for funnel features (we don't predict them)
        avg_views = float(df["views"].tail(14).mean()) if "views" in df else 0
        avg_clicks = float(df["clicks"].tail(14).mean()) if "clicks" in df else 0
        avg_carts = float(df["carts"].tail(14).mean()) if "carts" in df else 0
        avg_ad_spend = float(df["ad_spend"].tail(14).mean()) if "ad_spend" in df else 0

        forecast_pts = []
        feat_row = np.copy(last_feat)

        for step in range(1, forecast_days + 1):
            next_date = df["ds"].max() + pd.Timedelta(days=step)

            # Update calendar features
            feat_row[col_idx["day_of_week"]] = next_date.dayofweek
            feat_row[col_idx["is_weekend"]] = 1.0 if next_date.dayofweek >= 5 else 0.0

            # Keep funnel features stable (use recent averages)
            feat_row[col_idx["views_lag1"]] = avg_views
            feat_row[col_idx["views_lag2"]] = avg_views
            feat_row[col_idx["views_lag3"]] = avg_views
            feat_row[col_idx["clicks_lag1"]] = avg_clicks
            feat_row[col_idx["clicks_lag2"]] = avg_clicks
            feat_row[col_idx["carts_lag1"]] = avg_carts
            feat_row[col_idx["carts_lag2"]] = avg_carts
            feat_row[col_idx["ad_spend_lag1"]] = avg_ad_spend
            feat_row[col_idx["ad_spend_lag2"]] = avg_ad_spend
            feat_row[col_idx["ctr_lag1"]] = round(avg_clicks / max(avg_views, 1) * 100, 2)
            feat_row[col_idx["cart_rate_lag1"]] = round(avg_carts / max(avg_clicks, 1) * 100, 2)

            # Predict ORDERS only
            X_pred = feat_row.reshape(1, -1)
            raw_pred = max(float(model_ord.predict(X_pred)[0]), 0)

            # Floor: if prediction is below 30% of historical avg,
            # blend with historical average to avoid unrealistic near-zero forecasts
            floor_threshold = avg_daily_orders * 0.3
            if raw_pred < floor_threshold and avg_daily_orders > 0:
                # Blend: 30% model + 70% historical avg
                pred_orders = raw_pred * 0.3 + avg_daily_orders * 0.7
            else:
                pred_orders = raw_pred

            # Derive REVENUE from orders × avg_price (stable, no cascading error)
            pred_revenue = round(pred_orders * avg_price_per_order)

            # Confidence band
            band = std_orders * (1 + (step - 1) * 0.05)

            forecast_pts.append({
                "date": str(next_date.date()),
                "orders": round(pred_orders),
                "revenue": pred_revenue,
                "orders_low": round(max(pred_orders - band, 0)),
                "orders_high": round(pred_orders + band),
            })

            # Update orders/revenue history
            orders_history.append(pred_orders)
            revenue_history.append(pred_revenue)

            # Orders lags from history buffer (correct temporal distance)
            feat_row[col_idx["orders_lag1"]] = orders_history[-1]
            feat_row[col_idx["orders_lag2"]] = orders_history[-2] if len(orders_history) >= 2 else 0
            feat_row[col_idx["orders_lag3"]] = orders_history[-3] if len(orders_history) >= 3 else 0
            feat_row[col_idx["orders_lag7"]] = orders_history[-7] if len(orders_history) >= 7 else orders_history[0]

            # Revenue lags from history buffer
            feat_row[col_idx["revenue_lag1"]] = revenue_history[-1]
            feat_row[col_idx["revenue_lag7"]] = revenue_history[-7] if len(revenue_history) >= 7 else revenue_history[0]

            # Update MA from full recent window
            feat_row[col_idx["orders_ma7"]] = float(np.mean(orders_history[-7:]))
            feat_row[col_idx["orders_ma3"]] = float(np.mean(orders_history[-3:]))

        # Trend
        if len(forecast_pts) >= 2:
            first_v = forecast_pts[0]["orders"]
            last_v = forecast_pts[-1]["orders"]
            avg = df["orders"].mean() or 1
            slope_pct = round((last_v - first_v) / avg / len(forecast_pts) * 100, 1)
        else:
            slope_pct = 0

        trend_info = {
            "slope_pct": slope_pct,
            "direction": "up" if slope_pct > 0.1 else "down" if slope_pct < -0.1 else "flat",
        }

        return forecast_pts, trend_info, feat_imp

    except Exception as e:
        logger.warning("LightGBM SKU forecast failed: %s", e)
        mean_orders = df["orders"].mean() if len(df) > 0 else 0
        mean_revenue = df["revenue"].mean() if len(df) > 0 else 0
        last_dt = df["ds"].max() if len(df) > 0 else pd.Timestamp.today()
        pts = []
        for d in range(1, forecast_days + 1):
            fd = last_dt + pd.Timedelta(days=d)
            pts.append({
                "date": str(fd.date()),
                "orders": round(mean_orders),
                "revenue": round(mean_revenue),
                "orders_low": round(mean_orders * 0.7),
                "orders_high": round(mean_orders * 1.3),
            })
        return pts, {"slope_pct": 0, "direction": "flat"}, {}


@router.get("/ozon/forecast/sku")
async def get_ozon_sku_forecast(
    shop_id: int = Query(...),
    period: int = Query(120, ge=30, le=365),
    forecast_days: int = Query(30, ge=7, le=90),
    sku: Optional[int] = Query(None, description="Specific SKU, or top-20 by revenue"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Per-SKU forecast using LightGBM with funnel lagged features.

    Predicts orders, revenue, profit per SKU using:
    views, clicks, add_to_cart, ad_spend (lagged 1-3 days).
    """
    try:
        result = await db.execute(
            select(Shop).where(Shop.id == shop_id, Shop.user_id == user.id)
        )
        shop = result.scalar_one_or_none()
        if not shop:
            raise HTTPException(status_code=404, detail="Магазин не найден")

        from app.core.clickhouse import get_clickhouse_client
        ch = get_clickhouse_client()

        cur_end = date.today() - timedelta(days=1)
        cur_start = cur_end - timedelta(days=period - 1)

        params = {
            "shop_id": shop_id,
            "cur_start": str(cur_start),
            "cur_end": str(cur_end),
        }

        # ── 1. Get top SKUs by revenue ──
        if sku:
            sku_list = [sku]
        else:
            top_rows = ch.query("""
                SELECT sku, sum(price * quantity) AS revenue
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(in_process_at, 3)) >= {cur_start:Date}
                  AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
                GROUP BY sku
                ORDER BY revenue DESC
                LIMIT 5
            """, parameters=params).result_rows
            sku_list = [int(r[0]) for r in top_rows]

        if not sku_list:
            return {"shop_id": shop_id, "sku_forecasts": []}

        sku_str = ",".join(str(s) for s in sku_list)

        # ── 2. Daily orders per SKU ──
        orders_rows = ch.query(f"""
            SELECT
                sku,
                toDate(addHours(in_process_at, 3)) AS dt,
                sum(price * quantity) AS revenue,
                count() AS orders
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {{shop_id:UInt32}}
              AND toDate(addHours(in_process_at, 3)) >= {{cur_start:Date}}
              AND toDate(addHours(in_process_at, 3)) <= {{cur_end:Date}}
              AND sku IN ({sku_str})
            GROUP BY sku, dt
            ORDER BY sku, dt
        """, parameters=params).result_rows

        # ── 3. Daily ad funnel per SKU ──
        funnel_rows = ch.query(f"""
            SELECT
                sku,
                dt,
                sum(views) AS views,
                sum(clicks) AS clicks,
                sum(add_to_cart) AS carts,
                sum(money_spent) AS ad_spend
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {{shop_id:UInt32}}
              AND dt >= {{cur_start:Date}}
              AND dt <= {{cur_end:Date}}
              AND sku IN ({sku_str})
            GROUP BY sku, dt
            ORDER BY sku, dt
        """, parameters=params).result_rows

        # ── 4. Build per-SKU daily DataFrames ──
        import pandas as pd

        # Generate all dates in range
        all_dates = pd.date_range(cur_start, cur_end, freq="D")

        # Orders per sku per day
        sku_orders: dict[int, dict[str, dict]] = {}
        for r in orders_rows:
            s = int(r[0])
            dt_str = str(r[1])
            if s not in sku_orders:
                sku_orders[s] = {}
            sku_orders[s][dt_str] = {"revenue": float(r[2] or 0), "orders": int(r[3] or 0)}

        # Funnel per sku per day
        sku_funnel: dict[int, dict[str, dict]] = {}
        for r in funnel_rows:
            s = int(r[0])
            dt_str = str(r[1])
            if s not in sku_funnel:
                sku_funnel[s] = {}
            sku_funnel[s][dt_str] = {
                "views": int(r[2] or 0),
                "clicks": int(r[3] or 0),
                "carts": int(r[4] or 0),
                "ad_spend": float(r[5] or 0),
            }

        # ── 5. COGS + product info ──
        cost_map: dict[str, float] = {}
        sku_to_offer: dict[int, str] = {}
        sku_to_name: dict[int, str] = {}
        sku_to_image: dict[int, str] = {}
        try:
            placeholders = ",".join(f":sku_{i}" for i in range(len(sku_list)))
            bind_params = {f"sku_{i}": s for i, s in enumerate(sku_list)}
            bind_params["sid"] = shop_id
            pg_products = await db.execute(
                text(f"""
                    SELECT sku, name, offer_id,
                           COALESCE(NULLIF(primary_image_url, ''), '') AS image_url
                    FROM dim_ozon_products
                    WHERE shop_id = :sid AND sku IN ({placeholders})
                """),
                bind_params,
            )
            for row in pg_products.fetchall():
                sku_to_name[row[0]] = row[1]
                sku_to_offer[row[0]] = row[2] or str(row[0])
                sku_to_image[row[0]] = row[3]

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
            logger.warning("SKU forecast product info error: %s", e)

        # ── 6. Commission rates per SKU ──
        commission_rates: dict[int, float] = {}
        try:
            comm_rows = ch.query(f"""
                SELECT
                    sku,
                    sum(abs(sale_commission)) / nullIf(sum(abs(accruals_for_sale)), 0) AS commission_rate
                FROM mms_analytics.fact_ozon_transactions FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND category = 'Revenue'
                  AND sku IN ({sku_str})
                  AND toDate(operation_date) >= {{cur_start:Date}}
                  AND toDate(operation_date) <= {{cur_end:Date}}
                GROUP BY sku
            """, parameters=params).result_rows
            for r in comm_rows:
                commission_rates[int(r[0])] = min(float(r[1] or 0), 1.0)
        except Exception:
            pass

        # ── 7. Run LightGBM per SKU ──
        import asyncio

        sku_forecasts = []

        for s in sku_list:
            # Build daily data for this SKU
            daily_data = []
            for dt in all_dates:
                dt_str = str(dt.date())
                o_data = sku_orders.get(s, {}).get(dt_str, {"revenue": 0, "orders": 0})
                f_data = sku_funnel.get(s, {}).get(dt_str, {"views": 0, "clicks": 0, "carts": 0, "ad_spend": 0})
                daily_data.append({
                    "ds": dt_str,
                    "orders": o_data["orders"],
                    "revenue": o_data["revenue"],
                    "views": f_data["views"],
                    "clicks": f_data["clicks"],
                    "carts": f_data["carts"],
                    "ad_spend": f_data["ad_spend"],
                })

            # Run forecast
            forecast_pts, trend_info, feat_imp = _lightgbm_sku_forecast(
                daily_data, forecast_days
            )

            # Calculate profit for forecast
            oid = sku_to_offer.get(s, str(s))
            cogs_unit = cost_map.get(oid.lower(), 0)
            comm_rate = commission_rates.get(s, 0.15)  # default 15%

            for pt in forecast_pts:
                rev = pt["revenue"]
                ords = pt["orders"]
                commission = round(rev * comm_rate)
                logistics_per_order = 100  # rough estimate
                logistics = round(ords * logistics_per_order)
                cogs = round(cogs_unit * ords)
                # Use recent avg ad spend for future
                recent_ad = sum(
                    sku_funnel.get(s, {}).get(str(d.date()), {}).get("ad_spend", 0)
                    for d in all_dates[-7:]
                ) / 7
                ad_est = round(recent_ad)
                pt["profit"] = round(rev - commission - logistics - ad_est - cogs)
                pt["commission"] = commission
                pt["logistics"] = logistics
                pt["ad_spend_est"] = ad_est
                pt["cogs"] = cogs

            # History for chart (last 30 days)
            history = daily_data[-30:]

            total_forecast_orders = sum(p["orders"] for p in forecast_pts)
            total_forecast_revenue = sum(p["revenue"] for p in forecast_pts)
            total_forecast_profit = sum(p["profit"] for p in forecast_pts)

            sku_forecasts.append({
                "sku": s,
                "offer_id": oid,
                "name": sku_to_name.get(s, ""),
                "image_url": sku_to_image.get(s, ""),
                "history": history,
                "forecast": forecast_pts,
                "trend": trend_info,
                "feature_importance": feat_imp,
                "totals": {
                    "orders": total_forecast_orders,
                    "revenue": total_forecast_revenue,
                    "profit": total_forecast_profit,
                },
            })

        return {
            "shop_id": shop_id,
            "period": period,
            "forecast_days": forecast_days,
            "sku_forecasts": sku_forecasts,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Ozon SKU forecast error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка SKU прогноза: {e}",
        )
