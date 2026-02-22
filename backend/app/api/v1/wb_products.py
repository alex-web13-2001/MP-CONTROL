"""
WB Products API endpoints.

GET  /products/wb?shop_id=X&page=1&per_page=25&sort=revenue_7d&order=desc&filter=all&search=&period=7
PATCH /products/wb/cost  — update cost price for a WB product (by vendor_code)
POST /products/wb/cost/bulk — bulk upload cost prices via Excel
GET  /products/wb/cost/template — download Excel template
"""
import io
import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User
from sqlalchemy import select

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/products", tags=["WB Products"])


class WBCostUpdateRequest(BaseModel):
    shop_id: int
    vendor_code: str
    cost_price: float
    packaging_cost: float = 0


# ── WB Products List ──────────────────────────────────────

@router.get("/wb")
async def get_wb_products(
    shop_id: int = Query(...),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=10, le=100),
    sort: str = Query("revenue_7d"),
    order: str = Query("desc"),
    filter: str = Query("all"),
    search: str = Query(""),
    period: int = Query(7, ge=7, le=30),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get WB products list with sales, advertising, stock and margin data.
    """
    # Verify shop ownership
    shop_result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == current_user.id,
            Shop.marketplace == "wildberries",
        )
    )
    shop = shop_result.scalar_one_or_none()
    if not shop:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="WB магазин не найден")

    # ── Dates ─────────────────────────────────────────────
    today = date.today()
    d_end = today
    d_start = today - timedelta(days=period - 1)
    d_prev_end = d_start - timedelta(days=1)
    d_prev_start = d_prev_end - timedelta(days=period - 1)

    from app.core.clickhouse import get_clickhouse_client
    ch = get_clickhouse_client()

    # ── 1. Orders (current + prev period) ─────────────────
    try:
        orders_result = ch.query("""
            SELECT
                nm_id,
                any(supplier_article)  AS vendor_code,
                countIf(toDate(date) >= {d_start:Date})  AS orders_cur,
                sumIf(price_with_disc, toDate(date) >= {d_start:Date})  AS revenue_cur,
                countIf(toDate(date) < {d_start:Date})   AS orders_prev,
                sumIf(price_with_disc, toDate(date) < {d_start:Date})   AS revenue_prev
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(date) >= {d_prev_start:Date}
              AND toDate(date) <= {d_end:Date}
              AND is_cancel = 0
            GROUP BY nm_id
        """, parameters={
            "shop_id": shop_id,
            "d_start": d_start,
            "d_end": d_end,
            "d_prev_start": d_prev_start,
        })
        orders_map = {}
        for r in orders_result.result_rows:
            nm_id = int(r[0])
            orders_map[nm_id] = {
                "vendor_code": str(r[1] or ""),
                "orders_7d": int(r[2]),
                "revenue_7d": float(r[3]),
                "orders_prev": int(r[4]),
                "revenue_prev": float(r[5]),
            }
    except Exception as e:
        logger.warning("CH orders query failed: %s", e)
        orders_map = {}

    # ── 2. Ads (current period) ────────────────────────────
    try:
        ads_result = ch.query("""
            SELECT
                nm_id,
                sum(spend)  AS ad_spend,
                sum(views)  AS views,
                sum(clicks) AS clicks
            FROM mms_analytics.fact_advert_stats_v3 FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {d_start:Date}
              AND date <= {d_end:Date}
            GROUP BY nm_id
        """, parameters={"shop_id": shop_id, "d_start": d_start, "d_end": d_end})
        ads_map = {}
        for r in ads_result.result_rows:
            ads_map[int(r[0])] = {
                "ad_spend_7d": float(r[1]),
                "ad_views": int(r[2]),
                "ad_clicks": int(r[3]),
            }
    except Exception as e:
        logger.warning("CH ads query failed: %s", e)
        ads_map = {}

    # ── 3. Stocks (latest snapshot) ────────────────────────
    try:
        stocks_result = ch.query("""
            SELECT
                nm_id,
                sumIf(quantity, NOT startsWith(warehouse_name, 'FBS:')) AS stock_fbo,
                sumIf(quantity, startsWith(warehouse_name, 'FBS:'))     AS stock_fbs
            FROM mms_analytics.fact_inventory_snapshot FINAL
            WHERE shop_id = {shop_id:UInt32}
            GROUP BY nm_id
        """, parameters={"shop_id": shop_id})
        stocks_map = {}
        for r in stocks_result.result_rows:
            stocks_map[int(r[0])] = {
                "stock_fbo": int(r[1]),
                "stock_fbs": int(r[2]),
            }
    except Exception as e:
        logger.warning("CH stocks query failed: %s", e)
        stocks_map = {}

    # ── 4. WB Finance fees from fact_finances ─────────────────────────
    # fact_finances FINAL — исходные данные финотчёта WB.
    # По vendor_code (нижний регистр), за последние 90 дней.
    # Ставку комиссий применяем к выручке текущего периода.
    try:
        fees_result = ch.query("""
            SELECT
                lower(vendor_code)                          AS vc,
                sum(retail_amount)                          AS fin_revenue,
                sum(commission_amount)                      AS commission,
                sum(logistics_total + wb_delivery_rub)      AS logistics,
                sum(storage_fee + wb_storage_amount)        AS storage,
                sum(acceptance_fee)                         AS acceptance,
                sum(penalty_total)                          AS fines,
                sum(wb_acquiring)                           AS acquiring
            FROM mms_analytics.fact_finances FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND marketplace = 1
              AND event_date >= toDate(now()) - 90
            GROUP BY vc
            HAVING commission > 0 OR logistics > 0 OR storage > 0 OR acquiring > 0
        """, parameters={"shop_id": shop_id})
        fees_map = {}
        for r in fees_result.result_rows:
            vc = str(r[0]).lower()
            fees_map[vc] = {
                "fin_revenue": float(r[1]),
                "commission": float(r[2]),
                "logistics": float(r[3]),
                "storage": float(r[4]),
                "acceptance": float(r[5]),
                "fines": float(r[6]),
                "acquiring": float(r[7]),
            }
    except Exception as e:
        logger.warning("CH fees query failed: %s", e)
        fees_map = {}

    ch.close()

    # ── 4. Product catalog from PostgreSQL (dim_products) ──
    pg_result = await db.execute(
        text("""
            SELECT dp.nm_id,
                   dp.name,
                   COALESCE(dp.main_image_url, '') AS image_url,
                   COALESCE(dp.current_price, 0)   AS current_price,
                   COALESCE(dp.vendor_code, '')     AS vendor_code,
                   COALESCE(pc.cost_price, 0)       AS cost_price,
                   COALESCE(pc.packaging_cost, 0)   AS packaging_cost
            FROM dim_products dp
            LEFT JOIN product_costs pc
                ON pc.shop_id = dp.shop_id AND pc.offer_id = dp.vendor_code
            WHERE dp.shop_id = :shop_id
        """),
        {"shop_id": shop_id},
    )

    # ── 5. Merge all data ────────────────────────────────────
    all_nm_ids = set(orders_map.keys()) | set(ads_map.keys())
    # Add catalog products even if no orders in period
    pg_rows = pg_result.fetchall()
    pg_map = {}
    for row in pg_rows:
        nm_id = int(row[0])
        pg_map[nm_id] = {
            "nm_id": nm_id,
            "name": row[1] or "",
            "image_url": row[2],
            "current_price": float(row[3]),
            "vendor_code": row[4],
            "cost_price": float(row[5]),
            "packaging_cost": float(row[6]),
        }
        all_nm_ids.add(nm_id)

    products = []
    for nm_id in all_nm_ids:
        info = pg_map.get(nm_id, {})
        orders = orders_map.get(nm_id, {})
        ads = ads_map.get(nm_id, {})
        stocks = stocks_map.get(nm_id, {})

        vendor_code = info.get("vendor_code") or orders.get("vendor_code") or str(nm_id)
        revenue_7d = orders.get("revenue_7d", 0.0)
        revenue_prev = orders.get("revenue_prev", 0.0)
        orders_7d = orders.get("orders_7d", 0)
        ad_spend_7d = ads.get("ad_spend_7d", 0.0)
        cost_price = info.get("cost_price", 0.0)
        packaging_cost = info.get("packaging_cost", 0.0)

        # ── WB Finance fees (from finreport) ──────────────
        fees = fees_map.get(vendor_code.lower(), {})
        fin_revenue = fees.get("fin_revenue", 0.0)

        # Средняя доля каждого типа расходов от выручки (по финотчёту)
        # Масштабируем на выручку текущего периода
        raw_mp_fees = (
            fees.get("commission", 0.0)    # Комиссия WB (ppvz_sales_commission)
            + fees.get("logistics", 0.0)   # Логистика и доставка
            + fees.get("storage", 0.0)     # Хранение
            + fees.get("acceptance", 0.0)  # Платная приёмка
            + fees.get("fines", 0.0)       # Штрафы
            + fees.get("acquiring", 0.0)   # Эквайринг банка (acquiring_fee, ~4%)
        )
        if fin_revenue > 0 and raw_mp_fees > 0:
            # Доля комиссий от выручки (из финотчёта), применяем к текущей выручке
            mp_fees_rate = min(raw_mp_fees / fin_revenue, 0.95)  # cap 95%
            mp_fees = round(revenue_7d * mp_fees_rate, 2) if revenue_7d > 0 else 0.0
        else:
            mp_fees = 0.0
        mp_fees_percent = round(mp_fees / revenue_7d * 100, 1) if revenue_7d > 0 else 0.0

        # ── DRR ───────────────────────────────────────────
        drr = round(ad_spend_7d / revenue_7d * 100, 1) if revenue_7d > 0 else 0.0

        # ── Revenue delta ─────────────────────────────────
        if revenue_prev > 0:
            revenue_delta = round((revenue_7d - revenue_prev) / revenue_prev * 100, 1)
        elif revenue_7d > 0:
            revenue_delta = 100.0
        else:
            revenue_delta = 0.0

        # ── Gross profit = выручка - комиссии МП - себестоимость - реклама ──
        # Строго: прибыль не может быть > выручки
        gross_profit = None
        margin = None
        if cost_price > 0:
            total_cogs = (cost_price + packaging_cost) * orders_7d
            gross_profit = round(revenue_7d - mp_fees - total_cogs - ad_spend_7d, 2)
            if revenue_7d > 0:
                margin = round(gross_profit / revenue_7d * 100, 1)

        p = {
            "nm_id": nm_id,
            "vendor_code": vendor_code,
            "name": info.get("name", vendor_code),
            "image_url": info.get("image_url", ""),
            "current_price": info.get("current_price", 0.0),
            "cost_price": cost_price,
            "packaging_cost": packaging_cost,
            # Sales
            "orders_7d": orders_7d,
            "revenue_7d": revenue_7d,
            "orders_prev": orders.get("orders_prev", 0),
            "revenue_prev": revenue_prev,
            "revenue_delta": revenue_delta,
            # Ads
            "ad_spend_7d": ad_spend_7d,
            "ad_views": ads.get("ad_views", 0),
            "ad_clicks": ads.get("ad_clicks", 0),
            "drr": drr,
            # Stocks
            "stock_fbo": stocks.get("stock_fbo", 0),
            "stock_fbs": stocks.get("stock_fbs", 0),
            # WB Marketplace fees
            "mp_fees": mp_fees,
            "mp_fees_percent": mp_fees_percent,
            # Profit
            "gross_profit": gross_profit,
            "margin": margin,
        }
        products.append(p)


    # ── 6. Search ────────────────────────────────────────────
    if search:
        s = search.lower()
        products = [
            p for p in products
            if s in p["name"].lower() or s in p["vendor_code"].lower()
        ]

    # ── 7. Filter ────────────────────────────────────────────
    if filter == "with_ads":
        products = [p for p in products if p["ad_spend_7d"] > 0]
    elif filter == "no_ads":
        products = [p for p in products if p["ad_spend_7d"] == 0 and (p["orders_7d"] > 0 or p["revenue_7d"] > 0)]
    elif filter == "leaders":
        median = sorted([p["revenue_7d"] for p in products])[len(products) // 2] if products else 0
        products = [p for p in products if p["revenue_7d"] >= median and p["revenue_7d"] > 0]
    elif filter == "falling":
        products = [p for p in products if p["revenue_delta"] < -10]
    elif filter == "problems":
        products = [p for p in products if p["stock_fbo"] + p["stock_fbs"] == 0 and p["revenue_7d"] > 0]

    # ── 8. Sort ──────────────────────────────────────────────
    SORT_FIELDS = {
        "revenue_7d", "orders_7d", "ad_spend_7d", "drr",
        "revenue_delta", "stock_fbo", "stock_fbs", "current_price",
        "gross_profit", "margin",
    }
    sort_key = sort if sort in SORT_FIELDS else "revenue_7d"
    reverse = (order == "desc")

    def sort_val(p):
        v = p.get(sort_key)
        if v is None:
            return float("-inf") if reverse else float("inf")
        return v

    products.sort(key=sort_val, reverse=reverse)

    # ── 9. Paginate ──────────────────────────────────────────
    total = len(products)
    offset = (page - 1) * per_page
    page_products = products[offset: offset + per_page]

    cost_missing = sum(1 for p in products if p["cost_price"] == 0)

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "products": page_products,
        "cost_missing_count": cost_missing,
    }


# ── PATCH cost price (single product) ─────────────────────

@router.patch("/wb/cost")
async def update_wb_cost(
    req: WBCostUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update cost price for a single WB product (by vendor_code)."""
    shop_result = await db.execute(
        select(Shop).where(Shop.id == req.shop_id, Shop.user_id == current_user.id)
    )
    if not shop_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Shop not found")

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
            "shop_id": req.shop_id,
            "offer_id": req.vendor_code,
            "cost_price": req.cost_price,
            "packaging_cost": req.packaging_cost,
        },
    )
    await db.commit()
    return {"ok": True, "vendor_code": req.vendor_code, "cost_price": req.cost_price}


# ── POST bulk Excel upload ─────────────────────────────────

@router.post("/wb/cost/bulk")
async def upload_wb_cost_excel(
    shop_id: int = Query(...),
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Bulk upload WB cost prices from Excel.
    Format: column A = vendor_code (артикул), column B = cost_price.
    """
    shop_result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    if not shop_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Shop not found")

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

    for row_idx, row in enumerate(ws.iter_rows(min_col=1, max_col=2, values_only=True), start=1):
        vendor_code_raw, cost_raw = row
        if vendor_code_raw is None or cost_raw is None:
            continue
        vendor_code = str(vendor_code_raw).strip()
        if not vendor_code:
            continue
        try:
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
            {"shop_id": shop_id, "offer_id": vendor_code, "cost_price": cost_price},
        )
        updated += 1

    await db.commit()
    return {"ok": True, "updated": updated, "errors": errors[:20]}


# ── GET Excel template ─────────────────────────────────────

@router.get("/wb/cost/template")
async def download_wb_cost_template(
    shop_id: int = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Download Excel template with all WB vendor_codes for cost price entry."""
    shop_result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    if not shop_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Shop not found")

    result = await db.execute(
        text("""
            SELECT dp.vendor_code,
                   COALESCE(pc.cost_price, 0) AS cost_price,
                   dp.name
            FROM dim_products dp
            LEFT JOIN product_costs pc
                ON pc.shop_id = dp.shop_id AND pc.offer_id = dp.vendor_code
            WHERE dp.shop_id = :shop_id
              AND dp.vendor_code IS NOT NULL
              AND dp.vendor_code != ''
            ORDER BY dp.name
        """),
        {"shop_id": shop_id},
    )
    rows = result.fetchall()

    import openpyxl
    wb_xl = openpyxl.Workbook()
    ws = wb_xl.active
    ws.title = "Себестоимость WB"
    ws.append(["Артикул продавца", "Себестоимость", "Название (справочно)"])
    ws.column_dimensions['A'].width = 25
    ws.column_dimensions['B'].width = 18
    ws.column_dimensions['C'].width = 50
    for r in rows:
        ws.append([r[0], float(r[1]), r[2] or ""])

    buf = io.BytesIO()
    wb_xl.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=wb_cost_template_{shop_id}.xlsx"},
    )
