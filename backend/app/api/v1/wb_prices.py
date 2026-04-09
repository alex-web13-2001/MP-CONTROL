"""
WB Prices API — live prices from WB discounts-prices-api.

GET /products/wb/prices?shop_id=X
  → Fetches from WB API /api/v2/list/goods/filter
  → Joins with dim_products (cost, name) + ClickHouse (stocks)
  → Returns unified price table data
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.core.marketplace_client import MarketplaceClient
from app.core.encryption import decrypt_api_key
from app.models.shop import Shop
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/products", tags=["WB Prices"])

WB_PRICES_ENDPOINT = "/api/v2/list/goods/filter"
WB_PRICES_PAGE_SIZE = 1000


async def _fetch_wb_prices(db: AsyncSession, shop_id: int, api_key: str, api_key_encrypted: bytes = None) -> list[dict]:
    """Paginate through WB discounts-prices-api and return all goods."""
    all_goods = []
    offset = 0

    async with MarketplaceClient(
        db=db,
        shop_id=shop_id,
        marketplace="wildberries_prices",
        api_key=api_key,
        api_key_encrypted=api_key_encrypted,
    ) as client:
        while True:
            params = {"limit": WB_PRICES_PAGE_SIZE, "offset": offset}
            response = await client.get(WB_PRICES_ENDPOINT, params=params)

            if not response.is_success:
                logger.error(
                    "WB Prices API error: status=%s, error=%s",
                    response.status_code, response.error,
                )
                break

            data = response.data
            if not data:
                break

            list_goods = []
            if isinstance(data, dict):
                list_goods = data.get("data", {}).get("listGoods", [])

            if not list_goods:
                break

            for item in list_goods:
                nm_id = item.get("nmID")
                if not nm_id:
                    continue

                sizes = item.get("sizes", [])
                size = sizes[0] if sizes else {}

                all_goods.append({
                    "nm_id": nm_id,
                    "vendor_code": item.get("vendorCode", ""),
                    "price": size.get("price", 0),
                    "discounted_price": size.get("discountedPrice", 0),
                    "club_discounted_price": size.get("clubDiscountedPrice", 0),
                    "tech_size_name": size.get("techSizeName", ""),
                    "discount": item.get("discount", 0),
                    "club_discount": item.get("clubDiscount", 0),
                    "editable_size_price": item.get("editableSizePrice", False),
                    "is_bad_turnover": item.get("isBadTurnover", False),
                })

            if len(list_goods) < WB_PRICES_PAGE_SIZE:
                break
            offset += WB_PRICES_PAGE_SIZE

    logger.info("Fetched %d prices from WB API for shop %d", len(all_goods), shop_id)
    return all_goods


@router.get("/wb/prices")
async def get_wb_prices(
    shop_id: int = Query(...),
    search: str = Query(""),
    sort: str = Query("name"),
    order: str = Query("asc"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get WB product prices with live data from WB API.
    Joined with internal cost prices and stock data.
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

    # Get API key - prefer encrypted, fallback to plain
    api_key = shop.api_key or ""
    api_key_encrypted = shop.api_key_encrypted
    if not api_key and not api_key_encrypted:
        raise HTTPException(status_code=400, detail="API ключ не настроен для этого магазина")

    # ── 1. Fetch live prices from WB API ──
    try:
        wb_prices = await _fetch_wb_prices(db, shop_id, api_key, api_key_encrypted)
    except Exception as e:
        logger.error("Failed to fetch WB prices: %s", e)
        raise HTTPException(status_code=502, detail=f"Ошибка получения цен из WB API: {e}")

    if not wb_prices:
        return {
            "products": [],
            "total": 0,
            "page": page,
            "per_page": per_page,
            "cost_missing_count": 0,
        }

    # ── 2. Get catalog data from PostgreSQL ──
    pg_result = await db.execute(
        text("""
            SELECT dp.nm_id,
                   dp.name,
                   COALESCE(dp.main_image_url, '') AS image_url,
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
    pg_map = {}
    for row in pg_result.fetchall():
        nm_id = int(row[0])
        pg_map[nm_id] = {
            "name": row[1] or "",
            "image_url": row[2],
            "vendor_code": row[3],
            "cost_price": float(row[4]),
            "packaging_cost": float(row[5]),
        }

    # ── 3. Get stocks from ClickHouse ──
    stocks_map = {}
    try:
        from app.core.clickhouse import get_clickhouse_client
        ch = get_clickhouse_client()
        stocks_result = ch.query("""
            SELECT
                nm_id,
                sumIf(quantity, NOT startsWith(warehouse_name, 'FBS:')) AS stock_fbo,
                sumIf(quantity, startsWith(warehouse_name, 'FBS:'))     AS stock_fbs
            FROM mms_analytics.fact_inventory_snapshot FINAL
            WHERE shop_id = {shop_id:UInt32}
            GROUP BY nm_id
        """, parameters={"shop_id": shop_id})
        for r in stocks_result.result_rows:
            stocks_map[int(r[0])] = {
                "stock_fbo": int(r[1]),
                "stock_fbs": int(r[2]),
            }
    except Exception as e:
        logger.warning("CH stocks query failed: %s", e)

    # ── 4. Profit per unit from fact_finances (last 7 days of actual sales) ──
    # Uses last 7 days instead of 30 to reflect recent pricing changes accurately
    profit_map: dict[int, float | None] = {}
    try:
        from datetime import date, timedelta
        d7_start = date.today() - timedelta(days=6)
        finance_result = ch.query("""
            SELECT
                toUInt64(external_id)  AS nm_id,
                sum(payout_amount) / nullIf(
                    sumIf(quantity, operation_type = 'Продажа' AND quantity > 0), 0
                ) AS avg_payout_per_unit,
                sum(wb_delivery_rub) / nullIf(
                    sumIf(quantity, operation_type = 'Продажа' AND quantity > 0), 0
                ) AS avg_logistics_per_unit
            FROM mms_analytics.fact_finances FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND marketplace = 1
              AND event_date >= {d7_start:Date}
            GROUP BY nm_id
            HAVING sumIf(quantity, operation_type = 'Продажа' AND quantity > 0) > 0
        """, parameters={"shop_id": shop_id, "d7_start": d7_start})
        for r in finance_result.result_rows:
            nm = int(r[0])
            avg_payout = float(r[1] or 0)
            avg_logistics = abs(float(r[2] or 0))
            profit_map[nm] = (avg_payout, avg_logistics)
    except Exception as e:
        logger.warning("CH fact_finances profit query failed: %s", e)

    # ── 4b. Ad spend per product (7d) for DRR + profit with ads ──
    # Uses last 7 days to reflect current DRR after price changes
    ad_spend_map: dict[int, float] = {}
    orders_revenue_map: dict[int, tuple[float, int]] = {}  # nm_id → (revenue_7d, orders_7d)
    try:
        from datetime import date, timedelta
        d7_start = date.today() - timedelta(days=6)
        ad_result = ch.query("""
            SELECT
                nm_id,
                round(sum(spend), 2) AS ad_spend_7d
            FROM mms_analytics.fact_advert_stats_v3 FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {d7_start:Date}
            GROUP BY nm_id
        """, parameters={"shop_id": shop_id, "d7_start": d7_start})
        for r in ad_result.result_rows:
            ad_spend_map[int(r[0])] = float(r[1])

        # Revenue for 7d (for DRR calculation)
        rev_result = ch.query("""
            SELECT
                nm_id,
                round(sum(price_with_disc), 2) AS revenue_7d,
                count() AS orders_7d
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {d7_start:Date}
              AND is_cancel = 0
            GROUP BY nm_id
        """, parameters={"shop_id": shop_id, "d7_start": d7_start})
        for r in rev_result.result_rows:
            orders_revenue_map[int(r[0])] = (float(r[1]), int(r[2]))
    except Exception as e:
        logger.warning("CH ad_spend/revenue query failed: %s", e)

    # ── 4c. Last sale date per product ──
    last_sale_map: dict[int, str] = {}  # nm_id → ISO date string
    try:
        from datetime import date, timedelta
        last_sale_result = ch.query("""
            SELECT
                nm_id,
                max(date) AS last_sale_date
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND is_cancel = 0
            GROUP BY nm_id
        """, parameters={"shop_id": shop_id})
        for r in last_sale_result.result_rows:
            last_sale_map[int(r[0])] = str(r[1])
    except Exception as e:
        logger.warning("CH last_sale query failed: %s", e)

    ch.close()

    # ── 5. Merge WB prices with catalog data ──
    products = []
    for wp in wb_prices:
        nm_id = wp["nm_id"]
        cat = pg_map.get(nm_id, {})

        vendor_code = cat.get("vendor_code") or wp["vendor_code"] or str(nm_id)
        cost_price = cat.get("cost_price", 0.0)
        packaging_cost = cat.get("packaging_cost", 0.0)
        unit_cost = cost_price + packaging_cost
        stocks = stocks_map.get(nm_id, {})
        stock_fbo = stocks.get("stock_fbo", 0)
        stock_fbs = stocks.get("stock_fbs", 0)

        # Profit per unit (без учёта рекламы):
        # Вариант 1: fact_finances 30d average (точный)
        # Вариант 2: fallback — estimated_fees ~35% от discountedPrice
        discounted = wp["discounted_price"]
        profit_per_unit = None
        profit_source = None  # "finance" or "estimated"
        if cost_price > 0 and nm_id in profit_map:
            avg_payout, avg_logistics = profit_map[nm_id]
            profit_per_unit = round(avg_payout - avg_logistics - unit_cost, 2)
            profit_source = "finance"
        elif cost_price > 0 and discounted > 0:
            estimated_fees = round(discounted * 0.35, 2)
            profit_per_unit = round(discounted - unit_cost - estimated_fees, 2)
            profit_source = "estimated"

        # Ad spend & DRR (7d — reflects recent price changes)
        ad_spend_7d = ad_spend_map.get(nm_id, 0)
        rev_data = orders_revenue_map.get(nm_id)
        revenue_7d = rev_data[0] if rev_data else 0
        orders_7d = rev_data[1] if rev_data else 0
        drr = round(ad_spend_7d / revenue_7d * 100, 1) if revenue_7d > 0 and ad_spend_7d > 0 else None

        # Profit with ads = profit_per_unit - (ad_spend / orders)
        ad_per_unit = round(ad_spend_7d / orders_7d, 2) if orders_7d > 0 and ad_spend_7d > 0 else 0
        profit_with_ads = round(profit_per_unit - ad_per_unit, 2) if profit_per_unit is not None and ad_per_unit > 0 else None

        # WB Club: only include if club discount is actually active
        club_disc = wp["club_discount"]
        club_price_val = wp["club_discounted_price"] if club_disc > 0 else None

        products.append({
            "nm_id": nm_id,
            "vendor_code": vendor_code,
            "name": cat.get("name", vendor_code),
            "image_url": cat.get("image_url", ""),
            # WB prices
            "price": wp["price"],                         # Цена до скидки
            "discount": wp["discount"],                   # Скидка %
            "discounted_price": discounted,                # Цена со скидкой
            "club_discount": club_disc,                    # WB Клуб скидка %
            "club_discounted_price": club_price_val,       # Цена для WB Клуб (None if not active)
            "tech_size_name": wp["tech_size_name"],
            "editable_size_price": wp["editable_size_price"],
            "is_bad_turnover": wp["is_bad_turnover"],
            # Cost
            "cost_price": cost_price,
            "packaging_cost": packaging_cost,
            # Profit (без рекламы)
            "profit_per_unit": profit_per_unit,
            "profit_source": profit_source,  # "finance" | "estimated" | null
            # Ads (7d window for up-to-date DRR)
            "ad_spend_30d": ad_spend_7d,
            "drr": drr,
            "profit_with_ads": profit_with_ads,
            # Stocks
            "stock_fbo": stock_fbo,
            "stock_fbs": stock_fbs,
            # Last sale
            "last_sale_date": last_sale_map.get(nm_id),
        })

    # ── 5. Search ──
    if search:
        s = search.lower()
        products = [
            p for p in products
            if s in p["name"].lower()
            or s in p["vendor_code"].lower()
            or s in str(p["nm_id"])
        ]

    # ── 6. Sort ──
    SORT_FIELDS = {
        "name", "price", "discounted_price", "discount",
        "club_discounted_price", "cost_price", "profit_per_unit",
        "stock_fbo", "stock_fbs", "last_sale_date",
    }
    sort_key = sort if sort in SORT_FIELDS else "name"
    reverse = (order == "desc")

    def sort_val(p):
        v = p.get(sort_key)
        if v is None:
            return float("-inf") if reverse else float("inf")
        if isinstance(v, str):
            return v.lower()
        return v

    products.sort(key=sort_val, reverse=reverse)

    # ── 7. Totals ──
    cost_missing = sum(1 for p in products if p["cost_price"] == 0)

    # ── 8. Paginate ──
    total = len(products)
    offset = (page - 1) * per_page
    page_products = products[offset: offset + per_page]

    return {
        "products": page_products,
        "total": total,
        "page": page,
        "per_page": per_page,
        "cost_missing_count": cost_missing,
    }
