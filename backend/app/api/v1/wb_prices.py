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
from app.models.shop import Shop
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/products", tags=["WB Prices"])

WB_PRICES_ENDPOINT = "/api/v2/list/goods/filter"
WB_PRICES_PAGE_SIZE = 1000


async def _fetch_wb_prices(db: AsyncSession, shop_id: int, api_key: str) -> list[dict]:
    """Paginate through WB discounts-prices-api and return all goods."""
    all_goods = []
    offset = 0

    async with MarketplaceClient(
        db=db,
        shop_id=shop_id,
        marketplace="wildberries_prices",
        api_key=api_key,
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

    if not shop.api_key:
        raise HTTPException(status_code=400, detail="API ключ не настроен для этого магазина")

    # ── 1. Fetch live prices from WB API ──
    try:
        wb_prices = await _fetch_wb_prices(db, shop_id, shop.api_key)
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
        ch.close()
    except Exception as e:
        logger.warning("CH stocks query failed: %s", e)

    # ── 4. Merge WB prices with catalog data ──
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

        # Profit per unit: discountedPrice - unitCost - estimated_fees
        # (simplified: fees ~40% of discountedPrice for WB)
        discounted = wp["discounted_price"]
        profit_per_unit = None
        if cost_price > 0 and discounted > 0:
            estimated_fees = round(discounted * 0.35, 2)  # ~35% MP fees estimate
            profit_per_unit = round(discounted - unit_cost - estimated_fees, 2)

        products.append({
            "nm_id": nm_id,
            "vendor_code": vendor_code,
            "name": cat.get("name", vendor_code),
            "image_url": cat.get("image_url", ""),
            # WB prices
            "price": wp["price"],                         # Цена до скидки
            "discount": wp["discount"],                   # Скидка %
            "discounted_price": discounted,                # Цена со скидкой
            "club_discount": wp["club_discount"],          # WB Клуб скидка %
            "club_discounted_price": wp["club_discounted_price"],  # Цена для WB Клуб
            "tech_size_name": wp["tech_size_name"],
            "editable_size_price": wp["editable_size_price"],
            "is_bad_turnover": wp["is_bad_turnover"],
            # Cost
            "cost_price": cost_price,
            "packaging_cost": packaging_cost,
            # Profit
            "profit_per_unit": profit_per_unit,
            # Stocks
            "stock_fbo": stock_fbo,
            "stock_fbs": stock_fbs,
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
        "stock_fbo", "stock_fbs",
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
