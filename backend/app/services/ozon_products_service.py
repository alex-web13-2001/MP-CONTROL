"""
Ozon Products Service — Fetch products, content, inventory, commissions,
and content ratings from Ozon Seller API.

All requests go through MarketplaceClient (proxy, rate limiting, circuit breaker).

API Endpoints (tested, working):
    POST /v3/product/list — list all product_id + offer_id (paginated via last_id)
    POST /v3/product/info/list — detailed info (name, images, prices, stocks, commissions)
    POST /v1/product/info/description — product description (HTML)
    POST /v1/product/rating-by-sku — content rating (0-100) + group breakdown

Data flow:
    1. sync_ozon_products: list → info → upsert dim_ozon_products (PostgreSQL)
    2. sync_ozon_content: info + description → MD5 hashes → dim_ozon_product_content
    3. sync_ozon_inventory: info → prices + stocks → fact_ozon_inventory (ClickHouse)
    4. sync_ozon_commissions: info → commissions → fact_ozon_commissions (ClickHouse)
    5. sync_ozon_content_rating: SKUs → rating-by-sku → fact_ozon_content_rating (ClickHouse)
"""

import asyncio
import hashlib
import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────
PAGE_SIZE = 100  # max items per /v3/product/list request
INFO_BATCH_SIZE = 100  # max product_ids per /v3/product/info/list
CH_TABLE = "mms_analytics.fact_ozon_inventory"
CH_COLUMNS = [
    "fetched_at", "shop_id", "product_id", "offer_id",
    "price", "old_price", "min_price", "marketing_price",
    "stocks_fbo", "stocks_fbs",
]
CH_BATCH_SIZE = 500


def _md5(text: str) -> str:
    """MD5 hash of text."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _safe_decimal(val) -> float:
    """Convert string/float/None to float for ClickHouse."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _extract_stocks(item: dict) -> Tuple[int, int]:
    """Extract FBO and FBS stock counts from product info."""
    fbo = 0
    fbs = 0
    stocks = item.get("stocks", {})
    if isinstance(stocks, dict):
        for stock_entry in stocks.get("stocks", []):
            source = stock_entry.get("source", "")
            present = stock_entry.get("present", 0) or 0
            if source == "fbo":
                fbo = present
            elif source == "fbs":
                fbs = present
    return fbo, fbs


def _extract_sku(item: dict) -> Optional[int]:
    """Extract primary SKU from sources."""
    sources = item.get("sources", [])
    if sources:
        return sources[0].get("sku")
    return None


# ── Ozon Products Service ──────────────────────────────────
class OzonProductsService:
    """
    Fetch products from Ozon Seller API via MarketplaceClient.

    Uses proxy rotation, rate limiting, and circuit breaker
    from the shared MarketplaceClient infrastructure.
    """

    def __init__(
        self,
        db: AsyncSession,
        shop_id: int,
        api_key: str,
        client_id: str,
    ):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key
        self.client_id = client_id

    def _make_client(self):
        """Create MarketplaceClient context manager for Ozon."""
        return MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="ozon",
            api_key=self.api_key,
            client_id=self.client_id,
        )

    async def fetch_product_list(self) -> List[dict]:
        """
        Fetch ALL product_ids via POST /v3/product/list.

        Paginated via last_id cursor, PAGE_SIZE items per request.
        Returns list of {product_id, offer_id, has_fbo_stocks, ...}
        """
        all_items = []
        last_id = ""

        while True:
            async with self._make_client() as client:
                response = await client.post(
                    "/v3/product/list",
                    json={"filter": {}, "last_id": last_id, "limit": PAGE_SIZE},
                )

            if not response.is_success:
                logger.error(
                    "Ozon /v3/product/list error: status=%s error=%s",
                    response.status_code, response.error,
                )
                break

            data = response.data
            result = data.get("result", {})
            items = result.get("items", [])
            total = result.get("total", 0)

            all_items.extend(items)
            logger.info(
                "Ozon product/list: got %d items (total API: %d, loaded: %d)",
                len(items), total, len(all_items),
            )

            # Next page
            new_last_id = result.get("last_id", "")
            if not items or not new_last_id or new_last_id == last_id:
                break
            last_id = new_last_id

            await asyncio.sleep(0.5)  # small delay

        return all_items

    async def fetch_product_info(self, product_ids: List[int]) -> List[dict]:
        """
        Fetch detailed info via POST /v3/product/info/list.

        Returns full product data: name, images, prices, stocks, commissions.
        Max 100 product_ids per request.
        """
        all_items = []

        for i in range(0, len(product_ids), INFO_BATCH_SIZE):
            batch = product_ids[i:i + INFO_BATCH_SIZE]

            async with self._make_client() as client:
                response = await client.post(
                    "/v3/product/info/list",
                    json={"product_id": batch, "sku": []},
                )

            if not response.is_success:
                logger.error(
                    "Ozon /v3/product/info/list error: status=%s error=%s",
                    response.status_code, response.error,
                )
                continue

            items = response.data.get("items", [])
            all_items.extend(items)
            logger.info(
                "Ozon product/info/list: batch %d-%d → %d items",
                i, i + len(batch), len(items),
            )
            await asyncio.sleep(0.3)

        return all_items

    async def fetch_description(self, product_id: int) -> str:
        """
        Fetch description via POST /v1/product/info/description.

        Returns description HTML string.
        """
        async with self._make_client() as client:
            response = await client.post(
                "/v1/product/info/description",
                json={"product_id": product_id},
            )

        if not response.is_success:
            logger.warning(
                "Ozon description error for %d: %s",
                product_id, response.error,
            )
            return ""

        data = response.data
        return data.get("result", {}).get("description", data.get("description", ""))

    async def fetch_all_descriptions(self, product_ids: List[int]) -> Dict[int, str]:
        """
        Fetch descriptions for all products (sequential with rate limit).

        Returns {product_id: description_text}
        """
        descriptions = {}
        for pid in product_ids:
            desc = await self.fetch_description(pid)
            descriptions[pid] = desc
            await asyncio.sleep(0.2)  # rate limit safety
        return descriptions

    async def fetch_content_ratings(self, skus: List[int]) -> List[dict]:
        """
        Fetch content ratings via POST /v1/product/rating-by-sku.

        Returns list of {sku, rating, groups: [{key, name, rating, weight}]}
        Batch: up to 100 SKUs per request.
        """
        all_ratings = []
        BATCH = 100

        for i in range(0, len(skus), BATCH):
            batch = skus[i:i + BATCH]
            async with self._make_client() as client:
                response = await client.post(
                    "/v1/product/rating-by-sku",
                    json={"skus": batch},
                )

            if not response.is_success:
                logger.warning(
                    "Ozon /v1/product/rating-by-sku error: %s %s",
                    response.status_code, response.error,
                )
                continue

            products = response.data.get("products", [])
            all_ratings.extend(products)
            logger.info(
                "Ozon content ratings: batch %d-%d → %d items",
                i, i + len(batch), len(products),
            )
            await asyncio.sleep(0.3)

        return all_ratings


# ── Commission Extraction ─────────────────────────────────

def _extract_commissions(item: dict) -> dict:
    """
    Extract commissions from /v3/product/info/list response item.

    The 'commissions' field is a list of dicts with 'percent', 'min_value',
    'value', 'sale_schema', 'delivery_amount', 'return_amount'.
    We normalize into flat dict with sales_percent, fbo/fbs logistics.
    """
    commissions_list = item.get("commissions", [])
    result = {
        "sales_percent": 0.0,
        "fbo_fulfillment_amount": 0.0,
        "fbo_direct_flow_trans_min": 0.0,
        "fbo_direct_flow_trans_max": 0.0,
        "fbo_deliv_to_customer": 0.0,
        "fbo_return_flow": 0.0,
        "fbs_direct_flow_trans_min": 0.0,
        "fbs_direct_flow_trans_max": 0.0,
        "fbs_deliv_to_customer": 0.0,
        "fbs_first_mile_min": 0.0,
        "fbs_first_mile_max": 0.0,
        "fbs_return_flow": 0.0,
    }

    for comm in commissions_list:
        schema = comm.get("sale_schema", "")
        percent = _safe_decimal(comm.get("percent"))
        delivery = _safe_decimal(comm.get("delivery_amount"))
        return_am = _safe_decimal(comm.get("return_amount"))
        value = _safe_decimal(comm.get("value"))
        min_val = _safe_decimal(comm.get("min_value"))

        if schema in ("fbo", "FBO"):
            result["sales_percent"] = max(result["sales_percent"], percent)
            result["fbo_fulfillment_amount"] = value
            result["fbo_direct_flow_trans_min"] = min_val
            result["fbo_direct_flow_trans_max"] = value
            result["fbo_deliv_to_customer"] = delivery
            result["fbo_return_flow"] = return_am
        elif schema in ("fbs", "FBS"):
            result["fbs_direct_flow_trans_min"] = min_val
            result["fbs_direct_flow_trans_max"] = value
            result["fbs_deliv_to_customer"] = delivery
            result["fbs_first_mile_min"] = min_val
            result["fbs_first_mile_max"] = value
            result["fbs_return_flow"] = return_am
        elif schema in ("rfbs", "RFBS"):
            # Use rfbs percent as sales_percent if higher
            if percent > result["sales_percent"]:
                result["sales_percent"] = percent

    return result


# ── PostgreSQL Upsert ──────────────────────────────────────

def upsert_ozon_products(conn_params: dict, shop_id: int, products: List[dict]) -> Tuple[int, List[dict]]:
    """
    Upsert products into dim_ozon_products.

    Detects image hash changes and returns events.

    Args:
        conn_params: dict with host, port, user, password, database
        products: list of product info dicts from /v3/product/info/list

    Returns:
        (count, events_list)
    """
    import psycopg2
    import json as _json

    if not products:
        return 0, []

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    count = 0
    events = []

    try:
        for item in products:
            product_id = item.get("id")
            if not product_id:
                continue

            offer_id = item.get("offer_id", "")
            sku = _extract_sku(item)
            name = item.get("name", "")
            images = item.get("images", [])
            main_image = images[0] if images else None
            barcodes = item.get("barcodes", [])
            barcode = barcodes[0] if barcodes else None
            category_id = item.get("description_category_id")

            price = _safe_decimal(item.get("price"))
            old_price = _safe_decimal(item.get("old_price"))
            min_price = _safe_decimal(item.get("min_price"))
            marketing_price = _safe_decimal(item.get("marketing_price", 0))
            volume_weight = _safe_decimal(item.get("volume_weight"))

            fbo, fbs = _extract_stocks(item)
            is_archived = item.get("is_archived", False)

            # New fields
            created_at_ozon = item.get("created_at")
            updated_at_ozon = item.get("updated_at")
            vat = _safe_decimal(item.get("vat"))
            type_id = item.get("type_id")
            model_info = item.get("model_info", {}) or {}
            model_id = model_info.get("model_id")
            model_count = model_info.get("count", 0)

            # Price indexes
            pi = item.get("price_indexes", {}) or {}
            price_index_color = pi.get("color_index", "")
            ext_data = pi.get("external_index_data", {}) or {}
            price_index_value = _safe_decimal(ext_data.get("price_index_value", 0))
            competitor_min_price = _safe_decimal(ext_data.get("minimal_price", 0))
            is_kgt = item.get("is_kgt", False)

            # Statuses
            statuses = item.get("statuses", {}) or {}
            status = statuses.get("status", "")
            moderate_status = statuses.get("moderate_status", "")
            status_name = statuses.get("status_name", "")

            # Images hash
            all_images_json = _json.dumps(images) if images else "[]"
            images_hash = _md5("|".join(sorted(images))) if images else ""
            primary_imgs = item.get("primary_image", [])
            primary_image_url = primary_imgs[0] if primary_imgs else main_image

            # Availability
            avails = item.get("availabilities", [])
            availability = ""
            availability_source = ""
            if avails:
                availability = avails[0].get("availability", "")
                availability_source = avails[0].get("source", "")

            # Check for image hash change
            cursor.execute(
                "SELECT images_hash FROM dim_ozon_products WHERE shop_id = %s AND product_id = %s",
                (shop_id, product_id),
            )
            existing = cursor.fetchone()
            if existing and existing[0] and existing[0] != images_hash and images_hash:
                events.append({
                    "shop_id": shop_id,
                    "product_id": product_id,
                    "offer_id": offer_id,
                    "event_type": "OZON_PHOTO_CHANGE",
                    "field": "images",
                    "old_value": existing[0],
                    "new_value": images_hash,
                })

            cursor.execute("""
                INSERT INTO dim_ozon_products
                    (shop_id, product_id, offer_id, sku, name, main_image_url,
                     barcode, category_id, price, old_price, min_price,
                     marketing_price, volume_weight, stocks_fbo, stocks_fbs,
                     is_archived, has_fbo_stocks, has_fbs_stocks,
                     created_at_ozon, updated_at_ozon, vat, type_id,
                     model_id, model_count, price_index_color, price_index_value,
                     competitor_min_price, is_kgt, status, moderate_status,
                     status_name, all_images_json, images_hash,
                     primary_image_url, availability, availability_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (shop_id, product_id) DO UPDATE SET
                    offer_id = EXCLUDED.offer_id,
                    sku = EXCLUDED.sku,
                    name = EXCLUDED.name,
                    main_image_url = EXCLUDED.main_image_url,
                    barcode = EXCLUDED.barcode,
                    category_id = EXCLUDED.category_id,
                    price = EXCLUDED.price,
                    old_price = EXCLUDED.old_price,
                    min_price = EXCLUDED.min_price,
                    marketing_price = EXCLUDED.marketing_price,
                    volume_weight = EXCLUDED.volume_weight,
                    stocks_fbo = EXCLUDED.stocks_fbo,
                    stocks_fbs = EXCLUDED.stocks_fbs,
                    is_archived = EXCLUDED.is_archived,
                    has_fbo_stocks = EXCLUDED.has_fbo_stocks,
                    has_fbs_stocks = EXCLUDED.has_fbs_stocks,
                    created_at_ozon = EXCLUDED.created_at_ozon,
                    updated_at_ozon = EXCLUDED.updated_at_ozon,
                    vat = EXCLUDED.vat,
                    type_id = EXCLUDED.type_id,
                    model_id = EXCLUDED.model_id,
                    model_count = EXCLUDED.model_count,
                    price_index_color = EXCLUDED.price_index_color,
                    price_index_value = EXCLUDED.price_index_value,
                    competitor_min_price = EXCLUDED.competitor_min_price,
                    is_kgt = EXCLUDED.is_kgt,
                    status = EXCLUDED.status,
                    moderate_status = EXCLUDED.moderate_status,
                    status_name = EXCLUDED.status_name,
                    all_images_json = EXCLUDED.all_images_json,
                    images_hash = EXCLUDED.images_hash,
                    primary_image_url = EXCLUDED.primary_image_url,
                    availability = EXCLUDED.availability,
                    availability_source = EXCLUDED.availability_source,
                    updated_at = NOW()
            """, (
                shop_id, product_id, offer_id, sku, name, main_image,
                barcode, category_id, price, old_price, min_price,
                marketing_price, volume_weight, fbo, fbs,
                is_archived, fbo > 0, fbs > 0,
                created_at_ozon, updated_at_ozon, vat, type_id,
                model_id, model_count, price_index_color, price_index_value,
                competitor_min_price, is_kgt, status, moderate_status,
                status_name, all_images_json, images_hash,
                primary_image_url, availability, availability_source,
            ))
            count += 1

        conn.commit()
    finally:
        cursor.close()
        conn.close()

    logger.info(
        "Upserted %d products into dim_ozon_products, detected %d image events",
        count, len(events),
    )
    return count, events


def upsert_ozon_content(
    conn_params: dict,
    shop_id: int,
    products: List[dict],
    descriptions: Dict[int, str],
) -> Tuple[int, List[dict]]:
    """
    Upsert content hashes into dim_ozon_product_content.
    Detect changes and return events.

    Returns (count, events_list)
    """
    import psycopg2

    if not products:
        return 0, []

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    count = 0
    events = []

    try:
        for item in products:
            product_id = item.get("id")
            if not product_id:
                continue

            name = item.get("name", "")
            desc = descriptions.get(product_id, "")
            images = item.get("images", [])
            main_image = images[0] if images else ""

            title_hash = _md5(name)
            description_hash = _md5(desc) if desc else ""
            images_hash = _md5("|".join(sorted(images))) if images else ""
            images_count = len(images)

            # Check existing
            cursor.execute(
                "SELECT title_hash, description_hash, main_image_url, images_hash "
                "FROM dim_ozon_product_content WHERE shop_id = %s AND product_id = %s",
                (shop_id, product_id),
            )
            existing = cursor.fetchone()

            if existing:
                old_title, old_desc, old_image, old_images = existing

                if old_title and old_title != title_hash:
                    events.append({
                        "shop_id": shop_id,
                        "product_id": product_id,
                        "event_type": "OZON_SEO_CHANGE",
                        "field": "title",
                        "old_value": old_title,
                        "new_value": title_hash,
                    })

                if old_desc and old_desc != description_hash:
                    events.append({
                        "shop_id": shop_id,
                        "product_id": product_id,
                        "event_type": "OZON_SEO_CHANGE",
                        "field": "description",
                        "old_value": old_desc,
                        "new_value": description_hash,
                    })

                if old_image and old_image != main_image:
                    events.append({
                        "shop_id": shop_id,
                        "product_id": product_id,
                        "event_type": "OZON_PHOTO_CHANGE",
                        "field": "main_image",
                        "old_value": old_image,
                        "new_value": main_image,
                    })

                if old_images and old_images != images_hash:
                    events.append({
                        "shop_id": shop_id,
                        "product_id": product_id,
                        "event_type": "OZON_PHOTO_CHANGE",
                        "field": "images_order",
                        "old_value": old_images,
                        "new_value": images_hash,
                    })

            # Upsert
            cursor.execute("""
                INSERT INTO dim_ozon_product_content
                    (shop_id, product_id, title_hash, description_hash,
                     main_image_url, images_hash, images_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (shop_id, product_id) DO UPDATE SET
                    title_hash = EXCLUDED.title_hash,
                    description_hash = EXCLUDED.description_hash,
                    main_image_url = EXCLUDED.main_image_url,
                    images_hash = EXCLUDED.images_hash,
                    images_count = EXCLUDED.images_count,
                    updated_at = NOW()
            """, (
                shop_id, product_id, title_hash, description_hash,
                main_image, images_hash, images_count,
            ))
            count += 1

        conn.commit()
    finally:
        cursor.close()
        conn.close()

    logger.info(
        "Upserted %d content hashes, detected %d events",
        count, len(events),
    )
    return count, events


# ── ClickHouse Inventory Loader ────────────────────────────

class OzonInventoryLoader:
    """Insert inventory snapshots into ClickHouse fact_ozon_inventory."""

    def __init__(
        self,
        host: str = "clickhouse",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "mms_analytics",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._client: Optional[ClickHouseClient] = None

    def connect(self):
        self._client = clickhouse_connect.get_client(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            database=self.database,
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def insert_inventory(self, shop_id: int, products: List[dict]) -> int:
        """Insert inventory snapshot from product info list."""
        if not products or not self._client:
            return 0

        now = datetime.utcnow()
        rows = []

        for item in products:
            product_id = item.get("id")
            if not product_id:
                continue

            fbo, fbs = _extract_stocks(item)

            rows.append([
                now,
                shop_id,
                product_id,
                item.get("offer_id", ""),
                _safe_decimal(item.get("price")),
                _safe_decimal(item.get("old_price")),
                _safe_decimal(item.get("min_price")),
                _safe_decimal(item.get("marketing_price", 0)),
                fbo,
                fbs,
            ])

        total = 0
        for i in range(0, len(rows), CH_BATCH_SIZE):
            batch = rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d inventory snapshots into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        """Get current inventory stats."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(product_id) as unique_products,
                min(fetched_at) as min_date,
                max(fetched_at) as max_date,
                avg(price) as avg_price,
                sum(stocks_fbo) as total_fbo,
                sum(stocks_fbs) as total_fbs
            FROM fact_ozon_inventory FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0],
                "unique_products": r[1],
                "min_date": str(r[2]),
                "max_date": str(r[3]),
                "avg_price": float(r[4]),
                "total_fbo": r[5],
                "total_fbs": r[6],
            }
        return {}


# ── ClickHouse Commissions Loader ─────────────────────────

CH_COMM_TABLE = "mms_analytics.fact_ozon_commissions"
CH_COMM_COLUMNS = [
    "dt", "updated_at", "shop_id", "product_id", "offer_id", "sku",
    "sales_percent",
    "fbo_fulfillment_amount", "fbo_direct_flow_trans_min", "fbo_direct_flow_trans_max",
    "fbo_deliv_to_customer", "fbo_return_flow",
    "fbs_direct_flow_trans_min", "fbs_direct_flow_trans_max",
    "fbs_deliv_to_customer", "fbs_first_mile_min", "fbs_first_mile_max",
    "fbs_return_flow",
]


class OzonCommissionsLoader:
    """Insert commission snapshots into ClickHouse fact_ozon_commissions."""

    def __init__(
        self,
        host: str = "clickhouse",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "mms_analytics",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._client: Optional[ClickHouseClient] = None

    def connect(self):
        self._client = clickhouse_connect.get_client(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            database=self.database,
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def insert_commissions(self, shop_id: int, products: List[dict]) -> int:
        """
        Extract commissions from product info and insert into ClickHouse.

        Args:
            shop_id: shop identifier
            products: product info dicts from /v3/product/info/list
        """
        if not products or not self._client:
            return 0

        now = datetime.utcnow()
        today = now.date()
        rows = []

        for item in products:
            product_id = item.get("id")
            if not product_id:
                continue

            offer_id = item.get("offer_id", "")
            sku = _extract_sku(item) or 0
            comms = _extract_commissions(item)

            rows.append([
                today,
                now,
                shop_id,
                product_id,
                offer_id,
                sku,
                comms["sales_percent"],
                comms["fbo_fulfillment_amount"],
                comms["fbo_direct_flow_trans_min"],
                comms["fbo_direct_flow_trans_max"],
                comms["fbo_deliv_to_customer"],
                comms["fbo_return_flow"],
                comms["fbs_direct_flow_trans_min"],
                comms["fbs_direct_flow_trans_max"],
                comms["fbs_deliv_to_customer"],
                comms["fbs_first_mile_min"],
                comms["fbs_first_mile_max"],
                comms["fbs_return_flow"],
            ])

        total = 0
        for i in range(0, len(rows), CH_BATCH_SIZE):
            batch = rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_COMM_TABLE, batch, column_names=CH_COMM_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d commission snapshots into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        """Get commission stats."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(product_id) as unique_products,
                min(dt) as min_date,
                max(dt) as max_date,
                avg(sales_percent) as avg_sales_pct
            FROM fact_ozon_commissions FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0],
                "unique_products": r[1],
                "min_date": str(r[2]),
                "max_date": str(r[3]),
                "avg_sales_percent": float(r[4]),
            }
        return {}


# ── ClickHouse Content Rating Loader ──────────────────────

CH_RATING_TABLE = "mms_analytics.fact_ozon_content_rating"
CH_RATING_COLUMNS = [
    "dt", "updated_at", "shop_id", "sku", "product_id",
    "rating", "media_rating", "description_rating",
    "attributes_rating", "rich_content_rating",
]


class OzonContentRatingLoader:
    """Insert content rating snapshots into ClickHouse fact_ozon_content_rating."""

    def __init__(
        self,
        host: str = "clickhouse",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "mms_analytics",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._client: Optional[ClickHouseClient] = None

    def connect(self):
        self._client = clickhouse_connect.get_client(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            database=self.database,
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def insert_ratings(
        self, shop_id: int, ratings: List[dict],
        sku_to_product_id: Optional[Dict[int, int]] = None,
    ) -> int:
        """
        Insert content ratings from /v1/product/rating-by-sku into ClickHouse.

        Args:
            shop_id: shop identifier
            ratings: list of rating dicts from API
            sku_to_product_id: optional mapping sku → product_id
        """
        if not ratings or not self._client:
            return 0

        now = datetime.utcnow()
        today = now.date()
        rows = []
        sku_map = sku_to_product_id or {}

        for item in ratings:
            sku = item.get("sku", 0)
            overall_rating = _safe_decimal(item.get("rating"))
            product_id = sku_map.get(sku, 0)

            # Extract group ratings
            groups = item.get("groups", [])
            group_ratings = {}
            for g in groups:
                key = g.get("key", "")
                rating_val = _safe_decimal(g.get("rating"))
                group_ratings[key] = rating_val

            rows.append([
                today,
                now,
                shop_id,
                sku,
                product_id,
                overall_rating,
                group_ratings.get("media", 0.0),
                group_ratings.get("text", group_ratings.get("description", 0.0)),
                group_ratings.get("attributes", 0.0),
                group_ratings.get("rich_content", 0.0),
            ])

        total = 0
        for i in range(0, len(rows), CH_BATCH_SIZE):
            batch = rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_RATING_TABLE, batch, column_names=CH_RATING_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d content rating snapshots into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        """Get content rating stats."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(sku) as unique_skus,
                min(dt) as min_date,
                max(dt) as max_date,
                avg(rating) as avg_rating
            FROM fact_ozon_content_rating FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0],
                "unique_skus": r[1],
                "min_date": str(r[2]),
                "max_date": str(r[3]),
                "avg_rating": float(r[4]),
            }
        return {}


# ── ClickHouse Promotions Loader ──────────────────────────

CH_PROMO_TABLE = "mms_analytics.fact_ozon_promotions"
CH_PROMO_COLUMNS = [
    "dt", "updated_at", "shop_id", "product_id", "offer_id",
    "promo_type", "is_enabled",
]


class OzonPromotionsLoader:
    """Insert promotion snapshots into ClickHouse fact_ozon_promotions."""

    def __init__(
        self,
        host: str = "clickhouse",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "mms_analytics",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._client: Optional[ClickHouseClient] = None

    def connect(self):
        self._client = clickhouse_connect.get_client(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            database=self.database,
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def insert_promotions(self, shop_id: int, products: List[dict]) -> int:
        """
        Extract promotions from product info and insert into ClickHouse.

        Each product can have multiple promotions → one row per promo.
        """
        if not products or not self._client:
            return 0

        now = datetime.utcnow()
        today = now.date()
        rows = []

        for item in products:
            product_id = item.get("id")
            if not product_id:
                continue

            offer_id = item.get("offer_id", "")
            promotions = item.get("promotions", [])

            for promo in promotions:
                promo_type = promo.get("type", "UNKNOWN")
                is_enabled = 1 if promo.get("is_enabled", False) else 0

                rows.append([
                    today, now, shop_id, product_id, offer_id,
                    promo_type, is_enabled,
                ])

        if not rows:
            logger.info("No promotions to insert")
            return 0

        total = 0
        for i in range(0, len(rows), CH_BATCH_SIZE):
            batch = rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_PROMO_TABLE, batch, column_names=CH_PROMO_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d promotion snapshots into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        """Get promotion stats."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(product_id) as unique_products,
                countIf(is_enabled = 1) as enabled_count,
                min(dt) as min_date,
                max(dt) as max_date
            FROM fact_ozon_promotions FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0],
                "unique_products": r[1],
                "enabled_count": r[2],
                "min_date": str(r[3]),
                "max_date": str(r[4]),
            }
        return {}


# ── ClickHouse Availability Loader ────────────────────────

CH_AVAIL_TABLE = "mms_analytics.fact_ozon_availability"
CH_AVAIL_COLUMNS = [
    "dt", "updated_at", "shop_id", "product_id", "offer_id",
    "sku", "source", "availability",
]


class OzonAvailabilityLoader:
    """Insert availability snapshots into ClickHouse fact_ozon_availability."""

    def __init__(
        self,
        host: str = "clickhouse",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "mms_analytics",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._client: Optional[ClickHouseClient] = None

    def connect(self):
        self._client = clickhouse_connect.get_client(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            database=self.database,
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def insert_availability(self, shop_id: int, products: List[dict]) -> int:
        """
        Extract availabilities from product info and insert into ClickHouse.

        Each product can have multiple availability entries (per source).
        """
        if not products or not self._client:
            return 0

        now = datetime.utcnow()
        today = now.date()
        rows = []

        for item in products:
            product_id = item.get("id")
            if not product_id:
                continue

            offer_id = item.get("offer_id", "")
            availabilities = item.get("availabilities", [])

            for avail in availabilities:
                sku = avail.get("sku", 0)
                source = avail.get("source", "")
                availability = avail.get("availability", "")

                rows.append([
                    today, now, shop_id, product_id, offer_id,
                    sku, source, availability,
                ])

        if not rows:
            logger.info("No availability data to insert")
            return 0

        total = 0
        for i in range(0, len(rows), CH_BATCH_SIZE):
            batch = rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_AVAIL_TABLE, batch, column_names=CH_AVAIL_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d availability snapshots into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        """Get availability stats."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(product_id) as unique_products,
                countIf(availability = 'AVAILABLE') as available_count,
                countIf(availability != 'AVAILABLE') as unavailable_count,
                min(dt) as min_date,
                max(dt) as max_date
            FROM fact_ozon_availability FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0],
                "unique_products": r[1],
                "available_count": r[2],
                "unavailable_count": r[3],
                "min_date": str(r[4]),
                "max_date": str(r[5]),
            }
        return {}
