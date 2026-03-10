"""
Warehouses API — supply recommendations.

GET /warehouses/ozon/supply       — JSON recommendations per SKU × cluster
GET /warehouses/ozon/supply/export — Excel download
"""
import io
import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.user import User
from app.models.shop import Shop

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/warehouses", tags=["Warehouses"])


# ══════════════════════════════════════════════════════════════
# Delivery time matrix (hours) - source: Ozon normative 01/2026
# ══════════════════════════════════════════════════════════════

DELIVERY_HOURS: dict[str, dict[str, int]] = {
    "Москва, МО и Дальние регионы": {"Москва, МО и Дальние регионы": 28, "Санкт-Петербург и СЗО": 60, "Тверь": 45, "Ярославль": 45, "Воронеж": 60, "Казань": 60, "Самара": 75, "Саратов": 60, "Оренбург": 75, "Екатеринбург": 75, "Пермь": 75, "Уфа": 75, "Красноярск": 135, "Новосибирск": 105, "Омск": 105, "Тюмень": 90, "Ростов": 60, "Краснодар": 60, "Махачкала": 75, "Невинномысск": 75, "Дальний Восток": 150, "Калининград": 60, "Беларусь": 45, "Астана": 75, "Алматы": 75},
    "Санкт-Петербург и СЗО": {"Москва, МО и Дальние регионы": 60, "Санкт-Петербург и СЗО": 28, "Тверь": 60, "Ярославль": 60, "Воронеж": 75, "Казань": 75, "Самара": 90, "Саратов": 75, "Оренбург": 75, "Екатеринбург": 90, "Пермь": 90, "Уфа": 75, "Красноярск": 150, "Новосибирск": 120, "Омск": 120, "Тюмень": 90, "Ростов": 75, "Краснодар": 75, "Махачкала": 105, "Невинномысск": 105, "Дальний Восток": 150, "Калининград": 60, "Беларусь": 75, "Астана": 90, "Алматы": 90},
    "Тверь": {"Москва, МО и Дальние регионы": 60, "Санкт-Петербург и СЗО": 60, "Тверь": 28, "Ярославль": 45, "Воронеж": 60, "Казань": 60, "Самара": 75, "Саратов": 60, "Оренбург": 75, "Екатеринбург": 75, "Пермь": 75, "Уфа": 75, "Красноярск": 150, "Новосибирск": 120, "Омск": 120, "Тюмень": 90, "Ростов": 75, "Краснодар": 75, "Махачкала": 75, "Невинномысск": 75, "Дальний Восток": 150, "Калининград": 60, "Беларусь": 60, "Астана": 75, "Алматы": 90},
    "Ярославль": {"Москва, МО и Дальние регионы": 60, "Санкт-Петербург и СЗО": 60, "Тверь": 45, "Ярославль": 28, "Воронеж": 75, "Казань": 60, "Самара": 75, "Саратов": 75, "Оренбург": 90, "Екатеринбург": 75, "Пермь": 75, "Уфа": 75, "Красноярск": 150, "Новосибирск": 120, "Омск": 120, "Тюмень": 90, "Ростов": 75, "Краснодар": 75, "Махачкала": 75, "Невинномысск": 75, "Дальний Восток": 150, "Калининград": 60, "Беларусь": 60, "Астана": 75, "Алматы": 90},
    "Воронеж": {"Москва, МО и Дальние регионы": 60, "Санкт-Петербург и СЗО": 60, "Тверь": 60, "Ярославль": 60, "Воронеж": 28, "Казань": 60, "Самара": 60, "Саратов": 45, "Оренбург": 75, "Екатеринбург": 75, "Пермь": 75, "Уфа": 75, "Красноярск": 135, "Новосибирск": 105, "Омск": 105, "Тюмень": 90, "Ростов": 45, "Краснодар": 60, "Махачкала": 60, "Невинномысск": 60, "Дальний Восток": 150, "Калининград": 75, "Беларусь": 60, "Астана": 75, "Алматы": 90},
    "Казань": {"Москва, МО и Дальние регионы": 60, "Санкт-Петербург и СЗО": 60, "Тверь": 60, "Ярославль": 60, "Воронеж": 60, "Казань": 28, "Самара": 45, "Саратов": 60, "Оренбург": 60, "Екатеринбург": 45, "Пермь": 45, "Уфа": 45, "Красноярск": 120, "Новосибирск": 105, "Омск": 90, "Тюмень": 75, "Ростов": 75, "Краснодар": 75, "Махачкала": 90, "Невинномысск": 90, "Дальний Восток": 150, "Калининград": 75, "Беларусь": 60, "Астана": 75, "Алматы": 90},
    "Самара": {"Москва, МО и Дальние регионы": 60, "Санкт-Петербург и СЗО": 75, "Тверь": 60, "Ярославль": 60, "Воронеж": 45, "Казань": 45, "Самара": 28, "Саратов": 45, "Оренбург": 45, "Екатеринбург": 60, "Пермь": 60, "Уфа": 45, "Красноярск": 120, "Новосибирск": 105, "Омск": 105, "Тюмень": 75, "Ростов": 75, "Краснодар": 75, "Махачкала": 90, "Невинномысск": 90, "Дальний Восток": 150, "Калининград": 75, "Беларусь": 75, "Астана": 75, "Алматы": 90},
    "Саратов": {"Москва, МО и Дальние регионы": 75, "Санкт-Петербург и СЗО": 75, "Тверь": 75, "Ярославль": 90, "Воронеж": 60, "Казань": 60, "Самара": 28, "Саратов": 28, "Оренбург": 75, "Екатеринбург": 75, "Пермь": 75, "Уфа": 75, "Красноярск": 120, "Новосибирск": 105, "Омск": 105, "Тюмень": 75, "Ростов": 75, "Краснодар": 75, "Махачкала": 90, "Невинномысск": 90, "Дальний Восток": 150, "Калининград": 75, "Беларусь": 75, "Астана": 75, "Алматы": 90},
    "Оренбург": {"Москва, МО и Дальние регионы": 75, "Санкт-Петербург и СЗО": 75, "Тверь": 75, "Ярославль": 90, "Воронеж": 60, "Казань": 60, "Самара": 80, "Саратов": 80, "Оренбург": 28, "Екатеринбург": 60, "Пермь": 75, "Уфа": 45, "Красноярск": 120, "Новосибирск": 105, "Омск": 90, "Тюмень": 75, "Ростов": 75, "Краснодар": 75, "Махачкала": 90, "Невинномысск": 90, "Дальний Восток": 150, "Калининград": 90, "Беларусь": 75, "Астана": 75, "Алматы": 90},
    "Екатеринбург": {"Москва, МО и Дальние регионы": 75, "Санкт-Петербург и СЗО": 90, "Тверь": 105, "Ярославль": 75, "Воронеж": 75, "Казань": 75, "Самара": 75, "Саратов": 60, "Оренбург": 60, "Екатеринбург": 28, "Пермь": 45, "Уфа": 45, "Красноярск": 90, "Новосибирск": 75, "Омск": 75, "Тюмень": 45, "Ростов": 75, "Краснодар": 75, "Махачкала": 90, "Невинномысск": 90, "Дальний Восток": 150, "Калининград": 105, "Беларусь": 90, "Астана": 45, "Алматы": 45},
    "Пермь": {"Москва, МО и Дальние регионы": 75, "Санкт-Петербург и СЗО": 90, "Тверь": 105, "Ярославль": 75, "Воронеж": 75, "Казань": 60, "Самара": 60, "Саратов": 45, "Оренбург": 28, "Екатеринбург": 45, "Пермь": 28, "Уфа": 45, "Красноярск": 90, "Новосибирск": 75, "Омск": 75, "Тюмень": 60, "Ростов": 75, "Краснодар": 75, "Махачкала": 90, "Невинномысск": 90, "Дальний Восток": 150, "Калининград": 90, "Беларусь": 90, "Астана": 60, "Алматы": 60},
    "Уфа": {"Москва, МО и Дальние регионы": 75, "Санкт-Петербург и СЗО": 75, "Тверь": 75, "Ярославль": 75, "Воронеж": 60, "Казань": 45, "Самара": 45, "Саратов": 60, "Оренбург": 45, "Екатеринбург": 45, "Пермь": 45, "Уфа": 28, "Красноярск": 120, "Новосибирск": 105, "Омск": 90, "Тюмень": 75, "Ростов": 75, "Краснодар": 75, "Махачкала": 90, "Невинномысск": 90, "Дальний Восток": 150, "Калининград": 90, "Беларусь": 75, "Астана": 75, "Алматы": 90},
    "Красноярск": {"Москва, МО и Дальние регионы": 135, "Санкт-Петербург и СЗО": 150, "Тверь": 135, "Ярославль": 135, "Воронеж": 120, "Казань": 120, "Самара": 90, "Саратов": 90, "Оренбург": 75, "Екатеринбург": 75, "Пермь": 75, "Уфа": 75, "Красноярск": 28, "Новосибирск": 45, "Омск": 45, "Тюмень": 60, "Ростов": 120, "Краснодар": 120, "Махачкала": 120, "Невинномысск": 120, "Дальний Восток": 45, "Калининград": 150, "Беларусь": 150, "Астана": 105, "Алматы": 120},
    "Новосибирск": {"Москва, МО и Дальние регионы": 105, "Санкт-Петербург и СЗО": 120, "Тверь": 105, "Ярославль": 135, "Воронеж": 105, "Казань": 75, "Самара": 90, "Саратов": 105, "Оренбург": 75, "Екатеринбург": 60, "Пермь": 75, "Уфа": 75, "Красноярск": 45, "Новосибирск": 28, "Омск": 45, "Тюмень": 60, "Ростов": 90, "Краснодар": 90, "Махачкала": 120, "Невинномысск": 120, "Дальний Восток": 150, "Калининград": 105, "Беларусь": 105, "Астана": 75, "Алматы": 75},
    "Омск": {"Москва, МО и Дальние регионы": 105, "Санкт-Петербург и СЗО": 120, "Тверь": 105, "Ярославль": 120, "Воронеж": 105, "Казань": 90, "Самара": 105, "Саратов": 105, "Оренбург": 75, "Екатеринбург": 75, "Пермь": 75, "Уфа": 75, "Красноярск": 45, "Новосибирск": 45, "Омск": 28, "Тюмень": 60, "Ростов": 120, "Краснодар": 120, "Махачкала": 105, "Невинномысск": 105, "Дальний Восток": 45, "Калининград": 135, "Беларусь": 120, "Астана": 75, "Алматы": 75},
    "Тюмень": {"Москва, МО и Дальние регионы": 90, "Санкт-Петербург и СЗО": 90, "Тверь": 90, "Ярославль": 120, "Воронеж": 90, "Казань": 90, "Самара": 90, "Саратов": 90, "Оренбург": 75, "Екатеринбург": 45, "Пермь": 60, "Уфа": 75, "Красноярск": 75, "Новосибирск": 60, "Омск": 60, "Тюмень": 28, "Ростов": 90, "Краснодар": 90, "Махачкала": 105, "Невинномысск": 105, "Дальний Восток": 150, "Калининград": 120, "Беларусь": 90, "Астана": 45, "Алматы": 45},
    "Ростов": {"Москва, МО и Дальние регионы": 75, "Санкт-Петербург и СЗО": 75, "Тверь": 75, "Ярославль": 90, "Воронеж": 45, "Казань": 75, "Самара": 90, "Саратов": 60, "Оренбург": 75, "Екатеринбург": 75, "Пермь": 75, "Уфа": 75, "Красноярск": 135, "Новосибирск": 120, "Омск": 120, "Тюмень": 90, "Ростов": 28, "Краснодар": 45, "Махачкала": 60, "Невинномысск": 45, "Дальний Восток": 150, "Калининград": 75, "Беларусь": 90, "Астана": 105, "Алматы": 105},
    "Краснодар": {"Москва, МО и Дальние регионы": 75, "Санкт-Петербург и СЗО": 75, "Тверь": 75, "Ярославль": 90, "Воронеж": 60, "Казань": 75, "Самара": 90, "Саратов": 60, "Оренбург": 75, "Екатеринбург": 75, "Пермь": 75, "Уфа": 75, "Красноярск": 135, "Новосибирск": 120, "Омск": 120, "Тюмень": 90, "Ростов": 45, "Краснодар": 28, "Махачкала": 60, "Невинномысск": 60, "Дальний Восток": 150, "Калининград": 75, "Беларусь": 90, "Астана": 90, "Алматы": 90},
    "Махачкала": {"Москва, МО и Дальние регионы": 75, "Санкт-Петербург и СЗО": 75, "Тверь": 75, "Ярославль": 75, "Воронеж": 60, "Казань": 90, "Самара": 105, "Саратов": 105, "Оренбург": 90, "Екатеринбург": 90, "Пермь": 90, "Уфа": 90, "Красноярск": 135, "Новосибирск": 120, "Омск": 90, "Тюмень": 90, "Ростов": 60, "Краснодар": 60, "Махачкала": 28, "Невинномысск": 45, "Дальний Восток": 150, "Калининград": 90, "Беларусь": 120, "Астана": 90, "Алматы": 120},
    "Невинномысск": {"Москва, МО и Дальние регионы": 75, "Санкт-Петербург и СЗО": 75, "Тверь": 75, "Ярославль": 75, "Воронеж": 60, "Казань": 90, "Самара": 105, "Саратов": 105, "Оренбург": 90, "Екатеринбург": 90, "Пермь": 90, "Уфа": 90, "Красноярск": 135, "Новосибирск": 120, "Омск": 90, "Тюмень": 90, "Ростов": 45, "Краснодар": 45, "Махачкала": 45, "Невинномысск": 28, "Дальний Восток": 150, "Калининград": 90, "Беларусь": 90, "Астана": 90, "Алматы": 120},
    "Дальний Восток": {"Москва, МО и Дальние регионы": 150, "Санкт-Петербург и СЗО": 150, "Тверь": 150, "Ярославль": 150, "Воронеж": 150, "Казань": 150, "Самара": 150, "Саратов": 150, "Оренбург": 150, "Екатеринбург": 150, "Пермь": 150, "Уфа": 150, "Красноярск": 120, "Новосибирск": 150, "Омск": 150, "Тюмень": 150, "Ростов": 150, "Краснодар": 150, "Махачкала": 150, "Невинномысск": 150, "Дальний Восток": 28, "Калининград": 150, "Беларусь": 150, "Астана": 150, "Алматы": 150},
    "Калининград": {"Москва, МО и Дальние регионы": 999, "Санкт-Петербург и СЗО": 999, "Тверь": 999, "Ярославль": 999, "Воронеж": 999, "Казань": 999, "Самара": 999, "Саратов": 999, "Оренбург": 999, "Екатеринбург": 999, "Пермь": 999, "Уфа": 999, "Красноярск": 999, "Новосибирск": 999, "Омск": 999, "Тюмень": 999, "Ростов": 999, "Краснодар": 999, "Махачкала": 999, "Невинномысск": 999, "Дальний Восток": 999, "Калининград": 28, "Беларусь": 999, "Астана": 999, "Алматы": 999},
    "Беларусь": {"Москва, МО и Дальние регионы": 60, "Санкт-Петербург и СЗО": 60, "Тверь": 60, "Ярославль": 60, "Воронеж": 60, "Казань": 75, "Самара": 90, "Саратов": 75, "Оренбург": 105, "Екатеринбург": 90, "Пермь": 90, "Уфа": 105, "Красноярск": 150, "Новосибирск": 120, "Омск": 120, "Тюмень": 90, "Ростов": 75, "Краснодар": 75, "Махачкала": 105, "Невинномысск": 105, "Дальний Восток": 150, "Калининград": 60, "Беларусь": 28, "Астана": 90, "Алматы": 90},
    "Астана": {"Москва, МО и Дальние регионы": 90, "Санкт-Петербург и СЗО": 120, "Тверь": 90, "Ярославль": 150, "Воронеж": 90, "Казань": 105, "Самара": 105, "Саратов": 90, "Оренбург": 105, "Екатеринбург": 60, "Пермь": 60, "Уфа": 105, "Красноярск": 105, "Новосибирск": 75, "Омск": 75, "Тюмень": 75, "Ростов": 105, "Краснодар": 105, "Махачкала": 150, "Невинномысск": 150, "Дальний Восток": 150, "Калининград": 120, "Беларусь": 105, "Астана": 28, "Алматы": 28},
    "Алматы": {"Москва, МО и Дальние регионы": 90, "Санкт-Петербург и СЗО": 120, "Тверь": 90, "Ярославль": 150, "Воронеж": 90, "Казань": 105, "Самара": 105, "Саратов": 90, "Оренбург": 105, "Екатеринбург": 60, "Пермь": 60, "Уфа": 105, "Красноярск": 105, "Новосибирск": 75, "Омск": 75, "Тюмень": 75, "Ростов": 105, "Краснодар": 105, "Махачкала": 150, "Невинномысск": 150, "Дальний Восток": 150, "Калининград": 120, "Беларусь": 105, "Астана": 28, "Алматы": 28},
}


def _resolve_hub(demand_cluster: str) -> tuple[str, int]:
    """
    For a demand cluster, find the optimal hub (source) with min delivery time.
    Priority: self (28h) is always best; returns (hub_name, hours).
    """
    best_hub = demand_cluster
    best_hours = 28  # self-delivery
    for source, dests in DELIVERY_HOURS.items():
        hours = dests.get(demand_cluster, 999)
        if hours < best_hours:
            best_hours = hours
            best_hub = source
    return best_hub, best_hours


# Объединённые группы: hub → [clusters served]
# Вместо 25 отдельных поставок — 9 точек отгрузки
CONSOLIDATED_GROUPS: dict[str, list[str]] = {
    "Москва, МО и Дальние регионы": ["Москва, МО и Дальние регионы", "Тверь", "Ярославль", "Беларусь"],
    "Санкт-Петербург и СЗО": ["Санкт-Петербург и СЗО"],
    "Казань": ["Казань", "Самара", "Уфа"],
    "Екатеринбург": ["Екатеринбург", "Пермь", "Тюмень", "Оренбург"],
    "Воронеж": ["Воронеж", "Саратов"],
    "Ростов": ["Ростов", "Краснодар", "Невинномысск", "Махачкала"],
    "Красноярск": ["Красноярск", "Новосибирск", "Омск", "Дальний Восток"],
    "Калининград": ["Калининград"],
    "Астана": ["Астана", "Алматы"],
}

# Reverse mapping: cluster → consolidated hub
_CLUSTER_TO_GROUP = {cl: hub for hub, cls in CONSOLIDATED_GROUPS.items() for cl in cls}


# ══════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════

def _compute_supply(
    ch,
    shop_id: int,
    sales_period: int,
    target_days: int,
    safety: float,
    use_ad_boost: bool,
    product_info: dict,
):
    """
    Core computation: returns (kpi_dict, items_list).
    product_info: {offer_id: {name, image_url, sku}} from PostgreSQL.
    """

    today = date.today()
    d_sales_start = today - timedelta(days=sales_period)

    # ── 1. FBO stocks (latest snapshot) ──────────────────────
    fbo_rows = ch.query("""
        SELECT offer_id,
               sum(free_to_sell) AS fbo_free,
               sum(reserved)     AS fbo_reserved,
               count()           AS wh_count
        FROM mms_analytics.fact_ozon_warehouse_stocks FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND warehouse_type = 'fbo'
          AND dt = (
              SELECT max(dt)
              FROM mms_analytics.fact_ozon_warehouse_stocks
              WHERE shop_id = {shop_id:UInt32} AND warehouse_type = 'fbo'
          )
        GROUP BY offer_id
    """, parameters={"shop_id": shop_id})

    fbo_stock = {}
    for r in fbo_rows.result_rows:
        fbo_stock[r[0]] = {"free": r[1], "reserved": r[2], "wh_count": r[3]}

    # ── 2. Sales per SKU × cluster ───────────────────────────
    sales_rows = ch.query("""
        SELECT offer_id, cluster_to,
               sum(quantity)              AS qty,
               round(sum(price*quantity)) AS revenue,
               count(DISTINCT toDate(order_date)) AS active_days
        FROM mms_analytics.fact_ozon_orders FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND order_date >= {d_start:Date}
          AND status NOT IN ('cancelled', 'canceled')
        GROUP BY offer_id, cluster_to
        ORDER BY offer_id, qty DESC
    """, parameters={"shop_id": shop_id, "d_start": d_sales_start})

    sku_clusters = {}   # offer_id → [{cluster, qty, revenue, active_days}]
    sku_totals = {}     # offer_id → total qty
    for r in sales_rows.result_rows:
        offer, cluster, qty, rev, days = r
        sku_clusters.setdefault(offer, [])
        sku_clusters[offer].append({
            "cluster": cluster,
            "qty": qty,
            "revenue": float(rev),
            "active_days": days,
        })
        sku_totals[offer] = sku_totals.get(offer, 0) + qty

    # ── 3. Ad boost (7d vs prev 7d) ─────────────────────────
    boost_map = {}
    if use_ad_boost:
        sales_7d_rows = ch.query("""
            SELECT offer_id,
                   sumIf(quantity, order_date >= today() - 7)  AS qty_7d,
                   sumIf(quantity, order_date <  today() - 7)  AS qty_prev7d
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND order_date >= today() - 14
              AND status NOT IN ('cancelled', 'canceled')
            GROUP BY offer_id
        """, parameters={"shop_id": shop_id})
        s7 = {r[0]: {"q7": r[1], "qp7": r[2]} for r in sales_7d_rows.result_rows}

        # Build sku→offer_id mapping for ads
        offer_to_sku = {}
        for oid, info in product_info.items():
            if info.get("sku"):
                offer_to_sku[oid] = info["sku"]
        sku_to_offer = {v: k for k, v in offer_to_sku.items()}

        sku_list = list(sku_to_offer.keys())
        ad_data = {}
        if sku_list:
            ads_rows = ch.query("""
                SELECT sku,
                       sumIf(money_spent, dt >= today() - 7)  AS ad_7d,
                       sumIf(money_spent, dt <  today() - 7 AND dt >= today() - 14) AS ad_prev7d,
                       sumIf(views, dt >= today() - 7)        AS views_7d,
                       sumIf(clicks, dt >= today() - 7)       AS clicks_7d,
                       sumIf(orders, dt >= today() - 7)       AS ad_orders_7d,
                       sumIf(add_to_cart, dt >= today() - 7)  AS carts_7d
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND dt >= today() - 14
                GROUP BY sku
            """, parameters={"shop_id": shop_id})
            for r in ads_rows.result_rows:
                oid = sku_to_offer.get(r[0])
                if oid:
                    ad_data[oid] = {
                        "spend_7d": float(r[1]),
                        "spend_prev": float(r[2]),
                        "views": r[3], "clicks": r[4],
                        "orders": r[5], "carts": r[6],
                    }

        for offer in sku_totals:
            s = s7.get(offer, {"q7": 0, "qp7": 0})
            a = ad_data.get(offer, {"spend_7d": 0})
            has_ads = a["spend_7d"] > 0
            if has_ads and s["qp7"] > 0 and s["q7"] > s["qp7"]:
                boost_map[offer] = min(s["q7"] / s["qp7"], 2.0)
            elif has_ads and s["qp7"] == 0 and s["q7"] > 0:
                boost_map[offer] = 1.3
            else:
                boost_map[offer] = 1.0
    else:
        ad_data = {}

    # ── 4. Build recommendations ─────────────────────────────
    items = []
    total_need = 0
    critical_count = 0
    attention_count = 0
    weighted_days_sum = 0.0
    weighted_rev_sum = 0.0

    all_offers = set(list(sku_totals.keys()) + list(fbo_stock.keys()))

    for offer in all_offers:
        stock_info = fbo_stock.get(offer, {"free": 0, "reserved": 0, "wh_count": 0})
        stock = stock_info["free"]
        sold = sku_totals.get(offer, 0)
        boost = boost_map.get(offer, 1.0)
        daily = sold / sales_period if sales_period > 0 else 0
        boosted_daily = daily * boost
        days_supply = stock / boosted_daily if boosted_daily > 0 else 9999

        info = product_info.get(offer, {})
        ad = ad_data.get(offer, {})
        revenue = sum(c["revenue"] for c in sku_clusters.get(offer, []))

        # Status
        if days_supply < 14:
            item_status = "critical"
            critical_count += 1
        elif days_supply < target_days:
            item_status = "attention"
            attention_count += 1
        else:
            item_status = "ok"

        # Weighted days for KPI
        if revenue > 0:
            weighted_days_sum += days_supply * revenue
            weighted_rev_sum += revenue

        # Per-cluster recs
        clusters_out = []
        item_need = 0
        for cl in sku_clusters.get(offer, []):
            share = cl["qty"] / sold if sold > 0 else 0
            cl_stock = stock * share
            cl_daily = (cl["qty"] / sales_period) * boost
            cl_need = max(0, round(cl_daily * target_days * safety - cl_stock))
            item_need += cl_need
            hub_name, hub_hours = _resolve_hub(cl["cluster"])
            clusters_out.append({
                "cluster": cl["cluster"],
                "sold": cl["qty"],
                "share": round(share * 100, 1),
                "daily": round(cl["qty"] / sales_period, 2),
                "daily_boosted": round(cl_daily, 2),
                "est_stock": round(cl_stock),
                "need": cl_need,
                "revenue": cl["revenue"],
                "hub": hub_name,
                "hub_hours": hub_hours,
            })

        total_need += item_need

        items.append({
            "offer_id": offer,
            "name": info.get("name", offer),
            "image_url": info.get("image_url", ""),
            "sku": info.get("sku", 0),
            "sold": sold,
            "revenue": revenue,
            "fbo_stock": stock,
            "fbo_reserved": stock_info["reserved"],
            "fbo_warehouses": stock_info["wh_count"],
            "daily_avg": round(daily, 2),
            "boost": round(boost, 2),
            "boosted_daily": round(boosted_daily, 2),
            "days_supply": round(days_supply, 1),
            "status": item_status,
            "total_need": item_need,
            # Ad info
            "ad_spend_7d": ad.get("spend_7d", 0),
            "ad_views_7d": ad.get("views", 0),
            "ad_clicks_7d": ad.get("clicks", 0),
            "ad_orders_7d": ad.get("orders", 0),
            "ad_carts_7d": ad.get("carts", 0),
            "clusters": clusters_out,
        })

    # Sort: critical first, then by days_supply ascending
    items.sort(key=lambda x: (
        0 if x["status"] == "critical" else 1 if x["status"] == "attention" else 2,
        x["days_supply"],
    ))

    avg_days = round(weighted_days_sum / weighted_rev_sum, 1) if weighted_rev_sum > 0 else 0

    kpi = {
        "total_need": total_need,
        "critical_count": critical_count,
        "attention_count": attention_count,
        "avg_days_supply": avg_days,
        "total_fbo": sum(s["free"] for s in fbo_stock.values()),
        "total_sku": len(all_offers),
    }

    # ── 5. Build hub-level aggregation ──────────────────────────
    hubs_map: dict[str, dict] = {}  # hub_name → {items: [...], total_need, total_revenue}
    for item in items:
        for cl in item["clusters"]:
            if cl["need"] <= 0:
                continue
            hub = cl["hub"]
            if hub not in hubs_map:
                hubs_map[hub] = {"hub": hub, "items": [], "total_need": 0, "total_revenue": 0.0}
            hubs_map[hub]["items"].append({
                "offer_id": item["offer_id"],
                "name": item["name"],
                "image_url": item["image_url"],
                "cluster": cl["cluster"],
                "need": cl["need"],
                "revenue": cl["revenue"],
                "hub_hours": cl["hub_hours"],
                "daily_boosted": cl["daily_boosted"],
            })
            hubs_map[hub]["total_need"] += cl["need"]
            hubs_map[hub]["total_revenue"] += cl["revenue"]

    hubs = sorted(hubs_map.values(), key=lambda h: h["total_need"], reverse=True)

    return kpi, items, hubs


# ══════════════════════════════════════════════════════════════
# GET /warehouses/ozon/supply
# ══════════════════════════════════════════════════════════════

@router.get("/ozon/supply")
async def get_ozon_supply(
    shop_id: int = Query(..., description="Shop ID"),
    sales_period: int = Query(30, ge=7, le=90, description="Sales period for avg daily calc"),
    target_days: int = Query(60, ge=14, le=90, description="Target stock horizon"),
    safety: float = Query(1.15, ge=1.0, le=2.0, description="Safety coefficient"),
    use_ad_boost: bool = Query(True, description="Apply ad boost coefficient"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Supply recommendations per SKU × cluster for Ozon FBO."""

    # Verify shop
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

    # Product info from PostgreSQL
    pg_result = await db.execute(text("""
        SELECT offer_id, name, sku,
               COALESCE(NULLIF(primary_image_url, ''), main_image_url, '') AS image_url
        FROM dim_ozon_products
        WHERE shop_id = :shop_id
    """), {"shop_id": shop_id})

    product_info = {}
    for r in pg_result.fetchall():
        product_info[r[0]] = {"name": r[1] or r[0], "sku": r[2], "image_url": r[3] or ""}

    kpi, items, hubs = _compute_supply(
        ch, shop_id, sales_period, target_days, safety, use_ad_boost, product_info,
    )

    return {
        "shop_id": shop_id,
        "sales_period": sales_period,
        "target_days": target_days,
        "safety": safety,
        "use_ad_boost": use_ad_boost,
        "kpi": kpi,
        "items": items,
        "hubs": hubs,
    }


# ══════════════════════════════════════════════════════════════
# GET /warehouses/ozon/supply/export — Excel
# ══════════════════════════════════════════════════════════════

@router.get("/ozon/supply/export")
async def export_ozon_supply(
    shop_id: int = Query(...),
    sales_period: int = Query(30, ge=7, le=90),
    target_days: int = Query(60, ge=14, le=90),
    safety: float = Query(1.15, ge=1.0, le=2.0),
    use_ad_boost: bool = Query(True),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Download supply recommendations as Excel."""

    # Verify shop
    shop_result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = shop_result.scalar_one_or_none()
    if not shop or shop.marketplace != "ozon":
        raise HTTPException(status_code=404, detail="Shop not found")

    from app.core.clickhouse import get_clickhouse_client
    ch = get_clickhouse_client()

    pg_result = await db.execute(text("""
        SELECT offer_id, name, sku,
               COALESCE(NULLIF(primary_image_url, ''), main_image_url, '') AS image_url
        FROM dim_ozon_products
        WHERE shop_id = :shop_id
    """), {"shop_id": shop_id})
    product_info = {}
    for r in pg_result.fetchall():
        product_info[r[0]] = {"name": r[1] or r[0], "sku": r[2], "image_url": r[3] or ""}

    kpi, items, hubs = _compute_supply(
        ch, shop_id, sales_period, target_days, safety, use_ad_boost, product_info,
    )

    # ── Build Excel ──────────────────────────────────────────
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    wb = openpyxl.Workbook()

    # === Sheet 1: Рекомендации ===
    ws = wb.active
    ws.title = "Рекомендации"

    hdr_font = Font(bold=True, size=11, color="FFFFFF")
    hdr_fill = PatternFill("solid", fgColor="2F5496")
    red_fill = PatternFill("solid", fgColor="FFC7CE")
    yellow_fill = PatternFill("solid", fgColor="FFEB9C")
    green_fill = PatternFill("solid", fgColor="C6EFCE")
    need_fill = PatternFill("solid", fgColor="FFF2CC")
    sku_fill = PatternFill("solid", fgColor="D9E2F3")
    sku_font = Font(bold=True, size=11)
    boost_font = Font(bold=True, color="FF6600")
    thin = Side(style="thin", color="D0D0D0")
    num_fmt = "#,##0"
    pct_fmt = "0.0%"

    from openpyxl.utils import get_column_letter

    headers = [
        ("Статус", 12), ("Артикул", 24), ("Кластер", 40),
        ("Склад отгрузки", 28), ("Доставка, ч", 10),
        ("Продано", 10), ("Доля", 8), ("Ежедн.", 8),
        ("Boost", 7), ("Ежедн×b", 8), ("FBO", 8),
        ("Оц.стока", 8), ("Дн.зап", 8), ("ПОСТАВИТЬ", 12),
        ("Выручка", 12), ("Рекл₽", 10), ("Показы", 8),
        ("Клики", 7), ("Корзины", 8), ("Р.заказы", 8),
    ]
    for ci, (name, w) in enumerate(headers, 1):
        c = ws.cell(1, ci, name)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws.column_dimensions[get_column_letter(ci)].width = w

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"

    row = 2
    for item in items:
        status_label = {"critical": "🔴 Крит.", "attention": "🟡 Вним.", "ok": "🟢 Норма"}[item["status"]]
        s_fill = {"critical": red_fill, "attention": yellow_fill, "ok": green_fill}[item["status"]]
        boost_str = f"×{item['boost']:.2f}" if item["boost"] > 1.0 else "-"
        first = True

        for cl in item["clusters"]:
            r = row
            if first:
                ws.cell(r, 1, status_label).fill = s_fill
                ws.cell(r, 1).font = Font(bold=True, size=10)
                c = ws.cell(r, 2, item["offer_id"])
                c.font = sku_font
                c.fill = sku_fill
            else:
                ws.cell(r, 1, "")
                ws.cell(r, 2, "")

            ws.cell(r, 3, cl["cluster"])
            ws.cell(r, 4, cl["hub"])
            c_dh = ws.cell(r, 5, cl["hub_hours"])
            if cl["hub_hours"] <= 28:
                c_dh.font = Font(bold=True, color="00AA00")
            elif cl["hub_hours"] <= 45:
                c_dh.font = Font(bold=True, color="CC8800")
            ws.cell(r, 6, cl["sold"]).number_format = num_fmt
            ws.cell(r, 7, cl["share"] / 100).number_format = pct_fmt
            ws.cell(r, 8, cl["daily"]).number_format = "0.00"

            if first:
                c = ws.cell(r, 9, boost_str)
                if item["boost"] > 1.0:
                    c.font = boost_font
            else:
                ws.cell(r, 9, "")

            ws.cell(r, 10, cl["daily_boosted"]).number_format = "0.00"

            if first:
                ws.cell(r, 11, item["fbo_stock"]).number_format = num_fmt
            else:
                ws.cell(r, 11, "")

            ws.cell(r, 12, cl["est_stock"]).number_format = num_fmt

            if first:
                c = ws.cell(r, 13, round(item["days_supply"], 1))
                c.number_format = "0.0"
                if item["days_supply"] < 14:
                    c.font = Font(bold=True, color="CC0000")
            else:
                ws.cell(r, 13, "")

            c = ws.cell(r, 14, cl["need"])
            c.number_format = num_fmt
            if cl["need"] > 0:
                c.font = Font(bold=True, size=12, color="CC0000")
                c.fill = need_fill

            ws.cell(r, 15, cl["revenue"]).number_format = num_fmt

            if first:
                ws.cell(r, 16, item.get("ad_spend_7d", 0)).number_format = num_fmt
                ws.cell(r, 17, item.get("ad_views_7d", 0)).number_format = num_fmt
                ws.cell(r, 18, item.get("ad_clicks_7d", 0)).number_format = num_fmt
                ws.cell(r, 19, item.get("ad_carts_7d", 0)).number_format = num_fmt
                ws.cell(r, 20, item.get("ad_orders_7d", 0)).number_format = num_fmt
                for ci2 in range(1, len(headers) + 1):
                    ws.cell(r, ci2).border = Border(top=Side(style="medium", color="2F5496"))

            first = False
            row += 1

    # === Sheet 2: Сводка ===
    ws2 = wb.create_sheet("Сводка")
    s_hdr = [
        ("Статус", 12), ("Артикул", 24), ("Продано", 10), ("Выручка", 14),
        ("FBO", 8), ("Ежедн.", 8), ("Boost", 7), ("Дн.зап", 8),
        ("ПОСТАВИТЬ", 12), ("Кластеров", 9),
    ]
    for ci, (name, w) in enumerate(s_hdr, 1):
        c = ws2.cell(1, ci, name)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws2.column_dimensions[get_column_letter(ci)].width = w
    ws2.freeze_panes = "A2"

    for ri, item in enumerate(items, 2):
        sl = {"critical": "🔴 Крит.", "attention": "🟡 Вним.", "ok": "🟢 Норма"}[item["status"]]
        sf = {"critical": red_fill, "attention": yellow_fill, "ok": green_fill}[item["status"]]
        vals = [
            sl, item["offer_id"], item["sold"], item["revenue"],
            item["fbo_stock"], item["daily_avg"],
            f"×{item['boost']:.2f}" if item["boost"] > 1 else "-",
            round(item["days_supply"], 1), item["total_need"], len(item["clusters"]),
        ]
        for ci, v in enumerate(vals, 1):
            c = ws2.cell(ri, ci, v)
            if ci == 1:
                c.fill = sf
                c.font = Font(bold=True)
            if ci in (3, 4, 5, 9):
                c.number_format = num_fmt
            if ci == 9 and v > 0:
                c.font = Font(bold=True, size=12, color="CC0000")
                c.fill = need_fill

    # === Sheet 3: Методология ===
    ws3 = wb.create_sheet("Методология")
    ws3.column_dimensions["A"].width = 90
    lines = [
        ("МЕТОДОЛОГИЯ РЕКОМЕНДАЦИИ ПОСТАВКИ FBO", True, 14),
        ("", False, 11),
        (f"Магазин: {shop.name or shop_id} | Период продаж: {sales_period}д | Target: {target_days}д | Safety: {safety} | Ad boost: {'ДА' if use_ad_boost else 'НЕТ'}", False, 11),
        ("", False, 11),
        ("═══ ФОРМУЛА ═══", True, 12),
        ("daily_cluster = продажи_кластер / период × ad_boost", False, 11),
        (f"target_stock = daily_cluster × {target_days} дней × {safety} (safety)", False, 11),
        ("supply_need = max(0, target_stock − FBO_stock × доля_кластера)", False, 11),
        ("", False, 11),
        ("═══ ПАРАМЕТРЫ ═══", True, 12),
        (f"• Target = {target_days} дней", False, 11),
        (f"• Safety = ×{safety} ({round((safety-1)*100)}%)", False, 11),
        ("• Распределение: пропорционально продажам по кластерам", False, 11),
        ("", False, 11),
        ("═══ AD BOOST ═══", True, 12),
        ("Реклама + рост продаж → boost = min(рост_7д, 2.0x)", False, 11),
        ("Реклама + новый SKU → boost = 1.3x", False, 11),
        ("Нет рекламы → boost = 1.0", False, 11),
        ("", False, 11),
        ("═══ СТАТУСЫ ═══", True, 12),
        ("🔴 Критический — запас < 14 дней", False, 11),
        (f"🟡 Внимание — запас < {target_days} дней", False, 11),
        (f"🟢 Норма — запас ≥ {target_days} дней", False, 11),
    ]
    for ri, (t, bold, sz) in enumerate(lines, 1):
        c = ws3.cell(ri, 1, t)
        c.font = Font(bold=bold, size=sz)
        if "═══" in t:
            c.fill = PatternFill("solid", fgColor="D9E2F3")

    # === Sheet 4: Поставка по кластерам ===
    ws4 = wb.create_sheet("Поставка по кластерам")
    h4_headers = [
        ("Склад отгрузки (хаб)", 28), ("Артикул", 24), ("Название", 44),
        ("Кластер спроса", 28), ("Время доставки, ч", 14),
        ("Ежедн.×b", 10), ("ПОСТАВИТЬ", 12), ("Выручка", 12),
    ]
    for ci, (name, w) in enumerate(h4_headers, 1):
        c = ws4.cell(1, ci, name)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws4.column_dimensions[get_column_letter(ci)].width = w
    ws4.freeze_panes = "A2"

    hub_fill = PatternFill("solid", fgColor="BDD7EE")
    hub_font = Font(bold=True, size=11, color="1F4E79")
    row4 = 2
    for hub_data in hubs:
        # Hub header row
        c = ws4.cell(row4, 1, f"📦 {hub_data['hub']}")
        c.font = hub_font
        c.fill = hub_fill
        ws4.cell(row4, 7, hub_data["total_need"]).font = Font(bold=True, size=12, color="CC0000")
        ws4.cell(row4, 7).number_format = num_fmt
        ws4.cell(row4, 8, round(hub_data["total_revenue"])).number_format = num_fmt
        for ci2 in range(1, len(h4_headers) + 1):
            ws4.cell(row4, ci2).fill = hub_fill
        row4 += 1

        # Items in this hub
        for hi in hub_data["items"]:
            ws4.cell(row4, 1, "")
            ws4.cell(row4, 2, hi["offer_id"])
            ws4.cell(row4, 3, hi["name"])
            ws4.cell(row4, 4, hi["cluster"])
            ws4.cell(row4, 5, hi["hub_hours"])
            ws4.cell(row4, 6, hi["daily_boosted"]).number_format = "0.00"
            c = ws4.cell(row4, 7, hi["need"])
            c.number_format = num_fmt
            if hi["need"] > 0:
                c.font = Font(bold=True, color="CC0000")
                c.fill = need_fill
            ws4.cell(row4, 8, round(hi["revenue"])).number_format = num_fmt
            row4 += 1

        row4 += 1  # blank row between hubs

    # === Sheet 5: Объединённые кластеры ===
    ws5 = wb.create_sheet("Объединённые кластеры")
    h5_headers = [
        ("Хаб отгрузки", 30), ("Обслуживаемые кластеры", 50),
        ("Артикул", 24), ("Название", 44),
        ("Кластер спроса", 28), ("Время доставки, ч", 14),
        ("ПОСТАВИТЬ", 12), ("Выручка", 12),
    ]
    for ci, (name, w) in enumerate(h5_headers, 1):
        c = ws5.cell(1, ci, name)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws5.column_dimensions[get_column_letter(ci)].width = w
    ws5.freeze_panes = "A2"

    # Build consolidated data: group_hub → {sku → [{cluster, need, revenue, hours}]}
    consol: dict[str, dict] = {}  # hub → {items: {offer_id → [...]}, total_need, ...}
    for item in items:
        for cl in item["clusters"]:
            if cl["need"] <= 0:
                continue
            group_hub = _CLUSTER_TO_GROUP.get(cl["cluster"], cl["cluster"])
            if group_hub not in consol:
                consol[group_hub] = {"items": {}, "total_need": 0, "total_revenue": 0.0}
            g = consol[group_hub]
            g["total_need"] += cl["need"]
            g["total_revenue"] += cl["revenue"]
            oid = item["offer_id"]
            if oid not in g["items"]:
                g["items"][oid] = {"name": item["name"], "clusters": []}
            delivery_h = DELIVERY_HOURS.get(group_hub, {}).get(cl["cluster"], 999)
            g["items"][oid]["clusters"].append({
                "cluster": cl["cluster"],
                "need": cl["need"],
                "revenue": cl["revenue"],
                "hours": delivery_h,
            })

    # Sort by total_need desc
    sorted_groups = sorted(consol.items(), key=lambda x: x[1]["total_need"], reverse=True)

    grp_fill = PatternFill("solid", fgColor="E2EFDA")
    grp_font = Font(bold=True, size=12, color="375623")
    sub_fill = PatternFill("solid", fgColor="F2F7ED")
    row5 = 2

    for group_hub, gdata in sorted_groups:
        members = CONSOLIDATED_GROUPS.get(group_hub, [group_hub])
        members_str = ", ".join(members)

        # Group header row
        c = ws5.cell(row5, 1, f"📦 {group_hub}")
        c.font = grp_font
        ws5.cell(row5, 2, members_str).font = Font(italic=True, size=10, color="666666")
        ws5.cell(row5, 7, gdata["total_need"]).font = Font(bold=True, size=13, color="CC0000")
        ws5.cell(row5, 7).number_format = num_fmt
        ws5.cell(row5, 8, round(gdata["total_revenue"])).number_format = num_fmt
        for ci2 in range(1, len(h5_headers) + 1):
            ws5.cell(row5, ci2).fill = grp_fill
        row5 += 1

        # Items in this group
        for oid, odata in gdata["items"].items():
            first_cl = True
            for cl_info in odata["clusters"]:
                if first_cl:
                    ws5.cell(row5, 3, oid).font = Font(bold=True, size=10)
                    ws5.cell(row5, 4, odata["name"])
                else:
                    ws5.cell(row5, 3, "")
                    ws5.cell(row5, 4, "")

                ws5.cell(row5, 5, cl_info["cluster"])
                c_h = ws5.cell(row5, 6, cl_info["hours"])
                if cl_info["hours"] <= 28:
                    c_h.font = Font(bold=True, color="00AA00")
                elif cl_info["hours"] <= 45:
                    c_h.font = Font(bold=True, color="CC8800")
                else:
                    c_h.font = Font(color="CC0000")

                c = ws5.cell(row5, 7, cl_info["need"])
                c.number_format = num_fmt
                if cl_info["need"] > 0:
                    c.font = Font(bold=True, color="CC0000")
                    c.fill = need_fill

                ws5.cell(row5, 8, round(cl_info["revenue"])).number_format = num_fmt

                first_cl = False
                row5 += 1

        row5 += 1  # blank row

    # === Sheet 6: Анализ логистики (по объединённым группам) ===
    THRESHOLD_HOURS = 29  # Ozon recommended avg delivery time

    ws6 = wb.create_sheet("Анализ логистики")
    h6_headers = [
        ("Артикул", 24), ("Название", 40),
        ("Хаб отгрузки", 28), ("Кластер спроса", 28),
        ("Продано (шт)", 10), ("Время от хаба, ч", 12),
        ("Превышает 29ч", 10), ("Влияние (шт×ч)", 14), ("Доля влияния %", 12),
        ("Ср. время (взвеш.)", 14), ("Рекомендация", 55),
    ]
    for ci, (name, w) in enumerate(h6_headers, 1):
        c = ws6.cell(1, ci, name)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws6.column_dimensions[get_column_letter(ci)].width = w
    ws6.freeze_panes = "A2"

    # Threshold info row
    ws6.cell(2, 1, "⚙ Методика Ozon: порог 29ч").font = Font(bold=True, size=11, color="1F4E79")
    ws6.cell(2, 2, (
        "Анализ по объединённым группам. "
        "Когда товар грузится в 1 хаб (напр. Москва), "
        "маршруты в дальние кластеры (Тверь 45ч, Ярославль 45ч) увеличивают среднее время. "
        "Доля влияния показывает, какие маршруты дороже всего."
    )).font = Font(italic=True, size=9, color="666666")
    for ci2 in range(1, len(h6_headers) + 1):
        ws6.cell(2, ci2).fill = PatternFill("solid", fgColor="DAEEF3")
    row6 = 4

    high_fill = PatternFill("solid", fgColor="FCE4D6")
    critical_font = Font(bold=True, color="CC0000")
    warn_font = Font(bold=True, color="CC8800")
    ok_font = Font(color="00AA00")

    for item in items:
        if not item["clusters"] or item["sold"] <= 0:
            continue

        # Group clusters by consolidated hub
        hub_groups: dict[str, list] = {}  # hub_name → [{cluster, vol, hours_from_hub}]
        for cl in item["clusters"]:
            if cl["sold"] <= 0:
                continue
            group_hub = _CLUSTER_TO_GROUP.get(cl["cluster"], cl["cluster"])
            hours_from_hub = DELIVERY_HOURS.get(group_hub, {}).get(cl["cluster"], 60)
            if group_hub not in hub_groups:
                hub_groups[group_hub] = []
            hub_groups[group_hub].append({
                "cluster": cl["cluster"],
                "vol": cl["sold"],
                "hours": hours_from_hub,
            })

        if not hub_groups:
            continue

        # Calculate influence across ALL routes for this SKU
        all_routes = []
        for hub_name, members in hub_groups.items():
            for m in members:
                exceeds = m["hours"] > THRESHOLD_HOURS
                influence = m["vol"] * m["hours"] if exceeds else 0
                all_routes.append({
                    "hub": hub_name,
                    "cluster": m["cluster"],
                    "vol": m["vol"],
                    "hours": m["hours"],
                    "exceeds": exceeds,
                    "influence": influence,
                })

        total_influence = sum(r["influence"] for r in all_routes)
        total_vol = sum(r["vol"] for r in all_routes)
        weighted_hours = sum(r["vol"] * r["hours"] for r in all_routes)
        avg_hours = weighted_hours / total_vol if total_vol > 0 else 0

        # Generate recommendation
        recommendation = ""
        problem_routes = [r for r in all_routes if r["exceeds"]]
        if not problem_routes or avg_hours <= THRESHOLD_HOURS:
            recommendation = "✅ Среднее время ≤29ч — оптимально"
        else:
            # Find top problem route and suggest direct shipment
            top = max(problem_routes, key=lambda r: r["influence"])
            pct = (top["influence"] / total_influence * 100) if total_influence > 0 else 0
            dest = top["cluster"]
            local_h = DELIVERY_HOURS.get(dest, {}).get(dest, 28)
            if top["hub"] != dest and local_h <= THRESHOLD_HOURS:
                recommendation = (
                    f"⚠ Отделить {top['vol']} шт → напрямую в {dest} "
                    f"({top['hours']}ч → {local_h}ч). "
                    f"Снизит влияние на {pct:.0f}%"
                )
            else:
                recommendation = (
                    f"⚠ {top['hub']}→{dest} ({top['hours']}ч) = "
                    f"{pct:.0f}% влияния. Среднее {avg_hours:.0f}ч"
                )

        # SKU header row
        sku_hdr_fill = PatternFill("solid", fgColor="D6E4F0")
        ws6.cell(row6, 1, item["offer_id"]).font = Font(bold=True, size=11)
        ws6.cell(row6, 2, item["name"]).font = Font(bold=True, size=10)
        c_avg = ws6.cell(row6, 10, round(avg_hours, 1))
        c_avg.number_format = "0.0"
        c_avg.font = (
            critical_font if avg_hours > 45
            else warn_font if avg_hours > THRESHOLD_HOURS
            else ok_font
        )
        ws6.cell(row6, 11, recommendation).font = (
            Font(bold=True, size=10, color="CC6600")
            if avg_hours > THRESHOLD_HOURS
            else Font(size=10)
        )
        for ci2 in range(1, len(h6_headers) + 1):
            ws6.cell(row6, ci2).fill = sku_hdr_fill
        row6 += 1

        # Route rows sorted by influence desc
        for r in sorted(all_routes, key=lambda x: x["influence"], reverse=True):
            share_pct = (r["influence"] / total_influence * 100) if total_influence > 0 else 0

            ws6.cell(row6, 3, r["hub"])
            ws6.cell(row6, 4, r["cluster"])
            ws6.cell(row6, 5, r["vol"]).number_format = num_fmt

            c_h = ws6.cell(row6, 6, r["hours"])
            c_h.font = (
                ok_font if r["hours"] <= 28
                else warn_font if r["hours"] <= 45
                else critical_font
            )

            ws6.cell(row6, 7, "Да" if r["exceeds"] else "—").font = (
                Font(bold=True, color="CC0000") if r["exceeds"]
                else Font(color="999999")
            )

            ws6.cell(row6, 8, r["influence"]).number_format = num_fmt
            c_s = ws6.cell(row6, 9, round(share_pct, 1) if total_influence > 0 else 0)
            c_s.number_format = "0.0"
            if share_pct >= 40:
                c_s.font = critical_font
                c_s.fill = high_fill
            elif share_pct >= 20:
                c_s.font = warn_font

            row6 += 1

        row6 += 1  # blank separator

    # ── Save & return ────────────────────────────────────────
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    fname = f"supply_{shop_id}_{target_days}d.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )


# ══════════════════════════════════════════════════════════════
# WB Supply — рекомендации поставок с учётом хранения
# ══════════════════════════════════════════════════════════════

def _parse_ru_float(s: str) -> float:
    """Parse Russian-formatted float: '0,12' → 0.12"""
    if not s or s == "-":
        return 0.0
    return float(s.replace(",", "."))


async def _build_wb_supply_data(
    shop_id: int, sales_period: int, target_days: int, safety: float, db: AsyncSession
) -> dict:
    """
    Common data builder for WB supply (used by both JSON and XLSX endpoints).
    Returns dict with keys: items, tariffs, kpi, warehouse_summary.
    """
    from app.core.clickhouse import get_clickhouse_client

    ch = get_clickhouse_client()
    today = date.today()
    d_sales_start = today - timedelta(days=sales_period)

    # ── 1. WB Stocks (latest inventory snapshot) ─────────────
    stock_rows = ch.query(f"""
        SELECT
            nm_id,
            warehouse_name,
            argMax(quantity, fetched_at)  AS qty,
            argMax(price, fetched_at)     AS price,
            argMax(discount, fetched_at)  AS discount
        FROM mms_analytics.fact_inventory_snapshot
        WHERE shop_id = {shop_id}
        GROUP BY nm_id, warehouse_name
        HAVING qty > 0
    """).result_rows

    stocks_by_nm: dict[int, dict] = {}
    for row in stock_rows:
        nm_id = row[0]
        if nm_id not in stocks_by_nm:
            stocks_by_nm[nm_id] = {}
        stocks_by_nm[nm_id][row[1]] = {
            "qty": row[2], "price": float(row[3]), "discount": row[4]
        }

    # ── 2. WB Orders by warehouse ────────────────────────────
    sales_rows = ch.query(f"""
        SELECT
            nm_id,
            warehouse_name,
            count()          AS orders_count,
            sum(price_with_disc) AS revenue
        FROM mms_analytics.fact_orders_raw
        WHERE shop_id = {shop_id}
          AND date >= '{d_sales_start}'
          AND date <= '{today}'
          AND is_cancel = 0
        GROUP BY nm_id, warehouse_name
    """).result_rows

    sales_by_nm: dict[int, dict] = {}
    for row in sales_rows:
        nm_id = row[0]
        if nm_id not in sales_by_nm:
            sales_by_nm[nm_id] = {}
        sales_by_nm[nm_id][row[1]] = {
            "orders": row[2], "revenue": float(row[3])
        }

    # ── 3. Product info from PostgreSQL ──────────────────────
    prod_result = await db.execute(text("""
        SELECT nm_id, vendor_code, name,
               COALESCE(length, 0), COALESCE(width, 0), COALESCE(height, 0),
               COALESCE(current_price, 0)
        FROM dim_products
        WHERE shop_id = :sid
    """), {"sid": shop_id})
    prod_rows = prod_result.fetchall()

    product_info: dict[int, dict] = {}
    for r in prod_rows:
        l, w, h = float(r[3]), float(r[4]), float(r[5])
        vol_liters = (l * w * h) / 1000.0
        if vol_liters > 10000:
            vol_liters = (l * w * h) / 1_000_000.0
        product_info[r[0]] = {
            "vendor_code": r[1] or "",
            "name": r[2] or "",
            "length": l,
            "width": w,
            "height": h,
            "vol_liters": max(vol_liters, 0.1),
            "price": float(r[6]),
        }

    # ── 3b. Image URLs from Redis ────────────────────────────
    import os
    import redis as redis_lib
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    try:
        r = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        for nm_id in product_info:
            img = r.get(f"state:image_url:{shop_id}:{nm_id}")
            product_info[nm_id]["image_url"] = img or ""
        r.close()
    except Exception:
        for nm_id in product_info:
            product_info[nm_id]["image_url"] = ""

    # ── 4. WB Tariffs ────────────────────────────────────────
    tariffs: dict[str, dict] = {}
    try:
        tariff_rows = ch.query(f"""
            SELECT
                warehouse_name,
                argMax(storage_coef, updated_at) AS storage_coef,
                argMax(storage_base_liter, updated_at) AS storage_base_liter,
                argMax(storage_additional_liter, updated_at) AS storage_add_liter,
                argMax(delivery_coef, updated_at) AS delivery_coef,
                argMax(delivery_base_liter, updated_at) AS delivery_base_liter,
                argMax(delivery_additional_liter, updated_at) AS delivery_add_liter,
                argMax(coefficient, updated_at) AS acceptance_coef,
                argMax(allow_unload, updated_at) AS allow_unload
            FROM mms_analytics.fact_wb_acceptance_tariffs
            WHERE box_type_id = 2
              AND dt >= today()
            GROUP BY warehouse_name
        """).result_rows

        for r_t in tariff_rows:
            tariffs[r_t[0]] = {
                "storage_coef": _parse_ru_float(r_t[1]),
                "storage_base_liter": _parse_ru_float(r_t[2]),
                "storage_add_liter": _parse_ru_float(r_t[3]),
                "delivery_coef": _parse_ru_float(r_t[4]),
                "delivery_base_liter": _parse_ru_float(r_t[5]),
                "delivery_add_liter": _parse_ru_float(r_t[6]),
                "acceptance_coef": float(r_t[7]),
                "allow_unload": r_t[8],
            }
    except Exception:
        # Таблица wb_acceptance_tariffs может отсутствовать — работаем с дефолтами
        tariffs = {}

    # ── 5. Build recommendations per SKU × warehouse ─────────
    all_nm_ids = set(list(stocks_by_nm.keys()) + list(sales_by_nm.keys()))

    items = []
    for nm_id in sorted(all_nm_ids):
        pinfo = product_info.get(nm_id, {})
        vol = pinfo.get("vol_liters", 1.0)
        vendor = pinfo.get("vendor_code", str(nm_id))
        name = pinfo.get("name", "")
        image_url = pinfo.get("image_url", "")

        wh_stocks = stocks_by_nm.get(nm_id, {})
        wh_sales = sales_by_nm.get(nm_id, {})
        all_wh = set(list(wh_stocks.keys()) + list(wh_sales.keys()))

        total_sold = sum(s.get("orders", 0) for s in wh_sales.values())
        total_stock = sum(s.get("qty", 0) for s in wh_stocks.values())
        daily_avg = total_sold / max(sales_period, 1)

        turnover_days = total_stock / daily_avg if daily_avg > 0 else 999

        warehouses = []
        for wh in sorted(all_wh):
            stock = wh_stocks.get(wh, {}).get("qty", 0)
            orders = wh_sales.get(wh, {}).get("orders", 0)
            revenue = wh_sales.get(wh, {}).get("revenue", 0)
            wh_daily = orders / max(sales_period, 1)

            need = max(0, int(wh_daily * target_days * safety) - stock)

            t = tariffs.get(wh, {})
            stor_base = t.get("storage_base_liter", 0)
            stor_add = t.get("storage_add_liter", 0)
            if vol <= 1:
                storage_per_day = stor_base * vol
            else:
                storage_per_day = stor_base + stor_add * (vol - 1)

            wh_turnover = stock / wh_daily if wh_daily > 0 else 999
            ac = t.get("acceptance_coef", 0)

            warehouses.append({
                "warehouse": wh,
                "stock": stock,
                "orders": orders,
                "revenue": revenue,
                "daily": round(wh_daily, 2),
                "need": need,
                "storage_per_day": round(storage_per_day, 4),
                "storage_per_month": round(storage_per_day * 30, 2),
                "storage_coef": t.get("storage_coef", 0),
                "acceptance_coef": ac,
                "acceptance": "Без коэфф." if ac <= 0 or ac == -1 else f"x{ac:.0f}",
                "turnover_days": round(wh_turnover, 1),
            })

        # Status — хранение платное с 1-го дня, overstock = оборот > target_days
        if daily_avg == 0 and total_stock == 0:
            status = "ok"
        elif turnover_days > target_days and total_stock > 0:
            status = "overstock"
        elif turnover_days < 14:
            status = "critical"
        elif turnover_days < target_days:
            status = "attention"
        else:
            status = "ok"

        total_need = sum(w["need"] for w in warehouses)
        storage_cost_month = sum(w["storage_per_month"] * w["stock"] for w in warehouses)

        items.append({
            "nm_id": nm_id,
            "vendor_code": vendor,
            "name": name,
            "image_url": image_url,
            "vol_liters": round(vol, 2),
            "total_sold": total_sold,
            "total_stock": total_stock,
            "daily_avg": round(daily_avg, 2),
            "turnover_days": round(turnover_days, 1),
            "total_need": total_need,
            "status": status,
            "storage_cost_month": round(storage_cost_month, 2),
            "warehouses": warehouses,
        })

    # ── 6. KPI ───────────────────────────────────────────────
    kpi = {
        "total_need": sum(it["total_need"] for it in items),
        "critical_count": sum(1 for it in items if it["status"] == "critical"),
        "attention_count": sum(1 for it in items if it["status"] == "attention"),
        "overstock_count": sum(1 for it in items if it["status"] == "overstock"),
        "avg_days_supply": round(
            sum(it["turnover_days"] for it in items if it["turnover_days"] < 999) /
            max(sum(1 for it in items if it["turnover_days"] < 999), 1),
            1
        ),
        "total_stock": sum(it["total_stock"] for it in items),
        "total_sku": len(items),
        "total_storage_month": round(sum(it["storage_cost_month"] for it in items), 2),
    }

    # ── 7. Warehouse summary ─────────────────────────────────
    wh_agg: dict[str, dict] = {}
    for it in items:
        for wh in it["warehouses"]:
            wn = wh["warehouse"]
            if wn not in wh_agg:
                wh_agg[wn] = {
                    "warehouse": wn,
                    "total_stock": 0, "total_orders": 0,
                    "total_need": 0, "total_revenue": 0,
                    "items_count": 0,
                    "storage_coef": wh.get("storage_coef", 0),
                    "acceptance": wh.get("acceptance", ""),
                }
            wh_agg[wn]["total_stock"] += wh["stock"]
            wh_agg[wn]["total_orders"] += wh["orders"]
            wh_agg[wn]["total_need"] += wh["need"]
            wh_agg[wn]["total_revenue"] += wh["revenue"]
            wh_agg[wn]["items_count"] += 1

    warehouse_summary = sorted(wh_agg.values(), key=lambda x: x["total_orders"], reverse=True)

    return {
        "items": items,
        "tariffs": tariffs,
        "kpi": kpi,
        "warehouse_summary": warehouse_summary,
    }


@router.get("/wb/supply")
async def get_wb_supply(
    shop_id: int = Query(..., description="Shop ID"),
    sales_period: int = Query(30, ge=7, le=90, description="Sales period for avg daily calc"),
    target_days: int = Query(45, ge=14, le=60, description="Target stock days (max 60 = free storage)"),
    safety: float = Query(1.15, ge=1.0, le=2.0, description="Safety coefficient"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    WB Supply JSON — recommendations per SKU × warehouse.
    Returns kpi, items with nested warehouses, warehouse_summary.
    """
    shop = await db.get(Shop, shop_id)
    if not shop or shop.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="Shop not found")
    if shop.marketplace != "wildberries":
        raise HTTPException(status_code=400, detail="Shop is not Wildberries")

    result = await _build_wb_supply_data(shop_id, sales_period, target_days, safety, db)

    return {
        "shop_id": shop_id,
        "sales_period": sales_period,
        "target_days": target_days,
        "safety": safety,
        "kpi": result["kpi"],
        "items": result["items"],
        "warehouse_summary": result["warehouse_summary"],
    }


@router.get("/wb/supply/xlsx")
async def get_wb_supply_xlsx(
    shop_id: int = Query(..., description="Shop ID"),
    sales_period: int = Query(30, ge=7, le=90, description="Sales period for avg daily calc"),
    target_days: int = Query(45, ge=14, le=60, description="Target stock days (max 60 = free storage)"),
    safety: float = Query(1.15, ge=1.0, le=2.0, description="Safety coefficient"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    WB Supply Excel: recommendations per SKU × warehouse with storage cost analysis.
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, numbers
    from openpyxl.utils import get_column_letter

    shop = await db.get(Shop, shop_id)
    if not shop or shop.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="Shop not found")
    if shop.marketplace != "wildberries":
        raise HTTPException(status_code=400, detail="Shop is not Wildberries")

    result = await _build_wb_supply_data(shop_id, sales_period, target_days, safety, db)
    items = result["items"]
    tariffs = result["tariffs"]

    # ══════════════════════════════════════════════════════════
    # Генерация Excel
    # ══════════════════════════════════════════════════════════
    wb_xlsx = Workbook()

    hdr_font = Font(bold=True, size=11, color="FFFFFF")
    hdr_fill = PatternFill("solid", fgColor="1F4E79")
    num_fmt = "#,##0"
    money_fmt = "#,##0.00"
    warn_font = Font(bold=True, color="CC8800")
    critical_font = Font(bold=True, color="CC0000")
    ok_font = Font(color="00AA00")
    blue_font = Font(bold=True, color="0070C0")
    gray_font = Font(color="999999")
    sku_hdr_fill = PatternFill("solid", fgColor="D6E4F0")

    def _fmt_turnover(td):
        """Форматирование оборачиваемости: число → '25 дн' или 'Нет продаж'."""
        return f"{td:.0f} дн" if td < 999 else "Нет продаж"

    def _fmt_acceptance(ac):
        """Форматирование коэффициента приёмки: -1/0 → Без коэфф., >0 → x5."""
        if ac == -1 or ac == 0:
            return "Без коэфф."
        return f"x{ac:.0f}"

    def _acceptance_font(ac):
        if ac > 5:
            return critical_font
        elif ac > 0:
            return warn_font
        return ok_font

    # ══ Лист 1: Рекомендации по складам ══
    ws1 = wb_xlsx.active
    ws1.title = "Рекомендации по складам"
    h1 = [
        ("Артикул", 18), ("Название товара", 35), ("Склад WB", 28),
        ("Продано, шт", 12), ("Продаж в день", 10), ("Остаток, шт", 12),
        ("Оборачиваемость, дн", 14), ("Нужно довезти, шт", 14), ("Выручка, руб", 14),
        ("Хранение, руб/день", 12), ("Хранение, руб/мес", 12),
        ("Коэфф. хранения", 12), ("Приёмка", 14),
    ]
    for ci, (n, w) in enumerate(h1, 1):
        c = ws1.cell(1, ci, n)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws1.column_dimensions[get_column_letter(ci)].width = w
    ws1.freeze_panes = "A2"

    ws1.cell(2, 1, f"Параметры расчёта: период продаж {sales_period} дн, горизонт {target_days} дн, запас прочности ×{safety}").font = Font(bold=True, size=10, color="1F4E79")
    ws1.cell(2, 2, f"WB: хранение платное с 1-го дня, коэфф. фиксируется на 60 дн").font = Font(italic=True, size=9, color="666666")
    for ci2 in range(1, len(h1) + 1):
        ws1.cell(2, ci2).fill = PatternFill("solid", fgColor="DAEEF3")
    row1 = 4

    for item in items:
        if not item["warehouses"]:
            continue

        ws1.cell(row1, 1, item["vendor_code"]).font = Font(bold=True, size=11)
        ws1.cell(row1, 2, item["name"]).font = Font(bold=True, size=10)
        td_val = item["turnover_days"]
        ws1.cell(row1, 7, _fmt_turnover(td_val)).font = (
            critical_font if td_val > 60 else warn_font if td_val > 45 else ok_font
        )
        for ci2 in range(1, len(h1) + 1):
            ws1.cell(row1, ci2).fill = sku_hdr_fill
        row1 += 1

        for wh in sorted(item["warehouses"], key=lambda x: x["orders"], reverse=True):
            ws1.cell(row1, 3, wh["warehouse"])
            ws1.cell(row1, 4, wh["orders"]).number_format = num_fmt
            ws1.cell(row1, 5, wh["daily"]).number_format = "0.00"
            ws1.cell(row1, 6, wh["stock"]).number_format = num_fmt
            ws1.cell(row1, 7, _fmt_turnover(wh["turnover_days"])).font = (
                critical_font if wh["turnover_days"] > 60
                else warn_font if wh["turnover_days"] > 45
                else ok_font
            )
            ws1.cell(row1, 8, wh["need"]).number_format = num_fmt
            if wh["need"] > 0:
                ws1.cell(row1, 8).font = blue_font
            ws1.cell(row1, 9, wh["revenue"]).number_format = money_fmt
            ws1.cell(row1, 10, wh["storage_per_day"]).number_format = "0.0000"
            ws1.cell(row1, 11, wh["storage_per_month"]).number_format = money_fmt
            if wh["storage_per_month"] > 50:
                ws1.cell(row1, 11).font = warn_font

            sc = wh.get("storage_coef", 0)
            ac = wh.get("acceptance_coef", 0)
            ws1.cell(row1, 12, f"{sc:.0f}%" if sc else "нет данных")
            ws1.cell(row1, 13, _fmt_acceptance(ac)).font = _acceptance_font(ac)

            row1 += 1
        row1 += 1

    # ══ Лист 2: Сводка по товарам ══
    ws2 = wb_xlsx.create_sheet("Сводка по товарам")
    h2 = [
        ("Артикул", 18), ("Название товара", 35), ("Объём, литры", 10),
        ("Продано, шт", 12), ("Продаж в день", 10), ("Остаток, шт", 12),
        ("Оборачиваемость, дн", 14), ("Хранение, руб/мес", 14),
        ("Рекомендация", 55),
    ]
    for ci, (n, w) in enumerate(h2, 1):
        c = ws2.cell(1, ci, n)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws2.column_dimensions[get_column_letter(ci)].width = w
    ws2.freeze_panes = "A2"
    row2 = 2

    for item in items:
        if item["total_sold"] == 0 and item["total_stock"] == 0:
            continue

        total_storage_month = sum(wh["storage_per_month"] * wh["stock"]
                                  for wh in item["warehouses"])
        td = item["turnover_days"]

        if td > 90:
            rec = f"Критично! Более 90 дней оборота. Нужна распродажа или вывоз товара."
            rec_font = critical_font
        elif td > target_days:
            rec = f"Перезатарка: {td:.0f} дней оборота. Превышает горизонт поставки ({target_days} дн). Хранение платное с 1-го дня!"
            rec_font = warn_font
        elif td < 14:
            rec = f"Мало товара! Всего {td:.0f} дней запаса. Срочно нужна поставка!"
            rec_font = blue_font
        elif item["daily_avg"] > 0:
            rec = f"В норме: {td:.0f} дней запаса (цель — до {target_days} дней)"
            rec_font = ok_font
        else:
            rec = "Нет продаж за период анализа"
            rec_font = gray_font

        ws2.cell(row2, 1, item["vendor_code"])
        ws2.cell(row2, 2, item["name"])
        ws2.cell(row2, 3, item["vol_liters"]).number_format = "0.00"
        ws2.cell(row2, 4, item["total_sold"]).number_format = num_fmt
        ws2.cell(row2, 5, item["daily_avg"]).number_format = "0.00"
        ws2.cell(row2, 6, item["total_stock"]).number_format = num_fmt
        ws2.cell(row2, 7, _fmt_turnover(td)).font = (
            critical_font if td > 60 else warn_font if td > 45 else ok_font
        )
        ws2.cell(row2, 8, round(total_storage_month, 2)).number_format = money_fmt
        ws2.cell(row2, 9, rec).font = rec_font
        row2 += 1

    # ══ Лист 3: Тарифы складов WB ══
    ws3 = wb_xlsx.create_sheet("Тарифы складов WB")
    h3 = [
        ("Склад", 30), ("Коэфф. хранения", 14),
        ("Хранение: базовый, руб/литр", 14), ("Хранение: доп литры, руб/литр", 14),
        ("Коэфф. логистики", 14),
        ("Логистика: базовый, руб/литр", 14), ("Логистика: доп литры, руб/литр", 14),
        ("Приёмка", 14), ("Можно отгружать", 12),
    ]
    for ci, (n, w) in enumerate(h3, 1):
        c = ws3.cell(1, ci, n)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws3.column_dimensions[get_column_letter(ci)].width = w
    ws3.freeze_panes = "A2"
    row3 = 2

    for wh_name in sorted(tariffs.keys()):
        t = tariffs[wh_name]
        if "Маркетплейс" in wh_name:
            continue

        ws3.cell(row3, 1, wh_name)
        ws3.cell(row3, 2, f"{t['storage_coef']:.0f}%" if t['storage_coef'] else "нет данных")
        ws3.cell(row3, 3, t['storage_base_liter']).number_format = "0.00"
        ws3.cell(row3, 4, t['storage_add_liter']).number_format = "0.00"
        ws3.cell(row3, 5, f"{t['delivery_coef']:.0f}%" if t['delivery_coef'] else "нет данных")
        ws3.cell(row3, 6, t['delivery_base_liter']).number_format = "0.00"
        ws3.cell(row3, 7, t['delivery_add_liter']).number_format = "0.00"

        ac = t['acceptance_coef']
        ws3.cell(row3, 8, _fmt_acceptance(ac)).font = _acceptance_font(ac)
        ws3.cell(row3, 9, "Да" if t['allow_unload'] else "Нет").font = ok_font if t['allow_unload'] else critical_font
        row3 += 1

    # ══ Лист 4: Риск перезатаривания ══
    ws4 = wb_xlsx.create_sheet("Риск перезатаривания")
    h4 = [
        ("Артикул", 18), ("Название товара", 35), ("Объём, литры", 10),
        ("Остаток, шт", 12), ("Продаж в день", 10), ("Оборачиваемость, дн", 14),
        ("Превышение горизонта, дн", 14), ("Доп расходы хранение, руб/мес", 16),
        ("Рекомендация", 55),
    ]
    for ci, (n, w) in enumerate(h4, 1):
        c = ws4.cell(1, ci, n)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws4.column_dimensions[get_column_letter(ci)].width = w
    ws4.freeze_panes = "A2"
    row4 = 2

    risk_items = [it for it in items if it["turnover_days"] > 45 and it["total_stock"] > 0]
    risk_items.sort(key=lambda x: x["turnover_days"], reverse=True)

    for item in risk_items:
        excess_days = max(0, item["turnover_days"] - target_days)
        avg_storage_per_day = sum(
            wh["storage_per_day"] * wh["stock"] for wh in item["warehouses"] if wh["stock"] > 0
        )
        extra_cost = avg_storage_per_day * excess_days if excess_days > 0 else 0

        if item["turnover_days"] > 90:
            rec = f"Критично! {item['total_stock']} шт лежат более 90 дней. Рекомендуем распродажу или возврат товара!"
        elif item["turnover_days"] > target_days:
            rec = f"Перезатарка: {excess_days:.0f} дней сверх горизонта поставки ({target_days} дн). Хранение платное. Ускорьте продажи или снизьте запас."
        else:
            rec = f"Внимание: оборачиваемость {item['turnover_days']:.0f} дн. Контролируйте запасы."

        ws4.cell(row4, 1, item["vendor_code"])
        ws4.cell(row4, 2, item["name"])
        ws4.cell(row4, 3, item["vol_liters"]).number_format = "0.00"
        ws4.cell(row4, 4, item["total_stock"]).number_format = num_fmt
        ws4.cell(row4, 5, item["daily_avg"]).number_format = "0.00"
        ws4.cell(row4, 6, _fmt_turnover(item["turnover_days"])).font = (
            critical_font if item["turnover_days"] > 60 else warn_font
        )
        ws4.cell(row4, 7, f"{excess_days:.0f} дн").number_format = num_fmt
        ws4.cell(row4, 8, round(extra_cost, 2)).number_format = money_fmt
        if extra_cost > 0:
            ws4.cell(row4, 8).font = critical_font
        ws4.cell(row4, 9, rec).font = Font(size=10)
        row4 += 1

    if not risk_items:
        ws4.cell(row4, 1, "Нет товаров с риском перезатаривания")
        ws4.cell(row4, 1).font = Font(bold=True, size=12, color="00AA00")

    # ── Сохранение и отправка ────────────────────────────────
    buf = io.BytesIO()
    wb_xlsx.save(buf)
    buf.seek(0)

    fname = f"wb_supply_{shop_id}_{target_days}d.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )

