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


# ═══════════════════════════════════════════════════════════════
# Маппинг: округ покупателя → ближайшие склады WB
# Содержит ВСЕ варианты: обычные, : Питание, СГТ
# Фильтрация по типу товара происходит в runtime
# ═══════════════════════════════════════════════════════════════
REGION_TO_WAREHOUSES: dict[str, list[str]] = {
    "Центральный федеральный округ": [
        "Котовск", "Котовск: Питание",
        "Электросталь", "Электросталь: Питание",
        "Рязань (Тюшевское)", "Рязань (Тюшевское): Питание",
        "Истра", "Подольск 3", "Коледино",
        "Белые Столбы", "Обухово",
        "Домодедово-2", "Домодедово 2: Питание",
    ],
    "Северо-Западный федеральный округ": [
        "Санкт-Петербург Уткина Заводь", "Никольское",
        "Шушары: Питание",
    ],
    "Приволжский федеральный округ": [
        "Новосемейкино", "Новосемейкино: Питание",
        "Казань", "Казань: Питание",
        "Пенза",
    ],
    "Южный федеральный округ": [
        "Краснодар (Тихорецкая)", "Краснодар (Тихорецкая): Питание",
        "Волгоград", "Волгоград: Питание",
    ],
    "Северо-Кавказский федеральный округ": [
        "Невинномысск",
        "Краснодар (Тихорецкая)", "Краснодар (Тихорецкая): Питание",
    ],
    "Уральский федеральный округ": [
        "Екатеринбург - Перспективная 14",
        "Екатеринбург (Перспективная): Питание",
        "Екатеринбург - Испытателей 14г",
    ],
    "Сибирский федеральный округ": [
        "Новосибирск", "Новосибирск СГТ",
    ],
    "Дальневосточный федеральный округ": [
        "Владивосток СГТ",
    ],
}

# ── Warehouse type classification ──
_FOOD_SUFFIX = ": Питание"
_SGT_SUFFIX = "СГТ"
_MAX_BOX_WEIGHT_KG = 25  # Ограничение коробов по весу

# Список категорий WB, которые нуждаются в складах «Питание» (Меркурий / пищевая продукция)
_FOOD_CATEGORIES = {
    "Товары для животных",
    "Продукты питания",
    "Здоровое питание",
    "Детское питание",
    "Корма",
}


def _classify_product_wh_type(
    nm_id: int,
    product_categories: dict[int, str],
    wh_stocks: dict[str, dict],
    wh_sales: dict[str, dict],
) -> str:
    """
    Определяет тип товара:
    1) По категории из fact_orders_raw (приоритет)
    2) По складам с стоками/продажами (fallback)
    """
    # 1. По категории
    cat = product_categories.get(nm_id, "")
    if cat in _FOOD_CATEGORIES:
        return "food"

    # 2. По названию склада (если есть на складе с суффиксом)
    all_wh_names = set(list(wh_stocks.keys()) + list(wh_sales.keys()))
    for wh in all_wh_names:
        if _FOOD_SUFFIX in wh:
            return "food"
    for wh in all_wh_names:
        if _SGT_SUFFIX in wh:
            return "sgt"
    return "normal"


def _filter_wh_for_type(
    warehouse_name: str,
    product_type: str,
) -> bool:
    """
    Проверяет, подходит ли склад для типа товара.
    - food  → только ': Питание'
    - sgt   → только 'СГТ'
    - normal → БЕЗ ': Питание' и БЕЗ 'СГТ'
    """
    is_food_wh = _FOOD_SUFFIX in warehouse_name
    is_sgt_wh = _SGT_SUFFIX in warehouse_name

    if product_type == "food":
        return is_food_wh
    elif product_type == "sgt":
        return is_sgt_wh
    else:  # normal
        return not is_food_wh and not is_sgt_wh


def _match_warehouse(name: str, available: set[str]) -> str | None:
    """
    Точное сопоставление склада из маппинга с доступными складами в тарифах.
    Не используем startswith чтобы 'Котовск' не матчил 'Котовск: Питание'.
    """
    # 1. Точное совпадение
    if name in available:
        return name
    # 2. Расширенный поиск только для СГТ (отличаются названия)
    for avail_wh in available:
        # Матч только если оба одного типа
        name_is_food = _FOOD_SUFFIX in name
        avail_is_food = _FOOD_SUFFIX in avail_wh
        name_is_sgt = _SGT_SUFFIX in name
        avail_is_sgt = _SGT_SUFFIX in avail_wh
        if name_is_food != avail_is_food or name_is_sgt != avail_is_sgt:
            continue  # разные типы — не матчим
        # Одинаковый тип — проверяем базовое название
        if avail_wh.startswith(name) or name.startswith(avail_wh.split(":")[0].split(" СГТ")[0]):
            return avail_wh
    return None


def _estimate_weight_kg(vol_liters: float) -> float:
    """Оценка веса из объёма (~0.5 кг/л)."""
    return vol_liters * 0.5


async def _build_wb_supply_data(
    shop_id: int, sales_period: int, target_days: int, safety: float,
    use_ad_boost: bool, db: AsyncSession,
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

    # ── 2b. Ad boost (7d vs prev 7d) ─────────────────────────
    boost_map: dict[int, float] = {}  # nm_id → boost coefficient
    if use_ad_boost:
        try:
            # Sales 7d vs prev 7d per nm_id
            s7_rows = ch.query(f"""
                SELECT nm_id,
                       countIf(date >= today() - 7)  AS qty_7d,
                       countIf(date <  today() - 7)  AS qty_prev7d
                FROM mms_analytics.fact_orders_raw
                WHERE shop_id = {shop_id}
                  AND date >= today() - 14
                  AND is_cancel = 0
                GROUP BY nm_id
            """).result_rows
            s7 = {int(r[0]): {"q7": r[1], "qp7": r[2]} for r in s7_rows}

            # Ad spend per nm_id (7d)
            ad_rows = ch.query(f"""
                SELECT nm_id,
                       sumIf(spend, date >= today() - 7)  AS ad_7d
                FROM mms_analytics.fact_advert_stats_v3
                WHERE shop_id = {shop_id}
                  AND date >= today() - 14
                GROUP BY nm_id
            """).result_rows
            ad_spend_7d = {int(r[0]): float(r[1]) for r in ad_rows}

            # Calculate boost
            all_boost_nms = set(list(s7.keys()) + list(ad_spend_7d.keys()))
            for nm in all_boost_nms:
                s = s7.get(nm, {"q7": 0, "qp7": 0})
                has_ads = ad_spend_7d.get(nm, 0) > 0
                if has_ads and s["qp7"] > 0 and s["q7"] > s["qp7"]:
                    boost_map[nm] = min(s["q7"] / s["qp7"], 2.0)
                elif has_ads and s["qp7"] == 0 and s["q7"] > 0:
                    boost_map[nm] = 1.3
                else:
                    boost_map[nm] = 1.0
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("WB ad boost query failed: %s", e)

    # ── 2c. Demand by buyer region ───────────────────────────
    # Считаем заказы по nm_id × округ покупателя (а не склад отгрузки!)
    regional_demand: dict[int, dict[str, int]] = {}  # nm_id → {округ → qty}
    product_categories: dict[int, str] = {}  # nm_id → category
    try:
        rd_rows = ch.query(f"""
            SELECT nm_id, oblast_okrug_name, count() AS cnt,
                   any(category) AS cat
            FROM mms_analytics.fact_orders_raw
            WHERE shop_id = {shop_id}
              AND date >= '{d_sales_start}'
              AND date <= '{today}'
              AND is_cancel = 0
              AND oblast_okrug_name != ''
            GROUP BY nm_id, oblast_okrug_name
        """).result_rows
        for row in rd_rows:
            nm = int(row[0])
            if nm not in regional_demand:
                regional_demand[nm] = {}
            regional_demand[nm][row[1]] = row[2]
            if row[3] and nm not in product_categories:
                product_categories[nm] = row[3]
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("WB regional demand query failed: %s", e)

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
    # Используем РЕГИОНАЛЬНЫЙ спрос: округ покупателя → ближайший склад
    all_nm_ids = set(
        list(stocks_by_nm.keys()) +
        list(sales_by_nm.keys()) +
        list(regional_demand.keys())
    )

    # Собираем все доступные склады (из тарифов + маппинга)
    available_warehouses = set(tariffs.keys())

    items = []
    for nm_id in sorted(all_nm_ids):
        pinfo = product_info.get(nm_id, {})
        vol = pinfo.get("vol_liters", 1.0)
        vendor = pinfo.get("vendor_code", str(nm_id))
        name = pinfo.get("name", "")
        image_url = pinfo.get("image_url", "")

        wh_stocks = stocks_by_nm.get(nm_id, {})
        wh_sales = sales_by_nm.get(nm_id, {})
        nm_regions = regional_demand.get(nm_id, {})

        # ── 5a. Определяем тип товара (по категории + складам) ──
        product_type = _classify_product_wh_type(nm_id, product_categories, wh_stocks, wh_sales)
        est_weight = _estimate_weight_kg(vol)

        if est_weight > _MAX_BOX_WEIGHT_KG and product_type == "normal":
            product_type = "sgt"

        # ── 5b. Распределяем региональный спрос по складам ──
        demand_by_wh: dict[str, dict] = {}
        for region, qty in nm_regions.items():
            target_whs = REGION_TO_WAREHOUSES.get(region, [])
            typed_whs = [w for w in target_whs if _filter_wh_for_type(w, product_type)]
            if not typed_whs:
                typed_whs = target_whs

            placed = False
            for twh in typed_whs:
                matched_wh = _match_warehouse(twh, available_warehouses)
                if matched_wh:
                    if matched_wh not in demand_by_wh:
                        demand_by_wh[matched_wh] = {"regional_orders": 0, "regions": []}
                    demand_by_wh[matched_wh]["regional_orders"] += qty
                    demand_by_wh[matched_wh]["regions"].append(region)
                    placed = True
                    break
            if not placed:
                fallback_wh = typed_whs[0] if typed_whs else "Котовск"
                if fallback_wh not in demand_by_wh:
                    demand_by_wh[fallback_wh] = {"regional_orders": 0, "regions": []}
                demand_by_wh[fallback_wh]["regional_orders"] += qty
                demand_by_wh[fallback_wh]["regions"].append(region)

        # Объединяем склады и фильтруем по типу товара
        raw_wh = set(
            list(wh_stocks.keys()) +
            list(wh_sales.keys()) +
            list(demand_by_wh.keys())
        )
        # Для food — оставляем только ': Питание' + склады с текущим стоком (показываем но need=0)
        # Для normal — исключаем ': Питание' и 'СГТ'
        all_wh = set()
        for wh in raw_wh:
            is_stock_or_sales = wh in wh_stocks or wh in wh_sales
            if is_stock_or_sales:
                # Всегда показываем склады с текущими стоками/продажами
                all_wh.add(wh)
            elif _filter_wh_for_type(wh, product_type):
                # Новые склады — только подходящего типа
                all_wh.add(wh)

        total_sold = sum(s.get("orders", 0) for s in wh_sales.values())
        total_stock = sum(s.get("qty", 0) for s in wh_stocks.values())
        daily_avg = total_sold / max(sales_period, 1)
        boost = boost_map.get(nm_id, 1.0)
        boosted_daily = daily_avg * boost

        turnover_days = total_stock / boosted_daily if boosted_daily > 0 else 999

        warehouses = []
        for wh in sorted(all_wh):
            stock = wh_stocks.get(wh, {}).get("qty", 0)
            orders = wh_sales.get(wh, {}).get("orders", 0)
            revenue = wh_sales.get(wh, {}).get("revenue", 0)
            wh_daily = orders / max(sales_period, 1)

            # Региональный спрос → daily для этого склада
            rd = demand_by_wh.get(wh, {})
            regional_orders = rd.get("regional_orders", 0)
            regional_daily = regional_orders / max(sales_period, 1)
            demand_regions = rd.get("regions", [])

            # Используем МАКСИМУМ из (фактический daily, региональный daily)
            # чтобы не занизить если склад уже отгружает больше
            effective_daily = max(wh_daily, regional_daily)
            effective_daily_boosted = effective_daily * boost

            need = max(0, int(effective_daily_boosted * target_days * safety) - stock)

            t = tariffs.get(wh, {})
            stor_base = t.get("storage_base_liter", 0)
            stor_add = t.get("storage_add_liter", 0)
            if vol <= 1:
                storage_per_day = stor_base * vol
            else:
                storage_per_day = stor_base + stor_add * (vol - 1)

            wh_turnover = stock / effective_daily if effective_daily > 0 else 999
            ac = t.get("acceptance_coef", 0)

            warehouses.append({
                "warehouse": wh,
                "stock": stock,
                "orders": orders,
                "regional_orders": regional_orders,
                "demand_regions": list(set(demand_regions)),
                "daily_boosted": round(effective_daily_boosted, 2),
                "revenue": revenue,
                "daily": round(wh_daily, 2),
                "regional_daily": round(regional_daily, 2),
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
            "boost": round(boost, 2),
            "boosted_daily": round(boosted_daily, 2),
            "turnover_days": round(turnover_days, 1),
            "total_need": total_need,
            "status": status,
            "storage_cost_month": round(storage_cost_month, 2),
            "product_type": product_type,
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
    use_ad_boost: bool = Query(True, description="Apply ad boost coefficient"),
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

    result = await _build_wb_supply_data(shop_id, sales_period, target_days, safety, use_ad_boost, db)

    return {
        "shop_id": shop_id,
        "sales_period": sales_period,
        "target_days": target_days,
        "safety": safety,
        "use_ad_boost": use_ad_boost,
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
    use_ad_boost: bool = Query(True, description="Apply ad boost coefficient"),
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

    result = await _build_wb_supply_data(shop_id, sales_period, target_days, safety, use_ad_boost, db)
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

    ws1.cell(2, 1, f"Параметры расчёта: период продаж {sales_period} дн, горизонт {target_days} дн, запас прочности ×{safety} | Ad boost: {'ДА' if use_ad_boost else 'НЕТ'}").font = Font(bold=True, size=10, color="1F4E79")
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

    # ══ Лист 3: Поставка по складам ══
    # Группировка: склад → список SKU с need > 0
    ws_wh = wb_xlsx.create_sheet("Поставка по складам")
    h_wh = [
        ("Склад WB", 32), ("Артикул", 18), ("Название товара", 35),
        ("Заказы прямые", 12), ("Заказы региональные", 14),
        ("Ежедн. эфф.", 10), ("Остаток", 10), ("ПОСТАВИТЬ", 14),
        ("Выручка, руб", 14), ("Тип товара", 14),
    ]
    for ci, (n, w) in enumerate(h_wh, 1):
        c = ws_wh.cell(1, ci, n)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws_wh.column_dimensions[get_column_letter(ci)].width = w
    ws_wh.freeze_panes = "A2"

    # Собираем данные: склад → [{sku info}]
    wh_groups: dict[str, list[dict]] = {}
    for item in items:
        for wh in item.get("warehouses", []):
            if wh["need"] <= 0:
                continue
            wh_name = wh["warehouse"]
            if wh_name not in wh_groups:
                wh_groups[wh_name] = []
            wh_groups[wh_name].append({
                "vendor_code": item["vendor_code"],
                "name": item["name"],
                "orders": wh.get("orders", 0),
                "regional_orders": wh.get("regional_orders", 0),
                "daily_boosted": wh.get("daily_boosted", 0),
                "stock": wh["stock"],
                "need": wh["need"],
                "revenue": wh["revenue"],
                "product_type": item.get("product_type", ""),
            })

    wh_hdr_fill = PatternFill("solid", fgColor="BDD7EE")
    wh_hdr_font = Font(bold=True, size=11, color="1F4E79")
    need_fill = PatternFill("solid", fgColor="FFF2CC")
    regional_font = Font(bold=True, color="8B5CF6")
    row_wh = 2

    for wh_name in sorted(wh_groups.keys(), key=lambda w: sum(i["need"] for i in wh_groups[w]), reverse=True):
        sku_list = wh_groups[wh_name]
        total_need = sum(i["need"] for i in sku_list)
        total_revenue = sum(i["revenue"] for i in sku_list)
        total_regional = sum(i["regional_orders"] for i in sku_list)
        total_direct = sum(i["orders"] for i in sku_list)
        sku_count = len(sku_list)

        # Заголовок склада
        wh_label = f"📦 {wh_name}  —  {sku_count} SKU"
        c = ws_wh.cell(row_wh, 1, wh_label)
        c.font = wh_hdr_font
        c.fill = wh_hdr_fill
        ws_wh.cell(row_wh, 4, total_direct).number_format = num_fmt
        ws_wh.cell(row_wh, 5, total_regional).number_format = num_fmt
        if total_regional > 0:
            ws_wh.cell(row_wh, 5).font = regional_font
        ws_wh.cell(row_wh, 8, total_need).font = Font(bold=True, size=12, color="CC0000")
        ws_wh.cell(row_wh, 8).number_format = num_fmt
        ws_wh.cell(row_wh, 9, round(total_revenue)).number_format = num_fmt
        for ci2 in range(1, len(h_wh) + 1):
            ws_wh.cell(row_wh, ci2).fill = wh_hdr_fill
        row_wh += 1

        # SKU внутри склада
        for si in sorted(sku_list, key=lambda x: x["need"], reverse=True):
            ws_wh.cell(row_wh, 1, "")
            ws_wh.cell(row_wh, 2, si["vendor_code"])
            ws_wh.cell(row_wh, 3, si["name"])
            ws_wh.cell(row_wh, 4, si["orders"]).number_format = num_fmt
            c5 = ws_wh.cell(row_wh, 5, si["regional_orders"])
            c5.number_format = num_fmt
            if si["regional_orders"] > 0:
                c5.font = regional_font
            ws_wh.cell(row_wh, 6, si["daily_boosted"]).number_format = "0.00"
            ws_wh.cell(row_wh, 7, si["stock"]).number_format = num_fmt
            c = ws_wh.cell(row_wh, 8, si["need"])
            c.number_format = num_fmt
            c.font = Font(bold=True, color="CC0000")
            c.fill = need_fill
            ws_wh.cell(row_wh, 9, round(si["revenue"])).number_format = num_fmt
            pt = si.get("product_type", "")
            pt_label = {"food": "🍖 Питание", "sgt": "📦 СГТ"}.get(pt, "Обычный")
            ws_wh.cell(row_wh, 10, pt_label)
            row_wh += 1

        row_wh += 1  # пустая строка между складами

    if not wh_groups:
        ws_wh.cell(row_wh, 1, "Нет товаров к поставке")
        ws_wh.cell(row_wh, 1).font = Font(bold=True, size=12, color="00AA00")

    # ══ Лист 4: Тарифы складов WB ══
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


# ══════════════════════════════════════════════════════════════
# Ozon: Warehouse Analytics — аналитика по складам
# ══════════════════════════════════════════════════════════════

# Маппинг warehouse_name → cluster (для расчёта скорости доставки)
_WAREHOUSE_TO_CLUSTER: dict[str, str] = {
    # Московский регион
    "ДОМОДЕДОВО_РФЦ": "Москва, МО и Дальние регионы",
    "ЖУКОВСКИЙ_РФЦ": "Москва, МО и Дальние регионы",
    "ГРИВНО_РФЦ": "Москва, МО и Дальние регионы",
    "СОФЬИНО_РФЦ": "Москва, МО и Дальние регионы",
    "НОГИНСК_РФЦ": "Москва, МО и Дальние регионы",
    "ПЕТРОВСКОЕ_РФЦ": "Москва, МО и Дальние регионы",
    "Дедовск": "Москва, МО и Дальние регионы",
    # СПБ
    "СПБ_БУГРЫ_РФЦ": "Санкт-Петербург и СЗО",
    "СПБ_КОЛПИНО_РФЦ": "Санкт-Петербург и СЗО",
    "Санкт_Петербург_РФЦ": "Санкт-Петербург и СЗО",
    "САНКТ-ПЕТЕРБУРГ_РФЦ": "Санкт-Петербург и СЗО",
    # Юг
    "РОСТОВ_НА_ДОНУ_2_РФЦ": "Ростов",
    "АДЫГЕЙСК_РФЦ": "Краснодар",
    "НЕВИННОМЫССК_РФЦ": "Невинномысск",
    # Центр
    "ВОРОНЕЖ_2_РФЦ": "Воронеж",
    "САРАТОВ_РФЦ": "Саратов",
    # Поволжье
    "Казань_РФЦ_НОВЫЙ": "Казань",
    "КАЗАНЬ_РФЦ_НОВЫЙ": "Казань",
    "САМАРА_РФЦ": "Самара",
    "УФА_РФЦ": "Уфа",
    # Урал / Сибирь
    "Екатеринбург_РФЦ_НОВЫЙ": "Екатеринбург",
    "КРАСНОЯРСК_МРФЦ": "Красноярск",
    "НИЖНИЙ_НОВГОРОД_РФЦ": "Казань",
    "НОВОСИБИРСК_РФЦ": "Новосибирск",
}


def _get_cluster_for_warehouse(wh_name: str) -> str:
    """Resolve warehouse name to delivery cluster."""
    if not wh_name:
        return "Неизвестный"
    # Exact match
    cluster = _WAREHOUSE_TO_CLUSTER.get(wh_name)
    if cluster:
        return cluster
    # Fuzzy fallback by keywords
    wh_upper = wh_name.upper()
    keyword_map = {
        "ДОМОДЕДОВО": "Москва, МО и Дальние регионы",
        "ЖУКОВСКИЙ": "Москва, МО и Дальние регионы",
        "ГРИВНО": "Москва, МО и Дальние регионы",
        "СОФЬИНО": "Москва, МО и Дальние регионы",
        "НОГИНСК": "Москва, МО и Дальние регионы",
        "ПЕТРОВСКОЕ": "Москва, МО и Дальние регионы",
        "ДЕДОВСК": "Москва, МО и Дальние регионы",
        "ТВЕРЬ": "Тверь",
        "СПБ": "Санкт-Петербург и СЗО",
        "ПЕТЕРБУРГ": "Санкт-Петербург и СЗО",
        "РОСТОВ": "Ростов",
        "АДЫГЕЙСК": "Краснодар",
        "КРАСНОДАР": "Краснодар",
        "НЕВИННОМЫССК": "Невинномысск",
        "ВОРОНЕЖ": "Воронеж",
        "САРАТОВ": "Саратов",
        "КАЗАНЬ": "Казань",
        "САМАРА": "Самара",
        "УФА": "Уфа",
        "ЕКАТЕРИНБУРГ": "Екатеринбург",
        "КРАСНОЯРСК": "Красноярск",
        "НОВОСИБИРСК": "Новосибирск",
        "НИЖНИЙ": "Казань",
        "ПЕРМЬ": "Пермь",
        "ТЮМЕНЬ": "Тюмень",
        "ОМСК": "Омск",
    }
    for kw, cl in keyword_map.items():
        if kw in wh_upper:
            return cl
    return "Неизвестный"


# Тарифы хранения Ozon (руб/литр/день)
OZON_STORAGE_TARIFFS = {
    "free": {"max_days": 160, "rate": 0.0},
    "low": {"max_days": 180, "rate": 0.75},
    "high": {"max_days": 99999, "rate": 1.50},
}


@router.get("/ozon/analytics")
async def ozon_warehouse_analytics(
    shop_id: int = Query(...),
    period: int = Query(30, description="Period in days for sales calculation"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Warehouse analytics for Ozon FBO.

    Returns:
    - KPI summary (total stock, warehouses, avg turnover, costs)
    - Per-warehouse breakdown (stock, orders, turnover, delivery speed, costs)
    - Per-warehouse SKU details
    """
    import os
    import clickhouse_connect

    # Verify shop ownership
    shop = await db.get(Shop, shop_id)
    if not shop or shop.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="Shop not found")
    if shop.marketplace != "ozon":
        raise HTTPException(status_code=400, detail="Only Ozon shops supported")

    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_pass = os.getenv("CLICKHOUSE_PASSWORD", "")

    ch = clickhouse_connect.get_client(
        host=ch_host, port=ch_port,
        username=ch_user, password=ch_pass,
        database="mms_analytics",
    )

    try:
        # ── 1. Current stocks per warehouse ──────────────────────
        stocks_data = ch.query("""
            SELECT warehouse_name, warehouse_type,
                   groupArray(sku) as skus,
                   groupArray(offer_id) as offer_ids,
                   groupArray(product_name) as names,
                   groupArray(free_to_sell) as frees,
                   groupArray(reserved) as reserveds,
                   count() as sku_count,
                   sum(free_to_sell) as total_free,
                   sum(reserved) as total_reserved
            FROM fact_ozon_warehouse_stocks FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt = (SELECT max(dt) FROM fact_ozon_warehouse_stocks WHERE shop_id = {shop_id:UInt32})
              AND warehouse_type = 'fbo'
            GROUP BY warehouse_name, warehouse_type
            ORDER BY total_free DESC
        """, parameters={"shop_id": shop_id})

        # ── 2. Orders per warehouse (last N days) ────────────────
        orders_data = ch.query("""
            SELECT warehouse_name,
                   count() as order_count,
                   sum(quantity) as total_qty,
                   sum(price * quantity) as revenue,
                   uniq(sku) as active_skus
            FROM fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND order_date >= today() - {period:UInt32}
              AND status NOT IN ('cancelled')
            GROUP BY warehouse_name
        """, parameters={"shop_id": shop_id, "period": period})

        # ── 3. Geography: cluster_to distribution per warehouse ──
        geo_data = ch.query("""
            SELECT warehouse_name, cluster_to,
                   count() as orders, sum(quantity) as qty
            FROM fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND order_date >= today() - {period:UInt32}
              AND status NOT IN ('cancelled')
              AND cluster_to != ''
            GROUP BY warehouse_name, cluster_to
            ORDER BY warehouse_name, qty DESC
        """, parameters={"shop_id": shop_id, "period": period})

        # ── 4. Logistics costs from transactions ─────────────────
        costs_data = ch.query("""
            SELECT operation_type, operation_type_name,
                   count() as cnt, sum(amount) as total, sum(services_total) as svcs
            FROM fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND operation_date >= today() - {period:UInt32}
              AND operation_type IN (
                  'MarketplaceServiceItemCrossdocking',
                  'OperationMarketplaceServiceStorage',
                  'OperationMarketplaceSupplyAdditional',
                  'OperationMarketplaceSupplyExpirationDateProcessing',
                  'OperationMarketplaceServiceSupplyInboundCargoShortage'
              )
            GROUP BY operation_type, operation_type_name
        """, parameters={"shop_id": shop_id, "period": period})

        # ── 4b. Costs per warehouse (via JOIN with orders) ────────
        costs_per_wh_data = ch.query("""
            SELECT o.warehouse_name,
                   t.operation_type,
                   count() as cnt,
                   sum(t.amount) as total
            FROM fact_ozon_transactions t FINAL
            INNER JOIN (
                SELECT DISTINCT posting_number, warehouse_name
                FROM fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND warehouse_name != ''
            ) o USING (posting_number)
            WHERE t.shop_id = {shop_id:UInt32}
              AND t.operation_date >= today() - {period:UInt32}
              AND t.operation_type IN (
                  'MarketplaceServiceItemCrossdocking',
                  'OperationMarketplaceServiceStorage',
                  'OperationMarketplaceSupplyAdditional'
              )
              AND t.posting_number != ''
            GROUP BY o.warehouse_name, t.operation_type
        """, parameters={"shop_id": shop_id, "period": period})

        # ── 5. Turnover data (if available) ──────────────────────
        turnover_data = {}
        try:
            tr = ch.query("""
                SELECT sku, days_of_supply, avg_daily_sales,
                       stock_fbo, turnover_category
                FROM fact_ozon_turnover FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND dt = (SELECT max(dt) FROM fact_ozon_turnover WHERE shop_id = {shop_id:UInt32})
            """, parameters={"shop_id": shop_id})
            for row in tr.result_rows:
                turnover_data[row[0]] = {
                    "days_of_supply": float(row[1]),
                    "avg_daily_sales": float(row[2]),
                    "stock_fbo": int(row[3]),
                    "turnover_category": row[4],
                }
        except Exception:
            pass  # Table may not exist yet

        # ── 6. Product dimensions for storage cost estimation ────
        dims_data = {}
        try:
            dims = await db.execute(text("""
                SELECT product_id, length, width, height, weight
                FROM dim_ozon_products
                WHERE shop_id = :shop_id AND length > 0 AND width > 0 AND height > 0
            """), {"shop_id": shop_id})
            for row in dims.fetchall():
                # Volume in liters (dimensions in mm → cm → liters)
                vol_liters = (row[1] / 10) * (row[2] / 10) * (row[3] / 10) / 1000
                dims_data[row[0]] = {
                    "volume_liters": round(vol_liters, 2),
                    "weight_kg": round(row[4] / 1000, 2) if row[4] else 0,
                }
        except Exception:
            pass

        # ── Build response ───────────────────────────────────────
        # Index orders by warehouse
        orders_by_wh = {}
        for row in orders_data.result_rows:
            orders_by_wh[row[0]] = {
                "orders": int(row[1]),
                "qty": int(row[2]),
                "revenue": float(row[3]),
                "active_skus": int(row[4]),
            }

        # Index per-warehouse costs
        costs_by_wh: dict[str, dict] = {}
        for row in costs_per_wh_data.result_rows:
            wh = row[0]
            op = row[1]
            if wh not in costs_by_wh:
                costs_by_wh[wh] = {"crossdocking": 0, "storage": 0, "fbo_processing": 0,
                                    "crossdocking_cnt": 0, "storage_cnt": 0, "fbo_cnt": 0}
            amount = float(row[3])
            cnt = int(row[2])
            if op == "MarketplaceServiceItemCrossdocking":
                costs_by_wh[wh]["crossdocking"] += amount
                costs_by_wh[wh]["crossdocking_cnt"] += cnt
            elif op == "OperationMarketplaceServiceStorage":
                costs_by_wh[wh]["storage"] += amount
                costs_by_wh[wh]["storage_cnt"] += cnt
            elif op == "OperationMarketplaceSupplyAdditional":
                costs_by_wh[wh]["fbo_processing"] += amount
                costs_by_wh[wh]["fbo_cnt"] += cnt

        # Index geography by warehouse
        geo_by_wh: dict[str, list] = {}
        for row in geo_data.result_rows:
            wh = row[0]
            if wh not in geo_by_wh:
                geo_by_wh[wh] = []
            geo_by_wh[wh].append({
                "cluster": row[1],
                "orders": int(row[2]),
                "qty": int(row[3]),
            })

        # Costs summary
        costs_summary = {}
        total_crossdocking = 0
        total_storage = 0
        total_fbo_processing = 0
        for row in costs_data.result_rows:
            op_type = row[0]
            amount = float(row[3])
            costs_summary[op_type] = {
                "name": row[1],
                "count": int(row[2]),
                "amount": round(amount, 2),
            }
            if op_type == "MarketplaceServiceItemCrossdocking":
                total_crossdocking = amount
            elif op_type == "OperationMarketplaceServiceStorage":
                total_storage = amount
            elif op_type == "OperationMarketplaceSupplyAdditional":
                total_fbo_processing = amount

        # Build warehouse list
        warehouses = []
        total_stock = 0
        total_skus_set = set()

        for row in stocks_data.result_rows:
            wh_name = row[0]
            wh_type = row[1]
            skus = row[2]
            offer_ids = row[3]
            names = row[4]
            frees = row[5]
            reserveds = row[6]
            sku_count = int(row[7])
            stock_free = int(row[8])
            stock_reserved = int(row[9])

            total_stock += stock_free
            total_skus_set.update(skus)

            # Get orders for this warehouse
            wh_orders = orders_by_wh.get(wh_name, {})
            order_qty = wh_orders.get("qty", 0)
            order_revenue = wh_orders.get("revenue", 0)
            order_count = wh_orders.get("orders", 0)

            # Calculate daily sales and turnover
            daily_sales = order_qty / period if period > 0 else 0
            turnover_days = stock_free / daily_sales if daily_sales > 0 else 9999
            days_to_zero = int(turnover_days) if turnover_days < 9999 else None

            # Determine cluster
            cluster = _get_cluster_for_warehouse(wh_name)

            # Calculate average delivery speed
            delivery_speed_avg = 28  # default local
            geo = geo_by_wh.get(wh_name, [])
            if geo:
                cluster_routes = DELIVERY_HOURS.get(cluster, {})
                total_weighted = 0
                total_geo_qty = 0
                for g in geo:
                    dest_cluster = g["cluster"]
                    hours = cluster_routes.get(dest_cluster, 60)
                    total_weighted += hours * g["qty"]
                    total_geo_qty += g["qty"]
                if total_geo_qty > 0:
                    delivery_speed_avg = round(total_weighted / total_geo_qty, 1)

            # Storage cost estimation
            storage_risk = "ok"
            estimated_storage_cost = 0
            if turnover_days > 180:
                storage_risk = "critical"
                estimated_storage_cost = stock_free * 1.50  # rough per-day
            elif turnover_days > 160:
                storage_risk = "warning"
                estimated_storage_cost = stock_free * 0.75

            # Status
            if stock_free == 0 and order_qty > 0:
                wh_status = "empty"
            elif turnover_days < 14:
                wh_status = "critical"
            elif turnover_days < 30:
                wh_status = "attention"
            elif turnover_days > 180:
                wh_status = "overstocked"
            elif turnover_days > 160:
                wh_status = "storage_fee"
            else:
                wh_status = "ok"

            # Total sales for % calculation
            total_orders_qty = sum(o.get("qty", 0) for o in orders_by_wh.values())
            pct_of_sales = round(order_qty / total_orders_qty * 100, 1) if total_orders_qty > 0 else 0

            # SKU details
            sku_details = []
            for i in range(len(skus)):
                sku_id = int(skus[i])
                sku_free = int(frees[i])
                sku_turnover = turnover_data.get(sku_id, {})
                sku_daily = sku_turnover.get("avg_daily_sales", 0)
                sku_days_supply = sku_turnover.get("days_of_supply", 0)

                # Fallback: calculate from orders if no turnover data
                if sku_daily == 0 and daily_sales > 0 and sku_count > 0:
                    sku_daily = daily_sales / sku_count  # rough approximation

                sku_details.append({
                    "sku": sku_id,
                    "offer_id": offer_ids[i],
                    "name": names[i],
                    "stock": sku_free,
                    "reserved": int(reserveds[i]),
                    "daily_sales": round(sku_daily, 2),
                    "days_supply": round(sku_days_supply, 1) if sku_days_supply > 0 else (
                        round(sku_free / sku_daily, 0) if sku_daily > 0 else None
                    ),
                    "turnover_category": sku_turnover.get("turnover_category", ""),
                })

            # Sort SKU details: highest stock first
            sku_details.sort(key=lambda x: x["stock"], reverse=True)

            # Geography with shares
            geo_total = sum(g["qty"] for g in geo) if geo else 0
            clusters_served = []
            for g in sorted(geo, key=lambda x: x["qty"], reverse=True)[:10]:
                clusters_served.append({
                    "cluster": g["cluster"],
                    "orders": g["orders"],
                    "qty": g["qty"],
                    "share": round(g["qty"] / geo_total * 100, 1) if geo_total > 0 else 0,
                })

            # Per-warehouse costs
            wh_costs = costs_by_wh.get(wh_name, {})

            warehouses.append({
                "warehouse_name": wh_name,
                "cluster": cluster,
                "warehouse_type": wh_type,
                "stock_free": stock_free,
                "stock_reserved": stock_reserved,
                "sku_count": sku_count,
                "orders_period": order_count,
                "qty_period": order_qty,
                "revenue_period": round(order_revenue, 2),
                "daily_sales": round(daily_sales, 2),
                "turnover_days": round(turnover_days, 1) if turnover_days < 9999 else None,
                "days_to_zero": days_to_zero,
                "pct_of_total_sales": pct_of_sales,
                "delivery_speed_avg_h": delivery_speed_avg,
                "status": wh_status,
                "storage_risk": storage_risk,
                "estimated_storage_cost_day": round(estimated_storage_cost, 2),
                "costs": {
                    "crossdocking": round(wh_costs.get("crossdocking", 0), 2),
                    "crossdocking_cnt": wh_costs.get("crossdocking_cnt", 0),
                    "storage": round(wh_costs.get("storage", 0), 2),
                    "storage_cnt": wh_costs.get("storage_cnt", 0),
                    "fbo_processing": round(wh_costs.get("fbo_processing", 0), 2),
                    "fbo_cnt": wh_costs.get("fbo_cnt", 0),
                    "total": round(
                        wh_costs.get("crossdocking", 0)
                        + wh_costs.get("storage", 0)
                        + wh_costs.get("fbo_processing", 0), 2
                    ),
                },
                "clusters_served": clusters_served,
                "skus": sku_details,
            })

        # Sort: by daily sales descending
        warehouses.sort(key=lambda w: w["daily_sales"], reverse=True)

        # Averages
        wh_with_sales = [w for w in warehouses if w["daily_sales"] > 0]
        avg_turnover = (
            round(sum(w["turnover_days"] for w in wh_with_sales if w["turnover_days"])
                  / len(wh_with_sales), 1)
            if wh_with_sales else None
        )
        avg_delivery = (
            round(sum(w["delivery_speed_avg_h"] for w in wh_with_sales)
                  / len(wh_with_sales), 1)
            if wh_with_sales else None
        )

        critical_count = sum(1 for w in warehouses if w["status"] in ("critical", "empty"))
        overstocked_count = sum(1 for w in warehouses if w["status"] in ("overstocked", "storage_fee"))

        # ── Generate recommendations ─────────────────────────────
        recommendations = []

        # 1. Overstocked → recommend moving to low-stock warehouses
        overstocked_whs = [w for w in warehouses
                           if w["status"] in ("overstocked", "storage_fee")
                           and w["stock_free"] > 0]
        low_stock_whs = [w for w in warehouses
                         if w["status"] in ("critical", "attention")
                         and w["daily_sales"] > 0]

        for ow in overstocked_whs:
            excess_days = (ow["turnover_days"] or 9999) - 90  # target: 90 days
            if excess_days > 0 and ow["daily_sales"] > 0:
                excess_qty = int(excess_days * ow["daily_sales"])
            else:
                excess_qty = ow["stock_free"]

            # Find best target warehouse (same SKUs, low stock)
            ow_skus = {s["sku"] for s in ow["skus"] if s["stock"] > 10}
            for lw in low_stock_whs:
                if lw["warehouse_name"] == ow["warehouse_name"]:
                    continue
                lw_skus = {s["sku"] for s in lw["skus"]}
                common = ow_skus & lw_skus
                if common:
                    recommendations.append({
                        "type": "move_stock",
                        "severity": "high" if ow["status"] == "overstocked" else "medium",
                        "from_warehouse": ow["warehouse_name"],
                        "from_cluster": ow["cluster"],
                        "to_warehouse": lw["warehouse_name"],
                        "to_cluster": lw["cluster"],
                        "reason": f"{ow['warehouse_name']} перезатарен ({int(ow['turnover_days'] or 0)} дн), "
                                  f"{lw['warehouse_name']} ({lw['status']}, {int(lw['turnover_days'] or 0)} дн)",
                        "affected_skus": len(common),
                        "excess_qty": excess_qty,
                    })
                    break  # one rec per overstocked wh

        # 2. Warehouses with high crossdocking → recommend direct supply
        for w in warehouses:
            cd_cost = abs(w["costs"].get("crossdocking", 0))
            if cd_cost > 5000 and w["daily_sales"] > 0.5:
                recommendations.append({
                    "type": "optimize_crossdocking",
                    "severity": "medium",
                    "warehouse": w["warehouse_name"],
                    "cluster": w["cluster"],
                    "crossdocking_cost": round(cd_cost, 2),
                    "daily_sales": w["daily_sales"],
                    "reason": f"Кроссдокинг {w['warehouse_name']}: {round(cd_cost):,} ₽ за {period} дн. "
                              f"Рассмотри прямую поставку вместо кроссдокинга.",
                })

        # 3. Warehouses approaching paid storage threshold
        for w in warehouses:
            td = w["turnover_days"]
            if td and 120 < td <= 160:
                days_left = int(160 - td)
                recommendations.append({
                    "type": "storage_warning",
                    "severity": "medium",
                    "warehouse": w["warehouse_name"],
                    "cluster": w["cluster"],
                    "turnover_days": round(td),
                    "days_to_paid": days_left,
                    "stock": w["stock_free"],
                    "reason": f"{w['warehouse_name']}: оборачиваемость {int(td)} дн, "
                              f"до платного хранения ~{days_left} дн. Снизь стоки или цену.",
                })

        # 4. Already paying for storage
        for w in warehouses:
            td = w["turnover_days"]
            if td and td > 160:
                est_cost = w["costs"].get("storage", 0)
                recommendations.append({
                    "type": "paid_storage",
                    "severity": "high",
                    "warehouse": w["warehouse_name"],
                    "cluster": w["cluster"],
                    "turnover_days": round(td),
                    "stock": w["stock_free"],
                    "storage_cost": round(abs(est_cost), 2),
                    "reason": f"{w['warehouse_name']}: оборачиваемость {int(td)} дн — ПЛАТНОЕ хранение! "
                              f"Стоки: {w['stock_free']} ед. Расход: {round(abs(est_cost)):,} ₽.",
                })

        # Sort recs: high severity first
        severity_order = {"high": 0, "medium": 1, "low": 2}
        recommendations.sort(key=lambda r: severity_order.get(r["severity"], 9))

        return {
            "kpi": {
                "total_warehouses": len(warehouses),
                "total_stock": total_stock,
                "total_skus": len(total_skus_set),
                "avg_turnover_days": avg_turnover,
                "avg_delivery_h": avg_delivery,
                "total_crossdocking": round(total_crossdocking, 2),
                "total_storage_fee": round(total_storage, 2),
                "total_fbo_processing": round(total_fbo_processing, 2),
                "critical_warehouses": critical_count,
                "overstocked_warehouses": overstocked_count,
                "period_days": period,
            },
            "costs": costs_summary,
            "warehouses": warehouses,
            "recommendations": recommendations,
        }

    finally:
        ch.close()
