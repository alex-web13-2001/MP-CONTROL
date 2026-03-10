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

    # === Sheet 6: Анализ логистики ===
    THRESHOLD_HOURS = 29  # Ozon recommended avg delivery time

    ws6 = wb.create_sheet("Анализ логистики")
    h6_headers = [
        ("Артикул", 24), ("Название", 40), ("Склад отгрузки", 28),
        ("Кластер назначения", 28), ("Объём (шт)", 10), ("Время, ч", 10),
        ("Превышает порог", 10), ("Влияние (шт×ч)", 14), ("Доля влияния %", 14),
        ("Ср. время доставки", 14), ("Рекомендация", 50),
    ]
    for ci, (name, w) in enumerate(h6_headers, 1):
        c = ws6.cell(1, ci, name)
        c.font = hdr_font
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", wrap_text=True)
        ws6.column_dimensions[get_column_letter(ci)].width = w
    ws6.freeze_panes = "A2"

    # Threshold info row
    ws6.cell(2, 1, "⚙ Порог: 29 часов")
    ws6.cell(2, 1).font = Font(bold=True, size=11, color="1F4E79")
    ws6.cell(2, 2, "Маршруты ≤29ч не влияют на среднее время доставки (множитель 0)")
    ws6.cell(2, 2).font = Font(italic=True, size=10, color="666666")
    for ci2 in range(1, len(h6_headers) + 1):
        ws6.cell(2, ci2).fill = PatternFill("solid", fgColor="DAEEF3")
    row6 = 4

    # Orange/red fills for high influence
    high_fill = PatternFill("solid", fgColor="FCE4D6")
    critical_font = Font(bold=True, color="CC0000")
    warn_font = Font(bold=True, color="CC8800")
    ok_font = Font(color="00AA00")

    for item in items:
        if not item["clusters"]:
            continue

        total_sold_item = item["sold"]
        if total_sold_item <= 0:
            continue

        # 1. Calculate per-route influence
        routes = []
        for cl in item["clusters"]:
            vol = cl["sold"]
            hub = cl["hub"]
            hours = cl["hub_hours"]
            exceeds = hours > THRESHOLD_HOURS
            influence = vol * hours if exceeds else 0
            routes.append({
                "cluster": cl["cluster"],
                "hub": hub,
                "vol": vol,
                "hours": hours,
                "exceeds": exceeds,
                "influence": influence,
            })

        total_influence = sum(r["influence"] for r in routes)

        # 2. Calculate weighted avg delivery time for this SKU
        weighted_hours = sum(r["vol"] * r["hours"] for r in routes)
        avg_hours = weighted_hours / total_sold_item if total_sold_item > 0 else 0

        # 3. Generate recommendation
        recommendation = ""
        problem_routes = [r for r in routes if r["exceeds"]]
        if avg_hours <= THRESHOLD_HOURS:
            recommendation = "✅ Среднее время ≤29ч — оптимально"
        elif problem_routes:
            top_problem = max(problem_routes, key=lambda r: r["influence"])
            pct_top = (top_problem["influence"] / total_influence * 100) if total_influence > 0 else 0
            # Check if consolidating to a different hub would help
            dest_cluster = top_problem["cluster"]
            # Can we find a closer hub for the demand cluster?
            local_hours = DELIVERY_HOURS.get(dest_cluster, {}).get(dest_cluster, 28)
            if local_hours <= THRESHOLD_HOURS and top_problem["hours"] > THRESHOLD_HOURS:
                recommendation = (
                    f"⚠ Разбить поставку: {top_problem['vol']} шт → {dest_cluster} "
                    f"(вместо {top_problem['hub']}). "
                    f"Время: {top_problem['hours']}ч → {local_hours}ч. "
                    f"Это снизит влияние на {pct_top:.0f}%"
                )
            else:
                recommendation = (
                    f"⚠ Маршрут {top_problem['hub']}→{dest_cluster} "
                    f"({top_problem['hours']}ч) влияет на {pct_top:.0f}%. "
                    f"Рассмотрите прямую поставку"
                )

        # SKU header row
        sku_hdr_fill = PatternFill("solid", fgColor="D6E4F0")
        c = ws6.cell(row6, 1, item["offer_id"])
        c.font = Font(bold=True, size=11)
        ws6.cell(row6, 2, item["name"]).font = Font(bold=True, size=10)
        ws6.cell(row6, 10, round(avg_hours, 1)).font = (
            critical_font if avg_hours > 45
            else warn_font if avg_hours > THRESHOLD_HOURS
            else ok_font
        )
        ws6.cell(row6, 10).number_format = "0.0"
        ws6.cell(row6, 11, recommendation).font = Font(size=10)
        if avg_hours > THRESHOLD_HOURS:
            ws6.cell(row6, 11).font = Font(bold=True, size=10, color="CC6600")
        for ci2 in range(1, len(h6_headers) + 1):
            ws6.cell(row6, ci2).fill = sku_hdr_fill
        row6 += 1

        # Route detail rows
        for r in sorted(routes, key=lambda x: x["influence"], reverse=True):
            share_pct = (r["influence"] / total_influence * 100) if total_influence > 0 else 0

            ws6.cell(row6, 3, r["hub"])
            ws6.cell(row6, 4, r["cluster"])
            ws6.cell(row6, 5, r["vol"]).number_format = num_fmt
            c_h = ws6.cell(row6, 6, r["hours"])
            if r["hours"] <= 28:
                c_h.font = ok_font
            elif r["hours"] <= 45:
                c_h.font = warn_font
            else:
                c_h.font = critical_font

            ws6.cell(row6, 7, "Да" if r["exceeds"] else "Нет").font = (
                Font(bold=True, color="CC0000") if r["exceeds"]
                else Font(color="00AA00")
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
