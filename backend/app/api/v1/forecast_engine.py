"""Forecast recommendation engine — rule-based recommendations per SKU.

Analyzes forecast metrics and generates actionable recommendations
with severity levels (critical, warning, opportunity, info).
"""
from __future__ import annotations

import logging
from typing import TypedDict

logger = logging.getLogger(__name__)


class Recommendation(TypedDict):
    type: str       # critical | warning | opportunity | info
    message: str
    action: str
    metric: str     # which metric triggered it


def generate_sku_recommendations(
    *,
    # Historical (actual) metrics
    revenue: float,
    orders: int,
    ad_spend: float,
    commission: float,
    logistics: float,
    cogs: float,
    profit: float,
    margin_pct: float,
    roi: float,
    ctr: float,
    cart_rate: float,
    # Forecast metrics
    forecast_revenue: float,
    forecast_orders: int,
    forecast_profit: float,
    forecast_margin_pct: float,
    # Trend
    revenue_trend_pct: float,
    orders_trend_pct: float,
) -> list[Recommendation]:
    """Generate rule-based recommendations for a single SKU.

    Returns list of recommendations sorted by severity.
    """
    recs: list[Recommendation] = []

    # ── Critical: selling at a loss ──
    if margin_pct < 0:
        recs.append({
            "type": "critical",
            "message": f"Продажа в убыток! Маржинальность {margin_pct:.1f}%",
            "action": "Поднять цену или снять с рекламы",
            "metric": "margin",
        })
    elif forecast_margin_pct < 0 and margin_pct >= 0:
        recs.append({
            "type": "critical",
            "message": f"Прогнозируемый убыток! Маржа прогноз {forecast_margin_pct:.1f}%",
            "action": "Оптимизировать расходы или поднять цену",
            "metric": "forecast_margin",
        })

    # ── Warning: low margin ──
    if 0 <= margin_pct < 5 and margin_pct >= 0:
        recs.append({
            "type": "warning",
            "message": f"Критически низкая маржинальность ({margin_pct:.1f}%)",
            "action": "Пересмотреть ценообразование и расходы",
            "metric": "margin",
        })

    # ── Warning: unprofitable ads ──
    if ad_spend > 0 and roi < 100:
        recs.append({
            "type": "warning",
            "message": f"Реклама убыточна (ROI {roi:.0f}%)",
            "action": "Снизить рекламный бюджет или оптимизировать кампании",
            "metric": "roi",
        })

    # ── Warning: ad spend too high ──
    if revenue > 0 and ad_spend > 0:
        ad_share = ad_spend / revenue * 100
        if ad_share > 30:
            recs.append({
                "type": "warning",
                "message": f"Рекл. расходы {ad_share:.0f}% от выручки",
                "action": "Снизить расходы на рекламу до 15-25%",
                "metric": "ad_share",
            })

    # ── Warning: sales declining ──
    if revenue_trend_pct < -15:
        recs.append({
            "type": "warning",
            "message": f"Продажи падают ({revenue_trend_pct:+.1f}%)",
            "action": "Проверить карточку, цену и конкурентов",
            "metric": "revenue_trend",
        })

    # ── Warning: low CTR ──
    if ctr > 0 and ctr < 1.0 and ad_spend > 500:
        recs.append({
            "type": "warning",
            "message": f"Низкий CTR ({ctr:.2f}%)",
            "action": "Обновить главное фото и заголовок",
            "metric": "ctr",
        })

    # ── Warning: low cart rate ──
    if cart_rate > 0 and cart_rate < 5.0 and ad_spend > 500:
        recs.append({
            "type": "warning",
            "message": f"Низкая конверсия в корзину ({cart_rate:.1f}%)",
            "action": "Улучшить описание, добавить фото, снизить цену",
            "metric": "cart_rate",
        })

    # ── Opportunity: high ROI ──
    if ad_spend > 0 and roi > 300:
        recs.append({
            "type": "opportunity",
            "message": f"Высокий ROI ({roi:.0f}%)",
            "action": "Увеличить рекламный бюджет для масштабирования",
            "metric": "roi",
        })

    # ── Opportunity: organic growth ──
    if revenue_trend_pct > 10 and (ad_spend == 0 or (revenue > 0 and ad_spend / revenue < 0.05)):
        recs.append({
            "type": "opportunity",
            "message": f"Органический рост ({revenue_trend_pct:+.1f}%)",
            "action": "Подключить рекламу для ускорения роста",
            "metric": "organic_growth",
        })

    # ── Opportunity: growing sales with good margins ──
    if revenue_trend_pct > 10 and margin_pct > 20:
        recs.append({
            "type": "opportunity",
            "message": f"Рост продаж при хорошей марже ({margin_pct:.0f}%)",
            "action": "Увеличить запасы и рекламный бюджет",
            "metric": "growth_margin",
        })

    # ── Info: no ads ──
    if ad_spend == 0 and orders > 5:
        recs.append({
            "type": "info",
            "message": "Продажи без рекламы",
            "action": "Рассмотреть запуск рекламы для масштабирования",
            "metric": "no_ads",
        })

    # Sort: critical > warning > opportunity > info
    severity_order = {"critical": 0, "warning": 1, "opportunity": 2, "info": 3}
    recs.sort(key=lambda r: severity_order.get(r["type"], 9))

    return recs
