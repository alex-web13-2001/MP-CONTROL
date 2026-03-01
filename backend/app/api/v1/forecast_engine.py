"""Forecast recommendation engine — profit-focused «Сейчас → Будет → Делай».

Generates actionable recommendations per SKU with three columns:
- now: current metrics (last 14 days extrapolated)
- forecast: predicted metrics (next N days)
- action: specific steps to improve profit with impact estimates
"""
from __future__ import annotations

import logging
from typing import TypedDict

logger = logging.getLogger(__name__)


class SkuAnalysis(TypedDict):
    severity: str           # critical | warning | opportunity | ok
    title: str              # short summary
    now: dict               # current state metrics
    forecast: dict          # forecast metrics
    actions: list[dict]     # [{text, profit_impact, priority}]


def generate_sku_recommendations(
    *,
    # Historical (actual) period metrics
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
    avg_price: float,
    # Forecast metrics (next N days)
    forecast_revenue: float,
    forecast_orders: int,
    forecast_profit: float,
    forecast_margin_pct: float,
    forecast_ad_spend: float,
    # Trends
    revenue_trend_pct: float,
    orders_trend_pct: float,
    # Config
    period_days: int = 120,
    forecast_days: int = 30,
) -> SkuAnalysis:
    """Generate profit-focused analysis for a single SKU.

    Returns SkuAnalysis with severity, now/forecast state, and actions.
    """

    # ── Normalize to daily rates ──
    daily_revenue = revenue / max(period_days, 1)
    daily_orders = orders / max(period_days, 1)
    daily_ad = ad_spend / max(period_days, 1)
    daily_profit = profit / max(period_days, 1)

    fc_daily_rev = forecast_revenue / max(forecast_days, 1)
    fc_daily_profit = forecast_profit / max(forecast_days, 1)
    fc_daily_ad = forecast_ad_spend / max(forecast_days, 1)

    # DRR (ad spend / revenue %)
    drr = round(ad_spend / revenue * 100, 1) if revenue > 0 else 0
    fc_drr = round(forecast_ad_spend / forecast_revenue * 100, 1) if forecast_revenue > 0 else 0

    # ── Build NOW state ──
    now = {
        "profit": round(profit),
        "profit_daily": round(daily_profit),
        "margin_pct": margin_pct,
        "revenue": round(revenue),
        "orders": orders,
        "ad_spend": round(ad_spend),
        "drr": drr,
        "roi": roi,
        "ctr": ctr,
        "avg_price": round(avg_price),
    }

    # ── Build FORECAST state ──
    forecast = {
        "profit": round(forecast_profit),
        "profit_daily": round(fc_daily_profit),
        "margin_pct": forecast_margin_pct,
        "revenue": round(forecast_revenue),
        "orders": forecast_orders,
        "ad_spend": round(forecast_ad_spend),
        "drr": fc_drr,
    }

    # ── Determine severity ──
    actions: list[dict] = []

    if profit < 0 or forecast_profit < 0:
        severity = "critical"
    elif margin_pct < 5 or forecast_margin_pct < 5:
        severity = "warning"
    elif revenue_trend_pct > 10 and margin_pct > 15:
        severity = "opportunity"
    else:
        severity = "ok"

    # ── Generate profit-focused actions ──

    # === CASE 1: Currently losing money ===
    if profit < 0:
        loss = abs(profit)
        title = f"Убыток {_fmt(loss)} за период"

        # Причина 1: Высокий ДРР
        if ad_spend > 0 and drr > 25:
            # Сколько нужно снизить рекламу чтобы выйти в 0?
            breakeven_ad = max(revenue - commission - logistics - cogs, 0)
            ad_reduction = ad_spend - breakeven_ad
            if ad_reduction > 0:
                actions.append({
                    "text": f"Снизить рекламу с {_fmt(round(daily_ad))}/день до {_fmt(round(breakeven_ad / max(period_days, 1)))}/день",
                    "profit_impact": f"+{_fmt(round(ad_reduction))} к прибыли",
                    "priority": 1,
                })

        # Причина 2: Цена слишком низкая
        if orders > 0 and revenue > 0:
            # На сколько нужно поднять цену?
            loss_per_order = abs(profit) / orders
            price_increase_pct = round(loss_per_order / avg_price * 100, 0) if avg_price > 0 else 0
            if price_increase_pct > 0 and price_increase_pct <= 30:
                new_profit_est = round(loss_per_order * orders)
                actions.append({
                    "text": f"Поднять цену на {price_increase_pct:.0f}% (+{_fmt(round(loss_per_order))} к цене)",
                    "profit_impact": f"+{_fmt(new_profit_est)} к прибыли (выход в 0)",
                    "priority": 2,
                })

        # Если совсем убыточно
        if margin_pct < -20:
            actions.append({
                "text": "Снять товар с продвижения полностью",
                "profit_impact": f"Экономия {_fmt(round(ad_spend))} на рекламе",
                "priority": 3,
            })

        if not actions:
            actions.append({
                "text": "Пересмотреть себестоимость и ценообразование",
                "profit_impact": f"Текущий убыток {_fmt(loss)}",
                "priority": 1,
            })

    # === CASE 2: Profitable now, but forecast shows loss ===
    elif forecast_profit < 0 and profit >= 0:
        title = f"Прогноз убытка {_fmt(abs(round(forecast_profit)))}"

        if revenue_trend_pct < -10:
            actions.append({
                "text": "Продажи падают — обновить карточку, проверить конкурентов",
                "profit_impact": f"Без действий прибыль: {_fmt(round(forecast_profit))}",
                "priority": 1,
            })

        if fc_drr > drr + 5:
            actions.append({
                "text": f"ДРР растёт ({drr}% → {fc_drr}%) — оптимизировать кампании",
                "profit_impact": f"Снизить ДРР до {drr}% = +{_fmt(round(forecast_revenue * (fc_drr - drr) / 100))}",
                "priority": 1,
            })

        if not actions:
            actions.append({
                "text": "Контролировать расходы, тренд негативный",
                "profit_impact": f"Прогноз прибыли: {_fmt(round(forecast_profit))}",
                "priority": 1,
            })

    # === CASE 3: Low margin (0-5%) ===
    elif margin_pct < 5:
        title = f"Низкая маржа {margin_pct}%"

        if drr > 20 and ad_spend > 0:
            # Снизить рекламу на 30% → сколько прибыли добавит
            ad_save = round(ad_spend * 0.3)
            actions.append({
                "text": f"Снизить рекламу на 30% (−{_fmt(ad_save)})",
                "profit_impact": f"+{_fmt(ad_save)} к прибыли",
                "priority": 1,
            })

        if avg_price > 0:
            # Поднять цену на 5%
            price_bump_rev = round(revenue * 0.05)
            actions.append({
                "text": f"Поднять цену на 5% (+{_fmt(round(avg_price * 0.05))})",
                "profit_impact": f"+{_fmt(price_bump_rev)} к прибыли",
                "priority": 2,
            })

        if not actions:
            actions.append({
                "text": "Оптимизировать расходы для увеличения маржи",
                "profit_impact": f"Текущая маржа всего {margin_pct}%",
                "priority": 1,
            })

    # === CASE 4: Good margins + growth = opportunity ===
    elif margin_pct > 15 and revenue_trend_pct > 5:
        title = f"Возможность роста (маржа {margin_pct}%)"

        if ad_spend > 0 and roi > 200:
            # Увеличить бюджет на 50%
            extra_ad = round(ad_spend * 0.5)
            extra_rev_est = round(extra_ad * (roi / 100))
            extra_profit_est = round(extra_rev_est * margin_pct / 100)
            actions.append({
                "text": f"Увеличить рекл. бюджет на 50% (+{_fmt(extra_ad)})",
                "profit_impact": f"+{_fmt(extra_profit_est)} доп. прибыли (ROI {roi}%)",
                "priority": 1,
            })

        if ad_spend == 0 and orders > 3:
            est_daily_ad = round(avg_price * 0.15)  # 15% от цены
            actions.append({
                "text": f"Запустить рекламу (~{_fmt(est_daily_ad)}/день)",
                "profit_impact": "Потенциал роста продаж при хорошей марже",
                "priority": 1,
            })

        if not actions:
            actions.append({
                "text": "Увеличить запасы — товар растёт",
                "profit_impact": f"Тренд +{revenue_trend_pct:.0f}%, маржа {margin_pct}%",
                "priority": 1,
            })

    # === CASE 5: Stable, nothing critical ===
    else:
        title = f"Стабильно (маржа {margin_pct}%)"

        if ad_spend > 0 and drr > 25:
            ad_save = round(ad_spend * 0.2)
            actions.append({
                "text": f"Оптимизировать рекламу — ДРР {drr}% (снизить на 20%)",
                "profit_impact": f"+{_fmt(ad_save)} к прибыли",
                "priority": 2,
            })

        if ctr > 0 and ctr < 1.5 and ad_spend > 500:
            actions.append({
                "text": f"Обновить главное фото (CTR {ctr}% — ниже среднего)",
                "profit_impact": "Рост CTR → больше заказов → больше прибыли",
                "priority": 3,
            })

        if not actions:
            actions.append({
                "text": "Продолжать в текущем режиме",
                "profit_impact": f"Прогноз прибыли: {_fmt(round(forecast_profit))}",
                "priority": 3,
            })

    # Sort by priority
    actions.sort(key=lambda a: a.get("priority", 9))

    return {
        "severity": severity,
        "title": title,
        "now": now,
        "forecast": forecast,
        "actions": actions,
    }


def _fmt(v: float | int) -> str:
    """Format money value."""
    return f"{v:,.0f} ₽".replace(",", " ")
