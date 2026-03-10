"""
Finances Export — Excel report generation for Ozon & WB.

GET /finances/ozon/excel?shop_id=X&date_from=Y&date_to=Z
    → Returns .xlsx with 6 sheets: Сводка, Транзакции, По неделям,
      По месяцам, По товарам (SKU), Расходы детально
"""
import io
import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.utils import get_column_letter

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/finances", tags=["Finances Export"])

# ── Styles ────────────────────────────────────────────────────

HEADER_FILL = PatternFill(start_color="2563EB", end_color="2563EB", fill_type="solid")
HEADER_FONT = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
SECTION_FILL = PatternFill(start_color="EFF6FF", end_color="EFF6FF", fill_type="solid")
SECTION_FONT = Font(name="Calibri", bold=True, size=11)
TOTAL_FILL = PatternFill(start_color="DBEAFE", end_color="DBEAFE", fill_type="solid")
TOTAL_FONT = Font(name="Calibri", bold=True, size=11)
NORMAL_FONT = Font(name="Calibri", size=10)
MONEY_FMT = '#,##0'
MONEY_FMT_2 = '#,##0.00'
PCT_FMT = '0.0%'
DATE_FMT = 'DD.MM.YYYY'
THIN_BORDER = Border(
    left=Side(style='thin', color='D1D5DB'),
    right=Side(style='thin', color='D1D5DB'),
    top=Side(style='thin', color='D1D5DB'),
    bottom=Side(style='thin', color='D1D5DB'),
)
ALT_ROW_FILL = PatternFill(start_color="F9FAFB", end_color="F9FAFB", fill_type="solid")
GREEN_FONT = Font(name="Calibri", bold=True, color="16A34A", size=10)
RED_FONT = Font(name="Calibri", bold=True, color="DC2626", size=10)
PROFIT_GREEN = Font(name="Calibri", bold=True, color="16A34A", size=11)
PROFIT_RED = Font(name="Calibri", bold=True, color="DC2626", size=11)


def _style_header_row(ws, row_num: int, col_count: int):
    """Apply header styling to a row."""
    for col in range(1, col_count + 1):
        cell = ws.cell(row=row_num, column=col)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        cell.border = THIN_BORDER


def _style_data_row(ws, row_num: int, col_count: int, is_alt: bool = False):
    """Apply data row styling."""
    for col in range(1, col_count + 1):
        cell = ws.cell(row=row_num, column=col)
        cell.font = NORMAL_FONT
        cell.border = THIN_BORDER
        if is_alt:
            cell.fill = ALT_ROW_FILL


def _style_total_row(ws, row_num: int, col_count: int):
    """Apply total row styling."""
    for col in range(1, col_count + 1):
        cell = ws.cell(row=row_num, column=col)
        cell.font = TOTAL_FONT
        cell.fill = TOTAL_FILL
        cell.border = THIN_BORDER


def _auto_width(ws, min_width: int = 10, max_width: int = 40):
    """Auto-adjust column widths."""
    for col_cells in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col_cells[0].column)
        for cell in col_cells:
            if cell.value is not None:
                max_len = max(max_len, len(str(cell.value)))
        adjusted = min(max(max_len + 2, min_width), max_width)
        ws.column_dimensions[col_letter].width = adjusted


def _safe_delta(current: float, previous: float) -> float:
    if previous == 0:
        return 100.0 if current > 0 else (-100.0 if current < 0 else 0.0)
    return round((current - previous) / abs(previous) * 100, 1)


MONTHS_SHORT_RU = {
    1: "янв.", 2: "фев.", 3: "март.", 4: "апр.", 5: "мая", 6: "июн.",
    7: "июл.", 8: "авг.", 9: "сен.", 10: "окт.", 11: "нояб.", 12: "дек.",
}


def _fmt_date_ru(d) -> str:
    """Format date as '2 март.' for Excel readability."""
    if isinstance(d, str):
        d = date.fromisoformat(d)
    return f"{d.day} {MONTHS_SHORT_RU.get(d.month, str(d.month))}"


CAT_MAP = {
    "Logistics": "logistics",
    "Storage": "storage",
    "Acquiring": "acquiring",
    "Refund": "refunds",
    "Penalty": "penalties",
    "Compensation": "compensation",
    "Marketing": "marketing",
}


# ══════════════════════════════════════════════════════════════
# Endpoint
# ══════════════════════════════════════════════════════════════

@router.get("/ozon/excel")
async def export_ozon_excel(
    shop_id: int = Query(..., description="Shop ID"),
    date_from: date = Query(..., description="Start date"),
    date_to: date = Query(..., description="End date"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Generate and download comprehensive Ozon Excel financial report."""

    # ── Verify shop ──
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

    d_start = date_from
    d_end = date_to
    span = (d_end - d_start).days + 1
    d_prev_start = d_start - timedelta(days=span)
    d_prev_end = d_start - timedelta(days=1)

    # ══════════════════════════════════════════════════════════
    # Fetch all data from ClickHouse
    # ══════════════════════════════════════════════════════════

    # 1. Aggregate KPI (revenue, commission, services, payout, orders)
    txn_totals = ch.query("""
        SELECT
            sumIf(accruals_for_sale,
                toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                AND category = 'Revenue') AS accruals_cur,
            sumIf(accruals_for_sale,
                toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                AND category = 'Revenue') AS accruals_prev,
            sumIf(sale_commission,
                toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                AND category = 'Revenue') AS comm_cur,
            sumIf(sale_commission,
                toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                AND category = 'Revenue') AS comm_prev,
            sumIf(services_total,
                toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                AND category = 'Revenue') AS svc_cur,
            sumIf(services_total,
                toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                AND category = 'Revenue') AS svc_prev,
            sumIf(amount,
                toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                ) AS pay_cur,
            sumIf(amount,
                toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                ) AS pay_prev,
            countIf(
                toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}
                AND category = 'Revenue') AS ord_cur,
            countIf(
                toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}
                AND category = 'Revenue') AS ord_prev
        FROM mms_analytics.fact_ozon_transactions FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND toDate(operation_date) >= {d_prev_start:Date}
          AND toDate(operation_date) <= {d_end:Date}
    """, parameters={
        "shop_id": shop_id,
        "d_start": d_start, "d_end": d_end,
        "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
    })

    r = txn_totals.result_rows[0] if txn_totals.result_rows else [0]*10
    revenue_cur = float(r[0] or 0)
    revenue_prev = float(r[1] or 0)
    commission_cur = abs(float(r[2] or 0))
    commission_prev = abs(float(r[3] or 0))
    services_cur = abs(float(r[4] or 0))
    services_prev = abs(float(r[5] or 0))
    payout_cur = float(r[6] or 0)
    payout_prev = float(r[7] or 0)
    orders_cur = int(r[8] or 0)
    orders_prev = int(r[9] or 0)

    # 2. Bulk expense categories
    bulk_cur = {"logistics": 0.0, "storage": 0.0, "acquiring": 0.0, "refunds": 0.0,
                "penalties": 0.0, "compensation": 0.0, "marketing": 0.0, "other": 0.0}
    bulk_prev = dict(bulk_cur)

    cat_result = ch.query("""
        SELECT
            category,
            sumIf(amount, toDate(operation_date) >= {d_start:Date} AND toDate(operation_date) <= {d_end:Date}) AS total_cur,
            sumIf(amount, toDate(operation_date) >= {d_prev_start:Date} AND toDate(operation_date) <= {d_prev_end:Date}) AS total_prev
        FROM mms_analytics.fact_ozon_transactions FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND toDate(operation_date) >= {d_prev_start:Date}
          AND toDate(operation_date) <= {d_end:Date}
          AND category NOT IN ('Revenue')
        GROUP BY category
    """, parameters={
        "shop_id": shop_id,
        "d_start": d_start, "d_end": d_end,
        "d_prev_start": d_prev_start, "d_prev_end": d_prev_end,
    })
    for r in cat_result.result_rows:
        key = CAT_MAP.get(r[0], "other")
        if key in bulk_cur:
            bulk_cur[key] = float(r[1] or 0)
            bulk_prev[key] = float(r[2] or 0)
        else:
            bulk_cur["other"] += float(r[1] or 0)
            bulk_prev["other"] += float(r[2] or 0)

    # 3. Ad spend
    ad_spend_cur = abs(bulk_cur.get("marketing", 0))
    ad_spend_prev = abs(bulk_prev.get("marketing", 0))

    # 4. COGS
    cogs_cur = 0.0
    cogs_prev = 0.0
    try:
        sku_map_result = await db.execute(
            text("SELECT sku, offer_id FROM dim_ozon_products WHERE shop_id = :shop_id AND sku > 0"),
            {"shop_id": shop_id},
        )
        sku_to_offer = {int(r[0]): r[1] for r in sku_map_result.fetchall()}

        cost_result = await db.execute(
            text("""SELECT offer_id, COALESCE(cost_price, 0) + COALESCE(packaging_cost, 0) AS total_cost
                    FROM product_costs WHERE shop_id = :shop_id AND (cost_price > 0 OR packaging_cost > 0)"""),
            {"shop_id": shop_id},
        )
        cost_map = {r[0]: float(r[1]) for r in cost_result.fetchall()}

        if cost_map and sku_to_offer:
            sku_cost_map = {}
            for sku, offer_id in sku_to_offer.items():
                cost = cost_map.get(offer_id, 0)
                if cost > 0:
                    sku_cost_map[sku] = cost

            if sku_cost_map:
                sku_list = list(sku_cost_map.keys())
                cogs_ch = ch.query("""
                    SELECT toDate(operation_date) AS dt, sku, count() AS qty
                    FROM mms_analytics.fact_ozon_transactions FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(operation_date) >= {d_prev_start:Date}
                      AND toDate(operation_date) <= {d_end:Date}
                      AND category = 'Revenue'
                      AND sku IN {skus:Array(UInt64)}
                    GROUP BY dt, sku
                """, parameters={
                    "shop_id": shop_id,
                    "d_prev_start": d_prev_start, "d_end": d_end,
                    "skus": sku_list,
                })
                for r in cogs_ch.result_rows:
                    row_date = r[0]
                    sku = int(r[1])
                    qty = int(r[2] or 0)
                    cost = sku_cost_map.get(sku, 0)
                    if d_start <= row_date <= d_end:
                        cogs_cur += cost * qty
                    elif d_prev_start <= row_date <= d_prev_end:
                        cogs_prev += cost * qty
    except Exception as e:
        logger.warning("COGS calc failed: %s", e)

    # Derived metrics
    bulk_charges_cur = sum(abs(v) for k, v in bulk_cur.items() if k != "marketing")
    bulk_charges_prev = sum(abs(v) for k, v in bulk_prev.items() if k != "marketing")
    mp_fees_cur = commission_cur + services_cur + bulk_charges_cur
    mp_fees_prev = commission_prev + services_prev + bulk_charges_prev
    operating_cur = services_cur + bulk_charges_cur
    operating_prev = services_prev + bulk_charges_prev
    profit_cur = revenue_cur - mp_fees_cur - ad_spend_cur - cogs_cur
    profit_prev = revenue_prev - mp_fees_prev - ad_spend_prev - cogs_prev

    # 5. All raw transactions for the period
    raw_txns = ch.query("""
        SELECT
            operation_id, toDate(operation_date) AS dt, operation_type, operation_type_name,
            category, sku, item_name, amount, accruals_for_sale,
            sale_commission, services_total, posting_number, delivery_schema
        FROM mms_analytics.fact_ozon_transactions FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND toDate(operation_date) >= {d_start:Date}
          AND toDate(operation_date) <= {d_end:Date}
        ORDER BY toDate(operation_date), category, operation_type
    """, parameters={"shop_id": shop_id, "d_start": d_start, "d_end": d_end})

    # 6. Weekly aggregation — FULL RETROSPECTIVE (all weeks since shop creation)
    weekly_data = ch.query("""
        SELECT
            toMonday(toDate(operation_date)) AS week_start,
            toMonday(toDate(operation_date)) + 6 AS week_end,
            countIf(category = 'Revenue') AS qty,
            sumIf(accruals_for_sale, category = 'Revenue') AS sales,
            sumIf(abs(amount), category = 'Refund') AS returns,
            abs(sumIf(sale_commission, category = 'Revenue')) AS commission,
            sumIf(amount, category = 'Compensation') AS compensations,
            abs(sumIf(services_total, category = 'Revenue')) AS other_services,
            abs(sumIf(amount, category = 'Marketing')) AS marketing,
            abs(sumIf(amount, category IN ('Penalty', 'Other'))) AS other_charges,
            sumIf(abs(amount), category = 'Logistics' AND operation_type IN (
                'MarketplaceServiceItemCrossdocking',
                'OperationMarketplaceSupplyAdditional',
                'OperationMarketplaceSupplyExpirationDateProcessing',
                'OperationMarketplaceServiceSupplyInboundCargoShortage',
                'OperationMarketplaceServiceSupplyInboundSupplyShortage'
            )) AS fbo_services,
            abs(sumIf(amount, category = 'Acquiring')) AS acquiring,
            sumIf(abs(amount), category = 'Logistics' AND operation_type NOT IN (
                'MarketplaceServiceItemCrossdocking',
                'OperationMarketplaceSupplyAdditional',
                'OperationMarketplaceSupplyExpirationDateProcessing',
                'OperationMarketplaceServiceSupplyInboundCargoShortage',
                'OperationMarketplaceServiceSupplyInboundSupplyShortage'
            )) AS delivery_services,
            abs(sumIf(amount, category = 'Storage')) AS storage,
            sum(amount) AS payout
        FROM mms_analytics.fact_ozon_transactions FINAL
        WHERE shop_id = {shop_id:UInt32}
        GROUP BY week_start
        ORDER BY week_start
    """, parameters={"shop_id": shop_id})

    # 7. Monthly aggregation — FULL RETROSPECTIVE
    monthly_data = ch.query("""
        SELECT
            toYYYYMM(toDate(operation_date)) AS ym,
            min(toDate(operation_date)) AS m_start,
            max(toDate(operation_date)) AS m_end,
            countIf(category = 'Revenue') AS qty,
            sumIf(accruals_for_sale, category = 'Revenue') AS sales,
            sumIf(abs(amount), category = 'Refund') AS returns,
            abs(sumIf(sale_commission, category = 'Revenue')) AS commission,
            sumIf(amount, category = 'Compensation') AS compensations,
            abs(sumIf(services_total, category = 'Revenue')) AS other_services,
            abs(sumIf(amount, category = 'Marketing')) AS marketing,
            abs(sumIf(amount, category IN ('Penalty', 'Other'))) AS other_charges,
            sumIf(abs(amount), category = 'Logistics' AND operation_type IN (
                'MarketplaceServiceItemCrossdocking',
                'OperationMarketplaceSupplyAdditional',
                'OperationMarketplaceSupplyExpirationDateProcessing',
                'OperationMarketplaceServiceSupplyInboundCargoShortage',
                'OperationMarketplaceServiceSupplyInboundSupplyShortage'
            )) AS fbo_services,
            abs(sumIf(amount, category = 'Acquiring')) AS acquiring,
            sumIf(abs(amount), category = 'Logistics' AND operation_type NOT IN (
                'MarketplaceServiceItemCrossdocking',
                'OperationMarketplaceSupplyAdditional',
                'OperationMarketplaceSupplyExpirationDateProcessing',
                'OperationMarketplaceServiceSupplyInboundCargoShortage',
                'OperationMarketplaceServiceSupplyInboundSupplyShortage'
            )) AS delivery_services,
            abs(sumIf(amount, category = 'Storage')) AS storage,
            sum(amount) AS payout
        FROM mms_analytics.fact_ozon_transactions FINAL
        WHERE shop_id = {shop_id:UInt32}
        GROUP BY ym
        ORDER BY ym
    """, parameters={"shop_id": shop_id})

    # 8. Per-SKU P&L
    sku_data = ch.query("""
        SELECT
            sku,
            any(item_name) AS name,
            sumIf(accruals_for_sale, category = 'Revenue') AS revenue,
            countIf(category = 'Revenue') AS qty,
            abs(sumIf(sale_commission, category = 'Revenue')) AS commission,
            abs(sumIf(services_total, category = 'Revenue')) AS services,
            sum(amount) AS payout
        FROM mms_analytics.fact_ozon_transactions FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND toDate(operation_date) >= {d_start:Date}
          AND toDate(operation_date) <= {d_end:Date}
          AND sku > 0
        GROUP BY sku
        ORDER BY revenue DESC
    """, parameters={"shop_id": shop_id, "d_start": d_start, "d_end": d_end})

    # 9. Detailed expenses by operation_type
    expense_detail = ch.query("""
        SELECT
            operation_type,
            operation_type_name,
            category,
            count() AS cnt,
            round(sum(amount), 2) AS total,
            round(avg(amount), 2) AS avg_amount
        FROM mms_analytics.fact_ozon_transactions FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND toDate(operation_date) >= {d_start:Date}
          AND toDate(operation_date) <= {d_end:Date}
        GROUP BY operation_type, operation_type_name, category
        ORDER BY abs(sum(amount)) DESC
    """, parameters={"shop_id": shop_id, "d_start": d_start, "d_end": d_end})

    # ══════════════════════════════════════════════════════════
    # Build Excel workbook
    # ══════════════════════════════════════════════════════════

    wb = Workbook()

    # ── Sheet 1: Сводка ──────────────────────────────────────
    ws1 = wb.active
    ws1.title = "Сводка"
    ws1.sheet_properties.tabColor = "2563EB"

    # Title
    ws1.merge_cells('A1:D1')
    ws1['A1'] = f"Финансовый отчёт Ozon — {shop.name}"
    ws1['A1'].font = Font(name="Calibri", bold=True, size=16, color="2563EB")
    ws1.merge_cells('A2:D2')
    ws1['A2'] = f"Период: {d_start.strftime('%d.%m.%Y')} — {d_end.strftime('%d.%m.%Y')} ({span} дн.)"
    ws1['A2'].font = Font(name="Calibri", size=11, color="6B7280")

    # KPI section
    row = 4
    ws1.cell(row=row, column=1, value="Показатель").font = SECTION_FONT
    ws1.cell(row=row, column=2, value="Текущий период").font = SECTION_FONT
    ws1.cell(row=row, column=3, value="Предыдущий период").font = SECTION_FONT
    ws1.cell(row=row, column=4, value="Изменение %").font = SECTION_FONT
    _style_header_row(ws1, row, 4)

    kpi_rows = [
        ("Выручка (продажи)", revenue_cur, revenue_prev, True),
        ("Заказы", orders_cur, orders_prev, False),
        ("Комиссия", commission_cur, commission_prev, True),
        ("Сервисные услуги", services_cur, services_prev, True),
        ("Логистика", abs(bulk_cur["logistics"]), abs(bulk_prev["logistics"]), True),
        ("Хранение", abs(bulk_cur["storage"]), abs(bulk_prev["storage"]), True),
        ("Эквайринг", abs(bulk_cur["acquiring"]), abs(bulk_prev["acquiring"]), True),
        ("Возвраты", abs(bulk_cur["refunds"]), abs(bulk_prev["refunds"]), True),
        ("Штрафы", abs(bulk_cur["penalties"]), abs(bulk_prev["penalties"]), True),
        ("Компенсации", abs(bulk_cur["compensation"]), abs(bulk_prev["compensation"]), True),
        ("Прочее", abs(bulk_cur["other"]), abs(bulk_prev["other"]), True),
        ("Реклама (маркетинг)", ad_spend_cur, ad_spend_prev, True),
        ("Расходы МП (ОПЕКС)", operating_cur, operating_prev, True),
        ("Удержания МП (всего)", mp_fees_cur, mp_fees_prev, True),
        ("К перечислению", payout_cur, payout_prev, True),
        ("Себестоимость (COGS)", cogs_cur, cogs_prev, True),
        ("Чистая прибыль", profit_cur, profit_prev, True),
    ]

    for i, (label, cur, prev, is_money) in enumerate(kpi_rows):
        row = 5 + i
        ws1.cell(row=row, column=1, value=label)
        c2 = ws1.cell(row=row, column=2, value=round(cur, 2) if is_money else cur)
        c3 = ws1.cell(row=row, column=3, value=round(prev, 2) if is_money else prev)
        delta = _safe_delta(cur, prev)
        c4 = ws1.cell(row=row, column=4, value=f"{'+' if delta > 0 else ''}{delta}%")
        if is_money:
            c2.number_format = MONEY_FMT
            c3.number_format = MONEY_FMT
        c2.alignment = Alignment(horizontal='right')
        c3.alignment = Alignment(horizontal='right')
        c4.alignment = Alignment(horizontal='right')
        if delta > 0:
            c4.font = GREEN_FONT
        elif delta < 0:
            c4.font = RED_FONT
        _style_data_row(ws1, row, 4, is_alt=(i % 2 == 1))

        # Highlight profit row
        if label == "Чистая прибыль":
            _style_total_row(ws1, row, 4)
            c2.font = PROFIT_GREEN if cur >= 0 else PROFIT_RED

    # Revenue % breakdown
    row = 5 + len(kpi_rows) + 2
    ws1.cell(row=row, column=1, value="Структура расходов (% от выручки)").font = SECTION_FONT
    ws1.cell(row=row, column=1).fill = SECTION_FILL
    ws1.merge_cells(start_row=row, start_column=1, end_row=row, end_column=4)

    pct_rows = [
        ("Комиссия", commission_cur),
        ("Сервисные услуги", services_cur),
        ("Логистика", abs(bulk_cur["logistics"])),
        ("Хранение", abs(bulk_cur["storage"])),
        ("Эквайринг", abs(bulk_cur["acquiring"])),
        ("Возвраты", abs(bulk_cur["refunds"])),
        ("Штрафы", abs(bulk_cur["penalties"])),
        ("Прочее", abs(bulk_cur["other"])),
        ("Реклама", ad_spend_cur),
        ("Себестоимость", cogs_cur),
        ("Прибыль", profit_cur),
    ]
    for i, (label, val) in enumerate(pct_rows):
        r = row + 1 + i
        ws1.cell(row=r, column=1, value=label)
        ws1.cell(row=r, column=2, value=round(val, 2)).number_format = MONEY_FMT
        pct = val / revenue_cur if revenue_cur > 0 else 0
        ws1.cell(row=r, column=3, value=round(pct * 100, 1))
        ws1.cell(row=r, column=3).number_format = '0.0"%"'
        ws1.cell(row=r, column=3).alignment = Alignment(horizontal='right')
        _style_data_row(ws1, r, 4, is_alt=(i % 2 == 1))

    _auto_width(ws1)
    ws1.freeze_panes = 'A5'

    # ── Sheet 2: Все транзакции ───────────────────────────────
    ws2 = wb.create_sheet("Транзакции")
    ws2.sheet_properties.tabColor = "16A34A"

    txn_headers = [
        "ID операции", "Дата", "Тип операции", "Описание",
        "Категория", "SKU", "Товар", "Сумма",
        "Начисления (продажа)", "Комиссия", "Сервисы",
        "Номер отправления", "Схема доставки"
    ]
    for col, h in enumerate(txn_headers, 1):
        ws2.cell(row=1, column=col, value=h)
    _style_header_row(ws2, 1, len(txn_headers))

    for i, r in enumerate(raw_txns.result_rows):
        row_num = i + 2
        ws2.cell(row=row_num, column=1, value=r[0])  # operation_id
        ws2.cell(row=row_num, column=2, value=str(r[1])).number_format = DATE_FMT  # dt
        ws2.cell(row=row_num, column=3, value=r[2])   # operation_type
        ws2.cell(row=row_num, column=4, value=r[3])   # operation_type_name
        ws2.cell(row=row_num, column=5, value=r[4])   # category
        ws2.cell(row=row_num, column=6, value=r[5] if r[5] else "")  # sku
        ws2.cell(row=row_num, column=7, value=r[6])   # item_name
        ws2.cell(row=row_num, column=8, value=float(r[7] or 0)).number_format = MONEY_FMT_2  # amount
        ws2.cell(row=row_num, column=9, value=float(r[8] or 0)).number_format = MONEY_FMT_2  # accruals
        ws2.cell(row=row_num, column=10, value=float(r[9] or 0)).number_format = MONEY_FMT_2  # commission
        ws2.cell(row=row_num, column=11, value=float(r[10] or 0)).number_format = MONEY_FMT_2  # services
        ws2.cell(row=row_num, column=12, value=r[11] or "")  # posting_number
        ws2.cell(row=row_num, column=13, value=r[12] or "")  # delivery_schema
        _style_data_row(ws2, row_num, len(txn_headers), is_alt=(i % 2 == 1))

    # Totals row
    total_row = len(raw_txns.result_rows) + 2
    ws2.cell(row=total_row, column=1, value="ИТОГО")
    ws2.cell(row=total_row, column=7, value=f"{len(raw_txns.result_rows)} операций")
    # Sum formulas
    for col in [8, 9, 10, 11]:
        ws2.cell(row=total_row, column=col,
                 value=f"=SUM({get_column_letter(col)}2:{get_column_letter(col)}{total_row - 1})")
        ws2.cell(row=total_row, column=col).number_format = MONEY_FMT_2
    _style_total_row(ws2, total_row, len(txn_headers))

    _auto_width(ws2)
    ws2.freeze_panes = 'A2'
    ws2.auto_filter.ref = f"A1:{get_column_letter(len(txn_headers))}{total_row}"

    # ── Sheet 3: По неделям (FULL RETROSPECTIVE) ──────────────
    ws3 = wb.create_sheet("По неделям")
    ws3.sheet_properties.tabColor = "EA580C"

    wk_headers = [
        "Год", "Нед.", "Начало", "Конец", "Кол-во",
        "Продажи", "Возвраты", "Комиссия", "Компенсации",
        "Сервисы", "Реклама", "Прочее",
        "FBO", "Эквайринг", "Доставка", "Хранение",
        "К перечисл.", "С/С", "Прибыль",
        "Комис%", "Рекл%", "FBO%", "Дост%", "С/С%", "Приб%"
    ]
    for col, h in enumerate(wk_headers, 1):
        ws3.cell(row=1, column=col, value=h)
    _style_header_row(ws3, 1, len(wk_headers))

    # Build COGS per week
    weekly_cogs = {}
    if sku_to_offer and cost_map:
        try:
            cogs_wk_ch = ch.query("""
                SELECT toMonday(toDate(operation_date)) AS ws, sku, countIf(category = 'Revenue') AS qty
                FROM mms_analytics.fact_ozon_transactions FINAL
                WHERE shop_id = {shop_id:UInt32} AND category = 'Revenue' AND sku > 0
                GROUP BY ws, sku
            """, parameters={"shop_id": shop_id})
            for r in cogs_wk_ch.result_rows:
                ws_key = str(r[0])
                sku = int(r[1] or 0)
                qty = int(r[2] or 0)
                offer_id = sku_to_offer.get(sku, "")
                unit_cost = cost_map.get(offer_id, cost_map.get(offer_id.lower() if isinstance(offer_id, str) else "", 0))
                if unit_cost > 0 and qty > 0:
                    weekly_cogs[ws_key] = weekly_cogs.get(ws_key, 0) + unit_cost * qty
        except Exception as e:
            logger.warning("Weekly COGS query failed: %s", e)

    wk_totals = {k: 0.0 for k in [
        "sales", "returns", "commission", "compensations", "other_services",
        "marketing", "other_charges", "fbo_services", "acquiring",
        "delivery_services", "storage", "payout", "cogs", "profit"
    ]}
    wk_totals["qty"] = 0

    # weekly_data columns: 0=week_start, 1=week_end, 2=qty, 3=sales, 4=returns,
    # 5=commission, 6=compensations, 7=other_services, 8=marketing, 9=other_charges,
    # 10=fbo_services, 11=acquiring, 12=delivery_services, 13=storage, 14=payout
    for i, r in enumerate(weekly_data.result_rows):
        row_num = i + 2
        ws_date = r[0]
        we_date = r[1]
        iso_cal = ws_date.isocalendar() if hasattr(ws_date, 'isocalendar') else date.fromisoformat(str(ws_date)).isocalendar()

        qty = int(r[2] or 0)
        sales = float(r[3] or 0)
        returns_ = float(r[4] or 0)
        commission_ = float(r[5] or 0)
        compensations_ = float(r[6] or 0)
        other_services_ = float(r[7] or 0)
        marketing_ = float(r[8] or 0)
        other_charges_ = float(r[9] or 0)
        fbo_ = float(r[10] or 0)
        acquiring_ = float(r[11] or 0)
        delivery_ = float(r[12] or 0)
        storage_ = float(r[13] or 0)
        payout_ = float(r[14] or 0)
        cogs_wk = weekly_cogs.get(str(ws_date), 0)
        profit_wk = payout_ - cogs_wk

        ws3.cell(row=row_num, column=1, value=iso_cal[0])
        ws3.cell(row=row_num, column=2, value=iso_cal[1])
        ws3.cell(row=row_num, column=3, value=_fmt_date_ru(ws_date))
        ws3.cell(row=row_num, column=4, value=_fmt_date_ru(we_date))
        ws3.cell(row=row_num, column=5, value=qty)

        money_vals = [sales, returns_, commission_, compensations_, other_services_,
                      marketing_, other_charges_, fbo_, acquiring_, delivery_, storage_,
                      payout_, cogs_wk, profit_wk]
        money_keys = ["sales", "returns", "commission", "compensations", "other_services",
                       "marketing", "other_charges", "fbo_services", "acquiring",
                       "delivery_services", "storage", "payout", "cogs", "profit"]

        for j, v in enumerate(money_vals):
            c = ws3.cell(row=row_num, column=6 + j, value=round(v, 2))
            c.number_format = MONEY_FMT
            wk_totals[money_keys[j]] += v

        wk_totals["qty"] += qty

        # Profit coloring
        ws3.cell(row=row_num, column=19).font = GREEN_FONT if profit_wk >= 0 else RED_FONT

        # Percentage columns (% of sales) — colored green/red
        if sales > 0:
            pct_vals = [
                (20, round(commission_ / sales * 100, 1)),
                (21, round(marketing_ / sales * 100, 1)),
                (22, round(fbo_ / sales * 100, 1)),
                (23, round(delivery_ / sales * 100, 1)),
                (24, round(cogs_wk / sales * 100, 1)),
                (25, round(profit_wk / sales * 100, 1)),
            ]
            for col_idx, pct_val in pct_vals:
                pc = ws3.cell(row=row_num, column=col_idx, value=pct_val)
                pc.number_format = '0.0"%"'
                # Expenses: lower is better (green); Profit: higher is better (green)
                if col_idx == 25:
                    pc.font = GREEN_FONT if pct_val >= 0 else RED_FONT
                else:
                    pc.font = RED_FONT if pct_val > 35 else (GREEN_FONT if pct_val < 20 else NORMAL_FONT)
        else:
            for cc in range(20, 26):
                ws3.cell(row=row_num, column=cc, value=0)

        _style_data_row(ws3, row_num, len(wk_headers), is_alt=(i % 2 == 1))

    # Totals
    total_row = len(weekly_data.result_rows) + 2
    ws3.cell(row=total_row, column=1, value="ИТОГО")
    ws3.cell(row=total_row, column=5, value=wk_totals["qty"])
    for j, k in enumerate(money_keys):
        ws3.cell(row=total_row, column=6 + j, value=round(wk_totals[k], 2)).number_format = MONEY_FMT

    ts = wk_totals["sales"] or 1
    ws3.cell(row=total_row, column=20, value=round(wk_totals["commission"] / ts * 100, 1)).number_format = '0.0"%"'
    ws3.cell(row=total_row, column=21, value=round(wk_totals["marketing"] / ts * 100, 1)).number_format = '0.0"%"'
    ws3.cell(row=total_row, column=22, value=round(wk_totals["fbo_services"] / ts * 100, 1)).number_format = '0.0"%"'
    ws3.cell(row=total_row, column=23, value=round(wk_totals["delivery_services"] / ts * 100, 1)).number_format = '0.0"%"'
    ws3.cell(row=total_row, column=24, value=round(wk_totals["cogs"] / ts * 100, 1)).number_format = '0.0"%"'
    ws3.cell(row=total_row, column=25, value=round(wk_totals["profit"] / ts * 100, 1)).number_format = '0.0"%"'
    _style_total_row(ws3, total_row, len(wk_headers))

    _auto_width(ws3)
    ws3.freeze_panes = 'A2'

    # ── Sheet 4: По месяцам (FULL RETROSPECTIVE) ──────────────
    ws4 = wb.create_sheet("По месяцам")
    ws4.sheet_properties.tabColor = "7C3AED"

    MONTHS_RU = {1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
                 7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"}

    mo_headers = [
        "Месяц", "Начало", "Конец", "Кол-во",
        "Продажи", "Возвраты", "Комиссия", "Компенсации",
        "Сервисы", "Реклама", "Прочее",
        "FBO", "Эквайринг", "Доставка", "Хранение",
        "К перечисл.", "С/С", "Прибыль",
        "Δ Выручка%", "Δ Прибыль%"
    ]
    for col, h in enumerate(mo_headers, 1):
        ws4.cell(row=1, column=col, value=h)
    _style_header_row(ws4, 1, len(mo_headers))

    # Build COGS per month
    monthly_cogs = {}
    if sku_to_offer and cost_map:
        try:
            cogs_mo_ch = ch.query("""
                SELECT toYYYYMM(toDate(operation_date)) AS ym, sku, countIf(category = 'Revenue') AS qty
                FROM mms_analytics.fact_ozon_transactions FINAL
                WHERE shop_id = {shop_id:UInt32} AND category = 'Revenue' AND sku > 0
                GROUP BY ym, sku
            """, parameters={"shop_id": shop_id})
            for r in cogs_mo_ch.result_rows:
                ym_key = int(r[0])
                sku = int(r[1] or 0)
                qty = int(r[2] or 0)
                offer_id = sku_to_offer.get(sku, "")
                unit_cost = cost_map.get(offer_id, cost_map.get(offer_id.lower() if isinstance(offer_id, str) else "", 0))
                if unit_cost > 0 and qty > 0:
                    monthly_cogs[ym_key] = monthly_cogs.get(ym_key, 0) + unit_cost * qty
        except Exception as e:
            logger.warning("Monthly COGS query failed: %s", e)

    prev_revenue_mo = None
    prev_profit_mo = None

    # monthly_data columns: 0=ym, 1=m_start, 2=m_end, 3=qty, 4=sales, 5=returns,
    # 6=commission, 7=compensations, 8=other_services, 9=marketing, 10=other_charges,
    # 11=fbo_services, 12=acquiring, 13=delivery_services, 14=storage, 15=payout
    for i, r in enumerate(monthly_data.result_rows):
        row_num = i + 2
        ym = int(r[0])
        year = ym // 100
        month = ym % 100
        month_name = f"{MONTHS_RU.get(month, str(month))} {year}"

        qty = int(r[3] or 0)
        sales_mo = float(r[4] or 0)
        returns_mo = float(r[5] or 0)
        commission_mo = float(r[6] or 0)
        compensations_mo = float(r[7] or 0)
        other_services_mo = float(r[8] or 0)
        marketing_mo = float(r[9] or 0)
        other_charges_mo = float(r[10] or 0)
        fbo_mo = float(r[11] or 0)
        acquiring_mo = float(r[12] or 0)
        delivery_mo = float(r[13] or 0)
        storage_mo = float(r[14] or 0)
        payout_mo = float(r[15] or 0)
        cogs_mo = monthly_cogs.get(ym, 0)
        profit_mo = payout_mo - cogs_mo

        ws4.cell(row=row_num, column=1, value=month_name)
        ws4.cell(row=row_num, column=2, value=str(r[1]))
        ws4.cell(row=row_num, column=3, value=str(r[2]))
        ws4.cell(row=row_num, column=4, value=qty)

        money_vals_mo = [sales_mo, returns_mo, commission_mo, compensations_mo, other_services_mo,
                         marketing_mo, other_charges_mo, fbo_mo, acquiring_mo, delivery_mo, storage_mo,
                         payout_mo, cogs_mo, profit_mo]

        for j, v in enumerate(money_vals_mo):
            c = ws4.cell(row=row_num, column=5 + j, value=round(v, 2))
            c.number_format = MONEY_FMT

        # Profit coloring
        ws4.cell(row=row_num, column=18).font = GREEN_FONT if profit_mo >= 0 else RED_FONT

        # Delta vs previous month
        if prev_revenue_mo is not None:
            d_rev = _safe_delta(sales_mo, prev_revenue_mo)
            d_prof = _safe_delta(profit_mo, prev_profit_mo)
            dc1 = ws4.cell(row=row_num, column=19, value=f"{'+' if d_rev > 0 else ''}{d_rev}%")
            dc2 = ws4.cell(row=row_num, column=20, value=f"{'+' if d_prof > 0 else ''}{d_prof}%")
            dc1.font = GREEN_FONT if d_rev > 0 else RED_FONT
            dc2.font = GREEN_FONT if d_prof > 0 else RED_FONT
        else:
            ws4.cell(row=row_num, column=19, value="—")
            ws4.cell(row=row_num, column=20, value="—")

        prev_revenue_mo = sales_mo
        prev_profit_mo = profit_mo

        _style_data_row(ws4, row_num, len(mo_headers), is_alt=(i % 2 == 1))

    _auto_width(ws4)
    ws4.freeze_panes = 'A2'

    # ── Sheet 5: По товарам (SKU) — расширенный P&L ─────────
    ws5 = wb.create_sheet("По товарам (SKU)")
    ws5.sheet_properties.tabColor = "DB2777"

    sku_headers = [
        "SKU", "Название", "Кол-во", "Выручка",
        "Комиссия", "Сервисы", "Логистика", "Реклама",
        "Нетто (к перечисл.)",
        "С/С", "Прибыль", "Маржа %"
    ]
    for col, h in enumerate(sku_headers, 1):
        ws5.cell(row=1, column=col, value=h)
    _style_header_row(ws5, 1, len(sku_headers))

    # Build sku→cost map for COGS in per-SKU
    sku_cost_map_all = {}
    try:
        if sku_to_offer:
            for sku, offer_id in sku_to_offer.items():
                cost = cost_map.get(offer_id, 0)
                if cost > 0:
                    sku_cost_map_all[sku] = cost
    except Exception:
        pass

    # Fetch per-SKU logistics from transactions
    sku_logistics_map = {}
    try:
        sku_logistics_ch = ch.query("""
            SELECT sku, abs(sum(amount)) AS logistics
            FROM mms_analytics.fact_ozon_transactions FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(operation_date) >= {d_start:Date}
              AND toDate(operation_date) <= {d_end:Date}
              AND category = 'Logistics' AND sku > 0
            GROUP BY sku
        """, parameters={"shop_id": shop_id, "d_start": d_start, "d_end": d_end})
        for r in sku_logistics_ch.result_rows:
            sku_logistics_map[int(r[0])] = float(r[1] or 0)
    except Exception:
        pass

    # Fetch per-SKU ad spend from fact_ozon_ad_daily
    sku_ad_map = {}
    try:
        sku_ad_ch = ch.query("""
            SELECT sku, sum(money_spent) AS ad_spend
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {d_start:Date} AND dt <= {d_end:Date}
              AND sku > 0
            GROUP BY sku
        """, parameters={"shop_id": shop_id, "d_start": d_start, "d_end": d_end})
        for r in sku_ad_ch.result_rows:
            sku_ad_map[int(r[0])] = float(r[1] or 0)
    except Exception:
        pass

    sku_totals = {"qty": 0, "revenue": 0, "commission": 0, "services": 0,
                  "logistics": 0, "ad_spend": 0, "payout": 0, "cogs": 0, "profit": 0}

    for i, r in enumerate(sku_data.result_rows):
        row_num = i + 2
        sku = int(r[0] or 0)
        name = r[1] or ""
        revenue_sku = float(r[2] or 0)
        qty = int(r[3] or 0)
        comm = float(r[4] or 0)
        svcs = float(r[5] or 0)
        payout_sku = float(r[6] or 0)
        logistics_sku = sku_logistics_map.get(sku, 0)
        ad_sku = sku_ad_map.get(sku, 0)
        cogs_sku = sku_cost_map_all.get(sku, 0) * qty
        profit_sku = payout_sku - cogs_sku - ad_sku

        ws5.cell(row=row_num, column=1, value=sku)
        ws5.cell(row=row_num, column=2, value=name)
        ws5.cell(row=row_num, column=3, value=qty)
        ws5.cell(row=row_num, column=4, value=round(revenue_sku, 2)).number_format = MONEY_FMT
        ws5.cell(row=row_num, column=5, value=round(comm, 2)).number_format = MONEY_FMT
        ws5.cell(row=row_num, column=6, value=round(svcs, 2)).number_format = MONEY_FMT
        ws5.cell(row=row_num, column=7, value=round(logistics_sku, 2)).number_format = MONEY_FMT
        ws5.cell(row=row_num, column=8, value=round(ad_sku, 2)).number_format = MONEY_FMT
        ws5.cell(row=row_num, column=9, value=round(payout_sku, 2)).number_format = MONEY_FMT
        ws5.cell(row=row_num, column=10, value=round(cogs_sku, 2)).number_format = MONEY_FMT
        pc = ws5.cell(row=row_num, column=11, value=round(profit_sku, 2))
        pc.number_format = MONEY_FMT
        pc.font = GREEN_FONT if profit_sku >= 0 else RED_FONT
        margin = profit_sku / revenue_sku * 100 if revenue_sku > 0 else 0
        mc = ws5.cell(row=row_num, column=12, value=round(margin, 1))
        mc.number_format = '0.0"%"'
        mc.font = GREEN_FONT if margin >= 15 else (RED_FONT if margin < 5 else NORMAL_FONT)

        sku_totals["qty"] += qty
        sku_totals["revenue"] += revenue_sku
        sku_totals["commission"] += comm
        sku_totals["services"] += svcs
        sku_totals["logistics"] += logistics_sku
        sku_totals["ad_spend"] += ad_sku
        sku_totals["payout"] += payout_sku
        sku_totals["cogs"] += cogs_sku
        sku_totals["profit"] += profit_sku

        _style_data_row(ws5, row_num, len(sku_headers), is_alt=(i % 2 == 1))

    # Totals
    total_row = len(sku_data.result_rows) + 2
    ws5.cell(row=total_row, column=1, value="ИТОГО")
    ws5.cell(row=total_row, column=2, value=f"{len(sku_data.result_rows)} товаров")
    ws5.cell(row=total_row, column=3, value=sku_totals["qty"])
    ws5.cell(row=total_row, column=4, value=round(sku_totals["revenue"], 2)).number_format = MONEY_FMT
    ws5.cell(row=total_row, column=5, value=round(sku_totals["commission"], 2)).number_format = MONEY_FMT
    ws5.cell(row=total_row, column=6, value=round(sku_totals["services"], 2)).number_format = MONEY_FMT
    ws5.cell(row=total_row, column=7, value=round(sku_totals["logistics"], 2)).number_format = MONEY_FMT
    ws5.cell(row=total_row, column=8, value=round(sku_totals["ad_spend"], 2)).number_format = MONEY_FMT
    ws5.cell(row=total_row, column=9, value=round(sku_totals["payout"], 2)).number_format = MONEY_FMT
    ws5.cell(row=total_row, column=10, value=round(sku_totals["cogs"], 2)).number_format = MONEY_FMT
    ws5.cell(row=total_row, column=11, value=round(sku_totals["profit"], 2)).number_format = MONEY_FMT
    tm = sku_totals["profit"] / sku_totals["revenue"] * 100 if sku_totals["revenue"] > 0 else 0
    ws5.cell(row=total_row, column=12, value=round(tm, 1)).number_format = '0.0"%"'
    _style_total_row(ws5, total_row, len(sku_headers))

    _auto_width(ws5)
    ws5.freeze_panes = 'A2'
    ws5.auto_filter.ref = f"A1:{get_column_letter(len(sku_headers))}{total_row}"

    # ── Sheet 6: Расходы детально ────────────────────────────
    ws6 = wb.create_sheet("Расходы детально")
    ws6.sheet_properties.tabColor = "D97706"

    exp_headers = [
        "Тип операции (API)", "Описание", "Категория",
        "Кол-во", "Общая сумма", "Средняя сумма", "% от выручки"
    ]
    for col, h in enumerate(exp_headers, 1):
        ws6.cell(row=1, column=col, value=h)
    _style_header_row(ws6, 1, len(exp_headers))

    for i, r in enumerate(expense_detail.result_rows):
        row_num = i + 2
        ws6.cell(row=row_num, column=1, value=r[0])  # operation_type
        ws6.cell(row=row_num, column=2, value=r[1])  # operation_type_name
        ws6.cell(row=row_num, column=3, value=r[2])  # category
        ws6.cell(row=row_num, column=4, value=int(r[3] or 0))
        ws6.cell(row=row_num, column=5, value=float(r[4] or 0)).number_format = MONEY_FMT_2
        ws6.cell(row=row_num, column=6, value=float(r[5] or 0)).number_format = MONEY_FMT_2
        pct = abs(float(r[4] or 0)) / revenue_cur * 100 if revenue_cur > 0 else 0
        ws6.cell(row=row_num, column=7, value=round(pct, 2)).number_format = '0.00"%"'

        _style_data_row(ws6, row_num, len(exp_headers), is_alt=(i % 2 == 1))

    _auto_width(ws6)
    ws6.freeze_panes = 'A2'
    ws6.auto_filter.ref = f"A1:{get_column_letter(len(exp_headers))}{len(expense_detail.result_rows) + 1}"

    # ══════════════════════════════════════════════════════════
    # Save to bytes buffer and return
    # ══════════════════════════════════════════════════════════

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    filename = f"Ozon_Finance_{shop.name}_{d_start}_{d_end}.xlsx"

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
