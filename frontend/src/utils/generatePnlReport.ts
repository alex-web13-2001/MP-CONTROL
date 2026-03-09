/**
 * P&L PDF Report Generator — v3
 *
 * Light theme, Roboto Cyrillic, native charts (no html2canvas).
 *
 * Pages:
 * 1. Cover
 * 2. KPI cards + Waterfall chart (native)
 * 3. Dynamics chart (native line/bar)
 * 4. Comparison table
 * 5. Product SKU table
 * 6. Weekly report (detailed)
 */

import jsPDF from 'jspdf'
import autoTable from 'jspdf-autotable'
import type {
  FinancesResponse,
  WeeklyReportResponse,
  WBWeeklyReportRow,
  OzonWeeklyReportRow,
  ProductFinanceResponse,
} from '@/api/finances'
import { ROBOTO_REGULAR, ROBOTO_BOLD } from './robotoFont'

// ── Helpers ──────────────────────────────────────────────────

const MONTHS = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']

function fmtMoney(v: number): string {
  if (Math.abs(v) >= 1_000_000) return (v / 1_000_000).toFixed(1) + ' млн ₽'
  return v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function fmtNum(v: number): string {
  return v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })
}

function fmtDate(dateStr: string): string {
  const d = new Date(dateStr)
  return `${d.getDate()} ${MONTHS[d.getMonth()]} ${d.getFullYear()}`
}

function fmtShortDate(dateStr: string): string {
  const d = new Date(dateStr)
  const monthsShort = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
  return `${d.getDate()} ${monthsShort[d.getMonth()]}`
}

// ── Light theme colors ────────────────────────────────────────

type RGB = [number, number, number]

const C = {
  primary: [59, 130, 246] as RGB,
  green: [22, 163, 74] as RGB,
  red: [220, 38, 38] as RGB,
  orange: [234, 88, 12] as RGB,
  purple: [124, 58, 237] as RGB,
  teal: [20, 184, 166] as RGB,
  pink: [219, 39, 119] as RGB,
  amber: [217, 119, 6] as RGB,
  indigo: [79, 70, 229] as RGB,
  sky: [14, 165, 233] as RGB,
  // Backgrounds
  white: [255, 255, 255] as RGB,
  pageBg: [249, 250, 251] as RGB,
  cardBg: [255, 255, 255] as RGB,
  cardBorder: [229, 231, 235] as RGB,
  headerBg: [243, 244, 246] as RGB,
  altRow: [249, 250, 251] as RGB,
  chartGrid: [229, 231, 235] as RGB,
  // Text
  textDark: [17, 24, 39] as RGB,
  textBody: [55, 65, 81] as RGB,
  textMuted: [107, 114, 128] as RGB,
  textLight: [156, 163, 175] as RGB,
}

// ── Font setup ────────────────────────────────────────────────

function setupFonts(doc: jsPDF): void {
  doc.addFileToVFS('Roboto-normal.ttf', ROBOTO_REGULAR)
  doc.addFont('Roboto-normal.ttf', 'Roboto', 'normal')
  doc.addFileToVFS('Roboto-bold.ttf', ROBOTO_BOLD)
  doc.addFont('Roboto-bold.ttf', 'Roboto', 'bold')
  doc.setFont('Roboto', 'normal')
}

// ── Page helpers ──────────────────────────────────────────────

function drawPageBg(doc: jsPDF) {
  const w = doc.internal.pageSize.getWidth()
  const h = doc.internal.pageSize.getHeight()
  doc.setFillColor(...C.pageBg)
  doc.rect(0, 0, w, h, 'F')
}

function drawFooter(doc: jsPDF, pageNum: number, totalPages: number) {
  const w = doc.internal.pageSize.getWidth()
  const h = doc.internal.pageSize.getHeight()
  // Thin line
  doc.setDrawColor(...C.cardBorder)
  doc.setLineWidth(0.3)
  doc.line(20, h - 15, w - 20, h - 15)
  doc.setFont('Roboto', 'normal')
  doc.setFontSize(7)
  doc.setTextColor(...C.textLight)
  doc.text('MP-Control \u00B7 Финансовый отчёт', 20, h - 9)
  doc.text(`Стр. ${pageNum} из ${totalPages}`, w - 20, h - 9, { align: 'right' })
}

function drawSectionHeader(doc: jsPDF, title: string, y: number, subtitle?: string): number {
  doc.setFont('Roboto', 'bold')
  doc.setFontSize(15)
  doc.setTextColor(...C.textDark)
  doc.text(title, 20, y)
  doc.setDrawColor(...C.primary)
  doc.setLineWidth(0.8)
  doc.line(20, y + 2, 72, y + 2)
  if (subtitle) {
    doc.setFont('Roboto', 'normal')
    doc.setFontSize(8)
    doc.setTextColor(...C.textMuted)
    doc.text(subtitle, 20, y + 9)
    return y + 16
  }
  return y + 10
}

// ── Cover Page ────────────────────────────────────────────────

function drawCover(doc: jsPDF, shopName: string, dateFrom: string, dateTo: string, marketplace: string) {
  const w = doc.internal.pageSize.getWidth()
  const h = doc.internal.pageSize.getHeight()
  const cx = w / 2

  // White bg
  doc.setFillColor(...C.white)
  doc.rect(0, 0, w, h, 'F')

  // Top accent bar + gradient effect
  doc.setFillColor(...C.primary)
  doc.rect(0, 0, w, 8, 'F')

  // Logo
  doc.setFont('Roboto', 'bold')
  doc.setFontSize(28)
  doc.setTextColor(...C.primary)
  doc.text('MP-Control', cx, 55, { align: 'center' })

  doc.setFont('Roboto', 'normal')
  doc.setFontSize(11)
  doc.setTextColor(...C.textMuted)
  doc.text('АНАЛИТИКА', cx, 64, { align: 'center' })

  // Decorative line
  doc.setDrawColor(...C.cardBorder)
  doc.setLineWidth(0.4)
  doc.line(cx - 60, 75, cx + 60, 75)

  // Title
  doc.setFont('Roboto', 'bold')
  doc.setFontSize(34)
  doc.setTextColor(...C.textDark)
  doc.text('Финансовый отчёт', cx, 100, { align: 'center' })

  doc.setFont('Roboto', 'normal')
  doc.setFontSize(14)
  doc.setTextColor(...C.textMuted)
  doc.text('Profit & Loss', cx, 112, { align: 'center' })

  // Divider
  doc.setDrawColor(...C.primary)
  doc.setLineWidth(1)
  doc.line(cx - 30, 122, cx + 30, 122)

  // Shop name
  doc.setFont('Roboto', 'bold')
  doc.setFontSize(22)
  doc.setTextColor(...C.textDark)
  doc.text(shopName, cx, 145, { align: 'center' })

  // Marketplace badge
  const mpLabel = marketplace === 'wildberries' ? 'Wildberries' : 'Ozon'
  const mpColor: RGB = marketplace === 'wildberries' ? [150, 50, 200] : [0, 91, 227]
  doc.setFont('Roboto', 'bold')
  doc.setFontSize(12)
  doc.setTextColor(...mpColor)
  doc.text(mpLabel, cx, 158, { align: 'center' })

  // Period box
  doc.setFillColor(243, 244, 246)
  doc.roundedRect(cx - 55, 170, 110, 20, 4, 4, 'F')
  doc.setFont('Roboto', 'normal')
  doc.setFontSize(13)
  doc.setTextColor(...C.textBody)
  doc.text(`${fmtDate(dateFrom)} — ${fmtDate(dateTo)}`, cx, 183, { align: 'center' })

  // Generation date
  const now = new Date()
  doc.setFontSize(9)
  doc.setTextColor(...C.textLight)
  doc.text(`Сформирован: ${now.toLocaleDateString('ru-RU')} в ${now.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}`, cx, 200, { align: 'center' })

  // Bottom bar
  doc.setFillColor(...C.primary)
  doc.rect(0, h - 8, w, 8, 'F')
}

// ── KPI Cards ─────────────────────────────────────────────────

function drawKpiCards(doc: jsPDF, data: FinancesResponse, startY: number): number {
  const kpi = data.kpi

  const cards: Array<{
    title: string; value: string; subtitle: string
    delta: number; color: RGB
  }> = [
    { title: 'Выручка', value: fmtMoney(kpi.revenue), subtitle: `${fmtNum(kpi.orders)} заказов`, delta: kpi.revenue_delta, color: C.green },
    { title: 'К перечислению', value: fmtMoney(kpi.payout), subtitle: '', delta: kpi.payout_delta, color: C.primary },
    { title: 'Расходы МП', value: fmtMoney(kpi.operating), subtitle: kpi.payout > 0 ? `${(kpi.operating / kpi.payout * 100).toFixed(1)}% от перечисл.` : '', delta: kpi.operating_delta ?? kpi.mp_fees_delta, color: C.orange },
    { title: 'Реклама', value: fmtMoney(kpi.ad_spend), subtitle: kpi.revenue > 0 ? `ДРР ${(kpi.ad_spend / kpi.revenue * 100).toFixed(1)}%` : '', delta: kpi.ad_spend_delta, color: C.red },
    { title: 'Себестоимость', value: fmtMoney(kpi.cogs), subtitle: kpi.revenue > 0 ? `${(kpi.cogs / kpi.revenue * 100).toFixed(1)}% от выручки` : '', delta: kpi.cogs_delta, color: C.purple },
    { title: 'Чистая прибыль', value: fmtMoney(kpi.profit), subtitle: `${kpi.profit_pct.toFixed(1)}% от выручки`, delta: kpi.profit_delta, color: kpi.profit >= 0 ? C.green : C.red },
  ]

  const cardW = 82
  const cardH = 42
  const gap = 8
  const startX = 20

  cards.forEach((card, i) => {
    const col = i % 2
    const row = Math.floor(i / 2)
    const x = startX + col * (cardW + gap)
    const cy = startY + row * (cardH + gap)

    // Card bg + border
    doc.setFillColor(...C.cardBg)
    doc.setDrawColor(...C.cardBorder)
    doc.setLineWidth(0.3)
    doc.roundedRect(x, cy, cardW, cardH, 2, 2, 'FD')

    // Color accent left bar
    doc.setFillColor(...card.color)
    doc.rect(x, cy + 1, 2.5, cardH - 2, 'F')

    // Title
    doc.setFont('Roboto', 'normal')
    doc.setFontSize(8)
    doc.setTextColor(...C.textMuted)
    doc.text(card.title, x + 7, cy + 10)

    // Value
    doc.setFont('Roboto', 'bold')
    doc.setFontSize(16)
    doc.setTextColor(...C.textDark)
    doc.text(card.value, x + 7, cy + 24)

    // Subtitle
    if (card.subtitle) {
      doc.setFont('Roboto', 'normal')
      doc.setFontSize(7)
      doc.setTextColor(...C.textMuted)
      doc.text(card.subtitle, x + 7, cy + 34)
    }

    // Delta badge
    if (Math.abs(card.delta) > 0.1) {
      const isUp = card.delta > 0
      const deltaText = `${isUp ? '+' : ''}${card.delta.toFixed(1)}%`
      doc.setFont('Roboto', 'bold')
      doc.setFontSize(8)
      doc.setTextColor(...(isUp ? C.green : C.red))
      doc.text(deltaText, x + cardW - 5, cy + 10, { align: 'right' })
    }
  })

  return startY + 3 * (cardH + gap) + 4
}

// ── Native Waterfall Chart ────────────────────────────────────

function drawWaterfallChart(doc: jsPDF, data: FinancesResponse, startY: number): number {
  const bd = data.breakdown
  if (!bd) return startY

  const revenue = bd.revenue || 1
  const commissionNet = (bd.commission || 0) - (bd.acquiring || 0)
  const payoutVal = (bd.revenue || 0) - (bd.commission || 0)
  const bdAny = bd as any

  const bankTransfer = payoutVal
    - (bd.logistics || 0)
    - (bd.storage || 0)
    - (bdAny.deductions_ads || 0)
    - (bdAny.deductions_other || 0)
    - (bd.compensation || 0)

  // Build waterfall bars from breakdown object (matches BreakdownChart on UI)
  type WBar = { label: string; value: number; pct: number; color: RGB; isBold: boolean }
  const allBars: WBar[] = [
    { label: 'Выручка', value: bd.revenue, pct: 100, color: C.green, isBold: true },
    { label: 'Комиссия + скидки', value: commissionNet, pct: Math.abs(commissionNet) / revenue * 100, color: C.orange, isBold: false },
    { label: 'Эквайринг', value: bd.acquiring || 0, pct: Math.abs(bd.acquiring || 0) / revenue * 100, color: C.pink, isBold: false },
    { label: 'К перечислению', value: payoutVal, pct: payoutVal / revenue * 100, color: C.primary, isBold: true },
    { label: 'Логистика', value: bd.logistics || 0, pct: (bd.logistics || 0) / revenue * 100, color: C.red, isBold: false },
    { label: 'Хранение', value: bd.storage || 0, pct: (bd.storage || 0) / revenue * 100, color: C.amber, isBold: false },
    { label: 'ВБ Продвижение', value: bdAny.deductions_ads || 0, pct: (bdAny.deductions_ads || 0) / revenue * 100, color: C.purple, isBold: false },
    { label: 'Пр. удержания', value: bdAny.deductions_other || 0, pct: (bdAny.deductions_other || 0) / revenue * 100, color: C.teal, isBold: false },
    { label: 'Плат. приёмка', value: bd.compensation || 0, pct: (bd.compensation || 0) / revenue * 100, color: C.sky, isBold: false },
    { label: 'Итого к выплате', value: bankTransfer, pct: Math.abs(bankTransfer) / revenue * 100, color: C.indigo, isBold: true },
    { label: 'Себестоимость', value: bd.cogs || 0, pct: (bd.cogs || 0) / revenue * 100, color: C.textMuted, isBold: false },
    { label: 'Прибыль', value: bd.profit || 0, pct: Math.abs(bd.profit || 0) / revenue * 100, color: (bd.profit || 0) >= 0 ? C.green : C.red, isBold: true },
  ]

  // Filter out zero expense bars (keep bold rows always)
  const bars = allBars.filter(b => b.isBold || Math.abs(b.value) > 0)

  const chartX = 62
  const chartW = doc.internal.pageSize.getWidth() - chartX - 18
  const barH = 8
  const barGap = 3
  const maxVal = Math.max(...bars.map(b => Math.abs(b.value)), 1)

  let y = startY

  bars.forEach((item) => {
    const barW = Math.max((Math.abs(item.value) / maxVal) * chartW, 2)
    const color = item.color

    // Label
    doc.setFont('Roboto', item.isBold ? 'bold' : 'normal')
    doc.setFontSize(7)
    doc.setTextColor(...C.textBody)
    doc.text(item.label, chartX - 3, y + barH - 2, { align: 'right' })

    // Bar
    doc.setFillColor(...color)
    doc.roundedRect(chartX, y, barW, barH, 1, 1, 'F')

    // Value inside bar or next to it
    const valueText = fmtMoney(item.isBold ? item.value : Math.abs(item.value))
    doc.setFont('Roboto', 'bold')
    doc.setFontSize(6.5)
    if (barW > 30) {
      doc.setTextColor(255, 255, 255)
      doc.text(valueText, chartX + 3, y + barH - 2)
    } else {
      doc.setTextColor(...C.textBody)
      doc.text(valueText, chartX + barW + 2, y + barH - 2)
    }

    // Percentage on right
    if (item.pct > 0.05) {
      doc.setFont('Roboto', 'normal')
      doc.setFontSize(7)
      const isGreen = item.label === 'Выручка' || item.label === 'К перечислению' || item.label === 'Прибыль'
      doc.setTextColor(...(isGreen ? C.green : C.red))
      doc.text(`${item.pct.toFixed(1)}%`, doc.internal.pageSize.getWidth() - 18, y + barH - 2, { align: 'right' })
    }

    y += barH + barGap
  })

  return y + 4
}

// ── Native Dynamics Chart ─────────────────────────────────────

function drawDynamicsChart(doc: jsPDF, data: FinancesResponse, startY: number): number {
  const daily = data.daily
  if (!daily || daily.length === 0) return startY

  const chartX = 30
  const chartY = startY
  const chartW = doc.internal.pageSize.getWidth() - chartX - 20
  const chartH = 75
  const n = daily.length

  // Find ranges
  const revValues = daily.map(d => d.revenue || 0)
  const profitValues = daily.map(d => d.profit || 0)
  const allValues = [...revValues, ...profitValues]
  const maxVal = Math.max(...allValues, 1)
  const minVal = Math.min(...allValues, 0)
  const range = maxVal - minVal || 1

  // Background
  doc.setFillColor(...C.white)
  doc.setDrawColor(...C.cardBorder)
  doc.setLineWidth(0.3)
  doc.roundedRect(chartX - 8, chartY - 5, chartW + 16, chartH + 28, 3, 3, 'FD')

  // Grid lines
  doc.setDrawColor(...C.chartGrid)
  doc.setLineWidth(0.15)
  for (let i = 0; i <= 4; i++) {
    const gy = chartY + (i / 4) * chartH
    doc.line(chartX, gy, chartX + chartW, gy)
    // Grid label
    const gVal = maxVal - (i / 4) * range
    doc.setFont('Roboto', 'normal')
    doc.setFontSize(6)
    doc.setTextColor(...C.textLight)
    doc.text(fmtNum(Math.round(gVal)), chartX - 2, gy + 1.5, { align: 'right' })
  }

  // Zero line
  if (minVal < 0) {
    const zeroY = chartY + ((maxVal - 0) / range) * chartH
    doc.setDrawColor(...C.textLight)
    doc.setLineWidth(0.3)
    doc.line(chartX, zeroY, chartX + chartW, zeroY)
  }

  // Bar chart for profit
  const barW = Math.max((chartW / n) * 0.6, 2)
  daily.forEach((d, i) => {
    const x = chartX + (i / n) * chartW + (chartW / n) * 0.2
    const val = d.profit || 0
    const zeroY = chartY + ((maxVal - 0) / range) * chartH
    const valY = chartY + ((maxVal - val) / range) * chartH
    const h = Math.abs(valY - zeroY)

    doc.setFillColor(...(val >= 0 ? [200, 230, 201] as RGB : [255, 205, 210] as RGB))
    if (val >= 0) {
      doc.rect(x, valY, barW, h, 'F')
    } else {
      doc.rect(x, zeroY, barW, h, 'F')
    }
  })

  // Line chart for revenue
  doc.setDrawColor(...C.green)
  doc.setLineWidth(0.6)
  for (let i = 1; i < n; i++) {
    const x1 = chartX + ((i - 1) / n) * chartW + (chartW / n) * 0.5
    const x2 = chartX + (i / n) * chartW + (chartW / n) * 0.5
    const y1 = chartY + ((maxVal - (daily[i - 1].revenue || 0)) / range) * chartH
    const y2 = chartY + ((maxVal - (daily[i].revenue || 0)) / range) * chartH
    doc.line(x1, y1, x2, y2)
  }
  // Dots
  daily.forEach((d, i) => {
    const x = chartX + (i / n) * chartW + (chartW / n) * 0.5
    const y = chartY + ((maxVal - (d.revenue || 0)) / range) * chartH
    doc.setFillColor(...C.green)
    doc.circle(x, y, 0.8, 'F')
  })

  // X-axis labels
  const step = Math.max(1, Math.floor(n / 12))
  daily.forEach((d, i) => {
    if (i % step === 0 || i === n - 1) {
      const x = chartX + (i / n) * chartW + (chartW / n) * 0.5
      doc.setFont('Roboto', 'normal')
      doc.setFontSize(5.5)
      doc.setTextColor(...C.textLight)
      doc.text(fmtShortDate(d.date), x, chartY + chartH + 6, { align: 'center' })
    }
  })

  // Legend
  const legendY = chartY + chartH + 14
  doc.setFillColor(...C.green)
  doc.circle(chartX + 5, legendY - 1, 1.5, 'F')
  doc.setFont('Roboto', 'normal')
  doc.setFontSize(7)
  doc.setTextColor(...C.textBody)
  doc.text('Выручка', chartX + 9, legendY)

  doc.setFillColor(200, 230, 201)
  doc.rect(chartX + 35, legendY - 3, 6, 4, 'F')
  doc.text('Прибыль (>0)', chartX + 43, legendY)

  doc.setFillColor(255, 205, 210)
  doc.rect(chartX + 73, legendY - 3, 6, 4, 'F')
  doc.text('Убыток (<0)', chartX + 81, legendY)

  return legendY + 8
}

// ── Comparison Table ──────────────────────────────────────────

function drawComparisonTable(doc: jsPDF, data: FinancesResponse, startY: number): number {
  const comparison = data.comparison
  const rows = [
    { key: 'revenue', label: 'Выручка', bold: true },
    { key: 'orders', label: 'Заказы', isMoney: false },
    { key: 'payout', label: 'К перечислению' },
    { key: 'mp_fees', label: 'Удержания МП', bold: true },
    { key: 'commission', label: '  └ Комиссия + скидки' },
    { key: 'logistics', label: '     • Логистика' },
    { key: 'storage', label: '     • Хранение' },
    { key: 'acquiring', label: '     • Эквайринг' },
    { key: 'deductions_ads', label: '     • ВБ Продвижение' },
    { key: 'deductions_other', label: '     • Пр. удержания' },
    { key: 'acceptance', label: '     • Плат. приёмка' },
    { key: 'advertising', label: 'Реклама' },
    { key: 'cogs', label: 'Себестоимость' },
    { key: 'profit', label: 'Чистая прибыль', bold: true },
  ]

  const tableBody = rows.map(r => {
    const cur = comparison.current[r.key] ?? 0
    const prev = comparison.previous[r.key] ?? 0
    const delta = comparison.delta_pct[r.key] ?? 0
    const isMoney = r.isMoney !== false
    return [
      r.label,
      isMoney ? fmtMoney(cur) : fmtNum(cur),
      isMoney ? fmtMoney(prev) : fmtNum(prev),
      Math.abs(delta) > 0.1 ? `${delta > 0 ? '+' : ''}${delta.toFixed(1)}%` : '—',
    ]
  })

  autoTable(doc, {
    startY,
    head: [['Показатель', 'Текущий период', 'Предыдущий период', 'Изменение']],
    body: tableBody,
    theme: 'grid',
    styles: {
      font: 'Roboto',
      fontSize: 8.5,
      textColor: C.textBody,
      cellPadding: { top: 3.5, bottom: 3.5, left: 5, right: 5 },
      lineColor: C.cardBorder,
      lineWidth: 0.2,
    },
    headStyles: {
      fillColor: C.primary,
      textColor: C.white,
      fontStyle: 'bold',
      fontSize: 8.5,
    },
    alternateRowStyles: {
      fillColor: C.altRow,
    },
    bodyStyles: {
      fillColor: C.white,
    },
    columnStyles: {
      0: { cellWidth: 55 },
      1: { halign: 'right', cellWidth: 38 },
      2: { halign: 'right', cellWidth: 38 },
      3: { halign: 'right', cellWidth: 28 },
    },
    didParseCell: (cellData) => {
      const boldRows = [0, 3, 13]
      if (cellData.section === 'body' && boldRows.includes(cellData.row.index)) {
        cellData.cell.styles.fontStyle = 'bold'
        cellData.cell.styles.fillColor = [237, 242, 252]
        cellData.cell.styles.textColor = C.textDark
      }
      if (cellData.section === 'body' && cellData.row.index === 13 && cellData.column.index === 1) {
        const val = comparison.current['profit'] ?? 0
        cellData.cell.styles.textColor = val >= 0 ? C.green : C.red
      }
      if (cellData.section === 'body' && cellData.column.index === 3) {
        const text = String(cellData.cell.raw)
        if (text.startsWith('+')) cellData.cell.styles.textColor = C.green
        else if (text.startsWith('-')) cellData.cell.styles.textColor = C.red
      }
    },
  })

  return (doc as any).lastAutoTable?.finalY ?? startY + 100
}

// ── Product SKU Table ─────────────────────────────────────────

function drawProductTable(doc: jsPDF, productData: ProductFinanceResponse, marketplace: string, startY: number): number {
  const products = productData.products
    .filter(p => !p.vendor_code.startsWith('__'))
    .sort((a, b) => (b.current.revenue || 0) - (a.current.revenue || 0))
    .slice(0, 30) // Top 30

  const isWb = marketplace === 'wildberries' || marketplace === 'wb'

  let head: string[]
  let colKeys: string[]

  if (isWb) {
    head = ['Товар', 'Прод.', 'Выручка', 'Логист.', 'Хран.', 'Удерж.', 'Рекл.', 'С/С', 'Прибыль', 'Маржа']
    colKeys = ['sales', 'revenue', 'logistics', 'storage', 'deductions', 'ad_spend', 'cogs', 'profit']
  } else {
    head = ['Товар', 'Прод.', 'Выручка', 'Комис.', 'Логист.', 'Рекл.', 'С/С', 'Прибыль', 'Маржа']
    colKeys = ['sales', 'revenue', 'commission', 'logistics', 'ad_spend', 'cogs', 'profit']
  }

  const body = products.map(p => {
    const name = (p.name || p.vendor_code).substring(0, 28)
    const rev = p.current.revenue || 0
    const profit = p.current.profit || 0
    const margin = rev > 0 ? `${(profit / rev * 100).toFixed(1)}%` : '—'

    const row = [name]
    colKeys.forEach(k => {
      const val = p.current[k] ?? 0
      row.push(k === 'sales' ? `${val}` : fmtNum(val))
    })
    row.push(margin)
    return row
  })

  // Totals row
  const totals = productData.totals
  const totRev = totals.current.revenue || 1
  const totProfit = totals.current.profit || 0
  const totMargin = `${(totProfit / totRev * 100).toFixed(1)}%`
  const totRow = ['ИТОГО']
  colKeys.forEach(k => {
    const val = totals.current[k] ?? 0
    totRow.push(k === 'sales' ? `${val}` : fmtNum(val))
  })
  totRow.push(totMargin)
  body.push(totRow)

  autoTable(doc, {
    startY,
    head: [head],
    body,
    theme: 'grid',
    styles: {
      font: 'Roboto',
      fontSize: 7,
      textColor: C.textBody,
      cellPadding: { top: 2.5, bottom: 2.5, left: 2.5, right: 2.5 },
      lineColor: C.cardBorder,
      lineWidth: 0.15,
      halign: 'right',
    },
    headStyles: {
      fillColor: C.primary,
      textColor: C.white,
      fontStyle: 'bold',
      fontSize: 7,
      halign: 'center',
    },
    alternateRowStyles: {
      fillColor: C.altRow,
    },
    bodyStyles: {
      fillColor: C.white,
    },
    columnStyles: {
      0: { halign: 'left', cellWidth: 40 },
      1: { cellWidth: 10 },
    },
    didParseCell: (cellData) => {
      // Last row = ИТОГО
      if (cellData.section === 'body' && cellData.row.index === body.length - 1) {
        cellData.cell.styles.fontStyle = 'bold'
        cellData.cell.styles.fillColor = [237, 242, 252]
        cellData.cell.styles.textColor = C.textDark
      }
      // Profit column color
      const profitCol = isWb ? 8 : 7
      if (cellData.section === 'body' && cellData.column.index === profitCol) {
        const numVal = parseFloat(String(cellData.cell.raw).replace(/\s/g, '').replace(',', '.'))
        if (!isNaN(numVal)) {
          cellData.cell.styles.textColor = numVal >= 0 ? C.green : C.red
          cellData.cell.styles.fontStyle = 'bold'
        }
      }
      // Margin column color
      const marginCol = isWb ? 9 : 8
      if (cellData.section === 'body' && cellData.column.index === marginCol) {
        const text = String(cellData.cell.raw)
        const pct = parseFloat(text)
        if (!isNaN(pct)) {
          if (pct >= 20) cellData.cell.styles.textColor = C.green
          else if (pct >= 5) cellData.cell.styles.textColor = C.amber
          else cellData.cell.styles.textColor = C.red
          cellData.cell.styles.fontStyle = 'bold'
        }
      }
    },
  })

  return (doc as any).lastAutoTable?.finalY ?? startY + 100
}

// ── Weekly Report Table (Detailed) ────────────────────────────

function drawWeeklyTable(doc: jsPDF, weeklyData: WeeklyReportResponse, marketplace: string, startY: number): number {
  const weeks = weeklyData.weeks.slice(-12)
  const isWb = marketplace === 'wildberries' || marketplace === 'wb'

  let head: string[]
  let body: string[][]

  if (isWb) {
    head = ['Нед', 'Период', 'Кол-во', 'Выручка', 'К перечисл.', 'Комиссия', 'Логистика', 'Хранение', 'Удержания', 'ВБ Продв.', 'Приёмка', 'Реклама', 'С/С', 'Прибыль', 'Маржа']
    body = weeks.map(w => {
      const wk = w as WBWeeklyReportRow
      const rev = wk.revenue || 1
      const margin = `${(wk.gross_profit / rev * 100).toFixed(1)}%`
      return [
        `${wk.week}`,
        `${fmtShortDate(wk.week_start)} – ${fmtShortDate(wk.week_end)}`,
        `${wk.qty}`,
        fmtNum(wk.revenue),
        fmtNum(wk.payout),
        fmtNum(wk.commission),
        fmtNum(wk.logistics),
        fmtNum(wk.storage),
        fmtNum(wk.deductions),
        fmtNum(wk.wb_promo),
        fmtNum(wk.acceptance),
        fmtNum(wk.marketing),
        fmtNum(wk.cogs),
        fmtNum(wk.gross_profit),
        margin,
      ]
    })
  } else {
    head = ['Нед', 'Период', 'Кол-во', 'Продажи', 'К перечисл.', 'Комиссия', 'Логистика', 'Хранение', 'Доставка', 'FBO', 'Реклама', 'С/С', 'Прибыль', 'Маржа']
    body = weeks.map(w => {
      const wk = w as OzonWeeklyReportRow
      const rev = wk.sales || 1
      const margin = `${(wk.gross_profit / rev * 100).toFixed(1)}%`
      return [
        `${wk.week}`,
        `${fmtShortDate(wk.week_start)} – ${fmtShortDate(wk.week_end)}`,
        `${wk.qty}`,
        fmtNum(wk.sales),
        fmtNum(wk.payout),
        fmtNum(wk.commission),
        fmtNum(wk.delivery_services),
        fmtNum(wk.storage),
        fmtNum(wk.delivery_services),
        fmtNum(wk.fbo_services),
        fmtNum(wk.marketing),
        fmtNum(wk.cogs),
        fmtNum(wk.gross_profit),
        margin,
      ]
    })
  }

  // Totals row
  if (weeklyData.totals) {
    const t = weeklyData.totals
    if (isWb) {
      const rev = t.revenue || 1
      body.push([
        '', 'ИТОГО', `${t.qty || 0}`,
        fmtNum(t.revenue || 0), fmtNum(t.payout || 0), fmtNum(t.commission || 0),
        fmtNum(t.logistics || 0), fmtNum(t.storage || 0), fmtNum(t.deductions || 0),
        fmtNum(t.wb_promo || 0), fmtNum(t.acceptance || 0), fmtNum(t.marketing || 0),
        fmtNum(t.cogs || 0), fmtNum(t.gross_profit || 0),
        `${((t.gross_profit || 0) / rev * 100).toFixed(1)}%`,
      ])
    } else {
      const rev = t.sales || 1
      body.push([
        '', 'ИТОГО', `${t.qty || 0}`,
        fmtNum(t.sales || 0), fmtNum(t.payout || 0), fmtNum(t.commission || 0),
        fmtNum(t.delivery_services || 0), fmtNum(t.storage || 0), fmtNum(t.delivery_services || 0),
        fmtNum(t.fbo_services || 0), fmtNum(t.marketing || 0),
        fmtNum(t.cogs || 0), fmtNum(t.gross_profit || 0),
        `${((t.gross_profit || 0) / rev * 100).toFixed(1)}%`,
      ])
    }
  }

  autoTable(doc, {
    startY,
    head: [head],
    body,
    theme: 'grid',
    styles: {
      font: 'Roboto',
      fontSize: 6.5,
      textColor: C.textBody,
      cellPadding: { top: 2, bottom: 2, left: 1.5, right: 1.5 },
      lineColor: C.cardBorder,
      lineWidth: 0.15,
      halign: 'right',
      overflow: 'ellipsize',
    },
    headStyles: {
      fillColor: C.primary,
      textColor: C.white,
      fontStyle: 'bold',
      fontSize: 6,
      halign: 'center',
    },
    alternateRowStyles: {
      fillColor: C.altRow,
    },
    bodyStyles: {
      fillColor: C.white,
    },
    columnStyles: {
      0: { halign: 'center', cellWidth: 8 },
      1: { halign: 'left', cellWidth: 24 },
      2: { cellWidth: 9 },
    },
    tableWidth: 'auto',
    didParseCell: (cellData) => {
      // ИТОГО row
      if (cellData.section === 'body' && cellData.row.index === body.length - 1) {
        cellData.cell.styles.fontStyle = 'bold'
        cellData.cell.styles.fillColor = [237, 242, 252]
        cellData.cell.styles.textColor = C.textDark
      }
      // Profit column color
      const profitCol = isWb ? 13 : 12
      if (cellData.section === 'body' && cellData.column.index === profitCol) {
        const numVal = parseFloat(String(cellData.cell.raw).replace(/\s/g, '').replace(',', '.'))
        if (!isNaN(numVal)) {
          cellData.cell.styles.textColor = numVal >= 0 ? C.green : C.red
          cellData.cell.styles.fontStyle = 'bold'
        }
      }
      // Margin column
      const marginCol = isWb ? 14 : 13
      if (cellData.section === 'body' && cellData.column.index === marginCol) {
        const pct = parseFloat(String(cellData.cell.raw))
        if (!isNaN(pct)) {
          if (pct >= 15) cellData.cell.styles.textColor = C.green
          else if (pct >= 5) cellData.cell.styles.textColor = C.amber
          else cellData.cell.styles.textColor = C.red
          cellData.cell.styles.fontStyle = 'bold'
        }
      }
    },
  })

  return (doc as any).lastAutoTable?.finalY ?? startY + 100
}

// ── Main Export ────────────────────────────────────────────────

export interface PnlReportOptions {
  data: FinancesResponse
  weeklyData: WeeklyReportResponse | null
  productData: ProductFinanceResponse | null
  shopName: string
  marketplace: string
}

export async function generatePnlReport(opts: PnlReportOptions): Promise<void> {
  const { data, weeklyData, productData, shopName, marketplace } = opts

  const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' })
  setupFonts(doc)

  // ── Page 1: Cover ──
  drawCover(doc, shopName, data.date_from, data.date_to, marketplace)

  // ── Page 2: KPI ──
  doc.addPage()
  drawPageBg(doc)
  let y = drawSectionHeader(doc, 'Ключевые показатели', 25, `${fmtDate(data.date_from)} — ${fmtDate(data.date_to)}`)
  drawKpiCards(doc, data, y)

  // ── Page 3: Waterfall ──
  doc.addPage()
  drawPageBg(doc)
  y = drawSectionHeader(doc, 'Структура расходов', 25, 'Куда уходит каждый рубль выручки')
  drawWaterfallChart(doc, data, y)

  // ── Page 3: Dynamics ──
  doc.addPage()
  drawPageBg(doc)
  y = drawSectionHeader(doc, 'Динамика показателей', 25, 'Выручка (линия) и прибыль (столбцы) по дням')
  drawDynamicsChart(doc, data, y)

  // ── Page 4: Comparison Table ──
  doc.addPage()
  drawPageBg(doc)
  y = drawSectionHeader(doc, 'Сравнение периодов', 25, 'Текущий vs предыдущий период')
  drawComparisonTable(doc, data, y)

  // ── Page 5: Product SKU ──
  if (productData && productData.products.length > 0) {
    doc.addPage()
    drawPageBg(doc)
    y = drawSectionHeader(doc, 'Детализация по товарам', 25, `Топ-30 товаров по выручке`)
    drawProductTable(doc, productData, marketplace, y)
  }

  // ── Weekly Report (landscape) ──
  if (weeklyData && weeklyData.weeks.length > 0) {
    doc.addPage('a4', 'l')  // landscape
    drawPageBg(doc)
    y = drawSectionHeader(doc, 'Понедельный отчёт', 20, 'Последние 12 недель — детализация по всем показателям')
    drawWeeklyTable(doc, weeklyData, marketplace, y)
  }

  // ── Page numbers ──
  const totalPages = doc.getNumberOfPages()
  for (let i = 1; i <= totalPages; i++) {
    doc.setPage(i)
    drawFooter(doc, i, totalPages)
  }

  // ── Save ──
  const mpName = marketplace === 'wildberries' ? 'WB' : 'Ozon'
  const fileName = `PnL_${mpName}_${data.date_from}_${data.date_to}.pdf`
  doc.save(fileName)
}
