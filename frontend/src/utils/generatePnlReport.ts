/**
 * P&L PDF Report Generator
 *
 * Generates a professional PDF report with:
 * 1. Cover page with shop name and period
 * 2. KPI summary cards
 * 3. Waterfall (breakdown) chart screenshot
 * 4. Dynamics chart screenshot
 * 5. Period comparison table
 * 6. Weekly report table (last 12 weeks)
 */

import jsPDF from 'jspdf'
import autoTable from 'jspdf-autotable'
import html2canvas from 'html2canvas'
import type {
  FinancesResponse,
  WeeklyReportResponse,
  WBWeeklyReportRow,
  OzonWeeklyReportRow,
} from '@/api/finances'

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

// Colors
const C = {
  primary: [59, 130, 246] as [number, number, number],      // blue-500
  green: [16, 185, 129] as [number, number, number],         // emerald-500
  red: [239, 68, 68] as [number, number, number],            // red-500
  orange: [249, 115, 22] as [number, number, number],        // orange-500
  purple: [139, 92, 246] as [number, number, number],        // violet-500
  dark: [15, 23, 42] as [number, number, number],            // slate-900
  darkCard: [30, 41, 59] as [number, number, number],        // slate-800
  darkBorder: [51, 65, 85] as [number, number, number],      // slate-700
  text: [226, 232, 240] as [number, number, number],         // slate-200
  textMuted: [148, 163, 184] as [number, number, number],    // slate-400
  white: [255, 255, 255] as [number, number, number],
}

// ── Capture chart as image ────────────────────────────────────

/**
 * Capture a DOM element as a PNG data URL.
 * Temporarily inlines all computed color styles (resolved to RGB by the browser)
 * to avoid html2canvas failing on oklch()/hsl(var()) etc.
 */
async function captureElement(selector: string): Promise<string | null> {
  const el = document.querySelector(selector) as HTMLElement | null
  if (!el) return null

  // Collect original inline styles so we can restore them
  const originals: Array<{ node: HTMLElement | SVGElement; styles: Record<string, string>; attrs: Record<string, string | null> }> = []
  const COLOR_PROPS = ['color', 'backgroundColor', 'borderColor', 'borderTopColor', 'borderBottomColor', 'borderLeftColor', 'borderRightColor']

  function inlineResolvedColors(node: Element) {
    if (node instanceof HTMLElement) {
      const computed = getComputedStyle(node)
      const saved: Record<string, string> = {}
      for (const prop of COLOR_PROPS) {
        saved[prop] = node.style.getPropertyValue(prop === 'backgroundColor' ? 'background-color' : prop === 'borderColor' ? 'border-color' : prop)
        const resolved = computed.getPropertyValue(
          prop.replace(/([A-Z])/g, '-$1').toLowerCase()
        )
        if (resolved && resolved !== 'rgba(0, 0, 0, 0)' && resolved !== 'transparent') {
          ;(node.style as any)[prop] = resolved
        }
      }
      originals.push({ node, styles: saved, attrs: {} })
    }
    if (node instanceof SVGElement) {
      const computed = getComputedStyle(node)
      const savedAttrs: Record<string, string | null> = {
        fill: node.getAttribute('fill'),
        stroke: node.getAttribute('stroke'),
      }
      const fill = computed.fill
      const stroke = computed.stroke
      if (fill && fill !== 'none' && !fill.startsWith('url(')) node.setAttribute('fill', fill)
      if (stroke && stroke !== 'none' && !stroke.startsWith('url(')) node.setAttribute('stroke', stroke)
      originals.push({ node: node as any, styles: {}, attrs: savedAttrs })
    }
    for (const child of Array.from(node.children)) {
      inlineResolvedColors(child)
    }
  }

  try {
    inlineResolvedColors(el)

    const canvas = await html2canvas(el, {
      backgroundColor: '#0f172a',
      scale: 2,
      useCORS: true,
      logging: false,
    })

    return canvas.toDataURL('image/png')
  } catch (e) {
    console.warn('Failed to capture element:', selector, e)
    return null
  } finally {
    // Restore original inline styles
    for (const { node, styles, attrs } of originals) {
      if (node instanceof HTMLElement) {
        for (const [prop, val] of Object.entries(styles)) {
          const cssProp = prop.replace(/([A-Z])/g, '-$1').toLowerCase()
          if (val) {
            node.style.setProperty(cssProp, val)
          } else {
            node.style.removeProperty(cssProp)
          }
        }
      }
      for (const [attr, val] of Object.entries(attrs)) {
        if (val === null) {
          ;(node as SVGElement).removeAttribute(attr)
        } else {
          ;(node as SVGElement).setAttribute(attr, val)
        }
      }
    }
  }
}

// ── Page helpers ──────────────────────────────────────────────

function drawPageBg(doc: jsPDF) {
  const w = doc.internal.pageSize.getWidth()
  const h = doc.internal.pageSize.getHeight()
  doc.setFillColor(...C.dark)
  doc.rect(0, 0, w, h, 'F')
}

function drawFooter(doc: jsPDF, pageNum: number, totalPages: number) {
  const w = doc.internal.pageSize.getWidth()
  const h = doc.internal.pageSize.getHeight()
  doc.setFontSize(8)
  doc.setTextColor(...C.textMuted)
  doc.text(`MP-Control • Финансовый отчёт`, 20, h - 10)
  doc.text(`${pageNum} / ${totalPages}`, w - 20, h - 10, { align: 'right' })
}

function drawSection(doc: jsPDF, title: string, y: number): number {
  doc.setFontSize(16)
  doc.setTextColor(...C.white)
  doc.text(title, 20, y)
  // underline
  doc.setDrawColor(...C.primary)
  doc.setLineWidth(0.5)
  doc.line(20, y + 2, 100, y + 2)
  return y + 12
}

// ── Cover Page ────────────────────────────────────────────────

function drawCover(doc: jsPDF, shopName: string, dateFrom: string, dateTo: string, marketplace: string) {
  drawPageBg(doc)
  const w = doc.internal.pageSize.getWidth()
  const centerX = w / 2

  // Gradient accent bar at top
  doc.setFillColor(...C.primary)
  doc.rect(0, 0, w, 6, 'F')

  // Logo placeholder
  doc.setFontSize(24)
  doc.setTextColor(...C.primary)
  doc.text('MP-Control', centerX, 50, { align: 'center' })

  doc.setFontSize(10)
  doc.setTextColor(...C.textMuted)
  doc.text('ANALYTICS', centerX, 58, { align: 'center' })

  // Title
  doc.setFontSize(32)
  doc.setTextColor(...C.white)
  doc.text('Финансовый отчёт', centerX, 100, { align: 'center' })

  // Subtitle — P&L
  doc.setFontSize(14)
  doc.setTextColor(...C.textMuted)
  doc.text('Profit & Loss Statement', centerX, 112, { align: 'center' })

  // Divider
  doc.setDrawColor(...C.primary)
  doc.setLineWidth(0.8)
  doc.line(centerX - 40, 122, centerX + 40, 122)

  // Shop info
  doc.setFontSize(18)
  doc.setTextColor(...C.white)
  doc.text(shopName, centerX, 145, { align: 'center' })

  const mpLabel = marketplace === 'wildberries' ? 'Wildberries' : 'Ozon'
  doc.setFontSize(12)
  doc.setTextColor(...C.primary)
  doc.text(mpLabel, centerX, 157, { align: 'center' })

  // Period
  doc.setFontSize(14)
  doc.setTextColor(...C.text)
  doc.text(`${fmtDate(dateFrom)} — ${fmtDate(dateTo)}`, centerX, 180, { align: 'center' })

  // Generation date
  const now = new Date()
  doc.setFontSize(10)
  doc.setTextColor(...C.textMuted)
  doc.text(`Сформирован: ${now.toLocaleDateString('ru-RU')} ${now.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}`, centerX, 195, { align: 'center' })

  // Bottom accent bar
  doc.setFillColor(...C.primary)
  doc.rect(0, doc.internal.pageSize.getHeight() - 6, w, 6, 'F')
}

// ── KPI Page ──────────────────────────────────────────────────

function drawKpiPage(doc: jsPDF, data: FinancesResponse) {
  drawPageBg(doc)
  const kpi = data.kpi

  let y = drawSection(doc, 'Ключевые показатели', 30)

  const cards: Array<{
    title: string
    value: string
    subtitle: string
    delta: number
    color: [number, number, number]
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
    const cy = y + row * (cardH + gap)

    // Card background
    doc.setFillColor(...C.darkCard)
    doc.roundedRect(x, cy, cardW, cardH, 3, 3, 'F')

    // Color accent bar
    doc.setFillColor(...card.color)
    doc.rect(x, cy, 3, cardH, 'F')

    // Title
    doc.setFontSize(9)
    doc.setTextColor(...C.textMuted)
    doc.text(card.title, x + 8, cy + 10)

    // Value
    doc.setFontSize(16)
    doc.setTextColor(...C.white)
    doc.text(card.value, x + 8, cy + 24)

    // Subtitle + delta
    doc.setFontSize(8)
    if (card.subtitle) {
      doc.setTextColor(...C.textMuted)
      doc.text(card.subtitle, x + 8, cy + 34)
    }

    // Delta badge
    if (Math.abs(card.delta) > 0.1) {
      const isUp = card.delta > 0
      const deltaText = `${isUp ? '▲' : '▼'} ${Math.abs(card.delta).toFixed(1)}%`
      doc.setFontSize(8)
      doc.setTextColor(...(isUp ? C.green : C.red))
      doc.text(deltaText, x + cardW - 6, cy + 10, { align: 'right' })
    }
  })

  return y + 3 * (cardH + gap) + 10
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
    head: [['Показатель', 'Текущий', 'Предыдущий', 'Δ']],
    body: tableBody,
    theme: 'plain',
    styles: {
      fontSize: 8,
      textColor: C.text,
      cellPadding: { top: 3, bottom: 3, left: 4, right: 4 },
      lineWidth: 0,
    },
    headStyles: {
      fillColor: C.darkCard,
      textColor: C.textMuted,
      fontStyle: 'bold',
      fontSize: 8,
    },
    alternateRowStyles: {
      fillColor: [20, 30, 48],
    },
    bodyStyles: {
      fillColor: C.dark,
    },
    columnStyles: {
      0: { cellWidth: 60 },
      1: { halign: 'right', cellWidth: 35 },
      2: { halign: 'right', cellWidth: 35 },
      3: { halign: 'right', cellWidth: 25 },
    },
    didParseCell: (data) => {
      // Bold rows
      const boldKeys = [0, 3, 13]
      if (data.section === 'body' && boldKeys.includes(data.row.index)) {
        data.cell.styles.fontStyle = 'bold'
        data.cell.styles.fillColor = C.darkCard
      }
      // Profit color
      if (data.section === 'body' && data.row.index === 13 && data.column.index === 1) {
        const val = (comparison.current['profit'] ?? 0)
        data.cell.styles.textColor = val >= 0 ? C.green : C.red
      }
      // Delta color
      if (data.section === 'body' && data.column.index === 3) {
        const text = String(data.cell.raw)
        if (text.startsWith('+')) data.cell.styles.textColor = C.green
        else if (text.startsWith('-')) data.cell.styles.textColor = C.red
      }
    },
  })

  return (doc as any).lastAutoTable?.finalY ?? startY + 100
}

// ── Weekly Report Table ──────────────────────────────────────

function drawWeeklyTable(doc: jsPDF, weeklyData: WeeklyReportResponse, marketplace: string, startY: number): number {
  const weeks = weeklyData.weeks.slice(-12) // Last 12 weeks

  const isWb = marketplace === 'wildberries' || marketplace === 'wb'

  let head: string[]
  let body: string[][]

  if (isWb) {
    head = ['Нед', 'Период', 'Кол', 'Выр.', 'К пер.', 'Лог.', 'Хран.', 'Удерж.', 'Рекл.', 'С/С', 'Приб.']
    body = weeks.map(w => {
      const wk = w as WBWeeklyReportRow
      return [
        `${wk.week}`,
        `${fmtShortDate(wk.week_start)}`,
        `${wk.qty}`,
        fmtNum(wk.revenue),
        fmtNum(wk.payout),
        fmtNum(wk.logistics),
        fmtNum(wk.storage),
        fmtNum(wk.deductions + wk.wb_promo),
        fmtNum(wk.marketing),
        fmtNum(wk.cogs),
        fmtNum(wk.gross_profit),
      ]
    })
  } else {
    head = ['Нед', 'Период', 'Кол', 'Продажи', 'Комис.', 'Лог.', 'Хран.', 'Рекл.', 'С/С', 'Приб.']
    body = weeks.map(w => {
      const wk = w as OzonWeeklyReportRow
      return [
        `${wk.week}`,
        `${fmtShortDate(wk.week_start)}`,
        `${wk.qty}`,
        fmtNum(wk.sales),
        fmtNum(wk.commission),
        fmtNum(wk.delivery_services),
        fmtNum(wk.storage),
        fmtNum(wk.marketing),
        fmtNum(wk.cogs),
        fmtNum(wk.gross_profit),
      ]
    })
  }

  autoTable(doc, {
    startY,
    head: [head],
    body,
    theme: 'plain',
    styles: {
      fontSize: 7,
      textColor: C.text,
      cellPadding: { top: 2.5, bottom: 2.5, left: 2, right: 2 },
      lineWidth: 0,
      halign: 'right',
    },
    headStyles: {
      fillColor: C.darkCard,
      textColor: C.textMuted,
      fontStyle: 'bold',
      fontSize: 7,
      halign: 'center',
    },
    alternateRowStyles: {
      fillColor: [20, 30, 48],
    },
    bodyStyles: {
      fillColor: C.dark,
    },
    columnStyles: {
      0: { halign: 'center', cellWidth: 10 },
      1: { halign: 'left', cellWidth: 22 },
      2: { cellWidth: 10 },
    },
    didParseCell: (data) => {
      // Color profit column (last column)
      const lastCol = isWb ? 10 : 9
      if (data.section === 'body' && data.column.index === lastCol) {
        const numVal = parseFloat(String(data.cell.raw).replace(/\s/g, '').replace(',', '.'))
        if (!isNaN(numVal)) {
          data.cell.styles.textColor = numVal >= 0 ? C.green : C.red
          data.cell.styles.fontStyle = 'bold'
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
  shopName: string
  marketplace: string
  breakdownChartSelector?: string
  dynamicsChartSelector?: string
}

export async function generatePnlReport(opts: PnlReportOptions): Promise<void> {
  const { data, weeklyData, shopName, marketplace } = opts
  const breakdownSelector = opts.breakdownChartSelector || '[data-pdf="breakdown-chart"]'
  const dynamicsSelector = opts.dynamicsChartSelector || '[data-pdf="dynamics-chart"]'

  const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' })

  // ── Page 1: Cover ──
  drawCover(doc, shopName, data.date_from, data.date_to, marketplace)

  // ── Page 2: KPI + Breakdown Chart ──
  doc.addPage()
  drawPageBg(doc)
  const kpiEndY = drawKpiPage(doc, data)

  // Capture breakdown chart
  const breakdownImg = await captureElement(breakdownSelector)
  if (breakdownImg) {
    const imgY = kpiEndY + 5
    doc.setFontSize(11)
    doc.setTextColor(...C.textMuted)
    doc.text('Структура расходов', 20, imgY)
    const imgW = 170
    const imgH = 80
    doc.addImage(breakdownImg, 'PNG', 20, imgY + 4, imgW, imgH)
  }

  // ── Page 3: Dynamics Chart ──
  const dynamicsImg = await captureElement(dynamicsSelector)
  if (dynamicsImg) {
    doc.addPage()
    drawPageBg(doc)
    let y = drawSection(doc, 'Динамика показателей', 30)
    const imgW = 170
    const imgH = 90
    doc.addImage(dynamicsImg, 'PNG', 20, y, imgW, imgH)
  }

  // ── Page 4: Comparison Table ──
  doc.addPage()
  drawPageBg(doc)
  let y = drawSection(doc, 'Сравнение периодов', 30)
  y = drawComparisonTable(doc, data, y)

  // ── Page 5: Weekly Report ──
  if (weeklyData && weeklyData.weeks.length > 0) {
    // Check if enough space, otherwise new page
    if (y > 180) {
      doc.addPage()
      drawPageBg(doc)
      y = 30
    } else {
      y += 10
    }
    y = drawSection(doc, 'Понедельный отчёт (последние 12 недель)', y)
    drawWeeklyTable(doc, weeklyData, marketplace, y)
  }

  // ── Add page numbers ──
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
