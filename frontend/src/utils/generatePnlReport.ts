/**
 * P&L PDF Report Generator — Light Theme + Cyrillic (Roboto)
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

// ── Light theme colors ────────────────────────────────────────

const C = {
  primary: [59, 130, 246] as [number, number, number],
  green: [22, 163, 74] as [number, number, number],
  red: [220, 38, 38] as [number, number, number],
  orange: [234, 88, 12] as [number, number, number],
  purple: [124, 58, 237] as [number, number, number],
  // Light theme backgrounds
  white: [255, 255, 255] as [number, number, number],
  pageBg: [249, 250, 251] as [number, number, number],       // gray-50
  cardBg: [255, 255, 255] as [number, number, number],
  cardBorder: [229, 231, 235] as [number, number, number],   // gray-200
  headerBg: [243, 244, 246] as [number, number, number],     // gray-100
  altRow: [249, 250, 251] as [number, number, number],       // gray-50
  // Light theme text
  textDark: [17, 24, 39] as [number, number, number],        // gray-900
  textBody: [55, 65, 81] as [number, number, number],        // gray-700
  textMuted: [107, 114, 128] as [number, number, number],    // gray-500
  textLight: [156, 163, 175] as [number, number, number],    // gray-400
}

// ── Font loader ────────────────────────────────────────────────

import { ROBOTO_REGULAR, ROBOTO_BOLD } from './robotoFont'

function setupFonts(doc: jsPDF): void {
  doc.addFileToVFS('Roboto-normal.ttf', ROBOTO_REGULAR)
  doc.addFont('Roboto-normal.ttf', 'Roboto', 'normal')
  doc.addFileToVFS('Roboto-bold.ttf', ROBOTO_BOLD)
  doc.addFont('Roboto-bold.ttf', 'Roboto', 'bold')
  doc.setFont('Roboto', 'normal')
}

// ── Capture chart as image ────────────────────────────────────

async function captureElement(selector: string): Promise<string | null> {
  const el = document.querySelector(selector) as HTMLElement | null
  if (!el) return null

  const originals: Array<{ node: HTMLElement | SVGElement; styles: Record<string, string>; attrs: Record<string, string | null> }> = []
  const COLOR_PROPS = ['color', 'backgroundColor', 'borderColor', 'borderTopColor', 'borderBottomColor', 'borderLeftColor', 'borderRightColor']

  function inlineResolvedColors(node: Element) {
    if (node instanceof HTMLElement) {
      const computed = getComputedStyle(node)
      const saved: Record<string, string> = {}
      for (const prop of COLOR_PROPS) {
        const cssProp = prop.replace(/([A-Z])/g, '-$1').toLowerCase()
        saved[prop] = node.style.getPropertyValue(cssProp)
        const resolved = computed.getPropertyValue(cssProp)
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
      backgroundColor: '#ffffff',
      scale: 2,
      useCORS: true,
      logging: false,
    })
    return canvas.toDataURL('image/png')
  } catch (e) {
    console.warn('Failed to capture element:', selector, e)
    return null
  } finally {
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
  doc.setFillColor(...C.pageBg)
  doc.rect(0, 0, w, h, 'F')
}

function drawFooter(doc: jsPDF, pageNum: number, totalPages: number) {
  const w = doc.internal.pageSize.getWidth()
  const h = doc.internal.pageSize.getHeight()
  doc.setFont('Roboto', 'normal')
  doc.setFontSize(8)
  doc.setTextColor(...C.textLight)
  doc.text('MP-Control \u2022 \u0424\u0438\u043d\u0430\u043d\u0441\u043e\u0432\u044b\u0439 \u043e\u0442\u0447\u0451\u0442', 20, h - 10)
  doc.text(`${pageNum} / ${totalPages}`, w - 20, h - 10, { align: 'right' })
}

function drawSectionHeader(doc: jsPDF, title: string, y: number): number {
  doc.setFont('Roboto', 'bold')
  doc.setFontSize(16)
  doc.setTextColor(...C.textDark)
  doc.text(title, 20, y)
  doc.setDrawColor(...C.primary)
  doc.setLineWidth(0.7)
  doc.line(20, y + 2, 80, y + 2)
  return y + 12
}

// ── Cover Page ────────────────────────────────────────────────

function drawCover(doc: jsPDF, shopName: string, dateFrom: string, dateTo: string, marketplace: string) {
  const w = doc.internal.pageSize.getWidth()
  const h = doc.internal.pageSize.getHeight()
  const cx = w / 2

  // White background
  doc.setFillColor(...C.white)
  doc.rect(0, 0, w, h, 'F')

  // Top accent bar
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
  doc.text('\u0410\u041d\u0410\u041b\u0418\u0422\u0418\u041a\u0410', cx, 64, { align: 'center' })

  // Main title
  doc.setFont('Roboto', 'bold')
  doc.setFontSize(36)
  doc.setTextColor(...C.textDark)
  doc.text('\u0424\u0438\u043d\u0430\u043d\u0441\u043e\u0432\u044b\u0439 \u043e\u0442\u0447\u0451\u0442', cx, 105, { align: 'center' })

  doc.setFont('Roboto', 'normal')
  doc.setFontSize(14)
  doc.setTextColor(...C.textMuted)
  doc.text('Profit & Loss', cx, 116, { align: 'center' })

  // Divider
  doc.setDrawColor(...C.primary)
  doc.setLineWidth(1)
  doc.line(cx - 35, 126, cx + 35, 126)

  // Shop name
  doc.setFont('Roboto', 'bold')
  doc.setFontSize(22)
  doc.setTextColor(...C.textDark)
  doc.text(shopName, cx, 150, { align: 'center' })

  // Marketplace badge
  const mpLabel = marketplace === 'wildberries' ? 'Wildberries' : 'Ozon'
  const mpColor: [number, number, number] = marketplace === 'wildberries' ? [150, 50, 200] : [0, 91, 227]
  doc.setFont('Roboto', 'bold')
  doc.setFontSize(12)
  doc.setTextColor(...mpColor)
  doc.text(mpLabel, cx, 163, { align: 'center' })

  // Period
  doc.setFont('Roboto', 'normal')
  doc.setFontSize(16)
  doc.setTextColor(...C.textBody)
  doc.text(`${fmtDate(dateFrom)} \u2014 ${fmtDate(dateTo)}`, cx, 190, { align: 'center' })

  // Generation date
  const now = new Date()
  doc.setFontSize(10)
  doc.setTextColor(...C.textLight)
  doc.text(`\u0421\u0444\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u043d: ${now.toLocaleDateString('ru-RU')} ${now.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}`, cx, 205, { align: 'center' })

  // Bottom accent bar
  doc.setFillColor(...C.primary)
  doc.rect(0, h - 8, w, 8, 'F')
}

// ── KPI Page ──────────────────────────────────────────────────

function drawKpiPage(doc: jsPDF, data: FinancesResponse): number {
  drawPageBg(doc)
  const kpi = data.kpi

  let y = drawSectionHeader(doc, '\u041a\u043b\u044e\u0447\u0435\u0432\u044b\u0435 \u043f\u043e\u043a\u0430\u0437\u0430\u0442\u0435\u043b\u0438', 30)

  const cards: Array<{
    title: string; value: string; subtitle: string
    delta: number; color: [number, number, number]
  }> = [
    { title: '\u0412\u044b\u0440\u0443\u0447\u043a\u0430', value: fmtMoney(kpi.revenue), subtitle: `${fmtNum(kpi.orders)} \u0437\u0430\u043a\u0430\u0437\u043e\u0432`, delta: kpi.revenue_delta, color: C.green },
    { title: '\u041a \u043f\u0435\u0440\u0435\u0447\u0438\u0441\u043b\u0435\u043d\u0438\u044e', value: fmtMoney(kpi.payout), subtitle: '', delta: kpi.payout_delta, color: C.primary },
    { title: '\u0420\u0430\u0441\u0445\u043e\u0434\u044b \u041c\u041f', value: fmtMoney(kpi.operating), subtitle: kpi.payout > 0 ? `${(kpi.operating / kpi.payout * 100).toFixed(1)}% \u043e\u0442 \u043f\u0435\u0440\u0435\u0447\u0438\u0441\u043b.` : '', delta: kpi.operating_delta ?? kpi.mp_fees_delta, color: C.orange },
    { title: '\u0420\u0435\u043a\u043b\u0430\u043c\u0430', value: fmtMoney(kpi.ad_spend), subtitle: kpi.revenue > 0 ? `\u0414\u0420\u0420 ${(kpi.ad_spend / kpi.revenue * 100).toFixed(1)}%` : '', delta: kpi.ad_spend_delta, color: C.red },
    { title: '\u0421\u0435\u0431\u0435\u0441\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c', value: fmtMoney(kpi.cogs), subtitle: kpi.revenue > 0 ? `${(kpi.cogs / kpi.revenue * 100).toFixed(1)}% \u043e\u0442 \u0432\u044b\u0440\u0443\u0447\u043a\u0438` : '', delta: kpi.cogs_delta, color: C.purple },
    { title: '\u0427\u0438\u0441\u0442\u0430\u044f \u043f\u0440\u0438\u0431\u044b\u043b\u044c', value: fmtMoney(kpi.profit), subtitle: `${kpi.profit_pct.toFixed(1)}% \u043e\u0442 \u0432\u044b\u0440\u0443\u0447\u043a\u0438`, delta: kpi.profit_delta, color: kpi.profit >= 0 ? C.green : C.red },
  ]

  const cardW = 82
  const cardH = 44
  const gap = 8
  const startX = 20

  cards.forEach((card, i) => {
    const col = i % 2
    const row = Math.floor(i / 2)
    const x = startX + col * (cardW + gap)
    const cy = y + row * (cardH + gap)

    // Card bg + border
    doc.setFillColor(...C.cardBg)
    doc.setDrawColor(...C.cardBorder)
    doc.setLineWidth(0.3)
    doc.roundedRect(x, cy, cardW, cardH, 2, 2, 'FD')

    // Color accent left bar
    doc.setFillColor(...card.color)
    doc.rect(x, cy + 1, 3, cardH - 2, 'F')

    // Title
    doc.setFont('Roboto', 'normal')
    doc.setFontSize(9)
    doc.setTextColor(...C.textMuted)
    doc.text(card.title, x + 8, cy + 11)

    // Value
    doc.setFont('Roboto', 'bold')
    doc.setFontSize(18)
    doc.setTextColor(...C.textDark)
    doc.text(card.value, x + 8, cy + 26)

    // Subtitle
    if (card.subtitle) {
      doc.setFont('Roboto', 'normal')
      doc.setFontSize(8)
      doc.setTextColor(...C.textMuted)
      doc.text(card.subtitle, x + 8, cy + 36)
    }

    // Delta badge
    if (Math.abs(card.delta) > 0.1) {
      const isUp = card.delta > 0
      const arrow = isUp ? '\u25B2' : '\u25BC'
      const deltaText = `${arrow} ${Math.abs(card.delta).toFixed(1)}%`
      doc.setFont('Roboto', 'bold')
      doc.setFontSize(8)
      doc.setTextColor(...(isUp ? C.green : C.red))
      doc.text(deltaText, x + cardW - 6, cy + 11, { align: 'right' })
    }
  })

  return y + 3 * (cardH + gap) + 8
}

// ── Comparison Table ──────────────────────────────────────────

function drawComparisonTable(doc: jsPDF, data: FinancesResponse, startY: number): number {
  const comparison = data.comparison

  const rows = [
    { key: 'revenue', label: '\u0412\u044b\u0440\u0443\u0447\u043a\u0430', bold: true },
    { key: 'orders', label: '\u0417\u0430\u043a\u0430\u0437\u044b', isMoney: false },
    { key: 'payout', label: '\u041a \u043f\u0435\u0440\u0435\u0447\u0438\u0441\u043b\u0435\u043d\u0438\u044e' },
    { key: 'mp_fees', label: '\u0423\u0434\u0435\u0440\u0436\u0430\u043d\u0438\u044f \u041c\u041f', bold: true },
    { key: 'commission', label: '  \u2514 \u041a\u043e\u043c\u0438\u0441\u0441\u0438\u044f + \u0441\u043a\u0438\u0434\u043a\u0438' },
    { key: 'logistics', label: '     \u2022 \u041b\u043e\u0433\u0438\u0441\u0442\u0438\u043a\u0430' },
    { key: 'storage', label: '     \u2022 \u0425\u0440\u0430\u043d\u0435\u043d\u0438\u0435' },
    { key: 'acquiring', label: '     \u2022 \u042d\u043a\u0432\u0430\u0439\u0440\u0438\u043d\u0433' },
    { key: 'deductions_ads', label: '     \u2022 \u0412\u0411 \u041f\u0440\u043e\u0434\u0432\u0438\u0436\u0435\u043d\u0438\u0435' },
    { key: 'deductions_other', label: '     \u2022 \u041f\u0440. \u0443\u0434\u0435\u0440\u0436\u0430\u043d\u0438\u044f' },
    { key: 'acceptance', label: '     \u2022 \u041f\u043b\u0430\u0442. \u043f\u0440\u0438\u0451\u043c\u043a\u0430' },
    { key: 'advertising', label: '\u0420\u0435\u043a\u043b\u0430\u043c\u0430' },
    { key: 'cogs', label: '\u0421\u0435\u0431\u0435\u0441\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c' },
    { key: 'profit', label: '\u0427\u0438\u0441\u0442\u0430\u044f \u043f\u0440\u0438\u0431\u044b\u043b\u044c', bold: true },
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
      Math.abs(delta) > 0.1 ? `${delta > 0 ? '+' : ''}${delta.toFixed(1)}%` : '\u2014',
    ]
  })

  autoTable(doc, {
    startY,
    head: [['\u041f\u043e\u043a\u0430\u0437\u0430\u0442\u0435\u043b\u044c', '\u0422\u0435\u043a\u0443\u0449\u0438\u0439', '\u041f\u0440\u0435\u0434\u044b\u0434\u0443\u0449\u0438\u0439', '\u0394']],
    body: tableBody,
    theme: 'plain',
    styles: {
      font: 'Roboto',
      fontSize: 9,
      textColor: C.textBody,
      cellPadding: { top: 3.5, bottom: 3.5, left: 5, right: 5 },
      lineWidth: 0,
    },
    headStyles: {
      fillColor: C.headerBg,
      textColor: C.textMuted,
      fontStyle: 'bold',
      fontSize: 9,
    },
    alternateRowStyles: {
      fillColor: C.altRow,
    },
    bodyStyles: {
      fillColor: C.white,
    },
    columnStyles: {
      0: { cellWidth: 60 },
      1: { halign: 'right', cellWidth: 38 },
      2: { halign: 'right', cellWidth: 38 },
      3: { halign: 'right', cellWidth: 28 },
    },
    didParseCell: (cellData) => {
      const boldRows = [0, 3, 13]
      if (cellData.section === 'body' && boldRows.includes(cellData.row.index)) {
        cellData.cell.styles.fontStyle = 'bold'
        cellData.cell.styles.fillColor = C.headerBg
        cellData.cell.styles.textColor = C.textDark
      }
      // Profit color
      if (cellData.section === 'body' && cellData.row.index === 13 && cellData.column.index === 1) {
        const val = comparison.current['profit'] ?? 0
        cellData.cell.styles.textColor = val >= 0 ? C.green : C.red
      }
      // Delta color
      if (cellData.section === 'body' && cellData.column.index === 3) {
        const text = String(cellData.cell.raw)
        if (text.startsWith('+')) cellData.cell.styles.textColor = C.green
        else if (text.startsWith('-')) cellData.cell.styles.textColor = C.red
      }
    },
  })

  return (doc as any).lastAutoTable?.finalY ?? startY + 100
}

// ── Weekly Report Table ──────────────────────────────────────

function drawWeeklyTable(doc: jsPDF, weeklyData: WeeklyReportResponse, marketplace: string, startY: number): number {
  const weeks = weeklyData.weeks.slice(-12)
  const isWb = marketplace === 'wildberries' || marketplace === 'wb'

  let head: string[]
  let body: string[][]

  if (isWb) {
    head = ['\u041d\u0435\u0434', '\u041f\u0435\u0440\u0438\u043e\u0434', '\u041a\u043e\u043b', '\u0412\u044b\u0440\u0443\u0447\u043a\u0430', '\u041a \u043f\u0435\u0440\u0435\u0447.', '\u041b\u043e\u0433\u0438\u0441\u0442.', '\u0425\u0440\u0430\u043d.', '\u0423\u0434\u0435\u0440\u0436.', '\u0420\u0435\u043a\u043b.', '\u0421/\u0421', '\u041f\u0440\u0438\u0431\u044b\u043b\u044c']
    body = weeks.map(w => {
      const wk = w as WBWeeklyReportRow
      return [
        `${wk.week}`,
        fmtShortDate(wk.week_start),
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
    head = ['\u041d\u0435\u0434', '\u041f\u0435\u0440\u0438\u043e\u0434', '\u041a\u043e\u043b', '\u041f\u0440\u043e\u0434\u0430\u0436\u0438', '\u041a\u043e\u043c\u0438\u0441.', '\u041b\u043e\u0433\u0438\u0441\u0442.', '\u0425\u0440\u0430\u043d.', '\u0420\u0435\u043a\u043b.', '\u0421/\u0421', '\u041f\u0440\u0438\u0431\u044b\u043b\u044c']
    body = weeks.map(w => {
      const wk = w as OzonWeeklyReportRow
      return [
        `${wk.week}`,
        fmtShortDate(wk.week_start),
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
    theme: 'grid',
    styles: {
      font: 'Roboto',
      fontSize: 8,
      textColor: C.textBody,
      cellPadding: { top: 3, bottom: 3, left: 3, right: 3 },
      lineColor: C.cardBorder,
      lineWidth: 0.2,
      halign: 'right',
    },
    headStyles: {
      fillColor: C.primary,
      textColor: C.white,
      fontStyle: 'bold',
      fontSize: 8,
      halign: 'center',
    },
    alternateRowStyles: {
      fillColor: C.altRow,
    },
    bodyStyles: {
      fillColor: C.white,
    },
    columnStyles: {
      0: { halign: 'center', cellWidth: 10 },
      1: { halign: 'left', cellWidth: 22 },
      2: { cellWidth: 10 },
    },
    didParseCell: (cellData) => {
      const lastCol = isWb ? 10 : 9
      if (cellData.section === 'body' && cellData.column.index === lastCol) {
        const numVal = parseFloat(String(cellData.cell.raw).replace(/\s/g, '').replace(',', '.'))
        if (!isNaN(numVal)) {
          cellData.cell.styles.textColor = numVal >= 0 ? C.green : C.red
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

  // Load Cyrillic fonts
  setupFonts(doc)

  const pageW = doc.internal.pageSize.getWidth()

  // ── Page 1: Cover ──
  drawCover(doc, shopName, data.date_from, data.date_to, marketplace)

  // ── Page 2: KPI ──
  doc.addPage()
  const kpiEndY = drawKpiPage(doc, data)

  // ── Try to capture charts ──
  const breakdownImg = await captureElement(breakdownSelector)
  const dynamicsImg = await captureElement(dynamicsSelector)

  // ── Breakdown chart on same page or next ──
  if (breakdownImg) {
    const availH = 297 - kpiEndY - 20 // A4 height minus margins
    const imgW = pageW - 40
    const imgH = Math.min(availH, 95)
    
    if (availH > 50) {
      doc.setFont('Roboto', 'bold')
      doc.setFontSize(12)
      doc.setTextColor(...C.textDark)
      doc.text('\u0421\u0442\u0440\u0443\u043a\u0442\u0443\u0440\u0430 \u0440\u0430\u0441\u0445\u043e\u0434\u043e\u0432', 20, kpiEndY + 2)
      doc.addImage(breakdownImg, 'PNG', 20, kpiEndY + 6, imgW, imgH)
    } else {
      doc.addPage()
      drawPageBg(doc)
      let y = drawSectionHeader(doc, '\u0421\u0442\u0440\u0443\u043a\u0442\u0443\u0440\u0430 \u0440\u0430\u0441\u0445\u043e\u0434\u043e\u0432', 30)
      doc.addImage(breakdownImg, 'PNG', 20, y, imgW, 95)
    }
  }

  // ── Dynamics Chart ──
  if (dynamicsImg) {
    doc.addPage()
    drawPageBg(doc)
    const y = drawSectionHeader(doc, '\u0414\u0438\u043d\u0430\u043c\u0438\u043a\u0430 \u043f\u043e\u043a\u0430\u0437\u0430\u0442\u0435\u043b\u0435\u0439', 30)
    const imgW = pageW - 40
    doc.addImage(dynamicsImg, 'PNG', 20, y, imgW, 100)
  }

  // ── Comparison Table ──
  doc.addPage()
  drawPageBg(doc)
  let y = drawSectionHeader(doc, '\u0421\u0440\u0430\u0432\u043d\u0435\u043d\u0438\u0435 \u043f\u0435\u0440\u0438\u043e\u0434\u043e\u0432', 30)
  y = drawComparisonTable(doc, data, y)

  // ── Weekly Report ──
  if (weeklyData && weeklyData.weeks.length > 0) {
    if (y > 180) {
      doc.addPage()
      drawPageBg(doc)
      y = 30
    } else {
      y += 12
    }
    y = drawSectionHeader(doc, '\u041f\u043e\u043d\u0435\u0434\u0435\u043b\u044c\u043d\u044b\u0439 \u043e\u0442\u0447\u0451\u0442', y)
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
