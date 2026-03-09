/**
 * WeeklyReportTable — Понедельный финансовый отчёт (Excel-style).
 *
 * Horizontal-scrollable table with:
 * - Sticky columns: Year, Week#, Period (left side)
 * - Absolute values: Sales, Returns, Commission, etc.
 * - Percentage columns (green background, right side)
 * - Totals row at bottom
 * - Color-coding for profit/loss
 */

import { useState } from 'react'
import { motion } from 'framer-motion'
import { ArrowUpDown, ArrowUp, ArrowDown, Download } from 'lucide-react'
import type { WeeklyReportRow } from '../api/finances'

// ── Helpers ──────────────────────────────────────────────────

function formatMoney(v: number): string {
  if (Math.abs(v) >= 1_000_000) {
    return (v / 1_000_000).toFixed(1).replace('.0', '') + 'M'
  }
  if (Math.abs(v) >= 1_000) {
    return Math.round(v).toLocaleString('ru-RU')
  }
  return v.toFixed(0)
}

function formatMoneyFull(v: number): string {
  return v.toLocaleString('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 0 })
}

function formatPct(v: number): string {
  return v.toFixed(1) + '%'
}

function formatPeriod(start: string, end: string): string {
  const s = new Date(start)
  const e = new Date(end)
  const months = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
  return `${s.getDate()} ${months[s.getMonth()]} – ${e.getDate()} ${months[e.getMonth()]}`
}

// ── Column Definitions ──────────────────────────────────────

interface ColumnDef {
  key: string
  label: string
  shortLabel?: string
  group: 'sticky' | 'values' | 'pct'
  format: 'number' | 'money' | 'pct' | 'period'
  invertColor?: boolean  // true = red when positive (expenses)
  highlight?: boolean    // bold row
}

const COLUMNS: ColumnDef[] = [
  // Sticky
  { key: 'year', label: 'Год', group: 'sticky', format: 'number' },
  { key: 'week', label: 'Нед.', group: 'sticky', format: 'number' },
  { key: '_period', label: 'Период', group: 'sticky', format: 'period' },
  // Values
  { key: 'qty', label: 'Кол-во', group: 'values', format: 'number' },
  { key: 'sales', label: 'Σ Продажи', group: 'values', format: 'money', highlight: true },
  { key: 'returns', label: 'Возврат', group: 'values', format: 'money', invertColor: true },
  { key: 'commission', label: 'Σ Комиссия', group: 'values', format: 'money', invertColor: true },
  { key: 'compensations', label: 'Компенс. Ozon', group: 'values', format: 'money' },
  { key: 'other_services', label: 'Σ Др. услуги', group: 'values', format: 'money', invertColor: true },
  { key: 'marketing', label: 'Σ Продвижение', group: 'values', format: 'money', invertColor: true },
  { key: 'other_charges', label: 'Σ Пр. начисл.', group: 'values', format: 'money', invertColor: true },
  { key: 'fbo_services', label: 'Усл. ФБО', group: 'values', format: 'money', invertColor: true },
  { key: 'acquiring', label: 'Усл. агентов', group: 'values', format: 'money', invertColor: true },
  { key: 'delivery_services', label: 'Усл. доставки', group: 'values', format: 'money', invertColor: true },
  { key: 'payout', label: 'К перечислению', group: 'values', format: 'money', highlight: true },
  { key: 'cogs', label: 'Себестоимость', group: 'values', format: 'money', invertColor: true },
  { key: 'gross_profit', label: 'ВАЛ', group: 'values', format: 'money', highlight: true },
  // Percent columns
  { key: 'commission_pct', label: 'Комиссия, %', group: 'pct', format: 'pct' },
  { key: 'marketing_pct', label: 'Промо, %', shortLabel: 'Промо %', group: 'pct', format: 'pct' },
  { key: 'fbo_pct', label: 'ФБО, %', group: 'pct', format: 'pct' },
  { key: 'delivery_pct', label: 'Доставка, %', group: 'pct', format: 'pct' },
  { key: 'cogs_pct', label: 'Себест., %', group: 'pct', format: 'pct' },
  { key: 'gross_profit_pct', label: 'ВАЛ, %', group: 'pct', format: 'pct' },
]

// ── Sort ─────────────────────────────────────────────────────

type SortDir = 'asc' | 'desc' | null

// ── Main Component ───────────────────────────────────────────

interface Props {
  weeks: WeeklyReportRow[]
  totals: Record<string, number>
}

export default function WeeklyReportTable({ weeks, totals }: Props) {
  const [sortKey, setSortKey] = useState<string>('week_start')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  const handleSort = (key: string) => {
    if (key === '_period') key = 'week_start'
    if (sortKey === key) {
      setSortDir(prev => (prev === 'asc' ? 'desc' : prev === 'desc' ? null : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const sorted = [...weeks].sort((a, b) => {
    if (!sortDir || !sortKey) return 0
    const ak = (a as any)[sortKey] ?? 0
    const bk = (b as any)[sortKey] ?? 0
    if (typeof ak === 'string') return sortDir === 'asc' ? ak.localeCompare(bk) : bk.localeCompare(ak)
    return sortDir === 'asc' ? ak - bk : bk - ak
  })

  const getCellValue = (row: WeeklyReportRow, col: ColumnDef): string => {
    if (col.key === '_period') return formatPeriod(row.week_start, row.week_end)
    const val = (row as any)[col.key]
    if (col.format === 'money') return formatMoney(val)
    if (col.format === 'pct') return formatPct(val)
    return String(val ?? '')
  }

  const getCellColor = (row: WeeklyReportRow, col: ColumnDef): string => {
    if (col.key === 'gross_profit' || col.key === 'gross_profit_pct') {
      const gp = row.gross_profit
      return gp > 0 ? 'text-emerald-400' : gp < 0 ? 'text-red-400' : ''
    }
    return ''
  }

  const getTotalValue = (col: ColumnDef): string => {
    if (col.key === '_period') return 'Итого'
    if (col.key === 'year' || col.key === 'week') return ''
    const val = totals[col.key] ?? 0
    if (col.format === 'money') return formatMoneyFull(val)
    if (col.format === 'pct') return formatPct(val)
    if (col.format === 'number') return formatMoneyFull(val)
    return ''
  }

  // Export to CSV
  const exportCsv = () => {
    const headers = COLUMNS.map(c => c.label).join(';')
    const rows = sorted.map(row =>
      COLUMNS.map(col => {
        if (col.key === '_period') return `${row.week_start} – ${row.week_end}`
        return (row as any)[col.key] ?? ''
      }).join(';')
    )
    const csv = [headers, ...rows].join('\n')
    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `weekly_report_${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  // Sticky column widths
  const stickyWidths = [60, 50, 130] // year, week, period
  const stickyOffsets = [0, 60, 110]

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: 'easeOut' }}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-lg font-bold text-[hsl(var(--foreground))]">
            Понедельный отчёт
          </h3>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            {weeks.length} недель • с {weeks.length > 0 ? formatPeriod(weeks[weeks.length - 1]?.week_start ?? weeks[0]?.week_start, weeks[0]?.week_end ?? weeks[0]?.week_start) : '—'}
          </p>
        </div>
        <button
          onClick={exportCsv}
          className="inline-flex items-center gap-2 rounded-lg border border-[hsl(var(--border))] px-3 py-2 text-sm font-medium text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.15)] transition-colors"
        >
          <Download className="h-4 w-4" />
          CSV
        </button>
      </div>

      {/* Table */}
      <div
        className="overflow-x-auto rounded-xl border border-[hsl(var(--border)/0.5)] bg-[hsl(var(--card))]"
        style={{ maxHeight: '70vh' }}
      >
        <table className="w-max min-w-full text-[13px]">
          {/* ── Header ── */}
          <thead className="sticky top-0 z-20 bg-[hsl(var(--card))]">
            <tr className="border-b border-[hsl(var(--border)/0.5)]">
              {COLUMNS.map((col, i) => {
                const isSticky = col.group === 'sticky'
                const isPct = col.group === 'pct'
                const isSorted = sortKey === col.key || (col.key === '_period' && sortKey === 'week_start')
                const SortIcon = !isSorted ? ArrowUpDown : sortDir === 'asc' ? ArrowUp : ArrowDown

                return (
                  <th
                    key={col.key}
                    onClick={() => handleSort(col.key)}
                    className={`
                      px-3 py-2.5 text-[11px] font-semibold uppercase tracking-wider cursor-pointer
                      select-none whitespace-nowrap transition-colors
                      hover:bg-[hsl(var(--muted)/0.15)]
                      ${isSticky ? 'sticky z-30 bg-[hsl(var(--card))]' : ''}
                      ${isPct ? 'bg-emerald-950/30' : ''}
                      ${col.highlight ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground)/0.7)]'}
                    `}
                    style={isSticky ? {
                      left: stickyOffsets[i] + 'px',
                      minWidth: stickyWidths[i] + 'px',
                      maxWidth: stickyWidths[i] + 'px',
                      boxShadow: i === 2 ? '2px 0 8px -2px rgba(0,0,0,0.2)' : undefined,
                    } : { minWidth: isPct ? '85px' : '100px' }}
                  >
                    <div className="flex items-center gap-1 justify-end">
                      <span>{col.shortLabel || col.label}</span>
                      <SortIcon className={`h-3 w-3 shrink-0 ${isSorted ? 'opacity-100' : 'opacity-30'}`} />
                    </div>
                  </th>
                )
              })}
            </tr>
          </thead>

          {/* ── Body ── */}
          <tbody>
            {sorted.map((row, ri) => (
              <tr
                key={row.week_start}
                className={`
                  border-b border-[hsl(var(--border)/0.15)]
                  transition-colors hover:bg-[hsl(var(--muted)/0.1)]
                  ${ri % 2 === 0 ? '' : 'bg-[hsl(var(--muted)/0.04)]'}
                `}
              >
                {COLUMNS.map((col, ci) => {
                  const isSticky = col.group === 'sticky'
                  const isPct = col.group === 'pct'
                  const cellColor = getCellColor(row, col)

                  return (
                    <td
                      key={col.key}
                      className={`
                        px-3 py-2 text-right whitespace-nowrap font-mono text-[12px]
                        ${isSticky ? 'sticky z-10 bg-[hsl(var(--card))]' : ''}
                        ${isPct ? 'bg-emerald-950/20 font-semibold' : ''}
                        ${col.highlight ? 'font-bold text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))]'}
                        ${cellColor}
                        ${ri % 2 !== 0 && isSticky ? 'bg-[hsl(var(--muted)/0.04)]' : ''}
                      `}
                      style={isSticky ? {
                        left: stickyOffsets[ci] + 'px',
                        minWidth: stickyWidths[ci] + 'px',
                        maxWidth: stickyWidths[ci] + 'px',
                        boxShadow: ci === 2 ? '2px 0 8px -2px rgba(0,0,0,0.2)' : undefined,
                      } : undefined}
                      title={col.format === 'money' ? formatMoneyFull((row as any)[col.key] ?? 0) : undefined}
                    >
                      {getCellValue(row, col)}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>

          {/* ── Totals ── */}
          <tfoot className="sticky bottom-0 z-20">
            <tr className="border-t-2 border-[hsl(var(--border)/0.5)] bg-[hsl(var(--card))] font-bold">
              {COLUMNS.map((col, ci) => {
                const isSticky = col.group === 'sticky'
                const isPct = col.group === 'pct'
                const val = getTotalValue(col)
                const isGp = col.key === 'gross_profit' || col.key === 'gross_profit_pct'
                const gpColor = isGp
                  ? (totals.gross_profit ?? 0) > 0 ? 'text-emerald-400' : 'text-red-400'
                  : ''

                return (
                  <td
                    key={col.key}
                    className={`
                      px-3 py-3 text-right whitespace-nowrap font-mono text-[13px] font-bold
                      ${isSticky ? 'sticky z-30 bg-[hsl(var(--card))]' : 'bg-[hsl(var(--card))]'}
                      ${isPct ? 'bg-emerald-950/30' : ''}
                      ${col.key === '_period' ? 'text-left text-[hsl(var(--foreground))]' : ''}
                      ${gpColor}
                    `}
                    style={isSticky ? {
                      left: stickyOffsets[ci] + 'px',
                      minWidth: stickyWidths[ci] + 'px',
                      maxWidth: stickyWidths[ci] + 'px',
                      boxShadow: ci === 2 ? '2px 0 8px -2px rgba(0,0,0,0.2)' : undefined,
                    } : undefined}
                  >
                    {val}
                  </td>
                )
              })}
            </tr>
          </tfoot>
        </table>
      </div>
    </motion.div>
  )
}
