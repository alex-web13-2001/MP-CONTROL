/**
 * WeeklyReportTable — Понедельный финансовый отчёт.
 *
 * Premium Excel-style table:
 * - Fixed left panel (year + week + period) — NOT using CSS sticky (causes overlaps)
 *   Instead: two side-by-side containers with synced scroll
 * - Scrollable right panel with financial columns
 * - Green-tinted percentage section
 * - Sticky header + footer (totals)
 * - Sort, CSV export
 */

import { useState, useRef, useCallback } from 'react'
import { motion } from 'framer-motion'
import { ArrowUpDown, ArrowUp, ArrowDown, Download } from 'lucide-react'
import type { WeeklyReportRow } from '../api/finances'

// ── Helpers ──────────────────────────────────────────────────

const MONTHS = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']

function fmtMoney(v: number): string {
  if (v === 0) return '—'
  const sign = v < 0 ? '−' : ''
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return sign + (abs / 1_000_000).toFixed(1).replace('.0', '') + ' M'
  return sign + Math.round(abs).toLocaleString('ru-RU')
}

function fmtMoneyFull(v: number): string {
  return v.toLocaleString('ru-RU', { minimumFractionDigits: 0, maximumFractionDigits: 0 })
}

function fmtPct(v: number): string {
  if (v === 0) return '—'
  return v.toFixed(1) + '%'
}

function fmtPeriod(start: string, end: string): string {
  const s = new Date(start + 'T00:00:00')
  const e = new Date(end + 'T00:00:00')
  const sd = s.getDate()
  const sm = MONTHS[s.getMonth()]
  const ed = e.getDate()
  const em = MONTHS[e.getMonth()]
  if (sm === em) return `${sd}–${ed} ${sm}`
  return `${sd} ${sm} – ${ed} ${em}`
}

// ── Column config ───────────────────────────────────────────

interface Col {
  key: string
  label: string
  width: number        // px
  type: 'money' | 'pct' | 'count'
  section: 'values' | 'pct'
  accent?: boolean     // highlight column (sales, payout, profit)
  invertColor?: boolean
}

const VALUE_COLS: Col[] = [
  { key: 'qty',              label: 'Кол-во',        width: 70,  type: 'count',  section: 'values' },
  { key: 'sales',            label: 'Σ Продажи',     width: 110, type: 'money',  section: 'values', accent: true },
  { key: 'returns',          label: 'Возврат',        width: 90,  type: 'money',  section: 'values' },
  { key: 'commission',       label: 'Комиссия',       width: 100, type: 'money',  section: 'values' },
  { key: 'compensations',    label: 'Компенс.',       width: 95,  type: 'money',  section: 'values' },
  { key: 'other_services',   label: 'Др. услуги',     width: 95,  type: 'money',  section: 'values' },
  { key: 'marketing',        label: 'Продвижение',    width: 110, type: 'money',  section: 'values' },
  { key: 'other_charges',    label: 'Пр. начисл.',    width: 100, type: 'money',  section: 'values' },
  { key: 'fbo_services',     label: 'Усл. ФБО',       width: 95,  type: 'money',  section: 'values' },
  { key: 'acquiring',        label: 'Эквайринг',      width: 95,  type: 'money',  section: 'values' },
  { key: 'delivery_services',label: 'Доставка',       width: 95,  type: 'money',  section: 'values' },
  { key: 'payout',           label: 'К перечисл.',    width: 120, type: 'money',  section: 'values', accent: true },
  { key: 'cogs',             label: 'Себестоим.',      width: 110, type: 'money',  section: 'values' },
  { key: 'gross_profit',     label: 'ВАЛ',            width: 110, type: 'money',  section: 'values', accent: true },
]

const PCT_COLS: Col[] = [
  { key: 'commission_pct',    label: 'Комиссия %',  width: 90, type: 'pct', section: 'pct' },
  { key: 'marketing_pct',     label: 'Промо %',     width: 80, type: 'pct', section: 'pct' },
  { key: 'fbo_pct',           label: 'ФБО %',       width: 75, type: 'pct', section: 'pct' },
  { key: 'delivery_pct',      label: 'Доставка %',  width: 90, type: 'pct', section: 'pct' },
  { key: 'cogs_pct',          label: 'Себест. %',   width: 85, type: 'pct', section: 'pct' },
  { key: 'gross_profit_pct',  label: 'ВАЛ %',       width: 75, type: 'pct', section: 'pct' },
]

const ALL_COLS = [...VALUE_COLS, ...PCT_COLS]

// ── Styles ──────────────────────────────────────────────────

const cellBase = 'px-3 py-[7px] text-right whitespace-nowrap text-[12.5px] tabular-nums'
const headerBase = 'px-3 py-2.5 text-right whitespace-nowrap text-[11px] font-semibold tracking-wide cursor-pointer select-none transition-colors'

type SortDir = 'asc' | 'desc' | null

// ── Component ───────────────────────────────────────────────

interface Props {
  weeks: WeeklyReportRow[]
  totals: Record<string, number>
}

export default function WeeklyReportTable({ weeks, totals }: Props) {
  const [sortKey, setSortKey] = useState<string>('week_start')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const scrollRef = useRef<HTMLDivElement>(null)
  const fixedBodyRef = useRef<HTMLDivElement>(null)

  const handleSort = (key: string) => {
    const k = key === '_period' ? 'week_start' : key
    if (sortKey === k) {
      setSortDir(prev => (prev === 'asc' ? 'desc' : prev === 'desc' ? null : 'asc'))
    } else {
      setSortKey(k)
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

  // Sync vertical scroll between fixed and scrollable panels
  const onScroll = useCallback(() => {
    if (scrollRef.current && fixedBodyRef.current) {
      fixedBodyRef.current.scrollTop = scrollRef.current.scrollTop
    }
  }, [])

  const getCellVal = (row: WeeklyReportRow, col: Col): string => {
    const v = (row as any)[col.key] ?? 0
    if (col.type === 'money') return fmtMoney(v)
    if (col.type === 'pct') return fmtPct(v)
    return v === 0 ? '—' : String(v)
  }

  const profitColor = (row: WeeklyReportRow): string => {
    return row.gross_profit > 0 ? 'text-emerald-400' : row.gross_profit < 0 ? 'text-red-400' : ''
  }

  // Export CSV
  const exportCsv = () => {
    const headers = ['Год', 'Нед.', 'Начало', 'Конец', ...ALL_COLS.map(c => c.label)].join(';')
    const rows = sorted.map(row =>
      [row.year, row.week, row.week_start, row.week_end, ...ALL_COLS.map(c => (row as any)[c.key] ?? 0)].join(';')
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

  const SortIndicator = ({ colKey }: { colKey: string }) => {
    const k = colKey === '_period' ? 'week_start' : colKey
    const active = sortKey === k
    const Icon = !active ? ArrowUpDown : sortDir === 'asc' ? ArrowUp : ArrowDown
    return <Icon className={`h-3 w-3 shrink-0 inline-block ml-1 ${active ? 'opacity-80' : 'opacity-20'}`} />
  }

  const ROW_H = 33 // px — consistent row height

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: 'easeOut' }}
    >
      {/* ── Title bar ── */}
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-lg font-bold text-[hsl(var(--foreground))]">
            Понедельный отчёт
          </h3>
          <p className="text-[13px] text-[hsl(var(--muted-foreground))]">
            {weeks.length} нед. •{' '}
            {sorted.length > 0 && `${fmtPeriod(sorted[sorted.length - 1].week_start, sorted[sorted.length - 1].week_end)} → ${fmtPeriod(sorted[0].week_start, sorted[0].week_end)}`}
          </p>
        </div>
        <button
          onClick={exportCsv}
          className="inline-flex items-center gap-2 rounded-lg border border-[hsl(var(--border))]
                     px-3.5 py-2 text-sm font-medium text-[hsl(var(--muted-foreground))]
                     hover:bg-[hsl(var(--muted)/0.15)] transition-colors"
        >
          <Download className="h-4 w-4" />
          CSV
        </button>
      </div>

      {/* ── Table layout: Fixed left + Scrollable right ── */}
      <div className="flex rounded-xl border border-[hsl(var(--border)/0.4)] overflow-hidden bg-[hsl(var(--card))]">

        {/* ▌ FIXED LEFT PANEL — Year / Week / Period ▌ */}
        <div className="shrink-0 border-r-2 border-[hsl(var(--border)/0.3)] flex flex-col" style={{ width: 210 }}>
          {/* Fixed header */}
          <div
            className="flex border-b border-[hsl(var(--border)/0.4)] bg-[hsl(var(--muted)/0.08)]"
            style={{ height: 40 }}
          >
            <div
              className={`${headerBase} w-[48px] shrink-0 text-center text-[hsl(var(--muted-foreground)/0.6)]`}
              onClick={() => handleSort('year')}
            >
              Год<SortIndicator colKey="year" />
            </div>
            <div
              className={`${headerBase} w-[40px] shrink-0 text-center text-[hsl(var(--muted-foreground)/0.6)]`}
              onClick={() => handleSort('week')}
            >
              №<SortIndicator colKey="week" />
            </div>
            <div
              className={`${headerBase} flex-1 text-left text-[hsl(var(--muted-foreground)/0.6)]`}
              onClick={() => handleSort('_period')}
            >
              Период<SortIndicator colKey="_period" />
            </div>
          </div>

          {/* Fixed body */}
          <div
            ref={fixedBodyRef}
            className="overflow-hidden flex-1"
            style={{ maxHeight: 'calc(65vh - 80px)' }}
          >
            {sorted.map((row, ri) => (
              <div
                key={row.week_start}
                className={`flex items-center border-b border-[hsl(var(--border)/0.08)] ${ri % 2 ? 'bg-[hsl(var(--muted)/0.04)]' : ''}`}
                style={{ height: ROW_H }}
              >
                <div className="w-[48px] shrink-0 text-center text-[12px] text-[hsl(var(--muted-foreground)/0.5)] tabular-nums">
                  {row.year}
                </div>
                <div className="w-[40px] shrink-0 text-center text-[12.5px] font-semibold text-[hsl(var(--foreground))] tabular-nums">
                  {row.week}
                </div>
                <div className="flex-1 pl-2 text-[12px] text-[hsl(var(--muted-foreground))]">
                  {fmtPeriod(row.week_start, row.week_end)}
                </div>
              </div>
            ))}
          </div>

          {/* Fixed footer — totals */}
          <div
            className="flex items-center border-t-2 border-[hsl(var(--border)/0.4)] bg-[hsl(var(--muted)/0.08)]"
            style={{ height: 40 }}
          >
            <div className="w-[48px] shrink-0" />
            <div className="w-[40px] shrink-0" />
            <div className="flex-1 pl-2 text-[13px] font-bold text-[hsl(var(--foreground))]">
              Итого
            </div>
          </div>
        </div>

        {/* ▌ SCROLLABLE RIGHT PANEL — All data columns ▌ */}
        <div className="flex-1 overflow-hidden flex flex-col">
          {/* Scrollable header */}
          <div className="overflow-x-auto" style={{ height: 40 }}>
            <div className="flex" style={{ minWidth: ALL_COLS.reduce((s, c) => s + c.width, 0) }}>
              {ALL_COLS.map((col, ci) => {
                const isPct = col.section === 'pct'
                const isFirst = ci === 0
                const isPctFirst = col.key === 'commission_pct'

                return (
                  <div
                    key={col.key}
                    onClick={() => handleSort(col.key)}
                    className={`
                      ${headerBase} shrink-0
                      ${isPct ? 'bg-emerald-900/15 text-emerald-400/70' : 'bg-[hsl(var(--muted)/0.08)] text-[hsl(var(--muted-foreground)/0.6)]'}
                      ${col.accent ? '!text-[hsl(var(--foreground)/0.8)]' : ''}
                      ${isPctFirst ? 'border-l-2 border-emerald-800/30' : ''}
                      hover:bg-[hsl(var(--muted)/0.15)]
                    `}
                    style={{ width: col.width, minWidth: col.width }}
                  >
                    {col.label}
                    <SortIndicator colKey={col.key} />
                  </div>
                )
              })}
            </div>
          </div>

          {/* Scrollable body */}
          <div
            ref={scrollRef}
            className="overflow-x-auto overflow-y-auto flex-1"
            style={{ maxHeight: 'calc(65vh - 80px)' }}
            onScroll={onScroll}
          >
            <div style={{ minWidth: ALL_COLS.reduce((s, c) => s + c.width, 0) }}>
              {sorted.map((row, ri) => (
                <div
                  key={row.week_start}
                  className={`
                    flex border-b border-[hsl(var(--border)/0.08)]
                    transition-colors hover:bg-[hsl(var(--primary)/0.04)]
                    ${ri % 2 ? 'bg-[hsl(var(--muted)/0.04)]' : ''}
                  `}
                  style={{ height: ROW_H }}
                >
                  {ALL_COLS.map((col) => {
                    const isPct = col.section === 'pct'
                    const isPctFirst = col.key === 'commission_pct'
                    const isProfit = col.key === 'gross_profit' || col.key === 'gross_profit_pct'
                    const colorCls = isProfit ? profitColor(row) : col.accent ? 'text-[hsl(var(--foreground))] font-semibold' : ''

                    return (
                      <div
                        key={col.key}
                        className={`
                          ${cellBase} shrink-0
                          ${isPct ? 'bg-emerald-900/8 text-emerald-300/80 font-medium' : 'text-[hsl(var(--muted-foreground)/0.85)]'}
                          ${isPctFirst ? 'border-l-2 border-emerald-800/20' : ''}
                          ${colorCls}
                        `}
                        style={{ width: col.width, minWidth: col.width }}
                        title={col.type === 'money' ? fmtMoneyFull((row as any)[col.key] ?? 0) : undefined}
                      >
                        {getCellVal(row, col)}
                      </div>
                    )
                  })}
                </div>
              ))}
            </div>
          </div>

          {/* Scrollable footer — totals */}
          <div className="overflow-x-auto border-t-2 border-[hsl(var(--border)/0.4)]" style={{ height: 40 }}>
            <div className="flex bg-[hsl(var(--muted)/0.08)]" style={{ minWidth: ALL_COLS.reduce((s, c) => s + c.width, 0) }}>
              {ALL_COLS.map((col) => {
                const isPct = col.section === 'pct'
                const isPctFirst = col.key === 'commission_pct'
                const val = totals[col.key] ?? 0
                const isProfit = col.key === 'gross_profit' || col.key === 'gross_profit_pct'
                const gpColor = isProfit
                  ? (totals.gross_profit ?? 0) > 0 ? 'text-emerald-400' : 'text-red-400'
                  : ''

                return (
                  <div
                    key={col.key}
                    className={`
                      px-3 py-2 text-right whitespace-nowrap text-[13px] font-bold tabular-nums shrink-0
                      ${isPct ? 'bg-emerald-900/15 text-emerald-300' : 'text-[hsl(var(--foreground))]'}
                      ${isPctFirst ? 'border-l-2 border-emerald-800/30' : ''}
                      ${gpColor}
                    `}
                    style={{ width: col.width, minWidth: col.width }}
                  >
                    {col.type === 'money' ? fmtMoneyFull(val) :
                     col.type === 'pct' ? fmtPct(val) :
                     col.type === 'count' ? fmtMoneyFull(val) : ''}
                  </div>
                )
              })}
            </div>
          </div>
        </div>
      </div>
    </motion.div>
  )
}
