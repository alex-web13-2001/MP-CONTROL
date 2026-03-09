/**
 * WeeklyReportTable — Понедельный финансовый отчёт.
 *
 * Two-panel layout: fixed left (year/week/period) + scrollable right.
 * Theme-aware colors — works on both light and dark themes.
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
  width: number
  type: 'money' | 'pct' | 'count'
  section: 'values' | 'pct'
  accent?: boolean
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
    return row.gross_profit > 0 ? 'profit-positive' : row.gross_profit < 0 ? 'profit-negative' : ''
  }

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
    return <Icon className={`h-3 w-3 shrink-0 inline-block ml-1 ${active ? 'opacity-80' : 'opacity-25'}`} />
  }

  const ROW_H = 34

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: 'easeOut' }}
    >
      {/* ── Inline styles for theme-aware colors ── */}
      <style>{`
        .wrt-header-bg { background: hsl(var(--muted) / 0.15); }
        .wrt-cell { color: hsl(var(--foreground) / 0.75); }
        .wrt-cell-accent { color: hsl(var(--foreground)); font-weight: 600; }
        .wrt-row-alt { background: hsl(var(--muted) / 0.06); }
        .wrt-row:hover { background: hsl(var(--accent) / 0.08) !important; }
        .wrt-border { border-color: hsl(var(--border) / 0.4); }
        .wrt-border-light { border-color: hsl(var(--border) / 0.15); }
        .wrt-muted { color: hsl(var(--muted-foreground)); }
        .wrt-muted-soft { color: hsl(var(--muted-foreground) / 0.6); }

        /* % section — green tint that works on both themes */
        .wrt-pct-bg { background: hsl(152 60% 50% / 0.07); }
        .wrt-pct-header { background: hsl(152 60% 50% / 0.12); }
        .wrt-pct-text { color: hsl(152 60% 35%); font-weight: 500; }
        .wrt-pct-border { border-left: 2px solid hsl(152 60% 50% / 0.25); }
        .wrt-pct-total { color: hsl(152 60% 30%); font-weight: 700; }

        /* dark mode overrides */
        .dark .wrt-pct-text { color: hsl(152 60% 65%); }
        .dark .wrt-pct-total { color: hsl(152 60% 70%); }
        .dark .wrt-pct-bg { background: hsl(152 60% 50% / 0.05); }
        .dark .wrt-pct-header { background: hsl(152 60% 50% / 0.1); }

        /* Profit coloring — both themes */
        .profit-positive { color: hsl(152 60% 38%) !important; }
        .profit-negative { color: hsl(0 72% 50%) !important; }
        .dark .profit-positive { color: hsl(152 60% 60%) !important; }
        .dark .profit-negative { color: hsl(0 72% 65%) !important; }

        /* Footer highlight */
        .wrt-footer-bg { background: hsl(var(--muted) / 0.12); }
        .wrt-footer-text { color: hsl(var(--foreground)); font-weight: 700; }

        /* Fixed panel bg */
        .wrt-fixed-bg { background: hsl(var(--card)); }

        /* Scrollbar - minimal */
        .wrt-scroll::-webkit-scrollbar { height: 6px; width: 6px; }
        .wrt-scroll::-webkit-scrollbar-thumb { background: hsl(var(--border)); border-radius: 3px; }
        .wrt-scroll::-webkit-scrollbar-track { background: transparent; }
      `}</style>

      {/* ── Title bar ── */}
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-lg font-bold" style={{ color: 'hsl(var(--foreground))' }}>
            Понедельный отчёт
          </h3>
          <p className="text-[13px] wrt-muted">
            {weeks.length} нед. •{' '}
            {sorted.length > 0 && `${fmtPeriod(sorted[sorted.length - 1].week_start, sorted[sorted.length - 1].week_end)} → ${fmtPeriod(sorted[0].week_start, sorted[0].week_end)}`}
          </p>
        </div>
        <button
          onClick={exportCsv}
          className="inline-flex items-center gap-2 rounded-lg px-3.5 py-2 text-sm font-medium wrt-muted transition-colors hover:opacity-80"
          style={{ border: '1px solid hsl(var(--border))' }}
        >
          <Download className="h-4 w-4" />
          CSV
        </button>
      </div>

      {/* ── Table ── */}
      <div
        className="flex rounded-xl overflow-hidden wrt-border"
        style={{ border: '1px solid hsl(var(--border) / 0.4)' }}
      >

        {/* ▌ FIXED LEFT ▌ */}
        <div
          className="shrink-0 flex flex-col wrt-fixed-bg"
          style={{ width: 220, borderRight: '2px solid hsl(var(--border) / 0.3)' }}
        >
          {/* Header */}
          <div className="flex wrt-header-bg" style={{ height: 42, borderBottom: '1px solid hsl(var(--border) / 0.3)' }}>
            <div
              className="w-[50px] shrink-0 flex items-center justify-center text-[11px] font-semibold wrt-muted-soft cursor-pointer select-none"
              onClick={() => handleSort('year')}
            >
              Год<SortIndicator colKey="year" />
            </div>
            <div
              className="w-[38px] shrink-0 flex items-center justify-center text-[11px] font-semibold wrt-muted-soft cursor-pointer select-none"
              onClick={() => handleSort('week')}
            >
              №<SortIndicator colKey="week" />
            </div>
            <div
              className="flex-1 flex items-center pl-2 text-[11px] font-semibold wrt-muted-soft cursor-pointer select-none"
              onClick={() => handleSort('_period')}
            >
              Период<SortIndicator colKey="_period" />
            </div>
          </div>

          {/* Body */}
          <div
            ref={fixedBodyRef}
            className="overflow-hidden flex-1"
            style={{ maxHeight: 'calc(65vh - 84px)' }}
          >
            {sorted.map((row, ri) => (
              <div
                key={row.week_start}
                className={`flex items-center wrt-row wrt-border-light ${ri % 2 ? 'wrt-row-alt' : ''}`}
                style={{ height: ROW_H, borderBottom: '1px solid hsl(var(--border) / 0.1)' }}
              >
                <div className="w-[50px] shrink-0 text-center text-[12px] wrt-muted-soft tabular-nums">
                  {row.year}
                </div>
                <div className="w-[38px] shrink-0 text-center text-[13px] font-bold tabular-nums" style={{ color: 'hsl(var(--foreground))' }}>
                  {row.week}
                </div>
                <div className="flex-1 pl-2 text-[12px] wrt-muted">
                  {fmtPeriod(row.week_start, row.week_end)}
                </div>
              </div>
            ))}
          </div>

          {/* Footer */}
          <div
            className="flex items-center wrt-footer-bg"
            style={{ height: 42, borderTop: '2px solid hsl(var(--border) / 0.4)' }}
          >
            <div className="w-[50px] shrink-0" />
            <div className="w-[38px] shrink-0" />
            <div className="flex-1 pl-2 text-[13px] font-bold" style={{ color: 'hsl(var(--foreground))' }}>
              Итого
            </div>
          </div>
        </div>

        {/* ▌ SCROLLABLE RIGHT ▌ */}
        <div className="flex-1 overflow-hidden flex flex-col" style={{ background: 'hsl(var(--card))' }}>
          {/* Header */}
          <div className="overflow-x-auto wrt-scroll" style={{ height: 42 }}>
            <div className="flex" style={{ minWidth: ALL_COLS.reduce((s, c) => s + c.width, 0) }}>
              {ALL_COLS.map((col) => {
                const isPct = col.section === 'pct'
                const isPctFirst = col.key === 'commission_pct'

                return (
                  <div
                    key={col.key}
                    onClick={() => handleSort(col.key)}
                    className={`
                      shrink-0 flex items-center justify-end px-3 text-[11px] font-semibold
                      cursor-pointer select-none transition-colors
                      ${isPct ? 'wrt-pct-header wrt-pct-text' : 'wrt-header-bg wrt-muted-soft'}
                      ${col.accent ? 'wrt-cell-accent' : ''}
                      ${isPctFirst ? 'wrt-pct-border' : ''}
                    `}
                    style={{
                      width: col.width, minWidth: col.width,
                      borderBottom: '1px solid hsl(var(--border) / 0.3)',
                    }}
                  >
                    {col.label}
                    <SortIndicator colKey={col.key} />
                  </div>
                )
              })}
            </div>
          </div>

          {/* Body */}
          <div
            ref={scrollRef}
            className="overflow-x-auto overflow-y-auto flex-1 wrt-scroll"
            style={{ maxHeight: 'calc(65vh - 84px)' }}
            onScroll={onScroll}
          >
            <div style={{ minWidth: ALL_COLS.reduce((s, c) => s + c.width, 0) }}>
              {sorted.map((row, ri) => (
                <div
                  key={row.week_start}
                  className={`flex wrt-row ${ri % 2 ? 'wrt-row-alt' : ''}`}
                  style={{ height: ROW_H, borderBottom: '1px solid hsl(var(--border) / 0.1)' }}
                >
                  {ALL_COLS.map((col) => {
                    const isPct = col.section === 'pct'
                    const isPctFirst = col.key === 'commission_pct'
                    const isProfit = col.key === 'gross_profit' || col.key === 'gross_profit_pct'
                    const profitCls = isProfit ? profitColor(row) : ''

                    return (
                      <div
                        key={col.key}
                        className={`
                          shrink-0 flex items-center justify-end px-3 text-[12.5px] tabular-nums
                          ${isPct ? 'wrt-pct-bg wrt-pct-text' : col.accent ? 'wrt-cell-accent' : 'wrt-cell'}
                          ${isPctFirst ? 'wrt-pct-border' : ''}
                          ${profitCls}
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

          {/* Footer */}
          <div className="overflow-x-auto wrt-scroll" style={{ height: 42, borderTop: '2px solid hsl(var(--border) / 0.4)' }}>
            <div className="flex wrt-footer-bg" style={{ minWidth: ALL_COLS.reduce((s, c) => s + c.width, 0) }}>
              {ALL_COLS.map((col) => {
                const isPct = col.section === 'pct'
                const isPctFirst = col.key === 'commission_pct'
                const val = totals[col.key] ?? 0
                const isProfit = col.key === 'gross_profit' || col.key === 'gross_profit_pct'
                const profitCls = isProfit
                  ? (totals.gross_profit ?? 0) > 0 ? 'profit-positive' : 'profit-negative'
                  : ''

                return (
                  <div
                    key={col.key}
                    className={`
                      shrink-0 flex items-center justify-end px-3 text-[13px] tabular-nums
                      ${isPct ? 'wrt-pct-header wrt-pct-total' : 'wrt-footer-text'}
                      ${isPctFirst ? 'wrt-pct-border' : ''}
                      ${profitCls}
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
