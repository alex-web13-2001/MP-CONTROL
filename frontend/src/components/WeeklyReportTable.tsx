/**
 * WeeklyReportTable — Понедельный финансовый отчёт.
 *
 * Two-panel layout: fixed left (year/week/period) + scrollable right.
 * Right panel uses a SINGLE scroll container with sticky thead/tfoot
 * so header, body, and footer all scroll together horizontally.
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

const OZON_VALUE_COLS: Col[] = [
  { key: 'qty',              label: 'Кол-во',        width: 72,  type: 'count',  section: 'values' },
  { key: 'sales',            label: 'Σ Продажи',     width: 110, type: 'money',  section: 'values', accent: true },
  { key: 'returns',          label: 'Возврат',        width: 90,  type: 'money',  section: 'values' },
  { key: 'commission',       label: 'Комиссия',       width: 100, type: 'money',  section: 'values' },
  { key: 'compensations',    label: 'Компенс.',       width: 95,  type: 'money',  section: 'values' },
  { key: 'other_services',   label: 'Логистика',      width: 100, type: 'money',  section: 'values' },
  { key: 'marketing',        label: 'Продвижение',    width: 110, type: 'money',  section: 'values' },
  { key: 'other_charges',    label: 'Пр. начисл.',    width: 100, type: 'money',  section: 'values' },
  { key: 'fbo_services',     label: 'ФБО/Поставки',  width: 105, type: 'money',  section: 'values' },
  { key: 'acquiring',        label: 'Эквайринг',      width: 95,  type: 'money',  section: 'values' },
  { key: 'delivery_services',label: 'Возвр. лог.',     width: 95,  type: 'money',  section: 'values' },
  { key: 'storage',           label: 'Хранение',       width: 95,  type: 'money',  section: 'values' },
  { key: 'payout',           label: 'К перечисл.',    width: 120, type: 'money',  section: 'values', accent: true },
  { key: 'cogs',             label: 'Себестоим.',      width: 110, type: 'money',  section: 'values' },
  { key: 'gross_profit',     label: 'ВАЛ',            width: 110, type: 'money',  section: 'values', accent: true },
]

const OZON_PCT_COLS: Col[] = [
  { key: 'commission_pct',    label: 'Комиссия %',  width: 95, type: 'pct', section: 'pct' },
  { key: 'marketing_pct',     label: 'Промо %',     width: 85, type: 'pct', section: 'pct' },
  { key: 'fbo_pct',           label: 'ФБО %',       width: 75, type: 'pct', section: 'pct' },
  { key: 'cogs_pct',          label: 'Себест. %',   width: 90, type: 'pct', section: 'pct' },
  { key: 'gross_profit_pct',  label: 'ВАЛ %',       width: 78, type: 'pct', section: 'pct' },
]

// ── WB columns ──

const WB_VALUE_COLS: Col[] = [
  { key: 'qty',               label: 'Кол-во',         width: 72,  type: 'count',  section: 'values' },
  { key: 'revenue',           label: 'Все продажи',    width: 110, type: 'money',  section: 'values', accent: true },
  { key: 'commission',        label: 'Комиссия+СПП',   width: 115, type: 'money',  section: 'values' },
  { key: 'acquiring',         label: 'Эквайринг',      width: 95,  type: 'money',  section: 'values' },
  { key: 'payout',            label: 'К выплате',      width: 120, type: 'money',  section: 'values', accent: true },
  { key: 'logistics',         label: 'Логистика',      width: 100, type: 'money',  section: 'values' },
  { key: 'storage',           label: 'Хранение',       width: 95,  type: 'money',  section: 'values' },
  { key: 'wb_promo',          label: 'ВБ Продвиж.',    width: 110, type: 'money',  section: 'values' },
  { key: 'deductions',        label: 'Удержания',     width: 100, type: 'money',  section: 'values' },
  { key: 'acceptance',        label: 'Приёмка',        width: 90,  type: 'money',  section: 'values' },
  { key: 'marketing',         label: 'Реклама',        width: 100, type: 'money',  section: 'values' },
  { key: 'cogs',              label: 'Себестоим.',     width: 110, type: 'money',  section: 'values' },
  { key: 'gross_profit',      label: 'Прибыль',        width: 110, type: 'money',  section: 'values', accent: true },
]

const WB_PCT_COLS: Col[] = [
  { key: 'commission_pct',    label: 'Комиссия %',  width: 95, type: 'pct', section: 'pct' },
  { key: 'logistics_pct',     label: 'Логист. %',   width: 90, type: 'pct', section: 'pct' },
  { key: 'cogs_pct',          label: 'Себест. %',   width: 90, type: 'pct', section: 'pct' },
  { key: 'gross_profit_pct',  label: 'ВАЛ %',       width: 78, type: 'pct', section: 'pct' },
]

type SortDir = 'asc' | 'desc' | null

// ── Component ───────────────────────────────────────────────

interface Props {
  weeks: WeeklyReportRow[]
  totals: Record<string, number>
  marketplace?: 'ozon' | 'wb'
}

export default function WeeklyReportTable({ weeks, totals, marketplace = 'ozon' }: Props) {
  const VALUE_COLS = marketplace === 'wb' ? WB_VALUE_COLS : OZON_VALUE_COLS
  const PCT_COLS = marketplace === 'wb' ? WB_PCT_COLS : OZON_PCT_COLS
  const ALL_COLS = [...VALUE_COLS, ...PCT_COLS]
  const TABLE_MIN_W = ALL_COLS.reduce((s, c) => s + c.width, 0)

  const [sortKey, setSortKey] = useState<string>('week_start')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  // Refs for syncing vertical scroll between fixed left and scrollable right
  const rightScrollRef = useRef<HTMLDivElement>(null)
  const leftBodyRef = useRef<HTMLDivElement>(null)

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

  // Sync vertical scroll: right panel drives left panel
  const onRightScroll = useCallback(() => {
    if (rightScrollRef.current && leftBodyRef.current) {
      leftBodyRef.current.scrollTop = rightScrollRef.current.scrollTop
    }
  }, [])

  const getCellVal = (row: WeeklyReportRow, col: Col): string => {
    const v = (row as any)[col.key] ?? 0
    if (col.type === 'money') return fmtMoney(v)
    if (col.type === 'pct') return fmtPct(v)
    return v === 0 ? '—' : String(v)
  }

  const profitColorCls = (gp: number): string =>
    gp > 0 ? 'wrt-profit-pos' : gp < 0 ? 'wrt-profit-neg' : ''

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

  const SortIcon = ({ colKey }: { colKey: string }) => {
    const k = colKey === '_period' ? 'week_start' : colKey
    const active = sortKey === k
    const I = !active ? ArrowUpDown : sortDir === 'asc' ? ArrowUp : ArrowDown
    return <I className={`h-3 w-3 shrink-0 inline-block ml-1 ${active ? 'opacity-70' : 'opacity-20'}`} />
  }

  const ROW_H = 33
  const HEAD_H = 40
  const FOOT_H = 40

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: 'easeOut' }}
    >
      {/* ── Theme-aware styles ── */}
      <style>{`
        /* Base */
        .wrt-card { background: hsl(var(--card)); color: hsl(var(--foreground)); }
        .wrt-hdr { background: hsl(var(--muted) / 0.12); }
        .wrt-cell { color: hsl(var(--foreground) / 0.75); }
        .wrt-cell-accent { color: hsl(var(--foreground)); font-weight: 600; }
        .wrt-row-alt { background: hsl(var(--muted) / 0.05); }
        .wrt-row:hover { background: hsl(var(--accent) / 0.07) !important; }
        .wrt-muted { color: hsl(var(--muted-foreground)); }
        .wrt-muted-soft { color: hsl(var(--muted-foreground) / 0.6); }
        .wrt-footer { background: hsl(var(--muted) / 0.1); }

        /* % section */
        .wrt-pct-bg { background: hsl(152 55% 48% / 0.06); }
        .wrt-pct-hdr { background: hsl(152 55% 48% / 0.12); }
        .wrt-pct-text { color: hsl(152 55% 32%); font-weight: 500; }
        .wrt-pct-foot { color: hsl(152 55% 28%); font-weight: 700; }
        .wrt-pct-sep { border-left: 2px solid hsl(152 55% 48% / 0.22); }

        .dark .wrt-pct-text { color: hsl(152 55% 62%); }
        .dark .wrt-pct-foot { color: hsl(152 55% 68%); }
        .dark .wrt-pct-bg { background: hsl(152 55% 48% / 0.04); }
        .dark .wrt-pct-hdr { background: hsl(152 55% 48% / 0.08); }

        /* Profit */
        .wrt-profit-pos { color: hsl(152 55% 35%) !important; }
        .wrt-profit-neg { color: hsl(0 70% 48%) !important; }
        .dark .wrt-profit-pos { color: hsl(152 55% 58%) !important; }
        .dark .wrt-profit-neg { color: hsl(0 70% 62%) !important; }

        /* Scrollbar */
        .wrt-scr::-webkit-scrollbar { height: 7px; width: 7px; }
        .wrt-scr::-webkit-scrollbar-thumb { background: hsl(var(--border)); border-radius: 4px; }
        .wrt-scr::-webkit-scrollbar-track { background: transparent; }
      `}</style>

      {/* ── Title ── */}
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

      {/* ══════════════ TABLE ══════════════ */}
      <div
        className="flex rounded-xl overflow-hidden wrt-card"
        style={{ border: '1px solid hsl(var(--border) / 0.35)' }}
      >

        {/* ▌ FIXED LEFT PANEL ▌ */}
        <div className="shrink-0 flex flex-col wrt-card" style={{ width: 220, borderRight: '2px solid hsl(var(--border) / 0.25)' }}>

          {/* Left Header */}
          <div
            className="flex items-center wrt-hdr"
            style={{ height: HEAD_H, borderBottom: '1px solid hsl(var(--border) / 0.25)' }}
          >
            <div
              className="w-[50px] shrink-0 text-center text-[11px] font-semibold wrt-muted-soft cursor-pointer select-none"
              onClick={() => handleSort('year')}
            >
              Год<SortIcon colKey="year" />
            </div>
            <div
              className="w-[38px] shrink-0 text-center text-[11px] font-semibold wrt-muted-soft cursor-pointer select-none"
              onClick={() => handleSort('week')}
            >
              №<SortIcon colKey="week" />
            </div>
            <div
              className="flex-1 pl-2 text-[11px] font-semibold wrt-muted-soft cursor-pointer select-none"
              onClick={() => handleSort('_period')}
            >
              Период<SortIcon colKey="_period" />
            </div>
          </div>

          {/* Left Body (synced scroll — hidden scrollbar) */}
          <div
            ref={leftBodyRef}
            className="flex-1 overflow-hidden"
            style={{ maxHeight: 'calc(65vh - 80px)' }}
          >
            {sorted.map((row, ri) => (
              <div
                key={row.week_start}
                className={`flex items-center wrt-row ${ri % 2 ? 'wrt-row-alt' : ''}`}
                style={{ height: ROW_H, borderBottom: '1px solid hsl(var(--border) / 0.08)' }}
              >
                <div className="w-[50px] shrink-0 text-center text-[12px] wrt-muted-soft tabular-nums">{row.year}</div>
                <div className="w-[38px] shrink-0 text-center text-[13px] font-bold tabular-nums" style={{ color: 'hsl(var(--foreground))' }}>{row.week}</div>
                <div className="flex-1 pl-2 text-[12px] wrt-muted">{fmtPeriod(row.week_start, row.week_end)}</div>
              </div>
            ))}
          </div>

          {/* Left Footer */}
          <div
            className="flex items-center wrt-footer"
            style={{ height: FOOT_H, borderTop: '2px solid hsl(var(--border) / 0.3)' }}
          >
            <div className="w-[50px] shrink-0" />
            <div className="w-[38px] shrink-0" />
            <div className="flex-1 pl-2 text-[13px] font-bold" style={{ color: 'hsl(var(--foreground))' }}>Итого</div>
          </div>
        </div>

        {/* ▌ SCROLLABLE RIGHT PANEL — single scroll container ▌ */}
        <div
          ref={rightScrollRef}
          className="flex-1 overflow-x-auto overflow-y-auto wrt-scr"
          style={{ maxHeight: `calc(65vh)` }}
          onScroll={onRightScroll}
        >
          <table className="border-collapse" style={{ minWidth: TABLE_MIN_W }}>

            {/* ── THEAD (sticky top) ── */}
            <thead>
              <tr
                className="wrt-hdr"
                style={{
                  position: 'sticky', top: 0, zIndex: 10,
                  height: HEAD_H,
                  borderBottom: '1px solid hsl(var(--border) / 0.25)',
                }}
              >
                {ALL_COLS.map((col) => {
                  const isPct = col.section === 'pct'
                  const isPctFirst = col.key === 'commission_pct'
                  return (
                    <th
                      key={col.key}
                      onClick={() => handleSort(col.key)}
                      className={`
                        text-right px-3 text-[11px] font-semibold whitespace-nowrap
                        cursor-pointer select-none
                        ${isPct ? 'wrt-pct-hdr wrt-pct-text' : 'wrt-hdr wrt-muted-soft'}
                        ${col.accent ? 'wrt-cell-accent' : ''}
                        ${isPctFirst ? 'wrt-pct-sep' : ''}
                      `}
                      style={{ width: col.width, minWidth: col.width }}
                    >
                      {col.label}
                      <SortIcon colKey={col.key} />
                    </th>
                  )
                })}
              </tr>
            </thead>

            {/* ── TBODY ── */}
            <tbody>
              {sorted.map((row, ri) => (
                <tr
                  key={row.week_start}
                  className={`wrt-row ${ri % 2 ? 'wrt-row-alt' : ''}`}
                  style={{ height: ROW_H, borderBottom: '1px solid hsl(var(--border) / 0.08)' }}
                >
                  {ALL_COLS.map((col) => {
                    const isPct = col.section === 'pct'
                    const isPctFirst = col.key === 'commission_pct'
                    const isProfit = col.key === 'gross_profit' || col.key === 'gross_profit_pct'
                    const profitCls = isProfit ? profitColorCls(row.gross_profit) : ''

                    return (
                      <td
                        key={col.key}
                        className={`
                          text-right px-3 whitespace-nowrap text-[12.5px] tabular-nums
                          ${isPct ? 'wrt-pct-bg wrt-pct-text' : col.accent ? 'wrt-cell-accent' : 'wrt-cell'}
                          ${isPctFirst ? 'wrt-pct-sep' : ''}
                          ${profitCls}
                        `}
                        style={{ width: col.width, minWidth: col.width }}
                        title={col.type === 'money' ? fmtMoneyFull((row as any)[col.key] ?? 0) : undefined}
                      >
                        {getCellVal(row, col)}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>

            {/* ── TFOOT (sticky bottom) ── */}
            <tfoot>
              <tr
                className="wrt-footer"
                style={{
                  position: 'sticky', bottom: 0, zIndex: 10,
                  height: FOOT_H,
                  borderTop: '2px solid hsl(var(--border) / 0.3)',
                }}
              >
                {ALL_COLS.map((col) => {
                  const isPct = col.section === 'pct'
                  const isPctFirst = col.key === 'commission_pct'
                  const val = totals[col.key] ?? 0
                  const isProfit = col.key === 'gross_profit' || col.key === 'gross_profit_pct'
                  const profitCls = isProfit ? profitColorCls(totals.gross_profit ?? 0) : ''

                  return (
                    <td
                      key={col.key}
                      className={`
                        text-right px-3 whitespace-nowrap text-[13px] font-bold tabular-nums
                        ${isPct ? 'wrt-pct-hdr wrt-pct-foot' : ''}
                        ${isPctFirst ? 'wrt-pct-sep' : ''}
                        ${profitCls}
                      `}
                      style={{ width: col.width, minWidth: col.width, color: !isPct && !profitCls ? 'hsl(var(--foreground))' : undefined }}
                    >
                      {col.type === 'money' ? fmtMoneyFull(val) :
                       col.type === 'pct' ? fmtPct(val) :
                       col.type === 'count' ? fmtMoneyFull(val) : ''}
                    </td>
                  )
                })}
              </tr>
            </tfoot>
          </table>
        </div>
      </div>
    </motion.div>
  )
}
