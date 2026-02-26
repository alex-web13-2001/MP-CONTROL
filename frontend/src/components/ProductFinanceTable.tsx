/**
 * ProductFinanceTable — Product-level P&L breakdown.
 *
 * Sortable table showing per-product financials:
 * revenue, logistics, storage, ads, COGS, profit —
 * with delta % vs previous period and % of revenue.
 */
import { useState, useMemo } from 'react'
import { motion } from 'framer-motion'
import { ArrowUpDown, ArrowUp, ArrowDown, TrendingUp, TrendingDown, Package } from 'lucide-react'
import type { ProductFinanceItem } from '@/api/finances'

// ── Helpers ──────────────────────────────────────────────────

function formatMoney(v: number): string {
  if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M ₽`
  if (Math.abs(v) >= 1_000) return `${Math.round(v).toLocaleString('ru-RU')} ₽`
  return `${v.toFixed(0)} ₽`
}

function cn(...classes: (string | false | undefined)[]): string {
  return classes.filter(Boolean).join(' ')
}

// ── Column config ────────────────────────────────────────────

interface Column {
  key: string
  label: string
  shortLabel?: string
  invert?: boolean  // true = lower is better (costs)
  isCount?: boolean
}

const WB_COLUMNS: Column[] = [
  { key: 'sales', label: 'Продажи', isCount: true },
  { key: 'revenue', label: 'Выручка' },
  { key: 'logistics', label: 'Логистика', invert: true },
  { key: 'storage', label: 'Хранение', invert: true },
  { key: 'ad_spend', label: 'Реклама', invert: true },
  { key: 'cogs', label: 'Себестоимость', shortLabel: 'С/с', invert: true },
  { key: 'profit', label: 'Прибыль' },
]

const OZON_COLUMNS: Column[] = [
  { key: 'sales', label: 'Продажи', isCount: true },
  { key: 'revenue', label: 'Выручка' },
  { key: 'commission', label: 'Комиссия', invert: true },
  { key: 'logistics', label: 'Логистика', invert: true },
  { key: 'ad_spend', label: 'Реклама', invert: true },
  { key: 'cogs', label: 'Себестоимость', shortLabel: 'С/с', invert: true },
  { key: 'profit', label: 'Прибыль' },
]

// ── Delta badge ──────────────────────────────────────────────

function DeltaBadge({ value, invert }: { value: number; invert?: boolean }) {
  if (value === 0) return null
  const isPositive = value > 0
  // For costs: positive delta = bad (red), negative = good (green)
  const isGood = invert ? !isPositive : isPositive
  const Icon = isPositive ? TrendingUp : TrendingDown

  return (
    <span className={cn(
      'inline-flex items-center gap-0.5 text-[10px] font-medium leading-none',
      isGood ? 'text-emerald-400' : 'text-red-400',
    )}>
      <Icon className="h-2.5 w-2.5" />
      {isPositive ? '+' : ''}{value.toFixed(1)}%
    </span>
  )
}

// ── Main component ───────────────────────────────────────────

interface Props {
  products: ProductFinanceItem[]
  totals: {
    current: Record<string, number>
    previous: Record<string, number>
    delta_pct: Record<string, number>
  }
  marketplace: 'wildberries' | 'ozon'
}

type SortDir = 'asc' | 'desc'

export default function ProductFinanceTable({ products, totals, marketplace }: Props) {
  const [sortKey, setSortKey] = useState('revenue')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  const columns = marketplace === 'wildberries' ? WB_COLUMNS : OZON_COLUMNS

  const sorted = useMemo(() => {
    return [...products]
      .filter(p => !p.vendor_code.startsWith('__'))
      .sort((a, b) => {
        const av = a.current[sortKey] ?? 0
        const bv = b.current[sortKey] ?? 0
        return sortDir === 'desc' ? bv - av : av - bv
      })
  }, [products, sortKey, sortDir])

  // "Без привязки" row — aggregate of __unknown__ and __unmatched_ads__
  const unmatchedRow = useMemo(() => {
    const specials = products.filter(p => p.vendor_code.startsWith('__'))
    if (specials.length === 0) return null
    const cur: Record<string, number> = {}
    const prev: Record<string, number> = {}
    for (const col of columns) {
      cur[col.key] = specials.reduce((s, p) => s + (p.current[col.key] ?? 0), 0)
      prev[col.key] = specials.reduce((s, p) => s + (p.previous[col.key] ?? 0), 0)
    }
    return { cur, prev }
  }, [products, columns])

  const handleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const SortIcon = ({ active, dir }: { active: boolean; dir: SortDir }) => {
    if (!active) return <ArrowUpDown className="h-3 w-3 opacity-30" />
    return dir === 'desc'
      ? <ArrowDown className="h-3 w-3 text-[hsl(var(--primary))]" />
      : <ArrowUp className="h-3 w-3 text-[hsl(var(--primary))]" />
  }

  if (products.length === 0) {
    return (
      <div className="rounded-2xl border border-white/5 bg-[hsl(var(--card))] p-8 text-center text-sm text-white/40">
        <Package className="mx-auto mb-3 h-8 w-8 opacity-40" />
        Нет данных по товарам за выбранный период
      </div>
    )
  }

  // Margin color helper
  const marginColor = (profit: number, revenue: number) => {
    if (revenue <= 0) return 'text-white/40'
    const pct = (profit / revenue) * 100
    if (pct >= 20) return 'text-emerald-400'
    if (pct >= 5) return 'text-amber-400'
    return 'text-red-400'
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay: 0.2 }}
      className="rounded-2xl border border-white/5 bg-[hsl(var(--card))] overflow-hidden"
    >
      {/* Header */}
      <div className="px-6 py-4 border-b border-white/5">
        <h3 className="text-base font-semibold text-white">Детализация по товарам</h3>
        <p className="text-xs text-white/40 mt-0.5">P&L по каждому товару за выбранный период</p>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-white/5">
              <th className="sticky left-0 z-10 bg-[hsl(var(--card))] px-4 py-3 text-left text-xs font-medium text-white/50 min-w-[180px]">
                Товар
              </th>
              {columns.map(col => (
                <th
                  key={col.key}
                  className="px-3 py-3 text-right text-xs font-medium text-white/50 cursor-pointer hover:text-white/70 transition-colors whitespace-nowrap select-none"
                  onClick={() => handleSort(col.key)}
                >
                  <span className="inline-flex items-center gap-1 justify-end">
                    {col.shortLabel || col.label}
                    <SortIcon active={sortKey === col.key} dir={sortDir} />
                  </span>
                </th>
              ))}
              <th className="px-3 py-3 text-right text-xs font-medium text-white/50 whitespace-nowrap">
                Маржа
              </th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((product, idx) => {
              const rev = product.current.revenue || 0
              const profit = product.current.profit || 0
              const margin = rev > 0 ? (profit / rev * 100) : 0

              return (
                <tr
                  key={product.vendor_code}
                  className={cn(
                    'border-b border-white/[0.03] hover:bg-white/[0.02] transition-colors',
                    idx % 2 === 0 ? 'bg-transparent' : 'bg-white/[0.01]'
                  )}
                >
                  {/* Product name */}
                  <td className="sticky left-0 z-10 bg-[hsl(var(--card))] px-4 py-2.5">
                    <div className="text-xs font-medium text-white truncate max-w-[170px]" title={product.vendor_code}>
                      {product.vendor_code}
                    </div>
                    {product.nm_id ? (
                      <div className="text-[10px] text-white/30 mt-0.5">
                        {product.nm_id}
                      </div>
                    ) : null}
                  </td>

                  {/* Data columns */}
                  {columns.map(col => {
                    const val = product.current[col.key] ?? 0
                    const delta = product.delta_pct[col.key] ?? 0
                    const pctRev = product.pct_of_revenue[col.key]

                    return (
                      <td key={col.key} className="px-3 py-2.5 text-right">
                        <div className={cn(
                          'text-xs font-medium',
                          col.key === 'profit' ? marginColor(val, rev) : 'text-white',
                        )}>
                          {col.isCount ? val : formatMoney(val)}
                        </div>
                        <div className="flex items-center justify-end gap-1 mt-0.5">
                          {pctRev !== undefined && !col.isCount && col.key !== 'revenue' && (
                            <span className="text-[10px] text-white/25">{pctRev}%</span>
                          )}
                          <DeltaBadge value={delta} invert={col.invert} />
                        </div>
                      </td>
                    )
                  })}

                  {/* Margin */}
                  <td className="px-3 py-2.5 text-right">
                    <div className={cn('text-xs font-semibold', marginColor(profit, rev))}>
                      {margin.toFixed(1)}%
                    </div>
                  </td>
                </tr>
              )
            })}

            {/* Unmatched row */}
            {unmatchedRow && (
              <tr className="border-b border-white/[0.03] bg-white/[0.02]">
                <td className="sticky left-0 z-10 bg-[hsl(var(--card))] px-4 py-2.5">
                  <div className="text-xs text-white/40 italic">Без привязки к товару</div>
                </td>
                {columns.map(col => (
                  <td key={col.key} className="px-3 py-2.5 text-right">
                    <div className="text-xs text-white/40">
                      {col.isCount ? (unmatchedRow.cur[col.key] ?? 0) : formatMoney(unmatchedRow.cur[col.key] ?? 0)}
                    </div>
                  </td>
                ))}
                <td className="px-3 py-2.5" />
              </tr>
            )}
          </tbody>

          {/* Sticky totals footer */}
          <tfoot>
            <tr className="border-t-2 border-white/10 bg-white/[0.03]">
              <td className="sticky left-0 z-10 bg-[hsl(var(--card-muted,var(--card)))] px-4 py-3">
                <div className="text-xs font-semibold text-white">Итого</div>
              </td>
              {columns.map(col => {
                const val = totals.current[col.key] ?? 0
                const delta = totals.delta_pct[col.key] ?? 0
                const rev = totals.current.revenue || 1
                const pctRev = col.key !== 'revenue' && !col.isCount
                  ? (val / rev * 100).toFixed(1) : undefined

                return (
                  <td key={col.key} className="px-3 py-3 text-right">
                    <div className={cn(
                      'text-xs font-semibold',
                      col.key === 'profit'
                        ? marginColor(val, totals.current.revenue || 0)
                        : 'text-white',
                    )}>
                      {col.isCount ? val : formatMoney(val)}
                    </div>
                    <div className="flex items-center justify-end gap-1 mt-0.5">
                      {pctRev && <span className="text-[10px] text-white/25">{pctRev}%</span>}
                      <DeltaBadge value={delta} invert={col.invert} />
                    </div>
                  </td>
                )
              })}
              <td className="px-3 py-3 text-right">
                <div className={cn(
                  'text-xs font-bold',
                  marginColor(totals.current.profit ?? 0, totals.current.revenue ?? 0)
                )}>
                  {totals.current.revenue
                    ? ((totals.current.profit ?? 0) / totals.current.revenue * 100).toFixed(1)
                    : '0.0'}%
                </div>
              </td>
            </tr>
          </tfoot>
        </table>
      </div>
    </motion.div>
  )
}
