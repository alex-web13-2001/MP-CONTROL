/**
 * ProductFinanceTable — Product-level P&L breakdown.
 *
 * Sortable table showing per-product financials:
 * revenue, logistics, storage, ads, COGS, profit —
 * with delta % vs previous period and % of revenue.
 *
 * Unified style matching ABC/XYZ and Sales tables:
 * - rounded-2xl card container with title bar
 * - max-h-[600px] scrollable area
 * - sticky header (vertical) + sticky first column (horizontal)
 * - sticky ИТОГО footer
 * - zebra striping
 */
import { useState, useMemo } from 'react'
import { motion } from 'framer-motion'
import { TrendingUp, TrendingDown, Package } from 'lucide-react'
import type { ProductFinanceItem } from '@/api/finances'

// ── Sticky cell styles ────────────────────────────────────────
const stickyCol: React.CSSProperties = {
  position: 'sticky',
  left: 0,
  boxShadow: '2px 0 8px -2px rgba(0,0,0,0.15)',
}

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
  { key: 'deductions', label: 'Удержания', invert: true },
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

  if (products.length === 0) {
    return (
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-8 text-center text-sm text-[hsl(var(--muted-foreground))]">
        <Package className="mx-auto mb-3 h-8 w-8 opacity-40" />
        Нет данных по товарам за выбранный период
      </div>
    )
  }

  // Margin color helper
  const marginColor = (profit: number, revenue: number) => {
    if (revenue <= 0) return 'text-[hsl(var(--muted-foreground))]'
    const pct = (profit / revenue) * 100
    if (pct >= 20) return 'text-emerald-400'
    if (pct >= 5) return 'text-amber-400'
    return 'text-red-400'
  }

  const thCls = "px-4 py-3.5 text-right text-[13px] font-semibold whitespace-nowrap select-none cursor-pointer transition-colors"
  const tdCls = "px-4 py-3 text-right whitespace-nowrap"

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay: 0.2 }}
      className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden"
    >
      {/* Title bar */}
      <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
        <div>
          <h3 className="text-xl font-bold text-[hsl(var(--foreground))]">Детализация по товарам</h3>
          <p className="text-sm text-[hsl(var(--muted-foreground))] mt-0.5">P&L по каждому товару за выбранный период</p>
        </div>
        <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">
          {sorted.length} {sorted.length === 1 ? 'товар' : sorted.length < 5 ? 'товара' : 'товаров'}
        </span>
      </div>

      {/* Scrollable table — sticky header (vertical) + sticky first column (horizontal) + sticky footer */}
      <div className="overflow-auto max-h-[600px] relative">
        <table className="w-full border-collapse" style={{ minWidth: 900 }}>
          <thead className="sticky top-0 z-20">
            <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
              <th
                className="px-4 py-3.5 text-left text-[13px] font-semibold text-[hsl(var(--muted-foreground))] w-[250px] min-w-[250px] max-w-[250px] bg-[hsl(var(--card))]"
                style={{ ...stickyCol, zIndex: 30 }}
              >
                Товар
              </th>
              {columns.map(col => (
                <th
                  key={col.key}
                  className={cn(
                    thCls,
                    sortKey === col.key
                      ? 'text-[hsl(var(--foreground))]'
                      : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
                  )}
                  onClick={() => handleSort(col.key)}
                >
                  <span className="inline-flex items-center gap-1 justify-end">
                    {col.shortLabel || col.label}
                    {sortKey === col.key && (
                      <span className="text-[11px] text-[hsl(var(--primary))]">{sortDir === 'desc' ? '▼' : '▲'}</span>
                    )}
                  </span>
                </th>
              ))}
              <th className="px-4 py-3.5 text-right text-[13px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">
                Маржа
              </th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((product, idx) => {
              const rev = product.current.revenue || 0
              const profit = product.current.profit || 0
              const margin = rev > 0 ? (profit / rev * 100) : 0
              const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.06)]'

              return (
                <tr
                  key={product.vendor_code}
                  className={`border-b border-[hsl(var(--border)/0.2)] transition-colors ${rowBg} hover:bg-[hsl(var(--muted)/0.2)] group`}
                >
                  {/* Product name — sticky left */}
                  <td
                    className={`px-4 py-3 w-[250px] min-w-[250px] max-w-[250px] ${rowBg} group-hover:bg-[hsl(var(--muted)/0.2)]`}
                    style={{ ...stickyCol, zIndex: 10 }}
                  >
                    <div className="flex items-center gap-2.5">
                      <div className="w-9 h-9 rounded-lg bg-[hsl(var(--muted)/0.3)] shrink-0 flex items-center justify-center text-sm">📦</div>
                      <div className="min-w-0 flex-1">
                        <div className="text-[13px] font-medium text-[hsl(var(--foreground))] truncate" title={product.name || product.vendor_code}>
                          {product.name || product.vendor_code}
                        </div>
                        {product.name && product.name !== product.vendor_code ? (
                          <div className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-60 truncate" title={product.vendor_code}>
                            {product.vendor_code}
                          </div>
                        ) : product.nm_id ? (
                          <div className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-60 truncate" title={product.nm_id.toString()}>
                            {product.nm_id}
                          </div>
                        ) : null}
                      </div>
                    </div>
                  </td>

                  {/* Data columns */}
                  {columns.map(col => {
                    const val = product.current[col.key] ?? 0
                    const delta = product.delta_pct[col.key] ?? 0
                    const pctRev = product.pct_of_revenue[col.key]

                    return (
                      <td key={col.key} className={tdCls}>
                        <div className={cn(
                          'text-[13px] font-medium tabular-nums',
                          col.key === 'profit' ? marginColor(val, rev) : 'text-[hsl(var(--foreground))]',
                        )}>
                          {col.isCount ? val : formatMoney(val)}
                        </div>
                        <div className="flex items-center justify-end gap-1 mt-0.5">
                          {pctRev !== undefined && !col.isCount && col.key !== 'revenue' && (
                            <span className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-50">{pctRev}%</span>
                          )}
                          <DeltaBadge value={delta} invert={col.invert} />
                        </div>
                      </td>
                    )
                  })}

                  {/* Margin */}
                  <td className={tdCls}>
                    <div className={cn('text-[13px] font-semibold tabular-nums', marginColor(profit, rev))}>
                      {margin.toFixed(1)}%
                    </div>
                  </td>
                </tr>
              )
            })}

            {/* Unmatched row */}
            {unmatchedRow && (
              <tr className="border-b border-[hsl(var(--border)/0.2)] bg-[hsl(var(--muted)/0.04)]">
                <td
                  className="px-4 py-3 bg-[hsl(var(--muted)/0.04)] w-[250px] min-w-[250px] max-w-[250px]"
                  style={{ ...stickyCol, zIndex: 10 }}
                >
                  <div className="text-[13px] text-[hsl(var(--muted-foreground))] italic">Без привязки к товару</div>
                </td>
                {columns.map(col => (
                  <td key={col.key} className={tdCls}>
                    <div className="text-[13px] text-[hsl(var(--muted-foreground))] tabular-nums">
                      {col.isCount ? (unmatchedRow.cur[col.key] ?? 0) : formatMoney(unmatchedRow.cur[col.key] ?? 0)}
                    </div>
                  </td>
                ))}
                <td className={tdCls} />
              </tr>
            )}
          </tbody>

          {/* Sticky ИТОГО footer */}
          <tfoot className="sticky bottom-0 z-20">
            <tr className="border-t-2 border-[hsl(var(--border))] bg-[hsl(var(--card))]">
              <td
                className="px-4 py-4 bg-[hsl(var(--card))] w-[250px] min-w-[250px] max-w-[250px]"
                style={{ ...stickyCol, zIndex: 30 }}
              >
                <div className="text-[14px] font-bold text-[hsl(var(--foreground))]">Итого</div>
              </td>
              {columns.map(col => {
                const val = totals.current[col.key] ?? 0
                const delta = totals.delta_pct[col.key] ?? 0
                const rev = totals.current.revenue || 1
                const pctRev = col.key !== 'revenue' && !col.isCount
                  ? (val / rev * 100).toFixed(1) : undefined

                return (
                  <td key={col.key} className="px-4 py-4 text-right whitespace-nowrap">
                    <div className={cn(
                      'text-[13px] font-bold tabular-nums',
                      col.key === 'profit'
                        ? marginColor(val, totals.current.revenue || 0)
                        : 'text-[hsl(var(--foreground))]',
                    )}>
                      {col.isCount ? val : formatMoney(val)}
                    </div>
                    <div className="flex items-center justify-end gap-1 mt-0.5">
                      {pctRev && <span className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-50">{pctRev}%</span>}
                      <DeltaBadge value={delta} invert={col.invert} />
                    </div>
                  </td>
                )
              })}
              <td className="px-4 py-4 text-right whitespace-nowrap">
                <div className={cn(
                  'text-[13px] font-bold tabular-nums',
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
