/**
 * ProductFinanceTable — Product-level P&L breakdown.
 *
 * Redesigned to match the Products page style:
 * - Product photo (3:4), name, vendor_code, SKU in first column
 * - Sticky header + sticky first column + sticky ИТОГО footer
 * - Clean rounded card with scrollable area
 */
import { useState, useMemo } from 'react'
import { motion } from 'framer-motion'
import { TrendingUp, TrendingDown, Package } from 'lucide-react'
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
  tooltip?: string
  invert?: boolean  // true = lower is better (costs)
  isCount?: boolean
}

const WB_COLUMNS: Column[] = [
  { key: 'sales', label: 'Продажи', isCount: true, tooltip: 'Количество проданных единиц за период' },
  { key: 'revenue', label: 'Выручка', tooltip: 'Цена из ЛК × кол-во проданных штук' },
  { key: 'logistics', label: 'Логистика', invert: true, tooltip: 'Расходы на доставку товаров' },
  { key: 'storage', label: 'Хранение', invert: true, tooltip: 'Расходы на хранение на складе МП' },
  { key: 'deductions', label: 'Удержания', invert: true, tooltip: 'Удержания маркетплейса (привязанные к товару)' },
  { key: 'ad_spend', label: 'Реклама', invert: true, tooltip: 'Рекламные расходы за период' },
  { key: 'cogs', label: 'С/с', shortLabel: 'С/с', invert: true, tooltip: 'Себестоимость + упаковка × кол-во' },
  { key: 'profit', label: 'Прибыль', tooltip: 'Выплата − Услуги МП − С/с − Реклама' },
]

const OZON_COLUMNS: Column[] = [
  { key: 'sales', label: 'Продажи', isCount: true },
  { key: 'revenue', label: 'Выручка' },
  { key: 'commission', label: 'Комиссия', invert: true },
  { key: 'logistics', label: 'Логистика', invert: true },
  { key: 'payout', label: 'Нетто', shortLabel: 'Нетто' },
  { key: 'ad_spend', label: 'Реклама', invert: true },
  { key: 'cogs', label: 'С/с', shortLabel: 'С/с', invert: true },
  { key: 'profit', label: 'Прибыль' },
]

// ── Delta badge ──────────────────────────────────────────────

function DeltaBadge({ value, invert }: { value: number; invert?: boolean }) {
  if (value === 0) return null
  const isPositive = value > 0
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
  const [hoverImg, setHoverImg] = useState<{ url: string; x: number; y: number } | null>(null)

  const isWB = marketplace === 'wildberries'
  const columns = isWB ? WB_COLUMNS : OZON_COLUMNS

  const sorted = useMemo(() => {
    return [...products]
      .filter(p => !p.vendor_code.startsWith('__'))
      .sort((a, b) => {
        const av = a.current[sortKey] ?? 0
        const bv = b.current[sortKey] ?? 0
        return sortDir === 'desc' ? bv - av : av - bv
      })
  }, [products, sortKey, sortDir])

  // "Без привязки" row
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

  const marginColor = (profit: number, revenue: number) => {
    if (revenue <= 0) return 'text-[hsl(var(--muted-foreground))]'
    const pct = (profit / revenue) * 100
    if (pct >= 20) return 'text-emerald-400'
    if (pct >= 5) return 'text-amber-400'
    return 'text-red-400'
  }

  /** Get image for product */
  const getProductImage = (product: ProductFinanceItem): string => {
    if (product.image_url) return product.image_url
    return ''
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay: 0.2 }}
      className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden"
    >
      {/* Hover image preview */}
      {hoverImg && (
        <div
          className="fixed z-[9999] pointer-events-none"
          style={{ left: hoverImg.x, top: hoverImg.y }}
        >
          <img
            src={hoverImg.url}
            alt=""
            className="h-[240px] w-[180px] rounded-xl object-cover shadow-2xl border border-[hsl(var(--border))]"
          />
        </div>
      )}

      {/* Scrollable table */}
      <div className="overflow-x-auto overflow-y-auto max-h-[calc(100vh-360px)]">
        <table className="w-full" style={{ borderCollapse: 'collapse', minWidth: 960 }}>
          <thead className="sticky top-0 z-30" style={{ boxShadow: '0 1px 0 hsl(var(--border))' }}>
            <tr className="bg-[hsl(var(--card))]">
              {/* Product column header */}
              <th
                className="sticky left-0 z-40 w-[280px] bg-[hsl(var(--card))] pl-4 pr-2 py-2.5 text-left text-[12px] font-medium text-[hsl(var(--muted-foreground))]"
              >
                Товар
              </th>
              {columns.map(col => (
                <th
                  key={col.key}
                  className={cn(
                    'px-2 py-2.5 text-right text-[12px] font-medium whitespace-nowrap cursor-pointer transition-colors',
                    sortKey === col.key
                      ? 'text-[hsl(var(--primary))]'
                      : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
                  )}
                  onClick={() => handleSort(col.key)}
                >
                  <div className="flex items-center justify-end gap-1">
                    <span>{col.shortLabel || col.label}</span>
                    {sortKey === col.key && (
                      <span className="text-[11px]">{sortDir === 'desc' ? '↓' : '↑'}</span>
                    )}
                    {col.tooltip && (
                      <span className="relative group/tip">
                        <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                        <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[200px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">
                          {col.tooltip}
                        </span>
                      </span>
                    )}
                  </div>
                </th>
              ))}
              <th className="px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))] whitespace-nowrap">
                Маржа
              </th>
            </tr>

            {/* ── ИТОГО row in header (matching Products page) ── */}
            {(() => {
              const t = totals
              const tRev = t.current.revenue || 1
              const tProfit = t.current.profit ?? 0
              const tMargin = t.current.revenue > 0 ? (tProfit / t.current.revenue * 100) : 0
              return (
                <tr className="border-b border-[hsl(var(--border)/0.5)] bg-[hsl(var(--card))]">
                  <td className="sticky left-0 z-40 bg-[hsl(var(--card))] pl-4 pr-2 py-2">
                    <span className="text-[12px] font-bold text-[hsl(var(--foreground)/0.6)]">
                      Σ {sorted.length} товаров
                    </span>
                  </td>
                  {columns.map(col => {
                    const val = t.current[col.key] ?? 0
                    const delta = t.delta_pct[col.key] ?? 0
                    return (
                      <td key={col.key} className="px-2 py-2 text-right">
                        <p className={cn(
                          'text-[13px] font-bold tabular-nums',
                          col.key === 'profit' ? marginColor(val, tRev) : '',
                        )}>
                          {col.isCount ? val : formatMoney(val)}
                        </p>
                        {delta !== 0 && <DeltaBadge value={delta} invert={col.invert} />}
                      </td>
                    )
                  })}
                  <td className="px-2 py-2 text-right">
                    <p className={cn('text-[13px] font-bold tabular-nums', marginColor(tProfit, t.current.revenue || 0))}>
                      {tMargin.toFixed(1)}%
                    </p>
                  </td>
                </tr>
              )
            })()}
          </thead>

          <tbody>
            {sorted.map((product) => {
              const rev = product.current.revenue || 0
              const profit = product.current.profit || 0
              const margin = rev > 0 ? (profit / rev * 100) : 0
              const imgUrl = getProductImage(product)
              const displayName = product.name || product.vendor_code

              return (
                <tr
                  key={product.vendor_code}
                  className="border-b border-[hsl(var(--border)/0.15)] hover:bg-white/[0.025] group transition-colors"
                >
                  {/* ── Product cell (photo + name + vendor_code + SKU) ── */}
                  <td className="sticky left-0 z-10 bg-[hsl(var(--card))] group-hover:bg-[hsl(var(--card))] pl-4 pr-2 py-3.5 transition-colors">
                    <div className="flex items-center gap-3">
                      {/* Photo 3:4 */}
                      {imgUrl ? (
                        <div
                          className="relative shrink-0 cursor-pointer"
                          onMouseEnter={(e) => {
                            const r = e.currentTarget.getBoundingClientRect()
                            setHoverImg({ url: imgUrl, x: r.right + 12, y: r.top - 40 })
                          }}
                          onMouseLeave={() => setHoverImg(null)}
                        >
                          <img
                            src={imgUrl}
                            alt=""
                            className="h-[56px] w-[42px] rounded-lg object-cover bg-[hsl(var(--muted)/0.1)]"
                            loading="lazy"
                          />
                        </div>
                      ) : (
                        <div className="flex h-[56px] w-[42px] shrink-0 items-center justify-center rounded-lg bg-[hsl(var(--muted)/0.1)]">
                          <Package className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.2)]" />
                        </div>
                      )}
                      {/* Info */}
                      <div className="min-w-0">
                        <p className="text-[13px] font-medium leading-snug line-clamp-2" title={displayName}>
                          {displayName}
                        </p>
                        <div className="mt-0.5 flex items-center gap-1.5 whitespace-nowrap">
                          {product.name && product.name !== product.vendor_code && (
                            <span className="text-[13px] font-semibold text-[hsl(var(--foreground)/0.75)] font-mono tracking-wide uppercase">
                              {product.vendor_code}
                            </span>
                          )}
                          {product.nm_id && product.nm_id > 0 && (
                            <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.45)]">
                              SKU {product.nm_id}
                            </span>
                          )}
                        </div>
                      </div>
                    </div>
                  </td>

                  {/* Data columns */}
                  {columns.map(col => {
                    const val = product.current[col.key] ?? 0
                    const delta = product.delta_pct[col.key] ?? 0
                    const pctRev = product.pct_of_revenue[col.key]

                    return (
                      <td key={col.key} className="px-2 py-3 text-right whitespace-nowrap">
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
                  <td className="px-2 py-3 text-right whitespace-nowrap">
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
                  className="sticky left-0 z-10 bg-[hsl(var(--muted)/0.04)] pl-4 pr-2 py-3"
                >
                  <div className="flex items-center gap-3">
                    <div className="flex h-[56px] w-[42px] shrink-0 items-center justify-center rounded-lg bg-[hsl(var(--muted)/0.08)]">
                      <Package className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.2)]" />
                    </div>
                    <div className="text-[13px] text-[hsl(var(--muted-foreground))] italic">
                      Без привязки к товару
                    </div>
                  </div>
                </td>
                {columns.map(col => (
                  <td key={col.key} className="px-2 py-3 text-right whitespace-nowrap">
                    <div className="text-[13px] text-[hsl(var(--muted-foreground))] tabular-nums">
                      {col.isCount ? (unmatchedRow.cur[col.key] ?? 0) : formatMoney(unmatchedRow.cur[col.key] ?? 0)}
                    </div>
                  </td>
                ))}
                <td className="px-2 py-3" />
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </motion.div>
  )
}
