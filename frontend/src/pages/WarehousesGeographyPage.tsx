/**
 * Warehouses Geography — Sales geography by region/city.
 * WB: Shows sales by federal district + region, top products, search by SKU.
 * Ozon: redirects to /warehouses/analytics.
 */
import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { Navigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Globe,
  RefreshCw,
  AlertTriangle,
  ChevronRight,
  Search,
  MapPin,
  Package,
  TrendingUp,
  X,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBGeography,
  type WBGeographyResponse,
  type WBGeographyOkrug,
  type WBGeographyProduct,
} from '@/api/warehouses'

/* ── Helpers ── */
function fmt(v: number): string { return Math.round(v).toLocaleString('ru-RU') }
function fmtM(v: number): string { return Math.round(v).toLocaleString('ru-RU') + ' ₽' }

/* ── Period Options ── */
const PERIOD_OPTIONS = [
  { label: '7 дн', value: 7 },
  { label: '14 дн', value: 14 },
  { label: '30 дн', value: 30 },
  { label: '60 дн', value: 60 },
  { label: '90 дн', value: 90 },
]

/* ═══ KPI Summary ═══ */
function GeographyKpi({ data }: { data: WBGeographyResponse }) {
  const topOkrug = data.regions[0]
  const avgRevenuePerOrder = data.total_orders > 0 ? data.total_revenue / data.total_orders : 0

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
        <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">
          Всего заказов
        </div>
        <div className="text-2xl font-bold tabular-nums">{fmt(data.total_orders)}</div>
        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">за {data.period_days} дней</div>
      </div>
      <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
        <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">
          Выручка
        </div>
        <div className="text-2xl font-bold tabular-nums text-emerald-400">{fmtM(data.total_revenue)}</div>
        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">≈{fmtM(avgRevenuePerOrder)}/заказ</div>
      </div>
      <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
        <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">
          Округов
        </div>
        <div className="text-2xl font-bold tabular-nums">{data.regions.length}</div>
        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">с заказами</div>
      </div>
      <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
        <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">
          Топ округ
        </div>
        <div className="text-lg font-bold truncate">{topOkrug?.okrug?.replace(' федеральный округ', '') || '—'}</div>
        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
          {topOkrug ? `${topOkrug.share_pct}% заказов` : ''}
        </div>
      </div>
    </div>
  )
}

/* ═══ Okrug Distribution Bar ═══ */
function OkrugDistributionBar({ regions }: { regions: WBGeographyOkrug[] }) {
  const colors = [
    'bg-blue-500', 'bg-emerald-500', 'bg-violet-500', 'bg-amber-500',
    'bg-cyan-500', 'bg-rose-500', 'bg-indigo-500', 'bg-orange-500',
    'bg-teal-500', 'bg-pink-500',
  ]

  const totalOrders = regions.reduce((s, r) => s + r.orders, 0)
  if (totalOrders === 0) return null

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1 }}>
      <Card>
        <CardContent className="p-5">
          <h3 className="text-sm font-semibold text-[hsl(var(--muted-foreground))] mb-3">Распределение по округам</h3>
          <div className="flex rounded-lg overflow-hidden h-5">
            {regions.map((r, i) => (
              <div
                key={r.okrug}
                className={`${colors[i % colors.length]} relative group transition-all hover:brightness-110`}
                style={{ width: `${r.share_pct}%` }}
                title={`${r.okrug}: ${r.share_pct}%`}
              >
                {r.share_pct > 8 && (
                  <span className="absolute inset-0 flex items-center justify-center text-[9px] font-bold text-white">
                    {r.share_pct}%
                  </span>
                )}
              </div>
            ))}
          </div>
          <div className="flex flex-wrap gap-3 mt-3">
            {regions.map((r, i) => (
              <div key={r.okrug} className="flex items-center gap-1.5 text-[11px]">
                <span className={`w-2.5 h-2.5 rounded-sm ${colors[i % colors.length]}`} />
                <span className="text-[hsl(var(--muted-foreground))]">
                  {r.okrug.replace(' федеральный округ', '')} — {r.share_pct}%
                </span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══ Regions Table (expandable) ═══ */
function RegionsTable({
  regions,
  okrugTopProducts,
  onSelectProduct,
}: {
  regions: WBGeographyOkrug[]
  okrugTopProducts: Record<string, WBGeographyProduct[]>
  onSelectProduct: (nm_id: number) => void
}) {
  const [expandedOkrug, setExpandedOkrug] = useState<string | null>(null)

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.15 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
            <MapPin className="h-5 w-5 text-blue-400" />
            Регионы
          </h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            Нажмите для детализации по городам и товарам
          </span>
        </div>

        <div className="overflow-auto max-h-[600px]">
          {regions.map((okrug) => {
            const isExpanded = expandedOkrug === okrug.okrug
            const topProds = okrugTopProducts[okrug.okrug] || []

            return (
              <div key={okrug.okrug} className="border-b border-[hsl(var(--border)/0.15)]">
                {/* Okrug row */}
                <div
                  className={`flex items-center gap-4 px-6 py-4 cursor-pointer transition-colors ${
                    isExpanded ? 'bg-[hsl(var(--primary)/0.06)]' : 'hover:bg-[hsl(var(--muted)/0.08)]'
                  }`}
                  onClick={() => setExpandedOkrug(isExpanded ? null : okrug.okrug)}
                >
                  <motion.div animate={{ rotate: isExpanded ? 90 : 0 }} transition={{ duration: 0.15 }}>
                    <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                  </motion.div>
                  <div className="flex-1 min-w-0">
                    <div className="text-[14px] font-semibold text-[hsl(var(--foreground))]">
                      {okrug.okrug.replace(' федеральный округ', ' ФО')}
                    </div>
                    <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
                      {okrug.regions.length} регионов
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-[14px] font-bold tabular-nums">{fmt(okrug.orders)}</div>
                    <div className="text-[11px] text-[hsl(var(--muted-foreground))]">заказов</div>
                  </div>
                  <div className="text-right w-24">
                    <div className="text-[14px] font-bold tabular-nums text-emerald-400">{fmtM(okrug.revenue)}</div>
                    <div className="text-[11px] text-[hsl(var(--muted-foreground))]">выручка</div>
                  </div>
                  <div className="w-20 text-right">
                    <div className={`text-[14px] font-bold tabular-nums ${
                      okrug.share_pct > 20 ? 'text-blue-400' : 'text-[hsl(var(--foreground))]'
                    }`}>
                      {okrug.share_pct}%
                    </div>
                    <div className="text-[11px] text-[hsl(var(--muted-foreground))]">доля</div>
                  </div>
                </div>

                {/* Expanded detail */}
                <AnimatePresence>
                  {isExpanded && (
                    <motion.div
                      initial={{ height: 0, opacity: 0 }}
                      animate={{ height: 'auto', opacity: 1 }}
                      exit={{ height: 0, opacity: 0 }}
                      transition={{ duration: 0.2 }}
                      className="overflow-hidden"
                    >
                      <div className="bg-[hsl(var(--muted)/0.04)] border-t border-[hsl(var(--border)/0.2)] px-6 py-4">
                        <div className="grid grid-cols-1 lg:grid-cols-[1fr_300px] gap-6">
                          {/* Regions */}
                          <div>
                            <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-3 flex items-center gap-2">
                              <Globe className="h-4 w-4 text-blue-400" />
                              Города / регионы ({okrug.regions.length})
                            </h4>
                            <div className="space-y-1 max-h-[300px] overflow-y-auto">
                              {okrug.regions.map((region) => (
                                <div key={region.region} className="flex items-center gap-3 py-1.5 px-3 rounded-lg hover:bg-[hsl(var(--muted)/0.1)]">
                                  <div className="flex-1 min-w-0">
                                    <span className="text-[13px] font-medium truncate block">{region.region}</span>
                                  </div>
                                  <span className="text-[12px] tabular-nums font-semibold w-16 text-right">{fmt(region.orders)}</span>
                                  <span className="text-[12px] tabular-nums text-emerald-400 w-20 text-right">{fmtM(region.revenue)}</span>
                                  <span className="text-[11px] tabular-nums text-[hsl(var(--muted-foreground))] w-10 text-right">{region.share_pct}%</span>
                                </div>
                              ))}
                            </div>
                          </div>

                          {/* Top products for this okrug */}
                          {topProds.length > 0 && (
                            <div>
                              <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-3 flex items-center gap-2">
                                <Package className="h-4 w-4 text-emerald-400" />
                                Топ товары
                              </h4>
                              <div className="space-y-2">
                                {topProds.map((p, i) => (
                                  <div
                                    key={p.nm_id}
                                    className="flex items-center gap-3 p-2.5 rounded-xl bg-[hsl(var(--card))] border border-[hsl(var(--border)/0.2)] cursor-pointer hover:border-[hsl(var(--primary)/0.5)] transition-colors"
                                    onClick={(e) => { e.stopPropagation(); onSelectProduct(p.nm_id) }}
                                  >
                                    <span className="text-[11px] font-bold text-[hsl(var(--muted-foreground))] w-4">{i + 1}</span>
                                    <div className="flex-1 min-w-0">
                                      <div className="text-[12px] font-medium truncate">{p.name || p.vendor_code}</div>
                                      <div className="text-[10px] text-[hsl(var(--muted-foreground))]">{p.vendor_code}</div>
                                    </div>
                                    <div className="text-right shrink-0">
                                      <div className="text-[12px] font-bold tabular-nums">{fmt(p.orders)}</div>
                                      <div className="text-[10px] text-emerald-400 tabular-nums">{fmtM(p.revenue)}</div>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>
            )
          })}
        </div>
      </div>
    </motion.div>
  )
}

/* ═══ Top Products Table ═══ */
function TopProductsTable({
  products,
  onSelectProduct,
}: {
  products: WBGeographyProduct[]
  onSelectProduct: (nm_id: number) => void
}) {
  if (products.length === 0) return null

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-emerald-400" />
            Топ товары по географии
          </h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            Нажмите для просмотра географии конкретного товара
          </span>
        </div>

        <div className="overflow-auto max-h-[500px]">
          <table className="w-full border-collapse">
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-3 py-2.5 w-10 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">#</th>
                <th className="px-3 py-2.5 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Товар</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Заказов</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Выручка</th>
                <th className="px-3 py-2.5 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Округов</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Доля</th>
              </tr>
            </thead>
            <tbody>
              {products.slice(0, 30).map((p, idx) => (
                <tr
                  key={p.nm_id}
                  className={`border-b border-[hsl(var(--border)/0.15)] cursor-pointer transition-colors ${
                    idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.04)]'
                  } hover:bg-[hsl(var(--primary)/0.06)]`}
                  onClick={() => onSelectProduct(p.nm_id)}
                >
                  <td className="px-3 py-2.5 text-center text-[12px] text-[hsl(var(--muted-foreground))]">{idx + 1}</td>
                  <td className="px-3 py-2.5">
                    <div className="text-[13px] font-medium truncate max-w-[250px]">{p.name || p.vendor_code}</div>
                    <div className="text-[11px] text-[hsl(var(--muted-foreground))]">{p.vendor_code}</div>
                  </td>
                  <td className="px-3 py-2.5 text-right tabular-nums text-[13px] font-semibold">{fmt(p.orders)}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums text-[13px] text-emerald-400">{fmtM(p.revenue)}</td>
                  <td className="px-3 py-2.5 text-center tabular-nums text-[13px]">{p.okrug_count}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums text-[13px] font-semibold">{p.share_pct}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══ SKU Filter Header ═══ */
function SkuFilterHeader({
  skuFilter,
  onClear,
}: {
  skuFilter: { nm_id: number; vendor_code: string; name: string }
  onClear: () => void
}) {
  return (
    <motion.div initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }} transition={{ duration: 0.2 }}>
      <Card className="border-blue-500/30 bg-blue-500/5">
        <CardContent className="p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-blue-500/15">
              <Package className="h-4 w-4 text-blue-400" />
            </div>
            <div>
              <div className="text-[13px] font-semibold text-[hsl(var(--foreground))]">
                География товара: <span className="text-blue-400">{skuFilter.name || skuFilter.vendor_code}</span>
              </div>
              <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
                {skuFilter.vendor_code} • nm_id: {skuFilter.nm_id}
              </div>
            </div>
          </div>
          <button
            onClick={onClear}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[12px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all"
          >
            <X className="h-3.5 w-3.5" />
            Сбросить
          </button>
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══ Skeleton ═══ */
function GeographySkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-[90px] rounded-2xl" />
        ))}
      </div>
      <Skeleton className="h-[60px] rounded-2xl" />
      <Skeleton className="h-[400px] rounded-2xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function WarehousesGeographyPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isWB = currentShop?.marketplace === 'wildberries'
  const isOzon = currentShop?.marketplace === 'ozon'

  const [data, setData] = useState<WBGeographyResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState(30)
  const [selectedNmId, setSelectedNmId] = useState<number | undefined>(undefined)
  const [searchQuery, setSearchQuery] = useState('')

  const fetchData = useCallback(async (nmId?: number) => {
    if (!currentShop || !isWB) return
    setLoading(true)
    setError(null)
    try {
      const result = await getWBGeography({
        shop_id: currentShop.id,
        period,
        nm_id: nmId,
      })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [currentShop, isWB, period])

  useEffect(() => { if (isWB) fetchData(selectedNmId) }, [fetchData, isWB, selectedNmId])

  const handleSelectProduct = useCallback((nm_id: number) => {
    setSelectedNmId(nm_id)
  }, [])

  const handleClearFilter = useCallback(() => {
    setSelectedNmId(undefined)
    setSearchQuery('')
  }, [])

  const handleSearch = useCallback((e: React.FormEvent) => {
    e.preventDefault()
    const q = searchQuery.trim()
    if (!q) {
      handleClearFilter()
      return
    }
    // Try to parse as nm_id
    const num = parseInt(q, 10)
    if (!isNaN(num) && num > 0) {
      setSelectedNmId(num)
    }
  }, [searchQuery, handleClearFilter])

  // Filter regions by search query (when no nm_id filter)
  const filteredRegions = useMemo(() => {
    if (!data || selectedNmId) return data?.regions || []
    if (!searchQuery.trim()) return data.regions
    const q = searchQuery.toLowerCase()
    return data.regions
      .map((okrug) => ({
        ...okrug,
        regions: okrug.regions.filter((r) =>
          r.region.toLowerCase().includes(q)
        ),
      }))
      .filter((okrug) =>
        okrug.okrug.toLowerCase().includes(q) || okrug.regions.length > 0
      )
  }, [data, searchQuery, selectedNmId])

  if (isOzon) return <Navigate to="/warehouses/analytics" replace />

  const periodSelCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[13px] font-medium transition-all cursor-pointer ${
      active
        ? 'bg-[hsl(var(--primary))] text-white shadow-md'
        : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
    }`

  return (
    <div className="space-y-6 pb-10">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">География продаж</h1>
          <p className="text-[hsl(var(--muted-foreground))] mt-1">
            Продажи по округам, регионам и городам — поиск по товарам
          </p>
        </div>
        <button
          onClick={() => fetchData(selectedNmId)}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          Обновить
        </button>
      </div>

      {/* Controls: Period + Search */}
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between flex-wrap gap-3">
              <div className="flex items-center gap-3">
                <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Период</p>
                <div className="flex gap-1">
                  {PERIOD_OPTIONS.map(o => (
                    <button key={o.value} className={periodSelCls(period === o.value)} onClick={() => setPeriod(o.value)}>
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>
              <form onSubmit={handleSearch} className="flex items-center gap-2">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    placeholder="Артикул, nm_id или город..."
                    className="pl-9 pr-4 py-2 w-[260px] rounded-xl text-[13px] bg-[hsl(var(--muted)/0.15)] border border-[hsl(var(--border)/0.5)] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.5)] focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary)/0.3)] transition-all"
                  />
                </div>
                <button
                  type="submit"
                  className="px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--primary))] text-white hover:opacity-90 transition-all"
                >
                  Найти
                </button>
                {(selectedNmId || searchQuery) && (
                  <button
                    type="button"
                    onClick={handleClearFilter}
                    className="p-2 rounded-lg text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.3)] transition-all"
                  >
                    <X className="h-4 w-4" />
                  </button>
                )}
              </form>
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {loading && !data ? (
        <GeographySkeleton />
      ) : error ? (
        <Card>
          <CardContent className="p-10 text-center">
            <AlertTriangle className="h-10 w-10 mx-auto text-red-400 mb-3" />
            <p className="text-lg font-medium text-red-400">{error}</p>
            <button onClick={() => fetchData(selectedNmId)} className="mt-4 px-4 py-2 rounded-xl bg-[hsl(var(--primary))] text-white text-sm font-medium">
              Попробовать снова
            </button>
          </CardContent>
        </Card>
      ) : data ? (
        <>
          {/* SKU filter header */}
          {data.sku_filter && (
            <SkuFilterHeader skuFilter={data.sku_filter} onClear={handleClearFilter} />
          )}

          {/* KPI */}
          <GeographyKpi data={data} />

          {/* Distribution bar */}
          <OkrugDistributionBar regions={data.regions} />

          {/* Regions table */}
          <RegionsTable
            regions={filteredRegions}
            okrugTopProducts={data.okrug_top_products}
            onSelectProduct={handleSelectProduct}
          />

          {/* Top products (only when no SKU filter) */}
          {!selectedNmId && data.top_products.length > 0 && (
            <TopProductsTable products={data.top_products} onSelectProduct={handleSelectProduct} />
          )}
        </>
      ) : null}
    </div>
  )
}
