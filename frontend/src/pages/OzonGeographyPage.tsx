/**
 * OzonGeographyPage — Sales geography for Ozon shops.
 * Clusters → Cities (drill-down) → Products.
 * Mirrors WB Geography UX but uses Ozon data: cluster_to + city.
 */
import { useState, useEffect, useCallback, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Globe,
  RefreshCw,
  AlertTriangle,
  MapPin,
  ChevronRight,
  ChevronDown,
  X,
  Search,
  ShoppingCart,
  DollarSign,
  BarChart3,
  Activity,
  Package,
  Loader2,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getOzonGeography,
  getOzonGeographyCityProducts,
  searchOzonGeographyProducts,
  type OzonGeographyResponse,
  type OzonGeographyCluster,
  type OzonGeographyProduct,
  type OzonGeographySkuInfo,
  type OzonCityProductsResponse,
} from '@/api/warehouses'

/* ── Helpers ── */
function safeNum(v: unknown): number { return typeof v === 'number' && isFinite(v) ? v : Number(v) || 0 }
function fmt(v: unknown): string { return Math.round(safeNum(v)).toLocaleString('ru-RU') }
function fmtM(v: unknown): string { return Math.round(safeNum(v)).toLocaleString('ru-RU') + ' ₽' }

/* ── Period Options ── */
const PERIOD_OPTIONS = [
  { label: '7 дн', value: 7 },
  { label: '14 дн', value: 14 },
  { label: '30 дн', value: 30 },
  { label: '60 дн', value: 60 },
  { label: '90 дн', value: 90 },
]

/* ── Stability badge ── */
function StabilityBadge({ pct, compact }: { pct: number; compact?: boolean }) {
  const safePct = typeof pct === 'number' && isFinite(pct) ? pct : 0
  const dotColor = safePct >= 90 ? 'bg-emerald-400' : safePct >= 50 ? 'bg-amber-400' : 'bg-red-400'
  const textColor = safePct >= 90 ? 'text-emerald-400' : safePct >= 50 ? 'text-amber-400' : 'text-red-400'
  const label = safePct >= 90 ? 'Стабильный' : safePct >= 50 ? 'Средний' : 'Нестабильный'

  if (compact) {
    return (
      <span className={`inline-flex items-center gap-1.5 text-[12px] font-semibold tabular-nums ${textColor}`} title={`Стабильность спроса: ${safePct.toFixed(0)}% — ${label}`}>
        <span className={`w-2 h-2 rounded-full shrink-0 ${dotColor}`} />
        {safePct.toFixed(0)}%
      </span>
    )
  }

  const bgColor = safePct >= 90 ? 'bg-emerald-500/15' : safePct >= 50 ? 'bg-amber-500/15' : 'bg-red-500/15'
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold whitespace-nowrap ${bgColor} ${textColor}`}>
      <Activity className="h-3 w-3" />
      {safePct.toFixed(0)}% · {label}
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   Product Combobox — multi-select with autocomplete (Ozon)
   ═══════════════════════════════════════════════════════════ */

function OzonProductCombobox({
  shopId,
  selected,
  onSelect,
  onRemove,
  onClear,
}: {
  shopId: number
  selected: OzonGeographySkuInfo[]
  onSelect: (p: OzonGeographySkuInfo) => void
  onRemove: (sku: number) => void
  onClear: () => void
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [options, setOptions] = useState<OzonGeographySkuInfo[]>([])
  const [loading, setLoading] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout>>()

  useEffect(() => {
    clearTimeout(timerRef.current)
    timerRef.current = setTimeout(async () => {
      setLoading(true)
      try {
        const res = await searchOzonGeographyProducts({ shop_id: shopId, q: search })
        setOptions(res.products)
      } catch {
        setOptions([])
      } finally {
        setLoading(false)
      }
    }, 300)
    return () => clearTimeout(timerRef.current)
  }, [search, shopId])

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const selectedIds = new Set(selected.map(s => s.sku))
  const filteredOptions = options.filter(o => !selectedIds.has(o.sku))

  return (
    <div ref={dropdownRef} className="relative flex-1 min-w-0">
      <div
        className="flex flex-wrap items-center gap-1.5 px-3 py-2 min-h-[44px] rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))] cursor-text transition-all focus-within:ring-2 focus-within:ring-[hsl(var(--primary)/0.3)]"
        onClick={() => { inputRef.current?.focus(); setOpen(true) }}
      >
        <Search className="h-4 w-4 shrink-0 text-[hsl(var(--muted-foreground)/0.5)]" />
        {selected.map(s => (
          <span
            key={s.sku}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded-lg bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))] text-[12px] font-medium max-w-[200px] truncate"
          >
            {s.offer_id || `SKU ${s.sku}`}
            <X
              className="h-3 w-3 cursor-pointer hover:text-red-400 shrink-0"
              onClick={(e) => { e.stopPropagation(); onRemove(s.sku) }}
            />
          </span>
        ))}
        <input
          ref={inputRef}
          type="text"
          value={search}
          onChange={e => { setSearch(e.target.value); setOpen(true) }}
          onFocus={() => setOpen(true)}
          placeholder={selected.length === 0 ? 'Поиск по артикулу, названию или SKU...' : 'Ещё...'}
          className="flex-1 min-w-[80px] bg-transparent text-[13px] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.4)] outline-none"
        />
        {selected.length > 0 && (
          <button
            onClick={(e) => { e.stopPropagation(); onClear() }}
            className="shrink-0 text-[hsl(var(--muted-foreground)/0.5)] hover:text-red-400 transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        )}
      </div>

      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0, y: -4 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -4 }}
            transition={{ duration: 0.15 }}
            className="absolute z-50 mt-1 w-full max-h-[280px] overflow-auto rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-xl"
          >
            {loading ? (
              <div className="flex items-center justify-center py-6">
                <Loader2 className="h-5 w-5 animate-spin text-[hsl(var(--muted-foreground))]" />
              </div>
            ) : filteredOptions.length > 0 ? (
              filteredOptions.map(opt => (
                <button
                  key={opt.sku}
                  onClick={() => { onSelect(opt); setSearch(''); }}
                  className="w-full text-left px-4 py-2.5 hover:bg-[hsl(var(--muted)/0.1)] transition-colors border-b border-[hsl(var(--border)/0.2)] last:border-0"
                >
                  <div className="text-[13px] font-medium text-[hsl(var(--foreground))] line-clamp-1">
                    {opt.name || `Товар SKU ${opt.sku}`}
                  </div>
                  <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
                    {opt.offer_id && <span className="font-semibold">{opt.offer_id}</span>}
                    {opt.offer_id && ' · '}
                    SKU: {opt.sku}
                  </div>
                </button>
              ))
            ) : (
              <div className="px-4 py-6 text-center text-[13px] text-[hsl(var(--muted-foreground))]">
                {search ? 'Ничего не найдено' : 'Нет товаров'}
              </div>
            )}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   KPI Cards
   ═══════════════════════════════════════════════════════════ */

function GeographyKpiCards({ data }: { data: OzonGeographyResponse }) {
  const kpis = [
    { label: 'Всего заказов', value: fmt(data.total_orders), icon: ShoppingCart, color: 'text-blue-400' },
    { label: 'Выручка', value: fmtM(data.total_revenue), icon: DollarSign, color: 'text-emerald-400' },
    { label: 'Средний чек', value: fmtM(data.avg_check), icon: BarChart3, color: 'text-purple-400' },
    { label: 'Охват', value: `${data.total_clusters} кл. · ${data.total_cities} гор.`, icon: Globe, color: 'text-cyan-400' },
  ]
  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {kpis.map((k) => (
          <div key={k.label} className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
            <div className="flex items-center gap-2 mb-1">
              <k.icon className={`h-4 w-4 ${k.color}`} />
              <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">{k.label}</span>
            </div>
            <div className="text-2xl font-bold tabular-nums">{k.value}</div>
          </div>
        ))}
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Clusters Table with drill-down by city
   ═══════════════════════════════════════════════════════════ */

function ClustersTable({
  clusters,
  clusterTopProducts,
  shopId,
  period,
  selectedSkus,
}: {
  clusters: OzonGeographyCluster[]
  clusterTopProducts: Record<string, OzonGeographyProduct[]>
  shopId: number
  period: number
  selectedSkus?: string
}) {
  const [expandedCluster, setExpandedCluster] = useState<string | null>(null)
  const [expandedCity, setExpandedCity] = useState<string | null>(null)
  const [cityProducts, setCityProducts] = useState<OzonCityProductsResponse | null>(null)
  const [cityLoading, setCityLoading] = useState(false)

  const handleClusterClick = (cluster: string) => {
    setExpandedCluster(prev => prev === cluster ? null : cluster)
    setExpandedCity(null)
    setCityProducts(null)
  }

  const filteredProductCount = selectedSkus ? selectedSkus.split(',').length : 0
  const canDrillCity = filteredProductCount !== 1

  const handleCityClick = async (city: string) => {
    if (!canDrillCity) return
    if (expandedCity === city) {
      setExpandedCity(null)
      setCityProducts(null)
      return
    }
    setExpandedCity(city)
    setCityLoading(true)
    try {
      const res = await getOzonGeographyCityProducts({ shop_id: shopId, period, city, skus: selectedSkus })
      setCityProducts(res)
    } catch {
      setCityProducts(null)
    } finally {
      setCityLoading(false)
    }
  }

  const maxOrders = Math.max(...clusters.map(o => o.orders), 1)

  /* ── Unified Product Table ── */
  const ProductsTable = ({ products, title }: {
    products: { sku?: number; offer_id?: string; name: string; orders: number; revenue: number; avg_check?: number; stability_pct?: number }[]
    title: string
  }) => {
    if (products.length === 0) return null
    const hasStability = products.some(p => p.stability_pct != null)
    const gridCols = hasStability ? '28px 1fr 80px 100px 80px 70px' : '28px 1fr 80px 100px 80px'
    return (
      <div>
        <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-2.5 flex items-center gap-2">
          <Package className="h-4 w-4 text-amber-400" />
          {title}
        </h4>
        <div className="rounded-xl border border-[hsl(var(--border)/0.4)] overflow-hidden">
          <div className="grid items-center px-4 py-2.5 bg-[hsl(var(--muted)/0.06)] border-b border-[hsl(var(--border)/0.3)]" style={{ gridTemplateColumns: gridCols }}>
            <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">#</span>
            <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">ТОВАР</span>
            <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">ЗАКАЗЫ</span>
            <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">ВЫРУЧКА</span>
            <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">СР. ЧЕК</span>
            {hasStability && <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right" title="Стабильность спроса: % недель с заказами">СТАБ.</span>}
          </div>
          {products.map((p, i) => (
            <div
              key={p.sku || i}
              className={`grid items-center px-4 py-2.5 border-b border-[hsl(var(--border)/0.08)] last:border-0 ${i % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''}`}
              style={{ gridTemplateColumns: gridCols }}
            >
              <span className="text-[13px] font-bold text-[hsl(var(--muted-foreground)/0.4)] tabular-nums self-start pt-0.5">{i + 1}</span>
              <div className="min-w-0 pr-3">
                <div className="text-[13px] font-medium truncate">{p.name || `SKU ${p.sku}`}</div>
                <div className="text-[11px] text-[hsl(var(--primary))] font-semibold tabular-nums truncate">{p.offer_id || `SKU ${p.sku}`}</div>
              </div>
              <span className="text-[13px] font-bold text-right tabular-nums">{fmt(p.orders)}</span>
              <span className="text-[13px] text-right tabular-nums">{fmtM(p.revenue)}</span>
              <span className="text-[13px] text-right tabular-nums text-[hsl(var(--muted-foreground))]">{fmtM(p.avg_check ?? (p.orders > 0 ? p.revenue / p.orders : 0))}</span>
              {hasStability && p.stability_pct != null && (
                <span className="text-right">
                  <StabilityBadge pct={p.stability_pct} compact />
                </span>
              )}
            </div>
          ))}
        </div>
      </div>
    )
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.15 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
            <MapPin className="h-5 w-5 text-purple-400" />
            Кластеры доставки
          </h2>
          <p className="text-[12px] text-[hsl(var(--muted-foreground))] mt-1">
            Нажмите на кластер, чтобы увидеть города и топ товары
          </p>
        </div>

        <div className="divide-y divide-[hsl(var(--border)/0.3)]">
          {clusters.map((cl) => {
            const isExpanded = expandedCluster === cl.cluster
            const barPct = cl.orders / maxOrders * 100
            const topProds = clusterTopProducts[cl.cluster] || []
            return (
              <div key={cl.cluster}>
                {/* ── Cluster row ── */}
                <button
                  onClick={() => handleClusterClick(cl.cluster)}
                  className="w-full text-left px-6 py-4 hover:bg-[hsl(var(--muted)/0.05)] transition-colors"
                >
                  <div className="flex items-center gap-4">
                    <div className="shrink-0">
                      {isExpanded
                        ? <ChevronDown className="h-4 w-4 text-[hsl(var(--primary))]" />
                        : <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.5)]" />}
                    </div>
                    <div className="w-[220px] shrink-0">
                      <div className="text-[14px] font-bold">{cl.cluster}</div>
                      <div className="text-[12px] text-[hsl(var(--muted-foreground))]">{cl.cities.length} городов</div>
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="h-6 rounded-full bg-[hsl(var(--muted)/0.08)] overflow-hidden">
                        <div
                          className="h-full rounded-full bg-gradient-to-r from-[hsl(var(--primary)/0.3)] to-[hsl(var(--primary)/0.15)]"
                          style={{ width: `${barPct}%`, transition: 'width 0.5s ease' }}
                        />
                      </div>
                    </div>
                    <div className="flex items-center gap-5 shrink-0">
                      <div className="text-right w-[80px]">
                        <div className="text-[13px] font-bold tabular-nums">{fmt(cl.orders)}</div>
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">заказов</div>
                      </div>
                      <div className="text-right w-[90px]">
                        <div className="text-[13px] font-bold tabular-nums">{fmtM(cl.revenue)}</div>
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">выручка</div>
                      </div>
                      <div className="text-right w-[80px]">
                        <div className="text-[13px] font-bold tabular-nums">{fmtM(cl.avg_check)}</div>
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">ср. чек</div>
                      </div>
                      <div className="w-[50px] text-right">
                        <span className="text-[13px] font-bold text-[hsl(var(--primary))] tabular-nums">{cl.share_pct}%</span>
                      </div>
                      <div className="w-[140px] shrink-0">
                        <StabilityBadge pct={cl.stability_pct} />
                      </div>
                    </div>
                  </div>
                </button>

                {/* ── Expanded content: cities + products ── */}
                <AnimatePresence>
                  {isExpanded && (
                    <motion.div
                      initial={{ height: 0, opacity: 0 }}
                      animate={{ height: 'auto', opacity: 1 }}
                      exit={{ height: 0, opacity: 0 }}
                      transition={{ duration: 0.25 }}
                      className="overflow-hidden"
                    >
                      <div className="px-6 pb-6 space-y-5 bg-[hsl(var(--muted)/0.02)] border-t border-[hsl(var(--border)/0.15)]">

                        {/* ── 1. Cities table ── */}
                        <div className="pt-4">
                          <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-2.5 flex items-center gap-2">
                            <Globe className="h-4 w-4 text-cyan-400" />
                            Города
                            <span className="text-[12px] font-normal text-[hsl(var(--muted-foreground))]">({cl.cities.length})</span>
                          </h4>
                          <div className="rounded-xl border border-[hsl(var(--border)/0.4)] overflow-hidden">
                            <div className="grid grid-cols-[1fr_80px_100px_80px_70px_60px] items-center px-4 py-2.5 bg-[hsl(var(--muted)/0.06)] border-b border-[hsl(var(--border)/0.3)]">
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">ГОРОД</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">ЗАКАЗЫ</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">ВЫРУЧКА</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">СР. ЧЕК</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right" title="Стабильность спроса">СТАБ.</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">ДОЛЯ</span>
                            </div>
                            {cl.cities.map((city, idx) => {
                              const isCityExpanded = expandedCity === city.city
                              return (
                                <div key={city.city}>
                                  <button
                                    onClick={() => handleCityClick(city.city)}
                                    className={`w-full grid grid-cols-[1fr_80px_100px_80px_70px_60px] items-center px-4 py-2.5 text-[13px] border-b border-[hsl(var(--border)/0.08)] last:border-0 transition-colors ${canDrillCity ? 'cursor-pointer hover:bg-[hsl(var(--muted)/0.06)]' : 'cursor-default'} ${isCityExpanded ? 'bg-[hsl(var(--primary)/0.06)]' : idx % 2 ? 'bg-[hsl(var(--muted)/0.02)]' : ''}`}
                                  >
                                    <span className="flex items-center gap-2 text-left min-w-0">
                                      {canDrillCity && (
                                        isCityExpanded
                                          ? <ChevronDown className="h-3.5 w-3.5 shrink-0 text-[hsl(var(--primary))]" />
                                          : <ChevronRight className="h-3.5 w-3.5 shrink-0 text-[hsl(var(--muted-foreground)/0.3)]" />
                                      )}
                                      <span className="font-medium truncate">{city.city}</span>
                                    </span>
                                    <span className="text-right font-bold tabular-nums">{fmt(city.orders)}</span>
                                    <span className="text-right tabular-nums">{fmtM(city.revenue)}</span>
                                    <span className="text-right tabular-nums">{fmtM(city.avg_check)}</span>
                                    <span className="text-right">
                                      <StabilityBadge pct={city.stability_pct} compact />
                                    </span>
                                    <span className="text-right tabular-nums text-[hsl(var(--muted-foreground))]">{city.share_pct}%</span>
                                  </button>

                                  {/* ── City drill-down: products ── */}
                                  <AnimatePresence>
                                    {isCityExpanded && (
                                      <motion.div
                                        initial={{ height: 0, opacity: 0 }}
                                        animate={{ height: 'auto', opacity: 1 }}
                                        exit={{ height: 0, opacity: 0 }}
                                        transition={{ duration: 0.2 }}
                                        className="overflow-hidden"
                                      >
                                        <div className="px-6 py-4 bg-[hsl(var(--muted)/0.04)] border-b border-[hsl(var(--border)/0.15)]">
                                          {cityLoading ? (
                                            <div className="flex items-center gap-2 py-4 text-[hsl(var(--muted-foreground))]">
                                              <Loader2 className="h-4 w-4 animate-spin" />
                                              <span className="text-[13px]">Загрузка товаров...</span>
                                            </div>
                                          ) : cityProducts && cityProducts.products.length > 0 ? (
                                            <ProductsTable
                                              products={cityProducts.products.slice(0, 10)}
                                              title={`Топ товары · ${city.city}`}
                                            />
                                          ) : (
                                            <div className="text-[13px] text-[hsl(var(--muted-foreground))] py-3">Нет данных по товарам</div>
                                          )}
                                        </div>
                                      </motion.div>
                                    )}
                                  </AnimatePresence>
                                </div>
                              )
                            })}
                          </div>
                        </div>

                        {/* ── 2. Top products for the cluster ── */}
                        {topProds.length > 0 && (
                          <ProductsTable
                            products={topProds}
                            title={`Топ товары · ${cl.cluster}`}
                          />
                        )}
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

/* ═══════════════════════════════════════════════════════════
   Top Products Table
   ═══════════════════════════════════════════════════════════ */

function TopProductsTable({ products, onSelectProduct }: { products: OzonGeographyProduct[]; onSelectProduct?: (p: OzonGeographySkuInfo) => void }) {
  if (products.length === 0) return null
  const [sortKey, setSortKey] = useState<string>('orders')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  const handleSort = (key: string) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortKey(key); setSortDir('desc') }
  }

  const sorted = [...products].sort((a, b) => {
    const av = (a as any)[sortKey] ?? 0
    const bv = (b as any)[sortKey] ?? 0
    return sortDir === 'asc' ? av - bv : bv - av
  })

  const SortArrow = ({ col }: { col: string }) => (
    <span className={`ml-0.5 text-[8px] ${sortKey === col ? 'text-[hsl(var(--foreground))]' : 'opacity-30'}`}>
      {sortKey === col ? (sortDir === 'asc' ? '▲' : '▼') : '▼'}
    </span>
  )

  const thCls = "px-3 py-2.5 text-right text-[10px] font-semibold uppercase text-[hsl(var(--muted-foreground))] cursor-pointer select-none hover:text-[hsl(var(--foreground))] transition-colors"

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.25 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
            <Package className="h-5 w-5 text-amber-400" />
            Топ товары по географии
          </h2>
          <p className="text-[11px] text-[hsl(var(--muted-foreground))] mt-1">
            Нажмите на товар, чтобы отфильтровать географию
          </p>
        </div>

        <div className="overflow-auto max-h-[500px]">
          <table className="w-full border-collapse text-[13px]">
            <thead className="sticky top-0 bg-[hsl(var(--card))] z-10">
              <tr className="border-b border-[hsl(var(--border))]">
                <th className={`${thCls} !text-left !px-4`}>Товар</th>
                <th className={thCls} onClick={() => handleSort('orders')}>Заказов<SortArrow col="orders" /></th>
                <th className={thCls} onClick={() => handleSort('revenue')}>Выручка<SortArrow col="revenue" /></th>
                <th className={thCls} onClick={() => handleSort('avg_check')}>Ср. чек<SortArrow col="avg_check" /></th>
                <th className={thCls} onClick={() => handleSort('cluster_count')}>Кластеров<SortArrow col="cluster_count" /></th>
                <th className={thCls} onClick={() => handleSort('city_count')}>Городов<SortArrow col="city_count" /></th>
                <th className={thCls} onClick={() => handleSort('share_pct')}>Доля<SortArrow col="share_pct" /></th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((p, idx) => (
                <tr
                  key={p.sku}
                  onClick={() => onSelectProduct?.({ sku: p.sku, offer_id: p.offer_id, name: p.name })}
                  className={`border-b border-[hsl(var(--border)/0.1)] cursor-pointer ${idx % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''} hover:bg-[hsl(var(--primary)/0.05)] transition-colors`}
                >
                  <td className="px-4 py-2.5 text-left">
                    <div className="text-[12px] font-medium line-clamp-1 max-w-[250px]">{p.name || `SKU ${p.sku}`}</div>
                    <div className="text-[11px] font-bold text-[hsl(var(--muted-foreground))]">{p.offer_id}</div>
                  </td>
                  <td className="px-3 py-2.5 text-right tabular-nums font-semibold">{fmt(p.orders)}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums">{fmtM(p.revenue)}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums">{fmtM(p.avg_check)}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums">
                    <span className="text-[hsl(var(--primary))] font-semibold">{p.cluster_count}</span>
                  </td>
                  <td className="px-3 py-2.5 text-right tabular-nums">{p.city_count}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums text-[hsl(var(--muted-foreground))]">{p.share_pct}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Skeleton
   ═══════════════════════════════════════════════════════════ */

function GeographySkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {[1, 2, 3, 4].map(i => (
          <div key={i} className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
            <Skeleton className="h-4 w-24 mb-2" />
            <Skeleton className="h-8 w-32" />
          </div>
        ))}
      </div>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-6 space-y-3">
        {[1, 2, 3, 4, 5].map(i => <Skeleton key={i} className="h-16 w-full" />)}
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function OzonGeographyPage() {
  const { currentShop } = useAppStore()
  const [data, setData] = useState<OzonGeographyResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState(30)
  const [selectedProducts, setSelectedProducts] = useState<OzonGeographySkuInfo[]>([])

  const fetchData = useCallback(async () => {
    if (!currentShop) return
    setLoading(true)
    setError(null)
    try {
      const skus = selectedProducts.length > 0
        ? selectedProducts.map(p => p.sku).join(',')
        : undefined
      const result = await getOzonGeography({ shop_id: currentShop.id, period, skus })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [currentShop, period, selectedProducts])

  useEffect(() => { fetchData() }, [fetchData])

  const handleSelectProduct = (p: OzonGeographySkuInfo) => {
    if (!selectedProducts.find(s => s.sku === p.sku)) {
      setSelectedProducts(prev => [...prev, p])
    }
  }

  const handleRemoveProduct = (sku: number) => {
    setSelectedProducts(prev => prev.filter(p => p.sku !== sku))
  }

  const handleClearProducts = () => setSelectedProducts([])

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
            Продажи по кластерам доставки и городам — поиск по товарам
          </p>
        </div>
        <button
          onClick={fetchData}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          Обновить
        </button>
      </div>

      {/* Filters: Period + Product Combobox */}
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-4 flex-wrap">
              <div className="flex items-center gap-3 shrink-0">
                <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Период</p>
                <div className="flex gap-1">
                  {PERIOD_OPTIONS.map(o => (
                    <button key={o.value} className={periodSelCls(period === o.value)} onClick={() => setPeriod(o.value)}>
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>
              <div className="h-6 w-px bg-[hsl(var(--border))]" />
              {currentShop && (
                <OzonProductCombobox
                  shopId={currentShop.id}
                  selected={selectedProducts}
                  onSelect={handleSelectProduct}
                  onRemove={handleRemoveProduct}
                  onClear={handleClearProducts}
                />
              )}
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {/* SKU filter pills info */}
      {selectedProducts.length > 0 && data && (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
          <div className="flex items-center gap-2 text-[12px] text-[hsl(var(--muted-foreground))]">
            <Package className="h-3.5 w-3.5" />
            Фильтр: {selectedProducts.length} {selectedProducts.length === 1 ? 'товар' : selectedProducts.length < 5 ? 'товара' : 'товаров'}
            <button onClick={handleClearProducts} className="text-red-400 hover:text-red-300 ml-1 underline">Сбросить</button>
          </div>
        </motion.div>
      )}

      {/* Content */}
      {loading && !data ? (
        <GeographySkeleton />
      ) : error ? (
        <Card>
          <CardContent className="p-10 text-center">
            <AlertTriangle className="h-10 w-10 mx-auto text-red-400 mb-3" />
            <p className="text-lg font-medium text-red-400">{error}</p>
            <button onClick={fetchData} className="mt-4 px-4 py-2 rounded-xl bg-[hsl(var(--primary))] text-white text-sm font-medium">
              Попробовать снова
            </button>
          </CardContent>
        </Card>
      ) : data ? (
        <>
          <GeographyKpiCards data={data} />

          <ClustersTable
            clusters={data.clusters}
            clusterTopProducts={data.cluster_top_products}
            shopId={currentShop!.id}
            period={period}
            selectedSkus={selectedProducts.length > 0 ? selectedProducts.map(p => p.sku).join(',') : undefined}
          />

          {data.top_products.length > 0 && (
            <TopProductsTable
              products={data.top_products}
              onSelectProduct={handleSelectProduct}
            />
          )}
        </>
      ) : null}
    </div>
  )
}
