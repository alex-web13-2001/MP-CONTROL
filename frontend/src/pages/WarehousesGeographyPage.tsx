/**
 * WarehousesGeographyPage — Sales geography with multi-SKU combobox,
 * stability metrics, drill-down by region, top products.
 */
import { useState, useEffect, useCallback, useRef } from 'react'
import OzonGeographyPage from './OzonGeographyPage'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Globe,
  RefreshCw,
  Download,
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
  Sparkles,
  Brain,
  TrendingUp,
  Truck,
  ArrowRight,
  Lightbulb,
  ShieldAlert,
  Target,
  Layers,
  Zap,
  PackageSearch,
  Megaphone,
  Eye,
  ArrowRightLeft,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBGeography,
  getWBGeographyRegionProducts,
  searchGeographyProducts,
  getGeographyAIAnalysis,
  type WBGeographyResponse,
  type WBGeographyOkrug,
  type WBGeographyProduct,
  type WBGeographySkuInfo,
  type WBRegionProductsResponse,
  type GeoAIAnalysis,
  downloadGeoExcel,
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
   Product Combobox — multi-select with autocomplete
   ═══════════════════════════════════════════════════════════ */

function ProductCombobox({
  shopId,
  selected,
  onSelect,
  onRemove,
  onClear,
}: {
  shopId: number
  selected: WBGeographySkuInfo[]
  onSelect: (p: WBGeographySkuInfo) => void
  onRemove: (nmId: number) => void
  onClear: () => void
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [options, setOptions] = useState<WBGeographySkuInfo[]>([])
  const [loading, setLoading] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout>>()

  // Search with debounce
  useEffect(() => {
    clearTimeout(timerRef.current)
    timerRef.current = setTimeout(async () => {
      setLoading(true)
      try {
        const res = await searchGeographyProducts({ shop_id: shopId, q: search })
        setOptions(res.products)
      } catch {
        setOptions([])
      } finally {
        setLoading(false)
      }
    }, 300)
    return () => clearTimeout(timerRef.current)
  }, [search, shopId])

  // Close on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const selectedIds = new Set(selected.map(s => s.nm_id))
  const filteredOptions = options.filter(o => !selectedIds.has(o.nm_id))

  return (
    <div ref={dropdownRef} className="relative flex-1 min-w-0">
      {/* Input area with chips */}
      <div
        className="flex flex-wrap items-center gap-1.5 px-3 py-2 min-h-[44px] rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))] cursor-text transition-all focus-within:ring-2 focus-within:ring-[hsl(var(--primary)/0.3)]"
        onClick={() => { inputRef.current?.focus(); setOpen(true) }}
      >
        <Search className="h-4 w-4 shrink-0 text-[hsl(var(--muted-foreground)/0.5)]" />
        {selected.map(s => (
          <span
            key={s.nm_id}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded-lg bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))] text-[12px] font-medium max-w-[200px] truncate"
          >
            {s.vendor_code || `#${s.nm_id}`}
            <X
              className="h-3 w-3 cursor-pointer hover:text-red-400 shrink-0"
              onClick={(e) => { e.stopPropagation(); onRemove(s.nm_id) }}
            />
          </span>
        ))}
        <input
          ref={inputRef}
          type="text"
          value={search}
          onChange={e => { setSearch(e.target.value); setOpen(true) }}
          onFocus={() => setOpen(true)}
          placeholder={selected.length === 0 ? 'Поиск по артикулу, названию или nm_id...' : 'Ещё...'}
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

      {/* Dropdown */}
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
                  key={opt.nm_id}
                  onClick={() => { onSelect(opt); setSearch(''); }}
                  className="w-full text-left px-4 py-2.5 hover:bg-[hsl(var(--muted)/0.1)] transition-colors border-b border-[hsl(var(--border)/0.2)] last:border-0"
                >
                  <div className="text-[13px] font-medium text-[hsl(var(--foreground))] line-clamp-1">
                    {opt.name || `Товар #${opt.nm_id}`}
                  </div>
                  <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
                    {opt.vendor_code && <span className="font-semibold">{opt.vendor_code}</span>}
                    {opt.vendor_code && ' · '}
                    nm_id: {opt.nm_id}
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

function GeographyKpiCards({ data }: { data: WBGeographyResponse }) {
  const kpis = [
    { label: 'Всего заказов', value: fmt(data.total_orders), icon: ShoppingCart, color: 'text-blue-400' },
    { label: 'Выручка', value: fmtM(data.total_revenue), icon: DollarSign, color: 'text-emerald-400' },
    { label: 'Средний чек', value: fmtM(data.avg_check), icon: BarChart3, color: 'text-purple-400' },
    { label: 'Охват', value: `${data.total_okrugs} ФО · ${data.total_regions} рег.`, icon: Globe, color: 'text-cyan-400' },
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
   Okrugs Table with drill-down — v2 Redesign
   Full-width layout, stacked sections, unified product tables
   ═══════════════════════════════════════════════════════════ */

function OkrugsTable({
  okrugs,
  okrugTopProducts,
  shopId,
  period,
  selectedNmIds,
}: {
  okrugs: WBGeographyOkrug[]
  okrugTopProducts: Record<string, WBGeographyProduct[]>
  shopId: number
  period: number
  selectedNmIds?: string
}) {
  const [expandedOkrug, setExpandedOkrug] = useState<string | null>(null)
  const [expandedRegion, setExpandedRegion] = useState<string | null>(null)
  const [regionProducts, setRegionProducts] = useState<WBRegionProductsResponse | null>(null)
  const [regionLoading, setRegionLoading] = useState(false)

  const handleOkrugClick = (okrug: string) => {
    setExpandedOkrug(prev => prev === okrug ? null : okrug)
    setExpandedRegion(null)
    setRegionProducts(null)
  }

  const filteredProductCount = selectedNmIds ? selectedNmIds.split(',').length : 0
  const canDrillRegion = filteredProductCount !== 1

  const handleRegionClick = async (region: string) => {
    if (!canDrillRegion) return
    if (expandedRegion === region) {
      setExpandedRegion(null)
      setRegionProducts(null)
      return
    }
    setExpandedRegion(region)
    setRegionLoading(true)
    try {
      const res = await getWBGeographyRegionProducts({ shop_id: shopId, period, region, nm_ids: selectedNmIds })
      setRegionProducts(res)
    } catch {
      setRegionProducts(null)
    } finally {
      setRegionLoading(false)
    }
  }

  const maxOrders = Math.max(...okrugs.map(o => o.orders), 1)

  /* ── Unified Product Table ── */
  const ProductsTable = ({ products, title }: {
    products: { nm_id: number; vendor_code: string; name: string; orders: number; revenue: number; avg_check?: number; stability_pct?: number }[]
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
              key={p.nm_id}
              className={`grid items-center px-4 py-2.5 border-b border-[hsl(var(--border)/0.08)] last:border-0 ${i % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''}`}
              style={{ gridTemplateColumns: gridCols }}
            >
              <span className="text-[13px] font-bold text-[hsl(var(--muted-foreground)/0.4)] tabular-nums self-start pt-0.5">{i + 1}</span>
              <div className="min-w-0 pr-3">
                <div className="text-[13px] font-medium truncate">{p.name || `Товар #${p.nm_id}`}</div>
                <div className="text-[11px] text-[hsl(var(--primary))] font-semibold tabular-nums truncate">{p.vendor_code || `#${p.nm_id}`}</div>
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
            Федеральные округа
          </h2>
          <p className="text-[12px] text-[hsl(var(--muted-foreground))] mt-1">
            Нажмите на округ, чтобы увидеть регионы и топ товары
          </p>
        </div>

        <div className="divide-y divide-[hsl(var(--border)/0.3)]">
          {okrugs.map((ok) => {
            const isExpanded = expandedOkrug === ok.okrug
            const barPct = ok.orders / maxOrders * 100
            const topProds = okrugTopProducts[ok.okrug] || []
            return (
              <div key={ok.okrug}>
                {/* ── Okrug row ── */}
                <button
                  onClick={() => handleOkrugClick(ok.okrug)}
                  className="w-full text-left px-6 py-4 hover:bg-[hsl(var(--muted)/0.05)] transition-colors"
                >
                  <div className="flex items-center gap-4">
                    <div className="shrink-0">
                      {isExpanded
                        ? <ChevronDown className="h-4 w-4 text-[hsl(var(--primary))]" />
                        : <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.5)]" />}
                    </div>
                    <div className="w-[200px] shrink-0">
                      <div className="text-[14px] font-bold">{ok.okrug}</div>
                      <div className="text-[12px] text-[hsl(var(--muted-foreground))]">{ok.regions.length} регионов</div>
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
                        <div className="text-[13px] font-bold tabular-nums">{fmt(ok.orders)}</div>
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">заказов</div>
                      </div>
                      <div className="text-right w-[90px]">
                        <div className="text-[13px] font-bold tabular-nums">{fmtM(ok.revenue)}</div>
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">выручка</div>
                      </div>
                      <div className="text-right w-[80px]">
                        <div className="text-[13px] font-bold tabular-nums">{fmtM(ok.avg_check)}</div>
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">ср. чек</div>
                      </div>
                      <div className="w-[50px] text-right">
                        <span className="text-[13px] font-bold text-[hsl(var(--primary))] tabular-nums">{ok.share_pct}%</span>
                      </div>
                      <div className="w-[140px] shrink-0">
                        <StabilityBadge pct={ok.stability_pct} />
                      </div>
                    </div>
                  </div>
                </button>

                {/* ── Expanded content: full-width, stacked ── */}
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

                        {/* ── 1. Regions table ── */}
                        <div className="pt-4">
                          <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-2.5 flex items-center gap-2">
                            <Globe className="h-4 w-4 text-cyan-400" />
                            Регионы
                            <span className="text-[12px] font-normal text-[hsl(var(--muted-foreground))]">({ok.regions.length})</span>
                          </h4>
                          <div className="rounded-xl border border-[hsl(var(--border)/0.4)] overflow-hidden">
                            {/* Header */}
                            <div className="grid grid-cols-[1fr_80px_100px_80px_70px_60px] items-center px-4 py-2.5 bg-[hsl(var(--muted)/0.06)] border-b border-[hsl(var(--border)/0.3)]">
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">РЕГИОН</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">ЗАКАЗЫ</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">ВЫРУЧКА</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">СР. ЧЕК</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right" title="Стабильность спроса">СТАБ.</span>
                              <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))] text-right">ДОЛЯ</span>
                            </div>
                            {/* Region rows */}
                            {ok.regions.map((reg, idx) => {
                              const isRegExpanded = expandedRegion === reg.region
                              return (
                                <div key={reg.region}>
                                  <button
                                    onClick={() => handleRegionClick(reg.region)}
                                    className={`w-full grid grid-cols-[1fr_80px_100px_80px_70px_60px] items-center px-4 py-2.5 text-[13px] border-b border-[hsl(var(--border)/0.08)] last:border-0 transition-colors ${canDrillRegion ? 'cursor-pointer hover:bg-[hsl(var(--muted)/0.06)]' : 'cursor-default'} ${isRegExpanded ? 'bg-[hsl(var(--primary)/0.06)]' : idx % 2 ? 'bg-[hsl(var(--muted)/0.02)]' : ''}`}
                                  >
                                    <span className="flex items-center gap-2 text-left min-w-0">
                                      {canDrillRegion && (
                                        isRegExpanded
                                          ? <ChevronDown className="h-3.5 w-3.5 shrink-0 text-[hsl(var(--primary))]" />
                                          : <ChevronRight className="h-3.5 w-3.5 shrink-0 text-[hsl(var(--muted-foreground)/0.3)]" />
                                      )}
                                      <span className="font-medium truncate">{reg.region}</span>
                                    </span>
                                    <span className="text-right font-bold tabular-nums">{fmt(reg.orders)}</span>
                                    <span className="text-right tabular-nums">{fmtM(reg.revenue)}</span>
                                    <span className="text-right tabular-nums">{fmtM(reg.avg_check)}</span>
                                    <span className="text-right">
                                      <StabilityBadge pct={reg.stability_pct} compact />
                                    </span>
                                    <span className="text-right tabular-nums text-[hsl(var(--muted-foreground))]">{reg.share_pct}%</span>
                                  </button>

                                  {/* ── Region drill-down: products table ── */}
                                  <AnimatePresence>
                                    {isRegExpanded && (
                                      <motion.div
                                        initial={{ height: 0, opacity: 0 }}
                                        animate={{ height: 'auto', opacity: 1 }}
                                        exit={{ height: 0, opacity: 0 }}
                                        transition={{ duration: 0.2 }}
                                        className="overflow-hidden"
                                      >
                                        <div className="px-6 py-4 bg-[hsl(var(--muted)/0.04)] border-b border-[hsl(var(--border)/0.15)]">
                                          {regionLoading ? (
                                            <div className="flex items-center gap-2 py-4 text-[hsl(var(--muted-foreground))]">
                                              <Loader2 className="h-4 w-4 animate-spin" />
                                              <span className="text-[13px]">Загрузка товаров...</span>
                                            </div>
                                          ) : regionProducts && regionProducts.products.length > 0 ? (
                                            <ProductsTable
                                              products={regionProducts.products.slice(0, 10)}
                                              title={`Топ товары · ${reg.region}`}
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

                        {/* ── 2. Top products for the okrug ── */}
                        {topProds.length > 0 && (
                          <ProductsTable
                            products={topProds}
                            title={`Топ товары · ${ok.okrug}`}
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

function TopProductsTable({ products, onSelectProduct }: { products: WBGeographyProduct[]; onSelectProduct?: (p: WBGeographySkuInfo) => void }) {
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
                <th className={thCls} onClick={() => handleSort('okrug_count')}>Округов<SortArrow col="okrug_count" /></th>
                <th className={thCls} onClick={() => handleSort('region_count')}>Регионов<SortArrow col="region_count" /></th>
                <th className={thCls} onClick={() => handleSort('share_pct')}>Доля<SortArrow col="share_pct" /></th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((p, idx) => (
                <tr
                  key={p.nm_id}
                  onClick={() => onSelectProduct?.({ nm_id: p.nm_id, vendor_code: p.vendor_code, name: p.name })}
                  className={`border-b border-[hsl(var(--border)/0.1)] cursor-pointer ${idx % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''} hover:bg-[hsl(var(--primary)/0.05)] transition-colors`}
                >
                  <td className="px-4 py-2.5 text-left">
                    <div className="text-[12px] font-medium line-clamp-1 max-w-[250px]">{p.name || `#${p.nm_id}`}</div>
                    <div className="text-[11px] font-bold text-[hsl(var(--muted-foreground))]">{p.vendor_code}</div>
                  </td>
                  <td className="px-3 py-2.5 text-right tabular-nums font-semibold">{fmt(p.orders)}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums">{fmtM(p.revenue)}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums">{fmtM(p.avg_check)}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums">
                    <span className="text-[hsl(var(--primary))] font-semibold">{p.okrug_count}</span>
                  </td>
                  <td className="px-3 py-2.5 text-right tabular-nums">{p.region_count}</td>
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
   AI Geography Insight Block
   ═══════════════════════════════════════════════════════════ */

const INSIGHT_TYPE_CONFIG: Record<string, { icon: React.ElementType; color: string; label: string }> = {
  stable_leader:          { icon: TrendingUp,     color: 'text-emerald-400', label: 'Лидер' },
  unstable_demand:        { icon: Activity,       color: 'text-red-400',     label: 'Нестабильный' },
  regional_champion:      { icon: Target,         color: 'text-blue-400',    label: 'Региональный' },
  cross_delivery_problem: { icon: Truck,          color: 'text-amber-400',   label: 'Кросс-проблема' },
  dead_stock_risk:        { icon: PackageSearch,   color: 'text-red-400',     label: 'Залежалка' },
}

const ACTION_CONFIG: Record<string, { icon: React.ElementType; color: string; label: string }> = {
  redistribute:    { icon: ArrowRightLeft, color: 'text-cyan-400',    label: 'Перераспределить' },
  launch_ads:      { icon: Megaphone,      color: 'text-orange-400',  label: 'Запустить рекламу' },
  increase_supply: { icon: Package,        color: 'text-blue-400',    label: 'Увеличить поставку' },
  discount:        { icon: DollarSign,     color: 'text-amber-400',   label: 'Снизить цену' },
  monitor:         { icon: Eye,            color: 'text-gray-400',    label: 'Наблюдать' },
}

const RISK_LEVEL_CONFIG: Record<string, { bg: string; text: string; label: string }> = {
  high:   { bg: 'bg-red-500/15',     text: 'text-red-400',     label: 'Высокий' },
  medium: { bg: 'bg-amber-500/15',   text: 'text-amber-400',   label: 'Средний' },
  low:    { bg: 'bg-emerald-500/15', text: 'text-emerald-400', label: 'Низкий' },
}

function GeographyAIInsight({ shopId, period }: { shopId: number; period: number }) {
  const [data, setData] = useState<GeoAIAnalysis | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [modalOpen, setModalOpen] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [elapsed, setElapsed] = useState(0)
  const [retryCount, setRetryCount] = useState(0)
  const MAX_RETRIES = 2

  const fetchAI = useCallback(async (force = false, retry = 0) => {
    if (force) setRefreshing(true)
    else setLoading(true)
    setError(null)
    setElapsed(0)

    // Start elapsed timer
    const startTime = Date.now()
    const timer = setInterval(() => {
      setElapsed(Math.floor((Date.now() - startTime) / 1000))
    }, 1000)

    try {
      const result = await getGeographyAIAnalysis({ shop_id: shopId, period, force })
      setData(result)
      setRetryCount(0)
    } catch (e: any) {
      const isTimeout = e?.name === 'AbortError' || e?.message?.includes('таймаут') || e?.message?.includes('timeout')
      const isServerError = e?.message?.includes('HTTP 5')
      const errMsg = e?.message || 'Ошибка ИИ-анализа'

      if (retry < MAX_RETRIES && (isTimeout || isServerError)) {
        setRetryCount(retry + 1)
        clearInterval(timer)
        setTimeout(() => fetchAI(force, retry + 1), 2000)
        return
      }

      setError(errMsg)
      setRetryCount(0)
    } finally {
      clearInterval(timer)
      setLoading(false)
      setRefreshing(false)
    }
  }, [shopId, period])

  useEffect(() => { fetchAI() }, [fetchAI])

  // Lock body scroll when modal is open
  useEffect(() => {
    if (modalOpen) {
      document.body.style.overflow = 'hidden'
      return () => { document.body.style.overflow = '' }
    }
  }, [modalOpen])

  const severityConfig = {
    critical: { bg: 'from-red-500/10 to-red-500/5', border: 'border-red-500/30', icon: '🔴', label: 'Критично', bannerBg: 'bg-red-500/8', bannerBorder: 'border-red-500/25' },
    warning:  { bg: 'from-amber-500/10 to-amber-500/5', border: 'border-amber-500/30', icon: '🟡', label: 'Внимание', bannerBg: 'bg-amber-500/8', bannerBorder: 'border-amber-500/25' },
    ok:       { bg: 'from-emerald-500/10 to-emerald-500/5', border: 'border-emerald-500/30', icon: '🟢', label: 'Всё ОК', bannerBg: 'bg-emerald-500/8', bannerBorder: 'border-emerald-500/25' },
  }

  /* ── Loading state: progress bar with timer ── */
  if ((loading || refreshing) && !data) {
    return (
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
        <div className="flex items-center gap-3 px-4 py-3 rounded-xl border border-emerald-500/20 bg-[hsl(var(--card))]">
          <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-emerald-600 to-cyan-500 shadow-md shadow-emerald-500/20 flex items-center justify-center shrink-0">
            <Brain className="h-4 w-4 text-white animate-pulse" />
          </div>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">
                {retryCount > 0 ? `Повторная попытка ${retryCount}/${MAX_RETRIES}...` : 'ИИ-анализ загружается...'}
              </span>
              <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">
                {elapsed > 0 ? `${elapsed} сек` : ''}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <Skeleton className="h-3 w-32" />
              <Skeleton className="h-3 w-20" />
              <Skeleton className="h-3 w-16" />
            </div>
            <div className="mt-1.5 flex items-center gap-2">
              <div className="h-1 flex-1 max-w-[200px] rounded-full bg-[hsl(var(--muted)/0.2)] overflow-hidden">
                <div className="h-full bg-gradient-to-r from-emerald-500 to-cyan-500 rounded-full transition-all duration-1000"
                  style={{ width: `${Math.min(95, elapsed * 0.8)}%` }} />
              </div>
              <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.4)]">
                {elapsed > 10 && 'сбор данных'}
                {elapsed > 30 && ' • анализ'}
                {elapsed > 60 && ' • рекомендации'}
              </span>
            </div>
          </div>
        </div>
      </motion.div>
    )
  }

  /* ── Error state: compact error banner ── */
  if (error) {
    return (
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
        <div className="flex items-center gap-3 px-4 py-3 rounded-xl border border-red-500/20 bg-[hsl(var(--card))]">
          <AlertTriangle className="h-4 w-4 text-red-400 shrink-0" />
          <span className="text-[13px] text-[hsl(var(--muted-foreground))]">{error}</span>
          <button onClick={() => fetchAI(true)} className="ml-auto text-[13px] font-medium text-[hsl(var(--primary))] hover:underline">
            Повторить
          </button>
        </div>
      </motion.div>
    )
  }

  if (!data) return null

  const sev = severityConfig[data.severity] || severityConfig.warning
  const analyzedAtStr = data.analyzed_at
    ? `Анализ от ${new Date(data.analyzed_at * 1000).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })}`
    : null
  const km = data.key_metrics || { concentration_pct: 0, top_regions_count: 0, total_regions: 0, regions_with_stable_demand: 0, underserved_okrugs: 0 }
  const ctx = data.context || { total_orders: 0, total_revenue: 0, total_okrugs: 0, total_regions: 0, warehouses_count: 0 }

  const insightsCount = (data.product_insights?.length || 0) + (data.logistics_match?.length || 0)

  return (
    <>
      {/* ═══ Compact Banner ═══ */}
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
        <div className={`flex items-center gap-3 px-4 py-3 rounded-xl border ${sev.bannerBorder} bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted)/0.08)] transition-colors`}>
          <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-emerald-600 to-cyan-500 shadow-md shadow-emerald-500/20 flex items-center justify-center shrink-0">
            <Sparkles className="h-4 w-4 text-white" />
          </div>

          <div className="flex items-center gap-2 min-w-0 flex-1">
            <span className="text-sm">{sev.icon}</span>
            <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">{sev.label}</span>
            <span className="text-[13px] text-[hsl(var(--muted-foreground))] truncate hidden sm:inline">
              · {data.diagnosis?.substring(0, 100)}{(data.diagnosis?.length || 0) > 100 ? '…' : ''}
            </span>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            {insightsCount > 0 && (
              <span className="text-[11px] font-semibold px-2 py-0.5 rounded-full bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))]">
                {insightsCount} инсайт{insightsCount === 1 ? '' : insightsCount < 5 ? 'а' : 'ов'}
              </span>
            )}
            {analyzedAtStr && (
              <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)] hidden md:inline">{analyzedAtStr}</span>
            )}
            <button
              onClick={(e) => { e.stopPropagation(); fetchAI(true) }}
              disabled={refreshing}
              className="p-1.5 rounded-lg hover:bg-[hsl(var(--muted)/0.3)] transition-colors"
              title="Обновить анализ"
            >
              <RefreshCw className={`h-3.5 w-3.5 text-[hsl(var(--muted-foreground))] ${refreshing ? 'animate-spin' : ''}`} />
            </button>
            <button
              onClick={() => setModalOpen(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-[hsl(var(--primary))] text-white text-[12px] font-semibold hover:opacity-90 transition-opacity"
            >
              <Brain className="h-3.5 w-3.5" />
              Прочитать
            </button>
          </div>
        </div>
      </motion.div>

      {/* ═══ Full-screen Modal ═══ */}
      <AnimatePresence>
        {modalOpen && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="fixed inset-0 z-[100] flex items-start justify-center bg-black/60 backdrop-blur-sm"
            onClick={() => setModalOpen(false)}
          >
            <motion.div
              initial={{ opacity: 0, y: 40, scale: 0.97 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: 40, scale: 0.97 }}
              transition={{ duration: 0.25, ease: 'easeOut' }}
              className="w-full max-w-[1100px] max-h-[90vh] mt-[5vh] mx-4 rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-2xl overflow-hidden flex flex-col"
              onClick={(e) => e.stopPropagation()}
            >
              {/* Modal Header */}
              <div className={`px-8 py-5 bg-gradient-to-r ${sev.bg} border-b border-[hsl(var(--border)/0.3)] shrink-0`}>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="h-12 w-12 rounded-xl bg-gradient-to-br from-emerald-600 to-cyan-500 shadow-lg shadow-emerald-500/25 flex items-center justify-center">
                      <Sparkles className="h-6 w-6 text-white" />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <h3 className="text-lg font-bold text-[hsl(var(--foreground))]">ИИ-Анализ географии</h3>
                        <span className="text-sm">{sev.icon}</span>
                        <span className="text-sm font-semibold text-[hsl(var(--muted-foreground))]">{sev.label}</span>
                      </div>
                      <p className="text-[15px] text-[hsl(var(--muted-foreground))] mt-1 leading-relaxed max-w-[700px]">{data.diagnosis}</p>
                    </div>
                  </div>
                  <button
                    onClick={() => setModalOpen(false)}
                    className="p-2.5 rounded-xl hover:bg-[hsl(var(--muted)/0.3)] transition-colors"
                    title="Закрыть"
                  >
                    <X className="h-6 w-6 text-[hsl(var(--muted-foreground))]" />
                  </button>
                </div>
              </div>

              {/* Modal Body — scrollable */}
              <div className="flex-1 overflow-y-auto px-8 py-6 space-y-8">
                {/* 4 Metric cards */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-5">
                  <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <Layers className="h-5 w-5 text-blue-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Концентрация</span>
                    </div>
                    <p className={`text-2xl font-bold ${safeNum(km.concentration_pct) > 70 ? 'text-red-400' : safeNum(km.concentration_pct) > 40 ? 'text-amber-400' : 'text-emerald-400'}`}>
                      {km.concentration_pct}%
                    </p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">топ-{km.top_regions_count} регионов</p>
                  </div>

                  <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <MapPin className="h-5 w-5 text-emerald-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Регионы</span>
                    </div>
                    <p className="text-2xl font-bold text-[hsl(var(--foreground))]">{km.total_regions}</p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">со стаб. спросом: {km.regions_with_stable_demand}</p>
                  </div>

                  <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <Truck className="h-5 w-5 text-amber-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Недообслуженные</span>
                    </div>
                    <p className={`text-2xl font-bold ${safeNum(km.underserved_okrugs) > 2 ? 'text-red-400' : safeNum(km.underserved_okrugs) > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
                      {km.underserved_okrugs}
                    </p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">округов без стока</p>
                  </div>

                  <div className="rounded-xl bg-emerald-500/5 border border-emerald-500/20 p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <ShoppingCart className="h-5 w-5 text-emerald-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-emerald-500/60">Заказы</span>
                    </div>
                    <p className="text-2xl font-bold text-emerald-400">{fmt(ctx.total_orders)}</p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">{fmtM(ctx.total_revenue)} выручка</p>
                  </div>
                </div>

                {/* ═══ Concentration Block ═══ */}
                {data.concentration && (
                  <div className="space-y-3">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <Layers className="h-5 w-5" />
                      Концентрация продаж
                      {data.concentration.risk_level && (() => {
                        const rl = RISK_LEVEL_CONFIG[data.concentration.risk_level] || RISK_LEVEL_CONFIG.medium
                        return (
                          <span className={`text-[12px] font-bold px-2.5 py-1 rounded ${rl.bg} ${rl.text}`}>
                            Риск: {rl.label}
                          </span>
                        )
                      })()}
                    </h4>
                    <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)] p-6">
                      <p className="text-[16px] text-[hsl(var(--foreground)/0.85)] leading-relaxed mb-5">
                        {data.concentration.summary}
                      </p>
                      {data.concentration.top_regions?.length > 0 && (
                        <div className="overflow-x-auto">
                          <table className="w-full text-[15px]">
                            <thead>
                              <tr className="text-left text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.5)]">
                                <th className="pb-3 pr-6">Регион</th>
                                <th className="pb-3 pr-6 text-right">Заказов</th>
                                <th className="pb-3 pr-6 text-right">Доля</th>
                                <th className="pb-3 text-right">Стабильность</th>
                              </tr>
                            </thead>
                            <tbody>
                              {data.concentration.top_regions.map((r, i) => (
                                <tr key={i} className="border-t border-[hsl(var(--border)/0.1)]">
                                  <td className="py-3 pr-6 font-medium text-[hsl(var(--foreground))]">{r.region}</td>
                                  <td className="py-3 pr-6 text-right tabular-nums text-[hsl(var(--muted-foreground))]">{fmt(r.orders)}</td>
                                  <td className="py-3 pr-6 text-right tabular-nums font-semibold">{r.share_pct}%</td>
                                  <td className="py-3 text-right"><StabilityBadge pct={r.stability_pct} compact /></td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                      {data.concentration.recommendation && (
                        <div className="mt-5 flex items-start gap-3 px-4 py-3.5 rounded-lg bg-[hsl(var(--muted)/0.1)] border border-[hsl(var(--border)/0.4)]">
                          <Lightbulb className="h-5 w-5 text-amber-400 shrink-0 mt-0.5" />
                          <p className="text-[15px] text-[hsl(var(--foreground)/0.9)] leading-relaxed">{data.concentration.recommendation}</p>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* ═══ Product Insights ═══ */}
                {data.product_insights?.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <ShieldAlert className="h-5 w-5" />
                      Инсайты по товарам ({data.product_insights.length})
                    </h4>
                    {data.product_insights.map((pi, idx) => {
                      const itCfg = INSIGHT_TYPE_CONFIG[pi.insight_type] || INSIGHT_TYPE_CONFIG.stable_leader
                      const actCfg = ACTION_CONFIG[pi.action] || ACTION_CONFIG.monitor
                      const ItIcon = itCfg.icon
                      const ActIcon = actCfg.icon
                      return (
                        <div key={idx} className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)] overflow-hidden">
                          <div className="px-6 py-5">
                            <div className="flex items-start justify-between mb-3">
                              <div className="flex items-center gap-3 min-w-0">
                                <div className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-lg ${itCfg.color.replace('text-', 'bg-').replace('400', '500/10')}`}>
                                  <ItIcon className={`h-5 w-5 ${itCfg.color}`} />
                                </div>
                                <div className="min-w-0">
                                  <div className="flex items-center gap-2 flex-wrap">
                                    <span className="text-[16px] font-bold text-[hsl(var(--foreground))]">{pi.vendor_code}</span>
                                    <span className="text-[15px] text-[hsl(var(--muted-foreground))] truncate">{pi.name}</span>
                                  </div>
                                  <div className="flex items-center gap-3 mt-1 text-[14px] text-[hsl(var(--muted-foreground)/0.6)]">
                                    <span>{pi.orders} заказов</span>
                                    <span>·</span>
                                    <span>{pi.regions_count} регионов</span>
                                    <span>·</span>
                                    <StabilityBadge pct={pi.stability_pct} compact />
                                  </div>
                                </div>
                              </div>
                              <span className={`text-[12px] font-bold px-3 py-1.5 rounded-full whitespace-nowrap ${itCfg.color.replace('text-', 'bg-').replace('400', '500/15')} ${itCfg.color}`}>
                                {itCfg.label}
                              </span>
                            </div>
                            <p className="text-[15px] text-[hsl(var(--foreground)/0.9)] leading-relaxed mb-4">{pi.detail}</p>
                            <div className="flex items-center justify-between flex-wrap gap-2">
                              <div className="flex items-center gap-2">
                                <ActIcon className="h-5 w-5 text-[hsl(var(--foreground))]" />
                                <span className="text-[15px] font-bold text-[hsl(var(--foreground))]">{actCfg.label}</span>
                              </div>
                              {pi.expected_effect && (
                                <div className="flex items-center gap-1.5">
                                  <Zap className="h-4 w-4 text-[hsl(var(--foreground))]" />
                                  <span className="text-[15px] font-medium text-[hsl(var(--foreground)/0.85)]">{pi.expected_effect}</span>
                                </div>
                              )}
                            </div>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )}

                {/* ═══ Logistics Match ═══ */}
                {data.logistics_match?.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <Truck className="h-5 w-5" />
                      Логистическое соответствие ({data.logistics_match.length})
                    </h4>
                    {data.logistics_match.map((lm, idx) => (
                      <div key={idx} className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)] p-6">
                        <div className="flex items-center justify-between mb-3">
                          <div className="flex items-center gap-2">
                            <Globe className="h-5 w-5 text-blue-400" />
                            <span className="text-[16px] font-bold text-[hsl(var(--foreground))]">{lm.okrug}</span>
                            <span className="text-[15px] text-[hsl(var(--muted-foreground))]">— {lm.orders} заказов</span>
                          </div>
                          <span className={`text-[14px] font-bold px-3 py-1 rounded ${safeNum(lm.cross_pct) > 50 ? 'bg-red-500/15 text-red-400' : 'bg-amber-500/15 text-amber-400'}`}>
                            Кросс: {lm.cross_pct}%
                          </span>
                        </div>
                        <div className="rounded-lg border border-[hsl(var(--border)/0.2)] overflow-hidden mb-3">
                          <div className="bg-[hsl(var(--muted)/0.06)] px-5 py-3 flex items-center gap-3 border-b border-[hsl(var(--border)/0.15)]">
                            <ArrowRight className="h-5 w-5 text-amber-400" />
                            <span className="text-[15px] font-semibold text-[hsl(var(--foreground))]">Доставка из: {lm.serving_warehouse}</span>
                            <span className="text-[15px] text-[hsl(var(--muted-foreground))]">Ближайший склад: {lm.nearest_warehouse} (сток: {lm.warehouse_stock} шт)</span>
                          </div>
                          <div className="px-5 py-4">
                            <p className="text-[15px] text-[hsl(var(--foreground)/0.9)] leading-relaxed">{lm.detail}</p>
                          </div>
                        </div>
                        {lm.recommendation && (
                          <div className="flex items-start gap-3 px-4 py-3.5 rounded-lg bg-[hsl(var(--muted)/0.1)] border border-[hsl(var(--border)/0.4)]">
                            <Lightbulb className="h-5 w-5 text-amber-400 shrink-0 mt-0.5" />
                            <span className="text-[15px] font-medium text-[hsl(var(--foreground)/0.9)]">{lm.recommendation}</span>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}

                {/* ═══ General Tips ═══ */}
                {data.general_tips?.length > 0 && (
                  <div className="space-y-2">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <Lightbulb className="h-5 w-5" />
                      Рекомендации
                    </h4>
                    {data.general_tips.map((tip, i) => (
                      <div key={i} className="rounded-xl border border-[hsl(var(--border)/0.2)] bg-[hsl(var(--muted)/0.04)] p-5">
                        <p className="text-[16px] text-[hsl(var(--foreground)/0.95)] leading-relaxed">{tip}</p>
                      </div>
                    ))}
                  </div>
                )}

                {/* Context line */}
                <div className="flex flex-wrap items-center gap-x-5 gap-y-1 text-[13px] text-[hsl(var(--muted-foreground)/0.4)]">
                  <span>Период: {data.period_days}д</span>
                  <span>Округов: {ctx.total_okrugs}</span>
                  <span>Регионов: {ctx.total_regions}</span>
                  <span>Складов: {ctx.warehouses_count}</span>
                  <span>Заказов: {fmt(ctx.total_orders)}</span>
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  )
}

/* ═══════════════════════════════════════════════════════════
   Skeleton
   ═══════════════════════════════════════════════════════════ */

function GeographySkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-[80px] rounded-2xl" />)}
      </div>
      <Skeleton className="h-[400px] rounded-2xl" />
      <Skeleton className="h-[300px] rounded-2xl" />
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
  const [selectedProducts, setSelectedProducts] = useState<WBGeographySkuInfo[]>([])

  const fetchData = useCallback(async () => {
    if (!currentShop || !isWB) return
    setLoading(true)
    setError(null)
    try {
      const nm_ids = selectedProducts.length > 0
        ? selectedProducts.map(p => p.nm_id).join(',')
        : undefined
      const result = await getWBGeography({ shop_id: currentShop.id, period, nm_ids })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [currentShop, isWB, period, selectedProducts])

  useEffect(() => { if (isWB) fetchData() }, [fetchData, isWB])

  if (isOzon) return <OzonGeographyPage />

  const handleSelectProduct = (p: WBGeographySkuInfo) => {
    if (!selectedProducts.find(s => s.nm_id === p.nm_id)) {
      setSelectedProducts(prev => [...prev, p])
    }
  }

  const handleRemoveProduct = (nmId: number) => {
    setSelectedProducts(prev => prev.filter(p => p.nm_id !== nmId))
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
            Продажи по округам, регионам и городам — поиск по товарам
          </p>
        </div>
        <div className="flex items-center gap-2">
          {data && currentShop && (
            <button
              onClick={async () => {
                try {
                  await downloadGeoExcel({
                    shop_id: currentShop.id,
                    period,
                    marketplace: 'wildberries',
                  })
                } catch (e) {
                  console.error('Excel download failed:', e)
                }
              }}
              className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-emerald-600/15 text-emerald-400 hover:bg-emerald-600/25 transition-all"
            >
              <Download className="h-4 w-4" />
              Скачать Excel
            </button>
          )}
          <button
            onClick={fetchData}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            Обновить
          </button>
        </div>
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
                <ProductCombobox
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

          {currentShop && selectedProducts.length === 0 && (
            <GeographyAIInsight shopId={currentShop.id} period={period} />
          )}

          <OkrugsTable
            okrugs={data.regions}
            okrugTopProducts={data.okrug_top_products}
            shopId={currentShop!.id}
            period={period}
            selectedNmIds={selectedProducts.length > 0 ? selectedProducts.map(p => p.nm_id).join(',') : undefined}
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
