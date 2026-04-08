/**
 * ProductsPricesPage — Управление ценами, скидками, себестоимостью.
 *
 * WB: live-данные из WB API (цена до/после скидки, клубная, скидка %)
 * Ozon: данные из dim_ozon_products (price, marketing_price, min_price, price_index)
 *
 * Общее: себестоимость (редактируемая), остатки FBO/FBS, прибыль на шт.
 */
import { useState, useEffect, useCallback, useRef } from 'react'
import {
  Search,
  Package,
  AlertTriangle,
  TrendingUp,
  TrendingDown,
  Loader2,
  Check,
  X,
  Upload,
  Download,
  Info,
  Megaphone,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useAppStore } from '@/stores/appStore'
import {
  getWBPricesApi,
  updateWBCostApi,
  uploadWBCostExcelApi,
  downloadWBCostTemplate,
  type WBPriceProduct,
} from '@/api/wb-products'
import {
  getOzonProductsApi,
  updateOzonCostApi,
  uploadCostExcelApi,
  downloadCostTemplate,
  type OzonProduct,
} from '@/api/products'

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

function fmtMoney(v: number | null | undefined): string {
  const n = v ?? 0
  return n.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function fmtNum(v: number | null | undefined): string {
  return (v ?? 0).toLocaleString('ru-RU')
}



/* ═══════════════════════════════════════════════════════════
   Sort Header
   ═══════════════════════════════════════════════════════════ */

function SortTh({
  label, field, sort, order, onSort, className, info, align,
}: {
  label: string; field: string; sort: string; order: string
  onSort: (f: string) => void; className?: string; info?: string; align?: 'left' | 'right' | 'center'
}) {
  const active = sort === field
  const alignCls = align === 'right' ? 'justify-end text-right' : align === 'center' ? 'justify-center text-center' : 'justify-start text-left'
  return (
    <th
      className={cn(
        'px-1.5 py-2.5 text-[10px] uppercase tracking-wider font-semibold cursor-pointer select-none whitespace-nowrap hover:text-[hsl(var(--foreground))] transition-colors',
        align === 'right' && 'text-right',
        className,
      )}
      onClick={() => onSort(field)}
      title={info}
    >
      <span className={cn('inline-flex items-center gap-1', alignCls)}>
        {label}
        {info && <Info className="h-2.5 w-2.5 text-[hsl(var(--muted-foreground)/0.3)]" />}
        {active && (
          order === 'desc'
            ? <TrendingDown className="h-3 w-3 text-[hsl(var(--primary))]" />
            : <TrendingUp className="h-3 w-3 text-[hsl(var(--primary))]" />
        )}
      </span>
    </th>
  )
}

/* ═══════════════════════════════════════════════════════════
   Inline Cost Editor
   ═══════════════════════════════════════════════════════════ */

function CostEditor({ costPrice, onSave }: {
  costPrice: number; onSave: (cost: number) => Promise<void>
}) {
  const [editing, setEditing] = useState(false)
  const [val, setVal] = useState(costPrice.toString())
  const [saving, setSaving] = useState(false)
  const [closing, setClosing] = useState(false)
  const wrapRef = useRef<HTMLDivElement>(null)

  const close = useCallback(() => {
    setClosing(true)
    setTimeout(() => { setEditing(false); setClosing(false) }, 150)
  }, [])

  const save = async () => {
    const n = parseFloat(val)
    if (isNaN(n) || n < 0) return
    setSaving(true)
    try {
      await onSave(n)
      close()
    } catch { /* silent */ } finally { setSaving(false) }
  }

  useEffect(() => {
    if (!editing) return
    const h = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) close()
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [editing, close])

  const openEditor = () => {
    setVal(costPrice > 0 ? costPrice.toString() : '')
    setEditing(true)
    setClosing(false)
  }

  return (
    <div className="relative inline-flex" ref={wrapRef}>
      {costPrice === 0 ? (
        <button
          onClick={openEditor}
          className="inline-flex items-center gap-1 rounded-lg px-2 py-1 text-[11px] font-semibold bg-amber-500/12 text-amber-400 hover:bg-amber-500/20 transition-colors border border-amber-500/15"
        >
          <AlertTriangle className="h-3 w-3" />
          Указать
        </button>
      ) : (
        <button
          onClick={openEditor}
          className="text-sm font-medium text-[hsl(var(--foreground)/0.8)] hover:text-[hsl(var(--primary))] transition-colors cursor-pointer"
        >
          {fmtMoney(costPrice)}
        </button>
      )}

      {editing && (
        <div
          className={cn(
            'absolute right-0 top-full z-50 mt-1.5',
            'rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-2xl',
            'px-3 py-2.5',
            closing
              ? 'animate-[costPopOut_150ms_ease-in_forwards]'
              : 'animate-[costPopIn_200ms_ease-out_forwards]',
          )}
          style={{ minWidth: '180px' }}
        >
          <p className="text-[10px] font-semibold text-[hsl(var(--muted-foreground)/0.6)] uppercase tracking-wide mb-1.5">
            Себестоимость, ₽
          </p>
          <div className="flex items-center gap-1.5">
            <input
              type="number"
              className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-2.5 py-1.5 text-sm font-medium focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary)/0.3)] focus:border-[hsl(var(--primary)/0.5)] transition-all [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
              placeholder="0"
              value={val}
              onChange={(e) => setVal(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') save(); if (e.key === 'Escape') close() }}
              autoFocus
            />
            <button
              onClick={save}
              disabled={saving}
              className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-emerald-500/15 text-emerald-400 hover:bg-emerald-500/25 transition-colors"
            >
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Check className="h-4 w-4" />}
            </button>
            <button
              onClick={close}
              className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg hover:bg-white/8 text-[hsl(var(--muted-foreground)/0.5)] hover:text-[hsl(var(--muted-foreground))] transition-colors"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}

      <style>{`
        @keyframes costPopIn {
          from { opacity: 0; transform: translateY(-4px) scale(0.96); }
          to   { opacity: 1; transform: translateY(0) scale(1); }
        }
        @keyframes costPopOut {
          from { opacity: 1; transform: translateY(0) scale(1); }
          to   { opacity: 0; transform: translateY(-4px) scale(0.96); }
        }
      `}</style>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Unified Price Row type
   ═══════════════════════════════════════════════════════════ */

interface PriceRow {
  id: string              // unique key (nm_id or offer_id)
  nm_id?: number          // WB only
  vendor_code: string
  offer_id?: string       // Ozon only
  name: string
  image_url: string
  // Prices
  price_before_discount: number  // Цена до скидки
  discount_pct: number           // Скидка %
  price_after_discount: number   // Цена со скидкой
  club_price: number | null      // WB Клуб цена (null if club not active)
  club_discount: number | null   // WB Клуб скидка %
  min_price: number | null       // Ozon min_price
  price_index: number | null     // Ozon price_index_value
  price_index_color: string | null // Ozon price_index_color
  // Cost
  cost_price: number
  packaging_cost: number
  // Profit
  profit_per_unit: number | null     // без рекламы
  profit_source?: 'finance' | 'estimated' | null
  // Ads
  ad_spend_30d: number
  drr: number | null
  profit_with_ads: number | null     // с рекламой
  // Stocks
  stock_fbo: number
  stock_fbs: number
  // Warnings
  is_bad_turnover: boolean
}

function normalizeWB(p: WBPriceProduct): PriceRow {
  return {
    id: `wb-${p.nm_id}`,
    nm_id: p.nm_id,
    vendor_code: p.vendor_code,
    name: p.name,
    image_url: p.image_url || '',
    price_before_discount: p.price,
    discount_pct: p.discount,
    price_after_discount: p.discounted_price,
    club_price: p.club_discounted_price || null,
    club_discount: p.club_discount || null,
    min_price: null,
    price_index: null,
    price_index_color: null,
    cost_price: p.cost_price,
    packaging_cost: p.packaging_cost,
    profit_per_unit: p.profit_per_unit,
    profit_source: p.profit_source || null,
    ad_spend_30d: p.ad_spend_30d ?? 0,
    drr: p.drr ?? null,
    profit_with_ads: p.profit_with_ads ?? null,
    stock_fbo: p.stock_fbo,
    stock_fbs: p.stock_fbs,
    is_bad_turnover: p.is_bad_turnover,
  }
}

function normalizeOzon(p: OzonProduct): PriceRow {
  const cost = p.cost_price + p.packaging_cost
  let profit: number | null = null
  if (cost > 0 && p.price > 0) {
    const estimatedFees = p.price * (p.mp_fees_percent / 100 || 0.25)
    profit = Math.round(p.price - cost - estimatedFees)
  }

  const discountPct = p.old_price > 0 && p.price > 0
    ? Math.round((1 - p.price / p.old_price) * 100)
    : 0

  return {
    id: `ozon-${p.offer_id}`,
    offer_id: p.offer_id,
    vendor_code: p.offer_id,
    name: p.name,
    image_url: p.image_url || '',
    price_before_discount: p.marketing_price || p.old_price || p.price,
    discount_pct: discountPct,
    price_after_discount: p.price,
    club_price: null,
    club_discount: null,
    min_price: p.min_price || null,
    price_index: p.price_index_value || null,
    price_index_color: p.price_index_color || null,
    cost_price: p.cost_price,
    packaging_cost: p.packaging_cost,
    profit_per_unit: profit,
    ad_spend_30d: 0,
    drr: null,
    profit_with_ads: null,
    stock_fbo: p.stocks_fbo,
    stock_fbs: p.stocks_fbs,
    is_bad_turnover: false,
  }
}

/* ═══════════════════════════════════════════════════════════
   Price Index Badge (Ozon)
   ═══════════════════════════════════════════════════════════ */

function PriceIndexBadge({ value, color }: { value: number; color: string | null }) {
  const colorMap: Record<string, string> = {
    PROFIT: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/20',
    AVG_PROFIT: 'bg-blue-500/15 text-blue-400 border-blue-500/20',
    NON_PROFIT: 'bg-red-500/15 text-red-400 border-red-500/20',
  }
  const cls = colorMap[color || ''] || 'bg-gray-500/15 text-gray-400 border-gray-500/20'
  return (
    <span className={cn('inline-flex items-center rounded-md px-1.5 py-0.5 text-[10px] font-bold border', cls)}>
      {value.toFixed(2)}
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page Component
   ═══════════════════════════════════════════════════════════ */

export default function ProductsPricesPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isWB = currentShop?.marketplace === 'wildberries'
  const isOzon = currentShop?.marketplace === 'ozon'
  const shopId = currentShop?.id

  const [rows, setRows] = useState<PriceRow[]>([])
  const [total, setTotal] = useState(0)
  const [costMissing, setCostMissing] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [hasMore, setHasMore] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const perPage = 50
  const [sort, setSort] = useState('name')
  const [order, setOrder] = useState<'asc' | 'desc'>('asc')
  const [search, setSearch] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const [uploading, setUploading] = useState(false)
  const [hoverImg, setHoverImg] = useState<{ url: string; x: number; y: number } | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const sentinelRef = useRef<HTMLDivElement>(null)
  const loadingMoreRef = useRef(false)
  const pageRef = useRef(1)

  // Detect if any row has WB Club active (to show/hide column)
  const hasClubActive = isWB && rows.some(r => r.club_price !== null && r.club_discount !== null && r.club_discount > 0)
  // Detect if any row has ads
  const hasAnyAds = rows.some(r => r.ad_spend_30d > 0)

  // ── Fetch data ──
  const fetchData = useCallback(async () => {
    if (!shopId) return
    setLoading(true)
    setError(null)
    try {
      if (isWB) {
        const data = await getWBPricesApi({
          shop_id: shopId, search, sort, order, page: 1, per_page: perPage,
        })
        setRows(data.products.map(normalizeWB))
        setTotal(data.total)
        setCostMissing(data.cost_missing_count)
        setHasMore(data.products.length < data.total)
      } else if (isOzon) {
        const data = await getOzonProductsApi({
          shop_id: shopId, search, sort: sort === 'price_after_discount' ? 'price' : sort,
          order, page: 1, per_page: perPage,
        })
        setRows(data.products.map(normalizeOzon))
        setTotal(data.total)
        setCostMissing(data.cost_missing_count)
        setHasMore(data.products.length < data.total)
      }
      pageRef.current = 1
    } catch (e: any) {
      console.error('Failed to fetch prices', e)
      const detail = e?.response?.data?.detail || ''
      if (detail.includes('API ключ') || detail.includes('API key') || e?.response?.status === 400) {
        setError('API ключ не настроен для этого магазина. Добавьте ключ в разделе Настройки → Магазины.')
      } else if (e?.response?.status === 502) {
        setError('Ошибка подключения к API маркетплейса. Попробуйте позже.')
      } else {
        setError('Не удалось загрузить цены. Попробуйте обновить страницу.')
      }
      setRows([])
      setTotal(0)
    } finally {
      setLoading(false)
    }
  }, [shopId, isWB, isOzon, search, sort, order, perPage])

  // Load more (infinite scroll)
  const loadMore = useCallback(async () => {
    if (!shopId || loadingMoreRef.current || !hasMore) return
    loadingMoreRef.current = true
    setLoadingMore(true)
    const nextPage = pageRef.current + 1
    try {
      if (isWB) {
        const data = await getWBPricesApi({
          shop_id: shopId, search, sort, order, page: nextPage, per_page: perPage,
        })
        setRows(prev => {
          const existingIds = new Set(prev.map(p => p.id))
          const newItems = data.products.map(normalizeWB).filter(p => !existingIds.has(p.id))
          return [...prev, ...newItems]
        })
        setHasMore(data.products.length === perPage)
      } else if (isOzon) {
        const data = await getOzonProductsApi({
          shop_id: shopId, search,
          sort: sort === 'price_after_discount' ? 'price' : sort,
          order, page: nextPage, per_page: perPage,
        })
        setRows(prev => {
          const existingIds = new Set(prev.map(p => p.id))
          const newItems = data.products.map(normalizeOzon).filter(p => !existingIds.has(p.id))
          return [...prev, ...newItems]
        })
        setHasMore(data.products.length === perPage)
      }
      pageRef.current = nextPage
    } catch (e) {
      console.error('loadMore error', e)
    } finally {
      setLoadingMore(false)
      loadingMoreRef.current = false
    }
  }, [shopId, isWB, isOzon, search, sort, order, perPage, hasMore])

  useEffect(() => { fetchData() }, [fetchData])

  // Infinite scroll observer
  useEffect(() => {
    if (!sentinelRef.current) return
    const obs = new IntersectionObserver((entries) => {
      if (entries[0].isIntersecting) loadMore()
    }, { threshold: 0.1 })
    obs.observe(sentinelRef.current)
    return () => obs.disconnect()
  }, [loadMore])

  // Debounced search
  useEffect(() => {
    const t = setTimeout(() => setSearch(searchInput), 400)
    return () => clearTimeout(t)
  }, [searchInput])

  function toggleSort(field: string) {
    if (sort === field) {
      setOrder(o => o === 'desc' ? 'asc' : 'desc')
    } else {
      setSort(field)
      setOrder('desc')
    }
  }

  // ── Cost update (universal) ──
  const handleCostSave = async (row: PriceRow, cost: number) => {
    if (isWB) {
      await updateWBCostApi({ shop_id: shopId!, vendor_code: row.vendor_code, cost_price: cost })
    } else if (isOzon) {
      await updateOzonCostApi({ shop_id: shopId!, offer_id: row.offer_id || row.vendor_code, cost_price: cost })
    }
    setRows(prev => prev.map(r =>
      r.id === row.id ? { ...r, cost_price: cost } : r
    ))
  }

  // ── Excel upload ──
  const handleExcelUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file || !shopId) return
    setUploading(true)
    try {
      const res = isWB
        ? await uploadWBCostExcelApi(shopId, file)
        : await uploadCostExcelApi(shopId, file)
      if (res.ok) {
        alert(`✅ Загружено: ${res.updated} товаров`)
        fetchData()
      }
      if (res.errors?.length) console.warn('Excel errors:', res.errors)
    } catch {
      alert('Ошибка загрузки файла')
    } finally {
      setUploading(false)
      if (fileInputRef.current) fileInputRef.current.value = ''
    }
  }

  const handleDownloadTemplate = () => {
    if (!shopId) return
    isWB ? downloadWBCostTemplate(shopId) : downloadCostTemplate(shopId)
  }

  // Image URL helper
  const getImageUrl = (row: PriceRow) => {
    if (row.image_url) return row.image_url
    return ''
  }

  // ── Not a valid shop ──
  if (!currentShop || (!isWB && !isOzon)) {
    return (
      <div className="flex flex-col items-center justify-center h-[60vh] gap-4 text-center">
        <Package className="h-12 w-12 text-[hsl(var(--muted-foreground)/0.3)]" />
        <p className="text-[hsl(var(--muted-foreground)/0.7)]">
          Выберите магазин для управления ценами
        </p>
      </div>
    )
  }

  const colSpan = 8 + (hasClubActive ? 1 : 0) + (isOzon ? 2 : 0) + (hasAnyAds && isWB ? 2 : 0)

  return (
    <div className="space-y-4">

      {/* ── Header ──────────────────────────────────── */}
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Цены</h1>
          <p className="mt-0.5 text-sm text-[hsl(var(--muted-foreground))]">
            Управление ценами, скидками и себестоимостью товаров
            {total > 0 && (
              <span className="ml-2 text-[hsl(var(--muted-foreground)/0.5)]">
                · {total} товаров
              </span>
            )}
            {costMissing > 0 && (
              <span className="ml-2 text-amber-400">
                · у {costMissing} не указана с/с
              </span>
            )}
          </p>
        </div>

        {/* Actions */}
        <div className="flex items-center gap-2">
          <button
            title="Скачать шаблон Excel"
            onClick={handleDownloadTemplate}
            className="flex h-9 items-center gap-1.5 rounded-xl border border-[hsl(var(--border))] px-3 text-sm font-medium hover:bg-white/5 transition-colors text-[hsl(var(--muted-foreground)/0.7)] hover:text-[hsl(var(--foreground))]"
          >
            <Download className="h-4 w-4" />
            <span className="hidden sm:inline">Шаблон</span>
          </button>

          <label title={uploading ? 'Загрузка...' : 'Загрузить Excel с себестоимостью'}>
            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx,.xls"
              className="hidden"
              onChange={handleExcelUpload}
            />
            <div className={cn(
              'flex h-9 cursor-pointer items-center gap-1.5 rounded-xl border border-[hsl(var(--border))] px-3 text-sm font-medium transition-colors',
              uploading
                ? 'text-[hsl(var(--primary))]'
                : 'text-[hsl(var(--muted-foreground)/0.7)] hover:text-[hsl(var(--foreground))] hover:bg-white/5'
            )}>
              {uploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
              <span className="hidden sm:inline">Загрузить С/с</span>
            </div>
          </label>
        </div>
      </div>

      {/* ── Search ───────────────────────────────────── */}
      <div className="flex items-center gap-2">
        <div className="flex items-center gap-1.5 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] pl-3 pr-2 flex-1 max-w-md">
          <Search className="h-3.5 w-3.5 text-[hsl(var(--muted-foreground)/0.5)]" />
          <input
            type="text"
            placeholder="Артикул или название..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            className="bg-transparent py-2 text-sm text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.4)] focus:outline-none w-full"
          />
          {searchInput && (
            <button onClick={() => setSearchInput('')} className="p-0.5 rounded hover:bg-white/5">
              <X className="h-3.5 w-3.5 text-[hsl(var(--muted-foreground)/0.5)]" />
            </button>
          )}
        </div>
      </div>

      {/* ── Table ────────────────────────────────────── */}
      <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="overflow-auto max-h-[calc(100vh-220px)]">
          <table className="w-full min-w-[860px]" style={{ borderCollapse: 'collapse', tableLayout: 'auto' }}>
            <thead className="sticky top-0 z-30 bg-[hsl(var(--card))]" style={{ boxShadow: '0 1px 0 hsl(var(--border))' }}>
              <tr className="bg-[hsl(var(--card))]">
                <th className="pl-3 pr-1 py-2.5 text-left text-[10px] uppercase tracking-wider font-semibold text-[hsl(var(--muted-foreground))]" style={{ width: '35%' }}>
                  Товар
                </th>
                <SortTh
                  label="До скидки"
                  field="price"
                  sort={sort} order={order} onSort={toggleSort}
                  align="right"
                  info="Базовая цена до применения скидок"
                />
                <th className="px-1 py-2.5 text-center text-[10px] uppercase tracking-wider font-semibold text-[hsl(var(--muted-foreground))]">
                  Скидка
                </th>
                <SortTh
                  label="Со скидкой"
                  field="discounted_price"
                  sort={sort} order={order} onSort={toggleSort}
                  align="right"
                  info="Фактическая цена для покупателя"
                />
                {hasClubActive && (
                  <th className="px-1.5 py-2.5 text-right text-[10px] uppercase tracking-wider font-semibold text-[hsl(var(--muted-foreground))]">
                    WB Клуб
                  </th>
                )}
                {isOzon && (
                  <SortTh
                    label="Мин. цена"
                    field="min_price"
                    sort={sort} order={order} onSort={toggleSort}
                    align="right"
                    info="Минимальная допустимая цена Ozon"
                  />
                )}
                <SortTh
                  label="С/с"
                  field="cost_price"
                  sort={sort} order={order} onSort={toggleSort}
                  align="right"
                  info="Себестоимость + упаковка (редактируемая)"
                />
                {hasAnyAds && isWB && (
                  <th className="px-1.5 py-2.5 text-right text-[10px] uppercase tracking-wider font-semibold text-[hsl(var(--muted-foreground))]" title="Доля рекламных расходов (30 дней)">
                    ДРР
                  </th>
                )}
                <SortTh
                  label="Прибыль/шт"
                  field="profit_per_unit"
                  sort={sort} order={order} onSort={toggleSort}
                  align="right"
                  info="Прибыль на единицу: без рекламы / с рекламой"
                />
                <th className="px-1.5 py-2.5 text-right text-[10px] uppercase tracking-wider font-semibold text-[hsl(var(--muted-foreground))]">FBO</th>
                <th className="pl-1.5 pr-3 py-2.5 text-right text-[10px] uppercase tracking-wider font-semibold text-[hsl(var(--muted-foreground))]">FBS</th>
                {isOzon && (
                  <th className="px-2 py-2.5 text-center text-[10px] uppercase tracking-wider font-semibold text-[hsl(var(--muted-foreground))]" title="Price Index Ozon">
                    PI
                  </th>
                )}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={colSpan} className="py-20 text-center">
                    <Loader2 className="h-6 w-6 animate-spin mx-auto text-[hsl(var(--primary)/0.6)]" />
                    <p className="mt-2 text-sm text-[hsl(var(--muted-foreground)/0.5)]">
                      {isWB ? 'Загрузка цен из WB API...' : 'Загрузка цен...'}
                    </p>
                  </td>
                </tr>
              ) : error ? (
                <tr>
                  <td colSpan={colSpan} className="py-20 text-center">
                    <AlertTriangle className="h-10 w-10 mx-auto mb-3 text-amber-400/60" />
                    <p className="text-sm font-medium text-[hsl(var(--foreground)/0.8)] mb-1">
                      {error}
                    </p>
                    <a
                      href="/settings"
                      className="inline-flex items-center gap-1 mt-2 text-xs font-medium text-[hsl(var(--primary))] hover:underline"
                    >
                      Перейти в Настройки →
                    </a>
                  </td>
                </tr>
              ) : rows.length === 0 ? (
                <tr>
                  <td colSpan={colSpan} className="py-20 text-center text-[hsl(var(--muted-foreground)/0.5)]">
                    <Package className="h-8 w-8 mx-auto mb-2 opacity-30" />
                    Нет данных
                  </td>
                </tr>
              ) : (
                rows.map((row) => (
                  <tr
                    key={row.id}
                    className={cn(
                      'border-b border-[hsl(var(--border)/0.15)] hover:bg-white/[0.025] group transition-colors',
                      row.is_bad_turnover && 'bg-amber-500/[0.03]',
                    )}
                  >
                    {/* ── Товар (compact) ── */}
                    <td className="pl-3 pr-1 py-1.5" style={{ maxWidth: '260px' }}>
                      <div className="flex items-center gap-2">
                        {getImageUrl(row) ? (
                          <div
                            className="relative shrink-0 cursor-pointer"
                            onMouseEnter={(e) => {
                              const r = e.currentTarget.getBoundingClientRect()
                              setHoverImg({ url: getImageUrl(row), x: r.right + 12, y: r.top - 40 })
                            }}
                            onMouseLeave={() => setHoverImg(null)}
                          >
                            <img
                              src={getImageUrl(row)}
                              alt=""
                              className="h-10 w-10 rounded-lg object-cover bg-[hsl(var(--muted)/0.1)]"
                              loading="lazy"
                              onError={(e) => { (e.target as HTMLImageElement).style.visibility = 'hidden' }}
                            />
                          </div>
                        ) : (
                          <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-[hsl(var(--muted)/0.1)]">
                            <Package className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.2)]" />
                          </div>
                        )}
                        <div className="min-w-0">
                          <p className="text-[12px] font-medium leading-tight line-clamp-2" title={row.name}>
                            {row.name || row.vendor_code}
                          </p>
                          <p className="text-[10px] text-[hsl(var(--foreground)/0.55)] font-mono">
                            {row.vendor_code}
                          </p>
                          {row.is_bad_turnover && (
                            <span className="inline-flex items-center gap-0.5 text-[9px] font-bold text-amber-400">
                              <AlertTriangle className="h-2.5 w-2.5" />
                              Низкая оборач.
                            </span>
                          )}
                        </div>
                      </div>
                    </td>

                    {/* ── Цена до скидки ── */}
                    <td className="pl-1 pr-0.5 py-1.5 text-right whitespace-nowrap">
                      <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.6)] line-through decoration-[hsl(var(--muted-foreground)/0.35)]">
                        {fmtMoney(row.price_before_discount)}
                      </span>
                    </td>

                    {/* ── Скидка ── */}
                    <td className="px-0.5 py-1.5 text-center">
                      {row.discount_pct > 0 ? (
                        <span className="inline-flex items-center rounded-md px-1.5 py-0.5 text-[11px] font-bold bg-red-500/12 text-red-400 border border-red-500/15">
                          −{row.discount_pct}%
                        </span>
                      ) : (
                        <span className="text-[hsl(var(--muted-foreground)/0.3)]">—</span>
                      )}
                    </td>

                    {/* ── Цена со скидкой ── */}
                    <td className="pl-0.5 pr-2 py-1.5 text-right whitespace-nowrap">
                      <span className="text-[14px] font-bold text-[hsl(var(--foreground))]">
                        {fmtMoney(row.price_after_discount)}
                      </span>
                    </td>

                    {/* ── WB Club ── */}
                    {hasClubActive && (
                      <td className="px-2 py-1.5 text-right">
                        {row.club_price && row.club_discount && row.club_discount > 0 ? (
                          <div className="flex flex-col items-end gap-0">
                            <span className="text-[13px] font-medium text-violet-400">
                              {fmtMoney(row.club_price)}
                            </span>
                            <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.4)]">
                              −{row.club_discount}%
                            </span>
                          </div>
                        ) : (
                          <span className="text-[hsl(var(--muted-foreground)/0.3)]">—</span>
                        )}
                      </td>
                    )}

                    {/* ── Ozon min price ── */}
                    {isOzon && (
                      <td className="px-2 py-1.5 text-right">
                        {row.min_price ? (
                          <span className="text-[13px] font-medium text-[hsl(var(--foreground)/0.7)]">
                            {fmtMoney(row.min_price)}
                          </span>
                        ) : (
                          <span className="text-[hsl(var(--muted-foreground)/0.3)]">—</span>
                        )}
                      </td>
                    )}

                    {/* ── С/с ── */}
                    <td className="px-2 py-1.5 text-right whitespace-nowrap">
                      <CostEditor
                        costPrice={row.cost_price}
                        onSave={(cost) => handleCostSave(row, cost)}
                      />
                    </td>

                    {/* ── ДРР (30д) ── */}
                    {hasAnyAds && isWB && (
                      <td className="px-2 py-1.5 text-right whitespace-nowrap">
                        {row.drr !== null ? (
                          <span className={cn(
                            'text-[14px] font-bold tabular-nums',
                            row.drr > 30 ? 'text-red-400' : row.drr > 15 ? 'text-amber-400' : 'text-emerald-400'
                          )}>
                            {row.drr}%
                          </span>
                        ) : (
                          <span className="text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                        )}
                      </td>
                    )}

                    {/* ── Прибыль/шт (с рекламой / без) ── */}
                    <td className="px-2 py-1.5 text-right whitespace-nowrap">
                      {row.profit_per_unit !== null ? (
                        <div className="flex flex-col items-end gap-0.5">
                          {/* Прибыль с рекламой (основная) */}
                          {row.profit_with_ads !== null ? (
                            <>
                              <div className="flex items-center gap-1">
                                <Megaphone className="h-3 w-3 text-violet-400/70" />
                                <span className={cn(
                                  'text-[15px] font-bold tabular-nums',
                                  row.profit_with_ads < 0 ? 'text-red-400' : row.profit_with_ads < 50 ? 'text-amber-400' : 'text-emerald-400'
                                )}>
                                  {row.profit_with_ads > 0 ? '+' : ''}{fmtMoney(row.profit_with_ads)}
                                </span>
                              </div>
                              <span className="text-[13px] tabular-nums text-[hsl(var(--foreground)/0.6)]">
                                б/р {row.profit_per_unit > 0 ? '+' : ''}{fmtMoney(row.profit_per_unit)} ₽
                              </span>
                            </>
                          ) : (
                            <>
                              <span className={cn(
                                'text-[15px] font-bold tabular-nums',
                                row.profit_per_unit < 0 ? 'text-red-400' : row.profit_per_unit < 50 ? 'text-amber-400' : 'text-emerald-400'
                              )}>
                                {row.profit_per_unit > 0 ? '+' : ''}{fmtMoney(row.profit_per_unit)}
                              </span>
                              {row.profit_source === 'finance' && (
                                <span className="text-[10px] text-[hsl(var(--foreground)/0.4)]">без рекл.</span>
                              )}
                              {row.profit_source === 'estimated' && (
                                <span className="text-[10px] text-[hsl(var(--foreground)/0.4)]">≈ оценка</span>
                              )}
                            </>
                          )}
                        </div>
                      ) : (
                        <span className="text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── FBO ── */}
                    <td className="px-1.5 py-1.5 text-right">
                      <span className={cn(
                        'text-[13px] font-semibold tabular-nums',
                        row.stock_fbo === 0 ? 'text-red-400' : 'text-[hsl(var(--foreground)/0.8)]'
                      )}>
                        {fmtNum(row.stock_fbo)}
                      </span>
                    </td>

                    {/* ── FBS ── */}
                    <td className="pl-1.5 pr-3 py-1.5 text-right">
                      <span className={cn(
                        'text-[13px] font-semibold tabular-nums',
                        row.stock_fbs === 0 ? 'text-[hsl(var(--muted-foreground)/0.3)]' : 'text-[hsl(var(--foreground)/0.8)]'
                      )}>
                        {fmtNum(row.stock_fbs)}
                      </span>
                    </td>

                    {/* ── Ozon Price Index ── */}
                    {isOzon && (
                      <td className="px-2 py-2.5 text-center">
                        {row.price_index ? (
                          <PriceIndexBadge value={row.price_index} color={row.price_index_color} />
                        ) : (
                          <span className="text-[hsl(var(--muted-foreground)/0.3)]">—</span>
                        )}
                      </td>
                    )}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Infinite scroll sentinel */}
        <div ref={sentinelRef} className="h-2" />
        {loadingMore && (
          <div className="flex justify-center py-4">
            <Loader2 className="h-5 w-5 animate-spin text-[hsl(var(--primary)/0.5)]" />
          </div>
        )}
      </div>

      {/* Image hover preview */}
      {hoverImg && (
        <div
          className="pointer-events-none fixed z-50 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1.5 shadow-2xl"
          style={{ left: hoverImg.x, top: hoverImg.y }}
        >
          <img
            src={hoverImg.url}
            alt=""
            className="h-[200px] w-[150px] rounded-lg object-cover"
            onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
          />
        </div>
      )}
    </div>
  )
}
