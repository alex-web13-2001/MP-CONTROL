/**
 * Products Page — Каталог товаров с аналитикой.
 *
 * UI/UX v3 — Бизнес-подход:
 *  Приоритет данных для селлера:
 *   1. Идентификация товара (фото 3:4, название, артикул)
 *   2. Выручка и динамика продаж (главный KPI)
 *   3. Остатки (хватит ли?)
 *   4. Маржинальность (зарабатываю ли?)
 *   5. Реклама DRR (эффективна ли?)
 *   6. Возвраты % (качество)
 *   7. Цена (контроль)
 *   8. События (информационно)
 */
import { useState, useEffect, useCallback, useRef } from 'react'
import {
  Search,
  Package,
  AlertTriangle,
  Loader2,
  X,
  Upload,
  Download,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useAppStore } from '@/stores/appStore'
import {
  getOzonProductsApi,
  uploadCostExcelApi,
  downloadCostTemplate,
  type OzonProduct,
  type ProductEvent,
} from '@/api/products'
import {
  getWBProductsApi,
  uploadWBCostExcelApi,
  downloadWBCostTemplate,
  type WBProduct,
} from '@/api/wb-products'
import { PeriodSelector, type PeriodValue } from '@/components/DateRangePicker'

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

function fmtMoney(v: number | null | undefined): string {
  const n = v ?? 0
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace('.0', '') + 'M ₽'
  return n.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function fmtNum(v: number | null | undefined): string {
  return (v ?? 0).toLocaleString('ru-RU')
}

/** WB CDN image URL by nm_id */
function wbImageUrl(nmId: number): string {
  const vol = Math.floor(nmId / 100000)
  const part = Math.floor(nmId / 1000)
  const basket = String((vol % 17) + 1).padStart(2, '0')
  return `https://basket-${basket}.wbbasket.ru/vol${vol}/part${part}/${nmId}/images/small/1.webp`
}

/** Convert WBProduct → OzonProduct shape for unified table rendering */
function wbToOzon(p: WBProduct): OzonProduct {
  const grossProfit = p.gross_profit ?? null
  // Profit % from sales amount (selling price × qty), not from payout
  const grossProfitPct = (grossProfit !== null && p.sales_amount > 0)
    ? Math.round(grossProfit / p.sales_amount * 100)
    : null
  // Cost % = cost / selling price × 100 (always positive)
  const costPct = (p.cost_price > 0 && p.current_price > 0)
    ? Math.round((p.cost_price + (p.packaging_cost || 0)) / p.current_price * 1000) / 10
    : null
  return {
    product_id: p.nm_id,
    offer_id: p.vendor_code,
    sku: p.nm_id,
    name: p.name || p.vendor_code,
    barcode: null,
    image_url: p.image_url || wbImageUrl(p.nm_id),
    price: p.current_price,
    old_price: 0,
    min_price: 0,
    marketing_price: p.current_price,
    stocks_fbo: p.stock_fbo,
    stocks_fbs: p.stock_fbs,
    price_index_color: '',
    price_index_value: 0,
    competitor_min_price: 0,
    status: 'active',
    status_name: '',
    is_archived: false,
    volume_weight: 0,
    model_count: 0,
    images_count: 0,
    cost_price: p.cost_price,
    packaging_cost: p.packaging_cost,
    orders_7d: p.orders_7d,
    revenue_7d: p.revenue_7d,
    orders_prev_7d: p.orders_prev ?? 0,
    revenue_delta: p.revenue_delta,
    ad_spend_7d: p.ad_spend_7d,
    drr: p.drr,
    returns_30d: 0,
    orders_30d: p.orders_7d,
    content_rating: 0,
    commission_percent: 0,
    fbo_logistics: 0,
    margin: p.cost_price > 0 ? (p.cost_price + (p.packaging_cost || 0)) : null,
    margin_percent: costPct,
    payout_period: p.payout ?? 0,
    payout_prev: 0,
    gross_profit: grossProfit,
    gross_profit_percent: grossProfitPct,
    gross_profit_prev: null,
    gross_profit_delta: null,
    mp_fees: p.mp_fees ?? 0,
    mp_fees_percent: p.mp_fees_percent ?? 0,
    mp_fees_commission: 0,
    mp_fees_logistics: p.mp_fees_logistics ?? 0,
    mp_fees_storage: p.mp_fees_storage ?? 0,
    mp_fees_other: 0,
    mp_fees_deductions: p.mp_fees_deductions ?? 0,
    mp_fees_acceptance: p.mp_fees_acceptance ?? 0,
    mp_fees_fines: p.mp_fees_fines ?? 0,
    sales_amount: p.sales_amount ?? 0,
    avg_price: p.avg_price ?? 0,
    period: 7,
    events: [] as ProductEvent[],
    promotions: [] as string[],
    fees_source: p.fees_source,
  }
}

const FILTERS = [
  { key: 'all', label: 'Все' },
  { key: 'in_stock', label: 'В наличии' },
  { key: 'no_stock', label: 'Без остатков' },
  { key: 'with_ads', label: 'С рекламой' },
  { key: 'no_ads', label: 'Без рекламы' },
  { key: 'problems', label: 'Проблемные' },
  { key: 'archived', label: 'Архив' },
] as const

type FilterKey = typeof FILTERS[number]['key']









/* ═══════════════════════════════════════════════════════════
   Content Rating
   ═══════════════════════════════════════════════════════════ */

function ContentRating({ rating }: { rating: number }) {
  if (rating <= 0) return null
  // rating comes from backend already as percentage (e.g. 87.5)
  const pct = Math.round(rating)
  const c = pct >= 80 ? '#10b981' : pct >= 50 ? '#f59e0b' : '#ef4444'
  return (
    <span
      className="inline-flex items-center rounded px-1 py-[1px] text-[10px] font-bold leading-tight"
      style={{ background: c + '18', color: c }}
    >
      ★{pct}%
    </span>
  )
}

/* SortDropdown removed — sorting is in table headers */

/* ═══════════════════════════════════════════════════════════
   Main Page Component
   ═══════════════════════════════════════════════════════════ */

export default function ProductsPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isOzon = currentShop?.marketplace === 'ozon'
  const isWB = currentShop?.marketplace === 'wildberries'
  const shopId = currentShop?.id


  const [products, setProducts] = useState<OzonProduct[]>([])
  const [total, setTotal] = useState(0)
  const [costMissing, setCostMissing] = useState(0)
  const [apiTotals, setApiTotals] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [_page, setPage] = useState(1)
  const [hasMore, setHasMore] = useState(true)
  const perPage = 25
  const [sort, setSort] = useState('revenue_7d')
  const [order, setOrder] = useState<'asc' | 'desc'>('desc')
  const [filter, setFilter] = useState<FilterKey>('all')
  const [search, setSearch] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const [periodValue, setPeriodValue] = useState<PeriodValue>({
    mode: 'quick', period: 7, dateRange: null,
  })
  const [hoverImg, setHoverImg] = useState<{ url: string; x: number; y: number } | null>(null)
  const [uploading, setUploading] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const pageRef = useRef(1)
  const loadingMoreRef = useRef(false)

  // Fetch page 1 (reset)
  const fetchProducts = useCallback(async () => {
    if (!shopId || (!isOzon && !isWB)) return
    setLoading(true)
    const period = periodValue.period
    const dateParams = periodValue.mode === 'custom' && periodValue.dateRange?.from && periodValue.dateRange?.to ? {
      date_from: `${periodValue.dateRange.from.getFullYear()}-${String(periodValue.dateRange.from.getMonth() + 1).padStart(2, '0')}-${String(periodValue.dateRange.from.getDate()).padStart(2, '0')}`,
      date_to: `${periodValue.dateRange.to.getFullYear()}-${String(periodValue.dateRange.to.getMonth() + 1).padStart(2, '0')}-${String(periodValue.dateRange.to.getDate()).padStart(2, '0')}`,
    } : {}
    setProducts([])
    setTotal(0)
    setApiTotals(null)
    try {
      if (isWB) {
        const data = await getWBProductsApi({
          shop_id: shopId!, page: 1, per_page: perPage, sort, order, filter, search, period, ...dateParams,
        })
        setProducts(data.products.map(wbToOzon))
        setTotal(data.total)
        setCostMissing(data.cost_missing_count)
        setApiTotals(data.totals || null)
      } else {
        const data = await getOzonProductsApi({
          shop_id: shopId!, page: 1, per_page: perPage, sort, order, filter, search, period, ...dateParams,
        })
        setProducts(data.products)
        setTotal(data.total)
        setCostMissing(data.cost_missing_count)
        setApiTotals(data.totals || null)
      }
      pageRef.current = 1
      setPage(1)
      setHasMore(true)
    } catch (e) {
      console.error('Failed to fetch products', e)
    } finally {
      setLoading(false)
    }
  }, [shopId, isOzon, isWB, perPage, sort, order, filter, search, periodValue])

  // Load next page (append) — uses refs to prevent duplicate calls
  const loadMore = useCallback(async () => {
    if (!shopId || (!isOzon && !isWB) || loadingMoreRef.current || !hasMore) return
    loadingMoreRef.current = true
    setLoadingMore(true)
    const nextPage = pageRef.current + 1
    const period = periodValue.period
    const dateParams = periodValue.mode === 'custom' && periodValue.dateRange?.from && periodValue.dateRange?.to ? {
      date_from: `${periodValue.dateRange.from.getFullYear()}-${String(periodValue.dateRange.from.getMonth() + 1).padStart(2, '0')}-${String(periodValue.dateRange.from.getDate()).padStart(2, '0')}`,
      date_to: `${periodValue.dateRange.to.getFullYear()}-${String(periodValue.dateRange.to.getMonth() + 1).padStart(2, '0')}-${String(periodValue.dateRange.to.getDate()).padStart(2, '0')}`,
    } : {}
    try {
      let newProducts: OzonProduct[]
      let newTotal: number
      if (isWB) {
        const data = await getWBProductsApi({
          shop_id: shopId!, page: nextPage, per_page: perPage, sort, order, filter, search, period, ...dateParams,
        })
        newProducts = data.products.map(wbToOzon)
        newTotal = data.total
      } else {
        const data = await getOzonProductsApi({
          shop_id: shopId!, page: nextPage, per_page: perPage, sort, order, filter, search, period, ...dateParams,
        })
        newProducts = data.products
        newTotal = data.total
      }
      setProducts((prev) => {
        const existingIds = new Set(prev.map((p: OzonProduct) => p.offer_id))
        return [...prev, ...newProducts.filter((p) => !existingIds.has(p.offer_id))]
      })
      pageRef.current = nextPage
      setPage(nextPage)
      setHasMore(nextPage * perPage < newTotal)
    } catch (e) {
      console.error('Failed to load more', e)
    } finally {
      loadingMoreRef.current = false
      setLoadingMore(false)
    }
  }, [shopId, isOzon, isWB, perPage, sort, order, filter, search, periodValue, hasMore])

  useEffect(() => { fetchProducts() }, [fetchProducts])

  // Window scroll → load more when near bottom
  useEffect(() => {
    const handleScroll = () => {
      if (loadingMoreRef.current || !hasMore || loading) return
      const scrollBottom = window.innerHeight + window.scrollY
      const docHeight = document.documentElement.scrollHeight
      if (scrollBottom >= docHeight - 400) {
        loadMore()
      }
    }
    window.addEventListener('scroll', handleScroll, { passive: true })
    return () => window.removeEventListener('scroll', handleScroll)
  }, [loadMore, hasMore, loading])


  // Debounced search
  useEffect(() => {
    const t = setTimeout(() => setSearch(searchInput), 400)
    return () => clearTimeout(t)
  }, [searchInput])

  const toggleSort = (key: string) => {
    if (sort === key) setOrder((o) => (o === 'desc' ? 'asc' : 'desc'))
    else { setSort(key); setOrder('desc') }
  }

  const handleExcelUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file || !shopId) return
    setUploading(true)
    try {
      if (isWB) {
        const res = await uploadWBCostExcelApi(shopId, file)
        if (res.ok) fetchProducts()
      } else {
        const res = await uploadCostExcelApi(shopId, file)
        if (res.ok) fetchProducts()
      }
    } catch (err) {
      console.error('Excel upload failed', err)
    } finally {
      setUploading(false)
      if (fileInputRef.current) fileInputRef.current.value = ''
    }
  }




  /* ── Marketplace not selected ── */
  if (!isOzon && !isWB) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="text-center space-y-3">
          <Package className="mx-auto h-16 w-16 text-[hsl(var(--muted-foreground)/0.15)]" />
          <p className="text-lg font-medium text-[hsl(var(--muted-foreground))]">Выберите магазин</p>
          <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">Ozon или Wildberries</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* ── Header ── */}
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Ваши товары</h1>
          <p className="mt-0.5 text-sm text-[hsl(var(--muted-foreground))]">{total} товаров · Статистика за {periodValue.mode === 'custom' ? 'выбранный период' : periodValue.period === 7 ? 'неделю' : 'месяц'}</p>
        </div>
        {/* Search */}
        <div className="relative w-80">
          <Search className="absolute left-3.5 top-1/2 h-4 w-4 -translate-y-1/2 text-[hsl(var(--muted-foreground)/0.5)]" />
          <input
            type="text"
            placeholder="Поиск по названию, артикулу, SKU..."
            className="w-full rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] pl-10 pr-9 py-2 text-sm transition-all placeholder:text-[hsl(var(--muted-foreground)/0.4)] focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary)/0.25)] focus:border-[hsl(var(--primary)/0.4)]"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
          />
          {searchInput && (
            <button onClick={() => setSearchInput('')} className="absolute right-2.5 top-1/2 -translate-y-1/2 p-0.5 text-[hsl(var(--muted-foreground)/0.5)] hover:text-[hsl(var(--foreground))]">
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>

      {/* ── Period selector + Filters (dashboard-style) ── */}
      <div className="flex items-center justify-between gap-4">
        {/* Единый виджет периода */}
        <PeriodSelector
          value={periodValue}
          onChange={(v) => { setPeriodValue(v); setPage(1) }}
          popupAlign="left"
        />
        {/* Filters */}
        <div className="flex items-center gap-1.5 flex-wrap">
          {FILTERS.map((f) => (
            <button
              key={f.key}
              onClick={() => { setFilter(f.key); setPage(1) }}
              className={cn(
                'rounded-lg px-3.5 py-1.5 text-sm transition-all border',
                filter === f.key
                  ? 'bg-[hsl(var(--primary)/0.1)] border-[hsl(var(--primary)/0.3)] text-[hsl(var(--primary))] font-medium'
                  : 'border-transparent text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-white/5',
              )}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Cost Warning + Excel import ── */}
      {costMissing > 0 && (
        <div className="flex items-center gap-3 rounded-xl border border-amber-500/15 bg-amber-500/5 px-4 py-3">
          <AlertTriangle className="h-5 w-5 shrink-0 text-amber-400" />
          <div className="flex-1 text-sm">
            <span className="font-semibold text-amber-400">У {costMissing} товаров</span>{' '}
            <span className="text-[hsl(var(--muted-foreground))]">не указана себестоимость.</span>
          </div>
          <div className="flex items-center gap-2">
            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx,.xls"
              className="hidden"
              onChange={handleExcelUpload}
            />
            <button
              onClick={() => fileInputRef.current?.click()}
              disabled={uploading}
              className="inline-flex items-center gap-1.5 rounded-lg border border-amber-500/25 bg-amber-500/10 px-3 py-1.5 text-xs font-semibold text-amber-400 hover:bg-amber-500/20 transition-colors"
            >
              {uploading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Upload className="h-3.5 w-3.5" />}
              Загрузить Excel
            </button>
            {shopId && (
              <button
                onClick={() => isWB ? downloadWBCostTemplate(shopId) : downloadCostTemplate(shopId)}
                className="inline-flex items-center gap-1.5 rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-3 py-1.5 text-xs font-medium text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
              >
                <Download className="h-3.5 w-3.5" />
                Шаблон
              </button>
            )}
          </div>
        </div>
      )}

      {/* ─── 7-дней предупреждение ─── */}
      {periodValue.period === 7 && !loading && products.length > 0 && (
        <div className="flex items-start gap-2.5 rounded-lg border border-amber-500/20 bg-amber-500/5 px-4 py-2.5 mb-3">
          <svg className="mt-0.5 h-4 w-4 shrink-0 text-amber-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" /></svg>
          <p className="text-[12px] leading-relaxed text-amber-200/80">
            <span className="font-semibold text-amber-300">Данные за 7 дней могут быть неполными.</span>{' '}
            Финансовый отчёт маркетплейса формируется с задержкой 1–3 дня. Итоговые цифры по выплатам и удержаниям могут измениться после закрытия отчётного периода.
          </p>
        </div>
      )}

      {/* ═══════════════════════════════════════════════════
         Table — Business-priority columns:
         [Photo|Товар] sticky → Продажи → Остатки → С/с → DRR → Возвр% → Цена → Events
         ═══════════════════════════════════════════════════ */}
      <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="overflow-x-auto overflow-y-auto max-h-[calc(100vh-240px)]">
          <table className="w-full min-w-[1040px]" style={{ borderCollapse: 'collapse' }}>
            <thead className="sticky top-0 z-30" style={{ boxShadow: '0 1px 0 hsl(var(--border))' }}>
              <tr className="bg-[hsl(var(--card))]">
                <th className="sticky left-0 z-40 w-[280px] bg-[hsl(var(--card))] pl-4 pr-2 py-2.5 text-left text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  Товар
                </th>
                <th className="w-[70px] px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => toggleSort('price')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'price' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                      Цена {sort === 'price' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                    </button>
                    <span className="relative group/tip">
                      <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                      <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[200px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">Ваша цена из личного кабинета (до скидок площадки)</span>
                    </span>
                  </div>
                </th>
                <th className="w-[65px] px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => toggleSort('stocks')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'stocks' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                      Ост. {sort === 'stocks' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                    </button>
                    <span className="relative group/tip">
                      <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                      <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[160px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">Остатки на складах FBO и FBS</span>
                    </span>
                  </div>
                </th>
                <th className="w-[90px] px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => toggleSort('revenue_7d')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'revenue_7d' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                      Продажи {sort === 'revenue_7d' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                    </button>
                    <span className="relative group/tip">
                      <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                      <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[220px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">Цена из ЛК × кол-во проданных штук за выбранный период</span>
                    </span>
                  </div>
                </th>
                <th className="w-[90px] px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  <div className="flex items-center justify-end gap-1">
                    <span>Выплата</span>
                    <span className="relative group/tip">
                      <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                      <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[220px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">Сумма к перечислению за вычетом комиссии и эквайринга. Логистика, хранение и удержания вычитаются отдельно</span>
                    </span>
                  </div>
                </th>
                <th className="w-[70px] px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  <div className="flex items-center justify-end gap-1">
                    <span>Ср. выпл.</span>
                    <span className="relative group/tip">
                      <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                      <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[220px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">Средняя выплата за 1 единицу товара. Уже за вычетом комиссии, логистики и других удержаний маркетплейса</span>
                    </span>
                  </div>
                </th>
                <th className="w-[75px] px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => toggleSort('margin')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'margin' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                      С/с {sort === 'margin' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                    </button>
                    <span className="relative group/tip">
                      <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                      <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[200px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">Себестоимость + упаковка. Процент — доля С/с в цене из ЛК</span>
                    </span>
                  </div>
                </th>
                <th className="w-[80px] px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => toggleSort('drr')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'drr' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                      Реклама {sort === 'drr' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                    </button>
                    <span className="relative group/tip">
                      <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                      <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[220px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">Рекламные расходы за период. ДРР = расход / продажи × 100%</span>
                    </span>
                  </div>
                </th>
                <th className="w-[80px] px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  <div className="flex items-center justify-end gap-1">
                    <span>Услуги МП</span>
                    <span className="relative group/tip">
                      <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                      <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[240px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">Логистика + хранение + удержания + приёмка. Вычитаются из выплаты для расчёта прибыли</span>
                    </span>
                  </div>
                </th>
                <th className="w-[85px] px-2 py-2.5 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))]">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => toggleSort('gross_profit')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'gross_profit' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                      Прибыль {sort === 'gross_profit' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                    </button>
                    <span className="relative group/tip">
                      <span className="text-[10px] cursor-help text-[hsl(var(--muted-foreground)/0.4)] hover:text-[hsl(var(--muted-foreground))]">?</span>
                      <span className="absolute right-0 top-full mt-1 z-50 hidden group-hover/tip:block w-[240px] p-2 text-[11px] font-normal text-left rounded-lg bg-[hsl(var(--popover))] border border-[hsl(var(--border))] shadow-xl">Прибыль = Выплата − Услуги МП − С/с × шт − Реклама. Процент от продаж</span>
                    </span>
                  </div>
                </th>
              </tr>

              {/* ── Итого (данные с сервера по ВСЕМ товарам) ── */}
              {!loading && apiTotals && (() => {
                const t = apiTotals
                return (
                  <tr className="border-b border-[hsl(var(--border)/0.5)] bg-[hsl(var(--card))]">
                    <td className="sticky left-0 z-40 bg-[hsl(var(--card))] pl-4 pr-2 py-2">
                      <span className="text-[12px] font-bold text-[hsl(var(--foreground)/0.6)]">Σ {t.count} товаров</span>
                    </td>
                    <td className="px-2 py-2" />
                    <td className="px-2 py-2 text-right">
                      <p className="text-[13px] font-bold tabular-nums text-[hsl(var(--foreground)/0.7)]">{fmtNum(t.stocks)}</p>
                    </td>
                    <td className="px-2 py-2 text-right">
                      <p className="text-[13px] font-bold tabular-nums">{fmtMoney(t.sales ?? t.revenue)}</p>
                      <p className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">{fmtNum(t.orders)} шт</p>
                    </td>
                    <td className="px-2 py-2 text-right">
                      <p className="text-[13px] font-bold tabular-nums">{fmtMoney(t.payout ?? 0)}</p>
                    </td>
                    <td className="px-2 py-2 text-right">
                      {t.avg_price > 0 && (
                        <p className="text-[13px] font-bold tabular-nums text-[hsl(var(--foreground)/0.7)]">{fmtMoney(Math.round(t.avg_price))}</p>
                      )}
                    </td>
                    <td className="px-2 py-2 text-right">
                      {t.total_cogs > 0 && (
                        <div>
                          <p className="text-[13px] font-bold tabular-nums text-[hsl(var(--foreground)/0.7)]">{fmtMoney(t.total_cogs)}</p>
                          <p className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)] tabular-nums">
                            {fmtNum(t.orders)} шт{t.orders > 0 ? ` × ${fmtMoney(Math.round(t.total_cogs / t.orders))}/шт` : ''}
                          </p>
                        </div>
                      )}
                    </td>
                    <td className="px-2 py-2 text-right">
                      <p className="text-[13px] font-bold tabular-nums">{fmtMoney(t.ad_spend)}</p>
                      {t.drr > 0 && <p className={cn('text-[11px] font-semibold', t.drr > 20 ? 'text-red-400' : t.drr > 10 ? 'text-amber-400' : 'text-emerald-400')}>ДРР {t.drr}%</p>}
                    </td>
                    <td className="px-2 py-2 text-right">
                      <p className="text-[13px] font-bold tabular-nums">{fmtMoney(t.mp_fees)}</p>
                      <p className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">{t.mp_fees_pct}%</p>
                    </td>
                    <td className="px-2 py-2 text-right">
                      {t.profit_count > 0 ? (
                        <>
                          <p className={cn('text-[13px] font-bold tabular-nums', t.profit > 0 ? 'text-emerald-400' : 'text-red-400')}>
                            {t.profit > 0 ? '+' : ''}{fmtMoney(t.profit)}
                          </p>
                          <p className={cn('text-[11px] font-semibold', t.profit_pct > 0 ? 'text-emerald-400' : 'text-red-400')}>
                            {t.profit_pct > 0 ? '+' : ''}{t.profit_pct}%
                          </p>
                        </>
                      ) : <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>}
                    </td>
                  </tr>
                )
              })()}
            </thead>
            <tbody>
              {loading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i} className="border-b border-[hsl(var(--border)/0.2)]">
                    <td colSpan={10} className="px-4 py-4"><div className="h-12 animate-pulse rounded-lg bg-[hsl(var(--muted)/0.1)]" /></td>
                  </tr>
                ))
              ) : products.length === 0 ? (
                <tr>
                  <td colSpan={10} className="px-4 py-16 text-center">
                    <Package className="mx-auto mb-3 h-10 w-10 text-[hsl(var(--muted-foreground)/0.15)]" />
                    <p className="text-[hsl(var(--muted-foreground))]">Товары не найдены</p>
                    {search && <p className="mt-1 text-sm text-[hsl(var(--muted-foreground)/0.5)]">Попробуйте другой запрос</p>}
                  </td>
                </tr>
              ) : products.map((p) => {
                const totalStock = p.stocks_fbo + p.stocks_fbs

                return (
                  <tr key={p.offer_id} className="border-b border-[hsl(var(--border)/0.15)] hover:bg-white/[0.025] group transition-colors">

                    {/* ── 1. ТОВАР (sticky: фото 3:4 + название + артикул + цена) ── */}
                    <td className="sticky left-0 z-10 bg-[hsl(var(--card))] group-hover:bg-[hsl(var(--card))] pl-4 pr-2 py-3.5 transition-colors">
                      <div className="flex items-center gap-3">
                        {/* Photo 3:4 aspect — увеличено для читаемости */}
                        {p.image_url ? (
                          <div
                            className="relative shrink-0 cursor-pointer"
                            onMouseEnter={(e) => {
                              const r = e.currentTarget.getBoundingClientRect()
                              setHoverImg({ url: p.image_url, x: r.right + 12, y: r.top - 40 })
                            }}
                            onMouseLeave={() => setHoverImg(null)}
                          >
                            <img
                              src={p.image_url}
                              alt=""
                              className="h-[64px] w-[48px] rounded-lg object-cover bg-[hsl(var(--muted)/0.1)]"
                              loading="lazy"
                            />
                          </div>
                        ) : (
                          <div className="flex h-[64px] w-[48px] shrink-0 items-center justify-center rounded-lg bg-[hsl(var(--muted)/0.1)]">
                            <Package className="h-5 w-5 text-[hsl(var(--muted-foreground)/0.2)]" />
                          </div>
                        )}
                        {/* Info */}
                        <div className="min-w-0">
                          <p className="text-[13px] font-medium leading-snug line-clamp-2" title={p.name}>
                            {p.name}
                          </p>
                          <div className="mt-0.5 flex items-center gap-2">
                            <span className="text-[12px] font-semibold text-[hsl(var(--foreground)/0.75)] font-mono tracking-wide">{p.offer_id}</span>
                            {p.sku && <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.45)]">SKU {p.sku}</span>}
                          </div>
                          <div className="mt-1.5 flex items-center gap-1.5 flex-wrap">
                            <ContentRating rating={p.content_rating} />
                            {p.status === 'active' && (
                              <span className="rounded px-1 py-[1px] text-[10px] font-semibold bg-emerald-500/12 text-emerald-400 leading-tight">Продаётся</span>
                            )}
                            {p.ad_spend_7d > 0 && (
                              <span className="inline-flex items-center gap-1 rounded px-1.5 py-[1px] text-[10px] font-semibold bg-violet-500/12 text-violet-400 leading-tight">
                                <span className="inline-block h-1.5 w-1.5 rounded-full bg-violet-400 animate-pulse" />
                                Реклама
                              </span>
                            )}
                          </div>
                        </div>
                      </div>
                    </td>

                    {/* ── 2. ЦЕНА ── */}
                    <td className="px-2 py-2.5 text-right">
                      <p className="text-[13px] font-semibold tabular-nums">{fmtMoney(p.marketing_price || p.price)}</p>
                    </td>

                    {/* ── 3. ОСТАТКИ FBO / FBS ── */}
                    <td className="px-2 py-2.5 text-right">
                      {totalStock === 0 ? (
                        <span className="text-[11px] font-semibold text-red-400">Нет</span>
                      ) : (
                        <div className="flex flex-col items-end">
                          <div className="flex items-baseline gap-1">
                            <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)]">FBO</span>
                            <span className={cn('text-[14px] font-bold tabular-nums', p.stocks_fbo === 0 ? 'text-red-400' : '')}>{fmtNum(p.stocks_fbo)}</span>
                          </div>
                          <div className="flex items-baseline gap-1">
                            <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)]">FBS</span>
                            <span className={cn('text-[12px] font-semibold tabular-nums text-[hsl(var(--foreground)/0.7)]', p.stocks_fbs === 0 && 'text-[hsl(var(--muted-foreground)/0.3)]')}>{fmtNum(p.stocks_fbs)}</span>
                          </div>
                        </div>
                      )}
                    </td>

                    {/* ── 4. ПРОДАЖИ (цена × шт + qty) ── */}
                    <td className="px-2 py-2.5 text-right">
                      {p.orders_7d > 0 ? (
                        <div>
                          <p className="text-[14px] font-bold tabular-nums">{fmtMoney(p.sales_amount || (p.marketing_price || p.price) * p.orders_7d)}</p>
                          <p className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">
                            {p.orders_7d} шт{p.revenue_delta !== 0 && <span className={cn('ml-1 font-semibold', p.revenue_delta > 0 ? 'text-emerald-400' : 'text-red-400')}>{p.revenue_delta > 0 ? '+' : ''}{p.revenue_delta}%</span>}
                          </p>
                        </div>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── 5. ВЫПЛАТА (ppvz_for_pay) ── */}
                    <td className="px-2 py-2.5 text-right">
                      {p.payout_period > 0 ? (
                        <p className="text-[14px] font-semibold tabular-nums">{fmtMoney(p.payout_period)}</p>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── 5b. СР. ВЫПЛАТА ЗА ЕДИНИЦУ ── */}
                    <td className="px-2 py-2.5 text-right">
                      {p.orders_7d > 0 && (p.avg_price > 0 || p.revenue_7d > 0) ? (
                        <p className="text-[13px] font-medium tabular-nums text-[hsl(var(--foreground)/0.7)]">
                          {fmtMoney(Math.round(p.avg_price > 0 ? p.avg_price : p.revenue_7d / p.orders_7d))}
                        </p>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── 6. С/с (read-only: total + шт + за шт) ── */}
                    <td className="px-2 py-2.5 text-right">
                      {p.cost_price > 0 ? (() => {
                        const unitCost = p.cost_price + (p.packaging_cost || 0)
                        const totalCost = unitCost * p.orders_7d
                        return (
                          <div>
                            {p.orders_7d > 0 && (
                              <p className="text-[13px] font-semibold tabular-nums">{fmtMoney(totalCost)}</p>
                            )}
                            <p className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)] tabular-nums">
                              {p.orders_7d > 0 ? `${fmtNum(p.orders_7d)} шт × ` : ''}{fmtMoney(unitCost)}/шт
                            </p>
                            {p.margin_percent !== null && (
                              <p className={cn(
                                'text-[11px] font-semibold',
                                p.margin_percent < 30 ? 'text-emerald-400'
                                  : p.margin_percent < 50 ? 'text-amber-400'
                                  : 'text-red-400',
                              )}>
                                {p.margin_percent}%
                              </p>
                            )}
                          </div>
                        )
                      })() : (
                        <span className="inline-flex items-center gap-1 text-[11px] font-semibold text-amber-400/60">
                          <AlertTriangle className="h-3 w-3" />
                          Не указана
                        </span>
                      )}
                    </td>

                    {/* ── 7. РЕКЛАМА (расход + DRR текстом) ── */}
                    <td className="px-2 py-2.5 text-right">
                      {p.ad_spend_7d > 0 ? (
                        <div>
                          <p className="text-[13px] font-semibold tabular-nums">{fmtMoney(p.ad_spend_7d)}</p>
                          <p className={cn(
                            'text-[11px] font-semibold',
                            p.drr > 20 ? 'text-red-400'
                              : p.drr > 10 ? 'text-amber-400'
                              : 'text-emerald-400',
                          )}>
                            ДРР {p.drr}%
                          </p>
                        </div>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── 8. УСЛУГИ МП (с тултипом) ── */}
                    <td className="px-2 py-2.5 text-right">
                      {typeof p.mp_fees === 'number' && (p.revenue_7d > 0 || p.mp_fees !== 0) ? (
                        <div className="relative group/fees cursor-default">
                          <p className="text-[13px] font-semibold tabular-nums text-[hsl(var(--foreground)/0.85)]">
                            {p.fees_source === 'estimated' && <span className="text-amber-400/70 mr-0.5" title="Оценка по историческим данным">≈</span>}
                            {fmtMoney(p.mp_fees)}
                          </p>
                          <p className={cn(
                            'text-[11px] font-semibold',
                            p.mp_fees_percent > 35 ? 'text-orange-400' : 'text-[hsl(var(--muted-foreground)/0.5)]'
                          )}>{p.mp_fees_percent}%</p>
                          {/* Тултип */}
                          <div className="absolute right-0 bottom-full mb-2 z-50 hidden group-hover/fees:block">
                            <div className="bg-[hsl(var(--popover))] border border-[hsl(var(--border))] rounded-lg shadow-xl px-4 py-3 min-w-[260px] text-left">
                              <p className="text-[12px] font-bold text-[hsl(var(--foreground)/0.9)] mb-2">
                                Удержания маркетплейса
                                {p.fees_source === 'estimated' && (
                                  <span className="ml-1.5 text-[10px] font-medium text-amber-400/80 bg-amber-500/10 rounded px-1.5 py-0.5">≈ оценка</span>
                                )}
                              </p>
                              {p.fees_source === 'estimated' && (
                                <p className="text-[10px] text-amber-300/60 mb-2 -mt-0.5">Рассчитано по историческим данным товара (90 дней)</p>
                              )}
                              <div className="space-y-1.5">
                                {[
                                  { label: 'Логистика', val: p.mp_fees_logistics ?? 0 },
                                  { label: 'Хранение', val: p.mp_fees_storage ?? 0 },
                                  { label: 'Удержания', val: (p as any).mp_fees_deductions ?? 0 },
                                  { label: 'Приёмка', val: (p as any).mp_fees_acceptance ?? 0 },
                                  { label: 'Штрафы', val: (p as any).mp_fees_fines ?? 0 },
                                ].filter(r => r.val !== 0).map(r => (
                                  <div key={r.label} className="flex justify-between gap-4">
                                    <span className="text-[11px] text-[hsl(var(--muted-foreground))]">{r.label}</span>
                                    <div className="flex items-baseline gap-2">
                                      <span className="text-[12px] font-semibold tabular-nums">{fmtMoney(r.val)}</span>
                                      <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)] tabular-nums w-[32px] text-right">
                                        {p.revenue_7d > 0 ? `${(r.val / p.revenue_7d * 100).toFixed(1)}%` : '—'}
                                      </span>
                                    </div>
                                  </div>
                                ))}
                                <div className="border-t border-[hsl(var(--border)/0.5)] my-1" />
                                <div className="flex justify-between gap-4">
                                  <span className="text-[11px] font-bold">Итого</span>
                                  <div className="flex items-baseline gap-2">
                                    <span className="text-[12px] font-bold tabular-nums">{fmtMoney(p.mp_fees)}</span>
                                    <span className="text-[10px] font-bold tabular-nums w-[32px] text-right">
                                      {p.revenue_7d > 0 ? `${(p.mp_fees / p.revenue_7d * 100).toFixed(1)}%` : '—'}
                                    </span>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── 9. ПРИБЫЛЬ ── */}
                    <td className="px-2 py-2.5 text-right">
                      {p.cost_price === 0 ? (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.3)]">—</span>
                      ) : p.gross_profit !== null ? (
                        <div>
                          <p className={cn(
                            'text-[14px] font-bold tabular-nums',
                            p.gross_profit > 0 ? 'text-emerald-400' : 'text-red-400',
                          )}>
                            {p.fees_source === 'estimated' && <span className="text-amber-400/70 mr-0.5" title="Прибыль рассчитана по оценочным комиссиям">≈</span>}
                            {p.gross_profit > 0 ? '+' : ''}{fmtMoney(p.gross_profit)}
                          </p>
                          {p.gross_profit_percent !== null && (
                            <p className={cn(
                              'text-[11px] font-semibold',
                              p.gross_profit_percent > 15 ? 'text-emerald-400'
                                : p.gross_profit_percent > 0 ? 'text-amber-400'
                                : 'text-red-400',
                            )}>
                              {p.gross_profit_percent > 0 ? '+' : ''}{p.gross_profit_percent}%
                            </p>
                          )}
                        </div>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Infinite scroll loader ── */}
      {hasMore && !loading && (
        <div className="flex justify-center py-6">
          {loadingMore && (
            <div className="flex items-center gap-2 text-sm text-[hsl(var(--muted-foreground)/0.5)]">
              <Loader2 className="h-4 w-4 animate-spin" />
              Загрузка...
            </div>
          )}
        </div>
      )}
      {!hasMore && products.length > 0 && (
        <p className="text-center text-xs text-[hsl(var(--muted-foreground)/0.3)] py-4">Все {total} товаров загружены</p>
      )}

      {/* ── Hover Preview ── */}
      {hoverImg && (
        <div
          className="pointer-events-none fixed z-50 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1.5 shadow-2xl"
          style={{ left: hoverImg.x, top: hoverImg.y }}
        >
          <img src={hoverImg.url} alt="" className="h-[200px] w-[150px] rounded-lg object-cover" />
        </div>
      )}
    </div>
  )
}
