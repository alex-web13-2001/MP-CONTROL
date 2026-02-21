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
  ChevronLeft,
  ChevronRight,
  Camera,
  Pencil,
  TrendingUp,
  TrendingDown,
  Megaphone,
  XCircle,
  Tag,
  Loader2,
  Check,
  X,
  Upload,
  Download,
  FileSpreadsheet,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useAppStore } from '@/stores/appStore'
import {
  getOzonProductsApi,
  updateOzonCostApi,
  uploadCostExcelApi,
  downloadCostTemplate,
  type OzonProduct,
  type ProductEvent,
} from '@/api/products'

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

function fmtMoney(v: number): string {
  if (v >= 1_000_000) return (v / 1_000_000).toFixed(1).replace('.0', '') + 'M ₽'
  return v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function fmtNum(v: number): string {
  return v.toLocaleString('ru-RU')
}

const FILTERS = [
  { key: 'all', label: 'Все' },
  { key: 'in_stock', label: 'В наличии' },
  { key: 'no_stock', label: 'Без остатков' },
  { key: 'with_ads', label: 'С рекламой' },
  { key: 'problems', label: 'Проблемные' },
  { key: 'archived', label: 'Архив' },
] as const

type FilterKey = typeof FILTERS[number]['key']

const SORT_OPTIONS = [
  { key: 'revenue_7d', label: 'Выручке' },
  { key: 'orders_7d', label: 'Заказам' },
  { key: 'stocks', label: 'Остаткам' },
  { key: 'price', label: 'Цене' },
  { key: 'margin', label: 'Марже' },
  { key: 'gross_profit', label: 'Валу' },
  { key: 'drr', label: 'DRR' },
  { key: 'returns', label: 'Возвратам' },
  { key: 'name', label: 'Названию' },
]

/* ═══════════════════════════════════════════════════════════
   Event Badges
   ═══════════════════════════════════════════════════════════ */

const EV_MAP: Record<string, { icon: React.ElementType; color: string; label: string }> = {
  OZON_PHOTO_CHANGE: { icon: Camera, color: '#3b82f6', label: 'Фото' },
  OZON_SEO_CHANGE: { icon: Pencil, color: '#8b5cf6', label: 'SEO' },
  PRICE_UP: { icon: TrendingUp, color: '#ef4444', label: '↑ Цена' },
  PRICE_DOWN: { icon: TrendingDown, color: '#10b981', label: '↓ Цена' },
  ITEM_ADD: { icon: Megaphone, color: '#f97316', label: 'Реклама вкл' },
  ITEM_REMOVE: { icon: XCircle, color: '#6b7280', label: 'Реклама выкл' },
  BID_CHANGE: { icon: TrendingUp, color: '#eab308', label: 'Ставка' },
  STATUS_CHANGE: { icon: AlertTriangle, color: '#f59e0b', label: 'Статус' },
}

function EvBadge({ event }: { event: ProductEvent }) {
  const m = EV_MAP[event.type]
  if (!m) return null
  const Icon = m.icon
  const dateStr = event.date ? new Date(event.date).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' }) : ''
  let detail = m.label
  if (event.type === 'PRICE_UP' || event.type === 'PRICE_DOWN') {
    detail = `${event.old_value} → ${event.new_value} ₽`
  }

  return (
    <div className="group/ev relative inline-flex" title={detail}>
      <div
        className="flex h-7 w-7 items-center justify-center rounded-lg transition-colors hover:bg-white/5"
        style={{ color: m.color }}
      >
        <Icon className="h-3.5 w-3.5" />
      </div>
      <div className="pointer-events-none absolute bottom-full left-1/2 z-50 mb-2 -translate-x-1/2 whitespace-nowrap rounded-xl bg-[hsl(var(--card))] px-3 py-2 text-xs shadow-2xl border border-[hsl(var(--border))] opacity-0 transition-opacity group-hover/ev:opacity-100">
        <span className="font-medium" style={{ color: m.color }}>{detail}</span>
        {dateStr && <span className="ml-2 text-[hsl(var(--muted-foreground)/0.6)]">{dateStr}</span>}
      </div>
    </div>
  )
}

function PromoBadge({ type }: { type: string }) {
  return (
    <div className="group/ev relative inline-flex" title={`Акция: ${type}`}>
      <div className="flex h-7 w-7 items-center justify-center rounded-lg text-[#ec4899] hover:bg-white/5">
        <Tag className="h-3.5 w-3.5" />
      </div>
    </div>
  )
}

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
   Inline Cost Editor (styled, no native spinner)
   ═══════════════════════════════════════════════════════════ */

function CostEdit({ product, shopId, onSaved }: {
  product: OzonProduct; shopId: number; onSaved: (oid: string, cost: number) => void
}) {
  const [editing, setEditing] = useState(false)
  const [val, setVal] = useState(product.cost_price.toString())
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
      await updateOzonCostApi({ shop_id: shopId, offer_id: product.offer_id, cost_price: n })
      onSaved(product.offer_id, n)
      close()
    } catch { /* silently */ } finally { setSaving(false) }
  }

  // Close on outside click
  useEffect(() => {
    if (!editing) return
    const h = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) close()
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [editing, close])

  const openEditor = () => {
    setVal(product.cost_price > 0 ? product.cost_price.toString() : '')
    setEditing(true)
    setClosing(false)
  }

  return (
    <div className="relative inline-flex" ref={wrapRef}>
      {/* Trigger — always visible, preserves table width */}
      {product.cost_price === 0 ? (
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
          {fmtMoney(product.cost_price)}
        </button>
      )}

      {/* Floating popover — positioned absolutely, doesn't shift table */}
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

      {/* Popover animation keyframes */}
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
   Delta Badge (reusable)
   ═══════════════════════════════════════════════════════════ */

function DeltaBadge({ value, suffix = '%' }: { value: number; suffix?: string }) {
  if (value === 0) return null
  const positive = value > 0
  return (
    <span className={cn(
      'inline-flex items-center rounded px-1 py-[1px] text-[10px] font-bold leading-tight',
      positive ? 'bg-emerald-500/12 text-emerald-400' : 'bg-red-500/12 text-red-400',
    )}>
      {positive ? '+' : ''}{value}{suffix}
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page Component
   ═══════════════════════════════════════════════════════════ */

export default function ProductsPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isOzon = currentShop?.marketplace === 'ozon'
  const shopId = currentShop?.id

  const [products, setProducts] = useState<OzonProduct[]>([])
  const [total, setTotal] = useState(0)
  const [costMissing, setCostMissing] = useState(0)
  const [loading, setLoading] = useState(true)
  const [page, setPage] = useState(1)
  const [perPage] = useState(25)
  const [sort, setSort] = useState('revenue_7d')
  const [order, setOrder] = useState<'asc' | 'desc'>('desc')
  const [filter, setFilter] = useState<FilterKey>('all')
  const [search, setSearch] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const [period, setPeriod] = useState<7 | 30>(7)
  const [hoverImg, setHoverImg] = useState<{ url: string; x: number; y: number } | null>(null)
  const [uploading, setUploading] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const fetchProducts = useCallback(async () => {
    if (!shopId || !isOzon) return
    setLoading(true)
    try {
      const data = await getOzonProductsApi({
        shop_id: shopId!, page, per_page: perPage, sort, order, filter, search, period,
      })
      setProducts(data.products)
      setTotal(data.total)
      setCostMissing(data.cost_missing_count)
    } catch (e) {
      console.error('Failed to fetch products', e)
    } finally {
      setLoading(false)
    }
  }, [shopId, isOzon, page, perPage, sort, order, filter, search, period])

  useEffect(() => { fetchProducts() }, [fetchProducts])

  // Debounced search
  useEffect(() => {
    const t = setTimeout(() => { setSearch(searchInput); setPage(1) }, 400)
    return () => clearTimeout(t)
  }, [searchInput])

  const toggleSort = (key: string) => {
    if (sort === key) setOrder((o) => (o === 'desc' ? 'asc' : 'desc'))
    else { setSort(key); setOrder('desc') }
    setPage(1)
  }

  const handleCostSaved = (offerId: string, cost: number) => {
    setProducts((prev) => prev.map((p) => (p.offer_id === offerId ? { ...p, cost_price: cost } : p)))
    setCostMissing((c) => Math.max(0, c - 1))
    fetchProducts()
  }

  const handleExcelUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file || !shopId) return
    setUploading(true)
    try {
      const res = await uploadCostExcelApi(shopId, file)
      if (res.ok) {
        fetchProducts()
      }
    } catch (err) {
      console.error('Excel upload failed', err)
    } finally {
      setUploading(false)
      if (fileInputRef.current) fileInputRef.current.value = ''
    }
  }

  const totalPages = Math.ceil(total / perPage)

  /* ── No Ozon ── */
  if (!isOzon) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="text-center space-y-3">
          <Package className="mx-auto h-16 w-16 text-[hsl(var(--muted-foreground)/0.15)]" />
          <p className="text-lg font-medium text-[hsl(var(--muted-foreground))]">Доступно для Ozon</p>
          <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">Wildberries — в ближайшем обновлении</p>
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
          <p className="mt-0.5 text-sm text-[hsl(var(--muted-foreground))]">{total} товаров · Статистика за {period === 7 ? 'неделю' : 'месяц'}</p>
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
        {/* Period — enlarged, dashboard-style */}
        <div className="inline-flex rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1">
          {([7, 30] as const).map((d) => (
            <button
              key={d}
              onClick={() => { setPeriod(d); setPage(1) }}
              className={cn(
                'rounded-md px-5 py-2 text-sm font-medium transition-all duration-200',
                period === d
                  ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
                  : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]',
              )}
            >
              {d === 7 ? '7 дней' : '30 дней'}
            </button>
          ))}
        </div>
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
                onClick={() => downloadCostTemplate(shopId)}
                className="inline-flex items-center gap-1.5 rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-3 py-1.5 text-xs font-medium text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
              >
                <Download className="h-3.5 w-3.5" />
                Шаблон
              </button>
            )}
          </div>
        </div>
      )}

      {/* ═══════════════════════════════════════════════════
         Table — Business-priority columns:
         [Photo|Товар] sticky → Продажи → Остатки → С/с → DRR → Возвр% → Цена → Events
         ═══════════════════════════════════════════════════ */}
      <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full min-w-[1200px]">
            <thead>
              <tr className="border-b border-[hsl(var(--border))]">
                {/* Sticky: Photo + Product */}
                <th className="sticky left-0 z-20 w-[340px] bg-[hsl(var(--card))] pl-4 pr-2 py-3 text-left text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
                  Товар
                </th>
                {/* Business metrics — dashboard-style headers */}
                <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
                  <button onClick={() => toggleSort('revenue_7d')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'revenue_7d' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                    Продажи {sort === 'revenue_7d' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
                  <button onClick={() => toggleSort('stocks')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'stocks' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                    Остатки {sort === 'stocks' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
                  <button onClick={() => toggleSort('margin')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'margin' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                    С/с и маржа {sort === 'margin' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
                  <button onClick={() => toggleSort('gross_profit')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'gross_profit' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                    Вал {sort === 'gross_profit' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
                  <button onClick={() => toggleSort('drr')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'drr' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                    Реклама {sort === 'drr' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
                  <button onClick={() => toggleSort('returns')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'returns' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                    Возвр. {sort === 'returns' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
                  <button onClick={() => toggleSort('price')} className={cn('inline-flex items-center gap-1 transition-colors', sort === 'price' ? 'text-[hsl(var(--primary))]' : 'hover:text-[hsl(var(--foreground))]')}>
                    Цена {sort === 'price' && <span>{order === 'desc' ? '↓' : '↑'}</span>}
                  </button>
                </th>
                <th className="px-3 py-3 text-center text-[13px] font-medium text-[hsl(var(--muted-foreground))] w-[100px]">
                  События
                </th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i} className="border-b border-[hsl(var(--border)/0.2)]">
                    <td colSpan={9} className="px-4 py-4"><div className="h-12 animate-pulse rounded-lg bg-[hsl(var(--muted)/0.1)]" /></td>
                  </tr>
                ))
              ) : products.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-4 py-16 text-center">
                    <Package className="mx-auto mb-3 h-10 w-10 text-[hsl(var(--muted-foreground)/0.15)]" />
                    <p className="text-[hsl(var(--muted-foreground))]">Товары не найдены</p>
                    {search && <p className="mt-1 text-sm text-[hsl(var(--muted-foreground)/0.5)]">Попробуйте другой запрос</p>}
                  </td>
                </tr>
              ) : products.map((p) => {
                const totalStock = p.stocks_fbo + p.stocks_fbs
                const discount = p.old_price > 0 && p.old_price > p.price
                  ? Math.round((1 - p.price / p.old_price) * 100)
                  : 0
                // Correct return rate: 30d returns / 30d orders
                const returnPct = p.orders_30d > 0 ? Math.round(p.returns_30d / p.orders_30d * 100) : 0

                return (
                  <tr key={p.offer_id} className="border-b border-[hsl(var(--border)/0.15)] hover:bg-white/[0.02] group transition-colors">

                    {/* ── 1. ТОВАР (sticky: фото 3:4 + название + артикул + рейтинг) ── */}
                    <td className="sticky left-0 z-10 bg-[hsl(var(--card))] group-hover:bg-[hsl(var(--card))] pl-4 pr-2 py-2.5 transition-colors">
                      <div className="flex items-center gap-3">
                        {/* Photo 3:4 aspect */}
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
                              className="h-[52px] w-[40px] rounded-lg object-cover bg-[hsl(var(--muted)/0.1)]"
                              loading="lazy"
                            />
                          </div>
                        ) : (
                          <div className="flex h-[52px] w-[40px] shrink-0 items-center justify-center rounded-lg bg-[hsl(var(--muted)/0.1)]">
                            <Package className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.2)]" />
                          </div>
                        )}
                        {/* Info */}
                        <div className="min-w-0">
                          <p className="text-[13px] font-medium leading-snug line-clamp-2" title={p.name}>
                            {p.name}
                          </p>
                          <div className="mt-0.5 flex items-center gap-2 text-[11px] text-[hsl(var(--muted-foreground)/0.6)]">
                            <span className="font-mono">{p.offer_id}</span>
                            {p.sku && <span>SKU {p.sku}</span>}
                          </div>
                          <div className="mt-1 flex items-center gap-1.5 flex-wrap">
                            <ContentRating rating={p.content_rating} />
                            {p.status === 'active' && (
                              <span className="rounded px-1 py-[1px] text-[10px] font-semibold bg-emerald-500/12 text-emerald-400 leading-tight">Продаётся</span>
                            )}
                          </div>
                        </div>
                      </div>
                    </td>

                    {/* ── 2. ПРОДАЖИ 7д (главный KPI: выручка крупно, штуки мельче, дельта) ── */}
                    <td className="px-3 py-2.5 text-right">
                      {p.revenue_7d > 0 ? (
                        <div>
                          <p className="text-[15px] font-bold tabular-nums">{fmtMoney(p.revenue_7d)}</p>
                          <p className="text-[11px] text-[hsl(var(--muted-foreground)/0.6)] tabular-nums">{p.orders_7d} шт</p>
                          {p.revenue_delta !== 0 && <DeltaBadge value={p.revenue_delta} />}
                        </div>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── 3. ОСТАТКИ (число крупно, FBO/FBS компактно) ── */}
                    <td className="px-3 py-2.5 text-right">
                      {totalStock === 0 ? (
                        <span className="inline-flex items-center rounded px-1.5 py-0.5 text-[11px] font-semibold bg-red-500/12 text-red-400">
                          Нет
                        </span>
                      ) : (
                        <div>
                          <p className="text-[15px] font-bold tabular-nums">{fmtNum(totalStock)}</p>
                          <p className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)] tabular-nums">
                            FBO {fmtNum(p.stocks_fbo)} · FBS {fmtNum(p.stocks_fbs)}
                          </p>
                        </div>
                      )}
                    </td>

                    {/* ── 4. С/с И МАРЖА ── */}
                    <td className="px-3 py-2.5 text-right">
                      <CostEdit product={p} shopId={shopId!} onSaved={handleCostSaved} />
                      {p.margin !== null && p.margin_percent !== null && (
                        <p className={cn(
                          'text-[11px] font-bold mt-0.5',
                          p.margin_percent > 15 ? 'text-emerald-400' : p.margin_percent > 5 ? 'text-amber-400' : 'text-red-400',
                        )}>
                          {p.margin_percent > 0 ? '+' : ''}{p.margin_percent}%
                          <span className="font-normal text-[hsl(var(--muted-foreground)/0.5)] ml-1">{fmtMoney(p.margin)}</span>
                        </p>
                      )}
                    </td>

                    {/* ── 5. ВАЛОВАЯ ПРИБЫЛЬ (payout - cost × qty) + delta ── */}
                    <td className="px-3 py-2.5 text-right">
                      {p.cost_price === 0 ? (
                        <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.4)]">—</span>
                      ) : p.gross_profit !== null ? (
                        <div>
                          <p className={cn(
                            'text-sm font-bold tabular-nums',
                            p.gross_profit > 0 ? 'text-emerald-400' : 'text-red-400',
                          )}>
                            {p.gross_profit > 0 ? '+' : ''}{fmtMoney(p.gross_profit)}
                          </p>
                          {p.gross_profit_percent !== null && (
                            <span className={cn(
                              'inline-flex items-center rounded px-1 py-[1px] text-[10px] font-bold leading-tight mt-0.5',
                              p.gross_profit_percent > 15 ? 'bg-emerald-500/12 text-emerald-400'
                                : p.gross_profit_percent > 0 ? 'bg-amber-500/12 text-amber-400'
                                : 'bg-red-500/12 text-red-400',
                            )}>
                              {p.gross_profit_percent > 0 ? '+' : ''}{p.gross_profit_percent}%
                            </span>
                          )}
                          {p.gross_profit_delta !== null && (
                            <p className={cn(
                              'text-[10px] font-medium tabular-nums mt-0.5',
                              p.gross_profit_delta > 0 ? 'text-emerald-500/70' : 'text-red-400/70',
                            )}>
                              {p.gross_profit_delta > 0 ? '↑' : '↓'} {Math.abs(p.gross_profit_delta)}%
                            </p>
                          )}
                        </div>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── 6. РЕКЛАМА (расход + DRR) ── */}
                    <td className="px-3 py-2.5 text-right">
                      {p.ad_spend_7d > 0 ? (
                        <div>
                          <p className="text-sm font-semibold tabular-nums">{fmtMoney(p.ad_spend_7d)}</p>
                          <span className={cn(
                            'inline-flex items-center rounded px-1 py-[1px] text-[10px] font-bold leading-tight mt-0.5',
                            p.drr > 20 ? 'bg-red-500/12 text-red-400'
                              : p.drr > 10 ? 'bg-amber-500/12 text-amber-400'
                              : 'bg-emerald-500/12 text-emerald-400',
                          )}>
                            DRR {p.drr}%
                          </span>
                        </div>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── 6. ВОЗВРАТЫ % (30д returns / 30д orders) ── */}
                    <td className="px-3 py-2.5 text-right">
                      {p.returns_30d > 0 ? (
                        <div>
                          <span className={cn(
                            'text-sm font-bold tabular-nums',
                            returnPct > 10 ? 'text-red-400' : returnPct > 5 ? 'text-amber-400' : 'text-[hsl(var(--foreground)/0.8)]',
                          )}>
                            {returnPct}%
                          </span>
                          <p className="text-[10px] text-[hsl(var(--muted-foreground)/0.4)] tabular-nums">{p.returns_30d} шт</p>
                        </div>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.25)]">—</span>
                      )}
                    </td>

                    {/* ── 7. ЦЕНА (старая → скидка → текущая) ── */}
                    <td className="px-3 py-2.5 text-right">
                      <div>
                        {p.old_price > 0 && p.old_price !== p.price && (
                          <p className="text-[10px] text-[hsl(var(--muted-foreground)/0.35)] line-through tabular-nums">{fmtMoney(p.old_price)}</p>
                        )}
                        <p className="text-sm font-semibold tabular-nums">{fmtMoney(p.marketing_price || p.price)}</p>
                        {discount > 0 && (
                          <span className="inline-flex items-center rounded px-1 py-[1px] text-[10px] font-bold leading-tight bg-emerald-500/12 text-emerald-400">
                            -{discount}%
                          </span>
                        )}
                      </div>
                    </td>

                    {/* ── 8. СОБЫТИЯ ── */}
                    <td className="px-3 py-2.5">
                      <div className="flex items-center justify-center gap-0 flex-wrap">
                        {p.promotions.map((pt: string, i: number) => <PromoBadge key={`promo-${i}`} type={pt} />)}
                        {p.events.slice(0, 4).map((ev: ProductEvent, i: number) => <EvBadge key={`ev-${i}`} event={ev} />)}
                        {p.events.length === 0 && p.promotions.length === 0 && (
                          <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.2)]">—</span>
                        )}
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Pagination ── */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between px-1">
          <p className="text-sm text-[hsl(var(--muted-foreground)/0.6)]">
            {(page - 1) * perPage + 1}–{Math.min(page * perPage, total)} из {total}
          </p>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page === 1}
              className="flex h-9 w-9 items-center justify-center rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-white/5 disabled:opacity-20 transition-colors"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
              const pg = page <= 3 ? i + 1 : page >= totalPages - 2 ? totalPages - 4 + i : page - 2 + i
              if (pg < 1 || pg > totalPages) return null
              return (
                <button
                  key={pg}
                  onClick={() => setPage(pg)}
                  className={cn(
                    'flex h-9 min-w-[36px] items-center justify-center rounded-lg border px-2.5 text-sm font-medium transition-colors',
                    pg === page
                      ? 'border-[hsl(var(--primary)/0.3)] bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))]'
                      : 'border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-white/5',
                  )}
                >
                  {pg}
                </button>
              )
            })}
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
              className="flex h-9 w-9 items-center justify-center rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-white/5 disabled:opacity-20 transition-colors"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        </div>
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
