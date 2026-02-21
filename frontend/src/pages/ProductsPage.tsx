/**
 * Products Page — full product catalog with analytics.
 *
 * Features:
 * - 9-column table: Photo, Product, Price, Stocks, Sales 7d, Cost/Margin, Ads, Returns, Events
 * - Server-side filtering, search, sorting, pagination
 * - Inline cost price editing
 * - Event icons with tooltips
 * - Hover image preview
 */
import { useState, useEffect, useCallback } from 'react'
import {
  Search,
  Package,
  AlertTriangle,
  ChevronLeft,
  ChevronRight,
  ArrowUpDown,
  Camera,
  Pencil,
  TrendingUp,
  TrendingDown,
  Megaphone,
  XCircle,
  Tag,
  Ban,
  Loader2,
  Check,
  X,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useAppStore } from '@/stores/appStore'
import {
  getOzonProductsApi,
  updateOzonCostApi,
  type OzonProduct,
  type ProductEvent,
} from '@/api/products'

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

function formatMoney(v: number): string {
  return v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function formatNumber(v: number): string {
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
  { key: 'revenue_7d', label: 'Выручка 7д' },
  { key: 'orders_7d', label: 'Заказы 7д' },
  { key: 'stocks', label: 'Остатки' },
  { key: 'price', label: 'Цена' },
  { key: 'margin', label: 'Маржа' },
  { key: 'drr', label: 'DRR' },
  { key: 'returns', label: 'Возвраты' },
  { key: 'content_rating', label: 'Рейтинг контента' },
  { key: 'name', label: 'Название' },
]

/* ═══════════════════════════════════════════════════════════
   Event Icon Mapping
   ═══════════════════════════════════════════════════════════ */

const EVENT_ICON_MAP: Record<string, { icon: React.ElementType; color: string; label: string }> = {
  OZON_PHOTO_CHANGE: { icon: Camera, color: '#3b82f6', label: 'Изменение фото' },
  OZON_SEO_CHANGE: { icon: Pencil, color: '#8b5cf6', label: 'Изменение SEO' },
  PRICE_UP: { icon: TrendingUp, color: '#ef4444', label: 'Повышение цены' },
  PRICE_DOWN: { icon: TrendingDown, color: '#10b981', label: 'Снижение цены' },
  ITEM_ADD: { icon: Megaphone, color: '#f97316', label: 'Запуск рекламы' },
  ITEM_REMOVE: { icon: XCircle, color: '#6b7280', label: 'Остановка рекламы' },
  BID_CHANGE: { icon: TrendingUp, color: '#eab308', label: 'Изменение ставки' },
  STATUS_CHANGE: { icon: AlertTriangle, color: '#f59e0b', label: 'Изменение статуса' },
}

function EventBadge({ event }: { event: ProductEvent }) {
  const mapping = EVENT_ICON_MAP[event.type]
  if (!mapping) return null
  const Icon = mapping.icon
  const dateStr = event.date ? new Date(event.date).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' }) : ''

  let detail = mapping.label
  if (event.type === 'PRICE_UP' || event.type === 'PRICE_DOWN') {
    detail = `${mapping.label}: ${event.old_value} → ${event.new_value} ₽`
  }

  return (
    <div className="group/ev relative inline-flex">
      <div
        className="flex h-6 w-6 items-center justify-center rounded-md transition-colors hover:bg-[hsl(var(--muted)/0.3)]"
        style={{ color: mapping.color }}
      >
        <Icon className="h-3.5 w-3.5" />
      </div>
      <div className="pointer-events-none absolute bottom-full left-1/2 z-50 mb-2 -translate-x-1/2 whitespace-nowrap rounded-lg bg-[hsl(var(--card))] px-3 py-1.5 text-xs shadow-xl border border-[hsl(var(--border))] opacity-0 transition-opacity group-hover/ev:opacity-100">
        <span style={{ color: mapping.color }}>{detail}</span>
        {dateStr && <span className="ml-2 text-[hsl(var(--muted-foreground))]">{dateStr}</span>}
      </div>
    </div>
  )
}

function PromoBadge({ type }: { type: string }) {
  return (
    <div className="group/ev relative inline-flex">
      <div className="flex h-6 w-6 items-center justify-center rounded-md text-[#ec4899] hover:bg-[hsl(var(--muted)/0.3)]">
        <Tag className="h-3.5 w-3.5" />
      </div>
      <div className="pointer-events-none absolute bottom-full left-1/2 z-50 mb-2 -translate-x-1/2 whitespace-nowrap rounded-lg bg-[hsl(var(--card))] px-3 py-1.5 text-xs shadow-xl border border-[hsl(var(--border))] opacity-0 transition-opacity group-hover/ev:opacity-100">
        <span className="text-[#ec4899]">Акция: {type}</span>
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Price Index Color Dot
   ═══════════════════════════════════════════════════════════ */

function PriceIndexDot({ color }: { color: string }) {
  const colors: Record<string, string> = {
    WITHOUT_INDEX: '#6b7280',
    PROFIT: '#10b981',
    AVG_PROFIT: '#f59e0b',
    NON_PROFIT: '#ef4444',
  }
  const bg = colors[color] || '#6b7280'
  const labels: Record<string, string> = {
    WITHOUT_INDEX: 'Нет индекса',
    PROFIT: 'Выгодная',
    AVG_PROFIT: 'Средняя',
    NON_PROFIT: 'Невыгодная',
  }
  return (
    <div className="group/pi relative inline-flex">
      <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: bg }} />
      <div className="pointer-events-none absolute bottom-full left-1/2 z-50 mb-1 -translate-x-1/2 whitespace-nowrap rounded-md bg-[hsl(var(--card))] px-2 py-1 text-[10px] shadow-lg border border-[hsl(var(--border))] opacity-0 transition-opacity group-hover/pi:opacity-100">
        {labels[color] || color}
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Content Rating Badge
   ═══════════════════════════════════════════════════════════ */

function ContentRating({ rating }: { rating: number }) {
  if (rating <= 0) return null
  const pct = Math.round(rating * 100)
  const color = pct >= 80 ? '#10b981' : pct >= 50 ? '#f59e0b' : '#ef4444'
  return (
    <span
      className="inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-semibold"
      style={{ background: color + '20', color }}
    >
      ★ {pct}%
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   Inline Cost Editor
   ═══════════════════════════════════════════════════════════ */

function InlineCostEdit({
  product,
  shopId,
  onSaved,
}: {
  product: OzonProduct
  shopId: number
  onSaved: (offerId: string, cost: number) => void
}) {
  const [editing, setEditing] = useState(false)
  const [value, setValue] = useState(product.cost_price.toString())
  const [saving, setSaving] = useState(false)

  const save = async () => {
    const numValue = parseFloat(value)
    if (isNaN(numValue) || numValue < 0) return
    setSaving(true)
    try {
      await updateOzonCostApi({
        shop_id: shopId,
        offer_id: product.offer_id,
        cost_price: numValue,
      })
      onSaved(product.offer_id, numValue)
      setEditing(false)
    } catch {
      // Handle error silently
    } finally {
      setSaving(false)
    }
  }

  if (editing) {
    return (
      <div className="flex items-center gap-1">
        <input
          type="number"
          className="w-20 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') save()
            if (e.key === 'Escape') setEditing(false)
          }}
          autoFocus
        />
        <button onClick={save} disabled={saving} className="text-green-500 hover:text-green-400">
          {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Check className="h-3.5 w-3.5" />}
        </button>
        <button onClick={() => setEditing(false)} className="text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]">
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
    )
  }

  if (product.cost_price === 0) {
    return (
      <button
        onClick={() => setEditing(true)}
        className="inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-[11px] font-medium bg-yellow-500/15 text-yellow-500 hover:bg-yellow-500/25 transition-colors"
      >
        <AlertTriangle className="h-3 w-3" />
        Укажите
      </button>
    )
  }

  return (
    <button
      onClick={() => { setValue(product.cost_price.toString()); setEditing(true) }}
      className="text-sm hover:underline decoration-dotted underline-offset-2 cursor-pointer"
    >
      {formatMoney(product.cost_price)}
    </button>
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

  // Hover preview state
  const [hoverImg, setHoverImg] = useState<{ url: string; x: number; y: number } | null>(null)

  const fetchProducts = useCallback(async () => {
    if (!shopId || !isOzon) return
    setLoading(true)
    try {
      const data = await getOzonProductsApi({
        shop_id: shopId!,
        page,
        per_page: perPage,
        sort,
        order,
        filter,
        search,
      })
      setProducts(data.products)
      setTotal(data.total)
      setCostMissing(data.cost_missing_count)
    } catch (e) {
      console.error('Failed to fetch products', e)
    } finally {
      setLoading(false)
    }
  }, [shopId, isOzon, page, perPage, sort, order, filter, search])

  useEffect(() => {
    fetchProducts()
  }, [fetchProducts])

  // Debounced search
  useEffect(() => {
    const t = setTimeout(() => {
      setSearch(searchInput)
      setPage(1)
    }, 300)
    return () => clearTimeout(t)
  }, [searchInput])

  const toggleSort = (key: string) => {
    if (sort === key) {
      setOrder((o) => (o === 'desc' ? 'asc' : 'desc'))
    } else {
      setSort(key)
      setOrder('desc')
    }
    setPage(1)
  }

  const handleCostSaved = (offerId: string, cost: number) => {
    setProducts((prev) =>
      prev.map((p) => (p.offer_id === offerId ? { ...p, cost_price: cost } : p))
    )
    setCostMissing((c) => Math.max(0, c - 1))
  }

  const totalPages = Math.ceil(total / perPage)

  if (!isOzon) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="text-center space-y-2">
          <Package className="mx-auto h-12 w-12 text-[hsl(var(--muted-foreground)/0.3)]" />
          <p className="text-lg text-[hsl(var(--muted-foreground))]">
            Раздел «Товары» пока доступен для Ozon
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-5">
      {/* ── Header ────────────────────────────────────── */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Ваши товары</h1>
          <p className="mt-0.5 text-sm text-[hsl(var(--muted-foreground))]">
            {total} товаров
          </p>
        </div>

        {/* Search */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[hsl(var(--muted-foreground))]" />
          <input
            type="text"
            placeholder="Поиск по названию, артикулу, SKU..."
            className="w-80 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))] pl-10 pr-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary)/0.3)]"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
          />
        </div>
      </div>

      {/* ── Cost Warning Banner ────────────────────────── */}
      {costMissing > 0 && (
        <div className="flex items-center gap-3 rounded-xl border border-yellow-500/30 bg-yellow-500/10 px-4 py-3">
          <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
          <div className="text-sm">
            <span className="font-semibold text-yellow-500">
              У {costMissing} товаров не указана себестоимость.
            </span>{' '}
            <span className="text-[hsl(var(--muted-foreground))]">
              Без неё невозможно рассчитать маржинальность.
            </span>
          </div>
        </div>
      )}

      {/* ── Filters ────────────────────────────────────── */}
      <div className="flex items-center gap-2 flex-wrap">
        {FILTERS.map((f) => (
          <button
            key={f.key}
            onClick={() => { setFilter(f.key); setPage(1) }}
            className={cn(
              'rounded-full px-4 py-1.5 text-[13px] font-medium transition-all border',
              filter === f.key
                ? 'bg-[hsl(var(--primary)/0.12)] border-[hsl(var(--primary))] text-[hsl(var(--primary))]'
                : 'border-[hsl(var(--border))] text-[hsl(var(--muted-foreground))] hover:border-[hsl(var(--primary)/0.5)]',
            )}
          >
            {f.label}
          </button>
        ))}

        {/* Sort dropdown */}
        <div className="ml-auto flex items-center gap-2">
          <ArrowUpDown className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
          <select
            value={sort}
            onChange={(e) => { setSort(e.target.value); setPage(1) }}
            className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-3 py-1.5 text-sm focus:outline-none"
          >
            {SORT_OPTIONS.map((o) => (
              <option key={o.key} value={o.key}>{o.label}</option>
            ))}
          </select>
          <button
            onClick={() => setOrder((o) => (o === 'desc' ? 'asc' : 'desc'))}
            className="rounded-lg border border-[hsl(var(--border))] px-2 py-1.5 text-sm hover:bg-[hsl(var(--muted)/0.3)]"
          >
            {order === 'desc' ? '↓' : '↑'}
          </button>
        </div>
      </div>

      {/* ── Table ────────────────────────────────────── */}
      <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full min-w-[1200px]">
            <thead>
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.15)]">
                <th className="px-3 py-3 text-left text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider w-[60px]">Фото</th>
                <th className="px-3 py-3 text-left text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider w-[240px]">
                  <button onClick={() => toggleSort('name')} className="flex items-center gap-1 hover:text-[hsl(var(--foreground))]">
                    Товар {sort === 'name' && (order === 'desc' ? '↓' : '↑')}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider w-[130px]">
                  <button onClick={() => toggleSort('price')} className="flex items-center gap-1 ml-auto hover:text-[hsl(var(--foreground))]">
                    Цена {sort === 'price' && (order === 'desc' ? '↓' : '↑')}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider w-[100px]">
                  <button onClick={() => toggleSort('stocks')} className="flex items-center gap-1 ml-auto hover:text-[hsl(var(--foreground))]">
                    Остатки {sort === 'stocks' && (order === 'desc' ? '↓' : '↑')}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider w-[120px]">
                  <button onClick={() => toggleSort('revenue_7d')} className="flex items-center gap-1 ml-auto hover:text-[hsl(var(--foreground))]">
                    Продажи 7д {sort === 'revenue_7d' && (order === 'desc' ? '↓' : '↑')}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider w-[130px]">
                  <button onClick={() => toggleSort('margin')} className="flex items-center gap-1 ml-auto hover:text-[hsl(var(--foreground))]">
                    С/с и маржа {sort === 'margin' && (order === 'desc' ? '↓' : '↑')}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider w-[100px]">
                  <button onClick={() => toggleSort('drr')} className="flex items-center gap-1 ml-auto hover:text-[hsl(var(--foreground))]">
                    Реклама {sort === 'drr' && (order === 'desc' ? '↓' : '↑')}
                  </button>
                </th>
                <th className="px-3 py-3 text-right text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider w-[80px]">
                  <button onClick={() => toggleSort('returns')} className="flex items-center gap-1 ml-auto hover:text-[hsl(var(--foreground))]">
                    Возвр. {sort === 'returns' && (order === 'desc' ? '↓' : '↑')}
                  </button>
                </th>
                <th className="px-3 py-3 text-center text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider w-[120px]">События</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[hsl(var(--border)/0.5)]">
              {loading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i}>
                    <td colSpan={9} className="px-3 py-4">
                      <div className="h-10 animate-pulse rounded-lg bg-[hsl(var(--muted)/0.2)]" />
                    </td>
                  </tr>
                ))
              ) : products.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-3 py-12 text-center text-[hsl(var(--muted-foreground))]">
                    <Package className="mx-auto mb-2 h-8 w-8 opacity-30" />
                    Нет товаров
                  </td>
                </tr>
              ) : (
                products.map((product) => {
                  const totalStocks = product.stocks_fbo + product.stocks_fbs
                  const discount = product.old_price > 0 && product.old_price > product.price
                    ? Math.round((1 - product.price / product.old_price) * 100)
                    : 0

                  return (
                    <tr
                      key={product.offer_id}
                      className="transition-colors hover:bg-[hsl(var(--muted)/0.08)]"
                    >
                      {/* ── 1. Photo ── */}
                      <td className="px-3 py-2">
                        {product.image_url ? (
                          <div
                            className="relative cursor-pointer"
                            onMouseEnter={(e) => {
                              const rect = e.currentTarget.getBoundingClientRect()
                              setHoverImg({ url: product.image_url, x: rect.right + 8, y: rect.top })
                            }}
                            onMouseLeave={() => setHoverImg(null)}
                          >
                            <img
                              src={product.image_url}
                              alt=""
                              className="h-12 w-9 rounded-md object-cover bg-[hsl(var(--muted)/0.2)]"
                              loading="lazy"
                            />
                            {product.images_count > 1 && (
                              <span className="absolute -top-1 -right-1 flex h-4 min-w-[16px] items-center justify-center rounded-full bg-[hsl(var(--primary))] px-1 text-[9px] font-bold text-white">
                                {product.images_count}
                              </span>
                            )}
                          </div>
                        ) : (
                          <div className="flex h-12 w-9 items-center justify-center rounded-md bg-[hsl(var(--muted)/0.2)]">
                            <Package className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.3)]" />
                          </div>
                        )}
                      </td>

                      {/* ── 2. Product Info ── */}
                      <td className="px-3 py-2">
                        <div className="max-w-[220px]">
                          <p className="truncate text-sm font-medium" title={product.name}>
                            {product.name}
                          </p>
                          <p className="text-[11px] text-[hsl(var(--muted-foreground))]">
                            {product.offer_id}
                            {product.sku && <span className="ml-2 opacity-60">SKU {product.sku}</span>}
                          </p>
                          <div className="mt-0.5 flex items-center gap-1.5">
                            <ContentRating rating={product.content_rating} />
                            {product.status && product.status !== 'active' && (
                              <span className="inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium bg-yellow-500/15 text-yellow-500">
                                {product.status_name || product.status}
                              </span>
                            )}
                          </div>
                        </div>
                      </td>

                      {/* ── 3. Price ── */}
                      <td className="px-3 py-2 text-right">
                        <div className="flex items-center justify-end gap-1.5">
                          <div>
                            <p className="text-sm font-semibold">{formatMoney(product.marketing_price || product.price)}</p>
                            {product.old_price > 0 && product.old_price !== product.price && (
                              <p className="text-[11px] text-[hsl(var(--muted-foreground))] line-through">
                                {formatMoney(product.old_price)}
                              </p>
                            )}
                            {discount > 0 && (
                              <span className="inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-bold bg-green-500/15 text-green-500">
                                -{discount}%
                              </span>
                            )}
                          </div>
                          <PriceIndexDot color={product.price_index_color} />
                        </div>
                      </td>

                      {/* ── 4. Stocks ── */}
                      <td className="px-3 py-2 text-right">
                        {totalStocks === 0 ? (
                          <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-semibold bg-red-500/15 text-red-500">
                            <Ban className="h-3 w-3" /> Нет
                          </span>
                        ) : (
                          <div>
                            <p className="text-sm font-medium">{formatNumber(totalStocks)}</p>
                            <p className="text-[10px] text-[hsl(var(--muted-foreground))]">
                              FBO {product.stocks_fbo} · FBS {product.stocks_fbs}
                            </p>
                          </div>
                        )}
                      </td>

                      {/* ── 5. Sales 7d ── */}
                      <td className="px-3 py-2 text-right">
                        <p className="text-sm font-semibold">{product.orders_7d} шт</p>
                        <p className="text-[11px] text-[hsl(var(--muted-foreground))]">
                          {formatMoney(product.revenue_7d)}
                        </p>
                        {product.revenue_delta !== 0 && (
                          <span
                            className={cn(
                              'text-[10px] font-semibold',
                              product.revenue_delta > 0 ? 'text-green-500' : 'text-red-500',
                            )}
                          >
                            {product.revenue_delta > 0 ? '+' : ''}{product.revenue_delta}%
                          </span>
                        )}
                      </td>

                      {/* ── 6. Cost / Margin ── */}
                      <td className="px-3 py-2 text-right">
                        <InlineCostEdit
                          product={product}
                          shopId={shopId!}
                          onSaved={handleCostSaved}
                        />
                        {product.margin !== null && product.margin_percent !== null && (
                          <p
                            className={cn(
                              'text-[11px] font-semibold mt-0.5',
                              product.margin_percent > 15 ? 'text-green-500'
                                : product.margin_percent > 5 ? 'text-yellow-500'
                                : 'text-red-500',
                            )}
                          >
                            {formatMoney(product.margin)} ({product.margin_percent}%)
                          </p>
                        )}
                      </td>

                      {/* ── 7. Ads ── */}
                      <td className="px-3 py-2 text-right">
                        {product.ad_spend_7d > 0 ? (
                          <div>
                            <p className="text-sm">{formatMoney(product.ad_spend_7d)}</p>
                            <p
                              className={cn(
                                'text-[11px] font-semibold',
                                product.drr > 20 ? 'text-red-500' : product.drr > 10 ? 'text-yellow-500' : 'text-green-500',
                              )}
                            >
                              DRR {product.drr}%
                            </p>
                          </div>
                        ) : (
                          <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)]">—</span>
                        )}
                      </td>

                      {/* ── 8. Returns ── */}
                      <td className="px-3 py-2 text-right">
                        {product.returns_30d > 0 ? (
                          <span
                            className={cn(
                              'text-sm font-medium',
                              product.returns_30d > 5 ? 'text-red-500' : 'text-[hsl(var(--muted-foreground))]',
                            )}
                          >
                            {product.returns_30d}
                          </span>
                        ) : (
                          <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)]">—</span>
                        )}
                      </td>

                      {/* ── 9. Events ── */}
                      <td className="px-3 py-2">
                        <div className="flex items-center justify-center gap-0.5 flex-wrap">
                          {product.promotions.map((pt, i) => (
                            <PromoBadge key={i} type={pt} />
                          ))}
                          {product.events.slice(0, 5).map((ev, i) => (
                            <EventBadge key={i} event={ev} />
                          ))}
                          {product.events.length === 0 && product.promotions.length === 0 && (
                            <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.3)]">—</span>
                          )}
                        </div>
                      </td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Pagination ────────────────────────────────── */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between px-1">
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            Показано {(page - 1) * perPage + 1}–{Math.min(page * perPage, total)} из {total}
          </p>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page === 1}
              className="rounded-lg border border-[hsl(var(--border))] p-2 hover:bg-[hsl(var(--muted)/0.3)] disabled:opacity-30"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
              const pg = page <= 3 ? i + 1
                : page >= totalPages - 2 ? totalPages - 4 + i
                : page - 2 + i
              if (pg < 1 || pg > totalPages) return null
              return (
                <button
                  key={pg}
                  onClick={() => setPage(pg)}
                  className={cn(
                    'min-w-[36px] rounded-lg border px-3 py-2 text-sm font-medium transition-colors',
                    pg === page
                      ? 'border-[hsl(var(--primary))] bg-[hsl(var(--primary)/0.12)] text-[hsl(var(--primary))]'
                      : 'border-[hsl(var(--border))] hover:bg-[hsl(var(--muted)/0.3)]',
                  )}
                >
                  {pg}
                </button>
              )
            })}
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
              className="rounded-lg border border-[hsl(var(--border))] p-2 hover:bg-[hsl(var(--muted)/0.3)] disabled:opacity-30"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}

      {/* ── Hover Image Preview ───────────────────────── */}
      {hoverImg && (
        <div
          className="pointer-events-none fixed z-50 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1.5 shadow-2xl"
          style={{ left: hoverImg.x, top: hoverImg.y }}
        >
          <img
            src={hoverImg.url}
            alt=""
            className="h-[200px] w-[150px] rounded-lg object-cover"
          />
        </div>
      )}
    </div>
  )
}
