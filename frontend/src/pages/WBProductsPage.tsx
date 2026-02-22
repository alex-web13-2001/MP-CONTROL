/**
 * WB Products Page — Каталог товаров Wildberries с аналитикой.
 *
 * Колонки:
 *  1. Товар (фото, название, артикул)
 *  2. Заказы + выручка + Δ
 *  3. Остатки FBO/FBS
 *  4. Реклама + DRR
 *  5. Себестоимость + маржа
 *  6. Цена
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
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useAppStore } from '@/stores/appStore'
import {
  getWBProductsApi,
  updateWBCostApi,
  uploadWBCostExcelApi,
  downloadWBCostTemplate,
  type WBProduct,
} from '@/api/wb-products'

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

/* WB product image via CDN */
function wbImageUrl(nmId: number): string {
  const vol = Math.floor(nmId / 100000)
  const part = Math.floor(nmId / 1000)
  return `https://basket-${String(vol % 17 + 1).padStart(2, '0')}.wbbasket.ru/vol${vol}/part${part}/${nmId}/images/small/1.webp`
}

const FILTERS = [
  { key: 'all', label: 'Все' },
  { key: 'with_ads', label: 'С рекламой' },
  { key: 'no_ads', label: 'Без рекламы' },
  { key: 'leaders', label: 'Лидеры' },
  { key: 'falling', label: 'Падающие' },
  { key: 'problems', label: 'Нет остатков' },
] as const

type FilterKey = typeof FILTERS[number]['key']

/* ═══════════════════════════════════════════════════════════
   Delta Badge
   ═══════════════════════════════════════════════════════════ */

function DeltaBadge({ value }: { value: number }) {
  if (value === 0) return null
  const positive = value > 0
  return (
    <span className={cn(
      'inline-flex items-center rounded px-1 py-[1px] text-[10px] font-bold leading-tight',
      positive ? 'bg-emerald-500/12 text-emerald-400' : 'bg-red-500/12 text-red-400',
    )}>
      {positive ? '+' : ''}{value}%
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   WB Cost Editor (inline popover)
   ═══════════════════════════════════════════════════════════ */

function WBCostEdit({ product, shopId, onSaved }: {
  product: WBProduct; shopId: number; onSaved: (vc: string, cost: number) => void
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
      await updateWBCostApi({ shop_id: shopId, vendor_code: product.vendor_code, cost_price: n })
      onSaved(product.vendor_code, n)
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
    setVal(product.cost_price > 0 ? product.cost_price.toString() : '')
    setEditing(true)
    setClosing(false)
  }

  return (
    <div className="relative inline-flex" ref={wrapRef}>
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
   Sort Header
   ═══════════════════════════════════════════════════════════ */

function SortTh({
  label, field, sort, order, onSort, className
}: {
  label: string; field: string; sort: string; order: string
  onSort: (f: string) => void; className?: string
}) {
  const active = sort === field
  return (
    <th
      className={cn('px-3 py-3 text-left text-xs uppercase tracking-wide font-semibold cursor-pointer select-none whitespace-nowrap hover:text-[hsl(var(--foreground))] transition-colors', className)}
      onClick={() => onSort(field)}
    >
      <span className="flex items-center gap-1">
        {label}
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
   Main Page Component
   ═══════════════════════════════════════════════════════════ */

export default function WBProductsPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isWB = currentShop?.marketplace === 'wildberries'
  const shopId = currentShop?.id

  const [products, setProducts] = useState<WBProduct[]>([])
  const [total, setTotal] = useState(0)
  const [costMissing, setCostMissing] = useState(0)
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
  const [period, setPeriod] = useState<7 | 30>(7)
  const [uploading, setUploading] = useState(false)
  const [hoverImg, setHoverImg] = useState<{ url: string; x: number; y: number } | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const pageRef = useRef(1)
  const loadingMoreRef = useRef(false)
  const sentinelRef = useRef<HTMLDivElement>(null)

  // Fetch page 1 (reset)
  const fetchProducts = useCallback(async () => {
    if (!shopId || !isWB) return
    setLoading(true)
    try {
      const data = await getWBProductsApi({
        shop_id: shopId!, page: 1, per_page: perPage, sort, order, filter, search, period,
      })
      setProducts(data.products)
      setTotal(data.total)
      setCostMissing(data.cost_missing_count)
      pageRef.current = 1
      setPage(1)
      setHasMore(data.products.length < data.total)
    } catch (e) {
      console.error('Failed to fetch WB products', e)
    } finally {
      setLoading(false)
    }
  }, [shopId, isWB, perPage, sort, order, filter, search, period])

  // Load next page (infinite scroll)
  const loadMore = useCallback(async () => {
    if (!shopId || !isWB || loadingMoreRef.current || !hasMore) return
    loadingMoreRef.current = true
    setLoadingMore(true)
    const nextPage = pageRef.current + 1
    try {
      const data = await getWBProductsApi({
        shop_id: shopId!, page: nextPage, per_page: perPage, sort, order, filter, search, period,
      })
      setProducts((prev) => {
        const existingIds = new Set(prev.map((p) => p.nm_id))
        const newItems = data.products.filter((p) => !existingIds.has(p.nm_id))
        return [...prev, ...newItems]
      })
      pageRef.current = nextPage
      setPage(nextPage)
      setHasMore(data.products.length === perPage)
    } catch (e) {
      console.error('loadMore WB error', e)
    } finally {
      setLoadingMore(false)
      loadingMoreRef.current = false
    }
  }, [shopId, isWB, perPage, sort, order, filter, search, period, hasMore])

  useEffect(() => { fetchProducts() }, [fetchProducts])

  // Infinite scroll observer
  useEffect(() => {
    if (!sentinelRef.current) return
    const obs = new IntersectionObserver((entries) => {
      if (entries[0].isIntersecting) loadMore()
    }, { threshold: 0.1 })
    obs.observe(sentinelRef.current)
    return () => obs.disconnect()
  }, [loadMore])

  function toggleSort(field: string) {
    if (sort === field) {
      setOrder((o) => (o === 'desc' ? 'asc' : 'desc'))
    } else {
      setSort(field)
      setOrder('desc')
    }
  }

  // Debounced search
  useEffect(() => {
    const t = setTimeout(() => setSearch(searchInput), 400)
    return () => clearTimeout(t)
  }, [searchInput])

  // Excel upload
  const handleExcelUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file || !shopId) return
    setUploading(true)
    try {
      const res = await uploadWBCostExcelApi(shopId, file)
      if (res.ok) {
        alert(`✅ Загружено: ${res.updated} товаров`)
        fetchProducts()
      }
      if (res.errors?.length) {
        console.warn('Excel errors:', res.errors)
      }
    } catch {
      alert('Ошибка загрузки файла')
    } finally {
      setUploading(false)
      if (fileInputRef.current) fileInputRef.current.value = ''
    }
  }

  const updateCost = (vendorCode: string, cost: number) => {
    setProducts((prev) =>
      prev.map((p) => p.vendor_code === vendorCode ? { ...p, cost_price: cost } : p)
    )
  }

  // Not a WB shop
  if (!isWB) {
    return (
      <div className="flex flex-col items-center justify-center h-[60vh] gap-4 text-center">
        <Package className="h-12 w-12 text-[hsl(var(--muted-foreground)/0.3)]" />
        <p className="text-[hsl(var(--muted-foreground)/0.7)]">
          Выберите магазин Wildberries для просмотра товаров
        </p>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-4 p-4">

      {/* ── Header ─────────────────────────────────────── */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-bold text-[hsl(var(--foreground))]">Товары WB</h1>
          <p className="text-sm text-[hsl(var(--muted-foreground)/0.6)] mt-0.5">
            {total} товаров
            {costMissing > 0 && (
              <span className="ml-2 text-amber-400">· у {costMissing} не указана с/с</span>
            )}
          </p>
        </div>

        {/* Period selector */}
        <div className="flex items-center gap-2">
          <div className="flex rounded-xl border border-[hsl(var(--border))] overflow-hidden text-sm">
            {([7, 30] as const).map((p) => (
              <button
                key={p}
                onClick={() => setPeriod(p)}
                className={cn(
                  'px-3 py-1.5 font-medium transition-colors',
                  period === p
                    ? 'bg-[hsl(var(--primary))] text-white'
                    : 'text-[hsl(var(--muted-foreground)/0.7)] hover:text-[hsl(var(--foreground))]'
                )}
              >
                {p}д
              </button>
            ))}
          </div>

          {/* Excel templates */}
          <button
            title="Скачать шаблон"
            onClick={() => shopId && downloadWBCostTemplate(shopId)}
            className="flex h-8 w-8 items-center justify-center rounded-lg border border-[hsl(var(--border))] hover:bg-white/5 transition-colors text-[hsl(var(--muted-foreground)/0.6)] hover:text-[hsl(var(--foreground))]"
          >
            <Download className="h-4 w-4" />
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
              'flex h-8 w-8 cursor-pointer items-center justify-center rounded-lg border border-[hsl(var(--border))] transition-colors',
              uploading
                ? 'text-[hsl(var(--primary))]'
                : 'text-[hsl(var(--muted-foreground)/0.6)] hover:text-[hsl(var(--foreground))] hover:bg-white/5'
            )}>
              {uploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
            </div>
          </label>
        </div>
      </div>

      {/* ── Filters + Search ────────────────────────────── */}
      <div className="flex flex-wrap items-center gap-2">
        {FILTERS.map((f) => (
          <button
            key={f.key}
            onClick={() => setFilter(f.key)}
            className={cn(
              'rounded-xl px-3 py-1.5 text-xs font-semibold transition-colors',
              filter === f.key
                ? 'bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))] border border-[hsl(var(--primary)/0.3)]'
                : 'border border-[hsl(var(--border))] text-[hsl(var(--muted-foreground)/0.7)] hover:text-[hsl(var(--foreground))] hover:border-[hsl(var(--border)/0.8)]'
            )}
          >
            {f.label}
          </button>
        ))}

        <div className="ml-auto flex items-center gap-1.5 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] pl-3 pr-2">
          <Search className="h-3.5 w-3.5 text-[hsl(var(--muted-foreground)/0.5)]" />
          <input
            type="text"
            placeholder="Артикул или название..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            className="bg-transparent py-1.5 text-sm text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.4)] focus:outline-none w-48"
          />
        </div>
      </div>

      {/* ── Table ───────────────────────────────────────── */}
      <div className="rounded-2xl border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground)/0.7)] border-b border-[hsl(var(--border))]">
              <tr>
                <th className="px-3 py-3 text-left text-xs uppercase tracking-wide font-semibold w-[280px]">
                  Товар
                </th>
                <SortTh label="Заказы" field="orders_7d" sort={sort} order={order} onSort={toggleSort} />
                <SortTh label="Выручка" field="revenue_7d" sort={sort} order={order} onSort={toggleSort} />
                <th className="px-3 py-3 text-left text-xs uppercase tracking-wide font-semibold">FBO</th>
                <th className="px-3 py-3 text-left text-xs uppercase tracking-wide font-semibold">FBS</th>
                <SortTh label="Реклама" field="ad_spend_7d" sort={sort} order={order} onSort={toggleSort} />
                <SortTh label="DRR" field="drr" sort={sort} order={order} onSort={toggleSort} />
                <SortTh label="Цена" field="current_price" sort={sort} order={order} onSort={toggleSort} />
                <SortTh label="С/с и маржа" field="margin" sort={sort} order={order} onSort={toggleSort} />
              </tr>
            </thead>
            <tbody className="divide-y divide-[hsl(var(--border)/0.5)]">
              {loading ? (
                <tr>
                  <td colSpan={9} className="py-16 text-center">
                    <Loader2 className="h-6 w-6 animate-spin mx-auto text-[hsl(var(--primary)/0.6)]" />
                  </td>
                </tr>
              ) : products.length === 0 ? (
                <tr>
                  <td colSpan={9} className="py-16 text-center text-[hsl(var(--muted-foreground)/0.5)]">
                    <Package className="h-8 w-8 mx-auto mb-2 opacity-30" />
                    Нет данных
                  </td>
                </tr>
              ) : (
                products.map((p) => (
                  <tr
                    key={p.nm_id}
                    className="hover:bg-[hsl(var(--muted)/0.15)] transition-colors group"
                  >
                    {/* Product cell */}
                    <td className="px-3 py-2.5">
                      <div className="flex items-center gap-2.5">
                        {/* Photo */}
                        <div
                          className="relative h-10 w-[30px] shrink-0 cursor-zoom-in"
                          onMouseEnter={(e) => setHoverImg({ url: wbImageUrl(p.nm_id), x: e.clientX, y: e.clientY })}
                          onMouseLeave={() => setHoverImg(null)}
                        >
                          <img
                            src={wbImageUrl(p.nm_id)}
                            alt=""
                            className="h-10 w-[30px] rounded-md object-cover bg-[hsl(var(--muted)/0.3)]"
                            loading="lazy"
                            onError={(e) => { (e.target as HTMLImageElement).style.visibility = 'hidden' }}
                          />
                        </div>
                        {/* Info */}
                        <div className="min-w-0">
                          <p className="font-medium leading-tight truncate max-w-[200px]" title={p.name}>
                            {p.name || p.vendor_code}
                          </p>
                          <p className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)] font-mono mt-0.5">
                            {p.vendor_code}
                          </p>
                        </div>
                      </div>
                    </td>

                    {/* Orders */}
                    <td className="px-3 py-2.5">
                      <span className="font-semibold">{fmtNum(p.orders_7d)}</span>
                    </td>

                    {/* Revenue */}
                    <td className="px-3 py-2.5">
                      <div className="flex flex-col gap-0.5">
                        <span className="font-semibold">{fmtMoney(p.revenue_7d)}</span>
                        <DeltaBadge value={p.revenue_delta} />
                      </div>
                    </td>

                    {/* Stock FBO */}
                    <td className="px-3 py-2.5">
                      <span className={cn(
                        'font-medium',
                        p.stock_fbo === 0 ? 'text-red-400' : 'text-[hsl(var(--foreground)/0.8)]'
                      )}>
                        {fmtNum(p.stock_fbo)}
                      </span>
                    </td>

                    {/* Stock FBS */}
                    <td className="px-3 py-2.5">
                      <span className={cn(
                        'font-medium',
                        p.stock_fbs === 0 ? 'text-[hsl(var(--muted-foreground)/0.4)]' : 'text-[hsl(var(--foreground)/0.8)]'
                      )}>
                        {fmtNum(p.stock_fbs)}
                      </span>
                    </td>

                    {/* Ad spend */}
                    <td className="px-3 py-2.5">
                      {p.ad_spend_7d > 0 ? (
                        <span className="font-medium text-violet-400">{fmtMoney(p.ad_spend_7d)}</span>
                      ) : (
                        <span className="text-[hsl(var(--muted-foreground)/0.35)]">—</span>
                      )}
                    </td>

                    {/* DRR */}
                    <td className="px-3 py-2.5">
                      {p.drr > 0 ? (
                        <span className={cn(
                          'font-semibold',
                          p.drr > 30 ? 'text-red-400' : p.drr > 15 ? 'text-amber-400' : 'text-emerald-400'
                        )}>
                          {p.drr}%
                        </span>
                      ) : (
                        <span className="text-[hsl(var(--muted-foreground)/0.35)]">—</span>
                      )}
                    </td>

                    {/* Price */}
                    <td className="px-3 py-2.5">
                      <span className="font-medium">{fmtMoney(p.current_price)}</span>
                    </td>

                    {/* Cost & Margin */}
                    <td className="px-3 py-2.5">
                      <div className="flex flex-col gap-0.5">
                        <WBCostEdit product={p} shopId={shopId!} onSaved={updateCost} />
                        {p.margin !== null && (
                          <span className={cn(
                            'text-[11px] font-semibold',
                            p.margin < 0 ? 'text-red-400' : p.margin < 10 ? 'text-amber-400' : 'text-emerald-400'
                          )}>
                            {p.margin > 0 ? '+' : ''}{p.margin}%
                          </span>
                        )}
                      </div>
                    </td>
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
          className="pointer-events-none fixed z-50 rounded-xl overflow-hidden shadow-2xl border border-[hsl(var(--border))]"
          style={{ left: hoverImg.x + 16, top: hoverImg.y - 80, width: 120, height: 160 }}
        >
          <img
            src={hoverImg.url}
            alt=""
            className="w-full h-full object-cover"
            onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
          />
        </div>
      )}
    </div>
  )
}
