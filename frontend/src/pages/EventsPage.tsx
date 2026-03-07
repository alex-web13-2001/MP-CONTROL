/**
 * EventsPage — Лента событий.
 *
 * Отображает все события магазина, сгруппированные по дням.
 * Каждое событие привязано к товару (фото, название, артикул).
 */
import { useState, useEffect, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Activity,
  Megaphone,
  Palette,
  DollarSign,
  Image,
  TrendingDown,
  TrendingUp,
  Plus,
  Minus,
  RefreshCw,
  ChevronDown,
  Filter,
  ArrowRight,
  ArrowDown,
  ArrowUp,
  type LucideIcon,
} from 'lucide-react'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import { getEventsFeedApi, type EventsFeedResponse, type EventItem, type EventDay } from '@/api/events'

/* ═══════════════════════════════════════════════════════════
   Constants
   ═══════════════════════════════════════════════════════════ */

const PERIOD_OPTIONS = [
  { key: 'today', label: 'Сегодня' },
  { key: '7d', label: '7 дней' },
  { key: '30d', label: '30 дней' },
  { key: '90d', label: '90 дней' },
] as const

const CATEGORY_OPTIONS = [
  { key: '', label: 'Все', icon: Activity, color: '#a78bfa' },
  { key: 'advertising', label: 'Реклама', icon: Megaphone, color: '#3b82f6' },
  { key: 'content', label: 'Контент', icon: Palette, color: '#10b981' },
  { key: 'commercial', label: 'Коммерция', icon: DollarSign, color: '#f59e0b' },
] as const

/** Icon + color per event type */
const EVENT_STYLE: Record<string, { icon: LucideIcon; color: string; bg: string }> = {
  // Ozon Ads
  OZON_BID_CHANGE:      { icon: TrendingUp,  color: '#3b82f6', bg: '#3b82f620' },
  OZON_STATUS_CHANGE:   { icon: Activity,    color: '#6366f1', bg: '#6366f120' },
  OZON_BUDGET_CHANGE:   { icon: DollarSign,  color: '#8b5cf6', bg: '#8b5cf620' },
  OZON_ITEM_ADD:        { icon: Plus,        color: '#10b981', bg: '#10b98120' },
  OZON_ITEM_REMOVE:     { icon: Minus,       color: '#ef4444', bg: '#ef444420' },
  // Ozon Content
  OZON_SEO_CHANGE:      { icon: Palette,     color: '#14b8a6', bg: '#14b8a620' },
  OZON_PHOTO_CHANGE:    { icon: Image,       color: '#f97316', bg: '#f9731620' },
  OZON_CONTENT_CHANGE:  { icon: Palette,     color: '#14b8a6', bg: '#14b8a620' },
  // WB Ads
  BID_CHANGE:           { icon: TrendingUp,  color: '#3b82f6', bg: '#3b82f620' },
  STATUS_CHANGE:        { icon: Activity,    color: '#6366f1', bg: '#6366f120' },
  ITEM_ADD:             { icon: Plus,        color: '#10b981', bg: '#10b98120' },
  ITEM_REMOVE:          { icon: Minus,       color: '#ef4444', bg: '#ef444420' },
  ITEM_INACTIVE:        { icon: Minus,       color: '#f59e0b', bg: '#f59e0b20' },
  // WB Content
  CONTENT_CHANGE:           { icon: Palette, color: '#14b8a6', bg: '#14b8a620' },
  CONTENT_TITLE_CHANGED:    { icon: Palette, color: '#14b8a6', bg: '#14b8a620' },
  CONTENT_DESC_CHANGED:     { icon: Palette, color: '#0ea5e9', bg: '#0ea5e920' },
  CONTENT_MAIN_PHOTO_CHANGED:  { icon: Image, color: '#f97316', bg: '#f9731620' },
  CONTENT_PHOTO_ORDER_CHANGED: { icon: Image, color: '#f97316', bg: '#f9731620' },
  // Commercial
  PRICE_CHANGE:         { icon: DollarSign,  color: '#f59e0b', bg: '#f59e0b20' },
  OZON_PRICE_CHANGE:    { icon: DollarSign,  color: '#f59e0b', bg: '#f59e0b20' },
  OZON_STOCK_OUT:       { icon: TrendingDown,color: '#ef4444', bg: '#ef444420' },
  OZON_STOCK_REPLENISH: { icon: TrendingUp,  color: '#10b981', bg: '#10b98120' },
  STOCK_OUT:            { icon: TrendingDown,color: '#ef4444', bg: '#ef444420' },
  STOCK_REPLENISH:      { icon: TrendingUp,  color: '#10b981', bg: '#10b98120' },
}

const DEFAULT_STYLE = { icon: Activity, color: '#a78bfa', bg: '#a78bfa20' }

const CATEGORY_COLORS: Record<string, string> = {
  advertising: '#3b82f6',
  content: '#10b981',
  commercial: '#f59e0b',
  other: '#a78bfa',
}

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

const MONTHS_FULL = [
  'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
  'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря',
]
const DAYS_FULL = ['воскресенье', 'понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота']

function formatDayHeader(dateStr: string): string {
  const d = new Date(dateStr + 'T00:00:00')
  const today = new Date()
  const yesterday = new Date()
  yesterday.setDate(today.getDate() - 1)

  const isSameDay = (a: Date, b: Date) =>
    a.getFullYear() === b.getFullYear() &&
    a.getMonth() === b.getMonth() &&
    a.getDate() === b.getDate()

  if (isSameDay(d, today)) return 'Сегодня'
  if (isSameDay(d, yesterday)) return 'Вчера'

  const dayOfWeek = DAYS_FULL[d.getDay()]
  return `${d.getDate()} ${MONTHS_FULL[d.getMonth()]} — ${dayOfWeek}`
}

function formatEventTime(isoStr: string): string {
  const d = new Date(isoStr)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`
}

/** Contextual placeholder icon based on event category */
const CATEGORY_PLACEHOLDER: Record<string, LucideIcon> = {
  advertising: Megaphone,
  content: Palette,
  commercial: DollarSign,
  other: Activity,
}

/** Numeric event types where we show old→new with delta */
const NUMERIC_EVENT_TYPES = new Set([
  'OZON_BID_CHANGE', 'BID_CHANGE',
  'OZON_BUDGET_CHANGE',
  'PRICE_CHANGE', 'OZON_PRICE_CHANGE',
])

/** Parse a numeric string, stripping currency symbols */
function parseNum(val: string | null): number | null {
  if (!val) return null
  const cleaned = val.replace(/[^\d.,\-]/g, '').replace(',', '.')
  const n = parseFloat(cleaned)
  return isFinite(n) ? n : null
}

/** Format number for display */
function fmtNum(n: number, suffix = ''): string {
  if (Math.abs(n) >= 1000) {
    return n.toLocaleString('ru-RU', { maximumFractionDigits: 2 }) + suffix
  }
  return n.toFixed(2).replace(/\.?0+$/, '') + suffix
}

/** Value Change display — shows old→new with colored delta */
function ValueChange({ event }: { event: EventItem }) {
  const oldNum = parseNum(event.old_value)
  const newNum = parseNum(event.new_value)

  if (oldNum === null || newNum === null) return null

  const delta = newNum - oldNum
  const isUp = delta > 0
  const isDown = delta < 0
  const pct = oldNum !== 0 ? Math.abs((delta / oldNum) * 100) : 0

  // Determine suffix from event type
  const suffix = event.event_type.includes('BID') ? ' ₽'
    : event.event_type.includes('BUDGET') ? ' ₽'
    : event.event_type.includes('PRICE') ? ' ₽'
    : ''

  return (
    <div className="flex items-center gap-3 mt-2.5">
      {/* Old value */}
      <span className="text-[15px] font-medium text-[hsl(var(--muted-foreground)/0.7)] line-through decoration-[hsl(var(--muted-foreground)/0.3)]">
        {fmtNum(oldNum, suffix)}
      </span>

      <ArrowRight className="h-3.5 w-3.5 text-[hsl(var(--muted-foreground)/0.4)] shrink-0" />

      {/* New value */}
      <span className={`text-[16px] font-bold ${isUp ? 'text-emerald-400' : isDown ? 'text-red-400' : 'text-[hsl(var(--foreground))]'}`}>
        {fmtNum(newNum, suffix)}
      </span>

      {/* Delta badge */}
      {delta !== 0 && (
        <span
          className={`inline-flex items-center gap-0.5 px-2 py-0.5 rounded-md text-[12px] font-semibold
            ${isUp
              ? 'bg-emerald-500/15 text-emerald-400'
              : 'bg-red-500/15 text-red-400'
            }`}
        >
          {isUp ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />}
          {pct >= 0.1 ? `${pct.toFixed(1)}%` : `${isUp ? '+' : ''}${fmtNum(delta, suffix)}`}
        </span>
      )}
    </div>
  )
}

function EventCard({ event, index }: { event: EventItem; index: number }) {
  const style = EVENT_STYLE[event.event_type] || DEFAULT_STYLE
  const Icon = style.icon
  const catColor = CATEGORY_COLORS[event.category] || CATEGORY_COLORS.other
  const [imgError, setImgError] = useState(false)

  const hasImage = event.product?.image_url && !imgError
  const isNumericEvent = NUMERIC_EVENT_TYPES.has(event.event_type)

  // Contextual placeholder
  const PlaceholderIcon = CATEGORY_PLACEHOLDER[event.category] || Activity

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, delay: Math.min(index * 0.03, 0.3) }}
      className="group relative flex gap-4 rounded-2xl border border-[hsl(var(--border)/0.4)] bg-[hsl(var(--card))] p-5
                 hover:border-[hsl(var(--border)/0.7)] hover:shadow-lg hover:shadow-black/5
                 transition-all duration-200"
    >
      {/* Category accent line */}
      <div
        className="absolute left-0 top-4 bottom-4 w-[4px] rounded-full"
        style={{ background: catColor }}
      />

      {/* Product image or contextual placeholder */}
      <div className="shrink-0 ml-2">
        {hasImage ? (
          <img
            src={event.product!.image_url}
            alt={event.product!.name || 'Product'}
            className="w-[64px] h-[85px] rounded-xl object-cover border border-[hsl(var(--border)/0.3)]"
            loading="lazy"
            onError={() => setImgError(true)}
          />
        ) : (
          <div
            className="w-[64px] h-[85px] rounded-xl flex items-center justify-center border"
            style={{
              background: `${catColor}12`,
              borderColor: `${catColor}30`,
            }}
          >
            <PlaceholderIcon className="h-7 w-7" style={{ color: catColor, opacity: 0.6 }} />
          </div>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0">
        {/* Row 1: Event type badge */}
        <div className="flex items-center gap-2.5">
          <div
            className="shrink-0 h-7 w-7 rounded-lg flex items-center justify-center"
            style={{ background: style.bg }}
          >
            <Icon className="h-4 w-4" style={{ color: style.color }} />
          </div>
          <span className="text-[15px] font-semibold" style={{ color: style.color }}>
            {event.label}
          </span>
        </div>

        {/* Row 2: Detail text (non-numeric events) */}
        {event.detail && !isNumericEvent && (
          <p className="mt-1.5 text-[14px] text-[hsl(var(--foreground)/0.8)] leading-relaxed font-medium">
            {event.detail}
          </p>
        )}

        {/* Row 2b: Value change (numeric events) */}
        {isNumericEvent && <ValueChange event={event} />}

        {/* Row 3: Product name */}
        {event.product && event.product.name ? (
          <p className="mt-2.5 text-[14px] font-medium text-[hsl(var(--foreground)/0.85)] leading-snug line-clamp-2">
            {event.product.name}
          </p>
        ) : event.product ? (
          <p className="mt-2.5 text-[14px] font-medium text-[hsl(var(--foreground)/0.6)] leading-snug">
            SKU {event.product.nm_id}
          </p>
        ) : null}

        {/* Row 4: Article (offer_id) */}
        {event.product?.offer_id && (
          <p className="mt-1 text-[12px] font-semibold font-mono text-[hsl(var(--muted-foreground)/0.7)] tracking-wide uppercase">
            {event.product.offer_id}
          </p>
        )}

        {/* Campaign info */}
        {event.advert_id ? (
          <div className="flex items-center gap-2 mt-3 px-3 py-2 rounded-lg bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.2)]">
            <Megaphone className="h-4 w-4 shrink-0" style={{ color: catColor, opacity: 0.7 }} />
            <span className="text-[13px] font-semibold text-[hsl(var(--foreground)/0.8)] truncate max-w-[450px]">
              {event.campaign_title
                ? event.campaign_title
                : `Кампания #${event.advert_id}`}
            </span>
            <span className="text-[11px] font-mono px-1.5 py-0.5 rounded bg-[hsl(var(--muted)/0.15)] text-[hsl(var(--muted-foreground)/0.6)]">
              #{event.advert_id}
            </span>
          </div>
        ) : null}
      </div>

      {/* Time */}
      <div className="shrink-0 text-right pt-1">
        <span className="text-[13px] font-medium text-[hsl(var(--muted-foreground)/0.5)]">
          {event.created_at ? formatEventTime(event.created_at) : ''}
        </span>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Day Group Component
   ═══════════════════════════════════════════════════════════ */

function DayGroup({ day, dayIndex }: { day: EventDay; dayIndex: number }) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay: dayIndex * 0.1 }}
    >
      {/* Day header */}
      <div className="flex items-center gap-3 mb-4">
        <h3 className="text-[17px] font-bold text-[hsl(var(--foreground))]">
          {formatDayHeader(day.date)}
        </h3>
        <span className="inline-flex items-center justify-center rounded-full bg-[hsl(var(--muted)/0.3)] px-3 py-0.5 text-[12px] font-semibold text-[hsl(var(--muted-foreground))]">
          {day.events.length}
        </span>
        <div className="flex-1 h-px bg-[hsl(var(--border)/0.3)]" />
      </div>

      {/* Events */}
      <div className="space-y-3 pl-0">
        {day.events.map((event, i) => (
          <EventCard key={event.id} event={event} index={i} />
        ))}
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Loading Skeleton
   ═══════════════════════════════════════════════════════════ */

function EventsSkeleton() {
  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div className="space-y-2">
          <Skeleton className="h-8 w-32" />
          <Skeleton className="h-4 w-48" />
        </div>
        <Skeleton className="h-9 w-52 rounded-lg" />
      </div>
      <div className="flex gap-2">
        {[1, 2, 3, 4].map(i => (
          <Skeleton key={i} className="h-9 w-24 rounded-full" />
        ))}
      </div>
      {[1, 2, 3].map(d => (
        <div key={d} className="space-y-3">
          <Skeleton className="h-5 w-40" />
          {[1, 2, 3].map(e => (
            <Skeleton key={e} className="h-[88px] rounded-xl" />
          ))}
        </div>
      ))}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Empty State
   ═══════════════════════════════════════════════════════════ */

function EmptyState() {
  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      className="flex flex-col items-center justify-center py-24 gap-4"
    >
      <div className="h-16 w-16 rounded-2xl bg-[hsl(var(--muted)/0.2)] flex items-center justify-center">
        <Activity className="h-8 w-8 text-[hsl(var(--muted-foreground)/0.3)]" />
      </div>
      <div className="text-center">
        <p className="text-[15px] font-semibold text-[hsl(var(--foreground)/0.7)]">
          Нет событий за этот период
        </p>
        <p className="mt-1 text-[13px] text-[hsl(var(--muted-foreground)/0.5)]">
          Попробуйте выбрать другой период или снять фильтры
        </p>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function EventsPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const shopId = currentShop?.id

  const [data, setData] = useState<EventsFeedResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState('7d')
  const [category, setCategory] = useState('')
  const [page, setPage] = useState(1)
  const [loadingMore, setLoadingMore] = useState(false)

  const fetchData = useCallback(async (pageNum = 1, append = false) => {
    if (!shopId) return

    if (append) {
      setLoadingMore(true)
    } else {
      setLoading(true)
    }
    setError(null)

    try {
      const result = await getEventsFeedApi({
        shop_id: shopId,
        period,
        category: category || undefined,
        page: pageNum,
        page_size: 50,
      })

      if (append && data) {
        // Merge days
        const mergedDays = [...data.days]
        for (const day of result.days) {
          const existing = mergedDays.find(d => d.date === day.date)
          if (existing) {
            existing.events.push(...day.events)
          } else {
            mergedDays.push(day)
          }
        }
        // Sort days descending
        mergedDays.sort((a, b) => b.date.localeCompare(a.date))
        setData({ ...result, days: mergedDays })
      } else {
        setData(result)
      }
      setPage(pageNum)
    } catch (e: any) {
      console.error('Events fetch error:', e)
      setError(e.response?.data?.detail || 'Ошибка загрузки событий')
    } finally {
      setLoading(false)
      setLoadingMore(false)
    }
  }, [shopId, period, category, data])

  // Reset and fetch on filter change
  useEffect(() => {
    setPage(1)
    setData(null)
    fetchData(1, false)
  }, [shopId, period, category]) // eslint-disable-line react-hooks/exhaustive-deps

  const totalLoadedEvents = data?.days.reduce((sum, d) => sum + d.events.length, 0) || 0
  const hasMore = data ? totalLoadedEvents < data.total : false

  if (loading && !data) {
    return <EventsSkeleton />
  }

  if (error && !data) {
    return (
      <div className="flex h-[60vh] flex-col items-center justify-center gap-4">
        <p className="text-red-400">{error}</p>
        <button
          onClick={() => fetchData(1, false)}
          className="inline-flex items-center gap-2 rounded-lg bg-[hsl(var(--primary))] px-4 py-2 text-sm font-medium text-white"
        >
          <RefreshCw className="h-4 w-4" /> Повторить
        </button>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* ── Header ── */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[hsl(var(--foreground))]">Лента событий</h1>
          <p className="mt-1 text-sm text-[hsl(var(--muted-foreground))]">
            {data?.total ?? 0} событий за выбранный период
          </p>
        </div>

        {/* Period selector */}
        <div className="flex items-center gap-1 bg-[hsl(var(--muted)/0.15)] rounded-xl p-1">
          {PERIOD_OPTIONS.map(opt => (
            <button
              key={opt.key}
              onClick={() => setPeriod(opt.key)}
              className={`px-3.5 py-1.5 rounded-lg text-[13px] font-medium transition-all duration-200 ${
                period === opt.key
                  ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
                  : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.3)]'
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Category filter ── */}
      <div className="flex flex-wrap items-center gap-2">
        <Filter className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.5)]" />
        {CATEGORY_OPTIONS.map(opt => {
          const isActive = category === opt.key
          const Icon = opt.icon
          return (
            <button
              key={opt.key}
              onClick={() => setCategory(opt.key)}
              className="inline-flex items-center gap-1.5 rounded-full px-3.5 py-1.5 text-[13px] font-medium transition-all duration-200"
              style={{
                background: isActive ? opt.color + '18' : 'transparent',
                border: `1.5px solid ${isActive ? opt.color : 'hsl(var(--border))'}`,
                color: isActive ? opt.color : 'hsl(var(--muted-foreground))',
              }}
            >
              <Icon className="h-3.5 w-3.5" />
              {opt.label}
            </button>
          )
        })}
      </div>

      {/* ── Event feed ── */}
      <AnimatePresence mode="wait">
        {data && data.days.length > 0 ? (
          <motion.div
            key={`${period}-${category}`}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="space-y-8"
          >
            {data.days.map((day, i) => (
              <DayGroup key={day.date} day={day} dayIndex={i} />
            ))}

            {/* Load more */}
            {hasMore && (
              <div className="flex justify-center pt-4">
                <button
                  onClick={() => fetchData(page + 1, true)}
                  disabled={loadingMore}
                  className="inline-flex items-center gap-2 rounded-xl bg-[hsl(var(--muted)/0.2)] px-6 py-2.5 text-[13px] font-medium
                             text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.35)]
                             transition-all duration-200 disabled:opacity-50"
                >
                  {loadingMore ? (
                    <>
                      <RefreshCw className="h-4 w-4 animate-spin" />
                      Загрузка...
                    </>
                  ) : (
                    <>
                      <ChevronDown className="h-4 w-4" />
                      Загрузить ещё ({data.total - totalLoadedEvents})
                    </>
                  )}
                </button>
              </div>
            )}
          </motion.div>
        ) : !loading ? (
          <EmptyState />
        ) : null}
      </AnimatePresence>

      {/* Loading overlay for re-fetch */}
      {loading && data && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/10 backdrop-blur-[1px]">
          <div className="rounded-xl bg-[hsl(var(--card))] border border-[hsl(var(--border))] px-6 py-4 shadow-2xl flex items-center gap-3">
            <RefreshCw className="h-5 w-5 animate-spin text-[hsl(var(--primary))]" />
            <span className="text-sm font-medium">Обновление событий...</span>
          </div>
        </div>
      )}
    </div>
  )
}
