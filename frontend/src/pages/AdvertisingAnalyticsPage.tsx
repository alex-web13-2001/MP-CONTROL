import { useState, useEffect, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  DollarSign,
  ShoppingCart,
  ShoppingBag,
  Eye,
  MousePointerClick,
  Percent,
  TrendingUp,
  ArrowUpRight,
  ArrowDownRight,
  RefreshCw,
  Megaphone,
  Target,
  XCircle,
  BarChart2,
  Zap,
  Package,
  FileText,
  X,
  Clock,
  Tag,
} from 'lucide-react'
import {
  ComposedChart,
  Bar,
  Line,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getAdvertisingAnalytics,
  getEventsDetail,
  type AdvertisingAnalyticsResponse,
  type AdvertisingDailyPoint,
  type CampaignRow,
  type EventDaySummary,
  type EventDetail,
} from '@/api/advertising'

/* ═══════════════════════════════════════════════════════════
   Constants & Helpers
   ═══════════════════════════════════════════════════════════ */

const PERIODS = [
  { key: 'today', label: 'Сегодня' },
  { key: '7d', label: '7 дней' },
  { key: '14d', label: '14 дней' },
  { key: '30d', label: '30 дней' },
] as const

function formatMoney(value: number): string {
  return value.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function formatNumber(value: number): string {
  return value.toLocaleString('ru-RU')
}

function formatDelta(value: number, invert = false): { text: string; positive: boolean } {
  const sign = value > 0 ? '+' : ''
  const isUp = value > 0
  return {
    text: `${sign}${value.toFixed(1)}%`,
    positive: invert ? !isUp : isUp,
  }
}

const MONTHS_SHORT = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
const MONTHS_FULL = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
const DAYS_SHORT = ['вс.', 'пн.', 'вт.', 'ср.', 'чт.', 'пт.', 'сб.']

function formatChartDate(dateStr: string): string {
  const parts = dateStr.split('-')
  if (parts.length >= 3) {
    const day = parseInt(parts[2], 10)
    const month = parseInt(parts[1], 10) - 1
    return `${day} ${MONTHS_SHORT[month] || parts[1]}`
  }
  return dateStr.slice(5)
}

function formatTooltipDate(dateStr: string): string {
  const parts = dateStr.split('-')
  if (parts.length >= 3) {
    const y = parseInt(parts[0], 10)
    const m = parseInt(parts[1], 10) - 1
    const d = parseInt(parts[2], 10)
    const dt = new Date(y, m, d)
    const dayOfWeek = DAYS_SHORT[dt.getDay()]
    return `${d} ${MONTHS_FULL[m]} (${dayOfWeek})`
  }
  return dateStr
}

/* ═══════════════════════════════════════════════════════════
   KPI Card Component
   ═══════════════════════════════════════════════════════════ */

function KpiCard({
  title,
  value,
  subtitle,
  delta,
  invertDelta,
  icon: Icon,
  accent,
  delay,
}: {
  title: string
  value: string
  subtitle?: string
  delta: number
  invertDelta?: boolean
  icon: React.ElementType
  accent: string
  delay: number
}) {
  const d = formatDelta(delta, invertDelta)
  const isZero = delta === 0

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay, ease: 'easeOut' }}
      className="min-w-0"
    >
      <Card className="group relative overflow-hidden hover:shadow-xl hover:shadow-[hsl(var(--primary)/0.06)] transition-all duration-300 hover:-translate-y-0.5">
        <div
          className="absolute inset-x-0 top-0 h-[2px] opacity-60"
          style={{ background: `linear-gradient(90deg, transparent, ${accent}, transparent)` }}
        />
        <CardContent className="p-5">
          <div className="flex items-start justify-between">
            <div className="space-y-2 min-w-0">
              <p className="text-sm font-medium text-[hsl(var(--muted-foreground))] truncate">
                {title}
              </p>
              <p className="text-2xl font-bold tracking-tight text-[hsl(var(--foreground))]">
                {value}
              </p>
              <div className="flex items-center gap-2">
                {!isZero && (
                  <span
                    className={`inline-flex items-center gap-0.5 rounded-full px-2 py-0.5 text-xs font-semibold ${
                      d.positive
                        ? 'bg-emerald-500/10 text-emerald-400'
                        : 'bg-red-500/10 text-red-400'
                    }`}
                  >
                    {d.positive ? (
                      <ArrowUpRight className="h-3 w-3" />
                    ) : (
                      <ArrowDownRight className="h-3 w-3" />
                    )}
                    {d.text}
                  </span>
                )}
                {isZero && (
                  <span className="text-xs text-[hsl(var(--muted-foreground)/0.5)]">
                    Нет изменений
                  </span>
                )}
                {subtitle && (
                  <span className="text-xs text-[hsl(var(--muted-foreground)/0.6)] truncate">
                    {subtitle}
                  </span>
                )}
              </div>
            </div>
            <div
              className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl transition-transform duration-300 group-hover:scale-110"
              style={{ background: `${accent}15` }}
            >
              <Icon className="h-5 w-5" style={{ color: accent }} />
            </div>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Period Selector
   ═══════════════════════════════════════════════════════════ */

function PeriodSelector({
  current,
  onChange,
}: {
  current: string
  onChange: (period: string) => void
}) {
  return (
    <div className="inline-flex rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1">
      {PERIODS.map((p) => (
        <button
          key={p.key}
          onClick={() => onChange(p.key)}
          className={`rounded-md px-5 py-2 text-sm font-medium transition-all duration-200 ${
            current === p.key
              ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
              : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
          }`}
        >
          {p.label}
        </button>
      ))}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Ads Chart (with toggleable metrics)
   ═══════════════════════════════════════════════════════════ */

const ADS_METRICS = [
  { key: 'spend', label: 'Расход ₽', color: '#f97316', yAxis: 'left' },
  { key: 'views', label: 'Показы', color: '#3b82f6', yAxis: 'right' },
  { key: 'clicks', label: 'Клики', color: '#06b6d4', yAxis: 'right' },
  { key: 'cart', label: 'Корзины', color: '#8b5cf6', yAxis: 'right' },
  { key: 'orders', label: 'Заказы', color: '#10b981', yAxis: 'left' },
  { key: 'revenue', label: 'Выручка', color: '#22c55e', yAxis: 'left' },
  { key: 'ctr', label: 'CTR %', color: '#facc15', yAxis: 'percent' },
  { key: 'drr', label: 'ДРР %', color: '#ef4444', yAxis: 'percent' },
] as const

type AdsMetricKey = typeof ADS_METRICS[number]['key']

const ADS_METRIC_LABELS: Record<string, string> = {
  spend: 'Расход',
  views: 'Показы',
  clicks: 'Клики',
  cart: 'Корзины',
  orders: 'Заказы',
  revenue: 'Выручка',
  ctr: 'CTR',
  drr: 'ДРР',
}

const EVENT_CATEGORIES_CONFIG = [
  { key: 'advertising', label: 'Реклама', color: '#f59e0b', icon: Megaphone },
  { key: 'content', label: 'Контент', color: '#8b5cf6', icon: FileText },
  { key: 'price', label: 'Цена', color: '#06b6d4', icon: Tag },
  { key: 'stock', label: 'Склад', color: '#ef4444', icon: Package },
] as const

type EventCategoryKey = typeof EVENT_CATEGORIES_CONFIG[number]['key']

interface AdsChartProps {
  data: AdvertisingDailyPoint[]
  eventsByDay: Record<string, EventDaySummary>
  shopId: number
}

function AdsChart({ data, eventsByDay, shopId }: AdsChartProps) {
  const [activeMetrics, setActiveMetrics] = useState<Set<AdsMetricKey>>(
    new Set<AdsMetricKey>(['spend', 'orders', 'drr'])
  )
  const [showEvents, setShowEvents] = useState(true)
  const [activeEventCats, setActiveEventCats] = useState<Set<EventCategoryKey>>(
    new Set<EventCategoryKey>(['advertising', 'content', 'price', 'stock'])
  )
  const [selectedEventDate, setSelectedEventDate] = useState<string | null>(null)

  const toggleMetric = (key: AdsMetricKey) => {
    setActiveMetrics(prev => {
      const next = new Set(prev)
      if (next.has(key)) {
        if (next.size > 1) next.delete(key)
      } else {
        next.add(key)
      }
      return next
    })
  }

  const toggleEventCat = (key: EventCategoryKey) => {
    setActiveEventCats(prev => {
      const next = new Set(prev)
      if (next.has(key)) {
        next.delete(key)
        if (next.size === 0) setShowEvents(false)
      } else {
        next.add(key)
        setShowEvents(true)
      }
      return next
    })
  }

  const hasRightAxis = ADS_METRICS.some(
    m => activeMetrics.has(m.key) && m.yAxis === 'right'
  )
  const hasLeftAxis = ADS_METRICS.some(
    m => activeMetrics.has(m.key) && m.yAxis === 'left'
  )
  const hasPercentAxis = ADS_METRICS.some(
    m => activeMetrics.has(m.key) && m.yAxis === 'percent'
  )

  // Filter event days by active categories
  const visibleEventDates = showEvents
    ? Object.entries(eventsByDay).filter(([, summary]) => {
        return Array.from(activeEventCats).some(cat => (summary as any)[cat] > 0)
      }).map(([dateStr]) => dateStr)
    : []

  if (!data.length) {
    return (
      <div className="flex h-[300px] items-center justify-center rounded-xl border border-dashed border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.15)]">
        <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">Нет рекламных данных</p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Metric toggle chips */}
      <div className="flex flex-wrap gap-2">
        {ADS_METRICS.map(m => {
          const isActive = activeMetrics.has(m.key)
          return (
            <button
              key={m.key}
              onClick={() => toggleMetric(m.key)}
              className="inline-flex items-center gap-1.5 rounded-full px-3.5 py-1.5 text-[13px] font-medium transition-all duration-200"
              style={{
                background: isActive ? m.color + '20' : 'transparent',
                border: `1.5px solid ${isActive ? m.color : 'hsl(var(--border))'}`,
                color: isActive ? m.color : 'hsl(var(--muted-foreground))',
              }}
            >
              <span
                className="h-2 w-2 rounded-full transition-all"
                style={{ background: isActive ? m.color : 'hsl(var(--muted-foreground)/0.3)' }}
              />
              {m.label}
            </button>
          )
        })}

        {/* Separator */}
        <div className="w-px bg-[hsl(var(--border))] mx-1 self-stretch" />

        {/* Event category toggle chips */}
        <button
          onClick={() => {
            setShowEvents(!showEvents)
            if (!showEvents) setActiveEventCats(new Set(['advertising', 'content', 'price', 'stock']))
          }}
          className={`inline-flex items-center gap-1.5 rounded-full px-3.5 py-1.5 text-[13px] font-medium transition-all duration-200 ${
            showEvents
              ? 'bg-amber-500/20 border-amber-500 text-amber-500'
              : 'bg-transparent text-[hsl(var(--muted-foreground))]'
          }`}
          style={{ border: `1.5px solid ${showEvents ? '#f59e0b' : 'hsl(var(--border))'}` }}
        >
          <Zap className="h-3 w-3" />
          События
        </button>

        {showEvents && EVENT_CATEGORIES_CONFIG.map(cat => {
          const isActive = activeEventCats.has(cat.key)
          const Icon = cat.icon
          return (
            <button
              key={cat.key}
              onClick={() => toggleEventCat(cat.key)}
              className="inline-flex items-center gap-1.5 rounded-full px-3 py-1.5 text-[12px] font-medium transition-all duration-200"
              style={{
                background: isActive ? cat.color + '15' : 'transparent',
                border: `1.5px solid ${isActive ? cat.color : 'hsl(var(--border)/0.5)'}`,
                color: isActive ? cat.color : 'hsl(var(--muted-foreground)/0.5)',
              }}
            >
              <Icon className="h-3 w-3" />
              {cat.label}
            </button>
          )
        })}
      </div>

      {/* Chart */}
      <ResponsiveContainer width="100%" height={360}>
        <ComposedChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 40 }}>
          <defs>
            <linearGradient id="adSpendGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#f97316" stopOpacity={0.4} />
              <stop offset="100%" stopColor="#f97316" stopOpacity={0.05} />
            </linearGradient>
            <linearGradient id="adOrdersGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#10b981" stopOpacity={0.7} />
              <stop offset="100%" stopColor="#10b981" stopOpacity={0.2} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.4} />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
            tickFormatter={formatChartDate}
            interval={0}
            angle={-45}
            textAnchor="end"
            height={50}
            axisLine={false}
            tickLine={false}
          />
          {hasLeftAxis && (
            <YAxis
              yAxisId="left"
              tick={{ fontSize: 12, fill: 'hsl(var(--muted-foreground))' }}
              tickFormatter={(v: number) =>
                v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v.toString()
              }
              axisLine={false}
              tickLine={false}
              width={50}
            />
          )}
          {hasRightAxis && (
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
              tickFormatter={(v: number) =>
                v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v.toString()
              }
              axisLine={false}
              tickLine={false}
              width={50}
            />
          )}
          {hasPercentAxis && (
            <YAxis
              yAxisId="percent"
              orientation={hasRightAxis ? 'left' : 'right'}
              tick={{ fontSize: 12, fill: '#ef4444' }}
              tickFormatter={(v: number) => `${v}%`}
              axisLine={false}
              tickLine={false}
              width={45}
              domain={[0, 'auto']}
            />
          )}
          <Tooltip
            contentStyle={{
              background: 'hsl(var(--card))',
              border: '1px solid hsl(var(--border))',
              borderRadius: '8px',
              fontSize: '13px',
            }}
            formatter={(value: number, name: string) => [
              name === 'spend' || name === 'revenue' ? formatMoney(value)
                : (name === 'drr' || name === 'ctr') ? `${value.toFixed(1)}%`
                : formatNumber(value),
              ADS_METRIC_LABELS[name] || name,
            ]}
            labelFormatter={(label: string) => {
              const base = formatTooltipDate(label)
              const ev = eventsByDay[label]
              if (ev && showEvents && ev.total > 0) {
                const parts: string[] = []
                if (activeEventCats.has('advertising') && ev.advertising > 0) parts.push(`📣 ${ev.advertising} рекл.`)
                if (activeEventCats.has('content') && ev.content > 0) parts.push(`📝 ${ev.content} конт.`)
                if (activeEventCats.has('price') && ev.price > 0) parts.push(`💰 ${ev.price} цена`)
                if (activeEventCats.has('stock') && ev.stock > 0) parts.push(`📦 ${ev.stock} склад`)
                if (parts.length > 0) return `${base}\n⚡ ${parts.join(' · ')}`
              }
              return base
            }}
          />
          <Legend
            verticalAlign="top"
            height={30}
            formatter={(value: string) => ADS_METRIC_LABELS[value] || value}
            wrapperStyle={{ fontSize: '12px', color: 'hsl(var(--muted-foreground))' }}
          />

          {/* Event ReferenceLine markers */}
          {visibleEventDates.map(dateStr => {
            const ev = eventsByDay[dateStr]
            if (!ev) return null
            // Pick dominant category color
            let color = '#f59e0b'
            if (ev.stock > 0 && activeEventCats.has('stock')) color = '#ef4444'
            else if (ev.advertising > 0 && activeEventCats.has('advertising')) color = '#f59e0b'
            else if (ev.content > 0 && activeEventCats.has('content')) color = '#8b5cf6'
            else if (ev.price > 0 && activeEventCats.has('price')) color = '#06b6d4'
            return (
              <ReferenceLine
                key={`ev-${dateStr}`}
                x={dateStr}
                stroke={color}
                strokeWidth={2}
                strokeDasharray="4 2"
                strokeOpacity={0.7}
                yAxisId={hasLeftAxis ? 'left' : hasRightAxis ? 'right' : 'percent'}
                label={{
                  value: `⚡${ev.total}`,
                  position: 'top',
                  fill: color,
                  fontSize: 11,
                  fontWeight: 600,
                  cursor: 'pointer',
                }}
                cursor="pointer"
                onClick={() => setSelectedEventDate(dateStr)}
              />
            )
          })}

          {activeMetrics.has('spend') && (
            <Area
              yAxisId="left"
              type="monotone"
              dataKey="spend"
              fill="url(#adSpendGrad)"
              stroke="#f97316"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#f97316' }}
            />
          )}

          {activeMetrics.has('orders') && (
            <Bar
              yAxisId="left"
              dataKey="orders"
              fill="url(#adOrdersGrad)"
              radius={[3, 3, 0, 0]}
              barSize={16}
            />
          )}

          {activeMetrics.has('revenue') && (
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="revenue"
              stroke="#22c55e"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#22c55e' }}
            />
          )}

          {activeMetrics.has('views') && (
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="views"
              stroke="#3b82f6"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#3b82f6' }}
            />
          )}

          {activeMetrics.has('clicks') && (
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="clicks"
              stroke="#06b6d4"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#06b6d4' }}
            />
          )}

          {activeMetrics.has('cart') && (
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="cart"
              stroke="#8b5cf6"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#8b5cf6' }}
            />
          )}

          {activeMetrics.has('ctr') && (
            <Line
              yAxisId="percent"
              type="monotone"
              dataKey="ctr"
              stroke="#facc15"
              strokeWidth={2}
              strokeDasharray="6 3"
              dot={false}
              activeDot={{ r: 4, fill: '#facc15' }}
            />
          )}

          {activeMetrics.has('drr') && (
            <Line
              yAxisId="percent"
              type="monotone"
              dataKey="drr"
              stroke="#ef4444"
              strokeWidth={2}
              strokeDasharray="6 3"
              dot={false}
              activeDot={{ r: 4, fill: '#ef4444' }}
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {/* Events Detail Modal */}
      {selectedEventDate && (
        <EventsDetailModal
          shopId={shopId}
          date={selectedEventDate}
          summary={eventsByDay[selectedEventDate]}
          onClose={() => setSelectedEventDate(null)}
        />
      )}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Events Detail Modal
   ═══════════════════════════════════════════════════════════ */

const CATEGORY_COLORS: Record<string, string> = {
  advertising: '#f59e0b',
  content: '#8b5cf6',
  price: '#06b6d4',
  stock: '#ef4444',
}
const CATEGORY_LABELS: Record<string, string> = {
  advertising: 'Реклама',
  content: 'Контент',
  price: 'Цена',
  stock: 'Склад',
}
const CATEGORY_ICONS: Record<string, any> = {
  advertising: Megaphone,
  content: FileText,
  price: Tag,
  stock: Package,
}

function EventsDetailModal({
  shopId,
  date,
  summary,
  onClose,
}: {
  shopId: number
  date: string
  summary?: EventDaySummary
  onClose: () => void
}) {
  const [events, setEvents] = useState<EventDetail[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    getEventsDetail(shopId, date)
      .then(res => setEvents(res.events))
      .catch(() => setEvents([]))
      .finally(() => setLoading(false))
  }, [shopId, date])

  const formatModalDate = (d: string) => {
    const [y, m, day] = d.split('-')
    const months = ['', 'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
      'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
    return `${parseInt(day)} ${months[parseInt(m)]} ${y}`
  }

  // Group events by category
  const grouped = events.reduce<Record<string, EventDetail[]>>((acc, ev) => {
    if (!acc[ev.category]) acc[ev.category] = []
    acc[ev.category].push(ev)
    return acc
  }, {})

  return (
    <AnimatePresence>
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
        onClick={onClose}
      >
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 20 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 20 }}
          transition={{ duration: 0.2 }}
          className="w-full max-w-2xl max-h-[80vh] overflow-hidden rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-2xl"
          onClick={e => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between border-b border-[hsl(var(--border))] px-6 py-4">
            <div>
              <h2 className="text-lg font-semibold">События — {formatModalDate(date)}</h2>
              {summary && (
                <div className="flex gap-3 mt-1">
                  {EVENT_CATEGORIES_CONFIG.map(cat => {
                    const count = (summary as any)[cat.key]
                    if (!count) return null
                    return (
                      <span key={cat.key} className="text-xs font-medium" style={{ color: cat.color }}>
                        {cat.label}: {count}
                      </span>
                    )
                  })}
                </div>
              )}
            </div>
            <button
              onClick={onClose}
              className="rounded-lg p-2 hover:bg-[hsl(var(--muted))] transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Body */}
          <div className="overflow-y-auto max-h-[calc(80vh-80px)] px-6 py-4 space-y-6">
            {loading ? (
              <div className="space-y-3">
                {[1, 2, 3].map(i => (
                  <div key={i} className="h-16 rounded-lg bg-[hsl(var(--muted)/0.3)] animate-pulse" />
                ))}
              </div>
            ) : events.length === 0 ? (
              <p className="text-center text-[hsl(var(--muted-foreground))] py-8">Нет событий за этот день</p>
            ) : (
              Object.entries(grouped).map(([category, catEvents]) => {
                const CatIcon = CATEGORY_ICONS[category] || Zap
                const catColor = CATEGORY_COLORS[category] || '#888'
                const catLabel = CATEGORY_LABELS[category] || category
                return (
                  <div key={category}>
                    <div className="flex items-center gap-2 mb-3">
                      <CatIcon className="h-4 w-4" style={{ color: catColor }} />
                      <span className="text-sm font-semibold" style={{ color: catColor }}>
                        {catLabel}
                      </span>
                      <span className="text-xs text-[hsl(var(--muted-foreground))]">
                        ({catEvents.length})
                      </span>
                    </div>
                    <div className="space-y-2">
                      {catEvents.map(ev => (
                        <div
                          key={ev.id}
                          className="flex items-start gap-3 rounded-xl border border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.1)] p-3 hover:bg-[hsl(var(--muted)/0.2)] transition-colors"
                        >
                          {/* Product image */}
                          {ev.product?.image_url ? (
                            <img
                              src={ev.product.image_url}
                              alt=""
                              className="h-10 w-10 rounded-lg object-cover shrink-0"
                            />
                          ) : (
                            <div
                              className="h-10 w-10 rounded-lg shrink-0 flex items-center justify-center"
                              style={{ background: catColor + '20' }}
                            >
                              <CatIcon className="h-4 w-4" style={{ color: catColor }} />
                            </div>
                          )}
                          {/* Details */}
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2">
                              <span className="text-sm font-medium truncate">
                                {ev.label}
                              </span>
                              <span className="text-[11px] text-[hsl(var(--muted-foreground))] flex items-center gap-0.5">
                                <Clock className="h-3 w-3" />
                                {ev.time}
                              </span>
                            </div>
                            {ev.detail && (
                              <p className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5">
                                {ev.detail}
                              </p>
                            )}
                            {ev.product?.name && (
                              <p className="text-xs text-[hsl(var(--foreground)/0.7)] mt-0.5 truncate">
                                {ev.product.offer_id && <span className="text-[hsl(var(--muted-foreground))]">{ev.product.offer_id} · </span>}
                                {ev.product.name}
                              </p>
                            )}
                            {ev.campaign_title && (
                              <p className="text-[11px] text-[hsl(var(--muted-foreground))] mt-0.5">
                                📣 {ev.campaign_title}
                              </p>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )
              })
            )}
          </div>
        </motion.div>
      </motion.div>
    </AnimatePresence>
  )
}

/* ═══════════════════════════════════════════════════════════
   Campaigns Table
   ═══════════════════════════════════════════════════════════ */

function CampaignsTable({ campaigns }: { campaigns: CampaignRow[] }) {
  if (!campaigns.length) {
    return (
      <div className="flex h-32 items-center justify-center rounded-xl border border-dashed border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.15)]">
        <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">Нет данных о кампаниях</p>
      </div>
    )
  }

  return (
    <div className="overflow-x-auto -mx-5">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-[hsl(var(--border)/0.5)]">
            <th className="px-5 py-3 text-left text-[13px] font-medium text-[hsl(var(--muted-foreground))]">ID кампании</th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Расход</th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Показы</th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Клики</th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">CTR</th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">CPC</th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Заказы</th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Выручка</th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">ДРР</th>
          </tr>
        </thead>
        <tbody>
          {campaigns.map((c) => (
            <tr
              key={c.campaign_id}
              className="border-b border-[hsl(var(--border)/0.3)] transition-colors hover:bg-[hsl(var(--muted)/0.2)]"
            >
              <td className="px-5 py-3 font-medium">{c.campaign_id}</td>
              <td className="px-3 py-3 text-right">{formatMoney(c.spend)}</td>
              <td className="px-3 py-3 text-right">{formatNumber(c.views)}</td>
              <td className="px-3 py-3 text-right">{formatNumber(c.clicks)}</td>
              <td className="px-3 py-3 text-right">{c.ctr.toFixed(2)}%</td>
              <td className="px-3 py-3 text-right">{c.avg_cpc.toFixed(2)} ₽</td>
              <td className="px-3 py-3 text-right font-medium">{formatNumber(c.orders)}</td>
              <td className="px-3 py-3 text-right">{formatMoney(c.revenue)}</td>
              <td className="px-3 py-3 text-right">
                <span className={c.drr > 20 ? 'text-red-400 font-semibold' : c.drr > 10 ? 'text-yellow-400' : 'text-emerald-400'}>
                  {c.drr.toFixed(1)}%
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Top SKUs Table
   ═══════════════════════════════════════════════════════════ */

function TopSkusTable({ skus }: { skus: AdvertisingAnalyticsResponse['top_skus'] }) {
  const [hoverImg, setHoverImg] = useState<{ url: string; x: number; y: number } | null>(null)

  if (!skus.length) {
    return (
      <div className="flex h-32 items-center justify-center rounded-xl border border-dashed border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.15)]">
        <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">Нет данных по SKU</p>
      </div>
    )
  }

  return (
    <>
      <div className="overflow-x-auto -mx-5">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[hsl(var(--border)/0.5)]">
              <th className="px-5 py-3 text-left text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Товар</th>
              <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Расход</th>
              <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Заказы</th>
              <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Выручка</th>
              <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">ДРР</th>
            </tr>
          </thead>
          <tbody>
            {skus.map((s, i) => (
              <tr
                key={s.sku}
                className="border-b border-[hsl(var(--border)/0.3)] transition-colors hover:bg-[hsl(var(--muted)/0.2)]"
              >
                <td className="px-5 py-3">
                  <div className="flex items-center gap-3">
                    <span className="text-[13px] text-[hsl(var(--muted-foreground)/0.5)] w-5 text-center">{i + 1}</span>
                    {s.image_url ? (
                      <img
                        src={s.image_url}
                        alt={s.name}
                        className="h-12 w-10 rounded-lg object-cover shrink-0 cursor-pointer"
                        onMouseEnter={(e) => {
                          const rect = e.currentTarget.getBoundingClientRect()
                          setHoverImg({ url: s.image_url, x: rect.right + 8, y: rect.top })
                        }}
                        onMouseLeave={() => setHoverImg(null)}
                      />
                    ) : (
                      <div className="h-12 w-10 rounded-lg bg-[hsl(var(--muted)/0.4)] shrink-0" />
                    )}
                    <div className="min-w-0">
                      <p className="text-sm font-medium truncate max-w-[260px]">{s.name || s.offer_id}</p>
                      <p className="text-[13px] text-[hsl(var(--muted-foreground)/0.6)]">{s.offer_id}</p>
                    </div>
                  </div>
                </td>
                <td className="px-3 py-3 text-right">{formatMoney(s.spend)}</td>
                <td className="px-3 py-3 text-right font-medium">{formatNumber(s.orders)}</td>
                <td className="px-3 py-3 text-right">{formatMoney(s.revenue)}</td>
                <td className="px-3 py-3 text-right">
                  <span className={s.drr > 20 ? 'text-red-400 font-semibold' : s.drr > 10 ? 'text-yellow-400' : 'text-emerald-400'}>
                    {s.drr.toFixed(1)}%
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Fixed-position hover preview */}
      {hoverImg && (
        <div
          className="fixed z-[100] pointer-events-none animate-in fade-in-0 duration-150"
          style={{ left: hoverImg.x, top: hoverImg.y }}
        >
          <div className="rounded-xl shadow-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1.5">
            <img
              src={hoverImg.url}
              alt="Preview"
              className="h-52 w-40 rounded-lg object-cover"
            />
          </div>
        </div>
      )}
    </>
  )
}

/* ═══════════════════════════════════════════════════════════
   Loading Skeleton
   ═══════════════════════════════════════════════════════════ */

function AnalyticsSkeleton() {
  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div className="space-y-2">
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-4 w-72" />
        </div>
        <Skeleton className="h-9 w-52 rounded-lg" />
      </div>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {[...Array(9)].map((_, i) => (
          <Skeleton key={i} className="h-[110px] rounded-xl" />
        ))}
      </div>
      <Skeleton className="h-[430px] rounded-xl" />
      <Skeleton className="h-[300px] rounded-xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function AdvertisingAnalyticsPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const [period, setPeriod] = useState('today')
  const [data, setData] = useState<AdvertisingAnalyticsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchData = useCallback(async () => {
    if (!currentShop) return
    setLoading(true)
    setError(null)
    try {
      const result = await getAdvertisingAnalytics(currentShop.id, period)
      setData(result)
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || 'Ошибка загрузки данных'
      setError(msg)
    } finally {
      setLoading(false)
    }
  }, [currentShop, period])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  if (!currentShop) {
    return (
      <div className="flex h-64 items-center justify-center">
        <p className="text-[hsl(var(--muted-foreground))]">Выберите магазин</p>
      </div>
    )
  }

  if (loading) return <AnalyticsSkeleton />

  if (error) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Аналитика рекламы</h1>
        </div>
        <Card className="border-red-500/20 bg-red-500/5">
          <CardContent className="flex items-center gap-4 py-8">
            <XCircle className="h-8 w-8 text-red-400 shrink-0" />
            <div>
              <p className="font-medium text-red-400">Ошибка загрузки</p>
              <p className="text-sm text-[hsl(var(--muted-foreground))]">{error}</p>
            </div>
            <button
              onClick={fetchData}
              className="ml-auto flex items-center gap-2 rounded-lg bg-[hsl(var(--primary))] px-4 py-2 text-sm font-medium text-white hover:opacity-90 transition-opacity"
            >
              <RefreshCw className="h-4 w-4" />
              Повторить
            </button>
          </CardContent>
        </Card>
      </div>
    )
  }

  if (!data) return null

  const kpi = data.kpi

  return (
    <div className="space-y-8">
      {/* ── Header ── */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Аналитика рекламы</h1>
          <p className="text-[hsl(var(--muted-foreground))]">
            {data.marketplace === 'ozon' ? 'Ozon' : 'Wildberries'} · Рекламные кампании
          </p>
        </div>
        <div className="flex items-center gap-3">
          <PeriodSelector current={period} onChange={setPeriod} />
        </div>
      </div>

      {/* ── KPI Cards ── */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
        <KpiCard
          title="Расход"
          value={formatMoney(kpi.spend)}
          delta={kpi.spend_delta}
          invertDelta
          icon={DollarSign}
          accent="#f97316"
          delay={0}
        />
        <KpiCard
          title="Показы"
          value={formatNumber(kpi.views)}
          delta={kpi.views_delta}
          icon={Eye}
          accent="#3b82f6"
          delay={0.03}
        />
        <KpiCard
          title="Клики"
          value={formatNumber(kpi.clicks)}
          delta={kpi.clicks_delta}
          icon={MousePointerClick}
          accent="#06b6d4"
          delay={0.06}
        />
        <KpiCard
          title="CTR"
          value={`${kpi.ctr.toFixed(2)}%`}
          delta={kpi.ctr_delta}
          icon={Percent}
          accent="#facc15"
          delay={0.09}
          subtitle={`Δ ${kpi.ctr_delta >= 0 ? '+' : ''}${kpi.ctr_delta.toFixed(2)} п.п.`}
        />
        <KpiCard
          title="Корзины"
          value={formatNumber(kpi.cart)}
          delta={kpi.cart_delta}
          icon={ShoppingBag}
          accent="#8b5cf6"
          delay={0.12}
        />
        <KpiCard
          title="Конверсия в корзину"
          value={`${kpi.cart_rate.toFixed(1)}%`}
          delta={kpi.cart_rate_delta}
          icon={Percent}
          accent="#a78bfa"
          delay={0.14}
          subtitle={`Δ ${kpi.cart_rate_delta >= 0 ? '+' : ''}${kpi.cart_rate_delta.toFixed(1)} п.п.`}
        />
        <KpiCard
          title="Заказы"
          value={formatMoney(kpi.revenue)}
          delta={kpi.revenue_delta}
          icon={ShoppingCart}
          accent="#10b981"
          delay={0.15}
          subtitle={`${formatNumber(kpi.orders)} шт.`}
        />
        <KpiCard
          title="Конверсия в заказ"
          value={`${kpi.conversion_rate.toFixed(1)}%`}
          delta={kpi.conversion_rate_delta}
          icon={TrendingUp}
          accent="#22c55e"
          delay={0.18}
          subtitle={`Δ ${kpi.conversion_rate_delta >= 0 ? '+' : ''}${kpi.conversion_rate_delta.toFixed(1)} п.п.`}
        />
        <KpiCard
          title="Стоимость заказа"
          value={`${kpi.cpo.toFixed(0)} ₽`}
          delta={kpi.cpo_delta}
          invertDelta
          icon={Target}
          accent="#a855f7"
          delay={0.21}
          subtitle={`Δ ${kpi.cpo_delta >= 0 ? '+' : ''}${kpi.cpo_delta.toFixed(0)} ₽`}
        />
        <KpiCard
          title="ДРР рекламы"
          value={`${kpi.drr.toFixed(1)}%`}
          delta={kpi.drr_delta}
          invertDelta
          icon={Megaphone}
          accent="#ef4444"
          delay={0.24}
          subtitle={`Δ ${kpi.drr_delta >= 0 ? '+' : ''}${kpi.drr_delta.toFixed(1)} п.п.`}
        />
        <KpiCard
          title="Общий ДРР"
          value={`${kpi.total_drr.toFixed(1)}%`}
          delta={kpi.total_drr_delta}
          invertDelta
          icon={BarChart2}
          accent="#e11d48"
          delay={0.27}
          subtitle={`Δ ${kpi.total_drr_delta >= 0 ? '+' : ''}${kpi.total_drr_delta.toFixed(1)} п.п.`}
        />
        <KpiCard
          title="ROMI"
          value={`${kpi.romi.toFixed(0)}%`}
          delta={kpi.romi_delta}
          icon={TrendingUp}
          accent="#14b8a6"
          delay={0.3}
          subtitle={`Δ ${kpi.romi_delta >= 0 ? '+' : ''}${kpi.romi_delta.toFixed(1)} п.п.`}
        />
      </div>

      {/* ── Daily Chart ── */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, delay: 0.3 }}
      >
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-lg">Динамика рекламных показателей</CardTitle>
          </CardHeader>
          <CardContent>
            <AdsChart data={data.chart_daily} eventsByDay={data.events_by_day || {}} shopId={currentShop!.id} />
          </CardContent>
        </Card>
      </motion.div>

      {/* ── Campaigns Table ── */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, delay: 0.4 }}
      >
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-lg">Кампании за период</CardTitle>
          </CardHeader>
          <CardContent>
            <CampaignsTable campaigns={data.campaigns_table} />
          </CardContent>
        </Card>
      </motion.div>

      {/* ── Top SKUs ── */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, delay: 0.5 }}
      >
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-lg">Топ товаров по рекламному расходу</CardTitle>
          </CardHeader>
          <CardContent>
            <TopSkusTable skus={data.top_skus} />
          </CardContent>
        </Card>
      </motion.div>
    </div>
  )
}
