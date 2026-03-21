import { useState, useEffect, useCallback, Fragment, useRef } from 'react'
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
  ChevronDown,
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
  Search,
  CalendarDays,
  Check,
} from 'lucide-react'
import { DayPicker, type DateRange } from 'react-day-picker'
import { ru } from 'date-fns/locale/ru'
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
import { CampaignDetailModal } from '@/components/CampaignDetailModal'
import { CampaignInsights } from '@/components/CampaignInsights'
import { useAppStore } from '@/stores/appStore'
import {
  getAdvertisingAnalytics,
  getEventsDetail,
  type AdvertisingAnalyticsResponse,
  type AdvertisingDailyPoint,
  type CampaignRow,
  type CampaignSkuItem,
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
  customRange,
  onCustomRange,
}: {
  current: string
  onChange: (period: string) => void
  customRange: DateRange | null
  onCustomRange: (range: DateRange | null) => void
}) {
  const [calOpen, setCalOpen] = useState(false)
  const [draft, setDraft] = useState<DateRange | undefined>(undefined)
  const popRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!calOpen) return
    const h = (e: MouseEvent) => {
      if (popRef.current && !popRef.current.contains(e.target as Node)) {
        setCalOpen(false)
        setDraft(undefined)
      }
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [calOpen])

  const fmtDate = (d: Date) => d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })
  const isSameDay = (a: Date, b: Date) => a.toDateString() === b.toDateString()

  const applyCustom = () => {
    if (!draft?.from) return
    onCustomRange({ from: draft.from, to: draft.to ?? draft.from })
    setCalOpen(false)
    setDraft(undefined)
  }

  const customLabel = customRange?.from
    ? customRange.to && !isSameDay(customRange.from, customRange.to)
      ? `${fmtDate(customRange.from)} — ${fmtDate(customRange.to)}`
      : fmtDate(customRange.from)
    : null

  const isCustom = current === 'custom'

  const hint = !draft?.from
    ? 'Выберите начальную дату'
    : !draft.to || isSameDay(draft.from, draft.to)
      ? `${fmtDate(draft.from)} — один день`
      : `${fmtDate(draft.from)} — ${fmtDate(draft.to)}`

  return (
    <div className="relative" ref={popRef}>
      <div className="inline-flex rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1 gap-0.5">
        {PERIODS.map((p) => (
          <button
            key={p.key}
            onClick={() => { onChange(p.key); onCustomRange(null); setCalOpen(false) }}
            className={`rounded-lg px-4 py-2 text-sm font-medium transition-all duration-200 ${
              current === p.key && !isCustom
                ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
                : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-white/5'
            }`}
          >
            {p.label}
          </button>
        ))}

        <div className="h-5 w-px bg-[hsl(var(--border))] mx-0.5 self-center" />

        <button
          onClick={() => { setDraft(customRange ?? undefined); setCalOpen(true) }}
          className={`inline-flex items-center gap-1.5 rounded-lg px-3 py-2 text-sm font-medium transition-all duration-200 ${
            isCustom
              ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
              : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-white/5'
          } ${calOpen && !isCustom ? 'bg-white/5 text-[hsl(var(--foreground))]' : ''}`}
        >
          <CalendarDays className="h-3.5 w-3.5 shrink-0" />
          <span>{customLabel ?? 'Даты'}</span>
        </button>

        {isCustom && (
          <button
            onClick={() => { onCustomRange(null); onChange('today') }}
            className="rounded-lg p-1.5 text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-white/5 transition-colors"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        )}
      </div>

      {calOpen && (
        <div className="absolute right-0 top-full z-50 mt-2 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-2xl p-4"
             style={{ animation: 'dpPop 160ms ease-out', minWidth: 580 }}>
          <style>{`
            .rdp-root { --rdp-accent-color: hsl(var(--primary)); font-family: inherit; }
            .rdp-months { display: flex; flex-direction: row; flex-wrap: nowrap; gap: 24px; }
            .rdp-month_caption { display: flex; align-items: center; justify-content: center; padding-bottom: 10px; }
            .rdp-caption_label { font-size: 13px; font-weight: 600; color: hsl(var(--foreground)); text-transform: capitalize; }
            .rdp-nav { display: flex; align-items: center; gap: 4px; }
            .rdp-button_previous, .rdp-button_next {
              width: 28px; height: 28px; border-radius: 8px; display: flex; align-items: center; justify-content: center;
              color: hsl(var(--muted-foreground)); background: transparent; border: none; cursor: pointer;
              transition: background 150ms, color 150ms;
            }
            .rdp-button_previous:hover, .rdp-button_next:hover { background: rgba(255,255,255,0.08); color: hsl(var(--foreground)); }
            .rdp-weekdays { display: flex; }
            .rdp-weekday { width: 36px; text-align: center; font-size: 11px; font-weight: 500; color: hsl(var(--muted-foreground) / 0.45); padding-bottom: 4px; }
            .rdp-week { display: flex; margin-top: 2px; }
            .rdp-day { position: relative; padding: 0; }
            .rdp-day_button {
              width: 36px; height: 36px; border-radius: 8px; border: none; background: transparent; cursor: pointer;
              font-size: 13px; font-weight: 500; color: hsl(var(--foreground) / 0.85);
              transition: background 120ms, color 120ms;
              display: flex; align-items: center; justify-content: center;
            }
            .rdp-day_button:hover { background: rgba(255,255,255,0.08); }
            .rdp-selected .rdp-day_button { background: hsl(var(--primary)) !important; color: white !important; }
            .rdp-range_start .rdp-day_button { border-radius: 8px 0 0 8px; }
            .rdp-range_end .rdp-day_button { border-radius: 0 8px 8px 0; }
            .rdp-range_middle .rdp-day_button { background: hsl(var(--primary) / 0.15) !important; color: hsl(var(--primary)) !important; border-radius: 0; }
            .rdp-range_start.rdp-range_end .rdp-day_button { border-radius: 8px !important; }
            .rdp-today .rdp-day_button { font-weight: 700; text-decoration: underline; text-underline-offset: 2px; text-decoration-style: dotted; }
            .rdp-outside { opacity: 0; pointer-events: none; }
            .rdp-disabled .rdp-day_button { opacity: 0.2; cursor: not-allowed; }
            @keyframes dpPop { from { opacity:0; transform:translateY(-4px) scale(.98) } to { opacity:1; transform:none } }
          `}</style>

          <DayPicker
            mode="range"
            selected={draft}
            onSelect={setDraft}
            locale={ru}
            numberOfMonths={2}
            showOutsideDays={false}
            disabled={{ after: new Date() }}
            defaultMonth={
              draft?.from
                ? new Date(draft.from.getFullYear(), draft.from.getMonth() - 1)
                : new Date(new Date().getFullYear(), new Date().getMonth() - 1)
            }
          />

          <div className="mt-3 pt-3 border-t border-[hsl(var(--border)/0.4)] flex items-center justify-between gap-3">
            <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.55)]">{hint}</span>
            <div className="flex items-center gap-2">
              <button
                onClick={() => { setCalOpen(false); setDraft(undefined) }}
                className="rounded-lg border border-[hsl(var(--border))] px-3 py-1.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
              >
                Отмена
              </button>
              <button
                onClick={applyCustom}
                disabled={!draft?.from}
                className={`inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-[12px] font-semibold transition-all ${
                  draft?.from
                    ? 'bg-[hsl(var(--primary))] text-white hover:opacity-90'
                    : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground)/0.35)] cursor-not-allowed'
                }`}
              >
                <Check className="h-3.5 w-3.5" />
                Применить
              </button>
            </div>
          </div>
        </div>
      )}
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
  { key: 'orders', label: 'Заказы', color: '#10b981', yAxis: 'right' },
  { key: 'revenue', label: 'Выручка', color: '#22c55e', yAxis: 'left' },
  { key: 'ctr', label: 'CTR %', color: '#facc15', yAxis: 'percent' },
  { key: 'drr', label: 'ДРР %', color: '#ef4444', yAxis: 'percent' },
  { key: 'total_drr', label: 'Общий ДРР %', color: '#f43f5e', yAxis: 'percent' },
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
  total_drr: 'Общий ДРР',
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
      <ResponsiveContainer width="100%" height={400}>
        <ComposedChart
          data={data}
          margin={{ top: 5, right: 10, left: 0, bottom: 40 }}
          onClick={(state: any) => {
            if (state?.activeLabel && showEvents && eventsByDay[state.activeLabel]?.total > 0) {
              setSelectedEventDate(state.activeLabel)
            }
          }}
          style={{ cursor: showEvents && visibleEventDates.length > 0 ? 'pointer' : undefined }}
        >
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
            interval={data.length <= 15 ? 0 : Math.floor(data.length / 12)}
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
                : (name === 'drr' || name === 'ctr' || name === 'total_drr') ? `${value.toFixed(1)}%`
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
                if (parts.length > 0) return `${base}\n⚡ ${parts.join(' · ')}\n→ Кликните для деталей`
              }
              return base
            }}
          />
          <Legend
            verticalAlign="top"
            height={50}
            formatter={(value: string) => ADS_METRIC_LABELS[value] || value}
            wrapperStyle={{ fontSize: '13px', color: 'hsl(var(--muted-foreground))', paddingTop: '4px', paddingBottom: '20px' }}
          />

          {/* Event ReferenceLine markers — visual only, click handled at chart level */}
          {visibleEventDates.map(dateStr => {
            const ev = eventsByDay[dateStr]
            if (!ev) return null
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
                strokeOpacity={0.5}
                yAxisId={hasLeftAxis ? 'left' : hasRightAxis ? 'right' : 'percent'}
                label={{
                  value: `⚡${ev.total}`,
                  position: 'top',
                  fill: color,
                  fontSize: 10,
                  fontWeight: 700,
                  offset: 4,
                }}
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
              yAxisId="right"
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

          {activeMetrics.has('total_drr') && (
            <Line
              yAxisId="percent"
              type="monotone"
              dataKey="total_drr"
              stroke="#f43f5e"
              strokeWidth={2}
              strokeDasharray="3 3"
              dot={{ r: 2, fill: '#f43f5e' }}
              activeDot={{ r: 4, fill: '#f43f5e' }}
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

function CampaignsTable({ 
  campaigns,
  marketplace,
  dateFrom,
  dateTo
}: { 
  campaigns: CampaignRow[]
  marketplace: string
  dateFrom: string
  dateTo: string 
}) {
  const [sortKey, setSortKey] = useState<string>('spend')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set())
  const [modalState, setModalState] = useState<{isOpen: boolean, campaignId: number, title: string, items: CampaignSkuItem[], sku?: number} | null>(null)
  const [searchQuery, setSearchQuery] = useState('')

  if (!campaigns.length) {
    return (
      <div className="flex h-32 items-center justify-center rounded-xl border border-dashed border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.15)]">
        <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">Нет данных о кампаниях</p>
      </div>
    )
  }

  // Universal search filter
  const filtered = searchQuery.trim()
    ? campaigns.filter(c => {
        const q = searchQuery.trim().toLowerCase()
        // Search in campaign title & id
        if (c.title?.toLowerCase().includes(q)) return true
        if (String(c.campaign_id).includes(q)) return true
        // Search in items: sku, product_id, offer_id, name
        if (c.items?.some(item =>
          String(item.sku).includes(q) ||
          String(item.product_id).includes(q) ||
          item.offer_id?.toLowerCase().includes(q) ||
          item.name?.toLowerCase().includes(q)
        )) return true
        return false
      })
    : campaigns

  const sorted = [...filtered].sort((a, b) => {
    const va = (a as any)[sortKey] ?? 0
    const vb = (b as any)[sortKey] ?? 0
    if (typeof va === 'number' && typeof vb === 'number') return sortDir === 'asc' ? va - vb : vb - va
    return sortDir === 'asc' ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va))
  })

  const handleSort = (key: string) => {
    if (sortKey === key) setSortDir(prev => (prev === 'asc' ? 'desc' : 'asc'))
    else { setSortKey(key); setSortDir('desc') }
  }

  const toggleExpand = (cid: number) => {
    setExpandedRows(prev => {
      const next = new Set(prev)
      next.has(cid) ? next.delete(cid) : next.add(cid)
      return next
    })
  }

  const thCls = "px-3 py-3 text-[14px] font-medium text-[hsl(var(--muted-foreground))] cursor-pointer select-none hover:text-[hsl(var(--foreground))] transition-colors whitespace-nowrap text-right"
  const tdCls = "px-3 py-3 text-right text-[14px] whitespace-nowrap"

  const SortIcon = ({ k }: { k: string }) => sortKey === k ? <span className="ml-0.5 text-[10px]">{sortDir === 'desc' ? '▼' : '▲'}</span> : null

  const drrColor = (d: number) => d > 30 ? 'text-red-500 font-semibold' : d > 15 ? 'text-red-400' : d > 8 ? 'text-yellow-400' : 'text-emerald-400'

  const OZON_STATUS_MAP: Record<string, { label: string; cls: string }> = {
    CAMPAIGN_STATE_RUNNING: { label: 'Активна', cls: 'bg-emerald-500/20 text-emerald-400' },
    CAMPAIGN_STATE_PLANNED: { label: 'Запланирована', cls: 'bg-blue-500/20 text-blue-400' },
    CAMPAIGN_STATE_STOPPED: { label: 'Остановлена', cls: 'bg-yellow-500/20 text-yellow-400' },
    CAMPAIGN_STATE_INACTIVE: { label: 'Неактивна', cls: 'bg-gray-500/20 text-gray-400' },
    CAMPAIGN_STATE_ARCHIVED: { label: 'В архиве', cls: 'bg-gray-500/20 text-gray-400' },
    CAMPAIGN_STATE_MODERATION: { label: 'Модерация', cls: 'bg-orange-500/20 text-orange-400' },
    CAMPAIGN_STATE_NOT_MODERATED: { label: 'Не прошла', cls: 'bg-red-500/20 text-red-400' },
  }

  const CAMPAIGN_TYPE_MAP: Record<string, string> = {
    SEARCH_PROMO: 'Поиск',
    BANNER: 'Баннер',
    BRAND_SHELF: 'Полка бренда',
    ACTION: 'Акция',
    VIDEO: 'Видео',
    SKU: 'Товар в поиске',
  }

  const HaloBar = ({ direct, model, pct, isMoney }: { direct: number; model: number; pct: number; isMoney?: boolean }) => {
    const fmt = isMoney ? formatMoney : formatNumber
    return (
    <div className="flex flex-col items-end gap-0.5">
      <span className="font-semibold text-[13px]">{fmt(direct + model)}</span>
      {model > 0 && (
        <>
          <div className="flex items-center gap-1">
            <span className="text-[12px] text-[hsl(var(--foreground)/0.8)]">{fmt(direct)} прям.</span>
            <span className="text-[12px] text-teal-400 font-medium">+ {fmt(model)} halo</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-14 h-2 rounded-full bg-[hsl(var(--muted)/0.3)] overflow-hidden">
              <div className="h-full rounded-full bg-gradient-to-r from-emerald-400 to-teal-400" style={{ width: `${Math.min(pct, 100)}%` }} />
            </div>
            <span className="text-[11px] text-teal-400 font-medium">{pct}%</span>
          </div>
        </>
      )}
    </div>
    )
  }

  return (
    <div className="overflow-hidden">
      {/* Search input */}
      <div className="px-4 pt-1 pb-3">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[hsl(var(--muted-foreground)/0.5)]" />
          <input
            type="text"
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            placeholder="Поиск по названию, ID, артикулу, SKU или товару..."
            className="w-full pl-9 pr-8 py-2.5 text-[14px] rounded-lg border border-[hsl(var(--border)/0.4)] bg-[hsl(var(--muted)/0.1)] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.4)] focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary)/0.3)] focus:border-[hsl(var(--primary)/0.3)] transition-all"
          />
          {searchQuery && (
            <button
              onClick={() => setSearchQuery('')}
              className="absolute right-2 top-1/2 -translate-y-1/2 p-0.5 rounded hover:bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground)/0.5)]"
            >
              <X className="w-4 h-4" />
            </button>
          )}
        </div>
        {searchQuery.trim() && (
          <div className="text-[12px] text-[hsl(var(--muted-foreground)/0.6)] mt-1 ml-1">
            Найдено: {filtered.length} из {campaigns.length} кампаний
          </div>
        )}
      </div>
      <div className="overflow-x-auto overflow-y-auto max-h-[calc(100vh-300px)]">
        <table className="w-full min-w-[1200px]" style={{ borderCollapse: 'collapse' }}>
          <thead className="sticky top-0 z-30" style={{ boxShadow: '0 1px 0 hsl(var(--border))' }}>
            <tr className="bg-[hsl(var(--card))]">
              <th className="sticky left-0 z-40 w-[300px] bg-[hsl(var(--card))] pl-4 pr-2 py-3 text-left text-[14px] font-medium text-[hsl(var(--muted-foreground))] cursor-pointer select-none hover:text-[hsl(var(--foreground))] transition-colors" onClick={() => handleSort('campaign_id')}>
                Кампания<SortIcon k="campaign_id" />
              </th>
              <th className={thCls} onClick={() => handleSort('spend')}>Расход<SortIcon k="spend" /></th>
              <th className={thCls} onClick={() => handleSort('views')}>Показы<SortIcon k="views" /></th>
              <th className={thCls} onClick={() => handleSort('clicks')}>Клики<SortIcon k="clicks" /></th>
              <th className={thCls} onClick={() => handleSort('avg_cpc')}>CPC<SortIcon k="avg_cpc" /></th>
              <th className={thCls} onClick={() => handleSort('ctr')}>CTR<SortIcon k="ctr" /></th>
              <th className={thCls} onClick={() => handleSort('cart')}>Корз.<SortIcon k="cart" /></th>
              <th className={thCls} onClick={() => handleSort('cart_conv')}>CR корз.<SortIcon k="cart_conv" /></th>
              <th className={thCls} onClick={() => handleSort('orders')}>Заказы<SortIcon k="orders" /></th>
              <th className={thCls} onClick={() => handleSort('order_conv')}>CR заказ<SortIcon k="order_conv" /></th>
              <th className={thCls} onClick={() => handleSort('revenue')}>Выручка<SortIcon k="revenue" /></th>
              <th className={thCls}>CPO</th>
              <th className={thCls} onClick={() => handleSort('drr')}>ДРР<SortIcon k="drr" /></th>
            </tr>
          </thead>
        <tbody>
          {sorted.map((c) => {
            const isExpanded = expandedRows.has(c.campaign_id)
            return (
              <Fragment key={c.campaign_id}>
                {/* Campaign row */}
                <tr
                  className={`border-b border-[hsl(var(--border)/0.3)] transition-colors hover:bg-[hsl(var(--muted)/0.15)] cursor-pointer ${isExpanded ? 'bg-[hsl(var(--muted)/0.08)]' : ''}`}
                  onClick={() => c.items.length > 0 && toggleExpand(c.campaign_id)}
                >
                    <td className="sticky left-0 z-20 w-[300px] bg-[hsl(var(--card))] pl-4 pr-2 py-3">
                    <div className="flex items-center gap-2">
                      {c.items.length > 0 && (
                        <ChevronDown className={`h-4 w-4 shrink-0 text-[hsl(var(--muted-foreground)/0.5)] transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                      )}
                      <div className="flex flex-col min-w-0 gap-1 flex-1">
                        {c.title ? (
                          <span className="font-semibold text-[14px] leading-snug line-clamp-2" title={c.title}>{c.title}</span>
                        ) : (
                          <span className="font-semibold text-[14px]">{c.campaign_id}</span>
                        )}
                        {c.campaign_type && (
                          <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.8)] leading-tight mb-0.5">
                            {CAMPAIGN_TYPE_MAP[c.campaign_type] || c.campaign_type}
                          </span>
                        )}
                        <div className="flex items-center gap-1.5 flex-wrap">
                          <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.6)]">
                            ID: {c.campaign_id} · {c.sku_count} арт.
                          </span>
                          {c.status && OZON_STATUS_MAP[c.status] && (
                            <span className={`text-[12px] px-2 py-0.5 rounded-full font-semibold ${OZON_STATUS_MAP[c.status].cls}`}>
                              {OZON_STATUS_MAP[c.status].label}
                            </span>
                          )}
                        </div>
                      </div>
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          setModalState({ isOpen: true, campaignId: c.campaign_id, title: c.title || `Campaign #${c.campaign_id}`, items: c.items || [] })
                        }}
                        className="shrink-0 ml-auto w-8 h-8 flex items-center justify-center rounded-lg bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))] hover:bg-[hsl(var(--primary)/0.25)] transition-colors"
                        title="Статистика кампании"
                      >
                        <BarChart2 className="w-5 h-5" />
                      </button>
                    </div>
                  </td>
                  <td className={tdCls}>{formatMoney(c.spend)}</td>
                  <td className={tdCls}>{formatNumber(c.views)}</td>
                  <td className={tdCls}>{formatNumber(c.clicks)}</td>
                  <td className={tdCls}>{Math.round(c.avg_cpc)} ₽</td>
                  <td className={tdCls}>{c.ctr.toFixed(2)}%</td>
                  <td className={tdCls}>{formatNumber(c.cart)}</td>
                  <td className={tdCls}>{c.cart_conv > 0 ? `${c.cart_conv}%` : '—'}</td>
                  <td className={tdCls}>
                    <HaloBar direct={c.direct_orders} model={c.model_orders} pct={c.halo_pct} />
                  </td>
                  <td className={tdCls}>{c.order_conv > 0 ? `${c.order_conv}%` : '—'}</td>
                  <td className={tdCls}>
                    <HaloBar direct={c.direct_revenue} model={c.model_revenue} pct={c.halo_pct} isMoney />
                  </td>
                  <td className={tdCls}>
                    {(c.direct_orders + c.model_orders) > 0 ? formatMoney(Math.round(c.spend / (c.direct_orders + c.model_orders))) : '—'}
                  </td>
                  <td className={tdCls}>
                    <span className={drrColor(c.drr)}>{c.drr.toFixed(1)}%</span>
                  </td>
                </tr>

                {/* Per-SKU rows */}
                {isExpanded && c.items.map((s) => (
                  <tr
                    key={`${c.campaign_id}-${s.sku}`}
                    className="border-b border-[hsl(var(--border)/0.15)] bg-[hsl(var(--muted)/0.06)]"
                  >
                    <td className="sticky left-0 z-20 w-[300px] bg-[hsl(var(--muted)/0.06)] pl-4 pr-2 py-3">
                      <div className="flex flex-col pl-6 min-w-0 gap-1.5">
                        <span className="text-[13px] font-medium leading-snug line-clamp-2" title={s.name || `SKU ${s.sku}`}>
                          {s.name || `SKU ${s.sku}`}
                        </span>
                        <div className="flex flex-col gap-0.5 mt-0.5">
                          {s.offer_id && <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.6)]">Арт: <span className="text-[hsl(var(--foreground)/0.8)] font-medium">{s.offer_id}</span></span>}
                          <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.6)]">ID: <span className="text-[hsl(var(--foreground)/0.8)] font-medium">{s.product_id}</span></span>
                          {s.bid > 0 && (
                            <span className="text-[13px] font-bold text-teal-600 dark:text-teal-400 mt-1">
                              Текущая ставка: {s.bid} ₽
                            </span>
                          )}
                        </div>
                      </div>
                    </td>
                    <td className={`${tdCls} text-[12px]`}>{formatMoney(s.spend)}</td>
                    <td className={`${tdCls} text-[12px]`}>{formatNumber(s.views)}</td>
                    <td className={`${tdCls} text-[12px]`}>{formatNumber(s.clicks)}</td>
                    <td className={`${tdCls} text-[12px]`}>{Math.round(s.avg_cpc)} ₽</td>
                    <td className={`${tdCls} text-[12px]`}>{s.ctr.toFixed(2)}%</td>
                    <td className={`${tdCls} text-[12px]`}>{formatNumber(s.cart)}</td>
                    <td className={`${tdCls} text-[12px]`}>{s.cart_conv > 0 ? `${s.cart_conv}%` : '—'}</td>
                    <td className={`${tdCls} text-[12px]`}>
                      <HaloBar direct={s.direct_orders} model={s.model_orders} pct={s.halo_pct} />
                    </td>
                    <td className={`${tdCls} text-[12px]`}>{s.order_conv > 0 ? `${s.order_conv}%` : '—'}</td>
                    <td className={`${tdCls} text-[12px]`}>
                      <HaloBar direct={s.direct_revenue} model={s.model_revenue} pct={s.halo_pct} isMoney />
                    </td>
                    <td className={`${tdCls} text-[12px]`}>
                      {(s.direct_orders + s.model_orders) > 0 ? formatMoney(Math.round(s.spend / (s.direct_orders + s.model_orders))) : '—'}
                    </td>
                    <td className={`${tdCls} text-[12px]`}>
                      <div className="flex flex-col items-end">
                        <span className={drrColor(s.drr)}>{s.drr.toFixed(1)}%</span>
                        {s.total_drr > 0 && s.total_drr !== s.drr && (
                          <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)]" title="Общий ДРР">
                            общ: {s.total_drr.toFixed(1)}%
                          </span>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </Fragment>
            )
          })}
        </tbody>
        </table>
      </div>

      {modalState?.isOpen && (
        <CampaignDetailModal
          isOpen={modalState.isOpen}
          onClose={() => setModalState(null)}
          marketplace={marketplace}
          campaignId={modalState.campaignId}
          campaignTitle={modalState.title}
          startDate={dateFrom}
          endDate={dateTo}
          items={modalState.items}
          sku={modalState.sku}
        />
      )}
    </div>
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
  const [customRange, setCustomRange] = useState<DateRange | null>(null)
  const [data, setData] = useState<AdvertisingAnalyticsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchData = useCallback(async () => {
    if (!currentShop) return
    setLoading(true)
    setError(null)
    try {
      let dateFrom: string | undefined
      let dateTo: string | undefined
      if (customRange?.from) {
        const toDate = customRange.to ?? customRange.from
        dateFrom = customRange.from.toISOString().slice(0, 10)
        dateTo = toDate.toISOString().slice(0, 10)
      }
      const result = await getAdvertisingAnalytics(currentShop.id, customRange ? 'custom' : period, dateFrom, dateTo)
      setData(result)
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || 'Ошибка загрузки данных'
      setError(msg)
    } finally {
      setLoading(false)
    }
  }, [currentShop, period, customRange])

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
          <PeriodSelector current={customRange ? 'custom' : period} onChange={setPeriod} customRange={customRange} onCustomRange={setCustomRange} />
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

      {/* ── Campaign Insights ── */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, delay: 0.35 }}
      >
        <Card>
          <CardContent className="pt-5">
            <CampaignInsights
              shopId={currentShop!.id}
            />
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
            <div className="flex items-baseline gap-3">
              <CardTitle className="text-lg">Кампании за период</CardTitle>
              <span className="text-[15px] font-semibold text-[hsl(var(--foreground)/0.8)]">
                {new Date(data.date_from).toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' })}
                {' — '}
                {new Date(data.date_to).toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' })}
              </span>
            </div>
          </CardHeader>
          <CardContent>
            <CampaignsTable 
              campaigns={data.campaigns_table} 
              marketplace={currentShop!.marketplace}
              dateFrom={data.date_from}
              dateTo={data.date_to}
            />
          </CardContent>
        </Card>
      </motion.div>


    </div>
  )
}
