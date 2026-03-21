import { useEffect, useState, useCallback } from 'react'
import {
  X, Loader2, BarChart3, Calendar, Package, Search, Flame, ChevronDown,
  TrendingUp, TrendingDown, Activity, DollarSign, Palette,
  Image, Plus, Minus, AlertTriangle, Rocket, ArrowRight, ArrowUp, ArrowDown,
  type LucideIcon,
} from 'lucide-react'
import { format, parseISO, subDays } from 'date-fns'
import { ru } from 'date-fns/locale'
import {
  ComposedChart,
  Area,
  Line,
  Bar,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip as RTooltip,
  XAxis,
  YAxis,
  Legend,
  ReferenceLine,
} from 'recharts'

import { formatNumber } from '@/lib/utils'
import {
  getCampaignStats,
  getCampaignEvents,
  getCampaignPhrases,
  getCampaignHeatmap,
  getCampaignPurchases,
  getCampaignKpi,
  type CampaignStatsRow,
  type CampaignEventRow,
  type CampaignPhraseRow,
  type CampaignHeatmapRow,
  type CampaignPurchaseRow,
  type CampaignKpiResponse,
} from '@/api/campaignDetails'
import type { CampaignSkuItem } from '@/api/advertising'

/* ═══════════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════════ */

function formatMoney(value: number): string {
  return new Intl.NumberFormat('ru-RU', {
    style: 'currency', currency: 'RUB',
    minimumFractionDigits: 0, maximumFractionDigits: 0,
  }).format(value)
}

const MONTHS_SHORT = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
function formatChartDate(dateStr: string): string {
  const parts = dateStr.split('-')
  if (parts.length >= 3) {
    const day = parseInt(parts[2], 10)
    const month = parseInt(parts[1], 10) - 1
    return `${day} ${MONTHS_SHORT[month] || parts[1]}`
  }
  return dateStr.slice(5)
}

/* ── Event labels & styles (from EventsPage) ────────────────── */
const EVENT_LABELS: Record<string, string> = {
  OZON_BID_CHANGE: 'Ставка изменена',
  OZON_STATUS_CHANGE: 'Статус изменён',
  OZON_BUDGET_CHANGE: 'Бюджет изменён',
  OZON_ITEM_ADD: 'Товар добавлен',
  OZON_ITEM_REMOVE: 'Товар удалён',
  OZON_CAMPAIGN_CREATED: 'Кампания создана',
  OZON_SEO_CHANGE: 'SEO изменено',
  OZON_PHOTO_CHANGE: 'Фото изменено',
  OZON_CONTENT_CHANGE: 'Контент изменён',
  OZON_PRICE_CHANGE: 'Цена изменена',
  OZON_STOCK_OUT: 'Нет на складе',
  OZON_STOCK_REPLENISH: 'Товар на складе',
  BID_CHANGE: 'Ставка изменена',
  STATUS_CHANGE: 'Статус изменён',
  ITEM_ADD: 'Товар добавлен',
  ITEM_REMOVE: 'Товар удалён',
  ITEM_INACTIVE: 'Товар неактивен',
  CAMPAIGN_CREATED: 'Кампания создана',
  CONTENT_CHANGE: 'Контент изменён',
  CONTENT_TITLE_CHANGED: 'Название изменено',
  CONTENT_DESC_CHANGED: 'Описание изменено',
  CONTENT_MAIN_PHOTO_CHANGED: 'Фото изменено',
  CONTENT_PHOTO_ADDED: 'Фото добавлено',
  CONTENT_PHOTO_REMOVED: 'Фото удалено',
  CONTENT_PHOTO_ORDER_CHANGED: 'Порядок фото',
  PRICE_CHANGE: 'Цена изменена',
  STOCK_OUT: 'Нет на складе',
  STOCK_REPLENISH: 'Товар на складе',
  STOCK_OUT_FBO_TOTAL: 'Полный стокаут FBO',
  STOCK_OUT_FBS_TOTAL: 'Полный стокаут FBS',
}

const EVENT_STYLE: Record<string, { icon: LucideIcon; color: string; bg: string }> = {
  OZON_BID_CHANGE: { icon: TrendingUp, color: '#3b82f6', bg: '#3b82f620' },
  OZON_STATUS_CHANGE: { icon: Activity, color: '#6366f1', bg: '#6366f120' },
  OZON_BUDGET_CHANGE: { icon: DollarSign, color: '#8b5cf6', bg: '#8b5cf620' },
  OZON_ITEM_ADD: { icon: Plus, color: '#10b981', bg: '#10b98120' },
  OZON_ITEM_REMOVE: { icon: Minus, color: '#ef4444', bg: '#ef444420' },
  OZON_CAMPAIGN_CREATED: { icon: Rocket, color: '#22c55e', bg: '#22c55e25' },
  OZON_SEO_CHANGE: { icon: Palette, color: '#14b8a6', bg: '#14b8a620' },
  OZON_PHOTO_CHANGE: { icon: Image, color: '#f97316', bg: '#f9731620' },
  OZON_CONTENT_CHANGE: { icon: Palette, color: '#14b8a6', bg: '#14b8a620' },
  OZON_PRICE_CHANGE: { icon: DollarSign, color: '#f59e0b', bg: '#f59e0b20' },
  OZON_STOCK_OUT: { icon: TrendingDown, color: '#ef4444', bg: '#ef444420' },
  OZON_STOCK_REPLENISH: { icon: TrendingUp, color: '#10b981', bg: '#10b98120' },
  BID_CHANGE: { icon: TrendingUp, color: '#3b82f6', bg: '#3b82f620' },
  STATUS_CHANGE: { icon: Activity, color: '#6366f1', bg: '#6366f120' },
  ITEM_ADD: { icon: Plus, color: '#10b981', bg: '#10b98120' },
  ITEM_REMOVE: { icon: Minus, color: '#ef4444', bg: '#ef444420' },
  ITEM_INACTIVE: { icon: Minus, color: '#f59e0b', bg: '#f59e0b20' },
  CAMPAIGN_CREATED: { icon: Rocket, color: '#22c55e', bg: '#22c55e25' },
  CONTENT_CHANGE: { icon: Palette, color: '#14b8a6', bg: '#14b8a620' },
  PRICE_CHANGE: { icon: DollarSign, color: '#f59e0b', bg: '#f59e0b20' },
  STOCK_OUT: { icon: TrendingDown, color: '#ef4444', bg: '#ef444420' },
  STOCK_REPLENISH: { icon: TrendingUp, color: '#10b981', bg: '#10b98120' },
  STOCK_OUT_FBO_TOTAL: { icon: AlertTriangle, color: '#dc2626', bg: '#dc262640' },
  STOCK_OUT_FBS_TOTAL: { icon: AlertTriangle, color: '#dc2626', bg: '#dc262640' },
}

const DEFAULT_EV_STYLE = { icon: Activity, color: '#a78bfa', bg: '#a78bfa20' }

const NUMERIC_EVENTS = new Set([
  'OZON_BID_CHANGE', 'BID_CHANGE', 'OZON_BUDGET_CHANGE',
  'PRICE_CHANGE', 'OZON_PRICE_CHANGE',
])

function parseNum(val: string | null | undefined): number | null {
  if (!val) return null
  const n = parseFloat(val.replace(/[^\d.,\-]/g, '').replace(',', '.'))
  return isFinite(n) ? n : null
}
function fmtNum(n: number, suffix = ''): string {
  return (Math.abs(n) >= 1000 ? n.toLocaleString('ru-RU', { maximumFractionDigits: 2 }) : n.toFixed(2).replace(/\.?0+$/, '')) + suffix
}

/* ── Chart metric config ────────────────────────────────── */
const CHART_METRICS = [
  { key: 'spend', label: 'Расход', color: '#ef4444', type: 'area' },
  { key: 'revenue', label: 'Выручка рекл.', color: '#10b981', type: 'area' },
  { key: 'product_revenue', label: 'Выручка общая', color: '#06b6d4', type: 'area' },
  { key: 'orders', label: 'Заказы', color: '#8b5cf6', type: 'bar' },
  { key: 'cart', label: 'Корзины', color: '#f97316', type: 'bar' },
  { key: 'clicks', label: 'Клики', color: '#3b82f6', type: 'line' },
  { key: 'views', label: 'Показы', color: '#f59e0b', type: 'line' },
  { key: 'ctr', label: 'CTR %', color: '#14b8a6', type: 'line' },
  { key: 'drr', label: 'ДРР %', color: '#ec4899', type: 'line' },
] as const

/* ── Period options ──────────────────────────────────────── */
const PERIOD_OPTIONS = [
  { value: '7d', label: '7 дней', days: 7 },
  { value: '14d', label: '14 дней', days: 14 },
  { value: '30d', label: '30 дней', days: 30 },
  { value: '90d', label: '90 дней', days: 90 },
]

function computeDates(periodValue: string) {
  const opt = PERIOD_OPTIONS.find(p => p.value === periodValue) || PERIOD_OPTIONS[2]
  const end = new Date()
  const start = subDays(end, opt.days - 1)
  return { startDate: format(start, 'yyyy-MM-dd'), endDate: format(end, 'yyyy-MM-dd') }
}

/* ═══════════════════════════════════════════════════════════════
   Props & Types
   ═══════════════════════════════════════════════════════════════ */

interface CampaignDetailModalProps {
  isOpen: boolean
  onClose: () => void
  marketplace: string
  campaignId: number
  campaignTitle: string
  startDate: string
  endDate: string
  items?: CampaignSkuItem[]
  sku?: number
}

type TabType = 'stats' | 'events' | 'purchases' | 'phrases' | 'heatmap'

/* ═══════════════════════════════════════════════════════════════
   Main Component
   ═══════════════════════════════════════════════════════════════ */

export function CampaignDetailModal({
  isOpen, onClose, marketplace, campaignId, campaignTitle,
  items = [], sku: initialSku,
}: CampaignDetailModalProps) {
  const [activeTab, setActiveTab] = useState<TabType>('stats')
  const [loading, setLoading] = useState(false)
  const [period, setPeriod] = useState('30d')
  const [selectedSku, setSelectedSku] = useState<number | undefined>(initialSku)
  const [showSkuDropdown, setShowSkuDropdown] = useState(false)

  // Visible chart metrics
  const [visibleMetrics, setVisibleMetrics] = useState<Set<string>>(new Set(['spend', 'revenue', 'orders', 'cart']))

  const { startDate, endDate } = computeDates(period)

  // Data
  const [stats, setStats] = useState<CampaignStatsRow[]>([])
  const [events, setEvents] = useState<CampaignEventRow[]>([])
  const [phrases, setPhrases] = useState<CampaignPhraseRow[]>([])
  const [heatmap, setHeatmap] = useState<CampaignHeatmapRow[]>([])
  const [purchases, setPurchases] = useState<CampaignPurchaseRow[]>([])
  const [kpiData, setKpiData] = useState<CampaignKpiResponse | null>(null)
  const [loadedTabs, setLoadedTabs] = useState<Set<string>>(new Set())

  const loadTabData = useCallback(async (tab: TabType) => {
    const cacheKey = `${tab}_${startDate}_${endDate}_${selectedSku || 'all'}`
    if (loadedTabs.has(cacheKey)) return
    setLoading(true)
    try {
      switch (tab) {
        case 'stats': {
          const [s, ev, kpi] = await Promise.all([
            getCampaignStats(marketplace, campaignId, startDate, endDate, selectedSku),
            getCampaignEvents(marketplace, campaignId, selectedSku),
            getCampaignKpi(marketplace, campaignId, startDate, endDate, selectedSku),
          ])
          setStats(s)
          setEvents(ev)
          setKpiData(kpi)
          break
        }
        case 'events': {
          const promises: Promise<void>[] = []
          if (events.length === 0) {
            promises.push(getCampaignEvents(marketplace, campaignId, selectedSku).then(ev => setEvents(ev)))
          }
          if (stats.length === 0) {
            promises.push(
              getCampaignStats(marketplace, campaignId, startDate, endDate, selectedSku).then(s => setStats(s))
            )
          }
          await Promise.all(promises)
          break
        }
        case 'phrases': {
          const d = await getCampaignPhrases(marketplace, campaignId, startDate, endDate)
          setPhrases(d)
          break
        }
        case 'heatmap': {
          const d = await getCampaignHeatmap(marketplace, campaignId, startDate, endDate, selectedSku)
          setHeatmap(d)
          break
        }
        case 'purchases': {
          const d = await getCampaignPurchases(marketplace, campaignId, startDate, endDate)
          setPurchases(d)
          break
        }
      }
      setLoadedTabs(prev => new Set(prev).add(cacheKey))
    } catch (err) { console.error(`Failed to load ${tab}:`, err) }
    finally { setLoading(false) }
  }, [marketplace, campaignId, startDate, endDate, selectedSku, loadedTabs, events.length, stats.length])

  useEffect(() => { setLoadedTabs(new Set()) }, [period, selectedSku])
  useEffect(() => { if (isOpen) loadTabData(activeTab) }, [isOpen, activeTab, loadTabData])
  useEffect(() => {
    const h = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
  }, [onClose])

  // Lock body scroll when modal is open
  useEffect(() => {
    if (isOpen) {
      const scrollY = window.scrollY
      document.body.style.position = 'fixed'
      document.body.style.top = `-${scrollY}px`
      document.body.style.width = '100%'
      document.body.style.overflow = 'hidden'
      return () => {
        document.body.style.position = ''
        document.body.style.top = ''
        document.body.style.width = ''
        document.body.style.overflow = ''
        window.scrollTo(0, scrollY)
      }
    }
  }, [isOpen])

  if (!isOpen) return null

  const tabs: { id: TabType; label: string; icon: React.ElementType }[] = [
    { id: 'stats', label: 'Графики', icon: BarChart3 },
    { id: 'events', label: 'События', icon: Calendar },
    { id: 'purchases', label: 'Покупки', icon: Package },
    { id: 'phrases', label: 'Фразы', icon: Search },
    { id: 'heatmap', label: 'Активность', icon: Flame },
  ]

  /* ── EVENT DATE → events per day for chart markers ── */
  const eventsByDate: Record<string, CampaignEventRow[]> = {}
  events.forEach(ev => {
    try { const d = ev.timestamp.slice(0, 10); (eventsByDate[d] ??= []).push(ev) } catch {}
  })

  /* ════════════════════════════════════════════════════════════
     Stats Tab — chart with all metrics + event markers
     ════════════════════════════════════════════════════════════ */
  const renderStats = () => {
    if (loading && stats.length === 0) return <Spinner text="Загрузка графиков..." />
    if (stats.length === 0) return <Empty text="Нет данных по статистике за этот период" />

    const cur = kpiData?.current
    const prev = kpiData?.previous

    // Delta helper: returns percentage change
    const delta = (c?: number, p?: number) => {
      if (!c || !p || p === 0) return null
      return ((c - p) / p) * 100
    }
    const deltaStr = (d: number | null) => {
      if (d === null) return null
      const sign = d > 0 ? '+' : ''
      return `${sign}${d.toFixed(1)}%`
    }
    const deltaColor = (d: number | null, inverted = false) => {
      if (d === null) return ''
      // For spend/drr: decrease is good (green), increase is bad (red)
      if (inverted) return d < 0 ? 'text-emerald-400' : d > 0 ? 'text-red-400' : 'text-[hsl(var(--muted-foreground))]'
      // For revenue/orders: increase is good
      return d > 0 ? 'text-emerald-400' : d < 0 ? 'text-red-400' : 'text-[hsl(var(--muted-foreground))]'
    }

    // Enrich chart data with event counts
    const chartData = stats.map(s => ({
      ...s,
      eventCount: (eventsByDate[s.dt] || []).length,
    }))

    // Dates where events occurred for reference lines
    const eventDates = Object.keys(eventsByDate).filter(d =>
      stats.some(s => s.dt === d)
    )

    const toggleMetric = (key: string) => {
      setVisibleMetrics(prev => {
        const next = new Set(prev)
        if (next.has(key)) next.delete(key); else next.add(key)
        return next
      })
    }

    /* Custom Tooltip with events */
    const CustomTooltip = ({ active, payload, label }: any) => {
      if (!active || !payload?.length) return null
      const dtStr = label as string
      let dateLabel = dtStr
      try { dateLabel = format(parseISO(dtStr), 'dd MMMM yyyy', { locale: ru }) } catch {}
      const dayEvents = eventsByDate[dtStr] || []

      return (
        <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--popover))] shadow-xl p-3 text-[12px] max-w-[300px]">
          <div className="font-semibold text-[hsl(var(--foreground))] mb-1.5">{dateLabel}</div>
          {payload.map((entry: any, i: number) => {
            const m = CHART_METRICS.find(x => x.key === entry.dataKey)
            if (!m) return null
            let valStr = ''
            if (['drr', 'ctr'].includes(entry.dataKey)) valStr = `${Number(entry.value).toFixed(1)}%`
            else if (['spend', 'revenue'].includes(entry.dataKey)) valStr = formatMoney(Number(entry.value))
            else valStr = formatNumber(Number(entry.value))
            return (
              <div key={i} className="flex items-center justify-between gap-3 py-0.5">
                <span className="flex items-center gap-1.5">
                  <span className="w-2 h-2 rounded-full inline-block" style={{ backgroundColor: m.color }} />
                  <span className="text-[hsl(var(--muted-foreground))]">{m.label}</span>
                </span>
                <span className="font-semibold text-[hsl(var(--foreground))]">{valStr}</span>
              </div>
            )
          })}
          {dayEvents.length > 0 && (
            <div className="mt-2 pt-2 border-t border-[hsl(var(--border)/0.5)]">
              <div className="text-[11px] font-semibold text-[#a78bfa] mb-1">📌 События ({dayEvents.length})</div>
              {dayEvents.slice(0, 5).map((ev, i) => {
                const evLabel = EVENT_LABELS[ev.event_type] || ev.event_type
                const style = EVENT_STYLE[ev.event_type] || DEFAULT_EV_STYLE
                let detail = ''
                if (ev.old_value && ev.new_value) {
                  let ov = ev.old_value, nv = ev.new_value
                  if (ev.event_type === 'BID_CHANGE') { ov = String(Number(ov) / 100); nv = String(Number(nv) / 100) }
                  detail = ` ${ov} → ${nv}`
                }
                return (
                  <div key={i} className="flex items-center gap-1.5 py-0.5 text-[11px]">
                    <span className="w-1.5 h-1.5 rounded-full shrink-0" style={{ backgroundColor: style.color }} />
                    <span style={{ color: style.color }} className="font-medium">{evLabel}</span>
                    {detail && <span className="text-[hsl(var(--muted-foreground))]">{detail}</span>}
                  </div>
                )
              })}
              {dayEvents.length > 5 && (
                <div className="text-[10px] text-[hsl(var(--muted-foreground))] mt-0.5">...ещё {dayEvents.length - 5}</div>
              )}
            </div>
          )}
        </div>
      )
    }

    // Determine which axes are active
    const moneyActive = visibleMetrics.has('spend') || visibleMetrics.has('revenue') || visibleMetrics.has('product_revenue')
    const countActive = visibleMetrics.has('orders') || visibleMetrics.has('cart') || visibleMetrics.has('clicks') || visibleMetrics.has('views')

    // KPI delta values
    const spendD = delta(cur?.spend, prev?.spend)
    const adRevD = delta(cur?.ad_revenue, prev?.ad_revenue)
    const prodRevD = delta(cur?.product_revenue, prev?.product_revenue)
    const ordersD = delta(cur?.orders, prev?.orders)
    const cartD = delta(cur?.cart, prev?.cart)
    const drrAdD = cur && prev ? (cur.drr_ad - prev.drr_ad) : null // absolute pp change
    const drrProdD = cur && prev ? (cur.drr_product - prev.drr_product) : null
    const cpoD = delta(cur?.cpo, prev?.cpo)
    const clicksD = delta(cur?.clicks, prev?.clicks)
    const viewsD = delta(cur?.views, prev?.views)

    return (
      <div className="space-y-4">
        {/* KPI Cards — 6 cards in 2 rows */}
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
          {/* Расход */}
          <div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-3">
            <div className="text-[14px] text-[hsl(var(--muted-foreground))] font-semibold">Расход</div>
            <div className="text-lg font-bold text-red-400 mt-0.5">{formatMoney(cur?.spend || 0)}</div>
            {spendD !== null && (
              <div className={`text-[11px] font-medium ${deltaColor(spendD, true)}`}>
                {deltaStr(spendD)} vs пред. период
              </div>
            )}
          </div>

          {/* Выручка — реклама + товары */}
          <div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-3">
            <div className="text-[14px] text-[hsl(var(--muted-foreground))] font-semibold">Выручка</div>
            <div className="flex items-baseline gap-2 mt-0.5">
              <div>
                <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Рекламная</div>
                <div className="text-base font-bold text-emerald-400">{formatMoney(cur?.ad_revenue || 0)}</div>
                {adRevD !== null && <span className={`text-[13px] font-medium ${deltaColor(adRevD)}`}>{deltaStr(adRevD)}</span>}
              </div>
              <div className="border-l border-[hsl(var(--border))] pl-2">
                <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Товаров</div>
                <div className="text-base font-bold text-teal-400">{formatMoney(cur?.product_revenue || 0)}</div>
                {prodRevD !== null && <span className={`text-[13px] font-medium ${deltaColor(prodRevD)}`}>{deltaStr(prodRevD)}</span>}
              </div>
            </div>
          </div>

          {/* Заказы + Корзины */}
          <div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-3">
            <div className="text-[14px] text-[hsl(var(--muted-foreground))] font-semibold">Заказы / Корзины</div>
            <div className="flex items-baseline gap-3 mt-0.5">
              <div>
                <div className="text-lg font-bold text-purple-400">{cur?.orders || 0}</div>
                {ordersD !== null && <span className={`text-[13px] font-medium ${deltaColor(ordersD)}`}>{deltaStr(ordersD)}</span>}
              </div>
              <div className="text-[hsl(var(--muted-foreground))]">/</div>
              <div>
                <div className="text-lg font-bold text-orange-400">{cur?.cart || 0}</div>
                {cartD !== null && <span className={`text-[13px] font-medium ${deltaColor(cartD)}`}>{deltaStr(cartD)}</span>}
              </div>
            </div>
          </div>

          {/* ДРР — рекламный + общий */}
          <div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-3">
            <div className="text-[14px] text-[hsl(var(--muted-foreground))] font-semibold">ДРР</div>
            <div className="flex items-baseline gap-2 mt-0.5">
              <div>
                <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Рекламный</div>
                <div className="text-base font-bold text-pink-400">{(cur?.drr_ad || 0).toFixed(1)}%</div>
                {drrAdD !== null && <span className={`text-[13px] font-medium ${deltaColor(drrAdD, true)}`}>{drrAdD > 0 ? '+' : ''}{drrAdD.toFixed(1)} п.п.</span>}
              </div>
              <div className="border-l border-[hsl(var(--border))] pl-2">
                <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Общий</div>
                <div className="text-base font-bold text-amber-400">{(cur?.drr_product || 0).toFixed(1)}%</div>
                {drrProdD !== null && <span className={`text-[13px] font-medium ${deltaColor(drrProdD, true)}`}>{drrProdD > 0 ? '+' : ''}{drrProdD.toFixed(1)} п.п.</span>}
              </div>
            </div>
          </div>

          {/* CPO */}
          <div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-3">
            <div className="text-[14px] text-[hsl(var(--muted-foreground))] font-semibold">CPO (стоимость заказа)</div>
            <div className="text-lg font-bold text-purple-400 mt-0.5">{(cur?.cpo || 0) > 0 ? formatMoney(cur!.cpo) : '—'}</div>
            {cpoD !== null && (
              <div className={`text-[11px] font-medium ${deltaColor(cpoD, true)}`}>{deltaStr(cpoD)}</div>
            )}
          </div>

          {/* Клики + Показы + CTR */}
          <div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-3">
            <div className="text-[14px] text-[hsl(var(--muted-foreground))] font-semibold">Трафик</div>
            <div className="flex items-baseline gap-2 mt-0.5">
              <div>
                <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Клики</div>
                <div className="text-base font-bold text-blue-400">{formatNumber(cur?.clicks || 0)}</div>
                {clicksD !== null && <span className={`text-[13px] font-medium ${deltaColor(clicksD)}`}>{deltaStr(clicksD)}</span>}
              </div>
              <div className="border-l border-[hsl(var(--border))] pl-2">
                <div className="text-[13px] text-[hsl(var(--muted-foreground))]">CTR</div>
                <div className="text-base font-bold text-teal-400">{(cur?.ctr || 0).toFixed(2)}%</div>
              </div>
              <div className="border-l border-[hsl(var(--border))] pl-2">
                <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Показы</div>
                <div className="text-base font-bold text-amber-400">{formatNumber(cur?.views || 0)}</div>
                {viewsD !== null && <span className={`text-[13px] font-medium ${deltaColor(viewsD)}`}>{deltaStr(viewsD)}</span>}
              </div>
            </div>
          </div>
        </div>

        {/* Metric toggles */}
        <div className="flex flex-wrap gap-1.5">
          {CHART_METRICS.map(m => (
            <button
              key={m.key}
              onClick={() => toggleMetric(m.key)}
              className={`px-2.5 py-1 text-[11px] font-semibold rounded-md border transition-all ${
                visibleMetrics.has(m.key)
                  ? 'border-transparent text-white shadow-sm'
                  : 'border-[hsl(var(--border))] text-[hsl(var(--muted-foreground))] bg-transparent hover:bg-[hsl(var(--muted)/0.2)]'
              }`}
              style={visibleMetrics.has(m.key) ? { backgroundColor: m.color } : {}}
            >
              {m.label}
            </button>
          ))}
        </div>

        {/* Chart */}
        <div className="h-[300px] w-full rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-4">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={chartData} margin={{ top: 5, right: 5, left: -10, bottom: 0 }}>
              <defs>
                <linearGradient id="gSpendM" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#ef4444" stopOpacity={0.15} />
                  <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="gRevenueM" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.15} />
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="gProdRevM" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.15} />
                  <stop offset="95%" stopColor="#06b6d4" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
              <XAxis
                dataKey="dt"
                tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                tickLine={false} axisLine={false}
                tickFormatter={formatChartDate}
                interval={stats.length <= 15 ? 0 : Math.floor(stats.length / 12)}
                angle={-45}
                textAnchor="end"
                height={50}
              />
              {/* Left axis — money (spend, revenue) */}
              <YAxis
                yAxisId="money"
                tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }}
                tickLine={false} axisLine={false}
                tickFormatter={v => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v}
                hide={!moneyActive}
              />
              {/* Right axis — counts (orders, cart, clicks, views) + DRR% */}
              <YAxis
                yAxisId="count"
                orientation="right"
                tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }}
                tickLine={false} axisLine={false}
                hide={!countActive && !visibleMetrics.has('drr')}
              />

              <RTooltip content={<CustomTooltip />} />

              <Legend
                verticalAlign="top" height={36} iconType="circle" iconSize={9}
                formatter={v => CHART_METRICS.find(x => x.key === v)?.label || v}
                wrapperStyle={{ fontSize: '14px', fontWeight: 500, paddingBottom: '12px' }}
              />

              {/* Event markers — vertical reference lines */}
              {eventDates.map(d => (
                <ReferenceLine
                  key={d}
                  x={d}
                  yAxisId="money"
                  stroke="#a78bfa"
                  strokeDasharray="3 3"
                  strokeWidth={1.5}
                  label={{ value: `📌${(eventsByDate[d] || []).length}`, position: 'top', fontSize: 10, fill: '#a78bfa' }}
                />
              ))}

              {/* Areas on money axis */}
              {visibleMetrics.has('spend') && (
                <Area yAxisId="money" type="monotone" dataKey="spend" stroke="#ef4444" fill="url(#gSpendM)" strokeWidth={2} name="spend" />
              )}
              {visibleMetrics.has('revenue') && (
                <Area yAxisId="money" type="monotone" dataKey="revenue" stroke="#10b981" fill="url(#gRevenueM)" strokeWidth={2} name="revenue" />
              )}
              {visibleMetrics.has('product_revenue') && (
                <Area yAxisId="money" type="monotone" dataKey="product_revenue" stroke="#06b6d4" fill="url(#gProdRevM)" strokeWidth={2} name="product_revenue" />
              )}
              {/* Bars on count axis — stacked so both visible */}
              {visibleMetrics.has('orders') && (
                <Bar yAxisId="count" dataKey="orders" fill="#8b5cf6" radius={[0, 0, 0, 0]} barSize={14} name="orders" fillOpacity={0.85} stackId="bars" />
              )}
              {visibleMetrics.has('cart') && (
                <Bar yAxisId="count" dataKey="cart" fill="#f97316" radius={[3, 3, 0, 0]} barSize={14} name="cart" fillOpacity={0.85} stackId="bars" />
              )}
              {/* Lines on count axis */}
              {visibleMetrics.has('clicks') && (
                <Line yAxisId="count" type="monotone" dataKey="clicks" stroke="#3b82f6" strokeWidth={1.5} dot={false} name="clicks" />
              )}
              {visibleMetrics.has('views') && (
                <Line yAxisId="count" type="monotone" dataKey="views" stroke="#f59e0b" strokeWidth={1.5} dot={false} name="views" />
              )}
              {visibleMetrics.has('ctr') && (
                <Line yAxisId="count" type="monotone" dataKey="ctr" stroke="#14b8a6" strokeWidth={1.5} dot={false} strokeDasharray="4 2" name="ctr" />
              )}
              {visibleMetrics.has('drr') && (
                <Line yAxisId="count" type="monotone" dataKey="drr" stroke="#f97316" strokeWidth={2} dot={false} strokeDasharray="5 3" name="drr" />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {/* Event count info */}
        {events.length > 0 && (
          <div className="text-[11px] text-[hsl(var(--muted-foreground))] flex items-center gap-1.5">
            📌 На графике отмечено {eventDates.length} дней с событиями ({events.length} всего) — наведите для подробностей
          </div>
        )}
      </div>
    )
  }

  /* ════════════════════════════════════════════════════════════
     Events Tab — styled like EventsPage
     ════════════════════════════════════════════════════════════ */
  const renderEvents = () => {
    if (loading && events.length === 0) return <Spinner text="Загрузка событий..." />
    if (events.length === 0) return <Empty text="Событий не найдено за выбранный период" />

    // Group by date
    const byDay: Record<string, CampaignEventRow[]> = {}
    events.forEach(ev => {
      const d = ev.timestamp.slice(0, 10)
      ;(byDay[d] ??= []).push(ev)
    })
    const sortedDays = Object.keys(byDay).sort((a, b) => b.localeCompare(a))

    // Build impact data from daily stats
    const sortedStats = [...stats].sort((a, b) => a.dt.localeCompare(b.dt))
    const dateIdx = new Map(sortedStats.map((d, i) => [d.dt, i]))
    const avgArr = (arr: number[]) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0

    const calcImpact = (eventDate: string) => {
      const idx = dateIdx.get(eventDate)
      if (idx === undefined) return null
      const before = sortedStats.slice(Math.max(0, idx - 7), idx)
      const after = sortedStats.slice(idx + 1, Math.min(sortedStats.length, idx + 8))
      if (before.length < 3 || after.length < 3) return { insufficient: true, afterDays: after.length }
      const pct = (key: keyof CampaignStatsRow) => {
        const bv = avgArr(before.map(d => Number(d[key])))
        const av = avgArr(after.map(d => Number(d[key])))
        return bv > 0 ? ((av - bv) / bv) * 100 : 0
      }
      return {
        insufficient: false,
        afterDays: after.length,
        spend: pct('spend'),
        views: pct('views'),
        clicks: pct('clicks'),
        orders: pct('orders'),
        drr: pct('drr'),
        revenue: pct('revenue'),
        product_revenue: pct('product_revenue'),
      }
    }

    const ImpactLabel = ({ label, d, inv }: { label: string; d: number; inv?: boolean }) => {
      const color = inv
        ? (d < -5 ? 'text-emerald-400' : d > 5 ? 'text-red-400' : 'text-[hsl(var(--muted-foreground)/0.5)]')
        : (d > 5 ? 'text-emerald-400' : d < -5 ? 'text-red-400' : 'text-[hsl(var(--muted-foreground)/0.5)]')
      return (
        <span className="text-[13px]">
          <span className="text-[hsl(var(--muted-foreground)/0.5)]">{label}</span>{' '}
          <span className={`font-semibold ${color}`}>{d > 0 ? '+' : ''}{d.toFixed(0)}%</span>
        </span>
      )
    }

    return (
      <div className="space-y-5 max-h-[450px] overflow-y-auto pr-1">
        {sortedDays.map(day => {
          const dayEvents = byDay[day]
          let dayLabel: string
          try {
            const d = new Date(day + 'T00:00:00')
            const today = new Date()
            const yesterday = new Date(); yesterday.setDate(today.getDate() - 1)
            const same = (a: Date, b: Date) => a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate()
            if (same(d, today)) dayLabel = 'Сегодня'
            else if (same(d, yesterday)) dayLabel = 'Вчера'
            else dayLabel = format(d, 'dd MMMM — EEEE', { locale: ru })
          } catch { dayLabel = day }

          // Calculate impact for this day's events
          const impact = calcImpact(day)

          return (
            <div key={day}>
              {/* Day header */}
              <div className="flex items-center gap-2 mb-3">
                <h4 className="text-[14px] font-bold">{dayLabel}</h4>
                <span className="text-[11px] px-2 py-0.5 rounded-full bg-[hsl(var(--muted)/0.25)] text-[hsl(var(--muted-foreground))] font-medium">{dayEvents.length}</span>
                <div className="flex-1 h-px bg-[hsl(var(--border)/0.3)]" />
              </div>

              {/* Events */}
              <div className="space-y-2.5">
                {dayEvents.map(ev => {
                  const style = EVENT_STYLE[ev.event_type] || DEFAULT_EV_STYLE
                  const Icon = style.icon
                  const label = EVENT_LABELS[ev.event_type] || ev.event_type.replace(/_/g, ' ')
                  const isNumeric = NUMERIC_EVENTS.has(ev.event_type)
                  let timeStr = ''
                  try { timeStr = format(parseISO(ev.timestamp), 'HH:mm') } catch {}

                  // WB BID_CHANGE: kopecks → rubles
                  let oldNum = parseNum(ev.old_value)
                  let newNum = parseNum(ev.new_value)
                  if (ev.event_type === 'BID_CHANGE' && oldNum !== null) oldNum /= 100
                  if (ev.event_type === 'BID_CHANGE' && newNum !== null) newNum /= 100
                  const suffix = ev.event_type.includes('BID') || ev.event_type.includes('BUDGET') || ev.event_type.includes('PRICE') ? ' ₽' : ''

                  return (
                    <div key={ev.id} className="flex gap-3 rounded-xl border border-[hsl(var(--border)/0.4)] bg-[hsl(var(--card))] p-4 hover:border-[hsl(var(--border)/0.7)] transition-all">
                      {/* Icon */}
                      <div className="shrink-0 h-8 w-8 rounded-lg flex items-center justify-center" style={{ background: style.bg }}>
                        <Icon className="h-4 w-4" style={{ color: style.color }} />
                      </div>

                      {/* Content */}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-[14px] font-semibold" style={{ color: style.color }}>{label}</span>
                          <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">⏱ {timeStr}</span>
                        </div>
                        {ev.product_id && (
                          <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.6)] block mt-0.5">
                            {ev.offer_id && <span className="font-mono font-semibold uppercase text-[hsl(var(--primary)/0.8)]">{ev.offer_id} · </span>}
                            {ev.product_name || `SKU: ${ev.product_id}`}
                          </span>
                        )}

                        {/* Numeric value change */}
                        {isNumeric && oldNum !== null && newNum !== null && (
                          <div className="flex items-center gap-2.5 mt-2">
                            <span className="text-[14px] font-medium text-[hsl(var(--muted-foreground)/0.7)] line-through">{fmtNum(oldNum, suffix)}</span>
                            <ArrowRight className="h-3 w-3 text-[hsl(var(--muted-foreground)/0.4)]" />
                            <span className={`text-[15px] font-bold ${(newNum - oldNum) > 0 ? 'text-emerald-400' : 'text-red-400'}`}>{fmtNum(newNum, suffix)}</span>
                            {(newNum - oldNum) !== 0 && (
                              <span className={`inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[11px] font-semibold ${
                                (newNum - oldNum) > 0 ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/15 text-red-400'
                              }`}>
                                {(newNum - oldNum) > 0 ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />}
                                {oldNum !== 0 ? `${Math.abs(((newNum - oldNum) / oldNum) * 100).toFixed(1)}%` : fmtNum(newNum - oldNum, suffix)}
                              </span>
                            )}
                          </div>
                        )}

                        {/* Non-numeric: show old → new */}
                        {!isNumeric && (ev.old_value || ev.new_value) && (
                          <div className="flex items-center gap-2 mt-1.5 text-[12px]">
                            {ev.old_value && <span className="px-1.5 py-0.5 bg-red-500/10 text-red-400 rounded font-medium">{ev.old_value}</span>}
                            <ArrowRight className="h-3 w-3 text-[hsl(var(--muted-foreground)/0.4)]" />
                            {ev.new_value && <span className="px-1.5 py-0.5 bg-emerald-500/10 text-emerald-400 rounded font-medium">{ev.new_value}</span>}
                          </div>
                        )}
                      </div>
                    </div>
                  )
                })}
              </div>

              {/* Event Impact — after all events of this day */}
              {impact && !impact.insufficient && (
                <div className="mt-2 ml-11 rounded-lg border border-[hsl(var(--border)/0.2)] bg-[hsl(var(--muted)/0.05)] px-4 py-2.5">
                  <div className="text-[12px] text-[hsl(var(--muted-foreground)/0.4)] mb-1.5">
                    📊 Влияние (7 дн. до → {impact.afterDays} дн. после)
                  </div>
                  <div className="flex flex-wrap gap-x-4 gap-y-1">
                    <ImpactLabel label="Расход" d={impact.spend!} inv />
                    <ImpactLabel label="Показы" d={impact.views!} />
                    <ImpactLabel label="Клики" d={impact.clicks!} />
                    <ImpactLabel label="Заказы" d={impact.orders!} />
                    <ImpactLabel label="Рекл. выр." d={impact.revenue!} />
                    <ImpactLabel label="Общ. выр." d={impact.product_revenue!} />
                    <ImpactLabel label="ДРР" d={impact.drr!} inv />
                  </div>
                </div>
              )}
              {impact && impact.insufficient && (
                <div className="mt-2 ml-11 text-[12px] text-[hsl(var(--muted-foreground)/0.4)]">
                  Мало данных после события (нужно ≥3 дней)
                </div>
              )}
            </div>
          )
        })}
      </div>
    )
  }

  /* ════════════════════════════════════════════════════════════
     Purchases Tab
     ════════════════════════════════════════════════════════════ */
  const renderPurchases = () => {
    if (loading && purchases.length === 0) return <Spinner text="Загрузка покупок..." />
    if (purchases.length === 0) return <Empty text="Нет данных о покупках за этот период" />
    const totalQty = purchases.reduce((a, c) => a + c.quantity, 0)
    const totalRev = purchases.reduce((a, c) => a + c.revenue, 0)
    return (
      <div className="space-y-4">
        <div className="flex items-center gap-4 text-[13px] text-[hsl(var(--muted-foreground))]">
          <span>Товаров: <strong className="text-[hsl(var(--foreground))]">{purchases.length}</strong></span>
          <span>Заказано: <strong className="text-[hsl(var(--foreground))]">{totalQty} шт.</strong></span>
          <span>Выручка: <strong className="text-emerald-400">{formatMoney(totalRev)}</strong></span>
        </div>
        <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden max-h-[380px] overflow-y-auto">
          <table className="w-full text-left">
            <thead className="sticky top-0 bg-[hsl(var(--card))] z-10 shadow-[0_1px_0_hsl(var(--border))]">
              <tr>
                <th className="px-4 py-2.5 text-[11px] font-medium text-[hsl(var(--muted-foreground))] uppercase">Товар</th>
                <th className="px-4 py-2.5 text-[11px] font-medium text-[hsl(var(--muted-foreground))] text-right uppercase">Кол-во</th>
                <th className="px-4 py-2.5 text-[11px] font-medium text-[hsl(var(--muted-foreground))] text-right uppercase">Выручка</th>
                <th className="px-4 py-2.5 text-[11px] font-medium text-[hsl(var(--muted-foreground))] text-right uppercase">Ср. цена</th>
              </tr>
            </thead>
            <tbody>
              {purchases.map((p, i) => (
                <tr key={i} className="border-b border-[hsl(var(--border)/0.4)] last:border-0 hover:bg-[hsl(var(--muted)/0.15)]">
                  <td className="px-4 py-2.5">
                    <div className="text-[13px] font-medium truncate max-w-[280px]" title={p.product_name}>{p.product_name}</div>
                    <div className="text-[11px] text-[hsl(var(--muted-foreground))]">{p.offer_id || `SKU: ${p.sku}`}</div>
                  </td>
                  <td className="px-4 py-2.5 text-[13px] text-right font-medium">{p.quantity} шт.</td>
                  <td className="px-4 py-2.5 text-[13px] text-right text-emerald-400 font-medium">{formatMoney(p.revenue)}</td>
                  <td className="px-4 py-2.5 text-[13px] text-right">{formatMoney(p.avg_price)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    )
  }

  /* ════════════════════════════════════════════════════════════
     Phrases Tab
     ════════════════════════════════════════════════════════════ */
  const [phraseSort, setPhraseSort] = useState<{ key: string; dir: 'asc' | 'desc' }>({ key: 'views', dir: 'desc' })

  const renderPhrases = () => {
    if (loading && phrases.length === 0) return <Spinner text="Загрузка поисковых фраз..." />
    if (phrases.length === 0) return <Empty text="Нет данных по фразам. Данные выгружаются автоматически — возможно первая выгрузка ещё не произошла." />

    const sorted = [...phrases].sort((a, b) => {
      const av = (a as any)[phraseSort.key]
      const bv = (b as any)[phraseSort.key]
      if (typeof av === 'string') return phraseSort.dir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return phraseSort.dir === 'asc' ? av - bv : bv - av
    })

    const thSort = (key: string, label: string, align: string = 'text-right') => {
      const active = phraseSort.key === key
      return (
        <th
          className={`px-4 py-2.5 text-[11px] font-medium text-[hsl(var(--muted-foreground))] uppercase cursor-pointer select-none hover:text-[hsl(var(--foreground))] transition-colors ${align}`}
          onClick={() => setPhraseSort(prev => ({ key, dir: prev.key === key && prev.dir === 'desc' ? 'asc' : 'desc' }))}
        >
          {label} {active ? (phraseSort.dir === 'desc' ? '▼' : '▲') : ''}
        </th>
      )
    }

    return (
      <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden max-h-[400px] overflow-y-auto">
        <table className="w-full text-left">
          <thead className="sticky top-0 bg-[hsl(var(--card))] z-10 shadow-[0_1px_0_hsl(var(--border))]">
            <tr>
              {thSort('phrase', 'Фраза', 'text-left')}
              {thSort('views', 'Показы')}
              {thSort('clicks', 'Клики')}
              {thSort('ctr', 'CTR')}
            </tr>
          </thead>
          <tbody>
            {sorted.map((p, i) => (
              <tr key={i} className="border-b border-[hsl(var(--border)/0.4)] last:border-0 hover:bg-[hsl(var(--muted)/0.15)]">
                <td className="px-4 py-2.5 text-[13px] font-medium max-w-[260px] truncate" title={p.phrase}>{p.phrase}</td>
                <td className="px-4 py-2.5 text-[13px] text-right">{formatNumber(p.views)}</td>
                <td className="px-4 py-2.5 text-[13px] text-right">{formatNumber(p.clicks)}</td>
                <td className="px-4 py-2.5 text-[13px] text-right">{p.ctr.toFixed(2)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }

  /* ════════════════════════════════════════════════════════════
     Heatmap Tab
     ════════════════════════════════════════════════════════════ */
  const renderHeatmap = () => {
    if (loading && heatmap.length === 0) return <Spinner text="Расчет тепловой карты..." />
    if (heatmap.length === 0) return <Empty text="Нет заказов для построения тепловой карты" />
    const days = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
    const hours = Array.from({ length: 24 }, (_, i) => i)
    const grid = Array(7).fill(0).map(() => Array(24).fill(0))
    let maxOrders = 0
    heatmap.forEach(h => {
      const d = h.day_of_week - 1
      if (d >= 0 && d < 7 && h.hour >= 0 && h.hour < 24) {
        grid[d][h.hour] = h.orders
        if (h.orders > maxOrders) maxOrders = h.orders
      }
    })
    const getColor = (val: number) => {
      if (val === 0) return 'hsl(var(--muted)/0.15)'
      const r = val / maxOrders
      if (r > 0.75) return '#7c3aed'; if (r > 0.5) return '#8b5cf6'
      if (r > 0.25) return '#a78bfa'; return '#c4b5fd'
    }
    const totalOrders = heatmap.reduce((a, c) => a + c.orders, 0)
    return (
      <div className="space-y-4">
        <div className="text-[12px] text-[hsl(var(--muted-foreground))]">Заказы всех участвующих SKU · Всего: <strong className="text-[hsl(var(--foreground))]">{totalOrders}</strong></div>
        <div className="flex">
          <div className="flex flex-col justify-between pr-2 py-[3px] w-8 text-[10px] font-medium text-[hsl(var(--muted-foreground))]">
            {days.map((d, i) => <div key={i} className="h-[28px] flex items-center justify-end">{d}</div>)}
          </div>
          <div className="flex-1 overflow-x-auto"><div className="min-w-[550px]">
            {grid.map((row, dayIdx) => (
              <div key={dayIdx} className="flex gap-[3px] mb-[3px]">
                {row.map((val, hrIdx) => (
                  <div key={hrIdx} className="flex-1 h-[28px] rounded-[4px] flex items-center justify-center text-[10px] font-semibold transition-all select-none cursor-default hover:scale-110 hover:z-10"
                    style={{ backgroundColor: getColor(val), color: val > 0 ? 'white' : 'transparent' }}
                    title={`${days[dayIdx]} ${hrIdx}:00–${hrIdx}:59 → ${val} заказов`}
                  >{val > 0 ? val : ''}</div>
                ))}
              </div>
            ))}
            <div className="flex gap-[3px] mt-1.5">{hours.map(hr => (<div key={hr} className="flex-1 text-center text-[9px] text-[hsl(var(--muted-foreground))]">{hr}</div>))}</div>
          </div></div>
        </div>
      </div>
    )
  }

  /* ════════════════════════════════════════════════════════════
     Layout
     ════════════════════════════════════════════════════════════ */
  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/60 backdrop-blur-sm p-4" onClick={onClose}>
      <div className="w-full max-w-[1200px] max-h-[92vh] bg-[hsl(var(--background))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl flex flex-col animate-in fade-in zoom-in-95 duration-200" onClick={e => e.stopPropagation()}>
        {/* Header */}
        <div className="flex items-start justify-between p-5 px-6 border-b border-[hsl(var(--border))]">
          <div className="flex-1 min-w-0 pr-8">
            <div className="flex items-center gap-2 mb-1.5">
              <span className="text-[11px] uppercase font-bold tracking-wider px-2 py-0.5 rounded bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))]">{marketplace === 'ozon' ? 'OZON' : 'WB'}</span>
              <span className="text-[12px] text-[hsl(var(--muted-foreground))]">ID: {campaignId}</span>
            </div>
            <h2 className="text-[22px] font-bold leading-tight truncate" title={campaignTitle}>{campaignTitle}</h2>
          </div>
          <button onClick={onClose} className="p-2 rounded-lg hover:bg-[hsl(var(--muted))] text-[hsl(var(--muted-foreground))] transition-colors"><X className="w-5 h-5" /></button>
        </div>

        {/* Controls */}
        <div className="px-6 py-3 border-b border-[hsl(var(--border))] flex items-center gap-4 flex-wrap">
          <div className="flex items-center gap-1 bg-[hsl(var(--muted)/0.3)] rounded-lg p-0.5">
            {PERIOD_OPTIONS.map(opt => (
              <button key={opt.value} onClick={() => setPeriod(opt.value)} className={`px-3 py-1.5 text-[14px] font-medium rounded-md transition-all ${period === opt.value ? 'bg-[hsl(var(--background))] text-[hsl(var(--foreground))] shadow-sm' : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'}`}>{opt.label}</button>
            ))}
          </div>
          {items.length > 0 && (
            <div className="relative">
              <button onClick={() => setShowSkuDropdown(!showSkuDropdown)} className="flex items-center gap-2 px-3 py-1.5 text-[12px] border border-[hsl(var(--border))] rounded-lg hover:bg-[hsl(var(--muted)/0.3)] transition-colors">
                <Package className="w-3.5 h-3.5 text-[hsl(var(--muted-foreground))]" />
                {selectedSku ? `SKU: ${selectedSku}` : 'Все товары'}
                <ChevronDown className="w-3 h-3 text-[hsl(var(--muted-foreground))]" />
              </button>
              {showSkuDropdown && (
                <div className="absolute top-full left-0 mt-1 w-[280px] bg-[hsl(var(--popover))] border border-[hsl(var(--border))] rounded-xl shadow-xl z-50 py-1 max-h-[200px] overflow-y-auto">
                  <button onClick={() => { setSelectedSku(undefined); setShowSkuDropdown(false) }} className={`w-full text-left px-3 py-2 text-[12px] hover:bg-[hsl(var(--muted)/0.3)] ${!selectedSku ? 'font-semibold text-[hsl(var(--primary))]' : ''}`}>Все товары</button>
                  {items.map(item => (
                    <button key={item.sku} onClick={() => { setSelectedSku(item.sku); setShowSkuDropdown(false) }} className={`w-full text-left px-3 py-2 text-[12px] hover:bg-[hsl(var(--muted)/0.3)] ${selectedSku === item.sku ? 'font-semibold text-[hsl(var(--primary))]' : ''}`}>
                      <span className="truncate block">{item.name || item.offer_id || `SKU ${item.sku}`}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          )}
          <span className="text-[11px] text-[hsl(var(--muted-foreground))] ml-auto">
            {(() => { try { return `${format(parseISO(startDate), 'dd.MM.yy')} – ${format(parseISO(endDate), 'dd.MM.yy')}` } catch { return '' } })()}
          </span>
        </div>

        {/* Tabs */}
        <div className="px-6 flex gap-1 border-b border-[hsl(var(--border))]">
          {tabs.map(t => {
            const Icon = t.icon
            return (
              <button key={t.id} onClick={() => setActiveTab(t.id)} className={`flex items-center gap-1.5 py-3 px-3 text-[14px] font-medium border-b-2 transition-colors ${activeTab === t.id ? 'border-[hsl(var(--primary))] text-[hsl(var(--foreground))]' : 'border-transparent text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'}`}>
                <Icon className="w-4 h-4" />{t.label}
              </button>
            )
          })}
        </div>

        {/* Content */}
        <div className="p-6 overflow-y-auto flex-1">
          {activeTab === 'stats' && renderStats()}
          {activeTab === 'events' && renderEvents()}
          {activeTab === 'purchases' && renderPurchases()}
          {activeTab === 'phrases' && renderPhrases()}
          {activeTab === 'heatmap' && renderHeatmap()}
        </div>
      </div>
    </div>
  )
}

/* ── Small helpers ──────────────────────────────────────────── */
function Spinner({ text }: { text: string }) {
  return (<div className="flex flex-col items-center justify-center py-12 text-[hsl(var(--muted-foreground))]"><Loader2 className="w-6 h-6 animate-spin mb-2" /><span className="text-[13px]">{text}</span></div>)
}
function Empty({ text }: { text: string }) {
  return (<div className="flex items-center justify-center py-12 text-[13px] text-[hsl(var(--muted-foreground))]">{text}</div>)
}
