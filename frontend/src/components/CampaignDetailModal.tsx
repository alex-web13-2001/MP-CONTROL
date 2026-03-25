import { useEffect, useState, useCallback, useRef } from 'react'
import {
  X, Loader2, BarChart3, Calendar, Package, Search, Flame, ChevronDown, Crosshair,
  CalendarDays, ChevronLeft, ChevronRight as ChevronRightIcon, Sparkles,
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
  streamCampaignAiAnalysis,
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
  { key: 'direct_revenue', label: 'Прямые', color: '#10b981', type: 'bar' },
  { key: 'model_revenue', label: 'Модель', color: '#3b82f6', type: 'bar' },
  { key: 'associated_revenue', label: 'Ассоц.', color: '#f59e0b', type: 'bar' },
  { key: 'product_revenue', label: 'Выручка общая', color: '#06b6d4', type: 'area' },
  { key: 'orders', label: 'Заказы', color: '#8b5cf6', type: 'bar' },
  { key: 'cart', label: 'Корзины', color: '#f97316', type: 'bar' },
  { key: 'clicks', label: 'Клики', color: '#3b82f6', type: 'line' },
  { key: 'views', label: 'Показы', color: '#f59e0b', type: 'line' },
  { key: 'ctr', label: 'CTR %', color: '#14b8a6', type: 'line' },
  { key: 'drr', label: 'ДРР %', color: '#ec4899', type: 'line' },
] as const

/* ── Inline ModalDatePicker ──────────────────────────── */
const MONTHS_RU_CAL = ['Январь','Февраль','Март','Апрель','Май','Июнь','Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь']
const DAYS_CAL = ['Пн','Вт','Ср','Чт','Пт','Сб','Вс']
function padN(n: number) { return String(n).padStart(2, '0') }
function toDS(y: number, m: number, d: number) { return `${y}-${padN(m+1)}-${padN(d)}` }
function fmtD(s: string) { if(!s) return '—'; const [y,m,d]=s.split('-'); return `${d}.${m}.${y}` }

function ModalDatePicker({ from, to, onChange }: { from: string; to: string; onChange: (f: string, t: string) => void }) {
  const [open, setOpen] = useState(false)
  const [sel, setSel] = useState<'from'|'to'>('from')
  const ref = useRef<HTMLDivElement>(null)
  const ad = sel === 'from' ? from : to
  const id = ad ? new Date(ad) : new Date()
  const [vY, setVY] = useState(id.getFullYear())
  const [vM, setVM] = useState(id.getMonth())
  useEffect(() => { const d = sel === 'from' ? from : to; if(d){ const dt=new Date(d); setVY(dt.getFullYear()); setVM(dt.getMonth()) } }, [sel, from, to])
  useEffect(() => { if(!open) return; const h=(e:MouseEvent)=>{ if(ref.current && !ref.current.contains(e.target as Node)) setOpen(false) }; document.addEventListener('mousedown',h); return ()=>document.removeEventListener('mousedown',h) }, [open])
  const prev = () => { if(vM===0){setVM(11);setVY(y=>y-1)} else setVM(m=>m-1) }
  const next = () => { if(vM===11){setVM(0);setVY(y=>y+1)} else setVM(m=>m+1) }
  const pick = (day: number) => {
    const ds = toDS(vY, vM, day)
    if(sel==='from'){ onChange(to && ds > to ? ds : ds, to && ds > to ? ds : to); setSel('to') }
    else { onChange(from && ds < from ? ds : from, from && ds < from ? ds : ds); setOpen(false); setSel('from') }
  }
  const dim = new Date(vY,vM+1,0).getDate()
  const fd = (()=>{ const d=new Date(vY,vM,1).getDay(); return d===0?6:d-1 })()
  const now = new Date(); const tds = toDS(now.getFullYear(),now.getMonth(),now.getDate())
  return (
    <div className="relative" ref={ref}>
      <div className="flex items-center gap-1.5">
        <CalendarDays className="w-4 h-4 text-[hsl(var(--muted-foreground)/0.5)]" />
        <button onClick={()=>{setSel('from');setOpen(true)}} className={`px-2.5 py-1.5 text-[13px] font-medium rounded-lg border transition-all ${open&&sel==='from'?'border-[hsl(var(--primary))] bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))]':'border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.15)] text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.3)]'}`}>{fmtD(from)}</button>
        <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.3)]">—</span>
        <button onClick={()=>{setSel('to');setOpen(true)}} className={`px-2.5 py-1.5 text-[13px] font-medium rounded-lg border transition-all ${open&&sel==='to'?'border-[hsl(var(--primary))] bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))]':'border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.15)] text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.3)]'}`}>{fmtD(to)}</button>
      </div>
      {open && (
        <div className="absolute top-full left-0 mt-2 z-[60] animate-in fade-in-0 zoom-in-95 duration-150">
          <div className="bg-[hsl(var(--popover))] border border-[hsl(var(--border))] rounded-xl shadow-2xl p-4 w-[280px]">
            <div className="flex items-center justify-between mb-3">
              <button onClick={prev} className="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] transition-colors"><ChevronLeft className="w-4 h-4"/></button>
              <span className="text-[14px] font-semibold text-[hsl(var(--foreground))]">{MONTHS_RU_CAL[vM]} {vY}</span>
              <button onClick={next} className="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] transition-colors"><ChevronRightIcon className="w-4 h-4"/></button>
            </div>
            <div className="text-center text-[11px] text-[hsl(var(--muted-foreground)/0.5)] mb-2">{sel==='from'?'Выберите начало периода':'Выберите конец периода'}</div>
            <div className="grid grid-cols-7 gap-0 mb-1">{DAYS_CAL.map(d=><div key={d} className="text-center text-[11px] font-medium text-[hsl(var(--muted-foreground)/0.5)] py-1">{d}</div>)}</div>
            <div className="grid grid-cols-7 gap-0">
              {Array.from({length:fd}).map((_,i)=><div key={`e${i}`} className="h-8"/>)}
              {Array.from({length:dim}).map((_,i)=>{
                const day=i+1, ds=toDS(vY,vM,day), inR=from&&to&&ds>=from&&ds<=to, isS=ds===from, isE=ds===to, isT=ds===tds, isF=ds>tds
                return <button key={day} onClick={()=>pick(day)} disabled={isF} className={`h-8 text-[13px] font-medium rounded-lg transition-all relative ${isF?'text-[hsl(var(--muted-foreground)/0.2)] cursor-not-allowed':'hover:bg-[hsl(var(--muted)/0.3)] cursor-pointer'} ${isS||isE?'bg-[hsl(var(--primary))] text-white hover:bg-[hsl(var(--primary)/0.9)]':''} ${inR&&!isS&&!isE?'bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))]':''} ${!inR&&!isS&&!isE&&!isF?'text-[hsl(var(--foreground))]':''}`}>{day}{isT&&!isS&&!isE&&<span className="absolute bottom-0.5 left-1/2 -translate-x-1/2 w-1 h-1 rounded-full bg-[hsl(var(--primary))]"/>}</button>
              })}
            </div>
            <div className="mt-3 pt-2 border-t border-[hsl(var(--border)/0.3)] flex items-center justify-between">
              <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)]">{fmtD(from)} — {fmtD(to)}</span>
              <button onClick={()=>setOpen(false)} className="text-[12px] font-medium px-2.5 py-1 rounded-md bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))] hover:bg-[hsl(var(--primary)/0.2)] transition-colors">Готово</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

/* ── Period options ──────────────────────────────────────── */
const PERIOD_OPTIONS = [
  { value: '7d', label: '7 дней', days: 7 },
  { value: '14d', label: '14 дней', days: 14 },
  { value: '30d', label: '30 дней', days: 30 },
  { value: '90d', label: '90 дней', days: 90 },
]

function computeDates(periodValue: string, customFrom?: string, customTo?: string) {
  if (periodValue === 'custom' && customFrom && customTo) {
    return { startDate: customFrom, endDate: customTo }
  }
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

type TabType = 'stats' | 'events' | 'purchases' | 'phrases' | 'heatmap' | 'bids'

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
  const scope = 'all' // Always show direct product data (scope toggle removed)
  const [customFrom, setCustomFrom] = useState('')
  const [customTo, setCustomTo] = useState('')

  // AI analysis state
  const [aiText, setAiText] = useState('')
  const [aiLoading, setAiLoading] = useState(false)
  const [showAiPanel, setShowAiPanel] = useState(false)
  const [aiController, setAiController] = useState<AbortController | null>(null)
  const [savedAiAnalysis, setSavedAiAnalysis] = useState<string | null>(null)
  const [savedAiDate, setSavedAiDate] = useState<string | null>(null)
  const [savedAiPeriod, setSavedAiPeriod] = useState<string | null>(null)

  // Load saved analysis from localStorage on mount
  const aiStorageKey = `ai_campaign_${marketplace}_${campaignId}`
  useEffect(() => {
    try {
      const saved = localStorage.getItem(aiStorageKey)
      if (saved) {
        const parsed = JSON.parse(saved)
        setSavedAiAnalysis(parsed.text)
        setSavedAiDate(parsed.date)
        setSavedAiPeriod(parsed.period)
        setAiText(parsed.text)
        setShowAiPanel(true)
      }
    } catch {}
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [aiStorageKey])

  // Visible chart metrics
  const [visibleMetrics, setVisibleMetrics] = useState<Set<string>>(new Set(['spend', 'direct_revenue', 'model_revenue', 'associated_revenue', 'orders', 'cart']))

  const { startDate, endDate } = computeDates(period, customFrom, customTo)

  // Data
  const [stats, setStats] = useState<CampaignStatsRow[]>([])
  const [events, setEvents] = useState<CampaignEventRow[]>([])
  const [phrases, setPhrases] = useState<CampaignPhraseRow[]>([])
  const [heatmap, setHeatmap] = useState<CampaignHeatmapRow[]>([])
  const [purchases, setPurchases] = useState<CampaignPurchaseRow[]>([])
  const [kpiData, setKpiData] = useState<CampaignKpiResponse | null>(null)
  const [loadedTabs, setLoadedTabs] = useState<Set<string>>(new Set())
  const [eventDayDetail, setEventDayDetail] = useState<string | null>(null)

  const loadTabData = useCallback(async (tab: TabType) => {
    const cacheKey = `${tab}_${startDate}_${endDate}_${selectedSku || 'all'}_${scope}`
    if (loadedTabs.has(cacheKey)) return
    setLoading(true)
    try {
      switch (tab) {
        case 'stats': {
          const [s, ev, kpi] = await Promise.all([
            getCampaignStats(marketplace, campaignId, startDate, endDate, selectedSku, scope),
            getCampaignEvents(marketplace, campaignId, selectedSku),
            getCampaignKpi(marketplace, campaignId, startDate, endDate, selectedSku, scope),
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
              getCampaignStats(marketplace, campaignId, startDate, endDate, selectedSku, scope).then(s => setStats(s))
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
          const d = await getCampaignPurchases(marketplace, campaignId, startDate, endDate, scope)
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
    { id: 'bids', label: 'Ставки', icon: Crosshair },
    { id: 'phrases', label: 'Фразы', icon: Search },
    { id: 'heatmap', label: 'Активность', icon: Flame },
  ]

  // First stat date = approximate campaign creation date
  const firstStatDate = kpiData?.first_date || (stats.length > 0 ? [...stats].sort((a, b) => a.dt.localeCompare(b.dt))[0].dt : null)

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

    // Enrich chart data with event counts + inject empty rows for event-only dates
    const statsDates = new Set(stats.map(s => s.dt))
    const extraEventDates = Object.keys(eventsByDate).filter(d =>
      !statsDates.has(d) && d >= startDate && d <= endDate
    )
    const chartData = [
      ...stats.map(s => ({
        ...s,
        eventCount: (eventsByDate[s.dt] || []).length,
      })),
      ...extraEventDates.map(d => ({
        dt: d, views: 0, clicks: 0, orders: 0, cart: 0,
        revenue: 0, spend: 0, ctr: 0, drr: 0,
        product_revenue: 0, direct_revenue: 0, model_revenue: 0, associated_revenue: 0,
        eventCount: (eventsByDate[d] || []).length,
      })),
    ].sort((a, b) => a.dt.localeCompare(b.dt))

    // Dates where events occurred for reference lines
    const eventDates = Object.keys(eventsByDate).filter(d =>
      d >= startDate && d <= endDate
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
              {dayEvents.slice(0, 4).map((ev, i) => {
                const evLabel = EVENT_LABELS[ev.event_type] || ev.event_type
                const style = EVENT_STYLE[ev.event_type] || DEFAULT_EV_STYLE
                // Compact teaser: show numeric change for bids/prices, skip hashes for photos
                const PHOTO_EVENTS = ['CONTENT_MAIN_PHOTO_CHANGED', 'CONTENT_PHOTO_ORDER_CHANGED', 'CONTENT_PHOTO_ADDED', 'CONTENT_PHOTO_REMOVED', 'OZON_PHOTO_CHANGE']
                let detail = ''
                if (!PHOTO_EVENTS.includes(ev.event_type) && ev.old_value && ev.new_value) {
                  let ov = ev.old_value, nv = ev.new_value
                  if (ev.event_type === 'BID_CHANGE') { ov = String(Number(ov) / 100); nv = String(Number(nv) / 100) }
                  // Only show short details for numeric events
                  if (NUMERIC_EVENTS.has(ev.event_type)) detail = ` ${ov} → ${nv}`
                }
                return (
                  <div key={i} className="flex items-center gap-1.5 py-0.5 text-[11px]">
                    <span className="w-1.5 h-1.5 rounded-full shrink-0" style={{ backgroundColor: style.color }} />
                    <span style={{ color: style.color }} className="font-medium">{evLabel}</span>
                    {detail && <span className="text-[hsl(var(--muted-foreground))]">{detail}</span>}
                  </div>
                )
              })}
              {dayEvents.length > 4 && (
                <div className="text-[10px] text-[hsl(var(--muted-foreground))] mt-0.5">...ещё {dayEvents.length - 4}</div>
              )}
              <div className="text-[10px] text-[hsl(var(--muted-foreground)/0.6)] mt-1 italic">Кликните для подробностей</div>
            </div>
          )}
        </div>
      )
    }

    // Determine which axes are active
    const moneyActive = visibleMetrics.has('spend') || visibleMetrics.has('direct_revenue') || visibleMetrics.has('model_revenue') || visibleMetrics.has('associated_revenue') || visibleMetrics.has('product_revenue')
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
        {/* ═══ БЛОК 1: Вся кампания ═══ */}
        {cur && (() => {
          const totalOrders = cur.direct_orders + cur.model_orders + cur.associated_orders
          const totalRevenue = cur.direct_revenue + cur.model_revenue + cur.associated_revenue
          const spend = cur.spend
          const views = cur.views
          const clicks = cur.clicks
          const cpm = views > 0 ? (spend / views * 1000) : 0
          const cpc = clicks > 0 ? (spend / clicks) : 0
          const cr = clicks > 0 ? (totalOrders / clicks * 100) : 0
          const totalDrr = totalRevenue > 0 ? (spend / totalRevenue * 100) : 0
          const totalCpo = totalOrders > 0 ? (spend / totalOrders) : 0
          return (
            <div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-3">
              <div className="text-[14px] text-[hsl(var(--muted-foreground))] font-semibold mb-2">Вся кампания</div>
              <div className="grid grid-cols-4 gap-3">
                {/* Показы */}
                <div>
                  <div className="text-lg font-bold text-[hsl(var(--foreground))]">{formatNumber(views)}</div>
                  <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Показы</div>
                  {viewsD !== null && <span className={`text-[13px] font-medium ${deltaColor(viewsD)}`}>{deltaStr(viewsD)}</span>}
                  <div className="mt-1 text-[13px] text-[hsl(var(--muted-foreground))]">
                    CPM <span className="text-[hsl(var(--foreground))] font-medium">{formatMoney(cpm)}</span>
                  </div>
                </div>
                {/* Клики */}
                <div>
                  <div className="text-lg font-bold text-[hsl(var(--foreground))]">{formatNumber(clicks)}</div>
                  <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Клики</div>
                  {clicksD !== null && <span className={`text-[13px] font-medium ${deltaColor(clicksD)}`}>{deltaStr(clicksD)}</span>}
                  <div className="mt-1 text-[13px] text-[hsl(var(--muted-foreground))]">
                    CTR <span className="text-[hsl(var(--foreground))] font-medium">{cur.ctr.toFixed(2)}%</span>
                    {' · '}CPC <span className="text-[hsl(var(--foreground))] font-medium">{formatMoney(cpc)}</span>
                  </div>
                </div>
                {/* Заказы */}
                <div>
                  <div className="text-lg font-bold text-purple-400">{totalOrders}</div>
                  <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Заказы</div>
                  <div className="mt-1 text-[13px] text-[hsl(var(--muted-foreground))]">
                    CR <span className="text-[hsl(var(--foreground))] font-medium">{cr.toFixed(1)}%</span>
                    {' · '}CPO <span className="text-[hsl(var(--foreground))] font-medium">{formatMoney(totalCpo)}</span>
                  </div>
                </div>
                {/* Расход + Выручка + ДРР */}
                <div>
                  <div className="text-lg font-bold text-red-400">{totalDrr.toFixed(1)}%</div>
                  <div className="text-[13px] text-[hsl(var(--muted-foreground))]">ДРР</div>
                  <div className="mt-1 text-[13px] text-[hsl(var(--muted-foreground))]">
                    Выр. <span className="text-emerald-400 font-medium">{formatMoney(totalRevenue)}</span>
                  </div>
                  <div className="text-[13px] text-[hsl(var(--muted-foreground))]">
                    Расх. <span className="text-red-400 font-medium">{formatMoney(spend)}</span>
                    {spendD !== null && <span className={`ml-1 text-[13px] ${deltaColor(spendD, true)}`}>{deltaStr(spendD)}</span>}
                  </div>
                </div>
              </div>
            </div>
          )
        })()}

        {/* ═══ БЛОК 2: Прямой товар ═══ */}
        {cur && (() => {
          const totalRev = cur.direct_revenue + cur.model_revenue + cur.associated_revenue
          const directPct = totalRev > 0 ? (cur.direct_revenue / totalRev * 100) : 100
          const modelPct = totalRev > 0 ? (cur.model_revenue / totalRev * 100) : 0
          const assocPct = totalRev > 0 ? (cur.associated_revenue / totalRev * 100) : 0
          return (<>
        <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/5 p-3">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-[14px] text-emerald-400 font-semibold">Прямой товар</span>
            <span className="text-xl font-bold text-emerald-400">{directPct.toFixed(0)}%</span>
            <span className="text-[13px] text-[hsl(var(--muted-foreground))]">от выручки</span>
          </div>
          <div className="grid grid-cols-3 sm:grid-cols-6 gap-2">
            <div>
              <div className="text-lg font-bold text-emerald-400">{formatMoney(cur.ad_revenue)}</div>
              <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Выр. рекл.</div>
              {adRevD !== null && <span className={`text-[13px] font-medium ${deltaColor(adRevD)}`}>{deltaStr(adRevD)}</span>}
            </div>
            <div>
              <div className="text-lg font-bold text-teal-400">{formatMoney(cur.product_revenue)}</div>
              <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Выр. общая</div>
              {prodRevD !== null && <span className={`text-[13px] font-medium ${deltaColor(prodRevD)}`}>{deltaStr(prodRevD)}</span>}
            </div>
            <div>
              <div className="text-lg font-bold text-purple-400">{cur.orders} <span className="text-[hsl(var(--muted-foreground))] text-[14px] font-normal">/ {cur.cart}</span></div>
              <div className="text-[13px] text-[hsl(var(--muted-foreground))]">Заказы / Корз.</div>
              {ordersD !== null && <span className={`text-[13px] font-medium ${deltaColor(ordersD)}`}>{deltaStr(ordersD)}</span>}
            </div>
            <div>
              <div className="text-lg font-bold text-pink-400">{(cur.drr_ad || 0).toFixed(1)}%</div>
              <div className="text-[13px] text-[hsl(var(--muted-foreground))]">ДРР рекл.</div>
              {drrAdD !== null && <span className={`text-[13px] font-medium ${deltaColor(drrAdD, true)}`}>{drrAdD > 0 ? '+' : ''}{drrAdD.toFixed(1)} п.п.</span>}
            </div>
            <div>
              <div className="text-lg font-bold text-amber-400">{(cur.drr_product || 0).toFixed(1)}%</div>
              <div className="text-[13px] text-[hsl(var(--muted-foreground))]">ДРР общий</div>
              {drrProdD !== null && <span className={`text-[13px] font-medium ${deltaColor(drrProdD, true)}`}>{drrProdD > 0 ? '+' : ''}{drrProdD.toFixed(1)} п.п.</span>}
            </div>
            <div>
              <div className="text-lg font-bold text-purple-400">{cur.cpo > 0 ? formatMoney(cur.cpo) : '—'}</div>
              <div className="text-[13px] text-[hsl(var(--muted-foreground))]">CPO</div>
              {cpoD !== null && <span className={`text-[13px] font-medium ${deltaColor(cpoD, true)}`}>{deltaStr(cpoD)}</span>}
            </div>
          </div>
        </div>

        {/* ═══ БЛОК 3: Кросс-продажи ═══ */}
        {(cur.model_orders > 0 || cur.associated_orders > 0) && (
          <div className="grid grid-cols-2 gap-2">
            <div className="rounded-lg border border-blue-500/20 bg-blue-500/5 p-2.5">
              <div className="flex items-center gap-2">
                <span className="text-[14px] text-blue-400 font-semibold">Модель</span>
                <span className="text-xl font-bold text-blue-300">{modelPct.toFixed(0)}%</span>
              </div>
              <div className="flex items-baseline gap-2 mt-0.5">
                <span className="text-[15px] font-bold text-blue-300">{cur.model_orders} шт.</span>
                <span className="text-[14px] text-[hsl(var(--muted-foreground))]">{formatMoney(cur.model_revenue)}</span>
              </div>
            </div>
            <div className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-2.5">
              <div className="flex items-center gap-2">
                <span className="text-[14px] text-amber-400 font-semibold">Ассоц. конверсии</span>
                <span className="text-xl font-bold text-amber-300">{assocPct.toFixed(0)}%</span>
              </div>
              <div className="flex items-baseline gap-2 mt-0.5">
                <span className="text-[15px] font-bold text-amber-300">{cur.associated_orders} шт.</span>
                <span className="text-[14px] text-[hsl(var(--muted-foreground))]">{formatMoney(cur.associated_revenue)}</span>
              </div>
            </div>
          </div>
        )}
        </>)
        })()}

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
            <ComposedChart data={chartData} margin={{ top: 5, right: 5, left: -10, bottom: 0 }} onClick={(data: any) => {
              if (data?.activeLabel) {
                const clickedDate = data.activeLabel as string
                if (eventsByDate[clickedDate]?.length) setEventDayDetail(clickedDate)
              }
            }}>
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
              {visibleMetrics.has('product_revenue') && (
                <Area yAxisId="money" type="monotone" dataKey="product_revenue" stroke="#06b6d4" fill="url(#gProdRevM)" strokeWidth={2} name="product_revenue" />
              )}

              {/* Stacked revenue bars — direct (green) + model (blue) + associated (amber) */}
              {visibleMetrics.has('direct_revenue') && (
                <Bar yAxisId="money" dataKey="direct_revenue" fill="#10b981" fillOpacity={0.85} barSize={16} stackId="revBreakdown" name="direct_revenue" radius={[0, 0, 0, 0]} />
              )}
              {visibleMetrics.has('model_revenue') && (
                <Bar yAxisId="money" dataKey="model_revenue" fill="#3b82f6" fillOpacity={0.85} barSize={16} stackId="revBreakdown" name="model_revenue" radius={[0, 0, 0, 0]} />
              )}
              {visibleMetrics.has('associated_revenue') && (
                <Bar yAxisId="money" dataKey="associated_revenue" fill="#f59e0b" fillOpacity={0.85} barSize={16} stackId="revBreakdown" name="associated_revenue" radius={[3, 3, 0, 0]} />
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

                        {/* Photo change preview */}
                        {['CONTENT_MAIN_PHOTO_CHANGED', 'CONTENT_PHOTO_ORDER_CHANGED', 'CONTENT_PHOTO_ADDED', 'CONTENT_PHOTO_REMOVED', 'OZON_PHOTO_CHANGE'].includes(ev.event_type) && (() => {
                          const imgUrl = (ev.event_metadata?.main_image_url as string) || ''
                          const oldCount = (ev.event_metadata?.old_count as number) ?? null
                          const newCount = (ev.event_metadata?.new_count as number) ?? null
                          return (
                            <div className="flex items-center gap-3 mt-2">
                              {imgUrl ? (
                                <img
                                  src={imgUrl}
                                  alt="Фото товара"
                                  className="h-16 w-16 rounded-lg object-cover border border-[hsl(var(--border))] shadow-sm"
                                  onError={(e) => { (e.target as HTMLImageElement).style.display = 'none' }}
                                />
                              ) : (
                                <div className="h-16 w-16 rounded-lg bg-[hsl(var(--muted)/0.3)] border border-[hsl(var(--border))] flex items-center justify-center">
                                  <Image className="h-6 w-6 text-[hsl(var(--muted-foreground)/0.4)]" />
                                </div>
                              )}
                              <div className="text-[12px] text-[hsl(var(--muted-foreground))]">
                                {ev.event_type === 'CONTENT_MAIN_PHOTO_CHANGED' && (
                                  <span>Главное фото заменено</span>
                                )}
                                {ev.event_type === 'CONTENT_PHOTO_ADDED' && oldCount !== null && newCount !== null && (
                                  <span>Добавлено фото: {oldCount} → {newCount} шт.</span>
                                )}
                                {ev.event_type === 'CONTENT_PHOTO_REMOVED' && oldCount !== null && newCount !== null && (
                                  <span>Удалено фото: {oldCount} → {newCount} шт.</span>
                                )}
                                {ev.event_type === 'CONTENT_PHOTO_ORDER_CHANGED' && (
                                  <span>Порядок/состав фото изменён ({newCount ?? '?'} шт.)</span>
                                )}
                                {ev.event_type === 'OZON_PHOTO_CHANGE' && (
                                  <span>Фото обновлено</span>
                                )}
                              </div>
                            </div>
                          )
                        })()}

                        {/* Non-numeric, non-photo: show old → new */}
                        {!isNumeric && !['CONTENT_MAIN_PHOTO_CHANGED', 'CONTENT_PHOTO_ORDER_CHANGED', 'CONTENT_PHOTO_ADDED', 'CONTENT_PHOTO_REMOVED', 'OZON_PHOTO_CHANGE'].includes(ev.event_type) && (ev.old_value || ev.new_value) && (
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
    
    // Summary by sale_type
    const directItems = purchases.filter(p => p.sale_type === 'direct')
    const modelItems = purchases.filter(p => p.sale_type === 'model')
    const assocItems = purchases.filter(p => p.sale_type === 'associated')
    const directRev = directItems.reduce((a, c) => a + c.revenue, 0)
    const modelRev = modelItems.reduce((a, c) => a + c.revenue, 0)
    const assocRev = assocItems.reduce((a, c) => a + c.revenue, 0)
    const directQty = directItems.reduce((a, c) => a + c.quantity, 0)
    const modelQty = modelItems.reduce((a, c) => a + c.quantity, 0)
    const assocQty = assocItems.reduce((a, c) => a + c.quantity, 0)
    
    const saleTypeBadge = (type: string) => {
      const cfg = {
        direct: { label: 'Прямые', bg: 'bg-emerald-500/15', text: 'text-emerald-400', border: 'border-emerald-500/30' },
        model: { label: 'Модель', bg: 'bg-blue-500/15', text: 'text-blue-400', border: 'border-blue-500/30' },
        associated: { label: 'Ассоц.', bg: 'bg-amber-500/15', text: 'text-amber-400', border: 'border-amber-500/30' },
      }[type] || { label: type, bg: 'bg-gray-500/15', text: 'text-gray-400', border: 'border-gray-500/30' }
      return <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium ${cfg.bg} ${cfg.text} border ${cfg.border}`}>{cfg.label}</span>
    }
    
    const hasMultipleTypes = marketplace.toLowerCase() !== 'ozon' && (modelItems.length > 0 || assocItems.length > 0)
    
    return (
      <div className="space-y-4">
        <div className="flex items-center gap-4 text-[13px] text-[hsl(var(--muted-foreground))] flex-wrap">
          <span>Товаров: <strong className="text-[hsl(var(--foreground))]">{purchases.length}</strong></span>
          <span>Заказано: <strong className="text-[hsl(var(--foreground))]">{totalQty} шт.</strong></span>
          <span>Выручка: <strong className="text-emerald-400">{formatMoney(totalRev)}</strong></span>
        </div>
        {/* Breakdown by sale type (WB only, when there are cross-sales) */}
        {hasMultipleTypes && (
          <div className="flex items-center gap-3 text-[12px] flex-wrap">
            {directItems.length > 0 && (
              <span className="flex items-center gap-1.5 text-emerald-400">
                {saleTypeBadge('direct')}
                <span className="text-[hsl(var(--muted-foreground))]">{directQty} шт. • {formatMoney(directRev)}</span>
              </span>
            )}
            {modelItems.length > 0 && (
              <span className="flex items-center gap-1.5 text-blue-400">
                {saleTypeBadge('model')}
                <span className="text-[hsl(var(--muted-foreground))]">{modelQty} шт. • {formatMoney(modelRev)}</span>
              </span>
            )}
            {assocItems.length > 0 && (
              <span className="flex items-center gap-1.5 text-amber-400">
                {saleTypeBadge('associated')}
                <span className="text-[hsl(var(--muted-foreground))]">{assocQty} шт. • {formatMoney(assocRev)}</span>
              </span>
            )}
          </div>
        )}
        <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden max-h-[380px] overflow-y-auto">
          <table className="w-full text-left">
            <thead className="sticky top-0 bg-[hsl(var(--card))] z-10 shadow-[0_1px_0_hsl(var(--border))]">
              <tr>
                <th className="px-4 py-2.5 text-[11px] font-medium text-[hsl(var(--muted-foreground))] uppercase">Товар</th>
                {hasMultipleTypes && <th className="px-3 py-2.5 text-[11px] font-medium text-[hsl(var(--muted-foreground))] uppercase">Тип</th>}
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
                  {hasMultipleTypes && <td className="px-3 py-2.5">{saleTypeBadge(p.sale_type)}</td>}
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
     Bids Tab — current bids per SKU
     ════════════════════════════════════════════════════════════ */
  const renderBids = () => {
    if (items.length === 0) return <Empty text="Нет товаров в кампании" />
    const bidItems = [...items].sort((a, b) => b.bid - a.bid)
    const maxBid = Math.max(...bidItems.map(i => i.bid), 1)
    const isWB = marketplace === 'wb'
    return (
      <div className="space-y-3">
        <div className="text-[13px] text-[hsl(var(--muted-foreground))]">
          Текущие ставки по товарам в кампании {isWB && <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)]">(WB: копейки → рубли)</span>}
        </div>
        <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden max-h-[420px] overflow-y-auto">
          <table className="w-full text-left">
            <thead className="sticky top-0 bg-[hsl(var(--card))] z-10 shadow-[0_1px_0_hsl(var(--border))]">
              <tr>
                <th className="px-4 py-2.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))]">Товар</th>
                <th className="px-4 py-2.5 text-[13px] font-semibold text-[hsl(var(--foreground))] text-right">Ставка</th>
                <th className="px-4 py-2.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))] text-right">Расход</th>
                <th className="px-4 py-2.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))] text-right">Показы</th>
                <th className="px-4 py-2.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))] text-right">Клики</th>
                <th className="px-4 py-2.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))] text-right">CPC</th>
                <th className="px-4 py-2.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))] text-right">Заказы</th>
                <th className="px-4 py-2.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))] text-right">ДРР</th>
              </tr>
            </thead>
            <tbody>
              {bidItems.map(item => {
                const bidRub = isWB ? item.bid / 100 : item.bid
                const barW = maxBid > 0 ? (item.bid / maxBid) * 100 : 0
                return (
                  <tr key={item.sku} className="border-b border-[hsl(var(--border)/0.3)] last:border-0 hover:bg-[hsl(var(--muted)/0.1)]">
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        {item.image_url ? (
                          <img src={item.image_url} alt="" className="h-8 w-8 rounded object-cover shrink-0" />
                        ) : (
                          <div className="h-8 w-8 rounded bg-[hsl(var(--muted)/0.3)] shrink-0" />
                        )}
                        <div className="min-w-0">
                          <div className="text-[13px] font-medium truncate max-w-[180px]">{item.offer_id || item.name || `SKU ${item.sku}`}</div>
                          <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">{item.name && item.offer_id ? item.name.slice(0, 40) : `SKU: ${item.sku}`}</div>
                        </div>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-right">
                      <div className="inline-flex flex-col items-end">
                        <span className="text-[16px] font-bold text-[hsl(var(--foreground))]">{bidRub.toFixed(0)} ₽</span>
                        <div className="w-[60px] h-[4px] rounded-full bg-[hsl(var(--muted)/0.2)] mt-1">
                          <div className="h-full rounded-full bg-[hsl(var(--primary))]" style={{ width: `${barW}%` }} />
                        </div>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-[13px] text-right font-medium text-red-400">{formatMoney(item.spend)}</td>
                    <td className="px-4 py-3 text-[13px] text-right">{formatNumber(item.views)}</td>
                    <td className="px-4 py-3 text-[13px] text-right">{formatNumber(item.clicks)}</td>
                    <td className="px-4 py-3 text-[13px] text-right">{item.avg_cpc > 0 ? formatMoney(item.avg_cpc) : '—'}</td>
                    <td className="px-4 py-3 text-[13px] text-right font-medium">{item.orders}</td>
                    <td className="px-4 py-3 text-[13px] text-right">
                      <span className={item.drr > 30 ? 'text-red-400 font-semibold' : item.drr > 0 ? 'text-amber-400' : ''}>
                        {item.drr > 0 ? `${item.drr.toFixed(0)}%` : '—'}
                      </span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
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
              {firstStatDate && <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)]">· с {(() => { try { return format(parseISO(firstStatDate), 'dd MMM yyyy', { locale: ru }) } catch { return firstStatDate } })()}</span>}
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
          {/* Custom date range */}
          <ModalDatePicker
            from={startDate}
            to={endDate}
            onChange={(f, t) => { setCustomFrom(f); setCustomTo(t); setPeriod('custom') }}
          />
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
          {/* Scope toggle removed — KPI always shows direct product data, breakdown in cards */}
          <button
            onClick={async () => {
              if (aiLoading && aiController) { aiController.abort(); setAiLoading(false); setShowAiPanel(false); return }
              // Toggle: if panel is visible, hide it
              if (showAiPanel) { setShowAiPanel(false); return }
              // If we have saved text, just show it
              if (aiText) { setShowAiPanel(true); return }
              // Otherwise start new analysis
              setShowAiPanel(true); setAiText(''); setAiLoading(true)
              const ctrl = await streamCampaignAiAnalysis(
                { marketplace, campaignId, startDate, endDate, sku: selectedSku, previousAnalysis: savedAiAnalysis || undefined },
                (chunk) => setAiText(prev => prev + chunk),
                () => {
                  setAiLoading(false)
                  // Save to localStorage
                  setAiText(finalText => {
                    const saveData = {
                      text: finalText,
                      date: new Date().toISOString(),
                      period: `${startDate} — ${endDate}`
                    }
                    try { localStorage.setItem(aiStorageKey, JSON.stringify(saveData)) } catch {}
                    setSavedAiDate(saveData.date)
                    setSavedAiPeriod(saveData.period)
                    setSavedAiAnalysis(finalText)
                    return finalText
                  })
                },
                (err) => { setAiText(prev => prev + `\n\n❌ Ошибка: ${err}`); setAiLoading(false) },
              )
              setAiController(ctrl)
            }}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-[13px] font-medium rounded-lg border transition-all ${
              aiLoading
                ? 'border-[hsl(var(--primary))] bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))] animate-pulse'
                : 'border-[hsl(var(--border))] bg-gradient-to-r from-[hsl(var(--primary)/0.08)] to-[hsl(var(--primary)/0.02)] text-[hsl(var(--primary))] hover:from-[hsl(var(--primary)/0.15)] hover:to-[hsl(var(--primary)/0.08)]'
            }`}
          >
            <Sparkles className="w-4 h-4" />
            {aiLoading ? 'Остановить' : showAiPanel ? 'Скрыть анализ' : aiText ? 'Показать анализ' : 'ИИ-анализ'}
          </button>
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

        {/* AI Analysis Panel */}
        {showAiPanel && (
          <div className="border-b border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.05)]">
            <div className="px-6 py-3 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Sparkles className="w-4 h-4 text-[hsl(var(--primary))]" />
                <span className="text-[14px] font-semibold text-[hsl(var(--foreground))]">ИИ-анализ кампании</span>
                {aiLoading && <Loader2 className="w-4 h-4 animate-spin text-[hsl(var(--primary))]" />}
                {savedAiDate && !aiLoading && (
                  <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">
                    {(() => { try { return `${new Date(savedAiDate).toLocaleDateString('ru-RU')} ${new Date(savedAiDate).toLocaleTimeString('ru-RU', {hour:'2-digit',minute:'2-digit'})}` } catch { return '' } })()}
                    {savedAiPeriod && ` · период: ${savedAiPeriod}`}
                  </span>
                )}
              </div>
              <div className="flex items-center gap-3">
                {!aiLoading && aiText && (
                  <button
                    onClick={async () => {
                      setAiText(''); setAiLoading(true)
                      const ctrl = await streamCampaignAiAnalysis(
                        { marketplace, campaignId, startDate, endDate, sku: selectedSku, previousAnalysis: savedAiAnalysis || undefined },
                        (chunk) => setAiText(prev => prev + chunk),
                        () => {
                          setAiLoading(false)
                          setAiText(finalText => {
                            const saveData = { text: finalText, date: new Date().toISOString(), period: `${startDate} — ${endDate}` }
                            try { localStorage.setItem(aiStorageKey, JSON.stringify(saveData)) } catch {}
                            setSavedAiDate(saveData.date); setSavedAiPeriod(saveData.period); setSavedAiAnalysis(finalText)
                            return finalText
                          })
                        },
                        (err) => { setAiText(prev => prev + `\n\n❌ Ошибка: ${err}`); setAiLoading(false) },
                      )
                      setAiController(ctrl)
                    }}
                    className="text-[12px] text-[hsl(var(--primary))] hover:text-[hsl(var(--primary)/0.7)] transition-colors font-medium"
                  >🔄 Обновить</button>
                )}
                <button onClick={() => { setShowAiPanel(false) }} className="text-[12px] text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors">Скрыть</button>
              </div>
            </div>
            <div className="px-6 pb-4 max-h-[50vh] overflow-y-auto">
              {aiLoading ? (
                <div className="space-y-3">
                  <div className="flex items-center gap-3 text-[14px] text-[hsl(var(--primary))]">
                    <Loader2 className="w-5 h-5 animate-spin" />
                    <span className="font-medium">Анализирую кампанию...</span>
                  </div>
                  <div className="space-y-2">
                    {['Сбор данных...', 'Юнит-экономика...', 'Конверсия и Price Index...', 'Формирование стратегии...'].map((step, i) => (
                      <div key={i} className="flex items-center gap-2 text-[12px] text-[hsl(var(--muted-foreground)/0.5)]">
                        <div className="w-1.5 h-1.5 rounded-full bg-[hsl(var(--primary)/0.3)] animate-pulse" style={{animationDelay: `${i * 300}ms`}} />
                        {step}
                      </div>
                    ))}
                  </div>
                </div>
              ) : aiText ? (() => {
                // Try to parse JSON sections
                let sections: Array<{id: string; title: string; content: string; type: string; status?: string; actions?: Array<{action: string; value: string; priority: string}>}> | null = null
                try {
                  let jsonStr = aiText.trim()
                  const jsonMatch = jsonStr.match(/```json\s*([\s\S]*?)\s*```/)
                  if (jsonMatch) jsonStr = jsonMatch[1]
                  const arrStart = jsonStr.indexOf('[')
                  const arrEnd = jsonStr.lastIndexOf(']')
                  if (arrStart !== -1 && arrEnd !== -1) {
                    jsonStr = jsonStr.substring(arrStart, arrEnd + 1)
                  }
                  const parsed = JSON.parse(jsonStr)
                  if (Array.isArray(parsed) && parsed.length > 0 && parsed[0].id) {
                    sections = parsed
                  }
                } catch {}

                if (sections) {
                  return (
                    <div className="space-y-3">
                      {sections.map((sec) => {
                        const statusColors: Record<string, string> = {
                          negative: 'border-l-red-500 bg-red-500/5',
                          positive: 'border-l-green-500 bg-green-500/5',
                          neutral: 'border-l-yellow-500 bg-yellow-500/5',
                        }
                        const cardClass = sec.status && statusColors[sec.status]
                          ? `border-l-4 ${statusColors[sec.status]}`
                          : sec.type === 'strategy' ? 'border-l-4 border-l-blue-500 bg-blue-500/5'
                          : sec.type === 'verdict' ? 'border-l-4 border-l-purple-500 bg-purple-500/5'
                          : ''

                        return (
                          <div key={sec.id} className={`rounded-xl border border-[hsl(var(--border))] p-4 ${cardClass}`}>
                            <h3 className="text-[15px] font-bold mb-2 text-[hsl(var(--foreground))]">{sec.title}</h3>
                            <div
                              className="text-[13px] leading-relaxed text-[hsl(var(--foreground)/0.85)]"
                              dangerouslySetInnerHTML={{ __html: (() => {
                                // First, parse markdown tables into HTML tables
                                const lines = sec.content.split('\n')
                                const result: string[] = []
                                let i = 0
                                while (i < lines.length) {
                                  // Detect table: line with | and next line is separator (---|---)
                                  if (lines[i]?.includes('|') && i + 1 < lines.length && /^\|?[\s-:|]+\|/.test(lines[i + 1])) {
                                    // Parse table
                                    const headerCells = lines[i].split('|').map(c => c.trim()).filter(Boolean)
                                    i += 2 // skip header + separator
                                    const rows: string[][] = []
                                    while (i < lines.length && lines[i]?.includes('|')) {
                                      const cells = lines[i].split('|').map(c => c.trim()).filter(Boolean)
                                      if (cells.length > 0) rows.push(cells)
                                      i++
                                    }
                                    // Build HTML table
                                    let table = '<table style="width:100%;border-collapse:collapse;margin:8px 0;font-size:13px">'
                                    table += '<thead><tr>'
                                    headerCells.forEach(h => {
                                      const escaped = h.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
                                        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
                                      table += `<th style="text-align:left;padding:6px 10px;border-bottom:2px solid hsl(var(--border));font-weight:600;white-space:nowrap">${escaped}</th>`
                                    })
                                    table += '</tr></thead><tbody>'
                                    rows.forEach((row, ri) => {
                                      const bgColor = ri % 2 === 0 ? 'transparent' : 'hsl(var(--muted)/0.15)'
                                      table += `<tr style="background:${bgColor}">`
                                      row.forEach((cell, ci) => {
                                        const escaped = cell.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
                                          .replace(/\*\*(.+?)\*\*/g, '<strong style="color:hsl(var(--foreground))">$1</strong>')
                                        const isValue = ci > 0
                                        const align = isValue ? 'right' : 'left'
                                        table += `<td style="text-align:${align};padding:5px 10px;border-bottom:1px solid hsl(var(--border)/0.3);white-space:nowrap">${escaped}</td>`
                                      })
                                      table += '</tr>'
                                    })
                                    table += '</tbody></table>'
                                    result.push(table)
                                  } else {
                                    // Regular line — apply standard formatting
                                    let line = lines[i]
                                      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
                                      .replace(/\*\*(.+?)\*\*/g, '<strong style="color:hsl(var(--foreground))">$1</strong>')
                                    if (/^- /.test(line)) {
                                      line = `<div style="margin-left:12px;margin-bottom:2px">• ${line.slice(2)}</div>`
                                    } else {
                                      line += '<br/>'
                                    }
                                    result.push(line)
                                    i++
                                  }
                                }
                                return result.join('')
                              })() }}
                            />
                            {sec.actions && sec.actions.length > 0 && (
                              <div className="mt-3 space-y-1.5">
                                {sec.actions.map((a, i) => (
                                  <div key={i} className={`flex items-center gap-2 text-[12px] px-3 py-2 rounded-lg ${
                                    a.priority === 'high' ? 'bg-red-100 text-gray-800' :
                                    a.priority === 'medium' ? 'bg-amber-100 text-gray-800' :
                                    'bg-blue-100 text-gray-800'
                                  }`}>
                                    <span className={`w-2 h-2 rounded-full flex-shrink-0 ${
                                      a.priority === 'high' ? 'bg-red-500' : a.priority === 'medium' ? 'bg-amber-500' : 'bg-blue-500'
                                    }`} />
                                    <span className="font-semibold text-gray-900">{a.action}:</span>
                                    <span>{a.value}</span>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  )
                }

                // Fallback: render as markdown if JSON parse failed
                return (
                  <div
                    className="text-[13px] leading-relaxed text-[hsl(var(--foreground)/0.85)] whitespace-pre-wrap [&_h2]:text-[16px] [&_h2]:font-bold [&_h2]:mt-4 [&_h2]:mb-2"
                    dangerouslySetInnerHTML={{ __html: aiText
                      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
                      .replace(/^## (.+)$/gm, '<h2 class="text-[16px] font-bold mt-5 mb-2 text-[hsl(var(--foreground))]">$1</h2>')
                      .replace(/^### (.+)$/gm, '<h3 class="text-[14px] font-semibold mt-3 mb-1 text-[hsl(var(--foreground))]">$1</h3>')
                      .replace(/\*\*(.+?)\*\*/g, '<strong class="text-[hsl(var(--foreground))]">$1</strong>')
                      .replace(/^- (.+)$/gm, '<div class="ml-4 mb-0.5">• $1</div>')
                      .replace(/\n/g, '<br/>')
                    }}
                  />
                )
              })() : (
                <div className="text-[13px] text-[hsl(var(--muted-foreground)/0.5)] italic">Нажмите «ИИ-анализ» для запуска</div>
              )}
            </div>
          </div>
        )}

        {/* Content */}
        <div className="p-6 overflow-y-auto flex-1">
          {activeTab === 'stats' && renderStats()}
          {activeTab === 'events' && renderEvents()}
          {activeTab === 'purchases' && renderPurchases()}
          {activeTab === 'phrases' && renderPhrases()}
          {activeTab === 'heatmap' && renderHeatmap()}
          {activeTab === 'bids' && renderBids()}
        </div>
      </div>

      {/* ── Event Day Detail Popup ── */}
      {eventDayDetail && (() => {
        const dayEvts = eventsByDate[eventDayDetail] || []
        let dateLabel = eventDayDetail
        try { dateLabel = format(parseISO(eventDayDetail), 'dd MMMM yyyy', { locale: ru }) } catch {}
        return (
          <div className="fixed inset-0 z-[200] flex items-center justify-center bg-black/50 backdrop-blur-sm p-4" onClick={(e) => { e.stopPropagation(); setEventDayDetail(null) }}>
            <div className="w-full max-w-[520px] max-h-[80vh] bg-[hsl(var(--background))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl flex flex-col animate-in fade-in zoom-in-95 duration-200" onClick={e => e.stopPropagation()}>
              {/* Header */}
              <div className="flex items-center justify-between px-5 py-4 border-b border-[hsl(var(--border))]">
                <div>
                  <h3 className="text-[16px] font-bold text-[hsl(var(--foreground))]">📌 События за {dateLabel}</h3>
                  <p className="text-[12px] text-[hsl(var(--muted-foreground))] mt-0.5">{dayEvts.length} {dayEvts.length === 1 ? 'событие' : dayEvts.length < 5 ? 'события' : 'событий'}</p>
                </div>
                <button onClick={() => setEventDayDetail(null)} className="p-2 rounded-lg hover:bg-[hsl(var(--muted))] text-[hsl(var(--muted-foreground))] transition-colors"><X className="w-5 h-5" /></button>
              </div>
              {/* Events list */}
              <div className="p-5 overflow-y-auto flex-1 space-y-2.5">
                {dayEvts.map(ev => {
                  const style = EVENT_STYLE[ev.event_type] || DEFAULT_EV_STYLE
                  const EvIcon = style.icon
                  const label = EVENT_LABELS[ev.event_type] || ev.event_type.replace(/_/g, ' ')
                  const isNum = NUMERIC_EVENTS.has(ev.event_type)
                  const PHOTO_EVENTS_SET = ['CONTENT_MAIN_PHOTO_CHANGED', 'CONTENT_PHOTO_ORDER_CHANGED', 'CONTENT_PHOTO_ADDED', 'CONTENT_PHOTO_REMOVED', 'OZON_PHOTO_CHANGE']
                  const isPhoto = PHOTO_EVENTS_SET.includes(ev.event_type)
                  let timeStr = ''
                  try { timeStr = format(parseISO(ev.timestamp), 'HH:mm') } catch {}

                  let oldNum = parseNum(ev.old_value)
                  let newNum = parseNum(ev.new_value)
                  if (ev.event_type === 'BID_CHANGE' && oldNum !== null) oldNum /= 100
                  if (ev.event_type === 'BID_CHANGE' && newNum !== null) newNum /= 100
                  const suffix = ev.event_type.includes('BID') || ev.event_type.includes('BUDGET') || ev.event_type.includes('PRICE') ? ' ₽' : ''

                  return (
                    <div key={ev.id} className="flex gap-3 rounded-xl border border-[hsl(var(--border)/0.4)] bg-[hsl(var(--card))] p-4 hover:border-[hsl(var(--border)/0.7)] transition-all">
                      <div className="shrink-0 h-8 w-8 rounded-lg flex items-center justify-center" style={{ background: style.bg }}>
                        <EvIcon className="h-4 w-4" style={{ color: style.color }} />
                      </div>
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
                        {isNum && oldNum !== null && newNum !== null && (
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

                        {/* Photo preview */}
                        {isPhoto && (() => {
                          const imgUrl = (ev.event_metadata?.main_image_url as string) || ''
                          const oldCount = (ev.event_metadata?.old_count as number) ?? null
                          const newCount = (ev.event_metadata?.new_count as number) ?? null
                          return (
                            <div className="flex items-center gap-3 mt-2">
                              {imgUrl ? (
                                <img src={imgUrl} alt="Фото товара" className="h-16 w-16 rounded-lg object-cover border border-[hsl(var(--border))] shadow-sm" onError={e => { (e.target as HTMLImageElement).style.display = 'none' }} />
                              ) : (
                                <div className="h-16 w-16 rounded-lg bg-[hsl(var(--muted)/0.3)] border border-[hsl(var(--border))] flex items-center justify-center">
                                  <Image className="h-6 w-6 text-[hsl(var(--muted-foreground)/0.4)]" />
                                </div>
                              )}
                              <div className="text-[12px] text-[hsl(var(--muted-foreground))]">
                                {ev.event_type === 'CONTENT_MAIN_PHOTO_CHANGED' && <span>Главное фото заменено</span>}
                                {ev.event_type === 'CONTENT_PHOTO_ADDED' && oldCount !== null && newCount !== null && <span>Добавлено фото: {oldCount} → {newCount} шт.</span>}
                                {ev.event_type === 'CONTENT_PHOTO_REMOVED' && oldCount !== null && newCount !== null && <span>Удалено фото: {oldCount} → {newCount} шт.</span>}
                                {ev.event_type === 'CONTENT_PHOTO_ORDER_CHANGED' && <span>Порядок/состав фото изменён ({newCount ?? '?'} шт.)</span>}
                                {ev.event_type === 'OZON_PHOTO_CHANGE' && <span>Фото обновлено</span>}
                              </div>
                            </div>
                          )
                        })()}

                        {/* Non-numeric, non-photo: show old → new */}
                        {!isNum && !isPhoto && (ev.old_value || ev.new_value) && (
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
            </div>
          </div>
        )
      })()}
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
