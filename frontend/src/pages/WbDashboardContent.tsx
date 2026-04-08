import { useState, useCallback } from 'react'
import { motion } from 'framer-motion'
import {
  ShoppingCart, Megaphone, TrendingUp,
  ArrowUpRight, ArrowDownRight, Eye, MousePointerClick, ShoppingBag,
  AlertTriangle, Warehouse, Package, Banknote,
  Truck, Archive, Receipt, Copy, Zap, StopCircle,
  DollarSign, Pause, Play, BatteryWarning, ArrowUp,
} from 'lucide-react'
import {
  ComposedChart, Bar, Line, Area, XAxis, YAxis,
  CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import type {
  WbDashboardResponse, WbChartPoint, WbAlerts,
  WbFinanceSummary, WbOrderFeedItem,
} from '@/api/dashboard'

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

const MONTHS_SHORT = ['янв','фев','мар','апр','май','июн','июл','авг','сен','окт','ноя','дек']
const MONTHS_FULL = ['января','февраля','марта','апреля','мая','июня','июля','августа','сентября','октября','ноября','декабря']
const DAYS_SHORT = ['вс.','пн.','вт.','ср.','чт.','пт.','сб.']

function fmt(v: number) { return v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽' }
function fmtN(v: number) { return v.toLocaleString('ru-RU') }
function fmtPct(v: number) { return `${v.toFixed(1)}%` }

function fmtChartDate(d: string) {
  const p = d.split('-'); if (p.length >= 3) { return `${+p[2]} ${MONTHS_SHORT[+p[1]-1]||p[1]}` }; return d.slice(5)
}
function fmtTooltipDate(d: string) {
  const p = d.split('-'); if (p.length >= 3) { const dt = new Date(+p[0],+p[1]-1,+p[2]); return `${+p[2]} ${MONTHS_FULL[+p[1]-1]} (${DAYS_SHORT[dt.getDay()]})` }; return d
}

function DeltaBadge({ value, invert }: { value: number; invert?: boolean }) {
  if (value === 0) return <span className="text-xs text-[hsl(var(--muted-foreground)/0.5)]">—</span>
  const pos = invert ? value < 0 : value > 0
  const sign = value > 0 ? '+' : ''
  return (
    <span className={`inline-flex items-center gap-0.5 rounded-full px-2 py-0.5 text-xs font-semibold ${pos ? 'bg-emerald-500/10 text-emerald-400' : 'bg-red-500/10 text-red-400'}`}>
      {pos ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />}
      {sign}{value.toFixed(1)}%
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   Metric Card — individual KPI card
   ═══════════════════════════════════════════════════════════ */

function MetricCard({ label, value, delta, invert, icon: Icon, color, sub }: {
  label: string
  value: string
  delta: number
  invert?: boolean
  icon: React.ElementType
  color: string
  sub?: string
}) {
  return (
    <Card className="relative overflow-hidden transition-all duration-300 hover:shadow-md hover:shadow-[hsl(var(--primary)/0.04)]">
      <div className="absolute top-0 inset-x-0 h-[2px] opacity-60" style={{ background: `linear-gradient(90deg, ${color}, transparent)` }} />
      <CardContent className="p-5">
        <div className="flex items-center gap-2.5 mb-3">
          <div className="flex items-center justify-center h-9 w-9 rounded-xl shrink-0" style={{ background: color + '12' }}>
            <Icon className="h-[18px] w-[18px]" style={{ color }} />
          </div>
          <span className="text-sm font-medium text-[hsl(var(--muted-foreground))]">{label}</span>
        </div>
        <p className="text-[1.6rem] font-bold tracking-tight leading-none">{value}</p>
        <div className="flex items-center gap-2 mt-2.5">
          <DeltaBadge value={delta} invert={invert} />
          {sub && <span className="text-xs text-[hsl(var(--muted-foreground)/0.7)] font-medium">{sub}</span>}
        </div>
      </CardContent>
    </Card>
  )
}
/* ═══════════════════════════════════════════════════════════
   KPI Section — Продажи & Реклама (v3 compact)
   ═══════════════════════════════════════════════════════════ */

/* п.п. delta — inline */
function PPDelta({ value, invert, className = '' }: { value: number; invert?: boolean; className?: string }) {
  if (value === 0) return <span className={`text-[11px] text-[hsl(var(--muted-foreground)/0.4)] ${className}`}>—</span>
  const good = invert ? value < 0 : value > 0
  return (
    <span className={`inline-flex items-center gap-0.5 text-xs font-semibold ${good ? 'text-emerald-400' : 'text-red-400'} ${className}`}>
      {good ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />}
      {value > 0 ? '+' : ''}{value.toFixed(1)}<span className="text-[10px] font-normal opacity-70">п.п.</span>
    </span>
  )
}

function WbKpiSection({ kpi }: { kpi: WbDashboardResponse['kpi'] }) {
  if (!kpi?.sales || !kpi?.advertising || !kpi?.funnel) return null
  const { sales: s, advertising: a, funnel: f } = kpi

  return (
    <div className="space-y-5">
      {/* ── Продажи: Выручка → Заказы → Отмены → Прибыль ── */}
      <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.35 }}>
        <h2 className="text-lg font-bold tracking-tight text-[hsl(var(--foreground))] mb-3 flex items-center gap-2">
          💰 Продажи
        </h2>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <MetricCard label="Выручка" value={fmt(s.revenue)} delta={s.revenue_delta}
            icon={Banknote} color="#10b981" />
          <MetricCard label="Заказы" value={fmtN(s.orders)} delta={s.orders_delta}
            icon={ShoppingCart} color="#6366f1" />
          <MetricCard label="Отмены" value={fmtN(s.cancels)} delta={s.cancels_delta}
            icon={AlertTriangle} color="#ef4444" invert
            sub={`${s.cancel_rate}% отмен`} />
          <MetricCard label="Прибыль" value={fmt(s.profit)} delta={s.profit_delta}
            icon={TrendingUp} color={s.profit >= 0 ? '#10b981' : '#ef4444'}
            sub={s.profit_pct !== 0 ? `чист. маржа ${s.profit_pct}%` : 'за вычетом расходов'} />
        </div>
      </motion.div>

      {/* ── Реклама — единый компактный блок ── */}
      <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.08, duration: 0.35 }}>
        <h2 className="text-lg font-bold tracking-tight text-[hsl(var(--foreground))] mb-3 flex items-center gap-2">
          📢 Реклама
        </h2>

        {/* Расход → ДРР общий → ДРР рекл. */}
        <div className="grid grid-cols-3 gap-3 mb-3">
          <MetricCard label="Расход" value={fmt(a.ad_spend)} delta={a.ad_spend_delta}
            icon={Banknote} color="#f97316" invert />
          <MetricCard label="ДРР общий" value={fmtPct(a.drr_total)} delta={0}
            icon={Megaphone} color="#f97316"
            sub={`${a.drr_total_delta > 0 ? '+' : ''}${a.drr_total_delta.toFixed(1)} п.п.`} />
          <MetricCard label="ДРР рекл." value={fmtPct(a.drr_ad)} delta={0}
            icon={Receipt} color="#f59e0b"
            sub={`${a.drr_ad_delta > 0 ? '+' : ''}${a.drr_ad_delta.toFixed(1)} п.п.`} />
        </div>

        {/* Воронка — 4 компактные карточки */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {/* Показы */}
          <MetricCard label="Показы" value={fmtN(f.views)} delta={f.views_delta}
            icon={Eye} color="#3b82f6" />

          {/* Клики + CTR inline */}
          <Card className="relative overflow-hidden transition-all duration-200 hover:shadow-md">
            <div className="absolute top-0 inset-x-0 h-[2px] opacity-60" style={{ background: 'linear-gradient(90deg, #06b6d4, transparent)' }} />
            <CardContent className="p-3.5">
              <div className="flex items-center gap-2 mb-2">
                <div className="flex items-center justify-center h-8 w-8 rounded-lg shrink-0 bg-cyan-500/10">
                  <MousePointerClick className="h-4 w-4 text-cyan-500" />
                </div>
                <span className="text-sm font-medium text-[hsl(var(--muted-foreground))]">Клики</span>
              </div>
              <p className="text-2xl font-bold tracking-tight leading-none">{fmtN(f.clicks)}</p>
              <div className="flex items-center justify-between mt-2">
                <DeltaBadge value={f.clicks_delta} />
                <div className="flex items-center gap-1.5">
                  <span className="text-xs text-[hsl(var(--muted-foreground)/0.6)]">CTR</span>
                  <span className="text-sm font-bold text-cyan-500">{fmtPct(f.ctr)}</span>
                  <PPDelta value={f.ctr_delta} />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Корзины + клик→корзина inline */}
          <Card className="relative overflow-hidden transition-all duration-200 hover:shadow-md">
            <div className="absolute top-0 inset-x-0 h-[2px] opacity-60" style={{ background: 'linear-gradient(90deg, #8b5cf6, transparent)' }} />
            <CardContent className="p-3.5">
              <div className="flex items-center gap-2 mb-2">
                <div className="flex items-center justify-center h-8 w-8 rounded-lg shrink-0 bg-violet-500/10">
                  <ShoppingBag className="h-4 w-4 text-violet-500" />
                </div>
                <span className="text-sm font-medium text-[hsl(var(--muted-foreground))]">Корзины</span>
              </div>
              <p className="text-2xl font-bold tracking-tight leading-none">{fmtN(f.cart)}</p>
              <div className="flex items-center justify-between mt-2">
                <DeltaBadge value={f.cart_delta} />
                <div className="flex items-center gap-1.5">
                  <span className="text-xs text-[hsl(var(--muted-foreground)/0.6)]">клик→корз.</span>
                  <span className="text-sm font-bold text-violet-500">{fmtPct(f.click_to_cart)}</span>
                  <PPDelta value={f.click_to_cart_delta} />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Заказы + конверсии inline */}
          <Card className="relative overflow-hidden transition-all duration-200 hover:shadow-md">
            <div className="absolute top-0 inset-x-0 h-[2px] opacity-60" style={{ background: 'linear-gradient(90deg, #10b981, transparent)' }} />
            <CardContent className="p-3.5">
              <div className="flex items-center gap-2 mb-2">
                <div className="flex items-center justify-center h-8 w-8 rounded-lg shrink-0 bg-emerald-500/10">
                  <Package className="h-4 w-4 text-emerald-500" />
                </div>
                <span className="text-sm font-medium text-[hsl(var(--muted-foreground))]">Заказы</span>
              </div>
              <p className="text-2xl font-bold tracking-tight leading-none">{fmtN(f.orders)}</p>
              <div className="flex items-center justify-between mt-2">
                <DeltaBadge value={f.orders_delta} />
                <div className="flex items-center gap-1.5">
                  <span className="text-xs text-[hsl(var(--muted-foreground)/0.6)]">корз→зак</span>
                  <span className="text-sm font-bold text-emerald-500">{fmtPct(f.cart_conversion)}</span>
                  <PPDelta value={f.cart_conversion_delta} />
                </div>
              </div>
              <div className="flex items-center justify-end gap-1.5 mt-1">
                <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">клик→зак</span>
                <span className="text-xs font-semibold text-emerald-400/70">{fmtPct(f.conversion)}</span>
                <PPDelta value={f.conversion_delta} className="!text-[10px]" />
              </div>
            </CardContent>
          </Card>
        </div>
      </motion.div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Unified Chart
   ═══════════════════════════════════════════════════════════ */

const CHART_METRICS = [
  { key: 'revenue', label: 'Выручка', color: '#10b981', axis: 'left' },
  { key: 'orders', label: 'Заказы', color: '#6366f1', axis: 'right' },
  { key: 'ad_spend', label: 'Расход рекл.', color: '#f97316', axis: 'left' },
  { key: 'views', label: 'Показы', color: '#3b82f6', axis: 'right' },
  { key: 'clicks', label: 'Клики', color: '#06b6d4', axis: 'right' },
  { key: 'cart', label: 'Корзины', color: '#8b5cf6', axis: 'right' },
  { key: 'drr_total', label: 'ДРР общ.', color: '#ec4899', axis: 'pct' },
  { key: 'ctr', label: 'CTR', color: '#facc15', axis: 'pct' },
] as const
type ChartKey = typeof CHART_METRICS[number]['key']
const CHART_LABELS: Record<string,string> = Object.fromEntries(CHART_METRICS.map(m => [m.key, m.label]))

function WbUnifiedChart({ data }: { data: WbChartPoint[] }) {
  const [active, setActive] = useState<Set<ChartKey>>(new Set<ChartKey>(['revenue', 'ad_spend']))
  const toggle = (k: ChartKey) => setActive(p => { const n = new Set(p); n.has(k) ? (n.size > 1 && n.delete(k)) : n.add(k); return n })

  const hasL = CHART_METRICS.some(m => active.has(m.key) && m.axis === 'left')
  const hasR = CHART_METRICS.some(m => active.has(m.key) && m.axis === 'right')
  const hasP = CHART_METRICS.some(m => active.has(m.key) && m.axis === 'pct')

  if (!data?.length) return <div className="flex h-[300px] items-center justify-center rounded-xl border border-dashed border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.15)]"><p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">Нет данных</p></div>

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap gap-2">
        {CHART_METRICS.map(m => {
          const on = active.has(m.key)
          return (
            <button key={m.key} onClick={() => toggle(m.key)}
              className="inline-flex items-center gap-1.5 rounded-full px-3 py-1.5 text-[13px] font-medium transition-all"
              style={{ background: on ? m.color+'20' : 'transparent', border: `1.5px solid ${on ? m.color : 'hsl(var(--border))'}`, color: on ? m.color : 'hsl(var(--muted-foreground))' }}>
              <span className="h-2 w-2 rounded-full" style={{ background: on ? m.color : 'hsl(var(--muted-foreground)/0.3)' }} />
              {m.label}
            </button>
          )
        })}
      </div>
      <ResponsiveContainer width="100%" height={340}>
        <ComposedChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 40 }}>
          <defs>
            <linearGradient id="wbRevGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#10b981" stopOpacity={0.3} /><stop offset="100%" stopColor="#10b981" stopOpacity={0.03} /></linearGradient>
            <linearGradient id="wbSpendGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#f97316" stopOpacity={0.3} /><stop offset="100%" stopColor="#f97316" stopOpacity={0.03} /></linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.4} />
          <XAxis dataKey="date" tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }} tickFormatter={fmtChartDate} interval={0} angle={-45} textAnchor="end" height={50} axisLine={false} tickLine={false} />
          {hasL && <YAxis yAxisId="left" tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }} tickFormatter={(v: number) => v >= 1000 ? `${(v/1000).toFixed(0)}k` : String(v)} axisLine={false} tickLine={false} width={55} />}
          {hasR && <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }} tickFormatter={(v: number) => v >= 1000 ? `${(v/1000).toFixed(0)}k` : String(v)} axisLine={false} tickLine={false} width={50} />}
          {hasP && <YAxis yAxisId="pct" orientation={hasR ? 'left' : 'right'} tick={{ fontSize: 11, fill: '#ef4444' }} tickFormatter={(v: number) => `${v}%`} axisLine={false} tickLine={false} width={40} domain={[0, 'auto']} />}
          <Tooltip contentStyle={{ background: 'hsl(var(--card))', border: '1px solid hsl(var(--border))', borderRadius: '8px', fontSize: '13px' }}
            formatter={(value: number, name: string) => [name === 'revenue' || name === 'ad_spend' ? fmt(value) : (name === 'drr_total' || name === 'ctr') ? fmtPct(value) : fmtN(value), CHART_LABELS[name] || name]}
            labelFormatter={fmtTooltipDate} />
          <Legend verticalAlign="top" height={30} formatter={(v: string) => CHART_LABELS[v] || v} wrapperStyle={{ fontSize: '12px', color: 'hsl(var(--muted-foreground))' }} />

          {active.has('revenue') && <Area yAxisId="left" type="monotone" dataKey="revenue" fill="url(#wbRevGrad)" stroke="#10b981" strokeWidth={2} dot={false} activeDot={{ r: 4, fill: '#10b981' }} />}
          {active.has('ad_spend') && <Area yAxisId="left" type="monotone" dataKey="ad_spend" fill="url(#wbSpendGrad)" stroke="#f97316" strokeWidth={2} dot={false} activeDot={{ r: 4, fill: '#f97316' }} />}
          {active.has('orders') && <Bar yAxisId="right" dataKey="orders" fill="#6366f180" radius={[3,3,0,0]} barSize={14} />}
          {active.has('views') && <Line yAxisId="right" type="monotone" dataKey="views" stroke="#3b82f6" strokeWidth={2} dot={false} />}
          {active.has('clicks') && <Line yAxisId="right" type="monotone" dataKey="clicks" stroke="#06b6d4" strokeWidth={2} dot={false} />}
          {active.has('cart') && <Line yAxisId="right" type="monotone" dataKey="cart" stroke="#8b5cf6" strokeWidth={2} dot={false} />}
          {active.has('drr_total') && <Line yAxisId="pct" type="monotone" dataKey="drr_total" stroke="#ec4899" strokeWidth={2} strokeDasharray="6 3" dot={false} />}
          {active.has('ctr') && <Line yAxisId="pct" type="monotone" dataKey="ctr" stroke="#facc15" strokeWidth={2} strokeDasharray="6 3" dot={false} />}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Alerts Panel — 5 tabs ("Важное")
   ═══════════════════════════════════════════════════════════ */

type AlertTab = 'warehouses' | 'sales' | 'storage' | 'advertising' | 'finances'

const ALERT_TABS: { key: AlertTab; label: string; icon: React.ElementType; color: string }[] = [
  { key: 'warehouses', label: 'Склады', icon: Warehouse, color: '#ef4444' },
  { key: 'sales', label: 'Продажи', icon: ShoppingCart, color: '#f97316' },
  { key: 'storage', label: 'Хранение', icon: Archive, color: '#8b5cf6' },
  { key: 'advertising', label: 'Реклама', icon: Megaphone, color: '#3b82f6' },
  { key: 'finances', label: 'Финансы', icon: Banknote, color: '#ec4899' },
]

/* Copy-to-clipboard helper */
function CopyBtn({ text }: { text: string | number }) {
  const [copied, setCopied] = useState(false)
  const copy = useCallback(() => {
    navigator.clipboard.writeText(String(text))
    setCopied(true)
    setTimeout(() => setCopied(false), 1200)
  }, [text])
  return (
    <button onClick={copy} className="inline-flex items-center gap-0.5 text-[11px] text-[hsl(var(--muted-foreground)/0.5)] hover:text-[hsl(var(--foreground))] transition-colors cursor-pointer" title="Копировать">
      {String(text)}
      <Copy className={`h-3 w-3 shrink-0 ${copied ? 'text-emerald-400' : ''}`} />
    </button>
  )
}

/* Unified product row for all alert tabs */
function AlertProductRow({ imageUrl, name, vendorCode, nmId, children }: {
  imageUrl?: string
  name: string
  vendorCode?: string
  nmId?: number
  children: React.ReactNode
}) {
  const [imgErr, setImgErr] = useState(false)
  return (
    <div className="flex items-center gap-3 rounded-lg border border-[hsl(var(--border)/0.5)] p-3 hover:bg-[hsl(var(--muted)/0.15)] transition-colors">
      {/* Image */}
      <div className="h-12 w-10 rounded-md overflow-hidden shrink-0 bg-[hsl(var(--muted)/0.3)] flex items-center justify-center">
        {imageUrl && !imgErr ? (
          <img src={imageUrl} className="h-full w-full object-cover" onError={() => setImgErr(true)} />
        ) : (
          <Package className="h-5 w-5 text-[hsl(var(--muted-foreground)/0.3)]" />
        )}
      </div>
      {/* Info */}
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium truncate leading-tight">{name}</p>
        <div className="flex items-center gap-2 mt-0.5 flex-wrap">
          {vendorCode && <CopyBtn text={vendorCode} />}
          {nmId && nmId > 0 && <CopyBtn text={nmId} />}
        </div>
      </div>
      {/* Right side — tab-specific metric */}
      <div className="shrink-0 flex flex-col items-end gap-0.5">
        {children}
      </div>
    </div>
  )
}

function WbAlertsPanel({ alerts }: { alerts: WbAlerts }) {
  const [tab, setTab] = useState<AlertTab>('warehouses')

  if (!alerts) return null
  const totalAlerts = Object.values(alerts).reduce((s, v) => s + (v?.count ?? 0), 0)
  if (totalAlerts === 0) return (
    <Card><CardContent className="py-8 text-center"><p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">🎉 Нет активных оповещений</p></CardContent></Card>
  )

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg flex items-center gap-2">
            <Zap className="h-5 w-5 text-amber-400" /> Важное
            <span className="rounded-full bg-red-500/10 px-2 py-0.5 text-xs font-bold text-red-400">{totalAlerts}</span>
          </CardTitle>
        </div>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-2 mb-4">
          {ALERT_TABS.map(t => (
            <button key={t.key} onClick={() => setTab(t.key)}
              className={`inline-flex items-center gap-1.5 rounded-lg px-3.5 py-1.5 text-sm font-medium transition-all ${tab === t.key ? 'bg-[hsl(var(--primary))] text-white shadow-sm' : 'text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.3)]'}`}>
              <t.icon className="h-3.5 w-3.5" />
              {t.label}
              {alerts[t.key].count > 0 && <span className="rounded-full bg-red-500/20 px-1.5 text-xs font-bold text-red-400">{alerts[t.key].count}</span>}
            </button>
          ))}
        </div>

        {/* Tab content — fixed height + scroll */}
        <div className="max-h-[420px] overflow-y-auto pr-1 space-y-2">
          {tab === 'warehouses' && (<>
            {alerts.warehouses.items.length === 0 ? <p className="text-sm text-center py-4 text-[hsl(var(--muted-foreground)/0.5)]">Нет проблем со складами</p> : alerts.warehouses.items.map((a, i) => (
              <AlertProductRow key={i} imageUrl={a.image_url} name={a.name} vendorCode={a.vendor_code} nmId={a.nm_id}>
                <span className={`rounded-full px-2.5 py-1 text-xs font-bold ${a.severity === 'critical' ? 'bg-red-500/15 text-red-400' : 'bg-yellow-500/15 text-yellow-400'}`}>
                  {a.days_left != null ? `${a.days_left} дн.` : 'Нет'}
                </span>
                <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">
                  {a.stock} шт · {a.avg_daily_sales}/день
                </span>
              </AlertProductRow>
            ))}
          </>)}

          {tab === 'sales' && (<>
            {alerts.sales.items.length === 0 ? <p className="text-sm text-center py-4 text-[hsl(var(--muted-foreground)/0.5)]">Все товары продаются 🎉</p> : alerts.sales.items.map((a, i) => (
              <AlertProductRow key={i} imageUrl={a.image_url} name={a.name} vendorCode={a.vendor_code} nmId={a.nm_id}>
                <span className="rounded-full bg-orange-500/15 px-2.5 py-1 text-xs font-bold text-orange-400">
                  {a.days_without_sales > 365 ? 'Нет продаж' : `${a.days_without_sales} дн.`}
                </span>
                <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">
                  {a.stock} шт · {a.last_sale_date ? `Посл: ${a.last_sale_date}` : 'Нет продаж'}
                </span>
              </AlertProductRow>
            ))}
          </>)}

          {tab === 'storage' && (<>
            {alerts.storage.items.length === 0 ? <p className="text-sm text-center py-4 text-[hsl(var(--muted-foreground)/0.5)]">Нет данных о хранении</p> : <>
              <p className="text-xs text-[hsl(var(--muted-foreground))] mb-2">Итого за 7 дн: <span className="font-bold text-[hsl(var(--foreground))]">{fmt(alerts.storage.total_cost)}</span></p>
              {alerts.storage.items.map((a, i) => (
                <AlertProductRow key={i} imageUrl={a.image_url} name={a.name} vendorCode={a.vendor_code} nmId={a.nm_id}>
                  <span className="font-bold text-sm text-red-400">{fmt(a.cost_7d)}</span>
                  <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">{a.warehouse} · {a.volume_liters} л</span>
                </AlertProductRow>
              ))}
            </>}
          </>)}

          {tab === 'advertising' && (<>
            {alerts.advertising.items.length === 0 ? <p className="text-sm text-center py-4 text-[hsl(var(--muted-foreground)/0.5)]">Реклама работает нормально 🎉</p> : alerts.advertising.items.map((a, i) => {
              const iconMap: Record<string, React.ReactNode> = {
                budget_depleted: <BatteryWarning className="h-5 w-5 text-red-400" />,
                spending_no_revenue: <DollarSign className="h-5 w-5 text-red-400" />,
                high_drr: <TrendingUp className="h-5 w-5 text-orange-400" />,
                low_views: <ArrowUp className="h-5 w-5 text-amber-400" />,
                no_views: <Eye className="h-5 w-5 text-yellow-400" />,
                stopped_profitable: <StopCircle className="h-5 w-5 text-blue-400" />,
                clicks_no_orders: <MousePointerClick className="h-5 w-5 text-purple-400" />,
              }
              const colorMap: Record<string, string> = {
                budget_depleted: 'bg-red-500/15 text-red-400',
                spending_no_revenue: 'bg-red-500/15 text-red-400',
                high_drr: 'bg-orange-500/15 text-orange-400',
                low_views: 'bg-amber-500/15 text-amber-400',
                no_views: 'bg-yellow-500/15 text-yellow-400',
                stopped_profitable: 'bg-blue-500/15 text-blue-400',
                clicks_no_orders: 'bg-purple-500/15 text-purple-400',
              }
              const labelMap: Record<string, string> = {
                budget_depleted: '0 ₽ бюджет',
                spending_no_revenue: 'Слив бюджета',
                high_drr: 'Высокий ДРР',
                low_views: 'Мало показов',
                no_views: 'Нет показов',
                stopped_profitable: 'На паузе',
                clicks_no_orders: 'Нет конверсии',
              }
              const icon = iconMap[a.problem] || <AlertTriangle className="h-5 w-5" />
              const badgeColor = colorMap[a.problem] || 'bg-gray-500/15 text-gray-400'
              const badgeLabel = labelMap[a.problem] || a.problem
              // Status badge
              const statusEl = a.status === 'no_budget'
                ? <span className="inline-flex items-center gap-0.5 rounded-full px-1.5 py-0.5 text-[10px] font-semibold bg-red-500/15 text-red-400"><BatteryWarning className="h-2.5 w-2.5" />0₽</span>
                : a.status === 'paused'
                ? <span className="inline-flex items-center gap-0.5 rounded-full px-1.5 py-0.5 text-[10px] font-medium bg-gray-500/15 text-gray-400"><Pause className="h-2.5 w-2.5" />Пауза</span>
                : a.status === 'active'
                ? <span className="inline-flex items-center gap-0.5 rounded-full px-1.5 py-0.5 text-[10px] font-medium bg-emerald-500/15 text-emerald-400"><Play className="h-2.5 w-2.5" />Акт</span>
                : null
              // Key metric — the main value to highlight LARGE
              const keyMetric = a.problem === 'high_drr' && a.drr != null
                ? <span className="text-base font-bold text-orange-400">ДРР {a.drr}%</span>
                : a.problem === 'budget_depleted'
                ? <span className="text-base font-bold text-red-400">Бюджет 0 ₽</span>
                : a.problem === 'spending_no_revenue' && a.spend
                ? <span className="text-base font-bold text-red-400">−{fmt(a.spend)}</span>
                : a.problem === 'low_views' && a.views != null
                ? <span className="text-base font-bold text-amber-400">{a.views} показов</span>
                : a.problem === 'no_views'
                ? <span className="text-base font-bold text-yellow-400">0 показов</span>
                : a.problem === 'clicks_no_orders' && a.clicks
                ? <span className="text-base font-bold text-purple-400">{a.clicks} кликов</span>
                : null
              return (
                <div key={i} className="flex items-start gap-3 rounded-lg border border-[hsl(var(--border)/0.5)] p-3 hover:bg-[hsl(var(--muted)/0.15)] transition-colors">
                  <div className={`flex items-center justify-center h-10 w-10 rounded-lg shrink-0 mt-0.5 ${badgeColor.split(' ')[0]}`}>
                    {icon}
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <p className="text-sm font-medium truncate max-w-[220px]">{a.name || `Кампания #${a.advert_id}`}</p>
                      {statusEl}
                    </div>
                    {/* Key metric — LARGE and visible */}
                    {keyMetric && <div className="mt-1">{keyMetric}</div>}
                    {/* Context line */}
                    <p className="text-xs text-[hsl(var(--muted-foreground)/0.6)] mt-0.5">{a.reason || ''}</p>
                    {/* Secondary metrics */}
                    <div className="flex items-center gap-3 mt-1 text-xs text-[hsl(var(--muted-foreground)/0.5)]">
                      {a.spend != null && a.spend > 0 && <span>Расход: <b className="text-[hsl(var(--foreground)/0.8)]">{fmt(a.spend)}</b></span>}
                      {a.revenue != null && a.revenue > 0 && <span>Выручка: <b className="text-emerald-400/80">{fmt(a.revenue)}</b></span>}
                      {a.orders != null && a.orders > 0 && <span>{a.orders} заказов</span>}
                    </div>
                  </div>
                  <span className={`shrink-0 rounded-full px-2.5 py-1 text-xs font-bold whitespace-nowrap ${badgeColor}`}>
                    {badgeLabel}
                  </span>
                </div>
              )
            })}
          </>)}

          {tab === 'finances' && (<>
            {alerts.finances.items.length === 0 ? <p className="text-sm text-center py-4 text-[hsl(var(--muted-foreground)/0.5)]">Нет убыточных SKU 🎉</p> : alerts.finances.items.map((a, i) => (
              <AlertProductRow key={i} imageUrl={a.image_url} name={a.name} vendorCode={a.vendor_code} nmId={a.nm_id}>
                <span className="font-bold text-sm text-red-400">{fmt(a.profit)}</span>
                <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">{a.qty} шт · {a.reason || ''}</span>
              </AlertProductRow>
            ))}
          </>)}
        </div>
      </CardContent>
    </Card>
  )
}

/* ═══════════════════════════════════════════════════════════
   Finance Summary — full P&L breakdown (Mon-Sun week)
   ═══════════════════════════════════════════════════════════ */

/** Format ISO date → "6 апреля" */
function fmtHumanDate(d: string) {
  const p = d.split('-')
  if (p.length >= 3) return `${+p[2]} ${MONTHS_FULL[+p[1] - 1] || p[1]}`
  return d
}

/** Percentage delta between current and previous */
function pctDelta(cur: number, prev: number): number {
  if (!prev || prev === 0) return cur > 0 ? 100 : 0
  return ((cur - prev) / Math.abs(prev)) * 100
}

function WbFinanceSummaryCard({ data }: { data: WbFinanceSummary }) {
  const expenseRows: {
    label: string; value: number; prev: number; icon: React.ElementType; color: string; invert?: boolean
  }[] = [
    { label: 'Комиссия WB', value: data.commission, prev: data.commission_prev, icon: Receipt, color: '#f97316', invert: true },
    { label: 'Логистика', value: data.logistics, prev: data.logistics_prev, icon: Truck, color: '#3b82f6', invert: true },
    { label: 'Хранение', value: data.storage, prev: data.storage_prev, icon: Archive, color: '#8b5cf6', invert: true },
    { label: 'Реклама', value: data.ad_spend, prev: data.ad_spend_prev, icon: Megaphone, color: '#ec4899', invert: true },
    { label: 'Удержания', value: data.deductions, prev: data.deductions_prev, icon: AlertTriangle, color: '#ef4444', invert: true },
  ]

  // Only show non-zero optional rows
  if (data.acceptance > 0 || data.acceptance_prev > 0) {
    expenseRows.push({ label: 'Приёмка', value: data.acceptance, prev: data.acceptance_prev, icon: Truck, color: '#06b6d4', invert: true })
  }
  if (data.penalties > 0 || data.penalties_prev > 0) {
    expenseRows.push({ label: 'Штрафы', value: data.penalties, prev: data.penalties_prev, icon: AlertTriangle, color: '#dc2626', invert: true })
  }

  const totalExpenses = expenseRows.reduce((s, r) => s + r.value, 0)
  const totalExpensesPrev = expenseRows.reduce((s, r) => s + r.prev, 0)

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg flex items-center gap-2">
            <Banknote className="h-5 w-5 text-emerald-400" /> Финансы за неделю
          </CardTitle>
          <span className="rounded-lg bg-[hsl(var(--muted)/0.3)] px-3 py-1.5 text-sm font-semibold text-[hsl(var(--foreground))]">
            {fmtHumanDate(data.week_start)} — {fmtHumanDate(data.week_end)}
          </span>
        </div>
      </CardHeader>
      <CardContent className="space-y-1">
        {/* Revenue row — highlighted */}
        <div className="flex items-center justify-between py-2.5 px-3 rounded-lg bg-emerald-500/5">
          <div className="flex items-center gap-2.5">
            <Banknote className="h-4.5 w-4.5 text-emerald-400" />
            <span className="text-sm font-semibold">Выручка</span>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-base font-bold text-emerald-400">{fmt(data.revenue)}</span>
            <DeltaBadge value={data.revenue_delta} />
          </div>
        </div>

        {/* Returns row — if non-zero */}
        {(data.returns > 0 || data.returns_prev > 0) && (
          <div className="flex items-center justify-between py-2 px-3">
            <div className="flex items-center gap-2.5">
              <ShoppingCart className="h-4 w-4 text-orange-400" />
              <span className="text-sm text-[hsl(var(--muted-foreground))]">Возвраты</span>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-sm font-semibold text-orange-400">{fmt(data.returns)}</span>
              <DeltaBadge value={pctDelta(data.returns, data.returns_prev)} invert />
            </div>
          </div>
        )}

        {/* Divider */}
        <div className="border-t border-[hsl(var(--border)/0.4)] my-1" />

        {/* Expense rows */}
        {expenseRows.map(r => {
          const delta = pctDelta(r.value, r.prev)
          return (
            <div key={r.label} className="flex items-center justify-between py-2 px-3 rounded-md hover:bg-[hsl(var(--muted)/0.1)] transition-colors">
              <div className="flex items-center gap-2.5">
                <r.icon className="h-4 w-4" style={{ color: r.color }} />
                <span className="text-sm">{r.label}</span>
              </div>
              <div className="flex items-center gap-3">
                <span className="text-sm font-semibold">{fmt(r.value)}</span>
                {(delta !== 0 && (r.value > 0 || r.prev > 0)) && <DeltaBadge value={delta} invert={r.invert} />}
              </div>
            </div>
          )
        })}

        {/* Total expenses */}
        <div className="flex items-center justify-between py-2 px-3 border-t border-[hsl(var(--border)/0.4)]">
          <span className="text-sm font-medium text-[hsl(var(--muted-foreground))]">Итого расходы</span>
          <div className="flex items-center gap-3">
            <span className="text-sm font-bold text-red-400">{fmt(totalExpenses)}</span>
            <DeltaBadge value={pctDelta(totalExpenses, totalExpensesPrev)} invert />
          </div>
        </div>

        {/* Profit — prominent */}
        <div className="flex items-center justify-between pt-3 pb-1 px-3 border-t-2 border-[hsl(var(--border))]">
          <div className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5" style={{ color: data.profit >= 0 ? '#10b981' : '#ef4444' }} />
            <span className="text-base font-bold">Прибыль</span>
          </div>
          <div className="flex items-center gap-3">
            <span className={`text-xl font-bold ${data.profit >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>{fmt(data.profit)}</span>
            <DeltaBadge value={data.profit_delta} />
            <span className="text-sm text-[hsl(var(--muted-foreground)/0.6)] font-medium">{data.profit_pct}%</span>
          </div>
        </div>

        {/* Orders count */}
        <div className="flex items-center justify-between py-1.5 px-3">
          <span className="text-xs text-[hsl(var(--muted-foreground)/0.5)]">Продажи (шт)</span>
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-[hsl(var(--muted-foreground))]">{fmtN(data.orders)}</span>
            {data.orders_prev > 0 && <DeltaBadge value={pctDelta(data.orders, data.orders_prev)} />}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

/* ═══════════════════════════════════════════════════════════
   Orders Feed — fixed height, 3:4 images
   ═══════════════════════════════════════════════════════════ */

function WbOrdersFeed({ items }: { items: WbOrderFeedItem[] }) {
  if (!items?.length) return null

  /** Format date: "2026-04-08" → "8 апреля" */
  const fmtOrderDate = (d: string) => {
    const p = d.split('-')
    if (p.length >= 3) {
      return `${+p[2]} ${MONTHS_FULL[+p[1] - 1] || p[1]}`
    }
    return d
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-lg">📦 Заказы за период</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto -mx-5">
          {/* Fixed header */}
          <table className="w-full text-sm">
            <thead><tr className="border-b border-[hsl(var(--border)/0.5)]">
              <th className="px-5 py-2.5 text-left text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Товар</th>
              <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Заказы</th>
              <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Продажи</th>
              <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Посл. заказ</th>
            </tr></thead>
          </table>
          {/* Scrollable body */}
          <div className="max-h-[480px] overflow-y-auto">
            <table className="w-full text-sm">
              <tbody>
                {items.slice(0, 30).map((item) => (
                  <tr key={item.nm_id} className="border-b border-[hsl(var(--border)/0.3)] hover:bg-[hsl(var(--muted)/0.15)] transition-colors">
                    <td className="px-5 py-3">
                      <div className="flex items-center gap-3">
                        {item.image_url ? (
                          <img src={item.image_url} className="h-14 w-[42px] rounded-lg object-cover shrink-0" />
                        ) : (
                          <div className="h-14 w-[42px] rounded-lg bg-[hsl(var(--muted)/0.4)] shrink-0 flex items-center justify-center">
                            <Package className="h-5 w-5 text-[hsl(var(--muted-foreground)/0.3)]" />
                          </div>
                        )}
                        <div className="min-w-0">
                          <p className="text-sm font-medium truncate max-w-[260px] leading-tight">{item.name}</p>
                          <div className="flex items-center gap-2.5 mt-1 flex-wrap">
                            {(item.vendor_code || item.supplier_article) && (
                              <CopyBtn text={item.vendor_code || item.supplier_article} />
                            )}
                            {item.nm_id > 0 && <CopyBtn text={item.nm_id} />}
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="px-3 py-3 text-right">
                      <div className="flex items-center justify-end gap-1.5">
                        <span className="font-semibold text-base">{fmtN(item.orders)}</span>
                        {item.orders_prev > 0 && <DeltaBadge value={pctDelta(item.orders, item.orders_prev)} />}
                      </div>
                    </td>
                    <td className="px-3 py-3 text-right font-semibold text-base">{fmt(item.revenue)}</td>
                    <td className="px-3 py-3 text-right text-sm text-[hsl(var(--muted-foreground))]">{fmtOrderDate(item.last_order)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main WB Dashboard Content
   ═══════════════════════════════════════════════════════════ */

export default function WbDashboardContent({ data }: { data: WbDashboardResponse }) {
  return (
    <div className="space-y-6">
      {/* KPI Groups */}
      <WbKpiSection kpi={data.kpi} />

      {/* Unified Chart */}
      <motion.div initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4, delay: 0.25 }}>
        <Card>
          <CardHeader><CardTitle className="text-lg">📊 Динамика</CardTitle></CardHeader>
          <CardContent><WbUnifiedChart data={data.chart} /></CardContent>
        </Card>
      </motion.div>

      {/* Alerts Panel — right after chart */}
      <motion.div initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4, delay: 0.3 }}>
        <WbAlertsPanel alerts={data.alerts} />
      </motion.div>

      {/* Finance Summary */}
      {data.finance_summary && (
        <motion.div initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4, delay: 0.35 }}>
          <WbFinanceSummaryCard data={data.finance_summary} />
        </motion.div>
      )}

      {/* Orders Feed */}
      <motion.div initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.4, delay: 0.4 }}>
        <WbOrdersFeed items={data.orders_feed} />
      </motion.div>
    </div>
  )
}
