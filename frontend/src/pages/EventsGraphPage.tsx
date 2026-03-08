/**
 * EventsGraphPage — События + KPI на временной шкале.
 *
 * ComposedChart: столбцы событий (stacked по категориям) + линии KPI.
 * Toggle chips для включения/отключения метрик.
 * PeriodSelector + GroupBy selector.
 */
import { useState, useEffect, useMemo, useCallback } from 'react'
import {
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer,
} from 'recharts'
import {
  Activity, TrendingUp, Eye, MousePointer, ShoppingCart, DollarSign,
  BarChart3, Target, Loader2,
} from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { useAppStore } from '@/stores/appStore'
import { getEventsGraphApi, type EventsGraphPoint } from '@/api/events_graph'

/* ═══════════════════════════════════════════════════════════
   Constants
   ═══════════════════════════════════════════════════════════ */

const PERIODS = [
  { key: '7d', label: '7 дней' },
  { key: '30d', label: '30 дней' },
  { key: '90d', label: '90 дней' },
] as const

const GROUP_OPTIONS = [
  { key: 'day', label: 'День' },
  { key: 'week', label: 'Неделя' },
  { key: 'month', label: 'Месяц' },
] as const

const CATEGORY_COLORS: Record<string, string> = {
  advertising: '#3b82f6',
  content: '#10b981',
  commercial: '#f59e0b',
  stock: '#f97316',
}

const CATEGORY_LABELS: Record<string, string> = {
  advertising: '🔊 Реклама',
  content: '🎨 Контент',
  commercial: '💲 Коммерция',
  stock: '📦 Склад',
}

interface MetricConfig {
  key: string
  label: string
  icon: LucideIcon
  color: string
  yAxisId: string
  type: 'line' | 'area'
  defaultOn: boolean
  suffix?: string
  formatter?: (v: number) => string
}

const METRICS: MetricConfig[] = [
  { key: 'orders', label: 'Заказы', icon: ShoppingCart, color: '#10b981', yAxisId: 'right', type: 'line', defaultOn: true },
  { key: 'revenue', label: 'Выручка', icon: DollarSign, color: '#3b82f6', yAxisId: 'right', type: 'line', defaultOn: false, suffix: ' ₽', formatter: (v) => fmtK(v) + ' ₽' },
  { key: 'views', label: 'Показы', icon: Eye, color: '#0ea5e9', yAxisId: 'right2', type: 'line', defaultOn: false, formatter: fmtK },
  { key: 'clicks', label: 'Клики', icon: MousePointer, color: '#06b6d4', yAxisId: 'right2', type: 'line', defaultOn: false, formatter: fmtK },
  { key: 'carts', label: 'Корзины', icon: ShoppingCart, color: '#8b5cf6', yAxisId: 'right2', type: 'line', defaultOn: false, formatter: fmtK },
  { key: 'ad_spend', label: 'Расход рекламы', icon: TrendingUp, color: '#f97316', yAxisId: 'right', type: 'area', defaultOn: false, suffix: ' ₽', formatter: (v) => fmtK(v) + ' ₽' },
  { key: 'drr', label: 'DRR', icon: Target, color: '#ef4444', yAxisId: 'pct', type: 'line', defaultOn: false, suffix: '%', formatter: (v) => v.toFixed(1) + '%' },
  { key: 'cpo', label: 'CPO', icon: BarChart3, color: '#eab308', yAxisId: 'right', type: 'line', defaultOn: false, suffix: ' ₽', formatter: (v) => Math.round(v) + ' ₽' },
]

function fmtK(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return String(Math.round(n))
}

function fmtMoney(n: number): string {
  return n.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function fmtNum(n: number): string {
  return n.toLocaleString('ru-RU', { maximumFractionDigits: 0 })
}

const MONTHS_SHORT = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
const DAYS_FULL = ['воскресенье', 'понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота']

function formatXTick(dateStr: string): string {
  const d = new Date(dateStr + 'T00:00:00')
  return `${d.getDate()} ${MONTHS_SHORT[d.getMonth()]}`
}

function formatTooltipDate(dateStr: string): string {
  const d = new Date(dateStr + 'T00:00:00')
  return `${d.getDate()} ${MONTHS_SHORT[d.getMonth()]} — ${DAYS_FULL[d.getDay()]}`
}

/* ═══════════════════════════════════════════════════════════
   Custom Tooltip
   ═══════════════════════════════════════════════════════════ */

function GraphTooltip({ active, payload, label, enabledMetrics }: any) {
  if (!active || !payload?.length) return null

  const point = payload[0]?.payload as EventsGraphPoint | undefined
  if (!point) return null

  const byCategory = point.events_by_category || {}
  const brief = point.events_brief || []
  const total = point.events_total || 0

  return (
    <div style={{
      background: 'hsl(var(--card))',
      border: '1px solid hsl(var(--border))',
      borderRadius: 12,
      padding: '14px 16px',
      boxShadow: '0 8px 32px hsl(var(--foreground)/0.12)',
      maxWidth: 340,
      fontSize: 13,
    }}>
      {/* Date header */}
      <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 8, color: 'hsl(var(--foreground))' }}>
        {formatTooltipDate(point.date)}
      </div>

      {/* Events section */}
      {total > 0 && (
        <div style={{ marginBottom: 10 }}>
          <div style={{ fontWeight: 600, color: 'hsl(var(--foreground))', marginBottom: 6 }}>
            📊 {total} событий
          </div>
          {['advertising', 'content', 'commercial', 'stock'].map(cat => {
            const count = byCategory[cat]
            if (!count) return null
            const catBriefs = brief.filter((b: any) => b.category === cat)
            return (
              <div key={cat} style={{ marginBottom: 4 }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: CATEGORY_COLORS[cat], marginBottom: 2 }}>
                  {CATEGORY_LABELS[cat]} ({count})
                </div>
                {catBriefs.map((b: any, i: number) => (
                  <div key={i} style={{
                    fontSize: 11, color: 'hsl(var(--muted-foreground))',
                    paddingLeft: 12, lineHeight: 1.4,
                  }}>
                    • {b.text}
                  </div>
                ))}
              </div>
            )
          })}
        </div>
      )}

      {/* Metrics section */}
      <div style={{
        borderTop: total > 0 ? '1px solid hsl(var(--border))' : 'none',
        paddingTop: total > 0 ? 8 : 0,
        display: 'grid',
        gridTemplateColumns: '1fr 1fr',
        gap: '4px 16px',
      }}>
        {METRICS.filter(m => enabledMetrics?.has(m.key)).map(m => {
          const val = (point as any)[m.key]
          if (val === undefined || val === null) return null
          const formatted = m.key === 'revenue' || m.key === 'ad_spend'
            ? fmtMoney(val)
            : m.key === 'drr'
            ? val.toFixed(1) + '%'
            : m.key === 'cpo'
            ? Math.round(val) + ' ₽'
            : fmtNum(val)
          return (
            <div key={m.key} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <div style={{ width: 8, height: 8, borderRadius: '50%', background: m.color, flexShrink: 0 }} />
              <span style={{ color: 'hsl(var(--muted-foreground))', fontSize: 12 }}>{m.label}:</span>
              <span style={{ fontWeight: 600, color: 'hsl(var(--foreground))', fontSize: 12 }}>{formatted}</span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function EventsGraphPage() {
  const shop = useAppStore((s) => s.currentShop)
  const [period, setPeriod] = useState('30d')
  const [groupBy, setGroupBy] = useState('day')
  const [data, setData] = useState<EventsGraphPoint[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Toggle chips state
  const [enabledMetrics, setEnabledMetrics] = useState<Set<string>>(() => {
    const defaults = new Set<string>()
    METRICS.filter(m => m.defaultOn).forEach(m => defaults.add(m.key))
    return defaults
  })

  const toggleMetric = useCallback((key: string) => {
    setEnabledMetrics(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }, [])

  // Fetch data
  useEffect(() => {
    if (!shop) return
    let cancelled = false

    ;(async () => {
      setLoading(true)
      setError(null)
      try {
        const res = await getEventsGraphApi({
          shop_id: shop.id,
          period,
          group_by: groupBy,
        })
        if (!cancelled) setData(res.data)
      } catch (e: any) {
        if (!cancelled) setError(e?.response?.data?.detail || 'Ошибка загрузки')
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()

    return () => { cancelled = true }
  }, [shop?.id, period, groupBy])

  // Flatten stacked bar data
  const chartData = useMemo(() => {
    return data.map(p => ({
      ...p,
      ev_advertising: p.events_by_category?.advertising || 0,
      ev_content: p.events_by_category?.content || 0,
      ev_commercial: p.events_by_category?.commercial || 0,
      ev_stock: p.events_by_category?.stock || 0,
    }))
  }, [data])

  // Determine which Y-axes are needed
  const activeYAxes = useMemo(() => {
    const axes = new Set<string>()
    METRICS.filter(m => enabledMetrics.has(m.key)).forEach(m => axes.add(m.yAxisId))
    return axes
  }, [enabledMetrics])

  if (!shop) return null

  return (
    <div style={{ padding: '24px 32px', maxWidth: 1400, margin: '0 auto' }}>
      {/* ── Header ── */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 24, fontWeight: 700, color: 'hsl(var(--foreground))', margin: 0 }}>
          📊 График событий
        </h1>
        <p style={{ fontSize: 14, color: 'hsl(var(--muted-foreground))', marginTop: 4 }}>
          Влияние событий на ключевые показатели магазина
        </p>
      </div>

      {/* ── Controls ── */}
      <div style={{
        display: 'flex', gap: 12, marginBottom: 20, flexWrap: 'wrap', alignItems: 'center',
      }}>
        {/* Period selector */}
        <div style={{
          display: 'flex', gap: 0, borderRadius: 10, overflow: 'hidden',
          border: '1px solid hsl(var(--border))',
        }}>
          {PERIODS.map(p => (
            <button
              key={p.key}
              onClick={() => setPeriod(p.key)}
              style={{
                padding: '8px 16px', fontSize: 13, fontWeight: 500, cursor: 'pointer',
                background: period === p.key ? 'hsl(var(--primary))' : 'hsl(var(--card))',
                color: period === p.key ? 'hsl(var(--primary-foreground))' : 'hsl(var(--muted-foreground))',
                border: 'none', borderRight: '1px solid hsl(var(--border))',
                transition: 'all 0.15s',
              }}
            >
              {p.label}
            </button>
          ))}
        </div>

        {/* Group by selector */}
        <div style={{
          display: 'flex', gap: 0, borderRadius: 10, overflow: 'hidden',
          border: '1px solid hsl(var(--border))',
        }}>
          {GROUP_OPTIONS.map(g => (
            <button
              key={g.key}
              onClick={() => setGroupBy(g.key)}
              style={{
                padding: '8px 14px', fontSize: 13, fontWeight: 500, cursor: 'pointer',
                background: groupBy === g.key ? 'hsl(var(--primary))' : 'hsl(var(--card))',
                color: groupBy === g.key ? 'hsl(var(--primary-foreground))' : 'hsl(var(--muted-foreground))',
                border: 'none', borderRight: '1px solid hsl(var(--border))',
                transition: 'all 0.15s',
              }}
            >
              {g.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Metric Toggle Chips ── */}
      <div style={{
        display: 'flex', gap: 8, marginBottom: 20, flexWrap: 'wrap',
      }}>
        {METRICS.map(m => {
          const isOn = enabledMetrics.has(m.key)
          const Icon = m.icon
          return (
            <button
              key={m.key}
              onClick={() => toggleMetric(m.key)}
              style={{
                display: 'flex', alignItems: 'center', gap: 6,
                padding: '6px 14px', borderRadius: 20, fontSize: 13, fontWeight: 500,
                cursor: 'pointer', transition: 'all 0.15s',
                background: isOn ? m.color + '18' : 'hsl(var(--muted)/0.3)',
                color: isOn ? m.color : 'hsl(var(--muted-foreground))',
                border: isOn ? `1.5px solid ${m.color}60` : '1.5px solid transparent',
              }}
            >
              <Icon size={14} />
              {m.label}
            </button>
          )
        })}
      </div>

      {/* ── Loading / Error ── */}
      {loading && (
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          height: 400, gap: 10, color: 'hsl(var(--muted-foreground))',
        }}>
          <Loader2 size={20} style={{ animation: 'spin 1s linear infinite' }} />
          Загрузка данных...
        </div>
      )}

      {error && (
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          height: 400, color: 'hsl(var(--destructive))', fontSize: 14,
        }}>
          {error}
        </div>
      )}

      {/* ── Chart ── */}
      {!loading && !error && (
        <div style={{
          background: 'hsl(var(--card))',
          border: '1px solid hsl(var(--border))',
          borderRadius: 16,
          padding: '20px 16px 12px',
        }}>
          <ResponsiveContainer width="100%" height={480}>
            <ComposedChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="hsl(var(--border))"
                vertical={false}
              />
              <XAxis
                dataKey="date"
                tickFormatter={formatXTick}
                tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                axisLine={{ stroke: 'hsl(var(--border))' }}
                tickLine={false}
                interval={groupBy === 'day' ? (data.length > 60 ? 6 : data.length > 30 ? 3 : 1) : 0}
              />

              {/* Left Y-axis — Events count */}
              <YAxis
                yAxisId="left"
                orientation="left"
                tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                axisLine={false}
                tickLine={false}
                width={40}
                label={{ value: 'События', angle: -90, position: 'insideLeft', fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
              />

              {/* Right Y-axis — Orders / Revenue / Spend / CPO */}
              {activeYAxes.has('right') && (
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                  axisLine={false}
                  tickLine={false}
                  width={50}
                  tickFormatter={fmtK}
                />
              )}

              {/* Right2 Y-axis — Views / Clicks / Carts (can be much larger) */}
              {activeYAxes.has('right2') && (
                <YAxis
                  yAxisId="right2"
                  orientation="right"
                  tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                  axisLine={false}
                  tickLine={false}
                  width={50}
                  tickFormatter={fmtK}
                  hide={activeYAxes.has('right')}
                />
              )}

              {/* Percent Y-axis — DRR */}
              {activeYAxes.has('pct') && (
                <YAxis
                  yAxisId="pct"
                  orientation="right"
                  tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
                  axisLine={false}
                  tickLine={false}
                  width={40}
                  tickFormatter={(v: number) => v + '%'}
                />
              )}

              {/* Hidden axes for unused yAxisIds */}
              {!activeYAxes.has('right') && <YAxis yAxisId="right" hide />}
              {!activeYAxes.has('right2') && <YAxis yAxisId="right2" hide />}
              {!activeYAxes.has('pct') && <YAxis yAxisId="pct" hide />}

              <Tooltip
                content={<GraphTooltip enabledMetrics={enabledMetrics} />}
                cursor={{ fill: 'hsl(var(--muted)/0.15)' }}
              />

              {/* ── Stacked bars: events by category ── */}
              <Bar dataKey="ev_advertising" stackId="events" yAxisId="left"
                fill={CATEGORY_COLORS.advertising} radius={[0, 0, 0, 0]}
                name="Реклама" maxBarSize={groupBy === 'day' ? 24 : 40} />
              <Bar dataKey="ev_content" stackId="events" yAxisId="left"
                fill={CATEGORY_COLORS.content}
                name="Контент" maxBarSize={groupBy === 'day' ? 24 : 40} />
              <Bar dataKey="ev_commercial" stackId="events" yAxisId="left"
                fill={CATEGORY_COLORS.commercial}
                name="Коммерция" maxBarSize={groupBy === 'day' ? 24 : 40} />
              <Bar dataKey="ev_stock" stackId="events" yAxisId="left"
                fill={CATEGORY_COLORS.stock} radius={[3, 3, 0, 0]}
                name="Склад" maxBarSize={groupBy === 'day' ? 24 : 40} />

              {/* ── Metric lines ── */}
              {METRICS.filter(m => enabledMetrics.has(m.key)).map(m => (
                <Line
                  key={m.key}
                  type="monotone"
                  dataKey={m.key}
                  yAxisId={m.yAxisId}
                  stroke={m.color}
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, strokeWidth: 2 }}
                  name={m.label}
                  connectNulls
                />
              ))}
            </ComposedChart>
          </ResponsiveContainer>

          {/* ── Legend ── */}
          <div style={{
            display: 'flex', justifyContent: 'center', gap: 16, marginTop: 8,
            flexWrap: 'wrap', fontSize: 12, color: 'hsl(var(--muted-foreground))',
          }}>
            {/* Event categories */}
            {Object.entries(CATEGORY_LABELS).map(([key, label]) => (
              <div key={key} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <div style={{ width: 10, height: 10, borderRadius: 2, background: CATEGORY_COLORS[key] }} />
                {label}
              </div>
            ))}

            {/* Active metric lines */}
            {METRICS.filter(m => enabledMetrics.has(m.key)).map(m => (
              <div key={m.key} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <div style={{ width: 16, height: 2, borderRadius: 1, background: m.color }} />
                {m.label}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Summary cards ── */}
      {!loading && !error && data.length > 0 && (
        <div style={{
          display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
          gap: 12, marginTop: 16,
        }}>
          <SummaryCard
            label="Всего событий"
            value={data.reduce((s, d) => s + d.events_total, 0)}
            icon={Activity}
            color="#a78bfa"
          />
          <SummaryCard
            label="Заказы"
            value={data.reduce((s, d) => s + d.orders, 0)}
            icon={ShoppingCart}
            color="#10b981"
          />
          <SummaryCard
            label="Выручка"
            value={fmtMoney(data.reduce((s, d) => s + d.revenue, 0))}
            icon={DollarSign}
            color="#3b82f6"
          />
          <SummaryCard
            label="Расход рекламы"
            value={fmtMoney(data.reduce((s, d) => s + d.ad_spend, 0))}
            icon={TrendingUp}
            color="#f97316"
          />
          {(() => {
            const totalSpend = data.reduce((s, d) => s + d.ad_spend, 0)
            const totalRevenue = data.reduce((s, d) => s + d.revenue, 0)
            const avgDrr = totalRevenue > 0 ? (totalSpend / totalRevenue * 100).toFixed(1) + '%' : '—'
            return (
              <SummaryCard
                label="Средний DRR"
                value={avgDrr}
                icon={Target}
                color="#ef4444"
              />
            )
          })()}
        </div>
      )}

      {/* CSS for spin animation */}
      <style>{`
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
      `}</style>
    </div>
  )
}


/* ═══════════════════════════════════════════════════════════
   Summary Card
   ═══════════════════════════════════════════════════════════ */

function SummaryCard({ label, value, icon: Icon, color }: {
  label: string
  value: string | number
  icon: LucideIcon
  color: string
}) {
  return (
    <div style={{
      background: 'hsl(var(--card))',
      border: '1px solid hsl(var(--border))',
      borderRadius: 12,
      padding: '16px 20px',
      display: 'flex', alignItems: 'center', gap: 12,
    }}>
      <div style={{
        width: 36, height: 36, borderRadius: 10,
        background: color + '15',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}>
        <Icon size={18} style={{ color }} />
      </div>
      <div>
        <div style={{ fontSize: 11, color: 'hsl(var(--muted-foreground))', fontWeight: 500 }}>{label}</div>
        <div style={{ fontSize: 18, fontWeight: 700, color: 'hsl(var(--foreground))' }}>{value}</div>
      </div>
    </div>
  )
}
