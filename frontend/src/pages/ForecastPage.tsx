import { useState, useEffect, useCallback, useMemo } from 'react'
import {
  fetchOzonForecast,
  fetchWbForecast,
  type ForecastResponse,
  type ForecastProduct,
  type SkuAnalysis,
} from '@/api/forecast'
import { useAppStore } from '@/stores/appStore'
import {
  ResponsiveContainer, ComposedChart, Area, Line, XAxis, YAxis,
  CartesianGrid, Tooltip, Legend,
} from 'recharts'

/* ── helpers ── */
const fmtMoney = (v: number) =>
  v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
const fmtNum = (v: number) => v.toLocaleString('ru-RU')
const fmtDate = (d: string) => {
  const dt = new Date(d)
  return dt.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })
}

/* ── KPI card ── */
function KpiCard({ label, value, sub, trend, color }: {
  label: string; value: string; sub?: string; trend?: number; color: string
}) {
  return (
    <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] p-5 flex flex-col gap-1">
      <span className="text-xs font-medium text-[hsl(var(--muted-foreground))] uppercase tracking-wider">{label}</span>
      <span className="text-2xl font-bold" style={{ color }}>{value}</span>
      <div className="flex items-center gap-2 text-xs">
        {trend !== undefined && (
          <span className={`font-semibold ${trend >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
            {trend >= 0 ? '↑' : '↓'} {Math.abs(trend)}% / день
          </span>
        )}
        {sub && <span className="text-[hsl(var(--muted-foreground)/0.7)]">{sub}</span>}
      </div>
    </div>
  )
}

/* ── Severity styles ── */
const sevStyles = {
  critical: { border: 'border-red-500/40', bg: 'bg-red-500/5', icon: '🔴', label: 'Критично' },
  warning: { border: 'border-amber-500/40', bg: 'bg-amber-500/5', icon: '🟡', label: 'Внимание' },
  opportunity: { border: 'border-emerald-500/40', bg: 'bg-emerald-500/5', icon: '🟢', label: 'Возможность' },
  ok: { border: 'border-[hsl(var(--border)/0.3)]', bg: 'bg-[hsl(var(--card))]', icon: '✅', label: 'Ок' },
}

/* ── SKU Analysis Card (Сейчас → Будет → Делай) ── */
function SkuAnalysisCard({ product }: { product: ForecastProduct }) {
  const a = product.analysis
  const s = sevStyles[a.severity] || sevStyles.ok

  return (
    <div className={`rounded-xl border ${s.border} ${s.bg} p-4`}>
      {/* Header */}
      <div className="flex items-center gap-3 mb-3">
        {product.image_url && <img src={product.image_url} alt="" className="w-8 h-8 rounded object-cover" />}
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="text-sm font-bold truncate max-w-[200px]">{product.name || product.offer_id}</span>
            <span className="text-[10px] px-1.5 py-0.5 rounded bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))]">{product.offer_id}</span>
          </div>
          <div className="text-xs mt-0.5">
            <span className="font-semibold">{s.icon} {a.title}</span>
          </div>
        </div>
      </div>

      {/* Three columns: Сейчас | Будет | Делай */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {/* NOW */}
        <div className="rounded-lg bg-[hsl(var(--card)/0.7)] border border-[hsl(var(--border)/0.15)] p-3">
          <div className="text-[10px] uppercase tracking-wider text-[hsl(var(--muted-foreground))] font-semibold mb-2">📊 Сейчас</div>
          <div className="space-y-1.5">
            <MetricRow label="Прибыль" value={fmtMoney(a.now.profit)} color={a.now.profit >= 0 ? 'text-emerald-400' : 'text-red-400'} bold />
            <MetricRow label="Маржа" value={`${a.now.margin_pct}%`} color={a.now.margin_pct >= 10 ? 'text-emerald-400' : a.now.margin_pct >= 0 ? 'text-amber-400' : 'text-red-400'} />
            <MetricRow label="Выручка" value={fmtMoney(a.now.revenue)} />
            <MetricRow label="Реклама" value={fmtMoney(a.now.ad_spend)} />
            <MetricRow label="ДРР" value={`${a.now.drr}%`} color={a.now.drr <= 20 ? 'text-emerald-400' : a.now.drr <= 35 ? 'text-amber-400' : 'text-red-400'} />
            {a.now.roi !== 0 && <MetricRow label="ROI" value={`${a.now.roi}%`} color={a.now.roi >= 200 ? 'text-emerald-400' : a.now.roi >= 100 ? 'text-amber-400' : 'text-red-400'} />}
          </div>
        </div>

        {/* FORECAST */}
        <div className="rounded-lg bg-[hsl(var(--card)/0.7)] border border-[hsl(var(--border)/0.15)] p-3">
          <div className="text-[10px] uppercase tracking-wider text-[hsl(var(--muted-foreground))] font-semibold mb-2">🔮 Прогноз</div>
          <div className="space-y-1.5">
            <MetricRow label="Прибыль" value={fmtMoney(a.forecast.profit)} color={a.forecast.profit >= 0 ? 'text-emerald-400' : 'text-red-400'} bold />
            <MetricRow label="Маржа" value={`${a.forecast.margin_pct}%`} color={a.forecast.margin_pct >= 10 ? 'text-emerald-400' : a.forecast.margin_pct >= 0 ? 'text-amber-400' : 'text-red-400'} />
            <MetricRow label="Выручка" value={fmtMoney(a.forecast.revenue)} />
            <MetricRow label="Реклама" value={fmtMoney(a.forecast.ad_spend)} />
            <MetricRow label="ДРР" value={`${a.forecast.drr}%`} color={a.forecast.drr <= 20 ? 'text-emerald-400' : a.forecast.drr <= 35 ? 'text-amber-400' : 'text-red-400'} />
            {/* Delta indicators */}
            {a.forecast.profit !== a.now.profit && (
              <div className="mt-1 pt-1 border-t border-[hsl(var(--border)/0.1)]">
                <span className={`text-[10px] font-semibold ${a.forecast.profit > a.now.profit ? 'text-emerald-400' : 'text-red-400'}`}>
                  {a.forecast.profit > a.now.profit ? '↑' : '↓'} Прибыль {a.forecast.profit > a.now.profit ? '+' : ''}{fmtMoney(a.forecast.profit - a.now.profit)}
                </span>
              </div>
            )}
          </div>
        </div>

        {/* ACTIONS */}
        <div className="rounded-lg bg-[hsl(var(--card)/0.7)] border border-[hsl(var(--border)/0.15)] p-3">
          <div className="text-[10px] uppercase tracking-wider text-[hsl(var(--muted-foreground))] font-semibold mb-2">🎯 Что делать</div>
          <div className="space-y-2">
            {a.actions.map((act, i) => (
              <div key={i} className="text-xs">
                <div className="font-medium">{i + 1}. {act.text}</div>
                <div className="text-emerald-400/80 font-semibold mt-0.5">→ {act.profit_impact}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

/* Metric row helper */
function MetricRow({ label, value, color, bold }: { label: string; value: string; color?: string; bold?: boolean }) {
  return (
    <div className="flex justify-between items-center text-xs">
      <span className="text-[hsl(var(--muted-foreground)/0.7)]">{label}</span>
      <span className={`${color || ''} ${bold ? 'font-bold text-sm' : 'font-medium'}`}>{value}</span>
    </div>
  )
}

/* ═══════════════════════════════════════ */
/*          FORECAST PAGE                 */
/* ═══════════════════════════════════════ */

export default function ForecastPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const [data, setData] = useState<ForecastResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [period, setPeriod] = useState(120)
  const [forecastDays, setForecastDays] = useState(30)
  const [chartMetric, setChartMetric] = useState<'revenue' | 'orders' | 'profit'>('revenue')

  const load = useCallback(async () => {
    if (!currentShop) return
    setLoading(true)
    try {
      const fetchFn = currentShop.marketplace === 'wildberries' ? fetchWbForecast : fetchOzonForecast
      const res = await fetchFn(currentShop.id, period, forecastDays)
      setData(res)
    } catch (e) {
      console.error('Forecast fetch error', e)
    } finally {
      setLoading(false)
    }
  }, [currentShop, period, forecastDays])

  useEffect(() => { load() }, [load])

  /* Combined chart data */
  const chartData = useMemo(() => {
    if (!data) return []
    const hist = data.history.map(h => ({
      date: h.date,
      label: fmtDate(h.date),
      revenue: h.revenue,
      orders: h.orders,
      profit: null as number | null,
      forecast: null as number | null,
    }))

    if (hist.length > 0 && data.overall.forecast.length > 0) {
      const last = hist[hist.length - 1]
      const bridgeVal = chartMetric === 'revenue' ? last.revenue : last.orders
      hist[hist.length - 1] = { ...last, forecast: bridgeVal }
    }

    const fore = data.overall.forecast.map(f => ({
      date: f.date,
      label: fmtDate(f.date),
      revenue: null as number | null,
      orders: null as number | null,
      profit: null as number | null,
      forecast: f[chartMetric] ?? f.revenue,
    }))

    return [...hist, ...fore]
  }, [data, chartMetric])

  if (!currentShop) {
    return <div className="flex items-center justify-center h-64 text-[hsl(var(--muted-foreground))]">Выберите магазин</div>
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 gap-3">
        <div className="h-5 w-5 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
        <span className="text-sm text-[hsl(var(--muted-foreground))]">Загрузка прогноза… (до 15 сек)</span>
      </div>
    )
  }

  if (!data || !data.history.length) {
    return <div className="flex items-center justify-center h-64 text-[hsl(var(--muted-foreground))]">Нет данных для прогноза</div>
  }

  const { overall, recommendation_summary: recSum } = data

  // Sort products: critical first, then warning, opportunity, ok
  const sevOrder = { critical: 0, warning: 1, opportunity: 2, ok: 3 }
  const sortedProducts = [...data.products].sort((a, b) => {
    const sa = sevOrder[a.analysis.severity] ?? 9
    const sb = sevOrder[b.analysis.severity] ?? 9
    if (sa !== sb) return sa - sb
    return b.history_totals.revenue - a.history_totals.revenue
  })

  return (
    <div className="space-y-6 p-6">
      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Прогноз продаж</h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            Bottom-up прогноз × {data.products.length} SKU • {period} дн. истории → {forecastDays} дн. прогноз
          </p>
        </div>
        <div className="flex gap-2">
          <select
            value={period}
            onChange={e => setPeriod(Number(e.target.value))}
            className="rounded-lg border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] px-3 py-1.5 text-sm focus:outline-none"
          >
            <option value={30}>30 дней</option>
            <option value={60}>60 дней</option>
            <option value={90}>90 дней</option>
            <option value={120}>120 дней</option>
            <option value={180}>180 дней</option>
            <option value={365}>365 дней</option>
          </select>
          <select
            value={forecastDays}
            onChange={e => setForecastDays(Number(e.target.value))}
            className="rounded-lg border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] px-3 py-1.5 text-sm focus:outline-none"
          >
            <option value={7}>+7 дней</option>
            <option value={14}>+14 дней</option>
            <option value={30}>+30 дней</option>
            <option value={60}>+60 дней</option>
            <option value={90}>+90 дней</option>
          </select>
        </div>
      </div>

      {/* ── KPI Cards ── */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <KpiCard
          label="Прогноз выручки"
          value={fmtMoney(overall.totals.revenue)}
          sub={`за ${forecastDays} дн.`}
          trend={overall.trend.revenue_slope_pct}
          color="hsl(217, 91%, 60%)"
        />
        <KpiCard
          label="Прогноз заказов"
          value={fmtNum(overall.totals.orders)}
          sub={`за ${forecastDays} дн.`}
          color="hsl(142, 71%, 45%)"
        />
        <KpiCard
          label="Прогноз прибыли"
          value={fmtMoney(overall.totals.profit)}
          sub={`маржа ${overall.totals.margin_pct}%`}
          color={overall.totals.profit >= 0 ? 'hsl(142, 71%, 45%)' : 'hsl(0, 84%, 60%)'}
        />
        <KpiCard
          label="Рекл. расходы"
          value={fmtMoney(overall.totals.ad_spend)}
          sub={`за ${forecastDays} дн.`}
          color="hsl(38, 92%, 50%)"
        />
        <KpiCard
          label="Статус SKU"
          value={`${recSum.critical || 0} / ${recSum.warning || 0} / ${recSum.opportunity || 0}`}
          sub="🔴 крит. / 🟡 вним. / 🟢 рост"
          color="hsl(var(--foreground))"
        />
      </div>

      {/* ── Chart ── */}
      <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold">Тренд и прогноз</h2>
          <div className="flex gap-1 rounded-lg bg-[hsl(var(--muted)/0.3)] p-0.5">
            {(['revenue', 'orders', 'profit'] as const).map(m => (
              <button
                key={m}
                onClick={() => setChartMetric(m)}
                className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${
                  chartMetric === m
                    ? (m === 'revenue' ? 'bg-blue-500 text-white' : m === 'orders' ? 'bg-emerald-500 text-white' : 'bg-purple-500 text-white')
                    : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
                }`}
              >{m === 'revenue' ? 'Выручка' : m === 'orders' ? 'Заказы' : 'Прибыль'}</button>
            ))}
          </div>
        </div>

        <ResponsiveContainer width="100%" height={360}>
          <ComposedChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
            <defs>
              <linearGradient id="histGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={chartMetric === 'revenue' ? '#3b82f6' : chartMetric === 'orders' ? '#22c55e' : '#a855f7'} stopOpacity={0.3} />
                <stop offset="100%" stopColor={chartMetric === 'revenue' ? '#3b82f6' : chartMetric === 'orders' ? '#22c55e' : '#a855f7'} stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border)/0.15)" />
            <XAxis
              dataKey="label"
              tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 11 }}
              interval={Math.max(Math.floor(chartData.length / 12), 0)}
            />
            <YAxis
              tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 11 }}
              tickFormatter={v => chartMetric !== 'orders' ? `${(v / 1000).toFixed(0)}K` : String(v)}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border)/0.3)',
                borderRadius: 8,
                fontSize: 12,
              }}
              formatter={(value: any, name: string) => {
                if (value == null) return [null, null]
                const v = Number(value)
                const label = name === 'forecast' ? 'Прогноз' : chartMetric === 'revenue' ? 'Выручка' : chartMetric === 'orders' ? 'Заказы' : 'Прибыль'
                return [chartMetric !== 'orders' ? fmtMoney(v) : fmtNum(v), label]
              }}
            />
            <Legend
              formatter={(value: string) => {
                const labels: Record<string, string> = {
                  revenue: 'Выручка (факт)', orders: 'Заказы (факт)', profit: 'Прибыль (факт)',
                  forecast: 'Прогноз',
                }
                return labels[value] || value
              }}
            />
            <Area
              dataKey={chartMetric}
              stroke={chartMetric === 'revenue' ? '#3b82f6' : chartMetric === 'orders' ? '#22c55e' : '#a855f7'}
              strokeWidth={2}
              fill="url(#histGrad)"
              dot={false}
              name={chartMetric}
            />
            <Line
              dataKey="forecast"
              stroke="#f59e0b"
              strokeWidth={2}
              strokeDasharray="6 3"
              dot={false}
              name="forecast"
              connectNulls={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* ── Analysis: Сейчас → Будет → Делай ── */}
      <div>
        <h2 className="text-lg font-bold mb-4">🤖 Анализ по SKU: Сейчас → Будет → Делай</h2>
        <div className="space-y-4">
          {sortedProducts.map(p => (
            <SkuAnalysisCard key={p.sku} product={p} />
          ))}
        </div>
      </div>
    </div>
  )
}
