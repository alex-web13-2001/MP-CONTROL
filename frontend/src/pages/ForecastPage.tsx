import { useState, useEffect, useCallback, useMemo } from 'react'
import {
  fetchOzonForecast,
  type ForecastResponse,
  type ForecastProduct,
  type Recommendation,
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

/* ── Recommendation badge ── */
const recColors = {
  critical: { bg: 'bg-red-500/15', text: 'text-red-400', icon: '🔴' },
  warning: { bg: 'bg-amber-500/15', text: 'text-amber-400', icon: '🟡' },
  opportunity: { bg: 'bg-emerald-500/15', text: 'text-emerald-400', icon: '🟢' },
  info: { bg: 'bg-blue-500/15', text: 'text-blue-400', icon: 'ℹ️' },
}

function RecBadge({ rec }: { rec: Recommendation }) {
  const c = recColors[rec.type] || recColors.info
  return (
    <div className={`${c.bg} rounded-lg px-3 py-2 text-xs`}>
      <div className={`${c.text} font-semibold`}>{c.icon} {rec.message}</div>
      <div className="text-[hsl(var(--muted-foreground))] mt-0.5">→ {rec.action}</div>
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
      const res = await fetchOzonForecast(currentShop.id, period, forecastDays)
      setData(res)
    } catch (e) {
      console.error('Forecast fetch error', e)
    } finally {
      setLoading(false)
    }
  }, [currentShop, period, forecastDays])

  useEffect(() => { load() }, [load])

  /* Combined chart data: history + forecast */
  const chartData = useMemo(() => {
    if (!data) return []
    const hist = data.history.map(h => ({
      date: h.date,
      label: fmtDate(h.date),
      revenue: h.revenue,
      orders: h.orders,
      profit: null as number | null,
      forecast: null as number | null,
      forecastLow: null as number | null,
      forecastHigh: null as number | null,
    }))

    // Bridge: last history point as first forecast point
    if (hist.length > 0 && data.overall.forecast.length > 0) {
      const last = hist[hist.length - 1]
      const bridgeVal = chartMetric === 'revenue' ? last.revenue : last.orders
      hist[hist.length - 1] = {
        ...last,
        forecast: bridgeVal,
      }
    }

    const fore = data.overall.forecast.map(f => ({
      date: f.date,
      label: fmtDate(f.date),
      revenue: null as number | null,
      orders: null as number | null,
      profit: null as number | null,
      forecast: f[chartMetric] ?? f.revenue,
      forecastLow: null as number | null,
      forecastHigh: null as number | null,
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
        <span className="text-sm text-[hsl(var(--muted-foreground))]">Загрузка прогноза… (может занять до 30 сек)</span>
      </div>
    )
  }

  if (!data || !data.history.length) {
    return <div className="flex items-center justify-center h-64 text-[hsl(var(--muted-foreground))]">Нет данных для прогноза</div>
  }

  const { overall, recommendation_summary: recSum } = data

  return (
    <div className="space-y-6 p-6">
      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Прогноз продаж</h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            Bottom-up прогноз на основе {period} дней × {data.products.length} SKU
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
          label="Рекомендации"
          value={`${recSum.critical || 0} / ${recSum.warning || 0} / ${recSum.opportunity || 0}`}
          sub="🔴 / 🟡 / 🟢"
          color="hsl(var(--foreground))"
        />
      </div>

      {/* ── Chart ── */}
      <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold">Тренд и прогноз (bottom-up)</h2>
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

      {/* ── Recommendations summary ── */}
      {data.products.some(p => p.recommendations.length > 0) && (
        <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] p-6">
          <h2 className="text-lg font-semibold mb-4">🤖 AI-рекомендации по SKU</h2>
          <div className="space-y-3">
            {data.products
              .filter(p => p.recommendations.length > 0)
              .map(p => (
                <div key={p.sku} className="flex gap-3 items-start">
                  <div className="flex items-center gap-2 min-w-[220px] flex-shrink-0">
                    {p.image_url && <img src={p.image_url} alt="" className="w-6 h-6 rounded object-cover" />}
                    <div className="min-w-0">
                      <span className="text-xs font-medium truncate block max-w-[180px]">{p.name || p.offer_id}</span>
                      <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.6)]">{p.offer_id}</span>
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {p.recommendations.map((r, i) => (
                      <RecBadge key={i} rec={r} />
                    ))}
                  </div>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* ── Products table ── */}
      <ProductsTable products={data.products} forecastDays={forecastDays} />
    </div>
  )
}


/* ═══════════════════════════════════════ */
/*        PRODUCTS TABLE                  */
/* ═══════════════════════════════════════ */

function ProductsTable({ products, forecastDays }: { products: ForecastProduct[]; forecastDays: number }) {
  const [sortKey, setSortKey] = useState<string>('history_totals.revenue')
  const [sortDesc, setSortDesc] = useState(true)

  const sorted = useMemo(() => {
    return [...products].sort((a, b) => {
      let va = 0, vb = 0
      if (sortKey === 'history_totals.revenue') { va = a.history_totals.revenue; vb = b.history_totals.revenue }
      else if (sortKey === 'history_totals.orders') { va = a.history_totals.orders; vb = b.history_totals.orders }
      else if (sortKey === 'history_totals.profit') { va = a.history_totals.profit; vb = b.history_totals.profit }
      else if (sortKey === 'history_totals.margin_pct') { va = a.history_totals.margin_pct; vb = b.history_totals.margin_pct }
      else if (sortKey === 'history_totals.roi') { va = a.history_totals.roi; vb = b.history_totals.roi }
      else if (sortKey === 'forecast_totals.revenue') { va = a.forecast_totals.revenue; vb = b.forecast_totals.revenue }
      else if (sortKey === 'forecast_totals.profit') { va = a.forecast_totals.profit; vb = b.forecast_totals.profit }
      else if (sortKey === 'forecast_totals.margin_pct') { va = a.forecast_totals.margin_pct; vb = b.forecast_totals.margin_pct }
      else if (sortKey === 'recommendations') { va = a.recommendations.length; vb = b.recommendations.length }
      return sortDesc ? vb - va : va - vb
    })
  }, [products, sortKey, sortDesc])

  const toggleSort = (key: string) => {
    if (sortKey === key) setSortDesc(!sortDesc)
    else { setSortKey(key); setSortDesc(true) }
  }

  const thCls = 'px-3 py-2 text-right font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase tracking-wider cursor-pointer hover:text-[hsl(var(--foreground))] transition select-none'
  const tdCls = 'px-3 py-2.5 text-right text-sm whitespace-nowrap'

  const SortIcon = ({ k }: { k: string }) => (
    sortKey === k ? <span className="ml-0.5">{sortDesc ? '↓' : '↑'}</span> : null
  )

  return (
    <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] overflow-hidden">
      <div className="p-5 border-b border-[hsl(var(--border)/0.2)]">
        <h2 className="text-lg font-semibold">📊 Прогноз по товарам</h2>
        <p className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5">
          {products.length} SKU • факт за {products.length > 0 ? 'период' : '—'} + прогноз на {forecastDays} дн.
        </p>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[hsl(var(--border)/0.2)]">
              <th className="px-3 py-2 text-left font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase tracking-wider" style={{ position: 'sticky', left: 0, backgroundColor: 'hsl(var(--card))', zIndex: 10, minWidth: 200 }}>Товар</th>
              <th className={thCls} onClick={() => toggleSort('history_totals.revenue')}>Выручка<SortIcon k="history_totals.revenue" /></th>
              <th className={thCls} onClick={() => toggleSort('history_totals.orders')}>Заказы<SortIcon k="history_totals.orders" /></th>
              <th className={thCls} onClick={() => toggleSort('history_totals.profit')}>Прибыль<SortIcon k="history_totals.profit" /></th>
              <th className={thCls} onClick={() => toggleSort('history_totals.margin_pct')}>Маржа<SortIcon k="history_totals.margin_pct" /></th>
              <th className={thCls} onClick={() => toggleSort('history_totals.roi')}>ROI<SortIcon k="history_totals.roi" /></th>
              <th className="px-3 py-2 text-center font-medium text-xs text-blue-400 uppercase tracking-wider">—</th>
              <th className={`${thCls} !text-emerald-400`} onClick={() => toggleSort('forecast_totals.revenue')}>→ Выручка<SortIcon k="forecast_totals.revenue" /></th>
              <th className={`${thCls} !text-emerald-400`} onClick={() => toggleSort('forecast_totals.profit')}>→ Прибыль<SortIcon k="forecast_totals.profit" /></th>
              <th className={`${thCls} !text-emerald-400`} onClick={() => toggleSort('forecast_totals.margin_pct')}>→ Маржа<SortIcon k="forecast_totals.margin_pct" /></th>
              <th className={thCls} onClick={() => toggleSort('recommendations')}>Рекоменд.<SortIcon k="recommendations" /></th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(p => {
              const h = p.history_totals
              const f = p.forecast_totals
              const critCount = p.recommendations.filter(r => r.type === 'critical').length
              const warnCount = p.recommendations.filter(r => r.type === 'warning').length
              const oppCount = p.recommendations.filter(r => r.type === 'opportunity').length

              return (
                <tr key={p.sku} className="border-b border-[hsl(var(--border)/0.1)] hover:bg-[hsl(var(--muted)/0.15)] transition-colors">
                  {/* Product */}
                  <td className="px-3 py-2" style={{ position: 'sticky', left: 0, backgroundColor: 'hsl(var(--card))', zIndex: 5 }}>
                    <div className="flex items-center gap-2.5">
                      {p.image_url ? (
                        <img src={p.image_url} alt="" className="w-8 h-8 rounded object-cover flex-shrink-0" />
                      ) : (
                        <div className="w-8 h-8 rounded bg-[hsl(var(--muted)/0.3)] flex-shrink-0" />
                      )}
                      <div className="min-w-0">
                        <div className="text-sm font-medium truncate max-w-[180px]">{p.name || p.offer_id}</div>
                        <div className="text-[10px] text-[hsl(var(--muted-foreground)/0.6)]">{p.offer_id}</div>
                      </div>
                    </div>
                  </td>
                  {/* Historical */}
                  <td className={tdCls}>{fmtMoney(h.revenue)}</td>
                  <td className={tdCls}>{fmtNum(h.orders)}</td>
                  <td className={tdCls}>
                    <span className={h.profit >= 0 ? 'text-emerald-400' : 'text-red-400'}>{fmtMoney(h.profit)}</span>
                  </td>
                  <td className={tdCls}>
                    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${
                      h.margin_pct >= 20 ? 'bg-emerald-500/15 text-emerald-400' :
                      h.margin_pct >= 5 ? 'bg-amber-500/15 text-amber-400' :
                      'bg-red-500/15 text-red-400'
                    }`}>{h.margin_pct}%</span>
                  </td>
                  <td className={tdCls}>
                    {h.roi !== 0 ? (
                      <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${
                        h.roi >= 200 ? 'bg-emerald-500/15 text-emerald-400' :
                        h.roi >= 100 ? 'bg-amber-500/15 text-amber-400' :
                        'bg-red-500/15 text-red-400'
                      }`}>{h.roi}%</span>
                    ) : '—'}
                  </td>
                  {/* Separator */}
                  <td className="px-1 text-center text-[hsl(var(--border)/0.4)]">│</td>
                  {/* Forecast */}
                  <td className={`${tdCls} text-emerald-400/80`}>{fmtMoney(f.revenue)}</td>
                  <td className={tdCls}>
                    <span className={f.profit >= 0 ? 'text-emerald-400' : 'text-red-400'}>{fmtMoney(f.profit)}</span>
                  </td>
                  <td className={tdCls}>
                    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${
                      f.margin_pct >= 20 ? 'bg-emerald-500/15 text-emerald-400' :
                      f.margin_pct >= 5 ? 'bg-amber-500/15 text-amber-400' :
                      'bg-red-500/15 text-red-400'
                    }`}>{f.margin_pct}%</span>
                  </td>
                  {/* Recommendations */}
                  <td className="px-3 py-2 text-center">
                    <div className="flex justify-center gap-1">
                      {critCount > 0 && <span className="text-xs" title={`${critCount} критичных`}>🔴{critCount > 1 ? critCount : ''}</span>}
                      {warnCount > 0 && <span className="text-xs" title={`${warnCount} предупреждений`}>🟡{warnCount > 1 ? warnCount : ''}</span>}
                      {oppCount > 0 && <span className="text-xs" title={`${oppCount} возможностей`}>🟢{oppCount > 1 ? oppCount : ''}</span>}
                      {p.recommendations.length === 0 && <span className="text-xs text-[hsl(var(--muted-foreground)/0.4)]">—</span>}
                    </div>
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
