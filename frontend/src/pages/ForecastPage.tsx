import { useState, useEffect, useCallback, useMemo } from 'react'
import {
  fetchOzonForecast,
  fetchOzonSkuForecast,
  type ForecastResponse,
  type ForecastProduct,
  type SkuForecastResponse,
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

/* ═══════════════════════════════════════ */
/*          FORECAST PAGE                 */
/* ═══════════════════════════════════════ */

export default function ForecastPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const [data, setData] = useState<ForecastResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [period, setPeriod] = useState(120)
  const [forecastDays, setForecastDays] = useState(30)
  const [chartMetric, setChartMetric] = useState<'revenue' | 'orders'>('revenue')

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
      forecast: null as number | null,
      forecastLow: null as number | null,
      forecastHigh: null as number | null,
    }))

    // Bridge: last history point as first forecast point
    if (hist.length > 0) {
      const last = hist[hist.length - 1]
      const bridgeVal = chartMetric === 'revenue' ? last.revenue : last.orders
      hist[hist.length - 1] = {
        ...last,
        forecast: bridgeVal,
        forecastLow: bridgeVal,
        forecastHigh: bridgeVal,
      }
    }

    const fore = data.forecast.map(f => ({
      date: f.date,
      label: fmtDate(f.date),
      revenue: null as number | null,
      orders: null as number | null,
      forecast: chartMetric === 'revenue' ? f.revenue : f.orders,
      forecastLow: chartMetric === 'revenue' ? f.revenue_low : f.orders_low,
      forecastHigh: chartMetric === 'revenue' ? f.revenue_high : f.orders_high,
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
        <span className="text-sm text-[hsl(var(--muted-foreground))]">Загрузка прогноза…</span>
      </div>
    )
  }

  if (!data || !data.history.length) {
    return <div className="flex items-center justify-center h-64 text-[hsl(var(--muted-foreground))]">Нет данных для прогноза</div>
  }

  const { trend } = data

  return (
    <div className="space-y-6 p-6">
      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Прогноз продаж</h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            Тренд и симулятор на основе {period} дней данных
          </p>
        </div>
        <div className="flex gap-2">
          {/* Period selector */}
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
          {/* Forecast days */}
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
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Прогноз выручки"
          value={fmtMoney(trend.forecast_revenue)}
          sub={`за ${forecastDays} дн.`}
          trend={trend.revenue_slope_pct}
          color="hsl(217, 91%, 60%)"
        />
        <KpiCard
          label="Прогноз заказов"
          value={fmtNum(trend.forecast_orders)}
          sub={`за ${forecastDays} дн.`}
          trend={trend.orders_slope_pct}
          color="hsl(142, 71%, 45%)"
        />
        <KpiCard
          label="Тренд выручки"
          value={`${trend.revenue_slope_pct > 0 ? '+' : ''}${trend.revenue_slope_pct}%`}
          sub="в день"
          color={trend.revenue_slope_pct >= 0 ? 'hsl(142, 71%, 45%)' : 'hsl(0, 84%, 60%)'}
        />
        <KpiCard
          label="Направление"
          value={trend.direction === 'up' ? '📈 Рост' : trend.direction === 'down' ? '📉 Спад' : '➡️ Стабильно'}
          color="hsl(var(--foreground))"
        />
      </div>

      {/* ── Chart ── */}
      <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold">Тренд и прогноз</h2>
          <div className="flex gap-1 rounded-lg bg-[hsl(var(--muted)/0.3)] p-0.5">
            <button
              onClick={() => setChartMetric('revenue')}
              className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${
                chartMetric === 'revenue'
                  ? 'bg-blue-500 text-white shadow-sm'
                  : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
              }`}
            >Выручка</button>
            <button
              onClick={() => setChartMetric('orders')}
              className={`px-3 py-1 rounded-md text-xs font-medium transition-all ${
                chartMetric === 'orders'
                  ? 'bg-emerald-500 text-white shadow-sm'
                  : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
              }`}
            >Заказы</button>
          </div>
        </div>

        <ResponsiveContainer width="100%" height={360}>
          <ComposedChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
            <defs>
              <linearGradient id="histGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={chartMetric === 'revenue' ? '#3b82f6' : '#22c55e'} stopOpacity={0.3} />
                <stop offset="100%" stopColor={chartMetric === 'revenue' ? '#3b82f6' : '#22c55e'} stopOpacity={0.02} />
              </linearGradient>
              <linearGradient id="foreGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#f59e0b" stopOpacity={0.2} />
                <stop offset="100%" stopColor="#f59e0b" stopOpacity={0.02} />
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
              tickFormatter={v => chartMetric === 'revenue' ? `${(v / 1000).toFixed(0)}K` : String(v)}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border)/0.3)',
                borderRadius: 8,
                fontSize: 12,
              }}
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              formatter={(value: any, name: string) => {
                if (value == null || name === 'forecastBand' || name === 'forecastLow') return [null, null]
                const v = Number(value)
                const label = name === 'revenue' ? 'Выручка' : name === 'orders' ? 'Заказы' : 'Прогноз'
                return [chartMetric === 'revenue' ? fmtMoney(v) : fmtNum(v), label]
              }}
              labelFormatter={(label: string) => label}
            />
            <Legend
              formatter={(value: string) => {
                const labels: Record<string, string> = {
                  revenue: 'Выручка (факт)', orders: 'Заказы (факт)',
                  forecast: 'Прогноз', forecastBand: 'Доверительный коридор',
                }
                return labels[value] || value
              }}
            />

            {/* Confidence band */}
            <Area
              dataKey="forecastHigh"
              stroke="none"
              fill="url(#foreGrad)"
              fillOpacity={1}
              name="forecastBand"
              legendType="square"
              dot={false}
              activeDot={false}
              isAnimationActive={false}
            />
            <Area
              dataKey="forecastLow"
              stroke="none"
              fill="hsl(var(--card))"
              fillOpacity={1}
              legendType="none"
              dot={false}
              activeDot={false}
              isAnimationActive={false}
            />

            {/* History */}
            <Area
              dataKey={chartMetric}
              stroke={chartMetric === 'revenue' ? '#3b82f6' : '#22c55e'}
              strokeWidth={2}
              fill="url(#histGrad)"
              dot={false}
              name={chartMetric}
            />

            {/* Forecast line */}
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

      {/* ── Simulator ── */}
      <SimulatorTable products={data.products} />

      {/* ── Per-SKU LightGBM Forecast ── */}
      <SkuForecastSection shopId={currentShop.id} period={period} forecastDays={forecastDays} />
    </div>
  )
}


/* ═══════════════════════════════════════ */
/*    SKU FORECAST SECTION (LightGBM)     */
/* ═══════════════════════════════════════ */

const FEAT_LABELS: Record<string, string> = {
  orders_lag1: 'Заказы (вчера)',
  orders_lag2: 'Заказы (2 дн. назад)',
  orders_lag3: 'Заказы (3 дн. назад)',
  orders_lag7: 'Заказы (7 дн. назад)',
  orders_ma7: 'Сред. заказы (7 дн.)',
  orders_ma3: 'Сред. заказы (3 дн.)',
  revenue_lag1: 'Выручка (вчера)',
  revenue_lag7: 'Выручка (7 дн. назад)',
  views_lag1: 'Показы (вчера)',
  views_lag2: 'Показы (2 дн. назад)',
  views_lag3: 'Показы (3 дн. назад)',
  clicks_lag1: 'Клики (вчера)',
  clicks_lag2: 'Клики (2 дн. назад)',
  carts_lag1: 'Корзины (вчера)',
  carts_lag2: 'Корзины (2 дн. назад)',
  ad_spend_lag1: 'Реклама (вчера)',
  ad_spend_lag2: 'Реклама (2 дн. назад)',
  day_of_week: 'День недели',
  is_weekend: 'Выходной',
  ctr_lag1: 'CTR (вчера)',
  cart_rate_lag1: '% корзин (вчера)',
}

function SkuForecastSection({ shopId, period, forecastDays }: {
  shopId: number; period: number; forecastDays: number
}) {
  const [data, setData] = useState<SkuForecastResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [selectedSku, setSelectedSku] = useState<number | null>(null)
  const [expanded, setExpanded] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    fetchOzonSkuForecast(shopId, period, forecastDays)
      .then(r => { if (!cancelled) { setData(r); if (r.sku_forecasts.length) setSelectedSku(r.sku_forecasts[0].sku) } })
      .catch(e => console.error('SKU forecast error', e))
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [shopId, period, forecastDays])

  const activeForecast = data?.sku_forecasts.find(f => f.sku === selectedSku) ?? null

  const chartData = useMemo(() => {
    if (!activeForecast) return []
    const hist = activeForecast.history.map(h => ({
      date: h.ds,
      label: fmtDate(h.ds),
      orders: h.orders,
      forecast: null as number | null,
      forecastLow: null as number | null,
      forecastHigh: null as number | null,
    }))

    // Bridge
    if (hist.length > 0) {
      const last = hist[hist.length - 1]
      hist[hist.length - 1] = { ...last, forecast: last.orders, forecastLow: last.orders, forecastHigh: last.orders }
    }

    const fore = activeForecast.forecast.map(f => ({
      date: f.date,
      label: fmtDate(f.date),
      orders: null as number | null,
      forecast: f.orders,
      forecastLow: f.orders_low,
      forecastHigh: f.orders_high,
    }))

    return [...hist, ...fore]
  }, [activeForecast])

  const featData = useMemo(() => {
    if (!activeForecast?.feature_importance) return []
    return Object.entries(activeForecast.feature_importance)
      .slice(0, 10)
      .map(([key, val]) => ({
        name: FEAT_LABELS[key] || key,
        value: val,
      }))
  }, [activeForecast])

  return (
    <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] p-6 mt-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-lg font-bold">🤖 Прогноз по SKU (LightGBM)</h2>
          <p className="text-xs text-[hsl(var(--muted-foreground))]">
            Учитывает воронку: показы → клики → корзины → заказы (с задержкой 1-3 дня)
          </p>
        </div>
        <button onClick={() => setExpanded(!expanded)}
          className="text-xs px-3 py-1 rounded-lg bg-[hsl(var(--muted)/0.3)] hover:bg-[hsl(var(--muted)/0.5)] transition">
          {expanded ? 'Свернуть' : 'Развернуть'}
        </button>
      </div>

      {!expanded ? null : loading ? (
        <div className="flex items-center justify-center h-32 gap-2">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-purple-500 border-t-transparent" />
          <span className="text-sm text-[hsl(var(--muted-foreground))]">LightGBM модель обучается…</span>
        </div>
      ) : !data || data.sku_forecasts.length === 0 ? (
        <div className="text-center text-sm text-[hsl(var(--muted-foreground))] py-8">Нет данных для SKU прогноза</div>
      ) : (
        <div>
          {/* SKU Selector */}
          <div className="flex flex-wrap gap-2 mb-5">
            {data.sku_forecasts.map(sf => (
              <button
                key={sf.sku}
                onClick={() => setSelectedSku(sf.sku)}
                className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs transition border ${
                  selectedSku === sf.sku
                    ? 'bg-purple-500/20 border-purple-500/60 text-purple-300'
                    : 'bg-[hsl(var(--muted)/0.15)] border-transparent hover:bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))]'
                }`}
              >
                {sf.image_url && <img src={sf.image_url} className="w-5 h-5 rounded object-cover" alt="" />}
                <span className="truncate max-w-[120px]">{sf.name || sf.offer_id}</span>
              </button>
            ))}
          </div>

          {activeForecast && (
            <>
              {/* KPI Row */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-5">
                <KpiCard
                  label="Прогноз заказов"
                  value={fmtNum(activeForecast.totals.orders)}
                  sub={`за ${forecastDays} дн.`}
                  trend={activeForecast.trend.slope_pct}
                  color="#a855f7"
                />
                <KpiCard
                  label="Прогноз выручки"
                  value={fmtMoney(activeForecast.totals.revenue)}
                  sub={`за ${forecastDays} дн.`}
                  color="#3b82f6"
                />
                <KpiCard
                  label="Прогноз прибыли"
                  value={fmtMoney(activeForecast.totals.profit)}
                  sub={`за ${forecastDays} дн.`}
                  color={activeForecast.totals.profit >= 0 ? '#22c55e' : '#ef4444'}
                />
                <KpiCard
                  label="Направление"
                  value={activeForecast.trend.direction === 'up' ? '📈 Рост' : activeForecast.trend.direction === 'down' ? '📉 Спад' : '➡️ Стабильно'}
                  color="#f59e0b"
                />
              </div>

              {/* Chart + Feature Importance */}
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                {/* Orders Chart */}
                <div className="lg:col-span-2 rounded-xl border border-[hsl(var(--border)/0.15)] bg-[hsl(var(--card)/0.5)] p-4">
                  <h3 className="text-sm font-semibold mb-3">Заказы — история + прогноз</h3>
                  <ResponsiveContainer width="100%" height={260}>
                    <ComposedChart data={chartData}>
                      <defs>
                        <linearGradient id="skuHistGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#a855f7" stopOpacity={0.3} />
                          <stop offset="95%" stopColor="#a855f7" stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border)/0.15)" />
                      <XAxis dataKey="label" tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }} interval="preserveStartEnd" />
                      <YAxis tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10 }} />
                      <Tooltip
                        contentStyle={{ background: 'hsl(var(--card))', border: '1px solid hsl(var(--border)/0.3)', borderRadius: 8 }}
                        labelStyle={{ color: 'hsl(var(--muted-foreground))' }}
                        formatter={(value: any, name: string) => {
                          if (name === 'forecastLow' || name === 'forecastHigh') return [null, null]
                          const label = name === 'orders' ? 'Заказы (факт)' : name === 'forecast' ? 'Прогноз' : name
                          return [Math.round(value as number), label]
                        }}
                      />
                      {/* Confidence band */}
                      <Area dataKey="forecastHigh" stroke="none" fill="#a855f7" fillOpacity={0.1} legendType="none" dot={false} activeDot={false} isAnimationActive={false} />
                      <Area dataKey="forecastLow" stroke="none" fill="hsl(var(--card))" fillOpacity={1} legendType="none" dot={false} activeDot={false} isAnimationActive={false} />
                      {/* History */}
                      <Area dataKey="orders" stroke="#a855f7" strokeWidth={2} fill="url(#skuHistGrad)" dot={false} name="orders" />
                      {/* Forecast */}
                      <Line dataKey="forecast" stroke="#f59e0b" strokeWidth={2} strokeDasharray="6 3" dot={false} name="forecast" connectNulls={false} />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>

                {/* Feature Importance */}
                <div className="rounded-xl border border-[hsl(var(--border)/0.15)] bg-[hsl(var(--card)/0.5)] p-4">
                  <h3 className="text-sm font-semibold mb-3">🧬 Влияние факторов</h3>
                  {featData.length === 0 ? (
                    <p className="text-xs text-[hsl(var(--muted-foreground))]">Нет данных</p>
                  ) : (
                    <div className="space-y-1.5">
                      {featData.map(f => (
                        <div key={f.name}>
                          <div className="flex justify-between text-xs mb-0.5">
                            <span className="text-[hsl(var(--muted-foreground))] truncate mr-2">{f.name}</span>
                            <span className="font-medium text-purple-300">{f.value}%</span>
                          </div>
                          <div className="h-1.5 rounded-full bg-[hsl(var(--muted)/0.2)] overflow-hidden">
                            <div
                              className="h-full rounded-full bg-gradient-to-r from-purple-500 to-blue-500 transition-all duration-500"
                              style={{ width: `${Math.min(f.value * 2, 100)}%` }}
                            />
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>

              {/* Profit Breakdown */}
              {activeForecast.forecast.length > 0 && activeForecast.forecast[0].profit !== undefined && (
                <div className="mt-4 rounded-xl border border-[hsl(var(--border)/0.15)] bg-[hsl(var(--card)/0.5)] p-4">
                  <h3 className="text-sm font-semibold mb-3">💰 Структура прибыли (прогноз за {forecastDays} дн.)</h3>
                  <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-center">
                    <div>
                      <div className="text-xs text-[hsl(var(--muted-foreground))]">Выручка</div>
                      <div className="text-sm font-bold text-blue-400">{fmtMoney(activeForecast.totals.revenue)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-[hsl(var(--muted-foreground))]">Комиссия</div>
                      <div className="text-sm font-bold text-red-400">
                        −{fmtMoney(activeForecast.forecast.reduce((s, p) => s + (p.commission || 0), 0))}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-[hsl(var(--muted-foreground))]">Логистика</div>
                      <div className="text-sm font-bold text-red-400">
                        −{fmtMoney(activeForecast.forecast.reduce((s, p) => s + (p.logistics || 0), 0))}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-[hsl(var(--muted-foreground))]">Реклама</div>
                      <div className="text-sm font-bold text-red-400">
                        −{fmtMoney(activeForecast.forecast.reduce((s, p) => s + (p.ad_spend_est || 0), 0))}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-[hsl(var(--muted-foreground))]">Чистая прибыль</div>
                      <div className={`text-sm font-bold ${activeForecast.totals.profit >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                        {activeForecast.totals.profit >= 0 ? '+' : ''}{fmtMoney(activeForecast.totals.profit)}
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  )
}


/* ═══════════════════════════════════════ */
/*        SIMULATOR TABLE                 */
/* ═══════════════════════════════════════ */

function SimulatorTable({ products }: { products: ForecastProduct[] }) {
  // Per-product budget multiplier state
  const [budgetMults, setBudgetMults] = useState<Record<number, number>>({})
  const [priceMults, setPriceMults] = useState<Record<number, number>>({})

  const getBM = (sku: number) => budgetMults[sku] ?? 1
  const getPM = (sku: number) => priceMults[sku] ?? 1

  const simulate = (p: ForecastProduct, bm: number, pm: number) => {
    if (p.ad_clicks === 0 || p.cr === 0) {
      // No ads data — just scale price
      const newRevenue = p.revenue * pm
      return {
        newOrders: p.orders,
        newRevenue: Math.round(newRevenue),
        newAdSpend: Math.round(p.ad_spend * bm),
        newProfit: Math.round(newRevenue - p.commission * pm - p.logistics - p.ad_spend * bm - p.cogs),
      }
    }

    const newClicks = p.ad_clicks * bm
    const newAdSpend = p.ad_spend * bm
    const newOrders = Math.round(newClicks * (p.cr / 100))
    const newRevenue = Math.round(newOrders * p.avg_price * pm)
    // Scale commission proportionally to revenue change
    const revRatio = p.revenue > 0 ? newRevenue / p.revenue : 1
    const newCommission = p.commission * revRatio
    const newLogistics = p.logistics * (newOrders / Math.max(p.orders, 1))
    const newCogs = p.cogs * (newOrders / Math.max(p.orders, 1))
    const newProfit = Math.round(newRevenue - newCommission - newLogistics - newAdSpend - newCogs)

    return { newOrders, newRevenue, newAdSpend: Math.round(newAdSpend), newProfit }
  }

  // Total simulation
  const simTotals = useMemo(() => {
    let totalRev = 0, totalProfit = 0, totalAds = 0, totalOrders = 0
    for (const p of products) {
      const s = simulate(p, getBM(p.sku), getPM(p.sku))
      totalRev += s.newRevenue
      totalProfit += s.newProfit
      totalAds += s.newAdSpend
      totalOrders += s.newOrders
    }
    return { totalRev, totalProfit, totalAds, totalOrders }
  }, [products, budgetMults, priceMults])

  const origTotals = useMemo(() => ({
    revenue: products.reduce((s, p) => s + p.revenue, 0),
    profit: products.reduce((s, p) => s + p.profit, 0),
    adSpend: products.reduce((s, p) => s + p.ad_spend, 0),
    orders: products.reduce((s, p) => s + p.orders, 0),
  }), [products])

  const revDelta = origTotals.revenue > 0
    ? Math.round((simTotals.totalRev - origTotals.revenue) / origTotals.revenue * 100)
    : 0
  const profDelta = origTotals.profit !== 0
    ? Math.round((simTotals.totalProfit - origTotals.profit) / Math.abs(origTotals.profit) * 100)
    : 0

  const tdCls = 'px-3 py-2.5 text-right text-sm whitespace-nowrap'

  return (
    <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] overflow-hidden">
      <div className="p-5 border-b border-[hsl(var(--border)/0.2)]">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold">Симулятор «Что если?»</h2>
            <p className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5">
              Двигайте ползунки бюджета рекламы и цены чтобы увидеть прогнозное изменение
            </p>
          </div>
          <div className="flex gap-4">
            <div className="text-right">
              <div className="text-xs text-[hsl(var(--muted-foreground))]">Прогноз. выручка</div>
              <div className="text-lg font-bold">
                {fmtMoney(simTotals.totalRev)}
                {revDelta !== 0 && (
                  <span className={`ml-2 text-xs font-semibold ${revDelta >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                    {revDelta > 0 ? '+' : ''}{revDelta}%
                  </span>
                )}
              </div>
            </div>
            <div className="text-right">
              <div className="text-xs text-[hsl(var(--muted-foreground))]">Прогноз. прибыль</div>
              <div className="text-lg font-bold">
                {fmtMoney(simTotals.totalProfit)}
                {profDelta !== 0 && (
                  <span className={`ml-2 text-xs font-semibold ${profDelta >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                    {profDelta > 0 ? '+' : ''}{profDelta}%
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[hsl(var(--border)/0.2)]">
              <th className="px-3 py-2 text-left font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase tracking-wider" style={{ position: 'sticky', left: 0, backgroundColor: 'hsl(var(--card))', zIndex: 10 }}>Товар</th>
              <th className="px-3 py-2 text-right font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase">Заказы</th>
              <th className="px-3 py-2 text-right font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase">Выручка</th>
              <th className="px-3 py-2 text-right font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase">Реклама</th>
              <th className="px-3 py-2 text-right font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase">CPO</th>
              <th className="px-3 py-2 text-right font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase">ROI</th>
              <th className="px-3 py-2 text-center font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase" style={{ minWidth: 160 }}>Бюджет ×</th>
              <th className="px-3 py-2 text-center font-medium text-xs text-[hsl(var(--muted-foreground))] uppercase" style={{ minWidth: 140 }}>Цена ×</th>
              <th className="px-3 py-2 text-right font-medium text-xs text-emerald-400 uppercase">→ Заказы</th>
              <th className="px-3 py-2 text-right font-medium text-xs text-emerald-400 uppercase">→ Выручка</th>
              <th className="px-3 py-2 text-right font-medium text-xs text-emerald-400 uppercase">→ Прибыль</th>
            </tr>
          </thead>
          <tbody>
            {products.map(p => {
              const bm = getBM(p.sku)
              const pm = getPM(p.sku)
              const sim = simulate(p, bm, pm)
              const ordDelta = p.orders > 0 ? Math.round((sim.newOrders - p.orders) / p.orders * 100) : 0
              const revDeltaP = p.revenue > 0 ? Math.round((sim.newRevenue - p.revenue) / p.revenue * 100) : 0

              return (
                <tr key={p.sku} className="border-b border-[hsl(var(--border)/0.1)] hover:bg-[hsl(var(--muted)/0.15)] transition-colors">
                  {/* Product */}
                  <td className="px-3 py-2" style={{ position: 'sticky', left: 0, backgroundColor: 'hsl(var(--card))', zIndex: 5, maxWidth: 240 }}>
                    <div className="flex items-center gap-2.5">
                      {p.image_url ? (
                        <img src={p.image_url} alt="" className="w-8 h-8 rounded object-cover flex-shrink-0" />
                      ) : (
                        <div className="w-8 h-8 rounded bg-[hsl(var(--muted)/0.3)] flex-shrink-0" />
                      )}
                      <div className="min-w-0">
                        <div className="text-sm font-medium truncate max-w-[180px]">
                          {p.name || p.offer_id}
                        </div>
                        <div className="text-[10px] text-[hsl(var(--muted-foreground)/0.6)]">{p.offer_id}</div>
                      </div>
                    </div>
                  </td>
                  {/* Current metrics */}
                  <td className={tdCls}>{fmtNum(p.orders)}</td>
                  <td className={tdCls}>{fmtMoney(p.revenue)}</td>
                  <td className={tdCls}>{p.ad_spend > 0 ? fmtMoney(p.ad_spend) : '—'}</td>
                  <td className={tdCls}>{p.cpo > 0 ? fmtMoney(p.cpo) : '—'}</td>
                  <td className={tdCls}>
                    {p.roi !== 0 ? (
                      <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${
                        p.roi >= 200 ? 'bg-emerald-500/15 text-emerald-400' :
                        p.roi >= 100 ? 'bg-amber-500/15 text-amber-400' :
                        'bg-red-500/15 text-red-400'
                      }`}>{p.roi}%</span>
                    ) : '—'}
                  </td>
                  {/* Budget slider */}
                  <td className="px-2 py-2">
                    <div className="flex flex-col items-center gap-0.5">
                      <input
                        type="range"
                        min={0.5}
                        max={3}
                        step={0.1}
                        value={bm}
                        onChange={e => setBudgetMults(prev => ({ ...prev, [p.sku]: parseFloat(e.target.value) }))}
                        className="w-full h-1 accent-blue-500"
                        style={{ accentColor: bm > 1 ? '#3b82f6' : bm < 1 ? '#ef4444' : '#94a3b8' }}
                      />
                      <span className={`text-[10px] font-mono font-semibold ${bm !== 1 ? 'text-blue-400' : 'text-[hsl(var(--muted-foreground)/0.5)]'}`}>
                        ×{bm.toFixed(1)}
                      </span>
                    </div>
                  </td>
                  {/* Price slider */}
                  <td className="px-2 py-2">
                    <div className="flex flex-col items-center gap-0.5">
                      <input
                        type="range"
                        min={0.8}
                        max={1.2}
                        step={0.05}
                        value={pm}
                        onChange={e => setPriceMults(prev => ({ ...prev, [p.sku]: parseFloat(e.target.value) }))}
                        className="w-full h-1"
                        style={{ accentColor: pm > 1 ? '#22c55e' : pm < 1 ? '#ef4444' : '#94a3b8' }}
                      />
                      <span className={`text-[10px] font-mono font-semibold ${pm !== 1 ? 'text-emerald-400' : 'text-[hsl(var(--muted-foreground)/0.5)]'}`}>
                        ×{pm.toFixed(2)}
                      </span>
                    </div>
                  </td>
                  {/* Simulated results */}
                  <td className={tdCls}>
                    <span className="font-medium">{fmtNum(sim.newOrders)}</span>
                    {ordDelta !== 0 && (
                      <span className={`ml-1 text-[10px] font-semibold ${ordDelta >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                        {ordDelta > 0 ? '+' : ''}{ordDelta}%
                      </span>
                    )}
                  </td>
                  <td className={tdCls}>
                    <span className="font-medium">{fmtMoney(sim.newRevenue)}</span>
                    {revDeltaP !== 0 && (
                      <span className={`ml-1 text-[10px] font-semibold ${revDeltaP >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                        {revDeltaP > 0 ? '+' : ''}{revDeltaP}%
                      </span>
                    )}
                  </td>
                  <td className={tdCls}>
                    <span className={`font-medium ${sim.newProfit >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                      {fmtMoney(sim.newProfit)}
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
