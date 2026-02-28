import { useState, useEffect, useCallback, useMemo } from 'react'
import { motion } from 'framer-motion'
import {
  ShoppingCart,
  DollarSign,
  Megaphone,
  Percent,
  ArrowUpRight,
  ArrowDownRight,
  Eye,
  MousePointerClick,
  Package,
  XCircle,
  RefreshCw,
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
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getOzonDashboardApi,
  getWbDashboardApi,
  type DashboardResponse,
  type AdsDailyPoint,
} from '@/api/dashboard'

/* ═══════════════════════════════════════════════════════════
   Constants & Helpers
   ═══════════════════════════════════════════════════════════ */

const PERIODS = [
  { key: 'today', label: 'Сегодня' },
  { key: '7d', label: '7 дней' },
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
   Charts: Sales Daily
   ═══════════════════════════════════════════════════════════ */

function SalesChart({ data }: { data: DashboardResponse['charts']['sales_daily'] }) {
  if (!data.length) {
    return (
      <div className="flex h-[300px] items-center justify-center rounded-xl border border-dashed border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.15)]">
        <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">Нет данных о продажах</p>
      </div>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={300}>
      <ComposedChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 40 }}>
        <defs>
          <linearGradient id="barGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#8b5cf6" stopOpacity={0.8} />
            <stop offset="100%" stopColor="#8b5cf6" stopOpacity={0.3} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.5} />
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
        <YAxis
          yAxisId="left"
          tick={{ fontSize: 12, fill: 'hsl(var(--muted-foreground))' }}
          axisLine={false}
          tickLine={false}
          width={40}
        />
        <YAxis
          yAxisId="right"
          orientation="right"
          tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
          tickFormatter={(v: number) => v.toLocaleString('ru-RU')}
          axisLine={false}
          tickLine={false}
          width={60}
        />
        <Tooltip
          contentStyle={{
            background: 'hsl(var(--card))',
            border: '1px solid hsl(var(--border))',
            borderRadius: '8px',
            fontSize: '13px',
          }}
          formatter={(value: number, name: string) => [
            name === 'revenue' ? formatMoney(value) : formatNumber(value),
            name === 'revenue' ? 'Продажи' : 'Заказы',
          ]}
          labelFormatter={formatTooltipDate}
        />
        <Legend
          verticalAlign="top"
          height={30}
          formatter={(value: string) => value === 'orders' ? 'Заказы' : value === 'revenue' ? 'Продажи' : value}
          wrapperStyle={{ fontSize: '12px', color: 'hsl(var(--muted-foreground))' }}
        />
        <Bar yAxisId="left" dataKey="orders" fill="url(#barGrad)" radius={[4, 4, 0, 0]} barSize={20} />
        <Line
          yAxisId="right"
          dataKey="revenue"
          stroke="#10b981"
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 4, fill: '#10b981' }}
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}

/* ═══════════════════════════════════════════════════════════
   Charts: Ads Daily (Advertising Analytics)
   ═══════════════════════════════════════════════════════════ */

const ADS_METRICS = [
  { key: 'spend', label: 'Расход ₽', color: '#f97316', yAxis: 'left' },
  { key: 'views', label: 'Показы', color: '#3b82f6', yAxis: 'right' },
  { key: 'clicks', label: 'Клики', color: '#06b6d4', yAxis: 'right' },
  { key: 'cart', label: 'Корзины', color: '#8b5cf6', yAxis: 'right' },
  { key: 'orders', label: 'Заказы', color: '#10b981', yAxis: 'left' },
  { key: 'ctr', label: 'Общий CTR', color: '#facc15', yAxis: 'percent' },
  { key: 'drr_ad', label: 'ДРР рекламы', color: '#ef4444', yAxis: 'percent' },
  { key: 'drr_total', label: 'Общий ДРР', color: '#ec4899', yAxis: 'percent' },
] as const

type AdsMetricKey = typeof ADS_METRICS[number]['key']

const ADS_METRIC_LABELS: Record<string, string> = {
  spend: 'Расход',
  views: 'Показы',
  clicks: 'Клики',
  cart: 'Корзины',
  orders: 'Заказы',
  ctr: 'Общий CTR',
  drr_ad: 'ДРР рекламы',
  drr_total: 'Общий ДРР',
}

function AdsChart({ data }: { data: AdsDailyPoint[] }) {
  const [activeMetrics, setActiveMetrics] = useState<Set<AdsMetricKey>>(
    new Set<AdsMetricKey>(['spend', 'clicks'])
  )

  const toggleMetric = (key: AdsMetricKey) => {
    setActiveMetrics(prev => {
      const next = new Set(prev)
      if (next.has(key)) {
        if (next.size > 1) next.delete(key) // Keep at least 1 metric
      } else {
        next.add(key)
      }
      return next
    })
  }

  // Compute CTR (clicks/views*100) for each data point
  const chartData = useMemo(() =>
    data.map(d => ({
      ...d,
      ctr: d.views > 0 ? parseFloat(((d.clicks / d.views) * 100).toFixed(2)) : 0,
    })),
    [data],
  )

  // Check which axes are needed
  const hasRightAxis = ADS_METRICS.some(
    m => activeMetrics.has(m.key) && m.yAxis === 'right'
  )
  const hasLeftAxis = ADS_METRICS.some(
    m => activeMetrics.has(m.key) && m.yAxis === 'left'
  )
  const hasPercentAxis = ADS_METRICS.some(
    m => activeMetrics.has(m.key) && m.yAxis === 'percent'
  )

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
      </div>

      {/* Chart */}
      <ResponsiveContainer width="100%" height={320}>
        <ComposedChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 40 }}>
          <defs>
            <linearGradient id="spendGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#f97316" stopOpacity={0.4} />
              <stop offset="100%" stopColor="#f97316" stopOpacity={0.05} />
            </linearGradient>
            <linearGradient id="ordersGrad" x1="0" y1="0" x2="0" y2="1">
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
              name === 'spend' ? formatMoney(value)
                : (name === 'drr_ad' || name === 'drr_total' || name === 'ctr') ? `${value.toFixed(1)}%`
                : formatNumber(value),
              ADS_METRIC_LABELS[name] || name,
            ]}
            labelFormatter={formatTooltipDate}
          />
          <Legend
            verticalAlign="top"
            height={30}
            formatter={(value: string) => ADS_METRIC_LABELS[value] || value}
            wrapperStyle={{ fontSize: '12px', color: 'hsl(var(--muted-foreground))' }}
          />

          {/* Spend — area chart */}
          {activeMetrics.has('spend') && (
            <Area
              yAxisId="left"
              type="monotone"
              dataKey="spend"
              fill="url(#spendGrad)"
              stroke="#f97316"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#f97316' }}
            />
          )}

          {/* Orders — bar chart */}
          {activeMetrics.has('orders') && (
            <Bar
              yAxisId="left"
              dataKey="orders"
              fill="url(#ordersGrad)"
              radius={[3, 3, 0, 0]}
              barSize={16}
            />
          )}

          {/* Views — line */}
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

          {/* Clicks — line */}
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

          {/* Cart — line */}
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

          {/* Общий CTR — dashed line */}
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

          {/* DRR рекламы — dashed line */}
          {activeMetrics.has('drr_ad') && (
            <Line
              yAxisId="percent"
              type="monotone"
              dataKey="drr_ad"
              stroke="#ef4444"
              strokeWidth={2}
              strokeDasharray="6 3"
              dot={false}
              activeDot={{ r: 4, fill: '#ef4444' }}
            />
          )}

          {/* Общий ДРР — dashed line */}
          {activeMetrics.has('drr_total') && (
            <Line
              yAxisId="percent"
              type="monotone"
              dataKey="drr_total"
              stroke="#ec4899"
              strokeWidth={2}
              strokeDasharray="6 3"
              dot={false}
              activeDot={{ r: 4, fill: '#ec4899' }}
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Top Products Table
   ═══════════════════════════════════════════════════════════ */

type ProductTab = 'leaders' | 'falling' | 'problems'

function TopProductsTable({ products }: { products: DashboardResponse['top_products'] }) {
  const [tab, setTab] = useState<ProductTab>('leaders')
  const [hoverImg, setHoverImg] = useState<{ url: string; x: number; y: number } | null>(null)

  // Filter + sort based on active tab
  const filtered = (() => {
    const all = [...products]
    switch (tab) {
      case 'leaders':
        // Top by revenue (already sorted by backend), show first 10
        return all.slice(0, 10)
      case 'falling':
        // Only products with negative delta (sales dropping)
        return all
          .filter((p) => p.delta_pct < 0)
          .sort((a, b) => a.delta_pct - b.delta_pct)
          .slice(0, 10)
      case 'problems':
        // Products with zero stock OR DRR > 20%
        return all
          .filter((p) => p.stock_fbo + p.stock_fbs === 0 || p.drr > 20)
          .sort((a, b) => {
            // Zero stock first, then by DRR descending
            const aZero = a.stock_fbo + a.stock_fbs === 0 ? 1 : 0
            const bZero = b.stock_fbo + b.stock_fbs === 0 ? 1 : 0
            if (aZero !== bZero) return bZero - aZero
            return b.drr - a.drr
          })
          .slice(0, 10)
      default:
        return all.slice(0, 10)
    }
  })()

  const tabs: { key: ProductTab; label: string; icon: string }[] = [
    { key: 'leaders', label: 'Лидеры', icon: '🏆' },
    { key: 'falling', label: 'Падающие', icon: '📉' },
    { key: 'problems', label: 'Проблемные', icon: '⚠️' },
  ]

  function stockColor(stock: number) {
    if (stock === 0) return 'text-red-400 font-semibold'
    if (stock < 10) return 'text-yellow-400'
    return 'text-emerald-400'
  }

  const emptyMessages: Record<ProductTab, string> = {
    leaders: 'Нет данных о товарах',
    falling: 'Нет товаров с падением продаж 🎉',
    problems: 'Нет проблемных товаров 🎉',
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg">Товары</CardTitle>
          <div className="inline-flex rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.3)] p-1">
            {tabs.map((t) => (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className={`rounded-md px-4 py-1.5 text-sm font-medium transition-all ${
                  tab === t.key
                    ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
                    : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
                }`}
              >
                {t.icon} {t.label}
              </button>
            ))}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {filtered.length === 0 ? (
          <p className="py-8 text-center text-sm text-[hsl(var(--muted-foreground)/0.5)]">
            {emptyMessages[tab]}
          </p>
        ) : (
          <div className="overflow-x-auto -mx-5">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[hsl(var(--border)/0.5)]">
                  <th className="px-5 py-3 text-left text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Товар</th>
                  <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Заказы</th>
                  <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Продажи</th>
                  <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Δ</th>
                  <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">FBO</th>
                  <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">FBS</th>
                  <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Цена</th>
                  <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Рекл. ₽</th>
                  <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">DRR</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((p, i) => {
                  const deltaFmt = formatDelta(p.delta_pct)
                  return (
                    <tr
                      key={p.offer_id}
                      className="border-b border-[hsl(var(--border)/0.3)] transition-colors hover:bg-[hsl(var(--muted)/0.2)]"
                    >
                      <td className="px-5 py-3">
                        <div className="flex items-center gap-3">
                          <span className="text-[13px] text-[hsl(var(--muted-foreground)/0.5)] w-5 text-center">{i + 1}</span>
                          {p.image_url ? (
                            <img
                              src={p.image_url}
                              alt={p.name}
                              className="h-12 w-10 rounded-lg object-cover shrink-0 cursor-pointer"
                              onMouseEnter={(e) => {
                                const rect = e.currentTarget.getBoundingClientRect()
                                setHoverImg({ url: p.image_url!, x: rect.right + 8, y: rect.top })
                              }}
                              onMouseLeave={() => setHoverImg(null)}
                            />
                          ) : (
                            <div className="h-12 w-10 rounded-lg bg-[hsl(var(--muted)/0.4)] shrink-0" />
                          )}
                          <div className="min-w-0">
                            <p className="text-sm font-medium truncate max-w-[240px]">{p.name || p.offer_id}</p>
                            <p className="text-[13px] text-[hsl(var(--muted-foreground)/0.6)]">
                              {p.supplier_article ? `${p.offer_id} · ${p.supplier_article}` : p.offer_id}
                            </p>
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-3 text-right font-medium">{formatNumber(p.orders)}</td>
                      <td className="px-3 py-3 text-right font-medium">{formatMoney(p.revenue)}</td>
                      <td className="px-3 py-3 text-right">
                        <span
                          className={`text-[13px] font-semibold ${
                            deltaFmt.positive ? 'text-emerald-400' : 'text-red-400'
                          }`}
                        >
                          {deltaFmt.text}
                        </span>
                      </td>
                      <td className={`px-3 py-3 text-right ${stockColor(p.stock_fbo)}`}>{p.stock_fbo}</td>
                      <td className={`px-3 py-3 text-right ${stockColor(p.stock_fbs)}`}>{p.stock_fbs}</td>
                      <td className="px-3 py-3 text-right">{formatMoney(p.price)}</td>
                      <td className="px-3 py-3 text-right">{formatMoney(p.ad_spend)}</td>
                      <td className="px-3 py-3 text-right">
                        <span className={p.drr > 20 ? 'text-red-400 font-semibold' : p.drr > 10 ? 'text-yellow-400' : ''}>
                          {p.drr.toFixed(1)}%
                        </span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>

      {/* Fixed-position hover preview — rendered outside table overflow */}
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
    </Card>
  )
}

/* ═══════════════════════════════════════════════════════════
   Loading Skeleton
   ═══════════════════════════════════════════════════════════ */

function DashboardSkeleton() {
  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div className="space-y-2">
          <Skeleton className="h-8 w-32" />
          <Skeleton className="h-4 w-64" />
        </div>
        <Skeleton className="h-9 w-52 rounded-lg" />
      </div>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {[...Array(6)].map((_, i) => (
          <Skeleton key={i} className="h-[110px] rounded-xl" />
        ))}
      </div>
      <Skeleton className="h-[370px] rounded-xl" />
      <Skeleton className="h-[400px] rounded-xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Dashboard Page
   ═══════════════════════════════════════════════════════════ */

export default function DashboardPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const [period, setPeriod] = useState('today')
  const [data, setData] = useState<DashboardResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fetchDashboard = useCallback(async () => {
    if (!currentShop) return

    setLoading(true)
    setError(null)
    try {
      const result = currentShop.marketplace === 'ozon'
        ? await getOzonDashboardApi(currentShop.id, period)
        : await getWbDashboardApi(currentShop.id, period)
      setData(result)
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Неизвестная ошибка'
      setError(msg)
    } finally {
      setLoading(false)
    }
  }, [currentShop, period])

  useEffect(() => {
    fetchDashboard()
  }, [fetchDashboard])

  // Auto-refresh every 2 minutes
  useEffect(() => {
    if (!currentShop) return
    const interval = setInterval(fetchDashboard, 120_000)
    return () => clearInterval(interval)
  }, [currentShop, fetchDashboard])

  // ── No shop selected ──────────────────────────────
  if (!currentShop) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="text-center space-y-3">
          <Package className="h-12 w-12 mx-auto text-[hsl(var(--muted-foreground)/0.3)]" />
          <p className="text-lg font-medium text-[hsl(var(--muted-foreground))]">
            Выберите магазин
          </p>
          <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">
            Для просмотра аналитики выберите магазин в шапке
          </p>
        </div>
      </div>
    )
  }

  // Marketplace label for header
  const marketplaceLabel = currentShop.marketplace === 'ozon' ? 'Ozon' : 'Wildberries'

  // ── Loading state ──────────────────────────────────
  if (loading && !data) {
    return <DashboardSkeleton />
  }

  // ── Error state ────────────────────────────────────
  if (error && !data) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="text-center space-y-3">
          <XCircle className="h-12 w-12 mx-auto text-red-400" />
          <p className="text-lg font-medium text-red-400">Ошибка загрузки</p>
          <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">{error}</p>
          <button
            onClick={fetchDashboard}
            className="mt-2 inline-flex items-center gap-2 rounded-lg bg-[hsl(var(--primary))] px-4 py-2 text-sm font-medium text-white hover:opacity-90 transition-opacity"
          >
            <RefreshCw className="h-4 w-4" /> Повторить
          </button>
        </div>
      </div>
    )
  }

  if (!data) return null

  const kpi = data.kpi

  const kpiCards: {
    title: string
    value: string
    subtitle?: string
    delta: number
    invertDelta?: boolean
    icon: React.ElementType
    accent: string
  }[] = [
    {
      title: 'Заказы',
      value: formatNumber(kpi.orders_count),
      delta: kpi.orders_delta,
      icon: ShoppingCart,
      accent: '#6366f1',
    },
    {
      title: 'Продажи',
      value: formatMoney(kpi.revenue),
      subtitle: `ø${formatMoney(kpi.avg_check)}`,
      delta: kpi.revenue_delta,
      icon: DollarSign,
      accent: '#10b981',
    },
    {
      title: 'Показы',
      value: formatNumber(kpi.views),
      delta: kpi.views_delta,
      icon: Eye,
      accent: '#3b82f6',
    },
    {
      title: 'Клики',
      value: formatNumber(kpi.clicks),
      delta: kpi.clicks_delta,
      icon: MousePointerClick,
      accent: '#06b6d4',
    },
    {
      title: 'Расход рекламы',
      value: formatMoney(kpi.ad_spend),
      delta: kpi.ad_spend_delta,
      invertDelta: true,
      icon: Megaphone,
      accent: '#f97316',
    },
    {
      title: 'DRR',
      value: `${kpi.drr.toFixed(1)}%`,
      subtitle: kpi.drr > 20 ? '⚠️ высокий' : kpi.drr > 10 ? '⚡ средний' : '✅ норма',
      delta: kpi.drr_delta,
      invertDelta: true,
      icon: Percent,
      accent: '#8b5cf6',
    },
  ]

  return (
    <div className="space-y-6">
      {/* ── Header + Period ── */}
      <motion.div
        initial={{ opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
        className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between"
      >
        <div className="space-y-1">
          <h1 className="text-2xl font-bold tracking-tight text-[hsl(var(--foreground))]">
            Обзор
          </h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            {currentShop.name} • {marketplaceLabel}
            {loading && (
              <span className="ml-2 inline-flex items-center gap-1 text-[hsl(var(--primary))]">
                <RefreshCw className="h-3 w-3 animate-spin" /> Обновление…
              </span>
            )}
          </p>
        </div>
        <PeriodSelector current={period} onChange={setPeriod} />
      </motion.div>

      {/* ── KPI Cards ── */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3 xl:gap-5">
        {kpiCards.map((card, i) => (
          <KpiCard key={card.title} {...card} delay={i * 0.05} />
        ))}
      </div>

      {/* ── Sales Chart ── */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.25 }}
      >
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Динамика продаж</CardTitle>
          </CardHeader>
          <CardContent>
            <SalesChart data={data.charts.sales_daily} />
          </CardContent>
        </Card>
      </motion.div>

      {/* ── Ads Chart ── */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.30 }}
      >
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Рекламная аналитика</CardTitle>
          </CardHeader>
          <CardContent>
            <AdsChart data={data.charts.ads_daily} />
          </CardContent>
        </Card>
      </motion.div>

      {/* ── Top Products ── */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.35 }}
      >
        <TopProductsTable products={data.top_products} />
      </motion.div>
    </div>
  )
}
