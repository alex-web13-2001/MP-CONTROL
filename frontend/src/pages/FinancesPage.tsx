import { useState, useEffect, useCallback } from 'react'
import { motion } from 'framer-motion'
import {
  ShoppingCart,
  Wallet,
  Building2,
  Megaphone,
  Package,
  TrendingUp,
  TrendingDown,
  ArrowUpRight,
  ArrowDownRight,
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
import { PeriodSelector, type PeriodValue } from '@/components/DateRangePicker'
import {
  getOzonFinancesApi,
  type FinancesResponse,
  type FinancesDailyPoint,
} from '@/api/finances'

/* ═══════════════════════════════════════════════════════════
   Constants & Helpers
   ═══════════════════════════════════════════════════════════ */

const GROUP_OPTIONS = [
  { key: 'day', label: 'Дни' },
  { key: 'week', label: 'Недели' },
  { key: 'month', label: 'Месяцы' },
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
   KPI Card Component (reused from DashboardPage design)
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
   GroupBy Selector
   ═══════════════════════════════════════════════════════════ */

function GroupBySelector({
  current,
  onChange,
}: {
  current: string
  onChange: (g: string) => void
}) {
  return (
    <div className="inline-flex rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1">
      {GROUP_OPTIONS.map((g) => (
        <button
          key={g.key}
          onClick={() => onChange(g.key)}
          className={`rounded-md px-4 py-1.5 text-sm font-medium transition-all duration-200 ${
            current === g.key
              ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
              : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
          }`}
        >
          {g.label}
        </button>
      ))}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Waterfall Chart — Expense Breakdown
   ═══════════════════════════════════════════════════════════ */

const BREAKDOWN_ITEMS = [
  { key: 'revenue', label: 'Выручка', color: '#10b981' },
  { key: 'commission', label: 'Комиссия + скидки', color: '#f97316' },
  { key: 'logistics', label: 'Логистика', color: '#ef4444' },
  { key: 'storage', label: 'Хранение', color: '#f59e0b' },
  { key: 'acquiring', label: 'Эквайринг', color: '#ec4899' },
  { key: 'advertising', label: 'Реклама', color: '#8b5cf6' },
  { key: 'refunds', label: 'Возвраты', color: '#6366f1' },
  { key: 'penalties', label: 'Штрафы', color: '#dc2626' },
  { key: 'cogs', label: 'Себестоимость', color: '#64748b' },
  { key: 'profit', label: 'Прибыль', color: '#10b981' },
] as const

function BreakdownChart({ data }: { data: FinancesResponse['breakdown'] }) {
  const revenue = data.revenue || 1

  const items = BREAKDOWN_ITEMS.map((item) => {
    const val = (data as any)[item.key] || 0
    const pct = revenue > 0 ? Math.abs(val) / revenue * 100 : 0
    return {
      name: item.label,
      value: Math.abs(val),
      pct: Math.round(pct * 10) / 10,
      color: item.key === 'profit' ? (val >= 0 ? '#10b981' : '#ef4444') : item.color,
      isProfit: item.key === 'profit',
      isRevenue: item.key === 'revenue',
    }
  })

  return (
    <div className="space-y-2.5">
      {items.map((item, i) => (
        <motion.div
          key={item.name}
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.3, delay: i * 0.04 }}
          className="group"
        >
          <div className="flex items-center gap-3">
            <div className="w-[140px] shrink-0 text-right">
              <span className={`text-[13px] font-medium ${
                item.isRevenue || item.isProfit
                  ? 'text-[hsl(var(--foreground))]'
                  : 'text-[hsl(var(--muted-foreground))]'
              }`}>
                {item.isRevenue ? '' : '− '}{item.name}
              </span>
            </div>
            <div className="flex-1 relative h-7 rounded-md bg-[hsl(var(--muted)/0.15)] overflow-hidden">
              <motion.div
                initial={{ width: 0 }}
                animate={{ width: `${Math.min(item.pct, 100)}%` }}
                transition={{ duration: 0.6, delay: i * 0.04, ease: 'easeOut' }}
                className="absolute inset-y-0 left-0 rounded-md"
                style={{
                  background: `linear-gradient(90deg, ${item.color}40, ${item.color}80)`,
                  borderLeft: `3px solid ${item.color}`,
                }}
              />
              <div className="absolute inset-y-0 left-3 flex items-center">
                <span className="text-[12px] font-semibold text-[hsl(var(--foreground))] drop-shadow-sm">
                  {formatMoney(item.value)}
                </span>
              </div>
            </div>
            <div className="w-[50px] shrink-0 text-right">
              <span className={`text-[13px] font-bold ${
                item.isProfit ? (item.value > 0 ? 'text-emerald-400' : 'text-red-400') : 'text-[hsl(var(--muted-foreground))]'
              }`}>
                {item.pct.toFixed(1)}%
              </span>
            </div>
          </div>
          {/* Separator before profit */}
          {item.name === 'Себестоимость' && (
            <div className="mt-3 mb-1 border-t-2 border-dashed border-[hsl(var(--border)/0.5)]" />
          )}
        </motion.div>
      ))}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Dynamics Chart
   ═══════════════════════════════════════════════════════════ */

const DYNAMICS_METRICS = [
  { key: 'revenue', label: 'Выручка', color: '#10b981', yAxis: 'left' },
  { key: 'payout', label: 'К перечислению', color: '#3b82f6', yAxis: 'left' },
  { key: 'mp_fees', label: 'Услуги МП', color: '#f97316', yAxis: 'left' },
  { key: 'ad_spend', label: 'Реклама', color: '#ef4444', yAxis: 'left' },
  { key: 'cogs', label: 'Себестоимость', color: '#64748b', yAxis: 'left' },
  { key: 'profit', label: 'Прибыль', color: '#8b5cf6', yAxis: 'left' },
  { key: 'orders', label: 'Заказы', color: '#06b6d4', yAxis: 'right' },
] as const

type DynamicsMetricKey = typeof DYNAMICS_METRICS[number]['key']

const DYNAMICS_LABELS: Record<string, string> = {
  revenue: 'Выручка',
  payout: 'К перечислению',
  mp_fees: 'Услуги МП',
  ad_spend: 'Реклама',
  cogs: 'Себестоимость',
  profit: 'Прибыль',
  orders: 'Заказы',
}

function DynamicsChart({ data }: { data: FinancesDailyPoint[] }) {
  const [activeMetrics, setActiveMetrics] = useState<Set<DynamicsMetricKey>>(
    new Set<DynamicsMetricKey>(['revenue', 'profit'])
  )

  const toggleMetric = (key: DynamicsMetricKey) => {
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

  const hasRightAxis = DYNAMICS_METRICS.some(
    m => activeMetrics.has(m.key) && m.yAxis === 'right'
  )

  if (!data.length) {
    return (
      <div className="flex h-[300px] items-center justify-center rounded-xl border border-dashed border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.15)]">
        <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">Нет данных за выбранный период</p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Metric toggle chips */}
      <div className="flex flex-wrap gap-2">
        {DYNAMICS_METRICS.map(m => {
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
        <ComposedChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 40 }}>
          <defs>
            <linearGradient id="revenueGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#10b981" stopOpacity={0.4} />
              <stop offset="100%" stopColor="#10b981" stopOpacity={0.05} />
            </linearGradient>
            <linearGradient id="profitGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#8b5cf6" stopOpacity={0.7} />
              <stop offset="100%" stopColor="#8b5cf6" stopOpacity={0.2} />
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
          <YAxis
            yAxisId="left"
            tick={{ fontSize: 12, fill: 'hsl(var(--muted-foreground))' }}
            tickFormatter={(v: number) =>
              v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v.toString()
            }
            axisLine={false}
            tickLine={false}
            width={55}
          />
          {hasRightAxis && (
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
              axisLine={false}
              tickLine={false}
              width={40}
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
              name === 'orders' ? formatNumber(value) : formatMoney(value),
              DYNAMICS_LABELS[name] || name,
            ]}
            labelFormatter={formatTooltipDate}
          />
          <Legend
            verticalAlign="top"
            height={30}
            formatter={(value: string) => DYNAMICS_LABELS[value] || value}
            wrapperStyle={{ fontSize: '12px', color: 'hsl(var(--muted-foreground))' }}
          />

          {/* Revenue — area */}
          {activeMetrics.has('revenue') && (
            <Area
              yAxisId="left"
              type="monotone"
              dataKey="revenue"
              fill="url(#revenueGrad)"
              stroke="#10b981"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#10b981' }}
            />
          )}

          {/* Profit — bar */}
          {activeMetrics.has('profit') && (
            <Bar
              yAxisId="left"
              dataKey="profit"
              fill="url(#profitGrad)"
              radius={[3, 3, 0, 0]}
              barSize={16}
            />
          )}

          {/* Payout — line */}
          {activeMetrics.has('payout') && (
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="payout"
              stroke="#3b82f6"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#3b82f6' }}
            />
          )}

          {/* MP fees — line */}
          {activeMetrics.has('mp_fees') && (
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="mp_fees"
              stroke="#f97316"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#f97316' }}
            />
          )}

          {/* Ad spend — line dashed */}
          {activeMetrics.has('ad_spend') && (
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="ad_spend"
              stroke="#ef4444"
              strokeWidth={2}
              strokeDasharray="6 3"
              dot={false}
              activeDot={{ r: 4, fill: '#ef4444' }}
            />
          )}

          {/* COGS — line dashed */}
          {activeMetrics.has('cogs') && (
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="cogs"
              stroke="#64748b"
              strokeWidth={2}
              strokeDasharray="6 3"
              dot={false}
              activeDot={{ r: 4, fill: '#64748b' }}
            />
          )}

          {/* Orders — line on right axis */}
          {activeMetrics.has('orders') && (
            <Line
              yAxisId={hasRightAxis ? 'right' : 'left'}
              type="monotone"
              dataKey="orders"
              stroke="#06b6d4"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: '#06b6d4' }}
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Comparison Table
   ═══════════════════════════════════════════════════════════ */

const COMPARISON_ROWS = [
  { key: 'revenue', label: 'Выручка', isMoney: true },
  { key: 'orders', label: 'Заказы', isMoney: false },
  { key: 'payout', label: 'К перечислению', isMoney: true },
  { key: 'mp_fees', label: 'Услуги МП', isMoney: true, invert: true },
  { key: 'commission', label: '  └ Комиссия + скидки', isMoney: true, invert: true, indent: true },
  { key: 'logistics', label: '  └ Логистика', isMoney: true, invert: true, indent: true },
  { key: 'storage', label: '  └ Хранение', isMoney: true, invert: true, indent: true },
  { key: 'acquiring', label: '  └ Эквайринг', isMoney: true, invert: true, indent: true },
  { key: 'advertising', label: 'Реклама', isMoney: true, invert: true },
  { key: 'refunds', label: 'Возвраты', isMoney: true, invert: true },
  { key: 'penalties', label: 'Штрафы', isMoney: true, invert: true },
  { key: 'cogs', label: 'Себестоимость', isMoney: true, invert: true },
  { key: 'profit', label: 'Чистая прибыль', isMoney: true, bold: true },
]

function ComparisonTable({ comparison }: { comparison: FinancesResponse['comparison'] }) {
  return (
    <div className="overflow-x-auto -mx-5">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-[hsl(var(--border)/0.5)]">
            <th className="px-5 py-3 text-left text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
              Показатель
            </th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
              Текущий период
            </th>
            <th className="px-3 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
              Предыдущий период
            </th>
            <th className="px-5 py-3 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
              Δ
            </th>
          </tr>
        </thead>
        <tbody>
          {COMPARISON_ROWS.map((row) => {
            const curVal = comparison.current[row.key] ?? 0
            const prevVal = comparison.previous[row.key] ?? 0
            const delta = comparison.delta_pct[row.key] ?? 0
            const absD = Math.abs(delta)
            const deltaFmt = formatDelta(delta, row.invert)

            return (
              <tr
                key={row.key}
                className={`border-b border-[hsl(var(--border)/0.2)] transition-colors hover:bg-[hsl(var(--muted)/0.15)] ${
                  row.bold ? 'bg-[hsl(var(--muted)/0.08)]' : ''
                }`}
              >
                <td className={`px-5 py-2.5 text-left ${
                  row.bold ? 'font-bold text-[hsl(var(--foreground))]' : ''
                } ${row.indent ? 'text-[hsl(var(--muted-foreground)/0.7)] text-[12px]' : ''}`}>
                  {row.label}
                </td>
                <td className={`px-3 py-2.5 text-right font-medium ${
                  row.bold ? 'font-bold text-lg' : ''
                } ${row.key === 'profit' ? (curVal >= 0 ? 'text-emerald-400' : 'text-red-400') : ''}`}>
                  {row.isMoney ? formatMoney(curVal) : formatNumber(curVal)}
                </td>
                <td className="px-3 py-2.5 text-right text-[hsl(var(--muted-foreground))]">
                  {row.isMoney ? formatMoney(prevVal) : formatNumber(prevVal)}
                </td>
                <td className="px-5 py-2.5 text-right">
                  {absD > 0.1 ? (
                    <span
                      className={`inline-flex items-center gap-0.5 text-[13px] font-semibold ${
                        deltaFmt.positive ? 'text-emerald-400' : 'text-red-400'
                      }`}
                    >
                      {deltaFmt.positive ? (
                        <ArrowUpRight className="h-3 w-3" />
                      ) : (
                        <ArrowDownRight className="h-3 w-3" />
                      )}
                      {deltaFmt.text}
                    </span>
                  ) : (
                    <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.4)]">—</span>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Loading Skeleton
   ═══════════════════════════════════════════════════════════ */

function FinancesSkeleton() {
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
      <Skeleton className="h-[400px] rounded-xl" />
      <Skeleton className="h-[380px] rounded-xl" />
      <Skeleton className="h-[400px] rounded-xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Finances Page
   ═══════════════════════════════════════════════════════════ */

export default function FinancesPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const shopId = currentShop?.id

  const [data, setData] = useState<FinancesResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [groupBy, setGroupBy] = useState('day')
  const [periodValue, setPeriodValue] = useState<PeriodValue>({
    mode: 'quick',
    period: 7,
    dateRange: null,
  })

  const fetchData = useCallback(async () => {
    if (!shopId) return

    setLoading(true)
    setError(null)

    try {
      const params: any = {
        shop_id: shopId,
        group_by: groupBy,
      }

      if (periodValue.mode === 'custom' && periodValue.dateRange?.from) {
        const from = periodValue.dateRange.from
        const to = periodValue.dateRange.to ?? from
        params.date_from = from.toISOString().split('T')[0]
        params.date_to = to.toISOString().split('T')[0]
      } else {
        params.period = periodValue.period
      }

      const result = await getOzonFinancesApi(params)
      setData(result)
    } catch (e: any) {
      console.error('Finances fetch error:', e)
      setError(e.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [shopId, groupBy, periodValue])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  // Check if shop is Ozon
  const isOzon = currentShop?.marketplace === 'ozon'

  if (!shopId) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <p className="text-[hsl(var(--muted-foreground))]">Выберите магазин</p>
      </div>
    )
  }

  if (!isOzon) {
    return (
      <div className="flex h-[60vh] flex-col items-center justify-center gap-3">
        <p className="text-lg font-medium text-[hsl(var(--foreground))]">
          Раздел «Финансы» пока доступен только для Ozon
        </p>
        <p className="text-sm text-[hsl(var(--muted-foreground))]">
          Поддержка Wildberries появится в следующем обновлении
        </p>
      </div>
    )
  }

  if (loading && !data) {
    return <FinancesSkeleton />
  }

  if (error && !data) {
    return (
      <div className="flex h-[60vh] flex-col items-center justify-center gap-4">
        <p className="text-red-400">{error}</p>
        <button
          onClick={fetchData}
          className="inline-flex items-center gap-2 rounded-lg bg-[hsl(var(--primary))] px-4 py-2 text-sm font-medium text-white"
        >
          <RefreshCw className="h-4 w-4" /> Повторить
        </button>
      </div>
    )
  }

  if (!data) return null

  const kpi = data.kpi
  const ProfitIcon = kpi.profit >= 0 ? TrendingUp : TrendingDown
  const profitAccent = kpi.profit >= 0 ? '#10b981' : '#ef4444'

  return (
    <div className="space-y-8">
      {/* ── Header ── */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[hsl(var(--foreground))]">Финансы</h1>
          <p className="mt-1 text-sm text-[hsl(var(--muted-foreground))]">
            P&L и структура расходов • {data.date_from} — {data.date_to}
          </p>
        </div>
        <div className="flex items-center gap-3 flex-wrap">
          <GroupBySelector current={groupBy} onChange={setGroupBy} />
          <PeriodSelector value={periodValue} onChange={setPeriodValue} />
        </div>
      </div>

      {/* ── KPI Cards ── */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <KpiCard
          title="Выручка"
          value={formatMoney(kpi.revenue)}
          subtitle={`${formatNumber(kpi.orders)} заказов`}
          delta={kpi.revenue_delta}
          icon={ShoppingCart}
          accent="#10b981"
          delay={0}
        />
        <KpiCard
          title="К перечислению"
          value={formatMoney(kpi.payout)}
          delta={kpi.payout_delta}
          icon={Wallet}
          accent="#3b82f6"
          delay={0.05}
        />
        <KpiCard
          title="Услуги МП"
          value={formatMoney(kpi.mp_fees)}
          subtitle={kpi.revenue > 0 ? `${(kpi.mp_fees / kpi.revenue * 100).toFixed(1)}% от выручки` : undefined}
          delta={kpi.mp_fees_delta}
          invertDelta
          icon={Building2}
          accent="#f97316"
          delay={0.1}
        />
        <KpiCard
          title="Реклама"
          value={formatMoney(kpi.ad_spend)}
          subtitle={kpi.revenue > 0 ? `ДРР ${(kpi.ad_spend / kpi.revenue * 100).toFixed(1)}%` : undefined}
          delta={kpi.ad_spend_delta}
          invertDelta
          icon={Megaphone}
          accent="#ef4444"
          delay={0.15}
        />
        <KpiCard
          title="Себестоимость"
          value={formatMoney(kpi.cogs)}
          delta={kpi.cogs_delta}
          invertDelta
          icon={Package}
          accent="#8b5cf6"
          delay={0.2}
        />
        <KpiCard
          title="Чистая прибыль"
          value={formatMoney(kpi.profit)}
          subtitle={`${kpi.profit_pct.toFixed(1)}% от выручки`}
          delta={kpi.profit_delta}
          icon={ProfitIcon}
          accent={profitAccent}
          delay={0.25}
        />
      </div>

      {/* ── Expense Breakdown ── */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.3, ease: 'easeOut' }}
      >
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Структура расходов</CardTitle>
            <p className="text-sm text-[hsl(var(--muted-foreground))]">
              Куда уходит каждый рубль выручки
            </p>
          </CardHeader>
          <CardContent>
            <BreakdownChart data={data.breakdown} />
          </CardContent>
        </Card>
      </motion.div>

      {/* ── Dynamics Chart ── */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.35, ease: 'easeOut' }}
      >
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">
              Динамика{' '}
              <span className="text-[hsl(var(--muted-foreground))] font-normal text-sm">
                по {groupBy === 'day' ? 'дням' : groupBy === 'week' ? 'неделям' : 'месяцам'}
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <DynamicsChart data={data.daily} />
          </CardContent>
        </Card>
      </motion.div>

      {/* ── Comparison Table ── */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.4, ease: 'easeOut' }}
      >
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Сравнение периодов</CardTitle>
            <p className="text-sm text-[hsl(var(--muted-foreground))]">
              Текущий период vs предыдущий аналогичный период
            </p>
          </CardHeader>
          <CardContent>
            <ComparisonTable comparison={data.comparison} />
          </CardContent>
        </Card>
      </motion.div>

      {/* Loading overlay for re-fetch */}
      {loading && data && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/10 backdrop-blur-[1px]">
          <div className="rounded-xl bg-[hsl(var(--card))] border border-[hsl(var(--border))] px-6 py-4 shadow-2xl flex items-center gap-3">
            <RefreshCw className="h-5 w-5 animate-spin text-[hsl(var(--primary))]" />
            <span className="text-sm font-medium">Обновление данных...</span>
          </div>
        </div>
      )}
    </div>
  )
}
