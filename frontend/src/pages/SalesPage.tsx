import { useState, useEffect, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  ShoppingCart,
  DollarSign,
  RotateCcw,
  ArrowUpRight,
  ArrowDownRight,
  RefreshCw,
  MapPin,
  TrendingDown,
  ChevronDown,
} from 'lucide-react'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  BarChart,
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import { PeriodSelector, type PeriodValue } from '@/components/DateRangePicker'
import {
  getOzonSalesApi,
  type SalesResponse,
  type SalesDailyPoint,
  type SalesGeoItem,
  type SalesTopProduct,
  type SalesReturnReason,
} from '@/api/sales'

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

const MONTHS_SHORT = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']

function formatMoney(value: number): string {
  return Math.round(value).toLocaleString('ru-RU') + ' ₽'
}

function formatNumber(value: number): string {
  return value.toLocaleString('ru-RU')
}

function formatChartDate(dateStr: string): string {
  const d = new Date(dateStr + 'T00:00:00')
  return `${d.getDate()} ${MONTHS_SHORT[d.getMonth()]}`
}

/* ═══════════════════════════════════════════════════════════
   KPI Card — fixed height, no optional subtitle variance
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
  const positive = invertDelta ? delta < 0 : delta > 0
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay, duration: 0.4 }}
    >
      <Card className="relative overflow-hidden h-full">
        <CardContent className="p-5 flex flex-col justify-between h-full">
          <div className="flex items-start justify-between">
            <div className="space-y-1 min-w-0">
              <p className="text-[13px] font-medium text-[hsl(var(--muted-foreground))]">{title}</p>
              <p className="text-2xl font-bold tracking-tight">{value}</p>
            </div>
            <div className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br ${accent} shadow-lg`}>
              <Icon className="h-5 w-5 text-white" />
            </div>
          </div>
          <div className="mt-3 flex items-center gap-2 min-h-[24px]">
            {delta !== 0 && (
              <span className={`inline-flex items-center gap-0.5 rounded-full px-2 py-0.5 text-xs font-semibold ${
                positive
                  ? 'bg-emerald-500/15 text-emerald-400'
                  : 'bg-red-500/15 text-red-400'
              }`}>
                {positive ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />}
                {Math.abs(delta)}%
              </span>
            )}
            {subtitle && (
              <span className="text-[12px] text-[hsl(var(--muted-foreground))] truncate">{subtitle}</span>
            )}
            {delta !== 0 && !subtitle && (
              <span className="text-[11px] text-[hsl(var(--muted-foreground))]">к пред. периоду</span>
            )}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Sales Chart
   ═══════════════════════════════════════════════════════════ */

function SalesChart({ data }: { data: SalesDailyPoint[] }) {
  return (
    <ResponsiveContainer width="100%" height={340}>
      <ComposedChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.3} />
        <XAxis
          dataKey="date"
          tickFormatter={formatChartDate}
          tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12 }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          yAxisId="left"
          tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12 }}
          axisLine={false}
          tickLine={false}
          width={40}
        />
        <YAxis
          yAxisId="right"
          orientation="right"
          tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 12 }}
          axisLine={false}
          tickLine={false}
          tickFormatter={(v: number) => v >= 1000 ? `${Math.round(v / 1000)}K` : String(v)}
          width={50}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: 'hsl(var(--card))',
            border: '1px solid hsl(var(--border))',
            borderRadius: 12,
            fontSize: 13,
          }}
          labelFormatter={formatChartDate}
          formatter={(value: number, name: string) => [
            name === 'revenue' ? formatMoney(value) : formatNumber(value),
            name === 'revenue' ? 'Продажи' : name === 'orders' ? 'Заказы' : 'Возвраты',
          ]}
        />
        <Legend
          formatter={(value: string) =>
            value === 'orders' ? 'Заказы' : value === 'revenue' ? 'Продажи' : 'Возвраты'
          }
        />
        <Bar
          yAxisId="left"
          dataKey="orders"
          fill="hsl(var(--primary))"
          radius={[4, 4, 0, 0]}
          opacity={0.85}
        />
        <Line
          yAxisId="right"
          type="monotone"
          dataKey="revenue"
          stroke="#10b981"
          strokeWidth={2.5}
          dot={false}
        />
        <Line
          yAxisId="left"
          type="monotone"
          dataKey="returns"
          stroke="#ef4444"
          strokeWidth={2}
          strokeDasharray="5 5"
          dot={false}
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}

/* ═══════════════════════════════════════════════════════════
   Geography — full-width, collapsible, compact
   ═══════════════════════════════════════════════════════════ */

const GEO_COLLAPSED_COUNT = 5

function GeoSection({ data }: { data: SalesGeoItem[] }) {
  const [expanded, setExpanded] = useState(false)
  const maxRevenue = Math.max(...data.map(d => d.revenue), 1)
  const visible = expanded ? data : data.slice(0, GEO_COLLAPSED_COUNT)
  const hasMore = data.length > GEO_COLLAPSED_COUNT

  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center h-[120px] text-[hsl(var(--muted-foreground))] text-sm">
        Нет данных о географии за период
      </div>
    )
  }

  return (
    <div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[hsl(var(--border))]">
              <th className="px-3 py-2.5 text-left text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Город</th>
              <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Заказы</th>
              <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Продажи</th>
              <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Доля</th>
              <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Ср. чек</th>
              <th className="px-3 py-2.5 text-[13px] font-medium text-[hsl(var(--muted-foreground))] w-[180px]"></th>
            </tr>
          </thead>
          <tbody>
            <AnimatePresence initial={false}>
              {visible.map((row) => (
                <motion.tr
                  key={row.region}
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: 'auto' }}
                  exit={{ opacity: 0, height: 0 }}
                  className="border-b border-[hsl(var(--border)/0.3)] hover:bg-[hsl(var(--muted)/0.3)] transition-colors"
                >
                  <td className="px-3 py-2 font-medium">
                    <span className="flex items-center gap-2">
                      <MapPin className="h-3.5 w-3.5 text-[hsl(var(--muted-foreground))] shrink-0" />
                      {row.region}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums">{formatNumber(row.orders)}</td>
                  <td className="px-3 py-2 text-right tabular-nums font-medium">{formatMoney(row.revenue)}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-[hsl(var(--muted-foreground))]">{row.pct}%</td>
                  <td className="px-3 py-2 text-right tabular-nums">{formatMoney(row.avg_check)}</td>
                  <td className="px-3 py-2">
                    <div className="h-1.5 w-full rounded-full bg-[hsl(var(--muted)/0.3)]">
                      <div
                        className="h-1.5 rounded-full bg-gradient-to-r from-[hsl(var(--primary))] to-[hsl(var(--primary)/0.6)]"
                        style={{ width: `${Math.round(row.revenue / maxRevenue * 100)}%` }}
                      />
                    </div>
                  </td>
                </motion.tr>
              ))}
            </AnimatePresence>
          </tbody>
        </table>
      </div>
      {hasMore && (
        <button
          onClick={() => setExpanded(!expanded)}
          className="mt-2 w-full flex items-center justify-center gap-1.5 py-2 text-sm text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
        >
          <ChevronDown className={`h-4 w-4 transition-transform duration-200 ${expanded ? 'rotate-180' : ''}`} />
          {expanded ? 'Свернуть' : `Показать все ${data.length} городов`}
        </button>
      )}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Top Products Table
   ═══════════════════════════════════════════════════════════ */

function TopProductsTable({ data }: { data: SalesTopProduct[] }) {
  const [hoveredImg, setHoveredImg] = useState<{ url: string; x: number; y: number } | null>(null)

  return (
    <div className="overflow-x-auto relative">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-[hsl(var(--border))]">
            <th className="px-3 py-2.5 text-left text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Товар</th>
            <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Заказы</th>
            <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Продажи</th>
            <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Возвраты</th>
            <th className="px-3 py-2.5 text-right text-[13px] font-medium text-[hsl(var(--muted-foreground))]">% возвр.</th>
          </tr>
        </thead>
        <tbody>
          {data.map((p) => (
            <tr key={p.sku} className="border-b border-[hsl(var(--border)/0.3)] hover:bg-[hsl(var(--muted)/0.3)] transition-colors">
              <td className="px-3 py-2.5">
                <div className="flex items-center gap-3">
                  {p.image_url ? (
                    <div
                      className="relative"
                      onMouseEnter={(e) => {
                        const rect = (e.target as HTMLElement).getBoundingClientRect()
                        setHoveredImg({ url: p.image_url, x: rect.right + 8, y: rect.top })
                      }}
                      onMouseLeave={() => setHoveredImg(null)}
                    >
                      <img
                        src={p.image_url}
                        alt=""
                        className="h-10 w-[30px] rounded object-cover bg-[hsl(var(--muted))]"
                        loading="lazy"
                      />
                    </div>
                  ) : (
                    <div className="h-10 w-[30px] rounded bg-[hsl(var(--muted))] flex items-center justify-center">
                      <ShoppingCart className="h-3.5 w-3.5 text-[hsl(var(--muted-foreground))]" />
                    </div>
                  )}
                  <div className="min-w-0">
                    <p className="text-[13px] font-medium truncate max-w-[280px]">{p.name || p.offer_id}</p>
                    <p className="text-[11px] text-[hsl(var(--muted-foreground))] truncate">{p.offer_id}</p>
                  </div>
                </div>
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums font-medium">{formatNumber(p.orders)}</td>
              <td className="px-3 py-2.5 text-right tabular-nums font-medium">{formatMoney(p.revenue)}</td>
              <td className="px-3 py-2.5 text-right tabular-nums">{p.returns > 0 ? formatNumber(p.returns) : '—'}</td>
              <td className="px-3 py-2.5 text-right tabular-nums">
                {p.return_pct > 0 ? (
                  <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${
                    p.return_pct > 10 ? 'bg-red-500/15 text-red-400' :
                    p.return_pct > 5 ? 'bg-amber-500/15 text-amber-400' :
                    'bg-emerald-500/15 text-emerald-400'
                  }`}>
                    {p.return_pct}%
                  </span>
                ) : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Hover preview */}
      {hoveredImg && (
        <div
          className="fixed z-50 rounded-xl overflow-hidden shadow-2xl border border-[hsl(var(--border))]"
          style={{ left: hoveredImg.x, top: hoveredImg.y, width: 160, height: 208 }}
        >
          <img src={hoveredImg.url} alt="" className="h-full w-full object-cover" />
        </div>
      )}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Return Reasons — compact inline bars (no recharts)
   ═══════════════════════════════════════════════════════════ */

function ReturnReasons({ data, total }: { data: SalesReturnReason[]; total: number }) {
  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center h-[100px] text-[hsl(var(--muted-foreground))] text-sm">
        Нет данных о возвратах за период
      </div>
    )
  }

  const maxCount = Math.max(...data.map(d => d.count), 1)

  return (
    <div className="space-y-3">
      {data.map((item) => (
        <div key={item.reason} className="space-y-1">
          <div className="flex items-center justify-between text-sm">
            <span className="text-[13px] truncate max-w-[70%]">{item.reason}</span>
            <span className="text-[13px] tabular-nums text-[hsl(var(--muted-foreground))] shrink-0 ml-2">
              {item.count} ({item.pct}%)
            </span>
          </div>
          <div className="h-2 w-full rounded-full bg-[hsl(var(--muted)/0.3)]">
            <div
              className="h-2 rounded-full bg-gradient-to-r from-red-500 to-red-400 transition-all duration-500"
              style={{ width: `${Math.round(item.count / maxCount * 100)}%` }}
            />
          </div>
        </div>
      ))}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Skeleton Loader
   ═══════════════════════════════════════════════════════════ */

function SalesSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {[...Array(3)].map((_, idx) => (
          <Card key={idx}><CardContent className="p-5"><Skeleton className="h-24 w-full" /></CardContent></Card>
        ))}
      </div>
      <Card><CardContent className="p-5"><Skeleton className="h-[340px] w-full" /></CardContent></Card>
      <Card><CardContent className="p-5"><Skeleton className="h-[300px] w-full" /></CardContent></Card>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page Component
   ═══════════════════════════════════════════════════════════ */

export default function SalesPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isOzon = currentShop?.marketplace === 'ozon'

  const [data, setData] = useState<SalesResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [periodValue, setPeriodValue] = useState<PeriodValue>({
    mode: 'quick',
    period: 7,
    dateRange: null,
  })

  const fetchData = useCallback(async () => {
    if (!currentShop || !isOzon) return
    setLoading(true)
    setError(null)
    try {
      const params: any = { shop_id: currentShop.id }
      if (periodValue.mode === 'custom' && periodValue.dateRange?.from) {
        const from = periodValue.dateRange.from
        const to = periodValue.dateRange.to ?? from
        const fmtDate = (d: Date) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
        params.date_from = fmtDate(from)
        params.date_to = fmtDate(to)
      } else {
        params.period = periodValue.period
      }
      const result = await getOzonSalesApi(params)
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки')
    } finally {
      setLoading(false)
    }
  }, [currentShop, isOzon, periodValue])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  useEffect(() => {
    const interval = setInterval(fetchData, 120_000)
    return () => clearInterval(interval)
  }, [fetchData])

  if (!currentShop) {
    return (
      <div className="flex h-[60vh] items-center justify-center text-[hsl(var(--muted-foreground))]">
        Выберите магазин
      </div>
    )
  }

  if (!isOzon) {
    return (
      <div className="flex h-[60vh] items-center justify-center text-[hsl(var(--muted-foreground))]">
        <div className="text-center space-y-2">
          <ShoppingCart className="h-12 w-12 mx-auto opacity-30" />
          <p className="text-lg font-medium">Раздел «Продажи» пока доступен только для Ozon</p>
          <p className="text-sm">Для WB используйте раздел «Обзор»</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6 pb-10">
      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Продажи</h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            {currentShop.name} • Ozon
          </p>
        </div>
        <div className="flex items-center gap-3">
          <PeriodSelector value={periodValue} onChange={setPeriodValue} />
          <button
            onClick={fetchData}
            disabled={loading}
            className="p-2 rounded-lg border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted)/0.5)] transition-colors disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* ── Error ── */}
      {error && (
        <Card className="border-red-500/30 bg-red-500/5">
          <CardContent className="p-4 text-red-400 text-sm">{error}</CardContent>
        </Card>
      )}

      {/* ── Loading ── */}
      {loading && !data && <SalesSkeleton />}

      {/* ── Content ── */}
      {data && (
        <>
          {/* ── KPI Cards — 3 cards, equal height ── */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <KpiCard
              title="Заказы"
              value={formatNumber(data.kpi.orders_count)}
              delta={data.kpi.orders_delta}
              subtitle="к пред. периоду"
              icon={ShoppingCart}
              accent="from-violet-500 to-purple-600"
              delay={0}
            />
            <KpiCard
              title="Продажи"
              value={formatMoney(data.kpi.revenue)}
              subtitle={`Ср. чек ${formatMoney(data.kpi.avg_check)}`}
              delta={data.kpi.revenue_delta}
              icon={DollarSign}
              accent="from-emerald-500 to-green-600"
              delay={0.05}
            />
            <KpiCard
              title="Возвраты"
              value={formatNumber(data.kpi.returns_count)}
              subtitle={`${data.kpi.returns_pct}% от заказов`}
              delta={data.kpi.returns_delta}
              invertDelta
              icon={RotateCcw}
              accent="from-red-500 to-rose-600"
              delay={0.1}
            />
          </div>

          {/* ── Sales Chart ── */}
          <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.15 }}>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base font-semibold">Динамика продаж</CardTitle>
              </CardHeader>
              <CardContent>
                <SalesChart data={data.daily} />
              </CardContent>
            </Card>
          </motion.div>

          {/* ── Top Products (full width) ── */}
          <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-base font-semibold">Топ товаров по продажам</CardTitle>
              </CardHeader>
              <CardContent>
                <TopProductsTable data={data.top_products} />
              </CardContent>
            </Card>
          </motion.div>

          {/* ── Geography (full width, collapsible) + Returns side by side ── */}
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
            {/* Geography — 2/3 width */}
            <motion.div
              className="xl:col-span-2"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.25 }}
            >
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-base font-semibold flex items-center gap-2">
                    <MapPin className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                    География продаж
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <GeoSection data={data.geo} />
                </CardContent>
              </Card>
            </motion.div>

            {/* Returns — 1/3 width */}
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.3 }}
            >
              <Card className="h-full">
                <CardHeader className="pb-2">
                  <CardTitle className="text-base font-semibold flex items-center gap-2">
                    <TrendingDown className="h-4 w-4 text-red-400" />
                    Причины возвратов
                  </CardTitle>
                  {data.returns.total > 0 && (
                    <p className="text-sm text-[hsl(var(--muted-foreground))]">
                      Всего: {formatNumber(data.returns.total)}
                    </p>
                  )}
                </CardHeader>
                <CardContent>
                  <ReturnReasons data={data.returns.by_reason} total={data.returns.total} />
                </CardContent>
              </Card>
            </motion.div>
          </div>
        </>
      )}
    </div>
  )
}
