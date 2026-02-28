import { useState, useEffect, useCallback, useMemo } from 'react'
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
  Check,
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
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import { PeriodSelector, type PeriodValue } from '@/components/DateRangePicker'
import {
  getOzonSalesApi,
  getOzonProductDailyApi,
  type SalesResponse,
  type SalesDailyPoint,
  type SalesGeoItem,
  type SalesTopProduct,
  type SalesReturnReason,
  type ProductDailyPoint,
} from '@/api/sales'

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

const MONTHS_SHORT = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']

// 10 visually distinct product line colors
const PRODUCT_COLORS = [
  '#f97316', '#06b6d4', '#a855f7', '#f43f5e', '#14b8a6',
  '#eab308', '#ec4899', '#3b82f6', '#84cc16', '#6366f1',
]

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
   KPI Card — fixed height
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
   Sales Chart — with optional per-product overlay lines
   ═══════════════════════════════════════════════════════════ */

interface SelectedProduct {
  sku: number
  name: string
  color: string
}

function SalesChart({
  data,
  selectedProducts,
  productDailyData,
}: {
  data: SalesDailyPoint[]
  selectedProducts: SelectedProduct[]
  productDailyData: Record<string, ProductDailyPoint[]>
}) {
  // Merge per-product revenue into daily data
  const enrichedData = useMemo(() => {
    if (selectedProducts.length === 0) return data

    return data.map(point => {
      const enriched: Record<string, any> = { ...point }
      for (const sp of selectedProducts) {
        const productData = productDailyData[String(sp.sku)]
        const dayPoint = productData?.find(d => d.date === point.date)
        enriched[`product_${sp.sku}_revenue`] = dayPoint?.revenue ?? 0
        enriched[`product_${sp.sku}_orders`] = dayPoint?.orders ?? 0
      }
      return enriched
    })
  }, [data, selectedProducts, productDailyData])

  return (
    <ResponsiveContainer width="100%" height={340}>
      <ComposedChart data={enrichedData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
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
          formatter={(value: number, name: string) => {
            if (name === 'revenue') return [formatMoney(value), 'Продажи (все)']
            if (name === 'orders') return [formatNumber(value), 'Заказы (все)']
            if (name === 'returns') return [formatNumber(value), 'Возвраты']
            // Per-product lines
            const sp = selectedProducts.find(p => name === `product_${p.sku}_revenue`)
            if (sp) return [formatMoney(value), sp.name]
            return [formatNumber(value), name]
          }}
        />
        <Legend
          formatter={(value: string) => {
            if (value === 'orders') return 'Заказы'
            if (value === 'revenue') return 'Продажи'
            if (value === 'returns') return 'Возвраты'
            const sp = selectedProducts.find(p => value === `product_${p.sku}_revenue`)
            if (sp) return sp.name.length > 25 ? sp.name.slice(0, 23) + '…' : sp.name
            return value
          }}
        />
        {/* Base bars & lines */}
        <Bar
          yAxisId="left"
          dataKey="orders"
          fill="hsl(var(--primary))"
          radius={[4, 4, 0, 0]}
          opacity={selectedProducts.length > 0 ? 0.3 : 0.85}
        />
        <Line
          yAxisId="right"
          type="monotone"
          dataKey="revenue"
          stroke="#10b981"
          strokeWidth={selectedProducts.length > 0 ? 1.5 : 2.5}
          strokeOpacity={selectedProducts.length > 0 ? 0.4 : 1}
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

        {/* Per-product overlay lines */}
        {selectedProducts.map((sp) => (
          <Line
            key={sp.sku}
            yAxisId="right"
            type="monotone"
            dataKey={`product_${sp.sku}_revenue`}
            stroke={sp.color}
            strokeWidth={2.5}
            dot={{ r: 3, fill: sp.color }}
            activeDot={{ r: 5 }}
          />
        ))}
      </ComposedChart>
    </ResponsiveContainer>
  )
}

/* ═══════════════════════════════════════════════════════════
   Geography — full-width, collapsible
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

/* ─── Delta indicator ─── */
function Delta({ value, suffix = '%', invert = false }: { value: number; suffix?: string; invert?: boolean }) {
  if (value === 0) return null
  const isPositive = invert ? value < 0 : value > 0
  return (
    <span className={`inline-flex items-center gap-0.5 text-[10px] font-medium leading-none ${
      isPositive ? 'text-emerald-400' : 'text-red-400'
    }`}>
      {value > 0 ? '↑' : '↓'}
      {Math.abs(value).toFixed(suffix === 'pp' ? 2 : 1)}{suffix === 'pp' ? 'pp' : '%'}
    </span>
  )
}

function TopProductsTable({
  data,
  selectedSkus,
  onToggle,
}: {
  data: SalesTopProduct[]
  selectedSkus: Set<number>
  onToggle: (product: SalesTopProduct) => void
}) {
  const [hoveredImg, setHoveredImg] = useState<{ url: string; x: number; y: number } | null>(null)
  const hasAdData = data.some(p => p.ad_views > 0)

  const thCls = "px-3 py-2 text-right text-[12px] font-medium text-[hsl(var(--muted-foreground))] whitespace-nowrap"
  const tdCls = "px-3 py-2 text-right tabular-nums text-[13px]"

  return (
    <div className="overflow-x-auto relative">
      <table className="w-full text-sm" style={{ minWidth: hasAdData ? 1100 : undefined }}>
        <thead>
          {hasAdData && (
            <tr className="border-b border-[hsl(var(--border)/0.15)]">
              <th colSpan={2}></th>
              <th colSpan={4} className="text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] py-1.5 tracking-wide uppercase">
                Продажи
              </th>
              <th className="border-l border-[hsl(var(--border)/0.2)]" colSpan={6} style={{ textAlign: 'center', fontSize: 11, fontWeight: 600, color: 'hsl(var(--muted-foreground))', padding: '6px 0', letterSpacing: '0.05em', textTransform: 'uppercase' as const }}>
                Рекл. воронка
              </th>
            </tr>
          )}
          <tr className="border-b border-[hsl(var(--border))]">
            <th className="px-2 py-2 w-[36px]">
              <span className="text-[11px] text-[hsl(var(--muted-foreground))]">📊</span>
            </th>
            <th className="px-3 py-2 text-left text-[12px] font-medium text-[hsl(var(--muted-foreground))]">Товар</th>
            <th className={thCls}>Заказы</th>
            <th className={thCls}>Продажи</th>
            <th className={thCls}>Возвр.</th>
            <th className={thCls}>% возвр.</th>
            {hasAdData && (
              <>
                <th className={`${thCls} border-l border-[hsl(var(--border)/0.2)]`}>Показы</th>
                <th className={thCls}>Клики</th>
                <th className={thCls}>Корзины</th>
                <th className={thCls}>CTR</th>
                <th className={thCls}>CR→корз.</th>
                <th className={thCls}>CR→заказ</th>
              </>
            )}
          </tr>
        </thead>
        <tbody>
          {data.map((p) => {
            const isSelected = selectedSkus.has(p.sku)
            return (
              <tr
                key={p.sku}
                className={`border-b border-[hsl(var(--border)/0.3)] hover:bg-[hsl(var(--muted)/0.3)] transition-colors cursor-pointer ${
                  isSelected ? 'bg-[hsl(var(--primary)/0.08)]' : ''
                }`}
                onClick={() => onToggle(p)}
              >
                <td className="px-2 py-2 text-center">
                  <div className={`
                    h-5 w-5 rounded border-2 flex items-center justify-center mx-auto transition-all
                    ${isSelected
                      ? 'border-[hsl(var(--primary))] bg-[hsl(var(--primary))]'
                      : 'border-[hsl(var(--border))] hover:border-[hsl(var(--primary)/0.5)]'
                    }
                  `}>
                    {isSelected && <Check className="h-3 w-3 text-white" />}
                  </div>
                </td>
                <td className="px-3 py-2">
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
                      <div className="flex items-center gap-1.5">
                        {isSelected && (
                          <span
                            className="inline-block h-2.5 w-2.5 rounded-full shrink-0"
                            style={{ backgroundColor: PRODUCT_COLORS[
                              data.filter(d => selectedSkus.has(d.sku)).findIndex(d => d.sku === p.sku) % PRODUCT_COLORS.length
                            ] }}
                          />
                        )}
                        <p className="text-[13px] font-medium truncate max-w-[220px]">{p.name || p.offer_id}</p>
                      </div>
                      <p className="text-[11px] text-[hsl(var(--muted-foreground))] truncate">{p.offer_id}</p>
                    </div>
                  </div>
                </td>
                {/* Orders */}
                <td className={`${tdCls} font-medium`}>
                  <div className="flex flex-col items-end gap-0.5">
                    <span>{formatNumber(p.orders)}</span>
                    <Delta value={p.orders_delta} />
                  </div>
                </td>
                {/* Revenue */}
                <td className={`${tdCls} font-medium`}>
                  <div className="flex flex-col items-end gap-0.5">
                    <span>{formatMoney(p.revenue)}</span>
                    <Delta value={p.revenue_delta} />
                  </div>
                </td>
                {/* Returns */}
                <td className={tdCls}>{p.returns > 0 ? formatNumber(p.returns) : '—'}</td>
                {/* Return % */}
                <td className={tdCls}>
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
                {hasAdData && (
                  <>
                    {/* Ad Views */}
                    <td className={`${tdCls} border-l border-[hsl(var(--border)/0.15)]`}>
                      {p.ad_views > 0 ? (
                        <div className="flex flex-col items-end gap-0.5">
                          <span>{formatNumber(p.ad_views)}</span>
                          <Delta value={p.ad_views_delta} />
                        </div>
                      ) : <span className="text-[hsl(var(--muted-foreground)/0.4)]">—</span>}
                    </td>
                    {/* Ad Clicks */}
                    <td className={tdCls}>
                      {p.ad_clicks > 0 ? (
                        <div className="flex flex-col items-end gap-0.5">
                          <span>{formatNumber(p.ad_clicks)}</span>
                          <Delta value={p.ad_clicks_delta} />
                        </div>
                      ) : <span className="text-[hsl(var(--muted-foreground)/0.4)]">—</span>}
                    </td>
                    {/* Add to cart */}
                    <td className={tdCls}>
                      {p.ad_add_to_cart > 0 ? (
                        <div className="flex flex-col items-end gap-0.5">
                          <span>{formatNumber(p.ad_add_to_cart)}</span>
                          <Delta value={p.ad_add_to_cart_delta} />
                        </div>
                      ) : <span className="text-[hsl(var(--muted-foreground)/0.4)]">—</span>}
                    </td>
                    {/* CTR */}
                    <td className={tdCls}>
                      {p.ad_ctr > 0 ? (
                        <div className="flex flex-col items-end gap-0.5">
                          <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${
                            p.ad_ctr >= 3 ? 'bg-emerald-500/15 text-emerald-400' :
                            p.ad_ctr >= 1 ? 'bg-amber-500/15 text-amber-400' :
                            'bg-red-500/15 text-red-400'
                          }`}>
                            {p.ad_ctr}%
                          </span>
                          <Delta value={p.ad_ctr_delta} suffix="pp" />
                        </div>
                      ) : <span className="text-[hsl(var(--muted-foreground)/0.4)]">—</span>}
                    </td>
                    {/* CR → cart */}
                    <td className={tdCls}>
                      {p.ad_cart_rate > 0 ? (
                        <div className="flex flex-col items-end gap-0.5">
                          <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${
                            p.ad_cart_rate >= 15 ? 'bg-emerald-500/15 text-emerald-400' :
                            p.ad_cart_rate >= 5 ? 'bg-amber-500/15 text-amber-400' :
                            'bg-red-500/15 text-red-400'
                          }`}>
                            {p.ad_cart_rate}%
                          </span>
                          <Delta value={p.ad_cart_rate_delta} suffix="pp" />
                        </div>
                      ) : <span className="text-[hsl(var(--muted-foreground)/0.4)]">—</span>}
                    </td>
                    {/* CR → order */}
                    <td className={tdCls}>
                      {p.ad_order_rate > 0 ? (
                        <div className="flex flex-col items-end gap-0.5">
                          <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-semibold ${
                            p.ad_order_rate >= 30 ? 'bg-emerald-500/15 text-emerald-400' :
                            p.ad_order_rate >= 10 ? 'bg-amber-500/15 text-amber-400' :
                            'bg-red-500/15 text-red-400'
                          }`}>
                            {p.ad_order_rate}%
                          </span>
                          <Delta value={p.ad_order_rate_delta} suffix="pp" />
                        </div>
                      ) : <span className="text-[hsl(var(--muted-foreground)/0.4)]">—</span>}
                    </td>
                  </>
                )}
              </tr>
            )
          })}
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
   Return Reasons — compact inline bars
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

  // Per-product chart overlay
  const [selectedProducts, setSelectedProducts] = useState<SelectedProduct[]>([])
  const [productDailyData, setProductDailyData] = useState<Record<string, ProductDailyPoint[]>>({})
  const [productDailyLoading, setProductDailyLoading] = useState(false)

  const selectedSkus = useMemo(() => new Set(selectedProducts.map(p => p.sku)), [selectedProducts])

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
    // Clear product selections when period changes
    setSelectedProducts([])
    setProductDailyData({})
  }, [fetchData])

  useEffect(() => {
    const interval = setInterval(fetchData, 120_000)
    return () => clearInterval(interval)
  }, [fetchData])

  // Fetch per-product daily data when selection changes
  useEffect(() => {
    if (selectedProducts.length === 0 || !currentShop) {
      setProductDailyData({})
      return
    }

    const fetchProductDaily = async () => {
      setProductDailyLoading(true)
      try {
        const params: any = {
          shop_id: currentShop.id,
          skus: selectedProducts.map(p => p.sku).join(','),
        }
        if (periodValue.mode === 'custom' && periodValue.dateRange?.from) {
          const from = periodValue.dateRange.from
          const to = periodValue.dateRange.to ?? from
          const fmtDate = (d: Date) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
          params.date_from = fmtDate(from)
          params.date_to = fmtDate(to)
        } else {
          params.period = periodValue.period
        }
        const result = await getOzonProductDailyApi(params)
        setProductDailyData(result.products)
      } catch {
        // Silently fail — product overlay is non-critical
      } finally {
        setProductDailyLoading(false)
      }
    }

    fetchProductDaily()
  }, [selectedProducts, currentShop, periodValue])

  const handleToggleProduct = useCallback((product: SalesTopProduct) => {
    setSelectedProducts(prev => {
      const exists = prev.find(p => p.sku === product.sku)
      if (exists) {
        return prev.filter(p => p.sku !== product.sku)
      }
      if (prev.length >= 10) return prev // Max 10
      const usedColors = new Set(prev.map(p => p.color))
      const nextColor = PRODUCT_COLORS.find(c => !usedColors.has(c)) || PRODUCT_COLORS[prev.length % PRODUCT_COLORS.length]
      return [...prev, {
        sku: product.sku,
        name: product.name || product.offer_id,
        color: nextColor,
      }]
    })
  }, [])

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
          {/* ── KPI Cards ── */}
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
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base font-semibold">Динамика продаж</CardTitle>
                  {selectedProducts.length > 0 && (
                    <div className="flex items-center gap-2">
                      {productDailyLoading && (
                        <RefreshCw className="h-3.5 w-3.5 animate-spin text-[hsl(var(--muted-foreground))]" />
                      )}
                      <button
                        onClick={() => {
                          setSelectedProducts([])
                          setProductDailyData({})
                        }}
                        className="text-xs text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors px-2 py-1 rounded border border-[hsl(var(--border))]"
                      >
                        Сбросить ({selectedProducts.length})
                      </button>
                    </div>
                  )}
                </div>
                {selectedProducts.length > 0 && (
                  <p className="text-xs text-[hsl(var(--muted-foreground))] mt-1">
                    Выбрано товаров: {selectedProducts.length} — их продажи отображены на графике
                  </p>
                )}
              </CardHeader>
              <CardContent>
                <SalesChart
                  data={data.daily}
                  selectedProducts={selectedProducts}
                  productDailyData={productDailyData}
                />
              </CardContent>
            </Card>
          </motion.div>

          {/* ── Top Products with checkboxes ── */}
          <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
            <Card>
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base font-semibold">Топ товаров по продажам</CardTitle>
                  <p className="text-xs text-[hsl(var(--muted-foreground))]">
                    Нажмите на товар чтобы вывести его на график
                  </p>
                </div>
              </CardHeader>
              <CardContent>
                <TopProductsTable
                  data={data.top_products}
                  selectedSkus={selectedSkus}
                  onToggle={handleToggleProduct}
                />
              </CardContent>
            </Card>
          </motion.div>

          {/* ── Geography + Returns ── */}
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
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
