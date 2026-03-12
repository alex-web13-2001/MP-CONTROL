import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Warehouse,
  BarChart3,
  AlertTriangle,
  ArrowDownRight,
  ArrowRightLeft,
  Timer,
  ChevronRight,
  MapPin,
  RefreshCw,
  Ban,
  Truck,
  ShieldAlert,
  Package,
  Search,
  TrendingUp,
  Download,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getOzonWarehouseAnalytics,
  WarehouseAnalyticsResponse,
  WarehouseDetail,
  type Recommendation,
  type StorageRiskSku,
  type CrossdockingSku,
  type DistributionPlanWarehouse,
  type DistributionPlanItem,
  downloadDistributionPlanExcel,
} from '@/api/warehouses'


/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

function fmt(v: number): string { return Math.round(v).toLocaleString('ru-RU') }
function fmtM(v: number): string { return Math.round(v).toLocaleString('ru-RU') + ' ₽' }
function fmtD(v: number | null): string { return v != null ? `${Math.round(v)} дн` : '—' }

/* ═══════════════════════════════════════════════════════════
   KPI Card
   ═══════════════════════════════════════════════════════════ */

function KpiCard({
  title, value, subtitle, icon: Icon, accent, delay,
}: {
  title: string; value: string; subtitle?: string
  icon: React.ElementType; accent: string; delay: number
}) {
  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay, duration: 0.4 }}>
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
          {subtitle && (
            <div className="mt-3 min-h-[24px]">
              <span className="text-[12px] text-[hsl(var(--muted-foreground))]">{subtitle}</span>
            </div>
          )}
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Status Badge
   ═══════════════════════════════════════════════════════════ */

function StatusBadge({ status }: { status: WarehouseDetail['status'] }) {
  const map: Record<string, { label: string; cls: string }> = {
    critical: { label: 'Критично', cls: 'bg-red-500/15 text-red-400 ring-red-500/20' },
    empty: { label: 'Пусто', cls: 'bg-red-500/15 text-red-400 ring-red-500/20' },
    attention: { label: 'Внимание', cls: 'bg-amber-500/15 text-amber-400 ring-amber-500/20' },
    ok: { label: 'Норма', cls: 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' },
    overstocked: { label: 'Перезатарка', cls: 'bg-purple-500/15 text-purple-400 ring-purple-500/20' },
    storage_fee: { label: 'Платное хр.', cls: 'bg-orange-500/15 text-orange-400 ring-orange-500/20' },
  }
  const s = map[status] || map.ok
  return (
    <span className={`inline-flex items-center rounded-full px-2.5 py-1 text-[11px] font-bold ring-1 ring-inset ${s.cls}`}>
      {s.label}
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   Warehouse Table
   ═══════════════════════════════════════════════════════════ */

type SortKey = 'daily_sales' | 'stock_free' | 'turnover_days' | 'revenue_period' | 'delivery_speed_avg_h' | 'pct_of_total_sales'
type SortDir = 'asc' | 'desc'

function WarehouseTable({ warehouses }: { warehouses: WarehouseDetail[] }) {
  const [expandedWh, setExpandedWh] = useState<string | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('daily_sales')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  const toggleSort = (key: SortKey) => {
    if (key === sortKey) setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    else { setSortKey(key); setSortDir(key === 'turnover_days' || key === 'delivery_speed_avg_h' ? 'asc' : 'desc') }
  }

  const sorted = useMemo(() =>
    [...warehouses].sort((a, b) => {
      const av = (a[sortKey] ?? 9999) as number
      const bv = (b[sortKey] ?? 9999) as number
      return sortDir === 'desc' ? bv - av : av - bv
    }), [warehouses, sortKey, sortDir])

  const thBase = 'px-3 py-3.5 text-right text-[12px] font-semibold whitespace-nowrap select-none'
  const tdCls = 'px-3 py-3 text-right tabular-nums text-[13px] whitespace-nowrap'

  const SortTh = ({ k, children, className = '' }: { k: SortKey; children: React.ReactNode; className?: string }) => (
    <th className={`${thBase} ${className} cursor-pointer transition-colors ${
      sortKey === k ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
    }`} onClick={() => toggleSort(k)}>
      <span className="inline-flex items-center gap-1 justify-end">
        {children}
        {sortKey === k && <span className="text-[10px] text-[hsl(var(--primary))]">{sortDir === 'desc' ? '▼' : '▲'}</span>}
      </span>
    </th>
  )

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.3, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        {/* Title */}
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Склады FBO</h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">
            Нажмите для детализации по SKU и географии
          </span>
        </div>

        <div className="overflow-auto max-h-[700px]">
          <table className="w-full border-collapse" style={{ minWidth: 1000 }}>
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-3 py-3.5 w-[40px]"></th>
                <th className="px-3 py-3.5 text-left text-[12px] font-semibold text-[hsl(var(--muted-foreground))] w-[200px]">Склад</th>
                <th className={`${thBase} text-center`}>Статус</th>
                <SortTh k="stock_free">Остаток</SortTh>
                <SortTh k="daily_sales">Продажи/д</SortTh>
                <SortTh k="turnover_days">Оборач.</SortTh>
                <SortTh k="revenue_period">Выручка</SortTh>
                <SortTh k="pct_of_total_sales">Доля</SortTh>
                <SortTh k="delivery_speed_avg_h">СВД, ч</SortTh>
                <th className={`${thBase} text-center`}>SKU</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((wh, idx) => {
                const isExpanded = expandedWh === wh.warehouse_name
                const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.06)]'
                return (
                  <React.Fragment key={wh.warehouse_name}>
                    <tr
                      className={`border-b border-[hsl(var(--border)/0.2)] transition-colors cursor-pointer ${
                        isExpanded ? 'bg-[hsl(var(--primary)/0.06)]' : `${rowBg} hover:bg-[hsl(var(--muted)/0.15)]`
                      } group`}
                      onClick={() => setExpandedWh(isExpanded ? null : wh.warehouse_name)}
                    >
                      <td className="px-3 py-3 text-center">
                        <motion.div animate={{ rotate: isExpanded ? 90 : 0 }} transition={{ duration: 0.15 }}>
                          <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                        </motion.div>
                      </td>
                      <td className="px-3 py-3">
                        <div className="min-w-0">
                          <div className="text-[13px] font-medium text-[hsl(var(--foreground))] truncate" title={wh.warehouse_name}>
                            {wh.warehouse_name}
                          </div>
                          <div className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-60 flex items-center gap-1">
                            <MapPin className="h-3 w-3 shrink-0" />
                            {wh.cluster}
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-3 text-center"><StatusBadge status={wh.status} /></td>
                      <td className={`${tdCls} font-semibold ${wh.stock_free === 0 ? 'text-red-400' : ''}`}>
                        {fmt(wh.stock_free)}
                        {wh.stock_reserved > 0 && (
                          <span className="text-[10px] text-[hsl(var(--muted-foreground))] ml-1">+{wh.stock_reserved}р</span>
                        )}
                      </td>
                      <td className={`${tdCls} font-semibold`}>{wh.daily_sales.toFixed(1)}</td>
                      <td className={tdCls}>
                        <span className={`font-semibold ${
                          !wh.turnover_days ? 'text-[hsl(var(--muted-foreground))]' :
                          wh.turnover_days < 14 ? 'text-red-400' :
                          wh.turnover_days < 30 ? 'text-amber-400' :
                          wh.turnover_days > 180 ? 'text-purple-400' :
                          wh.turnover_days > 160 ? 'text-orange-400' :
                          'text-emerald-400'
                        }`}>
                          {fmtD(wh.turnover_days)}
                        </span>
                      </td>
                      <td className={`${tdCls} text-[hsl(var(--muted-foreground))]`}>{fmtM(wh.revenue_period)}</td>
                      <td className={tdCls}>
                        <span className="font-medium">{wh.pct_of_total_sales}%</span>
                      </td>
                      <td className={tdCls}>
                        <span className={`font-medium ${
                          wh.delivery_speed_avg_h <= 28 ? 'text-emerald-400' :
                          wh.delivery_speed_avg_h <= 48 ? 'text-yellow-400' :
                          'text-[hsl(var(--muted-foreground))]'
                        }`}>
                          {wh.delivery_speed_avg_h}ч
                        </span>
                      </td>
                      <td className={`${tdCls} text-center text-[hsl(var(--muted-foreground))]`}>{wh.sku_count}</td>
                    </tr>

                    {/* Expanded detail */}
                    {isExpanded && (
                      <tr>
                        <td colSpan={10} className="p-0">
                          <div className="bg-[hsl(var(--muted)/0.06)] border-t border-b border-[hsl(var(--border)/0.3)]">
                            <div className="flex flex-col lg:flex-row gap-4 p-5">
                              {/* SKU details */}
                              <div className="flex-1 min-w-0">
                                <h4 className="text-[13px] font-semibold mb-3 text-[hsl(var(--foreground))]">
                                  Товары на складе ({wh.skus.length})
                                </h4>
                                <div className="rounded-xl border border-[hsl(var(--border)/0.3)] overflow-hidden">
                                  <div className="overflow-auto max-h-[300px]">
                                    <table className="w-full text-[12px]">
                                      <thead className="sticky top-0 bg-[hsl(var(--card))]">
                                        <tr className="text-[hsl(var(--muted-foreground))] text-[11px] uppercase tracking-wider">
                                          <th className="text-left py-2 px-3 font-semibold">Товар</th>
                                          <th className="text-right py-2 px-3 font-semibold">Остаток</th>
                                          <th className="text-right py-2 px-3 font-semibold">Прод/д</th>
                                          <th className="text-right py-2 px-3 font-semibold">Дн. запаса</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {wh.skus.map((sku) => (
                                          <tr key={sku.sku} className="border-t border-[hsl(var(--border)/0.1)] hover:bg-[hsl(var(--muted)/0.08)]">
                                            <td className="py-2 px-3 text-left">
                                              <div className="max-w-[200px] truncate font-medium" title={sku.name}>
                                                {sku.name || sku.offer_id}
                                              </div>
                                              <div className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-60">{sku.offer_id}</div>
                                            </td>
                                            <td className={`py-2 px-3 text-right tabular-nums font-semibold ${sku.stock === 0 ? 'text-red-400' : ''}`}>
                                              {sku.stock}
                                            </td>
                                            <td className="py-2 px-3 text-right tabular-nums">{sku.daily_sales.toFixed(1)}</td>
                                            <td className="py-2 px-3 text-right tabular-nums">
                                              <span className={`font-semibold ${
                                                sku.days_supply == null ? 'text-[hsl(var(--muted-foreground))]' :
                                                sku.days_supply < 14 ? 'text-red-400' :
                                                sku.days_supply > 180 ? 'text-purple-400' :
                                                'text-emerald-400'
                                              }`}>
                                                {sku.days_supply != null ? `${Math.round(sku.days_supply)}` : '—'}
                                              </span>
                                            </td>
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                </div>
                              </div>

                              {/* Geography */}
                              {wh.clusters_served.length > 0 && (
                                <div className="w-full lg:w-[350px] shrink-0">
                                  <h4 className="text-[13px] font-semibold mb-3 text-[hsl(var(--foreground))]">
                                    География заказов
                                  </h4>
                                  <div className="rounded-xl border border-[hsl(var(--border)/0.3)] overflow-hidden">
                                    <table className="w-full text-[12px]">
                                      <thead>
                                        <tr className="text-[hsl(var(--muted-foreground))] text-[11px] uppercase tracking-wider">
                                          <th className="text-left py-2 px-3 font-semibold">Кластер</th>
                                          <th className="text-right py-2 px-3 font-semibold">Кол-во</th>
                                          <th className="text-right py-2 px-3 font-semibold">Доля</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {wh.clusters_served.map(cl => (
                                          <tr key={cl.cluster} className="border-t border-[hsl(var(--border)/0.1)]">
                                            <td className="py-2 px-3 text-left">
                                              <span className="flex items-center gap-1">
                                                <MapPin className="h-3 w-3 text-[hsl(var(--muted-foreground))] shrink-0" />
                                                {cl.cluster}
                                              </span>
                                            </td>
                                            <td className="py-2 px-3 text-right tabular-nums">{cl.qty}</td>
                                            <td className="py-2 px-3 text-right tabular-nums font-semibold">{cl.share}%</td>
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                </div>
                              )}

                              {/* Per-warehouse costs */}
                              {(wh as any).costs && (wh as any).costs.total !== 0 && (
                                <div className="w-full lg:w-[240px] shrink-0">
                                  <h4 className="text-[13px] font-semibold mb-3 text-[hsl(var(--foreground))]">
                                    Расходы за период
                                  </h4>
                                  <div className="rounded-xl border border-[hsl(var(--border)/0.3)] overflow-hidden p-3 space-y-2">
                                    {(wh as any).costs.crossdocking !== 0 && (
                                      <div className="flex items-center justify-between text-[12px]">
                                        <span className="text-[hsl(var(--muted-foreground))]">🔄 Кроссдокинг</span>
                                        <span className="font-bold text-red-400 tabular-nums">{fmtM((wh as any).costs.crossdocking)}</span>
                                      </div>
                                    )}
                                    {(wh as any).costs.storage !== 0 && (
                                      <div className="flex items-center justify-between text-[12px]">
                                        <span className="text-[hsl(var(--muted-foreground))]">📦 Хранение</span>
                                        <span className="font-bold text-orange-400 tabular-nums">{fmtM((wh as any).costs.storage)}</span>
                                      </div>
                                    )}
                                    {(wh as any).costs.fbo_processing !== 0 && (
                                      <div className="flex items-center justify-between text-[12px]">
                                        <span className="text-[hsl(var(--muted-foreground))]">🏭 Обработка FBO</span>
                                        <span className="font-bold text-amber-400 tabular-nums">{fmtM((wh as any).costs.fbo_processing)}</span>
                                      </div>
                                    )}
                                    <div className="border-t border-[hsl(var(--border)/0.2)] pt-2 mt-2 flex items-center justify-between text-[12px]">
                                      <span className="font-semibold">Итого</span>
                                      <span className="font-bold text-red-400 tabular-nums">{fmtM((wh as any).costs.total)}</span>
                                    </div>
                                  </div>
                                </div>
                              )}
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-4 border-t border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.05)]">
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            Складов: <strong>{warehouses.length}</strong>
          </span>
          <span className="text-sm font-bold">
            Всего стоков:{' '}
            <span className="text-blue-400 text-base">
              {fmt(warehouses.reduce((s, w) => s + w.stock_free, 0))} ед.
            </span>
          </span>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Costs Card
   ═══════════════════════════════════════════════════════════ */

function CostsCard({ costs, period }: { costs: Record<string, { name: string; count: number; amount: number }>; period: number }) {
  const items = Object.entries(costs)
  if (items.length === 0) return null

  const labels: Record<string, string> = {
    MarketplaceServiceItemCrossdocking: '🔄 Кроссдокинг',
    OperationMarketplaceServiceStorage: '📦 Хранение',
    OperationMarketplaceSupplyAdditional: '🏭 Обработка FBO',
    OperationMarketplaceSupplyExpirationDateProcessing: '📅 Обработка сроков годности',
    OperationMarketplaceServiceSupplyInboundCargoShortage: '⚠️ Недостача при приёмке',
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.25, duration: 0.4 }}>
      <Card>
        <CardContent className="p-5">
          <h3 className="text-lg font-bold mb-4">Расходы за {period} дней</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {items.map(([key, val]) => (
              <div key={key} className="flex items-center justify-between p-3 rounded-xl bg-[hsl(var(--muted)/0.1)] border border-[hsl(var(--border)/0.2)]">
                <div>
                  <div className="text-[13px] font-medium">{labels[key] || val.name}</div>
                  <div className="text-[11px] text-[hsl(var(--muted-foreground))]">{fmt(val.count)} операций</div>
                </div>
                <div className={`text-lg font-bold tabular-nums ${val.amount < 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                  {fmtM(val.amount)}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Recommendations Panel
   ═══════════════════════════════════════════════════════════ */

function RecommendationsPanel({ recs }: { recs: Recommendation[] }) {
  const [expandedIdx, setExpandedIdx] = useState<number | null>(0) // first open by default

  const iconMap: Record<string, React.ElementType> = {
    move_stock: ArrowRightLeft,
    optimize_crossdocking: Truck,
    storage_warning: ShieldAlert,
    paid_storage: AlertTriangle,
  }
  const colorMap: Record<string, { bg: string; border: string; icon: string; headerBg: string }> = {
    high: { bg: 'bg-red-500/5', border: 'border-red-500/20', icon: 'text-red-400', headerBg: 'bg-red-500/10' },
    medium: { bg: 'bg-amber-500/5', border: 'border-amber-500/20', icon: 'text-amber-400', headerBg: 'bg-amber-500/10' },
    low: { bg: 'bg-blue-500/5', border: 'border-blue-500/20', icon: 'text-blue-400', headerBg: 'bg-blue-500/10' },
  }

  const totalSavings = recs.reduce((s, r) => s + (r.est_savings || 0), 0)

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.28, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-gradient-to-br from-amber-600 to-orange-500 shadow-lg">
              <ShieldAlert className="h-4.5 w-4.5 text-white" />
            </div>
            <div>
              <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">{'\u0420\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0430\u0446\u0438\u0438'} ({recs.length})</h2>
              <p className="text-[12px] text-[hsl(var(--muted-foreground))]">
                {recs.filter(r => r.severity === 'high').length} {'\u0432\u0430\u0436\u043d\u044b\u0445'} {'\u2022'} {recs.filter(r => r.severity === 'medium').length} {'\u0441\u043e\u0432\u0435\u0442\u043e\u0432'}
              </p>
            </div>
          </div>
          {totalSavings > 0 && (
            <div className="text-right">
              <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">{'\u041f\u043e\u0442\u0435\u043d\u0446\u0438\u0430\u043b \u044d\u043a\u043e\u043d\u043e\u043c\u0438\u0438'}</div>
              <div className="text-xl font-bold text-emerald-400 tabular-nums">~{fmtM(totalSavings)}{'/\u043c\u0435\u0441'}</div>
            </div>
          )}
        </div>

        <div className="p-4 space-y-3">
          {recs.map((rec, idx) => {
            const Icon = iconMap[rec.type] || AlertTriangle
            const colors = colorMap[rec.severity] || colorMap.medium
            const isOpen = expandedIdx === idx

            return (
              <div
                key={idx}
                className={`rounded-xl border ${colors.border} overflow-hidden transition-all`}
              >
                {/* Clickable header */}
                <div
                  className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors ${
                    isOpen ? colors.headerBg : `${colors.bg} hover:${colors.headerBg}`
                  }`}
                  onClick={() => setExpandedIdx(isOpen ? null : idx)}
                >
                  <motion.div animate={{ rotate: isOpen ? 90 : 0 }} transition={{ duration: 0.15 }}>
                    <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                  </motion.div>
                  <div className={`flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-[hsl(var(--card))] ${colors.icon}`}>
                    <Icon className="h-3.5 w-3.5" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">
                      {rec.title || rec.reason?.substring(0, 60)}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    {(rec.est_savings ?? 0) > 0 && (
                      <span className="text-[11px] font-bold text-emerald-400 tabular-nums">
                        ~{fmtM(rec.est_savings!)}
                      </span>
                    )}
                    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold ring-1 ring-inset ${
                      rec.severity === 'high'
                        ? 'bg-red-500/15 text-red-400 ring-red-500/20'
                        : 'bg-amber-500/15 text-amber-400 ring-amber-500/20'
                    }`}>
                      {rec.severity === 'high' ? '\u0412\u0430\u0436\u043d\u043e' : '\u0421\u043e\u0432\u0435\u0442'}
                    </span>
                  </div>
                </div>

                {/* Expanded content */}
                {isOpen && (
                  <div className="px-5 py-4 border-t border-[hsl(var(--border)/0.2)] space-y-4">
                    {/* Reason */}
                    <div>
                      <h4 className="text-[11px] font-bold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1.5">
                        {'\u041f\u043e\u0447\u0435\u043c\u0443'}
                      </h4>
                      <p className="text-[13px] text-[hsl(var(--foreground))] leading-relaxed">{rec.reason}</p>
                    </div>

                    {/* Impact */}
                    {rec.impact && (
                      <div>
                        <h4 className="text-[11px] font-bold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1.5">
                          {'\u041f\u043e\u0441\u043b\u0435\u0434\u0441\u0442\u0432\u0438\u044f / \u044d\u043a\u043e\u043d\u043e\u043c\u0438\u044f'}
                        </h4>
                        <p className="text-[13px] text-[hsl(var(--foreground))] leading-relaxed">{rec.impact}</p>
                      </div>
                    )}

                    {/* Action items */}
                    {rec.action_items && rec.action_items.length > 0 && (
                      <div>
                        <h4 className="text-[11px] font-bold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1.5">
                          {'\u0427\u0442\u043e \u0434\u0435\u043b\u0430\u0442\u044c'}
                        </h4>
                        <ul className="space-y-1.5">
                          {rec.action_items.map((item: string, i: number) => (
                            <li key={i} className="flex items-start gap-2 text-[13px] text-[hsl(var(--foreground))]">
                              <span className="text-[hsl(var(--primary))] font-bold mt-0.5">{'\u2192'}</span>
                              <span>{item}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {/* Affected SKUs */}
                    {rec.affected_sku_names && rec.affected_sku_names.length > 0 && (
                      <div>
                        <h4 className="text-[11px] font-bold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1.5">
                          {'\u0417\u0430\u0442\u0440\u043e\u043d\u0443\u0442\u044b\u0435 \u0442\u043e\u0432\u0430\u0440\u044b'}
                        </h4>
                        <div className="flex flex-wrap gap-1.5">
                          {rec.affected_sku_names.map((name: string, i: number) => (
                            <span
                              key={i}
                              className="inline-flex items-center px-2.5 py-1 rounded-lg bg-[hsl(var(--muted)/0.15)] text-[11px] font-medium text-[hsl(var(--foreground))] border border-[hsl(var(--border)/0.2)]"
                            >
                              {name}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Storage Risk SKUs Table
   ═══════════════════════════════════════════════════════════ */

function StorageRiskTable({ skus }: { skus: StorageRiskSku[] }) {
  const [expanded, setExpanded] = useState<number | null>(null)

  const paidCount = skus.filter(s => s.zone === 'paid').length
  const warningCount = skus.filter(s => s.zone === 'warning').length
  const totalMonthlyCost = skus
    .filter(s => s.zone === 'paid')
    .reduce((s, sk) => s + sk.est_monthly_cost, 0)

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.22, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-gradient-to-br from-orange-600 to-red-500 shadow-lg">
              <Package className="h-4.5 w-4.5 text-white" />
            </div>
            <div>
              <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Платное хранение по SKU</h2>
              <p className="text-[12px] text-[hsl(var(--muted-foreground))]">
                {paidCount} SKU в зоне платного хранения{warningCount > 0 && ` • ${warningCount} приближаются`}
              </p>
            </div>
          </div>
          {totalMonthlyCost > 0 && (
            <div className="text-right">
              <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">Прогноз / мес</div>
              <div className="text-xl font-bold text-red-400 tabular-nums">~{fmtM(totalMonthlyCost)}</div>
            </div>
          )}
        </div>

        <div className="overflow-auto max-h-[600px]">
          <table className="w-full border-collapse">
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-1 py-2.5 w-8"></th>
                <th className="px-3 py-2.5 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">SKU</th>
                <th className="px-2 py-2.5 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Зона</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Остаток</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Прод/д</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Оборач.</th>
                <th className="px-2 py-2.5 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Реклама</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">~Стоим/мес</th>
                <th className="px-3 py-2.5 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Рекомендация</th>
              </tr>
            </thead>
            <tbody>
              {skus.map((sk, idx) => {
                const isExp = expanded === sk.sku
                const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.06)]'
                return (
                  <React.Fragment key={sk.sku}>
                    <tr
                      className={`border-b border-[hsl(var(--border)/0.2)] transition-colors cursor-pointer ${
                        isExp ? 'bg-[hsl(var(--primary)/0.06)]' : `${rowBg} hover:bg-[hsl(var(--muted)/0.15)]`
                      } group`}
                      onClick={() => setExpanded(isExp ? null : sk.sku)}
                    >
                      <td className="px-2 py-2.5 text-center">
                        <motion.div animate={{ rotate: isExp ? 90 : 0 }} transition={{ duration: 0.15 }}>
                          <ChevronRight className="h-3.5 w-3.5 text-[hsl(var(--muted-foreground))]" />
                        </motion.div>
                      </td>
                      <td className="px-3 py-2.5">
                        <div className="text-[13px] font-medium" title={sk.name}>
                          {sk.name || sk.offer_id}
                        </div>
                        <div className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">{sk.offer_id}</div>
                      </td>
                      <td className="px-2 py-2.5 text-center">
                        <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-bold whitespace-nowrap ${
                          sk.zone === 'paid'
                            ? 'bg-red-500/15 text-red-400'
                            : 'bg-amber-500/15 text-amber-400'
                        }`}>
                          {sk.zone === 'paid' ? 'Платное' : 'Скоро'}
                        </span>
                      </td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[13px] font-semibold">{fmt(sk.total_stock)}</td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[13px]">{sk.daily_sales.toFixed(1)}</td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[13px]">
                        <span className={`font-semibold ${
                          sk.turnover_days == null ? 'text-purple-400' :
                          sk.turnover_days > 160 ? 'text-red-400' :
                          'text-orange-400'
                        }`}>
                          {sk.turnover_days != null ? `${Math.round(sk.turnover_days)} дн` : '∞'}
                        </span>
                      </td>
                      <td className="px-2 py-2.5 text-center">
                        {sk.ad_info?.has_ads ? (
                          <span className="inline-block rounded px-1.5 py-0.5 text-[10px] font-bold bg-blue-500/15 text-blue-400"
                                title={`Расход: ${fmtM(sk.ad_info.spend_30d)} за 30д, заказов: ${sk.ad_info.orders_30d}`}>
                            Да
                          </span>
                        ) : (
                          <span className="text-[10px] text-[hsl(var(--muted-foreground))]">Нет</span>
                        )}
                      </td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[13px]">
                        <span className={`font-bold ${sk.zone === 'paid' ? 'text-red-400' : 'text-amber-400'}`}>
                          {sk.est_monthly_cost > 0 ? `~${fmtM(sk.est_monthly_cost)}` : '—'}
                        </span>
                      </td>
                      <td className="px-3 py-2.5">
                        {sk.recommendation && (
                          <div title={sk.recommendation.reason}>
                            <div className={`text-[12px] font-semibold ${
                              sk.recommendation.severity === 'critical' ? 'text-red-500' :
                              sk.recommendation.severity === 'high' ? 'text-orange-500' :
                              'text-[hsl(var(--foreground)/0.7)]'
                            }`}>
                              {sk.recommendation.action}
                            </div>
                            <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
                              {sk.recommendation.reason}
                            </div>
                          </div>
                        )}
                      </td>
                    </tr>
                    {/* Expanded: warehouses */}
                    {isExp && (
                      <tr>
                        <td colSpan={9} className="p-0">
                          <div className="bg-[hsl(var(--muted)/0.06)] border-t border-b border-[hsl(var(--border)/0.3)] px-5 py-4">
                            <div className="flex items-center gap-4 mb-3">
                              <h4 className="text-[13px] font-semibold text-[hsl(var(--foreground))]">
                                Распределение по складам ({sk.warehouses.length})
                              </h4>
                              {sk.revenue_period > 0 && (
                                <span className="text-[12px] text-[hsl(var(--muted-foreground))]">
                                  Выручка за период: <strong className="text-emerald-400">{fmtM(sk.revenue_period)}</strong>
                                </span>
                              )}
                            </div>
                            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-2">
                              {sk.warehouses.map(wh => (
                                <div
                                  key={wh.warehouse_name}
                                  className="flex items-center justify-between p-2.5 rounded-lg bg-[hsl(var(--card))] border border-[hsl(var(--border)/0.2)]"
                                >
                                  <div className="min-w-0">
                                    <div className="text-[11px] font-medium truncate" title={wh.warehouse_name}>
                                      {wh.warehouse_name.replace('_РФЦ', '').replace('_МРФЦ', '')}
                                    </div>
                                  </div>
                                  <span className="text-[13px] font-bold tabular-nums ml-2 shrink-0">{wh.stock}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-4 border-t border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.05)]">
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            SKU в зоне риска: <strong className="text-red-400">{paidCount} платных</strong>
            {warningCount > 0 && <>, <strong className="text-amber-400">{warningCount} приближаются</strong></>}
          </span>
          <span className="text-[11px] text-[hsl(var(--muted-foreground))]">
            * Расчёт по тарифу ~0.07 ₽/л/день × объём × кол-во
          </span>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Distribution Plan (replaces old CrossdockingTable)
   Groups SKUs by destination warehouse with unified action plan
   ═══════════════════════════════════════════════════════════ */

function DistributionPlanTable({ plan, totalCdCost }: { plan: DistributionPlanWarehouse[]; totalCdCost: number }) {
  const [expandedWh, setExpandedWh] = useState<string | null>(null)

  const totalItems = plan.reduce((s, w) => s + w.items.length, 0)
  const totalTransfers = plan.reduce((s, w) => s + w.transfer_count, 0)
  const totalSupplies = plan.reduce((s, w) => s + w.supply_count, 0)
  const totalQty = plan.reduce((s, w) => s + w.total_qty, 0)
  const totalMonthlyCd = plan.reduce((s, w) => s + w.total_cd_cost_monthly, 0)

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.15, duration: 0.4 }}>
      {/* Summary KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-5">
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">Складов с CD</div>
          <div className="text-2xl font-bold text-[hsl(var(--foreground))] tabular-nums">{plan.length}</div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">{totalItems} SKU, {fmt(totalQty)} шт</div>
        </div>
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">CD расход/мес</div>
          <div className="text-2xl font-bold text-orange-400 tabular-nums">{fmtM(totalMonthlyCd)}</div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">Можно сократить</div>
        </div>
        <div className="p-4 rounded-xl border border-purple-500/20 bg-[hsl(var(--card))]">
          <div className="text-[11px] font-semibold uppercase tracking-wider text-purple-400 mb-1">⇄ Переместить</div>
          <div className="text-2xl font-bold text-purple-400 tabular-nums">{totalTransfers}</div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">SKU с избытком на других</div>
        </div>
        <div className="p-4 rounded-xl border border-emerald-500/20 bg-[hsl(var(--card))]">
          <div className="text-[11px] font-semibold uppercase tracking-wider text-emerald-400 mb-1">↓ Поставить</div>
          <div className="text-2xl font-bold text-emerald-400 tabular-nums">{totalSupplies}</div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">Нет стока нигде</div>
        </div>
      </div>

      {/* Warehouse blocks */}
      <div className="space-y-3">
        {plan.map((wh, whIdx) => {
          const isExp = expandedWh === wh.warehouse_name
          const whShort = wh.warehouse_name.replace('_РФЦ', '').replace('_МРФЦ', '')

          return (
            <motion.div
              key={wh.warehouse_name}
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: whIdx * 0.05, duration: 0.3 }}
              className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden"
            >
              {/* Warehouse header — clickable */}
              <div
                className="flex items-center justify-between px-5 py-4 cursor-pointer hover:bg-[hsl(var(--muted)/0.08)] transition-colors group"
                onClick={() => setExpandedWh(isExp ? null : wh.warehouse_name)}
              >
                <div className="flex items-center gap-3">
                  <motion.div animate={{ rotate: isExp ? 90 : 0 }} transition={{ duration: 0.15 }}>
                    <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                  </motion.div>
                  <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-blue-600 to-indigo-500 shadow">
                    <MapPin className="h-4 w-4 text-white" />
                  </div>
                  <div>
                    <div className="text-[15px] font-bold text-[hsl(var(--foreground))]">{whShort}</div>
                    <div className="text-[11px] text-[hsl(var(--muted-foreground))] flex items-center gap-2">
                      <span>{wh.items.length} SKU</span>
                      <span>•</span>
                      <span>{fmt(wh.total_qty)} шт нужно</span>
                      {wh.transfer_count > 0 && (
                        <span className="text-purple-400">⇄ {wh.transfer_count} перем.</span>
                      )}
                      {wh.supply_count > 0 && (
                        <span className="text-emerald-400">↓ {wh.supply_count} пост.</span>
                      )}
                    </div>
                    {wh.top_demand_cities && wh.top_demand_cities.length > 0 && (
                      <div className="text-[13px] text-blue-400/80 mt-0.5 flex items-center gap-1 flex-wrap">
                        <span className="text-[hsl(var(--muted-foreground))]">📍 Спрос:</span>
                        {wh.top_demand_cities.slice(0, 4).map((dc, i) => (
                          <span key={dc.city}>
                            {dc.city} <span className="text-[hsl(var(--muted-foreground))]">({dc.qty})</span>
                            {i < Math.min(wh.top_demand_cities!.length, 4) - 1 && ', '}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-[13px] font-bold text-orange-400 tabular-nums">~{fmtM(wh.total_cd_cost_monthly)}/мес</div>
                  <div className="text-[10px] text-[hsl(var(--muted-foreground))]">расход CD</div>
                </div>
              </div>

              {/* Expanded: items table */}
              <AnimatePresence>
                {isExp && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.2 }}
                    className="overflow-hidden"
                  >
                    <div className="border-t border-[hsl(var(--border))]">
                      <table className="w-full border-collapse">
                        <thead>
                          <tr className="border-b border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.06)]">
                            <th className="px-5 py-2.5 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Действие</th>
                            <th className="px-3 py-2.5 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">SKU</th>
                            <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Кол-во</th>
                            <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Прод/д</th>
                            <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">CD/мес</th>
                            <th className="px-5 py-2.5 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Источник</th>
                          </tr>
                        </thead>
                        <tbody>
                          {wh.items.map((item, idx) => {
                            const isTransfer = item.action === 'transfer'
                            const rowBg = idx % 2 === 0 ? '' : 'bg-[hsl(var(--muted)/0.04)]'
                            return (
                              <React.Fragment key={`${item.sku}-${idx}`}>
                              <tr className={`border-b border-[hsl(var(--border)/0.15)] ${rowBg}`}>
                                <td className="px-5 py-3">
                                  <span className={`inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[10px] font-bold ring-1 ring-inset ${
                                    isTransfer
                                      ? 'bg-purple-500/15 text-purple-400 ring-purple-500/20'
                                      : 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20'
                                  }`}>
                                    {isTransfer ? '⇄ Переместить' : '↓ Поставить'}
                                  </span>
                                </td>
                                <td className="px-3 py-3">
                                  <div className="text-[13px] font-medium" title={item.name}>{item.name || item.offer_id}</div>
                                  <div className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">{item.offer_id}</div>
                                </td>
                                <td className="px-3 py-3 text-right tabular-nums">
                                  <span className="text-[14px] font-bold text-[hsl(var(--foreground))]">{fmt(item.qty)} шт</span>
                                </td>
                                <td className="px-3 py-3 text-right tabular-nums text-[13px] text-[hsl(var(--muted-foreground))]">
                                  {item.daily_sales_cd.toFixed(1)}
                                </td>
                                <td className="px-3 py-3 text-right tabular-nums text-[13px] text-orange-400 font-semibold">
                                  ~{fmtM(item.est_cd_cost_monthly)}
                                </td>
                                <td className="px-5 py-3">
                                  {isTransfer && item.source_warehouse ? (
                                    <div>
                                      <div className="text-[12px] text-purple-400 font-medium">
                                        ← {item.source_warehouse.replace('_РФЦ', '').replace('_МРФЦ', '')}
                                      </div>
                                      <div className="text-[12px] text-[hsl(var(--muted-foreground))]">
                                        избыток {fmt(item.source_excess || 0)} шт • перемещение {fmtM(item.transfer_cost || 0)}
                                      </div>
                                    </div>
                                  ) : (
                                    <div className="text-[12px] text-emerald-400/70">Новая поставка</div>
                                  )}
                                </td>
                              </tr>
                              {/* Context row: reason + benefit */}
                              {(item.reason || item.benefit) && (
                                <tr className={rowBg}>
                                  <td colSpan={6} className="px-5 pb-3 pt-0">
                                    <div className="flex flex-col gap-1 ml-2 pl-4 border-l-2 border-blue-500/30">
                                      {item.reason && (
                                        <div className="text-[14px] text-[hsl(var(--foreground)/0.8)] leading-snug">
                                          <span className="text-blue-400/80">📍 </span>{item.reason}
                                        </div>
                                      )}
                                      {item.benefit && (
                                        <div className="text-[14px] text-emerald-400 font-semibold leading-snug">
                                          💡 {item.benefit}
                                        </div>
                                      )}
                                    </div>
                                  </td>
                                </tr>
                              )}
                              </React.Fragment>
                            )
                          })}
                        </tbody>
                      </table>

                      {/* Warehouse summary footer */}
                      <div className="flex items-center justify-between px-5 py-3 bg-[hsl(var(--muted)/0.06)] border-t border-[hsl(var(--border)/0.3)]">
                        <span className="text-[12px] text-[hsl(var(--muted-foreground))]">
                          Итого: {fmt(wh.total_qty)} шт на склад {whShort}
                        </span>
                        <div className="flex items-center gap-4">
                          {wh.total_transfer_cost > 0 && (
                            <span className="text-[12px] text-purple-400 font-semibold">
                              Стоимость перемещений: {fmtM(wh.total_transfer_cost)}
                            </span>
                          )}
                          <span className="text-[12px] text-orange-400 font-semibold">
                            CD экономия: ~{fmtM(wh.total_cd_cost_monthly)}/мес
                          </span>
                        </div>
                      </div>
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          )
        })}
      </div>

      {/* Global footer */}
      {plan.length > 0 && (
        <div className="mt-4 p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.05)] text-[12px] text-[hsl(var(--muted-foreground))]">
          * Тарифы перемещения Ozon FBO 2024-2026 • До 5л: 50,4 ₽/шт • Зависит от объёма товара
        </div>
      )}
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Skeleton
   ═══════════════════════════════════════════════════════════ */

function AnalyticsSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
        {[...Array(5)].map((_, idx) => (
          <Card key={idx}><CardContent className="p-5"><Skeleton className="h-20 w-full" /></CardContent></Card>
        ))}
      </div>
      <Card><CardContent className="p-5"><Skeleton className="h-[400px] w-full" /></CardContent></Card>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Period Selector
   ═══════════════════════════════════════════════════════════ */

const PERIOD_OPTIONS = [
  { value: 14, label: '14 дн' },
  { value: 30, label: '30 дн' },
  { value: 60, label: '60 дн' },
  { value: 90, label: '90 дн' },
]


/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

type TabKey = 'overview' | 'storage' | 'crossdocking' | 'warehouses'

const TABS: { key: TabKey; label: string; icon: React.ElementType }[] = [
  { key: 'overview', label: 'Обзор', icon: BarChart3 },
  { key: 'storage', label: 'Хранение', icon: Package },
  { key: 'crossdocking', label: 'Кроссдокинг', icon: Truck },
  { key: 'warehouses', label: 'Склады и география', icon: MapPin },
]

/* ── Overview Tab ─────────────────────────────────────────── */

function OverviewTab({ data, onNavigate }: { data: WarehouseAnalyticsResponse; onNavigate: (tab: TabKey) => void }) {
  const avgTurnover = data.kpi.avg_turnover_days
  const turnoverStatus = !avgTurnover ? 'ok' : avgTurnover > 160 ? 'critical' : avgTurnover > 120 ? 'warning' : 'ok'
  const deliveryStatus = !data.kpi.avg_delivery_h ? 'ok' : data.kpi.avg_delivery_h > 48 ? 'warning' : 'ok'

  const kpiColor = (s: string) =>
    s === 'critical' ? 'text-red-400' : s === 'warning' ? 'text-amber-400' : 'text-emerald-400'
  const kpiBadge = (s: string) =>
    s === 'critical' ? '🔴 Критично' : s === 'warning' ? '🟡 Внимание' : '🟢 Норма'

  // Top problem SKUs
  const topStorage = [...(data.storage_risk_skus || [])].filter(s => s.zone === 'paid').sort((a, b) => b.est_monthly_cost - a.est_monthly_cost).slice(0, 3)
  const topCd = [...(data.crossdocking_skus || [])].sort((a, b) => b.est_cd_cost_monthly - a.est_cd_cost_monthly).slice(0, 3)

  return (
    <div className="space-y-5">
      {/* Text Summary */}
      {data.summary && (
        <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
          <Card>
            <CardContent className="p-5">
              <div className="flex items-start gap-3">
                <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-violet-600 to-indigo-500 shadow-lg mt-0.5">
                  <BarChart3 className="h-4.5 w-4.5 text-white" />
                </div>
                <div>
                  <h3 className="text-[15px] font-bold mb-2">Сводка проблем</h3>
                  <p className="text-[13px] text-[hsl(var(--foreground))] leading-relaxed whitespace-pre-line">
                    {data.summary}
                  </p>
                  <div className="flex flex-wrap gap-2 mt-3">
                    {(data.storage_risk_skus?.length ?? 0) > 0 && (
                      <button onClick={() => onNavigate('storage')} className="text-[11px] font-semibold px-3 py-1.5 rounded-lg bg-orange-500/10 text-orange-400 border border-orange-500/20 hover:bg-orange-500/20 transition-colors">
                        Хранение → {data.storage_risk_skus.filter(s => s.zone === 'paid').length} SKU
                      </button>
                    )}
                    {(data.crossdocking_skus?.length ?? 0) > 0 && (
                      <button onClick={() => onNavigate('crossdocking')} className="text-[11px] font-semibold px-3 py-1.5 rounded-lg bg-blue-500/10 text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors">
                        Кроссдокинг → {data.crossdocking_skus.length} SKU
                      </button>
                    )}
                    <button onClick={() => onNavigate('warehouses')} className="text-[11px] font-semibold px-3 py-1.5 rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 hover:bg-emerald-500/20 transition-colors">
                      Склады → {data.kpi.total_warehouses}
                    </button>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      )}

      {/* KPI Cards with benchmarks */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
        <KpiCard title="Складов FBO" value={String(data.kpi.total_warehouses)} subtitle={`${fmt(data.kpi.total_stock)} ед. на стоках`} icon={Warehouse} accent="from-blue-600 to-blue-500" delay={0} />
        <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05, duration: 0.4 }}>
          <Card className="relative overflow-hidden h-full">
            <CardContent className="p-5 flex flex-col justify-between h-full">
              <div className="flex items-start justify-between">
                <div className="space-y-1 min-w-0">
                  <p className="text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Ср. оборачиваемость</p>
                  <p className={`text-2xl font-bold tracking-tight ${kpiColor(turnoverStatus)}`}>{fmtD(avgTurnover)}</p>
                </div>
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-emerald-600 to-emerald-500 shadow-lg">
                  <BarChart3 className="h-5 w-5 text-white" />
                </div>
              </div>
              <div className="mt-3 min-h-[24px] flex items-center gap-2">
                <span className="text-[12px] text-[hsl(var(--muted-foreground))]">{kpiBadge(turnoverStatus)}</span>
                <span className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-60">порог: 160 дн</span>
              </div>
            </CardContent>
          </Card>
        </motion.div>
        <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1, duration: 0.4 }}>
          <Card className="relative overflow-hidden h-full">
            <CardContent className="p-5 flex flex-col justify-between h-full">
              <div className="flex items-start justify-between">
                <div className="space-y-1 min-w-0">
                  <p className="text-[13px] font-medium text-[hsl(var(--muted-foreground))]">Ср. доставка (СВД)</p>
                  <p className={`text-2xl font-bold tracking-tight ${kpiColor(deliveryStatus)}`}>{data.kpi.avg_delivery_h ? `${data.kpi.avg_delivery_h}ч` : '—'}</p>
                </div>
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-cyan-600 to-cyan-500 shadow-lg">
                  <Timer className="h-5 w-5 text-white" />
                </div>
              </div>
              <div className="mt-3 min-h-[24px] flex items-center gap-2">
                <span className="text-[12px] text-[hsl(var(--muted-foreground))]">{kpiBadge(deliveryStatus)}</span>
                <span className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-60">норма: ≤48ч</span>
              </div>
            </CardContent>
          </Card>
        </motion.div>
        <KpiCard title="Кроссдокинг" value={fmtM(data.kpi.total_crossdocking)} subtitle={`Хранение: ${fmtM(data.kpi.total_storage_fee)}`} icon={ArrowDownRight} accent="from-orange-600 to-orange-500" delay={0.15} />
        <KpiCard title="Проблемные" value={`${data.kpi.critical_warehouses + data.kpi.overstocked_warehouses}`} subtitle={`${data.kpi.critical_warehouses} крит. • ${data.kpi.overstocked_warehouses} перезат.`} icon={AlertTriangle} accent="from-red-600 to-red-500" delay={0.2} />
      </div>

      {/* Costs */}
      <CostsCard costs={data.costs} period={data.kpi.period_days} />

      {/* Top problems mini-tables */}
      {(topStorage.length > 0 || topCd.length > 0) && (
        <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.3 }}>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {topStorage.length > 0 && (
              <Card>
                <CardContent className="p-5">
                  <div className="flex items-center justify-between mb-3">
                    <h3 className="text-[14px] font-bold flex items-center gap-2">
                      <span className="text-orange-400">📦</span> Топ платное хранение
                    </h3>
                    <button onClick={() => onNavigate('storage')} className="text-[11px] font-medium text-[hsl(var(--primary))] hover:underline">Все →</button>
                  </div>
                  <div className="space-y-2">
                    {topStorage.map(sk => (
                      <div key={sk.sku} className="flex items-center justify-between p-2.5 rounded-lg bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.2)]">
                        <div className="min-w-0 flex-1 mr-3">
                          <div className="text-[12px] font-medium truncate">{sk.name || sk.offer_id}</div>
                          <div className="text-[10px] text-[hsl(var(--muted-foreground))]">{sk.offer_id} • {fmt(sk.total_stock)} ед.</div>
                        </div>
                        <span className="text-[13px] font-bold text-red-400 tabular-nums shrink-0">~{fmtM(sk.est_monthly_cost)}/м</span>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
            {topCd.length > 0 && (
              <Card>
                <CardContent className="p-5">
                  <div className="flex items-center justify-between mb-3">
                    <h3 className="text-[14px] font-bold flex items-center gap-2">
                      <span className="text-blue-400">🔄</span> Топ расходы на кроссдокинг
                    </h3>
                    <button onClick={() => onNavigate('crossdocking')} className="text-[11px] font-medium text-[hsl(var(--primary))] hover:underline">Все →</button>
                  </div>
                  <div className="space-y-2">
                    {topCd.map(sk => (
                      <div key={sk.sku} className="flex items-center justify-between p-2.5 rounded-lg bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.2)]">
                        <div className="min-w-0 flex-1 mr-3">
                          <div className="text-[12px] font-medium truncate">{sk.name || sk.offer_id}</div>
                          <div className="text-[10px] text-[hsl(var(--muted-foreground))]">{sk.offer_id} • {fmt(sk.total_sold_via_cd)} через CD</div>
                        </div>
                        <span className="text-[13px] font-bold text-orange-400 tabular-nums shrink-0">~{fmtM(sk.est_cd_cost_monthly)}/м</span>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        </motion.div>
      )}
    </div>
  )
}

/* ── Storage Tab ──────────────────────────────────────────── */

function StorageTab({ data }: { data: WarehouseAnalyticsResponse }) {
  const [filter, setFilter] = useState<'all' | 'paid' | 'warning'>('all')
  const [search, setSearch] = useState('')

  const paidSkus = (data.storage_risk_skus || []).filter(s => s.zone === 'paid')
  const warningSkus = (data.storage_risk_skus || []).filter(s => s.zone === 'warning')
  const totalMonthly = paidSkus.reduce((s, sk) => s + sk.est_monthly_cost, 0)
  const deadStockSkus = paidSkus.filter(s => s.daily_sales === 0)
  const criticalSkus = paidSkus.filter(s => s.recommendation?.severity === 'critical')

  const filtered = useMemo(() => {
    let items = data.storage_risk_skus || []
    if (filter !== 'all') items = items.filter(s => s.zone === filter)
    if (search.trim()) {
      const q = search.toLowerCase()
      items = items.filter(s => (s.name || '').toLowerCase().includes(q) || (s.offer_id || '').toLowerCase().includes(q))
    }
    return items
  }, [data.storage_risk_skus, filter, search])

  const selCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[12px] font-medium transition-all cursor-pointer ${active ? 'bg-[hsl(var(--primary))] text-white shadow-md' : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'}`

  return (
    <div className="space-y-4">
      {/* Summary banner */}
      {paidSkus.length > 0 && (
        <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}
          className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.15)]">
          <div className="flex items-start gap-3">
            <AlertTriangle className="h-5 w-5 text-orange-500 shrink-0 mt-0.5" />
            <div className="flex-1">
              <div className="text-[14px] font-semibold text-[hsl(var(--foreground))]">
                {paidSkus.length} SKU в зоне платного хранения
                {warningSkus.length > 0 && <span className="font-normal text-[hsl(var(--muted-foreground))]"> + {warningSkus.length} приближаются</span>}
              </div>
              <div className="text-[13px] text-[hsl(var(--foreground)/0.8)] mt-1 leading-relaxed">
                Прогноз расходов: <strong className="text-red-500">~{fmtM(totalMonthly)}</strong>
                {deadStockSkus.length > 0 && (
                  <span> • <strong className="text-orange-500">{deadStockSkus.length} SKU с нулевыми продажами</strong> — мёртвый сток, рекомендуется вывезти</span>
                )}
                {criticalSkus.length > 0 && criticalSkus.length !== deadStockSkus.length && (
                  <span> • {criticalSkus.length} SKU требуют срочных действий</span>
                )}
              </div>
            </div>
            <div className="shrink-0 text-right">
              <div className="text-[11px] text-[hsl(var(--muted-foreground))]">Прогноз/мес</div>
              <div className="text-[17px] font-bold text-red-500">~{fmtM(totalMonthly)}</div>
            </div>
          </div>
        </motion.div>
      )}
      {/* Filters */}
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex gap-1">
          <button className={selCls(filter === 'all')} onClick={() => setFilter('all')}>Все ({data.storage_risk_skus?.length || 0})</button>
          <button className={selCls(filter === 'paid')} onClick={() => setFilter('paid')}>💸 Платное ({data.storage_risk_skus?.filter(s => s.zone === 'paid').length || 0})</button>
          <button className={selCls(filter === 'warning')} onClick={() => setFilter('warning')}>⚠️ Скоро ({data.storage_risk_skus?.filter(s => s.zone === 'warning').length || 0})</button>
        </div>
        <input
          type="text"
          placeholder="Поиск по артикулу или названию..."
          className="flex-1 min-w-[200px] px-3 py-1.5 rounded-lg text-[12px] bg-[hsl(var(--muted)/0.15)] border border-[hsl(var(--border)/0.3)] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.5)] focus:outline-none focus:ring-1 focus:ring-[hsl(var(--primary)/0.5)]"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
      </div>
      <StorageRiskTable skus={filtered} />
    </div>
  )
}

/* ── Crossdocking Tab ─────────────────────────────────────── */

function CrossdockingTab({ data, shopId, period }: { data: WarehouseAnalyticsResponse; shopId: number; period: number }) {
  const [filter, setFilter] = useState<'all' | 'transfer' | 'supply'>('all')
  const [search, setSearch] = useState('')
  const [downloading, setDownloading] = useState(false)

  const totalCdCost = Math.abs((data.costs as any)?.MarketplaceServiceItemCrossdocking?.amount || 0)

  // Filter distribution_plan by action type and search query
  const filteredPlan = useMemo(() => {
    const plan = data.distribution_plan || []
    return plan.map(wh => {
      let items = wh.items
      if (filter !== 'all') items = items.filter(i => i.action === filter)
      if (search.trim()) {
        const q = search.toLowerCase()
        items = items.filter(i => (i.name || '').toLowerCase().includes(q) || (i.offer_id || '').toLowerCase().includes(q))
      }
      return { ...wh, items }
    }).filter(wh => wh.items.length > 0)
  }, [data.distribution_plan, filter, search])

  // Count totals from distribution_plan for filter buttons
  const allPlanItems = (data.distribution_plan || []).flatMap(w => w.items)
  const totalTransferItems = allPlanItems.filter(i => i.action === 'transfer').length
  const totalSupplyItems = allPlanItems.filter(i => i.action === 'supply').length

  const selCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[12px] font-medium transition-all cursor-pointer ${active ? 'bg-[hsl(var(--primary))] text-white shadow-md' : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'}`

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex gap-1">
          <button className={selCls(filter === 'all')} onClick={() => setFilter('all')}>Все ({allPlanItems.length})</button>
          <button className={selCls(filter === 'transfer')} onClick={() => setFilter('transfer')}>⇄ Переместить ({totalTransferItems})</button>
          <button className={selCls(filter === 'supply')} onClick={() => setFilter('supply')}>↓ Поставить ({totalSupplyItems})</button>
        </div>
        <input
          type="text"
          placeholder="Поиск по артикулу или названию..."
          className="flex-1 min-w-[200px] px-3 py-1.5 rounded-lg text-[12px] bg-[hsl(var(--muted)/0.15)] border border-[hsl(var(--border)/0.3)] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.5)] focus:outline-none focus:ring-1 focus:ring-[hsl(var(--primary)/0.5)]"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
        <button
          onClick={async () => {
            setDownloading(true)
            try {
              await downloadDistributionPlanExcel({ shop_id: shopId, period })
            } catch (e) {
              console.error('Download failed', e)
            } finally {
              setDownloading(false)
            }
          }}
          disabled={downloading || allPlanItems.length === 0}
          className="flex items-center gap-2 px-4 py-1.5 rounded-lg text-[13px] font-semibold bg-emerald-600 hover:bg-emerald-500 text-white transition-all shadow-md disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap"
        >
          <Download className="h-4 w-4" />
          {downloading ? 'Скачивание...' : 'Скачать Excel'}
        </button>
      </div>
      <DistributionPlanTable plan={filteredPlan} totalCdCost={totalCdCost} />
    </div>
  )
}

/* ── Warehouses & Geography Tab ───────────────────────────── */

function WarehousesGeoTab({ data }: { data: WarehouseAnalyticsResponse }) {
  const [selectedSku, setSelectedSku] = useState<number | null>(null)
  const [geoSearch, setGeoSearch] = useState('')
  const [geoDropdownOpen, setGeoDropdownOpen] = useState(false)
  const [whSearch, setWhSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const dropdownRef = useRef<HTMLDivElement>(null)

  const skuGeo = data.sku_geography || []
  const selectedSkuData = useMemo(() => skuGeo.find(s => s.sku === selectedSku), [skuGeo, selectedSku])

  // Filter SKUs for autocomplete
  const filteredSkuOptions = useMemo(() => {
    if (!geoSearch.trim()) return skuGeo.slice(0, 30)
    const q = geoSearch.toLowerCase()
    return skuGeo.filter(s =>
      (s.offer_id || '').toLowerCase().includes(q) ||
      (s.name || '').toLowerCase().includes(q) ||
      String(s.sku).includes(q)
    ).slice(0, 30)
  }, [skuGeo, geoSearch])

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setGeoDropdownOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const filteredWarehouses = useMemo(() => {
    let whs = data.warehouses
    if (statusFilter !== 'all') whs = whs.filter(w => w.status === statusFilter)
    if (whSearch.trim()) {
      const q = whSearch.toLowerCase()
      whs = whs.filter(w => w.warehouse_name.toLowerCase().includes(q) || w.cluster.toLowerCase().includes(q))
    }
    return whs
  }, [data.warehouses, statusFilter, whSearch])

  const selCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[12px] font-medium transition-all cursor-pointer ${active ? 'bg-[hsl(var(--primary))] text-white shadow-md' : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'}`

  // Total sales for selected SKU
  const totalSalesQty = selectedSkuData?.sales_clusters?.reduce((s, c) => s + c.qty, 0) || 0
  const totalSalesRev = selectedSkuData?.sales_clusters?.reduce((s, c) => s + c.revenue, 0) || 0

  return (
    <div className="space-y-4">
      {/* SKU Geography with Autocomplete */}
      {skuGeo.length > 0 && (
        <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}>
          <Card>
            <CardContent className="p-5">
              <h3 className="text-[15px] font-bold mb-3 flex items-center gap-2">
                <MapPin className="h-4 w-4 text-[hsl(var(--primary))]" />
                География по товару
              </h3>

              {/* Custom Autocomplete */}
              <div className="relative" ref={dropdownRef}>
                <div className="flex items-center gap-2">
                  <div className="relative flex-1 max-w-[600px]">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-[hsl(var(--muted-foreground)/0.5)]" />
                    <input
                      type="text"
                      placeholder="Введите артикул или название товара..."
                      className="w-full pl-9 pr-8 py-2.5 rounded-xl text-[13px] bg-[hsl(var(--muted)/0.1)] border border-[hsl(var(--border)/0.3)] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.4)] focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary)/0.3)] focus:border-[hsl(var(--primary)/0.5)] transition-all"
                      value={geoSearch}
                      onChange={e => { setGeoSearch(e.target.value); setGeoDropdownOpen(true) }}
                      onFocus={() => setGeoDropdownOpen(true)}
                    />
                    {selectedSku && (
                      <button
                        onClick={() => { setSelectedSku(null); setGeoSearch('') }}
                        className="absolute right-2 top-1/2 -translate-y-1/2 h-5 w-5 flex items-center justify-center rounded-full bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-colors"
                      >
                        ×
                      </button>
                    )}
                  </div>
                </div>

                {/* Dropdown */}
                {geoDropdownOpen && filteredSkuOptions.length > 0 && (
                  <div className="absolute z-50 left-0 right-0 max-w-[600px] mt-1 py-1 rounded-xl bg-[hsl(var(--card))] border border-[hsl(var(--border)/0.3)] shadow-xl max-h-[300px] overflow-auto">
                    {filteredSkuOptions.map(s => (
                      <button
                        key={s.sku}
                        className={`w-full text-left px-4 py-2.5 text-[12px] hover:bg-[hsl(var(--muted)/0.15)] transition-colors flex items-center justify-between gap-3 ${selectedSku === s.sku ? 'bg-[hsl(var(--primary)/0.08)]' : ''}`}
                        onClick={() => {
                          setSelectedSku(s.sku)
                          setGeoSearch(s.offer_id || s.name || '')
                          setGeoDropdownOpen(false)
                        }}
                      >
                        <div className="min-w-0 flex-1">
                          <div className="font-medium truncate">{s.name || s.offer_id}</div>
                          <div className="text-[10px] text-[hsl(var(--muted-foreground))] mt-0.5">
                            {s.offer_id} • SKU {s.sku}
                          </div>
                        </div>
                        <div className="text-right shrink-0">
                          <div className="text-[11px] font-semibold tabular-nums">{fmt(s.total_stock)} ед.</div>
                          <div className="text-[10px] text-[hsl(var(--muted-foreground))]">{s.warehouses.length} скл.</div>
                        </div>
                      </button>
                    ))}
                  </div>
                )}
              </div>

              {/* Selected SKU details */}
              {selectedSkuData && (
                <div className="mt-5 space-y-4">
                  <div className="text-[13px] text-[hsl(var(--muted-foreground))]">
                    <strong className="text-[hsl(var(--foreground))]">{selectedSkuData.name || selectedSkuData.offer_id}</strong>
                    <span className="text-[11px] ml-2 px-2 py-0.5 rounded-md bg-[hsl(var(--muted)/0.15)]">{selectedSkuData.offer_id}</span>
                    {' '} — {fmt(selectedSkuData.total_stock)} ед. • {selectedSkuData.total_daily_sales.toFixed(1)} прод/день
                  </div>

                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                    {/* Where stocked */}
                    <div>
                      <h4 className="text-[12px] font-bold mb-2 text-[hsl(var(--muted-foreground))] uppercase tracking-wider flex items-center gap-1.5">
                        <Package className="h-3.5 w-3.5" /> Где лежит ({selectedSkuData.warehouses.length})
                      </h4>
                      <div className="rounded-xl border border-[hsl(var(--border)/0.3)] overflow-hidden">
                        <div className="overflow-auto max-h-[280px]">
                          <table className="w-full text-[12px]">
                            <thead className="sticky top-0 bg-[hsl(var(--card))]">
                              <tr className="text-[hsl(var(--muted-foreground))] text-[10px] uppercase tracking-wider">
                                <th className="text-left py-2 px-3 font-semibold">Склад</th>
                                <th className="text-right py-2 px-3 font-semibold">Сток</th>
                                <th className="text-right py-2 px-3 font-semibold">Прод/д</th>
                                <th className="text-right py-2 px-3 font-semibold">Запас</th>
                              </tr>
                            </thead>
                            <tbody>
                              {selectedSkuData.warehouses.map(wh => (
                                <tr key={wh.warehouse_name} className="border-t border-[hsl(var(--border)/0.1)] hover:bg-[hsl(var(--muted)/0.06)]">
                                  <td className="py-1.5 px-3">
                                    <div className="font-medium text-[11px]">{wh.warehouse_name}</div>
                                    <div className="text-[9px] text-[hsl(var(--muted-foreground))]">{wh.cluster}</div>
                                  </td>
                                  <td className={`py-1.5 px-3 text-right tabular-nums font-semibold ${wh.stock === 0 ? 'text-red-400' : ''}`}>{wh.stock}</td>
                                  <td className="py-1.5 px-3 text-right tabular-nums">{wh.daily_sales.toFixed(1)}</td>
                                  <td className="py-1.5 px-3 text-right tabular-nums">
                                    <span className={`font-semibold ${!wh.days_supply ? 'text-[hsl(var(--muted-foreground))]' : wh.days_supply < 14 ? 'text-red-400' : wh.days_supply > 180 ? 'text-purple-400' : 'text-emerald-400'}`}>
                                      {wh.days_supply != null ? `${Math.round(wh.days_supply)}д` : '—'}
                                    </span>
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    </div>

                    {/* Where sold */}
                    <div>
                      <h4 className="text-[12px] font-bold mb-2 text-[hsl(var(--muted-foreground))] uppercase tracking-wider flex items-center gap-1.5">
                        <TrendingUp className="h-3.5 w-3.5" /> Куда продаётся ({selectedSkuData.sales_clusters?.length || 0})
                      </h4>
                      <div className="rounded-xl border border-[hsl(var(--border)/0.3)] overflow-hidden">
                        <div className="overflow-auto max-h-[280px]">
                          <table className="w-full text-[12px]">
                            <thead className="sticky top-0 bg-[hsl(var(--card))]">
                              <tr className="text-[hsl(var(--muted-foreground))] text-[10px] uppercase tracking-wider">
                                <th className="text-left py-2 px-3 font-semibold">Кластер доставки</th>
                                <th className="text-right py-2 px-3 font-semibold">Заказы</th>
                                <th className="text-right py-2 px-3 font-semibold">Шт.</th>
                                <th className="text-right py-2 px-3 font-semibold">Выручка</th>
                                <th className="text-right py-2 px-3 font-semibold">Доля</th>
                              </tr>
                            </thead>
                            <tbody>
                              {(selectedSkuData.sales_clusters || []).map(cl => {
                                const share = totalSalesQty > 0 ? (cl.qty / totalSalesQty * 100) : 0
                                return (
                                  <tr key={cl.cluster} className="border-t border-[hsl(var(--border)/0.1)] hover:bg-[hsl(var(--muted)/0.06)]">
                                    <td className="py-1.5 px-3 font-medium text-[11px]">{cl.cluster}</td>
                                    <td className="py-1.5 px-3 text-right tabular-nums">{cl.orders}</td>
                                    <td className="py-1.5 px-3 text-right tabular-nums font-semibold">{cl.qty}</td>
                                    <td className="py-1.5 px-3 text-right tabular-nums">{fmtM(cl.revenue)}</td>
                                    <td className="py-1.5 px-3 text-right">
                                      <div className="flex items-center justify-end gap-1.5">
                                        <div className="w-12 h-1.5 rounded-full bg-[hsl(var(--muted)/0.15)] overflow-hidden">
                                          <div className="h-full rounded-full bg-[hsl(var(--primary))]" style={{ width: `${Math.min(share, 100)}%` }} />
                                        </div>
                                        <span className="text-[10px] tabular-nums w-8 text-right">{share.toFixed(0)}%</span>
                                      </div>
                                    </td>
                                  </tr>
                                )
                              })}
                              {(selectedSkuData.sales_clusters || []).length === 0 && (
                                <tr><td colSpan={5} className="py-4 text-center text-[hsl(var(--muted-foreground))]">Нет данных о продажах за период</td></tr>
                              )}
                            </tbody>
                            {totalSalesQty > 0 && (
                              <tfoot className="sticky bottom-0 bg-[hsl(var(--card))] border-t-2 border-[hsl(var(--border)/0.2)]">
                                <tr className="font-semibold text-[11px]">
                                  <td className="py-2 px-3">Итого</td>
                                  <td className="py-2 px-3 text-right tabular-nums">{selectedSkuData.sales_clusters?.reduce((s, c) => s + c.orders, 0)}</td>
                                  <td className="py-2 px-3 text-right tabular-nums">{totalSalesQty}</td>
                                  <td className="py-2 px-3 text-right tabular-nums">{fmtM(totalSalesRev)}</td>
                                  <td className="py-2 px-3 text-right">100%</td>
                                </tr>
                              </tfoot>
                            )}
                          </table>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </motion.div>
      )}

      {/* Warehouse filters */}
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex gap-1">
          <button className={selCls(statusFilter === 'all')} onClick={() => setStatusFilter('all')}>Все ({data.warehouses.length})</button>
          <button className={selCls(statusFilter === 'critical')} onClick={() => setStatusFilter('critical')}>🔴 Критично</button>
          <button className={selCls(statusFilter === 'overstocked')} onClick={() => setStatusFilter('overstocked')}>🟣 Перезатарка</button>
          <button className={selCls(statusFilter === 'ok')} onClick={() => setStatusFilter('ok')}>🟢 Норма</button>
        </div>
        <input
          type="text"
          placeholder="Поиск по складу..."
          className="flex-1 min-w-[200px] px-3 py-1.5 rounded-lg text-[12px] bg-[hsl(var(--muted)/0.15)] border border-[hsl(var(--border)/0.3)] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.5)] focus:outline-none focus:ring-1 focus:ring-[hsl(var(--primary)/0.5)]"
          value={whSearch}
          onChange={e => setWhSearch(e.target.value)}
        />
      </div>

      <WarehouseTable warehouses={filteredWarehouses} />
    </div>
  )
}


/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function WarehouseAnalyticsPage() {
  const { currentShop } = useAppStore()

  const [period, setPeriod] = useState(30)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<WarehouseAnalyticsResponse | null>(null)
  const [activeTab, setActiveTab] = useState<TabKey>('overview')

  const isOzon = currentShop?.marketplace === 'ozon'

  const fetchData = useCallback(async () => {
    if (!currentShop || !isOzon) return
    setLoading(true)
    setError(null)
    try {
      const result = await getOzonWarehouseAnalytics({
        shop_id: currentShop.id,
        period,
      })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки')
    } finally {
      setLoading(false)
    }
  }, [currentShop, isOzon, period])

  useEffect(() => { if (isOzon) fetchData() }, [fetchData, isOzon])

  if (!currentShop) return null

  if (!isOzon) {
    return (
      <div className="flex flex-col items-center justify-center py-20 space-y-4">
        <Ban className="h-16 w-16 text-[hsl(var(--muted-foreground))] opacity-30" />
        <p className="text-lg text-[hsl(var(--muted-foreground))]">Аналитика складов доступна только для магазинов Ozon</p>
      </div>
    )
  }

  const periodSelCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[13px] font-medium transition-all cursor-pointer ${
      active
        ? 'bg-[hsl(var(--primary))] text-white shadow-md'
        : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
    }`

  const tabCls = (active: boolean) =>
    `flex items-center gap-2 px-4 py-2.5 rounded-xl text-[13px] font-semibold transition-all cursor-pointer border ${
      active
        ? 'bg-[hsl(var(--primary)/0.12)] text-[hsl(var(--primary))] border-[hsl(var(--primary)/0.25)] shadow-sm'
        : 'bg-transparent text-[hsl(var(--muted-foreground))] border-transparent hover:bg-[hsl(var(--muted)/0.15)] hover:text-[hsl(var(--foreground))]'
    }`

  return (
    <div className="space-y-6 pb-10">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Аналитика складов</h1>
          <p className="text-[hsl(var(--muted-foreground))] mt-1">
            Распределение стоков, оборачиваемость, скорость доставки и расходы по складам Ozon FBO
          </p>
        </div>
        <button
          onClick={fetchData}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          Обновить
        </button>
      </div>

      {/* Period + Tabs */}
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1 }}>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between flex-wrap gap-3">
              {/* Tabs */}
              <div className="flex gap-1">
                {TABS.map(tab => (
                  <button key={tab.key} className={tabCls(activeTab === tab.key)} onClick={() => setActiveTab(tab.key)}>
                    <tab.icon className="h-4 w-4" />
                    {tab.label}
                  </button>
                ))}
              </div>
              {/* Period */}
              <div className="flex items-center gap-3">
                <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Период</p>
                <div className="flex gap-1">
                  {PERIOD_OPTIONS.map(o => (
                    <button key={o.value} className={periodSelCls(period === o.value)} onClick={() => setPeriod(o.value)}>
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {loading && !data ? (
        <AnalyticsSkeleton />
      ) : error ? (
        <Card>
          <CardContent className="p-10 text-center">
            <AlertTriangle className="h-10 w-10 mx-auto text-red-400 mb-3" />
            <p className="text-lg font-medium text-red-400">{error}</p>
            <button onClick={fetchData} className="mt-4 px-4 py-2 rounded-xl bg-[hsl(var(--primary))] text-white text-sm font-medium">
              Попробовать снова
            </button>
          </CardContent>
        </Card>
      ) : data ? (
        <>
          {activeTab === 'overview' && <OverviewTab data={data} onNavigate={setActiveTab} />}
          {activeTab === 'storage' && <StorageTab data={data} />}
          {activeTab === 'crossdocking' && <CrossdockingTab data={data} shopId={currentShop!.id} period={period} />}
          {activeTab === 'warehouses' && <WarehousesGeoTab data={data} />}
        </>
      ) : null}
    </div>
  )
}
