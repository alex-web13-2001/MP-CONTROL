import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { motion } from 'framer-motion'
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
  const iconMap: Record<string, React.ElementType> = {
    move_stock: ArrowRightLeft,
    optimize_crossdocking: Truck,
    storage_warning: ShieldAlert,
    paid_storage: AlertTriangle,
  }
  const colorMap: Record<string, { bg: string; border: string; icon: string }> = {
    high: { bg: 'bg-red-500/8', border: 'border-red-500/20', icon: 'text-red-400' },
    medium: { bg: 'bg-amber-500/8', border: 'border-amber-500/20', icon: 'text-amber-400' },
    low: { bg: 'bg-blue-500/8', border: 'border-blue-500/20', icon: 'text-blue-400' },
  }
  const labelMap: Record<string, string> = {
    move_stock: 'Переместить сток',
    optimize_crossdocking: 'Оптимизация кроссдокинга',
    storage_warning: 'Риск платного хранения',
    paid_storage: 'ПЛАТНОЕ хранение',
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.28, duration: 0.4 }}>
      <Card>
        <CardContent className="p-5">
          <h3 className="text-lg font-bold mb-4 flex items-center gap-2">
            <ShieldAlert className="h-5 w-5 text-amber-400" />
            Рекомендации ({recs.length})
          </h3>
          <div className="space-y-3">
            {recs.map((rec, idx) => {
              const Icon = iconMap[rec.type] || AlertTriangle
              const colors = colorMap[rec.severity] || colorMap.medium
              return (
                <div
                  key={idx}
                  className={`flex items-start gap-3 p-4 rounded-xl ${colors.bg} border ${colors.border} transition-all`}
                >
                  <div className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-[hsl(var(--card))] ${colors.icon}`}>
                    <Icon className="h-4 w-4" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <span className={`text-[11px] font-bold uppercase tracking-wider ${colors.icon}`}>
                        {labelMap[rec.type] || rec.type}
                      </span>
                      <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold ring-1 ring-inset ${
                        rec.severity === 'high'
                          ? 'bg-red-500/15 text-red-400 ring-red-500/20'
                          : 'bg-amber-500/15 text-amber-400 ring-amber-500/20'
                      }`}>
                        {rec.severity === 'high' ? 'Важно' : 'Совет'}
                      </span>
                    </div>
                    <p className="text-[13px] text-[hsl(var(--foreground))]">{rec.reason}</p>
                  </div>
                </div>
              )
            })}
          </div>
        </CardContent>
      </Card>
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
          <table className="w-full border-collapse" style={{ minWidth: 900 }}>
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-3 py-3 w-[40px]"></th>
                <th className="px-3 py-3 text-left text-[12px] font-semibold text-[hsl(var(--muted-foreground))] w-[250px]">SKU</th>
                <th className="px-3 py-3 text-center text-[12px] font-semibold text-[hsl(var(--muted-foreground))]">Зона</th>
                <th className="px-3 py-3 text-right text-[12px] font-semibold text-[hsl(var(--muted-foreground))]">Остаток</th>
                <th className="px-3 py-3 text-right text-[12px] font-semibold text-[hsl(var(--muted-foreground))]">Прод/д</th>
                <th className="px-3 py-3 text-right text-[12px] font-semibold text-[hsl(var(--muted-foreground))]">Оборач.</th>
                <th className="px-3 py-3 text-right text-[12px] font-semibold text-[hsl(var(--muted-foreground))]">Сверх лимита</th>
                <th className="px-3 py-3 text-right text-[12px] font-semibold text-[hsl(var(--muted-foreground))]">~Стоим/мес</th>
                <th className="px-3 py-3 text-right text-[12px] font-semibold text-[hsl(var(--muted-foreground))]">Складов</th>
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
                      <td className="px-3 py-3 text-center">
                        <motion.div animate={{ rotate: isExp ? 90 : 0 }} transition={{ duration: 0.15 }}>
                          <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                        </motion.div>
                      </td>
                      <td className="px-3 py-3">
                        <div className="max-w-[250px] truncate text-[13px] font-medium" title={sk.name}>
                          {sk.name || sk.offer_id}
                        </div>
                        <div className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-60">{sk.offer_id}</div>
                      </td>
                      <td className="px-3 py-3 text-center">
                        <span className={`inline-flex items-center rounded-full px-2.5 py-1 text-[11px] font-bold ring-1 ring-inset ${
                          sk.zone === 'paid'
                            ? 'bg-red-500/15 text-red-400 ring-red-500/20'
                            : 'bg-amber-500/15 text-amber-400 ring-amber-500/20'
                        }`}>
                          {sk.zone === 'paid' ? '💸 Платное' : '⚠️ Скоро'}
                        </span>
                      </td>
                      <td className="px-3 py-3 text-right tabular-nums text-[13px] font-semibold">{fmt(sk.total_stock)}</td>
                      <td className="px-3 py-3 text-right tabular-nums text-[13px]">{sk.daily_sales.toFixed(1)}</td>
                      <td className="px-3 py-3 text-right tabular-nums text-[13px]">
                        <span className={`font-semibold ${
                          sk.turnover_days == null ? 'text-purple-400' :
                          sk.turnover_days > 160 ? 'text-red-400' :
                          'text-orange-400'
                        }`}>
                          {sk.turnover_days != null ? `${Math.round(sk.turnover_days)} дн` : '∞'}
                        </span>
                      </td>
                      <td className="px-3 py-3 text-right tabular-nums text-[13px]">
                        {sk.days_over_threshold > 0 ? (
                          <span className="font-semibold text-red-400">+{Math.round(sk.days_over_threshold)} дн</span>
                        ) : sk.zone === 'warning' ? (
                          <span className="text-amber-400 font-medium">~{Math.round(160 - (sk.turnover_days || 0))} дн до</span>
                        ) : (
                          <span className="text-[hsl(var(--muted-foreground))]">—</span>
                        )}
                      </td>
                      <td className="px-3 py-3 text-right tabular-nums text-[13px]">
                        <span className={`font-bold ${sk.zone === 'paid' ? 'text-red-400' : 'text-amber-400'}`}>
                          {sk.est_monthly_cost > 0 ? `~${fmtM(sk.est_monthly_cost)}` : '—'}
                        </span>
                      </td>
                      <td className="px-3 py-3 text-center tabular-nums text-[13px] text-[hsl(var(--muted-foreground))]">
                        {sk.warehouses.length}
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

export default function WarehouseAnalyticsPage() {
  const { currentShop } = useAppStore()

  const [period, setPeriod] = useState(30)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<WarehouseAnalyticsResponse | null>(null)

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

  const selCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[13px] font-medium transition-all cursor-pointer ${
      active
        ? 'bg-[hsl(var(--primary))] text-white shadow-md'
        : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
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

      {/* Period control */}
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1 }}>
        <Card>
          <CardContent className="p-5">
            <div className="flex items-center gap-4">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">
                Период анализа
              </p>
              <div className="flex gap-1">
                {PERIOD_OPTIONS.map(o => (
                  <button key={o.value} className={selCls(period === o.value)} onClick={() => setPeriod(o.value)}>
                    {o.label}
                  </button>
                ))}
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
          {/* KPI Cards */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
            <KpiCard
              title="Складов FBO"
              value={String(data.kpi.total_warehouses)}
              subtitle={`${fmt(data.kpi.total_stock)} ед. на стоках`}
              icon={Warehouse}
              accent="from-blue-600 to-blue-500"
              delay={0}
            />
            <KpiCard
              title="Ср. оборачиваемость"
              value={fmtD(data.kpi.avg_turnover_days)}
              subtitle={`${data.kpi.total_skus} SKU`}
              icon={BarChart3}
              accent="from-emerald-600 to-emerald-500"
              delay={0.05}
            />
            <KpiCard
              title="Ср. СВД (доставка)"
              value={data.kpi.avg_delivery_h ? `${data.kpi.avg_delivery_h}ч` : '—'}
              subtitle="Средневзвешенная скорость"
              icon={Timer}
              accent="from-cyan-600 to-cyan-500"
              delay={0.1}
            />
            <KpiCard
              title="Кроссдокинг"
              value={fmtM(data.kpi.total_crossdocking)}
              subtitle={`Хранение: ${fmtM(data.kpi.total_storage_fee)}`}
              icon={ArrowDownRight}
              accent="from-orange-600 to-orange-500"
              delay={0.15}
            />
            <KpiCard
              title="Проблемные"
              value={`${data.kpi.critical_warehouses + data.kpi.overstocked_warehouses}`}
              subtitle={`${data.kpi.critical_warehouses} крит. • ${data.kpi.overstocked_warehouses} перезат.`}
              icon={AlertTriangle}
              accent="from-red-600 to-red-500"
              delay={0.2}
            />
          </div>

          {/* Costs */}
          <CostsCard costs={data.costs} period={data.kpi.period_days} />

          {/* Storage Risk SKUs */}
          {data.storage_risk_skus && data.storage_risk_skus.length > 0 && (
            <StorageRiskTable skus={data.storage_risk_skus} />
          )}

          {/* Recommendations */}
          {data.recommendations && data.recommendations.length > 0 && (
            <RecommendationsPanel recs={data.recommendations} />
          )}

          {/* Warehouse Table */}
          <WarehouseTable warehouses={data.warehouses} />
        </>
      ) : null}
    </div>
  )
}
