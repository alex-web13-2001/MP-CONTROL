/**
 * WB Warehouse Analytics — warehouse-centric analytics for Wildberries.
 * Shows: KPI, AI insights, warehouse table with expand, cross-map, costs, storage SKUs, recommendations.
 */
import React, { useState, useEffect, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Warehouse,
  BarChart3,
  AlertTriangle,
  ArrowRightLeft,
  ChevronRight,
  ChevronDown,
  MapPin,
  RefreshCw,
  Package,
  Truck,
  ShieldAlert,
  Lightbulb,
  Boxes,
  CircleDollarSign,
  Megaphone,
  Ban,
  Brain,
  Sparkles,
  ArrowRight,
  Zap,
  PackageX,
  ArrowDownToLine,
  TrendingUp,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import {
  getWBWarehouseAnalytics,
  getWBWarehouseAIAnalysis,
  type WBWarehouseAnalyticsResponse,
  type WBAnalyticsWarehouse,
  type WBCrossMapRow,
  type WBCostSummary,
  type WBStorageSku,
  type WBRecommendation,
  type AIWarehouseAnalysis,
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
  title, value, subtitle, icon: Icon, accent, delay, status, statusText,
}: {
  title: string; value: string; subtitle?: string
  icon: React.ElementType; accent: string; delay: number
  status?: 'good' | 'warn' | 'bad'
  statusText?: string
}) {
  const statusColor = status === 'good'
    ? 'border-emerald-500'
    : status === 'warn'
      ? 'border-amber-500'
      : status === 'bad'
        ? 'border-red-500'
        : 'border-transparent'

  const statusDot = status === 'good'
    ? 'bg-emerald-500'
    : status === 'warn'
      ? 'bg-amber-500'
      : status === 'bad'
        ? 'bg-red-500'
        : ''

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay, duration: 0.4 }}>
      <Card className={`relative overflow-hidden h-full border-b-[3px] ${statusColor}`}>
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
          <div className="mt-3 min-h-[24px] space-y-0.5">
            {subtitle && (
              <span className="text-[12px] text-[hsl(var(--muted-foreground))]">{subtitle}</span>
            )}
            {statusText && status && (
              <div className="flex items-center gap-1.5">
                <span className={`inline-block h-1.5 w-1.5 rounded-full ${statusDot}`} />
                <span className={`text-[11px] font-medium ${
                  status === 'good' ? 'text-emerald-400' : status === 'warn' ? 'text-amber-400' : 'text-red-400'
                }`}>{statusText}</span>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Status Badge
   ═══════════════════════════════════════════════════════════ */

function StatusBadge({ status }: { status: WBAnalyticsWarehouse['status'] }) {
  const map: Record<string, { label: string; cls: string }> = {
    critical: { label: 'Критич.', cls: 'bg-red-500/15 text-red-400 ring-red-500/20' },
    attention: { label: 'Внимание', cls: 'bg-amber-500/15 text-amber-400 ring-amber-500/20' },
    ok: { label: 'Норма', cls: 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' },
    overstocked: { label: 'Перезат.', cls: 'bg-purple-500/15 text-purple-400 ring-purple-500/20' },
    empty: { label: 'Пусто', cls: 'bg-slate-500/15 text-slate-400 ring-slate-500/20' },
  }
  const s = map[status] || map.ok
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold ring-1 ring-inset ${s.cls}`}>
      {s.label}
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   Warehouse Type Badge
   ═══════════════════════════════════════════════════════════ */

function TypeBadge({ type }: { type: WBAnalyticsWarehouse['warehouse_type'] }) {
  if (type === 'normal') return null
  const map = {
    food: { label: '🍕 Питание', cls: 'bg-orange-500/10 text-orange-400' },
    sgt: { label: '📦 СГТ', cls: 'bg-blue-500/10 text-blue-400' },
  }
  const s = map[type]
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium ${s.cls}`}>
      {s.label}
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   Cross% Indicator
   ═══════════════════════════════════════════════════════════ */

function CrossPctBar({ pct, orders }: { pct: number; orders: number }) {
  if (orders === 0) return <span className="text-[hsl(var(--muted-foreground))] opacity-40">—</span>
  const color = pct > 70 ? 'bg-red-500' : pct > 40 ? 'bg-amber-500' : 'bg-emerald-500'
  const textColor = pct > 70 ? 'text-red-400' : pct > 40 ? 'text-amber-400' : 'text-emerald-400'
  return (
    <div className="flex items-center gap-2">
      <div className="w-12 h-1.5 rounded-full bg-[hsl(var(--muted)/0.2)] overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${Math.min(pct, 100)}%` }} />
      </div>
      <span className={`text-[12px] tabular-nums font-semibold ${textColor}`}>{pct}%</span>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Warehouse Detail (expanded row)
   ═══════════════════════════════════════════════════════════ */

function WarehouseExpandedDetail({ wh }: { wh: WBAnalyticsWarehouse }) {
  const [selectedSku, setSelectedSku] = useState<number | null>(null)

  return (
    <motion.div
      initial={{ height: 0, opacity: 0 }}
      animate={{ height: 'auto', opacity: 1 }}
      exit={{ height: 0, opacity: 0 }}
      transition={{ duration: 0.2 }}
      className="overflow-hidden"
    >
      <div className="px-6 py-4 bg-[hsl(var(--muted)/0.04)] border-t border-[hsl(var(--border)/0.2)]">
        <div className="grid grid-cols-1 lg:grid-cols-[1fr_280px] gap-6">
          {/* SKU Table */}
          <div>
            <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-3 flex items-center gap-2">
              <Package className="h-4 w-4 text-blue-400" />
              Товары на складе ({wh.skus.length})
              <span className="text-[10px] font-normal text-[hsl(var(--muted-foreground))]">• клик — география</span>
            </h4>
            <div className="rounded-lg border border-[hsl(var(--border)/0.2)] overflow-hidden max-h-[400px] overflow-y-auto">
              <table className="w-full text-[12px]">
                <thead className="sticky top-0 bg-[hsl(var(--card))] z-10">
                  <tr className="border-b border-[hsl(var(--border)/0.2)]">
                    <th className="px-3 py-2 text-left font-semibold text-[hsl(var(--muted-foreground))]">Товар</th>
                    <th className="px-3 py-2 text-left font-semibold text-[hsl(var(--muted-foreground))]">Артикул</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))]">Остаток</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))]">Заказов</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))]">Кросс%</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))]">Дн.зап.</th>
                  </tr>
                </thead>
                <tbody>
                  {wh.skus.slice(0, 30).map((sku) => {
                    const isSelected = selectedSku === sku.nm_id
                    return (
                      <React.Fragment key={sku.nm_id}>
                        <tr
                          className={`border-b border-[hsl(var(--border)/0.1)] cursor-pointer transition-colors ${
                            isSelected ? 'bg-[hsl(var(--primary)/0.08)]' : 'hover:bg-[hsl(var(--muted)/0.06)]'
                          }`}
                          onClick={() => setSelectedSku(isSelected ? null : sku.nm_id)}
                        >
                          <td className="px-3 py-1.5 text-left">
                            <div className="truncate max-w-[180px] font-medium" title={sku.name}>
                              {sku.name || `#${sku.nm_id}`}
                            </div>
                          </td>
                          <td className="px-3 py-1.5 text-left">
                            <span className="text-[11px] text-[hsl(var(--muted-foreground))] font-mono">
                              {sku.vendor_code || '—'}
                            </span>
                          </td>
                          <td className="px-3 py-1.5 text-center tabular-nums">{fmt(sku.stock)}</td>
                          <td className="px-3 py-1.5 text-center tabular-nums">{fmt(sku.orders)}</td>
                          <td className="px-3 py-1.5 text-center">
                            {sku.orders > 0 ? (
                              <div className="flex items-center justify-center gap-1.5">
                                <div className="w-[40px] h-[6px] rounded-full bg-[hsl(var(--muted)/0.15)] overflow-hidden">
                                  <div
                                    className={`h-full rounded-full ${sku.cross_pct > 50 ? 'bg-red-500' : sku.cross_pct > 25 ? 'bg-amber-500' : 'bg-emerald-500'}`}
                                    style={{ width: `${Math.min(sku.cross_pct, 100)}%` }}
                                  />
                                </div>
                                <span className={`text-[11px] font-semibold tabular-nums ${
                                  sku.cross_pct > 50 ? 'text-red-400' : sku.cross_pct > 25 ? 'text-amber-400' : 'text-emerald-400'
                                }`}>{sku.cross_pct}%</span>
                              </div>
                            ) : <span className="text-[hsl(var(--muted-foreground))] opacity-40">—</span>}
                          </td>
                          <td className="px-3 py-1.5 text-center tabular-nums">
                            <span className={`font-semibold ${
                              sku.days_supply === null ? '' :
                              sku.days_supply < 14 ? 'text-red-400' :
                              sku.days_supply < 30 ? 'text-amber-400' :
                              sku.days_supply > 120 ? 'text-purple-400' :
                              'text-emerald-400'
                            }`}>
                              {sku.days_supply === null ? '∞' : sku.days_supply > 999 ? '999+' : Math.round(sku.days_supply)}
                            </span>
                          </td>
                        </tr>
                        {/* Expanded SKU geography */}
                        {isSelected && sku.geography.length > 0 && (
                          <tr>
                            <td colSpan={6} className="px-3 py-2 bg-[hsl(var(--muted)/0.06)]">
                              <div className="flex items-center gap-2 mb-2">
                                <MapPin className="h-3 w-3 text-emerald-400" />
                                <span className="text-[11px] font-bold text-[hsl(var(--foreground))]">
                                  География «{sku.name?.slice(0, 30) || sku.vendor_code}» с {wh.warehouse_name}
                                </span>
                              </div>
                              <div className="grid grid-cols-2 gap-x-6 gap-y-1">
                                {sku.geography.map((g) => (
                                  <div key={g.okrug} className="flex items-center gap-2">
                                    <div className="flex-1 min-w-0 flex items-center gap-1.5">
                                      <span className="text-[11px] font-medium truncate">
                                        {g.okrug.replace(' федеральный округ', '')}
                                      </span>
                                      <span className={`text-[8px] px-1 py-0 rounded-full font-bold ${
                                        g.is_local
                                          ? 'bg-emerald-500/15 text-emerald-400'
                                          : 'bg-red-500/10 text-red-400'
                                      }`}>
                                        {g.is_local ? 'СВОЙ' : 'КРОСС'}
                                      </span>
                                    </div>
                                    <div className="w-[60px] h-[4px] rounded-full bg-[hsl(var(--muted)/0.15)] overflow-hidden">
                                      <div
                                        className={`h-full rounded-full ${g.is_local ? 'bg-emerald-500' : 'bg-red-500/60'}`}
                                        style={{ width: `${Math.min(g.share, 100)}%` }}
                                      />
                                    </div>
                                    <span className="text-[10px] tabular-nums font-medium w-[45px] text-right">
                                      {g.orders} ({g.share}%)
                                    </span>
                                  </div>
                                ))}
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
          </div>

          {/* Overall warehouse geography + logistics */}
          <div>
            <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-3 flex items-center gap-2">
              <MapPin className="h-4 w-4 text-emerald-400" />
              География склада
            </h4>
            {wh.geography.length > 0 ? (
              <div className="space-y-2">
                {wh.geography.map((g) => (
                  <div key={g.okrug} className="flex items-center gap-3">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="text-[12px] font-medium truncate">
                          {g.okrug.replace(' федеральный округ', '')}
                        </span>
                        {g.is_local && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-emerald-500/15 text-emerald-400 font-bold">
                            СВОЙ
                          </span>
                        )}
                        {!g.is_local && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-red-500/10 text-red-400 font-bold">
                            КРОСС
                          </span>
                        )}
                      </div>
                      <div className="w-full h-1.5 rounded-full bg-[hsl(var(--muted)/0.15)] mt-1 overflow-hidden">
                        <div
                          className={`h-full rounded-full ${g.is_local ? 'bg-emerald-500' : 'bg-red-500/60'}`}
                          style={{ width: `${Math.min(g.share, 100)}%` }}
                        />
                      </div>
                    </div>
                    <span className="text-[12px] tabular-nums font-medium w-14 text-right">
                      {g.orders} ({g.share}%)
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-[12px] text-[hsl(var(--muted-foreground))] opacity-50">Нет данных за период</p>
            )}
            {wh.logistics_cost > 0 && (
              <div className="mt-4 p-3 rounded-lg bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.15)]">
                <div className="flex items-center justify-between">
                  <span className="text-[11px] font-medium text-[hsl(var(--muted-foreground))]">Логистика склада</span>
                  <span className="text-[13px] font-bold">{fmtM(wh.logistics_cost)}</span>
                </div>
                <span className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-60">
                  {wh.logistics_count} операций
                </span>
              </div>
            )}
          </div>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Warehouses Table
   ═══════════════════════════════════════════════════════════ */

function WarehousesTable({ warehouses }: { warehouses: WBAnalyticsWarehouse[] }) {
  const [expandedWh, setExpandedWh] = useState<string | null>(null)

  const thBase = 'px-4 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider whitespace-nowrap'
  const tdCls = 'px-4 py-3 text-center tabular-nums text-[13px] whitespace-nowrap'

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Склады</h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">
            {warehouses.length} складов • Нажмите для детализации
          </span>
        </div>

        <div className="overflow-auto max-h-[700px]">
          <table className="w-full border-collapse" style={{ minWidth: 1100 }}>
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-3 py-3 w-[32px]"></th>
                <th className="px-4 py-3 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider min-w-[200px]">Склад</th>
                <th className={`${thBase} min-w-[80px]`}>Статус</th>
                <th className={`${thBase} min-w-[75px]`}>Остаток</th>
                <th className={`${thBase} min-w-[75px]`}>Заказов</th>
                <th className={`${thBase} min-w-[65px]`}>В день</th>
                <th className={`${thBase} min-w-[75px]`}>Оборач.</th>
                <th className={`${thBase} min-w-[90px]`}>Кросс%</th>
                <th className={`${thBase} min-w-[100px]`}>Логистика ₽</th>
                <th className={`${thBase} min-w-[100px]`}>Хранение ₽</th>
                <th className={`${thBase} min-w-[75px]`}>Хр.коэф</th>
                <th className={`${thBase} min-w-[80px]`}>Приёмка</th>
                <th className={`${thBase} min-w-[50px]`}>SKU</th>
              </tr>
            </thead>
            <tbody>
              {warehouses.map((wh, idx) => {
                const isExpanded = expandedWh === wh.warehouse_name
                const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.04)]'
                return (
                  <React.Fragment key={wh.warehouse_name}>
                    <tr
                      className={`border-b border-[hsl(var(--border)/0.15)] transition-colors cursor-pointer ${
                        isExpanded ? 'bg-[hsl(var(--primary)/0.06)]' : `${rowBg} hover:bg-[hsl(var(--muted)/0.1)]`
                      }`}
                      onClick={() => setExpandedWh(isExpanded ? null : wh.warehouse_name)}
                    >
                      <td className="px-2 py-3 text-center">
                        <motion.div animate={{ rotate: isExpanded ? 90 : 0 }} transition={{ duration: 0.15 }}>
                          <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                        </motion.div>
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <Warehouse className="h-4 w-4 text-blue-400 shrink-0" />
                          <div className="min-w-0">
                            <div className="text-[13px] font-semibold text-[hsl(var(--foreground))] flex items-center gap-1.5 flex-wrap">
                              {wh.warehouse_name}
                              <TypeBadge type={wh.warehouse_type} />
                            </div>
                            {wh.okrug && (
                              <div className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-60 truncate">
                                {wh.okrug.replace(' федеральный округ', '')}
                              </div>
                            )}
                          </div>
                        </div>
                      </td>
                      <td className={`${tdCls} text-center`}><StatusBadge status={wh.status} /></td>
                      <td className={tdCls}>{fmt(wh.stock)}</td>
                      <td className={tdCls}>{fmt(wh.orders)}</td>
                      <td className={tdCls}>{wh.daily_sales.toFixed(2)}</td>
                      <td className={tdCls}>
                        <span className={`font-semibold ${
                          wh.turnover_days === null ? '' :
                          wh.turnover_days < 14 ? 'text-red-400' :
                          wh.turnover_days < 30 ? 'text-amber-400' :
                          wh.turnover_days > 120 ? 'text-purple-400' :
                          'text-emerald-400'
                        }`}>
                          {fmtD(wh.turnover_days)}
                        </span>
                      </td>
                      <td className={tdCls}>
                        <CrossPctBar pct={wh.cross_pct} orders={wh.orders} />
                      </td>
                      <td className={`${tdCls} ${wh.logistics_cost > 10000 ? 'text-amber-400 font-semibold' : 'text-[hsl(var(--muted-foreground))]'}`}>
                        {wh.logistics_cost > 0 ? fmtM(wh.logistics_cost) : '—'}
                      </td>
                      <td className={`${tdCls} ${wh.storage_cost_month > 5000 ? 'text-purple-400 font-semibold' : wh.storage_cost_month > 0 ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))]'}`}>
                        {wh.storage_cost_month > 0 ? fmtM(wh.storage_cost_month) : '—'}
                      </td>
                      <td className={tdCls}>
                        <span className={`text-[12px] ${
                          wh.storage_coef > 300 ? 'text-red-400 font-bold' :
                          wh.storage_coef > 200 ? 'text-amber-400 font-semibold' :
                          'text-[hsl(var(--muted-foreground))]'
                        }`}>
                          {wh.storage_coef > 0 ? `×${(wh.storage_coef / 100).toFixed(1)}` : '—'}
                        </span>
                      </td>
                      <td className={tdCls}>
                        <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium ${
                          wh.acceptance === 'Без коэфф.' ? 'bg-emerald-500/10 text-emerald-400' :
                          wh.acceptance === 'Закрыт' ? 'bg-red-500/10 text-red-400' :
                          'bg-amber-500/10 text-amber-400'
                        }`}>{wh.acceptance}</span>
                      </td>
                      <td className={`${tdCls} text-[hsl(var(--muted-foreground))]`}>{wh.sku_count}</td>
                    </tr>
                    <AnimatePresence>
                      {isExpanded && (
                        <tr>
                          <td colSpan={13} className="p-0">
                            <WarehouseExpandedDetail wh={wh} />
                          </td>
                        </tr>
                      )}
                    </AnimatePresence>
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Cross-Map (warehouse × okrug matrix)
   ═══════════════════════════════════════════════════════════ */

function CrossMapTable({ crossMap, okrugList }: { crossMap: WBCrossMapRow[]; okrugList: string[] }) {
  if (crossMap.length === 0) return null

  const shortOkrug = (s: string) => s.replace(' федеральный округ', '').replace('Северо-', 'С-')

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.3, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
            <ArrowRightLeft className="h-5 w-5 text-blue-400" />
            Кросс-карта
          </h2>
          <p className="text-[12px] text-[hsl(var(--muted-foreground))] mt-1">
            Откуда отгружается → куда доставляется. <span className="text-emerald-400 font-medium">Зелёный</span> = свой округ, <span className="text-red-400 font-medium">красный</span> = кросс-отправка
          </p>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-[12px]">
            <thead>
              <tr className="border-b border-[hsl(var(--border)/0.3)]">
                <th className="px-4 py-3 text-left font-bold text-[hsl(var(--muted-foreground))] sticky left-0 bg-[hsl(var(--card))] z-10 min-w-[160px]">
                  Склад ↓ / Округ →
                </th>
                {okrugList.map((okrug) => (
                  <th key={okrug} className="px-3 py-3 text-center font-semibold text-[hsl(var(--muted-foreground))] text-[10px] uppercase tracking-wider min-w-[80px]">
                    {shortOkrug(okrug)}
                  </th>
                ))}
                <th className="px-3 py-3 text-center font-bold text-[hsl(var(--muted-foreground))] text-[10px]">ИТОГО</th>
              </tr>
            </thead>
            <tbody>
              {crossMap.map((row, idx) => (
                <tr key={row.warehouse} className={`border-b border-[hsl(var(--border)/0.1)] ${idx % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''}`}>
                  <td className="px-4 py-2.5 text-left font-medium sticky left-0 bg-[hsl(var(--card))] z-10">
                    {row.warehouse}
                  </td>
                  {okrugList.map((okrug) => {
                    const cell = row.okrugs[okrug]
                    if (!cell || cell.count === 0) {
                      return <td key={okrug} className="px-3 py-2.5 text-center text-[hsl(var(--muted-foreground))] opacity-20">—</td>
                    }
                    const intensity = Math.min(cell.count / row.total_orders * 3, 1)
                    return (
                      <td key={okrug} className="px-3 py-2.5 text-center">
                        <span
                          className={`inline-flex items-center justify-center min-w-[28px] px-1.5 py-0.5 rounded-md text-[12px] font-bold tabular-nums ${
                            cell.is_local
                              ? 'bg-emerald-500/15 text-emerald-400'
                              : `bg-red-500/${Math.round(10 + intensity * 20)} text-red-400`
                          }`}
                        >
                          {cell.count}
                        </span>
                      </td>
                    )
                  })}
                  <td className="px-3 py-2.5 text-center font-bold text-[13px]">{row.total_orders}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Costs Summary
   ═══════════════════════════════════════════════════════════ */

export function CostsSummary({ costs, crossData }: { costs: WBCostSummary[]; crossData?: { crossCost: number; crossPct: number; crossOrders: number } }) {
  if (costs.length === 0) return null

  const iconMap: Record<string, React.ElementType> = {
    truck: Truck,
    package: Package,
    alert: ShieldAlert,
    factory: Boxes,
    circle: CircleDollarSign,
    megaphone: Megaphone,
    ban: Ban,
    // Ozon icon names (kebab-case from backend)
    'arrow-right-left': ArrowRightLeft,
    'credit-card': CircleDollarSign,
    'alert-triangle': ShieldAlert,
    undo: Ban,
    boxes: Boxes,
  }

  const colorMap: Record<string, string> = {
    'Логистика': 'bg-blue-500',
    'Хранение': 'bg-purple-500',
    'Хранение (факт)': 'bg-purple-500',
    'Штрафы': 'bg-red-500',
    'Удержания': 'bg-orange-500',
    'Приёмка': 'bg-amber-500',
    'Кроссдокинг': 'bg-orange-500',
    'Эквайринг': 'bg-slate-500',
    'Возвраты': 'bg-rose-500',
    'Недостача': 'bg-red-500',
    'Излишки': 'bg-slate-500',
    'ФБО обработка': 'bg-amber-500',
  }

  const iconBgMap: Record<string, string> = {
    'Логистика': 'bg-blue-500/15 text-blue-400',
    'Хранение': 'bg-purple-500/15 text-purple-400',
    'Хранение (факт)': 'bg-purple-500/15 text-purple-400',
    'Штрафы': 'bg-red-500/15 text-red-400',
    'Удержания': 'bg-orange-500/15 text-orange-400',
    'Приёмка': 'bg-amber-500/15 text-amber-400',
    'Кроссдокинг': 'bg-orange-500/15 text-orange-400',
    'Эквайринг': 'bg-slate-500/15 text-slate-400',
    'Возвраты': 'bg-rose-500/15 text-rose-400',
    'Недостача': 'bg-red-500/15 text-red-400',
    'Излишки': 'bg-slate-500/15 text-slate-400',
    'ФБО обработка': 'bg-amber-500/15 text-amber-400',
  }

  const maxAmount = Math.max(...costs.map(c => Math.abs(c.amount)), 1)
  const totalAmount = costs.reduce((s, c) => s + Math.abs(c.amount), 0)

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.35, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-4 border-b border-[hsl(var(--border))] flex items-center justify-between">
          <h2 className="text-lg font-bold text-[hsl(var(--foreground))]">Расходы за период</h2>
          <span className="text-[13px] font-semibold text-[hsl(var(--muted-foreground))]">
            Итого: {fmtM(totalAmount)}
          </span>
        </div>
        <div className="divide-y divide-[hsl(var(--border)/0.3)]">
          {costs.map((cost) => {
            const Icon = iconMap[cost.icon] || CircleDollarSign
            const barColor = colorMap[cost.label] || 'bg-slate-500'
            const iconCls = iconBgMap[cost.label] || 'bg-slate-500/15 text-slate-400'
            const pct = Math.abs(cost.amount) / maxAmount * 100
            const sharePct = totalAmount > 0 ? (Math.abs(cost.amount) / totalAmount * 100) : 0
            return (
              <React.Fragment key={cost.operation_type}>
              <div className="flex items-center gap-3 px-6 py-3 hover:bg-[hsl(var(--muted)/0.04)] transition-colors">
                <div className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg ${iconCls}`}>
                  <Icon className="h-3.5 w-3.5" />
                </div>
                <div className="w-[120px] shrink-0">
                  <span className="text-[13px] font-medium text-[hsl(var(--foreground))]">{cost.label}</span>
                </div>
                <div className="flex-1 min-w-0">
                  <div className="h-5 rounded-full bg-[hsl(var(--muted)/0.1)] overflow-hidden">
                    <div
                      className={`h-full rounded-full ${barColor} opacity-30`}
                      style={{ width: `${pct}%`, transition: 'width 0.6s ease' }}
                    />
                  </div>
                </div>
                <div className="w-[100px] shrink-0 text-right">
                  <span className="text-[14px] font-bold tabular-nums">{fmtM(cost.amount)}</span>
                </div>
                <div className="w-[50px] shrink-0 text-right">
                  <span className="text-[11px] text-[hsl(var(--muted-foreground))] tabular-nums">{sharePct.toFixed(0)}%</span>
                </div>
                <div className="w-[70px] shrink-0 text-right">
                  <span className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-60 tabular-nums">{fmt(cost.count)} оп.</span>
                </div>
              </div>
              {cost.label === 'Логистика' && crossData && crossData.crossOrders > 0 && (
                <div className="flex items-center gap-3 px-6 py-2 bg-red-500/[0.03]">
                  <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-red-500/10 text-red-400">
                    <ArrowRightLeft className="h-3.5 w-3.5" />
                  </div>
                  <div className="w-[120px] shrink-0">
                    <span className="text-[12px] font-medium text-red-400">↳ Кросс</span>
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="h-5 rounded-full bg-[hsl(var(--muted)/0.1)] overflow-hidden">
                      <div
                        className="h-full rounded-full bg-red-500 opacity-25"
                        style={{ width: `${Math.min(crossData.crossPct, 100)}%`, transition: 'width 0.6s ease' }}
                      />
                    </div>
                  </div>
                  <div className="w-[100px] shrink-0 text-right">
                    <span className="text-[13px] font-bold tabular-nums text-red-400">{fmtM(crossData.crossCost)}</span>
                  </div>
                  <div className="w-[50px] shrink-0 text-right">
                    <span className="text-[11px] text-red-400 font-semibold tabular-nums">{crossData.crossPct}%</span>
                  </div>
                  <div className="w-[70px] shrink-0 text-right">
                    <span className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-60 tabular-nums">{fmt(crossData.crossOrders)} зак.</span>
                  </div>
                </div>
              )}
              </React.Fragment>
            )
          })}
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Copyable Text (click-to-copy with feedback)
   ═══════════════════════════════════════════════════════════ */

function CopyableText({ text, prefix, className }: { text: string; prefix?: string; className?: string }) {
  const [copied, setCopied] = React.useState(false)
  const handleCopy = (e: React.MouseEvent) => {
    e.stopPropagation()
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    })
  }
  return (
    <button
      onClick={handleCopy}
      title={`Копировать ${text}`}
      className={`inline-flex items-center gap-1 rounded px-1 -mx-1 hover:bg-[hsl(var(--muted)/0.2)] transition-colors cursor-copy ${className || ''}`}
    >
      {prefix}{text}
      {copied ? (
        <svg className="w-3 h-3 text-emerald-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" /></svg>
      ) : (
        <svg className="w-3 h-3 opacity-30 hover:opacity-70 shrink-0 transition-opacity" fill="none" stroke="currentColor" viewBox="0 0 24 24"><rect x="9" y="9" width="13" height="13" rx="2" strokeWidth={2} /><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" strokeWidth={2} /></svg>
      )}
    </button>
  )
}

/* ═══════════════════════════════════════════════════════════
   Storage SKUs Table
   ═══════════════════════════════════════════════════════════ */

export function StorageSkusTable({ skus, isEstimate, periodDays = 30 }: { skus: WBStorageSku[]; isEstimate?: boolean; periodDays?: number }) {
  if (skus.length === 0) return null

  const [search, setSearch] = React.useState('')
  const [sortKey, setSortKey] = React.useState<string>('est_cost_period')
  const [sortDir, setSortDir] = React.useState<'asc' | 'desc'>('desc')
  const [expandedSkus, setExpandedSkus] = React.useState<Set<number>>(new Set())

  const toggleExpand = (nmId: number) => {
    setExpandedSkus(prev => {
      const next = new Set(prev)
      next.has(nmId) ? next.delete(nmId) : next.add(nmId)
      return next
    })
  }

  const hasForecast = skus.some(s => s.forecast_30d != null)

  // Filter
  const q = search.toLowerCase().trim()
  const filtered = q
    ? skus.filter(s =>
        (s.name || '').toLowerCase().includes(q) ||
        (s.vendor_code || '').toLowerCase().includes(q) ||
        String(s.nm_id).includes(q)
      )
    : skus

  // Sort
  const sorted = [...filtered].sort((a, b) => {
    let av: number, bv: number
    switch (sortKey) {
      case 'name': {
        const an = (a.name || a.vendor_code || '').toLowerCase()
        const bn = (b.name || b.vendor_code || '').toLowerCase()
        return sortDir === 'asc' ? an.localeCompare(bn) : bn.localeCompare(an)
      }
      case 'vol_liters': av = a.vol_liters; bv = b.vol_liters; break
      case 'total_stock': av = a.total_stock; bv = b.total_stock; break
      case 'daily_sales': av = a.daily_sales ?? 0; bv = b.daily_sales ?? 0; break
      case 'days_to_sell': av = a.days_to_sell ?? 99999; bv = b.days_to_sell ?? 99999; break
      case 'has_active_ads': av = a.has_active_ads ? 1 : 0; bv = b.has_active_ads ? 1 : 0; break
      case 'est_cost_period': av = (a as any).est_cost_period ?? a.est_cost_month; bv = (b as any).est_cost_period ?? b.est_cost_month; break
      case 'est_cost_month': av = a.est_cost_month; bv = b.est_cost_month; break
      case 'forecast_30d': av = a.forecast_30d ?? 0; bv = b.forecast_30d ?? 0; break
      default: av = (a as any).est_cost_period ?? a.est_cost_month; bv = (b as any).est_cost_period ?? b.est_cost_month
    }
    return sortDir === 'asc' ? av - bv : bv - av
  })

  // Totals (from filtered)
  const totalCostPeriod = filtered.reduce((sum, s) => sum + ((s as any).est_cost_period ?? s.est_cost_month), 0)
  const totalStock = filtered.reduce((sum, s) => sum + s.total_stock, 0)
  const totalForecast = filtered.reduce((sum, s) => sum + (s.forecast_30d ?? 0), 0)

  const handleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir(key === 'name' ? 'asc' : 'desc')
    }
  }

  const SortIcon = ({ col }: { col: string }) => (
    <span className={`ml-1 inline-flex flex-col text-[8px] leading-none ${sortKey === col ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground)/0.3)]'}`}>
      <span className={sortKey === col && sortDir === 'asc' ? 'opacity-100' : 'opacity-30'}>▲</span>
      <span className={sortKey === col && sortDir === 'desc' ? 'opacity-100' : 'opacity-30'}>▼</span>
    </span>
  )

  const thCls = "px-3 py-3 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase cursor-pointer select-none hover:text-[hsl(var(--foreground))] transition-colors"

  const colCount = hasForecast ? 9 : 8 // +1 for expand chevron, +1 for ads

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.4, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <div>
            <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Хранение по SKU</h2>
            <p className="text-[11px] text-[hsl(var(--muted-foreground))] mt-1">
              {filtered.length === skus.length ? `${skus.length} SKU` : `${filtered.length} из ${skus.length} SKU`}
              {' • Нажмите на строку для детализации по складам'}
            </p>
          </div>
          <div className="text-right flex gap-6">
            <div>
              <div className="text-[11px] text-[hsl(var(--muted-foreground))] font-medium">{isEstimate ? `Расч. хранение/${periodDays}д` : `Хранение за ${periodDays}д`}</div>
              <div className={`text-lg font-bold ${isEstimate ? 'text-[hsl(var(--muted-foreground))]' : 'text-red-400'}`}>{fmtM(totalCostPeriod)}</div>
              <div className="text-[10px] text-[hsl(var(--muted-foreground))]">{isEstimate ? 'оценка рисков' : 'при текущих остатках'}</div>
            </div>
            {hasForecast && (
              <div>
                <div className="text-[11px] text-[hsl(var(--muted-foreground))] font-medium">{isEstimate ? 'Расч. прогноз 30д' : 'Прогноз 30д'}</div>
                <div className={`text-lg font-bold ${isEstimate ? 'text-[hsl(var(--muted-foreground))]' : 'text-amber-400'}`}>{fmtM(totalForecast)}</div>
                <div className="text-[10px] text-[hsl(var(--muted-foreground))]">{isEstimate ? 'оценка рисков' : 'с учётом продаж'}</div>
              </div>
            )}
          </div>
        </div>

        {/* Search */}
        <div className="px-6 py-3 border-b border-[hsl(var(--border)/0.5)]">
          <div className="relative max-w-sm">
            <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[hsl(var(--muted-foreground)/0.5)]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input
              type="text"
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Поиск по названию, артикулу или ID..."
              className="w-full pl-9 pr-4 py-2 text-[13px] rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.5)] focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary)/0.3)] transition-all"
            />
          </div>
        </div>

        {/* Table */}
        <div className="overflow-auto max-h-[600px]">
          <table className="w-full border-collapse text-[13px]">
            <thead className="sticky top-0 bg-[hsl(var(--card))] z-10">
              <tr className="border-b border-[hsl(var(--border))]">
                <th className="w-[32px] px-1 py-3"></th>
                <th className={`${thCls} !text-left !px-4`} onClick={() => handleSort('name')}>
                  Товар<SortIcon col="name" />
                </th>
                <th className={thCls} onClick={() => handleSort('vol_liters')}>
                  Объём<SortIcon col="vol_liters" />
                </th>
                <th className={thCls} onClick={() => handleSort('total_stock')}>
                  Остаток<SortIcon col="total_stock" />
                </th>
                <th className={thCls} onClick={() => handleSort('daily_sales')}>
                  Прод/д<SortIcon col="daily_sales" />
                </th>
                <th className={thCls} onClick={() => handleSort('days_to_sell')} title="Дней до полной распродажи">
                  Дней<SortIcon col="days_to_sell" />
                </th>
                <th className={`${thCls} !text-center`} onClick={() => handleSort('has_active_ads')} title="Активная реклама за 3 дня">
                  Рекл.<SortIcon col="has_active_ads" />
                </th>
                <th className={thCls} onClick={() => handleSort('est_cost_period')}>
                  Хранение/{periodDays}д<SortIcon col="est_cost_period" />
                </th>
                {hasForecast && (
                  <th className={thCls} onClick={() => handleSort('forecast_30d')} title="Прогноз расходов с учётом продаж (остатки убывают)">
                    Прогноз 30д<SortIcon col="forecast_30d" />
                  </th>
                )}
              </tr>
            </thead>
            <tbody>
              {sorted.map((sku, idx) => {
                const isExpanded = expandedSkus.has(sku.nm_id)
                const daysColor = sku.days_to_sell == null ? '' :
                  sku.days_to_sell > 180 ? 'text-red-400' :
                  sku.days_to_sell > 90 ? 'text-amber-400' : 'text-emerald-400'
                const forecastColor = sku.forecast_30d == null ? '' :
                  sku.forecast_30d > 1000 ? 'text-red-400' :
                  sku.forecast_30d > 300 ? 'text-amber-400' : ''
                const whList = (sku.warehouses || []).sort((a, b) => b.cost_month - a.cost_month)
                return (
                  <React.Fragment key={sku.nm_id}>
                    <tr
                      className={`border-b border-[hsl(var(--border)/0.1)] cursor-pointer transition-colors ${
                        isExpanded ? 'bg-[hsl(var(--primary)/0.06)]' : `${idx % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''} hover:bg-[hsl(var(--muted)/0.08)]`
                      }`}
                      onClick={() => toggleExpand(sku.nm_id)}
                    >
                      <td className="px-1 py-2.5 text-center">
                        <span className={`inline-block transition-transform duration-150 text-[hsl(var(--muted-foreground))] text-[11px] ${isExpanded ? 'rotate-90' : ''}`}>▶</span>
                      </td>
                      <td className="px-4 py-2.5 text-left">
                        <div className="text-[12px] font-medium line-clamp-2 max-w-[300px]">
                          {sku.name || `#${sku.nm_id}`}
                        </div>
                        <div className="flex items-center gap-2 mt-0.5">
                          <CopyableText text={sku.vendor_code} className="text-[11px] font-bold text-[hsl(var(--muted-foreground))]" />
                          <CopyableText text={String(sku.nm_id)} prefix="ID: " className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-60" />
                        </div>
                      </td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[hsl(var(--muted-foreground))]">{sku.vol_liters.toFixed(1)}л</td>
                      <td className="px-3 py-2.5 text-right tabular-nums">{fmt(sku.total_stock)}</td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[hsl(var(--muted-foreground))]">
                        {sku.daily_sales > 0 ? sku.daily_sales.toFixed(1) : <span className="text-red-400/70 text-[11px]">нет</span>}
                      </td>
                      <td className={`px-3 py-2.5 text-right tabular-nums font-medium ${daysColor}`}>
                        {sku.days_to_sell != null ? (sku.days_to_sell > 365 ? '365+' : `${sku.days_to_sell}`) : '∞'}
                      </td>
                      <td className="px-3 py-2.5 text-center">
                        {sku.has_active_ads ? (
                          <Megaphone className="h-4 w-4 text-emerald-400 mx-auto" />
                        ) : (
                          <Megaphone className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.15)] mx-auto" />
                        )}
                      </td>
                      <td className="px-3 py-2.5 text-right tabular-nums font-bold">
                        <div className="flex items-center justify-end gap-1.5">
                          {(() => {
                            const costPeriod = (sku as any).est_cost_period ?? sku.est_cost_month
                            return (
                              <span className={costPeriod > 500 ? 'text-red-400' : costPeriod > 100 ? 'text-amber-400' : ''}>
                                {fmtM(costPeriod)}
                              </span>
                            )
                          })()}
                          {(sku as any).storage_source === 'actual' && (
                            <span className="inline-flex items-center rounded-full px-1.5 py-0.5 text-[9px] font-bold bg-emerald-500/15 text-emerald-400">факт</span>
                          )}
                        </div>
                      </td>
                      {hasForecast && (
                        <td className={`px-3 py-2.5 text-right tabular-nums font-bold ${forecastColor}`}>
                          {sku.forecast_30d != null ? fmtM(sku.forecast_30d) : '—'}
                        </td>
                      )}
                    </tr>
                    {/* Expanded: warehouse breakdown */}
                    {isExpanded && whList.length > 0 && (
                      <tr>
                        <td colSpan={colCount} className="p-0">
                          <div className="bg-[hsl(var(--muted)/0.06)] border-b border-[hsl(var(--border)/0.2)]">
                            <div className="px-6 py-3">
                              <div className="flex items-center gap-2 mb-2">
                                <Warehouse className="h-3.5 w-3.5 text-blue-400" />
                                <span className="text-[12px] font-bold text-[hsl(var(--foreground))]">
                                  Хранение по складам ({whList.length})
                                </span>
                              </div>
                              <div className="rounded-lg border border-[hsl(var(--border)/0.3)] overflow-hidden">
                                <table className="w-full text-[12px]">
                                  <thead>
                                    <tr className="bg-[hsl(var(--muted)/0.08)] border-b border-[hsl(var(--border)/0.2)]">
                                      <th className="px-4 py-2 text-left font-semibold text-[hsl(var(--muted-foreground))]">Склад</th>
                                      <th className="px-3 py-2 text-right font-semibold text-[hsl(var(--muted-foreground))]">Остаток</th>
                                      <th className="px-3 py-2 text-right font-semibold text-[hsl(var(--muted-foreground))]">Хранение/30д</th>
                                      <th className="px-3 py-2 text-right font-semibold text-[hsl(var(--muted-foreground))]">Доля</th>
                                      <th className="px-3 py-2 text-right font-semibold text-[hsl(var(--muted-foreground))]">Прогноз 30д</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {whList.map((wh, wi) => {
                                      const whShare = sku.est_cost_month > 0 ? (wh.cost_month / sku.est_cost_month * 100) : 0
                                      const whForecast = (wh as any).forecast_30d
                                      const forecastColor = whForecast == null ? '' :
                                        whForecast > 500 ? 'text-red-400' :
                                        whForecast > 100 ? 'text-amber-400' : 'text-[hsl(var(--foreground))]'
                                      return (
                                        <tr key={wh.warehouse} className={`border-b border-[hsl(var(--border)/0.1)] ${wi % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''}`}>
                                          <td className="px-4 py-2 text-left">
                                            <div className="flex items-center gap-1.5">
                                              <Warehouse className="h-3 w-3 text-blue-400/60 shrink-0" />
                                              <span className="font-medium text-[hsl(var(--foreground))]">{wh.warehouse}</span>
                                            </div>
                                          </td>
                                          <td className="px-3 py-2 text-right tabular-nums">{fmt(wh.stock)} ед.</td>
                                          <td className="px-3 py-2 text-right tabular-nums font-bold">
                                            <span className={wh.cost_month > 300 ? 'text-red-400' : wh.cost_month > 50 ? 'text-amber-400' : 'text-[hsl(var(--foreground))]'}>
                                              {fmtM(wh.cost_month)}
                                            </span>
                                          </td>
                                          <td className="px-3 py-2 text-right tabular-nums text-[hsl(var(--muted-foreground))]">
                                            {whShare > 0 ? `${whShare.toFixed(0)}%` : '—'}
                                          </td>
                                          <td className={`px-3 py-2 text-right tabular-nums font-medium ${forecastColor}`}>
                                            {whForecast != null ? fmtM(whForecast) : '—'}
                                          </td>
                                        </tr>
                                      )
                                    })}
                                  </tbody>
                                </table>
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                    {isExpanded && whList.length === 0 && (
                      <tr>
                        <td colSpan={colCount} className="px-8 py-3 text-[12px] text-[hsl(var(--muted-foreground))] opacity-60 bg-[hsl(var(--muted)/0.06)]">
                          Нет данных по складам
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
              {sorted.length === 0 && (
                <tr>
                  <td colSpan={colCount} className="px-4 py-8 text-center text-[hsl(var(--muted-foreground))]">
                    Ничего не найдено по запросу «{search}»
                  </td>
                </tr>
              )}
            </tbody>
            <tfoot>
              <tr className="border-t-2 border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.06)]">
                <td className="px-1 py-3"></td>
                <td className="px-4 py-3 text-left font-bold text-[13px]">Итого ({filtered.length} SKU)</td>
                <td className="px-3 py-3"></td>
                <td className="px-3 py-3 text-right tabular-nums font-semibold">{fmt(totalStock)}</td>
                <td className="px-3 py-3"></td>
                <td className="px-3 py-3"></td>
                <td className="px-3 py-3"></td>
                <td className="px-3 py-3 text-right tabular-nums font-bold text-red-400 text-[14px]">{fmtM(totalCostPeriod)}</td>
                {hasForecast && (
                  <td className="px-3 py-3 text-right tabular-nums font-bold text-amber-400 text-[14px]">{fmtM(totalForecast)}</td>
                )}
              </tr>
            </tfoot>
          </table>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Recommendations
   ═══════════════════════════════════════════════════════════ */

export function RecommendationsPanel({ recommendations }: { recommendations: WBRecommendation[] }) {
  if (recommendations.length === 0) return null

  const severityStyles: Record<string, string> = {
    high: 'border-red-500/30 bg-red-500/5',
    medium: 'border-amber-500/30 bg-amber-500/5',
    low: 'border-blue-500/30 bg-blue-500/5',
  }

  const severityIcon: Record<string, React.ElementType> = {
    high: AlertTriangle,
    medium: ShieldAlert,
    low: Lightbulb,
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.45, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
            <Lightbulb className="h-5 w-5 text-amber-400" />
            Рекомендации оптимизации
          </h2>
        </div>
        <div className="p-4 space-y-3">
          {recommendations.map((rec, idx) => {
            const Icon = severityIcon[rec.severity] || Lightbulb
            return (
              <div key={idx} className={`p-4 rounded-xl border ${severityStyles[rec.severity] || severityStyles.low}`}>
                <div className="flex items-start gap-3">
                  <div className={`shrink-0 mt-0.5 ${rec.severity === 'high' ? 'text-red-400' : rec.severity === 'medium' ? 'text-amber-400' : 'text-blue-400'}`}>
                    <Icon className="h-5 w-5" />
                  </div>
                  <div className="min-w-0 flex-1">
                    <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))]">{rec.title}</h4>
                    <p className="text-[12px] text-[hsl(var(--muted-foreground))] mt-1">{rec.reason}</p>
                    {rec.impact && (
                      <p className="text-[12px] text-emerald-400 font-medium mt-1">💡 {rec.impact}</p>
                    )}
                    {rec.action_items.length > 0 && (
                      <ul className="mt-2 space-y-1">
                        {rec.action_items.map((item, i) => (
                          <li key={i} className="text-[11px] text-[hsl(var(--muted-foreground))] flex items-start gap-1.5">
                            <span className="text-[hsl(var(--primary))] mt-0.5">→</span>
                            {item}
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Skeleton
   ═══════════════════════════════════════════════════════════ */

function WBAnalyticsSkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="grid grid-cols-1 sm:grid-cols-3 lg:grid-cols-6 gap-4">
        {[...Array(6)].map((_, idx) => (
          <Card key={idx}><CardContent className="p-5"><Skeleton className="h-20 w-full" /></CardContent></Card>
        ))}
      </div>
      <Card><CardContent className="p-5"><Skeleton className="h-[400px] w-full" /></CardContent></Card>
      <Card><CardContent className="p-5"><Skeleton className="h-[200px] w-full" /></CardContent></Card>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   AI Warehouse Insight Block (2-block: SKU problems + redistribution)
   ═══════════════════════════════════════════════════════════ */

const OPTION_ICONS: Record<string, { icon: React.ElementType; color: string }> = {
  discount:      { icon: CircleDollarSign, color: 'text-amber-400' },
  launch_ads:    { icon: Megaphone,        color: 'text-orange-400' },
  withdraw:      { icon: PackageX,         color: 'text-red-400' },
  do_nothing:    { icon: Ban,              color: 'text-gray-400' },
  reduce_supply: { icon: ArrowDownToLine,  color: 'text-violet-400' },
}

const RISK_BADGES: Record<string, { label: string; color: string }> = {
  low:    { label: 'Низкий риск', color: 'bg-emerald-500/15 text-emerald-400' },
  medium: { label: 'Средний риск', color: 'bg-amber-500/15 text-amber-400' },
  high:   { label: 'Высокий риск', color: 'bg-red-500/15 text-red-400' },
}

function WarehouseAIInsight({ shopId, period }: { shopId: number; period: number }) {
  const [data, setData] = useState<AIWarehouseAnalysis | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [expanded, setExpanded] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [expandedSkus, setExpandedSkus] = useState<Set<string>>(new Set())

  const toggleSku = (vc: string) => {
    setExpandedSkus(prev => {
      const next = new Set(prev)
      next.has(vc) ? next.delete(vc) : next.add(vc)
      return next
    })
  }

  const fetchAI = useCallback(async (force = false) => {
    if (force) setRefreshing(true)
    else setLoading(true)
    setError(null)
    try {
      const result = await getWBWarehouseAIAnalysis({ shop_id: shopId, period, force })
      setData(result)
      // Auto-expand first 2 SKUs
      if (result.sku_actions?.length) {
        setExpandedSkus(new Set(result.sku_actions.slice(0, 2).map(s => s.vendor_code)))
      }
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка ИИ-анализа')
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [shopId, period])

  useEffect(() => { fetchAI() }, [fetchAI])

  const severityConfig = {
    critical: { bg: 'from-red-500/10 to-red-500/5', border: 'border-red-500/30', icon: '🔴', label: 'Критично' },
    warning:  { bg: 'from-amber-500/10 to-amber-500/5', border: 'border-amber-500/30', icon: '🟡', label: 'Внимание' },
    ok:       { bg: 'from-emerald-500/10 to-emerald-500/5', border: 'border-emerald-500/30', icon: '🟢', label: 'Всё ОК' },
  }

  if (loading && !data) {
    return (
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.35 }}>
        <Card className="border border-[hsl(var(--border))] overflow-hidden">
          <CardContent className="p-6">
            <div className="flex items-center gap-3 mb-4">
              <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-violet-600 to-blue-500 flex items-center justify-center">
                <Brain className="h-5 w-5 text-white animate-pulse" />
              </div>
              <div>
                <Skeleton className="h-5 w-48 mb-1" />
                <Skeleton className="h-3 w-64" />
              </div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
              <Skeleton className="h-24 w-full rounded-xl" />
              <Skeleton className="h-24 w-full rounded-xl" />
              <Skeleton className="h-24 w-full rounded-xl" />
            </div>
            <Skeleton className="h-32 w-full rounded-xl mb-3" />
            <Skeleton className="h-32 w-full rounded-xl" />
          </CardContent>
        </Card>
      </motion.div>
    )
  }

  if (error) {
    return (
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.35 }}>
        <Card className="border border-red-500/20">
          <CardContent className="p-5 flex items-center gap-3">
            <AlertTriangle className="h-5 w-5 text-red-400 shrink-0" />
            <span className="text-sm text-[hsl(var(--muted-foreground))]">{error}</span>
            <button
              onClick={() => fetchAI(true)}
              className="ml-auto text-[14px] font-medium text-[hsl(var(--primary))] hover:underline"
            >
              Повторить
            </button>
          </CardContent>
        </Card>
      </motion.div>
    )
  }

  if (!data) return null

  const sev = severityConfig[data.severity] || severityConfig.warning
  const analyzedAtStr = data.analyzed_at
    ? `Анализ от ${new Date(data.analyzed_at * 1000).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })}`
    : null

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.35, duration: 0.5 }}>
      <Card className={`border ${sev.border} overflow-hidden`}>
        <CardContent className="p-0">
          {/* Header */}
          <div
            className={`px-6 py-4 bg-gradient-to-r ${sev.bg} flex items-center justify-between cursor-pointer`}
            onClick={() => setExpanded(!expanded)}
          >
            <div className="flex items-center gap-3">
              <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-violet-600 to-blue-500 shadow-lg shadow-violet-500/25 flex items-center justify-center">
                <Sparkles className="h-5 w-5 text-white" />
              </div>
              <div>
                <div className="flex items-center gap-2">
                  <h3 className="text-base font-bold text-[hsl(var(--foreground))]">ИИ-Диагностика складов</h3>
                  <span className="text-sm">{sev.icon}</span>
                  <span className="text-xs font-semibold text-[hsl(var(--muted-foreground))]">{sev.label}</span>
                </div>
                <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-0.5 leading-snug">{data.diagnosis}</p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              {analyzedAtStr && (
                <span className="text-[11px] text-[hsl(var(--muted-foreground))]">{analyzedAtStr}</span>
              )}
              <button
                onClick={(e) => { e.stopPropagation(); fetchAI(true) }}
                disabled={refreshing}
                className="p-1.5 rounded-lg hover:bg-[hsl(var(--muted)/0.3)] transition-colors"
                title="Обновить анализ"
              >
                <RefreshCw className={`h-4 w-4 text-[hsl(var(--muted-foreground))] ${refreshing ? 'animate-spin' : ''}`} />
              </button>
              <motion.div animate={{ rotate: expanded ? 180 : 0 }} transition={{ duration: 0.2 }}>
                <ChevronDown className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
              </motion.div>
            </div>
          </div>

          <AnimatePresence>
            {expanded && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                transition={{ duration: 0.3 }}
                className="overflow-hidden"
              >
                <div className="px-6 py-5 space-y-6">
                  {/* 3 metric cards */}
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-4">
                      <div className="flex items-center gap-2 mb-2">
                        <Truck className="h-4 w-4 text-cyan-400" />
                        <span className="text-xs font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">
                          Кросс-логистика
                        </span>
                      </div>
                      <p className="text-xl font-bold text-red-400">
                        {data.key_metrics.cross_logistics_loss > 0
                          ? `−${fmtM(data.key_metrics.cross_logistics_loss)}`
                          : '—'}
                      </p>
                      <p className="text-xs text-[hsl(var(--muted-foreground))] mt-1">переплата / мес</p>
                    </div>

                    <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-4">
                      <div className="flex items-center gap-2 mb-2">
                        <Boxes className="h-4 w-4 text-purple-400" />
                        <span className="text-xs font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">
                          Избыточное хранение
                        </span>
                      </div>
                      <p className="text-xl font-bold text-amber-400">
                        {data.key_metrics.storage_excess > 0
                          ? `−${fmtM(data.key_metrics.storage_excess)}`
                          : '—'}
                      </p>
                      <p className="text-xs text-[hsl(var(--muted-foreground))] mt-1">можно сэкономить / мес</p>
                    </div>

                    <div className="rounded-xl bg-emerald-500/5 border border-emerald-500/20 p-4">
                      <div className="flex items-center gap-2 mb-2">
                        <Zap className="h-4 w-4 text-emerald-400" />
                        <span className="text-xs font-semibold uppercase tracking-wider text-emerald-500/60">
                          Потенциал экономии
                        </span>
                      </div>
                      <p className="text-xl font-bold text-emerald-400">
                        {data.total_potential_savings > 0
                          ? `+${fmtM(data.total_potential_savings)}`
                          : '—'}
                      </p>
                      <p className="text-xs text-[hsl(var(--muted-foreground))] mt-1">при выполнении / мес</p>
                    </div>
                  </div>

                  {/* ═══ BLOCK 1: SKU Problems ═══ */}
                  {data.sku_actions?.length > 0 && (
                    <div className="space-y-3">
                      <h4 className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)] flex items-center gap-2">
                        <ShieldAlert className="h-4 w-4" />
                        Проблемные товары — что делать ({data.sku_actions.length})
                      </h4>
                      {data.sku_actions.map((sku) => {
                        const isOpen = expandedSkus.has(sku.vendor_code)
                        const profitColor = (sku.net_profit_month ?? 0) < 0 ? 'text-red-400' : 'text-emerald-400'
                        return (
                          <div
                            key={sku.vendor_code}
                            className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)] overflow-hidden"
                          >
                            {/* SKU header — clickable */}
                            <div
                              className="flex items-center justify-between px-5 py-3.5 cursor-pointer hover:bg-[hsl(var(--muted)/0.08)] transition-colors"
                              onClick={() => toggleSku(sku.vendor_code)}
                            >
                              <div className="flex items-center gap-3 min-w-0">
                                <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-amber-500/10">
                                  <ShieldAlert className="h-4.5 w-4.5 text-amber-400" />
                                </div>
                                <div className="min-w-0">
                                  <div className="flex items-center gap-2">
                                    <span className="text-[14px] font-bold text-[hsl(var(--foreground))] truncate">
                                      {sku.vendor_code}
                                    </span>
                                    <span className="text-[13px] text-[hsl(var(--muted-foreground))] truncate">
                                      {sku.name}
                                    </span>
                                  </div>
                                  <p className="text-[13px] text-[hsl(var(--muted-foreground)/0.8)] mt-0.5 leading-snug">
                                    {sku.problem}
                                  </p>
                                </div>
                              </div>
                              <div className="flex items-center gap-4 shrink-0 ml-4">
                                <div className="text-right">
                                  <div className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)]">
                                    хранение
                                  </div>
                                  <div className="text-[14px] font-bold text-red-400">
                                    −{fmtM(sku.storage_cost_month)}/мес
                                  </div>
                                </div>
                                {sku.net_profit_month !== undefined && sku.net_profit_month !== null && (
                                  <div className="text-right">
                                    <div className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)]">
                                      чист. прибыль
                                    </div>
                                    <div className={`text-[14px] font-bold ${profitColor}`}>
                                      {sku.net_profit_month >= 0 ? '+' : ''}{fmtM(sku.net_profit_month)}/мес
                                    </div>
                                  </div>
                                )}
                                <motion.div animate={{ rotate: isOpen ? 180 : 0 }} transition={{ duration: 0.2 }}>
                                  <ChevronDown className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.4)]" />
                                </motion.div>
                              </div>
                            </div>

                            {/* Options grid */}
                            <AnimatePresence>
                              {isOpen && (
                                <motion.div
                                  initial={{ height: 0, opacity: 0 }}
                                  animate={{ height: 'auto', opacity: 1 }}
                                  exit={{ height: 0, opacity: 0 }}
                                  transition={{ duration: 0.2 }}
                                  className="overflow-hidden"
                                >
                                  <div className="px-5 pb-4 pt-1">
                                    <div className="flex items-center gap-5 mb-3 text-[12px] text-[hsl(var(--muted-foreground)/0.6)]">
                                      <span>Остаток: <b>{sku.stock}</b> шт</span>
                                      <span>Оборач.: <b>{sku.current_turnover_days}</b>д</span>
                                      <span>Хранение: <b>~{fmtM(sku.storage_cost_month)}</b>/мес</span>
                                    </div>
                                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                                      {sku.options.map((opt, oi) => {
                                        const iconCfg = OPTION_ICONS[opt.action] || OPTION_ICONS.do_nothing
                                        const OptIcon = iconCfg.icon
                                        const riskBadge = RISK_BADGES[opt.risk] || RISK_BADGES.medium
                                        const isRecommended = oi === sku.recommended_option

                                        return (
                                          <div
                                            key={oi}
                                            className={`rounded-xl border p-4 transition-colors ${
                                              isRecommended
                                                ? 'border-emerald-500/40 bg-emerald-500/5'
                                                : 'border-[hsl(var(--border)/0.2)] bg-[hsl(var(--muted)/0.03)] hover:bg-[hsl(var(--muted)/0.06)]'
                                            }`}
                                          >
                                            <div className="flex items-center justify-between mb-2.5">
                                              <div className="flex items-center gap-2">
                                                <OptIcon className={`h-4 w-4 ${iconCfg.color}`} />
                                                <span className="text-[14px] font-bold text-[hsl(var(--foreground))]">
                                                  {opt.label}
                                                </span>
                                              </div>
                                              {isRecommended && (
                                                <span className="text-[10px] font-bold bg-emerald-500/20 text-emerald-400 px-2 py-0.5 rounded">
                                                  ✓ Рекомендуем
                                                </span>
                                              )}
                                            </div>
                                            <p className="text-[13px] text-[hsl(var(--foreground)/0.8)] leading-relaxed mb-3">
                                              {opt.detail}
                                            </p>
                                            <div className="flex items-center justify-between">
                                              <span className={`text-[11px] font-medium rounded-full px-2.5 py-0.5 ${riskBadge.color}`}>
                                                {riskBadge.label}
                                              </span>
                                              {opt.expected_savings > 0 ? (
                                                <span className="text-[14px] font-bold text-emerald-400">
                                                  +{fmtM(opt.expected_savings)}/мес
                                                </span>
                                              ) : opt.expected_savings < 0 ? (
                                                <span className="text-[14px] font-bold text-red-400">
                                                  {fmtM(opt.expected_savings)}/мес
                                                </span>
                                              ) : (
                                                <span className="text-[13px] text-[hsl(var(--muted-foreground)/0.5)]">
                                                  без изменений
                                                </span>
                                              )}
                                            </div>
                                          </div>
                                        )
                                      })}
                                    </div>
                                  </div>
                                </motion.div>
                              )}
                            </AnimatePresence>
                          </div>
                        )
                      })}
                    </div>
                  )}

                  {/* ═══ BLOCK 2: Redistribution Transfers ═══ */}
                  {data.transfers?.length > 0 && (
                    <div className="space-y-3">
                      <h4 className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)] flex items-center gap-2">
                        <ArrowRightLeft className="h-4 w-4" />
                        Перераспределение по складам ({data.transfers.length})
                      </h4>
                      {data.transfers.map((tr, ti) => (
                        <div key={ti} className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)] p-5">
                          <div className="flex items-center justify-between mb-3">
                            <div className="flex items-center gap-2">
                              <Package className="h-4 w-4 text-blue-400" />
                              <span className="text-[14px] font-bold text-[hsl(var(--foreground))]">
                                {tr.vendor_code}
                              </span>
                              <span className="text-[13px] text-[hsl(var(--muted-foreground))]">
                                {tr.name}
                              </span>
                            </div>
                          </div>
                          
                          {/* From → To table */}
                          <div className="rounded-lg border border-[hsl(var(--border)/0.2)] overflow-hidden mb-3">
                            <div className="bg-[hsl(var(--muted)/0.06)] px-4 py-2 flex items-center gap-3 border-b border-[hsl(var(--border)/0.15)]">
                              <Warehouse className="h-4 w-4 text-amber-400" />
                              <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">
                                {tr.from_warehouse}
                              </span>
                              <span className="text-[13px] text-[hsl(var(--muted-foreground))]">
                                сток {tr.from_stock} шт → оставить {tr.keep_at_source} шт
                              </span>
                            </div>
                            {tr.destinations.map((dest, di) => (
                              <div key={di} className="px-4 py-3 flex items-start gap-3 border-b border-[hsl(var(--border)/0.1)] last:border-b-0">
                                <ArrowRight className="h-4 w-4 text-emerald-400 shrink-0 mt-0.5" />
                                <div className="flex-1">
                                  <div className="flex items-center gap-2">
                                    <span className="text-[14px] font-bold text-[hsl(var(--foreground))]">
                                      {dest.warehouse}
                                    </span>
                                    <span className="text-[13px] font-semibold text-emerald-400">
                                      +{dest.qty} шт
                                    </span>
                                  </div>
                                  <p className="text-[13px] text-[hsl(var(--foreground)/0.7)] mt-0.5 leading-snug">
                                    {dest.reason}
                                  </p>
                                </div>
                              </div>
                            ))}
                          </div>

                          <div className="flex items-center gap-2 px-1">
                            <Zap className="h-3.5 w-3.5 text-emerald-400" />
                            <span className="text-[13px] font-medium text-emerald-400">{tr.expected_effect}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* General tips from redistribution */}
                  {data.general_tips?.length > 0 && (
                    <div className="space-y-2">
                      <h4 className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)] flex items-center gap-2">
                        <Lightbulb className="h-4 w-4" />
                        Общие рекомендации
                      </h4>
                      {data.general_tips.map((tip, i) => (
                        <div key={i} className="rounded-xl border border-[hsl(var(--border)/0.2)] bg-[hsl(var(--muted)/0.04)] p-4">
                          <p className="text-[14px] text-[hsl(var(--foreground)/0.85)] leading-relaxed">{tip}</p>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Supply tip */}
                  {data.supply_tip && (
                    <div className="rounded-xl bg-blue-500/5 border border-blue-500/20 p-5">
                      <div className="flex items-start gap-3">
                        <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-blue-500/15">
                          <ArrowRight className="h-4.5 w-4.5 text-blue-400" />
                        </div>
                        <div className="flex-1">
                          <h5 className="text-[13px] font-bold text-blue-400 uppercase tracking-wider mb-1.5">
                            📦 При следующей поставке
                          </h5>
                          <p className="text-[14px] text-[hsl(var(--foreground))] leading-relaxed">
                            {data.supply_tip}
                          </p>
                          <a
                            href={`/warehouses/supply?shop_id=${shopId}`}
                            className="inline-flex items-center gap-1.5 mt-3 px-4 py-2 rounded-lg bg-blue-500/10 text-blue-400 text-[13px] font-semibold hover:bg-blue-500/20 transition-colors"
                          >
                            <Package className="h-4 w-4" />
                            Перейти в раздел Поставки
                            <ArrowRight className="h-3.5 w-3.5" />
                          </a>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Context line */}
                  <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-[hsl(var(--muted-foreground)/0.4)]">
                    <span>Период: {data.period_days}д</span>
                    <span>Складов: {data.context.warehouses_count}</span>
                    <span>Заказов: {fmt(data.context.total_orders)}</span>
                    <span>В рекламе: {data.context.skus_in_ads} SKU</span>
                    <span>Без рекламы: {data.context.skus_no_ads} SKU</span>
                    <span>Кросс: {data.context.cross_pct}%</span>
                  </div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </CardContent>
      </Card>
    </motion.div>
  )
}


/* ═══════════════════════════════════════════════════════════
   MAIN EXPORT
   ═══════════════════════════════════════════════════════════ */

const PERIOD_OPTIONS = [
  { value: 14, label: '14 дней' },
  { value: 30, label: '30 дней' },
  { value: 60, label: '60 дней' },
  { value: 90, label: '90 дней' },
]

export default function WBWarehouseAnalyticsContent({ shopId }: { shopId: number }) {
  const [data, setData] = useState<WBWarehouseAnalyticsResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState(30)

  const fetchData = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const result = await getWBWarehouseAnalytics({ shop_id: shopId, period })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [shopId, period])

  useEffect(() => { fetchData() }, [fetchData])

  const periodSelCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[13px] font-medium transition-all cursor-pointer ${
      active
        ? 'bg-[hsl(var(--primary))] text-white shadow-md'
        : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
    }`

  return (
    <div className="space-y-6">
      {/* Controls */}
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between flex-wrap gap-3">
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
              <button
                onClick={fetchData}
                disabled={loading}
                className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all disabled:opacity-50"
              >
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
                Обновить
              </button>
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {loading && !data ? (
        <WBAnalyticsSkeleton />
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
          {/* KPI */}
          {(() => {
            const kpi = data.kpi
            const orders = kpi.total_orders || 1
            const turnDays = kpi.avg_turnover_days
            const logPerOrder = kpi.total_logistics / orders
            const storVsLog = kpi.total_logistics > 0 ? kpi.total_storage / kpi.total_logistics * 100 : 0
            const crossPct = kpi.cross_pct

            const turnStatus: 'good' | 'warn' | 'bad' = !turnDays ? 'warn' : turnDays <= 45 ? 'good' : turnDays <= 120 ? 'warn' : 'bad'
            const turnText = !turnDays ? 'Нет данных' : turnDays <= 45 ? 'Быстрая оборачиваемость' : turnDays <= 120 ? 'Средняя, можно улучшить' : 'Затоваривание!'

            const logStatus: 'good' | 'warn' | 'bad' = logPerOrder <= 150 ? 'good' : logPerOrder <= 300 ? 'warn' : 'bad'
            const logText = `${Math.round(logPerOrder)} ₽/заказ`

            const storStatus: 'good' | 'warn' | 'bad' = storVsLog <= 10 ? 'good' : storVsLog <= 25 ? 'warn' : 'bad'
            const storText = kpi.total_penalties > 0
              ? `Штрафы: ${fmtM(kpi.total_penalties)}`
              : storVsLog <= 10 ? 'Под контролем' : storVsLog <= 25 ? 'Растёт, проверить' : 'Дорого!'

            const crossStatus: 'good' | 'warn' | 'bad' = crossPct <= 25 ? 'good' : crossPct <= 50 ? 'warn' : 'bad'
            const crossText = crossPct <= 25 ? 'Распределение ОК' : crossPct <= 50 ? 'Много кросс-логистики' : 'Критично! Перераспределить'

            return (
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
                <KpiCard
                  title="Складов"
                  value={String(kpi.total_warehouses)}
                  subtitle={`${kpi.total_sku} SKU`}
                  icon={Warehouse}
                  accent="from-blue-600 to-blue-500"
                  delay={0.05}
                />
                <KpiCard
                  title="Общий остаток"
                  value={`${fmt(kpi.total_stock)} ед.`}
                  subtitle={`${fmt(kpi.total_orders)} заказов`}
                  icon={Package}
                  accent="from-emerald-600 to-emerald-500"
                  delay={0.1}
                />
                <KpiCard
                  title="Оборачиваемость"
                  value={turnDays != null ? `${Math.round(turnDays)} дн` : '—'}
                  icon={BarChart3}
                  accent="from-violet-600 to-violet-500"
                  delay={0.15}
                  status={turnStatus}
                  statusText={turnText}
                />
                <KpiCard
                  title="Логистика"
                  value={fmtM(kpi.total_logistics)}
                  subtitle={`за ${kpi.period_days}д`}
                  icon={Truck}
                  accent="from-cyan-600 to-cyan-500"
                  delay={0.2}
                  status={logStatus}
                  statusText={logText}
                />
                <KpiCard
                  title={kpi.has_actual_storage ? 'Хранение (факт)' : 'Хранение'}
                  value={fmtM(kpi.total_storage_actual ?? kpi.total_storage)}
                  subtitle={kpi.has_actual_storage && kpi.total_storage_actual != null
                    ? `По отчётам WB • за ${kpi.period_days}д`
                    : `Из удержаний • за ${kpi.period_days}д`}
                  icon={Boxes}
                  accent="from-purple-600 to-purple-500"
                  delay={0.25}
                  status={storStatus}
                  statusText={storText}
                />
                {kpi.forecast_30d != null && (
                  <KpiCard
                    title="Прогноз 30д"
                    value={fmtM(kpi.forecast_30d)}
                    subtitle="с учётом продаж"
                    icon={TrendingUp}
                    accent="from-amber-600 to-amber-500"
                    delay={0.28}
                    status={kpi.forecast_30d > kpi.total_storage * 3 ? 'bad' : kpi.forecast_30d > kpi.total_storage * 1.5 ? 'warn' : 'good'}
                    statusText={kpi.forecast_30d > kpi.total_storage * 3 ? 'Растёт! Сокращать остатки' : kpi.forecast_30d > kpi.total_storage * 1.5 ? 'Проверить продажи' : 'Остатки сгорают'}
                  />
                )}
                <KpiCard
                  title="Кросс-отправки"
                  value={`${crossPct}%`}
                  subtitle="заказов в чужие округа"
                  icon={ArrowRightLeft}
                  accent={crossPct > 50 ? 'from-red-600 to-red-500' : crossPct > 25 ? 'from-amber-600 to-amber-500' : 'from-emerald-600 to-emerald-500'}
                  delay={0.3}
                  status={crossStatus}
                  statusText={crossText}
                />
              </div>
            )
          })()}

          {/* AI Insights */}
          <WarehouseAIInsight shopId={shopId} period={period} />

          {/* Warehouses Table */}
          <WarehousesTable warehouses={data.warehouses} />

          {/* Cross-map */}
          <CrossMapTable crossMap={data.cross_map} okrugList={data.okrug_list} />

          {/* Costs */}
          <CostsSummary costs={data.costs} />

          {/* Storage SKUs */}
          <StorageSkusTable skus={data.storage_skus} periodDays={period} />

          {/* Recommendations */}
          <RecommendationsPanel recommendations={data.recommendations} />
        </>
      ) : null}
    </div>
  )
}
