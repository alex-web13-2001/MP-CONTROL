/**
 * Warehouses Overview — marketplace-agnostic page.
 * WB: KPI + AI insights + warehouses table
 * Ozon: redirects to /warehouses/analytics (tab-based, refactored later)
 */
import React, { useState, useEffect, useCallback } from 'react'
import { Navigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import {
  Warehouse,
  BarChart3,
  ArrowRightLeft,
  RefreshCw,
  Package,
  Truck,
  Boxes,
  AlertTriangle,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBWarehouseAnalytics,
  type WBWarehouseAnalyticsResponse,
  type WBAnalyticsWarehouse,
} from '@/api/warehouses'

/* ── Helpers ── */
function fmt(v: number): string { return Math.round(v).toLocaleString('ru-RU') }
function fmtM(v: number): string { return Math.round(v).toLocaleString('ru-RU') + ' ₽' }
function fmtD(v: number | null): string { return v != null ? `${Math.round(v)} дн` : '—' }

/* ── Period Options ── */
const PERIOD_OPTIONS = [
  { label: '7 дн', value: 7 },
  { label: '14 дн', value: 14 },
  { label: '30 дн', value: 30 },
  { label: '60 дн', value: 60 },
  { label: '90 дн', value: 90 },
]

/* ═══ KPI Card ═══ */
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

/* ═══ Status Badge ═══ */
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

/* ═══ Type Badge ═══ */
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

/* ═══ Cross% Indicator ═══ */
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

/* ═══ Warehouses Table (overview-only, no expanded detail) ═══ */
function WarehousesTable({ warehouses }: { warehouses: WBAnalyticsWarehouse[] }) {
  const thBase = 'px-4 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider whitespace-nowrap'
  const tdCls = 'px-4 py-3 text-center tabular-nums text-[13px] whitespace-nowrap'

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Склады</h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">
            {warehouses.length} складов
          </span>
        </div>

        <div className="overflow-auto max-h-[700px]">
          <table className="w-full border-collapse" style={{ minWidth: 900 }}>
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-4 py-3 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider min-w-[200px]">Склад</th>
                <th className={`${thBase} min-w-[80px]`}>Статус</th>
                <th className={`${thBase} min-w-[75px]`}>Остаток</th>
                <th className={`${thBase} min-w-[75px]`}>Заказов</th>
                <th className={`${thBase} min-w-[65px]`}>В день</th>
                <th className={`${thBase} min-w-[75px]`}>Оборач.</th>
                <th className={`${thBase} min-w-[90px]`}>Кросс%</th>
                <th className={`${thBase} min-w-[100px]`}>Хранение ₽</th>
                <th className={`${thBase} min-w-[50px]`}>SKU</th>
              </tr>
            </thead>
            <tbody>
              {warehouses.map((wh, idx) => {
                const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.04)]'
                return (
                  <tr
                    key={wh.warehouse_name}
                    className={`border-b border-[hsl(var(--border)/0.15)] ${rowBg} hover:bg-[hsl(var(--muted)/0.1)] transition-colors`}
                  >
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
                    <td className={`${tdCls} ${wh.storage_cost_month > 5000 ? 'text-purple-400 font-semibold' : wh.storage_cost_month > 0 ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))]'}`}>
                      {wh.storage_cost_month > 0 ? fmtM(wh.storage_cost_month) : '—'}
                    </td>
                    <td className={`${tdCls} text-[hsl(var(--muted-foreground))]`}>{wh.sku_count}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══ Skeleton ═══ */
function OverviewSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} className="h-[130px] rounded-2xl" />
        ))}
      </div>
      <Skeleton className="h-[400px] rounded-2xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function WarehousesOverviewPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isWB = currentShop?.marketplace === 'wildberries'
  const isOzon = currentShop?.marketplace === 'ozon'

  const [data, setData] = useState<WBWarehouseAnalyticsResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState(30)

  const fetchData = useCallback(async () => {
    if (!currentShop || !isWB) return
    setLoading(true)
    setError(null)
    try {
      const result = await getWBWarehouseAnalytics({ shop_id: currentShop.id, period })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [currentShop, isWB, period])

  useEffect(() => { if (isWB) fetchData() }, [fetchData, isWB])

  // Ozon → redirect to existing analytics page
  if (isOzon) return <Navigate to="/warehouses/analytics" replace />

  const periodSelCls = (active: boolean) =>
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
          <h1 className="text-3xl font-bold tracking-tight">Обзор складов</h1>
          <p className="text-[hsl(var(--muted-foreground))] mt-1">
            KPI, остатки, оборачиваемость и кросс-логистика по складам WB
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

      {/* Period */}
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
        <Card>
          <CardContent className="p-4">
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
          </CardContent>
        </Card>
      </motion.div>

      {loading && !data ? (
        <OverviewSkeleton />
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
                <KpiCard title="Складов" value={String(kpi.total_warehouses)} subtitle={`${kpi.total_sku} SKU`} icon={Warehouse} accent="from-blue-600 to-blue-500" delay={0.05} />
                <KpiCard title="Общий остаток" value={`${fmt(kpi.total_stock)} ед.`} subtitle={`${fmt(kpi.total_orders)} заказов`} icon={Package} accent="from-emerald-600 to-emerald-500" delay={0.1} />
                <KpiCard title="Оборачиваемость" value={turnDays != null ? `${Math.round(turnDays)} дн` : '—'} icon={BarChart3} accent="from-violet-600 to-violet-500" delay={0.15} status={turnStatus} statusText={turnText} />
                <KpiCard title="Логистика" value={fmtM(kpi.total_logistics)} subtitle={`за ${kpi.period_days}д`} icon={Truck} accent="from-cyan-600 to-cyan-500" delay={0.2} status={logStatus} statusText={logText} />
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
                <KpiCard title="Кросс-отправки" value={`${crossPct}%`} subtitle="заказов в чужие округа" icon={ArrowRightLeft}
                  accent={crossPct > 50 ? 'from-red-600 to-red-500' : crossPct > 25 ? 'from-amber-600 to-amber-500' : 'from-emerald-600 to-emerald-500'}
                  delay={0.3} status={crossStatus} statusText={crossText}
                />
              </div>
            )
          })()}

          {/* Warehouses Table */}
          <WarehousesTable warehouses={data.warehouses} />
        </>
      ) : null}
    </div>
  )
}
