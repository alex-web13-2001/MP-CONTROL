/**
 * Warehouses Overview — "Problem Dashboard"
 * Shows KPIs with trends, cost breakdown, problem alerts, AI diagnostics, warehouses table.
 * WB only; Ozon redirects to /warehouses/analytics.
 */
import React, { useState, useEffect, useCallback } from 'react'
import { Navigate, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Warehouse,
  BarChart3,
  ArrowRightLeft,
  RefreshCw,
  Package,
  Truck,
  Boxes,
  AlertTriangle,
  TrendingUp,
  TrendingDown,
  Minus,
  ArrowRight,
  ChevronDown,
  ChevronUp,
  Sparkles,
  ShieldAlert,
  MapPin,
  PackagePlus,
  Loader2,
  Gavel,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBWarehouseAnalytics,
  getWBWarehouseAIAnalysis,
  type WBWarehouseAnalyticsResponse,
  type WBAnalyticsWarehouse,
  type AIWarehouseAnalysis,
} from '@/api/warehouses'
import { CostsSummary } from './WBWarehouseAnalyticsContent'

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

/* ── Trend helper ── */
function calcTrend(current: number, prev: number): { pct: number; direction: 'up' | 'down' | 'flat' } {
  if (prev === 0) return { pct: 0, direction: 'flat' }
  const pct = ((current - prev) / Math.abs(prev)) * 100
  if (Math.abs(pct) < 1) return { pct: 0, direction: 'flat' }
  return { pct: Math.round(pct), direction: pct > 0 ? 'up' : 'down' }
}

/* ═══ Trend KPI Card ═══ */
function TrendKpiCard({
  title, value, subtitle, icon: Icon, accent, delay,
  trend, trendInverted = false, statusColor,
}: {
  title: string; value: string; subtitle?: string
  icon: React.ElementType; accent: string; delay: number
  trend?: { pct: number; direction: 'up' | 'down' | 'flat' }
  trendInverted?: boolean // true = up is bad (costs going up)
  statusColor?: string
}) {
  const TrendIcon = trend?.direction === 'up' ? TrendingUp : trend?.direction === 'down' ? TrendingDown : Minus

  // For costs: up = bad (red), down = good (green)
  // For orders: up = good (green), down = bad (red)
  const getColor = () => {
    if (!trend || trend.direction === 'flat') return 'text-[hsl(var(--muted-foreground))]'
    if (trendInverted) {
      return trend.direction === 'up' ? 'text-red-400' : 'text-emerald-400'
    }
    return trend.direction === 'up' ? 'text-emerald-400' : 'text-red-400'
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay, duration: 0.4 }}>
      <Card className={`relative overflow-hidden h-full ${statusColor ? `border-b-[3px] ${statusColor}` : ''}`}>
        <CardContent className="p-5 flex flex-col justify-between h-full">
          <div className="flex items-start justify-between">
            <div className="space-y-1 min-w-0 flex-1">
              <p className="text-[13px] font-medium text-[hsl(var(--muted-foreground))]">{title}</p>
              <p className="text-2xl font-bold tracking-tight">{value}</p>
            </div>
            <div className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br ${accent} shadow-lg`}>
              <Icon className="h-5 w-5 text-white" />
            </div>
          </div>
          <div className="mt-3 flex items-center justify-between min-h-[24px]">
            {subtitle && (
              <span className="text-[12px] text-[hsl(var(--muted-foreground))]">{subtitle}</span>
            )}
            {trend && trend.direction !== 'flat' && (
              <div className={`flex items-center gap-1 ml-auto ${getColor()}`}>
                <TrendIcon className="h-3.5 w-3.5" />
                <span className="text-[12px] font-bold tabular-nums">
                  {trend.direction === 'up' ? '+' : ''}{trend.pct}%
                </span>
              </div>
            )}
            {trend && trend.direction === 'flat' && (
              <div className="flex items-center gap-1 ml-auto text-[hsl(var(--muted-foreground))]">
                <Minus className="h-3.5 w-3.5 opacity-40" />
                <span className="text-[11px] opacity-50">без изменений</span>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}


/* ═══ Problem Card ═══ */
function ProblemCard({
  severity, icon: Icon, title, details, link, linkLabel, delay,
}: {
  severity: 'critical' | 'warning' | 'info' | 'ok'
  icon: React.ElementType
  title: string
  details: string | React.ReactNode
  link?: string
  linkLabel?: string
  delay: number
}) {
  const navigate = useNavigate()
  const styles = {
    critical: { border: 'border-red-500/30', bg: 'bg-red-500/5', icon: 'text-red-400', dot: 'bg-red-500' },
    warning: { border: 'border-amber-500/30', bg: 'bg-amber-500/5', icon: 'text-amber-400', dot: 'bg-amber-500' },
    info: { border: 'border-blue-500/30', bg: 'bg-blue-500/5', icon: 'text-blue-400', dot: 'bg-blue-500' },
    ok: { border: 'border-emerald-500/30', bg: 'bg-emerald-500/5', icon: 'text-emerald-400', dot: 'bg-emerald-500' },
  }
  const s = styles[severity]

  return (
    <motion.div
      initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }} transition={{ delay, duration: 0.3 }}
    >
      <div className={`rounded-xl border ${s.border} ${s.bg} p-4 flex items-start gap-3`}>
        <div className={`mt-0.5 p-1.5 rounded-lg ${s.bg} ${s.icon}`}>
          <Icon className="h-4 w-4" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className={`h-2 w-2 rounded-full ${s.dot}`} />
            <span className="text-[13px] font-bold text-[hsl(var(--foreground))]">{title}</span>
          </div>
          <div className="text-[12px] text-[hsl(var(--muted-foreground))] leading-relaxed">
            {details}
          </div>
        </div>
        {link && (
          <button
            onClick={() => navigate(link)}
            className="shrink-0 flex items-center gap-1 text-[12px] font-medium text-[hsl(var(--primary))] hover:underline mt-1"
          >
            {linkLabel || 'Подробнее'} <ArrowRight className="h-3.5 w-3.5" />
          </button>
        )}
      </div>
    </motion.div>
  )
}


/* ═══ AI Diagnostics Block ═══ */
function AIDiagnosticsBlock({ shopId, period }: { shopId: number; period: number }) {
  const [data, setData] = useState<AIWarehouseAnalysis | null>(null)
  const [loading, setLoading] = useState(false)
  const [expanded, setExpanded] = useState(false)
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    getWBWarehouseAIAnalysis({ shop_id: shopId, period })
      .then(r => { if (!cancelled) setData(r) })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [shopId, period])

  if (loading) {
    return (
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.5 }}>
        <Card>
          <CardContent className="p-6 flex items-center gap-3">
            <Loader2 className="h-5 w-5 animate-spin text-violet-400" />
            <span className="text-sm text-[hsl(var(--muted-foreground))]">ИИ-аналитик загружает данные...</span>
          </CardContent>
        </Card>
      </motion.div>
    )
  }

  if (!data) return null

  const severityStyles = {
    critical: { border: 'border-l-red-500', icon: 'text-red-400', label: 'Критическая ситуация' },
    warning: { border: 'border-l-amber-500', icon: 'text-amber-400', label: 'Требует внимания' },
    ok: { border: 'border-l-emerald-500', icon: 'text-emerald-400', label: 'Всё под контролем' },
  }
  const sv = severityStyles[data.severity] || severityStyles.ok

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.5, duration: 0.4 }}>
      <Card className={`border-l-4 ${sv.border} overflow-hidden`}>
        <CardContent className="p-0">
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-4 border-b border-[hsl(var(--border)/0.5)]">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-xl bg-gradient-to-br from-violet-600 to-purple-500 shadow-lg">
                <Sparkles className="h-5 w-5 text-white" />
              </div>
              <div>
                <h3 className="text-[15px] font-bold text-[hsl(var(--foreground))]">ИИ-аналитик складов</h3>
                <p className="text-[11px] text-[hsl(var(--muted-foreground))]">
                  {sv.label}
                  {data.cached && data.cached_at && (
                    <> • Кеш от {new Date(data.cached_at * 1000).toLocaleDateString('ru-RU')}</>
                  )}
                </p>
              </div>
            </div>
            <button
              onClick={() => setExpanded(!expanded)}
              className="flex items-center gap-1 text-[12px] font-medium text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
            >
              {expanded ? 'Свернуть' : 'Развернуть'}
              {expanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
            </button>
          </div>

          {/* Diagnosis */}
          <div className="px-6 py-4">
            <p className="text-[13px] text-[hsl(var(--foreground))] leading-relaxed whitespace-pre-line">
              {data.diagnosis}
            </p>
          </div>

          {/* Expanded: tips + actions */}
          <AnimatePresence>
            {expanded && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                transition={{ duration: 0.25 }}
                className="overflow-hidden"
              >
                {/* General tips */}
                {data.general_tips.length > 0 && (
                  <div className="px-6 pb-4 space-y-2">
                    <h4 className="text-[12px] font-bold text-[hsl(var(--muted-foreground))] uppercase tracking-wider">Рекомендации</h4>
                    {data.general_tips.map((tip, i) => (
                      <div key={i} className="text-[12px] text-[hsl(var(--foreground))] pl-4 border-l-2 border-violet-500/30 py-1">
                        {tip}
                      </div>
                    ))}
                  </div>
                )}

                {/* Supply tip */}
                {data.supply_tip && (
                  <div className="px-6 pb-4">
                    <div className="p-3 rounded-lg bg-violet-500/5 border border-violet-500/20">
                      <p className="text-[12px] text-violet-300 font-medium">📦 {data.supply_tip}</p>
                    </div>
                  </div>
                )}

                {/* Top 3 SKU actions */}
                {data.sku_actions.length > 0 && (
                  <div className="px-6 pb-4">
                    <h4 className="text-[12px] font-bold text-[hsl(var(--muted-foreground))] uppercase tracking-wider mb-2">
                      Топ проблемных SKU
                    </h4>
                    <div className="space-y-1.5">
                      {data.sku_actions.slice(0, 3).map((sku, i) => (
                        <div key={i} className="flex items-center justify-between text-[12px] px-3 py-2 rounded-lg bg-[hsl(var(--muted)/0.06)]">
                          <div className="min-w-0">
                            <span className="font-bold text-[hsl(var(--foreground))]">{sku.vendor_code}</span>
                            <span className="text-[hsl(var(--muted-foreground))] ml-2 truncate">{sku.problem}</span>
                          </div>
                          <span className="text-red-400 font-bold shrink-0 ml-2">{fmtM(sku.storage_cost_month)}/мес</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* CTA */}
                <div className="px-6 pb-4">
                  <button
                    onClick={() => navigate('/warehouses/storage')}
                    className="flex items-center gap-2 text-[13px] font-medium text-violet-400 hover:text-violet-300 transition-colors"
                  >
                    Полный ИИ-анализ в разделе «Хранение» <ArrowRight className="h-4 w-4" />
                  </button>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
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

/* ═══ Warehouses Table (overview, no expand) ═══ */
function WarehousesTable({ warehouses }: { warehouses: WBAnalyticsWarehouse[] }) {
  const thBase = 'px-4 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider whitespace-nowrap'
  const tdCls = 'px-4 py-3 text-center tabular-nums text-[13px] whitespace-nowrap'

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.6, duration: 0.4 }}>
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
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-[130px] rounded-2xl" />
        ))}
      </div>
      <Skeleton className="h-[200px] rounded-2xl" />
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

  // Ozon → redirect
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
            Диагностика проблем • Расходы • Тренды
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
              <span className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-50 ml-2">
                сравнение с аналогичным предыдущим периодом
              </span>
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
          {/* ─── KPI Cards with Trends ─── */}
          {(() => {
            const kpi = data.kpi
            const prev = kpi.prev
            const orders = kpi.total_orders || 1
            const logPerOrder = kpi.total_logistics / orders
            const crossPct = kpi.cross_pct
            const totalExpenses = kpi.total_logistics + (kpi.total_storage_actual ?? kpi.total_storage) + kpi.total_penalties
            const prevTotalExpenses = prev
              ? prev.total_logistics + prev.total_storage + prev.total_penalties
              : 0

            // Trend calcs
            const trendExpenses = prev ? calcTrend(totalExpenses, prevTotalExpenses) : undefined
            const trendLogistics = prev ? calcTrend(kpi.total_logistics, prev.total_logistics) : undefined
            const trendStorage = prev ? calcTrend(kpi.total_storage_actual ?? kpi.total_storage, prev.total_storage) : undefined
            const trendOrders = prev ? calcTrend(kpi.total_orders, prev.total_orders) : undefined
            const trendPenalties = prev && kpi.total_penalties > 0 ? calcTrend(kpi.total_penalties, prev.total_penalties) : undefined

            // Cross-logistics estimated cost
            const crossCost = data.warehouses.reduce((s, w) => s + w.cross_orders * 33, 0)

            return (
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
                <TrendKpiCard
                  title="Общие расходы"
                  value={fmtM(totalExpenses)}
                  subtitle={`за ${kpi.period_days}д`}
                  icon={BarChart3}
                  accent="from-blue-600 to-blue-500"
                  delay={0.05}
                  trend={trendExpenses}
                  trendInverted
                />
                <TrendKpiCard
                  title="Логистика"
                  value={fmtM(kpi.total_logistics)}
                  subtitle={`${Math.round(logPerOrder)} ₽/заказ • кросс ~${fmtM(crossCost)}`}
                  icon={Truck}
                  accent="from-cyan-600 to-cyan-500"
                  delay={0.1}
                  trend={trendLogistics}
                  trendInverted
                  statusColor={logPerOrder > 300 ? 'border-red-500' : logPerOrder > 150 ? 'border-amber-500' : 'border-emerald-500'}
                />
                <TrendKpiCard
                  title={kpi.has_actual_storage ? 'Хранение (факт)' : 'Хранение'}
                  value={fmtM(kpi.total_storage_actual ?? kpi.total_storage)}
                  subtitle={kpi.forecast_30d ? `Прогноз 30д: ${fmtM(kpi.forecast_30d)}` : `за ${kpi.period_days}д`}
                  icon={Boxes}
                  accent="from-purple-600 to-purple-500"
                  delay={0.15}
                  trend={trendStorage}
                  trendInverted
                />
                <TrendKpiCard
                  title="Заказов"
                  value={fmt(kpi.total_orders)}
                  subtitle={`${fmt(kpi.total_stock)} шт на складах`}
                  icon={Package}
                  accent="from-emerald-600 to-emerald-500"
                  delay={0.2}
                  trend={trendOrders}
                />
                <TrendKpiCard
                  title="Кросс-отправки"
                  value={`${crossPct}%`}
                  subtitle={kpi.total_penalties > 0 ? `Штрафы: ${fmtM(kpi.total_penalties)}` : 'заказов в чужие округа'}
                  icon={ArrowRightLeft}
                  accent={crossPct > 50 ? 'from-red-600 to-red-500' : crossPct > 25 ? 'from-amber-600 to-amber-500' : 'from-emerald-600 to-emerald-500'}
                  delay={0.25}
                  statusColor={crossPct > 50 ? 'border-red-500' : crossPct > 25 ? 'border-amber-500' : 'border-emerald-500'}
                />
              </div>
            )
          })()}

          {/* ─── Problems Block ─── */}
          {(() => {
            const kpi = data.kpi
            const whs = data.warehouses
            const problems: React.ReactNode[] = []
            let delay = 0.3

            // 1. Cross-logistics
            const crossProblemSkus = whs.flatMap(w =>
              (w.skus || []).filter(s => s.cross_pct > 40 && s.orders > 5).map(s => ({
                ...s,
                warehouse: w.warehouse_name,
                loss: s.cross_orders * 33,
              }))
            )
            if (kpi.cross_pct > 25 || crossProblemSkus.length > 0) {
              const totalLoss = crossProblemSkus.reduce((s, x) => s + x.loss, 0)
              problems.push(
                <ProblemCard
                  key="cross"
                  severity={kpi.cross_pct > 50 ? 'critical' : 'warning'}
                  icon={ArrowRightLeft}
                  title={`Кросс-логистика: ${kpi.cross_pct}%`}
                  details={
                    crossProblemSkus.length > 0
                      ? `${crossProblemSkus.length} SKU с кросс > 40%, потери ~${fmtM(totalLoss)}/период. Худший склад: ${whs.reduce((a, b) => a.cross_pct > b.cross_pct ? a : b).warehouse_name} (${whs.reduce((a, b) => a.cross_pct > b.cross_pct ? a : b).cross_pct}%)`
                      : `Средний кросс ${kpi.cross_pct}% — доставка из неоптимальных складов`
                  }
                  link="/warehouses/cross"
                  linkLabel="Кросс-логистика"
                  delay={delay}
                />
              )
              delay += 0.08
            }

            // 2. Storage overstock
            const overstockedWhs = whs.filter(w => w.status === 'overstocked')
            if (overstockedWhs.length > 0) {
              problems.push(
                <ProblemCard
                  key="overstock"
                  severity="warning"
                  icon={Boxes}
                  title={`Затоваривание: ${overstockedWhs.length} складов`}
                  details={`Оборачиваемость > 120 дн: ${overstockedWhs.map(w => `${w.warehouse_name} (${fmtD(w.turnover_days)})`).slice(0, 3).join(', ')}${overstockedWhs.length > 3 ? ` + ещё ${overstockedWhs.length - 3}` : ''}`}
                  link="/warehouses/storage"
                  linkLabel="Хранение"
                  delay={delay}
                />
              )
              delay += 0.08
            }

            // 3. Critical supply (< 14 days)
            const criticalWhs = whs.filter(w => w.status === 'critical')
            if (criticalWhs.length > 0) {
              problems.push(
                <ProblemCard
                  key="supply"
                  severity="critical"
                  icon={PackagePlus}
                  title={`Нужна поставка: ${criticalWhs.length} складов`}
                  details={`Остаток < 14 дней: ${criticalWhs.map(w => `${w.warehouse_name} (${fmtD(w.turnover_days)})`).join(', ')}`}
                  link="/warehouses/supplies"
                  linkLabel="Поставки"
                  delay={delay}
                />
              )
              delay += 0.08
            }

            // 4. Penalties
            const penalties = kpi.total_penalties
            const penaltyDetails = kpi.penalty_details || []
            if (penalties > 0) {
              problems.push(
                <ProblemCard
                  key="penalties"
                  severity={penalties > 5000 ? 'critical' : 'warning'}
                  icon={Gavel}
                  title={`Штрафы за период: ${fmtM(penalties)}`}
                  details={
                    penaltyDetails.length > 0
                      ? penaltyDetails.map(d => `${d.reason}: ${fmtM(d.amount)} (${d.count} шт)`).join(' • ')
                      : 'Детализация по типам недоступна'
                  }
                  delay={delay}
                />
              )
              delay += 0.08
            }

            // 5. Geography (always, but green if OK)
            const activeWhs = whs.filter(w => w.stock > 0 || w.orders > 0)
            const okrugs = new Set(activeWhs.map(w => w.okrug).filter(Boolean))
            if (kpi.cross_pct <= 25) {
              problems.push(
                <ProblemCard
                  key="geo"
                  severity="ok"
                  icon={MapPin}
                  title="География в норме"
                  details={`${activeWhs.length} активных складов в ${okrugs.size} округах. Кросс-отправки ${kpi.cross_pct}% — под контролем.`}
                  link="/warehouses/geography"
                  linkLabel="География"
                  delay={delay}
                />
              )
            }

            if (problems.length === 0) return null

            return (
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.3 }}>
                <div className="space-y-3">
                  <div className="flex items-center gap-2 mb-1">
                    <ShieldAlert className="h-5 w-5 text-amber-400" />
                    <h2 className="text-lg font-bold text-[hsl(var(--foreground))]">Диагностика проблем</h2>
                  </div>
                  {problems}
                </div>
              </motion.div>
            )
          })()}

          {/* ─── AI Diagnostics ─── */}
          {currentShop && (
            <AIDiagnosticsBlock shopId={currentShop.id} period={period} />
          )}

          {/* ─── Costs Summary ─── */}
          <CostsSummary costs={data.costs} />

          {/* ─── Warehouses Table ─── */}
          <WarehousesTable warehouses={data.warehouses} />
        </>
      ) : null}
    </div>
  )
}
