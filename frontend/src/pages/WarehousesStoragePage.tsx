/**
 * Warehouses Storage — dedicated page for WB + Ozon storage analytics.
 * Uses real StorageSkusTable from WBWarehouseAnalyticsContent.
 * AI-powered storage analysis replaces static recommendations.
 */
import { useState, useEffect, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  RefreshCw,
  AlertTriangle,
  Boxes,
  TrendingUp,
  Sparkles,
  Brain,
  X,
  Lightbulb,
  CircleDollarSign,
  PackageX,
  Megaphone,
  PackageMinus,
  Flame,
  Ban,
  ChevronDown,
  ChevronRight,
  Warehouse,
  TrendingDown,
  AlertCircle,
  DollarSign,
  Package,
  Zap,
  Download,
  Clock,
  ShieldAlert,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBWarehouseAnalytics,
  getStorageAIAnalysis,
  downloadStorageExcel,
  getOzonStorage,
  type WBWarehouseAnalyticsResponse,
  type OzonStorageResponse,
  type StorageAIAnalysis,
  type StorageAISkuAction,
} from '@/api/warehouses'
import {
  StorageSkusTable,
} from './WBWarehouseAnalyticsContent'

/* ── Helpers ── */
function fmtM(v: number): string { return Math.round(v).toLocaleString('ru-RU') + ' ₽' }
function fmt(v: number): string { return Math.round(v).toLocaleString('ru-RU') }

/* ── Period Options ── */
const PERIOD_OPTIONS = [
  { label: '7 дн', value: 7 },
  { label: '14 дн', value: 14 },
  { label: '30 дн', value: 30 },
  { label: '60 дн', value: 60 },
  { label: '90 дн', value: 90 },
]

/* ═══ KPI Summary for Storage ═══ */
function StorageKpi({ data }: { data: WBWarehouseAnalyticsResponse }) {
  const kpi = data.kpi
  const hasForecast = data.storage_skus.some(s => s.forecast_30d != null)
  const totalForecast = hasForecast ? data.storage_skus.reduce((s, sk) => s + (sk.forecast_30d ?? 0), 0) : null

  const overstockCount = data.storage_skus.filter(s => (s.days_to_sell ?? 0) > 120).length
  const noSalesCount = data.storage_skus.filter(s => s.daily_sales <= 0).length

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
      <div className="grid grid-cols-3 gap-4">
        {/* Storage cost */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <Boxes className="h-4 w-4 text-purple-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              {kpi.has_actual_storage ? 'Хранение (факт)' : 'Хранение'}
            </span>
          </div>
          <div className="text-2xl font-bold tabular-nums text-red-400">
            {fmtM(kpi.total_storage_actual ?? kpi.total_storage)}
          </div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {kpi.has_actual_storage ? 'По отчётам WB' : 'Из удержаний'} • за {kpi.period_days}д
          </div>
        </div>

        {/* Forecast */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <TrendingUp className="h-4 w-4 text-cyan-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Прогноз 30д
            </span>
          </div>
          <div className="text-2xl font-bold tabular-nums text-cyan-400">
            {totalForecast != null ? fmtM(totalForecast) : '—'}
          </div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {totalForecast != null ? 'С учётом продаж (остатки убывают)' : 'Нет данных для прогноза'}
          </div>
        </div>

        {/* Problems */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <AlertTriangle className="h-4 w-4 text-red-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Проблемные SKU
            </span>
          </div>
          <div className="text-2xl font-bold tabular-nums">
            {overstockCount + noSalesCount}
          </div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {overstockCount > 0 && <span className="text-amber-400">{overstockCount} затоварено</span>}
            {overstockCount > 0 && noSalesCount > 0 && ' • '}
            {noSalesCount > 0 && <span className="text-red-400">{noSalesCount} без продаж</span>}
            {overstockCount === 0 && noSalesCount === 0 && 'Всё в норме ✓'}
          </div>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Storage AI Insight Component
   ═══════════════════════════════════════════════════════════ */

const OPTION_ICONS: Record<string, { icon: React.ElementType; color: string }> = {
  discount:       { icon: CircleDollarSign, color: 'text-amber-400' },
  withdraw:       { icon: PackageX,         color: 'text-red-400' },
  launch_ads:     { icon: Megaphone,        color: 'text-orange-400' },
  reduce_supply:  { icon: PackageMinus,     color: 'text-blue-400' },
  liquidate:      { icon: Flame,            color: 'text-red-500' },
  do_nothing:     { icon: Ban,              color: 'text-gray-400' },
}

const RISK_COLORS: Record<string, { bg: string; text: string; label: string }> = {
  low:    { bg: 'bg-emerald-500/15', text: 'text-emerald-400', label: 'Низкий' },
  medium: { bg: 'bg-amber-500/15',   text: 'text-amber-400',   label: 'Средний' },
  high:   { bg: 'bg-red-500/15',     text: 'text-red-400',     label: 'Высокий' },
}

function SkuActionCard({ action }: { action: StorageAISkuAction }) {
  const [expanded, setExpanded] = useState(false)
  const recommended = action.recommended_option ?? 0

  return (
    <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)] overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-6 py-5 text-left hover:bg-[hsl(var(--muted)/0.06)] transition-colors"
      >
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-[16px] font-bold text-[hsl(var(--foreground))]">{action.vendor_code}</span>
              <span className="text-[15px] text-[hsl(var(--muted-foreground))] truncate">{action.name}</span>
            </div>
            <p className="text-[14px] text-[hsl(var(--foreground)/0.85)] mt-1.5 leading-relaxed">{action.diagnosis}</p>
            <div className="flex items-center gap-4 mt-2 text-[13px] text-[hsl(var(--muted-foreground)/0.6)]">
              <span className="flex items-center gap-1">
                <Warehouse className="h-3.5 w-3.5" />
                {fmtM(action.current_storage_cost)}/мес
              </span>
              <span className="flex items-center gap-1">
                <TrendingDown className="h-3.5 w-3.5" />
                {action.current_turnover_days}д
              </span>
              <span className="flex items-center gap-1">
                <Package className="h-3.5 w-3.5" />
                {action.stock} шт
              </span>
            </div>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <span className="text-[12px] font-bold px-2.5 py-1 rounded bg-red-500/10 text-red-400 tabular-nums">
              {fmtM(action.current_storage_cost)}/мес
            </span>
            {expanded ? (
              <ChevronDown className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
            ) : (
              <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
            )}
          </div>
        </div>
      </button>

      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.25 }}
            className="overflow-hidden"
          >
            <div className="px-6 pb-6 pt-2 space-y-3 border-t border-[hsl(var(--border)/0.15)]">
              {action.options.map((opt, idx) => {
                const iconConfig = OPTION_ICONS[opt.action] || OPTION_ICONS.do_nothing
                const Icon = iconConfig.icon
                const riskConfig = RISK_COLORS[opt.risk] || RISK_COLORS.medium
                const isRecommended = idx === recommended

                return (
                  <div
                    key={idx}
                    className={`rounded-lg border p-4 transition-colors ${
                      isRecommended
                        ? 'border-[hsl(var(--primary)/0.4)] bg-[hsl(var(--primary)/0.05)]'
                        : 'border-[hsl(var(--border)/0.2)] bg-[hsl(var(--muted)/0.03)]'
                    }`}
                  >
                    <div className="flex items-start gap-3">
                      <div className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-lg ${iconConfig.color.replace('text-', 'bg-').replace(/400|500/, '500/10')}`}>
                        <Icon className={`h-4.5 w-4.5 ${iconConfig.color}`} />
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                          <span className="text-[15px] font-bold text-[hsl(var(--foreground))]">{opt.label}</span>
                          {isRecommended && (
                            <span className="text-[11px] font-bold px-2 py-0.5 rounded-full bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))]">
                              Рекомендуется
                            </span>
                          )}
                          <span className={`text-[11px] font-bold px-2 py-0.5 rounded ${riskConfig.bg} ${riskConfig.text}`}>
                            Риск: {riskConfig.label}
                          </span>
                        </div>
                        <p className="text-[14px] text-[hsl(var(--foreground)/0.8)] mt-1.5 leading-relaxed">
                          {opt.detail}
                        </p>
                        <div className="flex items-center gap-4 mt-2 text-[13px]">
                          {opt.expected_savings > 0 && (
                            <span className="flex items-center gap-1 text-emerald-400 font-semibold">
                              <Zap className="h-3.5 w-3.5" />
                              Экономия: {fmtM(opt.expected_savings)}/мес
                            </span>
                          )}
                          {opt.withdrawal_cost > 0 && (
                            <span className="flex items-center gap-1 text-red-400 font-semibold">
                              <DollarSign className="h-3.5 w-3.5" />
                              Стоимость вывоза: {fmtM(opt.withdrawal_cost)}
                            </span>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

function StorageAIInsight({ shopId, period, marketplace }: { shopId: number; period: number; marketplace?: string }) {
  const [data, setData] = useState<StorageAIAnalysis | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [modalOpen, setModalOpen] = useState(false)
  const [refreshing, setRefreshing] = useState(false)

  const fetchAI = useCallback(async (force = false) => {
    if (force) setRefreshing(true)
    else setLoading(true)
    setError(null)
    try {
      const result = await getStorageAIAnalysis({ shop_id: shopId, period, force, marketplace })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка ИИ-анализа')
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [shopId, period, marketplace])

  useEffect(() => { fetchAI() }, [fetchAI])

  // Lock body scroll when modal is open
  useEffect(() => {
    if (modalOpen) {
      document.body.style.overflow = 'hidden'
      return () => { document.body.style.overflow = '' }
    }
  }, [modalOpen])

  const severityConfig = {
    critical: { bg: 'from-red-500/10 to-red-500/5', border: 'border-red-500/30', icon: '🔴', label: 'Критично', bannerBg: 'bg-red-500/8', bannerBorder: 'border-red-500/25' },
    warning:  { bg: 'from-amber-500/10 to-amber-500/5', border: 'border-amber-500/30', icon: '🟡', label: 'Внимание', bannerBg: 'bg-amber-500/8', bannerBorder: 'border-amber-500/25' },
    ok:       { bg: 'from-emerald-500/10 to-emerald-500/5', border: 'border-emerald-500/30', icon: '🟢', label: 'Всё ОК', bannerBg: 'bg-emerald-500/8', bannerBorder: 'border-emerald-500/25' },
  }

  /* ── Loading state ── */
  if (loading && !data) {
    return (
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
        <div className="flex items-center gap-3 px-4 py-3 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-purple-600 to-pink-500 flex items-center justify-center">
            <Brain className="h-4 w-4 text-white animate-pulse" />
          </div>
          <Skeleton className="h-4 w-64" />
          <Skeleton className="h-4 w-24 ml-auto" />
        </div>
      </motion.div>
    )
  }

  /* ── Error state ── */
  if (error) {
    return (
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
        <div className="flex items-center gap-3 px-4 py-3 rounded-xl border border-red-500/20 bg-[hsl(var(--card))]">
          <AlertTriangle className="h-4 w-4 text-red-400 shrink-0" />
          <span className="text-[13px] text-[hsl(var(--muted-foreground))]">{error}</span>
          <button onClick={() => fetchAI(true)} className="ml-auto text-[13px] font-medium text-[hsl(var(--primary))] hover:underline">
            Повторить
          </button>
        </div>
      </motion.div>
    )
  }

  if (!data) return null

  const sev = severityConfig[data.severity] || severityConfig.warning
  const analyzedAtStr = data.analyzed_at
    ? `Анализ от ${new Date(data.analyzed_at * 1000).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })}`
    : null
  const km = data.key_metrics || { total_storage_monthly: 0, potential_savings: 0, storage_roi_pct: 0, overstock_skus: 0, loss_making_skus: 0, avg_turnover_days: 0 }
  const ctx = data.context || { total_storage_30d: 0, total_net_profit: 0, total_skus: 0, total_stock: 0, overstock_skus: 0, loss_making_skus: 0, avg_turnover_days: 0 }

  const actionsCount = data.sku_actions?.length || 0

  return (
    <>
      {/* ═══ Compact Banner ═══ */}
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.3 }}>
        <div className={`flex items-center gap-3 px-4 py-3 rounded-xl border ${sev.bannerBorder} bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted)/0.08)] transition-colors`}>
          <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-purple-600 to-pink-500 shadow-md shadow-purple-500/20 flex items-center justify-center shrink-0">
            <Sparkles className="h-4 w-4 text-white" />
          </div>

          <div className="flex items-center gap-2 min-w-0 flex-1">
            <span className="text-sm">{sev.icon}</span>
            <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">{sev.label}</span>
            <span className="text-[13px] text-[hsl(var(--muted-foreground))] truncate hidden sm:inline">
              · {data.diagnosis?.substring(0, 100)}{(data.diagnosis?.length || 0) > 100 ? '…' : ''}
            </span>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            {actionsCount > 0 && (
              <span className="text-[11px] font-semibold px-2 py-0.5 rounded-full bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))]">
                {actionsCount} рекомендаци{actionsCount === 1 ? 'я' : actionsCount < 5 ? 'и' : 'й'}
              </span>
            )}
            {analyzedAtStr && (
              <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)] hidden md:inline">{analyzedAtStr}</span>
            )}
            <button
              onClick={(e) => { e.stopPropagation(); fetchAI(true) }}
              disabled={refreshing}
              className="p-1.5 rounded-lg hover:bg-[hsl(var(--muted)/0.3)] transition-colors"
              title="Обновить анализ"
            >
              <RefreshCw className={`h-3.5 w-3.5 text-[hsl(var(--muted-foreground))] ${refreshing ? 'animate-spin' : ''}`} />
            </button>
            <button
              onClick={() => setModalOpen(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-[hsl(var(--primary))] text-white text-[12px] font-semibold hover:opacity-90 transition-opacity"
            >
              <Brain className="h-3.5 w-3.5" />
              Прочитать
            </button>
          </div>
        </div>
      </motion.div>

      {/* ═══ Full-screen Modal ═══ */}
      <AnimatePresence>
        {modalOpen && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="fixed inset-0 z-[100] flex items-start justify-center bg-black/60 backdrop-blur-sm"
            onClick={() => setModalOpen(false)}
          >
            <motion.div
              initial={{ opacity: 0, y: 40, scale: 0.97 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: 40, scale: 0.97 }}
              transition={{ duration: 0.25, ease: 'easeOut' }}
              className="w-full max-w-[1100px] max-h-[90vh] mt-[5vh] mx-4 rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-2xl overflow-hidden flex flex-col"
              onClick={(e) => e.stopPropagation()}
            >
              {/* Modal Header */}
              <div className={`px-8 py-5 bg-gradient-to-r ${sev.bg} border-b border-[hsl(var(--border)/0.3)] shrink-0`}>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="h-12 w-12 rounded-xl bg-gradient-to-br from-purple-600 to-pink-500 shadow-lg shadow-purple-500/25 flex items-center justify-center">
                      <Sparkles className="h-6 w-6 text-white" />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <h3 className="text-lg font-bold text-[hsl(var(--foreground))]">ИИ-Анализ хранения</h3>
                        <span className="text-sm">{sev.icon}</span>
                        <span className="text-sm font-semibold text-[hsl(var(--muted-foreground))]">{sev.label}</span>
                      </div>
                      <p className="text-[15px] text-[hsl(var(--muted-foreground))] mt-1 leading-relaxed max-w-[700px]">{data.diagnosis}</p>
                    </div>
                  </div>
                  <button
                    onClick={() => setModalOpen(false)}
                    className="p-2.5 rounded-xl hover:bg-[hsl(var(--muted)/0.3)] transition-colors"
                    title="Закрыть"
                  >
                    <X className="h-6 w-6 text-[hsl(var(--muted-foreground))]" />
                  </button>
                </div>
              </div>

              {/* Modal Body — scrollable */}
              <div className="flex-1 overflow-y-auto px-8 py-6 space-y-8">
                {/* 4 Metric cards */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-5">
                  <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <Warehouse className="h-5 w-5 text-purple-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Хранение/мес</span>
                    </div>
                    <p className="text-2xl font-bold text-red-400">{fmtM(km.total_storage_monthly || ctx.total_storage_30d)}</p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">расходы за 30д</p>
                  </div>

                  <div className="rounded-xl bg-emerald-500/5 border border-emerald-500/20 p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <Zap className="h-5 w-5 text-emerald-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-emerald-500/60">Экономия</span>
                    </div>
                    <p className="text-2xl font-bold text-emerald-400">{fmtM(km.potential_savings || 0)}</p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">потенциальная/мес</p>
                  </div>

                  <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <AlertCircle className="h-5 w-5 text-amber-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Затоваренных</span>
                    </div>
                    <p className={`text-2xl font-bold ${(km.overstock_skus || ctx.overstock_skus) > 5 ? 'text-red-400' : (km.overstock_skus || ctx.overstock_skus) > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
                      {km.overstock_skus || ctx.overstock_skus || 0}
                    </p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">SKU с оборач. &gt; 90д</p>
                  </div>

                  <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <TrendingDown className="h-5 w-5 text-red-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Убыточных</span>
                    </div>
                    <p className={`text-2xl font-bold ${(km.loss_making_skus || ctx.loss_making_skus) > 3 ? 'text-red-400' : (km.loss_making_skus || ctx.loss_making_skus) > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
                      {km.loss_making_skus || ctx.loss_making_skus || 0}
                    </p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">хранение &gt; прибыли</p>
                  </div>
                </div>

                {/* ═══ SKU Actions ═══ */}
                {data.sku_actions?.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <Package className="h-5 w-5" />
                      Рекомендации по товарам ({data.sku_actions.length})
                    </h4>
                    {data.sku_actions.map((action, idx) => (
                      <SkuActionCard key={idx} action={action} />
                    ))}
                  </div>
                )}

                {/* ═══ General Tips ═══ */}
                {data.general_tips?.length > 0 && (
                  <div className="space-y-2">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <Lightbulb className="h-5 w-5" />
                      Общие рекомендации
                    </h4>
                    {data.general_tips.map((tip, i) => (
                      <div key={i} className="rounded-xl border border-[hsl(var(--border)/0.2)] bg-[hsl(var(--muted)/0.04)] p-5">
                        <p className="text-[16px] text-[hsl(var(--foreground)/0.95)] leading-relaxed">{tip}</p>
                      </div>
                    ))}
                  </div>
                )}

                {/* Context line */}
                <div className="flex flex-wrap items-center gap-x-5 gap-y-1 text-[13px] text-[hsl(var(--muted-foreground)/0.4)]">
                  <span>Период: {data.period_days}д</span>
                  <span>SKU: {ctx.total_skus}</span>
                  <span>Остаток: {fmt(ctx.total_stock)} шт</span>
                  <span>Ср. оборачиваемость: {km.avg_turnover_days || ctx.avg_turnover_days}д</span>
                  {data.cached && <span>Из кеша</span>}
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  )
}

/* ═══ Ozon Storage KPIs ═══ */
function OzonStorageKpi({ data }: { data: OzonStorageResponse }) {
  const kpi = data.kpi
  const hasActual = kpi.has_actual_data ?? false

  // Forecast
  const hasForecast = data.storage_skus.some((s: any) => s.forecast_30d != null)
  const totalForecast = hasForecast ? data.storage_skus.reduce((sum: number, sk: any) => sum + (sk.forecast_30d ?? 0), 0) : null

  // Count SKUs by zone
  const paidSkus = data.storage_skus.filter((s: any) => s.zone === 'paid')
  const warningSkus = data.storage_skus.filter((s: any) => s.zone === 'warning')

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
      <div className="grid grid-cols-4 gap-4">
        {/* Storage cost */}
        <div className={`p-4 rounded-xl border ${hasActual ? 'border-emerald-500/30 bg-emerald-500/5' : 'border-[hsl(var(--border))] bg-[hsl(var(--card))]'}`}>
          <div className="flex items-center gap-2 mb-1">
            <Boxes className={`h-4 w-4 ${hasActual ? 'text-emerald-400' : 'text-purple-400'}`} />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              {hasActual ? 'Хранение (факт)' : 'Хранение (расчёт)'}
            </span>
          </div>
          <div className={`text-2xl font-bold tabular-nums ${hasActual ? 'text-emerald-400' : 'text-red-400'}`}>
            {fmtM(kpi.total_storage)}
          </div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {hasActual && kpi.actual_period
              ? `Отчёт Ozon: ${kpi.actual_period.from} — ${kpi.actual_period.to}`
              : `Расчёт ~0.14 ₽/л/день • за ${kpi.period_days}д`}
          </div>
        </div>

        {/* Turnover */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <Clock className="h-4 w-4 text-cyan-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Ср. оборачиваемость
            </span>
          </div>
          <div className="text-2xl font-bold tabular-nums text-cyan-400">
            {kpi.avg_turnover_days != null ? `${Math.round(kpi.avg_turnover_days)} дн` : '—'}
          </div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {kpi.total_skus} SKU • {fmt(kpi.total_stock)} шт на FBO
          </div>
        </div>

        {/* Forecast 30d */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <TrendingUp className="h-4 w-4 text-cyan-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Прогноз 30д
            </span>
          </div>
          <div className="text-2xl font-bold tabular-nums text-cyan-400">
            {totalForecast != null ? fmtM(totalForecast) : '—'}
          </div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {totalForecast != null ? 'С учётом продаж' : 'Нет данных'}
          </div>
        </div>

        {/* Risk SKUs */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <ShieldAlert className="h-4 w-4 text-red-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Риск платного хранения
            </span>
          </div>
          <div className="text-2xl font-bold tabular-nums">
            {paidSkus.length + warningSkus.length}
          </div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {paidSkus.length > 0 && <span className="text-red-400">{paidSkus.length} обор. &gt; 160д</span>}
            {paidSkus.length > 0 && warningSkus.length > 0 && ' • '}
            {warningSkus.length > 0 && <span className="text-amber-400">{warningSkus.length} обор. 120–160д</span>}
            {paidSkus.length === 0 && warningSkus.length === 0 && 'Всё в норме ✓'}
          </div>
        </div>
      </div>


    </motion.div>
  )
}

/* ═══ Skeleton ═══ */
function StorageSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        {Array.from({ length: 3 }).map((_, i) => (
          <Skeleton key={i} className="h-[100px] rounded-2xl" />
        ))}
      </div>
      <Skeleton className="h-[200px] rounded-2xl" />
      <Skeleton className="h-[500px] rounded-2xl" />
      <Skeleton className="h-[200px] rounded-2xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function WarehousesStoragePage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isWB = currentShop?.marketplace === 'wildberries'
  const isOzon = currentShop?.marketplace === 'ozon'

  // WB state
  const [wbData, setWbData] = useState<WBWarehouseAnalyticsResponse | null>(null)
  // Ozon state
  const [ozonData, setOzonData] = useState<OzonStorageResponse | null>(null)

  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState(30)
  const [downloading, setDownloading] = useState(false)

  const fetchData = useCallback(async () => {
    if (!currentShop) return
    setLoading(true)
    setError(null)
    try {
      if (isWB) {
        const result = await getWBWarehouseAnalytics({ shop_id: currentShop.id, period })
        setWbData(result)
        setOzonData(null)
      } else if (isOzon) {
        const result = await getOzonStorage({ shop_id: currentShop.id, period })
        setOzonData(result)
        setWbData(null)
      }
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [currentShop, isWB, isOzon, period])

  useEffect(() => { fetchData() }, [fetchData])

  const hasData = isWB ? !!wbData : !!ozonData

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
          <h1 className="text-3xl font-bold tracking-tight">Хранение</h1>
          <p className="text-[hsl(var(--muted-foreground))] mt-1">
            {isOzon
              ? 'Оборачиваемость FBO, зоны хранения и прогноз расходов'
              : 'Расходы на хранение, прогнозы и ИИ-рекомендации по оптимизации'}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {isWB && (
            <button
              onClick={async () => {
                if (!currentShop) return
                setDownloading(true)
                try {
                  await downloadStorageExcel({ shop_id: currentShop.id, period })
                } catch {
                  // ignore
                } finally {
                  setDownloading(false)
                }
              }}
              disabled={downloading || loading || !hasData}
              className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20 transition-all disabled:opacity-50"
            >
              <Download className={`h-4 w-4 ${downloading ? 'animate-bounce' : ''}`} />
              Excel
            </button>
          )}
          <button
            onClick={fetchData}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            Обновить
          </button>
        </div>
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

      {loading && !hasData ? (
        <StorageSkeleton />
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
      ) : isWB && wbData ? (
        <>
          {/* WB KPI */}
          <StorageKpi data={wbData} />

          {/* AI Storage Analysis — replaces static RecommendationsPanel */}
          {currentShop && <StorageAIInsight shopId={currentShop.id} period={period} marketplace="wildberries" />}

          {/* Storage SKUs Table — оригинальный с сортировкой, поиском, прогнозом 30д, итого */}
          <StorageSkusTable skus={wbData.storage_skus} />
        </>
      ) : isOzon && ozonData ? (
        <>
          {/* Ozon KPI */}
          <OzonStorageKpi data={ozonData} />

          {/* AI Storage Analysis */}
          {currentShop && <StorageAIInsight shopId={currentShop.id} period={period} marketplace="ozon" />}

          {/* Storage SKUs Table — reusing WB component, data format is compatible */}
          <StorageSkusTable skus={ozonData.storage_skus as any} isEstimate={!(ozonData.kpi.has_actual_data)} />
        </>
      ) : null}
    </div>
  )
}

