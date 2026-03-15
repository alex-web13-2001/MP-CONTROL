/**
 * Warehouses Storage — dedicated page for WB storage analytics.
 * Uses real CostsSummary, StorageSkusTable, RecommendationsPanel from WBWarehouseAnalyticsContent.
 * Ozon: redirects to /warehouses/analytics.
 */
import { useState, useEffect, useCallback } from 'react'
import { Navigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import {
  RefreshCw,
  AlertTriangle,
  Boxes,
  TrendingUp,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBWarehouseAnalytics,
  type WBWarehouseAnalyticsResponse,
} from '@/api/warehouses'
import {
  StorageSkusTable,
  RecommendationsPanel,
} from './WBWarehouseAnalyticsContent'

/* ── Helpers ── */
function fmtM(v: number): string { return Math.round(v).toLocaleString('ru-RU') + ' ₽' }

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
          <h1 className="text-3xl font-bold tracking-tight">Хранение</h1>
          <p className="text-[hsl(var(--muted-foreground))] mt-1">
            Расходы на хранение, прогнозы и рекомендации по оптимизации
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
      ) : data ? (
        <>
          {/* KPI */}
          <StorageKpi data={data} />

          {/* Storage SKUs Table — оригинальный с сортировкой, поиском, прогнозом 30д, итого */}
          <StorageSkusTable skus={data.storage_skus} />

          {/* Recommendations — оригинальный с severity-иконками и action items */}
          <RecommendationsPanel recommendations={data.recommendations} />
        </>
      ) : null}
    </div>
  )
}
