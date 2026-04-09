import { useState, useEffect, useCallback } from 'react'
import { motion } from 'framer-motion'
import {
  Package,
  XCircle,
  RefreshCw,
} from 'lucide-react'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getOzonDashboardApi,
  getWbDashboardApi,
  type WbDashboardResponse,
} from '@/api/dashboard'
import WbDashboardContent from './WbDashboardContent'
import OzonDashboardContent from './OzonDashboardContent'

/* ═══════════════════════════════════════════════════════════
   Constants
   ═══════════════════════════════════════════════════════════ */

const PERIODS = [
  { key: 'today', label: 'Сегодня' },
  { key: '7d', label: '7 дней' },
  { key: '30d', label: '30 дней' },
] as const

/* ═══════════════════════════════════════════════════════════
   Period Selector
   ═══════════════════════════════════════════════════════════ */

function PeriodSelector({
  current,
  onChange,
}: {
  current: string
  onChange: (period: string) => void
}) {
  return (
    <div className="inline-flex rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1">
      {PERIODS.map((p) => (
        <button
          key={p.key}
          onClick={() => onChange(p.key)}
          className={`rounded-md px-5 py-2 text-sm font-medium transition-all duration-200 ${
            current === p.key
              ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
              : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
          }`}
        >
          {p.label}
        </button>
      ))}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Loading Skeleton
   ═══════════════════════════════════════════════════════════ */

function DashboardSkeleton() {
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
      <Skeleton className="h-[370px] rounded-xl" />
      <Skeleton className="h-[400px] rounded-xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Dashboard Page
   ═══════════════════════════════════════════════════════════ */

export default function DashboardPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const [period, setPeriod] = useState('today')
  const [ozonData, setOzonData] = useState<WbDashboardResponse | null>(null)
  const [wbData, setWbData] = useState<WbDashboardResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const isWb = currentShop?.marketplace === 'wildberries'

  const fetchDashboard = useCallback(async () => {
    if (!currentShop) return

    setLoading(true)
    setError(null)
    try {
      if (currentShop.marketplace === 'ozon') {
        const result = await getOzonDashboardApi(currentShop.id, period)
        setOzonData(result)
        setWbData(null)
      } else {
        const result = await getWbDashboardApi(currentShop.id, period)
        setWbData(result)
        setOzonData(null)
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : 'Неизвестная ошибка'
      setError(msg)
    } finally {
      setLoading(false)
    }
  }, [currentShop, period])

  useEffect(() => {
    fetchDashboard()
  }, [fetchDashboard])

  // Auto-refresh every 2 minutes
  useEffect(() => {
    if (!currentShop) return
    const interval = setInterval(fetchDashboard, 120_000)
    return () => clearInterval(interval)
  }, [currentShop, fetchDashboard])

  // ── No shop selected ──────────────────────────────
  if (!currentShop) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="text-center space-y-3">
          <Package className="h-12 w-12 mx-auto text-[hsl(var(--muted-foreground)/0.3)]" />
          <p className="text-lg font-medium text-[hsl(var(--muted-foreground))]">
            Выберите магазин
          </p>
          <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">
            Для просмотра аналитики выберите магазин в шапке
          </p>
        </div>
      </div>
    )
  }

  const marketplaceLabel = isWb ? 'Wildberries' : 'Ozon'
  const hasData = isWb ? !!wbData : !!ozonData

  // ── Loading state ──────────────────────────────────
  if (loading && !hasData) {
    return <DashboardSkeleton />
  }

  // ── Error state ────────────────────────────────────
  if (error && !hasData) {
    return (
      <div className="flex h-[60vh] items-center justify-center">
        <div className="text-center space-y-3">
          <XCircle className="h-12 w-12 mx-auto text-red-400" />
          <p className="text-lg font-medium text-red-400">Ошибка загрузки</p>
          <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">{error}</p>
          <button
            onClick={fetchDashboard}
            className="mt-2 inline-flex items-center gap-2 rounded-lg bg-[hsl(var(--primary))] px-4 py-2 text-sm font-medium text-white hover:opacity-90 transition-opacity"
          >
            <RefreshCw className="h-4 w-4" /> Повторить
          </button>
        </div>
      </div>
    )
  }

  if (!hasData) return null

  // ── Dashboard content with header ──────────────────
  const data = isWb ? wbData! : ozonData!

  return (
    <div className="space-y-6">
      <motion.div
        initial={{ opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
        className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between"
      >
        <div className="space-y-1">
          <h1 className="text-2xl font-bold tracking-tight text-[hsl(var(--foreground))]">
            Обзор
          </h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            {currentShop.name} • {marketplaceLabel}
            {loading && (
              <span className="ml-2 inline-flex items-center gap-1 text-[hsl(var(--primary))]">
                <RefreshCw className="h-3 w-3 animate-spin" /> Обновление…
              </span>
            )}
          </p>
        </div>
        <PeriodSelector current={period} onChange={setPeriod} />
      </motion.div>

      {isWb ? (
        <WbDashboardContent data={data} />
      ) : (
        <OzonDashboardContent data={data} />
      )}
    </div>
  )
}
