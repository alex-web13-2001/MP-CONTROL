import { useState, useEffect, useCallback } from 'react'
import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import { ArrowLeft, RefreshCw, XCircle } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getAdvertisingAnalytics,
  type AdvertisingAnalyticsResponse,
} from '@/api/advertising'
import {
  PeriodSelector,
  CampaignsTable,
  formatMoney,
  formatNumber,
} from '@/pages/AdvertisingAnalyticsPage'
import type { DateRange } from 'react-day-picker'

/* ═══════════════════════════════════════════════════════════
   Loading Skeleton
   ═══════════════════════════════════════════════════════════ */

function CampaignsSkeleton() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="space-y-2">
          <Skeleton className="h-8 w-56" />
          <Skeleton className="h-4 w-72" />
        </div>
        <Skeleton className="h-9 w-52 rounded-lg" />
      </div>
      {/* Summary cards */}
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        {[...Array(4)].map((_, i) => (
          <Skeleton key={i} className="h-[80px] rounded-xl" />
        ))}
      </div>
      {/* Filters area */}
      <Skeleton className="h-10 w-full rounded-lg" />
      {/* Table rows */}
      <div className="space-y-2">
        {[...Array(8)].map((_, i) => (
          <Skeleton key={i} className="h-14 rounded-lg" />
        ))}
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Summary KPI Strip
   ═══════════════════════════════════════════════════════════ */

function SummaryStrip({ data }: { data: AdvertisingAnalyticsResponse }) {
  const kpi = data.kpi
  const items: { label: string; value: string; delta: number; color: string; invert?: boolean }[] = [
    { label: 'Расход', value: formatMoney(kpi.spend), delta: kpi.spend_delta, color: '#f97316' },
    { label: 'Показы', value: formatNumber(kpi.views), delta: kpi.views_delta, color: '#3b82f6' },
    { label: 'Корзины', value: formatNumber(kpi.cart), delta: kpi.cart_delta, color: '#a855f7' },
    { label: 'Заказы', value: formatNumber(kpi.orders), delta: kpi.orders_delta, color: '#10b981' },
    { label: 'Выручка', value: formatMoney(kpi.revenue), delta: kpi.revenue_delta, color: '#eab308' },
    { label: 'ДРР', value: `${kpi.drr.toFixed(1)}%`, delta: kpi.drr_delta, color: kpi.drr > 30 ? '#ef4444' : kpi.drr > 15 ? '#f59e0b' : '#10b981', invert: true },
  ]
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
      {items.map((it) => {
        const isPositive = it.invert ? it.delta < 0 : it.delta > 0
        const isNeutral = it.delta === 0
        const deltaColor = isNeutral ? 'text-[hsl(var(--muted-foreground)/0.4)]' : isPositive ? 'text-emerald-400' : 'text-red-400'
        const sign = it.delta > 0 ? '+' : ''
        return (
          <div
            key={it.label}
            className="flex items-center gap-3 rounded-xl border border-[hsl(var(--border)/0.5)] bg-[hsl(var(--card))] px-4 py-3"
          >
            <div className="h-2 w-2 rounded-full shrink-0" style={{ background: it.color }} />
            <div className="min-w-0">
              <p className="text-[12px] text-[hsl(var(--muted-foreground))]">{it.label}</p>
              <p className="text-[15px] font-bold">{it.value}</p>
              <p className={`text-[12px] font-medium ${deltaColor}`}>
                {isNeutral ? '—' : `${sign}${it.delta.toFixed(1)}%`}
              </p>
            </div>
          </div>
        )
      })}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function AdvertisingCampaignsPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const [period, setPeriod] = useState('7d')
  const [customRange, setCustomRange] = useState<DateRange | null>(null)
  const [data, setData] = useState<AdvertisingAnalyticsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchData = useCallback(async () => {
    if (!currentShop) return
    setLoading(true)
    setError(null)
    try {
      let dateFrom: string | undefined
      let dateTo: string | undefined
      if (customRange?.from) {
        const toDate = customRange.to ?? customRange.from
        const pad = (n: number) => String(n).padStart(2, '0')
        dateFrom = `${customRange.from.getFullYear()}-${pad(customRange.from.getMonth() + 1)}-${pad(customRange.from.getDate())}`
        dateTo = `${toDate.getFullYear()}-${pad(toDate.getMonth() + 1)}-${pad(toDate.getDate())}`
      }
      const result = await getAdvertisingAnalytics(currentShop.id, customRange ? 'custom' : period, dateFrom, dateTo)
      setData(result)
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || 'Ошибка загрузки данных'
      setError(msg)
    } finally {
      setLoading(false)
    }
  }, [currentShop, period, customRange])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  if (!currentShop) {
    return (
      <div className="flex h-64 items-center justify-center">
        <p className="text-[hsl(var(--muted-foreground))]">Выберите магазин</p>
      </div>
    )
  }

  if (loading && !data) return <CampaignsSkeleton />

  if (error) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Кампании</h1>
        </div>
        <Card className="border-red-500/20 bg-red-500/5">
          <CardContent className="flex items-center gap-4 py-8">
            <XCircle className="h-8 w-8 text-red-400 shrink-0" />
            <div>
              <p className="font-medium text-red-400">Ошибка загрузки</p>
              <p className="text-sm text-[hsl(var(--muted-foreground))]">{error}</p>
            </div>
            <button
              onClick={fetchData}
              className="ml-auto flex items-center gap-2 rounded-lg bg-[hsl(var(--primary))] px-4 py-2 text-sm font-medium text-white hover:opacity-90 transition-opacity"
            >
              <RefreshCw className="h-4 w-4" />
              Повторить
            </button>
          </CardContent>
        </Card>
      </div>
    )
  }

  if (!data) return null

  return (
    <div className="space-y-6">
      {/* ── Overlay Loader ── */}
      {loading && data && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/20 backdrop-blur-[2px]">
          <div className="flex items-center gap-3 rounded-xl bg-[hsl(var(--card))] border border-[hsl(var(--border))] px-6 py-4 shadow-2xl">
            <RefreshCw className="h-5 w-5 animate-spin text-[hsl(var(--primary))]" />
            <span className="text-sm font-medium">Обновление данных...</span>
          </div>
        </div>
      )}

      {/* ── Header ── */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-3">
          <Link
            to="/advertising/analytics"
            className="flex h-9 w-9 items-center justify-center rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:border-[hsl(var(--primary)/0.3)] transition-all"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <div>
            <h1 className="text-2xl font-bold tracking-tight">Кампании</h1>
            <p className="text-[hsl(var(--muted-foreground))]">
              {data.marketplace === 'ozon' ? 'Ozon' : 'Wildberries'} ·{' '}
              {new Date(data.date_from).toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' })}
              {' — '}
              {new Date(data.date_to).toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' })}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <PeriodSelector
            current={customRange ? 'custom' : period}
            onChange={setPeriod}
            customRange={customRange}
            onCustomRange={setCustomRange}
          />
        </div>
      </div>

      {/* ── Summary KPI ── */}
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.3 }}>
        <SummaryStrip data={data} />
      </motion.div>

      {/* ── Campaigns Table ── */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.1 }}
      >
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-lg">
              Кампании за период
              <span className="ml-2 text-[14px] font-normal text-[hsl(var(--muted-foreground))]">
                ({data.campaigns_table.length})
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent>
            <CampaignsTable
              campaigns={data.campaigns_table}
              marketplace={currentShop!.marketplace}
              dateFrom={data.date_from}
              dateTo={data.date_to}
            />
          </CardContent>
        </Card>
      </motion.div>
    </div>
  )
}
