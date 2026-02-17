import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '@/stores/authStore'
import ShopWizard from '@/components/shops/ShopWizard'
import { getSyncStatusApi, SyncStatusResponse } from '@/api/auth'
import { motion } from 'framer-motion'
import { useRef } from 'react'

/* ────────────────── Syncing screen for return visits ────────────────── */
function ReturnSyncView({
  shopId,
  shopName,
  marketplace,
  onDashboard,
}: {
  shopId: number
  shopName: string
  marketplace: string
  onDashboard: () => void
}) {
  const [syncStatus, setSyncStatus] = useState<SyncStatusResponse | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    const poll = async () => {
      try {
        const status = await getSyncStatusApi(shopId)
        setSyncStatus(status)
        if (status.status === 'done' || status.status === 'error') {
          if (intervalRef.current) {
            clearInterval(intervalRef.current)
            intervalRef.current = null
          }
        }
      } catch {
        // ignore
      }
    }
    poll()
    intervalRef.current = setInterval(poll, 3000)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [shopId])

  const isDone = syncStatus?.status === 'done' || syncStatus?.status === 'done_with_errors'
  const isError = syncStatus?.status === 'error'
  const percent = syncStatus?.percent ?? 0

  return (
    <div className="max-w-lg mx-auto space-y-6 text-center py-8">
      <div className="text-5xl">{isDone ? '🎉' : isError ? '⚠️' : '📦'}</div>
      <h2 className="text-2xl font-bold">
        {isDone ? 'Данные загружены!' : isError ? 'Загрузка завершена с ошибками' : 'Загрузка данных...'}
      </h2>
      <p className="text-[hsl(var(--muted-foreground))]">
        <strong>{shopName}</strong> ({marketplace === 'wildberries' ? 'Wildberries' : 'Ozon'})
      </p>
      <div className="space-y-3">
        <div className="h-3 w-full rounded-full bg-[hsl(var(--muted))] overflow-hidden">
          <motion.div
            className={`h-full rounded-full transition-all duration-500 ${isDone ? 'bg-green-500' : isError ? 'bg-amber-500' : 'bg-[hsl(var(--primary))]'}`}
            initial={{ width: 0 }}
            animate={{ width: `${percent}%` }}
            transition={{ duration: 0.5, ease: 'easeOut' }}
          />
        </div>
        <div className="flex items-center justify-between text-sm">
          <span className="text-[hsl(var(--muted-foreground))]">
            {syncStatus?.step_name || 'Ожидание начала загрузки...'}
          </span>
          <span className="font-mono font-semibold">{percent}%</span>
        </div>
        {syncStatus && syncStatus.total_steps > 0 && (
          <p className="text-xs text-[hsl(var(--muted-foreground))]">
            Шаг {syncStatus.current_step} из {syncStatus.total_steps}
          </p>
        )}
      </div>
      {isError && syncStatus?.error && (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-400 text-left">
          {syncStatus.error}
        </div>
      )}
      {!isDone && !isError && (
        <div className="flex items-center justify-center gap-2 text-xs text-[hsl(var(--muted-foreground))]">
          <div className="h-2 w-2 rounded-full bg-[hsl(var(--primary))] animate-pulse" />
          Это может занять несколько минут
        </div>
      )}
      {(isDone || isError) && (
        <motion.button
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          onClick={onDashboard}
          className={`w-full rounded-lg py-3 text-sm font-semibold text-white transition-all hover:opacity-90 ${isDone ? 'bg-green-600 hover:bg-green-500' : 'bg-[hsl(var(--primary))]'}`}
        >
          Перейти в Dashboard →
        </motion.button>
      )}
    </div>
  )
}

/* ────────────────── Main page ────────────────── */
export default function OnboardingPage() {
  const navigate = useNavigate()
  const { shops, setShops } = useAuthStore()

  // Return-visit: if a shop is already syncing, show sync progress directly
  const syncingShop = shops.find((s) => s.status === 'syncing')

  const handleComplete = () => {
    navigate('/')
  }

  const handleDashboard = async () => {
    try {
      const { apiClient } = await import('@/api/client')
      const shopsResp = await apiClient.get('/shops')
      setShops(shopsResp.data)
    } catch {
      // proceed anyway
    }
    navigate('/')
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-[hsl(var(--background))] px-4">
      <div className="w-full max-w-2xl">
        {/* Logo */}
        <div className="text-center mb-6">
          <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-xl bg-[hsl(var(--primary))] text-lg font-bold text-white">
            MP
          </div>
          <h1 className="text-xl font-bold">MP-Control</h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            Подключите магазин для начала работы
          </p>
        </div>

        {syncingShop ? (
          /* Return-visit: shop is syncing — show progress */
          <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-6 md:p-8 shadow-xl">
            <ReturnSyncView
              shopId={syncingShop.id}
              shopName={syncingShop.name}
              marketplace={syncingShop.marketplace}
              onDashboard={handleDashboard}
            />
          </div>
        ) : (
          /* Fresh flow: show wizard */
          <ShopWizard
            subtitle="Подключите ваш первый магазин для начала работы"
            onComplete={handleComplete}
          />
        )}
      </div>
    </div>
  )
}
