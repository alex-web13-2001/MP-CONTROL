import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Store, Plus, Trash2, User, RefreshCw, X } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useAuthStore } from '@/stores/authStore'
import { useAppStore } from '@/stores/appStore'
import { apiClient } from '@/api/client'
import ShopWizard from '@/components/shops/ShopWizard'

export default function SettingsPage() {
  const { shops, setShops, user, logout } = useAuthStore()
  const { currentShop, setCurrentShop } = useAppStore()
  const [showWizard, setShowWizard] = useState(false)
  const [deletingId, setDeletingId] = useState<number | null>(null)
  const [confirmDeleteId, setConfirmDeleteId] = useState<number | null>(null)
  const [deleteError, setDeleteError] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)

  // Refresh shops from API on mount
  useEffect(() => {
    const refresh = async () => {
      try {
        const resp = await apiClient.get('/shops')
        setShops(resp.data)
      } catch {
        // ignore
      }
    }
    refresh()
  }, [setShops])

  const handleDeleteShop = async (shopId: number) => {
    setDeletingId(shopId)
    setDeleteError(null)
    try {
      await apiClient.delete(`/shops/${shopId}`)
      // Refresh shops
      const resp = await apiClient.get('/shops')
      setShops(resp.data)

      // If deleted current shop, auto-select another
      if (currentShop?.id === shopId) {
        const remaining = resp.data as any[]
        const next = remaining.find((s: any) => s.is_active) || remaining[0]
        if (next) {
          setCurrentShop({
            id: next.id,
            name: next.name,
            marketplace: next.marketplace as 'wildberries' | 'ozon',
            isActive: next.is_active,
          })
        }
      }
    } catch (err: any) {
      const status = err.response?.status
      const detail = err.response?.data?.detail || 'Ошибка удаления магазина'

      if (status === 404) {
        // Shop doesn't exist in DB — refresh list to remove ghost entry
        try {
          const resp = await apiClient.get('/shops')
          setShops(resp.data)
        } catch {
          // ignore refresh error
        }
      } else {
        setDeleteError(detail)
      }
    } finally {
      setDeletingId(null)
      setConfirmDeleteId(null)
    }
  }

  const handleRefreshShops = async () => {
    setRefreshing(true)
    try {
      const resp = await apiClient.get('/shops')
      setShops(resp.data)
    } catch {
      // ignore  
    } finally {
      setRefreshing(false)
    }
  }

  const handleWizardComplete = () => {
    setShowWizard(false)
    // Shops already refreshed inside ShopWizard
  }

  const getStatusBadge = (status?: string) => {
    switch (status) {
      case 'active':
        return (
          <span className="inline-flex items-center gap-1 rounded-full bg-green-500/15 px-2.5 py-0.5 text-xs font-medium text-green-400">
            <span className="h-1.5 w-1.5 rounded-full bg-green-400" />
            Активен
          </span>
        )
      case 'syncing':
        return (
          <span className="inline-flex items-center gap-1 rounded-full bg-blue-500/15 px-2.5 py-0.5 text-xs font-medium text-blue-400">
            <span className="h-1.5 w-1.5 rounded-full bg-blue-400 animate-pulse" />
            Синхронизация
          </span>
        )
      case 'error':
        return (
          <span className="inline-flex items-center gap-1 rounded-full bg-red-500/15 px-2.5 py-0.5 text-xs font-medium text-red-400">
            <span className="h-1.5 w-1.5 rounded-full bg-red-400" />
            Ошибка
          </span>
        )
      default:
        return (
          <span className="inline-flex items-center gap-1 rounded-full bg-[hsl(var(--muted))] px-2.5 py-0.5 text-xs font-medium text-[hsl(var(--muted-foreground))]">
            {status || 'Неизвестно'}
          </span>
        )
    }
  }

  const getMarketplaceBadge = (mp: string) => {
    const isWb = mp === 'wildberries'
    return (
      <span
        className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-semibold ${
          isWb
            ? 'bg-purple-500/15 text-purple-400'
            : 'bg-blue-500/15 text-blue-400'
        }`}
      >
        {isWb ? 'WB' : 'Ozon'}
      </span>
    )
  }

  return (
    <div className="space-y-8">
      {/* ── Page Header ── */}
      <motion.div
        initial={{ opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
        className="space-y-1"
      >
        <h1 className="text-3xl font-bold tracking-tight text-[hsl(var(--foreground))]">
          Настройки
        </h1>
        <p className="text-[15px] text-[hsl(var(--muted-foreground))]">
          Управление магазинами и настройками аккаунта
        </p>
      </motion.div>

      {/* ── Add Shop Wizard (overlay) ── */}
      <AnimatePresence>
        {showWizard && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 20 }}
            transition={{ duration: 0.3 }}
          >
            <Card className="relative border-[hsl(var(--primary))/30] shadow-xl shadow-[hsl(var(--primary))/5]">
              <button
                onClick={() => setShowWizard(false)}
                className="absolute right-4 top-4 rounded-lg p-1.5 text-[hsl(var(--muted-foreground))]
                  transition-colors hover:bg-[hsl(var(--muted))] hover:text-[hsl(var(--foreground))]"
              >
                <X className="h-5 w-5" />
              </button>
              <CardContent className="pt-6">
                <ShopWizard
                  subtitle="Подключите дополнительный магазин"
                  onCancel={() => setShowWizard(false)}
                  onComplete={handleWizardComplete}
                />
              </CardContent>
            </Card>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── Shops Section ── */}
      {!showWizard && (
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, delay: 0.1 }}
        >
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-4">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-[hsl(var(--primary))/10]">
                  <Store className="h-5 w-5 text-[hsl(var(--primary))]" />
                </div>
                <div>
                  <CardTitle className="text-lg">Магазины</CardTitle>
                  <p className="text-sm text-[hsl(var(--muted-foreground))]">
                    {shops.length} {shops.length === 1 ? 'магазин' : shops.length < 5 ? 'магазина' : 'магазинов'} подключено
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={handleRefreshShops}
                  disabled={refreshing}
                  className="flex h-9 items-center gap-2 rounded-lg border border-[hsl(var(--border))] px-3 text-sm
                    text-[hsl(var(--muted-foreground))] transition-all hover:bg-[hsl(var(--muted))]
                    hover:text-[hsl(var(--foreground))] disabled:opacity-50"
                >
                  <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
                  <span className="hidden sm:inline">Обновить</span>
                </button>
                <button
                  onClick={() => setShowWizard(true)}
                  className="flex h-9 items-center gap-2 rounded-lg bg-[hsl(var(--primary))] px-4 text-sm
                    font-semibold text-white transition-all hover:opacity-90 shadow-sm"
                >
                  <Plus className="h-4 w-4" />
                  <span>Добавить магазин</span>
                </button>
              </div>
            </CardHeader>
            <CardContent className="space-y-3">
              {deleteError && (
                <div className="flex items-center justify-between rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400">
                  <span>{deleteError}</span>
                  <button
                    onClick={() => setDeleteError(null)}
                    className="ml-2 text-red-400 hover:text-red-300 transition-colors"
                  >
                    ✕
                  </button>
                </div>
              )}
              {shops.length === 0 ? (
                <div className="flex flex-col items-center gap-4 py-12 text-center">
                  <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-[hsl(var(--muted))]">
                    <Store className="h-8 w-8 text-[hsl(var(--muted-foreground))]" />
                  </div>
                  <div>
                    <p className="text-lg font-semibold">Нет подключённых магазинов</p>
                    <p className="text-sm text-[hsl(var(--muted-foreground))] mt-1">
                      Добавьте ваш первый магазин для начала работы
                    </p>
                  </div>
                  <button
                    onClick={() => setShowWizard(true)}
                    className="flex items-center gap-2 rounded-lg bg-[hsl(var(--primary))] px-6 py-2.5 text-sm
                      font-semibold text-white transition-all hover:opacity-90"
                  >
                    <Plus className="h-4 w-4" />
                    Добавить магазин
                  </button>
                </div>
              ) : (
                shops.map((shop) => (
                  <motion.div
                    key={shop.id}
                    layout
                    className={`
                      flex items-center justify-between rounded-xl border p-4 transition-all duration-200
                      ${currentShop?.id === shop.id
                        ? 'border-[hsl(var(--primary))/40] bg-[hsl(var(--primary))/5]'
                        : 'border-[hsl(var(--border))] hover:border-[hsl(var(--border))]/80 hover:bg-[hsl(var(--muted))/30]'
                      }
                    `}
                  >
                    <div className="flex items-center gap-4">
                      <div
                        className={`flex h-11 w-11 items-center justify-center rounded-xl text-white text-sm font-bold shadow-sm ${
                          shop.marketplace === 'wildberries'
                            ? 'bg-gradient-to-br from-purple-500 to-purple-700'
                            : 'bg-gradient-to-br from-blue-500 to-blue-700'
                        }`}
                      >
                        {shop.marketplace === 'wildberries' ? 'WB' : 'Oz'}
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <p className="font-semibold text-[hsl(var(--foreground))]">
                            {shop.name}
                          </p>
                          {getMarketplaceBadge(shop.marketplace)}
                          {currentShop?.id === shop.id && (
                            <span className="text-[10px] font-medium uppercase tracking-wider text-[hsl(var(--primary))]">
                              Текущий
                            </span>
                          )}
                        </div>
                        <div className="flex items-center gap-3 mt-1">
                          {getStatusBadge(shop.status)}
                          <span className="text-xs text-[hsl(var(--muted-foreground))]">
                            ID: {shop.id}
                          </span>
                        </div>
                      </div>
                    </div>

                    <div className="flex items-center gap-2">
                      {currentShop?.id !== shop.id && shop.status === 'active' && (
                        <button
                          onClick={() =>
                            setCurrentShop({
                              id: shop.id,
                              name: shop.name,
                              marketplace: shop.marketplace,
                              isActive: shop.isActive,
                            })
                          }
                          className="rounded-lg border border-[hsl(var(--border))] px-3 py-1.5 text-xs font-medium
                            text-[hsl(var(--muted-foreground))] transition-all hover:bg-[hsl(var(--muted))]
                            hover:text-[hsl(var(--foreground))]"
                        >
                          Выбрать
                        </button>
                      )}
                      {confirmDeleteId === shop.id ? (
                        <div className="flex items-center gap-1">
                          <button
                            onClick={() => handleDeleteShop(shop.id)}
                            disabled={deletingId === shop.id}
                            className="rounded-lg bg-red-600 px-3 py-1.5 text-xs font-semibold text-white
                              transition-all hover:bg-red-500 disabled:opacity-50"
                          >
                            {deletingId === shop.id ? '...' : 'Да, удалить'}
                          </button>
                          <button
                            onClick={() => setConfirmDeleteId(null)}
                            className="rounded-lg border border-[hsl(var(--border))] px-3 py-1.5 text-xs
                              text-[hsl(var(--muted-foreground))] transition-colors hover:bg-[hsl(var(--muted))]"
                          >
                            Отмена
                          </button>
                        </div>
                      ) : (
                        <button
                          onClick={() => setConfirmDeleteId(shop.id)}
                          className="flex h-8 w-8 items-center justify-center rounded-lg text-[hsl(var(--muted-foreground))]
                            transition-all hover:bg-red-500/10 hover:text-red-400"
                          title="Удалить магазин"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                      )}
                    </div>
                  </motion.div>
                ))
              )}
            </CardContent>
          </Card>
        </motion.div>
      )}

      {/* ── Profile Section ── */}
      {!showWizard && (
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, delay: 0.2 }}
        >
          <Card>
            <CardHeader>
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-[hsl(var(--primary))/10]">
                  <User className="h-5 w-5 text-[hsl(var(--primary))]" />
                </div>
                <CardTitle className="text-lg">Профиль</CardTitle>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                <div className="space-y-1.5">
                  <label className="text-xs font-medium uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
                    Имя
                  </label>
                  <p className="text-sm font-medium text-[hsl(var(--foreground))]">
                    {user?.name || '—'}
                  </p>
                </div>
                <div className="space-y-1.5">
                  <label className="text-xs font-medium uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
                    Email
                  </label>
                  <p className="text-sm font-medium text-[hsl(var(--foreground))]">
                    {user?.email || '—'}
                  </p>
                </div>
              </div>

              <div className="pt-2 border-t border-[hsl(var(--border))]">
                <button
                  onClick={logout}
                  className="rounded-lg border border-red-500/30 px-4 py-2 text-sm font-medium
                    text-red-400 transition-all hover:bg-red-500/10 hover:border-red-500/50"
                >
                  Выйти из аккаунта
                </button>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      )}
    </div>
  )
}
