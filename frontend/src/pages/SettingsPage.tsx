import { useState, useEffect, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Store, Plus, Trash2, User, RefreshCw, X, KeyRound, Save, Loader2 } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { useAuthStore } from '@/stores/authStore'
import { useAppStore } from '@/stores/appStore'
import { apiClient } from '@/api/client'
import ShopWizard from '@/components/shops/ShopWizard'
import { getSyncStatusApi, type SyncStatusResponse } from '@/api/auth'

/* ── Helper: format elapsed ────────────────────────────── */
function formatElapsed(sec: number | null | undefined): string | null {
  if (!sec || sec < 5) return null
  const m = Math.floor(sec / 60)
  const s = sec % 60
  if (m === 0) return `${s} сек`
  return `${m} мин ${s.toString().padStart(2, '0')} сек`
}

/* ── Inline sync progress for shop card ────────────────── */
function SyncProgressInline({ shopId }: { shopId: number }) {
  const [sync, setSync] = useState<SyncStatusResponse | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    const poll = async () => {
      try {
        const s = await getSyncStatusApi(shopId)
        setSync(s)
        if (s.status === 'done' || s.status === 'done_with_errors' || s.status === 'error') {
          if (intervalRef.current) {
            clearInterval(intervalRef.current)
            intervalRef.current = null
          }
        }
      } catch { /* ignore */ }
    }
    poll()
    intervalRef.current = setInterval(poll, 3000)
    return () => { if (intervalRef.current) clearInterval(intervalRef.current) }
  }, [shopId])

  if (!sync || sync.status === 'done' || sync.status === 'done_with_errors') return null

  const rawPercent = sync.percent ?? 0
  const percent = Math.min(rawPercent, 99)
  const elapsed = formatElapsed(sync.elapsed_sec)

  return (
    <motion.div
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: 'auto' }}
      exit={{ opacity: 0, height: 0 }}
      transition={{ duration: 0.25 }}
      className="overflow-hidden"
    >
      <div className="rounded-xl border border-blue-500/20 bg-blue-500/5 p-3 space-y-2">
        {/* Step name + percent */}
        <div className="flex items-center justify-between text-xs">
          <span className="text-[hsl(var(--muted-foreground))] truncate pr-2">
            {sync.step_name || 'Загрузка...'}
          </span>
          <span className="font-mono font-semibold text-[hsl(var(--foreground))] shrink-0">
            {percent}%
          </span>
        </div>
        {/* Progress bar */}
        <div className="h-1.5 w-full rounded-full bg-[hsl(var(--muted))] overflow-hidden">
          <motion.div
            className="h-full rounded-full bg-blue-500"
            initial={{ width: 0 }}
            animate={{ width: `${percent}%` }}
            transition={{ duration: 0.5, ease: 'easeOut' }}
          />
        </div>
        {/* Details row */}
        <div className="flex items-center gap-2 text-[10px] text-[hsl(var(--muted-foreground))]">
          {sync.sub_progress && (
            <span className="italic">{sync.sub_progress}</span>
          )}
          {sync.total_steps > 0 && (
            <span>Шаг {sync.current_step}/{sync.total_steps}</span>
          )}
          {sync.eta_message && (
            <>
              <span className="text-[hsl(var(--border))]">·</span>
              <span>Осталось {sync.eta_message}</span>
            </>
          )}
          {elapsed && (
            <>
              <span className="text-[hsl(var(--border))]">·</span>
              <span>Прошло: {elapsed}</span>
            </>
          )}
        </div>
      </div>
    </motion.div>
  )
}

export default function SettingsPage() {
  const { shops, setShops, user, logout } = useAuthStore()
  const { currentShop, setCurrentShop } = useAppStore()
  const [showWizard, setShowWizard] = useState(false)
  const [deletingId, setDeletingId] = useState<number | null>(null)
  const [confirmDeleteId, setConfirmDeleteId] = useState<number | null>(null)
  const [deleteError, setDeleteError] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)

  // Key editing state
  const [editingShopId, setEditingShopId] = useState<number | null>(null)
  const [keyForm, setKeyForm] = useState({ apiKey: '', clientId: '', perfClientId: '', perfClientSecret: '' })
  const [keyError, setKeyError] = useState<string | null>(null)
  const [keySuccess, setKeySuccess] = useState<string | null>(null)
  const [savingKeys, setSavingKeys] = useState(false)

  const startEditKeys = (shop: typeof shops[0]) => {
    setEditingShopId(shop.id)
    setKeyForm({ apiKey: '', clientId: '', perfClientId: '', perfClientSecret: '' })
    setKeyError(null)
    setKeySuccess(null)
  }

  const handleUpdateKeys = async (shopId: number, marketplace: string) => {
    if (!keyForm.apiKey.trim()) {
      setKeyError('Введите API-ключ')
      return
    }
    setSavingKeys(true)
    setKeyError(null)
    setKeySuccess(null)
    try {
      const payload: Record<string, string> = { api_key: keyForm.apiKey }
      if (marketplace === 'ozon') {
        if (keyForm.clientId) payload.client_id = keyForm.clientId
        if (keyForm.perfClientId) payload.perf_client_id = keyForm.perfClientId
        if (keyForm.perfClientSecret) payload.perf_client_secret = keyForm.perfClientSecret
      }
      await apiClient.patch(`/shops/${shopId}/keys`, payload)
      setKeySuccess('Ключ успешно обновлён')
      // Refresh shops list
      const resp = await apiClient.get('/shops')
      setShops(resp.data)
      setTimeout(() => {
        setEditingShopId(null)
        setKeySuccess(null)
      }, 1500)
    } catch (err: any) {
      const detail = err.response?.data?.detail || 'Ошибка обновления ключа'
      setKeyError(detail)
    } finally {
      setSavingKeys(false)
    }
  }

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
              <Button variant="ghost" size="icon-sm" onClick={() => setShowWizard(false)} className="absolute right-4 top-4">
                <X className="h-5 w-5" />
              </Button>
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
                <Button variant="outline" size="sm" onClick={handleRefreshShops} disabled={refreshing}>
                  <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
                  <span className="hidden sm:inline">Обновить</span>
                </Button>
                <Button onClick={() => setShowWizard(true)} size="sm">
                  <Plus className="h-4 w-4" />
                  Добавить магазин
                </Button>
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
                  <Button onClick={() => setShowWizard(true)}>
                    <Plus className="h-4 w-4" />
                    Добавить магазин
                  </Button>
                </div>
              ) : (
                shops.map((shop) => (
                  <div key={shop.id} className="space-y-2">
                  <motion.div
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
                        <Button
                          variant="outline"
                          size="xs"
                          onClick={() =>
                            setCurrentShop({
                              id: shop.id,
                              name: shop.name,
                              marketplace: shop.marketplace,
                              isActive: shop.isActive,
                            })
                          }
                        >
                          Выбрать
                        </Button>
                      )}
                      {confirmDeleteId === shop.id ? (
                        <div className="flex items-center gap-1">
                          <Button variant="destructive" size="xs" onClick={() => handleDeleteShop(shop.id)} disabled={deletingId === shop.id}>
                            {deletingId === shop.id ? '...' : 'Да, удалить'}
                          </Button>
                          <Button variant="outline" size="xs" onClick={() => setConfirmDeleteId(null)}>
                            Отмена
                          </Button>
                        </div>
                      ) : (
                        <div className="flex items-center gap-1">
                          <Button variant="ghost" size="icon-sm" onClick={() => startEditKeys(shop)} title="Обновить API-ключ"
                            className="hover:bg-amber-500/10 hover:text-amber-400">
                            <KeyRound className="h-4 w-4" />
                          </Button>
                          <Button variant="ghost" size="icon-sm" onClick={() => setConfirmDeleteId(shop.id)} title="Удалить магазин"
                            className="hover:bg-red-500/10 hover:text-red-400">
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                      )}
                    </div>
                  </motion.div>

                  {/* ── Inline Sync Progress (for syncing shops) ── */}
                  <AnimatePresence>
                    {shop.status === 'syncing' && (
                      <SyncProgressInline shopId={shop.id} />
                    )}
                  </AnimatePresence>

                  {/* ── Inline Key Edit Form ── */}
                  <AnimatePresence>
                    {editingShopId === shop.id && (
                      <motion.div
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: 'auto' }}
                        exit={{ opacity: 0, height: 0 }}
                        transition={{ duration: 0.2 }}
                        className="overflow-hidden"
                      >
                        <div className="rounded-xl border border-amber-500/30 bg-amber-500/5 p-4 space-y-3">
                          <div className="flex items-center justify-between">
                            <p className="text-sm font-semibold text-[hsl(var(--foreground))]">
                              🔑 Обновить API-ключ — {shop.name}
                            </p>
                            <Button variant="ghost" size="icon-sm" onClick={() => setEditingShopId(null)}>
                              <X className="h-4 w-4" />
                            </Button>
                          </div>

                          <div className="space-y-2">
                            <div>
                              <label className="text-xs font-medium text-[hsl(var(--muted-foreground))] mb-1 block">API-ключ *</label>
                              <input
                                type="password"
                                value={keyForm.apiKey}
                                onChange={(e) => setKeyForm({ ...keyForm, apiKey: e.target.value })}
                                placeholder="Вставьте новый API-ключ"
                                className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-3 py-2 text-sm
                                  text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))]/50
                                  focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary))]/30 focus:border-[hsl(var(--primary))]/50"
                              />
                            </div>

                            {shop.marketplace === 'ozon' && (
                              <>
                                <div>
                                  <label className="text-xs font-medium text-[hsl(var(--muted-foreground))] mb-1 block">Client-Id</label>
                                  <input
                                    type="text"
                                    value={keyForm.clientId}
                                    onChange={(e) => setKeyForm({ ...keyForm, clientId: e.target.value })}
                                    placeholder="Оставьте пустым если не изменился"
                                    className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-3 py-2 text-sm
                                      text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))]/50
                                      focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary))]/30 focus:border-[hsl(var(--primary))]/50"
                                  />
                                </div>
                                <div className="grid grid-cols-2 gap-2">
                                  <div>
                                    <label className="text-xs font-medium text-[hsl(var(--muted-foreground))] mb-1 block">Perf Client-Id</label>
                                    <input
                                      type="text"
                                      value={keyForm.perfClientId}
                                      onChange={(e) => setKeyForm({ ...keyForm, perfClientId: e.target.value })}
                                      placeholder="Опционально"
                                      className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-3 py-2 text-sm
                                        text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))]/50
                                        focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary))]/30 focus:border-[hsl(var(--primary))]/50"
                                    />
                                  </div>
                                  <div>
                                    <label className="text-xs font-medium text-[hsl(var(--muted-foreground))] mb-1 block">Perf Secret</label>
                                    <input
                                      type="password"
                                      value={keyForm.perfClientSecret}
                                      onChange={(e) => setKeyForm({ ...keyForm, perfClientSecret: e.target.value })}
                                      placeholder="Опционально"
                                      className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-3 py-2 text-sm
                                        text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))]/50
                                        focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary))]/30 focus:border-[hsl(var(--primary))]/50"
                                    />
                                  </div>
                                </div>
                              </>
                            )}
                          </div>

                          {keyError && (
                            <div className="rounded-lg border border-red-500/30 bg-red-500/10 p-2.5 text-xs text-red-400">
                              {keyError}
                            </div>
                          )}
                          {keySuccess && (
                            <div className="rounded-lg border border-green-500/30 bg-green-500/10 p-2.5 text-xs text-green-400">
                              ✓ {keySuccess}
                            </div>
                          )}

                          <div className="flex items-center gap-2 pt-1">
                            <Button size="sm" onClick={() => handleUpdateKeys(shop.id, shop.marketplace)} disabled={savingKeys}
                              className="bg-amber-600 hover:bg-amber-500">
                              {savingKeys ? (
                                <><Loader2 className="h-3.5 w-3.5 animate-spin" /> Проверяем...</>
                              ) : (
                                <><Save className="h-3.5 w-3.5" /> Сохранить</>
                              )}
                            </Button>
                            <Button variant="outline" size="sm" onClick={() => setEditingShopId(null)}>
                              Отмена
                            </Button>
                          </div>
                        </div>
                      </motion.div>
                    )}
                  </AnimatePresence>
                  </div>
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
                <Button variant="danger-ghost" size="sm" onClick={logout}>
                  Выйти из аккаунта
                </Button>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      )}
    </div>
  )
}
