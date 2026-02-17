import { useState, useEffect, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { apiClient } from '@/api/client'
import { getSyncStatusApi, SyncStatusResponse } from '@/api/auth'
import { useAuthStore } from '@/stores/authStore'
import { useAppStore } from '@/stores/appStore'

/* ────────────────── Types ────────────────── */
export type Marketplace = 'wildberries' | 'ozon' | null

interface ValidationResult {
  valid: boolean
  seller_valid?: boolean | null
  perf_valid?: boolean | null
  message: string
  shop_name?: string | null
  warnings?: string[] | null
}

/* ────────────────── Constants ────────────────── */
const WB_PERMISSIONS = [
  { name: 'Контент', access: 'Чтение', required: true },
  { name: 'Аналитика', access: 'Чтение', required: true },
  { name: 'Статистика', access: 'Чтение', required: true },
  { name: 'Цены и скидки', access: 'Чтение', required: true },
  { name: 'Маркетплейс', access: 'Чтение', required: true },
  { name: 'Реклама', access: 'Чтение + Управление', required: true },
  { name: 'Вопросы и отзывы', access: 'Чтение', required: false },
]

const OZON_SELLER_PERMISSIONS = [
  { name: 'Товары', access: 'Чтение', required: true },
  { name: 'Аналитические отчёты', access: 'Чтение', required: true },
  { name: 'Финансовые отчёты', access: 'Чтение', required: true },
  { name: 'Цены и остатки', access: 'Чтение', required: true },
  { name: 'Склады FBO/FBS', access: 'Чтение', required: true },
]

const OZON_PERF_PERMISSIONS = [
  { name: 'Управление кампаниями', access: 'Полный доступ', required: true },
  { name: 'Статистика рекламы', access: 'Чтение', required: true },
]

/* ────────────────── Step indicator ────────────────── */
const steps = ['Маркетплейс', 'API ключи', 'Проверка', 'Загрузка']

function StepIndicator({ current }: { current: number }) {
  return (
    <div className="flex items-center justify-center gap-2 mb-8">
      {steps.map((label, i) => (
        <div key={label} className="flex items-center gap-2">
          <div
            className={`
              flex h-8 w-8 items-center justify-center rounded-full text-sm font-semibold
              transition-all duration-300
              ${i < current
                ? 'bg-green-500 text-white'
                : i === current
                  ? 'bg-[hsl(var(--primary))] text-white shadow-lg shadow-[hsl(var(--primary))/30]'
                  : 'bg-[hsl(var(--muted))] text-[hsl(var(--muted-foreground))]'
              }
            `}
          >
            {i < current ? '✓' : i + 1}
          </div>
          {i < steps.length - 1 && (
            <div
              className={`
                h-0.5 w-8 rounded transition-colors duration-300
                ${i < current ? 'bg-green-500' : 'bg-[hsl(var(--muted))]'}
              `}
            />
          )}
        </div>
      ))}
    </div>
  )
}

/* ────────────────── Step 1: Marketplace ────────────────── */
function StepMarketplace({
  onSelect,
  subtitle,
}: {
  onSelect: (mp: Marketplace) => void
  subtitle?: string
}) {
  return (
    <div className="space-y-6 text-center">
      <h2 className="text-2xl font-bold">Выберите маркетплейс</h2>
      <p className="text-[hsl(var(--muted-foreground))]">
        {subtitle || 'Подключите ваш первый магазин для начала работы'}
      </p>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 max-w-lg mx-auto mt-8">
        {/* WB Card */}
        <button
          onClick={() => onSelect('wildberries')}
          className="group relative flex flex-col items-center gap-3 rounded-xl border border-[hsl(var(--border))]
            bg-[hsl(var(--card))] p-6 transition-all duration-200
            hover:border-purple-500 hover:shadow-lg hover:shadow-purple-500/10
            hover:-translate-y-1 cursor-pointer"
        >
          <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-gradient-to-br from-purple-500 to-purple-700 text-white text-2xl font-bold shadow-lg">
            WB
          </div>
          <span className="text-lg font-semibold">Wildberries</span>
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            1 API ключ
          </span>
        </button>

        {/* Ozon Card */}
        <button
          onClick={() => onSelect('ozon')}
          className="group relative flex flex-col items-center gap-3 rounded-xl border border-[hsl(var(--border))]
            bg-[hsl(var(--card))] p-6 transition-all duration-200
            hover:border-blue-500 hover:shadow-lg hover:shadow-blue-500/10
            hover:-translate-y-1 cursor-pointer"
        >
          <div className="flex h-16 w-16 items-center justify-center rounded-2xl bg-gradient-to-br from-blue-500 to-blue-700 text-white text-2xl font-bold shadow-lg">
            Oz
          </div>
          <span className="text-lg font-semibold">Ozon</span>
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            Seller API + Performance API
          </span>
        </button>
      </div>
    </div>
  )
}

/* ────────────────── Permission list component ────────────────── */
function PermissionList({
  title,
  permissions,
}: {
  title: string
  permissions: { name: string; access: string; required: boolean }[]
}) {
  return (
    <div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))]/50 p-4">
      <h4 className="text-sm font-semibold text-[hsl(var(--muted-foreground))] mb-3">
        {title}
      </h4>
      <div className="space-y-2">
        {permissions.map((p) => (
          <div key={p.name} className="flex items-center justify-between text-sm">
            <span className="flex items-center gap-2">
              <span className={p.required ? 'text-amber-400' : 'text-green-400'}>
                {p.required ? '⚡' : '✅'}
              </span>
              {p.name}
            </span>
            <span className="text-[hsl(var(--muted-foreground))] text-xs bg-[hsl(var(--muted))] px-2 py-0.5 rounded">
              {p.access}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

/* ────────────────── Step 2: API Keys ────────────────── */
function StepApiKeys({
  marketplace,
  onSubmit,
  onBack,
}: {
  marketplace: Marketplace
  onSubmit: (data: {
    apiKey: string
    clientId?: string
    perfClientId?: string
    perfClientSecret?: string
    shopName: string
  }) => void
  onBack: () => void
}) {
  const [shopName, setShopName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [clientId, setClientId] = useState('')
  const [perfClientId, setPerfClientId] = useState('')
  const [perfClientSecret, setPerfClientSecret] = useState('')

  const isWb = marketplace === 'wildberries'
  const canSubmit = isWb
    ? shopName.trim() && apiKey.trim()
    : shopName.trim() && apiKey.trim() && clientId.trim() && perfClientId.trim() && perfClientSecret.trim()

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!canSubmit) return
    onSubmit({
      apiKey: apiKey.trim(),
      clientId: clientId.trim() || undefined,
      perfClientId: perfClientId.trim() || undefined,
      perfClientSecret: perfClientSecret.trim() || undefined,
      shopName: shopName.trim(),
    })
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-6 max-w-lg mx-auto">
      <div className="flex items-center gap-3 mb-2">
        <button
          type="button"
          onClick={onBack}
          className="text-[hsl(var(--muted-foreground))] hover:text-white transition-colors"
        >
          ← Назад
        </button>
        <h2 className="text-2xl font-bold">
          {isWb ? 'Wildberries API' : 'Ozon API'}
        </h2>
      </div>

      {/* Shop name */}
      <div>
        <label className="block text-sm font-medium mb-1.5">Название магазина</label>
        <input
          value={shopName}
          onChange={(e) => setShopName(e.target.value)}
          placeholder="Например: Мой магазин"
          className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--input))]
            px-3 py-2.5 text-sm focus:outline-none focus:ring-2
            focus:ring-[hsl(var(--primary))] transition-all"
        />
      </div>

      {isWb ? (
        <>
          {/* WB: single key */}
          <div>
            <label className="block text-sm font-medium mb-1.5">API Токен</label>
            <input
              type="password"
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              placeholder="Вставьте токен из личного кабинета WB"
              className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--input))]
                px-3 py-2.5 text-sm font-mono focus:outline-none focus:ring-2
                focus:ring-[hsl(var(--primary))] transition-all"
            />
            <p className="text-xs text-[hsl(var(--muted-foreground))] mt-1">
              Настройки → Доступ к новому API → Создать токен
            </p>
          </div>

          <PermissionList
            title="⚙️ Необходимые права ключа"
            permissions={WB_PERMISSIONS}
          />
        </>
      ) : (
        <>
          {/* Ozon Seller API */}
          <div className="rounded-xl border border-blue-500/30 bg-blue-500/5 p-4 space-y-3">
            <h3 className="text-sm font-semibold flex items-center gap-2">
              <span className="flex h-6 w-6 items-center justify-center rounded bg-blue-500/20 text-blue-400 text-xs">1</span>
              Seller API
            </h3>
            <div className="grid grid-cols-1 gap-3">
              <div>
                <label className="block text-xs font-medium mb-1 text-[hsl(var(--muted-foreground))]">
                  Client-Id
                </label>
                <input
                  value={clientId}
                  onChange={(e) => setClientId(e.target.value)}
                  placeholder="Числовой ID продавца"
                  className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--input))]
                    px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2
                    focus:ring-blue-500/50 transition-all"
                />
              </div>
              <div>
                <label className="block text-xs font-medium mb-1 text-[hsl(var(--muted-foreground))]">
                  Api-Key
                </label>
                <input
                  type="password"
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                  placeholder="Ключ из раздела API в кабинете Ozon"
                  className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--input))]
                    px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2
                    focus:ring-blue-500/50 transition-all"
                />
              </div>
            </div>
          </div>

          {/* Ozon Performance API */}
          <div className="rounded-xl border border-purple-500/30 bg-purple-500/5 p-4 space-y-3">
            <h3 className="text-sm font-semibold flex items-center gap-2">
              <span className="flex h-6 w-6 items-center justify-center rounded bg-purple-500/20 text-purple-400 text-xs">2</span>
              Performance API (реклама)
            </h3>
            <div className="grid grid-cols-1 gap-3">
              <div>
                <label className="block text-xs font-medium mb-1 text-[hsl(var(--muted-foreground))]">
                  Client-Id
                </label>
                <input
                  value={perfClientId}
                  onChange={(e) => setPerfClientId(e.target.value)}
                  placeholder="Client-Id для Performance API"
                  className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--input))]
                    px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2
                    focus:ring-purple-500/50 transition-all"
                />
              </div>
              <div>
                <label className="block text-xs font-medium mb-1 text-[hsl(var(--muted-foreground))]">
                  Client-Secret
                </label>
                <input
                  type="password"
                  value={perfClientSecret}
                  onChange={(e) => setPerfClientSecret(e.target.value)}
                  placeholder="Client-Secret для Performance API"
                  className="w-full rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--input))]
                    px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2
                    focus:ring-purple-500/50 transition-all"
                />
              </div>
            </div>
            <p className="text-xs text-[hsl(var(--muted-foreground))]">
              Performance API → Настройки аккаунта → API ключи
            </p>
          </div>

          <div className="grid gap-3 grid-cols-1 sm:grid-cols-2">
            <PermissionList
              title="⚙️ Права Seller API"
              permissions={OZON_SELLER_PERMISSIONS}
            />
            <PermissionList
              title="📊 Права Performance API"
              permissions={OZON_PERF_PERMISSIONS}
            />
          </div>
        </>
      )}

      <button
        type="submit"
        disabled={!canSubmit}
        className="w-full rounded-lg bg-[hsl(var(--primary))] py-3 text-sm font-semibold
          text-white transition-all hover:opacity-90 disabled:opacity-40 disabled:cursor-not-allowed"
      >
        Проверить ключ →
      </button>
    </form>
  )
}

/* ────────────────── Step 3: Validation ────────────────── */
function StepValidation({
  marketplace,
  result,
  isLoading,
  onRetry,
  onContinue,
  onBack,
}: {
  marketplace: Marketplace
  result: ValidationResult | null
  isLoading: boolean
  onRetry: () => void
  onContinue: () => void
  onBack: () => void
}) {
  return (
    <div className="max-w-lg mx-auto space-y-6 text-center">
      <h2 className="text-2xl font-bold">Проверка API ключей</h2>

      {isLoading ? (
        <div className="flex flex-col items-center gap-4 py-12">
          <div className="h-12 w-12 rounded-full border-4 border-[hsl(var(--primary))] border-t-transparent animate-spin" />
          <p className="text-[hsl(var(--muted-foreground))]">
            Проверяем подключение к {marketplace === 'wildberries' ? 'Wildberries' : 'Ozon'}...
          </p>
        </div>
      ) : result ? (
        <div className="space-y-4">
          <div
            className={`
              rounded-xl border p-6
              ${result.valid
                ? 'border-green-500/30 bg-green-500/5'
                : 'border-red-500/30 bg-red-500/5'
              }
            `}
          >
            <div className="text-4xl mb-3">
              {result.valid ? '✅' : '❌'}
            </div>
            <p className="font-semibold text-lg mb-2">
              {result.valid ? 'Ключи валидны!' : 'Ошибка валидации'}
            </p>
            <p className="text-sm text-[hsl(var(--muted-foreground))]">
              {result.message}
            </p>

            {marketplace === 'ozon' && (
              <div className="flex items-center justify-center gap-6 mt-4 text-sm">
                <span className="flex items-center gap-1">
                  {result.seller_valid ? '✅' : '❌'} Seller API
                </span>
                {result.perf_valid !== null && result.perf_valid !== undefined && (
                  <span className="flex items-center gap-1">
                    {result.perf_valid ? '✅' : '❌'} Performance API
                  </span>
                )}
              </div>
            )}

            {/* WB permission warnings */}
            {result.warnings && result.warnings.length > 0 && (
              <div className="mt-4 rounded-lg border border-amber-500/30 bg-amber-500/10 p-4 text-left">
                <p className="text-sm font-semibold text-amber-400 mb-2">
                  ⚠️ Не хватает прав доступа:
                </p>
                <ul className="space-y-1">
                  {result.warnings.map((w, i) => (
                    <li key={i} className="text-sm text-amber-300/90">
                      {w}
                    </li>
                  ))}
                </ul>
                <p className="text-xs text-[hsl(var(--muted-foreground))] mt-3">
                  Добавьте недостающие права в настройках API-ключа в личном кабинете WB.
                  Без них часть данных не будет загружена.
                </p>
              </div>
            )}
          </div>

          <div className="flex gap-3">
            <button
              onClick={onBack}
              className="flex-1 rounded-lg border border-[hsl(var(--border))] py-2.5 text-sm
                font-medium transition-all hover:bg-[hsl(var(--muted))]"
            >
              ← Изменить ключи
            </button>
            {result.valid ? (
              <button
                onClick={onContinue}
                className="flex-1 rounded-lg bg-green-600 py-2.5 text-sm font-semibold
                  text-white transition-all hover:bg-green-500"
              >
                Подключить магазин →
              </button>
            ) : (
              <button
                onClick={onRetry}
                className="flex-1 rounded-lg bg-[hsl(var(--primary))] py-2.5 text-sm font-semibold
                  text-white transition-all hover:opacity-90"
              >
                Попробовать снова
              </button>
            )}
          </div>
        </div>
      ) : null}
    </div>
  )
}

/* ────────────────── Step 4: Syncing with progress ────────────────── */
function StepSyncing({
  shopId,
  shopName,
  marketplace,
  onDone,
}: {
  shopId: number | null
  shopName: string
  marketplace: Marketplace
  onDone: () => void
}) {
  const [syncStatus, setSyncStatus] = useState<SyncStatusResponse | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    if (!shopId) return

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
        // ignore polling errors
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
      <motion.div
        initial={{ scale: 0 }}
        animate={{ scale: 1 }}
        transition={{ type: 'spring', damping: 15 }}
        className="text-5xl"
      >
        {isDone ? '🎉' : isError ? '⚠️' : '📦'}
      </motion.div>

      <h2 className="text-2xl font-bold">
        {isDone
          ? 'Данные загружены!'
          : isError
            ? 'Загрузка завершена с ошибками'
            : 'Загрузка данных...'}
      </h2>

      <p className="text-[hsl(var(--muted-foreground))]">
        <strong>{shopName}</strong> ({marketplace === 'wildberries' ? 'Wildberries' : 'Ozon'})
      </p>

      {/* Progress bar */}
      <div className="space-y-3">
        <div className="h-3 w-full rounded-full bg-[hsl(var(--muted))] overflow-hidden">
          <motion.div
            className={`h-full rounded-full transition-all duration-500 ${
              isDone
                ? 'bg-green-500'
                : isError
                  ? 'bg-amber-500'
                  : 'bg-[hsl(var(--primary))]'
            }`}
            initial={{ width: 0 }}
            animate={{ width: `${percent}%` }}
            transition={{ duration: 0.5, ease: 'easeOut' }}
          />
        </div>

        <div className="flex items-center justify-between text-sm">
          <span className="text-[hsl(var(--muted-foreground))]">
            {syncStatus?.step_name || 'Ожидание начала загрузки...'}
          </span>
          <span className="font-mono font-semibold">
            {percent}%
          </span>
        </div>

        {syncStatus && syncStatus.total_steps > 0 && (
          <p className="text-xs text-[hsl(var(--muted-foreground))]">
            Шаг {syncStatus.current_step} из {syncStatus.total_steps}
          </p>
        )}
      </div>

      {/* Error details */}
      {isError && syncStatus?.error && (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-400 text-left">
          {syncStatus.error}
        </div>
      )}

      {/* Loading animation */}
      {!isDone && !isError && (
        <div className="flex items-center justify-center gap-2 text-xs text-[hsl(var(--muted-foreground))]">
          <div className="h-2 w-2 rounded-full bg-[hsl(var(--primary))] animate-pulse" />
          Это может занять несколько минут
        </div>
      )}

      {/* Done button */}
      {(isDone || isError) && (
        <motion.button
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          onClick={onDone}
          className={`w-full rounded-lg py-3 text-sm font-semibold text-white transition-all hover:opacity-90 ${
            isDone ? 'bg-green-600 hover:bg-green-500' : 'bg-[hsl(var(--primary))]'
          }`}
        >
          Готово →
        </motion.button>
      )}
    </div>
  )
}

/* ────────────────── Main ShopWizard component ────────────────── */
export interface ShopWizardProps {
  /** Subtitle for step 1 marketplace selection */
  subtitle?: string
  /** Called when wizard is cancelled (back on step 1) */
  onCancel?: () => void
  /** Called when shop is created and sync completes/starts */
  onComplete: (shopId: number, shopName: string, marketplace: Marketplace) => void
}

export default function ShopWizard({ subtitle, onCancel, onComplete }: ShopWizardProps) {
  const { setShops } = useAuthStore()

  const [step, setStep] = useState(0)
  const [marketplace, setMarketplace] = useState<Marketplace>(null)

  // Form data
  const [formData, setFormData] = useState<{
    apiKey: string
    clientId?: string
    perfClientId?: string
    perfClientSecret?: string
    shopName: string
  } | null>(null)

  // Validation
  const [validating, setValidating] = useState(false)
  const [validationResult, setValidationResult] = useState<ValidationResult | null>(null)

  // Error
  const [error, setError] = useState<string | null>(null)

  // Created shop id (for polling)
  const [createdShopId, setCreatedShopId] = useState<number | null>(null)

  /* Step handlers */
  const handleMarketplaceSelect = (mp: Marketplace) => {
    setMarketplace(mp)
    setStep(1)
  }

  const handleApiKeysSubmit = async (data: typeof formData) => {
    if (!data || !marketplace) return
    setFormData(data)
    setStep(2)
    setValidating(true)
    setValidationResult(null)

    try {
      const resp = await apiClient.post('/shops/validate-key', {
        marketplace,
        api_key: data.apiKey,
        client_id: data.clientId,
        perf_client_id: data.perfClientId,
        perf_client_secret: data.perfClientSecret,
      })
      setValidationResult(resp.data)
    } catch (err: any) {
      setValidationResult({
        valid: false,
        message: err.response?.data?.detail || 'Ошибка проверки ключа',
      })
    } finally {
      setValidating(false)
    }
  }

  const handleRetryValidation = () => {
    if (formData) {
      handleApiKeysSubmit(formData)
    }
  }

  const handleCreateShop = async () => {
    if (!formData || !marketplace) return
    setError(null)

    try {
      const resp = await apiClient.post('/shops', {
        name: formData.shopName,
        marketplace,
        api_key: formData.apiKey,
        client_id: formData.clientId,
        perf_client_id: formData.perfClientId,
        perf_client_secret: formData.perfClientSecret,
      })

      // Update auth store with latest shops
      const shopsResp = await apiClient.get('/shops')
      setShops(shopsResp.data)

      setCreatedShopId(resp.data.id)
      setStep(3)
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Ошибка создания магазина')
    }
  }

  const handleDone = async () => {
    // Refresh shops
    try {
      const shopsResp = await apiClient.get('/shops')
      setShops(shopsResp.data)

      // Auto-select newly created shop
      if (createdShopId) {
        const newShop = shopsResp.data.find((s: any) => s.id === createdShopId)
        if (newShop) {
          useAppStore.getState().setCurrentShop({
            id: newShop.id,
            name: newShop.name,
            marketplace: newShop.marketplace as 'wildberries' | 'ozon',
            isActive: newShop.is_active,
          })
        }
      }
    } catch {
      // proceed anyway
    }

    if (formData && marketplace) {
      onComplete(createdShopId!, formData.shopName, marketplace)
    }
  }

  return (
    <>
      {/* Step indicator */}
      <StepIndicator current={step} />

      {/* Error banner */}
      {error && (
        <div className="mb-4 rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-400 text-center">
          {error}
        </div>
      )}

      {/* Steps */}
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-6 md:p-8 shadow-xl">
        <AnimatePresence mode="wait">
          <motion.div
            key={step}
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: -20 }}
            transition={{ duration: 0.2 }}
          >
            {step === 0 && (
              <StepMarketplace
                onSelect={handleMarketplaceSelect}
                subtitle={subtitle}
              />
            )}
            {step === 1 && (
              <StepApiKeys
                marketplace={marketplace}
                onSubmit={handleApiKeysSubmit}
                onBack={() => {
                  if (onCancel) {
                    onCancel()
                  } else {
                    setStep(0)
                  }
                }}
              />
            )}
            {step === 2 && (
              <StepValidation
                marketplace={marketplace}
                result={validationResult}
                isLoading={validating}
                onRetry={handleRetryValidation}
                onContinue={handleCreateShop}
                onBack={() => setStep(1)}
              />
            )}
            {step === 3 && formData && (
              <StepSyncing
                shopId={createdShopId}
                shopName={formData.shopName}
                marketplace={marketplace}
                onDone={handleDone}
              />
            )}
          </motion.div>
        </AnimatePresence>
      </div>
    </>
  )
}
