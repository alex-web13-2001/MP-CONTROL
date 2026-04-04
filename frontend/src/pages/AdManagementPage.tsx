/**
 * AdManagementPage — WB Campaign Management (v3)
 *
 * Fixes:
 * - Actions (Play/Pause) moved to first column after checkbox
 * - Single action button: Active → Pause, Paused → Play
 * - Removed duplicate campaign type label (badge only)
 * - Wider campaign name (no unnecessary truncation)
 * - Removed budget column (budget not available in batch)
 * - Uses standard PeriodSelector with calendar for custom dates
 * - Styled type filter dropdown (no native <select>)
 * - All colors use CSS variables for light/dark theme support
 */
import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Search, Play, Pause, ChevronDown, ChevronUp, AlertTriangle,
  Plus, X, Loader2, Check, ChevronsUpDown, DollarSign, BarChart3,
} from 'lucide-react'
import { useAppStore } from '../stores/appStore'
import { PeriodSelector, type PeriodValue } from '../components/DateRangePicker'
import {
  getCampaignsFromDB, getCampaignStats, startCampaign, pauseCampaign,
  batchStatusChange, getCampaignBudget, depositBudget,
  formatMoney, formatNum,
  type EnrichedCampaign, type EnrichedCampaignsResponse,
} from '../api/ad-management'
import CampaignUnifiedModal from '../components/CampaignUnifiedModal'
import CreateCampaignModal from '../components/CreateCampaignModal'

// ── Sticky cell styles (matching ProductFinanceTable pattern) ─────
const stickyCol: React.CSSProperties = {
  position: 'sticky',
  left: 0,
  zIndex: 10,
  backgroundColor: 'hsl(var(--card))',
}
const stickyCol2: React.CSSProperties = { position: 'sticky', left: 40, zIndex: 10, backgroundColor: 'hsl(var(--card))' }
const stickyCol3: React.CSSProperties = { position: 'sticky', left: 80, zIndex: 10, backgroundColor: 'hsl(var(--card))', boxShadow: '4px 0 12px -2px rgba(0,0,0,0.25)' }

// ── Types & Constants ────────────────────────────────────────────

type SortKey = 'name' | 'status' | 'spend' | 'views' | 'clicks' | 'ctr' | 'cart' | 'orders' | 'revenue' | 'drr' | 'cpc' | 'cpm' | 'cpa_cart' | 'cpo'
type SortDir = 'asc' | 'desc'

const STATUS_LABELS: Record<number, string> = {
  9: 'Активна', 11: 'На паузе', 4: 'Готова к запуску',
  7: 'Завершена', [-1]: 'Удаляется', 8: 'Отказана',
}

const STATUS_COLORS: Record<number, { bg: string; text: string; dot: string }> = {
  9:    { bg: 'bg-emerald-500/15', text: 'text-emerald-600 dark:text-emerald-400', dot: 'bg-emerald-500' },
  11:   { bg: 'bg-amber-500/15',   text: 'text-amber-600 dark:text-amber-400',   dot: 'bg-amber-500' },
  4:    { bg: 'bg-blue-500/15',    text: 'text-blue-600 dark:text-blue-400',     dot: 'bg-blue-500' },
  7:    { bg: 'bg-zinc-500/15',    text: 'text-zinc-600 dark:text-zinc-400',     dot: 'bg-zinc-400' },
  [-1]: { bg: 'bg-red-500/15',     text: 'text-red-600 dark:text-red-400',       dot: 'bg-red-500' },
  8:    { bg: 'bg-red-500/15',     text: 'text-red-600 dark:text-red-400',       dot: 'bg-red-500' },
}

const PAYMENT_TYPE_LABELS: Record<string, string> = {
  cpm: 'CPM', cpc: 'CPC',
}



// ── Status Multi-Select Dropdown ─────────────────────────────────

function StatusMultiSelect({
  selected, onChange, counts,
}: {
  selected: number[]; onChange: (v: number[]) => void; counts: Record<number, number>
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const statuses = [9, 11, 4, 7, 8, -1]
  const selectedLabels = selected.length === 0
    ? 'Все статусы'
    : selected.map(s => STATUS_LABELS[s]).filter(Boolean).join(', ')

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 px-3 py-2 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted))] text-sm text-[hsl(var(--foreground))] transition-colors min-w-[160px]"
      >
        <span className="truncate flex-1 text-left">{selectedLabels}</span>
        <ChevronDown className="w-3.5 h-3.5 text-[hsl(var(--muted-foreground))] flex-shrink-0" />
      </button>
      {open && (
        <div className="absolute top-full left-0 mt-1 z-50 bg-[hsl(var(--popover))] border border-[hsl(var(--border))] rounded-xl shadow-2xl py-1 min-w-[200px]">
          <button
            onClick={() => { onChange([]); setOpen(false); }}
            className="w-full text-left px-3 py-1.5 text-xs text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted))] transition-colors"
          >
            Сбросить фильтр
          </button>
          {statuses.map(s => {
            const checked = selected.includes(s)
            const cnt = counts[s] || 0
            const sc = STATUS_COLORS[s] || STATUS_COLORS[8]
            return (
              <button
                key={s}
                onClick={() => {
                  onChange(checked ? selected.filter(x => x !== s) : [...selected, s])
                }}
                className="w-full flex items-center gap-2 px-3 py-1.5 hover:bg-[hsl(var(--muted))] transition-colors"
              >
                <div className={`w-4 h-4 rounded border ${checked ? 'bg-violet-600 border-violet-500' : 'border-[hsl(var(--border))]'} flex items-center justify-center`}>
                  {checked && <Check className="w-3 h-3 text-white" />}
                </div>
                <div className={`w-2 h-2 rounded-full ${sc.dot}`} />
                <span className="text-sm text-[hsl(var(--foreground))]">{STATUS_LABELS[s]}</span>
                <span className="ml-auto text-xs text-[hsl(var(--muted-foreground))]">{cnt}</span>
              </button>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ── Type Filter Dropdown ─────────────────────────────────────────

function TypeFilterDropdown({
  value, onChange,
}: {
  value: string; onChange: (v: string) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const types = [
    { key: 'all', label: 'Все типы' },
    { key: 'cpm', label: 'CPM' },
    { key: 'cpc', label: 'CPC' },

  ]
  const current = types.find(t => t.key === value) || types[0]

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 px-3 py-2 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted))] text-sm text-[hsl(var(--foreground))] transition-colors min-w-[120px]"
      >
        <span className="truncate flex-1 text-left">{current.label}</span>
        <ChevronDown className="w-3.5 h-3.5 text-[hsl(var(--muted-foreground))] flex-shrink-0" />
      </button>
      {open && (
        <div className="absolute top-full left-0 mt-1 z-50 bg-[hsl(var(--popover))] border border-[hsl(var(--border))] rounded-xl shadow-2xl py-1 min-w-[140px]">
          {types.map(t => (
            <button
              key={t.key}
              onClick={() => { onChange(t.key); setOpen(false); }}
              className={`w-full text-left px-3 py-1.5 text-sm hover:bg-[hsl(var(--muted))] transition-colors ${value === t.key ? 'text-violet-600 dark:text-violet-400 font-medium' : 'text-[hsl(var(--foreground))]'}`}
            >
              {t.label}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Budget Deposit Modal ─────────────────────────────────────────

function BudgetDepositModal({
  campaign, shopId, balance, onClose, onSuccess,
}: {
  campaign: EnrichedCampaign; shopId: number; balance: number; onClose: () => void; onSuccess: (depositedAmount: number, newBudgetTotal: number | null, campaignStarted: boolean) => void
}) {
  const [amount, setAmount] = useState<string>('3000')
  const [loading, setLoading] = useState(false)
  const [budgetData, setBudgetData] = useState<any>(null)
  const [loadingBudget, setLoadingBudget] = useState(true)
  const [error, setError] = useState('')
  const [success, setSuccess] = useState(false)
  const [autoStart, setAutoStart] = useState(true)
  const [startResult, setStartResult] = useState<{ tried: boolean; ok: boolean; msg: string } | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoadingBudget(true)
    getCampaignBudget(shopId, campaign.advert_id)
      .then(data => { if (!cancelled) setBudgetData(data) })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoadingBudget(false) })
    return () => { cancelled = true }
  }, [shopId, campaign.advert_id])

  const numAmount = Number(amount) || 0


  const handleDeposit = async () => {
    if (numAmount < 100) {
      setError('Минимальная сумма — 100 ₽')
      return
    }
    setLoading(true)
    setError('')
    try {
      const response = await depositBudget(shopId, campaign.advert_id, numAmount)
      
      // Auto-start campaign after deposit if checkbox is checked
      let didStart = false
      if (showAutoStart && autoStart) {
        try {
          const startRes = await startCampaign(shopId, campaign.advert_id)
          setStartResult({ tried: true, ok: startRes.success, msg: startRes.message })
          didStart = startRes.success
        } catch (e: any) {
          setStartResult({ tried: true, ok: false, msg: e?.response?.data?.detail || 'Ошибка запуска' })
        }
      }
      
      setSuccess(true)
      setTimeout(() => {
        onSuccess(numAmount, response.new_budget_total ?? null, didStart)
      }, 2000)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка пополнения')
    } finally {
      setLoading(false)
    }
  }

  const ptLabel = PAYMENT_TYPE_LABELS[campaign.payment_type] || campaign.payment_type?.toUpperCase() || ''
  const currentBudget = budgetData?.total || campaign.budget_total || 0

  // Show auto-start checkbox only when campaign is NOT active (9) and has zero budget
  const showAutoStart = campaign.status !== 9 && currentBudget === 0

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm"
      onClick={onClose}
    >
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: 10 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 10 }}
        onClick={e => e.stopPropagation()}
        className="relative bg-[hsl(var(--card))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl w-full max-w-md overflow-hidden"
      >
        {/* Success state */}
        {success ? (
          <div className="p-8 flex flex-col items-center gap-4">
            <div className="w-14 h-14 rounded-full bg-emerald-500/15 flex items-center justify-center">
              <Check className="w-7 h-7 text-emerald-500" />
            </div>
            <div className="text-center">
              <p className="text-lg font-semibold text-[hsl(var(--foreground))]">Бюджет пополнен</p>
              <p className="text-sm text-[hsl(var(--muted-foreground))] mt-1">
                +{numAmount.toLocaleString('ru-RU')} ₽ на кампанию {campaign.advert_id}
              </p>
              {startResult?.tried && (
                <p className={`text-sm mt-2 font-medium ${startResult.ok ? 'text-emerald-500' : 'text-amber-500'}`}>
                  {startResult.ok ? '▶ Кампания запущена' : `⚠ ${startResult.msg}`}
                </p>
              )}
            </div>
          </div>
        ) : (
          <>
            {/* Header */}
            <div className="px-6 pt-6 pb-4">
              <div className="flex items-start justify-between">
                <div className="flex-1 min-w-0">
                  <h3 className="text-lg font-bold text-[hsl(var(--foreground))]">Пополнение бюджета кампании</h3>
                  <p className="text-sm text-[hsl(var(--muted-foreground))] mt-1 truncate">
                    {campaign.name || '—'} · ID {campaign.advert_id}
                    {ptLabel && ` · ${ptLabel}`}
                  </p>
                </div>
                <button onClick={onClose} className="ml-3 p-1 rounded-lg text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))] transition-colors">
                  <X className="w-5 h-5" />
                </button>
              </div>
            </div>

            {/* Body */}
            <div className="px-6 pb-6 space-y-5">
              {/* Amount input */}
              <div>
                <label className="block text-sm font-medium text-[hsl(var(--foreground))] mb-2">Сумма пополнения (₽)</label>
                <input
                  type="number"
                  min={100}
                  step={100}
                  placeholder="3000"
                  value={amount}
                  onChange={e => setAmount(e.target.value)}
                  className="w-full px-4 py-3 rounded-xl bg-[hsl(var(--secondary))] border border-[hsl(var(--border))] text-[hsl(var(--foreground))] text-lg font-semibold focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all"
                />
                <p className="text-xs text-[hsl(var(--muted-foreground))] mt-1.5">Минимальный бюджет — 1 000 ₽</p>
              </div>

              {/* Source — Единый счёт */}
              <div>
                <label className="block text-sm font-medium text-[hsl(var(--foreground))] mb-2">Источники списания</label>
                <div className="flex items-center justify-between px-4 py-3 rounded-xl bg-[hsl(var(--secondary))] border border-[hsl(var(--border))]">
                  <span className="text-sm text-[hsl(var(--foreground))]">Единый счёт:</span>
                  <span className="text-sm font-bold text-[hsl(var(--foreground))]">
                    {balance.toLocaleString('ru-RU')} ₽
                  </span>
                </div>
              </div>

              {/* Current budget */}
              <div className="flex items-center justify-between px-4 py-3 rounded-xl bg-[hsl(var(--secondary))] border border-[hsl(var(--border))]">
                <span className="text-sm text-[hsl(var(--muted-foreground))]">Текущий бюджет:</span>
                {loadingBudget ? (
                  <Loader2 className="w-4 h-4 animate-spin text-[hsl(var(--muted-foreground))]" />
                ) : (
                  <span className="text-sm font-bold text-[hsl(var(--foreground))]">{currentBudget.toLocaleString('ru-RU')} ₽</span>
                )}
              </div>

              {/* Error */}
              {error && (
                <div className="flex items-center gap-2 text-sm text-red-500 bg-red-500/10 px-3 py-2 rounded-lg">
                  <AlertTriangle className="w-4 h-4 flex-shrink-0" /> {error}
                </div>
              )}

              {/* Auto-start checkbox */}
              {showAutoStart && (
                <label className="flex items-center gap-2.5 cursor-pointer select-none group">
                  <div className="relative">
                    <input
                      type="checkbox"
                      checked={autoStart}
                      onChange={e => setAutoStart(e.target.checked)}
                      className="sr-only peer"
                    />
                    <div className="w-5 h-5 rounded-md border-2 border-[hsl(var(--border))] bg-[hsl(var(--secondary))] peer-checked:bg-violet-600 peer-checked:border-violet-600 transition-all flex items-center justify-center">
                      {autoStart && <Check className="w-3.5 h-3.5 text-white" />}
                    </div>
                  </div>
                  <span className="text-sm text-[hsl(var(--muted-foreground))] group-hover:text-[hsl(var(--foreground))] transition-colors">
                    <Play className="w-3.5 h-3.5 inline-block mr-1 text-emerald-500" />
                    Запустить кампанию после пополнения
                  </span>
                </label>
              )}

              {/* Actions */}
              <div className="flex items-center gap-3 pt-1">
                <button
                  onClick={handleDeposit}
                  disabled={loading || numAmount < 100}
                  className="flex-1 px-5 py-3 rounded-xl bg-violet-600 hover:bg-violet-500 text-white text-sm font-semibold transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {loading ? (
                    <span className="flex items-center justify-center gap-2">
                      <Loader2 className="w-4 h-4 animate-spin" /> {showAutoStart && autoStart ? 'Пополняю и запускаю...' : 'Пополняю...'}
                    </span>
                  ) : (
                    showAutoStart && autoStart ? 'Пополнить и запустить' : 'Пополнить'
                  )}
                </button>
                <button
                  onClick={onClose}
                  className="px-5 py-3 rounded-xl border border-[hsl(var(--border))] text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))] text-sm font-medium transition-colors"
                >
                  Отменить
                </button>
              </div>
            </div>
          </>
        )}
      </motion.div>
    </motion.div>
  )
}

// ── Sort Header ──────────────────────────────────────────────────

function SortHeader({
  label, sortKey, currentSort, currentDir, onSort, align = 'right',
}: {
  label: string; sortKey: SortKey; currentSort: SortKey; currentDir: SortDir
  onSort: (key: SortKey) => void; align?: 'left' | 'right'
}) {
  const active = currentSort === sortKey
  return (
    <th
      className={`px-3 py-2.5 text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider cursor-pointer hover:text-[hsl(var(--foreground))] transition-colors select-none whitespace-nowrap ${align === 'left' ? 'text-left' : 'text-right'}`}
      onClick={() => onSort(sortKey)}
    >
      <div className={`flex items-center gap-1 ${align === 'right' ? 'justify-end' : 'justify-start'}`}>
        <span>{label}</span>
        {active ? (
          currentDir === 'asc' ? <ChevronUp className="w-3 h-3 text-violet-500" /> : <ChevronDown className="w-3 h-3 text-violet-500" />
        ) : (
          <ChevronsUpDown className="w-3 h-3 opacity-30" />
        )}
      </div>
    </th>
  )
}

// ══════════════════════════════════════════════════════════════════
// Main Page Component
// ══════════════════════════════════════════════════════════════════

export default function AdManagementPage() {
  const { currentShop } = useAppStore()
  const shopId = currentShop?.id
  const marketplace = currentShop?.marketplace

  const [data, setData] = useState<EnrichedCampaignsResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  // Period — uses PeriodSelector
  const [periodValue, setPeriodValue] = useState<PeriodValue>({
    mode: 'quick', period: 7, dateRange: null,
  })

  // Filters
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState<number[]>([9, 11, 4])
  const [typeFilter, setTypeFilter] = useState<string>('all')

  // Sort
  const [sortKey, setSortKey] = useState<SortKey>('spend')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  // Selection
  const [selected, setSelected] = useState<Set<number>>(new Set())

  // UI
  const [unifiedModal, setUnifiedModal] = useState<{ campaign: EnrichedCampaign; tab: 'management' | 'analytics' } | null>(null)
  const [budgetModal, setBudgetModal] = useState<EnrichedCampaign | null>(null)
  const [actionLoading, setActionLoading] = useState<string | null>(null)
  const [batchBudgetAmount, setBatchBudgetAmount] = useState<number | null>(null)
  const [showCreateModal, setShowCreateModal] = useState(false)

  // Budgets — from Redis cache (synced by Celery every 15 min)
  const [budgets, setBudgets] = useState<Record<number, { total: number; daily: number; loading: boolean }>>({})

  // sessionStorage-backed recent deposits — survives F5 page reload (unlike useRef)
  const DEPOSIT_STORAGE_KEY = 'recent_deposits'
  const getRecentDeposits = (): Record<number, { amount: number; ts: number }> => {
    try {
      const raw = sessionStorage.getItem(DEPOSIT_STORAGE_KEY)
      return raw ? JSON.parse(raw) : {}
    } catch { return {} }
  }
  const setRecentDeposit = (cid: number, amount: number) => {
    const deposits = getRecentDeposits()
    deposits[cid] = { amount, ts: Date.now() }
    sessionStorage.setItem(DEPOSIT_STORAGE_KEY, JSON.stringify(deposits))
  }
  const clearExpiredDeposits = () => {
    const deposits = getRecentDeposits()
    const now = Date.now()
    let changed = false
    for (const key of Object.keys(deposits)) {
      if (now - deposits[Number(key)].ts >= 120_000) {
        delete deposits[Number(key)]
        changed = true
      }
    }
    if (changed) sessionStorage.setItem(DEPOSIT_STORAGE_KEY, JSON.stringify(deposits))
    return deposits
  }

  // Track whether data has been loaded for this shop (to avoid re-fetching on period change)
  const wbDataLoadedForShop = useRef<number | null>(null)

  // ── Helper: build period params ────────────────────────────────

  const getPeriodParams = useCallback(() => {
    let dateFrom: string | undefined
    let dateTo: string | undefined
    let period = `${periodValue.period}d`

    if (periodValue.mode === 'custom' && periodValue.dateRange?.from) {
      const fmt = (d: Date) => d.toISOString().split('T')[0]
      dateFrom = fmt(periodValue.dateRange.from)
      dateTo = fmt(periodValue.dateRange.to || periodValue.dateRange.from)
      period = 'custom'
    }
    return { period, dateFrom, dateTo }
  }, [periodValue])

  // ── Initial Load (all from DB — 0 WB API calls) ─────────────

  const loadFullData = useCallback(async () => {
    if (!shopId) return
    setLoading(true)
    try {
      const { period, dateFrom, dateTo } = getPeriodParams()
      const result = await getCampaignsFromDB(shopId, period, dateFrom, dateTo)
      setData(result)
      setError('')
      wbDataLoadedForShop.current = shopId

      // Extract budgets from response (cached by Celery in Redis)
      const budgetState: Record<number, { total: number; daily: number; loading: boolean }> = {}
      const deposits = clearExpiredDeposits()
      for (const c of result.campaigns) {
        if (c.budget_total !== undefined || c.budget_daily !== undefined) {
          const apiTotal = c.budget_total || 0
          const deposit = deposits[c.advert_id]
          // If this campaign was recently deposited (within 2min) and API still shows lower value,
          // keep the optimistic value to prevent UI reset
          let total = apiTotal
          if (deposit && apiTotal < deposit.amount) {
            total = deposit.amount
          }
          budgetState[c.advert_id] = {
            total,
            daily: c.budget_daily || 0,
            loading: false,
          }
        }
      }
      setBudgets(budgetState)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [shopId, getPeriodParams])

  // ── Period Refresh (ClickHouse only — instant, no WB API) ─────

  const refreshStats = useCallback(async () => {
    if (!shopId || !data) return
    setLoading(true)
    try {
      const { period, dateFrom, dateTo } = getPeriodParams()
      const statsResult = await getCampaignStats(shopId, period, dateFrom, dateTo)

      // Merge new stats into existing campaign data
      const updatedCampaigns = data.campaigns.map(c => {
        const st = statsResult.stats[c.advert_id] || {}
        return {
          ...c,
          spend: st.spend ?? 0,
          views: st.views ?? 0,
          clicks: st.clicks ?? 0,
          cart: st.cart ?? 0,
          orders: st.orders ?? 0,
          revenue: st.revenue ?? 0,
          ctr: st.ctr ?? 0,
          drr: st.drr ?? 0,
          cpc: st.cpc ?? 0,
          cpm: st.cpm ?? 0,
          cpa_cart: st.cpa_cart ?? 0,
          cpo: st.cpo ?? 0,
        }
      })

      setData(prev => prev ? {
        ...prev,
        campaigns: updatedCampaigns,
        kpi: statsResult.kpi,
        kpi_deltas: statsResult.kpi_deltas,
        period: statsResult.period,
      } : prev)
      setError('')
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка обновления статистики')
    } finally {
      setLoading(false)
    }
  }, [shopId, data, getPeriodParams])

  // ── Effects ────────────────────────────────────────────────────

  // Initial load when shop changes (or first load)
  useEffect(() => {
    if (shopId && shopId !== wbDataLoadedForShop.current) {
      loadFullData()
    }
  }, [shopId, loadFullData])

  // Period change → only refresh stats (instant, no WB API calls)
  useEffect(() => {
    if (shopId && wbDataLoadedForShop.current === shopId && data) {
      refreshStats()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [periodValue])

  // Budgets are now included in the main response from /campaigns/from-db
  // No separate fetch needed — Celery syncs budgets to Redis every 15 min

  // ── Handlers ───────────────────────────────────────────────────

  const handleAction = async (advertId: number, action: 'start' | 'pause') => {
    if (!shopId) return
    setActionLoading(`${advertId}-${action}`)
    try {
      if (action === 'start') await startCampaign(shopId, advertId)
      else await pauseCampaign(shopId, advertId)

      // Optimistically update status in local state (don't wait for Celery sync)
      const newStatus = action === 'start' ? 9 : 11  // 9=Active, 11=Paused
      setData(prev => prev ? {
        ...prev,
        campaigns: prev.campaigns.map(c =>
          c.advert_id === advertId ? { ...c, status: newStatus } : c
        ),
      } : prev)
    } catch (e: any) {
      alert(e?.response?.data?.detail || 'Ошибка')
    } finally {
      setActionLoading(null)
    }
  }

  const handleBatchAction = async (action: 'start' | 'pause') => {
    if (!shopId || selected.size === 0) return
    setActionLoading(`batch-${action}`)
    try {
      await batchStatusChange(shopId, Array.from(selected), action)
      const newStatus = action === 'start' ? 9 : 11
      setData(prev => prev ? {
        ...prev,
        campaigns: prev.campaigns.map(c =>
          selected.has(c.advert_id) ? { ...c, status: newStatus } : c
        ),
      } : prev)
      setSelected(new Set())
    } catch (e: any) {
      alert(e?.response?.data?.detail || 'Ошибка')
    } finally {
      setActionLoading(null)
    }
  }

  const handleBatchBudget = async (amount: number) => {
    if (!shopId || selected.size === 0) return
    setActionLoading('batch-budget')
    try {
      const ids = Array.from(selected)
      for (const id of ids) {
        await depositBudget(shopId, id, amount)
      }
      setBatchBudgetAmount(null)
      setSelected(new Set())
      await loadFullData()
    } catch (e: any) {
      alert(e?.response?.data?.detail || 'Ошибка пополнения')
    } finally {
      setActionLoading(null)
    }
  }

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  // ── Filter & Sort ──────────────────────────────────────────────

  const campaigns = data?.campaigns || []

  const statusCounts = useMemo(() => {
    const counts: Record<number, number> = {}
    campaigns.forEach(c => { counts[c.status] = (counts[c.status] || 0) + 1 })
    return counts
  }, [campaigns])

  const filtered = useMemo(() => {
    let list = [...campaigns]

    if (statusFilter.length > 0) {
      list = list.filter(c => statusFilter.includes(c.status))
    }
    if (typeFilter !== 'all') {
      list = list.filter(c => c.payment_type === typeFilter)
    }
    if (search.trim()) {
      const q = search.toLowerCase().trim()
      list = list.filter(c => {
        // По названию кампании
        if (c.name?.toLowerCase().includes(q)) return true
        // По ID кампании
        if (String(c.advert_id).includes(q)) return true
        // По товарам внутри кампании: nm_id, артикул (vendor_code), название товара
        if (c.nm_settings?.some(s =>
          String(s.nm_id).includes(q) ||
          s.vendor_code?.toLowerCase().includes(q) ||
          s.product_name?.toLowerCase().includes(q) ||
          s.subject_name?.toLowerCase().includes(q)
        )) return true
        return false
      })
    }

    list.sort((a, b) => {
      let va: any, vb: any
      if (sortKey === 'name') {
        va = a.name || ''
        vb = b.name || ''
        return sortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va)
      } else if (sortKey === 'status') {
        va = a.status; vb = b.status
      } else {
        va = (a as any)[sortKey] || 0
        vb = (b as any)[sortKey] || 0
      }
      return sortDir === 'asc' ? va - vb : vb - va
    })

    return list
  }, [campaigns, statusFilter, typeFilter, search, sortKey, sortDir])

  // ── Select all ─────────────────────────────────────────────────

  const allSelected = filtered.length > 0 && filtered.every(c => selected.has(c.advert_id))
  const toggleAll = () => {
    if (allSelected) setSelected(new Set())
    else setSelected(new Set(filtered.map(c => c.advert_id)))
  }
  const toggleOne = (id: number) => {
    const next = new Set(selected)
    if (next.has(id)) next.delete(id)
    else next.add(id)
    setSelected(next)
  }

  // ── Non-WB guard ───────────────────────────────────────────────

  if (!shopId || marketplace !== 'wildberries') {
    return (
      <div className="flex flex-col items-center justify-center h-[60vh] gap-4">
        <div className="w-16 h-16 rounded-2xl bg-amber-500/10 flex items-center justify-center">
          <AlertTriangle className="w-8 h-8 text-amber-500" />
        </div>
        <p className="text-[hsl(var(--muted-foreground))] text-center max-w-sm">
          {!shopId ? 'Выберите магазин для управления рекламой' : 'Управление рекламой доступно только для Wildberries'}
        </p>
      </div>
    )
  }

  // ── KPI ────────────────────────────────────────────────────────

  const kpi = data?.kpi

  const balance = data?.balance
  const accountBalance = (balance as any)?.net || (balance as any)?.balance || 0

  return (
    <div className="space-y-5 pb-10">
      {/* ── Header ─────────────────────────────────────────────── */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-bold text-[hsl(var(--foreground))]">
            Рекламные кампании Wildberries
          </h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            Управление и статистика · {data?.total || 0} кампаний
          </p>
        </div>
        <div className="flex items-center gap-3">
          {accountBalance > 0 && (
            <div className="text-sm text-[hsl(var(--muted-foreground))]">
              Баланс: <span className="text-[hsl(var(--foreground))] font-semibold">{accountBalance.toLocaleString('ru-RU')} ₽</span>
            </div>
          )}
          <button
            onClick={() => setShowCreateModal(true)}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white font-medium text-sm transition-colors"
          >
            <Plus className="w-4 h-4" /> Создать кампанию
          </button>
        </div>
      </div>


      {/* ── Filters ────────────────────────────────────────────── */}
      <div className="flex items-center gap-3 flex-wrap">
        {/* Search */}
        <div className="relative flex-1 min-w-[200px] max-w-[400px]">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[hsl(var(--muted-foreground))]" />
          <input
            placeholder="Поиск по ID, названию, артикулу или товару..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="w-full pl-10 pr-8 py-2 rounded-xl bg-[hsl(var(--card))] border border-[hsl(var(--border))] text-sm text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))] focus:outline-none focus:ring-2 focus:ring-violet-500/30 transition-all"
          />
          {search && (
            <button onClick={() => setSearch('')} className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]">
              <X className="w-3.5 h-3.5" />
            </button>
          )}
        </div>

        {/* Period — standard component */}
        <PeriodSelector value={periodValue} onChange={setPeriodValue} />

        {/* Status filter */}
        <StatusMultiSelect selected={statusFilter} onChange={setStatusFilter} counts={statusCounts} />

        {/* Type filter — styled dropdown */}
        <TypeFilterDropdown value={typeFilter} onChange={setTypeFilter} />
      </div>

      {/* ── Batch Actions ──────────────────────────────────────── */}
      <AnimatePresence>
        {selected.size > 0 && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            className="flex items-center gap-3 px-4 py-2.5 rounded-xl bg-violet-600/10 border border-violet-500/30"
          >
            <span className="text-sm text-violet-600 dark:text-violet-300 font-medium">Выбрано: {selected.size}</span>
            <div className="flex-1" />
            <button
              onClick={() => handleBatchAction('start')}
              disabled={actionLoading !== null}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-600/20 hover:bg-emerald-600/30 text-emerald-600 dark:text-emerald-400 text-sm font-medium transition-colors disabled:opacity-50"
            >
              <Play className="w-3.5 h-3.5" /> Запустить
            </button>
            <button
              onClick={() => handleBatchAction('pause')}
              disabled={actionLoading !== null}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-amber-600/20 hover:bg-amber-600/30 text-amber-600 dark:text-amber-400 text-sm font-medium transition-colors disabled:opacity-50"
            >
              <Pause className="w-3.5 h-3.5" /> Пауза
            </button>
            <button
              onClick={() => setBatchBudgetAmount(1000)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-violet-600/20 hover:bg-violet-600/30 text-violet-600 dark:text-violet-400 text-sm font-medium transition-colors"
            >
              <DollarSign className="w-3.5 h-3.5" /> Пополнить бюджет
            </button>
            <button
              onClick={() => setSelected(new Set())}
              className="text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] text-sm transition-colors"
            >
              Снять выделение
            </button>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── Batch Budget Inline ─────────────────────────────────── */}
      <AnimatePresence>
        {batchBudgetAmount !== null && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            className="flex items-center gap-3 px-4 py-3 rounded-xl bg-[hsl(var(--card))] border border-[hsl(var(--border))]"
          >
            <span className="text-sm text-[hsl(var(--foreground))] font-medium">
              Пополнить {selected.size} кампаний на:
            </span>
            <input
              type="number"
              min={100}
              value={batchBudgetAmount}
              onChange={e => setBatchBudgetAmount(Number(e.target.value))}
              className="w-32 px-3 py-1.5 rounded-lg bg-[hsl(var(--secondary))] border border-[hsl(var(--border))] text-[hsl(var(--foreground))] text-sm font-semibold focus:outline-none focus:ring-2 focus:ring-violet-500/30"
            />
            <span className="text-sm text-[hsl(var(--muted-foreground))]">₽ каждую</span>
            <button
              onClick={() => handleBatchBudget(batchBudgetAmount)}
              disabled={actionLoading !== null || batchBudgetAmount < 100}
              className="px-4 py-1.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium transition-colors disabled:opacity-50"
            >
              {actionLoading === 'batch-budget' ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Пополнить'}
            </button>
            <button
              onClick={() => setBatchBudgetAmount(null)}
              className="text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] text-sm transition-colors"
            >
              Отмена
            </button>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── Loading / Error ──────────────────────────────────── */}
      {loading && data && (
        <div className="flex items-center gap-2 text-sm text-[hsl(var(--muted-foreground))]">
          <Loader2 className="w-4 h-4 animate-spin" /> Обновление данных...
        </div>
      )}

      {error && !data && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-4 text-red-500 text-sm">
          {error}
        </div>
      )}

      {loading && !data && (
        <div className="flex items-center justify-center h-40 gap-3 text-[hsl(var(--muted-foreground))]">
          <Loader2 className="w-6 h-6 animate-spin" />
          <span>Загрузка кампаний...</span>
        </div>
      )}

      {/* ── Table ──────────────────────────────────────────────── */}
      {data && (
        <div className="rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
          <div className="overflow-auto max-h-[75vh] relative">
            <table className="w-full border-collapse" style={{ minWidth: 1400 }}>
              <thead className="sticky top-0 z-20">
                <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                  {/* Frozen left block: checkbox + action + name */}
                  <th
                    className="px-3 py-2.5 w-10"
                    style={{ ...stickyCol, zIndex: 30 }}
                  >
                    <input
                      type="checkbox"
                      checked={allSelected}
                      onChange={toggleAll}
                      className="rounded accent-violet-600"
                    />
                  </th>
                  <th className="px-2 py-2.5 text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider text-center w-10" style={{ ...stickyCol2, zIndex: 30 }}></th>
                  <th
                    className="px-3 py-2.5 text-left text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider min-w-[260px] cursor-pointer select-none hover:text-[hsl(var(--foreground))] transition-colors"
                    style={{ ...stickyCol3, zIndex: 30 }}
                    onClick={() => handleSort('name')}
                  >
                    <span className="inline-flex items-center gap-1">
                      Кампания
                      {sortKey === 'name' && <span className="text-[11px] text-[hsl(var(--primary))]">{sortDir === 'desc' ? '▼' : '▲'}</span>}
                    </span>
                  </th>
                  <SortHeader label="Статус" sortKey="status" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} align="left" />
                  <th className="px-3 py-2.5 text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider text-center whitespace-nowrap">Бюджет</th>
                  <SortHeader label="Затраты" sortKey="spend" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="Продажи" sortKey="revenue" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="Показы" sortKey="views" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="Клики" sortKey="clicks" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="CTR" sortKey="ctr" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="Корзины" sortKey="cart" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="Заказы" sortKey="orders" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="ДРР" sortKey="drr" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="CPC" sortKey="cpc" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="CPM" sortKey="cpm" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="CPA корз." sortKey="cpa_cart" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                  <SortHeader label="CPO" sortKey="cpo" currentSort={sortKey} currentDir={sortDir} onSort={handleSort} />
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 && (
                  <tr>
                    <td colSpan={17} className="px-6 py-12 text-center text-[hsl(var(--muted-foreground))] text-sm">
                      {campaigns.length === 0 ? 'Нет кампаний' : 'Ничего не найдено по указанным фильтрам'}
                    </td>
                  </tr>
                )}
                {filtered.map(c => {
                  const sc = STATUS_COLORS[c.status] || STATUS_COLORS[8]
                  const ptLabel = PAYMENT_TYPE_LABELS[c.payment_type] || c.payment_type?.toUpperCase() || '—'
                  const drrColor = c.drr > 20 ? 'text-red-500' : c.drr > 10 ? 'text-amber-500' : 'text-emerald-500'

                  // Placements
                  const placements: string[] = []
                  if (c.search_enabled) placements.push('Поиск')
                  if (c.recommendations_enabled) placements.push('Полки')

                  return (
                    <React.Fragment key={c.advert_id}>
                      <tr
                        className={`group border-b border-[hsl(var(--border))]/40 hover:bg-[hsl(var(--muted)/0.15)] transition-colors ${selected.has(c.advert_id) ? 'bg-violet-600/5' : ''}`}
                      >
                        {/* Checkbox — sticky left */}
                        <td
                          className="px-3 py-2.5" onClick={e => e.stopPropagation()}
                          style={{ ...stickyCol }}
                        >
                          <input
                            type="checkbox"
                            checked={selected.has(c.advert_id)}
                            onChange={() => toggleOne(c.advert_id)}
                            className="rounded accent-violet-600"
                          />
                        </td>

                        {/* Action button — sticky left */}
                        <td className="px-2 py-2.5 text-center" style={{ ...stickyCol2 }}>
                          {c.status === 9 ? (
                            <button
                              onClick={() => handleAction(c.advert_id, 'pause')}
                              disabled={actionLoading !== null}
                              className="p-1.5 rounded-lg hover:bg-amber-500/20 text-amber-500 transition-colors disabled:opacity-30"
                              title="Поставить на паузу"
                            >
                              {actionLoading === `${c.advert_id}-pause` ? <Loader2 className="w-4 h-4 animate-spin" /> : <Pause className="w-4 h-4" />}
                            </button>
                          ) : (c.status === 11 || c.status === 4) ? (
                            <button
                              onClick={() => handleAction(c.advert_id, 'start')}
                              disabled={actionLoading !== null}
                              className="p-1.5 rounded-lg hover:bg-emerald-500/20 text-emerald-500 transition-colors disabled:opacity-30"
                              title="Запустить"
                            >
                              {actionLoading === `${c.advert_id}-start` ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                            </button>
                          ) : (
                            <span className="w-4 h-4 inline-block" />
                          )}
                        </td>

                        {/* Campaign name — sticky left with shadow */}
                        <td
                          className="px-3 py-2.5 min-w-[260px]"
                          style={{ ...stickyCol3 }}
                        >
                          <button
                            onClick={() => setUnifiedModal({ campaign: c, tab: 'management' })}
                            className="text-left w-full"
                          >
                            <div className="flex items-center gap-2">
                              <div className="flex-1 min-w-0">
                                <div className="text-sm font-semibold text-[hsl(var(--foreground))] leading-tight">
                                  {c.name || `Кампания ${c.advert_id}`}
                                </div>
                                <div className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5 flex items-center gap-1.5">
                                  <span>{c.advert_id}</span>
                                  {placements.length > 0 && (
                                    <>
                                      <span className="opacity-30">·</span>
                                      {placements.map(p => (
                                        <span key={p} className="text-[10px]">
                                          {p === 'Поиск' ? '🔍' : '📦'} {p}
                                        </span>
                                      ))}
                                    </>
                                  )}
                                </div>
                              </div>
                              <button
                                onClick={(e) => { e.stopPropagation(); setUnifiedModal({ campaign: c, tab: 'analytics' }) }}
                                className="p-1 rounded-md hover:bg-violet-500/15 text-[hsl(var(--muted-foreground))] hover:text-violet-500 transition-colors flex-shrink-0"
                                title="Статистика кампании"
                              >
                                <BarChart3 className="w-4 h-4" />
                              </button>
                              <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold flex-shrink-0
                                ${ptLabel === 'CPM' ? 'bg-violet-500/15 text-violet-600 dark:text-violet-400' :
                                  ptLabel === 'CPC' ? 'bg-cyan-500/15 text-cyan-600 dark:text-cyan-400' :
                                    'bg-emerald-500/15 text-emerald-600 dark:text-emerald-400'}`}
                              >
                                {ptLabel}
                              </span>
                            </div>
                          </button>
                        </td>

                        {/* Status */}
                        <td className="px-3 py-2.5 whitespace-nowrap">
                          <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${sc.bg} ${sc.text}`}>
                            <span className={`w-1.5 h-1.5 rounded-full ${sc.dot}`} />
                            {STATUS_LABELS[c.status] || '—'}
                          </span>
                        </td>

                        {/* Budget — shows real balance, click to deposit */}
                        <td className="px-3 py-2.5 text-center whitespace-nowrap" onClick={e => e.stopPropagation()}>
                          {(() => {
                            const b = budgets[c.advert_id]
                            if (b?.loading) {
                              return <Loader2 className="w-3.5 h-3.5 animate-spin text-[hsl(var(--muted-foreground))] mx-auto" />
                            }
                            if (b && b.total > 0) {
                              return (
                                <button
                                  onClick={() => setBudgetModal(c)}
                                  className="text-sm font-medium text-[hsl(var(--foreground))] hover:text-violet-500 transition-colors cursor-pointer"
                                  title="Нажмите для пополнения"
                                >
                                  {b.total.toLocaleString('ru-RU')} ₽
                                </button>
                              )
                            }
                            if ([9, 11, 4].includes(c.status)) {
                              return (
                                <button
                                  onClick={() => setBudgetModal(c)}
                                  className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium text-violet-600 dark:text-violet-400 hover:bg-violet-500/10 transition-colors"
                                  title="Пополнить бюджет"
                                >
                                  <Plus className="w-3 h-3" /> Пополнить
                                </button>
                              )
                            }
                            return <span className="text-[hsl(var(--muted-foreground))] text-xs">—</span>
                          })()}
                        </td>

                        {/* Stats columns */}
                        <td className="px-3 py-2.5 text-right text-sm font-medium text-[hsl(var(--foreground))] whitespace-nowrap">
                          {c.spend > 0 ? formatMoney(c.spend) : <span className="text-[hsl(var(--muted-foreground))]">0 ₽</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm font-medium text-[hsl(var(--foreground))] whitespace-nowrap">
                          {c.revenue > 0 ? formatMoney(c.revenue) : <span className="text-[hsl(var(--muted-foreground))]">0 ₽</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                          {c.views > 0 ? formatNum(c.views) : <span className="text-[hsl(var(--muted-foreground))]">0</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                          {c.clicks > 0 ? formatNum(c.clicks) : <span className="text-[hsl(var(--muted-foreground))]">0</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                          {c.ctr > 0 ? `${c.ctr.toFixed(2)}%` : <span className="text-[hsl(var(--muted-foreground))]">0%</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                          {c.cart > 0 ? formatNum(c.cart) : <span className="text-[hsl(var(--muted-foreground))]">0</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                          {c.orders > 0 ? formatNum(c.orders) : <span className="text-[hsl(var(--muted-foreground))]">0</span>}
                        </td>
                        <td className={`px-3 py-2.5 text-right text-sm font-semibold whitespace-nowrap ${drrColor}`}>
                          {c.drr > 0 ? `${c.drr.toFixed(1)}%` : <span className="text-[hsl(var(--muted-foreground))]">0%</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                          {c.cpc > 0 ? `${Math.round(c.cpc)} ₽` : <span className="text-[hsl(var(--muted-foreground))]">—</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                          {c.cpm > 0 ? `${Math.round(c.cpm)} ₽` : <span className="text-[hsl(var(--muted-foreground))]">—</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                          {c.cpa_cart > 0 ? `${Math.round(c.cpa_cart)} ₽` : <span className="text-[hsl(var(--muted-foreground))]">—</span>}
                        </td>
                        <td className="px-3 py-2.5 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                          {c.cpo > 0 ? `${Math.round(c.cpo)} ₽` : <span className="text-[hsl(var(--muted-foreground))]">—</span>}
                        </td>
                      </tr>

                      {/* Expanded row removed — replaced by management modal */}
                    </React.Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>

          {/* Footer count */}
          <div className="px-4 py-2.5 border-t border-[hsl(var(--border))] flex items-center justify-between text-xs text-[hsl(var(--muted-foreground))]">
            <span>Показано {filtered.length} из {campaigns.length} кампаний</span>
            {kpi && (
              <span>
                Расход: {formatMoney(kpi.spend)} · Заказы: {formatNum(kpi.orders)} · ДРР: {kpi.drr.toFixed(1)}%
              </span>
            )}
          </div>
        </div>
      )}

      {/* ── Budget Modal ───────────────────────────────────────── */}
      <AnimatePresence>
        {budgetModal && (
          <BudgetDepositModal
            campaign={budgetModal}
            shopId={shopId}
            balance={accountBalance}
            onClose={() => setBudgetModal(null)}
            onSuccess={(depositedAmount, newBudgetTotal, campaignStarted) => {
              const cid = budgetModal.advert_id
              // Use real budget total from WB API (returned by backend after deposit)
              // Fallback to optimistic calculation only if backend couldn't fetch it
              const actualTotal = newBudgetTotal != null
                ? newBudgetTotal
                : (budgets[cid]?.total || budgetModal.budget_total || 0) + depositedAmount
              // Track this deposit so loadFullData won't overwrite with stale Redis data
              setRecentDeposit(cid, actualTotal)
              // Update budget display with real value from WB API
              setBudgets(prev => ({
                ...prev,
                [cid]: {
                  total: actualTotal,
                  daily: prev[cid]?.daily || 0,
                  loading: false,
                },
              }))
              // If campaign was auto-started, optimistically update status to Active (9)
              if (campaignStarted) {
                setData(prev => prev ? {
                  ...prev,
                  campaigns: prev.campaigns.map(c =>
                    c.advert_id === cid ? { ...c, status: 9, status_label: 'Активна' } : c
                  ),
                } : prev)
              }
              setBudgetModal(null)
              // No loadFullData() here — it would overwrite with stale Redis.
              // Budget will sync naturally on next Celery cycle (every 15 min).
            }}
          />
        )}
      </AnimatePresence>

      {/* ── Unified Campaign Modal (Management + Analytics) ─────── */}
      <AnimatePresence>
        {unifiedModal && (
          <CampaignUnifiedModal
            campaign={unifiedModal.campaign}
            shopId={shopId}
            marketplace={marketplace || 'wildberries'}
            initialTab={unifiedModal.tab}
            onClose={() => setUnifiedModal(null)}
            onCampaignUpdate={(updated) => {
              setData(prev => prev ? {
                ...prev,
                campaigns: prev.campaigns.map(c =>
                  c.advert_id === updated.advert_id ? { ...c, ...updated } : c
                ),
              } : prev)
              setUnifiedModal(prev => prev ? { ...prev, campaign: updated } : null)
            }}
          />
        )}
      </AnimatePresence>

      {/* ── Create Campaign Modal ─────────────────────────────────── */}
      <AnimatePresence>
        {showCreateModal && (
          <CreateCampaignModal
            shopId={shopId}
            balance={accountBalance}
            onClose={() => setShowCreateModal(false)}
            onSuccess={(_advertId, _started) => {
              setShowCreateModal(false)
              loadFullData()  // Refresh campaign list
            }}
          />
        )}
      </AnimatePresence>
    </div>
  )
}
