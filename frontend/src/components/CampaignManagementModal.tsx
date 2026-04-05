/**
 * CampaignManagementModal — Full management popup for WB advertising campaigns.
 *
 * For UWB campaigns (bid_type=manual, payment_type=cpm):
 *   - Tab 1: Clusters (unified active + excluded with toggle, inline bid editing)
 *   - Tab 2: Products with bids per nm_id + add/remove
 *
 * For other types: simplified view (single bid / product bids only).
 *
 * Key feature: each cluster has an "Active/Excluded" toggle that
 * atomically calls set-minus on the WB API.
 */
import React, { useState, useEffect, useMemo, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  X, Loader2, Play, Pause, Target,
  Package, ChevronDown, ChevronUp, ChevronsUpDown,
  Check, AlertTriangle, Plus, Trash2,
  Save, RefreshCw, ToggleLeft, ToggleRight,
  Calendar, Search, History, ArrowUpRight, ArrowDownRight,
  Wallet, Zap,
} from 'lucide-react'
import {
  getClusterList, getClusterListCached, toggleClusterExclusion,
  setNormqueryBids, manageCampaignNms,
  changeBids, startCampaign, pauseCampaign,
  formatNum, depositBudget,
  getAutoBudgetSettings, saveAutoBudgetSettings,
  getCreationSubjects, getCreationProducts,
  type EnrichedCampaign, type NormqueryClusterStat,
  type ClusterListResponse, type ProductItem,
  type AutoBudgetSettings,
} from '../api/ad-management'
import { getCampaignEvents, type CampaignEventRow } from '../api/campaignDetails'

// ── Constants ─────────────────────────────────────────────────────

const STATUS_LABELS: Record<number, string> = {
  9: 'Активна', 11: 'На паузе', 4: 'Готова', 7: 'Завершена', 8: 'Отказ', [-1]: 'Удалена',
}
const STATUS_COLORS: Record<number, { bg: string; text: string; dot: string }> = {
  9: { bg: 'bg-emerald-500/10', text: 'text-emerald-600 dark:text-emerald-400', dot: 'bg-emerald-500' },
  11: { bg: 'bg-amber-500/10', text: 'text-amber-600 dark:text-amber-400', dot: 'bg-amber-500' },
  4: { bg: 'bg-blue-500/10', text: 'text-blue-600 dark:text-blue-400', dot: 'bg-blue-500' },
  7: { bg: 'bg-zinc-500/10', text: 'text-zinc-500', dot: 'bg-zinc-500' },
  8: { bg: 'bg-red-500/10', text: 'text-red-500', dot: 'bg-red-500' },
  [-1]: { bg: 'bg-red-500/10', text: 'text-red-500', dot: 'bg-red-500' },
}

type ClusterFilter = 'all' | 'active' | 'excluded'

interface Props {
  campaign: EnrichedCampaign
  shopId: number
  onClose: () => void
  onCampaignUpdate?: (campaign: EnrichedCampaign) => void
  /** When true, renders only internal content without overlay/header (for embedding in unified modal) */
  embedded?: boolean
}

// ── Clusters Sort ─────────────────────────────────────────────────

type ClusterSortKey = 'norm_query' | 'status' | 'views' | 'clicks' | 'ctr' | 'atbs' | 'orders' |
  'avg_pos' | 'cpc_rub' | 'cpm_rub' | 'spend_rub' | 'current_bid_rub' | 'cr_click_to_cart' | 'cr_click_to_order'

// ══════════════════════════════════════════════════════════════════
// Main Component
// ══════════════════════════════════════════════════════════════════

export default function CampaignManagementModal({ campaign, shopId, onClose, onCampaignUpdate, embedded = false }: Props) {
  const hasClusters = campaign.payment_type === 'cpm'
  const canEditClusterBids = campaign.bid_type === 'manual' && hasClusters

  const [showAddProduct, setShowAddProduct] = useState(false)
  const [loading, setLoading] = useState(false)
  const [actionLoading, setActionLoading] = useState<string | null>(null)
  const [toast, setToast] = useState<{ type: 'success' | 'error'; message: string } | null>(null)

  // Cluster data from unified endpoint
  const [clusterData, setClusterData] = useState<ClusterListResponse | null>(null)
  const [clusterError, setClusterError] = useState('')

  // Bid editing state: norm_query → new bid in kopecks
  const [editingBids, setEditingBids] = useState<Record<string, number>>({})
  const [savingBids, setSavingBids] = useState(false)

  // Cluster filter
  const [clusterFilter, setClusterFilter] = useState<ClusterFilter>('all')

  // Toggling state: norm_query → true while loading
  const [togglingClusters, setTogglingClusters] = useState<Record<string, boolean>>({})

  // Cluster sort
  const [clusterSort, setClusterSort] = useState<ClusterSortKey>('views')
  const [clusterSortDir, setClusterSortDir] = useState<'asc' | 'desc'>('desc')

  // Date range — configurable period
  type ClusterPeriod = 7 | 14 | 30
  const [clusterPeriod, setClusterPeriod] = useState<ClusterPeriod>(14)

  // Budget inline deposit
  const [depositAmount, setDepositAmount] = useState('')
  const [depositLoading, setDepositLoading] = useState(false)

  // Auto-budget settings
  const [autoBudget, setAutoBudget] = useState<AutoBudgetSettings | null>(null)

  const [autoBudgetSaving, setAutoBudgetSaving] = useState(false)
  const [abDraft, setAbDraft] = useState<Partial<AutoBudgetSettings>>({})

  // Load auto-budget settings
  useEffect(() => {
    getAutoBudgetSettings(shopId, campaign.advert_id)
      .then(data => {
        setAutoBudget(data)
        setAbDraft(data)
      })
      .catch(() => {})
  }, [shopId, campaign.advert_id])

  const currentBudget = campaign.budget_total ?? 0

  const handleDeposit = async () => {
    const amt = parseInt(depositAmount)
    if (!amt || amt <= 0) return
    setDepositLoading(true)
    try {
      const response = await depositBudget(shopId, campaign.advert_id, amt)
      // Use real balance from WB API when available, fallback to optimistic
      const newTotal = response.new_budget_total ?? (currentBudget + amt)
      onCampaignUpdate?.({ ...campaign, budget_total: newTotal })
      setToast({ type: 'success', message: `Бюджет пополнен на ${amt.toLocaleString('ru-RU')} ₽` })
      setDepositAmount('')
    } catch (e: any) {
      setToast({ type: 'error', message: e?.response?.data?.detail || 'Ошибка пополнения' })
    } finally {
      setDepositLoading(false)
    }
  }

  const handleSaveAutoBudget = async () => {
    setAutoBudgetSaving(true)
    try {
      await saveAutoBudgetSettings(shopId, campaign.advert_id, abDraft)
      setAutoBudget(prev => ({ ...prev, ...abDraft } as AutoBudgetSettings))
      setToast({ type: 'success', message: abDraft.enabled ? 'Автопополнение включено' : 'Автопополнение выключено' })
    } catch (e: any) {
      setToast({ type: 'error', message: e?.response?.data?.detail || 'Ошибка сохранения' })
    } finally {
      setAutoBudgetSaving(false)
    }
  }

  const dateRange = useMemo(() => {
    const end = new Date()
    const start = new Date()
    start.setDate(end.getDate() - (clusterPeriod - 1))
    const fmt = (d: Date) => d.toISOString().split('T')[0]
    return { start: fmt(start), end: fmt(end) }
  }, [clusterPeriod])

  // ── Load cluster data ──────────────────────────────────────────

  const firstNmId = campaign.nm_settings?.[0]?.nm_id || 0
  const [dataSource, setDataSource] = useState<'clickhouse' | 'api' | ''>('')

  const loadClusters = useCallback(async (forceLive = false) => {
    if (!hasClusters || !firstNmId) return
    setLoading(true)
    setClusterError('')
    try {
      const fetcher = forceLive ? getClusterList : getClusterListCached
      const data = await fetcher(shopId, campaign.advert_id, firstNmId, dateRange.start, dateRange.end)
      setClusterData(data)
      setDataSource((data as any).source === 'clickhouse' ? 'clickhouse' : 'api')
    } catch (e: any) {
      setClusterError(e?.response?.data?.detail || 'Ошибка загрузки данных кластеров')
    } finally {
      setLoading(false)
    }
  }, [shopId, campaign.advert_id, firstNmId, dateRange, hasClusters])

  useEffect(() => {
    loadClusters()
  }, [loadClusters])



  // ── Toast auto-hide ──────────────────────────────────────────────

  useEffect(() => {
    if (toast) {
      const t = setTimeout(() => setToast(null), 4000)
      return () => clearTimeout(t)
    }
  }, [toast])

  // ── Campaign status actions ──────────────────────────────────────

  const handleStatusAction = async (action: 'start' | 'pause') => {
    // Guardrail: block start with zero budget
    if (action === 'start' && currentBudget <= 0) {
      setToast({ type: 'error', message: '⚠️ Невозможно запустить: бюджет кампании = 0 ₽. Пополните бюджет.' })
      return
    }
    setActionLoading(action)
    try {
      if (action === 'start') await startCampaign(shopId, campaign.advert_id)
      else await pauseCampaign(shopId, campaign.advert_id)
      const newStatus = action === 'start' ? 9 : 11
      onCampaignUpdate?.({ ...campaign, status: newStatus })
      setToast({ type: 'success', message: action === 'start' ? 'Кампания запущена' : 'Кампания на паузе' })
    } catch (e: any) {
      setToast({ type: 'error', message: e?.response?.data?.detail || 'Ошибка' })
    } finally {
      setActionLoading(null)
    }
  }

  // ── Toggle cluster exclusion ─────────────────────────────────────

  const handleToggleCluster = async (normQuery: string, currentStatus: 'active' | 'excluded') => {
    if (!firstNmId) return
    const action = currentStatus === 'active' ? 'exclude' : 'include'
    setTogglingClusters(prev => ({ ...prev, [normQuery]: true }))
    try {
      await toggleClusterExclusion(shopId, campaign.advert_id, firstNmId, normQuery, action)
      // Optimistic update
      setClusterData(prev => {
        if (!prev) return prev
        return {
          ...prev,
          clusters: prev.clusters.map(c =>
            c.norm_query === normQuery
              ? { ...c, status: action === 'exclude' ? 'excluded' as const : 'active' as const }
              : c
          ),
          total_active: prev.total_active + (action === 'exclude' ? -1 : 1),
          total_excluded: prev.total_excluded + (action === 'exclude' ? 1 : -1),
        }
      })
      setToast({
        type: 'success',
        message: action === 'exclude'
          ? `«${normQuery}» исключён из показов`
          : `«${normQuery}» включён в показы`,
      })
    } catch (e: any) {
      setToast({ type: 'error', message: e?.response?.data?.detail || 'Ошибка переключения кластера' })
    } finally {
      setTogglingClusters(prev => {
        const next = { ...prev }
        delete next[normQuery]
        return next
      })
    }
  }

  // ── Save cluster bids ────────────────────────────────────────────

  const changedBidsCount = Object.keys(editingBids).length

  const handleSaveBids = async () => {
    if (changedBidsCount === 0 || !firstNmId) return
    setSavingBids(true)
    try {
      const bids = Object.entries(editingBids).map(([nq, bid]) => ({
        norm_query: nq,
        bid,
      }))
      await setNormqueryBids(shopId, campaign.advert_id, firstNmId, bids)
      setToast({ type: 'success', message: `Сохранено ${bids.length} ставок` })
      setEditingBids({})
      // Refresh data
      await loadClusters()
    } catch (e: any) {
      setToast({ type: 'error', message: e?.response?.data?.detail || 'Ошибка сохранения ставок' })
    } finally {
      setSavingBids(false)
    }
  }

  // ── Sorted & filtered clusters ──────────────────────────────────

  const sortedClusters = useMemo(() => {
    if (!clusterData?.clusters) return []
    let list = [...clusterData.clusters]

    // Filter
    if (clusterFilter !== 'all') {
      list = list.filter(c => c.status === clusterFilter)
    }

    // Sort
    list.sort((a, b) => {
      let va: any, vb: any
      if (clusterSort === 'norm_query') {
        va = a.norm_query; vb = b.norm_query
        return clusterSortDir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va)
      }
      if (clusterSort === 'status') {
        va = a.status === 'active' ? 0 : 1
        vb = b.status === 'active' ? 0 : 1
        return clusterSortDir === 'asc' ? va - vb : vb - va
      }
      va = (a as any)[clusterSort] || 0
      vb = (b as any)[clusterSort] || 0
      return clusterSortDir === 'asc' ? va - vb : vb - va
    })
    return list
  }, [clusterData?.clusters, clusterSort, clusterSortDir, clusterFilter])

  const handleClusterSort = (key: ClusterSortKey) => {
    if (clusterSort === key) {
      setClusterSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setClusterSort(key)
      setClusterSortDir('desc')
    }
  }

  // ── Cluster bid color logic ──────────────────────────────────────

  const getBidColor = (cluster: NormqueryClusterStat, bidKopecks: number) => {
    if (bidKopecks <= 0) return ''
    if (cluster.reach_min_bid > 0 && bidKopecks < cluster.reach_min_bid) return 'text-red-500'
    if (cluster.reach_max_bid > 0 && bidKopecks > cluster.reach_max_bid) return 'text-amber-500'
    if (cluster.reach_med_bid > 0 && bidKopecks >= cluster.reach_med_bid) return 'text-emerald-500'
    return ''
  }

  // ── Render ───────────────────────────────────────────────────────

  const sc = STATUS_COLORS[campaign.status] || STATUS_COLORS[8]
  const ptLabel = campaign.payment_type === 'cpm' ? 'CPM' : campaign.payment_type === 'cpc' ? 'CPC' : campaign.payment_type?.toUpperCase() || '—'

  // ── Inner content (shared between standalone and embedded modes) ──
  const innerContent = (
    <>

        {/* ── Base bids bar (UWB only) ───────────────────────────── */}
        {hasClusters && clusterData?.base_bids && (clusterData.base_bids.competitive_rub || clusterData.base_bids.leaders_rub) && (
          <div className="px-6 py-2 border-b border-[hsl(var(--border))] flex items-center gap-6 text-xs bg-[hsl(var(--muted))]/10 shrink-0">
            <span className="text-[hsl(var(--muted-foreground))]">Ориентиры рынка:</span>
            {clusterData.base_bids.competitive_rub && clusterData.base_bids.competitive_rub > 0 && (
              <span className="text-[hsl(var(--foreground))]">
                Конкурентная: <span className="font-semibold text-blue-500">{clusterData.base_bids.competitive_rub} ₽</span>
              </span>
            )}
            {clusterData.base_bids.leaders_rub && clusterData.base_bids.leaders_rub > 0 && (
              <span className="text-[hsl(var(--foreground))]">
                Лидерская: <span className="font-semibold text-violet-500">{clusterData.base_bids.leaders_rub} ₽</span>
              </span>
            )}
            <span className="text-[hsl(var(--muted-foreground))]">
              Активных: <span className="font-medium text-emerald-500">{clusterData.total_active}</span>
              {' / '}
              Исключённых: <span className="font-medium text-red-400">{clusterData.total_excluded}</span>
            </span>
          </div>
        )}



        {/* ── Scrollable Content ──────────────────────────────── */}
        <div className="flex-1 overflow-auto">
          {/* Toast */}
          <AnimatePresence>
            {toast && (
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                className={`mx-6 mt-3 px-4 py-2.5 rounded-lg text-sm font-medium flex items-center gap-2
                  ${toast.type === 'success' ? 'bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border border-emerald-500/20' :
                    'bg-red-500/10 text-red-500 border border-red-500/20'}`}
              >
                {toast.type === 'success' ? <Check className="w-4 h-4" /> : <AlertTriangle className="w-4 h-4" />}
                {toast.message}
              </motion.div>
            )}
          </AnimatePresence>

          {/* ── Unified Budget Card ────────────────────────────── */}
          <div className="mx-6 mt-4 mb-2 rounded-xl border border-[hsl(var(--border))] bg-gradient-to-br from-[hsl(var(--card))] to-[hsl(var(--muted))]/10 overflow-hidden">
            {/* Budget top row: balance + deposit */}
            <div className="px-5 pt-4 pb-3">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2.5">
                  <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${
                    currentBudget <= 0 ? 'bg-red-500/15' : currentBudget < 500 ? 'bg-amber-500/15' : 'bg-emerald-500/15'
                  }`}>
                    <Wallet className={`w-4 h-4 ${
                      currentBudget <= 0 ? 'text-red-500' : currentBudget < 500 ? 'text-amber-500' : 'text-emerald-500'
                    }`} />
                  </div>
                  <div>
                    <div className="text-[11px] text-[hsl(var(--muted-foreground))] leading-none mb-0.5">Бюджет кампании</div>
                    <div className={`text-xl font-bold tracking-tight ${
                      currentBudget <= 0 ? 'text-red-500' : currentBudget < 500 ? 'text-amber-500' : 'text-[hsl(var(--foreground))]'
                    }`}>
                      {currentBudget.toLocaleString('ru-RU')} <span className="text-sm font-medium text-[hsl(var(--muted-foreground))]">₽</span>
                    </div>
                  </div>
                  {autoBudget?.enabled && (
                    <span className="flex items-center gap-1 text-[10px] font-semibold px-2 py-0.5 rounded-full bg-violet-500/12 text-violet-500 border border-violet-500/20 ml-1">
                      <Zap className="w-2.5 h-2.5" />
                      Авто
                    </span>
                  )}
                </div>

                {/* Deposit inline */}
                <div className="flex items-center gap-2">
                  <div className="relative">
                    <input
                      type="text"
                      inputMode="numeric"
                      value={depositAmount}
                      onChange={e => {
                        const v = e.target.value.replace(/[^\d]/g, '')
                        setDepositAmount(v)
                      }}
                      placeholder="Сумма"
                      className="w-28 pl-3 pr-8 py-2 rounded-lg bg-[hsl(var(--muted))]/20 border border-[hsl(var(--border))] text-[hsl(var(--foreground))] text-sm outline-none focus:border-violet-500 focus:ring-1 focus:ring-violet-500/30 transition-all [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
                      onKeyDown={e => e.key === 'Enter' && handleDeposit()}
                    />
                    <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-[hsl(var(--muted-foreground))] pointer-events-none">₽</span>
                  </div>
                  <button
                    onClick={handleDeposit}
                    disabled={depositLoading || !depositAmount}
                    className="flex items-center gap-1.5 px-4 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 active:bg-violet-700 text-white text-sm font-medium transition-all disabled:opacity-40 disabled:cursor-not-allowed shadow-sm shadow-violet-600/20"
                  >
                    {depositLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Plus className="w-3.5 h-3.5" />}
                    Пополнить
                  </button>
                </div>
              </div>

              {/* Budget level indicator bar */}
              {(() => {
                const threshold = abDraft.threshold ?? 500
                const maxDisplay = Math.max(currentBudget, threshold * 3, 2000)
                const budgetPct = Math.min((currentBudget / maxDisplay) * 100, 100)
                const thresholdPct = abDraft.enabled ? Math.min((threshold / maxDisplay) * 100, 100) : 0
                const barColor = currentBudget <= 0 ? 'bg-red-500' : currentBudget < threshold ? 'bg-amber-500' : 'bg-emerald-500'

                return (
                  <div className="relative h-1.5 rounded-full bg-[hsl(var(--muted))]/30 overflow-hidden">
                    <motion.div
                      initial={{ width: 0 }}
                      animate={{ width: `${budgetPct}%` }}
                      transition={{ duration: 0.6, ease: 'easeOut' }}
                      className={`absolute inset-y-0 left-0 rounded-full ${barColor}`}
                    />
                    {abDraft.enabled && thresholdPct > 0 && (
                      <div
                        className="absolute top-[-3px] w-0.5 h-[calc(100%+6px)] bg-amber-400/70"
                        style={{ left: `${thresholdPct}%` }}
                        title={`Порог: ${threshold.toLocaleString('ru-RU')} ₽`}
                      />
                    )}
                  </div>
                )
              })()}
            </div>

            {/* Auto-budget divider + section */}
            <div className="border-t border-[hsl(var(--border))]/50">
              <div
                className="flex items-center justify-between px-5 py-2.5 cursor-pointer hover:bg-[hsl(var(--muted))]/5 transition-colors select-none"
                onClick={() => {
                  if (!autoBudget) return
                  setAbDraft(p => ({ ...p, enabled: !p.enabled }))
                }}
              >
                <div className="flex items-center gap-2">
                  <Zap className={`w-3.5 h-3.5 ${abDraft.enabled ? 'text-violet-500' : 'text-[hsl(var(--muted-foreground))]'}`} />
                  <span className="text-[13px] font-medium text-[hsl(var(--foreground))]">Автопополнение</span>
                  {abDraft.enabled && autoBudget && autoBudget.deposits_today > 0 && (
                    <span className="text-[10px] text-[hsl(var(--muted-foreground))] ml-1">
                      сегодня: {autoBudget.deposits_today}×
                    </span>
                  )}
                </div>
                <button
                  className={`relative w-9 h-5 rounded-full transition-colors shrink-0 ${abDraft.enabled ? 'bg-violet-600' : 'bg-[hsl(var(--muted))]/50'}`}
                  onClick={e => { e.stopPropagation(); if (!autoBudget) return; setAbDraft(p => ({ ...p, enabled: !p.enabled })) }}
                >
                  <span className={`absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white shadow-sm transition-transform duration-200 ${abDraft.enabled ? 'translate-x-4' : ''}`} />
                </button>
              </div>

              <AnimatePresence>
                {abDraft.enabled && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.2 }}
                    className="overflow-hidden"
                  >
                    <div className="px-5 pb-4 pt-1">
                      <div className="grid grid-cols-3 gap-3">
                        <div>
                          <label className="text-[11px] text-[hsl(var(--muted-foreground))] mb-1.5 block">Порог пополнения</label>
                          <div className="relative">
                            <input
                              type="text"
                              inputMode="numeric"
                              value={abDraft.threshold ?? 500}
                              onChange={e => {
                                const v = e.target.value.replace(/[^\d]/g, '')
                                setAbDraft(p => ({ ...p, threshold: parseInt(v) || 0 }))
                              }}
                              className="w-full pl-3 pr-7 py-2 rounded-lg bg-[hsl(var(--muted))]/15 border border-[hsl(var(--border))] text-[hsl(var(--foreground))] text-sm outline-none focus:border-violet-500 focus:ring-1 focus:ring-violet-500/30 transition-all [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
                            />
                            <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-[hsl(var(--muted-foreground))] pointer-events-none">₽</span>
                          </div>
                        </div>
                        <div>
                          <label className="text-[11px] text-[hsl(var(--muted-foreground))] mb-1.5 block">Сумма пополнения</label>
                          <div className="relative">
                            <input
                              type="text"
                              inputMode="numeric"
                              value={abDraft.amount ?? 1000}
                              onChange={e => {
                                const v = e.target.value.replace(/[^\d]/g, '')
                                setAbDraft(p => ({ ...p, amount: parseInt(v) || 0 }))
                              }}
                              className="w-full pl-3 pr-7 py-2 rounded-lg bg-[hsl(var(--muted))]/15 border border-[hsl(var(--border))] text-[hsl(var(--foreground))] text-sm outline-none focus:border-violet-500 focus:ring-1 focus:ring-violet-500/30 transition-all [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
                            />
                            <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-[hsl(var(--muted-foreground))] pointer-events-none">₽</span>
                          </div>
                        </div>
                        <div>
                          <label className="text-[11px] text-[hsl(var(--muted-foreground))] mb-1.5 block">Макс. в день</label>
                          <div className="relative">
                            <input
                              type="text"
                              inputMode="numeric"
                              value={abDraft.max_per_day ?? 5}
                              onChange={e => {
                                const v = e.target.value.replace(/[^\d]/g, '')
                                setAbDraft(p => ({ ...p, max_per_day: parseInt(v) || 1 }))
                              }}
                              className="w-full pl-3 pr-8 py-2 rounded-lg bg-[hsl(var(--muted))]/15 border border-[hsl(var(--border))] text-[hsl(var(--foreground))] text-sm outline-none focus:border-violet-500 focus:ring-1 focus:ring-violet-500/30 transition-all [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none"
                            />
                            <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-[hsl(var(--muted-foreground))] pointer-events-none">раз</span>
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center justify-between mt-3">
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
                          {autoBudget && autoBudget.deposits_today > 0 && (
                            <span>
                              Пополнено {autoBudget.deposits_today} раз
                              {autoBudget.last_deposit_at && (
                                <span className="ml-1">· {new Date(autoBudget.last_deposit_at).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}</span>
                              )}
                            </span>
                          )}
                        </div>
                        <button
                          onClick={handleSaveAutoBudget}
                          disabled={autoBudgetSaving}
                          className="flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-xs font-medium transition-colors disabled:opacity-50 shadow-sm shadow-violet-600/20"
                        >
                          {autoBudgetSaving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Check className="w-3.5 h-3.5" />}
                          Сохранить
                        </button>
                      </div>
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>

              {/* Confirm disable when was enabled */}
              {!abDraft.enabled && autoBudget?.enabled && (
                <div className="px-5 pb-3 flex justify-end">
                  <button
                    onClick={handleSaveAutoBudget}
                    disabled={autoBudgetSaving}
                    className="flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-red-500/10 hover:bg-red-500/20 text-red-500 text-xs font-medium transition-colors disabled:opacity-50"
                  >
                    {autoBudgetSaving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <X className="w-3.5 h-3.5" />}
                    Выключить
                  </button>
                </div>
              )}
            </div>
          </div>

          {/* ── Products Section ────────────────────────────────── */}
          <div className="px-6 pt-4 pb-2 border-t border-[hsl(var(--border))]/50">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <Package className="w-4 h-4 text-violet-500" />
                <h3 className="text-sm font-semibold text-[hsl(var(--foreground))]">Товары</h3>
                <span className="text-[10px] font-bold px-1.5 py-0.5 rounded-full bg-violet-500/15 text-violet-600 dark:text-violet-400">
                  {campaign.nm_settings?.length || 0}
                </span>
              </div>
              <button
                onClick={() => setShowAddProduct(true)}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-violet-600/10 hover:bg-violet-600/20 text-violet-600 dark:text-violet-400 text-xs font-medium transition-colors"
              >
                <Plus className="w-3.5 h-3.5" />
                Добавить товар
              </button>
            </div>

            <ProductsSection
              campaign={campaign}
              shopId={shopId}
              onToast={setToast}
              onCampaignUpdate={onCampaignUpdate}
              baseBids={hasClusters ? clusterData?.base_bids : undefined}
              advertId={campaign.advert_id}
            />
          </div>

          {/* ── Clusters Section ────────────────────────────────── */}
          {hasClusters && (
            <div className="border-t border-[hsl(var(--border))]/50 mt-2">
              {/* Clusters header + controls */}
              <div className="px-6 pt-4 pb-2">
                <div className="flex items-center gap-2 mb-2">
                  <Target className="w-4 h-4 text-violet-500" />
                  <h3 className="text-sm font-semibold text-[hsl(var(--foreground))]">Кластеры</h3>
                  {clusterData && (
                    <span className="text-xs text-[hsl(var(--muted-foreground))]">
                      <span className="text-emerald-500 font-medium">{clusterData.total_active}</span> активных
                      {clusterData.total_excluded > 0 && (
                        <> / <span className="text-red-400 font-medium">{clusterData.total_excluded}</span> исключённых</>
                      )}
                    </span>
                  )}
                  <div className="flex-1" />
                  <button
                    onClick={() => loadClusters(true)}
                    disabled={loading}
                    className="p-1.5 rounded-lg hover:bg-[hsl(var(--muted))]/30 text-[hsl(var(--muted-foreground))] transition-colors disabled:opacity-50"
                    title={dataSource === 'clickhouse' ? 'Обновить из WB API' : 'Обновить данные'}
                  >
                    <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
                  </button>
                  {dataSource && (
                    <span className={`text-[10px] font-medium px-1.5 py-0.5 rounded ${
                      dataSource === 'clickhouse'
                        ? 'bg-blue-500/10 text-blue-400'
                        : 'bg-emerald-500/10 text-emerald-400'
                    }`}>
                      {dataSource === 'clickhouse' ? 'кеш' : 'live'}
                    </span>
                  )}
                </div>

                {/* Period + Filter + Save bids */}
                <div className="flex items-center gap-2">
                  <div className="flex items-center gap-0.5 bg-[hsl(var(--muted))]/20 rounded-lg p-0.5">
                    <Calendar className="w-3 h-3 text-[hsl(var(--muted-foreground))] ml-1.5 mr-0.5" />
                    {([7, 14, 30] as const).map(p => (
                      <button
                        key={p}
                        onClick={() => setClusterPeriod(p)}
                        className={`px-2 py-1 rounded-md text-[11px] font-medium transition-colors
                          ${clusterPeriod === p
                            ? 'bg-violet-600 text-white shadow-sm'
                            : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))]/40'}`}
                      >
                        {p}д
                      </button>
                    ))}
                  </div>

                  {clusterData && (
                    <div className="flex items-center gap-1">
                      <FilterButton active={clusterFilter === 'all'} onClick={() => setClusterFilter('all')} label="Все" />
                      <FilterButton active={clusterFilter === 'active'} onClick={() => setClusterFilter('active')} label="Активные" color="emerald" />
                      <FilterButton active={clusterFilter === 'excluded'} onClick={() => setClusterFilter('excluded')} label="Исключённые" color="red" />
                    </div>
                  )}

                  <div className="flex-1" />

                  {changedBidsCount > 0 && (
                    <button
                      onClick={handleSaveBids}
                      disabled={savingBids}
                      className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-xs font-medium transition-colors disabled:opacity-50"
                    >
                      {savingBids ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
                      Сохранить {changedBidsCount} {changedBidsCount === 1 ? 'ставку' : changedBidsCount < 5 ? 'ставки' : 'ставок'}
                    </button>
                  )}
                </div>
              </div>

              {/* Loading */}
              {loading && !clusterData && (
                <div className="flex items-center justify-center h-32 gap-3 text-[hsl(var(--muted-foreground))]">
                  <Loader2 className="w-5 h-5 animate-spin" />
                  Загрузка данных кластеров...
                </div>
              )}

              {/* Error */}
              {clusterError && (
                <div className="mx-6 mb-4 p-4 rounded-xl bg-red-500/10 border border-red-500/20 text-red-500 text-sm">
                  {clusterError}
                </div>
              )}

              {/* Clusters table */}
              {clusterData && (
                <ClustersTab
                  clusters={sortedClusters}
                  editingBids={editingBids}
                  setEditingBids={setEditingBids}
                  sortKey={clusterSort}
                  sortDir={clusterSortDir}
                  onSort={handleClusterSort}
                  getBidColor={getBidColor}
                  onToggle={handleToggleCluster}
                  togglingClusters={togglingClusters}
                  loading={loading}
                  canEditBids={canEditClusterBids}
                />
              )}
            </div>
          )}


        </div>

        {/* ── Add Product Sub-Modal ────────────────────────────── */}
        <AnimatePresence>
          {showAddProduct && (
            <AddProductSubModal
              shopId={shopId}
              campaign={campaign}
              onClose={() => setShowAddProduct(false)}
              onAdded={(addedNmIds) => {
                if (onCampaignUpdate && campaign.nm_settings) {
                  const existing = new Set(campaign.nm_settings.map(ns => ns.nm_id))
                  const newSettings = addedNmIds
                    .filter(nm => !existing.has(nm))
                    .map(nm => ({
                      nm_id: nm, bid_search: 0, bid_recommendations: 0,
                      subject_name: '', product_name: `Товар ${nm}`, vendor_code: '',
                    }))
                  onCampaignUpdate({
                    ...campaign,
                    nm_settings: [...campaign.nm_settings, ...newSettings],
                  })
                }
                setShowAddProduct(false)
              }}
              onToast={setToast}
            />
          )}
        </AnimatePresence>
    </>
  )

  // ── Embedded mode: just render content, no overlay ──
  if (embedded) {
    return (
      <div className="flex flex-col">
        {/* Status action bar in embedded mode */}
        <div className="flex items-center gap-2 px-6 py-2 border-b border-[hsl(var(--border))] shrink-0">
          {campaign.status === 9 && (
            <button
              onClick={() => handleStatusAction('pause')}
              disabled={actionLoading !== null}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-amber-500/10 hover:bg-amber-500/20 text-amber-600 dark:text-amber-400 text-sm font-medium transition-colors disabled:opacity-50"
            >
              {actionLoading === 'pause' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Pause className="w-4 h-4" />}
              Пауза
            </button>
          )}
          {(campaign.status === 11 || campaign.status === 4) && (
            <button
              onClick={() => handleStatusAction('start')}
              disabled={actionLoading !== null}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-500/10 hover:bg-emerald-500/20 text-emerald-600 dark:text-emerald-400 text-sm font-medium transition-colors disabled:opacity-50"
            >
              {actionLoading === 'start' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
              Запустить
            </button>
          )}
        </div>
        {innerContent}
      </div>
    )
  }

  // ── Standalone mode: full overlay + card + header ──
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
      onClick={onClose}
    >
      <motion.div
        initial={{ scale: 0.95, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        exit={{ scale: 0.95, opacity: 0 }}
        transition={{ type: 'spring', duration: 0.3 }}
        onClick={e => e.stopPropagation()}
        className="w-full max-w-[1200px] max-h-[90vh] bg-[hsl(var(--card))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl flex flex-col overflow-hidden"
      >
        {/* ── Header ─────────────────────────────────────────────── */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-[hsl(var(--border))] shrink-0">
          <div className="flex items-center gap-4 min-w-0">
            <div className="min-w-0">
              <div className="flex items-center gap-2.5 flex-wrap">
                <h2 className="text-lg font-bold text-[hsl(var(--foreground))] truncate max-w-[400px]">
                  {campaign.name || `Кампания ${campaign.advert_id}`}
                </h2>
                <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${sc.bg} ${sc.text}`}>
                  <span className={`w-1.5 h-1.5 rounded-full ${sc.dot}`} />
                  {STATUS_LABELS[campaign.status] || '—'}
                </span>
                <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold
                  ${ptLabel === 'CPM' ? 'bg-violet-500/15 text-violet-600 dark:text-violet-400' :
                    ptLabel === 'CPC' ? 'bg-cyan-500/15 text-cyan-600 dark:text-cyan-400' :
                      'bg-emerald-500/15 text-emerald-600 dark:text-emerald-400'}`}
                >
                  {ptLabel} {campaign.bid_type === 'manual' ? 'Ручная' : campaign.bid_type === 'unified' ? 'Единая' : ''}
                </span>
              </div>
              <div className="flex items-center gap-3 mt-1 text-xs text-[hsl(var(--muted-foreground))]">
                <span>ID: {campaign.advert_id}</span>
                {campaign.search_enabled && <span>🔍 Поиск</span>}
                {campaign.recommendations_enabled && <span>📦 Полки</span>}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            {campaign.status === 9 && (
              <button
                onClick={() => handleStatusAction('pause')}
                disabled={actionLoading !== null}
                className="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-amber-500/10 hover:bg-amber-500/20 text-amber-600 dark:text-amber-400 text-sm font-medium transition-colors disabled:opacity-50"
              >
                {actionLoading === 'pause' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Pause className="w-4 h-4" />}
                Пауза
              </button>
            )}
            {(campaign.status === 11 || campaign.status === 4) && (
              <button
                onClick={() => handleStatusAction('start')}
                disabled={actionLoading !== null}
                className="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-emerald-500/10 hover:bg-emerald-500/20 text-emerald-600 dark:text-emerald-400 text-sm font-medium transition-colors disabled:opacity-50"
              >
                {actionLoading === 'start' ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                Запустить
              </button>
            )}

            <button
              onClick={onClose}
              className="p-2 rounded-lg hover:bg-[hsl(var(--muted))]/30 text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        {innerContent}
      </motion.div>
    </motion.div>
  )
}




// ── Filter Button ──────────────────────────────────────────────────

function FilterButton({ active, onClick, label, color }: {
  active: boolean; onClick: () => void; label: string; color?: string
}) {
  const colorClass = color === 'emerald'
    ? 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-400 border-emerald-500/30'
    : color === 'red'
      ? 'bg-red-500/15 text-red-500 border-red-500/30'
      : 'bg-[hsl(var(--muted))]/50 text-[hsl(var(--foreground))] border-[hsl(var(--border))]'

  return (
    <button
      onClick={onClick}
      className={`px-2.5 py-1 rounded-lg text-[11px] font-medium border transition-colors
        ${active ? colorClass : 'border-transparent text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted))]/30'}`}
    >
      {label}
    </button>
  )
}


// ══════════════════════════════════════════════════════════════════
// Clusters Tab — Unified Active + Excluded with Toggle
// ══════════════════════════════════════════════════════════════════

function ClustersTab({ clusters, editingBids, setEditingBids, sortKey, sortDir, onSort, getBidColor, onToggle, togglingClusters, loading, canEditBids }: {
  clusters: NormqueryClusterStat[]
  editingBids: Record<string, number>
  setEditingBids: React.Dispatch<React.SetStateAction<Record<string, number>>>
  sortKey: ClusterSortKey
  sortDir: 'asc' | 'desc'
  onSort: (key: ClusterSortKey) => void
  getBidColor: (cluster: NormqueryClusterStat, bid: number) => string
  onToggle: (normQuery: string, status: 'active' | 'excluded') => void
  togglingClusters: Record<string, boolean>
  loading: boolean
  canEditBids: boolean
}) {
  return (
    <div className="max-h-[60vh] overflow-y-auto overflow-x-auto">
      <table className="w-full border-collapse" style={{ minWidth: 1100 }}>
        <thead className="sticky top-0 z-10 bg-[hsl(var(--card))]">
          <tr className="border-b border-[hsl(var(--border))]">
            <th className="px-3 py-2.5 text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider text-center whitespace-nowrap w-20 pl-4">
              Статус
            </th>
            <SortTh label="Кластер" sortKey="norm_query" current={sortKey} dir={sortDir} onSort={onSort} align="left" className="min-w-[200px]" />
            <SortTh label="Показы" sortKey="views" current={sortKey} dir={sortDir} onSort={onSort} />
            <SortTh label="Клики" sortKey="clicks" current={sortKey} dir={sortDir} onSort={onSort} />
            <SortTh label="Корзины" sortKey="atbs" current={sortKey} dir={sortDir} onSort={onSort} />
            <SortTh label="Заказы" sortKey="orders" current={sortKey} dir={sortDir} onSort={onSort} />
            <SortTh label="Затраты" sortKey="spend_rub" current={sortKey} dir={sortDir} onSort={onSort} />
            <SortTh label="CTR" sortKey="ctr" current={sortKey} dir={sortDir} onSort={onSort} />
            <SortTh label="CPM" sortKey="cpm_rub" current={sortKey} dir={sortDir} onSort={onSort} />
            <SortTh label="CPC" sortKey="cpc_rub" current={sortKey} dir={sortDir} onSort={onSort} />
            <SortTh label="CR→🛒" sortKey="cr_click_to_cart" current={sortKey} dir={sortDir} onSort={onSort} />
            <SortTh label="Позиция" sortKey="avg_pos" current={sortKey} dir={sortDir} onSort={onSort} />
            {canEditBids && (
              <th className="px-3 py-2.5 text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider text-right whitespace-nowrap">
                Ставка
              </th>
            )}
            {canEditBids && (
              <th className="px-3 py-2.5 text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider text-center whitespace-nowrap pr-4">
                Рекоменд.
              </th>
            )}
          </tr>
        </thead>
        <tbody>
          {clusters.length === 0 && !loading && (
            <tr>
              <td colSpan={12} className="px-6 py-12 text-center text-[hsl(var(--muted-foreground))] text-sm">
                Нет данных по кластерам за выбранный период
              </td>
            </tr>
          )}
          {clusters.map(c => {
            const isExcluded = c.status === 'excluded'
            const isToggling = togglingClusters[c.norm_query] === true
            const currentBid = editingBids[c.norm_query] ?? c.current_bid_kopecks
            const isEdited = editingBids[c.norm_query] !== undefined
            const bidColor = getBidColor(c, currentBid)

            return (
              <tr
                key={c.norm_query}
                className={`border-b border-[hsl(var(--border))]/30 hover:bg-[hsl(var(--muted))]/10 transition-colors group
                  ${isExcluded ? 'opacity-60' : ''}`}
              >
                {/* Toggle (Active/Excluded) */}
                <td className="px-3 py-2 pl-4 text-center">
                  <button
                    onClick={() => onToggle(c.norm_query, c.status)}
                    disabled={isToggling}
                    className={`inline-flex items-center gap-1 px-2 py-1 rounded-full text-[10px] font-bold transition-all
                      ${isToggling ? 'opacity-50 cursor-wait' : 'cursor-pointer'}
                      ${isExcluded
                        ? 'bg-red-500/10 text-red-500 hover:bg-red-500/20'
                        : 'bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 hover:bg-emerald-500/20'
                      }`}
                    title={isExcluded ? 'Включить в показы' : 'Исключить из показов'}
                  >
                    {isToggling ? (
                      <Loader2 className="w-3 h-3 animate-spin" />
                    ) : isExcluded ? (
                      <ToggleLeft className="w-3.5 h-3.5" />
                    ) : (
                      <ToggleRight className="w-3.5 h-3.5" />
                    )}
                    {isExcluded ? 'Выкл' : 'Вкл'}
                  </button>
                </td>
                {/* Cluster name */}
                <td className="px-3 py-2">
                  <span className={`text-sm font-medium ${isExcluded ? 'text-[hsl(var(--muted-foreground))] line-through' : 'text-[hsl(var(--foreground))]'}`}>
                    {c.norm_query}
                  </span>
                </td>
                {/* Views */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {formatNum(c.views)}
                </td>
                {/* Clicks */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {formatNum(c.clicks)}
                </td>
                {/* Cart */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {c.atbs > 0 ? formatNum(c.atbs) : '—'}
                </td>
                {/* Orders */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {c.orders > 0 ? formatNum(c.orders) : '—'}
                </td>
                {/* Spend */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {c.spend_rub > 0 ? `${formatNum(c.spend_rub)} ₽` : '—'}
                </td>
                {/* CTR */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {c.ctr > 0 ? `${c.ctr.toFixed(2)}%` : '—'}
                </td>
                {/* CPM */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {c.cpm_rub > 0 ? `${formatNum(c.cpm_rub)} ₽` : '—'}
                </td>
                {/* CPC */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {c.cpc_rub > 0 ? `${Math.round(c.cpc_rub)} ₽` : '—'}
                </td>
                {/* CR click→cart */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {c.cr_click_to_cart > 0 ? `${c.cr_click_to_cart.toFixed(1)}%` : '—'}
                </td>
                {/* Avg position */}
                <td className="px-3 py-2 text-right text-sm text-[hsl(var(--foreground))]/70 whitespace-nowrap">
                  {c.avg_pos > 0 ? c.avg_pos.toFixed(1) : '—'}
                </td>

                {/* Bid — editable (only for manual CPM) */}
                {canEditBids && (
                  <td className="px-3 py-2 text-right whitespace-nowrap">
                    {isExcluded ? (
                      <span className="text-sm text-[hsl(var(--muted-foreground))]">—</span>
                    ) : (
                      <BidInput
                        valueKopecks={currentBid}
                        isEdited={isEdited}
                        bidColor={bidColor}
                        onChange={(newKopecks) => {
                          setEditingBids(prev => {
                            if (newKopecks === c.current_bid_kopecks) {
                              const next = { ...prev }
                              delete next[c.norm_query]
                              return next
                            }
                            return { ...prev, [c.norm_query]: newKopecks }
                          })
                        }}
                      />
                    )}
                  </td>
                )}

                {/* Recommendations (only for manual CPM) */}
                {canEditBids && (
                  <td className="px-3 py-2 pr-4 text-center whitespace-nowrap">
                    {(c.reach_max_bid > 0 || c.reach_med_bid > 0 || c.reach_min_bid > 0) ? (
                      <div className="flex flex-col items-center gap-0.5">
                        {c.reach_max_bid > 0 && (
                          <span className="text-[10px] text-emerald-500" title="Максимальный охват">
                            ↑ {(c.reach_max_bid / 100).toFixed(0)} ₽
                          </span>
                        )}
                        {c.reach_med_bid > 0 && (
                          <span className="text-[10px] text-blue-500" title="Средний охват">
                            ● {(c.reach_med_bid / 100).toFixed(0)} ₽
                          </span>
                        )}
                        {c.reach_min_bid > 0 && (
                          <span className="text-[10px] text-[hsl(var(--muted-foreground))]" title="Минимальный охват">
                            ↓ {(c.reach_min_bid / 100).toFixed(0)} ₽
                          </span>
                        )}
                      </div>
                    ) : (
                      <span className="text-[hsl(var(--muted-foreground))] text-xs">—</span>
                    )}
                  </td>
                )}
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}



// ── Sort Table Header ──────────────────────────────────────────────

function SortTh({ label, sortKey, current, dir, onSort, align = 'right', className = '' }: {
  label: string; sortKey: ClusterSortKey; current: ClusterSortKey; dir: 'asc' | 'desc'
  onSort: (key: ClusterSortKey) => void; align?: 'left' | 'right' | 'center'; className?: string
}) {
  const active = sortKey === current
  return (
    <th
      onClick={() => onSort(sortKey)}
      className={`px-3 py-2.5 text-xs font-semibold uppercase tracking-wider whitespace-nowrap cursor-pointer select-none transition-colors
        ${active ? 'text-violet-600 dark:text-violet-400' : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'}
        text-${align} ${className}`}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        {active ? (
          dir === 'asc' ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />
        ) : (
          <ChevronsUpDown className="w-3 h-3 opacity-30" />
        )}
      </span>
    </th>
  )
}


// ── Bid Input (inline edit) ─────────────────────────────────────

function BidInput({ valueKopecks, isEdited, bidColor, onChange }: {
  valueKopecks: number; isEdited: boolean; bidColor: string
  onChange: (kopecks: number) => void
}) {
  const [editing, setEditing] = useState(false)
  const [inputVal, setInputVal] = useState('')

  const rubValue = valueKopecks > 0 ? (valueKopecks / 100).toFixed(0) : '0'

  const handleStartEdit = () => {
    setInputVal(rubValue)
    setEditing(true)
  }

  const handleConfirm = () => {
    const num = parseInt(inputVal, 10)
    if (!isNaN(num) && num > 0) {
      onChange(num * 100) // rubles → kopecks
    }
    setEditing(false)
  }

  if (editing) {
    return (
      <div className="inline-flex items-center gap-1">
        <input
          autoFocus
          type="number"
          min={1}
          value={inputVal}
          onChange={e => setInputVal(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') handleConfirm()
            if (e.key === 'Escape') setEditing(false)
          }}
          onBlur={handleConfirm}
          className="w-20 px-2 py-1 rounded-md bg-[hsl(var(--secondary))] border border-violet-500/50 text-[hsl(var(--foreground))] text-sm text-right font-medium focus:outline-none focus:ring-2 focus:ring-violet-500/30"
        />
        <span className="text-xs text-[hsl(var(--muted-foreground))]">₽</span>
      </div>
    )
  }

  return (
    <button
      onClick={handleStartEdit}
      className={`text-sm font-semibold transition-colors cursor-pointer hover:underline
        ${isEdited ? 'text-violet-500 dark:text-violet-400' : bidColor || 'text-[hsl(var(--foreground))]'}`}
      title="Нажмите для изменения ставки"
    >
      {valueKopecks > 0 ? `${rubValue} ₽` : '—'}
      {isEdited && <span className="ml-1 text-[10px] text-violet-400">✎</span>}
    </button>
  )
}


// ══════════════════════════════════════════════════════════════════
// Products Tab (nm_id bids + add/remove + per-product bid history)
// ══════════════════════════════════════════════════════════════════

function ProductsSection({ campaign, shopId, onToast, onCampaignUpdate, baseBids, advertId }: {
  campaign: EnrichedCampaign
  shopId: number
  onToast: (t: { type: 'success' | 'error'; message: string }) => void
  onCampaignUpdate?: (campaign: EnrichedCampaign) => void
  baseBids?: { competitive_kopecks?: number; leaders_kopecks?: number; competitive_rub?: number; leaders_rub?: number }
  advertId: number
}) {
  const [editingProduct, setEditingProduct] = useState<{ nm_id: number; placement: 'search' | 'recommendations'; value: string } | null>(null)
  const [saving, setSaving] = useState(false)
  const [removingNm, setRemovingNm] = useState<number | null>(null)

  // Per-product bid history state
  const [expandedNm, setExpandedNm] = useState<number | null>(null)
  const [productBidHistory, setProductBidHistory] = useState<Record<number, CampaignEventRow[]>>({})
  const [loadingHistory, setLoadingHistory] = useState<number | null>(null)
  // Cache all events for campaign (loaded once)
  const [allBidEvents, setAllBidEvents] = useState<CampaignEventRow[] | null>(null)
  const [loadingAllEvents, setLoadingAllEvents] = useState(false)

  const isUnifiedBidType = campaign.bid_type === 'unified'

  // Load all bid events for campaign (lazy, once)
  const loadAllBidEvents = useCallback(async (): Promise<CampaignEventRow[]> => {
    if (allBidEvents !== null) return allBidEvents
    setLoadingAllEvents(true)
    try {
      const events = await getCampaignEvents('wb', advertId, undefined, 500)
      const bidEvents = events.filter(e => e.event_type === 'BID_CHANGE' || e.event_type === 'OZON_BID_CHANGE')
      setAllBidEvents(bidEvents)
      return bidEvents
    } catch {
      return []
    } finally {
      setLoadingAllEvents(false)
    }
  }, [advertId, allBidEvents])

  const toggleProductHistory = useCallback(async (nmId: number) => {
    if (expandedNm === nmId) {
      setExpandedNm(null)
      return
    }
    setExpandedNm(nmId)

    // If already loaded for this product, skip
    if (productBidHistory[nmId]) return

    setLoadingHistory(nmId)
    try {
      const events = await loadAllBidEvents()
      // Filter by product_id (nm_id stored as string)
      const nmStr = String(nmId)
      const productEvents = events.filter(e => e.product_id === nmStr)
      setProductBidHistory(prev => ({ ...prev, [nmId]: productEvents }))
    } catch {
      setProductBidHistory(prev => ({ ...prev, [nmId]: [] }))
    } finally {
      setLoadingHistory(null)
    }
  }, [expandedNm, productBidHistory, loadAllBidEvents])

  const handleSaveProductBid = async (nmId: number, placement: 'search' | 'recommendations', rublesStr: string) => {
    const rubles = parseInt(rublesStr, 10)
    if (isNaN(rubles) || rubles <= 0) return
    setSaving(true)
    try {
      const result = await changeBids(
        shopId,
        campaign.advert_id,
        placement,
        [{ nm_id: nmId, bid: rubles * 100 }],
        campaign.bid_type || 'manual',
      )
      if (result.success) {
        onToast({ type: 'success', message: `Ставка ${placement === 'search' ? 'поиска' : 'полок'} обновлена: ${rubles} ₽` })
        setEditingProduct(null)
        if (onCampaignUpdate && campaign.nm_settings) {
          const newBidKopecks = rubles * 100
          onCampaignUpdate({
            ...campaign,
            nm_settings: campaign.nm_settings.map(ns =>
              ns.nm_id === nmId
                ? {
                    ...ns,
                    bid_search: (placement === 'search' || isUnifiedBidType) ? newBidKopecks : ns.bid_search,
                    bid_recommendations: placement === 'recommendations' ? newBidKopecks : ns.bid_recommendations,
                  }
                : ns
            ),
          })
        }
      } else {
        onToast({ type: 'error', message: result.message || 'Ошибка изменения ставки' })
      }
    } catch (e: any) {
      onToast({ type: 'error', message: e?.response?.data?.detail || 'Ошибка изменения ставки' })
    } finally {
      setSaving(false)
    }
  }

  const handleRemoveNm = async (nmId: number) => {
    if (!confirm(`Удалить товар #${nmId} из кампании? Это действие нельзя отменить.`)) return
    setRemovingNm(nmId)
    try {
      await manageCampaignNms(shopId, campaign.advert_id, [], [nmId])
      onToast({ type: 'success', message: `Товар #${nmId} удалён из кампании` })
      if (onCampaignUpdate && campaign.nm_settings) {
        onCampaignUpdate({
          ...campaign,
          nm_settings: campaign.nm_settings.filter(ns => ns.nm_id !== nmId),
        })
      }
    } catch (e: any) {
      onToast({ type: 'error', message: e?.response?.data?.detail || 'Ошибка удаления товара' })
    } finally {
      setRemovingNm(null)
    }
  }

  if (!campaign.nm_settings || campaign.nm_settings.length === 0) {
    return (
      <div className="flex items-center justify-center h-20 text-sm text-[hsl(var(--muted-foreground))]">
        Нет товаров в этой кампании
      </div>
    )
  }

  // Column count for spanning the history row
  const colCount = 3 + (isUnifiedBidType && baseBids && (baseBids.competitive_rub || baseBids.leaders_rub) ? 1 : 0)
    + (!isUnifiedBidType ? 1 : 0) + 1

  return (
    <div className="overflow-auto rounded-xl border border-[hsl(var(--border))]/50">

      <table className="w-full border-collapse">
        <thead className="sticky top-0 z-10 bg-[hsl(var(--card))]">
          <tr className="border-b border-[hsl(var(--border))]">
            <th className="px-3 py-2.5 pl-6 text-left text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider min-w-[250px]">Товар</th>
            <th className="px-3 py-2.5 text-left text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider">Артикул</th>
            <th className="px-3 py-2.5 text-right text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider">
              {isUnifiedBidType ? 'Ставка (CPM)' : '🔍 Поиск'}
            </th>
            {isUnifiedBidType && baseBids && (baseBids.competitive_rub || baseBids.leaders_rub) && (
              <th className="px-3 py-2.5 text-center text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider">Рекоменд.</th>
            )}
            {!isUnifiedBidType && (
              <th className="px-3 py-2.5 text-right text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider">📦 Полки</th>
            )}
            <th className="px-3 py-2.5 text-center text-xs font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider pr-6 w-16"></th>
          </tr>
        </thead>
        <tbody>
          {campaign.nm_settings.map(ns => {
            const isUnifiedBid = campaign.bid_type === 'unified'
            const isRemoving = removingNm === ns.nm_id
            const isExpanded = expandedNm === ns.nm_id
            const historyEvents = productBidHistory[ns.nm_id]
            const isHistoryLoading = loadingHistory === ns.nm_id || (loadingAllEvents && expandedNm === ns.nm_id)
            return (
              <React.Fragment key={ns.nm_id}>
                <tr className={`border-b border-[hsl(var(--border))]/30 hover:bg-[hsl(var(--muted))]/10 transition-colors group ${isRemoving ? 'opacity-50' : ''} ${isExpanded ? 'bg-[hsl(var(--muted))]/5' : ''}`}>
                  <td className="px-3 py-3 pl-6">
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => toggleProductHistory(ns.nm_id)}
                        className={`p-1 rounded-md transition-all shrink-0 ${isExpanded
                          ? 'bg-violet-500/15 text-violet-500'
                          : 'text-[hsl(var(--muted-foreground))] hover:text-violet-500 hover:bg-violet-500/10'}`}
                        title="История ставок"
                      >
                        {isHistoryLoading
                          ? <Loader2 className="w-3.5 h-3.5 animate-spin" />
                          : <History className="w-3.5 h-3.5" />}
                      </button>
                      <div className="min-w-0">
                        <div className="text-sm font-medium text-[hsl(var(--foreground))]">
                          {ns.product_name || ns.subject_name || `Товар ${ns.nm_id}`}
                        </div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5 font-mono">
                          #{ns.nm_id}
                        </div>
                      </div>
                    </div>
                  </td>
                  <td className="px-3 py-3">
                    {ns.vendor_code && (
                      <span className="text-xs font-mono px-1.5 py-0.5 rounded bg-[hsl(var(--muted))]/40 text-[hsl(var(--muted-foreground))]">
                        {ns.vendor_code}
                      </span>
                    )}
                  </td>
                  {/* Search bid */}
                  <td className="px-3 py-3 text-right">
                    <ProductBidCell
                      nmId={ns.nm_id}
                      placement="search"
                      bidKopecks={isUnifiedBid ? (ns.bid_search || ns.bid_recommendations) : ns.bid_search}
                      editing={editingProduct}
                      setEditing={setEditingProduct}
                      onSave={handleSaveProductBid}
                      saving={saving}
                    />
                  </td>
                  {/* Recommendations for unified CPM */}
                  {isUnifiedBid && baseBids && (baseBids.competitive_rub || baseBids.leaders_rub) && (
                    <td className="px-3 py-3 text-center whitespace-nowrap">
                      <div className="flex flex-col items-center gap-0.5">
                        {baseBids.competitive_rub && baseBids.competitive_rub > 0 && (
                          <span className="text-[10px] text-blue-500" title="Конкурентная">
                            Конк.: {baseBids.competitive_rub} ₽
                          </span>
                        )}
                        {baseBids.leaders_rub && baseBids.leaders_rub > 0 && (
                          <span className="text-[10px] text-emerald-500" title="Лидерская">
                            Лидер.: {baseBids.leaders_rub} ₽
                          </span>
                        )}
                      </div>
                    </td>
                  )}
                  {/* Recommendations bid (manual only) */}
                  {!isUnifiedBid && (
                    <td className="px-3 py-3 text-right">
                      <ProductBidCell
                        nmId={ns.nm_id}
                        placement="recommendations"
                        bidKopecks={ns.bid_recommendations}
                        editing={editingProduct}
                        setEditing={setEditingProduct}
                        onSave={handleSaveProductBid}
                        saving={saving}
                      />
                    </td>
                  )}
                  {/* Remove button */}
                  <td className="px-3 py-3 text-center pr-6">
                    <button
                      onClick={() => handleRemoveNm(ns.nm_id)}
                      disabled={isRemoving}
                      className="p-1 rounded hover:bg-red-500/10 text-[hsl(var(--muted-foreground))] hover:text-red-500 opacity-0 group-hover:opacity-100 transition-all disabled:opacity-50"
                      title="Удалить товар из кампании"
                    >
                      {isRemoving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Trash2 className="w-3.5 h-3.5" />}
                    </button>
                  </td>
                </tr>
                {/* Per-product bid history (expandable row) */}
                <AnimatePresence>
                  {isExpanded && (
                    <tr>
                      <td colSpan={colCount} className="p-0">
                        <motion.div
                          initial={{ height: 0, opacity: 0 }}
                          animate={{ height: 'auto', opacity: 1 }}
                          exit={{ height: 0, opacity: 0 }}
                          transition={{ duration: 0.2 }}
                          className="overflow-hidden"
                        >
                          <ProductBidHistoryInline
                            events={historyEvents || []}
                            loading={isHistoryLoading}
                          />
                        </motion.div>
                      </td>
                    </tr>
                  )}
                </AnimatePresence>
              </React.Fragment>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}


// ── Per-product bid history inline ──────────────────────────────

function ProductBidHistoryInline({ events, loading }: {
  events: CampaignEventRow[]
  loading: boolean
}) {
  if (loading) {
    return (
      <div className="flex items-center gap-2 px-6 py-4 text-[hsl(var(--muted-foreground))] text-xs bg-[hsl(var(--muted))]/5">
        <Loader2 className="w-3.5 h-3.5 animate-spin" />
        Загрузка истории ставок...
      </div>
    )
  }

  if (events.length === 0) {
    return (
      <div className="px-6 py-4 text-center text-[hsl(var(--muted-foreground))] text-xs bg-[hsl(var(--muted))]/5">
        Нет изменений ставок для этого товара
      </div>
    )
  }

  // Group by date
  const grouped = events.reduce((acc, event) => {
    const dateStr = new Date(event.timestamp).toLocaleDateString('ru-RU', {
      day: 'numeric', month: 'long', year: 'numeric'
    })
    if (!acc[dateStr]) acc[dateStr] = []
    acc[dateStr].push(event)
    return acc
  }, {} as Record<string, CampaignEventRow[]>)

  const formatBidValue = (value?: string, eventType?: string): string => {
    if (!value) return '?'
    const num = parseFloat(value)
    if (isNaN(num)) return value
    if (eventType === 'BID_CHANGE') return `${Math.round(num / 100)} ₽`
    return `${Math.round(num)} ₽`
  }

  return (
    <div className="px-6 py-3 bg-[hsl(var(--muted))]/5 border-t border-[hsl(var(--border))]/20 max-h-[250px] overflow-auto">
      {Object.entries(grouped).map(([dateStr, dayEvents]) => (
        <div key={dateStr} className="mb-3 last:mb-0">
          <div className="flex items-center gap-2 mb-1.5">
            <div className="w-1.5 h-1.5 rounded-full bg-violet-500" />
            <span className="text-[10px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider">
              {dateStr}
            </span>
            <div className="flex-1 h-px bg-[hsl(var(--border))]/30" />
          </div>

          <div className="space-y-1 ml-4">
            {dayEvents.map(event => {
              const oldVal = parseFloat(event.old_value || '0')
              const newVal = parseFloat(event.new_value || '0')
              const isIncrease = newVal > oldVal
              const time = new Date(event.timestamp).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })
              const meta = event.event_metadata || {}
              const bidField = (meta as any).bid_field || ''
              const bidFieldLabel = bidField === 'search' ? 'поиск' : bidField === 'recommendations' ? 'рекоменд.' : ''

              return (
                <div
                  key={event.id}
                  className="flex items-center gap-2.5 px-2.5 py-1.5 rounded-md bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted))]/20 transition-colors text-xs"
                >
                  <span className="text-[hsl(var(--muted-foreground))] font-mono min-w-[36px]">
                    {time}
                  </span>

                  <div className={`flex items-center justify-center w-4 h-4 rounded-full shrink-0 ${isIncrease ? 'bg-red-500/15 text-red-500' : 'bg-emerald-500/15 text-emerald-500'}`}>
                    {isIncrease ? <ArrowUpRight className="w-2.5 h-2.5" /> : <ArrowDownRight className="w-2.5 h-2.5" />}
                  </div>

                  {bidFieldLabel && (
                    <span className="text-[10px] text-[hsl(var(--muted-foreground))] px-1.5 py-0.5 rounded bg-[hsl(var(--muted))]/30">
                      {bidFieldLabel}
                    </span>
                  )}

                  <div className="flex items-center gap-1 ml-auto">
                    <span className="text-[hsl(var(--muted-foreground))]">
                      {formatBidValue(event.old_value, event.event_type)}
                    </span>
                    <span className="text-[hsl(var(--muted-foreground))]">→</span>
                    <span className={`font-semibold ${isIncrease ? 'text-red-500' : 'text-emerald-500'}`}>
                      {formatBidValue(event.new_value, event.event_type)}
                    </span>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}


// ══════════════════════════════════════════════════════════════════
// Add Product Sub-Modal — search + select products to add to campaign
// ══════════════════════════════════════════════════════════════════

function AddProductSubModal({ shopId, campaign, onClose, onAdded, onToast }: {
  shopId: number
  campaign: EnrichedCampaign
  onClose: () => void
  onAdded: (nmIds: number[]) => void
  onToast: (t: { type: 'success' | 'error'; message: string }) => void
}) {
  const [, setSubjects] = useState<{ id: number; name: string; count: number }[]>([])
  const [loadingSubjects, setLoadingSubjects] = useState(true)
  const [products, setProducts] = useState<ProductItem[]>([])
  const [loadingProducts, setLoadingProducts] = useState(false)
  const [selectedNms, setSelectedNms] = useState<Set<number>>(new Set())
  const [searchQuery, setSearchQuery] = useState('')
  const [adding, setAdding] = useState(false)

  const existingNmIds = useMemo(() => {
    return new Set((campaign.nm_settings || []).map(ns => ns.nm_id))
  }, [campaign.nm_settings])

  // Load subjects on mount
  useEffect(() => {
    let cancelled = false
    setLoadingSubjects(true)
    getCreationSubjects(shopId, 'cpm')
      .then(r => {
        if (!cancelled && r.success && r.subjects?.length) {
          setSubjects(r.subjects)
          // Auto-load all products
          const subjectIds = r.subjects.map(s => s.id)
          setLoadingProducts(true)
          getCreationProducts(shopId, subjectIds)
            .then(pr => {
              if (!cancelled && pr.success) {
                setProducts(pr.products || [])
              }
            })
            .catch(() => {})
            .finally(() => { if (!cancelled) setLoadingProducts(false) })
        }
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoadingSubjects(false) })
    return () => { cancelled = true }
  }, [shopId])

  const filteredProducts = useMemo(() => {
    if (!searchQuery.trim()) return products
    const q = searchQuery.toLowerCase()
    return products.filter(p =>
      p.title?.toLowerCase().includes(q) ||
      String(p.nm).includes(q) ||
      p.vendor_code?.toLowerCase().includes(q)
    )
  }, [products, searchQuery])

  const toggleNm = (nm: number) => {
    setSelectedNms(prev => {
      const next = new Set(prev)
      if (next.has(nm)) next.delete(nm)
      else next.add(nm)
      return next
    })
  }

  const handleAdd = async () => {
    const nmsToAdd = Array.from(selectedNms).filter(nm => !existingNmIds.has(nm))
    if (nmsToAdd.length === 0) return
    setAdding(true)
    try {
      await manageCampaignNms(shopId, campaign.advert_id, nmsToAdd, [])
      onToast({ type: 'success', message: `Добавлено товаров: ${nmsToAdd.length}` })
      onAdded(nmsToAdd)
    } catch (e: any) {
      onToast({ type: 'error', message: e?.response?.data?.detail || 'Ошибка добавления товаров' })
    } finally {
      setAdding(false)
    }
  }

  const newSelectedCount = Array.from(selectedNms).filter(nm => !existingNmIds.has(nm)).length

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="absolute inset-0 z-20 bg-black/40 backdrop-blur-sm flex items-center justify-center p-6"
      onClick={onClose}
    >
      <motion.div
        initial={{ scale: 0.95, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        exit={{ scale: 0.95, opacity: 0 }}
        transition={{ type: 'spring', duration: 0.25 }}
        onClick={e => e.stopPropagation()}
        className="w-full max-w-[560px] max-h-[70vh] bg-[hsl(var(--card))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl flex flex-col overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-[hsl(var(--border))] shrink-0">
          <div>
            <h3 className="text-base font-bold text-[hsl(var(--foreground))]">Добавить товар в кампанию</h3>
            <p className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5">{campaign.name}</p>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg hover:bg-[hsl(var(--muted))]/30 text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Search */}
        <div className="px-5 py-3 border-b border-[hsl(var(--border))]/50 shrink-0">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[hsl(var(--muted-foreground))]" />
            <input
              type="text"
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              placeholder="Поиск по артикулу, ID или названию..."
              className="w-full pl-9 pr-3 py-2.5 rounded-lg bg-[hsl(var(--secondary))] border border-[hsl(var(--border))] text-sm text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))] focus:outline-none focus:ring-2 focus:ring-violet-500/50"
              autoFocus
            />
          </div>
        </div>

        {/* Product List */}
        <div className="flex-1 overflow-y-auto px-3 py-2">
          {(loadingSubjects || loadingProducts) ? (
            <div className="flex items-center justify-center h-32 gap-2 text-sm text-[hsl(var(--muted-foreground))]">
              <Loader2 className="w-4 h-4 animate-spin" />
              Загрузка товаров...
            </div>
          ) : filteredProducts.length === 0 ? (
            <div className="flex items-center justify-center h-32 text-sm text-[hsl(var(--muted-foreground))]">
              {products.length > 0 ? 'Ничего не найдено' : 'Нет доступных товаров'}
            </div>
          ) : (
            <div className="space-y-1">
              {filteredProducts.map(p => {
                const isExisting = existingNmIds.has(p.nm)
                const isSelected = selectedNms.has(p.nm)
                return (
                  <button
                    key={p.nm}
                    onClick={() => !isExisting && toggleNm(p.nm)}
                    disabled={isExisting}
                    className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-left transition-all ${
                      isExisting
                        ? 'opacity-40 cursor-not-allowed'
                        : isSelected
                          ? 'border border-violet-500 bg-violet-500/5'
                          : 'border border-transparent hover:bg-[hsl(var(--muted))]/40'
                    }`}
                  >
                    <div className={`w-5 h-5 rounded-md border-2 flex-shrink-0 flex items-center justify-center transition-all ${
                      isExisting
                        ? 'bg-zinc-400/30 border-zinc-400/30'
                        : isSelected
                          ? 'bg-violet-600 border-violet-600'
                          : 'border-[hsl(var(--border))] bg-[hsl(var(--secondary))]'
                    }`}>
                      {(isExisting || isSelected) && <Check className="w-3.5 h-3.5 text-white" />}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="text-sm text-[hsl(var(--foreground))] truncate">
                        {p.title || `Товар ${p.nm}`}
                      </div>
                      <div className="text-xs text-[hsl(var(--muted-foreground))] flex items-center gap-2 flex-wrap mt-0.5">
                        <span>nm: {p.nm}</span>
                        {p.vendor_code && (
                          <span className="px-1.5 py-0.5 rounded bg-[hsl(var(--muted))]/50 text-[10px] font-mono">
                            {p.vendor_code}
                          </span>
                        )}
                        {isExisting && (
                          <span className="px-1.5 py-0.5 rounded bg-amber-500/10 text-amber-500 text-[10px] font-medium">
                            уже в кампании
                          </span>
                        )}
                      </div>
                    </div>
                  </button>
                )
              })}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-5 py-3 border-t border-[hsl(var(--border))] flex items-center justify-between shrink-0">
          <span className="text-xs text-[hsl(var(--muted-foreground))]">
            {products.length} доступно · {newSelectedCount} выбрано
          </span>
          <div className="flex items-center gap-2">
            <button
              onClick={onClose}
              className="px-3 py-2 rounded-lg text-sm text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))]/30 transition-colors"
            >
              Отмена
            </button>
            <button
              onClick={handleAdd}
              disabled={newSelectedCount === 0 || adding}
              className="flex items-center gap-1.5 px-4 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium transition-colors disabled:opacity-50"
            >
              {adding ? <Loader2 className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
              Добавить {newSelectedCount > 0 ? `(${newSelectedCount})` : ''}
            </button>
          </div>
        </div>
      </motion.div>
    </motion.div>
  )
}


// ── Product Bid Cell ────────────────────────────────────────────

function ProductBidCell({ nmId, placement, bidKopecks, editing, setEditing, onSave, saving }: {
  nmId: number
  placement: 'search' | 'recommendations'
  bidKopecks: number
  editing: { nm_id: number; placement: string; value: string } | null
  setEditing: (v: { nm_id: number; placement: 'search' | 'recommendations'; value: string } | null) => void
  onSave: (nmId: number, placement: 'search' | 'recommendations', value: string) => void
  saving: boolean
}) {
  const isEditing = editing?.nm_id === nmId && editing?.placement === placement
  const rubStr = bidKopecks > 0 ? (bidKopecks / 100).toFixed(0) : '0'

  if (isEditing && editing) {
    return (
      <div className="inline-flex items-center gap-1">
        <input
          autoFocus
          type="number"
          min={1}
          value={editing.value}
          onChange={e => setEditing({ nm_id: editing.nm_id, placement: editing.placement as 'search' | 'recommendations', value: e.target.value })}
          onKeyDown={e => {
            if (e.key === 'Enter') onSave(nmId, placement, editing.value)
            if (e.key === 'Escape') setEditing(null)
          }}
          className="w-20 px-2 py-1 rounded-md bg-[hsl(var(--secondary))] border border-violet-500/50 text-[hsl(var(--foreground))] text-sm text-right font-medium focus:outline-none focus:ring-2 focus:ring-violet-500/30"
          disabled={saving}
        />
        <button
          onClick={() => onSave(nmId, placement, editing.value)}
          disabled={saving}
          className="p-1 rounded hover:bg-emerald-500/20 text-emerald-500 transition-colors disabled:opacity-50"
        >
          {saving ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Check className="w-3.5 h-3.5" />}
        </button>
      </div>
    )
  }

  return (
    <button
      onClick={() => setEditing({ nm_id: nmId, placement, value: rubStr })}
      className="text-sm font-semibold text-[hsl(var(--foreground))] hover:text-violet-500 hover:underline transition-colors cursor-pointer"
      title="Нажмите для изменения"
    >
      {bidKopecks > 0 ? `${rubStr} ₽` : '—'}
    </button>
  )
}
