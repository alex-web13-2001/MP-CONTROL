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
} from 'lucide-react'
import {
  getClusterList, getClusterListCached, toggleClusterExclusion,
  setNormqueryBids, manageCampaignNms,
  changeBids, startCampaign, pauseCampaign,
  formatNum,
  type EnrichedCampaign, type NormqueryClusterStat,
  type ClusterListResponse,
} from '../api/ad-management'

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

type Tab = 'clusters' | 'products'
type ClusterFilter = 'all' | 'active' | 'excluded'

interface Props {
  campaign: EnrichedCampaign
  shopId: number
  onClose: () => void
  onCampaignUpdate?: (campaign: EnrichedCampaign) => void
}

// ── Clusters Sort ─────────────────────────────────────────────────

type ClusterSortKey = 'norm_query' | 'status' | 'views' | 'clicks' | 'ctr' | 'atbs' | 'orders' |
  'avg_pos' | 'cpc_rub' | 'cpm_rub' | 'spend_rub' | 'current_bid_rub' | 'cr_click_to_cart' | 'cr_click_to_order'

// ══════════════════════════════════════════════════════════════════
// Main Component
// ══════════════════════════════════════════════════════════════════

export default function CampaignManagementModal({ campaign, shopId, onClose, onCampaignUpdate }: Props) {
  const hasClusters = campaign.payment_type === 'cpm'
  const canEditClusterBids = campaign.bid_type === 'manual' && hasClusters

  const [tab, setTab] = useState<Tab>(hasClusters ? 'clusters' : 'products')
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

  // Date range — last 14 days by default
  const dateRange = useMemo(() => {
    const end = new Date()
    const start = new Date()
    start.setDate(end.getDate() - 13)
    const fmt = (d: Date) => d.toISOString().split('T')[0]
    return { start: fmt(start), end: fmt(end) }
  }, [])

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
            {/* Status actions */}
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

        {/* ── Tabs ───────────────────────────────────────────────── */}
        <div className="px-6 pt-2 border-b border-[hsl(var(--border))] flex items-center gap-1 shrink-0">
          {hasClusters && (
            <TabButton active={tab === 'clusters'} onClick={() => setTab('clusters')} icon={<Target className="w-3.5 h-3.5" />} label="Кластеры" count={clusterData?.total_clusters} />
          )}
          <TabButton active={tab === 'products'} onClick={() => setTab('products')} icon={<Package className="w-3.5 h-3.5" />} label="Товары" count={campaign.nm_settings?.length} />

          {/* Right side: filter, save/refresh */}
          <div className="flex-1" />

          {/* Cluster filter */}
          {tab === 'clusters' && hasClusters && clusterData && (
            <div className="flex items-center gap-1 mr-2 mb-1">
              <FilterButton active={clusterFilter === 'all'} onClick={() => setClusterFilter('all')} label="Все" />
              <FilterButton active={clusterFilter === 'active'} onClick={() => setClusterFilter('active')} label="Активные" color="emerald" />
              <FilterButton active={clusterFilter === 'excluded'} onClick={() => setClusterFilter('excluded')} label="Исключённые" color="red" />
            </div>
          )}

          {tab === 'clusters' && changedBidsCount > 0 && (
            <button
              onClick={handleSaveBids}
              disabled={savingBids}
              className="flex items-center gap-1.5 px-3 py-1.5 mb-1 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-xs font-medium transition-colors disabled:opacity-50"
            >
              {savingBids ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Save className="w-3.5 h-3.5" />}
              Сохранить {changedBidsCount} {changedBidsCount === 1 ? 'ставку' : changedBidsCount < 5 ? 'ставки' : 'ставок'}
            </button>
          )}
          {hasClusters && (
            <button
              onClick={() => loadClusters(true)}
              disabled={loading}
              className="p-1.5 mb-1 rounded-lg hover:bg-[hsl(var(--muted))]/30 text-[hsl(var(--muted-foreground))] transition-colors disabled:opacity-50"
              title={dataSource === 'clickhouse' ? 'Обновить из WB API' : 'Обновить данные'}
            >
              <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
            </button>
          )}
          {dataSource && (
            <span className={`text-[10px] font-medium px-1.5 py-0.5 mb-1 rounded ${
              dataSource === 'clickhouse'
                ? 'bg-blue-500/10 text-blue-400'
                : 'bg-emerald-500/10 text-emerald-400'
            }`}>
              {dataSource === 'clickhouse' ? 'кеш' : 'live'}
            </span>
          )}
        </div>

        {/* ── Tab Content ────────────────────────────────────────── */}
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

          {/* Loading */}
          {loading && !clusterData && (
            <div className="flex items-center justify-center h-40 gap-3 text-[hsl(var(--muted-foreground))]">
              <Loader2 className="w-5 h-5 animate-spin" />
              Загрузка данных кластеров...
            </div>
          )}

          {/* Error */}
          {clusterError && (
            <div className="mx-6 mt-4 p-4 rounded-xl bg-red-500/10 border border-red-500/20 text-red-500 text-sm">
              {clusterError}
            </div>
          )}

          {/* Clusters Tab */}
          {tab === 'clusters' && hasClusters && clusterData && (
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

          {/* Products Tab */}
          {tab === 'products' && (
            <ProductsTab
              campaign={campaign}
              shopId={shopId}
              onToast={setToast}
              onCampaignUpdate={onCampaignUpdate}
              baseBids={hasClusters ? clusterData?.base_bids : undefined}
            />
          )}
        </div>
      </motion.div>
    </motion.div>
  )
}


// ══════════════════════════════════════════════════════════════════
// Tab Button
// ══════════════════════════════════════════════════════════════════

function TabButton({ active, onClick, icon, label, count }: {
  active: boolean; onClick: () => void; icon: React.ReactNode; label: string; count?: number
}) {
  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-1.5 px-3 py-2.5 text-sm font-medium border-b-2 transition-colors
        ${active
          ? 'border-violet-500 text-violet-600 dark:text-violet-400'
          : 'border-transparent text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:border-[hsl(var(--border))]'}`}
    >
      {icon}
      {label}
      {count !== undefined && count > 0 && (
        <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded-full
          ${active ? 'bg-violet-500/15 text-violet-600 dark:text-violet-400' : 'bg-[hsl(var(--muted))]/50 text-[hsl(var(--muted-foreground))]'}`}>
          {count}
        </span>
      )}
    </button>
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
    <div className="overflow-auto">
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
                  {c.cpc_rub > 0 ? `${c.cpc_rub.toFixed(2)} ₽` : '—'}
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
// Products Tab (nm_id bids + add/remove)
// ══════════════════════════════════════════════════════════════════

function ProductsTab({ campaign, shopId, onToast, onCampaignUpdate, baseBids }: {
  campaign: EnrichedCampaign
  shopId: number
  onToast: (t: { type: 'success' | 'error'; message: string }) => void
  onCampaignUpdate?: (campaign: EnrichedCampaign) => void
  baseBids?: { competitive_kopecks?: number; leaders_kopecks?: number; competitive_rub?: number; leaders_rub?: number }
}) {
  const [editingProduct, setEditingProduct] = useState<{ nm_id: number; placement: 'search' | 'recommendations'; value: string } | null>(null)
  const [saving, setSaving] = useState(false)
  const [removingNm, setRemovingNm] = useState<number | null>(null)
  const [showAddInput, setShowAddInput] = useState(false)
  const [newNmId, setNewNmId] = useState('')
  const [addingNm, setAddingNm] = useState(false)

  const isUnifiedBidType = campaign.bid_type === 'unified'

  const handleSaveProductBid = async (nmId: number, placement: 'search' | 'recommendations', rublesStr: string) => {
    const rubles = parseInt(rublesStr, 10)
    if (isNaN(rubles) || rubles <= 0) return
    setSaving(true)
    try {
      await changeBids(shopId, campaign.advert_id, placement, [{ nm_id: nmId, bid: rubles * 100 }])
      onToast({ type: 'success', message: `Ставка ${placement === 'search' ? 'поиска' : 'полок'} обновлена: ${rubles} ₽` })
      setEditingProduct(null)
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
      // Optimistic update
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

  const handleAddNm = async () => {
    const nmId = parseInt(newNmId, 10)
    if (isNaN(nmId) || nmId <= 0) return
    setAddingNm(true)
    try {
      await manageCampaignNms(shopId, campaign.advert_id, [nmId], [])
      onToast({ type: 'success', message: `Товар #${nmId} добавлен в кампанию` })
      setNewNmId('')
      setShowAddInput(false)
      // Optimistic update
      if (onCampaignUpdate && campaign.nm_settings) {
        onCampaignUpdate({
          ...campaign,
          nm_settings: [...campaign.nm_settings, {
            nm_id: nmId,
            bid_search: 0,
            bid_recommendations: 0,
            subject_name: '',
            product_name: `Товар ${nmId}`,
            vendor_code: '',
          }],
        })
      }
    } catch (e: any) {
      onToast({ type: 'error', message: e?.response?.data?.detail || 'Ошибка добавления товара' })
    } finally {
      setAddingNm(false)
    }
  }

  if (!campaign.nm_settings || campaign.nm_settings.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-32 gap-3">
        <span className="text-sm text-[hsl(var(--muted-foreground))]">Нет данных о товарах в этой кампании</span>
        {!showAddInput ? (
          <button
            onClick={() => setShowAddInput(true)}
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium transition-colors"
          >
            <Plus className="w-4 h-4" />
            Добавить товар
          </button>
        ) : (
          <AddNmInput
            newNmId={newNmId}
            setNewNmId={setNewNmId}
            onAdd={handleAddNm}
            onCancel={() => setShowAddInput(false)}
            adding={addingNm}
          />
        )}
      </div>
    )
  }

  return (
    <div className="overflow-auto">
      {/* Add product button */}
      <div className="px-6 py-3 flex items-center gap-3 border-b border-[hsl(var(--border))]/30">
        {!showAddInput ? (
          <button
            onClick={() => setShowAddInput(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-violet-600/10 hover:bg-violet-600/20 text-violet-600 dark:text-violet-400 text-xs font-medium transition-colors"
          >
            <Plus className="w-3.5 h-3.5" />
            Добавить товар
          </button>
        ) : (
          <AddNmInput
            newNmId={newNmId}
            setNewNmId={setNewNmId}
            onAdd={handleAddNm}
            onCancel={() => setShowAddInput(false)}
            adding={addingNm}
          />
        )}
        <span className="text-xs text-[hsl(var(--muted-foreground))]">
          Всего товаров: {campaign.nm_settings.length}
        </span>
      </div>

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
            return (
              <tr key={ns.nm_id} className={`border-b border-[hsl(var(--border))]/30 hover:bg-[hsl(var(--muted))]/10 transition-colors group ${isRemoving ? 'opacity-50' : ''}`}>
                <td className="px-3 py-3 pl-6">
                  <div className="text-sm font-medium text-[hsl(var(--foreground))]">
                    {ns.product_name || ns.subject_name || `Товар ${ns.nm_id}`}
                  </div>
                  <div className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5 font-mono">
                    #{ns.nm_id}
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
            )
          })}
        </tbody>
      </table>
    </div>
  )
}


// ── Add NM Input ────────────────────────────────────────────────

function AddNmInput({ newNmId, setNewNmId, onAdd, onCancel, adding }: {
  newNmId: string; setNewNmId: (v: string) => void
  onAdd: () => void; onCancel: () => void; adding: boolean
}) {
  return (
    <div className="flex items-center gap-2">
      <input
        autoFocus
        type="number"
        min={1}
        placeholder="nm_id товара"
        value={newNmId}
        onChange={e => setNewNmId(e.target.value)}
        onKeyDown={e => {
          if (e.key === 'Enter') onAdd()
          if (e.key === 'Escape') onCancel()
        }}
        className="w-40 px-3 py-1.5 rounded-lg bg-[hsl(var(--secondary))] border border-[hsl(var(--border))] text-sm text-[hsl(var(--foreground))] focus:outline-none focus:ring-2 focus:ring-violet-500/30"
        disabled={adding}
      />
      <button
        onClick={onAdd}
        disabled={adding || !newNmId.trim()}
        className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-xs font-medium transition-colors disabled:opacity-50"
      >
        {adding ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Plus className="w-3.5 h-3.5" />}
        Добавить
      </button>
      <button
        onClick={onCancel}
        className="p-1.5 rounded-lg hover:bg-[hsl(var(--muted))]/30 text-[hsl(var(--muted-foreground))]"
      >
        <X className="w-3.5 h-3.5" />
      </button>
    </div>
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
