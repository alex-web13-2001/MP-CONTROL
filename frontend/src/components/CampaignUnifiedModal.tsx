/**
 * CampaignUnifiedModal — Wrapper that combines Management + Analytics into one modal with tabs.
 *
 * Opens from either the campaign name click (→ Management tab) or the analytics icon (→ Analytics tab).
 * User can switch between tabs without closing the modal: "посмотрел статистику → принял решение → действие".
 */
import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { X, Settings2, BarChart3 } from 'lucide-react'
import CampaignManagementModal from './CampaignManagementModal'
import { CampaignDetailModal } from './CampaignDetailModal'
import type { EnrichedCampaign } from '../api/ad-management'

type UnifiedTab = 'management' | 'analytics'

interface CampaignUnifiedModalProps {
  campaign: EnrichedCampaign
  shopId: number
  marketplace: string
  initialTab?: UnifiedTab
  onClose: () => void
  onCampaignUpdate?: (campaign: EnrichedCampaign) => void
}

export default function CampaignUnifiedModal({
  campaign,
  shopId,
  marketplace,
  initialTab = 'management',
  onClose,
  onCampaignUpdate,
}: CampaignUnifiedModalProps) {
  const [activeTab, setActiveTab] = useState<UnifiedTab>(initialTab)
  const [currentCampaign, setCurrentCampaign] = useState(campaign)

  // Sync campaign from parent
  useEffect(() => {
    setCurrentCampaign(campaign)
  }, [campaign])

  // Handle campaign updates from management tab — propagate up
  const handleCampaignUpdate = (updated: EnrichedCampaign) => {
    setCurrentCampaign(updated)
    onCampaignUpdate?.(updated)
  }

  // Lock body scroll
  useEffect(() => {
    const scrollY = window.scrollY
    document.body.style.position = 'fixed'
    document.body.style.top = `-${scrollY}px`
    document.body.style.width = '100%'
    document.body.style.overflow = 'hidden'
    return () => {
      document.body.style.position = ''
      document.body.style.top = ''
      document.body.style.width = ''
      document.body.style.overflow = ''
      window.scrollTo(0, scrollY)
    }
  }, [])

  // ESC to close
  useEffect(() => {
    const h = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
  }, [onClose])

  const STATUS_LABELS: Record<number, string> = {
    9: 'Активна', 11: 'На паузе', 4: 'Готова', 7: 'Завершена', 8: 'Отказ',
  }
  const STATUS_COLORS: Record<number, { bg: string; text: string; dot: string }> = {
    9: { bg: 'bg-emerald-500/10', text: 'text-emerald-600 dark:text-emerald-400', dot: 'bg-emerald-500' },
    11: { bg: 'bg-amber-500/10', text: 'text-amber-600 dark:text-amber-400', dot: 'bg-amber-500' },
    4: { bg: 'bg-blue-500/10', text: 'text-blue-600 dark:text-blue-400', dot: 'bg-blue-500' },
    7: { bg: 'bg-zinc-500/10', text: 'text-zinc-500', dot: 'bg-zinc-500' },
    8: { bg: 'bg-red-500/10', text: 'text-red-500', dot: 'bg-red-500' },
  }

  const sc = STATUS_COLORS[currentCampaign.status] || STATUS_COLORS[8]
  const ptLabel = currentCampaign.payment_type === 'cpm' ? 'CPM' : currentCampaign.payment_type === 'cpc' ? 'CPC' : currentCampaign.payment_type?.toUpperCase() || '—'

  const tabs = [
    { id: 'management' as UnifiedTab, label: 'Управление', icon: Settings2 },
    { id: 'analytics' as UnifiedTab, label: 'Статистика', icon: BarChart3 },
  ]

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
        className="w-full max-w-[1200px] max-h-[92vh] bg-[hsl(var(--card))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl flex flex-col overflow-hidden"
      >
        {/* ── Unified Header ──────────────────────────────────────── */}
        <div className="flex items-center justify-between px-6 py-3.5 border-b border-[hsl(var(--border))] shrink-0">
          <div className="flex items-center gap-4 min-w-0 flex-1">
            <div className="min-w-0">
              <div className="flex items-center gap-2.5 flex-wrap">
                <h2 className="text-lg font-bold text-[hsl(var(--foreground))] truncate max-w-[400px]">
                  {currentCampaign.name || `Кампания ${currentCampaign.advert_id}`}
                </h2>
                <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${sc.bg} ${sc.text}`}>
                  <span className={`w-1.5 h-1.5 rounded-full ${sc.dot}`} />
                  {STATUS_LABELS[currentCampaign.status] || '—'}
                </span>
                <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold
                  ${ptLabel === 'CPM' ? 'bg-violet-500/15 text-violet-600 dark:text-violet-400' :
                    ptLabel === 'CPC' ? 'bg-cyan-500/15 text-cyan-600 dark:text-cyan-400' :
                      'bg-emerald-500/15 text-emerald-600 dark:text-emerald-400'}`}
                >
                  {ptLabel}
                </span>
              </div>
              <div className="flex items-center gap-3 mt-0.5 text-xs text-[hsl(var(--muted-foreground))]">
                <span>ID: {currentCampaign.advert_id}</span>
                {currentCampaign.search_enabled && <span>🔍 Поиск</span>}
                {currentCampaign.recommendations_enabled && <span>📦 Полки</span>}
              </div>
            </div>
          </div>

          {/* Meta-tabs — pill switcher */}
          <div className="flex items-center gap-1 bg-[hsl(var(--muted))]/20 rounded-xl p-1 mx-4">
            {tabs.map(t => {
              const Icon = t.icon
              const isActive = activeTab === t.id
              return (
                <button
                  key={t.id}
                  onClick={() => setActiveTab(t.id)}
                  className={`flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200
                    ${isActive
                      ? 'bg-[hsl(var(--card))] text-[hsl(var(--foreground))] shadow-sm border border-[hsl(var(--border))]/50'
                      : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))]/30'
                    }`}
                >
                  <Icon className="w-4 h-4" />
                  {t.label}
                </button>
              )
            })}
          </div>

          <button
            onClick={onClose}
            className="p-2 rounded-lg hover:bg-[hsl(var(--muted))]/30 text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors shrink-0"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* ── Tab Content ────────────────────────────────────────── */}
        <div className="flex-1 overflow-y-auto">
          {activeTab === 'management' && (
            <CampaignManagementModalInner
              campaign={currentCampaign}
              shopId={shopId}
              onCampaignUpdate={handleCampaignUpdate}
            />
          )}
          {activeTab === 'analytics' && (
            <CampaignDetailModalInner
              marketplace={marketplace === 'wildberries' ? 'wb' : marketplace}
              campaignId={currentCampaign.advert_id}
              campaignTitle={currentCampaign.name || `Кампания ${currentCampaign.advert_id}`}
            />
          )}
        </div>
      </motion.div>
    </motion.div>
  )
}

/**
 * Inner management content — renders CampaignManagementModal WITHOUT its own overlay/wrapper.
 * We import the component and render only its content portion.
 */
function CampaignManagementModalInner({
  campaign,
  shopId,
  onCampaignUpdate,
}: {
  campaign: EnrichedCampaign
  shopId: number
  onCampaignUpdate?: (campaign: EnrichedCampaign) => void
}) {
  // Render the full management modal, but in "embedded" mode (no overlay)
  return (
    <CampaignManagementModal
      campaign={campaign}
      shopId={shopId}
      onClose={() => {}} // noop — close is handled by unified wrapper
      onCampaignUpdate={onCampaignUpdate}
      embedded={true}
    />
  )
}

/**
 * Inner analytics content — renders CampaignDetailModal WITHOUT its own overlay/wrapper.
 */
function CampaignDetailModalInner({
  marketplace,
  campaignId,
  campaignTitle,
}: {
  marketplace: string
  campaignId: number
  campaignTitle: string
}) {
  return (
    <CampaignDetailModal
      isOpen={true}
      onClose={() => {}} // noop — close is handled by unified wrapper
      marketplace={marketplace}
      campaignId={campaignId}
      campaignTitle={campaignTitle}
      startDate=""
      endDate=""
      embedded={true}
    />
  )
}
