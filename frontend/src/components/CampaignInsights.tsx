import { useMemo, useState, useEffect } from 'react'
import {
  AlertTriangle,
  TrendingUp,
  Zap,
  Ban,
  Trophy,
  DollarSign,
  BarChart2,
  Eye,
  MousePointerClick,
  Target,
  AlertCircle,
  CheckCircle2,
  Info,
  Activity,
  ArrowUp,
  ArrowDown,
  Minus,
  Gauge,
  ChevronDown,
} from 'lucide-react'
import type { CampaignRow, EventDaySummary, CampaignDailyPoint, CampaignEvent } from '@/api/advertising'
import { getCampaignDailyStats } from '@/api/advertising'

/* ═══ Helpers ═══ */

const fmt = (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })
const fmtM = (v: number) => fmt(v) + ' ₽'
const pct = (v: number) => v.toFixed(1) + '%'
const avg = (arr: number[]) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0

function getTotalRev(c: CampaignRow, fallback: Record<number, number>) {
  const raw = c.total_revenue || fallback[c.campaign_id] || 0
  return raw > 0 ? Math.max(raw, c.revenue) : 0
}

function dCol(d: number, inv = false) {
  const good = inv ? d < -5 : d > 5
  const bad = inv ? d > 5 : d < -5
  return good ? 'text-emerald-400' : bad ? 'text-red-400' : 'text-[hsl(var(--muted-foreground)/0.5)]'
}
function DArr({ d, inv = false }: { d: number; inv?: boolean }) {
  const good = inv ? d < -5 : d > 5
  const bad = inv ? d > 5 : d < -5
  const Icon = good ? ArrowUp : bad ? ArrowDown : Minus
  const c = good ? 'text-emerald-400' : bad ? 'text-red-400' : 'text-[hsl(var(--muted-foreground)/0.3)]'
  return <Icon className={`h-3 w-3 ${c} inline`} />
}

/* ═══ Tab button ═══ */
function Tab({ active, icon, label, count, cc, onClick }: {
  active: boolean; icon: React.ReactNode; label: string; count: number; cc: string; onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-[13px] font-medium transition-all whitespace-nowrap ${
        active
          ? 'bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))] border border-[hsl(var(--primary)/0.25)]'
          : 'text-[hsl(var(--muted-foreground)/0.6)] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.1)]'
      }`}
    >
      {icon}
      <span>{label}</span>
      <span className={`text-[12px] font-semibold ${active ? cc : 'text-[hsl(var(--muted-foreground)/0.4)]'}`}>{count}</span>
    </button>
  )
}

/* ═══ Scrollable content with expand ═══ */
function LimitedHeight({ children, maxH = 360 }: { children: React.ReactNode; maxH?: number }) {
  const [expanded, setExpanded] = useState(false)
  const [needsExpand, setNeedsExpand] = useState(false)
  const ref = useState<HTMLDivElement | null>(null)

  return (
    <div className="relative">
      <div
        ref={el => { ref[1](el); if (el) setNeedsExpand(el.scrollHeight > maxH) }}
        className="overflow-hidden transition-[max-height] duration-300"
        style={{ maxHeight: expanded ? 'none' : `${maxH}px` }}
      >
        {children}
      </div>
      {needsExpand && !expanded && (
        <div className="absolute bottom-0 left-0 right-0 h-16 bg-gradient-to-t from-[hsl(var(--card))] to-transparent flex items-end justify-center pb-2">
          <button
            onClick={() => setExpanded(true)}
            className="flex items-center gap-1 px-4 py-1.5 rounded-full bg-[hsl(var(--muted)/0.3)] hover:bg-[hsl(var(--muted)/0.5)] text-[12px] font-medium text-[hsl(var(--foreground))] transition-colors border border-[hsl(var(--border)/0.3)]"
          >
            Показать всё <ChevronDown className="h-3 w-3" />
          </button>
        </div>
      )}
    </div>
  )
}

/* ═══ Compact campaign row ═══ */
function CRow({ c, children, border = 'border-[hsl(var(--border)/0.2)]', bg = '' }: {
  c: CampaignRow; children: React.ReactNode; border?: string; bg?: string
}) {
  return (
    <div className={`rounded-lg border ${border} ${bg} px-3 py-2`}>
      <div className="font-semibold text-[13px] text-[hsl(var(--foreground))] truncate mb-1" title={c.title}>{c.title}</div>
      {children}
    </div>
  )
}

/* Single row of metrics — compact */
function Metrics({ items }: { items: Array<{ l: string; v: string | number; bad?: boolean; good?: boolean }> }) {
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-0.5 text-[13px]">
      {items.map(({ l, v, bad, good }, i) => (
        <span key={i} className="whitespace-nowrap">
          <span className="text-[hsl(var(--muted-foreground)/0.5)]">{l}: </span>
          <span className={`font-medium ${bad ? 'text-red-400' : good ? 'text-emerald-400' : 'text-[hsl(var(--foreground))]'}`}>{v}</span>
        </span>
      ))}
    </div>
  )
}

/* ═══ Inline Event Impact ═══ */
function InlineEventImpact({ campaign, dailyData, events, totalRevenue }: {
  campaign: CampaignRow; dailyData: CampaignDailyPoint[]; events: CampaignEvent[]; totalRevenue: number
}) {
  const sorted = useMemo(() => [...dailyData].sort((a, b) => a.date.localeCompare(b.date)), [dailyData])
  const dateIdx = useMemo(() => new Map(sorted.map((d, i) => [d.date, i])), [sorted])

  const eventDates = useMemo(() => {
    const m = new Map<string, CampaignEvent[]>()
    for (const e of events) { m.set(e.date, [...(m.get(e.date) || []), e]) }
    return [...m.entries()].sort((a, b) => a[0].localeCompare(b[0]))
  }, [events])

  if (events.length === 0) return null

  return (
    <div className="rounded-lg border border-blue-500/15 bg-blue-500/[0.02] p-3">
      <div className="flex items-baseline gap-2 mb-2">
        <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">{campaign.title}</span>
        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)] ml-auto">
          расход {fmtM(campaign.spend)} · рекл. {fmtM(campaign.revenue)} · общая {fmtM(totalRevenue)}
        </span>
      </div>
      <div className="space-y-1.5">
        {eventDates.map(([evDate, dayEvents]) => {
          const idx = dateIdx.get(evDate)
          const dateStr = new Date(evDate + 'T00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })
          let delta: Record<string, number> | null = null
          if (idx !== undefined) {
            const before = sorted.slice(Math.max(0, idx - 3), idx)
            const after = sorted.slice(idx + 1, Math.min(sorted.length, idx + 4))
            if (before.length >= 1 && after.length >= 1) {
              const bS = avg(before.map(d => d.spend)), aS = avg(after.map(d => d.spend))
              const bV = avg(before.map(d => d.views)), aV = avg(after.map(d => d.views))
              const bC = avg(before.map(d => d.clicks)), aC = avg(after.map(d => d.clicks))
              const bO = avg(before.map(d => d.orders)), aO = avg(after.map(d => d.orders))
              const bD = avg(before.map(d => d.drr)), aD = avg(after.map(d => d.drr))
              delta = {
                spend: bS > 0 ? ((aS - bS) / bS) * 100 : 0,
                views: bV > 0 ? ((aV - bV) / bV) * 100 : 0,
                clicks: bC > 0 ? ((aC - bC) / bC) * 100 : 0,
                orders: bO > 0 ? ((aO - bO) / bO) * 100 : 0,
                drr: bD > 0 ? ((aD - bD) / bD) * 100 : 0,
              }
            }
          }
          const hasChange = delta && Object.values(delta).some(v => Math.abs(v) > 5)
          return (
            <div key={evDate} className="rounded-md border border-[hsl(var(--border)/0.15)] bg-[hsl(var(--muted)/0.04)] p-2">
              {dayEvents.map(e => (
                <div key={e.id} className="flex items-baseline gap-2 text-[12px] leading-relaxed">
                  <span className="text-[hsl(var(--muted-foreground)/0.4)] shrink-0">{dateStr} {e.time}</span>
                  <span className={
                    e.category === 'advertising' ? 'text-blue-400' :
                    e.category === 'price' ? 'text-amber-400' :
                    e.category === 'content' ? 'text-purple-400' : 'text-cyan-400'
                  }>{e.label}</span>
                  {e.detail && <span className="text-[hsl(var(--muted-foreground)/0.6)]">{e.detail}</span>}
                </div>
              ))}
              {hasChange && delta ? (
                <div className="flex flex-wrap gap-x-3 gap-y-0.5 mt-1 text-[11px]">
                  <span className="text-[hsl(var(--muted-foreground)/0.35)]">→</span>
                  <InD label="расход" d={delta.spend} inv />
                  <InD label="показы" d={delta.views} />
                  <InD label="клики" d={delta.clicks} />
                  <InD label="заказы" d={delta.orders} />
                  <InD label="ДРР" d={delta.drr} inv />
                </div>
              ) : delta === null ? (
                <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.3)] mt-0.5">→ нет данных для сравнения</div>
              ) : (
                <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.3)] mt-0.5">→ незначительные изменения</div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

function InD({ label, d, inv = false }: { label: string; d: number; inv?: boolean }) {
  return (
    <span className="whitespace-nowrap">
      <span className="text-[hsl(var(--muted-foreground)/0.4)]">{label} </span>
      <span className={`font-medium ${dCol(d, inv)}`}><DArr d={d} inv={inv} /> {d > 0 ? '+' : ''}{d.toFixed(0)}%</span>
    </span>
  )
}

/* ═══ Tab IDs ═══ */
type TabId = 'lowSpend' | 'burning' | 'highDrr' | 'lowCtr' | 'effective' | 'events' | 'recs'

/* ═══ Main Component ═══ */
export function CampaignInsights({ campaigns, eventsByDay, shopId, dateFrom, dateTo }: {
  campaigns: CampaignRow[]; eventsByDay: Record<string, EventDaySummary>
  shopId: number; dateFrom: string; dateTo: string
}) {
  const [campaignDaily, setCampaignDaily] = useState<Record<number, CampaignDailyPoint[]>>({})
  const [eventsByCampaign, setEventsByCampaign] = useState<Record<number, CampaignEvent[]>>({})
  const [campaignTotalRev, setCampaignTotalRev] = useState<Record<number, number>>({})
  const [loading, setLoading] = useState(false)
  const [activeTab, setActiveTab] = useState<TabId>('recs')

  useEffect(() => {
    if (!shopId || !dateFrom || !dateTo) return
    setLoading(true)
    getCampaignDailyStats(shopId, dateFrom, dateTo)
      .then(d => {
        setCampaignDaily(d.campaigns_daily || {})
        setEventsByCampaign(d.events_by_campaign || {})
        setCampaignTotalRev(d.campaign_total_revenue || {})
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [shopId, dateFrom, dateTo])

  const periodDays = useMemo(() => {
    if (!dateFrom || !dateTo) return 7
    const d1 = new Date(dateFrom), d2 = new Date(dateTo)
    return Math.max(1, Math.round((d2.getTime() - d1.getTime()) / 86400000) + 1)
  }, [dateFrom, dateTo])

  const a = useMemo(() => {
    const active = campaigns.filter(c => c.status !== 'archived')
    const totalSpend = active.reduce((s, c) => s + c.spend, 0)
    const totalRev = active.reduce((s, c) => s + c.revenue, 0)
    const totalOrders = active.reduce((s, c) => s + c.orders, 0)
    const totalTotalRev = active.reduce((s, c) => s + getTotalRev(c, campaignTotalRev), 0)
    const withOrders = active.filter(c => c.orders > 0)
    const withoutOrders = active.filter(c => c.orders === 0 && c.spend > 0)

    const avgCpc = active.filter(c => c.avg_cpc > 0).reduce((s, c) => s + c.avg_cpc, 0) / (active.filter(c => c.avg_cpc > 0).length || 1)
    const avgDrr = withOrders.length ? withOrders.reduce((s, c) => s + c.drr, 0) / withOrders.length : 0
    const avgCpo = withOrders.length ? withOrders.reduce((s, c) => s + c.spend / c.orders, 0) / withOrders.length : 0

    const lowSpendThreshold = 30 * periodDays
    const lowSpend = active.filter(c =>
      c.spend > 0 && c.spend < lowSpendThreshold && c.views < 500 * periodDays
    ).sort((a, b) => a.spend - b.spend)
    const lowSpendIds = new Set(lowSpend.map(c => c.campaign_id))

    const burning = withoutOrders.filter(c => !lowSpendIds.has(c.campaign_id)).sort((a, b) => b.spend - a.spend)
    const unprofitable = withOrders.filter(c => c.drr > 50).sort((a, b) => b.drr - a.drr)
    const lowCtr = active.filter(c => c.ctr < 1 && c.views > 2000 && c.spend > 100).sort((a, b) => a.ctr - b.ctr)
    const effective = withOrders.filter(c => c.drr < 40 && c.drr > 0 && c.orders >= 2 && c.spend > 100).sort((a, b) => a.drr - b.drr)
    const withEvents = active.filter(c => {
      const cid = c.campaign_id
      return eventsByCampaign[cid]?.length > 0 && campaignDaily[cid]?.length >= 3
    })

    const evtSum = Object.values(eventsByDay).reduce(
      (acc, d) => ({ a: acc.a + d.advertising, c: acc.c + d.content, p: acc.p + d.price, s: acc.s + d.stock, t: acc.t + d.total }),
      { a: 0, c: 0, p: 0, s: 0, t: 0 },
    )

    const wastedSpend = burning.reduce((s, c) => s + c.spend, 0)

    // Unique problem campaigns count
    const problemIds = new Set([
      ...lowSpend.map(c => c.campaign_id),
      ...burning.map(c => c.campaign_id),
      ...unprofitable.map(c => c.campaign_id),
      ...lowCtr.map(c => c.campaign_id),
    ])
    const problemCount = problemIds.size

    return { totalSpend, totalRev, totalTotalRev, totalOrders, wastedSpend,
      avgCpc, avgDrr, avgCpo,
      count: active.length, withOrdersN: withOrders.length, withoutN: withoutOrders.length,
      burning, unprofitable, lowCtr, effective, withEvents, evtSum, lowSpend, problemCount }
  }, [campaigns, eventsByDay, eventsByCampaign, campaignDaily, campaignTotalRev, periodDays])

  // Always start on recommendations
  useEffect(() => {
    setActiveTab('recs')
  }, [campaigns])

  if (!campaigns.length) return null

  /* ═══ Tab definitions ═══ */
  const tabs: Array<{ id: TabId; icon: React.ReactNode; label: string; count: number; cc: string }> = [
    { id: 'recs' as TabId, icon: <AlertCircle className="h-3.5 w-3.5" />, label: 'Рекомендации', count: a.problemCount, cc: a.problemCount > 0 ? 'text-red-400' : 'text-emerald-400' },
    ...(a.lowSpend.length > 0 ? [{ id: 'lowSpend' as TabId, icon: <Gauge className="h-3.5 w-3.5" />, label: 'Мало показов', count: a.lowSpend.length, cc: 'text-zinc-400' }] : []),
    ...(a.burning.length > 0 ? [{ id: 'burning' as TabId, icon: <Ban className="h-3.5 w-3.5" />, label: 'Сливают', count: a.burning.length, cc: 'text-red-400' }] : []),
    ...(a.unprofitable.length > 0 ? [{ id: 'highDrr' as TabId, icon: <AlertTriangle className="h-3.5 w-3.5" />, label: 'Высокий ДРР', count: a.unprofitable.length, cc: 'text-amber-400' }] : []),
    ...(a.lowCtr.length > 0 ? [{ id: 'lowCtr' as TabId, icon: <Eye className="h-3.5 w-3.5" />, label: 'Низкий CTR', count: a.lowCtr.length, cc: 'text-orange-400' }] : []),
    ...(a.effective.length > 0 ? [{ id: 'effective' as TabId, icon: <Trophy className="h-3.5 w-3.5" />, label: 'Эффективные', count: a.effective.length, cc: 'text-emerald-400' }] : []),
    { id: 'events' as TabId, icon: <Zap className="h-3.5 w-3.5" />, label: 'События', count: a.evtSum.t, cc: 'text-blue-400' },
  ]

  return (
    <div className="space-y-4">
      {/* Title */}
      <div className="flex items-center gap-2">
        <Activity className="h-5 w-5 text-[hsl(var(--primary))]" />
        <h3 className="text-[18px] font-bold text-[hsl(var(--foreground))]">Анализ рекламных кампаний</h3>
      </div>

      {/* Summary */}
      <div className="rounded-2xl border border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.06)] p-4">
        <div className="grid grid-cols-3 gap-4 sm:grid-cols-6">
          <SC icon={<Target className="h-3.5 w-3.5 text-[hsl(var(--primary))]" />} l="Кампаний" v={a.count} />
          <SC icon={<CheckCircle2 className="h-3.5 w-3.5 text-emerald-400" />} l="С заказами" v={a.withOrdersN} vc="text-emerald-400" />
          <SC icon={<Ban className="h-3.5 w-3.5 text-red-400" />} l="Без заказов" v={a.withoutN} vc="text-red-400" />
          <SC icon={<MousePointerClick className="h-3.5 w-3.5 text-blue-400" />} l="Ср. CPC" v={fmtM(a.avgCpc)} />
          <SC icon={<BarChart2 className="h-3.5 w-3.5 text-amber-400" />} l="Ср. ДРР" v={pct(a.avgDrr)} />
          <SC icon={<DollarSign className="h-3.5 w-3.5 text-purple-400" />} l="Ср. CPO" v={fmtM(a.avgCpo)} />
        </div>
        <div className="mt-3 pt-3 border-t border-[hsl(var(--border)/0.3)] text-[13px] text-[hsl(var(--muted-foreground))]">
          Расход <b className="text-[hsl(var(--foreground))]">{fmtM(a.totalSpend)}</b>
          {' → '}<b className="text-[hsl(var(--foreground))]">{a.totalOrders}</b> рекл. заказов
          {' на '}<b className="text-[hsl(var(--foreground))]">{fmtM(a.totalRev)}</b> (рекл.)
          {a.totalTotalRev > 0 && <>, общая выручка товаров <b className="text-[hsl(var(--foreground))]">{fmtM(a.totalTotalRev)}</b></>}
          {a.wastedSpend > 0 && <>. Впустую: <b className="text-red-400">{fmtM(a.wastedSpend)}</b></>}
        </div>
      </div>

      {/* Tabs */}
      <div className="rounded-2xl border border-[hsl(var(--border)/0.5)] overflow-hidden">
        {/* Tab bar */}
        <div className="flex gap-1 px-3 py-2 overflow-x-auto border-b border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)]">
          {tabs.map(t => (
            <Tab key={t.id} active={activeTab === t.id} icon={t.icon} label={t.label}
              count={t.count} cc={t.cc} onClick={() => setActiveTab(t.id)} />
          ))}
        </div>

        {/* Tab content */}
        <div className="p-4">
          <LimitedHeight>
            {activeTab === 'lowSpend' && <TabLowSpend items={a.lowSpend} periodDays={periodDays} />}
            {activeTab === 'burning' && <TabBurning items={a.burning} campaignTotalRev={campaignTotalRev} />}
            {activeTab === 'highDrr' && <TabHighDrr items={a.unprofitable} campaignTotalRev={campaignTotalRev} />}
            {activeTab === 'lowCtr' && <TabLowCtr items={a.lowCtr} />}
            {activeTab === 'effective' && <TabEffective items={a.effective} campaignTotalRev={campaignTotalRev} />}
            {activeTab === 'events' && (
              <TabEvents
                loading={loading} withEvents={a.withEvents} evtSum={a.evtSum}
                campaignDaily={campaignDaily} eventsByCampaign={eventsByCampaign}
                campaignTotalRev={campaignTotalRev}
              />
            )}
            {activeTab === 'recs' && <TabRecs a={a} />}
          </LimitedHeight>
        </div>
      </div>
    </div>
  )
}

/* ═══ Tab Content Components ═══ */

function TabLowSpend({ items, periodDays }: { items: CampaignRow[]; periodDays: number }) {
  return (
    <>
      <p className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)] mb-3">
        Ставка слишком низкая — алгоритм Ozon не показывает объявление. Повысьте ставку.
      </p>
      <div className="space-y-1.5">
        {items.map(c => (
          <CRow key={c.campaign_id} c={c} border="border-zinc-500/15" bg="bg-zinc-500/[0.02]">
            <Metrics items={[
              { l: 'Расход', v: fmtM(c.spend) },
              { l: '₽/день', v: fmtM(c.spend / periodDays), bad: true },
              { l: 'Показы', v: fmt(c.views) },
              { l: 'Клики', v: fmt(c.clicks) },
              ...(c.orders > 0 ? [{ l: 'Заказы', v: c.orders }] : []),
            ]} />
          </CRow>
        ))}
      </div>
    </>
  )
}

function TabBurning({ items, campaignTotalRev }: { items: CampaignRow[]; campaignTotalRev: Record<number, number> }) {
  return (
    <div className="space-y-1.5">
      {items.map(c => {
        const reason = c.cart === 0
          ? `${fmt(c.views)} показов, ${c.clicks} кликов, 0 корзин — товар не интересен`
          : `${c.cart} корзин → 0 заказов — проблема с ценой/доставкой`
        const tRev = getTotalRev(c, campaignTotalRev)
        return (
          <CRow key={c.campaign_id} c={c} border="border-red-500/15" bg="bg-red-500/[0.02]">
            <Metrics items={[
              { l: 'Расход', v: fmtM(c.spend), bad: true },
              { l: 'CTR', v: pct(c.ctr), bad: c.ctr < 0.5 },
              ...(tRev > 0 ? [{ l: 'Выручка общ.', v: fmtM(tRev) }] : []),
              ...(tRev > 0 ? [{ l: 'ДРР(общ)', v: pct(c.spend / tRev * 100), bad: c.spend / tRev * 100 > 50 }] : []),
            ]} />
            <div className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)] mt-0.5">• {reason}</div>
          </CRow>
        )
      })}
    </div>
  )
}

function TabHighDrr({ items, campaignTotalRev }: { items: CampaignRow[]; campaignTotalRev: Record<number, number> }) {
  return (
    <div className="space-y-1.5">
      {items.map(c => {
        const tRev = getTotalRev(c, campaignTotalRev)
        const tDrr = tRev > 0 ? c.spend / tRev * 100 : 0
        return (
          <CRow key={c.campaign_id} c={c} border="border-amber-500/15" bg="bg-amber-500/[0.02]">
            <Metrics items={[
              { l: 'Расход', v: fmtM(c.spend) },
              { l: 'Заказы', v: c.orders },
              { l: 'Выручка рекл.', v: fmtM(c.revenue) },
              { l: 'ДРР рекл.', v: pct(c.drr), bad: c.drr > 100 },
              ...(tRev > 0 ? [{ l: 'Выручка общ.', v: fmtM(tRev) }] : []),
              ...(tDrr > 0 ? [{ l: 'ДРР общ.', v: pct(tDrr), bad: tDrr > 50, good: tDrr < 20 }] : []),
            ]} />
          </CRow>
        )
      })}
    </div>
  )
}

function TabLowCtr({ items }: { items: CampaignRow[] }) {
  return (
    <div className="space-y-1.5">
      {items.map(c => (
        <CRow key={c.campaign_id} c={c} border="border-orange-500/15" bg="bg-orange-500/[0.02]">
          <Metrics items={[
            { l: 'CTR', v: pct(c.ctr), bad: true },
            { l: 'Показы', v: fmt(c.views) },
            { l: 'Клики', v: fmt(c.clicks) },
            { l: 'Расход', v: fmtM(c.spend) },
          ]} />
        </CRow>
      ))}
    </div>
  )
}

function TabEffective({ items, campaignTotalRev }: { items: CampaignRow[]; campaignTotalRev: Record<number, number> }) {
  return (
    <div className="space-y-1.5">
      {items.map(c => {
        const romi = c.spend > 0 ? c.revenue / c.spend * 100 : 0
        const cpo = c.orders > 0 ? c.spend / c.orders : 0
        const tRev = getTotalRev(c, campaignTotalRev)
        const tDrr = tRev > 0 ? c.spend / tRev * 100 : 0
        return (
          <CRow key={c.campaign_id} c={c} border="border-emerald-500/15" bg="bg-emerald-500/[0.02]">
            <Metrics items={[
              { l: 'Расход', v: fmtM(c.spend) },
              { l: 'Заказы', v: c.orders, good: true },
              { l: 'Выручка рекл.', v: fmtM(c.revenue), good: true },
              { l: 'ДРР', v: pct(c.drr), good: true },
              { l: 'CPO', v: fmtM(cpo) },
              { l: 'ROMI', v: `${fmt(romi)}%`, good: romi > 200 },
              ...(tRev > 0 ? [{ l: 'Выручка общ.', v: fmtM(tRev), good: true }] : []),
              ...(tDrr > 0 ? [{ l: 'ДРР общ.', v: pct(tDrr), good: tDrr < 20 }] : []),
            ]} />
            {c.direct_orders > 0 && c.model_orders > 0 && (
              <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)] mt-0.5">
                Прямые: {c.direct_orders}, модельные: {c.model_orders}
              </div>
            )}
          </CRow>
        )
      })}
    </div>
  )
}

function TabEvents({ loading, withEvents, evtSum, campaignDaily, eventsByCampaign, campaignTotalRev }: {
  loading: boolean; withEvents: CampaignRow[]
  evtSum: { a: number; c: number; p: number; s: number; t: number }
  campaignDaily: Record<number, CampaignDailyPoint[]>
  eventsByCampaign: Record<number, CampaignEvent[]>
  campaignTotalRev: Record<number, number>
}) {
  if (loading) return <div className="py-4 text-center text-[13px] text-[hsl(var(--muted-foreground)/0.4)]">Загрузка...</div>
  if (withEvents.length === 0) return (
    <div className="py-3 text-center text-[13px] text-[hsl(var(--muted-foreground)/0.4)]">
      {evtSum.t > 0 ? 'Недостаточно данных (нужно ≥3 дня статистики)' : 'Нет событий'}
    </div>
  )
  return (
    <>
      <p className="text-[13px] text-[hsl(var(--muted-foreground))] mb-2">
        Для каждой кампании: событие → изменение метрик (3 дня до/после).
      </p>
      <div className="flex gap-3 mb-3 text-[12px]">
        {evtSum.a > 0 && <span className="text-blue-400">● Рекламные {evtSum.a}</span>}
        {evtSum.c > 0 && <span className="text-purple-400">● Контент {evtSum.c}</span>}
        {evtSum.p > 0 && <span className="text-amber-400">● Цена {evtSum.p}</span>}
        {evtSum.s > 0 && <span className="text-cyan-400">● Склад {evtSum.s}</span>}
      </div>
      <div className="space-y-2">
        {withEvents.map(c => (
          <InlineEventImpact
            key={c.campaign_id}
            campaign={c}
            dailyData={campaignDaily[c.campaign_id] || []}
            events={eventsByCampaign[c.campaign_id] || []}
            totalRevenue={getTotalRev(c, campaignTotalRev)}
          />
        ))}
      </div>
    </>
  )
}

function TabRecs({ a }: { a: { lowSpend: CampaignRow[]; burning: CampaignRow[]; unprofitable: CampaignRow[]; effective: CampaignRow[]; evtSum: { t: number } } }) {
  const noProblems = a.lowSpend.length === 0 && a.burning.length === 0 && a.unprofitable.length === 0
  return (
    <div className="space-y-3">
      {noProblems && (
        <div className="flex items-center gap-2 text-[14px] text-emerald-400 py-2">
          <CheckCircle2 className="h-4 w-4" /> Проблем не обнаружено
        </div>
      )}

      {/* Мало показов */}
      {a.lowSpend.length > 0 && (
        <RecGroup
          s="i"
          title="Мало показов — повысьте ставку"
          desc="Мизерный расход, алгоритм Ozon не показывает объявления:"
          items={a.lowSpend}
          detail={c => `${fmtM(c.spend)}, ${fmt(c.views)} показов`}
        />
      )}

      {/* Сливают бюджет */}
      {a.burning.length > 0 && (
        <RecGroup
          s="c"
          title={`Сливают бюджет — ${fmtM(a.burning.reduce((s, c) => s + c.spend, 0))} впустую`}
          desc="Есть расход, но 0 заказов — отключите или пересмотрите:"
          items={a.burning}
          detail={c => c.cart === 0 ? `${fmtM(c.spend)}, 0 корзин` : `${fmtM(c.spend)}, ${c.cart} корзин → 0 заказов`}
        />
      )}

      {/* Высокий ДРР */}
      {a.unprofitable.length > 0 && (
        <RecGroup
          s="w"
          title="Высокий ДРР — снизьте ставки"
          desc="ДРР > 50%, реклама убыточна:"
          items={a.unprofitable}
          detail={c => `ДРР ${pct(c.drr)}, ${fmtM(c.spend)} расход, ${c.orders} заказов`}
        />
      )}

      {/* Эффективные */}
      {a.effective.length > 0 && (
        <RecGroup
          s="s"
          title="Эффективные — масштабируйте"
          desc="Низкий ДРР, хорошая конверсия — увеличьте бюджет:"
          items={a.effective}
          detail={c => `ДРР ${pct(c.drr)}, ${c.orders} заказов, ROMI ${fmt(c.spend > 0 ? c.revenue / c.spend * 100 : 0)}%`}
        />
      )}

      {a.evtSum.t > 50 && (
        <Rec s="i" t={`${a.evtSum.t} событий за период — частые изменения мешают алгоритмам Ozon оптимизировать показы`} />
      )}
    </div>
  )
}

/* ═══ Subcomponents ═══ */

function SC({ icon, l, v, vc }: { icon: React.ReactNode; l: string; v: string | number; vc?: string }) {
  return (
    <div>
      <div className="flex items-center gap-1 mb-0.5">{icon}<span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">{l}</span></div>
      <div className={`text-[16px] font-bold ${vc || 'text-[hsl(var(--foreground))]'}`}>{v}</div>
    </div>
  )
}

function RecGroup({ s, title, desc, items, detail }: {
  s: 'c' | 'w' | 's' | 'i'; title: string; desc: string
  items: CampaignRow[]; detail: (c: CampaignRow) => string
}) {
  const cfg = {
    c: { c: 'text-red-400', bg: 'bg-red-500/[0.04]', b: 'border-red-500/15', dot: 'bg-red-400' },
    w: { c: 'text-amber-400', bg: 'bg-amber-500/[0.04]', b: 'border-amber-500/15', dot: 'bg-amber-400' },
    s: { c: 'text-emerald-400', bg: 'bg-emerald-500/[0.04]', b: 'border-emerald-500/15', dot: 'bg-emerald-400' },
    i: { c: 'text-blue-400', bg: 'bg-blue-500/[0.04]', b: 'border-blue-500/15', dot: 'bg-blue-400' },
  }[s]
  return (
    <div className={`rounded-lg border ${cfg.b} ${cfg.bg} px-4 py-3`}>
      <div className={`text-[14px] font-semibold ${cfg.c} mb-1`}>{title}</div>
      <div className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)] mb-2">{desc}</div>
      <div className="space-y-1">
        {items.map(c => (
          <div key={c.campaign_id} className="flex items-baseline gap-2 text-[13px]">
            <span className={`w-1.5 h-1.5 rounded-full ${cfg.dot} shrink-0 mt-1.5`} />
            <span className="text-[hsl(var(--foreground))] font-medium">{c.title}</span>
            <span className="text-[hsl(var(--muted-foreground)/0.5)] text-[12px]">— {detail(c)}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function Rec({ s, t }: { s: 'c' | 'w' | 's' | 'i'; t: string }) {
  const cfg = {
    c: { i: <AlertCircle className="h-3.5 w-3.5" />, c: 'text-red-400', bg: 'bg-red-500/[0.04]', b: 'border-red-500/15' },
    w: { i: <AlertTriangle className="h-3.5 w-3.5" />, c: 'text-amber-400', bg: 'bg-amber-500/[0.04]', b: 'border-amber-500/15' },
    s: { i: <TrendingUp className="h-3.5 w-3.5" />, c: 'text-emerald-400', bg: 'bg-emerald-500/[0.04]', b: 'border-emerald-500/15' },
    i: { i: <Info className="h-3.5 w-3.5" />, c: 'text-blue-400', bg: 'bg-blue-500/[0.04]', b: 'border-blue-500/15' },
  }[s]
  return (
    <div className={`flex items-start gap-2 rounded-lg border ${cfg.b} ${cfg.bg} px-3 py-2`}>
      <span className={`mt-0.5 shrink-0 ${cfg.c}`}>{cfg.i}</span>
      <span className="text-[13px] text-[hsl(var(--foreground))]">{t}</span>
    </div>
  )
}
