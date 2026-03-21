import { useMemo, useState, useEffect } from 'react'
import {
  AlertTriangle,
  TrendingUp,
  Zap,
  Ban,
  Trophy,
  DollarSign,
  BarChart2,
  ChevronDown,
  ChevronUp,
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
} from 'lucide-react'
import type { CampaignRow, EventDaySummary, CampaignDailyPoint, CampaignEvent } from '@/api/advertising'
import { getCampaignDailyStats } from '@/api/advertising'

/* ═══ Helpers ═══ */

const fmt = (v: number) => v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })
const fmtM = (v: number) => fmt(v) + ' ₽'
const pct = (v: number) => v.toFixed(1) + '%'
const avg = (arr: number[]) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0

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

/* ═══ Collapsible ═══ */

function Sec({ icon, title, count, cc, accent, badge, open: defOpen = true, children }: {
  icon: React.ReactNode; title: string; count?: number; cc?: string; accent?: string
  badge?: React.ReactNode; open?: boolean; children: React.ReactNode
}) {
  const [open, setOpen] = useState(defOpen)
  return (
    <div className="rounded-2xl border overflow-hidden" style={{ borderColor: accent ? `${accent}30` : 'hsl(var(--border)/0.5)' }}>
      <button onClick={() => setOpen(!open)} className="w-full flex items-center justify-between px-5 py-3 text-left transition-colors hover:bg-[hsl(var(--muted)/0.1)]">
        <div className="flex items-center gap-3">
          {icon}
          <span className="text-[15px] font-bold text-[hsl(var(--foreground))]">{title}</span>
          {count !== undefined && <span className={`text-[14px] font-semibold ${cc || ''}`}>{count}</span>}
          {badge}
        </div>
        {open ? <ChevronUp className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.3)]" /> : <ChevronDown className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.3)]" />}
      </button>
      {open && <div className="px-5 pb-4 border-t border-[hsl(var(--border)/0.3)]">{children}</div>}
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
  
  // Group events by date, sort by date
  const eventDates = useMemo(() => {
    const m = new Map<string, CampaignEvent[]>()
    for (const e of events) { m.set(e.date, [...(m.get(e.date) || []), e]) }
    return [...m.entries()].sort((a, b) => a[0].localeCompare(b[0]))
  }, [events])

  if (events.length === 0) return null

  return (
    <div className="mt-2 rounded-lg border border-blue-500/15 bg-blue-500/[0.02] p-3">
      <div className="flex items-center gap-2 mb-2">
        <Zap className="h-3.5 w-3.5 text-blue-400" />
        <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">{campaign.title}</span>
        <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)] ml-auto">
          расход {fmtM(campaign.spend)} · рекл. {fmtM(campaign.revenue)} · общая {fmtM(totalRevenue)}
        </span>
      </div>

      {/* Timeline: event → inline delta */}
      <div className="space-y-1.5">
        {eventDates.map(([evDate, dayEvents]) => {
          const idx = dateIdx.get(evDate)
          const dateStr = new Date(evDate + 'T00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })
          
          // Compute before/after if possible
          let delta: Record<string, number> | null = null
          if (idx !== undefined) {
            const before = sorted.slice(Math.max(0, idx - 3), idx)
            const after = sorted.slice(idx + 1, Math.min(sorted.length, idx + 4))
            if (before.length >= 1 && after.length >= 1) {
              const bSpend = avg(before.map(d => d.spend)), aSpend = avg(after.map(d => d.spend))
              const bViews = avg(before.map(d => d.views)), aViews = avg(after.map(d => d.views))
              const bClicks = avg(before.map(d => d.clicks)), aClicks = avg(after.map(d => d.clicks))
              const bOrders = avg(before.map(d => d.orders)), aOrders = avg(after.map(d => d.orders))
              const bDrr = avg(before.map(d => d.drr)), aDrr = avg(after.map(d => d.drr))
              delta = {
                spend: bSpend > 0 ? ((aSpend - bSpend) / bSpend) * 100 : 0,
                views: bViews > 0 ? ((aViews - bViews) / bViews) * 100 : 0,
                clicks: bClicks > 0 ? ((aClicks - bClicks) / bClicks) * 100 : 0,
                orders: bOrders > 0 ? ((aOrders - bOrders) / bOrders) * 100 : 0,
                drr: bDrr > 0 ? ((aDrr - bDrr) / bDrr) * 100 : 0,
              }
            }
          }

          const hasChange = delta && Object.values(delta).some(v => Math.abs(v) > 5)

          return (
            <div key={evDate} className="rounded-md border border-[hsl(var(--border)/0.15)] bg-[hsl(var(--muted)/0.04)] p-2">
              {/* Events on this date */}
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
              {/* Inline delta */}
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

/* ═══ Main Component ═══ */

export function CampaignInsights({ campaigns, eventsByDay, shopId, dateFrom, dateTo }: {
  campaigns: CampaignRow[]; eventsByDay: Record<string, EventDaySummary>
  shopId: number; dateFrom: string; dateTo: string
}) {
  const [campaignDaily, setCampaignDaily] = useState<Record<number, CampaignDailyPoint[]>>({})
  const [eventsByCampaign, setEventsByCampaign] = useState<Record<number, CampaignEvent[]>>({})
  const [campaignTotalRev, setCampaignTotalRev] = useState<Record<number, number>>({})
  const [loading, setLoading] = useState(false)

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

  const a = useMemo(() => {
    const active = campaigns.filter(c => c.status !== 'archived')
    const totalSpend = active.reduce((s, c) => s + c.spend, 0)
    const totalRev = active.reduce((s, c) => s + c.revenue, 0)
    const totalOrders = active.reduce((s, c) => s + c.orders, 0)
    const totalTotalRev = active.reduce((s, c) => s + (c.total_revenue || 0), 0)
    const withOrders = active.filter(c => c.orders > 0)
    const withoutOrders = active.filter(c => c.orders === 0 && c.spend > 0)
    const wastedSpend = withoutOrders.reduce((s, c) => s + c.spend, 0)

    const avgCpc = active.filter(c => c.avg_cpc > 0).reduce((s, c) => s + c.avg_cpc, 0) / (active.filter(c => c.avg_cpc > 0).length || 1)
    const avgDrr = withOrders.length ? withOrders.reduce((s, c) => s + c.drr, 0) / withOrders.length : 0
    const avgCpo = withOrders.length ? withOrders.reduce((s, c) => s + c.spend / c.orders, 0) / withOrders.length : 0

    // 1. Burning money — ALL with no orders, sorted by spend
    const burning = withoutOrders.sort((a, b) => b.spend - a.spend)

    // 2. Unprofitable (DRR > 50%) — ALL
    const unprofitable = withOrders.filter(c => c.drr > 50).sort((a, b) => b.drr - a.drr)

    // 3. Low CTR — ALL with ctr < 1% and views > 2000
    const lowCtr = active.filter(c => c.ctr < 1 && c.views > 2000 && c.spend > 100).sort((a, b) => a.ctr - b.ctr)

    // 4. Effective (DRR < 40%, orders >= 2) — ALL
    const effective = withOrders.filter(c => c.drr < 40 && c.drr > 0 && c.orders >= 2 && c.spend > 100).sort((a, b) => a.drr - b.drr)

    // 5. Campaigns with events
    const withEvents = active.filter(c => {
      const cid = c.campaign_id
      return eventsByCampaign[cid]?.length > 0 && campaignDaily[cid]?.length >= 3
    })

    const evtSum = Object.values(eventsByDay).reduce(
      (acc, d) => ({ a: acc.a + d.advertising, c: acc.c + d.content, p: acc.p + d.price, s: acc.s + d.stock, t: acc.t + d.total }),
      { a: 0, c: 0, p: 0, s: 0, t: 0 },
    )

    return { totalSpend, totalRev, totalTotalRev, totalOrders, wastedSpend,
      wastePct: totalSpend > 0 ? wastedSpend / totalSpend * 100 : 0,
      avgCpc, avgDrr, avgCpo,
      count: active.length, withOrdersN: withOrders.length, withoutN: withoutOrders.length,
      burning, unprofitable, lowCtr, effective, withEvents, evtSum }
  }, [campaigns, eventsByDay, eventsByCampaign, campaignDaily])

  if (!campaigns.length) return null

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
          {a.wastedSpend > 0 && <>. Впустую: <b className="text-red-400">{fmtM(a.wastedSpend)}</b> ({pct(a.wastePct)})</>}
        </div>
      </div>

      {/* 1. Burning budget */}
      {a.burning.length > 0 && (
        <Sec icon={<Ban className="h-4 w-4 text-red-400" />} title="Сливают бюджет" count={a.burning.length} cc="text-red-400" accent="rgb(239 68 68)"
          badge={<span className="rounded-md bg-red-500/10 px-2 py-0.5 text-[12px] font-semibold text-red-400">−{fmtM(a.wastedSpend)}</span>}>
          <div className="space-y-1.5 mt-2">
            {a.burning.map(c => {
              const reason = c.cart === 0
                ? `${fmt(c.views)} показов, ${c.clicks} кликов, 0 корзин — товар не интересен`
                : `${c.cart} корзин → 0 заказов — проблема с ценой/доставкой`
              const tRev = c.total_revenue || campaignTotalRev[c.campaign_id] || 0
              return (
                <CRow key={c.campaign_id} c={c} border="border-red-500/15" bg="bg-red-500/[0.02]">
                  <Metrics items={[
                    { l: 'Расход', v: fmtM(c.spend), bad: true },
                    { l: 'CTR', v: pct(c.ctr), bad: c.ctr < 0.5 },
                    ...(tRev > 0 ? [{ l: 'Общая выручка', v: fmtM(tRev) }] : []),
                    ...(tRev > 0 ? [{ l: 'ДРР(общ)', v: pct(c.spend / tRev * 100), bad: c.spend / tRev * 100 > 50 }] : []),
                  ]} />
                  <div className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)] mt-0.5">• {reason}</div>
                </CRow>
              )
            })}
          </div>
        </Sec>
      )}

      {/* 2. Unprofitable (DRR > 50%) */}
      {a.unprofitable.length > 0 && (
        <Sec icon={<AlertTriangle className="h-4 w-4 text-amber-400" />} title="Высокий ДРР" count={a.unprofitable.length} cc="text-amber-400" accent="rgb(245 158 11)">
          <div className="space-y-1.5 mt-2">
            {a.unprofitable.map(c => {
              const tRev = c.total_revenue || campaignTotalRev[c.campaign_id] || 0
              const tDrr = tRev > 0 ? c.spend / tRev * 100 : 0
              return (
                <CRow key={c.campaign_id} c={c} border="border-amber-500/15" bg="bg-amber-500/[0.02]">
                  <Metrics items={[
                    { l: 'Расход', v: fmtM(c.spend) },
                    { l: 'Заказы', v: c.orders },
                    { l: 'Выручка рекл.', v: fmtM(c.revenue) },
                    { l: 'ДРР рекл.', v: pct(c.drr), bad: c.drr > 100 },
                    ...(tRev > 0 ? [{ l: 'Выручка общ.', v: fmtM(tRev) }] : []),
                    ...(tRev > 0 ? [{ l: 'ДРР общ.', v: pct(tDrr), bad: tDrr > 50, good: tDrr < 20 }] : []),
                  ]} />
                </CRow>
              )
            })}
          </div>
        </Sec>
      )}

      {/* 3. Low CTR */}
      {a.lowCtr.length > 0 && (
        <Sec icon={<Eye className="h-4 w-4 text-orange-400" />} title="Низкий CTR" count={a.lowCtr.length} cc="text-orange-400" accent="rgb(251 146 60)">
          <div className="space-y-1.5 mt-2">
            {a.lowCtr.map(c => (
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
        </Sec>
      )}

      {/* 4. Effective */}
      {a.effective.length > 0 && (
        <Sec icon={<Trophy className="h-4 w-4 text-emerald-400" />} title="Эффективные" count={a.effective.length} cc="text-emerald-400" accent="rgb(52 211 153)"
          badge={<span className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)]">ДРР &lt; 40%, 2+ заказа</span>}>
          <div className="space-y-1.5 mt-2">
            {a.effective.map(c => {
              const romi = c.spend > 0 ? c.revenue / c.spend * 100 : 0
              const cpo = c.orders > 0 ? c.spend / c.orders : 0
              const tRev = c.total_revenue || campaignTotalRev[c.campaign_id] || 0
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
                    ...(tRev > 0 ? [{ l: 'ДРР общ.', v: pct(tDrr), good: tDrr < 20 }] : []),
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
        </Sec>
      )}

      {/* 5. Event Impact — per-campaign inline */}
      <Sec icon={<Zap className="h-4 w-4 text-blue-400" />} title="Влияние событий" count={a.evtSum.t} cc="text-blue-400" accent="rgb(96 165 250)"
        badge={a.withEvents.length > 0 ? <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)]">{a.withEvents.length} кампаний</span> : undefined}>
        {loading ? (
          <div className="py-4 text-center text-[13px] text-[hsl(var(--muted-foreground)/0.4)]">Загрузка...</div>
        ) : a.withEvents.length === 0 ? (
          <div className="py-3 text-center text-[13px] text-[hsl(var(--muted-foreground)/0.4)]">
            {a.evtSum.t > 0 ? 'Недостаточно данных (нужно ≥3 дня статистики)' : 'Нет событий'}
          </div>
        ) : (
          <>
            <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-2 mb-2">
              Для каждой кампании: событие → изменение метрик (3 дня до/после).
            </p>
            {/* Event type counts */}
            <div className="flex gap-3 mb-3 text-[12px]">
              {a.evtSum.a > 0 && <span className="text-blue-400">● Рекламные {a.evtSum.a}</span>}
              {a.evtSum.c > 0 && <span className="text-purple-400">● Контент {a.evtSum.c}</span>}
              {a.evtSum.p > 0 && <span className="text-amber-400">● Цена {a.evtSum.p}</span>}
              {a.evtSum.s > 0 && <span className="text-cyan-400">● Склад {a.evtSum.s}</span>}
            </div>
            {/* Per-campaign */}
            <div className="space-y-2">
              {a.withEvents.map(c => (
                <InlineEventImpact
                  key={c.campaign_id}
                  campaign={c}
                  dailyData={campaignDaily[c.campaign_id] || []}
                  events={eventsByCampaign[c.campaign_id] || []}
                  totalRevenue={c.total_revenue || campaignTotalRev[c.campaign_id] || 0}
                />
              ))}
            </div>
          </>
        )}
      </Sec>

      {/* 6. Recommendations */}
      <Sec icon={<Info className="h-4 w-4 text-[hsl(var(--primary))]" />} title="Рекомендации" accent="hsl(var(--primary))">
        <div className="space-y-2 mt-2">
          {a.burning.length > 0 && <Rec s="c" t={`Отключите ${a.burning.length} кампаний без заказов — ${fmtM(a.wastedSpend)} впустую`} />}
          {a.unprofitable.length > 0 && <Rec s="w" t={`${a.unprofitable.length} кампаний убыточны (ДРР > 50%) — снизьте ставки`} />}
          {a.effective.length > 0 && <Rec s="s" t={`Масштабируйте ${a.effective.length} эффективных кампаний — увеличьте бюджет`} />}
          {a.evtSum.t > 50 && <Rec s="i" t={`${a.evtSum.t} событий — частые изменения мешают алгоритмам Ozon оптимизировать показы`} />}
        </div>
      </Sec>
    </div>
  )
}

/* Subcomponents */

function SC({ icon, l, v, vc }: { icon: React.ReactNode; l: string; v: string | number; vc?: string }) {
  return (
    <div>
      <div className="flex items-center gap-1 mb-0.5">{icon}<span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">{l}</span></div>
      <div className={`text-[16px] font-bold ${vc || 'text-[hsl(var(--foreground))]'}`}>{v}</div>
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
