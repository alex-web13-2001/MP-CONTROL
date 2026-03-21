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

/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

function fmt(v: number): string {
  return v.toLocaleString('ru-RU', { maximumFractionDigits: 0 })
}
function fmtMoney(v: number): string {
  return fmt(v) + ' ₽'
}
function pct(v: number): string {
  return v.toFixed(1) + '%'
}

function deltaColor(delta: number, inverse = false) {
  const good = inverse ? delta < -5 : delta > 5
  const bad = inverse ? delta > 5 : delta < -5
  if (good) return 'text-emerald-400'
  if (bad) return 'text-red-400'
  return 'text-[hsl(var(--muted-foreground)/0.5)]'
}

function deltaArrow(delta: number, inverse = false) {
  const good = inverse ? delta < -5 : delta > 5
  const bad = inverse ? delta > 5 : delta < -5
  if (good) return <ArrowUp className="h-3 w-3 text-emerald-400 inline" />
  if (bad) return <ArrowDown className="h-3 w-3 text-red-400 inline" />
  return <Minus className="h-3 w-3 text-[hsl(var(--muted-foreground)/0.3)] inline" />
}

function avg(arr: number[]): number {
  return arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0
}

/* ═══════════════════════════════════════════════════════════
   Collapsible Section
   ═══════════════════════════════════════════════════════════ */

function Section({
  icon,
  title,
  count,
  countColor = 'text-[hsl(var(--muted-foreground))]',
  accentColor,
  badge,
  defaultOpen = true,
  children,
}: {
  icon: React.ReactNode
  title: string
  count?: number
  countColor?: string
  accentColor?: string
  badge?: React.ReactNode
  defaultOpen?: boolean
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div
      className="rounded-2xl border overflow-hidden"
      style={{ borderColor: accentColor ? `${accentColor}30` : 'hsl(var(--border)/0.5)' }}
    >
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-5 py-4 text-left transition-colors hover:bg-[hsl(var(--muted)/0.1)]"
      >
        <div className="flex items-center gap-3">
          {icon}
          <span className="text-[16px] font-bold text-[hsl(var(--foreground))]">{title}</span>
          {count !== undefined && (
            <span className={`text-[14px] font-semibold ${countColor}`}>{count}</span>
          )}
          {badge}
        </div>
        {open ? (
          <ChevronUp className="h-5 w-5 text-[hsl(var(--muted-foreground)/0.4)]" />
        ) : (
          <ChevronDown className="h-5 w-5 text-[hsl(var(--muted-foreground)/0.4)]" />
        )}
      </button>
      {open && <div className="px-5 pb-5 border-t border-[hsl(var(--border)/0.3)]">{children}</div>}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Campaign line & metrics
   ═══════════════════════════════════════════════════════════ */

function CampaignLine({
  c,
  columns,
  problems,
  highlights,
  borderColor = 'border-[hsl(var(--border)/0.3)]',
  bgColor = 'bg-transparent',
  children,
}: {
  c: CampaignRow
  columns: React.ReactNode
  problems?: string[]
  highlights?: string[]
  borderColor?: string
  bgColor?: string
  children?: React.ReactNode
}) {
  return (
    <div className={`rounded-xl border ${borderColor} ${bgColor} p-4`}>
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div className="font-semibold text-[14px] text-[hsl(var(--foreground))] min-w-0 flex-1 truncate" title={c.title}>
          {c.title}
        </div>
        <div className="flex items-center gap-5 shrink-0 text-[14px]">{columns}</div>
      </div>
      {problems && problems.length > 0 && (
        <div className="mt-2 space-y-1">
          {problems.map((p, i) => (
            <div key={i} className="flex items-start gap-2">
              <span className="text-red-400 mt-0.5">•</span>
              <span className="text-[13px] text-[hsl(var(--muted-foreground))]">{p}</span>
            </div>
          ))}
        </div>
      )}
      {highlights && highlights.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1.5">
          {highlights.map((h, i) => (
            <span key={i} className="inline-flex items-center gap-1 rounded-lg bg-emerald-500/10 px-2.5 py-1 text-[12px] font-medium text-emerald-400">
              <CheckCircle2 className="h-3 w-3" />{h}
            </span>
          ))}
        </div>
      )}
      {children}
    </div>
  )
}

function Metric({ label, value, bad, good }: { label: string; value: string | number; bad?: boolean; good?: boolean }) {
  const color = bad ? 'text-red-400' : good ? 'text-emerald-400' : 'text-[hsl(var(--foreground))]'
  return (
    <div className="text-center min-w-[60px]">
      <div className="text-[10px] uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.45)] mb-0.5">{label}</div>
      <div className={`text-[14px] font-semibold ${color}`}>{value}</div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Per-campaign before/after component
   ═══════════════════════════════════════════════════════════ */

function CampaignEventImpact({
  campaign,
  dailyData,
  events,
}: {
  campaign: CampaignRow
  dailyData: CampaignDailyPoint[]
  events: CampaignEvent[]
}) {
  // Group events by date
  const eventsByDate = useMemo(() => {
    const map = new Map<string, CampaignEvent[]>()
    for (const e of events) {
      const arr = map.get(e.date) || []
      arr.push(e)
      map.set(e.date, arr)
    }
    return map
  }, [events])

  // Build date index
  const sorted = useMemo(() => [...dailyData].sort((a, b) => a.date.localeCompare(b.date)), [dailyData])
  const dateToIndex = useMemo(() => new Map(sorted.map((d, i) => [d.date, i])), [sorted])

  // Compute before/after for each event date
  const impacts = useMemo(() => {
    const results: Array<{
      date: string
      events: CampaignEvent[]
      before: { spend: number; views: number; clicks: number; ctr: number; orders: number; drr: number }
      after: { spend: number; views: number; clicks: number; ctr: number; orders: number; drr: number }
      deltas: { spend: number; views: number; clicks: number; ctr: number; orders: number; drr: number }
    }> = []

    for (const [eventDate, dateEvents] of eventsByDate) {
      const idx = dateToIndex.get(eventDate)
      if (idx === undefined) continue

      const beforeDays = sorted.slice(Math.max(0, idx - 3), idx)
      const afterDays = sorted.slice(idx + 1, Math.min(sorted.length, idx + 4))

      if (beforeDays.length < 1 || afterDays.length < 1) continue

      const before = {
        spend: avg(beforeDays.map(d => d.spend)),
        views: avg(beforeDays.map(d => d.views)),
        clicks: avg(beforeDays.map(d => d.clicks)),
        ctr: avg(beforeDays.map(d => d.ctr)),
        orders: avg(beforeDays.map(d => d.orders)),
        drr: avg(beforeDays.map(d => d.drr)),
      }
      const after = {
        spend: avg(afterDays.map(d => d.spend)),
        views: avg(afterDays.map(d => d.views)),
        clicks: avg(afterDays.map(d => d.clicks)),
        ctr: avg(afterDays.map(d => d.ctr)),
        orders: avg(afterDays.map(d => d.orders)),
        drr: avg(afterDays.map(d => d.drr)),
      }
      const deltas = {
        spend: before.spend > 0 ? ((after.spend - before.spend) / before.spend) * 100 : 0,
        views: before.views > 0 ? ((after.views - before.views) / before.views) * 100 : 0,
        clicks: before.clicks > 0 ? ((after.clicks - before.clicks) / before.clicks) * 100 : 0,
        ctr: before.ctr > 0 ? ((after.ctr - before.ctr) / before.ctr) * 100 : 0,
        orders: before.orders > 0 ? ((after.orders - before.orders) / before.orders) * 100 : 0,
        drr: before.drr > 0 ? ((after.drr - before.drr) / before.drr) * 100 : 0,
      }

      results.push({ date: eventDate, events: dateEvents, before, after, deltas })
    }

    return results.sort((a, b) => a.date.localeCompare(b.date))
  }, [eventsByDate, sorted, dateToIndex])

  if (impacts.length === 0) return null

  return (
    <div className="mt-3 rounded-xl border border-blue-500/15 bg-blue-500/[0.02] p-4">
      <div className="flex items-center gap-2 mb-3">
        <Zap className="h-4 w-4 text-blue-400" />
        <span className="text-[14px] font-semibold text-[hsl(var(--foreground))]">{campaign.title}</span>
        <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)]">
          {events.length} событий за период
        </span>
      </div>

      {/* List events */}
      <div className="mb-3 space-y-1">
        {events.slice(0, 10).map(e => (
          <div key={e.id} className="flex items-baseline gap-2 text-[13px]">
            <span className="text-[hsl(var(--muted-foreground)/0.4)] shrink-0 w-[75px]">
              {new Date(e.date + 'T00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })} {e.time}
            </span>
            <span className={`shrink-0 ${
              e.category === 'advertising' ? 'text-blue-400' :
              e.category === 'price' ? 'text-amber-400' :
              e.category === 'content' ? 'text-purple-400' : 'text-cyan-400'
            }`}>
              {e.label}
            </span>
            {e.detail && <span className="text-[hsl(var(--muted-foreground)/0.6)]">{e.detail}</span>}
          </div>
        ))}
        {events.length > 10 && (
          <div className="text-[12px] text-[hsl(var(--muted-foreground)/0.4)]">
            ...ещё {events.length - 10} событий
          </div>
        )}
      </div>

      {/* Before/After per event date */}
      {impacts.map(impact => {
        const d = impact.deltas
        const hasChange = Object.values(d).some(v => Math.abs(v) > 5)
        if (!hasChange) return null

        return (
          <div key={impact.date} className="mb-2 rounded-lg border border-[hsl(var(--border)/0.2)] p-3">
            <div className="flex items-center gap-2 mb-2">
              <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">
                После {new Date(impact.date + 'T00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })}:
              </span>
              <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)]">
                среднее 3 дня до → 3 дня после
              </span>
            </div>
            <div className="grid grid-cols-3 gap-2 sm:grid-cols-6">
              <BA label="Расход" before={fmtMoney(impact.before.spend)} after={fmtMoney(impact.after.spend)} delta={d.spend} inverse />
              <BA label="Показы" before={fmt(impact.before.views)} after={fmt(impact.after.views)} delta={d.views} />
              <BA label="Клики" before={fmt(impact.before.clicks)} after={fmt(impact.after.clicks)} delta={d.clicks} />
              <BA label="CTR" before={pct(impact.before.ctr)} after={pct(impact.after.ctr)} delta={d.ctr} />
              <BA label="Заказы" before={impact.before.orders.toFixed(1)} after={impact.after.orders.toFixed(1)} delta={d.orders} />
              <BA label="ДРР" before={pct(impact.before.drr)} after={pct(impact.after.drr)} delta={d.drr} inverse />
            </div>
          </div>
        )
      })}
    </div>
  )
}

function BA({ label, before, after, delta, inverse = false }: { label: string; before: string; after: string; delta: number; inverse?: boolean }) {
  return (
    <div className="rounded-lg bg-[hsl(var(--muted)/0.08)] p-2">
      <div className="text-[10px] uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.4)] mb-0.5">{label}</div>
      <div className="flex items-center gap-1 text-[12px] text-[hsl(var(--muted-foreground)/0.5)]">
        <span>{before}</span>
        <span>→</span>
        <span className={`font-semibold ${deltaColor(delta, inverse)}`}>{after}</span>
      </div>
      <div className={`text-[12px] font-semibold mt-0.5 ${deltaColor(delta, inverse)}`}>
        {deltaArrow(delta, inverse)} {delta > 0 ? '+' : ''}{delta.toFixed(0)}%
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Component
   ═══════════════════════════════════════════════════════════ */

export function CampaignInsights({
  campaigns,
  eventsByDay,
  shopId,
  dateFrom,
  dateTo,
}: {
  campaigns: CampaignRow[]
  eventsByDay: Record<string, EventDaySummary>
  shopId: number
  dateFrom: string
  dateTo: string
}) {
  // Load per-campaign daily stats
  const [campaignDaily, setCampaignDaily] = useState<Record<number, CampaignDailyPoint[]>>({})
  const [eventsByCampaign, setEventsByCampaign] = useState<Record<number, CampaignEvent[]>>({})
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!shopId || !dateFrom || !dateTo) return
    setLoading(true)
    getCampaignDailyStats(shopId, dateFrom, dateTo)
      .then(data => {
        setCampaignDaily(data.campaigns_daily || {})
        setEventsByCampaign(data.events_by_campaign || {})
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [shopId, dateFrom, dateTo])

  const analysis = useMemo(() => {
    const active = campaigns.filter(c => c.status !== 'archived')
    const totalSpend = active.reduce((s, c) => s + c.spend, 0)
    const totalRevenue = active.reduce((s, c) => s + c.revenue, 0)
    const totalOrders = active.reduce((s, c) => s + c.orders, 0)
    const withOrders = active.filter(c => c.orders > 0)
    const withoutOrders = active.filter(c => c.orders === 0 && c.spend > 0)

    const avgCpc = active.filter(c => c.avg_cpc > 0).length > 0
      ? active.filter(c => c.avg_cpc > 0).reduce((s, c) => s + c.avg_cpc, 0) / active.filter(c => c.avg_cpc > 0).length
      : 0
    const avgDrr = withOrders.length > 0 ? withOrders.reduce((s, c) => s + c.drr, 0) / withOrders.length : 0
    const avgCpo = withOrders.length > 0 ? withOrders.reduce((s, c) => s + c.spend / c.orders, 0) / withOrders.length : 0
    const wastedSpend = withoutOrders.reduce((s, c) => s + c.spend, 0)

    // 1. Сливают бюджет (0 заказов) — ВСЕ, без ограничений
    const burningMoney = withoutOrders
      .sort((a, b) => b.spend - a.spend)
      .map(c => {
        const problems: string[] = []
        problems.push(`Расход ${fmtMoney(c.spend)} → 0 рекл. заказов`)
        if (c.cart === 0) problems.push(`0 корзин — товар не интересен`)
        else problems.push(`${c.cart} корзин, 0 заказов — проблема с ценой/доставкой`)
        if (c.ctr < 0.5 && c.views > 1000) problems.push(`CTR ${pct(c.ctr)} — карточка не привлекает`)
        return { campaign: c, problems }
      })

    // 2. Убыточные (DRR > 50%) — ВСЕ
    const unprofitable = withOrders
      .filter(c => c.drr > 50)
      .sort((a, b) => b.drr - a.drr)
      .map(c => {
        const problems: string[] = []
        if (c.drr > 100) problems.push(`ДРР ${pct(c.drr)} — убыток ${fmtMoney(c.spend - c.revenue)}`)
        else problems.push(`ДРР ${pct(c.drr)} — на грани рентабельности`)
        problems.push(`Расход ${fmtMoney(c.spend)} → ${c.orders} заказов на ${fmtMoney(c.revenue)}`)
        return { campaign: c, problems }
      })

    // 3. Низкий CTR — ВСЕ
    const lowCtr = active
      .filter(c => c.ctr < 1 && c.views > 2000 && c.spend > 100)
      .sort((a, b) => a.ctr - b.ctr)
      .map(c => ({ campaign: c, problems: [`CTR ${pct(c.ctr)} при ${fmt(c.views)} показах — проверьте фото, цену`] }))

    // 4. Эффективные (DRR < 40%, orders >= 2) — ВСЕ
    const effective = withOrders
      .filter(c => c.drr < 40 && c.drr > 0 && c.orders >= 2 && c.spend > 100)
      .sort((a, b) => a.drr - b.drr)
      .map(c => {
        const romi = c.spend > 0 ? (c.revenue / c.spend) * 100 : 0
        const cpo = c.orders > 0 ? c.spend / c.orders : 0
        const highlights: string[] = [`ДРР ${pct(c.drr)}`]
        if (romi > 300) highlights.push(`ROMI ${fmt(romi)}%`)
        if (c.direct_orders > 0 && c.model_orders > 0)
          highlights.push(`Прямые: ${c.direct_orders}, модельные: ${c.model_orders}`)
        return { campaign: c, romi, cpo, highlights }
      })

    // 5. Кампании с событиями (для before/after) — ВСЕ
    const campaignsWithEvents = active.filter(c => {
      const cid = c.campaign_id
      return eventsByCampaign[cid] && eventsByCampaign[cid].length > 0 && campaignDaily[cid] && campaignDaily[cid].length >= 3
    })

    const evtSummary = Object.values(eventsByDay).reduce(
      (acc, d) => ({ advertising: acc.advertising + d.advertising, content: acc.content + d.content, price: acc.price + d.price, stock: acc.stock + d.stock, total: acc.total + d.total }),
      { advertising: 0, content: 0, price: 0, stock: 0, total: 0 },
    )

    return {
      totalSpend, totalRevenue, totalOrders, wastedSpend,
      wastePercent: totalSpend > 0 ? (wastedSpend / totalSpend) * 100 : 0,
      avgCpc, avgDrr, avgCpo,
      activeCampaigns: active.length,
      campaignsWithOrders: withOrders.length,
      campaignsWithoutOrders: withoutOrders.length,
      burningMoney, unprofitable, lowCtr, effective,
      campaignsWithEvents, evtSummary,
    }
  }, [campaigns, eventsByDay, eventsByCampaign, campaignDaily])

  if (campaigns.length === 0) return null

  const a = analysis

  return (
    <div className="space-y-5">
      {/* ═══ Title ═══ */}
      <div className="flex items-center gap-3">
        <Activity className="h-6 w-6 text-[hsl(var(--primary))]" />
        <h3 className="text-[20px] font-bold text-[hsl(var(--foreground))]">Анализ рекламных кампаний</h3>
        <span className="text-[13px] text-[hsl(var(--muted-foreground)/0.5)]">рекламные заказы (атрибуция Ozon)</span>
      </div>

      {/* ═══ Summary ═══ */}
      <div className="rounded-2xl border border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.06)] p-5">
        <div className="grid grid-cols-2 gap-5 sm:grid-cols-3 lg:grid-cols-6">
          <SummaryCard icon={<Target className="h-4 w-4 text-[hsl(var(--primary))]" />} label="Кампаний" value={a.activeCampaigns} />
          <SummaryCard icon={<CheckCircle2 className="h-4 w-4 text-emerald-400" />} label="С заказами" value={a.campaignsWithOrders} valueColor="text-emerald-400" />
          <SummaryCard icon={<Ban className="h-4 w-4 text-red-400" />} label="Без заказов" value={a.campaignsWithoutOrders} valueColor="text-red-400" />
          <SummaryCard icon={<MousePointerClick className="h-4 w-4 text-blue-400" />} label="Ср. CPC" value={fmtMoney(a.avgCpc)} />
          <SummaryCard icon={<BarChart2 className="h-4 w-4 text-amber-400" />} label="Ср. ДРР" value={pct(a.avgDrr)} />
          <SummaryCard icon={<DollarSign className="h-4 w-4 text-purple-400" />} label="Ср. CPO" value={fmtMoney(a.avgCpo)} />
        </div>
        <div className="mt-4 pt-4 border-t border-[hsl(var(--border)/0.3)] text-[14px] leading-relaxed text-[hsl(var(--muted-foreground))]">
          За период потрачено <strong className="text-[hsl(var(--foreground))]">{fmtMoney(a.totalSpend)}</strong> на рекламу,
          получено <strong className="text-[hsl(var(--foreground))]">{a.totalOrders} рекламных заказов</strong> на{' '}
          <strong className="text-[hsl(var(--foreground))]">{fmtMoney(a.totalRevenue)}</strong> (выручка атрибуции — только из рекламы).
          {a.wastedSpend > 0 && (
            <> Из них <strong className="text-red-400">{fmtMoney(a.wastedSpend)} ({pct(a.wastePercent)})</strong> потрачено без заказов.</>
          )}
        </div>
      </div>

      {/* ═══ 1. Сливают бюджет ═══ */}
      {a.burningMoney.length > 0 && (
        <Section icon={<Ban className="h-5 w-5 text-red-400" />} title="Сливают бюджет" count={a.burningMoney.length} countColor="text-red-400" accentColor="rgb(239 68 68)"
          badge={<span className="rounded-lg bg-red-500/10 px-3 py-1 text-[13px] font-semibold text-red-400">−{fmtMoney(a.wastedSpend)}</span>}
        >
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            Кампании с расходом, но без единого рекламного заказа.
          </p>
          <div className="space-y-2">
            {a.burningMoney.map(({ campaign: c, problems }) => (
              <CampaignLine key={c.campaign_id} c={c} borderColor="border-red-500/15" bgColor="bg-red-500/[0.02]" problems={problems}
                columns={<>
                  <Metric label="Расход" value={fmtMoney(c.spend)} bad />
                  <Metric label="Показы" value={fmt(c.views)} />
                  <Metric label="Клики" value={fmt(c.clicks)} />
                  <Metric label="CTR" value={pct(c.ctr)} bad={c.ctr < 0.5} />
                  <Metric label="Корзины" value={c.cart} bad={c.cart === 0} />
                  <Metric label="Заказы" value={0} bad />
                </>}
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 2. Высокий ДРР ═══ */}
      {a.unprofitable.length > 0 && (
        <Section icon={<AlertTriangle className="h-5 w-5 text-amber-400" />} title="Высокий ДРР" count={a.unprofitable.length} countColor="text-amber-400" accentColor="rgb(245 158 11)">
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            Кампании с ДРР &gt; 50%. Расход больше, чем выручка атрибуции.
          </p>
          <div className="space-y-2">
            {a.unprofitable.map(({ campaign: c, problems }) => (
              <CampaignLine key={c.campaign_id} c={c} borderColor="border-amber-500/15" bgColor="bg-amber-500/[0.02]" problems={problems}
                columns={<>
                  <Metric label="Расход" value={fmtMoney(c.spend)} />
                  <Metric label="Заказы" value={c.orders} />
                  <Metric label="Выручка рекл." value={fmtMoney(c.revenue)} />
                  <Metric label="ДРР" value={pct(c.drr)} bad={c.drr > 100} />
                  <Metric label="CPO" value={fmtMoney(c.orders > 0 ? c.spend / c.orders : 0)} />
                </>}
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 3. Низкий CTR ═══ */}
      {a.lowCtr.length > 0 && (
        <Section icon={<Eye className="h-5 w-5 text-orange-400" />} title="Низкий CTR" count={a.lowCtr.length} countColor="text-orange-400" accentColor="rgb(251 146 60)">
          <div className="space-y-2 mt-3">
            {a.lowCtr.map(({ campaign: c, problems }) => (
              <CampaignLine key={c.campaign_id} c={c} borderColor="border-orange-500/15" bgColor="bg-orange-500/[0.02]" problems={problems}
                columns={<>
                  <Metric label="CTR" value={pct(c.ctr)} bad />
                  <Metric label="Показы" value={fmt(c.views)} />
                  <Metric label="Клики" value={fmt(c.clicks)} />
                  <Metric label="Расход" value={fmtMoney(c.spend)} />
                </>}
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 4. Эффективные (DRR < 40%, orders >= 2) ═══ */}
      {a.effective.length > 0 && (
        <Section icon={<Trophy className="h-5 w-5 text-emerald-400" />} title="Эффективные кампании" count={a.effective.length} countColor="text-emerald-400" accentColor="rgb(52 211 153)"
          badge={<span className="text-[12px] text-[hsl(var(--muted-foreground)/0.4)]">ДРР &lt; 40%, 2+ заказа</span>}
        >
          <div className="space-y-2 mt-3">
            {a.effective.map(({ campaign: c, romi, cpo, highlights }) => (
              <CampaignLine key={c.campaign_id} c={c} borderColor="border-emerald-500/15" bgColor="bg-emerald-500/[0.02]" highlights={highlights}
                columns={<>
                  <Metric label="Расход" value={fmtMoney(c.spend)} />
                  <Metric label="Заказы" value={c.orders} good />
                  <Metric label="Выручка рекл." value={fmtMoney(c.revenue)} good />
                  <Metric label="ДРР" value={pct(c.drr)} good />
                  <Metric label="CPO" value={fmtMoney(cpo)} />
                  <Metric label="ROMI" value={`${fmt(romi)}%`} good={romi > 200} />
                </>}
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 5. Per-campaign before/after анализ событий ═══ */}
      <Section
        icon={<Zap className="h-5 w-5 text-blue-400" />}
        title="Влияние событий на кампании"
        count={a.evtSummary.total}
        countColor="text-blue-400"
        accentColor="rgb(96 165 250)"
        badge={
          a.campaignsWithEvents.length > 0 ? (
            <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.4)]">
              {a.campaignsWithEvents.length} кампаний с событиями
            </span>
          ) : undefined
        }
      >
        {loading ? (
          <div className="py-6 text-center text-[14px] text-[hsl(var(--muted-foreground)/0.5)]">Загрузка подневных данных кампаний...</div>
        ) : a.campaignsWithEvents.length === 0 ? (
          <div className="py-4 text-center text-[14px] text-[hsl(var(--muted-foreground)/0.5)]">
            {a.evtSummary.total > 0
              ? 'Недостаточно данных для before/after анализа (нужно минимум 3 дня статистики по кампании)'
              : 'Нет событий за период'}
          </div>
        ) : (
          <>
            <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-2">
              Для каждой кампании, где были события, сравниваем метрики: среднее за 3 дня до → 3 дня после события.
            </p>

            {/* Event type summary */}
            <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
              {a.evtSummary.advertising > 0 && <EventTypeCard label="Рекламные" count={a.evtSummary.advertising} color="text-blue-400" bg="bg-blue-500/5" border="border-blue-500/15" />}
              {a.evtSummary.content > 0 && <EventTypeCard label="Контент" count={a.evtSummary.content} color="text-purple-400" bg="bg-purple-500/5" border="border-purple-500/15" />}
              {a.evtSummary.price > 0 && <EventTypeCard label="Ценовые" count={a.evtSummary.price} color="text-amber-400" bg="bg-amber-500/5" border="border-amber-500/15" />}
              {a.evtSummary.stock > 0 && <EventTypeCard label="Складские" count={a.evtSummary.stock} color="text-cyan-400" bg="bg-cyan-500/5" border="border-cyan-500/15" />}
            </div>

            {/* Per-campaign impacts */}
            <div className="space-y-3">
              {a.campaignsWithEvents.map(c => (
                <CampaignEventImpact
                  key={c.campaign_id}
                  campaign={c}
                  dailyData={campaignDaily[c.campaign_id] || []}
                  events={eventsByCampaign[c.campaign_id] || []}
                />
              ))}
            </div>
          </>
        )}
      </Section>

      {/* ═══ 6. Рекомендации ═══ */}
      <Section icon={<Info className="h-5 w-5 text-[hsl(var(--primary))]" />} title="Рекомендации" accentColor="hsl(var(--primary))">
        <div className="space-y-3 mt-3">
          {a.burningMoney.length > 0 && (
            <Rec severity="critical" title={`Отключите ${a.burningMoney.length} кампаний без заказов`}
              text={`${fmtMoney(a.wastedSpend)} потрачено впустую. Отключите или снизьте ставки до минимума.`} />
          )}
          {a.unprofitable.length > 0 && (
            <Rec severity="warning" title={`${a.unprofitable.length} кампаний убыточны (ДРР > 50%)`}
              text="Снизьте ставки — расход превышает выручку от рекламных заказов." />
          )}
          {a.effective.length > 0 && (
            <Rec severity="success" title={`Масштабируйте ${a.effective.length} эффективных кампаний`}
              text={`Средний ROMI: ${fmt(a.effective.reduce((s, e) => s + e.romi, 0) / a.effective.length)}%. Увеличьте бюджет.`} />
          )}
          {a.evtSummary.total > 50 && (
            <Rec severity="info" title="Много изменений за период"
              text={`${a.evtSummary.total} событий. Частые изменения мешают алгоритмам Ozon оптимизировать показы.`} />
          )}
        </div>
      </Section>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Subcomponents
   ═══════════════════════════════════════════════════════════ */

function SummaryCard({ icon, label, value, valueColor }: { icon: React.ReactNode; label: string; value: string | number; valueColor?: string }) {
  return (
    <div>
      <div className="flex items-center gap-1.5 mb-1">{icon}<span className="text-[12px] text-[hsl(var(--muted-foreground)/0.6)]">{label}</span></div>
      <div className={`text-[18px] font-bold ${valueColor || 'text-[hsl(var(--foreground))]'}`}>{value}</div>
    </div>
  )
}

function EventTypeCard({ label, count, color, bg, border }: { label: string; count: number; color: string; bg: string; border: string }) {
  return (
    <div className={`rounded-xl border ${border} ${bg} p-3`}>
      <div className={`text-[12px] ${color} font-medium mb-0.5`}>{label}</div>
      <div className={`text-[20px] font-bold ${color}`}>{count}</div>
    </div>
  )
}

function Rec({ severity, title, text }: { severity: 'critical' | 'warning' | 'success' | 'info'; title: string; text: string }) {
  const cfg = {
    critical: { icon: <AlertCircle className="h-4 w-4" />, color: 'text-red-400', bg: 'bg-red-500/[0.04]', border: 'border-red-500/15' },
    warning: { icon: <AlertTriangle className="h-4 w-4" />, color: 'text-amber-400', bg: 'bg-amber-500/[0.04]', border: 'border-amber-500/15' },
    success: { icon: <TrendingUp className="h-4 w-4" />, color: 'text-emerald-400', bg: 'bg-emerald-500/[0.04]', border: 'border-emerald-500/15' },
    info: { icon: <Info className="h-4 w-4" />, color: 'text-blue-400', bg: 'bg-blue-500/[0.04]', border: 'border-blue-500/15' },
  }[severity]
  return (
    <div className={`rounded-xl border ${cfg.border} ${cfg.bg} px-4 py-3`}>
      <div className="flex items-start gap-3">
        <span className={`mt-0.5 shrink-0 ${cfg.color}`}>{cfg.icon}</span>
        <div>
          <div className="text-[14px] font-semibold text-[hsl(var(--foreground))]">{title}</div>
          <div className="mt-1 text-[13px] leading-relaxed text-[hsl(var(--muted-foreground))]">{text}</div>
        </div>
      </div>
    </div>
  )
}
