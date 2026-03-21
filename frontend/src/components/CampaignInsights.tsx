import { useMemo, useState, useEffect } from 'react'
import {
  AlertTriangle,
  TrendingUp,

  Zap,
  ShoppingCart,
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
import type { CampaignRow, EventDaySummary, EventDetail, AdvertisingDailyPoint } from '@/api/advertising'
import { getEventsDetail } from '@/api/advertising'

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

function deltaIcon(delta: number) {
  if (delta > 5) return <ArrowUp className="h-3 w-3 text-emerald-400 inline" />
  if (delta < -5) return <ArrowDown className="h-3 w-3 text-red-400 inline" />
  return <Minus className="h-3 w-3 text-[hsl(var(--muted-foreground)/0.3)] inline" />
}

function deltaColor(delta: number, inverse = false) {
  const good = inverse ? delta < -5 : delta > 5
  const bad = inverse ? delta > 5 : delta < -5
  if (good) return 'text-emerald-400'
  if (bad) return 'text-red-400'
  return 'text-[hsl(var(--muted-foreground)/0.5)]'
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
   Campaign line
   ═══════════════════════════════════════════════════════════ */

function CampaignLine({
  c,
  columns,
  problems,
  highlights,
  borderColor = 'border-[hsl(var(--border)/0.3)]',
  bgColor = 'bg-transparent',
}: {
  c: CampaignRow
  columns: React.ReactNode
  problems?: string[]
  highlights?: string[]
  borderColor?: string
  bgColor?: string
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
            <span
              key={i}
              className="inline-flex items-center gap-1 rounded-lg bg-emerald-500/10 px-2.5 py-1 text-[12px] font-medium text-emerald-400"
            >
              <CheckCircle2 className="h-3 w-3" />
              {h}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

function Metric({
  label,
  value,
  bad,
  good,
}: {
  label: string
  value: string | number
  bad?: boolean
  good?: boolean
}) {
  const color = bad ? 'text-red-400' : good ? 'text-emerald-400' : 'text-[hsl(var(--foreground))]'
  return (
    <div className="text-center min-w-[60px]">
      <div className="text-[10px] uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.45)] mb-0.5">
        {label}
      </div>
      <div className={`text-[14px] font-semibold ${color}`}>{value}</div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Before/After event impact analysis
   ═══════════════════════════════════════════════════════════ */

interface EventImpact {
  date: string
  category: string
  events: EventDetail[]
  campaignTitles: string[]
  description: string
  before: { spend: number; views: number; clicks: number; ctr: number; orders: number; drr: number }
  after: { spend: number; views: number; clicks: number; ctr: number; orders: number; drr: number }
  deltas: { spend: number; views: number; clicks: number; ctr: number; orders: number; drr: number }
}

function avg(arr: number[]): number {
  return arr.length > 0 ? arr.reduce((a, b) => a + b, 0) / arr.length : 0
}

// Compute per-date impacts using eventsByDay and chart_daily
function computeDateImpacts(
  eventsByDay: Record<string, EventDaySummary>,
  chartDaily: AdvertisingDailyPoint[],
  allEvents: EventDetail[],
  campaigns: CampaignRow[],
): EventImpact[] {
  const sorted = [...chartDaily].sort((a, b) => a.date.localeCompare(b.date))
  const dateToIndex = new Map(sorted.map((d, i) => [d.date, i]))
  
  // Build product_id → campaign titles
  const prodToCampaigns = new Map<number, string[]>()
  for (const c of campaigns) {
    for (const item of c.items || []) {
      if (item.product_id) {
        const arr = prodToCampaigns.get(item.product_id) || []
        if (!arr.includes(c.title)) arr.push(c.title)
        prodToCampaigns.set(item.product_id, arr)
      }
    }
  }

  // Group events by date — events come from getEventsDetail(date)
  // We need to associate events with dates. Since getEventsDetail returns events for a specific date,
  // and we loaded ALL events across all dates, we need a way to know which date each event belongs to.
  // We'll use the EventDetail fields to reconstruct this.
  // Actually the simplest approach: compute impacts from eventsByDay (known-dates-with-events)
  // and use allEvents to enrich descriptions

  const impacts: EventImpact[] = []
  const eventDates = Object.keys(eventsByDay).filter(d => (eventsByDay[d]?.total || 0) > 0).sort()

  for (const eventDate of eventDates) {
    const dateIdx = dateToIndex.get(eventDate)
    if (dateIdx === undefined) continue

    const summary = eventsByDay[eventDate]
    
    // Get 3 days before and 3 days after this date
    const beforeDays = sorted.slice(Math.max(0, dateIdx - 3), dateIdx)
    const afterDays = sorted.slice(dateIdx + 1, Math.min(sorted.length, dateIdx + 4))
    
    if (beforeDays.length < 2 || afterDays.length < 2) continue

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

    // Build description from event types
    const parts: string[] = []
    if (summary.advertising > 0) parts.push(`${summary.advertising} рекл.`)
    if (summary.content > 0) parts.push(`${summary.content} контент`)
    if (summary.price > 0) parts.push(`${summary.price} ценов.`)
    if (summary.stock > 0) parts.push(`${summary.stock} склад.`)

    // Determine category
    let category = 'mixed'
    if (summary.advertising > 0 && summary.content === 0 && summary.price === 0) category = 'advertising'
    else if (summary.content > 0 && summary.advertising === 0 && summary.price === 0) category = 'content'
    else if (summary.price > 0 && summary.advertising === 0 && summary.content === 0) category = 'price'

    // Get campaign titles from events for this date
    const campTitles = new Set<string>()
    const dateEvents: EventDetail[] = []
    for (const e of allEvents) {
      // Match events to date — check if event title matches for this date
      if (e.campaign_title) campTitles.add(e.campaign_title)
      if (e.product?.nm_id) {
        const camps = prodToCampaigns.get(e.product.nm_id)
        if (camps) camps.forEach(t => campTitles.add(t))
      }
    }

    impacts.push({
      date: eventDate,
      category,
      events: dateEvents,
      campaignTitles: [...campTitles].slice(0, 5),
      description: parts.join(', '),
      before,
      after,
      deltas,
    })
  }

  return impacts
}

/* ═══════════════════════════════════════════════════════════
   Main Component
   ═══════════════════════════════════════════════════════════ */

export function CampaignInsights({
  campaigns,
  eventsByDay,
  chartDaily,
  shopId,
  dateFrom,
  dateTo,
}: {
  campaigns: CampaignRow[]
  eventsByDay: Record<string, EventDaySummary>
  chartDaily: AdvertisingDailyPoint[]
  shopId: number
  dateFrom: string
  dateTo: string
}) {
  const [allEvents, setAllEvents] = useState<EventDetail[]>([])
  const [eventsLoading, setEventsLoading] = useState(false)

  useEffect(() => {
    if (!shopId || !dateFrom || !dateTo) return
    const dates = Object.keys(eventsByDay).filter(d => (eventsByDay[d]?.total || 0) > 0)
    if (dates.length === 0) return

    setEventsLoading(true)
    Promise.all(dates.map(d => getEventsDetail(shopId, d).catch(() => null)))
      .then(results => {
        const events: EventDetail[] = []
        for (const r of results) {
          if (r?.events) events.push(...r.events)
        }
        setAllEvents(events)
      })
      .finally(() => setEventsLoading(false))
  }, [shopId, dateFrom, dateTo, eventsByDay])

  const analysis = useMemo(() => {
    const active = campaigns.filter(c => c.status !== 'archived')
    const totalSpend = active.reduce((s, c) => s + c.spend, 0)
    const totalRevenue = active.reduce((s, c) => s + c.revenue, 0)
    const totalOrders = active.reduce((s, c) => s + c.orders, 0)
    const withOrders = active.filter(c => c.orders > 0)
    const withoutOrders = active.filter(c => c.orders === 0 && c.spend > 0)

    const avgCpc = active.filter(c => c.avg_cpc > 0).length > 0
      ? active.filter(c => c.avg_cpc > 0).reduce((s, c) => s + c.avg_cpc, 0) /
        active.filter(c => c.avg_cpc > 0).length
      : 0
    const avgDrr = withOrders.length > 0
      ? withOrders.reduce((s, c) => s + c.drr, 0) / withOrders.length
      : 0
    const avgCpo = withOrders.length > 0
      ? withOrders.reduce((s, c) => s + c.spend / c.orders, 0) / withOrders.length
      : 0
    const wastedSpend = withoutOrders.reduce((s, c) => s + c.spend, 0)

    // 1. Сливают бюджет (0 заказов)
    const burningMoney = withoutOrders
      .sort((a, b) => b.spend - a.spend)
      .map(c => {
        const problems: string[] = []
        problems.push(`Расход ${fmtMoney(c.spend)} → 0 рекламных заказов`)
        if (c.cart === 0) {
          problems.push(`0 корзин — аудитория не заинтересована`)
        } else {
          problems.push(`${c.cart} корзин, но 0 заказов — проблема с ценой или доставкой`)
        }
        if (c.ctr < 0.5 && c.views > 1000) problems.push(`CTR ${pct(c.ctr)} — карточка не привлекает`)
        if (c.avg_cpc > avgCpc * 1.5 && avgCpc > 0) problems.push(`CPC ${fmtMoney(c.avg_cpc)} — в ${(c.avg_cpc / avgCpc).toFixed(1)}x выше среднего`)
        return { campaign: c, problems }
      })

    // 2. Убыточные (DRR > 50%)
    const unprofitable = withOrders
      .filter(c => c.drr > 50)
      .sort((a, b) => b.drr - a.drr)
      .map(c => {
        const problems: string[] = []
        if (c.drr > 100) {
          problems.push(`ДРР ${pct(c.drr)} — убыток ${fmtMoney(c.spend - c.revenue)}`)
        } else {
          problems.push(`ДРР ${pct(c.drr)} — на грани рентабельности`)
        }
        problems.push(`Расход ${fmtMoney(c.spend)} → ${c.orders} заказов на ${fmtMoney(c.revenue)}`)
        return { campaign: c, problems }
      })

    // 3. Низкий CTR
    const lowCtr = active
      .filter(c => c.ctr < 1 && c.views > 2000 && c.spend > 100)
      .sort((a, b) => a.ctr - b.ctr)
      .map(c => ({
        campaign: c,
        problems: [
          `CTR ${pct(c.ctr)} при ${fmt(c.views)} показах`,
          `Пользователи видят рекламу, но не кликают. Проверьте фото, цену и название`,
        ],
      }))

    // 4. Низкая конверсия в корзину
    const lowCartConv = active
      .filter(c => c.clicks > 20 && c.cart_conv < 5 && c.cart_conv >= 0)
      .sort((a, b) => a.cart_conv - b.cart_conv)
      .map(c => ({
        campaign: c,
        problems: [
          `Конверсия в корзину ${pct(c.cart_conv)} (${c.cart} из ${c.clicks} кликов)`,
          `Кликают, но не добавляют в корзину — цена, описание, отзывы`,
        ],
      }))

    // 5. Эффективные (ЖЁСТКИЕ критерии: DRR < 40%, orders >= 2)
    const effective = withOrders
      .filter(c => c.drr < 40 && c.drr > 0 && c.orders >= 2 && c.spend > 100)
      .sort((a, b) => a.drr - b.drr)
      .map(c => {
        const romi = c.spend > 0 ? (c.revenue / c.spend) * 100 : 0
        const cpo = c.orders > 0 ? c.spend / c.orders : 0
        const highlights: string[] = []
        highlights.push(`ДРР ${pct(c.drr)}`)
        if (romi > 300) highlights.push(`ROMI ${fmt(romi)}%`)
        if (cpo < avgCpo * 0.7 && avgCpo > 0) highlights.push(`Дешёвый CPO ${fmtMoney(cpo)}`)
        if (c.ctr > 3) highlights.push(`Высокий CTR ${pct(c.ctr)}`)
        if (c.direct_orders > 0 && c.model_orders > 0) {
          highlights.push(`Прямые: ${c.direct_orders}, модельные: ${c.model_orders}`)
        }
        return { campaign: c, romi, cpo, highlights }
      })

    // Events summary
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
      burningMoney, unprofitable, lowCtr, lowCartConv, effective, evtSummary,
    }
  }, [campaigns, eventsByDay])

  const dateImpacts = useMemo(
    () => computeDateImpacts(eventsByDay, chartDaily, allEvents, campaigns),
    [eventsByDay, chartDaily, allEvents, campaigns],
  )

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
          получено <strong className="text-[hsl(var(--foreground))]">{a.totalOrders} рекламных заказов</strong> на сумму{' '}
          <strong className="text-[hsl(var(--foreground))]">{fmtMoney(a.totalRevenue)}</strong>.
          {a.wastedSpend > 0 && (
            <> Из них <strong className="text-red-400">{fmtMoney(a.wastedSpend)} ({pct(a.wastePercent)})</strong> потрачено на кампании без заказов.</>
          )}
        </div>
      </div>

      {/* ═══ 1. Сливают бюджет ═══ */}
      {a.burningMoney.length > 0 && (
        <Section icon={<Ban className="h-5 w-5 text-red-400" />} title="Сливают бюджет" count={a.burningMoney.length} countColor="text-red-400" accentColor="rgb(239 68 68)"
          badge={<span className="rounded-lg bg-red-500/10 px-3 py-1 text-[13px] font-semibold text-red-400">−{fmtMoney(a.wastedSpend)}</span>}
        >
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            Кампании с расходом, но без единого рекламного заказа. Отключите или снизьте ставку до минимума.
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
            Кампании с ДРР &gt; 50%. Реклама на грани рентабельности или убыточна. Снизьте ставки.
          </p>
          <div className="space-y-2">
            {a.unprofitable.map(({ campaign: c, problems }) => (
              <CampaignLine key={c.campaign_id} c={c} borderColor="border-amber-500/15" bgColor="bg-amber-500/[0.02]" problems={problems}
                columns={<>
                  <Metric label="Расход" value={fmtMoney(c.spend)} />
                  <Metric label="Заказы" value={c.orders} />
                  <Metric label="Выручка" value={fmtMoney(c.revenue)} />
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
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            CTR ниже 1% — видят рекламу, но не кликают. Проверьте фото, цену, название.
          </p>
          <div className="space-y-2">
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

      {/* ═══ 4. Низкая конверсия в корзину ═══ */}
      {a.lowCartConv.length > 0 && (
        <Section icon={<ShoppingCart className="h-5 w-5 text-rose-400" />} title="Низкая конверсия в корзину" count={a.lowCartConv.length} countColor="text-rose-400" accentColor="rgb(251 113 133)">
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            Конверсия в корзину ниже 5%. Кликают, но не добавляют в корзину — проблема на карточке.
          </p>
          <div className="space-y-2">
            {a.lowCartConv.map(({ campaign: c, problems }) => (
              <CampaignLine key={c.campaign_id} c={c} borderColor="border-rose-500/15" bgColor="bg-rose-500/[0.02]" problems={problems}
                columns={<>
                  <Metric label="CR корз." value={pct(c.cart_conv)} bad />
                  <Metric label="Клики" value={fmt(c.clicks)} />
                  <Metric label="Корзины" value={c.cart} />
                  <Metric label="Расход" value={fmtMoney(c.spend)} />
                </>}
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 5. Эффективные (DRR < 40%, orders >= 2) ═══ */}
      {a.effective.length > 0 && (
        <Section icon={<Trophy className="h-5 w-5 text-emerald-400" />} title="Эффективные кампании" count={a.effective.length} countColor="text-emerald-400" accentColor="rgb(52 211 153)"
          badge={<span className="text-[12px] text-[hsl(var(--muted-foreground)/0.4)]">ДРР &lt; 40%, 2+ заказа</span>}
        >
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            Кампании с низким ДРР и стабильными заказами. Рассмотрите увеличение бюджета.
          </p>
          <div className="space-y-2">
            {a.effective.map(({ campaign: c, romi, cpo, highlights }) => (
              <CampaignLine key={c.campaign_id} c={c} borderColor="border-emerald-500/15" bgColor="bg-emerald-500/[0.02]" highlights={highlights}
                columns={<>
                  <Metric label="Расход" value={fmtMoney(c.spend)} />
                  <Metric label="Заказы" value={c.orders} good />
                  <Metric label="Выручка" value={fmtMoney(c.revenue)} good />
                  <Metric label="ДРР" value={pct(c.drr)} good />
                  <Metric label="CPO" value={fmtMoney(cpo)} />
                  <Metric label="ROMI" value={`${fmt(romi)}%`} good={romi > 200} />
                </>}
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 6. Влияние событий — До/После ═══ */}
      <Section
        icon={<Zap className="h-5 w-5 text-blue-400" />}
        title="Влияние событий на метрики"
        count={a.evtSummary.total}
        countColor="text-blue-400"
        accentColor="rgb(96 165 250)"
        defaultOpen={true}
      >
        {eventsLoading ? (
          <div className="py-6 text-center text-[14px] text-[hsl(var(--muted-foreground)/0.5)]">Загрузка событий...</div>
        ) : (
          <>
            {/* Event type cards */}
            <div className="mt-3 mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
              {a.evtSummary.advertising > 0 && <EventTypeCard label="Рекламные" count={a.evtSummary.advertising} desc="Ставки, статусы, бюджеты" color="text-blue-400" bg="bg-blue-500/5" border="border-blue-500/15" />}
              {a.evtSummary.content > 0 && <EventTypeCard label="Контент" count={a.evtSummary.content} desc="Фото, описания" color="text-purple-400" bg="bg-purple-500/5" border="border-purple-500/15" />}
              {a.evtSummary.price > 0 && <EventTypeCard label="Ценовые" count={a.evtSummary.price} desc="Изменения цен" color="text-amber-400" bg="bg-amber-500/5" border="border-amber-500/15" />}
              {a.evtSummary.stock > 0 && <EventTypeCard label="Складские" count={a.evtSummary.stock} desc="Остатки" color="text-cyan-400" bg="bg-cyan-500/5" border="border-cyan-500/15" />}
            </div>

            {/* Before/After comparison per event date */}
            {dateImpacts.length > 0 && (
              <div className="space-y-3">
                <h4 className="text-[15px] font-semibold text-[hsl(var(--foreground))]">
                  До / После — как события повлияли на метрики
                </h4>
                <p className="text-[13px] text-[hsl(var(--muted-foreground)/0.6)] mb-3">
                  Сравнение средних показателей за 3 дня до и 3 дня после каждой даты с событиями (агрегированные по всем кампаниям)
                </p>

                {dateImpacts.map(impact => {
                  const d = impact.deltas
                  const hasChange = Object.values(d).some(v => Math.abs(v) > 5)
                  
                  return (
                    <div key={impact.date} className={`rounded-xl border p-4 ${hasChange ? 'border-blue-500/20 bg-blue-500/[0.02]' : 'border-[hsl(var(--border)/0.3)]'}`}>
                      {/* Date & description */}
                      <div className="flex items-center gap-3 mb-3">
                        <span className="text-[14px] font-bold text-[hsl(var(--foreground))]">
                          {new Date(impact.date + 'T00:00').toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' })}
                        </span>
                        <span className="text-[13px] text-[hsl(var(--muted-foreground))]">
                          {impact.description}
                        </span>
                        {!hasChange && (
                          <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.4)]">— без значимого влияния</span>
                        )}
                      </div>

                      {/* Before/After metrics grid */}
                      {hasChange && (
                        <div className="grid grid-cols-3 gap-3 sm:grid-cols-6">
                          <BeforeAfterMetric
                            label="Расход"
                            before={fmtMoney(impact.before.spend)}
                            after={fmtMoney(impact.after.spend)}
                            delta={d.spend}
                            inverse
                          />
                          <BeforeAfterMetric
                            label="Показы"
                            before={fmt(impact.before.views)}
                            after={fmt(impact.after.views)}
                            delta={d.views}
                          />
                          <BeforeAfterMetric
                            label="Клики"
                            before={fmt(impact.before.clicks)}
                            after={fmt(impact.after.clicks)}
                            delta={d.clicks}
                          />
                          <BeforeAfterMetric
                            label="CTR"
                            before={pct(impact.before.ctr)}
                            after={pct(impact.after.ctr)}
                            delta={d.ctr}
                          />
                          <BeforeAfterMetric
                            label="Заказы"
                            before={impact.before.orders.toFixed(1)}
                            after={impact.after.orders.toFixed(1)}
                            delta={d.orders}
                          />
                          <BeforeAfterMetric
                            label="ДРР"
                            before={pct(impact.before.drr)}
                            after={pct(impact.after.drr)}
                            delta={d.drr}
                            inverse
                          />
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </>
        )}
      </Section>

      {/* ═══ 7. Рекомендации ═══ */}
      <Section icon={<Info className="h-5 w-5 text-[hsl(var(--primary))]" />} title="Рекомендации" accentColor="hsl(var(--primary))">
        <div className="space-y-3 mt-3">
          {a.burningMoney.length > 0 && (
            <Rec severity="critical" title={`Отключите ${a.burningMoney.length} кампаний без заказов`}
              text={`${fmtMoney(a.wastedSpend)} потрачено впустую. Отключите или снизьте ставки. Перераспределите на эффективные.`} />
          )}
          {a.burningMoney.filter(b => b.campaign.cart === 0).length > 0 && (
            <Rec severity="warning" title="Проверьте карточки товаров"
              text={`${a.burningMoney.filter(b => b.campaign.cart === 0).length} кампаний без корзин — проблема в товаре, не в рекламе. Улучшите фото, цену, отзывы.`} />
          )}
          {a.unprofitable.length > 0 && (
            <Rec severity="warning" title={`Снизьте ставки на ${a.unprofitable.length} убыточных кампаниях`}
              text="Кампании с ДРР > 50% тратят слишком много. Снизьте ставку за клик." />
          )}
          {a.lowCtr.length > 0 && (
            <Rec severity="warning" title="Улучшите кликабельность"
              text={`${a.lowCtr.length} кампаний с CTR < 1%. Смените фото, добавьте бейдж скидки, проверьте цену.`} />
          )}
          {a.effective.length > 0 && (
            <Rec severity="success" title={`Масштабируйте ${a.effective.length} эффективных кампаний`}
              text={`Увеличьте бюджет на лучших — они окупаются. Средний ROMI: ${fmt(a.effective.reduce((s, e) => s + e.romi, 0) / a.effective.length)}%.`} />
          )}
          {a.evtSummary.total > 50 && (
            <Rec severity="info" title="Много изменений за период"
              text={`${a.evtSummary.total} событий. Частые изменения мешают алгоритмам Ozon оптимизировать показы. Дайте 2-3 дня без изменений.`} />
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

function EventTypeCard({ label, count, desc, color, bg, border }: { label: string; count: number; desc: string; color: string; bg: string; border: string }) {
  return (
    <div className={`rounded-xl border ${border} ${bg} p-4`}>
      <div className={`text-[12px] ${color} font-medium mb-1`}>{label}</div>
      <div className={`text-[22px] font-bold ${color}`}>{count}</div>
      <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)] mt-0.5">{desc}</div>
    </div>
  )
}

function BeforeAfterMetric({ label, before, after, delta, inverse = false }: { label: string; before: string; after: string; delta: number; inverse?: boolean }) {
  return (
    <div className="rounded-lg bg-[hsl(var(--muted)/0.08)] p-2.5">
      <div className="text-[10px] uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.4)] mb-1">{label}</div>
      <div className="flex items-center gap-1 text-[12px] text-[hsl(var(--muted-foreground)/0.5)]">
        <span>{before}</span>
        <span>→</span>
        <span className={`font-semibold ${deltaColor(delta, inverse)}`}>{after}</span>
      </div>
      <div className={`text-[12px] font-semibold mt-0.5 ${deltaColor(delta, inverse)}`}>
        {deltaIcon(inverse ? -delta : delta)}{' '}
        {delta > 0 ? '+' : ''}{delta.toFixed(0)}%
      </div>
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
