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
} from 'lucide-react'
import type { CampaignRow, EventDaySummary, EventDetail } from '@/api/advertising'
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
   CampaignRow renderer — single campaign line
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

/* metric column pill */
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
  const color = bad
    ? 'text-red-400'
    : good
      ? 'text-emerald-400'
      : 'text-[hsl(var(--foreground))]'
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
   Event impact analysis
   ═══════════════════════════════════════════════════════════ */

interface CampaignEventImpact {
  campaign_title: string
  campaign_id: number
  events: EventDetail[]
  eventTypes: string[]
  summary: string
}

function buildEventImpacts(events: EventDetail[], campaigns: CampaignRow[]): CampaignEventImpact[] {
  // Group events by campaign
  const byCampaign = new Map<number, EventDetail[]>()
  for (const e of events) {
    if (e.campaign_id) {
      const arr = byCampaign.get(e.campaign_id) || []
      arr.push(e)
      byCampaign.set(e.campaign_id, arr)
    }
  }

  const impacts: CampaignEventImpact[] = []
  for (const [campId, campEvents] of byCampaign) {
    const campaign = campaigns.find((c) => c.campaign_id === campId)
    if (!campaign) continue

    const eventTypes = [...new Set(campEvents.map((e) => e.event_type))]
    const parts: string[] = []

    const bidChanges = campEvents.filter((e) =>
      e.event_type.includes('BID') || e.event_type.includes('bid'),
    )
    const statusChanges = campEvents.filter((e) =>
      e.event_type.includes('STATUS') || e.event_type.includes('status'),
    )
    const contentChanges = campEvents.filter((e) =>
      e.category === 'content',
    )
    const priceChanges = campEvents.filter((e) =>
      e.category === 'price',
    )

    if (bidChanges.length > 0) {
      parts.push(`${bidChanges.length} ${bidChanges.length === 1 ? 'смена' : 'смен'} ставок`)
    }
    if (statusChanges.length > 0) {
      parts.push(`${statusChanges.length} ${statusChanges.length === 1 ? 'смена' : 'смен'} статуса`)
    }
    if (contentChanges.length > 0) {
      parts.push(`${contentChanges.length} ${contentChanges.length === 1 ? 'изменение' : 'изменений'} контента`)
    }
    if (priceChanges.length > 0) {
      parts.push(`${priceChanges.length} ${priceChanges.length === 1 ? 'изменение' : 'изменений'} цены`)
    }

    // Add campaign metrics context
    const metricsParts: string[] = []
    if (campaign.orders > 0) {
      metricsParts.push(`${campaign.orders} заказов, ДРР ${pct(campaign.drr)}`)
    } else {
      metricsParts.push(`0 заказов, расход ${fmtMoney(campaign.spend)}`)
    }

    const summary =
      parts.join(', ') +
      (metricsParts.length > 0 ? ` → текущие показатели: ${metricsParts.join(', ')}` : '')

    impacts.push({
      campaign_title: campaign.title,
      campaign_id: campId,
      events: campEvents,
      eventTypes,
      summary,
    })
  }

  return impacts.sort((a, b) => b.events.length - a.events.length)
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
  const [allEvents, setAllEvents] = useState<EventDetail[]>([])
  const [eventsLoading, setEventsLoading] = useState(false)

  // Load all events for the period
  useEffect(() => {
    if (!shopId || !dateFrom || !dateTo) return
    const dates = Object.keys(eventsByDay).filter(
      (d) => (eventsByDay[d]?.total || 0) > 0,
    )
    if (dates.length === 0) return

    setEventsLoading(true)
    Promise.all(dates.map((d) => getEventsDetail(shopId, d).catch(() => null)))
      .then((results) => {
        const events: EventDetail[] = []
        for (const r of results) {
          if (r?.events) events.push(...r.events)
        }
        setAllEvents(events)
      })
      .finally(() => setEventsLoading(false))
  }, [shopId, dateFrom, dateTo, eventsByDay])

  const analysis = useMemo(() => {
    const active = campaigns.filter((c) => c.status !== 'archived')
    const totalSpend = active.reduce((s, c) => s + c.spend, 0)
    const totalRevenue = active.reduce((s, c) => s + c.revenue, 0)
    const totalOrders = active.reduce((s, c) => s + c.orders, 0)
    const withOrders = active.filter((c) => c.orders > 0)
    const withoutOrders = active.filter((c) => c.orders === 0 && c.spend > 0)

    const avgCpc = active.filter((c) => c.avg_cpc > 0).length > 0
      ? active.filter((c) => c.avg_cpc > 0).reduce((s, c) => s + c.avg_cpc, 0) /
        active.filter((c) => c.avg_cpc > 0).length
      : 0
    const avgDrr = withOrders.length > 0
      ? withOrders.reduce((s, c) => s + c.drr, 0) / withOrders.length
      : 0
    const avgCpo = withOrders.length > 0
      ? withOrders.reduce((s, c) => s + c.spend / c.orders, 0) / withOrders.length
      : 0
    const wastedSpend = withoutOrders.reduce((s, c) => s + c.spend, 0)

    // ── 1. Сливают бюджет (0 заказов, расход > 0) ──
    const burningMoney = withoutOrders
      .sort((a, b) => b.spend - a.spend)
      .map((c) => {
        const problems: string[] = []
        problems.push(`Расход ${fmtMoney(c.spend)} → 0 заказов`)
        if (c.cart === 0) {
          problems.push(`0 корзин — аудитория не заинтересована в товаре`)
        } else {
          problems.push(`${c.cart} корзин, но ни одного заказа — возможно проблема с ценой или доставкой`)
        }
        if (c.ctr < 0.5 && c.views > 1000) {
          problems.push(`CTR ${pct(c.ctr)} — карточка не привлекает внимание`)
        }
        if (c.avg_cpc > avgCpc * 1.5 && avgCpc > 0) {
          problems.push(`CPC ${fmtMoney(c.avg_cpc)} — в ${(c.avg_cpc / avgCpc).toFixed(1)}x выше среднего`)
        }
        return { campaign: c, problems }
      })

    // ── 2. Убыточные (есть заказы, но DRR > 50%) ──
    const unprofitable = withOrders
      .filter((c) => c.drr > 50)
      .sort((a, b) => b.drr - a.drr)
      .map((c) => {
        const problems: string[] = []
        if (c.drr > 100) {
          problems.push(
            `ДРР ${pct(c.drr)} — убыток ${fmtMoney(c.spend - c.revenue)}`,
          )
        } else {
          problems.push(`ДРР ${pct(c.drr)} — на грани рентабельности`)
        }
        problems.push(
          `Расход ${fmtMoney(c.spend)} → ${c.orders} заказов на ${fmtMoney(c.revenue)}`,
        )
        if (c.avg_cpc > avgCpc * 1.3 && avgCpc > 0) {
          problems.push(`CPC ${fmtMoney(c.avg_cpc)} — выше среднего (${fmtMoney(avgCpc)})`)
        }
        return { campaign: c, problems }
      })

    // ── 3. Низкий CTR ──
    const lowCtr = active
      .filter((c) => c.ctr < 1 && c.views > 2000 && c.spend > 100)
      .sort((a, b) => a.ctr - b.ctr)
      .map((c) => {
        const problems: string[] = []
        problems.push(`CTR ${pct(c.ctr)} при ${fmt(c.views)} показах`)
        problems.push(
          `Пользователи видят объявление, но не кликают. Проверьте главное фото, цену и название`,
        )
        return { campaign: c, problems }
      })

    // ── 4. Низкая конверсия в корзину ──
    const lowCartConv = active
      .filter((c) => c.clicks > 20 && c.cart_conv < 5 && c.cart_conv >= 0)
      .sort((a, b) => a.cart_conv - b.cart_conv)
      .map((c) => {
        const problems: string[] = []
        problems.push(
          `Конверсия в корзину ${pct(c.cart_conv)} (${c.cart} из ${c.clicks} кликов)`,
        )
        problems.push(
          `Кликают, но не добавляют в корзину — проблема на карточке товара (цена, описание, отзывы)`,
        )
        return { campaign: c, problems }
      })

    // ── 5. Эффективные кампании ──
    const effective = withOrders
      .filter((c) => c.orders >= 1 && c.spend > 50)
      .sort((a, b) => (b.revenue / b.spend) - (a.revenue / a.spend))
      .map((c) => {
        const romi = c.spend > 0 ? (c.revenue / c.spend) * 100 : 0
        const cpo = c.orders > 0 ? c.spend / c.orders : 0
        const highlights: string[] = []
        if (c.drr < 30) highlights.push(`ДРР ${pct(c.drr)}`)
        if (romi > 300) highlights.push(`ROMI ${fmt(romi)}%`)
        if (cpo < avgCpo * 0.7 && avgCpo > 0) highlights.push(`Дешёвый CPO ${fmtMoney(cpo)}`)
        if (c.ctr > 3) highlights.push(`Высокий CTR ${pct(c.ctr)}`)
        if (c.cart_conv > 20) highlights.push(`Конверсия в корзину ${pct(c.cart_conv)}`)
        if (c.direct_orders > 0 && c.model_orders > 0) {
          highlights.push(`Прямые: ${c.direct_orders}, модельные: ${c.model_orders}`)
        }
        return { campaign: c, romi, cpo, highlights }
      })

    // ── Events summary ──
    const evtSummary = Object.values(eventsByDay).reduce(
      (acc, d) => ({
        advertising: acc.advertising + d.advertising,
        content: acc.content + d.content,
        price: acc.price + d.price,
        stock: acc.stock + d.stock,
        total: acc.total + d.total,
      }),
      { advertising: 0, content: 0, price: 0, stock: 0, total: 0 },
    )

    return {
      totalSpend,
      totalRevenue,
      totalOrders,
      wastedSpend,
      wastePercent: totalSpend > 0 ? (wastedSpend / totalSpend) * 100 : 0,
      avgCpc,
      avgDrr,
      avgCpo,
      activeCampaigns: active.length,
      campaignsWithOrders: withOrders.length,
      campaignsWithoutOrders: withoutOrders.length,
      burningMoney,
      unprofitable,
      lowCtr,
      lowCartConv,
      effective,
      evtSummary,
    }
  }, [campaigns, eventsByDay])

  const eventImpacts = useMemo(
    () => (allEvents.length > 0 ? buildEventImpacts(allEvents, campaigns) : []),
    [allEvents, campaigns],
  )

  if (campaigns.length === 0) return null

  const a = analysis

  return (
    <div className="space-y-5">
      {/* ═══ Title ═══ */}
      <div className="flex items-center gap-3">
        <Activity className="h-6 w-6 text-[hsl(var(--primary))]" />
        <h3 className="text-[20px] font-bold text-[hsl(var(--foreground))]">
          Анализ рекламных кампаний
        </h3>
        <span className="text-[13px] text-[hsl(var(--muted-foreground)/0.5)]">
          рекламные заказы (атрибуция Ozon)
        </span>
      </div>

      {/* ═══ Summary ═══ */}
      <div className="rounded-2xl border border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.06)] p-5">
        <div className="grid grid-cols-2 gap-5 sm:grid-cols-3 lg:grid-cols-6">
          <SummaryCard icon={<Target className="h-4 w-4 text-[hsl(var(--primary))]" />} label="Кампаний" value={a.activeCampaigns} />
          <SummaryCard
            icon={<CheckCircle2 className="h-4 w-4 text-emerald-400" />}
            label="С заказами"
            value={a.campaignsWithOrders}
            valueColor="text-emerald-400"
          />
          <SummaryCard
            icon={<Ban className="h-4 w-4 text-red-400" />}
            label="Без заказов"
            value={a.campaignsWithoutOrders}
            valueColor="text-red-400"
          />
          <SummaryCard icon={<MousePointerClick className="h-4 w-4 text-blue-400" />} label="Ср. CPC" value={fmtMoney(a.avgCpc)} />
          <SummaryCard icon={<BarChart2 className="h-4 w-4 text-amber-400" />} label="Ср. ДРР" value={pct(a.avgDrr)} />
          <SummaryCard icon={<DollarSign className="h-4 w-4 text-purple-400" />} label="Ср. CPO" value={fmtMoney(a.avgCpo)} />
        </div>

        <div className="mt-4 pt-4 border-t border-[hsl(var(--border)/0.3)] text-[14px] leading-relaxed text-[hsl(var(--muted-foreground))]">
          За период потрачено <strong className="text-[hsl(var(--foreground))]">{fmtMoney(a.totalSpend)}</strong> на рекламу,
          получено <strong className="text-[hsl(var(--foreground))]">{a.totalOrders} рекламных заказов</strong> на сумму{' '}
          <strong className="text-[hsl(var(--foreground))]">{fmtMoney(a.totalRevenue)}</strong>.
          {a.wastedSpend > 0 && (
            <>
              {' '}Из них{' '}
              <strong className="text-red-400">{fmtMoney(a.wastedSpend)} ({pct(a.wastePercent)})</strong>{' '}
              потрачено на кампании, которые не принесли ни одного заказа.
            </>
          )}
        </div>
      </div>

      {/* ═══ 1. Сливают бюджет (0 заказов) ═══ */}
      {a.burningMoney.length > 0 && (
        <Section
          icon={<Ban className="h-5 w-5 text-red-400" />}
          title="Сливают бюджет"
          count={a.burningMoney.length}
          countColor="text-red-400"
          accentColor="rgb(239 68 68)"
          badge={
            <span className="rounded-lg bg-red-500/10 px-3 py-1 text-[13px] font-semibold text-red-400">
              −{fmtMoney(a.wastedSpend)}
            </span>
          }
        >
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            Кампании с расходом, но без единого рекламного заказа за весь период. Рекомендуется отключить или снизить ставку до минимума.
          </p>
          <div className="space-y-2">
            {a.burningMoney.map(({ campaign: c, problems }) => (
              <CampaignLine
                key={c.campaign_id}
                c={c}
                borderColor="border-red-500/15"
                bgColor="bg-red-500/[0.02]"
                problems={problems}
                columns={
                  <>
                    <Metric label="Расход" value={fmtMoney(c.spend)} bad />
                    <Metric label="Показы" value={fmt(c.views)} />
                    <Metric label="Клики" value={fmt(c.clicks)} />
                    <Metric label="CTR" value={pct(c.ctr)} bad={c.ctr < 0.5} />
                    <Metric label="Корзины" value={c.cart} bad={c.cart === 0} />
                    <Metric label="Заказы" value={0} bad />
                  </>
                }
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 2. Убыточные кампании (DRR > 50%) ═══ */}
      {a.unprofitable.length > 0 && (
        <Section
          icon={<AlertTriangle className="h-5 w-5 text-amber-400" />}
          title="Высокий ДРР"
          count={a.unprofitable.length}
          countColor="text-amber-400"
          accentColor="rgb(245 158 11)"
        >
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            Кампании с заказами, но ДРР выше 50% — реклама на грани рентабельности или убыточна. Снизьте ставки за клик.
          </p>
          <div className="space-y-2">
            {a.unprofitable.map(({ campaign: c, problems }) => (
              <CampaignLine
                key={c.campaign_id}
                c={c}
                borderColor="border-amber-500/15"
                bgColor="bg-amber-500/[0.02]"
                problems={problems}
                columns={
                  <>
                    <Metric label="Расход" value={fmtMoney(c.spend)} />
                    <Metric label="Заказы" value={c.orders} />
                    <Metric label="Выручка" value={fmtMoney(c.revenue)} />
                    <Metric label="ДРР" value={pct(c.drr)} bad={c.drr > 100} />
                    <Metric label="CPO" value={fmtMoney(c.orders > 0 ? c.spend / c.orders : 0)} />
                  </>
                }
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 3. Низкий CTR ═══ */}
      {a.lowCtr.length > 0 && (
        <Section
          icon={<Eye className="h-5 w-5 text-orange-400" />}
          title="Низкий CTR"
          count={a.lowCtr.length}
          countColor="text-orange-400"
          accentColor="rgb(251 146 60)"
        >
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            CTR ниже 1% — пользователи видят рекламу, но не кликают. Проверьте главное фото, цену на карточке и название товара.
          </p>
          <div className="space-y-2">
            {a.lowCtr.map(({ campaign: c, problems }) => (
              <CampaignLine
                key={c.campaign_id}
                c={c}
                borderColor="border-orange-500/15"
                bgColor="bg-orange-500/[0.02]"
                problems={problems}
                columns={
                  <>
                    <Metric label="CTR" value={pct(c.ctr)} bad />
                    <Metric label="Показы" value={fmt(c.views)} />
                    <Metric label="Клики" value={fmt(c.clicks)} />
                    <Metric label="Расход" value={fmtMoney(c.spend)} />
                  </>
                }
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 4. Низкая конверсия в корзину ═══ */}
      {a.lowCartConv.length > 0 && (
        <Section
          icon={<ShoppingCart className="h-5 w-5 text-rose-400" />}
          title="Низкая конверсия в корзину"
          count={a.lowCartConv.length}
          countColor="text-rose-400"
          accentColor="rgb(251 113 133)"
        >
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            Конверсия в корзину ниже 5% — кликают на рекламу, но не добавляют товар в корзину. Проблема на карточке: цена, описание, отзывы, характеристики.
          </p>
          <div className="space-y-2">
            {a.lowCartConv.map(({ campaign: c, problems }) => (
              <CampaignLine
                key={c.campaign_id}
                c={c}
                borderColor="border-rose-500/15"
                bgColor="bg-rose-500/[0.02]"
                problems={problems}
                columns={
                  <>
                    <Metric label="CR корз." value={pct(c.cart_conv)} bad />
                    <Metric label="Клики" value={fmt(c.clicks)} />
                    <Metric label="Корзины" value={c.cart} />
                    <Metric label="Расход" value={fmtMoney(c.spend)} />
                  </>
                }
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 5. Эффективные кампании ═══ */}
      {a.effective.length > 0 && (
        <Section
          icon={<Trophy className="h-5 w-5 text-emerald-400" />}
          title="Эффективные кампании"
          count={a.effective.length}
          countColor="text-emerald-400"
          accentColor="rgb(52 211 153)"
        >
          <p className="text-[14px] text-[hsl(var(--muted-foreground))] mt-3 mb-4">
            Кампании с заказами. Рассмотрите увеличение бюджета на лучших из них.
          </p>
          <div className="space-y-2">
            {a.effective.map(({ campaign: c, romi, cpo, highlights }) => (
              <CampaignLine
                key={c.campaign_id}
                c={c}
                borderColor="border-emerald-500/15"
                bgColor="bg-emerald-500/[0.02]"
                highlights={highlights}
                columns={
                  <>
                    <Metric label="Расход" value={fmtMoney(c.spend)} />
                    <Metric label="Заказы" value={c.orders} good />
                    <Metric label="Выручка" value={fmtMoney(c.revenue)} good />
                    <Metric label="ДРР" value={pct(c.drr)} good={c.drr < 30} />
                    <Metric label="CPO" value={fmtMoney(cpo)} />
                    <Metric label="ROMI" value={`${fmt(romi)}%`} good={romi > 200} />
                  </>
                }
              />
            ))}
          </div>
        </Section>
      )}

      {/* ═══ 6. Влияние событий на кампании ═══ */}
      {(eventImpacts.length > 0 || eventsLoading) && (
        <Section
          icon={<Zap className="h-5 w-5 text-blue-400" />}
          title="Влияние событий на кампании"
          count={a.evtSummary.total}
          countColor="text-blue-400"
          accentColor="rgb(96 165 250)"
          defaultOpen={true}
        >
          {eventsLoading ? (
            <div className="py-6 text-center text-[14px] text-[hsl(var(--muted-foreground)/0.5)]">
              Загрузка событий...
            </div>
          ) : (
            <>
              {/* Event type summary */}
              <div className="mt-3 mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
                {a.evtSummary.advertising > 0 && (
                  <EventTypeCard label="Рекламные" count={a.evtSummary.advertising} desc="Ставки, статусы" color="text-blue-400" bg="bg-blue-500/5" border="border-blue-500/15" />
                )}
                {a.evtSummary.content > 0 && (
                  <EventTypeCard label="Контент" count={a.evtSummary.content} desc="Изменения карточек" color="text-purple-400" bg="bg-purple-500/5" border="border-purple-500/15" />
                )}
                {a.evtSummary.price > 0 && (
                  <EventTypeCard label="Ценовые" count={a.evtSummary.price} desc="Изменения цен" color="text-amber-400" bg="bg-amber-500/5" border="border-amber-500/15" />
                )}
                {a.evtSummary.stock > 0 && (
                  <EventTypeCard label="Складские" count={a.evtSummary.stock} desc="Остатки" color="text-cyan-400" bg="bg-cyan-500/5" border="border-cyan-500/15" />
                )}
              </div>

              {/* Per-campaign events */}
              {eventImpacts.length > 0 && (
                <div className="space-y-2">
                  <h4 className="text-[14px] font-semibold text-[hsl(var(--foreground))] mb-2">
                    События по кампаниям
                  </h4>
                  {eventImpacts.slice(0, 15).map((impact) => (
                    <div
                      key={impact.campaign_id}
                      className="rounded-xl border border-blue-500/10 bg-blue-500/[0.02] p-4"
                    >
                      <div className="flex items-center gap-2 mb-2">
                        <Zap className="h-4 w-4 text-blue-400 shrink-0" />
                        <span className="text-[14px] font-semibold text-[hsl(var(--foreground))] truncate" title={impact.campaign_title}>
                          {impact.campaign_title}
                        </span>
                        <span className="text-[13px] text-blue-400 font-medium shrink-0">
                          {impact.events.length} {impact.events.length === 1 ? 'событие' : impact.events.length < 5 ? 'события' : 'событий'}
                        </span>
                      </div>
                      <div className="text-[13px] text-[hsl(var(--muted-foreground))] mb-2">
                        {impact.summary}
                      </div>
                      {/* Individual events */}
                      <div className="space-y-1 ml-6">
                        {impact.events.slice(0, 5).map((e) => (
                          <div key={e.id} className="flex items-start gap-2 text-[12px]">
                            <span className="text-[hsl(var(--muted-foreground)/0.4)] shrink-0 mt-0.5">
                              {e.time}
                            </span>
                            <span className="text-[hsl(var(--muted-foreground)/0.7)]">
                              {e.label}{e.detail ? ` — ${e.detail}` : ''}
                            </span>
                          </div>
                        ))}
                        {impact.events.length > 5 && (
                          <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.4)]">
                            +{impact.events.length - 5} ещё
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                  {eventImpacts.length > 15 && (
                    <p className="text-[13px] text-[hsl(var(--muted-foreground)/0.5)] text-center py-2">
                      Показаны 15 из {eventImpacts.length} кампаний с событиями
                    </p>
                  )}
                </div>
              )}
            </>
          )}
        </Section>
      )}

      {/* ═══ 7. Рекомендации ═══ */}
      <Section
        icon={<Info className="h-5 w-5 text-[hsl(var(--primary))]" />}
        title="Рекомендации"
        accentColor="hsl(var(--primary))"
      >
        <div className="space-y-3 mt-3">
          {a.burningMoney.length > 0 && (
            <Rec
              severity="critical"
              title={`Отключите ${a.burningMoney.length} кампаний без заказов`}
              text={`${fmtMoney(a.wastedSpend)} потрачено впустую. Отключите эти кампании или снизьте ставки до минимума. Перераспределите бюджет на эффективные кампании.`}
            />
          )}
          {a.burningMoney.filter((b) => b.campaign.cart === 0).length > 0 && (
            <Rec
              severity="warning"
              title="Проверьте карточки товаров"
              text={`${a.burningMoney.filter((b) => b.campaign.cart === 0).length} кампаний без единой корзины — проблема не в рекламе, а в самом товаре. Улучшите главное фото, проверьте цену, добавьте отзывы.`}
            />
          )}
          {a.unprofitable.length > 0 && (
            <Rec
              severity="warning"
              title={`Снизьте ставки на ${a.unprofitable.length} кампаниях с высоким ДРР`}
              text={`Кампании с ДРР выше 50% тратят слишком много на привлечение. Снизьте ставку за клик — Ozon будет показывать реже, но дешевле.`}
            />
          )}
          {a.lowCtr.length > 0 && (
            <Rec
              severity="warning"
              title="Улучшите кликабельность"
              text={`${a.lowCtr.length} кампаний с CTR ниже 1%. Смените главное фото, добавьте яркий бейдж скидки, проверьте соответствие цены рынку.`}
            />
          )}
          {a.effective.length > 0 && (
            <Rec
              severity="success"
              title={`Масштабируйте ${a.effective.length} эффективных кампаний`}
              text={`Увеличьте недельный бюджет на лучших кампаниях — они окупаются. Средний ROMI лучших: ${fmt(a.effective.reduce((s, e) => s + e.romi, 0) / a.effective.length)}%.`}
            />
          )}
          {a.evtSummary.total > 50 && (
            <Rec
              severity="info"
              title="Много изменений за период"
              text={`${a.evtSummary.total} событий. Частые изменения ставок мешают алгоритмам Ozon оптимизировать показы. Дайте кампаниям 2-3 дня без изменений для набора статистики.`}
            />
          )}
        </div>
      </Section>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Small subcomponents
   ═══════════════════════════════════════════════════════════ */

function SummaryCard({
  icon,
  label,
  value,
  valueColor,
}: {
  icon: React.ReactNode
  label: string
  value: string | number
  valueColor?: string
}) {
  return (
    <div>
      <div className="flex items-center gap-1.5 mb-1">
        {icon}
        <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.6)]">{label}</span>
      </div>
      <div className={`text-[18px] font-bold ${valueColor || 'text-[hsl(var(--foreground))]'}`}>
        {value}
      </div>
    </div>
  )
}

function EventTypeCard({
  label,
  count,
  desc,
  color,
  bg,
  border,
}: {
  label: string
  count: number
  desc: string
  color: string
  bg: string
  border: string
}) {
  return (
    <div className={`rounded-xl border ${border} ${bg} p-4`}>
      <div className={`text-[12px] ${color} font-medium mb-1`}>{label}</div>
      <div className={`text-[22px] font-bold ${color}`}>{count}</div>
      <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)] mt-0.5">{desc}</div>
    </div>
  )
}

function Rec({
  severity,
  title,
  text,
}: {
  severity: 'critical' | 'warning' | 'success' | 'info'
  title: string
  text: string
}) {
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
          <div className="mt-1 text-[13px] leading-relaxed text-[hsl(var(--muted-foreground))]">
            {text}
          </div>
        </div>
      </div>
    </div>
  )
}
