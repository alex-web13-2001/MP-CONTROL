import { useMemo } from 'react'
import {
  AlertTriangle,
  TrendingDown,
  Zap,
  ShoppingCart,
  Ban,
  Trophy,
  ThumbsUp,
  ArrowDown,
  DollarSign,
  BarChart2,
} from 'lucide-react'
import type { CampaignRow, EventDaySummary } from '@/api/advertising'

/* ═══════════════════════════════════════════════════════════
   Types
   ═══════════════════════════════════════════════════════════ */

type Severity = 'critical' | 'warning' | 'success' | 'info'

interface Insight {
  id: string
  severity: Severity
  icon: React.ReactNode
  title: string
  description: string
  campaigns: string[]
  metric?: string
}

/* ═══════════════════════════════════════════════════════════
   Rule engine
   ═══════════════════════════════════════════════════════════ */

function formatMoney(v: number): string {
  return v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
}

function generateInsights(
  campaigns: CampaignRow[],
  eventsByDay: Record<string, EventDaySummary>,
): Insight[] {
  const insights: Insight[] = []
  const active = campaigns.filter((c) => c.status !== 'archived' && c.status !== 'stopped')
  if (active.length === 0) return insights

  const avgCpc = active.reduce((s, c) => s + c.avg_cpc, 0) / active.length
  const totalSpend = active.reduce((s, c) => s + c.spend, 0)

  // 1. Слив бюджета без заказов
  const noOrders = active.filter((c) => c.spend > 1500 && c.orders === 0)
  if (noOrders.length > 0) {
    const wastedSpend = noOrders.reduce((s, c) => s + c.spend, 0)
    insights.push({
      id: 'no-orders',
      severity: 'critical',
      icon: <Ban className="h-4 w-4" />,
      title: `${noOrders.length} ${noOrders.length === 1 ? 'кампания' : noOrders.length < 5 ? 'кампании' : 'кампаний'} без заказов`,
      description: `Потрачено ${formatMoney(wastedSpend)} без единого заказа. Рекомендуется отключить или снизить ставки.`,
      campaigns: noOrders.map((c) => c.title),
      metric: formatMoney(wastedSpend),
    })
  }

  // 2. Слив без корзин
  const noCarts = active.filter((c) => c.spend > 800 && c.cart === 0 && c.orders === 0)
  if (noCarts.length > 0 && noCarts.length !== noOrders.length) {
    insights.push({
      id: 'no-carts',
      severity: 'critical',
      icon: <ShoppingCart className="h-4 w-4" />,
      title: `${noCarts.length} ${noCarts.length === 1 ? 'кампания' : 'кампаний'} без корзин`,
      description: `Нет ни одного добавления в корзину — проблема скорее всего в карточке товара (фото, цена, отзывы), а не в рекламе.`,
      campaigns: noCarts.map((c) => c.title),
    })
  }

  // 3. Убыточные кампании (DRR > 100%)
  const unprofitable = active.filter((c) => c.drr > 100 && c.spend > 500)
  if (unprofitable.length > 0) {
    const lossSpend = unprofitable.reduce((s, c) => s + c.spend, 0)
    const lossRevenue = unprofitable.reduce((s, c) => s + c.revenue, 0)
    insights.push({
      id: 'unprofitable',
      severity: 'critical',
      icon: <TrendingDown className="h-4 w-4" />,
      title: `${unprofitable.length} убыточн${unprofitable.length === 1 ? 'ая' : 'ых'} кампани${unprofitable.length === 1 ? 'я' : 'й'}`,
      description: `ДРР > 100% — расход (${formatMoney(lossSpend)}) превышает выручку (${formatMoney(lossRevenue)}). Снизьте ставки.`,
      campaigns: unprofitable.map((c) => `${c.title} (ДРР ${c.drr.toFixed(1)}%)`),
      metric: `${formatMoney(lossSpend - lossRevenue)} убыток`,
    })
  }

  // 4. Высокий DRR (50-100%)
  const highDrr = active.filter((c) => c.drr >= 50 && c.drr <= 100 && c.spend > 500)
  if (highDrr.length > 0) {
    insights.push({
      id: 'high-drr',
      severity: 'warning',
      icon: <AlertTriangle className="h-4 w-4" />,
      title: `${highDrr.length} кампани${highDrr.length === 1 ? 'я' : 'й'} с высоким ДРР`,
      description: `ДРР от 50% до 100% — реклама на грани рентабельности. Следите за динамикой.`,
      campaigns: highDrr.map((c) => `${c.title} (ДРР ${c.drr.toFixed(1)}%)`),
    })
  }

  // 5. Дорогой клик без конверсии
  const expensiveClicks = active.filter(
    (c) => c.avg_cpc > avgCpc * 1.8 && c.avg_cpc > 10 && c.orders === 0 && c.clicks > 10,
  )
  if (expensiveClicks.length > 0) {
    insights.push({
      id: 'expensive-clicks',
      severity: 'warning',
      icon: <DollarSign className="h-4 w-4" />,
      title: `Дорогие клики без конверсии`,
      description: `CPC выше среднего (${avgCpc.toFixed(1)}₽) при нулевых заказах. Снизьте ставку.`,
      campaigns: expensiveClicks.map((c) => `${c.title} (CPC ${c.avg_cpc.toFixed(1)}₽)`),
    })
  }

  // 6. Лучшие кампании по ROMI
  const withRevenue = active
    .filter((c) => c.orders >= 2 && c.spend > 100 && c.revenue > 0)
    .sort((a, b) => b.revenue / b.spend - a.revenue / a.spend)
    .slice(0, 3)

  if (withRevenue.length > 0) {
    insights.push({
      id: 'best-romi',
      severity: 'success',
      icon: <Trophy className="h-4 w-4" />,
      title: `Топ ${withRevenue.length} по эффективности`,
      description: `Лучшие кампании по соотношению выручка/расход.`,
      campaigns: withRevenue.map(
        (c) =>
          `${c.title} — ROMI ${((c.revenue / c.spend) * 100).toFixed(0)}%, ДРР ${c.drr.toFixed(1)}%, ${c.orders} заказов`,
      ),
    })
  }

  // 7. Дешёвые заказы
  const avgCpo =
    active.filter((c) => c.orders > 0).reduce((s, c) => s + c.spend / c.orders, 0) /
    (active.filter((c) => c.orders > 0).length || 1)

  const cheapOrders = active
    .filter((c) => c.orders >= 2 && c.spend / c.orders < avgCpo * 0.6)
    .sort((a, b) => a.spend / a.orders - b.spend / b.orders)
    .slice(0, 3)

  if (cheapOrders.length > 0) {
    insights.push({
      id: 'cheap-orders',
      severity: 'success',
      icon: <ThumbsUp className="h-4 w-4" />,
      title: `Дешёвые заказы`,
      description: `CPO значительно ниже среднего (${formatMoney(avgCpo)}).`,
      campaigns: cheapOrders.map(
        (c) => `${c.title} — CPO ${formatMoney(c.spend / c.orders)} (${c.orders} заказов)`,
      ),
    })
  }

  // 8. Низкий CTR при большом кол-ве показов
  const lowCtr = active.filter(
    (c) => c.ctr < 0.5 && c.views > 3000 && c.spend > 300,
  )
  if (lowCtr.length > 0) {
    insights.push({
      id: 'low-ctr',
      severity: 'warning',
      icon: <BarChart2 className="h-4 w-4" />,
      title: `Низкий CTR`,
      description: `CTR ниже 0.5% при большом числе показов — карточка не привлекает внимание. Проверьте фото и цену.`,
      campaigns: lowCtr.map(
        (c) => `${c.title} — CTR ${c.ctr.toFixed(2)}%, ${c.views.toLocaleString()} показов`,
      ),
    })
  }

  // 9. События за период
  const totalEvents = Object.values(eventsByDay).reduce(
    (acc, d) => ({
      advertising: acc.advertising + d.advertising,
      content: acc.content + d.content,
      price: acc.price + d.price,
      stock: acc.stock + d.stock,
      total: acc.total + d.total,
    }),
    { advertising: 0, content: 0, price: 0, stock: 0, total: 0 },
  )

  if (totalEvents.total > 0) {
    const parts: string[] = []
    if (totalEvents.advertising > 0) parts.push(`${totalEvents.advertising} рекламных`)
    if (totalEvents.content > 0) parts.push(`${totalEvents.content} контентных`)
    if (totalEvents.price > 0) parts.push(`${totalEvents.price} ценовых`)
    if (totalEvents.stock > 0) parts.push(`${totalEvents.stock} складских`)

    insights.push({
      id: 'events-summary',
      severity: 'info',
      icon: <Zap className="h-4 w-4" />,
      title: `${totalEvents.total} событий за период`,
      description: parts.join(', ') + '. Наведите на маркеры на графике для деталей.',
      campaigns: [],
    })
  }

  // 10. Общая сводка — доля расхода без заказов
  const wasteRatio =
    totalSpend > 0
      ? active.filter((c) => c.orders === 0).reduce((s, c) => s + c.spend, 0) / totalSpend
      : 0

  if (wasteRatio > 0.3 && totalSpend > 3000) {
    insights.push({
      id: 'waste-ratio',
      severity: 'warning',
      icon: <ArrowDown className="h-4 w-4" />,
      title: `${(wasteRatio * 100).toFixed(0)}% бюджета — без заказов`,
      description: `${formatMoney(wasteRatio * totalSpend)} из ${formatMoney(totalSpend)} потрачено на кампании, которые не принесли ни одного заказа.`,
      campaigns: [],
    })
  }

  return insights
}

/* ═══════════════════════════════════════════════════════════
   Styles
   ═══════════════════════════════════════════════════════════ */

const severityConfig: Record<
  Severity,
  { bg: string; border: string; icon: string; badge: string }
> = {
  critical: {
    bg: 'bg-red-500/5',
    border: 'border-red-500/20',
    icon: 'text-red-400',
    badge: 'bg-red-500/15 text-red-400',
  },
  warning: {
    bg: 'bg-amber-500/5',
    border: 'border-amber-500/20',
    icon: 'text-amber-400',
    badge: 'bg-amber-500/15 text-amber-400',
  },
  success: {
    bg: 'bg-emerald-500/5',
    border: 'border-emerald-500/20',
    icon: 'text-emerald-400',
    badge: 'bg-emerald-500/15 text-emerald-400',
  },
  info: {
    bg: 'bg-blue-500/5',
    border: 'border-blue-500/20',
    icon: 'text-blue-400',
    badge: 'bg-blue-500/15 text-blue-400',
  },
}

/* ═══════════════════════════════════════════════════════════
   Component
   ═══════════════════════════════════════════════════════════ */

export function CampaignInsights({
  campaigns,
  eventsByDay,
}: {
  campaigns: CampaignRow[]
  eventsByDay: Record<string, EventDaySummary>
}) {
  const insights = useMemo(
    () => generateInsights(campaigns, eventsByDay),
    [campaigns, eventsByDay],
  )

  if (insights.length === 0) return null

  const criticalCount = insights.filter((i) => i.severity === 'critical').length
  const warningCount = insights.filter((i) => i.severity === 'warning').length
  const successCount = insights.filter((i) => i.severity === 'success').length

  return (
    <div className="space-y-3">
      {/* Header row */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <h3 className="text-[15px] font-semibold text-[hsl(var(--foreground))]">
            Анализ кампаний
          </h3>
          <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.5)]">
            {insights.length} {insights.length === 1 ? 'наблюдение' : insights.length < 5 ? 'наблюдения' : 'наблюдений'}
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          {criticalCount > 0 && (
            <span className="inline-flex items-center gap-1 rounded-md bg-red-500/15 px-2 py-0.5 text-[11px] font-medium text-red-400">
              {criticalCount} критичн.
            </span>
          )}
          {warningCount > 0 && (
            <span className="inline-flex items-center gap-1 rounded-md bg-amber-500/15 px-2 py-0.5 text-[11px] font-medium text-amber-400">
              {warningCount} вним.
            </span>
          )}
          {successCount > 0 && (
            <span className="inline-flex items-center gap-1 rounded-md bg-emerald-500/15 px-2 py-0.5 text-[11px] font-medium text-emerald-400">
              {successCount} хорошо
            </span>
          )}
        </div>
      </div>

      {/* Insights grid */}
      <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
        {insights.map((ins) => {
          const cfg = severityConfig[ins.severity]
          return (
            <div
              key={ins.id}
              className={`group relative rounded-xl border ${cfg.border} ${cfg.bg} p-3.5 transition-all duration-200 hover:shadow-sm`}
            >
              <div className="flex items-start gap-3">
                <div className={`mt-0.5 shrink-0 ${cfg.icon}`}>{ins.icon}</div>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">
                      {ins.title}
                    </span>
                    {ins.metric && (
                      <span
                        className={`shrink-0 rounded-md px-1.5 py-0.5 text-[10px] font-semibold ${cfg.badge}`}
                      >
                        {ins.metric}
                      </span>
                    )}
                  </div>
                  <p className="mt-0.5 text-[12px] leading-relaxed text-[hsl(var(--muted-foreground))]">
                    {ins.description}
                  </p>
                  {ins.campaigns.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-1">
                      {ins.campaigns.slice(0, 4).map((name, i) => (
                        <span
                          key={i}
                          className="inline-block max-w-[260px] truncate rounded-md bg-[hsl(var(--muted)/0.3)] px-1.5 py-0.5 text-[10px] text-[hsl(var(--muted-foreground)/0.7)]"
                          title={name}
                        >
                          {name}
                        </span>
                      ))}
                      {ins.campaigns.length > 4 && (
                        <span className="rounded-md bg-[hsl(var(--muted)/0.2)] px-1.5 py-0.5 text-[10px] text-[hsl(var(--muted-foreground)/0.4)]">
                          +{ins.campaigns.length - 4}
                        </span>
                      )}
                    </div>
                  )}
                </div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
