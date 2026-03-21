import { useMemo, useState } from 'react'
import {
  AlertTriangle,
  TrendingUp,
  Zap,
  ShoppingCart,
  Ban,
  Trophy,
  ThumbsUp,
  DollarSign,
  BarChart2,
  ChevronDown,
  ChevronUp,
  ArrowRight,
  Activity,
  Target,
  AlertCircle,
  CheckCircle2,
  Info,
} from 'lucide-react'
import type { CampaignRow, EventDaySummary } from '@/api/advertising'

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
   Analysis logic
   ═══════════════════════════════════════════════════════════ */

interface ProblemCampaign {
  campaign: CampaignRow
  problems: string[]
  severity: 'critical' | 'warning'
  wastedSpend: number
}

interface GoodCampaign {
  campaign: CampaignRow
  romi: number
  cpo: number
  highlights: string[]
}

interface AnalysisResult {
  problemCampaigns: ProblemCampaign[]
  goodCampaigns: GoodCampaign[]
  totalWasted: number
  totalSpend: number
  wastePercent: number
  avgCpc: number
  avgDrr: number
  avgCpo: number
  totalOrders: number
  totalRevenue: number
  eventsSummary: { advertising: number; content: number; price: number; stock: number; total: number }
  activeCampaigns: number
  campaignsWithOrders: number
  campaignsWithoutOrders: number
}

function analyzeAll(
  campaigns: CampaignRow[],
  eventsByDay: Record<string, EventDaySummary>,
): AnalysisResult {
  const active = campaigns.filter((c) => c.status !== 'archived')
  const totalSpend = active.reduce((s, c) => s + c.spend, 0)
  const totalRevenue = active.reduce((s, c) => s + c.revenue, 0)
  const totalOrders = active.reduce((s, c) => s + c.orders, 0)
  const withOrders = active.filter((c) => c.orders > 0)
  const withoutOrders = active.filter((c) => c.orders === 0 && c.spend > 0)

  const avgCpc = active.length > 0 ? active.reduce((s, c) => s + c.avg_cpc, 0) / active.length : 0
  const avgDrr =
    withOrders.length > 0 ? withOrders.reduce((s, c) => s + c.drr, 0) / withOrders.length : 0
  const avgCpo =
    withOrders.length > 0
      ? withOrders.reduce((s, c) => s + c.spend / c.orders, 0) / withOrders.length
      : 0

  const totalWasted = withoutOrders.reduce((s, c) => s + c.spend, 0)

  // Problem campaigns
  const problemCampaigns: ProblemCampaign[] = []

  for (const c of active) {
    if (c.spend < 100) continue
    const problems: string[] = []
    let severity: 'critical' | 'warning' = 'warning'

    if (c.orders === 0 && c.spend > 500) {
      problems.push(`Расход ${fmtMoney(c.spend)} без единого заказа`)
      severity = 'critical'
    }
    if (c.cart === 0 && c.orders === 0 && c.spend > 300) {
      problems.push(`Нет добавлений в корзину — проблема в карточке товара`)
      if (c.spend > 500) severity = 'critical'
    }
    if (c.drr > 100 && c.orders > 0) {
      problems.push(
        `ДРР ${pct(c.drr)} — расход (${fmtMoney(c.spend)}) превышает выручку (${fmtMoney(c.revenue)})`,
      )
      severity = 'critical'
    }
    if (c.drr >= 50 && c.drr <= 100 && c.orders > 0) {
      problems.push(`ДРР ${pct(c.drr)} — на грани рентабельности`)
    }
    if (c.ctr < 0.5 && c.views > 3000) {
      problems.push(`CTR ${pct(c.ctr)} при ${fmt(c.views)} показах — карточка не привлекает`)
    }
    if (c.avg_cpc > avgCpc * 2 && c.avg_cpc > 15 && c.orders === 0) {
      problems.push(`CPC ${fmtMoney(c.avg_cpc)} — в ${(c.avg_cpc / avgCpc).toFixed(1)}x выше среднего`)
    }

    if (problems.length > 0) {
      problemCampaigns.push({
        campaign: c,
        problems,
        severity,
        wastedSpend: c.orders === 0 ? c.spend : 0,
      })
    }
  }

  // Sort: critical first, then by spend
  problemCampaigns.sort((a, b) => {
    if (a.severity !== b.severity) return a.severity === 'critical' ? -1 : 1
    return b.campaign.spend - a.campaign.spend
  })

  // Good campaigns
  const goodCampaigns: GoodCampaign[] = withOrders
    .filter((c) => c.orders >= 2 && c.spend > 100)
    .map((c) => {
      const romi = c.spend > 0 ? (c.revenue / c.spend) * 100 : 0
      const cpo = c.orders > 0 ? c.spend / c.orders : 0
      const highlights: string[] = []

      if (c.drr < 30 && c.drr > 0) highlights.push(`Низкий ДРР ${pct(c.drr)}`)
      if (romi > 300) highlights.push(`ROMI ${fmt(romi)}%`)
      if (cpo < avgCpo * 0.6 && avgCpo > 0) highlights.push(`Дешёвый CPO`)
      if (c.ctr > 3) highlights.push(`Высокий CTR ${pct(c.ctr)}`)
      if (c.cart_conv > 30) highlights.push(`Конверсия в корзину ${pct(c.cart_conv)}`)

      return { campaign: c, romi, cpo, highlights }
    })
    .filter((g) => g.highlights.length > 0)
    .sort((a, b) => b.romi - a.romi)

  // Events summary
  const eventsSummary = Object.values(eventsByDay).reduce(
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
    problemCampaigns,
    goodCampaigns,
    totalWasted,
    totalSpend,
    wastePercent: totalSpend > 0 ? (totalWasted / totalSpend) * 100 : 0,
    avgCpc,
    avgDrr,
    avgCpo,
    totalOrders,
    totalRevenue,
    eventsSummary,
    activeCampaigns: active.length,
    campaignsWithOrders: withOrders.length,
    campaignsWithoutOrders: withoutOrders.filter((c) => c.spend > 0).length,
  }
}

/* ═══════════════════════════════════════════════════════════
   Section components
   ═══════════════════════════════════════════════════════════ */

function SectionHeader({
  icon,
  title,
  count,
  color,
  defaultOpen = true,
  children,
}: {
  icon: React.ReactNode
  title: string
  count?: number
  color: string
  defaultOpen?: boolean
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="rounded-xl border border-[hsl(var(--border)/0.5)] overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className={`w-full flex items-center justify-between px-4 py-3 text-left transition-colors hover:bg-[hsl(var(--muted)/0.15)]`}
      >
        <div className="flex items-center gap-2.5">
          <span className={color}>{icon}</span>
          <span className="text-[14px] font-semibold text-[hsl(var(--foreground))]">{title}</span>
          {count !== undefined && (
            <span
              className={`rounded-full px-2 py-0.5 text-[11px] font-semibold ${color} bg-current/10`}
              style={{ backgroundColor: 'currentcolor', opacity: 0.1 }}
            >
              <span style={{ opacity: 10 }}>{count}</span>
            </span>
          )}
          {count !== undefined && (
            <span className={`text-[12px] font-medium ${color}`}>{count}</span>
          )}
        </div>
        {open ? (
          <ChevronUp className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.5)]" />
        ) : (
          <ChevronDown className="h-4 w-4 text-[hsl(var(--muted-foreground)/0.5)]" />
        )}
      </button>
      {open && <div className="px-4 pb-4">{children}</div>}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main component
   ═══════════════════════════════════════════════════════════ */

export function CampaignInsights({
  campaigns,
  eventsByDay,
}: {
  campaigns: CampaignRow[]
  eventsByDay: Record<string, EventDaySummary>
}) {
  const analysis = useMemo(() => analyzeAll(campaigns, eventsByDay), [campaigns, eventsByDay])

  if (campaigns.length === 0) return null

  const {
    problemCampaigns,
    goodCampaigns,
    totalWasted,
    totalSpend,
    wastePercent,
    avgCpc,
    avgDrr,
    avgCpo,
    totalOrders,
    totalRevenue,
    eventsSummary,
    activeCampaigns,
    campaignsWithOrders,
    campaignsWithoutOrders,
  } = analysis

  return (
    <div className="space-y-4">
      {/* ── Title ── */}
      <div className="flex items-center gap-2.5">
        <Activity className="h-5 w-5 text-[hsl(var(--primary))]" />
        <h3 className="text-[16px] font-bold text-[hsl(var(--foreground))]">
          Анализ рекламных кампаний
        </h3>
      </div>

      {/* ── Summary bar ── */}
      <div className="rounded-xl border border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.08)] p-4">
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
          <div>
            <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.6)] mb-0.5">Кампаний</div>
            <div className="text-[15px] font-bold">{activeCampaigns}</div>
          </div>
          <div>
            <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.6)] mb-0.5">С заказами</div>
            <div className="text-[15px] font-bold text-emerald-400">{campaignsWithOrders}</div>
          </div>
          <div>
            <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.6)] mb-0.5">Без заказов</div>
            <div className="text-[15px] font-bold text-red-400">{campaignsWithoutOrders}</div>
          </div>
          <div>
            <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.6)] mb-0.5">Ср. CPC</div>
            <div className="text-[15px] font-bold">{fmtMoney(avgCpc)}</div>
          </div>
          <div>
            <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.6)] mb-0.5">Ср. ДРР</div>
            <div className="text-[15px] font-bold">{pct(avgDrr)}</div>
          </div>
          <div>
            <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.6)] mb-0.5">Ср. CPO</div>
            <div className="text-[15px] font-bold">{fmtMoney(avgCpo)}</div>
          </div>
        </div>

        {/* Text summary */}
        <div className="mt-3 pt-3 border-t border-[hsl(var(--border)/0.3)] text-[12px] leading-relaxed text-[hsl(var(--muted-foreground))]">
          <p>
            За период потрачено <strong className="text-[hsl(var(--foreground))]">{fmtMoney(totalSpend)}</strong> на рекламу,
            получено <strong className="text-[hsl(var(--foreground))]">{totalOrders} заказов</strong> на сумму{' '}
            <strong className="text-[hsl(var(--foreground))]">{fmtMoney(totalRevenue)}</strong>.
            {totalWasted > 0 && (
              <>
                {' '}Из них{' '}
                <strong className="text-red-400">{fmtMoney(totalWasted)} ({pct(wastePercent)})</strong>{' '}
                потрачено на кампании без заказов.
              </>
            )}
            {campaignsWithOrders > 0 && avgDrr > 0 && (
              <>
                {' '}Средний ДРР по кампаниям с заказами — <strong className="text-[hsl(var(--foreground))]">{pct(avgDrr)}</strong>.
              </>
            )}
          </p>
        </div>
      </div>

      {/* ── Problem campaigns ── */}
      {problemCampaigns.length > 0 && (
        <SectionHeader
          icon={<AlertCircle className="h-4.5 w-4.5" />}
          title="Проблемные кампании"
          count={problemCampaigns.length}
          color="text-red-400"
        >
          <div className="space-y-1.5">
            {/* Table header */}
            <div className="grid grid-cols-[1fr_100px_80px_70px_70px_70px] gap-2 px-2 py-1.5 text-[10px] font-medium text-[hsl(var(--muted-foreground)/0.5)] uppercase tracking-wider">
              <div>Кампания</div>
              <div className="text-right">Расход</div>
              <div className="text-right">Показы</div>
              <div className="text-right">Клики</div>
              <div className="text-right">Корзины</div>
              <div className="text-right">Заказы</div>
            </div>

            {problemCampaigns.map(({ campaign: c, problems, severity }) => (
              <div
                key={c.campaign_id}
                className={`rounded-lg border px-2 py-2.5 ${
                  severity === 'critical'
                    ? 'border-red-500/20 bg-red-500/[0.03]'
                    : 'border-amber-500/20 bg-amber-500/[0.03]'
                }`}
              >
                <div className="grid grid-cols-[1fr_100px_80px_70px_70px_70px] gap-2 items-center">
                  <div className="min-w-0">
                    <div className="flex items-center gap-1.5">
                      {severity === 'critical' ? (
                        <Ban className="h-3 w-3 shrink-0 text-red-400" />
                      ) : (
                        <AlertTriangle className="h-3 w-3 shrink-0 text-amber-400" />
                      )}
                      <span className="truncate text-[12px] font-medium text-[hsl(var(--foreground))]" title={c.title}>
                        {c.title}
                      </span>
                    </div>
                  </div>
                  <div className="text-right text-[12px] font-semibold text-[hsl(var(--foreground))]">
                    {fmtMoney(c.spend)}
                  </div>
                  <div className="text-right text-[12px] text-[hsl(var(--muted-foreground))]">
                    {fmt(c.views)}
                  </div>
                  <div className="text-right text-[12px] text-[hsl(var(--muted-foreground))]">
                    {fmt(c.clicks)}
                  </div>
                  <div className={`text-right text-[12px] ${c.cart === 0 ? 'text-red-400 font-medium' : 'text-[hsl(var(--muted-foreground))]'}`}>
                    {c.cart}
                  </div>
                  <div className={`text-right text-[12px] ${c.orders === 0 ? 'text-red-400 font-medium' : 'text-[hsl(var(--muted-foreground))]'}`}>
                    {c.orders}
                  </div>
                </div>
                {/* Problems list */}
                <div className="mt-1.5 ml-5 space-y-0.5">
                  {problems.map((p, i) => (
                    <div key={i} className="flex items-start gap-1.5">
                      <ArrowRight className="h-3 w-3 shrink-0 mt-0.5 text-[hsl(var(--muted-foreground)/0.35)]" />
                      <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.7)]">{p}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))}

            {/* Total wasted */}
            {totalWasted > 0 && (
              <div className="flex items-center justify-between rounded-lg bg-red-500/[0.06] px-3 py-2 mt-2">
                <span className="text-[12px] font-medium text-red-400">
                  Итого потрачено без заказов
                </span>
                <span className="text-[14px] font-bold text-red-400">{fmtMoney(totalWasted)}</span>
              </div>
            )}
          </div>
        </SectionHeader>
      )}

      {/* ── Good campaigns ── */}
      {goodCampaigns.length > 0 && (
        <SectionHeader
          icon={<CheckCircle2 className="h-4.5 w-4.5" />}
          title="Эффективные кампании"
          count={goodCampaigns.length}
          color="text-emerald-400"
        >
          <div className="space-y-1.5">
            {/* Table header */}
            <div className="grid grid-cols-[1fr_90px_80px_70px_80px_80px] gap-2 px-2 py-1.5 text-[10px] font-medium text-[hsl(var(--muted-foreground)/0.5)] uppercase tracking-wider">
              <div>Кампания</div>
              <div className="text-right">Расход</div>
              <div className="text-right">Заказы</div>
              <div className="text-right">ДРР</div>
              <div className="text-right">CPO</div>
              <div className="text-right">ROMI</div>
            </div>

            {goodCampaigns.map(({ campaign: c, romi, cpo, highlights }) => (
              <div
                key={c.campaign_id}
                className="rounded-lg border border-emerald-500/20 bg-emerald-500/[0.03] px-2 py-2.5"
              >
                <div className="grid grid-cols-[1fr_90px_80px_70px_80px_80px] gap-2 items-center">
                  <div className="min-w-0">
                    <div className="flex items-center gap-1.5">
                      <Trophy className="h-3 w-3 shrink-0 text-emerald-400" />
                      <span className="truncate text-[12px] font-medium text-[hsl(var(--foreground))]" title={c.title}>
                        {c.title}
                      </span>
                    </div>
                  </div>
                  <div className="text-right text-[12px] text-[hsl(var(--muted-foreground))]">
                    {fmtMoney(c.spend)}
                  </div>
                  <div className="text-right text-[12px] font-semibold text-emerald-400">
                    {c.orders}
                  </div>
                  <div className="text-right text-[12px] font-semibold text-emerald-400">
                    {pct(c.drr)}
                  </div>
                  <div className="text-right text-[12px] text-[hsl(var(--muted-foreground))]">
                    {fmtMoney(cpo)}
                  </div>
                  <div className="text-right text-[12px] font-semibold text-emerald-400">
                    {fmt(romi)}%
                  </div>
                </div>
                {/* Highlights */}
                <div className="mt-1.5 ml-5 flex flex-wrap gap-1">
                  {highlights.map((h, i) => (
                    <span
                      key={i}
                      className="inline-flex items-center gap-1 rounded-md bg-emerald-500/10 px-1.5 py-0.5 text-[10px] font-medium text-emerald-400"
                    >
                      <ThumbsUp className="h-2.5 w-2.5" />
                      {h}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </SectionHeader>
      )}

      {/* ── Events summary ── */}
      {eventsSummary.total > 0 && (
        <SectionHeader
          icon={<Zap className="h-4.5 w-4.5" />}
          title="Активность за период"
          count={eventsSummary.total}
          color="text-blue-400"
          defaultOpen={false}
        >
          <div className="space-y-3">
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              {eventsSummary.advertising > 0 && (
                <div className="rounded-lg border border-blue-500/15 bg-blue-500/[0.03] p-3">
                  <div className="flex items-center gap-1.5 mb-1">
                    <Target className="h-3.5 w-3.5 text-blue-400" />
                    <span className="text-[11px] text-[hsl(var(--muted-foreground))]">Рекламные</span>
                  </div>
                  <div className="text-[18px] font-bold text-blue-400">{eventsSummary.advertising}</div>
                  <div className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)] mt-0.5">
                    Смены ставок, статусов кампаний
                  </div>
                </div>
              )}
              {eventsSummary.content > 0 && (
                <div className="rounded-lg border border-purple-500/15 bg-purple-500/[0.03] p-3">
                  <div className="flex items-center gap-1.5 mb-1">
                    <BarChart2 className="h-3.5 w-3.5 text-purple-400" />
                    <span className="text-[11px] text-[hsl(var(--muted-foreground))]">Контент</span>
                  </div>
                  <div className="text-[18px] font-bold text-purple-400">{eventsSummary.content}</div>
                  <div className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)] mt-0.5">
                    Изменения карточек товаров
                  </div>
                </div>
              )}
              {eventsSummary.price > 0 && (
                <div className="rounded-lg border border-amber-500/15 bg-amber-500/[0.03] p-3">
                  <div className="flex items-center gap-1.5 mb-1">
                    <DollarSign className="h-3.5 w-3.5 text-amber-400" />
                    <span className="text-[11px] text-[hsl(var(--muted-foreground))]">Ценовые</span>
                  </div>
                  <div className="text-[18px] font-bold text-amber-400">{eventsSummary.price}</div>
                  <div className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)] mt-0.5">
                    Изменения цен на товары
                  </div>
                </div>
              )}
              {eventsSummary.stock > 0 && (
                <div className="rounded-lg border border-cyan-500/15 bg-cyan-500/[0.03] p-3">
                  <div className="flex items-center gap-1.5 mb-1">
                    <ShoppingCart className="h-3.5 w-3.5 text-cyan-400" />
                    <span className="text-[11px] text-[hsl(var(--muted-foreground))]">Складские</span>
                  </div>
                  <div className="text-[18px] font-bold text-cyan-400">{eventsSummary.stock}</div>
                  <div className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)] mt-0.5">
                    Изменения остатков
                  </div>
                </div>
              )}
            </div>

            <p className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">
              Наведите на маркеры 📌 на графике выше, чтобы увидеть детали каждого события.
            </p>
          </div>
        </SectionHeader>
      )}

      {/* ── Recommendations ── */}
      <SectionHeader
        icon={<Info className="h-4.5 w-4.5" />}
        title="Рекомендации"
        color="text-[hsl(var(--primary))]"
        defaultOpen={true}
      >
        <div className="space-y-2">
          {problemCampaigns.filter((p) => p.severity === 'critical').length > 0 && (
            <RecommendationRow
              severity="critical"
              title="Отключите убыточные кампании"
              text={`${problemCampaigns.filter((p) => p.campaign.orders === 0 && p.campaign.spend > 500).length} кампаний тратят бюджет без заказов. Отключите их или снизьте ставки до минимума. Экономия: до ${fmtMoney(totalWasted)} за период.`}
            />
          )}
          {problemCampaigns.filter((p) => p.campaign.cart === 0 && p.campaign.orders === 0).length > 0 && (
            <RecommendationRow
              severity="warning"
              title="Проверьте карточки товаров"
              text="Кампании без корзин — проблема не в рекламе, а в самом товаре. Проверьте фото, цену, отзывы и описание."
            />
          )}
          {problemCampaigns.filter((p) => p.campaign.drr > 100).length > 0 && (
            <RecommendationRow
              severity="warning"
              title="Снизьте ставки на убыточных"
              text={`Кампании с ДРР > 100% тратят больше, чем приносят выручки. Снизьте ставку за клик.`}
            />
          )}
          {goodCampaigns.length > 0 && avgCpo > 0 && (
            <RecommendationRow
              severity="success"
              title="Масштабируйте эффективные"
              text={`${goodCampaigns.length} кампаний показывают хороший ROMI. Рассмотрите увеличение бюджета на лучших — они окупаются.`}
            />
          )}
          {wastePercent > 30 && totalSpend > 3000 && (
            <RecommendationRow
              severity="critical"
              title={`${pct(wastePercent)} бюджета — без результата`}
              text={`${fmtMoney(totalWasted)} из ${fmtMoney(totalSpend)} потрачено на кампании без единого заказа. Перераспределите бюджет на эффективные кампании.`}
            />
          )}
          {eventsSummary.total > 50 && (
            <RecommendationRow
              severity="info"
              title="Высокая активность изменений"
              text={`${eventsSummary.total} событий за период. Частые изменения ставок и контента могут дестабилизировать алгоритмы Ozon. Дайте кампаниям 2-3 дня без изменений для набора статистики.`}
            />
          )}
        </div>
      </SectionHeader>
    </div>
  )
}

function RecommendationRow({
  severity,
  title,
  text,
}: {
  severity: 'critical' | 'warning' | 'success' | 'info'
  title: string
  text: string
}) {
  const cfg = {
    critical: {
      icon: <AlertTriangle className="h-3.5 w-3.5" />,
      color: 'text-red-400',
      bg: 'bg-red-500/[0.04]',
      border: 'border-red-500/15',
    },
    warning: {
      icon: <AlertTriangle className="h-3.5 w-3.5" />,
      color: 'text-amber-400',
      bg: 'bg-amber-500/[0.04]',
      border: 'border-amber-500/15',
    },
    success: {
      icon: <TrendingUp className="h-3.5 w-3.5" />,
      color: 'text-emerald-400',
      bg: 'bg-emerald-500/[0.04]',
      border: 'border-emerald-500/15',
    },
    info: {
      icon: <Info className="h-3.5 w-3.5" />,
      color: 'text-blue-400',
      bg: 'bg-blue-500/[0.04]',
      border: 'border-blue-500/15',
    },
  }[severity]

  return (
    <div className={`rounded-lg border ${cfg.border} ${cfg.bg} px-3 py-2.5`}>
      <div className="flex items-start gap-2">
        <span className={`mt-0.5 shrink-0 ${cfg.color}`}>{cfg.icon}</span>
        <div>
          <div className="text-[12px] font-semibold text-[hsl(var(--foreground))]">{title}</div>
          <div className="mt-0.5 text-[11px] leading-relaxed text-[hsl(var(--muted-foreground)/0.7)]">
            {text}
          </div>
        </div>
      </div>
    </div>
  )
}
