/**
 * Warehouses Storage — расходы + хранение по SKU.
 * WB: CostsSummary + StorageSkusTable + RecommendationsPanel
 * Ozon: redirects to /warehouses/analytics (storage tab).
 */
import React, { useState, useEffect, useCallback } from 'react'
import { Navigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  RefreshCw,
  AlertTriangle,
  Package,
  ChevronRight,
  ShieldAlert,
  Boxes,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBWarehouseAnalytics,
  type WBWarehouseAnalyticsResponse,
  type WBCostSummary,
  type WBStorageSku,
  type WBRecommendation,
} from '@/api/warehouses'

/* ── Cost extraction helper ── */
function extractCosts(costs: WBCostSummary[]) {
  const find = (type: string) => costs.find(c => c.operation_type === type)?.amount ?? 0
  return {
    storage: Math.abs(find('Хранение')),
    logistics: Math.abs(find('Логистика')),
    crossdocking: Math.abs(find('Кросс-докинг')),
    penalties: Math.abs(find('Штраф')),
  }
}

/* ── Helpers ── */
function fmt(v: number): string { return Math.round(v).toLocaleString('ru-RU') }
function fmtM(v: number): string { return Math.round(v).toLocaleString('ru-RU') + ' ₽' }

/* ── Period Options ── */
const PERIOD_OPTIONS = [
  { label: '7 дн', value: 7 },
  { label: '14 дн', value: 14 },
  { label: '30 дн', value: 30 },
  { label: '60 дн', value: 60 },
  { label: '90 дн', value: 90 },
]

/* ═══ Costs Summary ═══ */
function CostsSummary({ costs }: { costs: WBCostSummary[] }) {
  const c = extractCosts(costs)
  const items = [
    { label: '📦 Хранение', value: c.storage, color: 'text-purple-400' },
    { label: '🚚 Логистика', value: c.logistics, color: 'text-cyan-400' },
    { label: '🔄 Кросс-лог.', value: c.crossdocking || 0, color: 'text-orange-400' },
    { label: '⚠️ Штрафы', value: c.penalties, color: 'text-red-400' },
  ].filter(x => x.value > 0 || x.label === '📦 Хранение')

  const total = items.reduce((s, x) => s + Math.abs(x.value), 0)

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.15, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
            <Boxes className="h-5 w-5 text-purple-400" />
            Расходы за период
          </h2>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            {items.map((item) => (
              <div key={item.label} className="p-4 rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.2)]">
                <p className="text-[13px] font-medium text-[hsl(var(--muted-foreground))]">{item.label}</p>
                <p className={`text-xl font-bold tabular-nums mt-1 ${item.color}`}>
                  {fmtM(Math.abs(item.value))}
                </p>
              </div>
            ))}
          </div>
          <div className="mt-4 pt-4 border-t border-[hsl(var(--border)/0.2)] flex items-center justify-between">
            <span className="text-[14px] font-semibold text-[hsl(var(--muted-foreground))]">Итого расходов</span>
            <span className="text-xl font-bold text-red-400 tabular-nums">{fmtM(total)}</span>
          </div>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══ Storage SKUs Table ═══ */
function StorageSkusTable({ skus }: { skus: WBStorageSku[] }) {
  const [expanded, setExpanded] = useState<string | null>(null)

  const paidCount = skus.filter(s => s.zone === 'paid').length
  const warningCount = skus.filter(s => s.zone === 'warning').length
  const totalMonthlyCost = skus.filter(s => s.zone === 'paid').reduce((s, sk) => s + sk.est_monthly_cost, 0)

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-gradient-to-br from-orange-600 to-red-500 shadow-lg">
              <Package className="h-4.5 w-4.5 text-white" />
            </div>
            <div>
              <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Хранение по SKU</h2>
              <p className="text-[12px] text-[hsl(var(--muted-foreground))]">
                {paidCount} SKU в зоне платного хранения{warningCount > 0 && ` • ${warningCount} приближаются`}
              </p>
            </div>
          </div>
          {totalMonthlyCost > 0 && (
            <div className="text-right">
              <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">Прогноз / мес</div>
              <div className="text-xl font-bold text-red-400 tabular-nums">~{fmtM(totalMonthlyCost)}</div>
            </div>
          )}
        </div>

        <div className="overflow-auto max-h-[600px]">
          <table className="w-full border-collapse">
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-1 py-2.5 w-8"></th>
                <th className="px-3 py-2.5 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">SKU</th>
                <th className="px-2 py-2.5 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Зона</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Остаток</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Прод/д</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Оборач.</th>
                <th className="px-2 py-2.5 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">Реклама</th>
                <th className="px-3 py-2.5 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap">~Стоим/мес</th>
                <th className="px-3 py-2.5 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Рекомендация</th>
              </tr>
            </thead>
            <tbody>
              {skus.map((sk, idx) => {
                const isExp = expanded === sk.vendor_code
                const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.06)]'
                return (
                  <React.Fragment key={sk.vendor_code}>
                    <tr
                      className={`border-b border-[hsl(var(--border)/0.2)] transition-colors cursor-pointer ${
                        isExp ? 'bg-[hsl(var(--primary)/0.06)]' : `${rowBg} hover:bg-[hsl(var(--muted)/0.15)]`
                      } group`}
                      onClick={() => setExpanded(isExp ? null : sk.vendor_code)}
                    >
                      <td className="px-2 py-2.5 text-center">
                        <motion.div animate={{ rotate: isExp ? 90 : 0 }} transition={{ duration: 0.15 }}>
                          <ChevronRight className="h-3.5 w-3.5 text-[hsl(var(--muted-foreground))]" />
                        </motion.div>
                      </td>
                      <td className="px-3 py-2.5">
                        <div className="text-[13px] font-medium" title={sk.name}>{sk.name || sk.vendor_code}</div>
                        <div className="text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">{sk.vendor_code}</div>
                      </td>
                      <td className="px-2 py-2.5 text-center">
                        <span className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-bold whitespace-nowrap ${
                          sk.zone === 'paid'
                            ? 'bg-red-500/15 text-red-400'
                            : 'bg-amber-500/15 text-amber-400'
                        }`}>
                          {sk.zone === 'paid' ? 'Платное' : 'Скоро'}
                        </span>
                      </td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[13px] font-semibold">{fmt(sk.total_stock)}</td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[13px]">{sk.daily_sales.toFixed(1)}</td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[13px]">
                        <span className={`font-semibold ${
                          sk.turnover_days == null ? 'text-purple-400' :
                          sk.turnover_days > 160 ? 'text-red-400' :
                          'text-orange-400'
                        }`}>
                          {sk.turnover_days != null ? `${Math.round(sk.turnover_days)} дн` : '∞'}
                        </span>
                      </td>
                      <td className="px-2 py-2.5 text-center">
                        {sk.ad_info?.has_ads ? (
                          <span className="inline-block rounded px-1.5 py-0.5 text-[10px] font-bold bg-blue-500/15 text-blue-400"
                                title={`Расход: ${fmtM(sk.ad_info.spend_30d)} за 30д, заказов: ${sk.ad_info.orders_30d}`}>
                            Да
                          </span>
                        ) : (
                          <span className="text-[10px] text-[hsl(var(--muted-foreground))]">Нет</span>
                        )}
                      </td>
                      <td className="px-3 py-2.5 text-right tabular-nums text-[13px]">
                        <span className={`font-bold ${sk.zone === 'paid' ? 'text-red-400' : 'text-amber-400'}`}>
                          {sk.est_monthly_cost > 0 ? `~${fmtM(sk.est_monthly_cost)}` : '—'}
                        </span>
                      </td>
                      <td className="px-3 py-2.5">
                        {sk.recommendation && (
                          <div title={sk.recommendation.reason}>
                            <div className={`text-[12px] font-semibold ${
                              sk.recommendation.severity === 'critical' ? 'text-red-500' :
                              sk.recommendation.severity === 'high' ? 'text-orange-500' :
                              'text-[hsl(var(--foreground)/0.7)]'
                            }`}>{sk.recommendation.action}</div>
                            <div className="text-[11px] text-[hsl(var(--muted-foreground))]">{sk.recommendation.reason}</div>
                          </div>
                        )}
                      </td>
                    </tr>
                    {/* Expanded: warehouses */}
                    {isExp && (
                      <tr>
                        <td colSpan={9} className="p-0">
                          <div className="bg-[hsl(var(--muted)/0.06)] border-t border-b border-[hsl(var(--border)/0.3)] px-5 py-4">
                            <div className="flex items-center gap-4 mb-3">
                              <h4 className="text-[13px] font-semibold text-[hsl(var(--foreground))]">
                                Распределение по складам ({sk.warehouses.length})
                              </h4>
                              {sk.revenue_period > 0 && (
                                <span className="text-[12px] text-[hsl(var(--muted-foreground))]">
                                  Выручка за период: <strong className="text-emerald-400">{fmtM(sk.revenue_period)}</strong>
                                </span>
                              )}
                            </div>
                            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-2">
                              {sk.warehouses.map(wh => (
                                <div
                                  key={wh.warehouse_name}
                                  className="flex items-center justify-between p-2.5 rounded-lg bg-[hsl(var(--card))] border border-[hsl(var(--border)/0.2)]"
                                >
                                  <div className="min-w-0">
                                    <div className="text-[11px] font-medium truncate" title={wh.warehouse_name}>
                                      {wh.warehouse_name}
                                    </div>
                                  </div>
                                  <span className="text-[13px] font-bold tabular-nums ml-2 shrink-0">{wh.stock}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        </div>

        <div className="flex items-center justify-between px-6 py-4 border-t border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.05)]">
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            SKU в зоне риска: <strong className="text-red-400">{paidCount} платных</strong>
            {warningCount > 0 && <>, <strong className="text-amber-400">{warningCount} приближаются</strong></>}
          </span>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══ Recommendations Panel ═══ */
function RecommendationsPanel({ recommendations }: { recommendations: WBRecommendation[] }) {
  const [expandedIdx, setExpandedIdx] = useState<number | null>(0)

  if (recommendations.length === 0) return null

  const severityColor: Record<string, string> = {
    critical: 'text-red-400 bg-red-500/10 border-red-500/20',
    high: 'text-orange-400 bg-orange-500/10 border-orange-500/20',
    medium: 'text-amber-400 bg-amber-500/10 border-amber-500/20',
    low: 'text-blue-400 bg-blue-500/10 border-blue-500/20',
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.25, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="flex items-center gap-3 px-6 py-5 border-b border-[hsl(var(--border))]">
          <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-gradient-to-br from-amber-600 to-orange-500 shadow-lg">
            <ShieldAlert className="h-4.5 w-4.5 text-white" />
          </div>
          <div>
            <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Рекомендации ({recommendations.length})</h2>
            <p className="text-[12px] text-[hsl(var(--muted-foreground))]">
              {recommendations.filter(r => r.severity === 'critical' || r.severity === 'high').length} важных
            </p>
          </div>
        </div>

        <div className="p-4 space-y-3">
          {recommendations.map((rec, idx) => {
            const isOpen = expandedIdx === idx
            const colors = severityColor[rec.severity] || severityColor.medium

            return (
              <div key={idx} className={`rounded-xl border overflow-hidden ${colors.split(' ').slice(2).join(' ')}`}>
                <div
                  className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors ${colors.split(' ').slice(0, 2).join(' ')} hover:opacity-80`}
                  onClick={() => setExpandedIdx(isOpen ? null : idx)}
                >
                  <motion.div animate={{ rotate: isOpen ? 90 : 0 }} transition={{ duration: 0.15 }}>
                    <ChevronRight className="h-4 w-4" />
                  </motion.div>
                  <span className="text-[13px] font-semibold flex-1">{rec.title}</span>
                  <span className={`text-[10px] font-bold rounded-full px-2 py-0.5 ${colors.split(' ').slice(0, 1).join(' ')}`}>
                    {rec.severity === 'critical' ? 'Критично' : rec.severity === 'high' ? 'Важно' : 'Совет'}
                  </span>
                </div>
                <AnimatePresence>
                  {isOpen && (
                    <motion.div
                      initial={{ height: 0, opacity: 0 }}
                      animate={{ height: 'auto', opacity: 1 }}
                      exit={{ height: 0, opacity: 0 }}
                      transition={{ duration: 0.2 }}
                      className="overflow-hidden"
                    >
                      <div className="px-5 py-4 space-y-3 border-t border-[hsl(var(--border)/0.2)]">
                        <p className="text-[13px] text-[hsl(var(--foreground))] leading-relaxed">{rec.reason}</p>
                        {rec.action_items && rec.action_items.length > 0 && (
                          <div>
                            <h5 className="text-[11px] font-bold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1.5">Что делать</h5>
                            <ul className="space-y-1">
                              {rec.action_items.map((item, i) => (
                                <li key={i} className="flex items-start gap-2 text-[13px] text-[hsl(var(--foreground))]">
                                  <span className="text-[hsl(var(--primary))] font-bold mt-0.5">→</span>
                                  <span>{item}</span>
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}
                        {rec.affected_skus && rec.affected_skus.length > 0 && (
                          <div>
                            <h5 className="text-[11px] font-bold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1.5">Затронутые товары</h5>
                            <div className="flex flex-wrap gap-1.5">
                              {rec.affected_skus.map((sku, i) => (
                                <span key={i} className="inline-flex items-center px-2.5 py-1 rounded-lg bg-[hsl(var(--muted)/0.15)] text-[11px] font-medium border border-[hsl(var(--border)/0.2)]">
                                  {sku}
                                </span>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>
            )
          })}
        </div>
      </div>
    </motion.div>
  )
}

/* ═══ Skeleton ═══ */
function StorageSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-[100px] rounded-2xl" />
        ))}
      </div>
      <Skeleton className="h-[400px] rounded-2xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function WarehousesStoragePage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isWB = currentShop?.marketplace === 'wildberries'
  const isOzon = currentShop?.marketplace === 'ozon'

  const [data, setData] = useState<WBWarehouseAnalyticsResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState(30)

  const fetchData = useCallback(async () => {
    if (!currentShop || !isWB) return
    setLoading(true)
    setError(null)
    try {
      const result = await getWBWarehouseAnalytics({ shop_id: currentShop.id, period })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [currentShop, isWB, period])

  useEffect(() => { if (isWB) fetchData() }, [fetchData, isWB])

  if (isOzon) return <Navigate to="/warehouses/analytics" replace />

  const periodSelCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[13px] font-medium transition-all cursor-pointer ${
      active
        ? 'bg-[hsl(var(--primary))] text-white shadow-md'
        : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
    }`

  return (
    <div className="space-y-6 pb-10">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Хранение</h1>
          <p className="text-[hsl(var(--muted-foreground))] mt-1">
            Расходы на хранение, платное хранение по SKU и рекомендации
          </p>
        </div>
        <button
          onClick={fetchData}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all disabled:opacity-50"
        >
          <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          Обновить
        </button>
      </div>

      {/* Period */}
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center gap-3">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Период</p>
              <div className="flex gap-1">
                {PERIOD_OPTIONS.map(o => (
                  <button key={o.value} className={periodSelCls(period === o.value)} onClick={() => setPeriod(o.value)}>
                    {o.label}
                  </button>
                ))}
              </div>
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {loading && !data ? (
        <StorageSkeleton />
      ) : error ? (
        <Card>
          <CardContent className="p-10 text-center">
            <AlertTriangle className="h-10 w-10 mx-auto text-red-400 mb-3" />
            <p className="text-lg font-medium text-red-400">{error}</p>
            <button onClick={fetchData} className="mt-4 px-4 py-2 rounded-xl bg-[hsl(var(--primary))] text-white text-sm font-medium">
              Попробовать снова
            </button>
          </CardContent>
        </Card>
      ) : data ? (
        <>
          {/* Costs Summary */}
          <CostsSummary costs={data.costs} />

          {/* Storage SKUs */}
          {data.storage_skus.length > 0 && <StorageSkusTable skus={data.storage_skus} />}

          {/* Recommendations */}
          <RecommendationsPanel recommendations={data.recommendations} />
        </>
      ) : null}
    </div>
  )
}
