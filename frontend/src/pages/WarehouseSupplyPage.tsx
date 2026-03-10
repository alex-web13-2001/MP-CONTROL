import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { motion } from 'framer-motion'
import {
  Package,
  AlertTriangle,
  AlertCircle,
  BarChart3,
  Download,
  RefreshCw,
  ChevronRight,
  Megaphone,
  TrendingUp,
  MapPin,
  Warehouse,
  ListTree,
  Wallet,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getOzonSupplyApi,
  downloadSupplyExcel,
  SupplyResponse,
  SupplyItem,
  SupplyCluster,
  HubSummary,
  getWBSupplyApi,
  downloadWBSupplyExcel,
  WBSupplyResponse,
  WBSupplyItem,
  WBWarehouseDetail,
  WBWarehouseSummary,
} from '@/api/warehouses'


/* ═══════════════════════════════════════════════════════════
   Helpers
   ═══════════════════════════════════════════════════════════ */

function formatNumber(v: number): string {
  return Math.round(v).toLocaleString('ru-RU')
}
function formatMoney(v: number): string {
  return Math.round(v).toLocaleString('ru-RU') + ' ₽'
}

/* ═══════════════════════════════════════════════════════════
   KPI Card
   ═══════════════════════════════════════════════════════════ */

function KpiCard({
  title, value, subtitle, icon: Icon, accent, delay,
}: {
  title: string; value: string; subtitle?: string
  icon: React.ElementType; accent: string; delay: number
}) {
  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay, duration: 0.4 }}>
      <Card className="relative overflow-hidden h-full">
        <CardContent className="p-5 flex flex-col justify-between h-full">
          <div className="flex items-start justify-between">
            <div className="space-y-1 min-w-0">
              <p className="text-[13px] font-medium text-[hsl(var(--muted-foreground))]">{title}</p>
              <p className="text-2xl font-bold tracking-tight">{value}</p>
            </div>
            <div className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br ${accent} shadow-lg`}>
              <Icon className="h-5 w-5 text-white" />
            </div>
          </div>
          {subtitle && (
            <div className="mt-3 min-h-[24px]">
              <span className="text-[12px] text-[hsl(var(--muted-foreground))]">{subtitle}</span>
            </div>
          )}
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Settings Panel
   ═══════════════════════════════════════════════════════════ */

const TARGET_OPTIONS = [
  { value: 14, label: '14 дней' },
  { value: 30, label: '30 дней' },
  { value: 45, label: '45 дней' },
  { value: 60, label: '60 дней' },
  { value: 90, label: '90 дней' },
]

const PERIOD_OPTIONS = [
  { value: 14, label: '14 дней' },
  { value: 30, label: '30 дней' },
  { value: 60, label: '60 дней' },
  { value: 90, label: '90 дней' },
]

const SAFETY_OPTIONS = [
  { value: 1.0, label: '0%' },
  { value: 1.1, label: '10%' },
  { value: 1.15, label: '15%' },
  { value: 1.2, label: '20%' },
  { value: 1.3, label: '30%' },
]

function SettingsPanel({
  targetDays, setTargetDays,
  salesPeriod, setSalesPeriod,
  safety, setSafety,
  useAdBoost, setUseAdBoost,
  loading, onRefresh, onExport, exporting,
}: {
  targetDays: number; setTargetDays: (v: number) => void
  salesPeriod: number; setSalesPeriod: (v: number) => void
  safety: number; setSafety: (v: number) => void
  useAdBoost: boolean; setUseAdBoost: (v: boolean) => void
  loading: boolean; onRefresh: () => void
  onExport: () => void; exporting: boolean
}) {
  const selCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[13px] font-medium transition-all cursor-pointer ${
      active
        ? 'bg-[hsl(var(--primary))] text-white shadow-md'
        : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
    }`

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.1, duration: 0.4 }}
    >
      <Card>
        <CardContent className="p-5">
          <div className="flex flex-wrap items-center gap-6">
            {/* Target */}
            <div className="space-y-1.5">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Горизонт поставки</p>
              <div className="flex gap-1">
                {TARGET_OPTIONS.map(o => (
                  <button key={o.value} className={selCls(targetDays === o.value)} onClick={() => setTargetDays(o.value)}>
                    {o.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Sales period */}
            <div className="space-y-1.5">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Период для анализа продаж</p>
              <div className="flex gap-1">
                {PERIOD_OPTIONS.map(o => (
                  <button key={o.value} className={selCls(salesPeriod === o.value)} onClick={() => setSalesPeriod(o.value)}>
                    {o.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Safety */}
            <div className="space-y-1.5">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Страховой буфер</p>
              <div className="flex gap-1">
                {SAFETY_OPTIONS.map(o => (
                  <button key={o.value} className={selCls(safety === o.value)} onClick={() => setSafety(o.value)}>
                    {o.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Ad boost */}
            <div className="space-y-1.5">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Реклама</p>
              <button
                className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-[13px] font-medium transition-all ${
                  useAdBoost
                    ? 'bg-orange-500/15 text-orange-400 border border-orange-500/30'
                    : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))]'
                }`}
                onClick={() => setUseAdBoost(!useAdBoost)}
              >
                <Megaphone className="h-3.5 w-3.5" />
                {useAdBoost ? 'Ad Boost ВКЛ' : 'Ad Boost ВЫКЛ'}
              </button>
            </div>

            {/* Actions — push right */}
            <div className="ml-auto flex items-center gap-2">
              <button
                onClick={onRefresh}
                disabled={loading}
                className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all disabled:opacity-50"
              >
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
                Обновить
              </button>
              <button
                onClick={onExport}
                disabled={exporting || loading}
                className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-semibold bg-gradient-to-r from-emerald-600 to-emerald-500 text-white shadow-lg shadow-emerald-500/25 hover:shadow-emerald-500/40 transition-all disabled:opacity-50"
              >
                <Download className="h-4 w-4" />
                {exporting ? 'Генерация...' : 'Скачать Excel'}
              </button>
            </div>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Cluster Detail Row
   ═══════════════════════════════════════════════════════════ */

function ClusterTable({ clusters }: { clusters: SupplyCluster[] }) {
  return (
    <tr>
      <td colSpan={10} className="p-0">
        <div className="bg-[hsl(var(--muted)/0.08)] border-t border-b border-[hsl(var(--border)/0.3)] px-6 py-3">
          <table className="w-full text-[13px]">
            <thead>
              <tr className="text-[hsl(var(--muted-foreground))] text-[11px] uppercase tracking-wider">
                <th className="text-left py-1.5 font-semibold">Кластер доставки</th>
                <th className="text-right py-1.5 font-semibold">Продано</th>
                <th className="text-right py-1.5 font-semibold">Доля</th>
                <th className="text-right py-1.5 font-semibold">Ежедн.</th>
                <th className="text-right py-1.5 font-semibold">Ежедн.×boost</th>
                <th className="text-right py-1.5 font-semibold">Оц. стока</th>
                <th className="text-right py-1.5 font-semibold">Выручка</th>
                <th className="text-right py-1.5 font-semibold w-[100px]">ПОСТАВИТЬ</th>
              </tr>
            </thead>
            <tbody>
              {clusters.map((cl) => (
                <tr key={cl.cluster} className="border-t border-[hsl(var(--border)/0.15)] hover:bg-[hsl(var(--muted)/0.1)] transition-colors">
                  <td className="py-2 text-left">
                    <span className="flex items-center gap-1.5">
                      <MapPin className="h-3 w-3 text-[hsl(var(--muted-foreground))] shrink-0" />
                      {cl.cluster}
                    </span>
                  </td>
                  <td className="py-2 text-right tabular-nums">{cl.sold}</td>
                  <td className="py-2 text-right tabular-nums text-[hsl(var(--muted-foreground))]">{cl.share}%</td>
                  <td className="py-2 text-right tabular-nums">{cl.daily.toFixed(2)}</td>
                  <td className="py-2 text-right tabular-nums">{cl.daily_boosted.toFixed(2)}</td>
                  <td className="py-2 text-right tabular-nums">{cl.est_stock}</td>
                  <td className="py-2 text-right tabular-nums">{formatMoney(cl.revenue)}</td>
                  <td className="py-2 text-right tabular-nums font-bold">
                    {cl.need > 0 ? (
                      <span className="inline-flex items-center gap-1 rounded-full bg-red-500/15 text-red-400 px-2.5 py-0.5 text-[13px] font-bold">
                        {cl.need}
                      </span>
                    ) : (
                      <span className="text-emerald-400">✓</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </td>
    </tr>
  )
}

/* ═══════════════════════════════════════════════════════════
   Status Badge
   ═══════════════════════════════════════════════════════════ */

function StatusBadge({ status }: { status: SupplyItem['status'] }) {
  const cfg = {
    critical: { label: 'Критич.', cls: 'bg-red-500/15 text-red-400' },
    attention: { label: 'Внимание', cls: 'bg-amber-500/15 text-amber-400' },
    ok: { label: 'Норма', cls: 'bg-emerald-500/15 text-emerald-400' },
  }[status]
  return (
    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-[11px] font-semibold ${cfg.cls}`}>
      {cfg.label}
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   Supply Table
   ═══════════════════════════════════════════════════════════ */

type SortKey = 'days_supply' | 'sold' | 'fbo_stock' | 'total_need' | 'revenue' | 'boost'
type SortDir = 'asc' | 'desc'

function SupplyTable({ items }: { items: SupplyItem[] }) {
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [hoveredImg, setHoveredImg] = useState<{ url: string; x: number; y: number } | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('days_supply')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const toggleSort = (key: SortKey) => {
    if (key === sortKey) {
      setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    } else {
      setSortKey(key)
      setSortDir(key === 'days_supply' ? 'asc' : 'desc')
    }
  }

  const sorted = useMemo(() => {
    return [...items].sort((a, b) => {
      const av = a[sortKey] as number
      const bv = b[sortKey] as number
      return sortDir === 'desc' ? bv - av : av - bv
    })
  }, [items, sortKey, sortDir])

  const thBase = "px-4 py-3.5 text-right text-[13px] font-semibold whitespace-nowrap select-none"
  const tdCls = "px-4 py-3 text-right tabular-nums text-[13px] whitespace-nowrap"

  const SortTh = ({ k, children, className = '' }: { k: SortKey; children: React.ReactNode; className?: string }) => (
    <th
      className={`${thBase} ${className} cursor-pointer transition-colors ${
        sortKey === k ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
      }`}
      onClick={() => toggleSort(k)}
    >
      <span className="inline-flex items-center gap-1 justify-end">
        {children}
        {sortKey === k && (
          <span className="text-[11px] text-[hsl(var(--primary))]">{sortDir === 'desc' ? '▼' : '▲'}</span>
        )}
      </span>
    </th>
  )

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.3, duration: 0.4 }}
    >
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        {/* Title bar */}
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Рекомендации поставки по SKU</h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">
            Нажмите на строку для просмотра кластеров
          </span>
        </div>

        <div className="overflow-auto max-h-[700px]">
          <table className="w-full border-collapse" style={{ minWidth: 900 }}>
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-4 py-3.5 text-center text-[13px] font-semibold text-[hsl(var(--muted-foreground))] w-[40px]"></th>
                <th className="px-4 py-3.5 text-left text-[13px] font-semibold text-[hsl(var(--muted-foreground))] w-[280px] min-w-[280px]">Товар</th>
                <th className={`${thBase} text-center`}>Статус</th>
                <SortTh k="sold">Продано</SortTh>
                <SortTh k="fbo_stock">FBO</SortTh>
                <SortTh k="days_supply">Дн.зап.</SortTh>
                <SortTh k="boost">Boost</SortTh>
                <SortTh k="total_need">ПОСТАВИТЬ</SortTh>
                <SortTh k="revenue">Выручка</SortTh>
                <th className={`${thBase} text-center`}>Класт.</th>
              </tr>
            </thead>
            <tbody>
                {sorted.map((item, idx) => {
                  const isExpanded = expandedId === item.offer_id
                  const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.06)]'
                  return (
                    <React.Fragment key={item.offer_id}>
                      <tr
                        className={`border-b border-[hsl(var(--border)/0.2)] transition-colors cursor-pointer ${
                          isExpanded ? 'bg-[hsl(var(--primary)/0.06)]' : `${rowBg} hover:bg-[hsl(var(--muted)/0.15)]`
                        } group`}
                        onClick={() => setExpandedId(isExpanded ? null : item.offer_id)}
                      >
                        {/* Expand arrow */}
                        <td className="px-3 py-3 text-center">
                          <motion.div animate={{ rotate: isExpanded ? 90 : 0 }} transition={{ duration: 0.15 }}>
                            <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                          </motion.div>
                        </td>

                        {/* Product info */}
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-2.5">
                            {item.image_url ? (
                              <div
                                className="relative shrink-0"
                                onMouseEnter={(e) => {
                                  const rect = (e.target as HTMLElement).getBoundingClientRect()
                                  setHoveredImg({ url: item.image_url, x: rect.right + 8, y: rect.top })
                                }}
                                onMouseLeave={() => setHoveredImg(null)}
                              >
                                <img src={item.image_url} alt="" className="w-9 h-9 rounded-lg object-cover border border-[hsl(var(--border)/0.3)]" loading="lazy" />
                              </div>
                            ) : (
                              <div className="w-9 h-9 rounded-lg bg-[hsl(var(--muted)/0.3)] shrink-0 flex items-center justify-center text-sm">📦</div>
                            )}
                            <div className="min-w-0 flex-1">
                              <div className="text-[13px] font-medium text-[hsl(var(--foreground))] truncate" title={item.name}>{item.name}</div>
                              <div className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-60 truncate">{item.offer_id}</div>
                            </div>
                          </div>
                        </td>

                        {/* Status */}
                        <td className="px-4 py-3 text-center"><StatusBadge status={item.status} /></td>

                        {/* Sold */}
                        <td className={`${tdCls} font-semibold`}>{formatNumber(item.sold)}</td>

                        {/* FBO */}
                        <td className={tdCls}>
                          <span className={item.fbo_stock === 0 ? 'text-red-400 font-bold' : ''}>
                            {formatNumber(item.fbo_stock)}
                          </span>
                        </td>

                        {/* Days supply */}
                        <td className={tdCls}>
                          <span className={`font-semibold ${
                            item.days_supply < 14 ? 'text-red-400' :
                            item.days_supply < 30 ? 'text-amber-400' :
                            item.days_supply < 60 ? 'text-yellow-400' :
                            'text-emerald-400'
                          }`}>
                            {item.days_supply > 999 ? '999+' : item.days_supply.toFixed(0)}
                          </span>
                        </td>

                        {/* Boost */}
                        <td className={tdCls}>
                          {item.boost > 1 ? (
                            <span className="inline-flex items-center gap-0.5 rounded-full bg-orange-500/15 text-orange-400 px-2 py-0.5 text-[12px] font-bold">
                              <TrendingUp className="h-3 w-3" />
                              ×{item.boost.toFixed(1)}
                            </span>
                          ) : (
                            <span className="text-[hsl(var(--muted-foreground))] opacity-40">—</span>
                          )}
                        </td>

                        {/* Total need */}
                        <td className={tdCls}>
                          {item.total_need > 0 ? (
                            <span className="inline-flex items-center justify-center min-w-[40px] rounded-lg bg-red-500/15 text-red-400 px-3 py-1 text-[15px] font-bold">
                              {item.total_need}
                            </span>
                          ) : (
                            <span className="text-emerald-400 font-medium">✓</span>
                          )}
                        </td>

                        {/* Revenue */}
                        <td className={`${tdCls} text-[hsl(var(--muted-foreground))]`}>{formatMoney(item.revenue)}</td>

                        {/* Clusters count */}
                        <td className={`${tdCls} text-center text-[hsl(var(--muted-foreground))]`}>{item.clusters.length}</td>
                      </tr>

                      {/* Expanded cluster table */}
                      {isExpanded && <ClusterTable clusters={item.clusters} />}
                    </React.Fragment>
                  )
                })}
            </tbody>
          </table>
        </div>

        {/* Total row */}
        <div className="flex items-center justify-between px-6 py-4 border-t border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.05)]">
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            Всего SKU: <strong>{items.length}</strong>
          </span>
          <span className="text-sm font-bold">
            Итого поставить:{' '}
            <span className="text-red-400 text-base">
              {formatNumber(items.reduce((s, i) => s + i.total_need, 0))} ед.
            </span>
          </span>
        </div>

        {/* Image hover popup */}
        {hoveredImg && (
          <div
            className="fixed z-50 rounded-xl overflow-hidden shadow-2xl border border-[hsl(var(--border))]"
            style={{ left: hoveredImg.x, top: hoveredImg.y, width: 160, height: 208 }}
          >
            <img src={hoveredImg.url} alt="" className="h-full w-full object-cover" />
          </div>
        )}
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Hub Table — consolidated by delivery cluster
   ═══════════════════════════════════════════════════════════ */

function HubTable({ hubs }: { hubs: HubSummary[] }) {
  const [expandedHub, setExpandedHub] = useState<string | null>(null)

  const totalNeed = useMemo(() => hubs.reduce((s, h) => s + h.total_need, 0), [hubs])

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.3, duration: 0.4 }}
    >
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        {/* Title bar */}
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Консолидация по складам отгрузки</h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">
            {hubs.length} складов • {totalNeed} ед. к поставке
          </span>
        </div>

        <div className="overflow-auto max-h-[700px]">
          {hubs.length === 0 ? (
            <div className="p-10 text-center text-[hsl(var(--muted-foreground))]">
              Нет SKU, требующих поставки
            </div>
          ) : (
            <div className="divide-y divide-[hsl(var(--border)/0.3)]">
              {hubs.map((hub) => {
                const isOpen = expandedHub === hub.hub
                return (
                  <div key={hub.hub}>
                    {/* Hub header row */}
                    <div
                      className={`flex items-center gap-4 px-6 py-4 cursor-pointer transition-colors ${
                        isOpen ? 'bg-[hsl(var(--primary)/0.06)]' : 'hover:bg-[hsl(var(--muted)/0.1)]'
                      }`}
                      onClick={() => setExpandedHub(isOpen ? null : hub.hub)}
                    >
                      <motion.div animate={{ rotate: isOpen ? 90 : 0 }} transition={{ duration: 0.15 }}>
                        <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                      </motion.div>

                      <Warehouse className="h-5 w-5 text-blue-400 shrink-0" />

                      <div className="flex-1 min-w-0">
                        <div className="text-[15px] font-bold text-[hsl(var(--foreground))]">{hub.hub}</div>
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
                          {hub.items.length} позиций • Выручка: {formatMoney(hub.total_revenue)}
                        </div>
                      </div>

                      <span className="inline-flex items-center justify-center min-w-[50px] rounded-lg bg-red-500/15 text-red-400 px-3 py-1.5 text-[16px] font-bold">
                        {hub.total_need}
                      </span>
                    </div>

                    {/* Expanded items */}
                    {isOpen && (
                      <div className="bg-[hsl(var(--muted)/0.06)] border-t border-[hsl(var(--border)/0.2)]">
                        <table className="w-full text-[13px]">
                          <thead>
                            <tr className="text-[hsl(var(--muted-foreground))] text-[11px] uppercase tracking-wider">
                              <th className="text-left py-2 px-6 font-semibold">Товар</th>
                              <th className="text-left py-2 px-3 font-semibold">Кластер спроса</th>
                              <th className="text-right py-2 px-3 font-semibold">Доставка, ч</th>
                              <th className="text-right py-2 px-3 font-semibold">Ежедн.×b</th>
                              <th className="text-right py-2 px-3 font-semibold">Выручка</th>
                              <th className="text-right py-2 px-6 font-semibold w-[100px]">ПОСТАВИТЬ</th>
                            </tr>
                          </thead>
                          <tbody>
                            {hub.items.map((hi, idx) => (
                              <tr key={`${hi.offer_id}-${hi.cluster}-${idx}`} className="border-t border-[hsl(var(--border)/0.15)] hover:bg-[hsl(var(--muted)/0.1)] transition-colors">
                                <td className="py-2.5 px-6">
                                  <div className="flex items-center gap-2">
                                    {hi.image_url ? (
                                      <img src={hi.image_url} alt="" className="w-7 h-7 rounded-md object-cover border border-[hsl(var(--border)/0.3)]" loading="lazy" />
                                    ) : (
                                      <div className="w-7 h-7 rounded-md bg-[hsl(var(--muted)/0.3)] flex items-center justify-center text-xs">📦</div>
                                    )}
                                    <div className="min-w-0">
                                      <div className="text-[12px] font-medium truncate max-w-[200px]" title={hi.name}>{hi.name}</div>
                                      <div className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-60">{hi.offer_id}</div>
                                    </div>
                                  </div>
                                </td>
                                <td className="py-2.5 px-3">
                                  <span className="flex items-center gap-1">
                                    <MapPin className="h-3 w-3 text-[hsl(var(--muted-foreground))] shrink-0" />
                                    {hi.cluster}
                                  </span>
                                </td>
                                <td className="py-2.5 px-3 text-right tabular-nums">
                                  <span className={`font-medium ${hi.hub_hours <= 28 ? 'text-emerald-400' : hi.hub_hours <= 45 ? 'text-yellow-400' : 'text-[hsl(var(--muted-foreground))]'}`}>
                                    {hi.hub_hours}ч
                                  </span>
                                </td>
                                <td className="py-2.5 px-3 text-right tabular-nums">{hi.daily_boosted.toFixed(2)}</td>
                                <td className="py-2.5 px-3 text-right tabular-nums text-[hsl(var(--muted-foreground))]">{formatMoney(hi.revenue)}</td>
                                <td className="py-2.5 px-6 text-right">
                                  <span className="inline-flex items-center gap-1 rounded-full bg-red-500/15 text-red-400 px-2.5 py-0.5 text-[13px] font-bold">
                                    {hi.need}
                                  </span>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          )}
        </div>

        {/* Total */}
        <div className="flex items-center justify-between px-6 py-4 border-t border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.05)]">
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            Складов: <strong>{hubs.length}</strong>
          </span>
          <span className="text-sm font-bold">
            Итого поставить:{' '}
            <span className="text-red-400 text-base">
              {formatNumber(totalNeed)} ед.
            </span>
          </span>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Skeleton Loader
   ═══════════════════════════════════════════════════════════ */

function SupplySkeleton() {
  return (
    <div className="space-y-6 animate-pulse">
      <div className="grid grid-cols-1 sm:grid-cols-4 gap-4">
        {[...Array(4)].map((_, idx) => (
          <Card key={idx}><CardContent className="p-5"><Skeleton className="h-20 w-full" /></CardContent></Card>
        ))}
      </div>
      <Card><CardContent className="p-5"><Skeleton className="h-16 w-full" /></CardContent></Card>
      <Card><CardContent className="p-5"><Skeleton className="h-[400px] w-full" /></CardContent></Card>
    </div>
  )
}


/* ═══════════════════════════════════════════════════════════
   WB — Status Badge (4 statuses: critical, attention, ok, overstock)
   ═══════════════════════════════════════════════════════════ */

function WBStatusBadge({ status }: { status: WBSupplyItem['status'] }) {
  const map: Record<string, { label: string; cls: string }> = {
    critical: { label: 'Критично', cls: 'bg-red-500/15 text-red-400 ring-red-500/20' },
    attention: { label: 'Внимание', cls: 'bg-amber-500/15 text-amber-400 ring-amber-500/20' },
    ok: { label: 'В норме', cls: 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' },
    overstock: { label: 'Перезатарка', cls: 'bg-purple-500/15 text-purple-400 ring-purple-500/20' },
  }
  const s = map[status] || map.ok
  return (
    <span className={`inline-flex items-center rounded-full px-2.5 py-1 text-[11px] font-bold ring-1 ring-inset ${s.cls}`}>
      {s.label}
    </span>
  )
}

/* ═══════════════════════════════════════════════════════════
   WB — Warehouse Detail Row (expanded)
   ═══════════════════════════════════════════════════════════ */

function WBWarehouseDetailTable({ warehouses }: { warehouses: WBWarehouseDetail[] }) {
  const tdCls = 'px-3 py-2 text-right tabular-nums text-[12px] whitespace-nowrap'
  return (
    <tr>
      <td colSpan={10} className="p-0">
        <div className="mx-6 my-2 rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)] overflow-hidden">
          <table className="w-full border-collapse">
            <thead>
              <tr className="border-b border-[hsl(var(--border)/0.2)]">
                <th className="px-3 py-2 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Склад WB</th>
                <th className="px-3 py-2 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Остаток</th>
                <th className="px-3 py-2 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Заказов</th>
                <th className="px-3 py-2 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">В день</th>
                <th className="px-3 py-2 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Оборач.</th>
                <th className="px-3 py-2 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Поставить</th>
                <th className="px-3 py-2 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Хран. ₽/мес</th>
                <th className="px-3 py-2 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Приёмка</th>
                <th className="px-3 py-2 text-right text-[11px] font-semibold text-[hsl(var(--muted-foreground))]">Выручка</th>
              </tr>
            </thead>
            <tbody>
              {warehouses.map((wh) => (
                <tr key={wh.warehouse} className="border-b border-[hsl(var(--border)/0.1)] last:border-0 hover:bg-[hsl(var(--muted)/0.08)]">
                  <td className="px-3 py-2 text-left text-[12px] font-medium">{wh.warehouse}</td>
                  <td className={tdCls}>{formatNumber(wh.stock)}</td>
                  <td className={tdCls}>{formatNumber(wh.orders)}</td>
                  <td className={tdCls}>{wh.daily.toFixed(2)}</td>
                  <td className={tdCls}>
                    <span className={`font-semibold ${
                      wh.turnover_days > 60 ? 'text-purple-400' :
                      wh.turnover_days > 45 ? 'text-amber-400' :
                      wh.turnover_days < 14 ? 'text-red-400' :
                      'text-emerald-400'
                    }`}>
                      {wh.turnover_days >= 999 ? '∞' : `${wh.turnover_days.toFixed(0)} дн`}
                    </span>
                  </td>
                  <td className={tdCls}>
                    {wh.need > 0 ? (
                      <span className="inline-flex items-center justify-center min-w-[32px] rounded-md bg-blue-500/15 text-blue-400 px-2 py-0.5 text-[12px] font-bold">
                        {wh.need}
                      </span>
                    ) : (
                      <span className="text-emerald-400 font-medium">✓</span>
                    )}
                  </td>
                  <td className={`${tdCls} ${wh.storage_per_month > 50 ? 'text-amber-400 font-semibold' : 'text-[hsl(var(--muted-foreground))]'}`}>
                    {wh.storage_per_month.toFixed(2)}
                  </td>
                  <td className={tdCls}>
                    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium ${
                      wh.acceptance === 'Бесплатно' ? 'bg-emerald-500/10 text-emerald-400' : 'bg-amber-500/10 text-amber-400'
                    }`}>{wh.acceptance}</span>
                  </td>
                  <td className={`${tdCls} text-[hsl(var(--muted-foreground))]`}>{formatMoney(wh.revenue)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </td>
    </tr>
  )
}

/* ═══════════════════════════════════════════════════════════
   WB — Supply Table by SKU
   ═══════════════════════════════════════════════════════════ */

type WBSortKey = 'turnover_days' | 'total_sold' | 'total_stock' | 'total_need' | 'storage_cost_month'

function WBSupplyTable({ items }: { items: WBSupplyItem[] }) {
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [hoveredImg, setHoveredImg] = useState<{ url: string; x: number; y: number } | null>(null)
  const [sortKey, setSortKey] = useState<WBSortKey>('turnover_days')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const toggleSort = (key: WBSortKey) => {
    if (key === sortKey) setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    else { setSortKey(key); setSortDir(key === 'turnover_days' ? 'asc' : 'desc') }
  }

  const sorted = useMemo(() =>
    [...items].sort((a, b) => {
      const av = a[sortKey] as number, bv = b[sortKey] as number
      return sortDir === 'desc' ? bv - av : av - bv
    }), [items, sortKey, sortDir])

  const thBase = 'px-4 py-3.5 text-right text-[13px] font-semibold whitespace-nowrap select-none'
  const tdCls = 'px-4 py-3 text-right tabular-nums text-[13px] whitespace-nowrap'

  const SortTh = ({ k, children, className = '' }: { k: WBSortKey; children: React.ReactNode; className?: string }) => (
    <th className={`${thBase} ${className} cursor-pointer transition-colors ${
      sortKey === k ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
    }`} onClick={() => toggleSort(k)}>
      <span className="inline-flex items-center gap-1 justify-end">
        {children}
        {sortKey === k && <span className="text-[11px] text-[hsl(var(--primary))]">{sortDir === 'desc' ? '▼' : '▲'}</span>}
      </span>
    </th>
  )

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.3, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Рекомендации поставки по товарам</h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">Нажмите для детализации по складам</span>
        </div>
        <div className="overflow-auto max-h-[700px]">
          <table className="w-full border-collapse" style={{ minWidth: 960 }}>
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-4 py-3.5 w-[40px]"></th>
                <th className="px-4 py-3.5 text-left text-[13px] font-semibold text-[hsl(var(--muted-foreground))] w-[280px] min-w-[280px]">Товар</th>
                <th className={`${thBase} text-center`}>Статус</th>
                <SortTh k="total_sold">Продано</SortTh>
                <SortTh k="total_stock">Остаток</SortTh>
                <SortTh k="turnover_days">Оборач.</SortTh>
                <SortTh k="total_need">ПОСТАВИТЬ</SortTh>
                <SortTh k="storage_cost_month">Хран. ₽/мес</SortTh>
                <th className={`${thBase} text-center`}>Склады</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((item, idx) => {
                const isExpanded = expandedId === item.vendor_code
                const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.06)]'
                return (
                  <React.Fragment key={item.vendor_code || item.nm_id}>
                    <tr
                      className={`border-b border-[hsl(var(--border)/0.2)] transition-colors cursor-pointer ${
                        isExpanded ? 'bg-[hsl(var(--primary)/0.06)]' : `${rowBg} hover:bg-[hsl(var(--muted)/0.15)]`
                      }`}
                      onClick={() => setExpandedId(isExpanded ? null : item.vendor_code)}
                    >
                      <td className="px-3 py-3 text-center">
                        <motion.div animate={{ rotate: isExpanded ? 90 : 0 }} transition={{ duration: 0.15 }}>
                          <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                        </motion.div>
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2.5">
                          {item.image_url ? (
                            <div className="relative shrink-0"
                              onMouseEnter={(e) => { const r = (e.target as HTMLElement).getBoundingClientRect(); setHoveredImg({ url: item.image_url, x: r.right + 8, y: r.top }) }}
                              onMouseLeave={() => setHoveredImg(null)}>
                              <img src={item.image_url} alt="" className="w-9 h-9 rounded-lg object-cover border border-[hsl(var(--border)/0.3)]" loading="lazy" />
                            </div>
                          ) : (
                            <div className="w-9 h-9 rounded-lg bg-[hsl(var(--muted)/0.3)] shrink-0 flex items-center justify-center text-sm">📦</div>
                          )}
                          <div className="min-w-0 flex-1">
                            <div className="text-[13px] font-medium truncate" title={item.name}>{item.name}</div>
                            <div className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-60 truncate">{item.vendor_code}</div>
                          </div>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-center"><WBStatusBadge status={item.status} /></td>
                      <td className={`${tdCls} font-semibold`}>{formatNumber(item.total_sold)}</td>
                      <td className={tdCls}><span className={item.total_stock === 0 ? 'text-red-400 font-bold' : ''}>{formatNumber(item.total_stock)}</span></td>
                      <td className={tdCls}>
                        <span className={`font-semibold ${
                          item.turnover_days >= 999 ? 'text-[hsl(var(--muted-foreground))] opacity-50' :
                          item.turnover_days > 60 ? 'text-purple-400' :
                          item.turnover_days > 45 ? 'text-amber-400' :
                          item.turnover_days < 14 ? 'text-red-400' : 'text-emerald-400'
                        }`}>
                          {item.turnover_days >= 999 ? '∞' : `${item.turnover_days.toFixed(0)} дн`}
                        </span>
                      </td>
                      <td className={tdCls}>
                        {item.total_need > 0 ? (
                          <span className="inline-flex items-center justify-center min-w-[40px] rounded-lg bg-red-500/15 text-red-400 px-3 py-1 text-[15px] font-bold">{item.total_need}</span>
                        ) : <span className="text-emerald-400 font-medium">✓</span>}
                      </td>
                      <td className={`${tdCls} ${item.storage_cost_month > 100 ? 'text-amber-400 font-semibold' : 'text-[hsl(var(--muted-foreground))]'}`}>
                        {item.storage_cost_month > 0 ? formatMoney(item.storage_cost_month) : '—'}
                      </td>
                      <td className={`${tdCls} text-center text-[hsl(var(--muted-foreground))]`}>{item.warehouses.length}</td>
                    </tr>
                    {isExpanded && <WBWarehouseDetailTable warehouses={item.warehouses} />}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
        <div className="flex items-center justify-between px-6 py-4 border-t border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.05)]">
          <span className="text-sm text-[hsl(var(--muted-foreground))]">Всего SKU: <strong>{items.length}</strong></span>
          <span className="text-sm font-bold">Итого поставить: <span className="text-red-400 text-base">{formatNumber(items.reduce((s, i) => s + i.total_need, 0))} ед.</span></span>
        </div>
        {hoveredImg && (
          <div className="fixed z-50 rounded-xl overflow-hidden shadow-2xl border border-[hsl(var(--border))]" style={{ left: hoveredImg.x, top: hoveredImg.y, width: 160, height: 208 }}>
            <img src={hoveredImg.url} alt="" className="h-full w-full object-cover" />
          </div>
        )}
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   WB — Warehouse Summary Table
   ═══════════════════════════════════════════════════════════ */

function WBWarehouseSummaryTable({ warehouses }: { warehouses: WBWarehouseSummary[] }) {
  const thBase = 'px-4 py-3.5 text-right text-[13px] font-semibold text-[hsl(var(--muted-foreground))] whitespace-nowrap'
  const tdCls = 'px-4 py-3.5 text-right tabular-nums text-[13px] whitespace-nowrap'
  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.3, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold">Сводка по складам WB</h2>
        </div>
        <div className="overflow-auto">
          <table className="w-full border-collapse" style={{ minWidth: 800 }}>
            <thead>
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-4 py-3.5 text-left text-[13px] font-semibold text-[hsl(var(--muted-foreground))]">Склад</th>
                <th className={thBase}>Товаров</th>
                <th className={thBase}>Остаток</th>
                <th className={thBase}>Заказов</th>
                <th className={thBase}>Поставить</th>
                <th className={thBase}>Выручка</th>
                <th className={`${thBase} text-center`}>Коэфф. хран.</th>
                <th className={`${thBase} text-center`}>Приёмка</th>
              </tr>
            </thead>
            <tbody>
              {warehouses.map((wh, idx) => (
                <tr key={wh.warehouse} className={`border-b border-[hsl(var(--border)/0.2)] hover:bg-[hsl(var(--muted)/0.1)] ${idx % 2 ? 'bg-[hsl(var(--muted)/0.06)]' : ''}`}>
                  <td className="px-4 py-3.5"><div className="flex items-center gap-2"><Warehouse className="h-4 w-4 text-[hsl(var(--muted-foreground))]" /><span className="text-[13px] font-medium">{wh.warehouse}</span></div></td>
                  <td className={tdCls}>{wh.items_count}</td>
                  <td className={`${tdCls} font-semibold`}>{formatNumber(wh.total_stock)}</td>
                  <td className={tdCls}>{formatNumber(wh.total_orders)}</td>
                  <td className={tdCls}>
                    {wh.total_need > 0 ? (
                      <span className="inline-flex items-center rounded-md bg-blue-500/15 text-blue-400 px-2 py-0.5 text-[13px] font-bold">{formatNumber(wh.total_need)}</span>
                    ) : <span className="text-emerald-400 font-medium">✓</span>}
                  </td>
                  <td className={tdCls}>{formatMoney(wh.total_revenue)}</td>
                  <td className={`${tdCls} text-center`}>{wh.storage_coef ? <span className={wh.storage_coef > 200 ? 'text-amber-400 font-medium' : ''}>{wh.storage_coef}%</span> : '—'}</td>
                  <td className={`${tdCls} text-center`}>
                    <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-[11px] font-medium ${
                      wh.acceptance === 'Бесплатно' ? 'bg-emerald-500/10 text-emerald-400' : 'bg-amber-500/10 text-amber-400'
                    }`}>{wh.acceptance}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="flex items-center justify-between px-6 py-4 border-t border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.05)]">
          <span className="text-sm text-[hsl(var(--muted-foreground))]">Складов: <strong>{warehouses.length}</strong></span>
          <span className="text-sm font-bold">Итого поставить: <span className="text-blue-400 text-base">{formatNumber(warehouses.reduce((s, w) => s + w.total_need, 0))} ед.</span></span>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   WB — Settings Panel (no Ad Boost, max 60 days)
   ═══════════════════════════════════════════════════════════ */

const WB_TARGET_OPTIONS = [
  { value: 14, label: '14 дней' },
  { value: 30, label: '30 дней' },
  { value: 45, label: '45 дней' },
  { value: 60, label: '60 дней' },
]

function WBSettingsPanel({
  targetDays, setTargetDays,
  salesPeriod, setSalesPeriod,
  safety, setSafety,
  loading, onRefresh, onExport, exporting,
}: {
  targetDays: number; setTargetDays: (v: number) => void
  salesPeriod: number; setSalesPeriod: (v: number) => void
  safety: number; setSafety: (v: number) => void
  loading: boolean; onRefresh: () => void
  onExport: () => void; exporting: boolean
}) {
  const selCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[13px] font-medium transition-all cursor-pointer ${
      active ? 'bg-[hsl(var(--primary))] text-white shadow-md' : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
    }`
  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1, duration: 0.4 }}>
      <Card>
        <CardContent className="p-5">
          <div className="flex flex-wrap items-center gap-6">
            <div className="space-y-1.5">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Горизонт поставки</p>
              <div className="flex gap-1">
                {WB_TARGET_OPTIONS.map(o => <button key={o.value} className={selCls(targetDays === o.value)} onClick={() => setTargetDays(o.value)}>{o.label}</button>)}
              </div>
              <p className="text-[11px] text-amber-400/80">⚠ WB: бесплатное хранение до 60 дней</p>
            </div>
            <div className="space-y-1.5">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Период для анализа продаж</p>
              <div className="flex gap-1">
                {PERIOD_OPTIONS.map(o => <button key={o.value} className={selCls(salesPeriod === o.value)} onClick={() => setSalesPeriod(o.value)}>{o.label}</button>)}
              </div>
            </div>
            <div className="space-y-1.5">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Страховой буфер</p>
              <div className="flex gap-1">
                {SAFETY_OPTIONS.map(o => <button key={o.value} className={selCls(safety === o.value)} onClick={() => setSafety(o.value)}>{o.label}</button>)}
              </div>
            </div>
            <div className="ml-auto flex items-center gap-2">
              <button onClick={onRefresh} disabled={loading} className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-medium bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)] transition-all disabled:opacity-50">
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} /> Обновить
              </button>
              <button onClick={onExport} disabled={exporting || loading} className="flex items-center gap-2 px-4 py-2 rounded-xl text-[13px] font-semibold bg-gradient-to-r from-emerald-600 to-emerald-500 text-white shadow-lg shadow-emerald-500/25 hover:shadow-emerald-500/40 transition-all disabled:opacity-50">
                <Download className="h-4 w-4" /> {exporting ? 'Генерация...' : 'Скачать Excel'}
              </button>
            </div>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   WB — Full Content (state + fetch + KPIs + tabs)
   ═══════════════════════════════════════════════════════════ */

function WBContent({ shopId }: { shopId: number }) {
  const [data, setData] = useState<WBSupplyResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [exporting, setExporting] = useState(false)
  const [targetDays, setTargetDays] = useState(45)
  const [salesPeriod, setSalesPeriod] = useState(30)
  const [safety, setSafety] = useState(1.15)
  const [activeTab, setActiveTab] = useState<'sku' | 'warehouses'>('sku')

  const fetchData = useCallback(async () => {
    setLoading(true); setError(null)
    try {
      const result = await getWBSupplyApi({ shop_id: shopId, sales_period: salesPeriod, target_days: targetDays, safety })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки')
    } finally { setLoading(false) }
  }, [shopId, salesPeriod, targetDays, safety])

  useEffect(() => { fetchData() }, [fetchData])

  const handleExport = async () => {
    setExporting(true)
    try { await downloadWBSupplyExcel({ shop_id: shopId, sales_period: salesPeriod, target_days: targetDays, safety }) }
    catch { /* silent */ }
    finally { setExporting(false) }
  }

  if (loading && !data) return <SupplySkeleton />

  if (error) return (
    <Card>
      <CardContent className="p-10 text-center">
        <AlertCircle className="h-10 w-10 mx-auto text-red-400 mb-3" />
        <p className="text-lg font-medium text-red-400">{error}</p>
        <button onClick={fetchData} className="mt-4 px-4 py-2 rounded-xl bg-[hsl(var(--primary))] text-white text-sm font-medium">Попробовать снова</button>
      </CardContent>
    </Card>
  )

  if (!data) return null

  return (
    <>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard title="Итого поставить" value={`${formatNumber(data.kpi.total_need)} ед.`} subtitle={`Всего SKU: ${data.kpi.total_sku}`} icon={Package} accent="from-blue-600 to-blue-500" delay={0} />
        <KpiCard title="Критические SKU" value={String(data.kpi.critical_count)} subtitle="Запас < 14 дней" icon={AlertTriangle} accent="from-red-600 to-red-500" delay={0.05} />
        <KpiCard title="Перезатарка" value={String(data.kpi.overstock_count)} subtitle="Оборот > 60 дн — платное хранение" icon={AlertCircle} accent="from-purple-600 to-purple-500" delay={0.1} />
        <KpiCard title="Хранение / мес" value={formatMoney(data.kpi.total_storage_month)} subtitle={`Средний запас: ${data.kpi.avg_days_supply} дн.`} icon={Wallet} accent="from-amber-600 to-amber-500" delay={0.15} />
      </div>
      <WBSettingsPanel targetDays={targetDays} setTargetDays={setTargetDays} salesPeriod={salesPeriod} setSalesPeriod={setSalesPeriod} safety={safety} setSafety={setSafety} loading={loading} onRefresh={fetchData} onExport={handleExport} exporting={exporting} />
      <div className="flex gap-1 p-1 rounded-xl bg-[hsl(var(--muted)/0.15)] w-fit">
        <button className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all ${activeTab === 'sku' ? 'bg-[hsl(var(--card))] text-[hsl(var(--foreground))] shadow-sm' : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'}`} onClick={() => setActiveTab('sku')}>
          <ListTree className="h-4 w-4" /> По товарам
        </button>
        <button className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all ${activeTab === 'warehouses' ? 'bg-[hsl(var(--card))] text-[hsl(var(--foreground))] shadow-sm' : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'}`} onClick={() => setActiveTab('warehouses')}>
          <Warehouse className="h-4 w-4" /> По складам
        </button>
      </div>
      {activeTab === 'sku' ? <WBSupplyTable items={data.items} /> : <WBWarehouseSummaryTable warehouses={data.warehouse_summary} />}
    </>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page — auto-switches by marketplace
   ═══════════════════════════════════════════════════════════ */

export default function WarehouseSupplyPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isOzon = currentShop?.marketplace === 'ozon'
  const isWB = currentShop?.marketplace === 'wildberries'

  const [data, setData] = useState<SupplyResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [exporting, setExporting] = useState(false)

  // Settings
  const [targetDays, setTargetDays] = useState(60)
  const [salesPeriod, setSalesPeriod] = useState(30)
  const [safety, setSafety] = useState(1.15)
  const [useAdBoost, setUseAdBoost] = useState(true)
  const [activeTab, setActiveTab] = useState<'sku' | 'hubs'>('sku')

  const fetchData = useCallback(async () => {
    if (!currentShop || !isOzon) return
    setLoading(true)
    setError(null)
    try {
      const result = await getOzonSupplyApi({
        shop_id: currentShop.id,
        sales_period: salesPeriod,
        target_days: targetDays,
        safety,
        use_ad_boost: useAdBoost,
      })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки')
    } finally {
      setLoading(false)
    }
  }, [currentShop, isOzon, salesPeriod, targetDays, safety, useAdBoost])

  useEffect(() => { if (isOzon) fetchData() }, [fetchData, isOzon])

  const handleExport = async () => {
    if (!currentShop) return
    setExporting(true)
    try {
      await downloadSupplyExcel({
        shop_id: currentShop.id,
        sales_period: salesPeriod,
        target_days: targetDays,
        safety,
        use_ad_boost: useAdBoost,
      })
    } catch {
      // silent
    } finally {
      setExporting(false)
    }
  }

  // ── WB marketplace → WBContent ──
  if (currentShop && isWB) {
    return (
      <div className="space-y-6 pb-10">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Планирование поставки</h1>
          <p className="text-[hsl(var(--muted-foreground))] mt-1">
            Рекомендации по пополнению стоков Wildberries с учётом платного хранения
          </p>
        </div>
        <WBContent shopId={currentShop.id} />
      </div>
    )
  }

  return (
    <div className="space-y-6 pb-10">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Поставки FBO</h1>
        <p className="text-[hsl(var(--muted-foreground))] mt-1">
          Рекомендации по пополнению стоков Ozon FBO с детализацией по кластерам
        </p>
      </div>

      {loading && !data ? (
        <SupplySkeleton />
      ) : error ? (
        <Card>
          <CardContent className="p-10 text-center">
            <AlertCircle className="h-10 w-10 mx-auto text-red-400 mb-3" />
            <p className="text-lg font-medium text-red-400">{error}</p>
            <button onClick={fetchData} className="mt-4 px-4 py-2 rounded-xl bg-[hsl(var(--primary))] text-white text-sm font-medium">
              Попробовать снова
            </button>
          </CardContent>
        </Card>
      ) : data ? (
        <>
          {/* KPI */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            <KpiCard
              title="Итого поставить"
              value={`${formatNumber(data.kpi.total_need)} ед.`}
              subtitle={`Всего SKU: ${data.kpi.total_sku}`}
              icon={Package}
              accent="from-blue-600 to-blue-500"
              delay={0}
            />
            <KpiCard
              title="Критические SKU"
              value={String(data.kpi.critical_count)}
              subtitle="Запас < 14 дней"
              icon={AlertTriangle}
              accent="from-red-600 to-red-500"
              delay={0.05}
            />
            <KpiCard
              title="На внимании"
              value={String(data.kpi.attention_count)}
              subtitle={`Запас < ${targetDays} дней`}
              icon={AlertCircle}
              accent="from-amber-600 to-amber-500"
              delay={0.1}
            />
            <KpiCard
              title="Средний запас"
              value={`${data.kpi.avg_days_supply} дн.`}
              subtitle={`FBO: ${formatNumber(data.kpi.total_fbo)} ед.`}
              icon={BarChart3}
              accent="from-emerald-600 to-emerald-500"
              delay={0.15}
            />
          </div>

          {/* Settings */}
          <SettingsPanel
            targetDays={targetDays} setTargetDays={setTargetDays}
            salesPeriod={salesPeriod} setSalesPeriod={setSalesPeriod}
            safety={safety} setSafety={setSafety}
            useAdBoost={useAdBoost} setUseAdBoost={setUseAdBoost}
            loading={loading} onRefresh={fetchData}
            onExport={handleExport} exporting={exporting}
          />

          {/* Tabs + Table */}
          <div className="flex gap-1 p-1 rounded-xl bg-[hsl(var(--muted)/0.15)] w-fit">
            <button
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all ${
                activeTab === 'sku'
                  ? 'bg-[hsl(var(--card))] text-[hsl(var(--foreground))] shadow-sm'
                  : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
              }`}
              onClick={() => setActiveTab('sku')}
            >
              <ListTree className="h-4 w-4" />
              По SKU
            </button>
            <button
              className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all ${
                activeTab === 'hubs'
                  ? 'bg-[hsl(var(--card))] text-[hsl(var(--foreground))] shadow-sm'
                  : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
              }`}
              onClick={() => setActiveTab('hubs')}
            >
              <Warehouse className="h-4 w-4" />
              По кластерам
            </button>
          </div>

          {activeTab === 'sku' ? (
            <SupplyTable items={data.items} />
          ) : (
            <HubTable hubs={data.hubs} />
          )}
        </>
      ) : null}
    </div>
  )
}
