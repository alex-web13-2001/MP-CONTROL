/**
 * Warehouses Cross-logistics — кросс-карта WB (warehouse × okrug matrix).
 * Ozon: redirects to /warehouses/analytics (crossdocking tab).
 */
import React, { useState, useEffect, useCallback } from 'react'
import { Navigate } from 'react-router-dom'
import { motion } from 'framer-motion'
import {
  ArrowRightLeft,
  RefreshCw,
  AlertTriangle,
  MapPin,
  Package,
  ChevronRight,
  Warehouse,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBWarehouseAnalytics,
  type WBWarehouseAnalyticsResponse,
  type WBCrossMapRow,
  type WBAnalyticsWarehouse,
} from '@/api/warehouses'

/* ── Helpers ── */
function fmt(v: number): string { return Math.round(v).toLocaleString('ru-RU') }
function fmtD(v: number | null): string { return v != null ? `${Math.round(v)} дн` : '—' }

/* ── Period Options ── */
const PERIOD_OPTIONS = [
  { label: '7 дн', value: 7 },
  { label: '14 дн', value: 14 },
  { label: '30 дн', value: 30 },
  { label: '60 дн', value: 60 },
  { label: '90 дн', value: 90 },
]

/* ═══ Cross-Map Table ═══ */
function CrossMapTable({ crossMap, okrugList }: { crossMap: WBCrossMapRow[]; okrugList: string[] }) {
  if (crossMap.length === 0) return null

  const shortOkrug = (s: string) => s.replace(' федеральный округ', '').replace('Северо-', 'С-')

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.15, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
            <ArrowRightLeft className="h-5 w-5 text-blue-400" />
            Кросс-карта
          </h2>
          <p className="text-[12px] text-[hsl(var(--muted-foreground))] mt-1">
            Откуда отгружается → куда доставляется. <span className="text-emerald-400 font-medium">Зелёный</span> = свой округ, <span className="text-red-400 font-medium">красный</span> = кросс-отправка
          </p>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full border-collapse text-[12px]">
            <thead>
              <tr className="border-b border-[hsl(var(--border)/0.3)]">
                <th className="px-4 py-3 text-left font-bold text-[hsl(var(--muted-foreground))] sticky left-0 bg-[hsl(var(--card))] z-10 min-w-[160px]">
                  Склад ↓ / Округ →
                </th>
                {okrugList.map((okrug) => (
                  <th key={okrug} className="px-3 py-3 text-center font-semibold text-[hsl(var(--muted-foreground))] text-[10px] uppercase tracking-wider min-w-[80px]">
                    {shortOkrug(okrug)}
                  </th>
                ))}
                <th className="px-3 py-3 text-center font-bold text-[hsl(var(--muted-foreground))] text-[10px]">ИТОГО</th>
              </tr>
            </thead>
            <tbody>
              {crossMap.map((row, idx) => (
                <tr key={row.warehouse} className={`border-b border-[hsl(var(--border)/0.1)] ${idx % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''}`}>
                  <td className="px-4 py-2.5 text-left font-medium sticky left-0 bg-[hsl(var(--card))] z-10">
                    {row.warehouse}
                  </td>
                  {okrugList.map((okrug) => {
                    const cell = row.okrugs[okrug]
                    if (!cell || cell.count === 0) {
                      return <td key={okrug} className="px-3 py-2.5 text-center text-[hsl(var(--muted-foreground))] opacity-20">—</td>
                    }
                    const intensity = Math.min(cell.count / row.total_orders * 3, 1)
                    return (
                      <td key={okrug} className="px-3 py-2.5 text-center">
                        <span
                          className={`inline-flex items-center justify-center min-w-[28px] px-1.5 py-0.5 rounded-md text-[12px] font-bold tabular-nums ${
                            cell.is_local
                              ? 'bg-emerald-500/15 text-emerald-400'
                              : `bg-red-500/${Math.round(10 + intensity * 20)} text-red-400`
                          }`}
                        >
                          {cell.count}
                        </span>
                      </td>
                    )
                  })}
                  <td className="px-3 py-2.5 text-center font-bold text-[13px]">{row.total_orders}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══ Warehouse Expanded Detail (SKU geography) ═══ */
function WarehouseExpandedDetail({ wh }: { wh: WBAnalyticsWarehouse }) {
  const [selectedSku, setSelectedSku] = useState<number | null>(null)

  return (
    <motion.div
      initial={{ height: 0, opacity: 0 }}
      animate={{ height: 'auto', opacity: 1 }}
      exit={{ height: 0, opacity: 0 }}
      transition={{ duration: 0.2 }}
      className="overflow-hidden"
    >
      <div className="px-6 py-4 bg-[hsl(var(--muted)/0.04)] border-t border-[hsl(var(--border)/0.2)]">
        <div className="grid grid-cols-1 lg:grid-cols-[1fr_280px] gap-6">
          {/* SKU Table */}
          <div>
            <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-3 flex items-center gap-2">
              <Package className="h-4 w-4 text-blue-400" />
              Товары на складе ({wh.skus.length})
              <span className="text-[10px] font-normal text-[hsl(var(--muted-foreground))]">• клик — география</span>
            </h4>
            <div className="rounded-lg border border-[hsl(var(--border)/0.2)] overflow-hidden max-h-[400px] overflow-y-auto">
              <table className="w-full text-[12px]">
                <thead className="sticky top-0 bg-[hsl(var(--card))] z-10">
                  <tr className="border-b border-[hsl(var(--border)/0.2)]">
                    <th className="px-3 py-2 text-left font-semibold text-[hsl(var(--muted-foreground))]">Товар</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))]">Остаток</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))]">Заказов</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))]">Кросс%</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))]">Дн.зап.</th>
                  </tr>
                </thead>
                <tbody>
                  {wh.skus.slice(0, 30).map((sku) => {
                    const isSelected = selectedSku === sku.nm_id
                    return (
                      <React.Fragment key={sku.nm_id}>
                        <tr
                          className={`border-b border-[hsl(var(--border)/0.1)] cursor-pointer transition-colors ${
                            isSelected ? 'bg-[hsl(var(--primary)/0.08)]' : 'hover:bg-[hsl(var(--muted)/0.06)]'
                          }`}
                          onClick={() => setSelectedSku(isSelected ? null : sku.nm_id)}
                        >
                          <td className="px-3 py-1.5 text-left">
                            <div className="truncate max-w-[180px] font-medium" title={sku.name}>
                              {sku.name || `#${sku.nm_id}`}
                            </div>
                          </td>
                          <td className="px-3 py-1.5 text-center tabular-nums">{fmt(sku.stock)}</td>
                          <td className="px-3 py-1.5 text-center tabular-nums">{fmt(sku.orders)}</td>
                          <td className="px-3 py-1.5 text-center">
                            {sku.orders > 0 ? (
                              <span className={`text-[11px] font-semibold tabular-nums ${
                                sku.cross_pct > 50 ? 'text-red-400' : sku.cross_pct > 25 ? 'text-amber-400' : 'text-emerald-400'
                              }`}>{sku.cross_pct}%</span>
                            ) : <span className="text-[hsl(var(--muted-foreground))] opacity-40">—</span>}
                          </td>
                          <td className="px-3 py-1.5 text-center tabular-nums">
                            <span className={`font-semibold ${
                              sku.days_supply === null ? '' :
                              sku.days_supply < 14 ? 'text-red-400' :
                              sku.days_supply < 30 ? 'text-amber-400' :
                              sku.days_supply > 120 ? 'text-purple-400' :
                              'text-emerald-400'
                            }`}>
                              {sku.days_supply === null ? '∞' : sku.days_supply > 999 ? '999+' : Math.round(sku.days_supply)}
                            </span>
                          </td>
                        </tr>
                        {/* Expanded SKU geography */}
                        {isSelected && sku.geography.length > 0 && (
                          <tr>
                            <td colSpan={5} className="px-3 py-2 bg-[hsl(var(--muted)/0.06)]">
                              <div className="flex items-center gap-2 mb-2">
                                <MapPin className="h-3 w-3 text-emerald-400" />
                                <span className="text-[11px] font-bold text-[hsl(var(--foreground))]">
                                  География «{sku.name?.slice(0, 30) || sku.vendor_code}» с {wh.warehouse_name}
                                </span>
                              </div>
                              <div className="grid grid-cols-2 gap-x-6 gap-y-1">
                                {sku.geography.map((g) => (
                                  <div key={g.okrug} className="flex items-center gap-2">
                                    <span className="text-[11px] font-medium truncate flex-1 min-w-0">
                                      {g.okrug.replace(' федеральный округ', '')}
                                    </span>
                                    <span className={`text-[8px] px-1 py-0 rounded-full font-bold ${
                                      g.is_local
                                        ? 'bg-emerald-500/15 text-emerald-400'
                                        : 'bg-red-500/10 text-red-400'
                                    }`}>
                                      {g.is_local ? 'СВОЙ' : 'КРОСС'}
                                    </span>
                                    <span className="text-[10px] tabular-nums font-medium w-[45px] text-right">
                                      {g.orders} ({g.share}%)
                                    </span>
                                  </div>
                                ))}
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
          </div>

          {/* Overall warehouse geography */}
          <div>
            <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-3 flex items-center gap-2">
              <MapPin className="h-4 w-4 text-emerald-400" />
              География склада
            </h4>
            {wh.geography.length > 0 ? (
              <div className="space-y-2">
                {wh.geography.map((g) => (
                  <div key={g.okrug} className="flex items-center gap-3">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="text-[12px] font-medium truncate">
                          {g.okrug.replace(' федеральный округ', '')}
                        </span>
                        <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold ${
                          g.is_local ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/10 text-red-400'
                        }`}>
                          {g.is_local ? 'СВОЙ' : 'КРОСС'}
                        </span>
                      </div>
                      <div className="w-full h-1.5 rounded-full bg-[hsl(var(--muted)/0.15)] mt-1 overflow-hidden">
                        <div
                          className={`h-full rounded-full ${g.is_local ? 'bg-emerald-500' : 'bg-red-500/60'}`}
                          style={{ width: `${Math.min(g.share, 100)}%` }}
                        />
                      </div>
                    </div>
                    <span className="text-[12px] tabular-nums font-medium w-14 text-right">
                      {g.orders} ({g.share}%)
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-[12px] text-[hsl(var(--muted-foreground))] opacity-50">Нет данных за период</p>
            )}
          </div>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══ Warehouses with cross details table ═══ */
function CrossWarehousesTable({ warehouses }: { warehouses: WBAnalyticsWarehouse[] }) {
  const [expandedWh, setExpandedWh] = useState<string | null>(null)
  // Filter only warehouses with cross activity
  const crossWarehouses = warehouses.filter(wh => wh.cross_pct > 0 || wh.orders > 0)

  if (crossWarehouses.length === 0) return null

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.25, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Кросс-анализ по складам</h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">
            Нажмите для детализации по SKU и географии
          </span>
        </div>

        <div className="overflow-auto max-h-[600px]">
          <table className="w-full border-collapse" style={{ minWidth: 700 }}>
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-2 py-3 w-[32px]"></th>
                <th className="px-4 py-3 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">Склад</th>
                <th className="px-3 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">Заказов</th>
                <th className="px-3 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">Кросс%</th>
                <th className="px-3 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">Оборач.</th>
                <th className="px-3 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">SKU</th>
              </tr>
            </thead>
            <tbody>
              {crossWarehouses.map((wh, idx) => {
                const isExpanded = expandedWh === wh.warehouse_name
                const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.04)]'
                return (
                  <React.Fragment key={wh.warehouse_name}>
                    <tr
                      className={`border-b border-[hsl(var(--border)/0.15)] transition-colors cursor-pointer ${
                        isExpanded ? 'bg-[hsl(var(--primary)/0.06)]' : `${rowBg} hover:bg-[hsl(var(--muted)/0.1)]`
                      }`}
                      onClick={() => setExpandedWh(isExpanded ? null : wh.warehouse_name)}
                    >
                      <td className="px-2 py-3 text-center">
                        <motion.div animate={{ rotate: isExpanded ? 90 : 0 }} transition={{ duration: 0.15 }}>
                          <ChevronRight className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
                        </motion.div>
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <Warehouse className="h-4 w-4 text-blue-400 shrink-0" />
                          <span className="text-[13px] font-semibold">{wh.warehouse_name}</span>
                        </div>
                      </td>
                      <td className="px-3 py-3 text-center tabular-nums text-[13px]">{fmt(wh.orders)}</td>
                      <td className="px-3 py-3 text-center">
                        <span className={`text-[13px] font-bold tabular-nums ${
                          wh.cross_pct > 50 ? 'text-red-400' : wh.cross_pct > 25 ? 'text-amber-400' : 'text-emerald-400'
                        }`}>{wh.cross_pct}%</span>
                      </td>
                      <td className="px-3 py-3 text-center tabular-nums text-[13px]">{fmtD(wh.turnover_days)}</td>
                      <td className="px-3 py-3 text-center tabular-nums text-[13px] text-[hsl(var(--muted-foreground))]">{wh.sku_count}</td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={6} className="p-0">
                          <WarehouseExpandedDetail wh={wh} />
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══ Skeleton ═══ */
function CrossSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-[100px] rounded-2xl" />
      <Skeleton className="h-[400px] rounded-2xl" />
      <Skeleton className="h-[300px] rounded-2xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function WarehousesCrossPage() {
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
          <h1 className="text-3xl font-bold tracking-tight">Кросс-логистика</h1>
          <p className="text-[hsl(var(--muted-foreground))] mt-1">
            Анализ кросс-складских отправок: откуда и куда отгружаются заказы
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
        <CrossSkeleton />
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
          {/* KPI summary */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
              <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">Кросс-отправки</div>
              <div className={`text-2xl font-bold tabular-nums ${
                data.kpi.cross_pct > 50 ? 'text-red-400' : data.kpi.cross_pct > 25 ? 'text-amber-400' : 'text-emerald-400'
              }`}>{data.kpi.cross_pct}%</div>
              <div className="text-[11px] text-[hsl(var(--muted-foreground))]">заказов в чужие округа</div>
            </div>
            <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
              <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">Складов</div>
              <div className="text-2xl font-bold tabular-nums">{data.kpi.total_warehouses}</div>
              <div className="text-[11px] text-[hsl(var(--muted-foreground))]">{data.warehouses.filter(w => w.cross_pct > 30).length} с высоким кросс%</div>
            </div>
            <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
              <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">Всего заказов</div>
              <div className="text-2xl font-bold tabular-nums">{fmt(data.kpi.total_orders)}</div>
              <div className="text-[11px] text-[hsl(var(--muted-foreground))]">за {data.kpi.period_days} дней</div>
            </div>
            <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
              <div className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))] mb-1">Округов</div>
              <div className="text-2xl font-bold tabular-nums">{data.okrug_list.length}</div>
              <div className="text-[11px] text-[hsl(var(--muted-foreground))]">с заказами</div>
            </div>
          </div>

          {/* Cross-map */}
          <CrossMapTable crossMap={data.cross_map} okrugList={data.okrug_list} />

          {/* Warehouses with cross details */}
          <CrossWarehousesTable warehouses={data.warehouses} />
        </>
      ) : null}
    </div>
  )
}
