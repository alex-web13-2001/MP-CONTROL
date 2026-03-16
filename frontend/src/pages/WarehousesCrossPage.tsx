/**
 * Warehouses Cross-logistics — Redesigned cross-logistics analysis.
 * Features:
 * - Smart KPI cards with benchmarks and loss estimates
 * - Cross-map with toggle: by warehouse / by product
 * - Unified product display (vendor_code + nm_id + copy)
 * - Expanded geography panel with progress bars
 * - Auto-recommendations for high-cross SKUs
 *
 * Ozon: redirects to /warehouses/analytics (crossdocking tab).
 */
import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { Navigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  ArrowRightLeft,
  RefreshCw,
  AlertTriangle,
  MapPin,
  Package,
  ChevronRight,
  Warehouse,
  Copy,
  Check,
  TrendingUp,
  Truck,
  Target,
  ShoppingCart,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBWarehouseAnalytics,
  type WBWarehouseAnalyticsResponse,
  type WBCrossMapRow,
  type WBAnalyticsWarehouse,
  type WBAnalyticsSkuDetail,
  type WBAnalyticsGeography,
} from '@/api/warehouses'

/* ── Constants ── */
const CROSS_COST_PER_ORDER = 33 // ₽ WB cross-delivery surcharge
const CROSS_BENCHMARK_GOOD = 25
const CROSS_BENCHMARK_BAD = 50

/* ── Helpers ── */
function fmt(v: number): string { return Math.round(v).toLocaleString('ru-RU') }
function fmtM(v: number): string { return Math.round(v).toLocaleString('ru-RU') + ' ₽' }
function fmtD(v: number | null): string { return v != null ? `${Math.round(v)} дн` : '—' }
function pctColor(pct: number): string {
  if (pct > CROSS_BENCHMARK_BAD) return 'text-red-400'
  if (pct > CROSS_BENCHMARK_GOOD) return 'text-amber-400'
  return 'text-emerald-400'
}
function pctBg(pct: number): string {
  if (pct > CROSS_BENCHMARK_BAD) return 'bg-red-500'
  if (pct > CROSS_BENCHMARK_GOOD) return 'bg-amber-500'
  return 'bg-emerald-500'
}

/* ── Period Options ── */
const PERIOD_OPTIONS = [
  { label: '7 дн', value: 7 },
  { label: '14 дн', value: 14 },
  { label: '30 дн', value: 30 },
  { label: '60 дн', value: 60 },
  { label: '90 дн', value: 90 },
]

/* ── Copy to clipboard helper ── */
function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      onClick={(e) => {
        e.stopPropagation()
        navigator.clipboard.writeText(text)
        setCopied(true)
        setTimeout(() => setCopied(false), 1500)
      }}
      className="inline-flex items-center justify-center h-5 w-5 rounded hover:bg-[hsl(var(--muted)/0.3)] transition-colors shrink-0"
      title={`Копировать ${text}`}
    >
      {copied ? (
        <Check className="h-3 w-3 text-emerald-400" />
      ) : (
        <Copy className="h-3 w-3 text-[hsl(var(--muted-foreground)/0.4)]" />
      )}
    </button>
  )
}

/* ── Product cell ── */
function ProductCell({ sku }: { sku: WBAnalyticsSkuDetail }) {
  return (
    <div className="min-w-0">
      <div className="text-[13px] font-medium text-[hsl(var(--foreground))] leading-snug">
        {sku.name || `Товар #${sku.nm_id}`}
      </div>
      <div className="flex items-center gap-3 mt-0.5">
        {sku.vendor_code && (
          <div className="flex items-center gap-1">
            <span className="text-[11px] font-bold text-[hsl(var(--primary))]">{sku.vendor_code}</span>
            <CopyButton text={sku.vendor_code} />
          </div>
        )}
        <div className="flex items-center gap-1">
          <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">ID: {sku.nm_id}</span>
          <CopyButton text={String(sku.nm_id)} />
        </div>
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   KPI Cards — Redesigned with benchmarks and business metrics
   ═══════════════════════════════════════════════════════════ */
function CrossKpiCards({ data }: { data: WBWarehouseAnalyticsResponse }) {
  const kpi = data.kpi
  const warehouses = data.warehouses

  // Calculate total cross orders and estimated loss
  const totalCrossOrders = warehouses.reduce((s, w) => s + w.cross_orders, 0)
  const crossLoss = totalCrossOrders * CROSS_COST_PER_ORDER

  // Find worst warehouse
  const worstWh = warehouses
    .filter(w => w.orders >= 10)
    .sort((a, b) => b.cross_pct - a.cross_pct)[0]

  // Find worst SKU across all warehouses
  const allSkus = warehouses.flatMap(w =>
    w.skus.map(s => ({ ...s, warehouse: w.warehouse_name }))
  )
  const worstSku = allSkus
    .filter(s => s.orders >= 5)
    .sort((a, b) => b.cross_pct - a.cross_pct)[0]

  // Critical warehouses (cross > 50%)
  const criticalWarehouses = warehouses.filter(w => w.cross_pct > CROSS_BENCHMARK_BAD && w.orders >= 5)

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 }}>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Cross Loss */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <Truck className="h-4 w-4 text-red-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Кросс-потери
            </span>
          </div>
          <div className="text-2xl font-bold tabular-nums text-red-400">{fmtM(crossLoss)}</div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {fmt(totalCrossOrders)} заказов × {CROSS_COST_PER_ORDER}₽
          </div>
        </div>

        {/* Average cross % with benchmark */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <ArrowRightLeft className="h-4 w-4 text-blue-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Средний кросс%
            </span>
          </div>
          <div className={`text-2xl font-bold tabular-nums ${pctColor(kpi.cross_pct)}`}>
            {kpi.cross_pct}%
          </div>
          <div className="flex items-center gap-1.5 mt-0.5">
            <div className={`h-1.5 w-1.5 rounded-full ${kpi.cross_pct <= CROSS_BENCHMARK_GOOD ? 'bg-emerald-400' : kpi.cross_pct <= CROSS_BENCHMARK_BAD ? 'bg-amber-400' : 'bg-red-400'}`} />
            <span className="text-[11px] text-[hsl(var(--muted-foreground))]">
              {kpi.cross_pct <= CROSS_BENCHMARK_GOOD ? 'Норма (≤25%)' : kpi.cross_pct <= CROSS_BENCHMARK_BAD ? 'Выше нормы (25%)' : 'Критично (>50%)'}
            </span>
          </div>
        </div>

        {/* Worst SKU */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <Target className="h-4 w-4 text-amber-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Топ-проблема
            </span>
          </div>
          {worstSku ? (
            <>
              <div className={`text-2xl font-bold tabular-nums ${pctColor(worstSku.cross_pct)}`}>
                {worstSku.cross_pct}% кросс
              </div>
              <div className="text-[11px] text-[hsl(var(--muted-foreground))] truncate" title={worstSku.vendor_code || worstSku.name}>
                {worstSku.vendor_code || `#${worstSku.nm_id}`} • {worstSku.orders} заказов
              </div>
            </>
          ) : (
            <>
              <div className="text-2xl font-bold text-emerald-400">—</div>
              <div className="text-[11px] text-[hsl(var(--muted-foreground))]">Нет проблемных SKU</div>
            </>
          )}
        </div>

        {/* Critical warehouses */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <Warehouse className="h-4 w-4 text-purple-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Склады с кроссом
            </span>
          </div>
          <div className={`text-2xl font-bold tabular-nums ${criticalWarehouses.length > 3 ? 'text-red-400' : criticalWarehouses.length > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
            {criticalWarehouses.length} из {kpi.total_warehouses}
          </div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {worstWh ? `Худший: ${worstWh.warehouse_name} (${worstWh.cross_pct}%)` : 'Все в норме'}
          </div>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Cross-Map Table — with toggle by warehouses / by products
   ═══════════════════════════════════════════════════════════ */
type CrossMapMode = 'warehouses' | 'products'

function CrossMapSection({ data }: { data: WBWarehouseAnalyticsResponse }) {
  const [mode, setMode] = useState<CrossMapMode>('warehouses')
  const { crossMap, okrugList } = { crossMap: data.cross_map, okrugList: data.okrug_list }

  // Build product-level cross data
  const productCrossData = useMemo(() => {
    const skuMap: Record<number, {
      nm_id: number; vendor_code: string; name: string;
      totalOrders: number; crossOrders: number; crossPct: number;
      topFlows: { from: string; to: string; count: number }[];
    }> = {}

    for (const wh of data.warehouses) {
      for (const sku of wh.skus) {
        if (!skuMap[sku.nm_id]) {
          skuMap[sku.nm_id] = {
            nm_id: sku.nm_id, vendor_code: sku.vendor_code, name: sku.name,
            totalOrders: 0, crossOrders: 0, crossPct: 0, topFlows: [],
          }
        }
        const entry = skuMap[sku.nm_id]
        entry.totalOrders += sku.orders
        entry.crossOrders += sku.cross_orders

        // Track cross flows
        for (const geo of sku.geography) {
          if (!geo.is_local && geo.orders > 0) {
            entry.topFlows.push({
              from: wh.warehouse_name,
              to: geo.okrug.replace(' федеральный округ', ''),
              count: geo.orders,
            })
          }
        }
      }
    }

    return Object.values(skuMap)
      .map(s => ({
        ...s,
        crossPct: s.totalOrders > 0 ? Math.round(s.crossOrders / s.totalOrders * 100 * 10) / 10 : 0,
        topFlows: s.topFlows.sort((a, b) => b.count - a.count).slice(0, 3),
      }))
      .filter(s => s.totalOrders >= 3)
      .sort((a, b) => b.crossOrders - a.crossOrders)
  }, [data.warehouses])

  const shortOkrug = (s: string) => s.replace(' федеральный округ', '').replace('Северо-', 'С-')

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.15, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-5 border-b border-[hsl(var(--border))] flex items-center justify-between">
          <div>
            <h2 className="text-xl font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
              <ArrowRightLeft className="h-5 w-5 text-blue-400" />
              Кросс-карта
            </h2>
            <p className="text-[12px] text-[hsl(var(--muted-foreground))] mt-1">
              {mode === 'warehouses'
                ? <>Откуда отгружается → куда доставляется. <span className="text-emerald-400 font-medium">Зелёный</span> = свой округ, <span className="text-red-400 font-medium">красный</span> = кросс-отправка</>
                : 'Какие товары генерируют кросс-отправки и куда'
              }
            </p>
          </div>
          {/* Toggle */}
          <div className="flex gap-1 bg-[hsl(var(--muted)/0.15)] p-1 rounded-lg shrink-0">
            <button
              className={`px-3 py-1.5 rounded-md text-[12px] font-semibold transition-all ${
                mode === 'warehouses'
                  ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
                  : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
              }`}
              onClick={() => setMode('warehouses')}
            >
              <Warehouse className="h-3.5 w-3.5 inline mr-1.5" />По складам
            </button>
            <button
              className={`px-3 py-1.5 rounded-md text-[12px] font-semibold transition-all ${
                mode === 'products'
                  ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
                  : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
              }`}
              onClick={() => setMode('products')}
            >
              <Package className="h-3.5 w-3.5 inline mr-1.5" />По товарам
            </button>
          </div>
        </div>

        {mode === 'warehouses' ? (
          /* ── Warehouse × Okrug Matrix ── */
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
                          <span className={`inline-flex items-center justify-center min-w-[28px] px-1.5 py-0.5 rounded-md text-[12px] font-bold tabular-nums ${
                            cell.is_local
                              ? 'bg-emerald-500/15 text-emerald-400'
                              : `bg-red-500/${Math.round(10 + intensity * 20)} text-red-400`
                          }`}>
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
        ) : (
          /* ── Product Cross Table ── */
          <div className="overflow-auto max-h-[500px]">
            <table className="w-full border-collapse text-[12px]">
              <thead className="sticky top-0 bg-[hsl(var(--card))] z-10">
                <tr className="border-b border-[hsl(var(--border)/0.3)]">
                  <th className="px-4 py-3 text-left font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase">Товар</th>
                  <th className="px-3 py-3 text-center font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase w-[80px]">Заказов</th>
                  <th className="px-3 py-3 text-center font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase w-[80px]">Кросс</th>
                  <th className="px-3 py-3 text-center font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase w-[80px]">Кросс%</th>
                  <th className="px-4 py-3 text-left font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase">Откуда → Куда (топ-3)</th>
                </tr>
              </thead>
              <tbody>
                {productCrossData.slice(0, 50).map((sku, idx) => (
                  <tr
                    key={sku.nm_id}
                    className={`border-b border-[hsl(var(--border)/0.1)] ${idx % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''}`}
                  >
                    <td className="px-4 py-3 text-left">
                      <div className="min-w-0">
                        <div className="text-[13px] font-medium text-[hsl(var(--foreground))] leading-snug">
                          {sku.name || `Товар #${sku.nm_id}`}
                        </div>
                        <div className="flex items-center gap-3 mt-0.5">
                          {sku.vendor_code && (
                            <div className="flex items-center gap-1">
                              <span className="text-[11px] font-bold text-[hsl(var(--primary))]">{sku.vendor_code}</span>
                              <CopyButton text={sku.vendor_code} />
                            </div>
                          )}
                          <div className="flex items-center gap-1">
                            <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)]">ID: {sku.nm_id}</span>
                            <CopyButton text={String(sku.nm_id)} />
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="px-3 py-3 text-center tabular-nums text-[13px]">{fmt(sku.totalOrders)}</td>
                    <td className="px-3 py-3 text-center tabular-nums text-[13px] text-red-400 font-semibold">{fmt(sku.crossOrders)}</td>
                    <td className="px-3 py-3 text-center">
                      <span className={`text-[13px] font-bold tabular-nums ${pctColor(sku.crossPct)}`}>
                        {sku.crossPct}%
                      </span>
                    </td>
                    <td className="px-4 py-3 text-left">
                      <div className="space-y-0.5">
                        {sku.topFlows.length > 0 ? sku.topFlows.map((flow, fi) => (
                          <div key={fi} className="flex items-center gap-1.5 text-[11px]">
                            <span className="font-medium text-[hsl(var(--foreground)/0.7)]">{flow.from}</span>
                            <span className="text-[hsl(var(--muted-foreground)/0.4)]">→</span>
                            <span className="text-red-400 font-medium">{flow.to}</span>
                            <span className="text-[hsl(var(--muted-foreground)/0.5)] tabular-nums">({flow.count})</span>
                          </div>
                        )) : (
                          <span className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-40">—</span>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   SKU Geography Expanded Panel — with progress bars
   ═══════════════════════════════════════════════════════════ */
function SkuGeographyPanel({ sku, warehouseName }: { sku: WBAnalyticsSkuDetail; warehouseName: string }) {
  if (sku.geography.length === 0) return null

  const maxOrders = Math.max(...sku.geography.map(g => g.orders), 1)
  const crossGeo = sku.geography.filter(g => !g.is_local)
  const topCrossOkrugs = crossGeo.slice(0, 3).map(g => g.okrug.replace(' федеральный округ', ''))
  const totalCrossOrders = crossGeo.reduce((s, g) => s + g.orders, 0)
  const estimatedSavings = totalCrossOrders * CROSS_COST_PER_ORDER

  return (
    <motion.div
      initial={{ height: 0, opacity: 0 }}
      animate={{ height: 'auto', opacity: 1 }}
      exit={{ height: 0, opacity: 0 }}
      transition={{ duration: 0.2 }}
      className="overflow-hidden"
    >
      <div className="px-6 py-4 bg-[hsl(var(--muted)/0.04)] border-t border-[hsl(var(--border)/0.15)]">
        <div className="flex items-center gap-2 mb-3">
          <MapPin className="h-4 w-4 text-emerald-400" />
          <span className="text-[13px] font-bold text-[hsl(var(--foreground))]">
            География «{sku.vendor_code || sku.name?.slice(0, 25)}» с {warehouseName}
          </span>
        </div>

        {/* Progress bar geography */}
        <div className="space-y-2 max-w-[600px]">
          {sku.geography.map((g) => {
            const barWidth = Math.max((g.orders / maxOrders) * 100, 4)
            return (
              <div key={g.okrug} className="flex items-center gap-3">
                <span className="text-[12px] font-medium w-[140px] truncate shrink-0">
                  {g.okrug.replace(' федеральный округ', '')}
                </span>
                <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold shrink-0 ${
                  g.is_local ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/10 text-red-400'
                }`}>
                  {g.is_local ? 'СВОЙ' : 'КРОСС'}
                </span>
                <div className="flex-1 h-2 rounded-full bg-[hsl(var(--muted)/0.15)] overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${g.is_local ? 'bg-emerald-500' : 'bg-red-500/60'}`}
                    style={{ width: `${barWidth}%` }}
                  />
                </div>
                <span className="text-[12px] tabular-nums font-medium w-[65px] text-right shrink-0">
                  {g.orders} ({g.share}%)
                </span>
              </div>
            )
          })}
        </div>

        {/* Auto-recommendation if cross > 40% */}
        {sku.cross_pct > 40 && topCrossOkrugs.length > 0 && (
          <div className="mt-3 p-3 rounded-lg bg-amber-500/6 border border-amber-500/15">
            <div className="flex items-start gap-2">
              <TrendingUp className="h-4 w-4 text-amber-400 mt-0.5 shrink-0" />
              <div>
                <p className="text-[12px] font-semibold text-amber-300">
                  Рекомендация: довезти на {topCrossOkrugs.join(', ')}
                </p>
                <p className="text-[11px] text-[hsl(var(--muted-foreground))] mt-0.5">
                  {totalCrossOrders} кросс-заказов → потенциальная экономия ~{fmtM(estimatedSavings)}
                </p>
              </div>
            </div>
          </div>
        )}
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Warehouse Expanded Detail — Redesigned with full product info
   ═══════════════════════════════════════════════════════════ */
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
        <div className="grid grid-cols-1 lg:grid-cols-[1fr_300px] gap-6">
          {/* SKU Table */}
          <div>
            <h4 className="text-[13px] font-bold text-[hsl(var(--foreground))] mb-3 flex items-center gap-2">
              <Package className="h-4 w-4 text-blue-400" />
              Товары на складе ({wh.skus.length})
              <span className="text-[10px] font-normal text-[hsl(var(--muted-foreground))]">• клик — география</span>
            </h4>
            <div className="rounded-lg border border-[hsl(var(--border)/0.2)] overflow-hidden max-h-[500px] overflow-y-auto">
              <table className="w-full text-[12px]">
                <thead className="sticky top-0 bg-[hsl(var(--card))] z-10">
                  <tr className="border-b border-[hsl(var(--border)/0.2)]">
                    <th className="px-3 py-2 text-left font-semibold text-[hsl(var(--muted-foreground))]">Товар</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))] w-[60px]">Ост.</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))] w-[60px]">Заказов</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))] w-[65px]">Кросс%</th>
                    <th className="px-3 py-2 text-center font-semibold text-[hsl(var(--muted-foreground))] w-[55px]">Дн.зап.</th>
                  </tr>
                </thead>
                <tbody>
                  {wh.skus.slice(0, 40).map((sku) => {
                    const isSelected = selectedSku === sku.nm_id
                    return (
                      <React.Fragment key={sku.nm_id}>
                        <tr
                          className={`border-b border-[hsl(var(--border)/0.1)] cursor-pointer transition-colors ${
                            isSelected ? 'bg-[hsl(var(--primary)/0.08)]' : 'hover:bg-[hsl(var(--muted)/0.06)]'
                          }`}
                          onClick={() => setSelectedSku(isSelected ? null : sku.nm_id)}
                        >
                          <td className="px-3 py-2">
                            <ProductCell sku={sku} />
                          </td>
                          <td className="px-3 py-2 text-center tabular-nums">{fmt(sku.stock)}</td>
                          <td className="px-3 py-2 text-center tabular-nums">{fmt(sku.orders)}</td>
                          <td className="px-3 py-2 text-center">
                            {sku.orders > 0 ? (
                              <span className={`text-[11px] font-semibold tabular-nums ${pctColor(sku.cross_pct)}`}>
                                {sku.cross_pct}%
                              </span>
                            ) : <span className="text-[hsl(var(--muted-foreground))] opacity-40">—</span>}
                          </td>
                          <td className="px-3 py-2 text-center tabular-nums">
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
                        <AnimatePresence>
                          {isSelected && sku.geography.length > 0 && (
                            <tr>
                              <td colSpan={5} className="p-0">
                                <SkuGeographyPanel sku={sku} warehouseName={wh.warehouse_name} />
                              </td>
                            </tr>
                          )}
                        </AnimatePresence>
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
              <div className="space-y-2.5">
                {wh.geography.map((g) => {
                  const maxShare = Math.max(...wh.geography.map(x => x.share), 1)
                  const barWidth = Math.max((g.share / maxShare) * 100, 4)
                  return (
                    <div key={g.okrug}>
                      <div className="flex items-center gap-2 mb-1">
                        <span className="text-[12px] font-medium truncate flex-1 min-w-0">
                          {g.okrug.replace(' федеральный округ', '')}
                        </span>
                        <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold ${
                          g.is_local ? 'bg-emerald-500/15 text-emerald-400' : 'bg-red-500/10 text-red-400'
                        }`}>
                          {g.is_local ? 'СВОЙ' : 'КРОСС'}
                        </span>
                        <span className="text-[12px] tabular-nums font-medium w-14 text-right">
                          {g.orders} ({g.share}%)
                        </span>
                      </div>
                      <div className="w-full h-1.5 rounded-full bg-[hsl(var(--muted)/0.15)] overflow-hidden">
                        <div
                          className={`h-full rounded-full ${g.is_local ? 'bg-emerald-500' : 'bg-red-500/60'}`}
                          style={{ width: `${barWidth}%` }}
                        />
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <p className="text-[12px] text-[hsl(var(--muted-foreground))] opacity-50">Нет данных за период</p>
            )}

            {/* Warehouse-level recommendation */}
            {wh.cross_pct > 40 && (
              <div className="mt-4 p-3 rounded-lg bg-amber-500/6 border border-amber-500/15">
                <div className="flex items-start gap-2">
                  <TrendingUp className="h-4 w-4 text-amber-400 mt-0.5 shrink-0" />
                  <div>
                    <p className="text-[12px] font-semibold text-amber-300">
                      Кросс {wh.cross_pct}% — выше нормы
                    </p>
                    <p className="text-[11px] text-[hsl(var(--muted-foreground))] mt-0.5">
                      ~{fmtM(wh.cross_orders * CROSS_COST_PER_ORDER)} потерь.
                      Рассмотрите довоз товаров в{' '}
                      {wh.geography
                        .filter(g => !g.is_local)
                        .slice(0, 2)
                        .map(g => g.okrug.replace(' федеральный округ', ''))
                        .join(', ')}
                    </p>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </motion.div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Warehouses with cross details table
   ═══════════════════════════════════════════════════════════ */
function CrossWarehousesTable({ warehouses }: { warehouses: WBAnalyticsWarehouse[] }) {
  const [expandedWh, setExpandedWh] = useState<string | null>(null)
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

        <div className="overflow-auto max-h-[700px]">
          <table className="w-full border-collapse" style={{ minWidth: 750 }}>
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-2 py-3 w-[32px]"></th>
                <th className="px-4 py-3 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">Склад</th>
                <th className="px-3 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">Заказов</th>
                <th className="px-3 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">Кросс%</th>
                <th className="px-3 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">Потери</th>
                <th className="px-3 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">Оборач.</th>
                <th className="px-3 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">SKU</th>
              </tr>
            </thead>
            <tbody>
              {crossWarehouses.map((wh, idx) => {
                const isExpanded = expandedWh === wh.warehouse_name
                const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.04)]'
                const loss = wh.cross_orders * CROSS_COST_PER_ORDER
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
                        <span className={`text-[13px] font-bold tabular-nums ${pctColor(wh.cross_pct)}`}>{wh.cross_pct}%</span>
                      </td>
                      <td className="px-3 py-3 text-center">
                        {loss > 0 ? (
                          <span className="text-[12px] font-semibold text-red-400 tabular-nums">~{fmtM(loss)}</span>
                        ) : (
                          <span className="text-[hsl(var(--muted-foreground))] opacity-40">—</span>
                        )}
                      </td>
                      <td className="px-3 py-3 text-center tabular-nums text-[13px]">{fmtD(wh.turnover_days)}</td>
                      <td className="px-3 py-3 text-center tabular-nums text-[13px] text-[hsl(var(--muted-foreground))]">{wh.sku_count}</td>
                    </tr>
                    {isExpanded && (
                      <tr>
                        <td colSpan={7} className="p-0">
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
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-[100px] rounded-2xl" />
        ))}
      </div>
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
            Анализ кросс-отправок, потери и рекомендации по оптимизации размещения товаров
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
          <CrossKpiCards data={data} />

          {/* Cross-map with toggle */}
          <CrossMapSection data={data} />

          {/* Warehouses with cross details */}
          <CrossWarehousesTable warehouses={data.warehouses} />
        </>
      ) : null}
    </div>
  )
}
