/**
 * Warehouses Cross-logistics — Redesigned cross-logistics analysis.
 * Features:
 * - Smart KPI cards with benchmarks and loss estimates
 * - Cross-map with toggle: by warehouse / by product
 * - Unified product display (vendor_code + nm_id + copy)
 * - Expanded geography panel with progress bars
 * - Auto-recommendations for high-cross SKUs
 *
 * Supports both WB and Ozon marketplaces.
 */
import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  ArrowRightLeft,
  RefreshCw,
  AlertTriangle,
  MapPin,
  Package,
  ChevronRight,
  ChevronDown,
  Warehouse,
  Copy,
  Check,
  TrendingUp,
  Truck,
  Target,
  PackageX,
  Brain,
  Sparkles,
  X,
  Lightbulb,
  Zap,
  ArrowRight,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBWarehouseAnalytics,
  getOzonWarehouseAnalytics,
  getOzonCrossAIAnalysis,
  type WBWarehouseAnalyticsResponse,
  type WBAnalyticsWarehouse,
  type WBAnalyticsSkuDetail,
  type WarehouseAnalyticsResponse,
  type OzonCrossAIAnalysis,
} from '@/api/warehouses'

/* ── Constants ── */
// Cross cost is calculated per-warehouse: logistics_cost × (cross_orders / orders)
// WB does NOT split logistics cost into cross/local in financial reports
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

  // Calculate cross cost per-warehouse: logistics_cost × (cross_orders / orders)
  // This uses REAL logistics costs from fact_finances
  let crossCost = 0
  let totalCrossOrders = 0
  warehouses.forEach(w => {
    totalCrossOrders += w.cross_orders
    if (w.orders > 0 && w.logistics_cost > 0) {
      crossCost += w.logistics_cost * (w.cross_orders / w.orders)
    }
  })
  crossCost = Math.round(crossCost)

  // Find worst warehouse
  const worstWh = warehouses
    .filter(w => w.orders >= 10)
    .sort((a, b) => b.cross_pct - a.cross_pct)[0]

  // Find problematic SKUs across all warehouses (cross > 40%)
  const allSkus = warehouses.flatMap(w =>
    w.skus.map(s => ({ ...s, warehouse: w.warehouse_name }))
  )
  const problemSkus = allSkus
    .filter(s => s.orders >= 5 && s.cross_pct > 40)
    .sort((a, b) => b.cross_pct - a.cross_pct)

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
              Кросс-логистика
            </span>
          </div>
          <div className="text-2xl font-bold tabular-nums text-red-400">≈ {fmtM(crossCost)}</div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {fmt(totalCrossOrders)} кросс-заказов • доля от логистики
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

        {/* Problem SKUs count */}
        <div className="p-4 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="flex items-center gap-2 mb-1">
            <Target className="h-4 w-4 text-amber-400" />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
              Проблемных SKU
            </span>
          </div>
          <div className={`text-2xl font-bold tabular-nums ${problemSkus.length > 5 ? 'text-red-400' : problemSkus.length > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
            {problemSkus.length}
          </div>
          <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
            {problemSkus.length > 0 ? `Кросс > 40%, ≥5 заказов` : 'Все SKU в норме ✓'}
          </div>
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
   Top Problem SKUs Block — shows all SKUs with cross > 40%
   ═══════════════════════════════════════════════════════════ */
function TopProblemSkus({ data }: { data: WBWarehouseAnalyticsResponse }) {
  const allSkus = data.warehouses.flatMap(w =>
    w.skus.map(s => ({ ...s, warehouse: w.warehouse_name, whOkrug: w.okrug }))
  )
  const problemSkus = allSkus
    .filter(s => s.orders >= 5 && s.cross_pct > 40)
    .sort((a, b) => b.cross_pct - a.cross_pct)

  const [expanded, setExpanded] = useState(false)

  if (problemSkus.length === 0) return null

  const shown = expanded ? problemSkus : problemSkus.slice(0, 5)

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1 }}>
      <div className="rounded-2xl border border-amber-500/20 bg-[hsl(var(--card))] overflow-hidden">
        <div className="px-6 py-4 border-b border-[hsl(var(--border))] flex items-center justify-between">
          <h2 className="text-lg font-bold text-[hsl(var(--foreground))] flex items-center gap-2">
            <Target className="h-5 w-5 text-amber-400" />
            Топ-проблемные SKU
            <span className="text-[12px] font-medium text-amber-400 px-2 py-0.5 rounded-full bg-amber-500/10">
              {problemSkus.length}
            </span>
          </h2>
          <span className="text-[12px] text-[hsl(var(--muted-foreground))]">
            Кросс &gt; 40% • ≥ 5 заказов за период
          </span>
        </div>

        <div className="overflow-auto">
          <table className="w-full border-collapse text-[12px]">
            <thead>
              <tr className="border-b border-[hsl(var(--border)/0.2)]">
                <th className="px-4 py-2.5 text-left font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase">Товар</th>
                <th className="px-3 py-2.5 text-left font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase w-[140px]">Склад</th>
                <th className="px-3 py-2.5 text-center font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase w-[70px]">Заказов</th>
                <th className="px-3 py-2.5 text-center font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase w-[70px]">Кросс%</th>
                <th className="px-3 py-2.5 text-center font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase w-[90px]">Потери</th>
                <th className="px-4 py-2.5 text-left font-semibold text-[hsl(var(--muted-foreground))] text-[11px] uppercase">Куда довезти</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((sku, idx) => {
                // Find the warehouse to get its logistics_cost for proportional calc
                const whData = data.warehouses.find(w => w.warehouse_name === (sku as any).warehouse)
                const skuLoss = whData && whData.orders > 0 && whData.logistics_cost > 0
                  ? Math.round(whData.logistics_cost * (sku.cross_orders / whData.orders))
                  : 0
                const topCrossOkrugs = sku.geography
                  .filter(g => !g.is_local)
                  .slice(0, 2)
                  .map(g => g.okrug.replace(' федеральный округ', ''))
                return (
                  <tr key={`${sku.nm_id}-${(sku as any).warehouse}-${idx}`}
                    className={`border-b border-[hsl(var(--border)/0.1)] ${idx % 2 ? 'bg-[hsl(var(--muted)/0.03)]' : ''}`}
                  >
                    <td className="px-4 py-2.5">
                      <ProductCell sku={sku} />
                    </td>
                    <td className="px-3 py-2.5 text-[12px] text-[hsl(var(--muted-foreground))]">{(sku as any).warehouse}</td>
                    <td className="px-3 py-2.5 text-center tabular-nums text-[13px]">{fmt(sku.orders)}</td>
                    <td className="px-3 py-2.5 text-center">
                      <span className={`text-[13px] font-bold tabular-nums ${pctColor(sku.cross_pct)}`}>
                        {sku.cross_pct}%
                      </span>
                    </td>
                    <td className="px-3 py-2.5 text-center">
                      {skuLoss > 0 ? (
                        <span className="text-[12px] font-semibold text-red-400 tabular-nums" title="Оценка: доля от реальной логистики склада">≈ {fmtM(skuLoss)}</span>
                      ) : (
                        <span className="text-[hsl(var(--muted-foreground))] opacity-40">—</span>
                      )}
                    </td>
                    <td className="px-4 py-2.5">
                      {topCrossOkrugs.length > 0 ? (
                        <span className="text-[12px] text-[hsl(var(--foreground)/0.7)]">
                          {topCrossOkrugs.join(', ')}
                        </span>
                      ) : (
                        <span className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-40">—</span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>

        {problemSkus.length > 5 && (
          <button
            onClick={() => setExpanded(!expanded)}
            className="w-full px-6 py-2.5 text-center text-[12px] font-semibold text-[hsl(var(--primary))] hover:bg-[hsl(var(--muted)/0.08)] transition-colors border-t border-[hsl(var(--border)/0.15)] flex items-center justify-center gap-1.5"
          >
            {expanded ? (
              <><ChevronDown className="h-3.5 w-3.5" /> Свернуть</>
            ) : (
              <><ChevronRight className="h-3.5 w-3.5" /> Показать все {problemSkus.length} SKU</>
            )}
          </button>
        )}
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
                    Склад ↓ / Регион →
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
                  {totalCrossOrders} кросс-заказов за период
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
              {wh.skus.length > 0 ? (
                <>Товары на складе ({wh.skus.length}) <span className="text-[10px] font-normal text-[hsl(var(--muted-foreground))]">• клик — география</span></>
              ) : (
                <>Товары на складе</>
              )}
            </h4>
            {/* Empty state for warehouses with orders but no current stock */}
            {wh.skus.length === 0 && (
              <div className="rounded-lg border border-[hsl(var(--border)/0.2)] bg-[hsl(var(--muted)/0.04)] p-6 text-center">
                <PackageX className="h-8 w-8 mx-auto text-[hsl(var(--muted-foreground)/0.3)] mb-2" />
                <p className="text-[13px] font-medium text-[hsl(var(--foreground)/0.7)]">
                  Остатки на складе закончились
                </p>
                <p className="text-[11px] text-[hsl(var(--muted-foreground))] mt-1">
                  За выбранный период с этого склада был{wh.orders === 1 ? '' : 'о'} отгружен{wh.orders === 1 ? '' : 'о'} {fmt(wh.orders)} заказ{wh.orders === 1 ? '' : wh.orders < 5 ? 'а' : 'ов'},
                  но текущий остаток = 0. Заказы кросс-доставлялись с других складов.
                </p>
              </div>
            )}
            {wh.skus.length > 0 && <div className="rounded-lg border border-[hsl(var(--border)/0.2)] overflow-hidden max-h-[500px] overflow-y-auto">
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
            </div>}
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
                      {wh.cross_orders} кросс-заказов из {wh.orders}.
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
                const whCrossCost = wh.orders > 0 && wh.logistics_cost > 0
                  ? Math.round(wh.logistics_cost * (wh.cross_orders / wh.orders))
                  : 0
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
                        {whCrossCost > 0 ? (
                          <span className="text-[12px] font-semibold text-red-400 tabular-nums" title="Доля от реальной логистики склада">≈ {fmtM(whCrossCost)}</span>
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

/* ── Normalize Ozon analytics → WB cross-logistics format ── */
function normalizeOzonToCrossData(ozon: WarehouseAnalyticsResponse): WBWarehouseAnalyticsResponse {
  const warehouses: WBAnalyticsWarehouse[] = ozon.warehouses.map(w => ({
    warehouse_name: w.warehouse_name,
    okrug: w.cluster,
    warehouse_type: 'normal' as const,
    status: w.status === 'storage_fee' ? 'overstocked' as const : w.status as any,
    stock: w.stock_free,
    sku_count: w.sku_count,
    orders: w.orders_period,
    revenue: w.revenue_period,
    daily_sales: w.daily_sales,
    turnover_days: w.turnover_days,
    pct_of_total_sales: w.pct_of_total_sales,
    cross_pct: w.cross_pct ?? 0,
    cross_orders: w.cross_orders ?? 0,
    local_orders: w.local_orders ?? 0,
    logistics_cost: Math.abs(w.costs.crossdocking) + Math.abs(w.costs.fbo_processing),
    logistics_count: w.costs.crossdocking_cnt + w.costs.fbo_cnt,
    storage_coef: 0,
    storage_cost_actual: Math.abs(w.costs.storage),
    storage_cost_month: 0,
    acceptance_coef: 0,
    acceptance: '—',
    skus: w.skus.map(s => ({
      nm_id: s.sku,
      vendor_code: s.offer_id,
      name: s.name,
      stock: s.stock,
      daily_sales: s.daily_sales,
      days_supply: s.days_supply,
      orders: s.orders ?? 0,
      cross_orders: s.cross_orders ?? 0,
      cross_pct: s.cross_pct ?? 0,
      geography: (s.geography ?? []).map(g => ({
        okrug: g.cluster,
        orders: g.orders,
        share: g.share,
        is_local: g.is_local,
      })),
    })),
    geography: w.clusters_served.map(cs => ({
      okrug: cs.cluster,
      orders: cs.orders,
      share: cs.share ?? 0,
      is_local: cs.is_local ?? false,
    })),
  }))

  const cross_map = (ozon.cross_map ?? []).map(row => ({
    warehouse: row.warehouse,
    home_okrug: row.home_cluster,
    total_orders: row.total_orders,
    okrugs: Object.fromEntries(
      Object.entries(row.clusters).map(([k, v]) => [k, v])
    ),
  }))

  return {
    kpi: {
      total_warehouses: ozon.kpi.total_warehouses,
      total_stock: ozon.kpi.total_stock,
      total_sku: ozon.kpi.total_skus,
      avg_turnover_days: ozon.kpi.avg_turnover_days,
      total_logistics: Math.abs(ozon.kpi.total_crossdocking) + Math.abs(ozon.kpi.total_fbo_processing),
      total_storage: Math.abs(ozon.kpi.total_storage_fee),
      total_storage_actual: null,
      total_penalties: 0,
      cross_pct: ozon.kpi.cross_pct ?? 0,
      total_orders: warehouses.reduce((s, w) => s + w.orders, 0),
      period_days: ozon.kpi.period_days,
      has_actual_storage: false,
      forecast_30d: null,
    },
    warehouses,
    products_summary: [],
    cross_map,
    okrug_list: ozon.cluster_list ?? [],
    costs: [],
    storage_skus: [],
    recommendations: [],
    period_days: ozon.kpi.period_days,
  }
}

/* ═══════════════════════════════════════════════════════════
   Ozon Cross AI Analysis — Overview Banner + Modal
   ═══════════════════════════════════════════════════════════ */
function OzonCrossAIInsight({ shopId, period }: { shopId: number; period: number }) {
  const [data, setData] = useState<OzonCrossAIAnalysis | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [modalOpen, setModalOpen] = useState(false)
  const [refreshing, setRefreshing] = useState(false)

  const fetchAI = useCallback(async (force = false) => {
    if (force) setRefreshing(true)
    else setLoading(true)
    setError(null)
    try {
      const result = await getOzonCrossAIAnalysis({ shop_id: shopId, period, force })
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка ИИ-анализа')
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [shopId, period])

  useEffect(() => { fetchAI() }, [fetchAI])
  useEffect(() => {
    if (modalOpen) {
      document.body.style.overflow = 'hidden'
      return () => { document.body.style.overflow = '' }
    }
  }, [modalOpen])

  const severityConfig = {
    critical: { bg: 'from-red-500/10 to-red-500/5', border: 'border-red-500/30', icon: '🔴', label: 'Критично', bannerBorder: 'border-red-500/25' },
    warning:  { bg: 'from-amber-500/10 to-amber-500/5', border: 'border-amber-500/30', icon: '🟡', label: 'Внимание', bannerBorder: 'border-amber-500/25' },
    ok:       { bg: 'from-emerald-500/10 to-emerald-500/5', border: 'border-emerald-500/30', icon: '🟢', label: 'Всё ОК', bannerBorder: 'border-emerald-500/25' },
  }
  const whStatusCfg = {
    critical: { color: 'text-red-400', bg: 'bg-red-500/10', border: 'border-red-500/20', label: 'Критично' },
    warning:  { color: 'text-amber-400', bg: 'bg-amber-500/10', border: 'border-amber-500/20', label: 'Внимание' },
    ok:       { color: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/20', label: 'Норма' },
  }

  if (loading && !data) {
    return (
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
        <div className="flex items-center gap-3 px-4 py-3 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-purple-600 to-blue-500 flex items-center justify-center">
            <Brain className="h-4 w-4 text-white animate-pulse" />
          </div>
          <Skeleton className="h-4 w-48" />
          <Skeleton className="h-4 w-24 ml-auto" />
        </div>
      </motion.div>
    )
  }

  if (error) {
    return (
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}>
        <div className="flex items-center gap-3 px-4 py-3 rounded-xl border border-red-500/20 bg-[hsl(var(--card))]">
          <AlertTriangle className="h-4 w-4 text-red-400 shrink-0" />
          <span className="text-[13px] text-[hsl(var(--muted-foreground))]">{error}</span>
          <button onClick={() => fetchAI(true)} className="ml-auto text-[13px] font-medium text-[hsl(var(--primary))] hover:underline">Повторить</button>
        </div>
      </motion.div>
    )
  }

  if (!data) return null

  const sev = severityConfig[data.severity] || severityConfig.warning
  const analyzedAtStr = data.analyzed_at
    ? `Анализ от ${new Date(data.analyzed_at * 1000).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' })}`
    : null
  const km = data.key_metrics || { cross_pct: 0, cross_orders: 0, total_orders: 0, warehouses_with_cross: 0, skus_with_high_cross: 0 }
  const problemCount = data.problem_skus?.length || 0

  return (
    <>
      {/* ═══ Compact Banner ═══ */}
      <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.12 }}>
        <div className={`flex items-center gap-3 px-4 py-3 rounded-xl border ${sev.bannerBorder} bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted)/0.08)] transition-colors`}>
          <div className="h-8 w-8 rounded-lg bg-gradient-to-br from-purple-600 to-blue-500 shadow-md shadow-purple-500/20 flex items-center justify-center shrink-0">
            <Sparkles className="h-4 w-4 text-white" />
          </div>
          <div className="flex items-center gap-2 min-w-0 flex-1">
            <span className="text-sm">{sev.icon}</span>
            <span className="text-[13px] font-semibold text-[hsl(var(--foreground))]">{sev.label}</span>
            <span className="text-[13px] text-[hsl(var(--muted-foreground))] truncate hidden sm:inline">
              · {data.diagnosis?.substring(0, 100)}{(data.diagnosis?.length || 0) > 100 ? '…' : ''}
            </span>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            {problemCount > 0 && (
              <span className="text-[11px] font-semibold px-2 py-0.5 rounded-full bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))]">
                {problemCount} SKU
              </span>
            )}
            {analyzedAtStr && (
              <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.5)] hidden md:inline">{analyzedAtStr}</span>
            )}
            <button onClick={(e) => { e.stopPropagation(); fetchAI(true) }} disabled={refreshing}
              className="p-1.5 rounded-lg hover:bg-[hsl(var(--muted)/0.3)] transition-colors" title="Обновить анализ">
              <RefreshCw className={`h-3.5 w-3.5 text-[hsl(var(--muted-foreground))] ${refreshing ? 'animate-spin' : ''}`} />
            </button>
            <button onClick={() => setModalOpen(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-[hsl(var(--primary))] text-white text-[12px] font-semibold hover:opacity-90 transition-opacity">
              <Brain className="h-3.5 w-3.5" />
              Прочитать
            </button>
          </div>
        </div>
      </motion.div>

      {/* ═══ Full-screen Modal ═══ */}
      <AnimatePresence>
        {modalOpen && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }}
            className="fixed inset-0 z-[100] flex items-start justify-center bg-black/60 backdrop-blur-sm"
            onClick={() => setModalOpen(false)}>
            <motion.div initial={{ opacity: 0, y: 40, scale: 0.97 }} animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: 40, scale: 0.97 }} transition={{ duration: 0.25, ease: 'easeOut' }}
              className="w-full max-w-[1100px] max-h-[90vh] mt-[5vh] mx-4 rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-2xl overflow-hidden flex flex-col"
              onClick={(e) => e.stopPropagation()}>

              {/* Header */}
              <div className={`px-8 py-5 bg-gradient-to-r ${sev.bg} border-b border-[hsl(var(--border)/0.3)] shrink-0`}>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="h-12 w-12 rounded-xl bg-gradient-to-br from-purple-600 to-blue-500 shadow-lg shadow-purple-500/25 flex items-center justify-center">
                      <Sparkles className="h-6 w-6 text-white" />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <h3 className="text-lg font-bold text-[hsl(var(--foreground))]">Обзор кросс-логистики</h3>
                        <span className="text-sm">{sev.icon}</span>
                        <span className="text-sm font-semibold text-[hsl(var(--muted-foreground))]">{sev.label}</span>
                      </div>
                      <p className="text-[15px] text-[hsl(var(--muted-foreground))] mt-1 leading-relaxed max-w-[700px]">{data.diagnosis}</p>
                    </div>
                  </div>
                  <button onClick={() => setModalOpen(false)} className="p-2.5 rounded-xl hover:bg-[hsl(var(--muted)/0.3)] transition-colors">
                    <X className="h-6 w-6 text-[hsl(var(--muted-foreground))]" />
                  </button>
                </div>
              </div>

              {/* Body */}
              <div className="flex-1 overflow-y-auto px-8 py-6 space-y-8">
                {/* 4 metrics */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-5">
                  <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <ArrowRightLeft className="h-5 w-5 text-red-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Кросс %</span>
                    </div>
                    <p className={`text-2xl font-bold ${km.cross_pct > 40 ? 'text-red-400' : km.cross_pct > 20 ? 'text-amber-400' : 'text-emerald-400'}`}>{km.cross_pct}%</p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">{fmt(km.cross_orders)} из {fmt(km.total_orders)}</p>
                  </div>
                  <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <Target className="h-5 w-5 text-amber-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Проблемных SKU</span>
                    </div>
                    <p className={`text-2xl font-bold ${km.skus_with_high_cross > 5 ? 'text-red-400' : km.skus_with_high_cross > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>{km.skus_with_high_cross}</p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">кросс &gt;30%</p>
                  </div>
                  <div className="rounded-xl bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)] p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <Warehouse className="h-5 w-5 text-purple-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground)/0.6)]">Складов с кроссом</span>
                    </div>
                    <p className="text-2xl font-bold text-[hsl(var(--foreground))]">{km.warehouses_with_cross}</p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">из {data.context?.warehouses_count ?? '?'}</p>
                  </div>
                  <div className="rounded-xl bg-purple-500/5 border border-purple-500/20 p-5">
                    <div className="flex items-center gap-2 mb-2">
                      <AlertTriangle className="h-5 w-5 text-purple-400" />
                      <span className="text-[13px] font-semibold uppercase tracking-wider text-purple-500/60">Проблемных складов</span>
                    </div>
                    <p className="text-2xl font-bold text-purple-400">{data.warehouse_assessments?.filter(w => w.status !== 'ok').length || 0}</p>
                    <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">из {data.warehouse_assessments?.length || '?'}</p>
                  </div>
                </div>

                {/* Priority Actions */}
                {data.priority_actions?.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <Zap className="h-5 w-5" /> Что делать ({data.priority_actions.length})
                    </h4>
                    {data.priority_actions.map((pa, idx) => (
                      <div key={idx} className="flex items-start gap-4 px-5 py-4 rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)]">
                        <div className="flex items-center justify-center h-8 w-8 rounded-lg bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))] font-bold text-sm shrink-0 mt-0.5">{idx + 1}</div>
                        <div className="flex-1 min-w-0">
                          <p className="text-[15px] font-medium text-[hsl(var(--foreground))] leading-relaxed">{pa.action}</p>
                          <p className="text-[13px] text-[hsl(var(--muted-foreground))] mt-1">{pa.impact}</p>
                        </div>
                        {pa.link_to_supply && (
                          <a href="/warehouses/supply"
                            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-[12px] font-semibold hover:bg-emerald-500/20 transition-colors shrink-0 whitespace-nowrap">
                            <Package className="h-3.5 w-3.5" /> Поставки
                          </a>
                        )}
                      </div>
                    ))}
                  </div>
                )}

                {/* Warehouse Assessments */}
                {data.warehouse_assessments?.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <Warehouse className="h-5 w-5" /> Оценка складов ({data.warehouse_assessments.length})
                    </h4>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                      {data.warehouse_assessments.map((wa, idx) => {
                        const ws = whStatusCfg[wa.status] || whStatusCfg.ok
                        return (
                          <div key={idx} className={`rounded-xl border ${ws.border} ${ws.bg} p-5`}>
                            <div className="flex items-center justify-between mb-2">
                              <div className="flex items-center gap-2">
                                <Warehouse className="h-4 w-4 text-blue-400" />
                                <span className="text-[14px] font-bold text-[hsl(var(--foreground))]">{wa.warehouse}</span>
                              </div>
                              <span className={`text-[11px] font-bold px-2 py-0.5 rounded-full ${ws.bg} ${ws.color}`}>{ws.label}</span>
                            </div>
                            <div className="flex items-center gap-3 text-[12px] text-[hsl(var(--muted-foreground))] mb-2">
                              <span>{wa.cluster}</span><span>·</span><span>{wa.total_orders} заказов</span><span>·</span>
                              <span className={wa.cross_pct > 40 ? 'text-red-400 font-semibold' : wa.cross_pct > 20 ? 'text-amber-400 font-semibold' : ''}>{wa.cross_pct}% кросс</span>
                            </div>
                            {wa.main_cross_destinations?.length > 0 && (
                              <div className="flex flex-wrap gap-1 mb-2">
                                {wa.main_cross_destinations.map((d, i) => (
                                  <span key={i} className="text-[11px] px-1.5 py-0.5 rounded bg-red-500/8 text-red-400/80">{d}</span>
                                ))}
                              </div>
                            )}
                            <p className="text-[13px] text-[hsl(var(--foreground)/0.8)] leading-relaxed">{wa.assessment}</p>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                )}

                {/* Problem SKUs */}
                {data.problem_skus?.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <Target className="h-5 w-5" /> Проблемные SKU ({data.problem_skus.length})
                    </h4>
                    {data.problem_skus.map((ps, idx) => (
                      <div key={idx} className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)] overflow-hidden">
                        <div className="px-6 py-5">
                          <div className="flex items-start justify-between mb-3">
                            <div>
                              <div className="flex items-center gap-2 flex-wrap">
                                <span className="text-[16px] font-bold text-[hsl(var(--foreground))]">{ps.offer_id}</span>
                                <span className="text-[14px] text-[hsl(var(--muted-foreground))] truncate">{ps.name}</span>
                              </div>
                              <div className="flex items-center gap-3 mt-1 text-[14px] text-[hsl(var(--muted-foreground)/0.6)]">
                                <span>{ps.total_orders} заказов</span><span>·</span>
                                <span className="text-red-400 font-semibold">{ps.cross_orders} кросс ({ps.cross_pct}%)</span>
                              </div>
                            </div>
                            <span className={`text-[12px] font-bold px-3 py-1.5 rounded-full whitespace-nowrap ${
                              ps.cross_pct > 60 ? 'bg-red-500/15 text-red-400' : ps.cross_pct > 30 ? 'bg-amber-500/15 text-amber-400' : 'bg-emerald-500/15 text-emerald-400'
                            }`}>Кросс {ps.cross_pct}%</span>
                          </div>
                          {ps.stock_distribution?.length > 0 && (
                            <div className="mb-3">
                              <span className="text-[12px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider">Стоки:</span>
                              <div className="flex flex-wrap gap-2 mt-1">
                                {ps.stock_distribution.map((sd, i) => (
                                  <span key={i} className={`text-[12px] px-2 py-0.5 rounded-md tabular-nums ${sd.stock === 0 ? 'bg-red-500/10 text-red-400' : 'bg-[hsl(var(--muted)/0.15)] text-[hsl(var(--foreground)/0.8)]'}`}>
                                    {sd.warehouse}: <b>{sd.stock}</b>
                                  </span>
                                ))}
                              </div>
                            </div>
                          )}
                          {ps.top_cross_routes?.length > 0 && (
                            <div className="mb-3">
                              <span className="text-[12px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider">Маршруты кросс:</span>
                              <div className="flex flex-wrap gap-2 mt-1">
                                {ps.top_cross_routes.map((rt, i) => (
                                  <span key={i} className="text-[12px] px-2 py-1 rounded-md bg-red-500/8 text-[hsl(var(--foreground)/0.8)] inline-flex items-center gap-1">
                                    {rt.from_warehouse} <ArrowRight className="h-3 w-3 text-red-400" /> {rt.to_cluster}
                                    <span className="text-red-400 font-bold ml-0.5">{rt.orders}</span>
                                  </span>
                                ))}
                              </div>
                            </div>
                          )}
                          {ps.recommendation && (
                            <div className="flex items-start gap-3 px-4 py-3 rounded-lg bg-[hsl(var(--muted)/0.08)] border border-[hsl(var(--border)/0.3)]">
                              <Lightbulb className="h-4 w-4 text-amber-400 shrink-0 mt-0.5" />
                              <p className="text-[14px] text-[hsl(var(--foreground)/0.85)] leading-relaxed">{ps.recommendation}</p>
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {/* General Tips */}
                {data.general_tips?.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="text-[15px] font-bold uppercase tracking-wider text-[hsl(var(--foreground))] flex items-center gap-2">
                      <Lightbulb className="h-5 w-5" /> Рекомендации
                    </h4>
                    <div className="space-y-2">
                      {data.general_tips.map((tip, i) => (
                        <div key={i} className="flex items-start gap-3 px-5 py-3.5 rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--muted)/0.04)]">
                          <span className="text-sm mt-0.5">💡</span>
                          <p className="text-[15px] text-[hsl(var(--foreground)/0.9)] leading-relaxed">{tip}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Footer */}
                <div className="flex items-center justify-between text-[12px] text-[hsl(var(--muted-foreground)/0.4)] pt-4 border-t border-[hsl(var(--border)/0.1)]">
                  <span>Gemini 2.5 Flash · Обзор кросс-логистики Ozon</span>
                  <span>{analyzedAtStr} {data.cached ? '(кеш)' : ''}</span>
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  )
}

export default function WarehousesCrossPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const isWB = currentShop?.marketplace === 'wildberries'
  const isOzon = currentShop?.marketplace === 'ozon'

  const [data, setData] = useState<WBWarehouseAnalyticsResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState(30)

  const fetchData = useCallback(async () => {
    if (!currentShop || (!isWB && !isOzon)) return
    setLoading(true)
    setError(null)
    try {
      if (isWB) {
        const result = await getWBWarehouseAnalytics({ shop_id: currentShop.id, period })
        setData(result)
      } else {
        const ozonResult = await getOzonWarehouseAnalytics({ shop_id: currentShop.id, period })
        setData(normalizeOzonToCrossData(ozonResult))
      }
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [currentShop, isWB, isOzon, period])

  useEffect(() => { fetchData() }, [fetchData])

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

          {/* AI Analysis (Ozon only) */}
          {isOzon && currentShop && (
            <OzonCrossAIInsight shopId={currentShop.id} period={period} />
          )}

          {/* Top Problem SKUs */}
          <TopProblemSkus data={data} />

          {/* Cross-map with toggle */}
          <CrossMapSection data={data} />

          {/* Warehouses with cross details */}
          <CrossWarehousesTable warehouses={data.warehouses} />
        </>
      ) : null}
    </div>
  )
}
