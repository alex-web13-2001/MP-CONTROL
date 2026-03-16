/**
 * Warehouses Overview — "Problem Dashboard"
 * Shows KPIs with trends, cost breakdown, problem alerts, AI diagnostics, warehouses table.
 * WB only; Ozon redirects to /warehouses/analytics.
 */
import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import { Navigate, useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  Warehouse,
  BarChart3,
  ArrowRightLeft,
  RefreshCw,
  Package,
  Truck,
  Boxes,
  AlertTriangle,
  TrendingUp,
  TrendingDown,
  Minus,
  ArrowRight,
  ChevronDown,
  ChevronUp,
  Sparkles,
  ShieldAlert,
  MapPin,
  PackagePlus,
  Loader2,
  Gavel,
  Copy,
  X,
  Check,
  Search,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'
import {
  getWBWarehouseAnalytics,
  getWBWarehouseAIAnalysis,
  type WBWarehouseAnalyticsResponse,
  type WBAnalyticsWarehouse,
  type AIWarehouseAnalysis,
  type AIAnalysisSection,
} from '@/api/warehouses'
import { CostsSummary } from './WBWarehouseAnalyticsContent'

/* ── Helpers ── */
function fmt(v: number): string { return Math.round(v).toLocaleString('ru-RU') }
function fmtM(v: number): string { return Math.round(v).toLocaleString('ru-RU') + ' ₽' }
function fmtD(v: number | null): string { return v != null ? `${Math.round(v)} дн` : '—' }

/* ── CopyButton ── */
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

/* ── Period Options ── */
const PERIOD_OPTIONS = [
  { label: '7 дн', value: 7 },
  { label: '14 дн', value: 14 },
  { label: '30 дн', value: 30 },
  { label: '60 дн', value: 60 },
  { label: '90 дн', value: 90 },
]

/* ── Trend helper ── */
function calcTrend(current: number, prev: number): { pct: number; direction: 'up' | 'down' | 'flat' } {
  if (prev === 0) return { pct: 0, direction: 'flat' }
  const pct = ((current - prev) / Math.abs(prev)) * 100
  if (Math.abs(pct) < 1) return { pct: 0, direction: 'flat' }
  return { pct: Math.round(pct), direction: pct > 0 ? 'up' : 'down' }
}

/* ═══ Trend KPI Card ═══ */
function TrendKpiCard({
  title, value, subtitle, icon: Icon, accent, delay,
  trend, trendInverted = false, statusColor,
}: {
  title: string; value: string; subtitle?: string
  icon: React.ElementType; accent: string; delay: number
  trend?: { pct: number; direction: 'up' | 'down' | 'flat' }
  trendInverted?: boolean // true = up is bad (costs going up)
  statusColor?: string
}) {
  const TrendIcon = trend?.direction === 'up' ? TrendingUp : trend?.direction === 'down' ? TrendingDown : Minus

  // For costs: up = bad (red), down = good (green)
  // For orders: up = good (green), down = bad (red)
  const getColor = () => {
    if (!trend || trend.direction === 'flat') return 'text-[hsl(var(--muted-foreground))]'
    if (trendInverted) {
      return trend.direction === 'up' ? 'text-red-400' : 'text-emerald-400'
    }
    return trend.direction === 'up' ? 'text-emerald-400' : 'text-red-400'
  }

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay, duration: 0.4 }}>
      <Card className={`relative overflow-hidden h-full ${statusColor ? `border-b-[3px] ${statusColor}` : ''}`}>
        <CardContent className="p-4 sm:p-5 flex flex-col justify-between h-full">
          <div className="flex items-start justify-between gap-2">
            <div className="space-y-1 min-w-0 flex-1">
              <p className="text-[11px] sm:text-[13px] font-medium text-[hsl(var(--muted-foreground))] truncate">{title}</p>
              <p className="text-lg sm:text-2xl font-bold tracking-tight whitespace-nowrap">{value}</p>
            </div>
            <div className={`flex h-8 w-8 sm:h-10 sm:w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br ${accent} shadow-lg`}>
              <Icon className="h-4 w-4 sm:h-5 sm:w-5 text-white" />
            </div>
          </div>
          <div className="mt-2 sm:mt-3 flex items-center justify-between min-h-[20px] gap-1">
            {subtitle && (
              <span className="text-[10px] sm:text-[12px] text-[hsl(var(--muted-foreground))] truncate min-w-0">{subtitle}</span>
            )}
            {trend && trend.direction !== 'flat' && (
              <div className={`flex items-center gap-0.5 ml-auto shrink-0 ${getColor()}`}>
                <TrendIcon className="h-3 w-3 sm:h-3.5 sm:w-3.5" />
                <span className="text-[10px] sm:text-[12px] font-bold tabular-nums">
                  {trend.direction === 'up' ? '+' : ''}{trend.pct}%
                </span>
              </div>
            )}
            {trend && trend.direction === 'flat' && (
              <div className="flex items-center gap-0.5 ml-auto shrink-0 text-[hsl(var(--muted-foreground))]">
                <Minus className="h-3 w-3 opacity-40" />
                <span className="text-[10px] opacity-50">—</span>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}


/* ═══ Problem Card ═══ */
function ProblemCard({
  severity, icon: Icon, title, details, link, linkLabel, delay,
}: {
  severity: 'critical' | 'warning' | 'info' | 'ok'
  icon: React.ElementType
  title: string
  details: string | React.ReactNode
  link?: string
  linkLabel?: string
  delay: number
}) {
  const navigate = useNavigate()
  const styles = {
    critical: { border: 'border-red-500/30', bg: 'bg-red-500/5', icon: 'text-red-400', dot: 'bg-red-500' },
    warning: { border: 'border-amber-500/30', bg: 'bg-amber-500/5', icon: 'text-amber-400', dot: 'bg-amber-500' },
    info: { border: 'border-blue-500/30', bg: 'bg-blue-500/5', icon: 'text-blue-400', dot: 'bg-blue-500' },
    ok: { border: 'border-emerald-500/30', bg: 'bg-emerald-500/5', icon: 'text-emerald-400', dot: 'bg-emerald-500' },
  }
  const s = styles[severity]

  return (
    <motion.div
      initial={{ opacity: 0, x: -20 }} animate={{ opacity: 1, x: 0 }} transition={{ delay, duration: 0.3 }}
    >
      <div className={`rounded-xl border ${s.border} ${s.bg} p-4 flex items-start gap-3`}>
        <div className={`mt-0.5 p-1.5 rounded-lg ${s.bg} ${s.icon}`}>
          <Icon className="h-4 w-4" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className={`h-2 w-2 rounded-full ${s.dot}`} />
            <span className="text-[13px] font-bold text-[hsl(var(--foreground))]">{title}</span>
          </div>
          <div className="text-[12px] text-[hsl(var(--muted-foreground))] leading-relaxed">
            {details}
          </div>
        </div>
        {link && (
          <button
            onClick={() => navigate(link)}
            className="shrink-0 flex items-center gap-1 text-[12px] font-medium text-[hsl(var(--primary))] hover:underline mt-1"
          >
            {linkLabel || 'Подробнее'} <ArrowRight className="h-3.5 w-3.5" />
          </button>
        )}
      </div>
    </motion.div>
  )
}


/* ═══ AI Frontend Cache ═══ */
const _aiCache = new Map<string, { ts: number; data: AIWarehouseAnalysis }>()
const _AI_FE_TTL = 10 * 60 * 1000 // 10 min

/* ═══ AI Diagnostics Block ═══ */
function AIDiagnosticsBlock({ shopId, period }: { shopId: number; period: number }) {
  const [data, setData] = useState<AIWarehouseAnalysis | null>(null)
  const [loading, setLoading] = useState(false)
  const [expanded, setExpanded] = useState(false)
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false
    setData(null) // Reset on shop/period change
    const cacheKey = `${shopId}_${period}`
    const cached = _aiCache.get(cacheKey)
    if (cached && Date.now() - cached.ts < _AI_FE_TTL) {
      setData(cached.data)
      return
    }
    setLoading(true)
    getWBWarehouseAIAnalysis({ shop_id: shopId, period })
      .then(r => {
        if (!cancelled) {
          setData(r)
          _aiCache.set(cacheKey, { ts: Date.now(), data: r })
        }
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [shopId, period])

  if (loading) {
    return (
      <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.5 }}>
        <Card>
          <CardContent className="p-6 flex items-center gap-3">
            <Loader2 className="h-5 w-5 animate-spin text-violet-400" />
            <span className="text-sm text-[hsl(var(--muted-foreground))]">ИИ-аналитик загружает данные...</span>
          </CardContent>
        </Card>
      </motion.div>
    )
  }

  if (!data) return null

  const severityStyles = {
    critical: { border: 'border-l-red-500', icon: 'text-red-400', label: 'Критическая ситуация' },
    warning: { border: 'border-l-amber-500', icon: 'text-amber-400', label: 'Требует внимания' },
    ok: { border: 'border-l-emerald-500', icon: 'text-emerald-400', label: 'Всё под контролем' },
  }
  const sv = severityStyles[data.severity] || severityStyles.ok

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.5, duration: 0.4 }}>
      <Card className={`border-l-4 ${sv.border} overflow-hidden`}>
        <CardContent className="p-0">
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-4 border-b border-[hsl(var(--border)/0.5)]">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-xl bg-gradient-to-br from-violet-600 to-purple-500 shadow-lg">
                <Sparkles className="h-5 w-5 text-white" />
              </div>
              <div>
                <h3 className="text-[15px] font-bold text-[hsl(var(--foreground))]">ИИ-аналитик складов</h3>
                <p className="text-[11px] text-[hsl(var(--muted-foreground))]">
                  {sv.label}
                  {data.shop_name && <> • {data.shop_name}</>}
                  {data.cached && data.cached_at && (
                    <> • Кеш от {new Date(data.cached_at * 1000).toLocaleDateString('ru-RU')}</>
                  )}
                </p>
              </div>
            </div>
            <button
              onClick={() => setExpanded(!expanded)}
              className="flex items-center gap-1 text-[12px] font-medium text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
            >
              {expanded ? 'Свернуть' : 'Развернуть'}
              {expanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
            </button>
          </div>

          {/* Sections-based diagnosis */}
          <div className="px-6 py-4 space-y-3">
            {(data.analysis_sections || []).length > 0 ? (
              (data.analysis_sections || []).map((sec: AIAnalysisSection) => {
                const secStyles = {
                  critical: { dot: 'bg-red-500', text: 'text-red-400' },
                  warning: { dot: 'bg-amber-500', text: 'text-amber-400' },
                  ok: { dot: 'bg-emerald-500', text: 'text-emerald-400' },
                }
                const sectionLinks: Record<string, string> = {
                  cross_logistics: '/warehouses/cross',
                  storage: '/warehouses/storage',
                  supply: '/warehouses/supplies',
                  geography: '/warehouses/geography',
                }
                const sectionLabels: Record<string, string> = {
                  cross_logistics: 'Кросс-логистика',
                  storage: 'Хранение',
                  supply: 'Поставки',
                  geography: 'География',
                }
                const ss = secStyles[sec.severity] || secStyles.ok
                return (
                  <div key={sec.section} className="flex items-start gap-2">
                    <span className={`mt-1.5 h-2 w-2 rounded-full shrink-0 ${ss.dot}`} />
                    <div className="flex-1 min-w-0">
                      <span className={`text-[12px] font-bold ${ss.text}`}>{sectionLabels[sec.section] || sec.section}</span>
                      <p className="text-[12px] text-[hsl(var(--muted-foreground))] leading-relaxed">{sec.summary}</p>
                    </div>
                    <button
                      onClick={() => navigate(sectionLinks[sec.section] || '/warehouses')}
                      className="shrink-0 text-[11px] font-medium text-violet-400 hover:text-violet-300 whitespace-nowrap"
                    >
                      {sec.action_text || 'Подробнее →'}
                    </button>
                  </div>
                )
              })
            ) : (
              <p className="text-[13px] text-[hsl(var(--foreground))] leading-relaxed whitespace-pre-line">
                {data.diagnosis}
              </p>
            )}
          </div>

          {/* Expanded: tips + actions */}
          <AnimatePresence>
            {expanded && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                transition={{ duration: 0.25 }}
                className="overflow-hidden"
              >
                {/* General tips */}
                {data.general_tips.length > 0 && (
                  <div className="px-6 pb-4 space-y-2">
                    <h4 className="text-[12px] font-bold text-[hsl(var(--muted-foreground))] uppercase tracking-wider">Рекомендации</h4>
                    {data.general_tips.map((tip, i) => (
                      <div key={i} className="text-[12px] text-[hsl(var(--foreground))] pl-4 border-l-2 border-violet-500/30 py-1">
                        {tip}
                      </div>
                    ))}
                  </div>
                )}

                {/* Supply tip */}
                {data.supply_tip && (
                  <div className="px-6 pb-4">
                    <div className="p-3 rounded-lg bg-violet-500/5 border border-violet-500/20">
                      <p className="text-[12px] text-violet-300 font-medium">📦 {data.supply_tip}</p>
                    </div>
                  </div>
                )}

                {/* Top 3 SKU actions */}
                {data.sku_actions.length > 0 && (
                  <div className="px-6 pb-4">
                    <h4 className="text-[12px] font-bold text-[hsl(var(--muted-foreground))] uppercase tracking-wider mb-2">
                      Топ проблемных SKU
                    </h4>
                    <div className="space-y-1.5">
                      {data.sku_actions.slice(0, 3).map((sku, i) => (
                        <div key={i} className="flex items-center justify-between text-[12px] px-3 py-2 rounded-lg bg-[hsl(var(--muted)/0.06)]">
                          <div className="min-w-0">
                            <span className="font-bold text-[hsl(var(--foreground))]">{sku.vendor_code}</span>
                            <span className="text-[hsl(var(--muted-foreground))] ml-2 truncate">{sku.problem}</span>
                          </div>
                          <span className="text-red-400 font-bold shrink-0 ml-2">{fmtM(sku.storage_cost_month)}/мес</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* CTA */}
                <div className="px-6 pb-4">
                  <button
                    onClick={() => navigate('/warehouses/storage')}
                    className="flex items-center gap-2 text-[13px] font-medium text-violet-400 hover:text-violet-300 transition-colors"
                  >
                    Полный ИИ-анализ в разделе «Хранение» <ArrowRight className="h-4 w-4" />
                  </button>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </CardContent>
      </Card>
    </motion.div>
  )
}


/* ═══ Status Badge ═══ */
function StatusBadge({ status }: { status: WBAnalyticsWarehouse['status'] }) {
  const map: Record<string, { label: string; cls: string }> = {
    critical: { label: 'Критич.', cls: 'bg-red-500/15 text-red-400 ring-red-500/20' },
    attention: { label: 'Внимание', cls: 'bg-amber-500/15 text-amber-400 ring-amber-500/20' },
    ok: { label: 'Норма', cls: 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' },
    overstocked: { label: 'Перезат.', cls: 'bg-purple-500/15 text-purple-400 ring-purple-500/20' },
    empty: { label: 'Пусто', cls: 'bg-slate-500/15 text-slate-400 ring-slate-500/20' },
  }
  const s = map[status] || map.ok
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold ring-1 ring-inset ${s.cls}`}>
      {s.label}
    </span>
  )
}

/* ═══ Type Badge ═══ */
function TypeBadge({ type }: { type: WBAnalyticsWarehouse['warehouse_type'] }) {
  if (type === 'normal') return null
  const map = {
    food: { label: '🍕 Питание', cls: 'bg-orange-500/10 text-orange-400' },
    sgt: { label: '📦 СГТ', cls: 'bg-blue-500/10 text-blue-400' },
  }
  const s = map[type]
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium ${s.cls}`}>
      {s.label}
    </span>
  )
}

/* ═══ Cross% Indicator ═══ */
function CrossPctBar({ pct, orders }: { pct: number; orders: number }) {
  if (orders === 0) return <span className="text-[hsl(var(--muted-foreground))] opacity-40">—</span>
  const color = pct > 70 ? 'bg-red-500' : pct > 40 ? 'bg-amber-500' : 'bg-emerald-500'
  const textColor = pct > 70 ? 'text-red-400' : pct > 40 ? 'text-amber-400' : 'text-emerald-400'
  return (
    <div className="flex items-center gap-2">
      <div className="w-12 h-1.5 rounded-full bg-[hsl(var(--muted)/0.2)] overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${Math.min(pct, 100)}%` }} />
      </div>
      <span className={`text-[12px] tabular-nums font-semibold ${textColor}`}>{pct}%</span>
    </div>
  )
}

/* ═══ SKU Combobox (local multi-select) ═══ */
interface SkuOption { nm_id: number; vendor_code: string; name: string }

function SkuCombobox({ allSkus, selected, onSelect, onRemove, onClear }: {
  allSkus: SkuOption[]
  selected: SkuOption[]
  onSelect: (s: SkuOption) => void
  onRemove: (nm: number) => void
  onClear: () => void
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const selectedIds = new Set(selected.map(s => s.nm_id))
  const q = search.toLowerCase().trim()
  const filtered = allSkus.filter(o => !selectedIds.has(o.nm_id) && (
    !q || (o.vendor_code || '').toLowerCase().includes(q) || (o.name || '').toLowerCase().includes(q) || String(o.nm_id).includes(q)
  )).slice(0, 30)

  return (
    <div ref={dropdownRef} className="relative flex-1 min-w-0">
      <div
        className="flex flex-wrap items-center gap-1.5 px-3 py-2 min-h-[40px] rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))] cursor-text transition-all focus-within:ring-2 focus-within:ring-[hsl(var(--primary)/0.3)]"
        onClick={() => { inputRef.current?.focus(); setOpen(true) }}
      >
        <Search className="h-4 w-4 shrink-0 text-[hsl(var(--muted-foreground)/0.5)]" />
        {selected.map(s => (
          <span key={s.nm_id} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-lg bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))] text-[12px] font-medium max-w-[200px] truncate">
            {s.vendor_code || `#${s.nm_id}`}
            <X className="h-3 w-3 cursor-pointer hover:text-red-400 shrink-0" onClick={(e) => { e.stopPropagation(); onRemove(s.nm_id) }} />
          </span>
        ))}
        <input
          ref={inputRef}
          type="text"
          value={search}
          onChange={e => { setSearch(e.target.value); setOpen(true) }}
          onFocus={() => setOpen(true)}
          placeholder={selected.length === 0 ? 'Поиск по артикулу, названию или ID...' : 'Ещё...'}
          className="flex-1 min-w-[80px] bg-transparent text-[13px] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground)/0.4)] outline-none"
        />
        {selected.length > 0 && (
          <button onClick={(e) => { e.stopPropagation(); onClear() }} className="shrink-0 text-[hsl(var(--muted-foreground)/0.5)] hover:text-red-400 transition-colors">
            <X className="h-4 w-4" />
          </button>
        )}
      </div>
      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0, y: -4 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -4 }} transition={{ duration: 0.15 }}
            className="absolute z-50 mt-1 w-full max-h-[280px] overflow-auto rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-xl"
          >
            {filtered.length > 0 ? filtered.map(opt => (
              <button key={opt.nm_id} onClick={() => { onSelect(opt); setSearch('') }}
                className="w-full text-left px-4 py-2.5 hover:bg-[hsl(var(--muted)/0.1)] transition-colors border-b border-[hsl(var(--border)/0.2)] last:border-0">
                <div className="text-[13px] font-medium text-[hsl(var(--foreground))] line-clamp-1">{opt.name || `Товар #${opt.nm_id}`}</div>
                <div className="text-[11px] text-[hsl(var(--muted-foreground))]">
                  {opt.vendor_code && <span className="font-semibold">{opt.vendor_code}</span>}
                  {opt.vendor_code && ' · '}
                  ID: {opt.nm_id}
                </div>
              </button>
            )) : (
              <div className="px-4 py-6 text-center text-[13px] text-[hsl(var(--muted-foreground))]">
                {search ? 'Ничего не найдено' : 'Нет товаров'}
              </div>
            )}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

/* ═══ Warehouses Table (with expandable SKU rows + combobox filter) ═══ */
function WarehousesTable({ warehouses }: { warehouses: WBAnalyticsWarehouse[] }) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [selectedSkus, setSelectedSkus] = useState<SkuOption[]>([])
  const thBase = 'px-4 py-3 text-center text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider whitespace-nowrap'
  const tdCls = 'px-4 py-3 text-center tabular-nums text-[13px] whitespace-nowrap'

  const toggle = (name: string) => {
    setExpanded(prev => {
      const next = new Set(prev)
      next.has(name) ? next.delete(name) : next.add(name)
      return next
    })
  }

  // Collect unique SKUs from all warehouses
  const allSkus = useMemo(() => {
    const map = new Map<number, SkuOption>()
    warehouses.forEach(wh => (wh.skus || []).forEach(s => {
      if (!map.has(s.nm_id)) map.set(s.nm_id, { nm_id: s.nm_id, vendor_code: s.vendor_code, name: s.name })
    }))
    return Array.from(map.values())
  }, [warehouses])

  // Filter warehouses by selected SKUs
  const selectedIds = new Set(selectedSkus.map(s => s.nm_id))
  const filteredWarehouses = selectedIds.size > 0
    ? warehouses.filter(wh => (wh.skus || []).some(s => selectedIds.has(s.nm_id)))
    : warehouses

  return (
    <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.6, duration: 0.4 }}>
      <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
        <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Склады</h2>
          <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">
            {filteredWarehouses.length === warehouses.length ? `${warehouses.length} складов` : `${filteredWarehouses.length} из ${warehouses.length}`}
          </span>
        </div>

        {/* SKU Combobox */}
        <div className="px-6 py-3 border-b border-[hsl(var(--border)/0.5)]">
          <SkuCombobox
            allSkus={allSkus}
            selected={selectedSkus}
            onSelect={s => setSelectedSkus(prev => [...prev, s])}
            onRemove={nm => setSelectedSkus(prev => prev.filter(x => x.nm_id !== nm))}
            onClear={() => setSelectedSkus([])}
          />
        </div>

        <div className="overflow-auto max-h-[700px]">
          <table className="w-full border-collapse" style={{ minWidth: 900 }}>
            <thead className="sticky top-0 z-20">
              <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
                <th className="px-4 py-3 text-left text-[11px] font-semibold text-[hsl(var(--muted-foreground))] uppercase tracking-wider min-w-[200px]">Склад</th>
                <th className={`${thBase} min-w-[80px]`}>Статус</th>
                <th className={`${thBase} min-w-[75px]`}>Остаток</th>
                <th className={`${thBase} min-w-[75px]`}>Заказов</th>
                <th className={`${thBase} min-w-[65px]`}>В день</th>
                <th className={`${thBase} min-w-[75px]`}>Оборач.</th>
                <th className={`${thBase} min-w-[90px]`}>Кросс%</th>
                <th className={`${thBase} min-w-[100px]`}>Хранение ₽</th>
                <th className={`${thBase} min-w-[50px]`}>SKU</th>
              </tr>
            </thead>
            <tbody>
              {filteredWarehouses.map((wh, idx) => {
                const hasSelectedFilter = selectedIds.size > 0
                const isExp = expanded.has(wh.warehouse_name)
                const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.04)]'
                const hasSkus = (wh.skus || []).length > 0
                return (
                  <React.Fragment key={wh.warehouse_name}>
                    <tr
                      className={`border-b border-[hsl(var(--border)/0.15)] ${rowBg} hover:bg-[hsl(var(--muted)/0.1)] transition-colors ${hasSkus ? 'cursor-pointer' : ''}`}
                      onClick={() => hasSkus && toggle(wh.warehouse_name)}
                    >
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          {hasSkus ? (
                            <ChevronDown className={`h-4 w-4 text-[hsl(var(--muted-foreground))] shrink-0 transition-transform ${isExp ? 'rotate-180' : ''}`} />
                          ) : (
                            <Warehouse className="h-4 w-4 text-blue-400 shrink-0" />
                          )}
                          <div className="min-w-0">
                            <div className="text-[13px] font-semibold text-[hsl(var(--foreground))] flex items-center gap-1.5 flex-wrap">
                              {wh.warehouse_name}
                              <TypeBadge type={wh.warehouse_type} />
                            </div>
                            {wh.okrug && (
                              <div className="text-[10px] text-[hsl(var(--muted-foreground))] opacity-60 truncate">
                                {wh.okrug.replace(' федеральный округ', '')}
                              </div>
                            )}
                          </div>
                        </div>
                      </td>
                      <td className={`${tdCls} text-center`}><StatusBadge status={wh.status} /></td>
                      <td className={tdCls}>{fmt(wh.stock)}</td>
                      <td className={tdCls}>{fmt(wh.orders)}</td>
                      <td className={tdCls}>{wh.daily_sales.toFixed(2)}</td>
                      <td className={tdCls}>
                        <span className={`font-semibold ${
                          wh.turnover_days === null ? '' :
                          wh.turnover_days < 14 ? 'text-red-400' :
                          wh.turnover_days < 30 ? 'text-amber-400' :
                          wh.turnover_days > 120 ? 'text-purple-400' :
                          'text-emerald-400'
                        }`}>
                          {fmtD(wh.turnover_days)}
                        </span>
                      </td>
                      <td className={tdCls}>
                        <CrossPctBar pct={wh.cross_pct} orders={wh.orders} />
                      </td>
                      <td className={`${tdCls} ${wh.storage_cost_month > 5000 ? 'text-purple-400 font-semibold' : wh.storage_cost_month > 0 ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))]'}`}>
                        {wh.storage_cost_month > 0 ? fmtM(wh.storage_cost_month) : '—'}
                      </td>
                      <td className={`${tdCls} text-[hsl(var(--muted-foreground))]`}>{wh.sku_count}</td>
                    </tr>
                    {/* Expanded SKU rows */}
                    {isExp && (wh.skus || []).length > 0 && (
                      <tr>
                        <td colSpan={9} className="p-0">
                          <div className="bg-[hsl(var(--muted)/0.06)] border-b border-[hsl(var(--border)/0.3)]">
                            <table className="w-full">
                              <thead>
                                <tr className="text-[10px] font-semibold text-[hsl(var(--muted-foreground))] uppercase">
                                  <th className="pl-12 pr-2 py-2 text-left">Товар</th>
                                  <th className="px-2 py-2 text-center">Остаток</th>
                                  <th className="px-2 py-2 text-center">Заказов</th>
                                  <th className="px-2 py-2 text-center">В день</th>
                                  <th className="px-2 py-2 text-center">Запас дн</th>
                                  <th className="px-2 py-2 text-center">Кросс%</th>
                                </tr>
                              </thead>
                              <tbody>
                                {(() => {
                                  const filtSkus = hasSelectedFilter
                                    ? wh.skus.filter(s => selectedIds.has(s.nm_id))
                                    : [...wh.skus]
                                  return filtSkus.sort((a, b) => b.stock - a.stock).map((sku) => (
                                  <tr key={sku.nm_id} className="border-t border-[hsl(var(--border)/0.1)] hover:bg-[hsl(var(--muted)/0.08)]">
                                    <td className="pl-12 pr-2 py-2">
                                      <div className="min-w-0">
                                        <div className="text-[12px] font-medium text-[hsl(var(--foreground))] leading-snug truncate max-w-[250px]">
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
                                            <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)]">ID: {sku.nm_id}</span>
                                            <CopyButton text={String(sku.nm_id)} />
                                          </div>
                                        </div>
                                      </div>
                                    </td>
                                    <td className="px-2 py-1.5 text-[12px] text-center tabular-nums">{fmt(sku.stock)}</td>
                                    <td className="px-2 py-1.5 text-[12px] text-center tabular-nums">{fmt(sku.orders)}</td>
                                    <td className="px-2 py-1.5 text-[12px] text-center tabular-nums">{sku.daily_sales.toFixed(2)}</td>
                                    <td className={`px-2 py-1.5 text-[12px] text-center tabular-nums font-semibold ${
                                      sku.days_supply === null ? '' :
                                      sku.days_supply < 14 ? 'text-red-400' :
                                      sku.days_supply < 30 ? 'text-amber-400' :
                                      sku.days_supply > 120 ? 'text-purple-400' :
                                      'text-emerald-400'
                                    }`}>{fmtD(sku.days_supply)}</td>
                                    <td className="px-2 py-1.5 text-center"><CrossPctBar pct={sku.cross_pct} orders={sku.orders} /></td>
                                  </tr>
                                  ))
                                })()}
                              </tbody>
                            </table>
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
    </motion.div>
  )
}

/* ═══ Skeleton ═══ */
function OverviewSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-[130px] rounded-2xl" />
        ))}
      </div>
      <Skeleton className="h-[200px] rounded-2xl" />
      <Skeleton className="h-[400px] rounded-2xl" />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════
   Main Page
   ═══════════════════════════════════════════════════════════ */

export default function WarehousesOverviewPage() {
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

  // Ozon → redirect
  if (isOzon) return <Navigate to="/warehouses/analytics" replace />

  const periodSelCls = (active: boolean) =>
    `px-3 py-1.5 rounded-lg text-[13px] font-medium transition-all cursor-pointer ${
      active
        ? 'bg-[hsl(var(--primary))] text-white shadow-md'
        : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
    }`

  return (
    <div className="space-y-6 pb-10">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Обзор складов</h1>
          <p className="text-[hsl(var(--muted-foreground))] mt-1">
            Диагностика проблем • Расходы • Тренды
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
              <span className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-50 ml-2">
                сравнение с аналогичным предыдущим периодом
              </span>
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {loading && !data ? (
        <OverviewSkeleton />
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
          {/* ─── KPI Cards with Trends ─── */}
          {(() => {
            const kpi = data.kpi
            const prev = kpi.prev
            const orders = kpi.total_orders || 1
            const logPerOrder = kpi.total_logistics / orders
            const crossPct = kpi.cross_pct
            const totalExpenses = kpi.total_logistics + (kpi.total_storage_actual ?? kpi.total_storage) + kpi.total_penalties
            const prevTotalExpenses = prev
              ? prev.total_logistics + prev.total_storage + prev.total_penalties
              : 0

            // Trend calcs
            const trendExpenses = prev ? calcTrend(totalExpenses, prevTotalExpenses) : undefined
            const trendLogistics = prev ? calcTrend(kpi.total_logistics, prev.total_logistics) : undefined
            const trendStorage = prev ? calcTrend(kpi.total_storage_actual ?? kpi.total_storage, prev.total_storage) : undefined
            const trendOrders = prev ? calcTrend(kpi.total_orders, prev.total_orders) : undefined

            // Cross-logistics estimated cost
            const crossCost = data.warehouses.reduce((s, w) => s + w.cross_orders * 33, 0)

            return (
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
                <TrendKpiCard
                  title="Общие расходы"
                  value={fmtM(totalExpenses)}
                  subtitle={`за ${kpi.period_days}д`}
                  icon={BarChart3}
                  accent="from-blue-600 to-blue-500"
                  delay={0.05}
                  trend={trendExpenses}
                  trendInverted
                />
                <TrendKpiCard
                  title="Логистика"
                  value={fmtM(kpi.total_logistics)}
                  subtitle={`${Math.round(logPerOrder)} ₽/заказ • кросс ~${fmtM(crossCost)}`}
                  icon={Truck}
                  accent="from-cyan-600 to-cyan-500"
                  delay={0.1}
                  trend={trendLogistics}
                  trendInverted
                  statusColor={logPerOrder > 300 ? 'border-red-500' : logPerOrder > 150 ? 'border-amber-500' : 'border-emerald-500'}
                />
                <TrendKpiCard
                  title={kpi.has_actual_storage ? 'Хранение (факт)' : 'Хранение'}
                  value={fmtM(kpi.total_storage_actual ?? kpi.total_storage)}
                  subtitle={kpi.forecast_30d ? `Прогноз 30д: ${fmtM(kpi.forecast_30d)}` : `за ${kpi.period_days}д`}
                  icon={Boxes}
                  accent="from-purple-600 to-purple-500"
                  delay={0.15}
                  trend={trendStorage}
                  trendInverted
                />
                <TrendKpiCard
                  title="Заказов"
                  value={fmt(kpi.total_orders)}
                  subtitle={`${fmt(kpi.total_stock)} шт на складах`}
                  icon={Package}
                  accent="from-emerald-600 to-emerald-500"
                  delay={0.2}
                  trend={trendOrders}
                />
                <TrendKpiCard
                  title="Кросс-отправки"
                  value={`${crossPct}%`}
                  subtitle={kpi.total_penalties > 0 ? `Штрафы: ${fmtM(kpi.total_penalties)}` : 'заказов в чужие округа'}
                  icon={ArrowRightLeft}
                  accent={crossPct > 50 ? 'from-red-600 to-red-500' : crossPct > 25 ? 'from-amber-600 to-amber-500' : 'from-emerald-600 to-emerald-500'}
                  delay={0.25}
                  statusColor={crossPct > 50 ? 'border-red-500' : crossPct > 25 ? 'border-amber-500' : 'border-emerald-500'}
                />
              </div>
            )
          })()}

          {/* ─── Problems Block ─── */}
          {(() => {
            const kpi = data.kpi
            const whs = data.warehouses
            const problems: React.ReactNode[] = []
            let delay = 0.3

            // 1. Cross-logistics
            const crossProblemSkus = whs.flatMap(w =>
              (w.skus || []).filter(s => s.cross_pct > 40 && s.orders > 5).map(s => ({
                ...s,
                warehouse: w.warehouse_name,
                loss: s.cross_orders * 33,
              }))
            )
            if (kpi.cross_pct > 25 || crossProblemSkus.length > 0) {
              const totalLoss = crossProblemSkus.reduce((s, x) => s + x.loss, 0)
              problems.push(
                <ProblemCard
                  key="cross"
                  severity={kpi.cross_pct > 50 ? 'critical' : 'warning'}
                  icon={ArrowRightLeft}
                  title={`Кросс-логистика: ${kpi.cross_pct}%`}
                  details={
                    crossProblemSkus.length > 0
                      ? `${crossProblemSkus.length} SKU с кросс > 40%, потери ~${fmtM(totalLoss)}/период. Худший склад: ${whs.reduce((a, b) => a.cross_pct > b.cross_pct ? a : b).warehouse_name} (${whs.reduce((a, b) => a.cross_pct > b.cross_pct ? a : b).cross_pct}%)`
                      : `Средний кросс ${kpi.cross_pct}% — доставка из неоптимальных складов`
                  }
                  link="/warehouses/cross"
                  linkLabel="Кросс-логистика"
                  delay={delay}
                />
              )
              delay += 0.08
            }

            // 2. Storage overstock — with top SKU details
            const overstockedWhs = whs.filter(w => w.status === 'overstocked')
            if (overstockedWhs.length > 0) {
              const topOverstockSkus = overstockedWhs
                .flatMap(w => (w.skus || []).filter(s => s.days_supply !== null && s.days_supply > 120).map(s => ({ ...s, wh: w.warehouse_name })))
                .sort((a, b) => (b.days_supply ?? 0) - (a.days_supply ?? 0))
                .slice(0, 3)
              problems.push(
                <ProblemCard
                  key="overstock"
                  severity="warning"
                  icon={Boxes}
                  title={`Затоваривание: ${overstockedWhs.length} складов`}
                  details={
                    <div className="space-y-1.5">
                      <div>Склады: {overstockedWhs.map(w => `${w.warehouse_name} (${fmtD(w.turnover_days)})`).slice(0, 3).join(', ')}</div>
                      {topOverstockSkus.length > 0 && (
                        <div className="mt-1.5 space-y-1">
                          <span className="text-[10px] font-bold uppercase tracking-wider opacity-60">Топ SKU:</span>
                          {topOverstockSkus.map((s, i) => (
                            <div key={i} className="flex items-center gap-2 text-[11px]">
                              <span className="font-semibold text-[hsl(var(--foreground))]">{s.vendor_code}</span>
                              <span className="opacity-60">{s.wh}</span>
                              <span className="ml-auto font-bold text-purple-400">{fmtD(s.days_supply)}</span>
                              <span className="opacity-40">{fmt(s.stock)} шт</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  }
                  link="/warehouses/storage"
                  linkLabel="Хранение"
                  delay={delay}
                />
              )
              delay += 0.08
            }

            // 3. Critical supply (< 14 days) + out-of-stock alert
            const criticalWhs = whs.filter(w => w.status === 'critical')
            // Global out-of-stock: aggregate SKU across all warehouses
            const globalSkuMap = new Map<string, { vc: string; name: string; stock: number; daily: number }>()
            whs.forEach(w => (w.skus || []).forEach(s => {
              const existing = globalSkuMap.get(s.vendor_code)
              if (existing) {
                existing.stock += s.stock
                existing.daily += s.daily_sales
              } else {
                globalSkuMap.set(s.vendor_code, { vc: s.vendor_code, name: s.name, stock: s.stock, daily: s.daily_sales })
              }
            }))
            const outOfStockSkus = Array.from(globalSkuMap.values())
              .filter(s => s.daily > 0 && s.stock > 0 && (s.stock / s.daily) < 14)
              .sort((a, b) => (a.stock / a.daily) - (b.stock / b.daily))
              .slice(0, 5)

            if (outOfStockSkus.length > 0) {
              problems.push(
                <ProblemCard
                  key="outofstock"
                  severity="critical"
                  icon={AlertTriangle}
                  title={`Скоро out-of-stock: ${outOfStockSkus.length} SKU`}
                  details={
                    <div className="space-y-1">
                      <div>SKU заканчиваются на ВСЕХ складах:</div>
                      {outOfStockSkus.map((s, i) => {
                        const daysLeft = Math.round(s.stock / s.daily)
                        return (
                          <div key={i} className="flex items-center gap-2 text-[11px]">
                            <span className="font-semibold text-[hsl(var(--foreground))]">{s.vc}</span>
                            <span className="ml-auto font-bold text-red-400">{daysLeft} дн</span>
                            <span className="opacity-40">{fmt(s.stock)} шт / {s.daily.toFixed(1)}/день</span>
                          </div>
                        )
                      })}
                    </div>
                  }
                  link="/warehouses/supplies"
                  linkLabel="Поставки"
                  delay={delay}
                />
              )
              delay += 0.08
            }

            if (criticalWhs.length > 0) {
              // Include ALL SKUs from critical warehouses, not just those with daily_sales > 0
              const critSkus = criticalWhs
                .flatMap(w => (w.skus || []).filter(s => {
                  // Include if: low days_supply, OR null days_supply with stock (sitting dead), OR just has stock on critical warehouse
                  if (s.days_supply !== null && s.days_supply < 14) return true
                  if (s.days_supply === null && s.stock > 0 && s.daily_sales === 0) return true // dead stock on critical wh
                  return false
                }).map(s => ({ ...s, wh: w.warehouse_name, needQty: s.daily_sales > 0 ? Math.max(0, Math.ceil(s.daily_sales * 14 - s.stock)) : 0 })))
                .sort((a, b) => (a.days_supply ?? 999) - (b.days_supply ?? 999))
                .slice(0, 6)
              const totalNeedQty = critSkus.reduce((s, x) => s + x.needQty, 0)
              problems.push(
                <ProblemCard
                  key="supply"
                  severity="critical"
                  icon={PackagePlus}
                  title={`Нужна поставка: ${criticalWhs.length} скл. • ${critSkus.length} SKU`}
                  details={
                    <div className="space-y-1.5">
                      <div className="text-[11px]">
                        Склады: {criticalWhs.map(w => `${w.warehouse_name} (${fmtD(w.turnover_days)})`).join(', ')}
                      </div>
                      {critSkus.length > 0 && (
                        <div className="mt-1 space-y-1">
                          <span className="text-[10px] font-bold uppercase tracking-wider opacity-60">Что поставлять (до 14 дн. запаса):</span>
                          {critSkus.map((s, i) => (
                            <div key={i} className="flex items-center gap-2 text-[11px]">
                              <span className="font-semibold text-[hsl(var(--foreground))]">{s.vendor_code}</span>
                              <span className="text-[10px] opacity-50">{s.wh}</span>
                              <span className="ml-auto font-bold text-amber-400">+{fmt(s.needQty)} шт</span>
                              <span className="text-red-400 font-bold">{fmtD(s.days_supply)}</span>
                            </div>
                          ))}
                          {totalNeedQty > 0 && (
                            <div className="text-[10px] opacity-50 mt-0.5">Итого нужно: ~{fmt(totalNeedQty)} шт на 14 дней</div>
                          )}
                        </div>
                      )}
                    </div>
                  }
                  link="/warehouses/supplies"
                  linkLabel="Поставки"
                  delay={delay}
                />
              )
              delay += 0.08
            }

            // 4. Penalties — pill badges
            const penalties = kpi.total_penalties
            const penaltyDetails = kpi.penalty_details || []
            if (penalties > 0) {
              problems.push(
                <ProblemCard
                  key="penalties"
                  severity={penalties > 5000 ? 'critical' : 'warning'}
                  icon={Gavel}
                  title={`Штрафы за период: ${fmtM(penalties)}`}
                  details={
                    penaltyDetails.length > 0
                      ? (
                        <div className="flex flex-wrap gap-1.5 mt-1">
                          {penaltyDetails.map((d, i) => (
                            <span key={i} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-medium bg-[hsl(var(--muted)/0.15)] text-[hsl(var(--foreground))]">
                              {d.reason}: <span className="font-bold">{fmtM(d.amount)}</span>
                              <span className="opacity-40">({d.count})</span>
                            </span>
                          ))}
                        </div>
                      )
                      : 'Детализация по типам недоступна'
                  }
                  delay={delay}
                />
              )
              delay += 0.08
            }

            // 5. Geography (always, but green if OK)
            const activeWhs = whs.filter(w => w.stock > 0 || w.orders > 0)
            const okrugs = new Set(activeWhs.map(w => w.okrug).filter(Boolean))
            if (kpi.cross_pct <= 25) {
              problems.push(
                <ProblemCard
                  key="geo"
                  severity="ok"
                  icon={MapPin}
                  title="География в норме"
                  details={`${activeWhs.length} активных складов в ${okrugs.size} округах. Кросс-отправки ${kpi.cross_pct}% — под контролем.`}
                  link="/warehouses/geography"
                  linkLabel="География"
                  delay={delay}
                />
              )
            }

            if (problems.length === 0) return null

            return (
              <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.3 }}>
                <div className="space-y-3">
                  <div className="flex items-center gap-2 mb-1">
                    <ShieldAlert className="h-5 w-5 text-amber-400" />
                    <h2 className="text-lg font-bold text-[hsl(var(--foreground))]">Диагностика проблем</h2>
                  </div>
                  {problems}
                </div>
              </motion.div>
            )
          })()}

          {/* ─── Costs Summary (with cross-logistics row) ─── */}
          {(() => {
            const totalOrders = data.warehouses.reduce((s, w) => s + w.orders, 0)
            const crossOrders = data.warehouses.reduce((s, w) => s + w.cross_orders, 0)
            const totalLogistics = data.costs.find(c => c.label === 'Логистика')?.amount || 0
            // Cross cost = proportion of total logistics attributable to cross deliveries
            const crossCost = totalOrders > 0 ? Math.round(totalLogistics * crossOrders / totalOrders) : 0
            const crossPct = totalLogistics > 0 ? Math.round(crossCost / totalLogistics * 100) : 0
            const cd = crossOrders > 0 ? { crossCost, crossPct, crossOrders } : undefined
            return <CostsSummary costs={data.costs} crossData={cd} />
          })()}

          {/* ─── Warehouses Table ─── */}
          <WarehousesTable warehouses={data.warehouses} />
        </>
      ) : null}
    </div>
  )
}
