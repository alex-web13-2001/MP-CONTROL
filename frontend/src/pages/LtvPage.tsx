import { useState, useEffect, useCallback, useMemo } from 'react'
import {
  fetchLtv,
  fetchPurchaseChain,
  type LtvResponse,
  type ChainResponse,
  type SkuRepeatRow,
  type CohortRow,
} from '@/api/ltv'
import { useAppStore } from '@/stores/appStore'
import {
  ResponsiveContainer, BarChart, Bar, XAxis, YAxis,
  CartesianGrid, Tooltip,
} from 'recharts'
import {
  Users, Repeat, TrendingUp, ShoppingCart, DollarSign,
  ChevronRight, Search, ArrowUpDown, Package,
} from 'lucide-react'

/* ── helpers ── */
const fmtMoney = (v: number) =>
  v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
const fmtNum = (v: number) => v.toLocaleString('ru-RU')
const fmtPct = (v: number) => v.toFixed(1) + '%'

const PERIODS = [
  { value: '30d', label: '30 дн' },
  { value: '90d', label: '90 дн' },
  { value: '6m', label: '6 мес' },
  { value: '1y', label: '1 год' },
  { value: 'all', label: 'Всё время' },
]

/* ══════════════════════════════════════════════════════
   KPI Card
   ══════════════════════════════════════════════════════ */
function KpiCard({ label, value, sub, icon: Icon, color }: {
  label: string; value: string; sub?: string
  icon: React.ElementType; color: string
}) {
  return (
    <div className="rounded-xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] p-5 flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <span className="text-[11px] font-semibold text-[hsl(var(--muted-foreground)/0.7)] uppercase tracking-widest">{label}</span>
        <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: `${color}15` }}>
          <Icon size={16} style={{ color }} />
        </div>
      </div>
      <span className="text-[26px] font-extrabold leading-tight" style={{ color }}>{value}</span>
      {sub && <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.8)] font-medium">{sub}</span>}
    </div>
  )
}

/* ══════════════════════════════════════════════════════
   Cohort Heatmap
   ══════════════════════════════════════════════════════ */
function CohortMatrix({ cohorts }: { cohorts: CohortRow[] }) {
  if (!cohorts.length) return null

  const maxOffset = Math.max(
    ...cohorts.flatMap(c => Object.keys(c.months).map(Number))
  )
  const offsets = Array.from({ length: Math.min(maxOffset + 1, 7) }, (_, i) => i)

  const monthNames: Record<string, string> = {
    '01': 'Янв', '02': 'Фев', '03': 'Мар', '04': 'Апр',
    '05': 'Май', '06': 'Июн', '07': 'Июл', '08': 'Авг',
    '09': 'Сен', '10': 'Окт', '11': 'Ноя', '12': 'Дек',
  }

  const formatCohort = (c: string) => {
    const [y, m] = c.split('-')
    return `${monthNames[m] || m} ${y.slice(2)}`
  }

  const getColor = (rate: number) => {
    if (rate >= 50) return 'rgba(139, 92, 246, 0.85)'
    if (rate >= 30) return 'rgba(139, 92, 246, 0.65)'
    if (rate >= 20) return 'rgba(139, 92, 246, 0.50)'
    if (rate >= 10) return 'rgba(139, 92, 246, 0.35)'
    if (rate >= 5)  return 'rgba(139, 92, 246, 0.22)'
    if (rate > 0)   return 'rgba(139, 92, 246, 0.12)'
    return 'transparent'
  }

  return (
    <div className="rounded-2xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] overflow-hidden">
      <div className="px-6 py-4 border-b border-[hsl(var(--border)/0.15)]">
        <h3 className="text-base font-semibold text-[hsl(var(--foreground))]">Когортная матрица (Retention)</h3>
        <p className="text-xs text-[hsl(var(--muted-foreground))] mt-1">Процент клиентов, вернувшихся через N месяцев</p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-[13px]">
          <thead>
            <tr className="border-b border-[hsl(var(--border)/0.15)]">
              <th className="text-left px-4 py-3 font-semibold text-[hsl(var(--muted-foreground))] w-[120px] sticky left-0 bg-[hsl(var(--card))] z-10">Когорта</th>
              <th className="text-center px-3 py-3 font-semibold text-[hsl(var(--muted-foreground))] w-[70px]">Размер</th>
              {offsets.map(o => (
                <th key={o} className="text-center px-3 py-3 font-semibold text-[hsl(var(--muted-foreground))] min-w-[80px]">
                  {o === 0 ? 'Мес 0' : `+${o} мес`}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {cohorts.map((c, ri) => (
              <tr key={c.cohort} className={ri % 2 === 0 ? '' : 'bg-[hsl(var(--muted)/0.06)]'}>
                <td className="px-4 py-3 font-medium text-[hsl(var(--foreground))] sticky left-0 bg-[hsl(var(--card))] z-10 whitespace-nowrap">
                  {formatCohort(c.cohort)}
                </td>
                <td className="px-3 py-3 text-center font-semibold text-[hsl(var(--foreground))]">{fmtNum(c.size)}</td>
                {offsets.map(o => {
                  const m = c.months[String(o)]
                  if (!m) return <td key={o} className="px-3 py-3" />
                  return (
                    <td key={o} className="px-3 py-2 text-center" title={`${m.clients} клиентов`}>
                      <div
                        className="rounded-lg px-2 py-2 mx-auto w-fit min-w-[56px] transition-all"
                        style={{ background: getColor(m.rate) }}
                      >
                        <div className="text-[13px] font-bold text-white">{fmtPct(m.rate)}</div>
                        <div className="text-[10px] text-white/60">{m.clients}</div>
                      </div>
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

/* ══════════════════════════════════════════════════════
   ChainCard — informative product card in chain level
   Shows: offer_id, name, buyers, % of L1, avg revenue
   ══════════════════════════════════════════════════════ */
function ChainCard({ product, rank, isRepeat, l1Buyers }: {
  product: { sku: number; offer_id: string; name: string; buyers: number; avg_revenue: number; pct_of_l1: number }
  rank: number; isRepeat: boolean; l1Buyers: number
}) {
  const barWidth = Math.max((product.buyers / Math.max(l1Buyers, 1)) * 100, 3)
  return (
    <div className={`rounded-xl border p-3 transition-all ${
      isRepeat
        ? 'border-emerald-500/40 bg-emerald-500/5'
        : 'border-[hsl(var(--border)/0.2)] bg-[hsl(var(--muted)/0.03)] hover:border-[hsl(var(--border)/0.4)]'
    }`}>
      {/* Row 1: rank + offer_id + repeat badge */}
      <div className="flex items-center gap-1.5 mb-1.5">
        <span className="text-[11px] font-bold text-violet-400/70">#{rank}</span>
        <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-[hsl(var(--muted)/0.15)] text-[hsl(var(--muted-foreground))] truncate max-w-[140px]">
          {product.offer_id}
        </span>
        {isRepeat && (
          <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-emerald-500/15 text-emerald-400 font-semibold whitespace-nowrap">
            🔁 Повтор
          </span>
        )}
      </div>

      {/* Row 2: name (2 lines max) */}
      <p className="text-[12px] text-[hsl(var(--foreground)/0.9)] leading-snug line-clamp-2 mb-2 font-medium">
        {product.name}
      </p>

      {/* Row 3: buyers count + % bar */}
      <div className="flex items-center gap-2 mb-1.5">
        <span className="text-[14px] font-bold text-[hsl(var(--foreground))]">{fmtNum(product.buyers)}</span>
        <span className="text-[11px] font-semibold text-violet-400">{fmtPct(product.pct_of_l1)}</span>
        <span className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)]">от L1</span>
      </div>

      {/* Progress bar */}
      <div className="h-1 rounded-full bg-[hsl(var(--muted)/0.15)] overflow-hidden mb-1.5">
        <div
          className="h-full rounded-full"
          style={{
            width: `${barWidth}%`,
            background: isRepeat
              ? 'linear-gradient(90deg, #10b981, #34d399)'
              : 'linear-gradient(90deg, #8b5cf6, #a78bfa)',
          }}
        />
      </div>

      {/* Row 4: avg revenue */}
      <div className="text-[11px] text-[hsl(var(--muted-foreground)/0.7)]">
        💰 {fmtMoney(product.avg_revenue)} / заказ
      </div>
    </div>
  )
}

/* ══════════════════════════════════════════════════════
   Purchase Chain Visualization
   Full info: offer_id, name, buyers, %, revenue, days, conversion
   ══════════════════════════════════════════════════════ */
function PurchaseChain({ chain, loading }: {
  chain: ChainResponse | null; loading: boolean
}) {
  if (!chain) {
    return (
      <div className="rounded-2xl border border-dashed border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] p-10 text-center">
        <Package size={36} className="mx-auto mb-3 text-violet-400 opacity-30" />
        <p className="text-[13px] text-[hsl(var(--muted-foreground)/0.7)]">
          {loading ? 'Загрузка цепочки...' : 'Выберите товар в таблице ниже, чтобы увидеть цепочку покупок'}
        </p>
      </div>
    )
  }

  const l1 = chain.l1
  const levels = chain.chain
  const daysMap = chain.avg_days_between
  const daysLabels: Record<number, number> = {
    2: daysMap.l1_to_l2, 3: daysMap.l2_to_l3,
    4: daysMap.l3_to_l4, 5: daysMap.l4_to_l5,
  }
  const stepNames = ['', '① Первая', '② Вторая', '③ Третья', '④ Четвёртая', '⑤ Пятая']

  return (
    <div className="rounded-2xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] overflow-hidden">
      {/* Header */}
      <div className="px-6 py-4 border-b border-[hsl(var(--border)/0.15)] flex items-center justify-between">
        <div>
          <h3 className="text-[15px] font-bold text-[hsl(var(--foreground))]">
            🔗 Цепочка покупок (1 → 2 → 3 → 4 → 5)
          </h3>
          <p className="text-[12px] text-[hsl(var(--muted-foreground)/0.7)] mt-0.5">
            Что покупают клиенты после покупки выбранного товара
          </p>
        </div>
        <div className="text-right shrink-0">
          <div className="text-[22px] font-extrabold text-violet-400 leading-none">{fmtNum(l1.total_buyers)}</div>
          <div className="text-[10px] text-[hsl(var(--muted-foreground)/0.5)] mt-0.5">покупателей L1</div>
        </div>
      </div>

      {/* Chain body */}
      <div className="p-5 overflow-x-auto">
        <div className="flex items-start min-w-[1300px]">
          {[1, 2, 3, 4, 5].map(lvlNum => {
            const lvl = levels.find(l => l.level === lvlNum)
            if (!lvl) return null
            const products = lvl.products
            if (products.length === 0 && lvlNum > 1) return null

            return (
              <div key={lvlNum} className="flex items-start">
                {/* Arrow with conversion + days */}
                {lvlNum > 1 && (
                  <div className="flex flex-col items-center justify-start pt-[50px] w-[60px] shrink-0">
                    <div className="text-[16px] font-extrabold text-violet-400 leading-none">
                      {fmtPct(lvl.conversion_from_prev)}
                    </div>
                    <div className="text-[9px] text-[hsl(var(--muted-foreground)/0.5)] mb-1">конверсия</div>
                    <ChevronRight size={18} className="text-violet-400/50" />
                    {daysLabels[lvlNum] > 0 && (
                      <div className="mt-1 text-[11px] font-bold text-amber-400 bg-amber-400/10 rounded-md px-2 py-0.5 whitespace-nowrap">
                        ⏱ {daysLabels[lvlNum]} дн
                      </div>
                    )}
                  </div>
                )}

                {/* Products column */}
                <div className="w-[230px] shrink-0">
                  <div className="text-[11px] font-bold text-[hsl(var(--muted-foreground)/0.6)] uppercase tracking-wider mb-2.5 text-center">
                    {stepNames[lvlNum]} покупка
                  </div>

                  {lvlNum === 1 ? (
                    /* L1 — target product card */
                    <div className="rounded-xl border-2 border-violet-500/40 bg-violet-500/5 p-4">
                      <div className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-violet-500/15 text-violet-400 w-fit mb-2">
                        {l1.offer_id}
                      </div>
                      <p className="text-[13px] text-[hsl(var(--foreground))] leading-snug line-clamp-3 mb-3 font-semibold">
                        {l1.name}
                      </p>
                      <div className="text-[22px] font-extrabold text-violet-400 mb-1 leading-none">
                        {fmtNum(l1.total_buyers)} <span className="text-[13px] font-medium text-[hsl(var(--muted-foreground))]">чел</span>
                      </div>
                      <div className="text-[11px] text-[hsl(var(--muted-foreground))] leading-relaxed">
                        {fmtNum(l1.total_qty)} шт · ~{fmtMoney(l1.avg_price)} / шт
                      </div>
                      <div className="mt-2 pt-2 border-t border-violet-500/20 text-[11px] text-[hsl(var(--muted-foreground))]">
                        Перешли ко 2-й: <b className="text-violet-400">
                          {levels[0] ? fmtNum(levels[0].total_buyers) : 0}
                        </b> ({levels[0] ? fmtPct(levels[0].conversion_from_l1) : '0%'})
                      </div>
                    </div>
                  ) : (
                    /* L2..L5 — product cards */
                    <div className="flex flex-col gap-2 max-h-[450px] overflow-y-auto pr-1">
                      {products.map((p, i) => (
                        <ChainCard
                          key={p.sku}
                          product={p}
                          rank={i + 1}
                          isRepeat={p.sku === l1.sku}
                          l1Buyers={l1.total_buyers}
                        />
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

/* ══════════════════════════════════════════════════════
   Time Distribution Histogram
   ══════════════════════════════════════════════════════ */
function TimeDistribution({ data }: { data: { bucket: string; count: number; avg_days: number }[] }) {
  if (!data.length) return null

  const total = data.reduce((s, d) => s + d.count, 0)
  const chartData = data.map(d => ({
    ...d,
    pct: Math.round(d.count / total * 100),
  }))

  return (
    <div className="rounded-2xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] overflow-hidden">
      <div className="px-6 py-4 border-b border-[hsl(var(--border)/0.15)]">
        <h3 className="text-base font-semibold text-[hsl(var(--foreground))]">⏱ Время до повторной покупки</h3>
        <p className="text-xs text-[hsl(var(--muted-foreground))] mt-1">Распределение дней между покупками</p>
      </div>
      <div className="p-5">
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.15} />
            <XAxis dataKey="bucket" tick={{ fontSize: 12, fill: 'hsl(var(--muted-foreground))' }} />
            <YAxis tick={{ fontSize: 12, fill: 'hsl(var(--muted-foreground))' }} />
            <Tooltip
              contentStyle={{
                background: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border))',
                borderRadius: 8,
                fontSize: 12,
              }}
              formatter={(value: number, _name: string) => [
                `${value} клиентов`, 'Кол-во'
              ]}
            />
            <Bar dataKey="count" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
        <div className="flex gap-4 mt-3 justify-center">
          {chartData.map(d => (
            <div key={d.bucket} className="text-center">
              <div className="text-[11px] text-[hsl(var(--muted-foreground))]">{d.bucket} дн</div>
              <div className="text-sm font-bold text-[hsl(var(--foreground))]">{d.pct}%</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}


/* ══════════════════════════════════════════════════════
   SKU Repeat Purchase Table
   ══════════════════════════════════════════════════════ */
function SkuTable({ rows, onSelectSku, selectedSku }: {
  rows: SkuRepeatRow[]
  onSelectSku: (sku: number) => void
  selectedSku: number | null
}) {
  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState<keyof SkuRepeatRow>('repeat_buyers')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  const handleSort = (key: keyof SkuRepeatRow) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const filtered = useMemo(() => {
    let result = [...rows]
    if (search) {
      const q = search.toLowerCase()
      result = result.filter(r =>
        r.name.toLowerCase().includes(q) ||
        r.offer_id.toLowerCase().includes(q) ||
        String(r.sku).includes(q)
      )
    }
    result.sort((a, b) => {
      const av = a[sortKey] as number
      const bv = b[sortKey] as number
      return sortDir === 'desc' ? bv - av : av - bv
    })
    return result
  }, [rows, search, sortKey, sortDir])

  const SortTh = ({ label, field }: { label: string; field: keyof SkuRepeatRow }) => (
    <th
      onClick={() => handleSort(field)}
      className="px-4 py-3.5 text-[13px] font-semibold text-[hsl(var(--muted-foreground))] cursor-pointer hover:text-[hsl(var(--foreground))] whitespace-nowrap select-none"
    >
      <div className="flex items-center gap-1 justify-end">
        {label}
        <ArrowUpDown size={12} className={sortKey === field ? 'text-violet-400' : 'opacity-30'} />
      </div>
    </th>
  )

  return (
    <div className="rounded-2xl border border-[hsl(var(--border)/0.3)] bg-[hsl(var(--card))] overflow-hidden">
      <div className="px-6 py-4 border-b border-[hsl(var(--border)/0.15)] flex items-center justify-between flex-wrap gap-3">
        <div>
          <h3 className="text-base font-semibold text-[hsl(var(--foreground))]">📦 Товары — повторные покупки</h3>
          <p className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5">{fmtNum(rows.length)} товаров · Кликните для цепочки</p>
        </div>
        <div className="relative">
          <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-[hsl(var(--muted-foreground))] opacity-50" />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Поиск по артикулу..."
            className="pl-8 pr-3 py-2 text-[13px] rounded-lg bg-[hsl(var(--muted)/0.1)] border border-[hsl(var(--border)/0.2)] w-[200px] focus:outline-none focus:border-violet-500/50"
          />
        </div>
      </div>

      <div className="overflow-auto max-h-[500px]">
        <table className="w-full text-[13px]">
          <thead className="sticky top-0 z-20 bg-[hsl(var(--card))]">
            <tr className="border-b border-[hsl(var(--border)/0.15)]">
              <th className="text-left px-4 py-3.5 text-[13px] font-semibold text-[hsl(var(--muted-foreground))] sticky left-0 z-30 bg-[hsl(var(--card))] min-w-[240px]">
                Товар
              </th>
              <SortTh label="Покупат." field="total_buyers" />
              <SortTh label="Повтор." field="repeat_buyers" />
              <SortTh label="Conv→2" field="conv_to_2" />
              <SortTh label="Conv→3" field="conv_to_3" />
              <SortTh label="Avg дней" field="avg_days_between" />
              <SortTh label="LTV повт." field="avg_ltv_repeat" />
              <SortTh label="Выручка" field="total_revenue" />
            </tr>
          </thead>
          <tbody>
            {filtered.map((r, i) => (
              <tr
                key={r.sku}
                onClick={() => onSelectSku(r.sku)}
                className={`cursor-pointer transition-colors border-b border-[hsl(var(--border)/0.06)] ${
                  selectedSku === r.sku
                    ? 'bg-violet-500/10'
                    : i % 2 === 0
                      ? 'hover:bg-[hsl(var(--muted)/0.08)]'
                      : 'bg-[hsl(var(--muted)/0.04)] hover:bg-[hsl(var(--muted)/0.10)]'
                }`}
              >
                <td className="px-4 py-3 sticky left-0 z-10 bg-inherit min-w-[240px]"
                  style={selectedSku === r.sku ? { background: 'hsl(var(--card))' } : undefined}
                >
                  <div className="flex flex-col gap-0.5">
                    <span className="text-[12px] text-[hsl(var(--foreground))] font-medium leading-tight line-clamp-1">
                      {r.name}
                    </span>
                    <span className="text-[10px] font-mono text-[hsl(var(--muted-foreground))]">{r.offer_id}</span>
                  </div>
                </td>
                <td className="px-4 py-3 text-right font-medium">{fmtNum(r.total_buyers)}</td>
                <td className="px-4 py-3 text-right">
                  <span className="font-bold text-violet-400">{fmtNum(r.repeat_buyers)}</span>
                  <span className="text-[hsl(var(--muted-foreground))] ml-1 text-[11px]">
                    ({fmtPct(r.repeat_buyers / Math.max(r.total_buyers, 1) * 100)})
                  </span>
                </td>
                <td className="px-4 py-3 text-right font-semibold">
                  <span className={r.conv_to_2 > 30 ? 'text-emerald-400' : r.conv_to_2 > 0 ? 'text-amber-400' : 'text-[hsl(var(--muted-foreground))]'}>
                    {fmtPct(r.conv_to_2)}
                  </span>
                </td>
                <td className="px-4 py-3 text-right font-semibold">
                  <span className={r.conv_to_3 > 30 ? 'text-emerald-400' : r.conv_to_3 > 0 ? 'text-amber-400' : 'text-[hsl(var(--muted-foreground))]'}>
                    {r.conv_to_3 > 0 ? fmtPct(r.conv_to_3) : '—'}
                  </span>
                </td>
                <td className="px-4 py-3 text-right text-[hsl(var(--muted-foreground))]">
                  {r.avg_days_between > 0 ? `${r.avg_days_between} дн` : '—'}
                </td>
                <td className="px-4 py-3 text-right font-medium text-emerald-400">
                  {r.avg_ltv_repeat > 0 ? fmtMoney(r.avg_ltv_repeat) : '—'}
                </td>
                <td className="px-4 py-3 text-right font-medium">{fmtMoney(r.total_revenue)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}


/* ══════════════════════════════════════════════════════
   MAIN PAGE
   ══════════════════════════════════════════════════════ */
export default function LtvPage() {
  const { currentShop } = useAppStore()
  const [period, setPeriod] = useState('6m')
  const [data, setData] = useState<LtvResponse | null>(null)
  const [chain, setChain] = useState<ChainResponse | null>(null)
  const [selectedSku, setSelectedSku] = useState<number | null>(null)
  const [loading, setLoading] = useState(false)
  const [chainLoading, setChainLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const isOzon = currentShop?.marketplace === 'ozon'

  // Load main LTV data
  const loadData = useCallback(async () => {
    if (!currentShop || !isOzon) return
    setLoading(true)
    setError(null)
    try {
      const result = await fetchLtv(currentShop.id, period)
      setData(result)
    } catch (e: any) {
      setError(e?.response?.data?.detail || 'Ошибка загрузки данных')
    } finally {
      setLoading(false)
    }
  }, [currentShop, period, isOzon])

  useEffect(() => { loadData() }, [loadData])

  // Load chain when SKU selected
  const loadChain = useCallback(async (sku: number) => {
    if (!currentShop) return
    setSelectedSku(sku)
    setChainLoading(true)
    try {
      const result = await fetchPurchaseChain(currentShop.id, sku, period)
      setChain(result)
    } catch {
      setChain(null)
    } finally {
      setChainLoading(false)
    }
  }, [currentShop, period])

  // Not Ozon — show message
  if (!isOzon) {
    return (
      <div className="flex items-center justify-center h-[60vh]">
        <div className="text-center">
          <Users size={48} className="mx-auto mb-4 text-[hsl(var(--muted-foreground))] opacity-30" />
          <h2 className="text-lg font-semibold text-[hsl(var(--foreground))] mb-2">LTV-анализ доступен только для Ozon</h2>
          <p className="text-sm text-[hsl(var(--muted-foreground))]">
            Wildberries не предоставляет ID клиента в данных заказов
          </p>
        </div>
      </div>
    )
  }

  // Loading
  if (loading && !data) {
    return (
      <div className="flex items-center justify-center h-[60vh]">
        <div className="text-center">
          <div className="w-10 h-10 border-2 border-violet-500/30 border-t-violet-500 rounded-full animate-spin mx-auto mb-3" />
          <p className="text-sm text-[hsl(var(--muted-foreground))]">Анализируем клиентов…</p>
        </div>
      </div>
    )
  }

  // Error
  if (error) {
    return (
      <div className="flex items-center justify-center h-[60vh]">
        <div className="text-center">
          <p className="text-red-400 mb-2">{error}</p>
          <button onClick={loadData} className="text-sm text-violet-400 hover:underline">Попробовать снова</button>
        </div>
      </div>
    )
  }

  if (!data) return null

  const kpi = data.kpi

  return (
    <div className="space-y-6 pb-10">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-[hsl(var(--foreground))]">👥 LTV клиентов</h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))] mt-1">
            Анализ повторных покупок и цепочек продаж · {data.date_range.start} — {data.date_range.end}
          </p>
        </div>
        <div className="flex gap-1.5 bg-[hsl(var(--muted)/0.1)] rounded-lg p-1">
          {PERIODS.map(p => (
            <button
              key={p.value}
              onClick={() => setPeriod(p.value)}
              className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all ${
                period === p.value
                  ? 'bg-violet-600 text-white shadow-sm'
                  : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'
              }`}
            >
              {p.label}
            </button>
          ))}
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
        <KpiCard
          label="Уникальные клиенты"
          value={fmtNum(kpi.total_clients)}
          icon={Users}
          color="#8b5cf6"
        />
        <KpiCard
          label="Повторные"
          value={fmtNum(kpi.repeat_clients)}
          sub={`${fmtPct(kpi.repeat_rate)} от всех`}
          icon={Repeat}
          color="#10b981"
        />
        <KpiCard
          label="Средний LTV"
          value={fmtMoney(kpi.avg_ltv)}
          sub="выручка / клиент"
          icon={TrendingUp}
          color="#f59e0b"
        />
        <KpiCard
          label="Средний чек"
          value={fmtMoney(kpi.avg_check)}
          icon={ShoppingCart}
          color="#6366f1"
        />
        <KpiCard
          label="Заказов / клиент"
          value={kpi.avg_orders_per_client.toFixed(2)}
          sub={`Выручка: ${fmtMoney(kpi.total_revenue)}`}
          icon={DollarSign}
          color="#ec4899"
        />
      </div>

      {/* Cohort Matrix */}
      <CohortMatrix cohorts={data.cohort_matrix} />

      {/* Purchase Chain */}
      <div className="relative">
        {chainLoading && (
          <div className="absolute inset-0 bg-[hsl(var(--card)/0.7)] z-50 flex items-center justify-center rounded-2xl">
            <div className="w-8 h-8 border-2 border-violet-500/30 border-t-violet-500 rounded-full animate-spin" />
          </div>
        )}
        <PurchaseChain chain={chain} loading={chainLoading} />
      </div>

      {/* SKU Table */}
      <SkuTable rows={data.sku_table} onSelectSku={loadChain} selectedSku={selectedSku} />

      {/* Time Distribution */}
      <TimeDistribution data={data.time_distribution} />
    </div>
  )
}
