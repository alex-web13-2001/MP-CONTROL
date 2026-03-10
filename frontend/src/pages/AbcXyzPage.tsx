import { useState, useEffect, useMemo, useCallback } from 'react'
import { fetchAbcXyz, fetchWbAbcXyz, downloadAbcXyzXlsx, type AbcXyzProduct, type AbcXyzResponse } from '@/api/abc-xyz'
import { useAppStore } from '@/stores/appStore'

/* Sticky cell styles — must be opaque to hide content scrolling behind */
const stickyBase: React.CSSProperties = {
  position: 'sticky',
  left: 0,
  boxShadow: '2px 0 8px -2px rgba(0,0,0,0.15)',
}

/* ── helpers ── */
const fmtMoney = (v: number) =>
  v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
const fmtNum = (v: number) => v.toLocaleString('ru-RU')
const fmtPct = (v: number) => v.toFixed(1) + '%'

/** Русское склонение: 1 товар, 2 товара, 5 товаров */
function pluralize(n: number, one: string, few: string, many: string) {
  const abs = Math.abs(n) % 100
  const last = abs % 10
  if (abs > 10 && abs < 20) return `${n} ${many}`
  if (last > 1 && last < 5) return `${n} ${few}`
  if (last === 1) return `${n} ${one}`
  return `${n} ${many}`
}

/* ── color config ── */
const ABC_CFG = {
  A: { gradient: 'from-emerald-500/20 to-emerald-600/5', border: 'border-emerald-500/30', badge: 'bg-emerald-500/20 text-emerald-400', label: 'Лидеры', desc: '80% дохода' },
  B: { gradient: 'from-amber-500/20 to-amber-600/5', border: 'border-amber-500/30', badge: 'bg-amber-500/20 text-amber-400', label: 'Средние', desc: '15% дохода' },
  C: { gradient: 'from-red-500/20 to-red-600/5', border: 'border-red-500/30', badge: 'bg-red-500/20 text-red-400', label: 'Аутсайдеры', desc: '5% дохода' },
} as const

const XYZ_CFG = {
  X: { gradient: 'from-blue-500/20 to-blue-600/5', border: 'border-blue-500/30', badge: 'bg-blue-500/20 text-blue-400', label: 'Стабильные', desc: 'CV < 10%' },
  Y: { gradient: 'from-violet-500/20 to-violet-600/5', border: 'border-violet-500/30', badge: 'bg-violet-500/20 text-violet-400', label: 'Колеблющиеся', desc: 'CV 10–25%' },
  Z: { gradient: 'from-orange-500/20 to-orange-600/5', border: 'border-orange-500/30', badge: 'bg-orange-500/20 text-orange-400', label: 'Хаотичные', desc: 'CV > 25%' },
} as const

/* Matrix cell gradient — green for best, red for worst */
const MATRIX_CELL: Record<string, { bg: string; text: string; emoji: string }> = {
  AX: { bg: 'bg-gradient-to-br from-emerald-500/30 to-emerald-600/10', text: 'text-emerald-300', emoji: '🌟' },
  AY: { bg: 'bg-gradient-to-br from-emerald-500/20 to-amber-500/10', text: 'text-emerald-400', emoji: '📈' },
  AZ: { bg: 'bg-gradient-to-br from-amber-500/20 to-orange-500/10', text: 'text-amber-400', emoji: '⚡' },
  BX: { bg: 'bg-gradient-to-br from-blue-500/20 to-emerald-500/10', text: 'text-blue-400', emoji: '✅' },
  BY: { bg: 'bg-gradient-to-br from-amber-500/20 to-amber-600/10', text: 'text-amber-400', emoji: '📊' },
  BZ: { bg: 'bg-gradient-to-br from-orange-500/20 to-red-500/10', text: 'text-orange-400', emoji: '⚠️' },
  CX: { bg: 'bg-gradient-to-br from-blue-500/15 to-blue-600/5', text: 'text-blue-400', emoji: '💤' },
  CY: { bg: 'bg-gradient-to-br from-orange-500/15 to-orange-600/5', text: 'text-orange-400', emoji: '📉' },
  CZ: { bg: 'bg-gradient-to-br from-red-500/25 to-red-600/10', text: 'text-red-400', emoji: '🚫' },
}

const MATRIX_DESC: Record<string, string> = {
  AX: 'Звёзды — высокий доход, стабильный спрос',
  AY: 'Высокий доход, умеренные колебания',
  AZ: 'Высокий доход, непредсказуемый спрос',
  BX: 'Стабильный спрос, средний доход',
  BY: 'Средний доход, умеренные колебания',
  BZ: 'Средний доход, хаотичный спрос',
  CX: 'Стабильные, но малый доход',
  CY: 'Слабые с колебаниями',
  CZ: 'Проблемные — малый доход и хаос',
}

/* ═══════════════════════════════════════ */
/*              PAGE COMPONENT            */
/* ═══════════════════════════════════════ */

export default function AbcXyzPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const [data, setData] = useState<AbcXyzResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [period, setPeriod] = useState(90)
  const [useProfit, setUseProfit] = useState(false)
  const [selectedCell, setSelectedCell] = useState<string | null>(null)
  const [downloading, setDownloading] = useState(false)

  const handleDownloadXlsx = async () => {
    if (!currentShop) return
    setDownloading(true)
    try {
      await downloadAbcXyzXlsx(
        currentShop.marketplace as 'ozon' | 'wildberries',
        currentShop.id,
        period,
        useProfit,
      )
    } catch (e) {
      console.error('XLSX download error', e)
    } finally {
      setDownloading(false)
    }
  }

  const isWb = currentShop?.marketplace === 'wildberries'
  const isOzon = currentShop?.marketplace === 'ozon'

  const load = useCallback(async () => {
    if (!currentShop) return
    if (!isOzon && !isWb) return
    setLoading(true)
    try {
      const apiFn = isWb ? fetchWbAbcXyz : fetchAbcXyz
      const res = await apiFn(currentShop.id, period, useProfit)
      setData(res)
    } catch (e) {
      console.error('ABC/XYZ fetch error', e)
    } finally {
      setLoading(false)
    }
  }, [currentShop, period, useProfit, isOzon, isWb])

  useEffect(() => { load() }, [load])

  const filteredProducts = useMemo(() => {
    if (!data) return []
    if (!selectedCell) return data.products
    const [abc, xyz] = [selectedCell[0], selectedCell[1]]
    return data.products.filter((p) => p.abc_group === abc && p.xyz_group === xyz)
  }, [data, selectedCell])

  if (!currentShop) {
    return (
      <div className="flex items-center justify-center h-64 text-[hsl(var(--muted-foreground))]">
        Выберите магазин
      </div>
    )
  }

  return (
    <div className="space-y-8 p-6 lg:p-8 max-w-[1600px] mx-auto">

      {/* ═══ HEADER ═══ */}
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-[hsl(var(--foreground))] tracking-tight">
            ABC / XYZ Анализ
          </h1>
          <p className="text-[15px] text-[hsl(var(--muted-foreground))] mt-2 leading-relaxed">
            Классификация товаров по вкладу в {useProfit ? 'прибыль' : 'выручку'} и стабильности спроса
          </p>
        </div>

        <div className="flex items-center gap-3 shrink-0">
          {/* Period picker */}
          <div className="flex rounded-xl overflow-hidden border border-[hsl(var(--border))] bg-[hsl(var(--card))]">
            {[30, 60, 90].map((p) => (
              <button
                key={p}
                onClick={() => { setPeriod(p); setSelectedCell(null) }}
                className={`px-5 py-2.5 text-sm font-semibold transition-all ${
                  period === p
                    ? 'bg-[hsl(var(--primary))] text-white shadow-lg shadow-[hsl(var(--primary)/0.3)]'
                    : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
                }`}
              >
                {p} дн.
              </button>
            ))}
          </div>

          {/* Profit / Revenue toggle */}
          <button
            onClick={() => { setUseProfit(!useProfit); setSelectedCell(null) }}
            className={`px-5 py-2.5 text-sm font-semibold rounded-xl border-2 transition-all ${
              useProfit
                ? 'border-emerald-500/50 bg-emerald-500/10 text-emerald-400 shadow-lg shadow-emerald-500/10'
                : 'border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.5)]'
            }`}
          >
            {useProfit ? '📊 По прибыли' : '💰 По выручке'}
          </button>

          {/* Download Excel */}
          {data && data.products.length > 0 && (
            <button
              onClick={handleDownloadXlsx}
              disabled={downloading}
              className="px-5 py-2.5 text-sm font-semibold rounded-xl border-2 border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.5)] hover:border-[hsl(var(--primary)/0.5)] transition-all disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {downloading ? '⏳ Загрузка...' : '📥 Excel'}
            </button>
          )}
        </div>
      </div>

      {loading ? (
        <div className="flex items-center justify-center h-80">
          <div className="flex flex-col items-center gap-3">
            <div className="animate-spin rounded-full h-10 w-10 border-[3px] border-[hsl(var(--primary))] border-t-transparent" />
            <span className="text-sm text-[hsl(var(--muted-foreground))]">Загрузка данных…</span>
          </div>
        </div>
      ) : data && data.products.length > 0 ? (
        <>
          {/* ═══ SUMMARY CARDS ═══ */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* ABC group */}
            <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-6">
              <div className="flex items-center gap-2 mb-5">
                <span className="text-lg font-bold text-[hsl(var(--foreground))]">ABC</span>
                <span className="text-sm text-[hsl(var(--muted-foreground))]">— вклад в {useProfit ? 'прибыль' : 'выручку'}</span>
              </div>
              <div className="grid grid-cols-3 gap-4">
                {(['A', 'B', 'C'] as const).map((g) => {
                  const s = data.summary[g]
                  const c = ABC_CFG[g]
                  return (
                    <div key={g} className={`rounded-xl border ${c.border} bg-gradient-to-br ${c.gradient} p-4 text-center`}>
                      <span className={`inline-flex items-center justify-center w-10 h-10 rounded-xl ${c.badge} font-bold text-xl mb-3`}>{g}</span>
                      <div className="text-3xl font-bold text-[hsl(var(--foreground))]">{s?.count || 0}</div>
                      <div className="text-sm text-[hsl(var(--muted-foreground))] mt-1">{c.label}</div>
                      <div className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5 opacity-70">
                        {fmtPct(s?.revenue_share || 0)} {useProfit ? 'прибыли' : 'выручки'}
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>

            {/* XYZ group */}
            <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-6">
              <div className="flex items-center gap-2 mb-5">
                <span className="text-lg font-bold text-[hsl(var(--foreground))]">XYZ</span>
                <span className="text-sm text-[hsl(var(--muted-foreground))]">— стабильность спроса</span>
              </div>
              <div className="grid grid-cols-3 gap-4">
                {(['X', 'Y', 'Z'] as const).map((g) => {
                  const s = data.summary[g]
                  const c = XYZ_CFG[g]
                  return (
                    <div key={g} className={`rounded-xl border ${c.border} bg-gradient-to-br ${c.gradient} p-4 text-center`}>
                      <span className={`inline-flex items-center justify-center w-10 h-10 rounded-xl ${c.badge} font-bold text-xl mb-3`}>{g}</span>
                      <div className="text-3xl font-bold text-[hsl(var(--foreground))]">{s?.count || 0}</div>
                      <div className="text-sm text-[hsl(var(--muted-foreground))] mt-1">{c.label}</div>
                      <div className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5 opacity-70">{c.desc}</div>
                    </div>
                  )
                })}
              </div>
            </div>
          </div>

          {/* ═══ MATRIX 3×3 ═══ */}
          <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-6 lg:p-8">
            <div className="flex items-center justify-between mb-6">
              <div>
                <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Матрица ABC × XYZ</h2>
                <p className="text-sm text-[hsl(var(--muted-foreground))] mt-1">Нажмите на ячейку для фильтрации таблицы</p>
              </div>
              {selectedCell && (
                <button
                  onClick={() => setSelectedCell(null)}
                  className="px-4 py-2 text-sm font-medium rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.6)] transition-all"
                >
                  ✕ Сбросить фильтр
                </button>
              )}
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-[1fr_280px] gap-8">
              {/* Grid */}
              <div>
                <div className="grid grid-cols-[60px_1fr_1fr_1fr] gap-2 mb-2">
                  <div />
                  {(['X', 'Y', 'Z'] as const).map((x) => (
                    <div key={x} className="text-center">
                      <span className={`inline-flex items-center justify-center w-9 h-9 rounded-xl ${XYZ_CFG[x].badge} font-bold text-base`}>{x}</span>
                      <div className="text-[11px] text-[hsl(var(--muted-foreground))] mt-1">{XYZ_CFG[x].label}</div>
                    </div>
                  ))}
                </div>

                {(['A', 'B', 'C'] as const).map((a) => (
                  <div key={a} className="grid grid-cols-[60px_1fr_1fr_1fr] gap-2 mb-2">
                    <div className="flex flex-col items-center justify-center">
                      <span className={`inline-flex items-center justify-center w-9 h-9 rounded-xl ${ABC_CFG[a].badge} font-bold text-base`}>{a}</span>
                      <div className="text-[11px] text-[hsl(var(--muted-foreground))] mt-1">{ABC_CFG[a].label}</div>
                    </div>
                    {(['X', 'Y', 'Z'] as const).map((x) => {
                      const key = `${a}${x}`
                      const count = data.matrix[key] || 0
                      const cell = MATRIX_CELL[key]
                      const isSelected = selectedCell === key
                      return (
                        <button
                          key={key}
                          onClick={() => setSelectedCell(isSelected ? null : key)}
                          title={MATRIX_DESC[key]}
                          className={`
                            relative rounded-2xl py-5 px-4 text-center transition-all duration-200 cursor-pointer
                            border-2 ${cell.bg}
                            ${isSelected
                              ? 'border-[hsl(var(--primary))] ring-2 ring-[hsl(var(--primary)/0.3)] scale-[1.02] shadow-lg'
                              : 'border-transparent hover:border-[hsl(var(--border))] hover:scale-[1.01] hover:shadow-md'
                            }
                          `}
                        >
                          <div className={`text-3xl font-bold ${cell.text}`}>{count}</div>
                          <div className="text-xs text-[hsl(var(--muted-foreground))] mt-1">
                            {pluralize(count, 'товар', 'товара', 'товаров')}
                          </div>
                          {count > 0 && <div className="text-sm mt-1 opacity-50">{cell.emoji}</div>}
                        </button>
                      )
                    })}
                  </div>
                ))}
              </div>

              {/* Legend */}
              <div className="space-y-2.5">
                <div className="text-sm font-bold text-[hsl(var(--foreground))] mb-3">Легенда</div>
                {Object.entries(MATRIX_DESC).map(([key, desc]) => {
                  const cell = MATRIX_CELL[key]
                  const count = data.matrix[key] || 0
                  return (
                    <div
                      key={key}
                      className={`flex items-center gap-3 p-2 rounded-lg transition-colors ${
                        selectedCell === key ? 'bg-[hsl(var(--muted)/0.5)]' : 'hover:bg-[hsl(var(--muted)/0.2)]'
                      }`}
                    >
                      <span className={`shrink-0 inline-flex items-center justify-center w-10 h-7 rounded-lg ${cell.bg} ${cell.text} text-xs font-bold`}>{key}</span>
                      <div className="min-w-0">
                        <div className="text-sm text-[hsl(var(--foreground))] leading-tight">{desc}</div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))]">{pluralize(count, 'товар', 'товара', 'товаров')}</div>
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          </div>

          {/* ═══ PRODUCTS TABLE ═══ */}
          <ProductsTable
            products={filteredProducts}
            selectedCell={selectedCell}
            onClearFilter={() => setSelectedCell(null)}
          />
        </>
      ) : (
        <div className="flex flex-col items-center justify-center h-64 gap-2">
          <div className="text-4xl">📦</div>
          <div className="text-lg text-[hsl(var(--muted-foreground))]">Нет данных за выбранный период</div>
          <div className="text-sm text-[hsl(var(--muted-foreground))] opacity-60">Попробуйте увеличить период или переключить магазин</div>
        </div>
      )}
    </div>
  )
}

/* ═══════════════════════════════════════ */
/*           PRODUCTS TABLE               */
/* ═══════════════════════════════════════ */

type SortKey = keyof AbcXyzProduct

function ProductsTable({
  products,
  selectedCell,
  onClearFilter,
}: {
  products: AbcXyzProduct[]
  selectedCell: string | null
  onClearFilter: () => void
}) {
  const [sortKey, setSortKey] = useState<SortKey>('revenue')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  const toggleSort = useCallback((key: SortKey) => {
    if (key === sortKey) {
      setSortDir((d) => (d === 'desc' ? 'asc' : 'desc'))
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }, [sortKey])

  const sorted = useMemo(() => {
    return [...products].sort((a, b) => {
      const av = a[sortKey] as number
      const bv = b[sortKey] as number
      return sortDir === 'desc' ? bv - av : av - bv
    })
  }, [products, sortKey, sortDir])

  const SortTh = ({ k, children }: { k: SortKey; children: React.ReactNode }) => (
    <th
      className={`
        px-4 py-3.5 text-right text-[13px] font-semibold whitespace-nowrap select-none cursor-pointer
        transition-colors
        ${sortKey === k ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'}
      `}
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
    <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
      {/* Table title bar */}
      <div className="flex items-center justify-between px-6 py-5 border-b border-[hsl(var(--border))]">
        <div className="flex items-center gap-3">
          <h2 className="text-xl font-bold text-[hsl(var(--foreground))]">Товары</h2>
          {selectedCell && (
            <span className="flex items-center gap-2">
              <span className={`inline-flex items-center px-3 py-1 rounded-lg text-sm font-semibold ${MATRIX_CELL[selectedCell]?.bg} ${MATRIX_CELL[selectedCell]?.text}`}>
                {selectedCell}
              </span>
              <button onClick={onClearFilter} className="text-sm text-[hsl(var(--primary))] hover:underline font-medium">
                ✕ Сбросить
              </button>
            </span>
          )}
        </div>
        <span className="text-sm text-[hsl(var(--muted-foreground))] font-medium">
          {pluralize(products.length, 'товар', 'товара', 'товаров')}
        </span>
      </div>

      {/* Scrollable table — sticky header (vertical) + sticky first column (horizontal) */}
      <div className="overflow-auto max-h-[600px] relative">
        <table className="w-full border-collapse">
          <thead className="sticky top-0 z-20">
            <tr className="border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
              <th
                className="px-4 py-3.5 text-left text-[13px] font-semibold text-[hsl(var(--muted-foreground))] w-[220px] min-w-[220px] max-w-[220px] bg-[hsl(var(--card))]"
                style={{ ...stickyBase, zIndex: 30 }}
              >
                Товар
              </th>
              <SortTh k="revenue">Выручка</SortTh>
              <SortTh k="profit">Прибыль</SortTh>
              <SortTh k="margin_pct">Маржа</SortTh>
              <SortTh k="orders">Заказы</SortTh>
              <SortTh k="avg_price">Ср. цена</SortTh>
              <SortTh k="cost_price">С/с</SortTh>
              <SortTh k="mp_fees">МП расх.</SortTh>
              <SortTh k="ad_spend">Реклама</SortTh>
              <th className="px-4 py-3.5 text-center text-[13px] font-semibold text-[hsl(var(--muted-foreground))]">ABC</th>
              <SortTh k="abc_share">Доля</SortTh>
              <th className="px-4 py-3.5 text-center text-[13px] font-semibold text-[hsl(var(--muted-foreground))]">XYZ</th>
              <SortTh k="xyz_cv">CV%</SortTh>
            </tr>
          </thead>
          <tbody>
            {sorted.map((p, idx) => {
              const abc = ABC_CFG[p.abc_group as keyof typeof ABC_CFG]
              const xyz = XYZ_CFG[p.xyz_group as keyof typeof XYZ_CFG]
              const marginColor = p.margin_pct > 20 ? 'text-emerald-400' : p.margin_pct > 0 ? 'text-amber-400' : 'text-red-400'
              const rowBg = idx % 2 === 0 ? 'bg-[hsl(var(--card))]' : 'bg-[hsl(var(--muted)/0.06)]'

              return (
                <tr
                  key={p.sku}
                  className={`border-b border-[hsl(var(--border)/0.2)] transition-colors ${rowBg} hover:bg-[hsl(var(--muted)/0.2)] group`}
                >
                  {/* Product — sticky left */}
                  <td
                    className={`px-4 py-3 w-[220px] min-w-[220px] max-w-[220px] ${rowBg} group-hover:bg-[hsl(var(--muted)/0.2)]`}
                    style={{ ...stickyBase, zIndex: 10 }}
                  >
                    <div className="flex items-center gap-2.5">
                      {p.image_url ? (
                        <img src={p.image_url} alt="" className="w-9 h-9 rounded-lg object-cover shrink-0 border border-[hsl(var(--border)/0.3)]" />
                      ) : (
                        <div className="w-9 h-9 rounded-lg bg-[hsl(var(--muted)/0.3)] shrink-0 flex items-center justify-center text-sm">📦</div>
                      )}
                      <div className="min-w-0 flex-1">
                        <div className="text-[13px] font-medium text-[hsl(var(--foreground))] truncate" title={p.name || p.offer_id}>
                          {p.name || p.offer_id}
                        </div>
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))] opacity-60 truncate">{p.offer_id}</div>
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-[13px] font-semibold text-[hsl(var(--foreground))] whitespace-nowrap">{fmtMoney(p.revenue)}</td>
                  <td className={`px-4 py-3 text-right tabular-nums text-[13px] font-semibold whitespace-nowrap ${p.profit > 0 ? 'text-emerald-400' : p.profit < 0 ? 'text-red-400' : 'text-[hsl(var(--muted-foreground))]'}`}>{fmtMoney(p.profit)}</td>
                  <td className={`px-4 py-3 text-right tabular-nums text-[13px] font-semibold whitespace-nowrap ${marginColor}`}>{fmtPct(p.margin_pct)}</td>
                  <td className="px-4 py-3 text-right tabular-nums text-[13px] whitespace-nowrap">{fmtNum(p.orders)}</td>
                  <td className="px-4 py-3 text-right tabular-nums text-[13px] text-[hsl(var(--muted-foreground))] whitespace-nowrap">{fmtMoney(p.avg_price)}</td>
                  <td className="px-4 py-3 text-right tabular-nums text-[13px] text-[hsl(var(--muted-foreground))] whitespace-nowrap">
                    {p.cost_price > 0 ? fmtMoney(p.cost_price) : <span className="opacity-40">—</span>}
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-[13px] text-[hsl(var(--muted-foreground))] whitespace-nowrap">
                    {(p.mp_fees ?? 0) > 0 ? fmtMoney(p.mp_fees ?? 0) : <span className="opacity-40">—</span>}
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-[13px] text-[hsl(var(--muted-foreground))] whitespace-nowrap">
                    {(p.ad_spend ?? 0) > 0 ? fmtMoney(p.ad_spend ?? 0) : <span className="opacity-40">—</span>}
                  </td>
                  <td className="px-4 py-3 text-center">
                    <span className={`inline-flex items-center justify-center w-7 h-7 rounded-lg ${abc.badge} font-bold text-xs`}>{p.abc_group}</span>
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-[13px] whitespace-nowrap">{fmtPct(p.abc_share)}</td>
                  <td className="px-4 py-3 text-center">
                    <span className={`inline-flex items-center justify-center w-7 h-7 rounded-lg ${xyz.badge} font-bold text-xs`}>{p.xyz_group}</span>
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-[13px] whitespace-nowrap">{fmtPct(p.xyz_cv)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
