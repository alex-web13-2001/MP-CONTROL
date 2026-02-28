import { useState, useEffect, useMemo, useCallback } from 'react'
import { fetchAbcXyz, type AbcXyzProduct, type AbcXyzResponse } from '@/api/abc-xyz'
import { useAppStore } from '@/stores/appStore'

/* ── helpers ── */
const formatMoney = (v: number) =>
  v.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) + ' ₽'
const formatNumber = (v: number) => v.toLocaleString('ru-RU')

/* ── ABC / XYZ color maps ── */
const abcColors: Record<string, { bg: string; text: string; label: string }> = {
  A: { bg: 'bg-emerald-500/15', text: 'text-emerald-400', label: 'Лидеры — 80% дохода' },
  B: { bg: 'bg-amber-500/15', text: 'text-amber-400', label: 'Средние — 15% дохода' },
  C: { bg: 'bg-red-500/15', text: 'text-red-400', label: 'Аутсайдеры — 5% дохода' },
}
const xyzColors: Record<string, { bg: string; text: string; label: string }> = {
  X: { bg: 'bg-blue-500/15', text: 'text-blue-400', label: 'Стабильный спрос' },
  Y: { bg: 'bg-violet-500/15', text: 'text-violet-400', label: 'Умеренная колебания' },
  Z: { bg: 'bg-orange-500/15', text: 'text-orange-400', label: 'Непредсказуемый спрос' },
}

/* ── Matrix cell colors ── */
const matrixCellColor: Record<string, string> = {
  AX: 'bg-emerald-500/25 text-emerald-300',
  AY: 'bg-emerald-500/15 text-emerald-400',
  AZ: 'bg-amber-500/15 text-amber-400',
  BX: 'bg-emerald-500/15 text-emerald-400',
  BY: 'bg-amber-500/15 text-amber-400',
  BZ: 'bg-orange-500/15 text-orange-400',
  CX: 'bg-amber-500/15 text-amber-400',
  CY: 'bg-orange-500/15 text-orange-400',
  CZ: 'bg-red-500/20 text-red-400',
}

const matrixDescriptions: Record<string, string> = {
  AX: 'Топ — высокий доход, стабильный спрос',
  AY: 'Высокий доход, колеблющийся спрос',
  AZ: 'Высокий доход, непредсказуемый спрос',
  BX: 'Средний доход, стабильный спрос',
  BY: 'Средний доход, колеблющийся спрос',
  BZ: 'Средний доход, непредсказуемый спрос',
  CX: 'Низкий доход, стабильный спрос',
  CY: 'Низкий доход, колеблющийся спрос',
  CZ: 'Проблемные — низкий доход, хаотичный спрос',
}

/* ── Page Component ── */
export default function AbcXyzPage() {
  const currentShop = useAppStore((s) => s.currentShop)
  const [data, setData] = useState<AbcXyzResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [period, setPeriod] = useState(90)
  const [useProfit, setUseProfit] = useState(false)
  const [selectedCell, setSelectedCell] = useState<string | null>(null)

  const load = useCallback(async () => {
    if (!currentShop) return
    setLoading(true)
    try {
      const res = await fetchAbcXyz(currentShop.id, period, useProfit)
      setData(res)
    } catch (e) {
      console.error('ABC/XYZ fetch error', e)
    } finally {
      setLoading(false)
    }
  }, [currentShop, period, useProfit])

  useEffect(() => { load() }, [load])

  /* Filter by matrix cell */
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
    <div className="space-y-6 p-6 max-w-[1600px] mx-auto">
      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[hsl(var(--foreground))]">ABC / XYZ Анализ</h1>
          <p className="text-sm text-[hsl(var(--muted-foreground))] mt-1">
            Классификация товаров по вкладу в доход и стабильности спроса
          </p>
        </div>

        <div className="flex items-center gap-3">
          {/* Period selector */}
          <div className="flex rounded-xl border border-[hsl(var(--border))] overflow-hidden">
            {[30, 60, 90].map((p) => (
              <button
                key={p}
                onClick={() => setPeriod(p)}
                className={`px-4 py-2 text-sm font-medium transition-colors ${
                  period === p
                    ? 'bg-[hsl(var(--primary))] text-white'
                    : 'bg-[hsl(var(--card))] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted))]'
                }`}
              >
                {p} дн.
              </button>
            ))}
          </div>

          {/* Profit/Revenue toggle */}
          <button
            onClick={() => setUseProfit(!useProfit)}
            className={`px-4 py-2 text-sm font-medium rounded-xl border transition-colors ${
              useProfit
                ? 'border-emerald-500/50 bg-emerald-500/10 text-emerald-400'
                : 'border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--muted))]'
            }`}
          >
            {useProfit ? '📊 По прибыли' : '💰 По выручке'}
          </button>
        </div>
      </div>

      {loading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-8 w-8 border-2 border-[hsl(var(--primary))] border-t-transparent" />
        </div>
      ) : data && data.products.length > 0 ? (
        <>
          {/* ── Summary Cards ── */}
          <div className="grid grid-cols-6 gap-3">
            {['A', 'B', 'C'].map((g) => {
              const s = data.summary[g]
              const c = abcColors[g]
              return (
                <div key={g} className={`rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-4`}>
                  <div className="flex items-center gap-2 mb-2">
                    <span className={`inline-flex items-center justify-center w-8 h-8 rounded-lg ${c.bg} ${c.text} font-bold text-lg`}>{g}</span>
                    <span className="text-xs text-[hsl(var(--muted-foreground))]">{c.label}</span>
                  </div>
                  <div className="text-2xl font-bold text-[hsl(var(--foreground))]">{s?.count || 0}</div>
                  <div className="text-xs text-[hsl(var(--muted-foreground))]">{s?.revenue_share || 0}% выручки</div>
                </div>
              )
            })}
            {['X', 'Y', 'Z'].map((g) => {
              const s = data.summary[g]
              const c = xyzColors[g]
              return (
                <div key={g} className={`rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-4`}>
                  <div className="flex items-center gap-2 mb-2">
                    <span className={`inline-flex items-center justify-center w-8 h-8 rounded-lg ${c.bg} ${c.text} font-bold text-lg`}>{g}</span>
                    <span className="text-xs text-[hsl(var(--muted-foreground))]">{c.label}</span>
                  </div>
                  <div className="text-2xl font-bold text-[hsl(var(--foreground))]">{s?.count || 0}</div>
                  <div className="text-xs text-[hsl(var(--muted-foreground))]">{s?.count || 0} товаров</div>
                </div>
              )
            })}
          </div>

          {/* ── Matrix 3x3 ── */}
          <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-6">
            <h2 className="text-lg font-semibold text-[hsl(var(--foreground))] mb-4">Матрица ABC × XYZ</h2>
            <p className="text-xs text-[hsl(var(--muted-foreground))] mb-4">Нажмите на ячейку для фильтрации таблицы товаров</p>

            <div className="flex gap-6">
              {/* Matrix grid */}
              <div className="flex-1">
                <div className="grid grid-cols-4 gap-1">
                  {/* Header row */}
                  <div />
                  {['X', 'Y', 'Z'].map((x) => (
                    <div key={x} className="text-center py-2">
                      <span className={`inline-flex items-center justify-center w-7 h-7 rounded-lg ${xyzColors[x].bg} ${xyzColors[x].text} font-bold text-sm`}>{x}</span>
                    </div>
                  ))}

                  {/* Data rows */}
                  {['A', 'B', 'C'].map((a) => (
                    <>
                      <div key={`label-${a}`} className="flex items-center justify-center py-2">
                        <span className={`inline-flex items-center justify-center w-7 h-7 rounded-lg ${abcColors[a].bg} ${abcColors[a].text} font-bold text-sm`}>{a}</span>
                      </div>
                      {['X', 'Y', 'Z'].map((x) => {
                        const key = `${a}${x}` as keyof typeof data.matrix
                        const count = data.matrix[key] || 0
                        const isSelected = selectedCell === `${a}${x}`
                        return (
                          <button
                            key={key}
                            onClick={() => setSelectedCell(isSelected ? null : `${a}${x}`)}
                            className={`rounded-xl p-4 text-center transition-all duration-200 cursor-pointer border-2 ${
                              isSelected
                                ? 'border-[hsl(var(--primary))] ring-2 ring-[hsl(var(--primary)/0.3)]'
                                : 'border-transparent hover:border-[hsl(var(--border))]'
                            } ${matrixCellColor[key] || 'bg-[hsl(var(--muted)/0.3)]'}`}
                          >
                            <div className="text-2xl font-bold">{count}</div>
                            <div className="text-[10px] opacity-70 mt-0.5">товаров</div>
                          </button>
                        )
                      })}
                    </>
                  ))}
                </div>
              </div>

              {/* Legend */}
              <div className="w-64 space-y-2 text-xs">
                <div className="font-semibold text-[hsl(var(--foreground))] mb-2">Легенда</div>
                {Object.entries(matrixDescriptions).map(([key, desc]) => (
                  <div key={key} className="flex items-start gap-2">
                    <span className={`shrink-0 inline-flex items-center justify-center w-7 h-5 rounded ${matrixCellColor[key]} text-[10px] font-bold`}>{key}</span>
                    <span className="text-[hsl(var(--muted-foreground))]">{desc}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* ── Products Table ── */}
          <ProductsTable
            products={filteredProducts}
            selectedCell={selectedCell}
            onClearFilter={() => setSelectedCell(null)}
          />
        </>
      ) : (
        <div className="flex items-center justify-center h-64 text-[hsl(var(--muted-foreground))]">
          Нет данных за выбранный период
        </div>
      )}
    </div>
  )
}

/* ── Products Table ── */
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

  const thBase = 'px-3 py-2.5 text-right text-[12px] font-medium whitespace-nowrap select-none'

  const SortTh = ({ k, children, className = '' }: { k: SortKey; children: React.ReactNode; className?: string }) => (
    <th
      className={`${thBase} ${className} cursor-pointer hover:text-[hsl(var(--foreground))] transition-colors ${
        sortKey === k ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))]'
      }`}
      onClick={() => toggleSort(k)}
    >
      <span className="inline-flex items-center gap-1 justify-end">
        {children}
        {sortKey === k && (
          <span className="text-[10px] opacity-70">{sortDir === 'desc' ? '▼' : '▲'}</span>
        )}
      </span>
    </th>
  )

  return (
    <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden">
      <div className="flex items-center justify-between px-6 py-4 border-b border-[hsl(var(--border))]">
        <h2 className="text-lg font-semibold text-[hsl(var(--foreground))]">
          Товары
          {selectedCell && (
            <span className="ml-2 text-sm font-normal text-[hsl(var(--muted-foreground))]">
              — группа {selectedCell}
              <button
                onClick={onClearFilter}
                className="ml-2 text-[hsl(var(--primary))] hover:underline"
              >
                Сбросить
              </button>
            </span>
          )}
        </h2>
        <span className="text-sm text-[hsl(var(--muted-foreground))]">{products.length} товаров</span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[hsl(var(--border))]">
              <th className="px-4 py-2.5 text-left text-[12px] font-medium text-[hsl(var(--muted-foreground))] w-[280px]">Товар</th>
              <SortTh k="revenue">Выручка</SortTh>
              <SortTh k="profit">Прибыль</SortTh>
              <SortTh k="orders">Заказы</SortTh>
              <SortTh k="avg_price">Ср. цена</SortTh>
              <SortTh k="cost_price">С/с</SortTh>
              <th className={`${thBase} text-center text-[hsl(var(--muted-foreground))]`}>ABC</th>
              <SortTh k="abc_share">Доля</SortTh>
              <SortTh k="abc_cumulative">Кумул.</SortTh>
              <th className={`${thBase} text-center text-[hsl(var(--muted-foreground))]`}>XYZ</th>
              <SortTh k="xyz_cv">CV%</SortTh>
            </tr>
          </thead>
          <tbody>
            {sorted.map((p) => {
              const abc = abcColors[p.abc_group]
              const xyz = xyzColors[p.xyz_group]
              return (
                <tr
                  key={p.sku}
                  className="border-b border-[hsl(var(--border)/0.3)] hover:bg-[hsl(var(--muted)/0.3)] transition-colors"
                >
                  {/* Product */}
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-3">
                      {p.image_url ? (
                        <img src={p.image_url} alt="" className="w-9 h-9 rounded-lg object-cover shrink-0" />
                      ) : (
                        <div className="w-9 h-9 rounded-lg bg-[hsl(var(--muted))] shrink-0" />
                      )}
                      <div className="min-w-0">
                        <div className="text-sm font-medium text-[hsl(var(--foreground))] truncate max-w-[200px]">{p.name || p.offer_id}</div>
                        <div className="text-[11px] text-[hsl(var(--muted-foreground))]">{p.offer_id}</div>
                      </div>
                    </div>
                  </td>
                  {/* Revenue */}
                  <td className="px-3 py-3 text-right tabular-nums text-[13px] font-medium">{formatMoney(p.revenue)}</td>
                  {/* Profit */}
                  <td className={`px-3 py-3 text-right tabular-nums text-[13px] font-medium ${p.profit > 0 ? 'text-emerald-400' : p.profit < 0 ? 'text-red-400' : ''}`}>
                    {formatMoney(p.profit)}
                  </td>
                  {/* Orders */}
                  <td className="px-3 py-3 text-right tabular-nums text-[13px]">{formatNumber(p.orders)}</td>
                  {/* Avg Price */}
                  <td className="px-3 py-3 text-right tabular-nums text-[13px]">{formatMoney(p.avg_price)}</td>
                  {/* Cost */}
                  <td className="px-3 py-3 text-right tabular-nums text-[13px]">
                    {p.cost_price > 0 ? formatMoney(p.cost_price) : <span className="text-[hsl(var(--muted-foreground))]">—</span>}
                  </td>
                  {/* ABC */}
                  <td className="px-3 py-3 text-center">
                    <span className={`inline-flex items-center justify-center w-7 h-7 rounded-lg ${abc.bg} ${abc.text} font-bold text-sm`}>
                      {p.abc_group}
                    </span>
                  </td>
                  {/* Share */}
                  <td className="px-3 py-3 text-right tabular-nums text-[13px]">{p.abc_share}%</td>
                  {/* Cumulative */}
                  <td className="px-3 py-3 text-right tabular-nums text-[13px] text-[hsl(var(--muted-foreground))]">{p.abc_cumulative}%</td>
                  {/* XYZ */}
                  <td className="px-3 py-3 text-center">
                    <span className={`inline-flex items-center justify-center w-7 h-7 rounded-lg ${xyz.bg} ${xyz.text} font-bold text-sm`}>
                      {p.xyz_group}
                    </span>
                  </td>
                  {/* CV */}
                  <td className="px-3 py-3 text-right tabular-nums text-[13px]">{p.xyz_cv}%</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
