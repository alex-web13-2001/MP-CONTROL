import { motion } from 'framer-motion'
import {
  ShoppingCart,
  DollarSign,
  Eye,
  TrendingUp,
  Package,
  Megaphone,
  ArrowUpRight,
  ArrowDownRight,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/stores/appStore'

/* ── Animated KPI Card ── */
function KpiCard({
  title,
  value,
  delta,
  icon: Icon,
  delay,
  accent,
}: {
  title: string
  value: string
  delta: string
  icon: React.ElementType
  delay: number
  accent: string
}) {
  const isPositive = delta.startsWith('+')
  const isNeutral = delta === '0' || delta === '+0%'

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay, ease: 'easeOut' }}
      className="min-w-0"
    >
      <Card className="group relative overflow-hidden hover:shadow-xl hover:shadow-[hsl(var(--primary)/0.06)] transition-all duration-300 hover:-translate-y-0.5">
        {/* Accent gradient top border */}
        <div
          className="absolute inset-x-0 top-0 h-[2px] opacity-60"
          style={{ background: `linear-gradient(90deg, transparent, ${accent}, transparent)` }}
        />

        <CardContent className="p-6">
          <div className="flex items-start justify-between">
            <div className="space-y-3">
              <p className="text-[13px] font-medium text-[hsl(var(--muted-foreground))]">
                {title}
              </p>
              <p className="text-3xl font-bold tracking-tight text-[hsl(var(--foreground))]">
                {value}
              </p>
              {!isNeutral && (
                <div className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-semibold ${
                  isPositive
                    ? 'bg-emerald-500/10 text-emerald-400'
                    : 'bg-red-500/10 text-red-400'
                }`}>
                  {isPositive ? (
                    <ArrowUpRight className="h-3 w-3" />
                  ) : (
                    <ArrowDownRight className="h-3 w-3" />
                  )}
                  {delta}
                </div>
              )}
              {isNeutral && (
                <p className="text-xs text-[hsl(var(--muted-foreground)/0.5)]">
                  Нет данных
                </p>
              )}
            </div>

            <div
              className="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl transition-transform duration-300 group-hover:scale-110"
              style={{ background: `${accent}15` }}
            >
              <Icon className="h-5 w-5" style={{ color: accent }} />
            </div>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

export default function DashboardPage() {
  const currentShop = useAppStore((s) => s.currentShop)

  const kpis = [
    { title: 'Заказы сегодня', value: '—', delta: '+0%', icon: ShoppingCart, accent: '#6366f1' },
    { title: 'Выручка', value: '—', delta: '+0%', icon: DollarSign, accent: '#10b981' },
    { title: 'Просмотры', value: '—', delta: '+0%', icon: Eye, accent: '#3b82f6' },
    { title: 'Конверсия', value: '—', delta: '+0%', icon: TrendingUp, accent: '#f59e0b' },
    { title: 'Остатки FBO', value: '—', delta: '0', icon: Package, accent: '#8b5cf6' },
    { title: 'Расход рекламы', value: '—', delta: '+0%', icon: Megaphone, accent: '#ef4444' },
  ]

  return (
    <div className="space-y-8">
      {/* ── Page Header ── */}
      <motion.div
        initial={{ opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
        className="space-y-1"
      >
        <h1 className="text-3xl font-bold tracking-tight text-[hsl(var(--foreground))]">
          Обзор
        </h1>
        <p className="text-[15px] text-[hsl(var(--muted-foreground))]">
          {currentShop
            ? `${currentShop.name} • ${currentShop.marketplace === 'wildberries' ? 'Wildberries' : 'Ozon'}`
            : 'Выберите магазин для просмотра аналитики'}
        </p>
      </motion.div>

      {/* ── KPI Grid: 3 columns on large screens ── */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3 xl:gap-5">
        {kpis.map((kpi, i) => (
          <KpiCard key={kpi.title} {...kpi} delay={i * 0.06} />
        ))}
      </div>

      {/* ── Charts Row ── */}
      <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, delay: 0.3 }}
        >
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Динамика продаж</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex h-72 items-center justify-center rounded-xl border border-dashed border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.2)]">
                <div className="text-center space-y-2">
                  <TrendingUp className="h-8 w-8 mx-auto text-[hsl(var(--muted-foreground)/0.3)]" />
                  <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">
                    Подключите магазин для<br />отображения графика
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, delay: 0.35 }}
        >
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Воронка продаж</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex h-72 items-center justify-center rounded-xl border border-dashed border-[hsl(var(--border)/0.5)] bg-[hsl(var(--muted)/0.2)]">
                <div className="text-center space-y-2">
                  <Eye className="h-8 w-8 mx-auto text-[hsl(var(--muted-foreground)/0.3)]" />
                  <p className="text-sm text-[hsl(var(--muted-foreground)/0.5)]">
                    Подключите магазин для<br />отображения воронки
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      </div>

      {/* ── Top Products Table ── */}
      <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, delay: 0.4 }}
      >
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Топ товары</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {[1, 2, 3, 4, 5].map((i) => (
              <div key={i} className="flex items-center gap-4 rounded-lg p-2 transition-colors hover:bg-[hsl(var(--muted)/0.3)]">
                <Skeleton className="h-12 w-12 rounded-xl shrink-0" />
                <div className="flex-1 space-y-2">
                  <Skeleton className="h-4 w-2/3" />
                  <Skeleton className="h-3 w-2/5" />
                </div>
                <Skeleton className="h-7 w-24 rounded-lg" />
              </div>
            ))}
          </CardContent>
        </Card>
      </motion.div>
    </div>
  )
}
