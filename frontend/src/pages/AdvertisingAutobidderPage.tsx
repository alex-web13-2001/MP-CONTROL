import { motion } from 'framer-motion'
import { Bot, Construction } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'

export default function AdvertisingAutobidderPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Автобиддер</h1>
        <p className="text-[hsl(var(--muted-foreground))]">
          Автоматическое управление ставками на основе ИИ
        </p>
      </div>

      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
      >
        <Card className="border-dashed">
          <CardContent className="flex flex-col items-center justify-center py-20 text-center">
            <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-[hsl(var(--primary)/0.1)]">
              <Construction className="h-8 w-8 text-[hsl(var(--primary))]" />
            </div>
            <h2 className="text-xl font-semibold mb-2">Раздел в разработке</h2>
            <p className="max-w-md text-[hsl(var(--muted-foreground))]">
              Здесь будет автоматическое управление ставками: ИИ-стратегии, 
              расписание ставок по часам, таргеты ROI/ДРР и автоматическая оптимизация.
            </p>
            <div className="mt-6 flex items-center gap-2 rounded-full bg-violet-500/10 px-4 py-2 text-sm font-medium text-violet-400">
              <Bot className="h-4 w-4" />
              Скоро
            </div>
          </CardContent>
        </Card>
      </motion.div>
    </div>
  )
}
