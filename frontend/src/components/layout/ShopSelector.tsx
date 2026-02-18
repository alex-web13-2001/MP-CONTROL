import { useState, useRef, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { ChevronDown, Check, Store } from 'lucide-react'
import { Badge } from '@/components/ui/badge'

interface Shop {
  id: number
  name: string
  marketplace: string
  isActive: boolean
}

interface ShopSelectorProps {
  shops: Shop[]
  currentShop: Shop | null
  onSelect: (shop: Shop) => void
}

export function ShopSelector({ shops, currentShop, onSelect }: ShopSelectorProps) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  // Close on click outside
  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    if (open) {
      document.addEventListener('mousedown', handleClickOutside)
      return () => document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [open])

  // Close on Escape
  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false)
    }
    if (open) {
      document.addEventListener('keydown', handleKey)
      return () => document.removeEventListener('keydown', handleKey)
    }
  }, [open])

  const mpIcon = (marketplace: string) =>
    marketplace === 'wildberries' ? (
      <span className="flex h-6 w-6 items-center justify-center rounded-md bg-gradient-to-br from-purple-500 to-purple-700 text-[10px] font-bold text-white shadow-sm">
        WB
      </span>
    ) : (
      <span className="flex h-6 w-6 items-center justify-center rounded-md bg-gradient-to-br from-blue-500 to-blue-700 text-[10px] font-bold text-white shadow-sm">
        Oz
      </span>
    )

  return (
    <div ref={ref} className="relative">
      {/* Trigger */}
      <button
        onClick={() => setOpen(!open)}
        className={`
          flex items-center gap-2.5 rounded-xl border px-3 py-2 text-sm font-medium
          transition-all duration-200 cursor-pointer select-none
          ${open
            ? 'border-[hsl(var(--primary)/0.5)] bg-[hsl(var(--primary)/0.05)] shadow-md'
            : 'border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-sm hover:border-[hsl(var(--muted-foreground)/0.3)] hover:shadow-md'
          }
        `}
      >
        {currentShop ? (
          <>
            {mpIcon(currentShop.marketplace)}
            <span className="max-w-[120px] truncate text-[hsl(var(--foreground))]">
              {currentShop.name}
            </span>
          </>
        ) : (
          <>
            <Store className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
            <span className="text-[hsl(var(--muted-foreground))]">Выберите магазин</span>
          </>
        )}
        <ChevronDown
          className={`h-3.5 w-3.5 text-[hsl(var(--muted-foreground))] transition-transform duration-200 ${
            open ? 'rotate-180' : ''
          }`}
        />
      </button>

      {/* Dropdown */}
      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0, y: -4, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -4, scale: 0.98 }}
            transition={{ duration: 0.15, ease: 'easeOut' }}
            className="absolute left-0 top-full z-50 mt-1.5 w-64 origin-top-left overflow-hidden rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-xl backdrop-blur-xl"
          >
            <div className="p-1.5">
              <p className="px-2.5 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-[hsl(var(--muted-foreground))]">
                Магазины
              </p>
              {shops.map((shop) => {
                const isActive = currentShop?.id === shop.id
                return (
                  <button
                    key={shop.id}
                    onClick={() => {
                      onSelect(shop)
                      setOpen(false)
                    }}
                    className={`
                      flex w-full items-center gap-3 rounded-lg px-2.5 py-2 text-left text-sm
                      transition-all duration-150 cursor-pointer
                      ${isActive
                        ? 'bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--foreground))]'
                        : 'text-[hsl(var(--foreground))] hover:bg-[hsl(var(--secondary))]'
                      }
                    `}
                  >
                    {mpIcon(shop.marketplace)}
                    <div className="flex-1 min-w-0">
                      <p className="truncate font-medium">{shop.name}</p>
                    </div>
                    <Badge
                      variant={shop.marketplace === 'wildberries' ? 'wb' : 'ozon'}
                      className="shrink-0 text-[10px]"
                    >
                      {shop.marketplace === 'wildberries' ? 'WB' : 'Ozon'}
                    </Badge>
                    {isActive && (
                      <Check className="h-4 w-4 shrink-0 text-[hsl(var(--primary))]" />
                    )}
                  </button>
                )
              })}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
