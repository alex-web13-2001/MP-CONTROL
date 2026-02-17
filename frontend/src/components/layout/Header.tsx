import { Moon, Sun, Bell, User, Store } from 'lucide-react'
import { useAppStore } from '@/stores/appStore'
import { useAuthStore } from '@/stores/authStore'
import { Badge } from '@/components/ui/badge'

export function Header() {
  const theme = useAppStore((s) => s.theme)
  const toggleTheme = useAppStore((s) => s.toggleTheme)
  const currentShop = useAppStore((s) => s.currentShop)
  const setCurrentShop = useAppStore((s) => s.setCurrentShop)

  const shops = useAuthStore((s) => s.shops)
  const user = useAuthStore((s) => s.user)
  const logout = useAuthStore((s) => s.logout)

  return (
    <header className="sticky top-0 z-30 flex h-16 shrink-0 items-center justify-between border-b border-[hsl(var(--border))] bg-[hsl(var(--background)/0.85)] px-8 backdrop-blur-xl">
      {/* ── Left: Shop Selector ── */}
      <div className="flex items-center gap-3">
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-[hsl(var(--muted))]">
          <Store className="h-4 w-4 text-[hsl(var(--muted-foreground))]" />
        </div>

        {shops.length > 0 ? (
          <select
            className="rounded-lg border border-[hsl(var(--input))] bg-[hsl(var(--card))] px-3 py-1.5 text-sm text-[hsl(var(--foreground))] focus:border-[hsl(var(--primary))] focus:outline-none focus:ring-2 focus:ring-[hsl(var(--ring)/0.3)] transition-colors"
            value={currentShop?.id || ''}
            onChange={(e) => {
              const shop = shops.find((s) => s.id === Number(e.target.value))
              if (shop) setCurrentShop(shop)
            }}
          >
            {shops.map((shop) => (
              <option key={shop.id} value={shop.id}>
                {shop.name}
              </option>
            ))}
          </select>
        ) : (
          <span className="text-sm text-[hsl(var(--muted-foreground))]">
            Магазин не выбран
          </span>
        )}

        {currentShop && (
          <Badge variant={currentShop.marketplace === 'wildberries' ? 'wb' : 'ozon'}>
            {currentShop.marketplace === 'wildberries' ? 'WB' : 'Ozon'}
          </Badge>
        )}
      </div>

      {/* ── Right: Controls ── */}
      <div className="flex items-center gap-1">
        {/* Notifications */}
        <button
          className="relative flex h-9 w-9 items-center justify-center rounded-lg text-[hsl(var(--muted-foreground))] transition-all duration-200 hover:bg-[hsl(var(--secondary))] hover:text-[hsl(var(--foreground))]"
          title="Уведомления"
        >
          <Bell className="h-[18px] w-[18px]" />
        </button>

        {/* Theme Toggle */}
        <button
          onClick={toggleTheme}
          className="flex h-9 w-9 items-center justify-center rounded-lg text-[hsl(var(--muted-foreground))] transition-all duration-200 hover:bg-[hsl(var(--secondary))] hover:text-[hsl(var(--foreground))]"
          title={theme === 'dark' ? 'Светлая тема' : 'Тёмная тема'}
        >
          {theme === 'dark' ? (
            <Sun className="h-[18px] w-[18px]" />
          ) : (
            <Moon className="h-[18px] w-[18px]" />
          )}
        </button>

        {/* Separator */}
        <div className="mx-3 h-6 w-px bg-[hsl(var(--border))]" />

        {/* User Menu */}
        <div className="flex items-center gap-3">
          <div className="flex h-8 w-8 items-center justify-center rounded-full bg-gradient-to-br from-[hsl(var(--primary))] to-[hsl(245_80%_70%)] text-white">
            <User className="h-4 w-4" />
          </div>
          <div className="hidden md:block">
            <p className="text-sm font-medium leading-tight text-[hsl(var(--foreground))]">
              {user?.name || 'Пользователь'}
            </p>
            <p className="text-xs leading-tight text-[hsl(var(--muted-foreground))]">
              {user?.email || ''}
            </p>
          </div>
          <button
            onClick={logout}
            className="ml-1 rounded-md px-2 py-1 text-xs text-[hsl(var(--muted-foreground))] transition-colors hover:bg-[hsl(var(--destructive)/0.1)] hover:text-[hsl(var(--destructive))]"
          >
            Выйти
          </button>
        </div>
      </div>
    </header>
  )
}
