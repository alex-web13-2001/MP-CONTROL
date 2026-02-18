import { Moon, Sun, Bell, User } from 'lucide-react'
import { useAppStore } from '@/stores/appStore'
import { useAuthStore } from '@/stores/authStore'
import { ShopSelector } from '@/components/layout/ShopSelector'
import { Button } from '@/components/ui/button'

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
      <ShopSelector
        shops={shops}
        currentShop={currentShop}
        onSelect={(shop) => setCurrentShop({ ...shop, marketplace: shop.marketplace as 'wildberries' | 'ozon' })}
      />

      {/* ── Right: Controls ── */}
      <div className="flex items-center gap-1">
        {/* Notifications */}
        <Button variant="ghost" size="icon" title="Уведомления">
          <Bell className="h-[18px] w-[18px]" />
        </Button>

        {/* Theme Toggle */}
        <Button variant="ghost" size="icon" onClick={toggleTheme} title={theme === 'dark' ? 'Светлая тема' : 'Тёмная тема'}>
          {theme === 'dark' ? (
            <Sun className="h-[18px] w-[18px]" />
          ) : (
            <Moon className="h-[18px] w-[18px]" />
          )}
        </Button>

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
          <Button variant="danger-ghost" size="xs" onClick={logout}>
            Выйти
          </Button>
        </div>
      </div>
    </header>
  )
}
