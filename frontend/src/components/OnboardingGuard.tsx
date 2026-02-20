import { useEffect, useMemo } from 'react'
import { Navigate } from 'react-router-dom'
import { useAuthStore } from '@/stores/authStore'
import { useAppStore } from '@/stores/appStore'

/**
 * OnboardingGuard — redirects to /onboarding if user has no shops
 * or all shops are still syncing.
 *
 * Auto-selects the first active shop if none is currently selected.
 * 
 * CRITICAL: blocks rendering children until currentShop in zustand store
 * is validated against actual shops list. This prevents race condition
 * where stale shop_id from localStorage causes 404 on dashboard API.
 */
export default function OnboardingGuard({
  children,
}: {
  children: React.ReactNode
}) {
  const { shops, isAuthenticated } = useAuthStore()
  const currentShop = useAppStore((s) => s.currentShop)
  const setCurrentShop = useAppStore((s) => s.setCurrentShop)

  // Check if currentShop from store is valid (exists in shops list)
  const isShopValid = useMemo(() => {
    if (shops.length === 0) return true // no shops loaded yet, don't block
    if (!currentShop) return false
    return shops.some((s) => s.id === currentShop.id)
  }, [currentShop, shops])

  // Fix stale currentShop — pick first active shop
  useEffect(() => {
    if (shops.length === 0) return
    if (isShopValid) return

    const best = shops.find((s) => s.status === 'active') || shops[0]
    setCurrentShop({
      id: best.id,
      name: best.name,
      marketplace: best.marketplace,
      isActive: best.isActive,
    })
  }, [isShopValid, shops, setCurrentShop])

  // Not authenticated — AuthGuard handles redirect to /login
  if (!isAuthenticated) {
    return <>{children}</>
  }

  // No shops — redirect to onboarding
  if (shops.length === 0) {
    return <Navigate to="/onboarding" replace />
  }

  // All shops still syncing — show progress screen
  const allSyncing = shops.every((s) => s.status === 'syncing')
  if (allSyncing) {
    return <Navigate to="/onboarding" replace />
  }

  // BLOCK children until currentShop is valid!
  // This prevents DashboardPage from requesting with stale shop_id
  if (!isShopValid) {
    return null // will re-render after useEffect updates the store
  }

  return <>{children}</>
}
