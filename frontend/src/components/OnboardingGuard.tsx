import { useEffect } from 'react'
import { Navigate } from 'react-router-dom'
import { useAuthStore } from '@/stores/authStore'
import { useAppStore } from '@/stores/appStore'

/**
 * OnboardingGuard — redirects to /onboarding if user has no shops
 * or all shops are still syncing.
 *
 * Also auto-selects the first active shop if none is currently selected.
 */
export default function OnboardingGuard({
  children,
}: {
  children: React.ReactNode
}) {
  const { shops, isAuthenticated } = useAuthStore()
  const currentShop = useAppStore((s) => s.currentShop)
  const setCurrentShop = useAppStore((s) => s.setCurrentShop)

  // Auto-select first active shop if none selected,
  // or if persisted shop no longer exists in API response (stale localStorage)
  useEffect(() => {
    if (shops.length === 0) return

    const shopStillExists = currentShop && shops.some((s) => s.id === currentShop.id)
    if (!shopStillExists) {
      const activeShop = shops.find((s) => s.status === 'active') || shops[0]
      setCurrentShop({
        id: activeShop.id,
        name: activeShop.name,
        marketplace: activeShop.marketplace,
        isActive: activeShop.isActive,
      })
    }
  }, [currentShop, shops, setCurrentShop])

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

  return <>{children}</>
}
