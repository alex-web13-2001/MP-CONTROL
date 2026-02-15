import { Navigate } from 'react-router-dom'
import { useAuthStore } from '@/stores/authStore'

/**
 * OnboardingGuard — redirects to /onboarding if user has no shops
 * or all shops are still syncing.
 *
 * Wraps protected routes so that new users must add at least one shop
 * and wait for initial data load before accessing the dashboard.
 */
export default function OnboardingGuard({
  children,
}: {
  children: React.ReactNode
}) {
  const { shops, isAuthenticated } = useAuthStore()

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
