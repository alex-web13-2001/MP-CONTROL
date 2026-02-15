import { Outlet } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Sidebar } from './Sidebar'
import { Header } from './Header'
import { useAppStore } from '@/stores/appStore'

/**
 * Main application layout with Sidebar (fixed) + Header + Content.
 * Sidebar is position:fixed, so we offset the content panel with
 * paddingLeft (not marginLeft) and let it fill 100% width naturally.
 */
export function AppLayout() {
  const sidebarCollapsed = useAppStore((s) => s.sidebarCollapsed)
  const sidebarWidth = sidebarCollapsed ? 72 : 256

  return (
    <div className="min-h-screen bg-[hsl(var(--background))]">
      <Sidebar />

      {/* ── Content Panel ── */}
      <motion.div
        initial={false}
        animate={{ paddingLeft: sidebarWidth }}
        transition={{ duration: 0.2, ease: 'easeInOut' }}
        className="flex min-h-screen flex-col"
      >
        <Header />

        {/* ── Main Content ── */}
        <main className="flex-1 overflow-x-hidden px-4 py-6 md:px-6 lg:px-8">
          <div className="mx-auto max-w-[1600px]">
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.35, ease: 'easeOut' }}
            >
              <Outlet />
            </motion.div>
          </div>
        </main>
      </motion.div>
    </div>
  )
}
