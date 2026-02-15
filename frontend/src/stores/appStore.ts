import { create } from 'zustand'
import { persist } from 'zustand/middleware'

type Theme = 'dark' | 'light'

export interface AppShop {
  id: number
  name: string
  marketplace: 'wildberries' | 'ozon'
  isActive: boolean
}

interface AppState {
  /* ── Theme ── */
  theme: Theme
  setTheme: (theme: Theme) => void
  toggleTheme: () => void

  /* ── Sidebar ── */
  sidebarCollapsed: boolean
  toggleSidebar: () => void
  setSidebarCollapsed: (collapsed: boolean) => void

  /* ── Current Shop ── */
  currentShop: AppShop | null
  setCurrentShop: (shop: AppShop) => void
}

export const useAppStore = create<AppState>()(
  persist(
    (set, get) => ({
      /* ── Theme ── */
      theme: 'dark',
      setTheme: (theme) => {
        document.documentElement.classList.toggle('light', theme === 'light')
        set({ theme })
      },
      toggleTheme: () => {
        const next = get().theme === 'dark' ? 'light' : 'dark'
        document.documentElement.classList.toggle('light', next === 'light')
        set({ theme: next })
      },

      /* ── Sidebar ── */
      sidebarCollapsed: false,
      toggleSidebar: () => set((s) => ({ sidebarCollapsed: !s.sidebarCollapsed })),
      setSidebarCollapsed: (collapsed) => set({ sidebarCollapsed: collapsed }),

      /* ── Current Shop ── */
      currentShop: null,
      setCurrentShop: (shop) => set({ currentShop: shop }),
    }),
    {
      name: 'mp-control-app',
      partialize: (state) => ({
        theme: state.theme,
        sidebarCollapsed: state.sidebarCollapsed,
        currentShop: state.currentShop,
      }),
      onRehydrateStorage: () => (state) => {
        if (state?.theme === 'light') {
          document.documentElement.classList.add('light')
        }
      },
    }
  )
)
