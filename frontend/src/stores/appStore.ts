import { create } from 'zustand'

interface AppState {
  // Loading state
  isLoading: boolean
  setLoading: (loading: boolean) => void

  // Selected shop
  selectedShopId: number | null
  setSelectedShop: (shopId: number | null) => void

  // Date range filter
  dateRange: {
    start: Date | null
    end: Date | null
  }
  setDateRange: (start: Date | null, end: Date | null) => void

  // Sidebar state
  isSidebarOpen: boolean
  toggleSidebar: () => void
}

export const useAppStore = create<AppState>((set) => ({
  // Loading state
  isLoading: false,
  setLoading: (loading) => set({ isLoading: loading }),

  // Selected shop
  selectedShopId: null,
  setSelectedShop: (shopId) => set({ selectedShopId: shopId }),

  // Date range filter
  dateRange: {
    start: null,
    end: null,
  },
  setDateRange: (start, end) => set({ dateRange: { start, end } }),

  // Sidebar state
  isSidebarOpen: true,
  toggleSidebar: () => set((state) => ({ isSidebarOpen: !state.isSidebarOpen })),
}))
