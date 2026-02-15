import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type { UserResponse, ShopResponse } from '@/api/auth'

export interface User {
  id: string
  email: string
  name: string
}

export interface Shop {
  id: number
  name: string
  marketplace: 'wildberries' | 'ozon'
  isActive: boolean
  status?: string
}

interface AuthState {
  user: User | null
  token: string | null
  refreshToken: string | null
  isAuthenticated: boolean
  shops: Shop[]

  loginFromApi: (data: { access_token: string; refresh_token: string; user: UserResponse }) => void
  logout: () => void
  setUser: (user: User) => void
  setShops: (shops: ShopResponse[]) => void
  updateTokens: (accessToken: string, refreshToken: string) => void
}

function mapShops(apiShops: ShopResponse[]): Shop[] {
  return apiShops.map((s) => ({
    id: s.id,
    name: s.name,
    marketplace: s.marketplace as 'wildberries' | 'ozon',
    isActive: s.is_active,
    status: s.status,
  }))
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      user: null,
      token: null,
      refreshToken: null,
      isAuthenticated: false,
      shops: [],

      loginFromApi: (data) =>
        set({
          user: {
            id: data.user.id,
            email: data.user.email,
            name: data.user.name,
          },
          token: data.access_token,
          refreshToken: data.refresh_token,
          isAuthenticated: true,
          shops: mapShops(data.user.shops),
        }),

      logout: () =>
        set({
          user: null,
          token: null,
          refreshToken: null,
          isAuthenticated: false,
          shops: [],
        }),

      setUser: (user) => set({ user }),

      setShops: (apiShops) => set({ shops: mapShops(apiShops) }),

      updateTokens: (accessToken, refreshToken) =>
        set({ token: accessToken, refreshToken }),
    }),
    {
      name: 'mp-control-auth',
      partialize: (state) => ({
        user: state.user,
        token: state.token,
        refreshToken: state.refreshToken,
        isAuthenticated: state.isAuthenticated,
        shops: state.shops,
      }),
    }
  )
)
