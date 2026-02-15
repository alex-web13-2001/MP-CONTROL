import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────────

export interface UserResponse {
  id: string
  email: string
  name: string
  is_active: boolean
  shops: ShopResponse[]
}

export interface ShopResponse {
  id: number
  name: string
  marketplace: string
  is_active: boolean
}

export interface TokenResponse {
  access_token: string
  refresh_token: string
  token_type: string
  user: UserResponse
}

export interface RegisterPayload {
  email: string
  password: string
  name: string
}

export interface LoginPayload {
  email: string
  password: string
}

export interface ShopCreatePayload {
  name: string
  marketplace: 'wildberries' | 'ozon'
  api_key: string
  client_id?: string
}

// ── Auth API ─────────────────────────────────────────────────────

export async function registerApi(data: RegisterPayload): Promise<TokenResponse> {
  const res = await apiClient.post<TokenResponse>('/auth/register', data)
  return res.data
}

export async function loginApi(data: LoginPayload): Promise<TokenResponse> {
  const res = await apiClient.post<TokenResponse>('/auth/login', data)
  return res.data
}

export async function refreshTokenApi(refreshToken: string): Promise<TokenResponse> {
  const res = await apiClient.post<TokenResponse>('/auth/refresh', {
    refresh_token: refreshToken,
  })
  return res.data
}

export async function getMeApi(): Promise<UserResponse> {
  const res = await apiClient.get<UserResponse>('/auth/me')
  return res.data
}

// ── Shops API ────────────────────────────────────────────────────

export async function getShopsApi(): Promise<ShopResponse[]> {
  const res = await apiClient.get<ShopResponse[]>('/shops')
  return res.data
}

export async function createShopApi(data: ShopCreatePayload): Promise<ShopResponse> {
  const res = await apiClient.post<ShopResponse>('/shops', data)
  return res.data
}

export async function deleteShopApi(shopId: number): Promise<void> {
  await apiClient.delete(`/shops/${shopId}`)
}
