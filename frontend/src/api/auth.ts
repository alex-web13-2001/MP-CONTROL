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
  status?: string
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
  perf_client_id?: string
  perf_client_secret?: string
}

export interface ValidateKeyPayload {
  marketplace: 'wildberries' | 'ozon'
  api_key: string
  client_id?: string
  perf_client_id?: string
  perf_client_secret?: string
}

export interface ValidateKeyResponse {
  valid: boolean
  seller_valid?: boolean | null
  perf_valid?: boolean | null
  message: string
  shop_name?: string | null
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

export async function validateKeyApi(data: ValidateKeyPayload): Promise<ValidateKeyResponse> {
  const res = await apiClient.post<ValidateKeyResponse>('/shops/validate-key', data)
  return res.data
}

// ── Sync Status API ──────────────────────────────────────────────

export interface SyncStatusResponse {
  status: 'loading' | 'done' | 'error' | string
  current_step: number
  total_steps: number
  step_name: string
  percent: number
  error: string | null
}

export async function getSyncStatusApi(shopId: number): Promise<SyncStatusResponse> {
  const res = await apiClient.get<SyncStatusResponse>(`/shops/${shopId}/sync-status`)
  return res.data
}
