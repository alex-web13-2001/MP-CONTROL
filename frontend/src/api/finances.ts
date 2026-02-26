/**
 * Finances API client.
 */
import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────

export interface FinancesKpi {
  revenue: number
  revenue_delta: number
  payout: number
  payout_delta: number
  mp_fees: number
  mp_fees_delta: number
  ad_spend: number
  ad_spend_delta: number
  cogs: number
  cogs_delta: number
  profit: number
  profit_delta: number
  profit_pct: number
  orders: number
  orders_delta: number
}

export interface FinancesBreakdown {
  revenue: number
  commission: number
  logistics: number
  storage: number
  acquiring: number
  advertising: number
  refunds: number
  penalties: number
  compensation: number
  cogs: number
  profit: number
}

export interface FinancesDailyPoint {
  date: string
  revenue: number
  payout: number
  mp_fees: number
  ad_spend: number
  cogs: number
  orders: number
  profit: number
}

export interface FinancesComparison {
  current: Record<string, number>
  previous: Record<string, number>
  delta_pct: Record<string, number>
}

export interface FinancesResponse {
  shop_id: number
  period: number
  date_from: string
  date_to: string
  group_by: string
  kpi: FinancesKpi
  breakdown: FinancesBreakdown
  daily: FinancesDailyPoint[]
  comparison: FinancesComparison
}

// ── API ──────────────────────────────────────────────────────

export async function getOzonFinancesApi(params: {
  shop_id: number
  period?: number
  group_by?: string
  date_from?: string
  date_to?: string
}): Promise<FinancesResponse> {
  const { data } = await apiClient.get<FinancesResponse>('/finances/ozon', { params })
  return data
}

export async function getWbFinancesApi(params: {
  shop_id: number
  period?: number
  group_by?: string
  date_from?: string
  date_to?: string
}): Promise<FinancesResponse> {
  const { data } = await apiClient.get<FinancesResponse>('/finances/wb', { params })
  return data
}

// ── Product-level P&L ────────────────────────────────────────

export interface ProductFinanceItem {
  vendor_code: string
  nm_id?: number
  current: Record<string, number>
  previous: Record<string, number>
  delta_pct: Record<string, number>
  pct_of_revenue: Record<string, number>
}

export interface ProductFinanceResponse {
  shop_id: number
  date_from: string
  date_to: string
  products: ProductFinanceItem[]
  totals: {
    current: Record<string, number>
    previous: Record<string, number>
    delta_pct: Record<string, number>
  }
}

export async function getWbProductsFinanceApi(params: {
  shop_id: number
  period?: number
  date_from?: string
  date_to?: string
}): Promise<ProductFinanceResponse> {
  const { data } = await apiClient.get<ProductFinanceResponse>('/finances/wb/products', { params })
  return data
}

export async function getOzonProductsFinanceApi(params: {
  shop_id: number
  period?: number
  date_from?: string
  date_to?: string
}): Promise<ProductFinanceResponse> {
  const { data } = await apiClient.get<ProductFinanceResponse>('/finances/ozon/products', { params })
  return data
}
