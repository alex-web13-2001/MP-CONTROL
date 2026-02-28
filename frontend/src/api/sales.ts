/**
 * Sales API client.
 */
import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────

export interface SalesKpi {
  orders_count: number
  orders_delta: number
  revenue: number
  revenue_delta: number
  avg_check: number
  returns_count: number
  returns_delta: number
  returns_pct: number
}

export interface SalesDailyPoint {
  date: string
  orders: number
  revenue: number
  returns: number
}

export interface SalesGeoItem {
  region: string
  orders: number
  revenue: number
  pct: number
  avg_check: number
}

export interface SalesTopProduct {
  sku: number
  offer_id: string
  name: string
  image_url: string
  orders: number
  revenue: number
  returns: number
  return_pct: number
  orders_delta: number
  revenue_delta: number
  // Ad funnel
  ad_views: number
  ad_clicks: number
  ad_add_to_cart: number
  ad_ctr: number
  ad_cart_rate: number
  ad_order_rate: number
  // Ad funnel deltas
  ad_views_delta: number
  ad_clicks_delta: number
  ad_add_to_cart_delta: number
  ad_ctr_delta: number
  ad_cart_rate_delta: number
  ad_order_rate_delta: number
}

export interface SalesReturnReason {
  reason: string
  count: number
  pct: number
}

export interface SalesReturns {
  total: number
  by_reason: SalesReturnReason[]
}

export interface SalesResponse {
  shop_id: number
  date_from: string
  date_to: string
  kpi: SalesKpi
  daily: SalesDailyPoint[]
  geo: SalesGeoItem[]
  top_products: SalesTopProduct[]
  returns: SalesReturns
}

// ── API ──────────────────────────────────────────────────────

export async function getOzonSalesApi(params: {
  shop_id: number
  period?: number
  date_from?: string
  date_to?: string
}): Promise<SalesResponse> {
  const { data } = await apiClient.get<SalesResponse>('/sales/ozon', { params })
  return data
}

// ── Per-product daily dynamics ───────────────────────────────

export interface ProductDailyPoint {
  date: string
  orders: number
  revenue: number
}

export interface ProductDailyResponse {
  shop_id: number
  date_from: string
  date_to: string
  products: Record<string, ProductDailyPoint[]>
}

export async function getOzonProductDailyApi(params: {
  shop_id: number
  skus: string
  period?: number
  date_from?: string
  date_to?: string
}): Promise<ProductDailyResponse> {
  const { data } = await apiClient.get<ProductDailyResponse>('/sales/ozon/product-daily', { params })
  return data
}
