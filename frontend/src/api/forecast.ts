import { apiClient } from './client'

/* ── Types ── */

export interface ForecastHistoryPoint {
  date: string
  revenue: number
  orders: number
}

export interface ForecastPoint {
  date: string
  revenue: number
  revenue_low: number
  revenue_high: number
  orders: number
  orders_low: number
  orders_high: number
}

export interface ForecastTrend {
  revenue_slope_pct: number
  orders_slope_pct: number
  direction: 'up' | 'down' | 'flat'
  forecast_revenue: number
  forecast_orders: number
}

export interface ForecastProduct {
  sku: number
  offer_id: string
  name: string
  image_url: string
  orders: number
  revenue: number
  avg_price: number
  ad_spend: number
  ad_views: number
  ad_clicks: number
  commission: number
  logistics: number
  cogs: number
  profit: number
  margin_pct: number
  ctr: number
  cpc: number
  cpo: number
  cr: number
  roi: number
}

export interface ForecastResponse {
  shop_id: number
  period: number
  forecast_days: number
  history: ForecastHistoryPoint[]
  forecast: ForecastPoint[]
  trend: ForecastTrend
  products: ForecastProduct[]
}

/* ── API ── */

export async function fetchOzonForecast(
  shopId: number,
  period: number = 90,
  forecastDays: number = 14,
): Promise<ForecastResponse> {
  const { data } = await apiClient.get<ForecastResponse>('/sales/ozon/forecast', {
    params: { shop_id: shopId, period, forecast_days: forecastDays },
  })
  return data
}

/* ── SKU Forecast (LightGBM) ── */

export interface SkuForecastPoint {
  date: string
  orders: number
  revenue: number
  orders_low: number
  orders_high: number
  profit?: number
  commission?: number
  logistics?: number
  ad_spend_est?: number
  cogs?: number
}

export interface SkuHistoryPoint {
  ds: string
  orders: number
  revenue: number
  views: number
  clicks: number
  carts: number
  ad_spend: number
}

export interface SkuForecast {
  sku: number
  offer_id: string
  name: string
  image_url: string
  history: SkuHistoryPoint[]
  forecast: SkuForecastPoint[]
  trend: { slope_pct: number; direction: 'up' | 'down' | 'flat' }
  feature_importance: Record<string, number>
  totals: { orders: number; revenue: number; profit: number }
}

export interface SkuForecastResponse {
  shop_id: number
  period: number
  forecast_days: number
  sku_forecasts: SkuForecast[]
}

export async function fetchOzonSkuForecast(
  shopId: number,
  period: number = 90,
  forecastDays: number = 14,
  sku?: number,
): Promise<SkuForecastResponse> {
  const params: Record<string, unknown> = { shop_id: shopId, period, forecast_days: forecastDays }
  if (sku) params.sku = sku
  const { data } = await apiClient.get<SkuForecastResponse>('/sales/ozon/forecast/sku', { params, timeout: 120000 })
  return data
}
