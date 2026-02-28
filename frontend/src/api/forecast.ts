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
