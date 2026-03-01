import { apiClient } from './client'

/* ── Types ── */

export interface ForecastHistoryPoint {
  date: string
  revenue: number
  orders: number
}

export interface OverallForecastPoint {
  date: string
  revenue: number
  orders: number
  profit: number
  ad_spend: number
}

export interface OverallTrend {
  revenue_slope_pct: number
  direction: 'up' | 'down' | 'flat'
}

export interface OverallTotals {
  revenue: number
  orders: number
  profit: number
  ad_spend: number
  margin_pct: number
}

export interface SkuAction {
  text: string
  profit_impact: string
  priority: number
}

export interface SkuNowState {
  profit: number
  profit_daily: number
  margin_pct: number
  revenue: number
  orders: number
  ad_spend: number
  drr: number
  roi: number
  ctr: number
  avg_price: number
}

export interface SkuForecastState {
  profit: number
  profit_daily: number
  margin_pct: number
  revenue: number
  orders: number
  ad_spend: number
  drr: number
}

export interface SkuAnalysis {
  severity: 'critical' | 'warning' | 'opportunity' | 'ok'
  title: string
  now: SkuNowState
  forecast: SkuForecastState
  actions: SkuAction[]
}

export interface ProductHistoryTotals {
  revenue: number
  orders: number
  ad_spend: number
  profit: number
  margin_pct: number
  roi: number
  avg_price: number
  ctr: number
  cart_rate: number
}

export interface ProductForecastTotals {
  revenue: number
  orders: number
  ad_spend: number
  profit: number
  margin_pct: number
}

export interface ProductForecastPoint {
  date: string
  orders: number
  revenue: number
  orders_low: number
  orders_high: number
  ad_spend: number
  commission: number
  logistics: number
  cogs: number
  profit: number
  margin_pct: number
}

export interface ForecastProduct {
  sku: number
  offer_id: string
  name: string
  image_url: string
  history_totals: ProductHistoryTotals
  forecast: ProductForecastPoint[]
  trend: { slope_pct: number; direction: 'up' | 'down' | 'flat' }
  forecast_totals: ProductForecastTotals
  analysis: SkuAnalysis
  feature_importance: Record<string, number>
}

export interface ForecastResponse {
  shop_id: number
  period: number
  forecast_days: number
  history: ForecastHistoryPoint[]
  overall: {
    forecast: OverallForecastPoint[]
    trend: OverallTrend
    totals: OverallTotals
  }
  recommendation_summary: Record<string, number>
  products: ForecastProduct[]
}

/* ── API ── */

export async function fetchOzonForecast(
  shopId: number,
  period: number = 120,
  forecastDays: number = 30,
): Promise<ForecastResponse> {
  const { data } = await apiClient.get<ForecastResponse>('/sales/ozon/forecast', {
    params: { shop_id: shopId, period, forecast_days: forecastDays },
    timeout: 120000,
  })
  return data
}

/* ── SKU Forecast (LightGBM) — legacy endpoint ── */

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
  period: number = 120,
  forecastDays: number = 30,
  sku?: number,
): Promise<SkuForecastResponse> {
  const params: Record<string, unknown> = { shop_id: shopId, period, forecast_days: forecastDays }
  if (sku) params.sku = sku
  const { data } = await apiClient.get<SkuForecastResponse>('/sales/ozon/forecast/sku', { params, timeout: 120000 })
  return data
}
