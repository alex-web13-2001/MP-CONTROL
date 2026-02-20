import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────────

export interface DashboardKpi {
  orders_count: number
  orders_delta: number
  revenue: number
  revenue_delta: number
  avg_check: number
  ad_spend: number
  ad_spend_delta: number
  views: number
  views_delta: number
  clicks: number
  clicks_delta: number
  drr: number
  drr_delta: number
}

export interface SalesDailyPoint {
  date: string
  orders: number
  revenue: number
}

export interface AdsDailyPoint {
  date: string
  spend: number
  views: number
  clicks: number
  cart: number
  orders: number
  drr_ad: number
  drr_total: number
}

export interface TopProduct {
  offer_id: string
  name: string
  image_url: string
  orders: number
  revenue: number
  delta_pct: number
  stock_fbo: number
  stock_fbs: number
  price: number
  ad_spend: number
  drr: number
}

export interface DashboardResponse {
  shop_id: number
  period: string
  kpi: DashboardKpi
  charts: {
    sales_daily: SalesDailyPoint[]
    ads_daily: AdsDailyPoint[]
  }
  top_products: TopProduct[]
}

// ── API ──────────────────────────────────────────────────────────

export async function getOzonDashboardApi(
  shopId: number,
  period: string = '7d',
): Promise<DashboardResponse> {
  const res = await apiClient.get<DashboardResponse>('/dashboard/ozon', {
    params: { shop_id: shopId, period },
  })
  return res.data
}
