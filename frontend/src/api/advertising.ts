import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────────

export interface AdvertisingKpi {
  spend: number
  spend_delta: number
  views: number
  views_delta: number
  clicks: number
  clicks_delta: number
  ctr: number
  ctr_delta: number
  cart: number
  cart_delta: number
  orders: number
  orders_delta: number
  conversion_rate: number
  conversion_rate_delta: number
  cpo: number
  cpo_delta: number
  drr: number
  drr_delta: number
  total_drr: number
  total_drr_delta: number
  romi: number
  romi_delta: number
  revenue: number
  revenue_delta: number
  avg_cpc: number
  avg_cpc_delta: number
  roas: number
  roas_delta: number
}

export interface AdvertisingDailyPoint {
  date: string
  spend: number
  views: number
  clicks: number
  cart: number
  orders: number
  revenue: number
  ctr: number
  drr: number
}

export interface CampaignRow {
  campaign_id: number
  spend: number
  views: number
  clicks: number
  orders: number
  revenue: number
  ctr: number
  avg_cpc: number
  drr: number
}

export interface TopSkuRow {
  sku: number
  offer_id: string
  name: string
  image_url: string
  spend: number
  orders: number
  revenue: number
  drr: number
}

export interface AdvertisingAnalyticsResponse {
  shop_id: number
  marketplace: string
  period: string
  kpi: AdvertisingKpi
  chart_daily: AdvertisingDailyPoint[]
  campaigns_table: CampaignRow[]
  top_skus: TopSkuRow[]
}

// ── API ──────────────────────────────────────────────────────────

export async function getAdvertisingAnalytics(
  shopId: number,
  period: string = '7d',
): Promise<AdvertisingAnalyticsResponse> {
  const res = await apiClient.get<AdvertisingAnalyticsResponse>('/advertising-analytics', {
    params: { shop_id: shopId, period },
  })
  return res.data
}
