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
  cart_rate: number
  cart_rate_delta: number
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
  total_drr: number
}

export interface CampaignSkuItem {
  sku: number
  product_id: number
  offer_id: string
  name: string
  image_url: string
  spend: number
  views: number
  clicks: number
  cart: number
  cart_conv: number
  orders: number
  order_conv: number
  direct_orders: number
  model_orders: number
  revenue: number
  direct_revenue: number
  model_revenue: number
  halo_pct: number
  ctr: number
  avg_cpc: number
  drr: number
  total_revenue: number
  total_drr: number
  bid: number
}

export interface CampaignRow {
  campaign_id: number
  title: string
  status: string
  status_code?: number
  campaign_type: string
  payment_type?: string
  bid_type?: string
  placements?: string[]
  sku_count: number
  items: CampaignSkuItem[]
  associated_items?: CampaignSkuItem[]
  spend: number
  views: number
  clicks: number
  cart: number
  cart_conv: number
  orders: number
  order_conv: number
  direct_orders: number
  model_orders: number
  revenue: number
  direct_revenue: number
  model_revenue: number
  halo_pct: number
  ctr: number
  avg_cpc: number
  drr: number
  total_revenue: number
  total_drr: number
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
  date_from: string
  date_to: string
  kpi: AdvertisingKpi
  chart_daily: AdvertisingDailyPoint[]
  campaigns_table: CampaignRow[]
  top_skus: TopSkuRow[]
  events_by_day: Record<string, EventDaySummary>
}

export interface EventDaySummary {
  advertising: number
  content: number
  price: number
  stock: number
  total: number
}

export interface EventDetail {
  id: number
  time: string
  event_type: string
  category: string
  label: string
  detail: string
  campaign_id: number | null
  campaign_title: string
  product: {
    nm_id: number
    name: string
    offer_id: string
    image_url: string
  } | null
}

export interface EventDetailResponse {
  date: string
  total: number
  events: EventDetail[]
}

// ── API ──────────────────────────────────────────────────────────

export async function getAdvertisingAnalytics(
  shopId: number,
  period: string = '7d',
  dateFrom?: string,
  dateTo?: string,
): Promise<AdvertisingAnalyticsResponse> {
  const params: Record<string, unknown> = { shop_id: shopId, period }
  if (dateFrom && dateTo) {
    params.date_from = dateFrom
    params.date_to = dateTo
  }
  const res = await apiClient.get<AdvertisingAnalyticsResponse>('/advertising-analytics', {
    params,
  })
  return res.data
}

export async function getEventsDetail(
  shopId: number,
  eventDate: string,
  category?: string,
): Promise<EventDetailResponse> {
  const res = await apiClient.get<EventDetailResponse>('/advertising-analytics/events-detail', {
    params: { shop_id: shopId, event_date: eventDate, ...(category ? { category } : {}) },
  })
  return res.data
}

// ── Per-campaign daily stats ─────────────────────────────────────

export interface CampaignDailyPoint {
  date: string
  spend: number
  views: number
  clicks: number
  cart: number
  orders: number
  revenue: number
  ctr: number
  drr: number
  total_revenue?: number
  total_drr?: number
}

export interface CampaignEvent {
  id: number
  date: string
  time: string
  event_type: string
  category: string
  label: string
  detail: string
  campaign_title: string
  nm_id: number | null
  offer_id: string
}

export interface CampaignDailyStatsResponse {
  campaigns_daily: Record<number, CampaignDailyPoint[]>
  events_by_campaign: Record<number, CampaignEvent[]>
  campaign_total_revenue: Record<number, number>
}

export async function getCampaignDailyStats(
  shopId: number,
  dateFrom: string,
  dateTo: string,
): Promise<CampaignDailyStatsResponse> {
  const res = await apiClient.get<CampaignDailyStatsResponse>('/advertising-analytics/campaign-daily-stats', {
    params: { shop_id: shopId, date_from: dateFrom, date_to: dateTo },
  })
  return res.data
}

// ── Ad Launch Recommendations ────────────────────────────────────

export type AdRecommendationCategory = 'selling_no_ads' | 'demand_no_ads' | 'ads_paused' | 'stagnant'

export interface AdRecommendationItem {
  nm_id: number
  sku: number
  offer_id: string
  name: string
  image_url: string
  category: AdRecommendationCategory
  reason: string
  orders_7d: number
  revenue_7d: number
  stock: number
  out_of_stock: boolean
  price: number
  last_ad_date: string | null
  priority: number
}

export interface AdRecommendationsResponse {
  total: number
  recommendations: AdRecommendationItem[]
}

export async function getAdLaunchRecommendations(
  shopId: number,
): Promise<AdRecommendationsResponse> {
  const res = await apiClient.get<AdRecommendationsResponse>('/advertising-analytics/ad-launch-recommendations', {
    params: { shop_id: shopId },
  })
  return res.data
}
