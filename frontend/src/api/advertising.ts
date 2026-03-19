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
  total_drr: number
}

export interface CampaignRow {
  campaign_id: number
  title: string
  sku_count: number
  items: CampaignSkuItem[]
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
): Promise<AdvertisingAnalyticsResponse> {
  const res = await apiClient.get<AdvertisingAnalyticsResponse>('/advertising-analytics', {
    params: { shop_id: shopId, period },
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
