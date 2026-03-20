import { apiClient } from './client'

export interface CampaignStatsRow {
  dt: string
  views: number
  clicks: number
  orders: number
  cart: number
  revenue: number
  spend: number
  ctr: number
  drr: number
  product_revenue: number
}

export interface CampaignEventRow {
  id: number
  timestamp: string
  event_type: string
  product_id?: string
  product_name?: string
  offer_id?: string
  old_value?: string
  new_value?: string
}

export interface CampaignPhraseRow {
  phrase: string
  views: number
  clicks: number
  ctr: number
  spend: number
  orders: number
  revenue: number
}

export interface CampaignHeatmapRow {
  day_of_week: number
  hour: number
  orders: number
}

export interface CampaignPurchaseRow {
  sku: number
  product_name: string
  offer_id: string
  quantity: number
  revenue: number
  avg_price: number
}

export interface KpiPeriod {
  spend: number
  ad_revenue: number
  product_revenue: number
  orders: number
  cart: number
  clicks: number
  views: number
  ctr: number
  drr_ad: number
  drr_product: number
  cpo: number
}

export interface CampaignKpiResponse {
  current: KpiPeriod
  previous: KpiPeriod
}

export const getCampaignKpi = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string,
  sku?: number
): Promise<CampaignKpiResponse> => {
  const { data } = await apiClient.get<CampaignKpiResponse>(
    `/campaign-details/${marketplace}/${campaignId}/kpi`,
    { params: { start_date: startDate, end_date: endDate, sku } }
  )
  return data
}

export const getCampaignStats = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string,
  sku?: number
): Promise<CampaignStatsRow[]> => {
  const { data } = await apiClient.get<CampaignStatsRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/stats`,
    { params: { start_date: startDate, end_date: endDate, sku } }
  )
  return data
}

export const getCampaignEvents = async (
  marketplace: string,
  campaignId: number,
  sku?: number,
  limit: number = 50
): Promise<CampaignEventRow[]> => {
  const { data } = await apiClient.get<CampaignEventRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/events`,
    { params: { sku, limit } }
  )
  return data
}

export const getCampaignPhrases = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string
): Promise<CampaignPhraseRow[]> => {
  const { data } = await apiClient.get<CampaignPhraseRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/phrases`,
    { params: { start_date: startDate, end_date: endDate } }
  )
  return data
}

export const getCampaignHeatmap = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string,
  sku?: number
): Promise<CampaignHeatmapRow[]> => {
  const { data } = await apiClient.get<CampaignHeatmapRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/heatmap`,
    { params: { start_date: startDate, end_date: endDate, sku } }
  )
  return data
}

export const getCampaignPurchases = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string
): Promise<CampaignPurchaseRow[]> => {
  const { data } = await apiClient.get<CampaignPurchaseRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/purchases`,
    { params: { start_date: startDate, end_date: endDate } }
  )
  return data
}
