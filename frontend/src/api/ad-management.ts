/**
 * Ad Management API client — WRITE operations + enriched data for WB advertising.
 *
 * All bids are in KOPECKS (×100 from rubles).
 * Example: bid = 15000 means 150₽
 */
import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────────

export interface NmSetting {
  nm_id: number
  bid_search: number        // kopecks
  bid_recommendations: number // kopecks
  subject_name: string
  product_name: string
  vendor_code: string
}

export interface EnrichedCampaign {
  advert_id: number
  name: string
  type: number
  status: number
  status_label: string
  payment_type: string
  bid_type: string
  search_enabled: boolean
  recommendations_enabled: boolean
  change_time: string | null
  nm_settings: NmSetting[]
  // Stats from ClickHouse
  spend: number
  views: number
  clicks: number
  cart: number
  orders: number
  revenue: number
  ctr: number
  drr: number
  cpc: number
  cpm: number
  cpa_cart: number
  cpo: number
  // Budget from Redis cache (synced every 15 min)
  budget_total?: number
  budget_daily?: number
}

export interface KpiData {
  spend: number
  views: number
  clicks: number
  cart: number
  orders: number
  revenue: number
  ctr: number
  drr: number
}

export interface EnrichedCampaignsResponse {
  campaigns: EnrichedCampaign[]
  total: number
  balance: { balance?: number; bonus?: number; net?: number } | null
  kpi: KpiData
  kpi_deltas: Record<string, number>
  period: { start: string; end: string }
}

export interface CampaignWithBids {
  advert_id: number
  name: string
  type: number
  status: number
  status_label: string
  payment_type: string
  bid_type: string
  search_enabled: boolean
  recommendations_enabled: boolean
  change_time: string | null
  nm_settings: NmSetting[]
}

export interface CampaignsListResponse {
  campaigns: CampaignWithBids[]
  total: number
  balance: { balance?: number; bonus?: number } | null
}

export interface StatusChangeResponse {
  success: boolean
  message: string
  status_code: number
}

export interface ChangeBidsResponse {
  success: boolean
  message: string
  status_code: number
  bids_applied: number
}

export interface BatchStatusResult {
  advert_id: number
  success: boolean
  message: string
}

export interface BatchStatusResponse {
  results: BatchStatusResult[]
  total: number
  success_count: number
  failed_count: number
}

export interface BalanceResponse {
  success: boolean
  balance: number | null
  bonus: number | null
  message: string
}

export interface AuditLogEntry {
  id: number
  action: string
  advert_id: number | null
  details: Record<string, unknown> | null
  created_at: string
  user_name: string | null
}

export interface AuditLogResponse {
  entries: AuditLogEntry[]
  total: number
}

// ── API Functions ────────────────────────────────────────────────

const PREFIX = '/ad-management/wb'

/**
 * Get enriched campaigns (management + ClickHouse stats + KPI).
 */
export async function getEnrichedCampaigns(
  shopId: number,
  period: string = '30d',
  dateFrom?: string,
  dateTo?: string,
): Promise<EnrichedCampaignsResponse> {
  const params: Record<string, string | number> = { shop_id: shopId, period }
  if (dateFrom) params.date_from = dateFrom
  if (dateTo) params.date_to = dateTo
  const res = await apiClient.get<EnrichedCampaignsResponse>(
    `${PREFIX}/campaigns/enriched`,
    { params },
  )
  return res.data
}

/** Stats per campaign from ClickHouse — NO WB API calls. Use for period changes. */
export interface CampaignStatsResponse {
  stats: Record<number, {
    spend: number; views: number; clicks: number; cart: number
    orders: number; revenue: number; ctr: number; drr: number
    cpc: number; cpm: number; cpa_cart: number; cpo: number
  }>
  kpi: KpiData
  kpi_deltas: Record<string, number>
  period: { start: string; end: string }
}

export async function getCampaignStats(
  shopId: number,
  period: string = '30d',
  dateFrom?: string,
  dateTo?: string,
): Promise<CampaignStatsResponse> {
  const params: Record<string, string | number> = { shop_id: shopId, period }
  if (dateFrom) params.date_from = dateFrom
  if (dateTo) params.date_to = dateTo
  const res = await apiClient.get<CampaignStatsResponse>(
    `${PREFIX}/campaigns/stats`,
    { params },
  )
  return res.data
}

/**
 * Get campaigns from DB (0 WB API calls).
 * All data from ClickHouse + Redis cache.
 */
export async function getCampaignsFromDB(
  shopId: number,
  period: string = '7d',
  dateFrom?: string,
  dateTo?: string,
): Promise<EnrichedCampaignsResponse> {
  const params: Record<string, string | number> = { shop_id: shopId, period }
  if (dateFrom) params.date_from = dateFrom
  if (dateTo) params.date_to = dateTo
  const res = await apiClient.get<EnrichedCampaignsResponse>(
    `${PREFIX}/campaigns/from-db`,
    { params },
  )
  return res.data
}

/**
 * Get all campaigns with current bids + balance.
 */
export async function getWBCampaigns(
  shopId: number,
): Promise<CampaignsListResponse> {
  const res = await apiClient.get<CampaignsListResponse>(
    `${PREFIX}/campaigns`,
    { params: { shop_id: shopId } },
  )
  return res.data
}

/**
 * Start (resume) a paused campaign.
 */
export async function startCampaign(
  shopId: number,
  advertId: number,
): Promise<StatusChangeResponse> {
  const res = await apiClient.post<StatusChangeResponse>(
    `${PREFIX}/campaigns/start`,
    { shop_id: shopId, advert_id: advertId },
  )
  return res.data
}

/**
 * Pause an active campaign.
 */
export async function pauseCampaign(
  shopId: number,
  advertId: number,
): Promise<StatusChangeResponse> {
  const res = await apiClient.post<StatusChangeResponse>(
    `${PREFIX}/campaigns/pause`,
    { shop_id: shopId, advert_id: advertId },
  )
  return res.data
}

/**
 * Stop a campaign PERMANENTLY (irreversible!).
 */
export async function stopCampaign(
  shopId: number,
  advertId: number,
): Promise<StatusChangeResponse> {
  const res = await apiClient.post<StatusChangeResponse>(
    `${PREFIX}/campaigns/stop`,
    { shop_id: shopId, advert_id: advertId },
  )
  return res.data
}

/**
 * Change bids for nm_ids in a campaign.
 */
export async function changeBids(
  shopId: number,
  advertId: number,
  placement: 'search' | 'recommendations' | 'combined',
  bids: { nm_id: number; bid: number }[],
  bidType: string = 'manual',
): Promise<ChangeBidsResponse> {
  const res = await apiClient.post<ChangeBidsResponse>(
    `${PREFIX}/bids/change`,
    { shop_id: shopId, advert_id: advertId, placement, bids, bid_type: bidType },
  )
  return res.data
}

/**
 * Batch start/pause for multiple campaigns.
 */
export async function batchStatusChange(
  shopId: number,
  advertIds: number[],
  action: 'start' | 'pause',
): Promise<BatchStatusResponse> {
  const res = await apiClient.post<BatchStatusResponse>(
    `${PREFIX}/status/batch`,
    { shop_id: shopId, advert_ids: advertIds, action },
  )
  return res.data
}

/**
 * Get current advertising balance.
 */
export async function getWBBalance(
  shopId: number,
): Promise<BalanceResponse> {
  const res = await apiClient.get<BalanceResponse>(
    `${PREFIX}/balance`,
    { params: { shop_id: shopId } },
  )
  return res.data
}

/**
 * Get budget for a specific campaign.
 */
export async function getCampaignBudget(
  shopId: number,
  advertId: number,
): Promise<Record<string, any>> {
  const res = await apiClient.get(
    `${PREFIX}/budget`,
    { params: { shop_id: shopId, advert_id: advertId } },
  )
  return res.data
}

/**
 * Batch-fetch budgets for multiple campaigns (with Redis cache on backend).
 */
export async function getBudgetsBatch(
  shopId: number,
  advertIds: number[],
): Promise<Record<number, { total: number; daily: number }>> {
  const res = await apiClient.post(
    `${PREFIX}/budgets/batch`,
    { shop_id: shopId, advert_ids: advertIds },
  )
  return res.data?.budgets || {}
}

/**
 * Deposit (top-up) budget for a campaign.
 */
export async function depositBudget(
  shopId: number,
  advertId: number,
  amount: number,
  budgetType: number = 1,
): Promise<{ success: boolean; message: string; new_budget_total?: number | null }> {
  const res = await apiClient.post(
    `${PREFIX}/budget/deposit`,
    { shop_id: shopId, advert_id: advertId, amount, budget_type: budgetType },
  )
  return res.data
}

/**
 * Get audit log of management actions.
 */
export async function getAuditLog(
  shopId: number,
  limit = 50,
  offset = 0,
): Promise<AuditLogResponse> {
  const res = await apiClient.get<AuditLogResponse>(
    `${PREFIX}/audit-log`,
    { params: { shop_id: shopId, limit, offset } },
  )
  return res.data
}

// ── Helpers ──────────────────────────────────────────────────────

/** Convert kopecks to rubles string with ₽ sign */
export function kopecksToRubles(kopecks: number): string {
  const rub = kopecks / 100
  return `${rub.toLocaleString('ru-RU', { maximumFractionDigits: 0 })} ₽`
}

/** Convert rubles to kopecks */
export function rublesToKopecks(rubles: number): number {
  return Math.round(rubles * 100)
}

/** Format money value */
export function formatMoney(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M ₽`
  return `${Math.round(value).toLocaleString('ru-RU')} ₽`
}

/** Format number with locale */
export function formatNum(value: number): string {
  return value.toLocaleString('ru-RU', { maximumFractionDigits: 0 })
}

/** Status code to color class mapping */
export const STATUS_COLORS: Record<number, string> = {
  9: 'text-emerald-400',
  11: 'text-amber-400',
  4: 'text-blue-400',
  7: 'text-zinc-500',
  [-1]: 'text-red-400',
  8: 'text-red-400',
}

/** Action labels for audit log */
export const ACTION_LABELS: Record<string, string> = {
  campaign_start: '▶ Запуск кампании',
  campaign_pause: '⏸ Пауза кампании',
  campaign_stop: '⏹ Остановка кампании',
  bid_change: '💰 Изменение ставки',
  batch_status: '📋 Массовое изменение статуса',
  batch_bid_change: '📋 Массовое изменение ставок',
  budget_deposit: '💳 Пополнение бюджета',
  normquery_bid_change: '🎯 Изменение ставок кластеров',
  normquery_minus_phrases: '🚫 Изменение минус-фраз',
  normquery_toggle_exclude: '🔄 Переключение кластера',
  campaign_nms_manage: '📦 Управление товарами',
}

// ── Normquery (Search Cluster) Types ──────────────────────────────

export interface NormqueryClusterStat {
  norm_query: string
  status: 'active' | 'excluded'
  views: number
  clicks: number
  atbs: number
  orders: number
  avg_pos: number
  spend_rub: number
  cpc_kopecks: number
  cpm_kopecks: number
  cpm_rub: number
  ctr: number
  current_bid_kopecks: number
  current_bid_rub: number
  reach_max_bid: number
  reach_med_bid: number
  reach_min_bid: number
  cr_click_to_order: number
  cr_click_to_cart: number
  cpc_rub: number
}

export interface ClusterListResponse {
  clusters: NormqueryClusterStat[]
  total_active: number
  total_excluded: number
  total_clusters: number
  base_bids: {
    competitive_kopecks?: number
    leaders_kopecks?: number
    competitive_rub?: number
    leaders_rub?: number
  }
}

export interface NormqueryAnalyticsResponse {
  clusters: NormqueryClusterStat[]
  excluded_clusters: string[]
  minus_phrases: string[]
  base_bids: {
    competitive_kopecks?: number
    leaders_kopecks?: number
    competitive_rub?: number
    leaders_rub?: number
  }
  total_clusters: number
}

// ── Normquery API Functions ──────────────────────────────────────

/**
 * Get normquery (search cluster) analytics for a UWB campaign.
 * Combines stats + bids + minus phrases + recommendations from WB API.
 */
export async function getNormqueryAnalytics(
  shopId: number,
  campaignId: number,
  startDate: string,
  endDate: string,
): Promise<NormqueryAnalyticsResponse> {
  const res = await apiClient.get<NormqueryAnalyticsResponse>(
    `/campaign-details/wb/${campaignId}/normquery-analytics`,
    { params: { shop_id: shopId, start_date: startDate, end_date: endDate } },
  )
  return res.data
}

/**
 * Get unified cluster list (active + excluded) with stats, bids, recommendations.
 * LIVE version — calls WB API directly. Use getClusterListCached for fast reads.
 */
export async function getClusterList(
  shopId: number,
  advertId: number,
  nmId: number,
  startDate: string,
  endDate: string,
): Promise<ClusterListResponse> {
  const res = await apiClient.post<ClusterListResponse>(
    `${PREFIX}/normquery/cluster-list`,
    { shop_id: shopId, advert_id: advertId, nm_id: nmId, start_date: startDate, end_date: endDate },
  )
  return res.data
}

/**
 * Get unified cluster list from ClickHouse (pre-collected by background sync).
 * FAST — no WB API calls. Falls back to live API if no cached data.
 */
export async function getClusterListCached(
  shopId: number,
  advertId: number,
  nmId: number,
  startDate: string,
  endDate: string,
): Promise<ClusterListResponse & { source?: string }> {
  const res = await apiClient.post<ClusterListResponse & { source?: string }>(
    `${PREFIX}/normquery/cluster-list-cached`,
    { shop_id: shopId, advert_id: advertId, nm_id: nmId, start_date: startDate, end_date: endDate },
  )
  return res.data
}

/**
 * Toggle a cluster's exclusion status (exclude ↔ include).
 * Atomic: get-minus → modify → set-minus.
 */
export async function toggleClusterExclusion(
  shopId: number,
  advertId: number,
  nmId: number,
  normQuery: string,
  action: 'exclude' | 'include',
): Promise<{ success: boolean; message: string; phrases?: string[] }> {
  const res = await apiClient.post(
    `${PREFIX}/normquery/toggle-exclude`,
    { shop_id: shopId, advert_id: advertId, nm_id: nmId, norm_query: normQuery, action },
  )
  return res.data
}

/**
 * Set bids per search cluster (normquery) for a UWB campaign.
 * Bids are in KOPECKS.
 */
export async function setNormqueryBids(
  shopId: number,
  advertId: number,
  nmId: number,
  bids: { norm_query: string; bid: number }[],
): Promise<{ success: boolean; message: string }> {
  const res = await apiClient.post(
    `${PREFIX}/normquery/set-bids`,
    { shop_id: shopId, advert_id: advertId, nm_id: nmId, bids },
  )
  return res.data
}

/**
 * Set minus phrases for a campaign (replaces the full list).
 */
export async function setMinusPhrases(
  shopId: number,
  advertId: number,
  nmId: number,
  normQueries: string[],
): Promise<{ success: boolean; message: string }> {
  const res = await apiClient.post(
    `${PREFIX}/normquery/set-minus`,
    { shop_id: shopId, advert_id: advertId, nm_id: nmId, norm_queries: normQueries },
  )
  return res.data
}

/**
 * Add/remove products (nm_ids) from a campaign.
 */
export async function manageCampaignNms(
  shopId: number,
  advertId: number,
  add: number[],
  remove: number[],
): Promise<{ success: boolean; message: string }> {
  const res = await apiClient.patch(
    `${PREFIX}/campaigns/nms`,
    { shop_id: shopId, advert_id: advertId, add, delete: remove },
  )
  return res.data
}

