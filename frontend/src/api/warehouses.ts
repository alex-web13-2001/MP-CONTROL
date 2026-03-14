/**
 * Warehouses API client — supply recommendations.
 */
import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────

export interface CrossClusterDrain {
  cluster: string
  qty: number
  daily: number
}

export interface SupplyCluster {
  cluster: string
  sold: number
  share: number
  daily: number
  daily_boosted: number
  est_stock: number
  wh_stock: number
  need: number
  revenue: number
  hub: string
  hub_hours: number
  warehouses: string[]
  // Cross-cluster analysis
  effective_days?: number
  post_restock_days?: number
  cross_consumption?: number
  cross_clusters?: CrossClusterDrain[]
}

export interface SupplyItem {
  offer_id: string
  name: string
  image_url: string
  sku: number
  sold: number
  revenue: number
  fbo_stock: number
  fbo_reserved: number
  fbo_warehouses: number
  daily_avg: number
  boost: number
  boosted_daily: number
  days_supply: number
  effective_days?: number
  status: 'critical' | 'attention' | 'ok'
  total_need: number
  ad_spend_7d: number
  ad_views_7d: number
  ad_clicks_7d: number
  ad_orders_7d: number
  ad_carts_7d: number
  clusters: SupplyCluster[]
}

export interface SupplyKpi {
  total_need: number
  critical_count: number
  attention_count: number
  avg_days_supply: number
  total_fbo: number
  total_sku: number
}

export interface HubItem {
  offer_id: string
  name: string
  image_url: string
  cluster: string
  need: number
  wh_stock: number
  revenue: number
  hub_hours: number
  daily_boosted: number
}

export interface HubSummary {
  hub: string
  items: HubItem[]
  total_need: number
  total_revenue: number
}

export interface SupplyResponse {
  shop_id: number
  sales_period: number
  target_days: number
  safety: number
  use_ad_boost: boolean
  kpi: SupplyKpi
  items: SupplyItem[]
  hubs: HubSummary[]
}

// ── API ──────────────────────────────────────────────────────

export interface SupplyParams {
  shop_id: number
  sales_period?: number
  target_days?: number
  safety?: number
  use_ad_boost?: boolean
}

export async function getOzonSupplyApi(params: SupplyParams): Promise<SupplyResponse> {
  const { data } = await apiClient.get<SupplyResponse>('/warehouses/ozon/supply', { params })
  return data
}

export async function downloadSupplyExcel(params: SupplyParams): Promise<void> {
  const response = await apiClient.get('/warehouses/ozon/supply/export', {
    params,
    responseType: 'blob',
  })
  const url = window.URL.createObjectURL(new Blob([response.data]))
  const link = document.createElement('a')
  link.href = url
  const target = params.target_days ?? 60
  link.setAttribute('download', `supply_${params.shop_id}_${target}d.xlsx`)
  document.body.appendChild(link)
  link.click()
  link.remove()
  window.URL.revokeObjectURL(url)
}

// ── WB Supply Types ──────────────────────────────────────────

export interface WBWarehouseDetail {
  warehouse: string
  stock: number
  orders: number
  revenue: number
  daily: number
  need: number
  storage_per_day: number
  storage_per_month: number
  storage_coef: number
  acceptance_coef: number
  acceptance: string
  turnover_days: number
}

export interface WBSupplyItem {
  nm_id: number
  vendor_code: string
  name: string
  image_url: string
  vol_liters: number
  total_sold: number
  total_stock: number
  daily_avg: number
  boost: number
  boosted_daily: number
  turnover_days: number
  total_need: number
  status: 'critical' | 'attention' | 'ok' | 'overstock'
  storage_cost_month: number
  warehouses: WBWarehouseDetail[]
}

export interface WBSupplyKpi {
  total_need: number
  critical_count: number
  attention_count: number
  overstock_count: number
  avg_days_supply: number
  total_stock: number
  total_sku: number
  total_storage_month: number
}

export interface WBWarehouseSummary {
  warehouse: string
  total_stock: number
  total_orders: number
  total_need: number
  total_revenue: number
  items_count: number
  storage_coef: number
  acceptance: string
}

export interface WBSupplyResponse {
  shop_id: number
  sales_period: number
  target_days: number
  safety: number
  use_ad_boost: boolean
  kpi: WBSupplyKpi
  items: WBSupplyItem[]
  warehouse_summary: WBWarehouseSummary[]
}

export interface WBSupplyParams {
  shop_id: number
  sales_period?: number
  target_days?: number
  safety?: number
  use_ad_boost?: boolean
}

export async function getWBSupplyApi(params: WBSupplyParams): Promise<WBSupplyResponse> {
  const { data } = await apiClient.get<WBSupplyResponse>('/warehouses/wb/supply', { params })
  return data
}

export async function downloadWBSupplyExcel(params: WBSupplyParams): Promise<void> {
  const response = await apiClient.get('/warehouses/wb/supply/xlsx', {
    params,
    responseType: 'blob',
  })
  const url = window.URL.createObjectURL(new Blob([response.data]))
  const link = document.createElement('a')
  link.href = url
  const target = params.target_days ?? 45
  link.setAttribute('download', `wb_supply_${params.shop_id}_${target}d.xlsx`)
  document.body.appendChild(link)
  link.click()
  link.remove()
  window.URL.revokeObjectURL(url)
}


// ── Ozon Warehouse Analytics Types ───────────────────────────

export interface WarehouseAnalyticsKpi {
  total_warehouses: number
  total_stock: number
  total_skus: number
  avg_turnover_days: number | null
  avg_delivery_h: number | null
  total_crossdocking: number
  total_storage_fee: number
  total_fbo_processing: number
  critical_warehouses: number
  overstocked_warehouses: number
  period_days: number
}

export interface WarehouseClusterServed {
  cluster: string
  orders: number
  qty: number
  share: number
}

export interface WarehouseSkuDetail {
  sku: number
  offer_id: string
  name: string
  stock: number
  reserved: number
  daily_sales: number
  days_supply: number | null
  turnover_category: string
}

export interface WarehouseDetail {
  warehouse_name: string
  cluster: string
  warehouse_type: string
  stock_free: number
  stock_reserved: number
  sku_count: number
  orders_period: number
  qty_period: number
  revenue_period: number
  daily_sales: number
  turnover_days: number | null
  days_to_zero: number | null
  pct_of_total_sales: number
  delivery_speed_avg_h: number
  status: 'critical' | 'empty' | 'attention' | 'overstocked' | 'storage_fee' | 'ok'
  storage_risk: 'critical' | 'warning' | 'ok'
  estimated_storage_cost_day: number
  costs: {
    crossdocking: number
    crossdocking_cnt: number
    storage: number
    storage_cnt: number
    fbo_processing: number
    fbo_cnt: number
    total: number
  }
  clusters_served: WarehouseClusterServed[]
  skus: WarehouseSkuDetail[]
}

export interface CostItem {
  name: string
  count: number
  amount: number
}

export interface Recommendation {
  type: 'move_stock' | 'optimize_crossdocking' | 'storage_warning' | 'paid_storage'
  severity: 'high' | 'medium' | 'low'
  title?: string
  reason: string
  impact?: string
  action_items?: string[]
  affected_sku_names?: string[]
  est_savings?: number
  warehouse?: string
  from_warehouse?: string
  to_warehouse?: string
  [key: string]: any
}

export interface StorageRiskSku {
  sku: number
  offer_id: string
  name: string
  total_stock: number
  sold_period: number
  daily_sales: number
  turnover_days: number | null
  days_over_threshold: number
  zone: 'paid' | 'warning' | 'free'
  volume_liters: number
  est_daily_cost: number
  est_monthly_cost: number
  revenue_period: number
  ad_info: { has_ads: boolean; spend_30d: number; orders_30d: number }
  recommendation: { action: string; reason: string; severity: string }
  warehouses: { warehouse_name: string; stock: number; reserved: number }[]
}

export interface CrossdockingSku {
  sku: number
  offer_id: string
  name: string
  total_sold_via_cd: number
  total_revenue: number
  daily_sales_cd: number
  est_cd_cost_monthly: number
  recommended_supply: number
  warehouse_count: number
  action: 'transfer' | 'supply'
  transfer_qty: number
  supply_qty: number
  volume_liters: number
  transfer_cost_per_unit: number
  total_transfer_cost: number
  source_warehouses: { warehouse_name: string; stock: number; sales: number; daily_sales: number; turnover_days: number; excess: number }[]
  demand_warehouses: { warehouse_name: string; sold: number; revenue: number; current_stock: number }[]
}

export interface SkuGeographyWarehouse {
  warehouse_name: string
  cluster: string
  stock: number
  reserved: number
  daily_sales: number
  days_supply: number | null
  warehouse_status: string
}

export interface SalesCluster {
  cluster: string
  orders: number
  qty: number
  revenue: number
}

export interface SkuGeography {
  sku: number
  offer_id: string
  name: string
  total_stock: number
  total_daily_sales: number
  warehouses: SkuGeographyWarehouse[]
  sales_clusters: SalesCluster[]
}

export interface DistributionPlanItem {
  sku: number
  offer_id: string
  name: string
  action: 'transfer' | 'supply'
  qty: number
  sold_via_cd: number
  daily_sales_cd: number
  revenue: number
  volume_liters: number
  transfer_cost_per_unit: number
  est_cd_cost_monthly: number
  source_warehouse?: string
  source_excess?: number
  transfer_cost?: number
  demand_cities?: { city: string; qty: number }[]
  shipped_from?: { cluster: string; qty: number }[]
  reason?: string
  benefit?: string
}

export interface DistributionPlanWarehouse {
  warehouse_name: string
  items: DistributionPlanItem[]
  total_cd_cost_monthly: number
  total_transfer_cost: number
  transfer_count: number
  supply_count: number
  total_qty: number
  top_demand_cities?: { city: string; qty: number }[]
  total_orders_cd?: number
}

export interface WarehouseAnalyticsResponse {
  kpi: WarehouseAnalyticsKpi
  summary: string
  costs: Record<string, CostItem>
  warehouses: WarehouseDetail[]
  recommendations: Recommendation[]
  storage_risk_skus: StorageRiskSku[]
  crossdocking_skus: CrossdockingSku[]
  distribution_plan: DistributionPlanWarehouse[]
  sku_geography: SkuGeography[]
}

export async function getOzonWarehouseAnalytics(params: {
  shop_id: number
  period?: number
}): Promise<WarehouseAnalyticsResponse> {
  const { data } = await apiClient.get<WarehouseAnalyticsResponse>('/warehouses/ozon/analytics', { params })
  return data
}

export async function downloadDistributionPlanExcel(params: {
  shop_id: number
  period?: number
}): Promise<void> {
  const response = await apiClient.get('/warehouses/ozon/analytics/distribution-plan/excel', {
    params,
    responseType: 'blob',
  })
  const url = window.URL.createObjectURL(new Blob([response.data]))
  const link = document.createElement('a')
  link.href = url
  link.setAttribute('download', `distribution_plan_shop${params.shop_id}_${params.period ?? 30}d.xlsx`)
  document.body.appendChild(link)
  link.click()
  link.remove()
  window.URL.revokeObjectURL(url)
}


// ── WB Warehouse Analytics ──────────────────────────────────

export interface WBAnalyticsKpi {
  total_warehouses: number
  total_stock: number
  total_sku: number
  avg_turnover_days: number | null
  total_logistics: number
  total_storage: number
  total_storage_actual: number | null
  total_penalties: number
  cross_pct: number
  total_orders: number
  period_days: number
  has_actual_storage: boolean
  forecast_30d: number | null
}

export interface WBAnalyticsSkuDetail {
  nm_id: number
  vendor_code: string
  name: string
  stock: number
  daily_sales: number
  days_supply: number | null
  orders: number
  cross_orders: number
  cross_pct: number
  geography: WBAnalyticsGeography[]
}

export interface WBAnalyticsGeography {
  okrug: string
  orders: number
  share: number
  is_local: boolean
}

export interface WBAnalyticsWarehouse {
  warehouse_name: string
  okrug: string
  warehouse_type: 'food' | 'sgt' | 'normal'
  status: 'critical' | 'attention' | 'ok' | 'overstocked' | 'empty'
  stock: number
  sku_count: number
  orders: number
  revenue: number
  daily_sales: number
  turnover_days: number | null
  pct_of_total_sales: number
  cross_pct: number
  cross_orders: number
  local_orders: number
  logistics_cost: number
  logistics_count: number
  storage_coef: number
  storage_cost_actual: number
  storage_cost_month: number
  acceptance_coef: number
  acceptance: string
  skus: WBAnalyticsSkuDetail[]
  geography: WBAnalyticsGeography[]
}

export interface WBCrossMapRow {
  warehouse: string
  home_okrug: string
  total_orders: number
  okrugs: Record<string, { count: number; is_local: boolean }>
}

export interface WBCostSummary {
  operation_type: string
  label: string
  icon: string
  count: number
  amount: number
}

export interface WBStorageSku {
  nm_id: number
  vendor_code: string
  name: string
  vol_liters: number
  total_stock: number
  est_cost_month: number
  storage_source: 'actual' | 'estimated'
  forecast_30d: number | null
  daily_sales: number
  daily_cost: number | null
  days_to_sell: number | null
  warehouses: { warehouse: string; stock: number; stor_base: number; cost_month: number; source: string }[]
}

export interface WBRecommendation {
  type: string
  severity: 'high' | 'medium' | 'low'
  title: string
  reason: string
  impact?: string
  action_items: string[]
  warehouse: string
}

export interface WBWarehouseAnalyticsResponse {
  kpi: WBAnalyticsKpi
  warehouses: WBAnalyticsWarehouse[]
  cross_map: WBCrossMapRow[]
  okrug_list: string[]
  costs: WBCostSummary[]
  storage_skus: WBStorageSku[]
  recommendations: WBRecommendation[]
  period_days: number
}

export async function getWBWarehouseAnalytics(params: {
  shop_id: number
  period?: number
}): Promise<WBWarehouseAnalyticsResponse> {
  const { data } = await apiClient.get<WBWarehouseAnalyticsResponse>('/warehouses/wb/analytics', { params })
  return data
}


// ═══════════════════════════════════════════════════════════
// AI Warehouse Analysis (2-block: SKU problems + redistribution)
// ═══════════════════════════════════════════════════════════

export interface AISkuOption {
  action: 'discount' | 'launch_ads' | 'withdraw' | 'do_nothing' | 'reduce_supply'
  label: string
  detail: string
  expected_savings: number
  risk: 'low' | 'medium' | 'high'
}

export interface AISkuAction {
  vendor_code: string
  name: string
  problem: string
  storage_cost_month: number
  net_profit_month: number
  current_turnover_days: number
  stock: number
  options: AISkuOption[]
  recommended_option: number
}

export interface AITransferDestination {
  warehouse: string
  qty: number
  reason: string
}

export interface AITransfer {
  vendor_code: string
  name: string
  from_warehouse: string
  from_stock: number
  keep_at_source: number
  destinations: AITransferDestination[]
  expected_effect: string
}

export interface AIKeyMetrics {
  cross_logistics_loss: number
  storage_excess: number
  unprofitable_skus_count: number
}

export interface AIAnalysisContext {
  total_orders: number
  total_stock: number
  cross_pct: number
  costs_logistics: number
  costs_storage: number
  costs_penalties: number
  skus_in_ads: number
  skus_no_ads: number
  warehouses_count: number
}

export interface AIWarehouseAnalysis {
  severity: 'critical' | 'warning' | 'ok'
  diagnosis: string
  total_potential_savings: number
  key_metrics: AIKeyMetrics
  sku_actions: AISkuAction[]
  transfers: AITransfer[]
  general_tips: string[]
  supply_tip: string
  shop_name: string
  period_days: number
  analyzed_at: number
  context: AIAnalysisContext
  cached: boolean
  cached_at?: number
}

export async function getWBWarehouseAIAnalysis(params: {
  shop_id: number
  period?: number
  force?: boolean
}): Promise<AIWarehouseAnalysis> {
  const { data } = await apiClient.get<AIWarehouseAnalysis>('/warehouses/wb/ai-analysis', { params })
  return data
}

