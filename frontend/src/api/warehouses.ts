/**
 * Warehouses API client — supply recommendations.
 */
import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────

export interface SupplyCluster {
  cluster: string
  sold: number
  share: number
  daily: number
  daily_boosted: number
  est_stock: number
  need: number
  revenue: number
  hub: string
  hub_hours: number
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
  warehouses: { warehouse_name: string; stock: number; reserved: number }[]
}

export interface WarehouseAnalyticsResponse {
  kpi: WarehouseAnalyticsKpi
  costs: Record<string, CostItem>
  warehouses: WarehouseDetail[]
  recommendations: Recommendation[]
  storage_risk_skus: StorageRiskSku[]
}

export async function getOzonWarehouseAnalytics(params: {
  shop_id: number
  period?: number
}): Promise<WarehouseAnalyticsResponse> {
  const { data } = await apiClient.get<WarehouseAnalyticsResponse>('/warehouses/ozon/analytics', { params })
  return data
}
