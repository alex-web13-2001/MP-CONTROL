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

