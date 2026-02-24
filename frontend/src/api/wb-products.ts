/**
 * WB Products API client.
 */
import { apiClient } from './client'

export interface WBProduct {
  nm_id: number
  vendor_code: string
  name: string
  image_url: string
  current_price: number
  cost_price: number
  packaging_cost: number
  // P&L waterfall
  avg_price: number
  orders_7d: number
  revenue_7d: number
  orders_prev: number
  revenue_prev: number
  revenue_delta: number
  // Ads
  ad_spend_7d: number
  ad_views: number
  ad_clicks: number
  drr: number
  // Stocks
  stock_fbo: number
  stock_fbs: number
  // Marketplace fees (from fact_finances)
  mp_fees: number
  mp_fees_percent: number
  mp_fees_commission: number
  mp_fees_logistics: number
  payout: number
  // Profit
  gross_profit: number | null
  margin: number | null
}

export interface WBProductsResponse {
  products: WBProduct[]
  total: number
  page: number
  per_page: number
  cost_missing_count: number
  totals?: Record<string, number>
}

export async function getWBProductsApi(params: {
  shop_id: number
  page?: number
  per_page?: number
  sort?: string
  order?: string
  filter?: string
  search?: string
  period?: number
  date_from?: string
  date_to?: string
}): Promise<WBProductsResponse> {
  const q = new URLSearchParams()
  q.set('shop_id', String(params.shop_id))
  q.set('page', String(params.page ?? 1))
  q.set('per_page', String(params.per_page ?? 25))
  q.set('sort', params.sort ?? 'revenue_7d')
  q.set('order', params.order ?? 'desc')
  q.set('filter', params.filter ?? 'all')
  q.set('search', params.search ?? '')
  q.set('period', String(params.period ?? 7))
  if (params.date_from) q.set('date_from', params.date_from)
  if (params.date_to) q.set('date_to', params.date_to)
  const resp = await apiClient.get(`/products/wb?${q.toString()}`)
  return resp.data
}

export async function updateWBCostApi(data: {
  shop_id: number
  vendor_code: string
  cost_price: number
  packaging_cost?: number
}): Promise<void> {
  await apiClient.patch('/products/wb/cost', data)
}

export async function uploadWBCostExcelApi(shopId: number, file: File): Promise<{
  ok: boolean; updated: number; errors: string[]
}> {
  const form = new FormData()
  form.append('file', file)
  const resp = await apiClient.post(`/products/wb/cost/bulk?shop_id=${shopId}`, form, {
    headers: { 'Content-Type': 'multipart/form-data' },
  })
  return resp.data
}

export async function downloadWBCostTemplate(shopId: number): Promise<void> {
  const resp = await apiClient.get(`/products/wb/cost/template?shop_id=${shopId}`, {
    responseType: 'blob',
  })
  const url = URL.createObjectURL(resp.data)
  const a = document.createElement('a')
  a.href = url
  a.download = `wb_cost_template_${shopId}.xlsx`
  a.click()
  URL.revokeObjectURL(url)
}
