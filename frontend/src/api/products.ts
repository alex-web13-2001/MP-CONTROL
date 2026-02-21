/**
 * Products API client.
 */
import { apiClient } from './client'

export interface ProductEvent {
  type: string
  old_value: string | null
  new_value: string | null
  date: string | null
}

export interface OzonProduct {
  product_id: number
  offer_id: string
  sku: number | null
  name: string
  barcode: string | null
  image_url: string
  price: number
  old_price: number
  min_price: number
  marketing_price: number
  stocks_fbo: number
  stocks_fbs: number
  price_index_color: string
  price_index_value: number
  competitor_min_price: number
  status: string
  status_name: string
  is_archived: boolean
  volume_weight: number
  model_count: number
  images_count: number
  cost_price: number
  packaging_cost: number
  orders_7d: number
  revenue_7d: number
  orders_prev_7d: number
  revenue_delta: number
  ad_spend_7d: number
  drr: number
  returns_30d: number
  orders_30d: number
  content_rating: number
  commission_percent: number
  fbo_logistics: number
  margin: number | null
  margin_percent: number | null
  payout_period: number
  payout_prev: number
  gross_profit: number | null
  gross_profit_percent: number | null
  gross_profit_prev: number | null
  gross_profit_delta: number | null
  period: number
  events: ProductEvent[]
  promotions: string[]
}

export interface ProductsResponse {
  products: OzonProduct[]
  total: number
  page: number
  per_page: number
  cost_missing_count: number
  period: number
}

export async function getOzonProductsApi(params: {
  shop_id: number
  page?: number
  per_page?: number
  sort?: string
  order?: string
  filter?: string
  search?: string
  period?: number
}): Promise<ProductsResponse> {
  const { data } = await apiClient.get('/products/ozon', { params })
  return data
}

export async function updateOzonCostApi(body: {
  shop_id: number
  offer_id: string
  cost_price: number
  packaging_cost?: number
}): Promise<{ ok: boolean; offer_id: string; cost_price: number }> {
  const { data } = await apiClient.patch('/products/ozon/cost', body)
  return data
}

export async function uploadCostExcelApi(
  shopId: number,
  file: File,
): Promise<{ ok: boolean; updated: number; errors: string[] }> {
  const formData = new FormData()
  formData.append('file', file)
  const { data } = await apiClient.post(`/products/ozon/cost/bulk?shop_id=${shopId}`, formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  })
  return data
}

export async function downloadCostTemplate(shopId: number): Promise<void> {
  const { data } = await apiClient.get(`/products/ozon/cost/template?shop_id=${shopId}`, {
    responseType: 'blob',
  })
  const url = window.URL.createObjectURL(data)
  const a = document.createElement('a')
  a.href = url
  a.download = `cost_template_${shopId}.xlsx`
  a.click()
  window.URL.revokeObjectURL(url)
}
