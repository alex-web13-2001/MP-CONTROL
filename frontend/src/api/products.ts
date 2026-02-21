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
  content_rating: number
  commission_percent: number
  fbo_logistics: number
  margin: number | null
  margin_percent: number | null
  events: ProductEvent[]
  promotions: string[]
}

export interface ProductsResponse {
  products: OzonProduct[]
  total: number
  page: number
  per_page: number
  cost_missing_count: number
}

export async function getOzonProductsApi(params: {
  shop_id: number
  page?: number
  per_page?: number
  sort?: string
  order?: string
  filter?: string
  search?: string
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
