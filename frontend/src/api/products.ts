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
  cancels: number
  cancel_rate: number
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
  mp_fees: number
  mp_fees_percent: number
  mp_fees_commission: number
  mp_fees_logistics: number
  mp_fees_storage: number
  mp_fees_other: number
  mp_fees_deductions: number
  mp_fees_acceptance: number
  mp_fees_fines: number
  sales_amount: number
  avg_price: number
  period: number
  events: ProductEvent[]
  promotions: string[]
  // Fee estimation indicator (WB only)
  fees_source?: 'actual' | 'estimated'
  // Active ad campaigns indicator
  has_active_ads?: boolean
}

export interface ProductsResponse {
  products: OzonProduct[]
  total: number
  page: number
  per_page: number
  cost_missing_count: number
  period: number
  totals?: Record<string, number>
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
  date_from?: string
  date_to?: string
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

// ── Ozon Prices (for Prices page) ────────────────────────

export interface OzonPriceProduct {
  product_id: number
  offer_id: string
  sku: number | null
  name: string
  image_url: string
  // Price fields
  price: number           // base seller price
  old_price: number       // original/strike-through
  marketing_price: number // real buyer price (after all discounts)
  min_price: number
  discount_pct: number    // calculated discount %
  // Price index
  price_index_color: string
  price_index_value: number
  competitor_min_price: number
  // Cost
  cost_price: number
  packaging_cost: number
  // Profit
  profit_per_unit: number | null
  profit_source: 'finance' | 'estimated' | null
  profit_with_ads: number | null
  // Ads
  ad_spend_30d: number
  drr: number | null
  // Stocks
  stock_fbo: number
  stock_fbs: number
  // Last sale
  last_sale_date: string | null
}

export interface OzonPricesResponse {
  products: OzonPriceProduct[]
  total: number
  page: number
  per_page: number
  cost_missing_count: number
}

export async function getOzonPricesApi(params: {
  shop_id: number
  search?: string
  sort?: string
  order?: string
  page?: number
  per_page?: number
}): Promise<OzonPricesResponse> {
  const { data } = await apiClient.get('/products/ozon/prices', { params })
  return data
}


// ── Price History (for Prices page popup) ────────────────

export interface PriceHistoryItem {
  id: number
  date: string
  old_price: number | null
  new_price: number | null
  direction: 'up' | 'down' | null
  change_pct: number | null
  price_field: string
}

export interface PriceHistoryResponse {
  shop_id: number
  nm_id: number
  history: PriceHistoryItem[]
}

export async function getPriceHistoryApi(params: {
  shop_id: number
  nm_id: number
}): Promise<PriceHistoryResponse> {
  const { data } = await apiClient.get('/events/price-history', { params })
  return data
}
