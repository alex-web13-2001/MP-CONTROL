/**
 * LTV Analysis API module (Ozon only).
 */
import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────

export interface LtvKpi {
  total_clients: number
  repeat_clients: number
  repeat_rate: number
  avg_ltv: number
  avg_check: number
  avg_orders_per_client: number
  total_revenue: number
}

export interface CohortMonth {
  clients: number
  rate: number
}

export interface CohortRow {
  cohort: string
  size: number
  months: Record<string, CohortMonth>
}

export interface SkuRepeatRow {
  sku: number
  offer_id: string
  name: string
  image_url: string
  total_buyers: number
  total_qty: number
  total_revenue: number
  repeat_buyers: number
  buyers_3plus: number
  conv_to_2: number
  conv_to_3: number
  avg_days_between: number
  avg_ltv_repeat: number
}

export interface TimeBucket {
  bucket: string
  count: number
  avg_days: number
}

export interface MonthlyBuyers {
  month: string
  total: number
  new_buyers: number
  repeat_buyers: number
  new_revenue: number
  repeat_revenue: number
}

export interface LtvResponse {
  shop_id: number
  period: string
  date_range: { start: string; end: string }
  kpi: LtvKpi
  cohort_matrix: CohortRow[]
  sku_table: SkuRepeatRow[]
  time_distribution: TimeBucket[]
  monthly_buyers: MonthlyBuyers[]
}

// ── Chain Types ──────────────────────────────────────────

export interface ChainProduct {
  sku: number
  offer_id: string
  name: string
  buyers: number
  total_qty: number
  total_revenue: number
  avg_revenue: number
  pct_of_l1: number
  pct_of_level: number
}

export interface ChainLevel {
  level: number
  total_buyers: number
  conversion_from_prev: number
  conversion_from_l1: number
  products: ChainProduct[]
}

export interface ChainL1 {
  sku: number
  offer_id: string
  name: string
  total_buyers: number
  total_qty: number
  total_revenue: number
  avg_price: number
}

export interface ChainResponse {
  shop_id: number
  target_sku: number
  period: string
  date_range: { start: string; end: string }
  l1: ChainL1
  chain: ChainLevel[]
  avg_days_between: {
    l1_to_l2: number
    l2_to_l3: number
    l3_to_l4: number
    l4_to_l5: number
  }
}

// ── API calls ────────────────────────────────────────────

export async function fetchLtv(
  shopId: number,
  period = '6m',
): Promise<LtvResponse> {
  const { data } = await apiClient.get<LtvResponse>('/sales/ozon/ltv', {
    params: { shop_id: shopId, period },
  })
  return data
}

export async function fetchPurchaseChain(
  shopId: number,
  sku: number,
  period = '6m',
): Promise<ChainResponse> {
  const { data } = await apiClient.get<ChainResponse>('/sales/ozon/ltv/chain', {
    params: { shop_id: shopId, sku, period },
  })
  return data
}
