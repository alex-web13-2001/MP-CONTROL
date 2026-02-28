import { apiClient } from './client'

export interface AbcXyzProduct {
  sku: number
  offer_id: string
  name: string
  image_url: string
  revenue: number
  profit: number
  orders: number
  avg_price: number
  cost_price: number
  commission: number
  logistics: number
  storage: number
  acquiring: number
  mp_fees: number
  ad_spend: number
  cogs: number
  margin_pct: number
  weekly_data: number[]
  abc_group: 'A' | 'B' | 'C'
  abc_share: number
  abc_cumulative: number
  xyz_group: 'X' | 'Y' | 'Z'
  xyz_cv: number
}


export interface AbcXyzSummary {
  [key: string]: {
    count: number
    revenue_share: number
  }
}

export interface AbcXyzMatrix {
  [key: string]: number
  AX: number; AY: number; AZ: number
  BX: number; BY: number; BZ: number
  CX: number; CY: number; CZ: number
}

export interface AbcXyzResponse {
  shop_id: number
  period: number
  use_profit: boolean
  products: AbcXyzProduct[]
  summary: AbcXyzSummary
  matrix: AbcXyzMatrix
}

export async function fetchAbcXyz(
  shopId: number,
  period: number = 90,
  useProfit: boolean = false,
): Promise<AbcXyzResponse> {
  const { data } = await apiClient.get<AbcXyzResponse>('/sales/ozon/abc-xyz', {
    params: { shop_id: shopId, period, use_profit: useProfit },
  })
  return data
}

export async function fetchWbAbcXyz(
  shopId: number,
  period: number = 90,
  useProfit: boolean = false,
): Promise<AbcXyzResponse> {
  const { data } = await apiClient.get<AbcXyzResponse>('/sales/wb/abc-xyz', {
    params: { shop_id: shopId, period, use_profit: useProfit },
  })
  return data
}
