import { apiClient } from './client'

// ═══════════════════════════════════════════════════════════════
// Ozon Dashboard Types (legacy flat structure)
// ═══════════════════════════════════════════════════════════════

export interface DashboardKpi {
  orders_count: number
  orders_delta: number
  revenue: number
  revenue_delta: number
  avg_check: number
  ad_spend: number
  ad_spend_delta: number
  views: number
  views_delta: number
  clicks: number
  clicks_delta: number
  drr: number
  drr_delta: number
}

export interface SalesDailyPoint {
  date: string
  orders: number
  revenue: number
}

export interface AdsDailyPoint {
  date: string
  spend: number
  views: number
  clicks: number
  cart: number
  orders: number
  drr_ad: number
  drr_total: number
}

export interface TopProduct {
  offer_id: string
  supplier_article?: string
  name: string
  image_url: string
  orders: number
  revenue: number
  delta_pct: number
  stock_fbo: number
  stock_fbs: number
  price: number
  ad_spend: number
  drr: number
}

export interface DashboardResponse {
  shop_id: number
  period: string
  kpi: DashboardKpi
  charts: {
    sales_daily: SalesDailyPoint[]
    ads_daily: AdsDailyPoint[]
  }
  top_products: TopProduct[]
}

// ═══════════════════════════════════════════════════════════════
// WB Dashboard Types (new rich structure)
// ═══════════════════════════════════════════════════════════════

export interface WbKpiSales {
  revenue: number
  revenue_delta: number
  profit: number
  profit_delta: number
  profit_pct: number
  orders: number
  orders_delta: number
  cancels: number
  cancels_delta: number
  cancel_rate: number
  cancel_rate_delta: number
}

export interface WbKpiAdvertising {
  ad_spend: number
  ad_spend_delta: number
  drr_total: number
  drr_total_delta: number
  drr_ad: number
  drr_ad_delta: number
}

export interface WbKpiFunnel {
  views: number
  views_delta: number
  clicks: number
  clicks_delta: number
  ctr: number
  ctr_delta: number
  cart: number
  cart_delta: number
  click_to_cart: number
  click_to_cart_delta: number
  cart_conversion: number
  cart_conversion_delta: number
  orders: number
  orders_delta: number
  conversion: number
  conversion_delta: number
}

export interface WbChartPoint {
  date: string
  revenue: number
  orders: number
  ad_spend: number
  views: number
  clicks: number
  cart: number
  ad_orders: number
  drr_ad: number
  drr_total: number
  ctr: number
}

// ── Alert types ──

export interface WbAlertWarehouse {
  nm_id: number
  name: string
  vendor_code: string
  image_url: string
  stock: number
  avg_daily_sales: number
  days_left: number | null
  severity: 'critical' | 'warning'
}

export interface WbAlertSales {
  nm_id: number
  name: string
  vendor_code: string
  image_url: string
  stock: number
  last_sale_date: string | null
  days_without_sales: number
}

export interface WbAlertStorage {
  nm_id: number
  name: string
  vendor_code: string
  image_url: string
  warehouse: string
  cost_7d: number
  volume_liters: number
}

export interface WbAlertAdvertising {
  advert_id: number
  name: string
  problem: 'budget_depleted' | 'spending_no_revenue' | 'no_views' | 'high_drr' | 'low_views' | 'stopped_profitable' | 'clicks_no_orders'
  priority?: number
  status?: 'active' | 'paused' | 'no_budget'
  budget_total?: number | null
  reason?: string
  views?: number
  clicks?: number
  spend?: number
  drr?: number | null
  avg_drr?: number
  orders?: number
  revenue?: number
}

export interface WbAlertFinance {
  vendor_code: string
  nm_id: number
  name: string
  image_url: string
  revenue: number
  expenses: number
  profit: number
  profit_pct: number
  qty: number
  reason?: string
}

export interface WbAlerts {
  warehouses: { count: number; items: WbAlertWarehouse[] }
  sales: { count: number; items: WbAlertSales[] }
  storage: { count: number; total_cost: number; items: WbAlertStorage[] }
  advertising: { count: number; items: WbAlertAdvertising[] }
  finances: { count: number; items: WbAlertFinance[] }
}

export interface WbOrderFeedItem {
  nm_id: number
  name: string
  supplier_article: string
  vendor_code?: string
  orders: number
  revenue: number
  orders_prev: number
  revenue_prev: number
  last_order: string
  image_url: string
}

export interface WbFinanceSummary {
  week_start: string
  week_end: string
  // Revenue
  revenue: number
  revenue_prev: number
  revenue_delta: number
  // Expenses
  commission: number
  commission_prev: number
  logistics: number
  logistics_prev: number
  storage: number
  storage_prev: number
  ad_spend: number
  ad_spend_prev: number
  deductions: number
  deductions_prev: number
  acceptance: number
  acceptance_prev: number
  penalties: number
  penalties_prev: number
  returns: number
  returns_prev: number
  // Orders
  orders: number
  orders_prev: number
  // Profit
  profit: number
  profit_prev: number
  profit_pct: number
  profit_delta: number
}

export interface WbDashboardResponse {
  shop_id: number
  period: string
  date_from: string
  date_to: string
  kpi: {
    sales: WbKpiSales
    advertising: WbKpiAdvertising
    funnel: WbKpiFunnel
  }
  chart: WbChartPoint[]
  alerts: WbAlerts
  orders_feed: WbOrderFeedItem[]
  finance_summary: WbFinanceSummary | null
}

// ═══════════════════════════════════════════════════════════════
// API functions
// ═══════════════════════════════════════════════════════════════

export async function getOzonDashboardApi(
  shopId: number,
  period: string = '7d',
): Promise<WbDashboardResponse> {
  const res = await apiClient.get<WbDashboardResponse>('/dashboard/ozon', {
    params: { shop_id: shopId, period },
  })
  return res.data
}

export async function getWbDashboardApi(
  shopId: number,
  period: string = '7d',
): Promise<WbDashboardResponse> {
  const res = await apiClient.get<WbDashboardResponse>('/dashboard/wb', {
    params: { shop_id: shopId, period },
  })
  return res.data
}
