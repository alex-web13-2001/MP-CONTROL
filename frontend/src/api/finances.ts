/**
 * Finances API client.
 */
import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────

export interface FinancesKpi {
  revenue: number
  revenue_delta: number
  payout: number
  payout_delta: number
  mp_fees: number
  mp_fees_delta: number
  operating: number
  operating_delta: number
  ad_spend: number
  ad_spend_delta: number
  cogs: number
  cogs_delta: number
  profit: number
  profit_delta: number
  profit_pct: number
  orders: number
  orders_delta: number
}

export interface FinancesBreakdown {
  revenue: number
  commission: number
  logistics: number
  storage: number
  acquiring: number
  advertising: number
  refunds: number
  penalties: number
  deductions: number
  compensation: number
  operating: number
  cogs: number
  profit: number
}

export interface FinancesDailyPoint {
  date: string
  revenue: number
  payout: number
  mp_fees: number
  operating: number
  ad_spend: number
  cogs: number
  orders: number
  profit: number
}

export interface FinancesComparison {
  current: Record<string, number>
  previous: Record<string, number>
  delta_pct: Record<string, number>
}

export interface FinancesResponse {
  shop_id: number
  period: number
  date_from: string
  date_to: string
  prev_date_from: string
  prev_date_to: string
  group_by: string
  kpi: FinancesKpi
  breakdown: FinancesBreakdown
  daily: FinancesDailyPoint[]
  comparison: FinancesComparison
}

// ── API ──────────────────────────────────────────────────────

export async function getOzonFinancesApi(params: {
  shop_id: number
  period?: number
  group_by?: string
  date_from?: string
  date_to?: string
}): Promise<FinancesResponse> {
  const { data } = await apiClient.get<FinancesResponse>('/finances/ozon', { params })
  return data
}

export async function getWbFinancesApi(params: {
  shop_id: number
  period?: number
  group_by?: string
  date_from?: string
  date_to?: string
}): Promise<FinancesResponse> {
  const { data } = await apiClient.get<FinancesResponse>('/finances/wb', { params })
  return data
}

// ── Product-level P&L ────────────────────────────────────────

export interface ProductFinanceItem {
  vendor_code: string
  name?: string
  nm_id?: number
  image_url?: string
  current: Record<string, number>
  previous: Record<string, number>
  delta_pct: Record<string, number>
  pct_of_revenue: Record<string, number>
}

export interface ProductFinanceResponse {
  shop_id: number
  date_from: string
  date_to: string
  products: ProductFinanceItem[]
  totals: {
    current: Record<string, number>
    previous: Record<string, number>
    delta_pct: Record<string, number>
  }
}

export async function getWbProductsFinanceApi(params: {
  shop_id: number
  period?: number
  date_from?: string
  date_to?: string
}): Promise<ProductFinanceResponse> {
  const { data } = await apiClient.get<ProductFinanceResponse>('/finances/wb/products', { params })
  return data
}

export async function getOzonProductsFinanceApi(params: {
  shop_id: number
  period?: number
  date_from?: string
  date_to?: string
}): Promise<ProductFinanceResponse> {
  const { data } = await apiClient.get<ProductFinanceResponse>('/finances/ozon/products', { params })
  return data
}

// ── Weekly Report (Понедельный отчёт) ────────────────────────

export interface OzonWeeklyReportRow {
  year: number
  week: number
  week_start: string
  week_end: string
  qty: number
  sales: number
  returns: number
  commission: number
  compensations: number
  other_services: number
  marketing: number
  other_charges: number
  fbo_services: number
  acquiring: number
  delivery_services: number
  storage: number
  payout: number
  cogs: number
  gross_profit: number
  commission_pct: number
  marketing_pct: number
  fbo_pct: number
  delivery_pct: number
  cogs_pct: number
  gross_profit_pct: number
}

export interface WBWeeklyReportRow {
  year: number
  week: number
  week_start: string
  week_end: string
  qty: number
  revenue: number
  payout: number
  commission: number
  returns_qty: number
  returns_amount: number
  logistics: number
  storage: number
  acquiring: number
  acceptance: number
  deductions: number
  wb_promo: number
  marketing: number
  cogs: number
  gross_profit: number
  commission_pct: number
  logistics_pct: number
  cogs_pct: number
  gross_profit_pct: number
}

export type WeeklyReportRow = OzonWeeklyReportRow | WBWeeklyReportRow

export interface WeeklyReportResponse {
  shop_id: number
  weeks: WeeklyReportRow[]
  totals: Record<string, number>
}

export async function getOzonWeeklyReportApi(params: {
  shop_id: number
}): Promise<WeeklyReportResponse> {
  const { data } = await apiClient.get<WeeklyReportResponse>('/finances/ozon/weekly-report', { params })
  return data
}

export async function getWbWeeklyReportApi(params: {
  shop_id: number
}): Promise<WeeklyReportResponse> {
  const { data } = await apiClient.get<WeeklyReportResponse>('/finances/wb/weekly-report', { params })
  return data
}

// ── Excel Export ──────────────────────────────────────────────

export async function downloadOzonExcelReport(params: {
  shop_id: number
  date_from: string
  date_to: string
}): Promise<void> {
  const response = await apiClient.get('/finances/ozon/excel', {
    params,
    responseType: 'blob',
  })
  const blob = new Blob([response.data], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  })
  const url = window.URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  // Extract filename from Content-Disposition header or use default
  const cd = response.headers['content-disposition']
  const match = cd?.match(/filename="?(.+?)"?$/)
  link.download = match ? match[1] : `Ozon_Finance_${params.date_from}_${params.date_to}.xlsx`
  document.body.appendChild(link)
  link.click()
  link.remove()
  window.URL.revokeObjectURL(url)
}

export async function downloadWbExcelReport(params: {
  shop_id: number
  date_from: string
  date_to: string
}): Promise<void> {
  const response = await apiClient.get('/finances/wb/excel', {
    params,
    responseType: 'blob',
  })
  const blob = new Blob([response.data], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  })
  const url = window.URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  const cd = response.headers['content-disposition']
  const match = cd?.match(/filename="?(.+?)"?$/)
  link.download = match ? match[1] : `WB_Finance_${params.date_from}_${params.date_to}.xlsx`
  document.body.appendChild(link)
  link.click()
  link.remove()
  window.URL.revokeObjectURL(url)
}

