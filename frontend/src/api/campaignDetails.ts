import { apiClient } from './client'

export interface CampaignStatsRow {
  dt: string
  views: number
  clicks: number
  orders: number
  cart: number
  revenue: number
  spend: number
  ctr: number
  drr: number
  product_revenue: number
  direct_revenue: number
  model_revenue: number
  associated_revenue: number
}

export interface CampaignEventRow {
  id: number
  timestamp: string
  event_type: string
  product_id?: string
  product_name?: string
  offer_id?: string
  old_value?: string
  new_value?: string
  event_metadata?: Record<string, unknown>
}

export interface CampaignPhraseRow {
  phrase: string
  views: number
  clicks: number
  ctr: number
  spend: number
  orders: number
  revenue: number
}

export interface CampaignHeatmapRow {
  day_of_week: number
  hour: number
  orders: number
}

export interface CampaignPurchaseRow {
  sku: number
  product_name: string
  offer_id: string
  quantity: number
  revenue: number
  avg_price: number
  sale_type: 'direct' | 'model' | 'associated'
}

export interface KpiPeriod {
  spend: number
  ad_revenue: number
  product_revenue: number  // total product sales (organic+ad) from fact_orders_raw
  orders: number
  cart: number
  clicks: number
  views: number
  ctr: number
  drr_ad: number
  drr_product: number
  cpo: number
  // Breakdown by sale type (ad-attributed)
  direct_revenue: number
  direct_orders: number
  model_revenue: number
  model_orders: number
  associated_revenue: number
  associated_orders: number
}

export interface CampaignKpiResponse {
  current: KpiPeriod
  previous: KpiPeriod
  first_date?: string | null
}

export const getCampaignKpi = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string,
  sku?: number,
  scope?: string
): Promise<CampaignKpiResponse> => {
  const { data } = await apiClient.get<CampaignKpiResponse>(
    `/campaign-details/${marketplace}/${campaignId}/kpi`,
    { params: { start_date: startDate, end_date: endDate, sku, scope } }
  )
  return data
}

export const getCampaignStats = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string,
  sku?: number,
  scope?: string
): Promise<CampaignStatsRow[]> => {
  const { data } = await apiClient.get<CampaignStatsRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/stats`,
    { params: { start_date: startDate, end_date: endDate, sku, scope } }
  )
  return data
}

export const getCampaignEvents = async (
  marketplace: string,
  campaignId: number,
  sku?: number,
  limit: number = 50
): Promise<CampaignEventRow[]> => {
  const { data } = await apiClient.get<CampaignEventRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/events`,
    { params: { sku, limit } }
  )
  return data
}

export const getCampaignPhrases = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string
): Promise<CampaignPhraseRow[]> => {
  const { data } = await apiClient.get<CampaignPhraseRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/phrases`,
    { params: { start_date: startDate, end_date: endDate } }
  )
  return data
}

export const getCampaignHeatmap = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string,
  sku?: number
): Promise<CampaignHeatmapRow[]> => {
  const { data } = await apiClient.get<CampaignHeatmapRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/heatmap`,
    { params: { start_date: startDate, end_date: endDate, sku } }
  )
  return data
}

export const getCampaignPurchases = async (
  marketplace: string,
  campaignId: number,
  startDate: string,
  endDate: string,
  scope?: string
): Promise<CampaignPurchaseRow[]> => {
  const { data } = await apiClient.get<CampaignPurchaseRow[]>(
    `/campaign-details/${marketplace}/${campaignId}/purchases`,
    { params: { start_date: startDate, end_date: endDate, scope } }
  )
  return data
}

// ── AI Campaign Analysis (SSE streaming) ─────────────────────

export async function streamCampaignAiAnalysis(
  params: {
    marketplace: string
    campaignId: number
    startDate: string
    endDate: string
    sku?: number
    previousAnalysis?: string
  },
  onChunk: (text: string) => void,
  onDone: () => void,
  onError: (error: string) => void,
): Promise<AbortController> {
  const controller = new AbortController()

  const baseUrl = import.meta.env.VITE_API_URL || '/api/v1'
  const qs = new URLSearchParams()
  qs.set('start_date', params.startDate)
  qs.set('end_date', params.endDate)
  if (params.sku) qs.set('sku', String(params.sku))

  const { useAuthStore } = await import('@/stores/authStore')
  const token = useAuthStore.getState().token

  // Send previous_analysis in POST body (not query) to avoid HTTP 414
  const postBody = params.previousAnalysis
    ? JSON.stringify({ previous_analysis: params.previousAnalysis })
    : undefined

  try {
    const response = await fetch(
      `${baseUrl}/campaign-details/${params.marketplace}/${params.campaignId}/ai-analysis?${qs.toString()}`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: postBody,
        signal: controller.signal,
      }
    )

    if (!response.ok) {
      onError(`HTTP ${response.status}`)
      return controller
    }

    const reader = response.body?.getReader()
    if (!reader) {
      onError('No response body')
      return controller
    }

    const decoder = new TextDecoder()
    let buffer = ''

    while (true) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop() || ''

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue
        const payload = line.slice(6).trim()
        if (payload === '[DONE]') {
          onDone()
          return controller
        }
        try {
          const data = JSON.parse(payload)
          if (data.error) {
            onError(data.error)
            return controller
          }
          if (data.content) {
            onChunk(data.content)
          }
        } catch {
          // skip malformed JSON
        }
      }
    }

    onDone()
  } catch (err: unknown) {
    if ((err as Error).name !== 'AbortError') {
      onError((err as Error).message || 'Unknown error')
    }
  }

  return controller
}
