/**
 * Events Graph API client — События + KPI на временной шкале.
 */
import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────

export interface EventBrief {
  category: string
  text: string
}

export interface EventsGraphPoint {
  date: string
  events_total: number
  events_by_category: Record<string, number>
  events_brief: EventBrief[]
  orders: number
  revenue: number
  views: number
  clicks: number
  carts: number
  ad_spend: number
  ad_orders: number
  drr: number
  cpo: number
}

export interface EventsGraphResponse {
  shop_id: number
  marketplace: string
  group_by: string
  period: string
  date_from: string
  date_to: string
  data: EventsGraphPoint[]
}

// ── API ──────────────────────────────────────────────────────

export async function getEventsGraphApi(params: {
  shop_id: number
  period?: string
  group_by?: string
  date_from?: string
  date_to?: string
}): Promise<EventsGraphResponse> {
  const { data } = await apiClient.get<EventsGraphResponse>('/events/graph', { params })
  return data
}

// ── AI Analysis (SSE streaming) ─────────────────────────────

export async function streamEventsAnalysis(
  params: {
    shop_id: number
    period?: string
    group_by?: string
    date_from?: string
    date_to?: string
  },
  onChunk: (text: string) => void,
  onDone: () => void,
  onError: (error: string) => void,
): Promise<AbortController> {
  const controller = new AbortController()

  // Build URL
  const baseUrl = import.meta.env.VITE_API_URL || '/api/v1'
  const qs = new URLSearchParams()
  qs.set('shop_id', String(params.shop_id))
  if (params.period) qs.set('period', params.period)
  if (params.group_by) qs.set('group_by', params.group_by)
  if (params.date_from) qs.set('date_from', params.date_from)
  if (params.date_to) qs.set('date_to', params.date_to)

  // Get auth token
  const { useAuthStore } = await import('@/stores/authStore')
  const token = useAuthStore.getState().token

  try {
    const response = await fetch(`${baseUrl}/events/analysis?${qs.toString()}`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      signal: controller.signal,
    })

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

