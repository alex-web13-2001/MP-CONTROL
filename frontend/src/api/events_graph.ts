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
