/**
 * Events API client — Лента событий.
 */
import { apiClient } from './client'

// ── Types ────────────────────────────────────────────────────

export interface EventProduct {
  nm_id: number
  name: string
  offer_id: string
  image_url: string
}

export interface EventItem {
  id: number
  created_at: string
  event_type: string
  category: 'advertising' | 'content' | 'commercial' | 'other'
  label: string
  detail: string
  advert_id: number | null
  campaign_title: string
  old_value: string | null
  new_value: string | null
  product: EventProduct | null
}

export interface EventDay {
  date: string
  events: EventItem[]
}

export interface EventsFeedResponse {
  shop_id: number
  marketplace: string
  total: number
  page: number
  page_size: number
  days: EventDay[]
}

// ── API ──────────────────────────────────────────────────────

export async function getEventsFeedApi(params: {
  shop_id: number
  period?: string
  event_types?: string
  category?: string
  page?: number
  page_size?: number
}): Promise<EventsFeedResponse> {
  const { data } = await apiClient.get<EventsFeedResponse>('/events/feed', { params })
  return data
}
