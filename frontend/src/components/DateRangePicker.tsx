/**
 * PeriodSelector — единый виджет выбора периода.
 * Содержит кнопки быстрого выбора (7д / 30д) и календарь произвольного диапазона.
 * Всё в одном стилизованном блоке.
 */
import { useState, useRef, useEffect } from 'react'
import { DayPicker, type DateRange } from 'react-day-picker'
import { ru } from 'date-fns/locale/ru'
import { CalendarDays, Check, X } from 'lucide-react'
import { cn } from '@/lib/utils'

export type { DateRange }

export interface PeriodValue {
  mode: 'quick' | 'custom'
  period: 7 | 30          // используется при mode='quick'
  dateRange: DateRange | null  // используется при mode='custom'
}

interface PeriodSelectorProps {
  value: PeriodValue
  onChange: (v: PeriodValue) => void
  className?: string
}

function fmt(d: Date) {
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })
}
function isSameDay(a: Date, b: Date) {
  return a.toDateString() === b.toDateString()
}

export function PeriodSelector({ value, onChange, className }: PeriodSelectorProps) {
  const [calOpen, setCalOpen] = useState(false)
  const [draft, setDraft] = useState<DateRange | undefined>(undefined)
  const popRef = useRef<HTMLDivElement>(null)

  // Закрыть попап при клике снаружи
  useEffect(() => {
    if (!calOpen) return
    const h = (e: MouseEvent) => {
      if (popRef.current && !popRef.current.contains(e.target as Node)) {
        setCalOpen(false)
        setDraft(undefined)
      }
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [calOpen])

  const openCalendar = () => {
    setDraft(value.dateRange ?? undefined)
    setCalOpen(true)
  }

  const applyCustom = () => {
    if (!draft?.from) return
    onChange({
      mode: 'custom',
      period: value.period,
      dateRange: { from: draft.from, to: draft.to ?? draft.from },
    })
    setCalOpen(false)
    setDraft(undefined)
  }

  const clearCustom = () => {
    onChange({ mode: 'quick', period: value.period, dateRange: null })
    setCalOpen(false)
    setDraft(undefined)
  }

  // Подсказка для черновика
  const hint = !draft?.from
    ? 'Выберите начальную дату'
    : !draft.to || isSameDay(draft.from, draft.to)
      ? `${fmt(draft.from)} — один день`
      : `${fmt(draft.from)} — ${fmt(draft.to)}`

  // Лейбл кнопки «Даты»
  const customLabel = value.mode === 'custom' && value.dateRange?.from
    ? value.dateRange.to && !isSameDay(value.dateRange.from, value.dateRange.to)
      ? `${fmt(value.dateRange.from)} — ${fmt(value.dateRange.to)}`
      : fmt(value.dateRange.from)
    : null

  return (
    <div className={cn('relative', className)} ref={popRef}>
      {/* ── Единый блок с кнопками ── */}
      <div className="inline-flex items-center rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-1 gap-0.5">
        {/* 7 дней */}
        <button
          onClick={() => { onChange({ mode: 'quick', period: 7, dateRange: null }); setCalOpen(false) }}
          className={cn(
            'rounded-lg px-4 py-1.5 text-sm font-medium transition-all duration-200',
            value.mode === 'quick' && value.period === 7
              ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
              : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-white/5',
          )}
        >
          7 дней
        </button>

        {/* 30 дней */}
        <button
          onClick={() => { onChange({ mode: 'quick', period: 30, dateRange: null }); setCalOpen(false) }}
          className={cn(
            'rounded-lg px-4 py-1.5 text-sm font-medium transition-all duration-200',
            value.mode === 'quick' && value.period === 30
              ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
              : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-white/5',
          )}
        >
          30 дней
        </button>

        {/* Разделитель */}
        <div className="h-5 w-px bg-[hsl(var(--border))] mx-0.5" />

        {/* Кнопка произвольных дат */}
        <button
          onClick={openCalendar}
          className={cn(
            'inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium transition-all duration-200',
            value.mode === 'custom'
              ? 'bg-[hsl(var(--primary))] text-white shadow-sm'
              : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-white/5',
            calOpen && value.mode !== 'custom' && 'bg-white/5 text-[hsl(var(--foreground))]',
          )}
        >
          <CalendarDays className="h-3.5 w-3.5 shrink-0" />
          <span>{customLabel ?? 'Даты'}</span>
        </button>

        {/* Крестик сброса кастомного диапазона */}
        {value.mode === 'custom' && (
          <button
            onClick={clearCustom}
            className="rounded-lg p-1.5 text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-white/5 transition-colors"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        )}
      </div>

      {/* ── Попап календаря ── */}
      {calOpen && (
        <div
          className="absolute left-0 top-full z-50 mt-2 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-2xl p-4"
          style={{ animation: 'dpPop 160ms ease-out' }}
        >
          <style>{`
            .rdp-root { --rdp-accent-color: hsl(var(--primary)); font-family: inherit; }
            .rdp-months { display: flex; gap: 24px; }
            .rdp-month { }
            .rdp-month_caption { display: flex; align-items: center; justify-content: center; padding-bottom: 10px; }
            .rdp-caption_label { font-size: 13px; font-weight: 600; color: hsl(var(--foreground)); text-transform: capitalize; }
            .rdp-nav { display: flex; align-items: center; gap: 4px; }
            .rdp-button_previous, .rdp-button_next {
              width: 28px; height: 28px; border-radius: 8px; display: flex; align-items: center; justify-content: center;
              color: hsl(var(--muted-foreground)); background: transparent; border: none; cursor: pointer;
              transition: background 150ms, color 150ms;
            }
            .rdp-button_previous:hover, .rdp-button_next:hover { background: rgba(255,255,255,0.08); color: hsl(var(--foreground)); }
            .rdp-weekdays { display: flex; }
            .rdp-weekday { width: 36px; text-align: center; font-size: 11px; font-weight: 500; color: hsl(var(--muted-foreground) / 0.45); padding-bottom: 4px; }
            .rdp-weeks { }
            .rdp-week { display: flex; margin-top: 2px; }
            .rdp-day { position: relative; padding: 0; }
            .rdp-day_button {
              width: 36px; height: 36px; border-radius: 8px; border: none; background: transparent; cursor: pointer;
              font-size: 13px; font-weight: 500; color: hsl(var(--foreground) / 0.85);
              transition: background 120ms, color 120ms;
              display: flex; align-items: center; justify-content: center;
            }
            .rdp-day_button:hover { background: rgba(255,255,255,0.08); }
            .rdp-day_button:focus-visible { outline: 2px solid hsl(var(--primary) / 0.5); outline-offset: 1px; }
            /* Selected single */
            .rdp-selected .rdp-day_button { background: hsl(var(--primary)) !important; color: white !important; }
            /* Range start */
            .rdp-range_start .rdp-day_button { background: hsl(var(--primary)) !important; color: white !important; border-radius: 8px 0 0 8px; }
            /* Range end */
            .rdp-range_end .rdp-day_button { background: hsl(var(--primary)) !important; color: white !important; border-radius: 0 8px 8px 0; }
            /* Range middle */
            .rdp-range_middle .rdp-day_button { background: hsl(var(--primary) / 0.15) !important; color: hsl(var(--primary)) !important; border-radius: 0; }
            /* Range start+end same day (single) */
            .rdp-range_start.rdp-range_end .rdp-day_button { border-radius: 8px !important; }
            /* Today */
            .rdp-today .rdp-day_button { font-weight: 700; text-decoration: underline; text-underline-offset: 2px; text-decoration-style: dotted; }
            /* Outside month */
            .rdp-outside { opacity: 0; pointer-events: none; }
            /* Disabled */
            .rdp-disabled .rdp-day_button { opacity: 0.2; cursor: not-allowed; }
            @keyframes dpPop { from { opacity:0; transform:translateY(-4px) scale(.98) } to { opacity:1; transform:none } }
          `}</style>

          <DayPicker
            mode="range"
            selected={draft}
            onSelect={setDraft}
            locale={ru}
            numberOfMonths={2}
            showOutsideDays={false}
            disabled={{ after: new Date() }}
            defaultMonth={
              draft?.from
                ? new Date(draft.from.getFullYear(), draft.from.getMonth() - 1)
                : new Date(new Date().getFullYear(), new Date().getMonth() - 1)
            }
          />

          {/* Подсказка + кнопки */}
          <div className="mt-3 pt-3 border-t border-[hsl(var(--border)/0.4)] flex items-center justify-between gap-3">
            <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.55)]">{hint}</span>
            <div className="flex items-center gap-2">
              <button
                onClick={() => { setCalOpen(false); setDraft(undefined) }}
                className="rounded-lg border border-[hsl(var(--border))] px-3 py-1.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
              >
                Отмена
              </button>
              <button
                onClick={applyCustom}
                disabled={!draft?.from}
                className={cn(
                  'inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-[12px] font-semibold transition-all',
                  draft?.from
                    ? 'bg-[hsl(var(--primary))] text-white hover:opacity-90'
                    : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground)/0.35)] cursor-not-allowed',
                )}
              >
                <Check className="h-3.5 w-3.5" />
                Применить
              </button>
            </div>
          </div>
        </div>
      )}

    </div>
  )
}
