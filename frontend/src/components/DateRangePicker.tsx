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
            classNames={{
              months: 'flex gap-6',
              month: 'space-y-1',
              month_caption: 'flex items-center justify-center pb-2',
              caption_label: 'text-[13px] font-semibold text-[hsl(var(--foreground))] capitalize',
              nav: 'flex items-center gap-1',
              button_previous: 'h-7 w-7 rounded-lg flex items-center justify-center text-[hsl(var(--muted-foreground))] hover:bg-white/8 hover:text-[hsl(var(--foreground))] transition-colors',
              button_next:     'h-7 w-7 rounded-lg flex items-center justify-center text-[hsl(var(--muted-foreground))] hover:bg-white/8 hover:text-[hsl(var(--foreground))] transition-colors',
              month_grid: 'w-full',
              weekdays: 'flex',
              weekday: 'w-9 text-center text-[11px] font-medium text-[hsl(var(--muted-foreground)/0.4)] pb-1',
              week: 'flex mt-0.5',
              day: 'relative p-0',
              day_button: 'h-9 w-9 text-[13px] font-medium transition-colors rounded-lg w-full hover:bg-white/8 text-[hsl(var(--foreground)/0.85)] focus:outline-none',
              selected: '!bg-[hsl(var(--primary))] !text-white',
              range_start: '!bg-[hsl(var(--primary))] !text-white !rounded-l-lg !rounded-r-none',
              range_end:   '!bg-[hsl(var(--primary))] !text-white !rounded-r-lg !rounded-l-none',
              range_middle: '!bg-[hsl(var(--primary)/0.14)] !text-[hsl(var(--primary))] !rounded-none',
              today: 'font-bold underline decoration-dotted underline-offset-2',
              outside: 'opacity-0 pointer-events-none',
              disabled: '!opacity-20 !cursor-not-allowed',
            }}
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

      <style>{`@keyframes dpPop { from { opacity:0; transform:translateY(-4px) scale(.98) } to { opacity:1; transform:none } }`}</style>
    </div>
  )
}
