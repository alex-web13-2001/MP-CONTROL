/**
 * DateRangePicker — выбор диапазона дат (или одного дня).
 * Двухшаговый выбор: 1й клик = начало, 2й клик = конец.
 * Применяется только по кнопке «Применить» или двойному клику на одну дату.
 */
import { useState, useRef, useEffect } from 'react'
import { DayPicker, type DateRange } from 'react-day-picker'
import { ru } from 'date-fns/locale/ru'
import { CalendarDays, X, Check } from 'lucide-react'
import { cn } from '@/lib/utils'

export type { DateRange }

interface DateRangePickerProps {
  value: DateRange | null
  onChange: (range: DateRange | null) => void
  className?: string
}

function fmt(d: Date): string {
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })
}

function isSameDay(a: Date, b: Date) {
  return a.getFullYear() === b.getFullYear() &&
    a.getMonth() === b.getMonth() &&
    a.getDate() === b.getDate()
}

export function DateRangePicker({ value, onChange, className }: DateRangePickerProps) {
  const [open, setOpen] = useState(false)
  const [draft, setDraft] = useState<DateRange | undefined>(undefined)
  const wrapRef = useRef<HTMLDivElement>(null)

  // Close on outside click — without applying (discard draft)
  useEffect(() => {
    if (!open) return
    const h = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setOpen(false)
        setDraft(undefined)
      }
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [open])

  const handleOpen = () => {
    setDraft(value ?? undefined)
    setOpen(true)
  }

  const handleSelect = (range: DateRange | undefined) => {
    if (!range) { setDraft(undefined); return }

    // react-day-picker v9 в режиме range при клике на ту же дату что уже стоит from
    // может сбросить selection. Просто сохраняем черновик без немедленного применения.
    setDraft(range)
  }

  const apply = () => {
    if (!draft?.from) return
    // Если выбрана только начальная дата — одиночный день (from = to)
    const result: DateRange = {
      from: draft.from,
      to: draft.to ?? draft.from,
    }
    onChange(result)
    setOpen(false)
    setDraft(undefined)
  }

  const clear = (e: React.MouseEvent) => {
    e.stopPropagation()
    onChange(null)
    setDraft(undefined)
  }

  const hasValue = !!(value?.from)
  const label = hasValue
    ? value!.to && !isSameDay(value!.from!, value!.to)
      ? `${fmt(value!.from!)} — ${fmt(value!.to)}`
      : fmt(value!.from!)
    : 'Даты'

  // Описание состояния черновика для подсказки пользователю
  const hint = !draft?.from
    ? 'Выберите начальную дату'
    : !draft?.to
      ? 'Теперь выберите конечную дату'
      : isSameDay(draft.from, draft.to)
        ? `${fmt(draft.from)} — один день`
        : `${fmt(draft.from)} — ${fmt(draft.to)}`

  const canApply = !!draft?.from

  return (
    <div className={cn('relative', className)} ref={wrapRef}>
      {/* ── Trigger ── */}
      <button
        onClick={handleOpen}
        className={cn(
          'inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-sm font-medium transition-all',
          hasValue
            ? 'border-[hsl(var(--primary)/0.4)] bg-[hsl(var(--primary)/0.08)] text-[hsl(var(--primary))]'
            : 'border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]',
          open && 'ring-2 ring-[hsl(var(--primary)/0.2)]',
        )}
      >
        <CalendarDays className="h-4 w-4 shrink-0" />
        <span>{label}</span>
        {hasValue && (
          <span role="button" onClick={clear}
            className="ml-0.5 rounded p-0.5 hover:bg-white/10 transition-colors"
          >
            <X className="h-3 w-3" />
          </span>
        )}
      </button>

      {/* ── Popover ── */}
      {open && (
        <div
          className={cn(
            'absolute left-0 top-full z-50 mt-2',
            'rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-2xl',
            'p-4',
          )}
          style={{ animation: 'dpPop 180ms ease-out' }}
        >
          <DayPicker
            mode="range"
            selected={draft}
            onSelect={handleSelect}
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
              month: 'space-y-2',
              month_caption: 'flex items-center justify-center pb-2',
              caption_label: 'text-[13px] font-semibold text-[hsl(var(--foreground))] capitalize',
              nav: 'flex items-center gap-1',
              button_previous: [
                'h-7 w-7 rounded-lg flex items-center justify-center transition-colors',
                'text-[hsl(var(--muted-foreground))] hover:bg-white/8 hover:text-[hsl(var(--foreground))]',
              ].join(' '),
              button_next: [
                'h-7 w-7 rounded-lg flex items-center justify-center transition-colors',
                'text-[hsl(var(--muted-foreground))] hover:bg-white/8 hover:text-[hsl(var(--foreground))]',
              ].join(' '),
              month_grid: 'w-full',
              weekdays: 'flex',
              weekday: 'w-9 text-center text-[11px] font-medium text-[hsl(var(--muted-foreground)/0.45)] pb-1',
              week: 'flex mt-0.5',
              day: 'relative p-0',
              day_button: [
                'h-9 w-9 text-[13px] font-medium transition-colors rounded-lg w-full',
                'hover:bg-white/8 text-[hsl(var(--foreground)/0.85)]',
                'focus:outline-none',
              ].join(' '),
              selected: '!bg-[hsl(var(--primary))] !text-white',
              range_start: '!bg-[hsl(var(--primary))] !text-white !rounded-l-lg !rounded-r-none',
              range_end: '!bg-[hsl(var(--primary))] !text-white !rounded-r-lg !rounded-l-none',
              range_middle: '!bg-[hsl(var(--primary)/0.14)] !text-[hsl(var(--primary))] !rounded-none',
              today: 'font-bold underline decoration-dotted underline-offset-2',
              outside: 'opacity-0 pointer-events-none',
              disabled: '!opacity-20 !cursor-not-allowed',
            }}
          />

          {/* ── Подсказка + кнопки ── */}
          <div className="mt-3 pt-3 border-t border-[hsl(var(--border)/0.5)] flex items-center justify-between gap-3">
            <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.6)]">{hint}</span>
            <div className="flex items-center gap-2">
              <button
                onClick={() => { setOpen(false); setDraft(undefined) }}
                className="rounded-lg border border-[hsl(var(--border))] px-3 py-1.5 text-[12px] font-medium text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
              >
                Отмена
              </button>
              <button
                onClick={apply}
                disabled={!canApply}
                className={cn(
                  'inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-[12px] font-semibold transition-all',
                  canApply
                    ? 'bg-[hsl(var(--primary))] text-white hover:opacity-90'
                    : 'bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground)/0.4)] cursor-not-allowed',
                )}
              >
                <Check className="h-3.5 w-3.5" />
                Применить
              </button>
            </div>
          </div>
        </div>
      )}

      <style>{`@keyframes dpPop { from { opacity:0; transform:translateY(-6px) scale(.97) } to { opacity:1; transform:none } }`}</style>
    </div>
  )
}
