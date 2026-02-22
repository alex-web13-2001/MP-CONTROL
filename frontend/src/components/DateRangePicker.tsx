/**
 * DateRangePicker — выбор произвольного диапазона дат.
 * Стилизован под тёмную тему проекта (hsl CSS vars).
 *
 * Использование:
 *   <DateRangePicker value={range} onChange={setRange} />
 */
import { useState, useRef, useEffect } from 'react'
import { DayPicker, type DateRange } from 'react-day-picker'
import { ru } from 'date-fns/locale'
import { CalendarDays, X } from 'lucide-react'
import { cn } from '@/lib/utils'

export type { DateRange }

interface DateRangePickerProps {
  value: DateRange | null
  onChange: (range: DateRange | null) => void
  className?: string
}

function formatDate(d: Date): string {
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: '2-digit' })
}

export function DateRangePicker({ value, onChange, className }: DateRangePickerProps) {
  const [open, setOpen] = useState(false)
  const [selecting, setSelecting] = useState<DateRange | undefined>(value ?? undefined)
  const wrapRef = useRef<HTMLDivElement>(null)

  // Close on outside click
  useEffect(() => {
    if (!open) return
    const h = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [open])

  // Sync external value → internal
  useEffect(() => {
    setSelecting(value ?? undefined)
  }, [value])

  const handleSelect = (range: DateRange | undefined) => {
    setSelecting(range)
    // Apply immediately when both dates selected
    if (range?.from && range?.to) {
      onChange(range)
      setOpen(false)
    }
  }

  const clear = (e: React.MouseEvent) => {
    e.stopPropagation()
    setSelecting(undefined)
    onChange(null)
  }

  const label = value?.from && value?.to
    ? `${formatDate(value.from)} — ${formatDate(value.to)}`
    : 'Выбрать даты'

  const hasValue = !!(value?.from && value?.to)

  return (
    <div className={cn('relative', className)} ref={wrapRef}>
      {/* Trigger button */}
      <button
        onClick={() => setOpen((o) => !o)}
        className={cn(
          'inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-sm font-medium transition-all',
          hasValue
            ? 'border-[hsl(var(--primary)/0.4)] bg-[hsl(var(--primary)/0.08)] text-[hsl(var(--primary))]'
            : 'border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:border-[hsl(var(--border))/0.8]',
          open && 'ring-2 ring-[hsl(var(--primary)/0.2)]',
        )}
      >
        <CalendarDays className="h-4 w-4 shrink-0" />
        <span>{label}</span>
        {hasValue && (
          <span
            role="button"
            onClick={clear}
            className="ml-1 rounded p-0.5 hover:bg-white/10 transition-colors"
          >
            <X className="h-3 w-3" />
          </span>
        )}
      </button>

      {/* Popover */}
      {open && (
        <div
          className={cn(
            'absolute left-0 top-full z-50 mt-2',
            'rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-2xl',
            'animate-[dpPopIn_180ms_ease-out_forwards] p-3',
          )}
        >
          <DayPicker
            mode="range"
            selected={selecting}
            onSelect={handleSelect}
            locale={ru}
            numberOfMonths={2}
            showOutsideDays={false}
            disabled={{ after: new Date() }}
            defaultMonth={
              selecting?.from
                ? new Date(selecting.from.getFullYear(), selecting.from.getMonth() - 1)
                : new Date(new Date().getFullYear(), new Date().getMonth() - 1)
            }
            classNames={{
              months: 'flex gap-4',
              month: 'space-y-2',
              month_caption: 'flex items-center justify-center pt-1 pb-2',
              caption_label: 'text-sm font-semibold text-[hsl(var(--foreground))] capitalize',
              nav: 'flex items-center gap-1',
              button_previous: 'h-7 w-7 rounded-lg hover:bg-white/8 flex items-center justify-center text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors',
              button_next: 'h-7 w-7 rounded-lg hover:bg-white/8 flex items-center justify-center text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors',
              month_grid: 'w-full border-collapse',
              weekdays: 'flex',
              weekday: 'w-9 text-center text-[11px] font-medium text-[hsl(var(--muted-foreground)/0.5)] pb-1',
              week: 'flex mt-1',
              day: 'relative p-0',
              day_button: cn(
                'h-9 w-9 rounded-lg text-[13px] font-medium transition-colors',
                'hover:bg-white/8 text-[hsl(var(--foreground)/0.85)]',
                'focus:outline-none focus:ring-1 focus:ring-[hsl(var(--primary)/0.4)]',
              ),
              selected: '!bg-[hsl(var(--primary))] !text-white rounded-lg',
              range_start: '!bg-[hsl(var(--primary))] !text-white rounded-l-lg',
              range_end: '!bg-[hsl(var(--primary))] !text-white rounded-r-lg',
              range_middle: '!bg-[hsl(var(--primary)/0.12)] !text-[hsl(var(--primary))] rounded-none',
              today: 'font-bold text-[hsl(var(--primary))]',
              outside: 'opacity-0 pointer-events-none',
              disabled: '!opacity-25 !cursor-not-allowed',
            }}
          />

          <style>{`
            @keyframes dpPopIn {
              from { opacity: 0; transform: translateY(-6px) scale(0.97); }
              to   { opacity: 1; transform: translateY(0)   scale(1); }
            }
          `}</style>
        </div>
      )}
    </div>
  )
}
