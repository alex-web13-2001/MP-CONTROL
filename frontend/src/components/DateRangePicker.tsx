import { useState, useRef, useEffect } from 'react'
import { ChevronLeft, ChevronRight, CalendarDays } from 'lucide-react'

const MONTHS_RU = [
  'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
  'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь',
]
const DAYS_SHORT = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']

function getDaysInMonth(year: number, month: number) {
  return new Date(year, month + 1, 0).getDate()
}
function getFirstDayOfWeek(year: number, month: number) {
  const d = new Date(year, month, 1).getDay()
  return d === 0 ? 6 : d - 1 // Monday = 0
}
function pad(n: number) { return String(n).padStart(2, '0') }
function toDateStr(y: number, m: number, d: number) { return `${y}-${pad(m + 1)}-${pad(d)}` }
function formatDisplay(dateStr: string) {
  if (!dateStr) return '—'
  const [y, m, d] = dateStr.split('-')
  return `${d}.${m}.${y}`
}

interface Props {
  from: string // yyyy-MM-dd
  to: string
  onChange: (from: string, to: string) => void
}

export function DateRangePicker({ from, to, onChange }: Props) {
  const [open, setOpen] = useState(false)
  const [selecting, setSelecting] = useState<'from' | 'to'>('from')
  const ref = useRef<HTMLDivElement>(null)

  // Calendar state — derive from the active date
  const activeDate = selecting === 'from' ? from : to
  const initDate = activeDate ? new Date(activeDate) : new Date()
  const [viewYear, setViewYear] = useState(initDate.getFullYear())
  const [viewMonth, setViewMonth] = useState(initDate.getMonth())

  // Update view when switching selecting mode
  useEffect(() => {
    const d = selecting === 'from' ? from : to
    if (d) {
      const dt = new Date(d)
      setViewYear(dt.getFullYear())
      setViewMonth(dt.getMonth())
    }
  }, [selecting, from, to])

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    if (open) document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const prevMonth = () => {
    if (viewMonth === 0) { setViewMonth(11); setViewYear(y => y - 1) }
    else setViewMonth(m => m - 1)
  }
  const nextMonth = () => {
    if (viewMonth === 11) { setViewMonth(0); setViewYear(y => y + 1) }
    else setViewMonth(m => m + 1)
  }

  const handleDayClick = (day: number) => {
    const dateStr = toDateStr(viewYear, viewMonth, day)
    if (selecting === 'from') {
      if (to && dateStr > to) {
        onChange(dateStr, dateStr)
      } else {
        onChange(dateStr, to)
      }
      setSelecting('to')
    } else {
      if (from && dateStr < from) {
        onChange(dateStr, dateStr)
      } else {
        onChange(from, dateStr)
      }
      setOpen(false)
      setSelecting('from')
    }
  }

  const daysInMonth = getDaysInMonth(viewYear, viewMonth)
  const firstDay = getFirstDayOfWeek(viewYear, viewMonth)
  const today = new Date()
  const todayStr = toDateStr(today.getFullYear(), today.getMonth(), today.getDate())

  const isInRange = (dateStr: string) => from && to && dateStr >= from && dateStr <= to
  const isStart = (dateStr: string) => dateStr === from
  const isEnd = (dateStr: string) => dateStr === to

  return (
    <div className="relative" ref={ref}>
      {/* Trigger buttons */}
      <div className="flex items-center gap-1.5">
        <CalendarDays className="w-4 h-4 text-[hsl(var(--muted-foreground)/0.5)]" />
        <button
          onClick={() => { setSelecting('from'); setOpen(true) }}
          className={`px-2.5 py-1.5 text-[13px] font-medium rounded-lg border transition-all ${
            open && selecting === 'from'
              ? 'border-[hsl(var(--primary))] bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))]'
              : 'border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.15)] text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.3)]'
          }`}
        >
          {formatDisplay(from)}
        </button>
        <span className="text-[12px] text-[hsl(var(--muted-foreground)/0.3)]">—</span>
        <button
          onClick={() => { setSelecting('to'); setOpen(true) }}
          className={`px-2.5 py-1.5 text-[13px] font-medium rounded-lg border transition-all ${
            open && selecting === 'to'
              ? 'border-[hsl(var(--primary))] bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))]'
              : 'border-[hsl(var(--border))] bg-[hsl(var(--muted)/0.15)] text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted)/0.3)]'
          }`}
        >
          {formatDisplay(to)}
        </button>
      </div>

      {/* Calendar dropdown */}
      {open && (
        <div className="absolute top-full left-0 mt-2 z-[60] animate-in fade-in-0 zoom-in-95 duration-150">
          <div className="bg-[hsl(var(--popover))] border border-[hsl(var(--border))] rounded-xl shadow-2xl p-4 w-[280px]">
            {/* Month navigation */}
            <div className="flex items-center justify-between mb-3">
              <button onClick={prevMonth} className="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] transition-colors">
                <ChevronLeft className="w-4 h-4" />
              </button>
              <span className="text-[14px] font-semibold text-[hsl(var(--foreground))]">
                {MONTHS_RU[viewMonth]} {viewYear}
              </span>
              <button onClick={nextMonth} className="w-7 h-7 flex items-center justify-center rounded-lg hover:bg-[hsl(var(--muted)/0.3)] text-[hsl(var(--muted-foreground))] transition-colors">
                <ChevronRight className="w-4 h-4" />
              </button>
            </div>

            {/* Selecting hint */}
            <div className="text-center text-[11px] text-[hsl(var(--muted-foreground)/0.5)] mb-2">
              {selecting === 'from' ? 'Выберите начало периода' : 'Выберите конец периода'}
            </div>

            {/* Day names */}
            <div className="grid grid-cols-7 gap-0 mb-1">
              {DAYS_SHORT.map(d => (
                <div key={d} className="text-center text-[11px] font-medium text-[hsl(var(--muted-foreground)/0.5)] py-1">{d}</div>
              ))}
            </div>

            {/* Days grid */}
            <div className="grid grid-cols-7 gap-0">
              {Array.from({ length: firstDay }).map((_, i) => (
                <div key={`e-${i}`} className="h-8" />
              ))}
              {Array.from({ length: daysInMonth }).map((_, i) => {
                const day = i + 1
                const dateStr = toDateStr(viewYear, viewMonth, day)
                const inRange = isInRange(dateStr)
                const start = isStart(dateStr)
                const end = isEnd(dateStr)
                const isToday = dateStr === todayStr
                const isFuture = dateStr > todayStr

                return (
                  <button
                    key={day}
                    onClick={() => handleDayClick(day)}
                    disabled={isFuture}
                    className={`
                      h-8 text-[13px] font-medium rounded-lg transition-all relative
                      ${isFuture ? 'text-[hsl(var(--muted-foreground)/0.2)] cursor-not-allowed' : 'hover:bg-[hsl(var(--muted)/0.3)] cursor-pointer'}
                      ${start || end ? 'bg-[hsl(var(--primary))] text-white hover:bg-[hsl(var(--primary)/0.9)]' : ''}
                      ${inRange && !start && !end ? 'bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))]' : ''}
                      ${!inRange && !start && !end && !isFuture ? 'text-[hsl(var(--foreground))]' : ''}
                    `}
                  >
                    {day}
                    {isToday && !start && !end && (
                      <span className="absolute bottom-0.5 left-1/2 -translate-x-1/2 w-1 h-1 rounded-full bg-[hsl(var(--primary))]" />
                    )}
                  </button>
                )
              })}
            </div>

            {/* Footer */}
            <div className="mt-3 pt-2 border-t border-[hsl(var(--border)/0.3)] flex items-center justify-between">
              <span className="text-[11px] text-[hsl(var(--muted-foreground)/0.4)]">
                {formatDisplay(from)} — {formatDisplay(to)}
              </span>
              <button
                onClick={() => setOpen(false)}
                className="text-[12px] font-medium px-2.5 py-1 rounded-md bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))] hover:bg-[hsl(var(--primary)/0.2)] transition-colors"
              >
                Готово
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
