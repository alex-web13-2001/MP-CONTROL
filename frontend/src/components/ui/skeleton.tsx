import { cn } from '@/lib/utils'

function Skeleton({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        'rounded-lg bg-[hsl(var(--muted))]',
        'bg-gradient-to-r from-[hsl(var(--muted))] via-[hsl(var(--muted)/0.5)] to-[hsl(var(--muted))]',
        'bg-[length:200%_100%] animate-[shimmer_1.5s_infinite]',
        className
      )}
      {...props}
    />
  )
}

export { Skeleton }
