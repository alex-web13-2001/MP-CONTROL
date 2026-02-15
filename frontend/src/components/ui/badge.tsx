import * as React from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '@/lib/utils'

const badgeVariants = cva(
  'inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium transition-colors',
  {
    variants: {
      variant: {
        default: 'bg-[hsl(var(--primary)/0.15)] text-[hsl(var(--primary))]',
        success: 'bg-[hsl(var(--success)/0.15)] text-[hsl(var(--success))]',
        warning: 'bg-[hsl(var(--warning)/0.15)] text-[hsl(var(--warning))]',
        destructive: 'bg-[hsl(var(--destructive)/0.15)] text-[hsl(var(--destructive))]',
        secondary: 'bg-[hsl(var(--secondary))] text-[hsl(var(--secondary-foreground))]',
        outline: 'border border-[hsl(var(--border))] text-[hsl(var(--foreground))]',
        wb: 'bg-[var(--color-wb)/0.15] text-[var(--color-wb-light)]',
        ozon: 'bg-[var(--color-ozon)/0.15] text-[var(--color-ozon-light)]',
      },
    },
    defaultVariants: {
      variant: 'default',
    },
  }
)

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return (
    <div className={cn(badgeVariants({ variant }), className)} {...props} />
  )
}

export { Badge, badgeVariants }
