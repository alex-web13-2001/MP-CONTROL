import { useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import {
  LayoutDashboard,
  ShoppingCart,
  Warehouse,
  DollarSign,
  Megaphone,
  Activity,
  Settings,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  BarChart3,
  Package,
  TrendingUp,
  Grid3X3,
  Sparkles,
  type LucideIcon,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useAppStore } from '@/stores/appStore'

interface NavChild {
  path: string
  label: string
  icon: LucideIcon
}

interface NavItem {
  path: string
  label: string
  icon: LucideIcon
  children?: NavChild[]
}

interface NavSection {
  title: string
  items: NavItem[]
}

const navSections: NavSection[] = [
  {
    title: 'Аналитика',
    items: [
      { path: '/', label: 'Обзор', icon: LayoutDashboard },
      { path: '/products', label: 'Товары', icon: Package },
      {
        path: '/sales',
        label: 'Продажи',
        icon: ShoppingCart,
        children: [
          { path: '/sales', label: 'Обзор продаж', icon: TrendingUp },
          { path: '/sales/abc-xyz', label: 'ABC/XYZ анализ', icon: Grid3X3 },
          { path: '/sales/forecast', label: 'Прогноз', icon: Sparkles },
        ],
      },
      { path: '/funnel', label: 'Воронка', icon: BarChart3 },
      { path: '/warehouses', label: 'Склады', icon: Warehouse },
      { path: '/finances', label: 'Финансы', icon: DollarSign },
    ],
  },
  {
    title: 'Управление',
    items: [
      { path: '/advertising', label: 'Реклама', icon: Megaphone },
      { path: '/events', label: 'События', icon: Activity },
    ],
  },
  {
    title: 'Система',
    items: [
      { path: '/settings', label: 'Настройки', icon: Settings },
    ],
  },
]

/* ── Shared link styles ── */
const linkBase = 'group relative flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-medium transition-all duration-200'
const linkActive = 'bg-[hsl(var(--sidebar-accent)/0.12)] text-[hsl(var(--sidebar-accent))]'
const linkInactive = 'text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--sidebar-muted)/0.5)] hover:text-[hsl(var(--sidebar-foreground))]'

/* ── Active indicator bar ── */
function ActiveBar() {
  return (
    <motion.div
      layoutId="activeNav"
      className="absolute left-0 top-1/2 h-5 w-[3px] -translate-y-1/2 rounded-r-full bg-[hsl(var(--sidebar-accent))]"
      transition={{ duration: 0.2, ease: 'easeInOut' }}
    />
  )
}

/* ── Animated label (hides when sidebar collapsed) ── */
function AnimatedLabel({ label, collapsed }: { label: string; collapsed: boolean }) {
  return (
    <AnimatePresence>
      {!collapsed && (
        <motion.span
          initial={{ opacity: 0, width: 0 }}
          animate={{ opacity: 1, width: 'auto' }}
          exit={{ opacity: 0, width: 0 }}
          transition={{ duration: 0.15 }}
          className="overflow-hidden whitespace-nowrap"
        >
          {label}
        </motion.span>
      )}
    </AnimatePresence>
  )
}

export function Sidebar() {
  const collapsed = useAppStore((s) => s.sidebarCollapsed)
  const toggleSidebar = useAppStore((s) => s.toggleSidebar)
  const location = useLocation()

  /* Track which parent groups are expanded */
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  /* Auto-expand group if current path matches */
  const isGroupActive = (item: NavItem) =>
    item.path === '/'
      ? location.pathname === '/'
      : location.pathname.startsWith(item.path)

  const toggleExpand = (path: string) => {
    setExpanded((prev) => ({ ...prev, [path]: !prev[path] }))
  }

  const isExpanded = (item: NavItem) =>
    expanded[item.path] !== undefined ? expanded[item.path] : isGroupActive(item)

  return (
    <motion.aside
      initial={false}
      animate={{ width: collapsed ? 72 : 256 }}
      transition={{ duration: 0.2, ease: 'easeInOut' }}
      className="fixed left-0 top-0 z-40 flex h-screen flex-col border-r border-[hsl(var(--sidebar-border))] bg-[hsl(var(--sidebar))]"
    >
      {/* ── Logo ── */}
      <div className="flex h-16 items-center gap-3 px-5 border-b border-[hsl(var(--sidebar-border))]">
        <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-[hsl(var(--primary))] to-[hsl(245_80%_70%)] text-white font-bold text-sm shadow-md shadow-[hsl(var(--primary)/0.25)]">
          MP
        </div>
        <AnimatePresence>
          {!collapsed && (
            <motion.div
              initial={{ opacity: 0, width: 0 }}
              animate={{ opacity: 1, width: 'auto' }}
              exit={{ opacity: 0, width: 0 }}
              transition={{ duration: 0.15 }}
              className="overflow-hidden"
            >
              <span className="whitespace-nowrap text-base font-bold text-[hsl(var(--sidebar-foreground))]">
                MP-Control
              </span>
              <span className="block whitespace-nowrap text-[10px] font-medium uppercase tracking-widest text-[hsl(var(--muted-foreground))]">
                Analytics
              </span>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* ── Navigation ── */}
      <nav className="flex-1 overflow-y-auto py-4 px-3 space-y-6">
        {navSections.map((section) => (
          <div key={section.title}>
            {/* Section Title */}
            <AnimatePresence>
              {!collapsed && (
                <motion.p
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  className="mb-2 px-3 text-[10px] font-semibold uppercase tracking-widest text-[hsl(var(--muted-foreground)/0.6)]"
                >
                  {section.title}
                </motion.p>
              )}
            </AnimatePresence>

            <div className="space-y-0.5">
              {section.items.map((item) => {
                const hasChildren = item.children && item.children.length > 0
                const groupActive = isGroupActive(item)

                /* ── Item WITH children (collapsible group) ── */
                if (hasChildren) {
                  const open = isExpanded(item)

                  return (
                    <div key={item.path}>
                      {/* Parent button */}
                      <button
                        onClick={() => toggleExpand(item.path)}
                        className={cn(
                          linkBase, 'w-full',
                          groupActive ? linkActive : linkInactive,
                        )}
                      >
                        {groupActive && <ActiveBar />}

                        <item.icon className={cn(
                          'h-[18px] w-[18px] shrink-0 transition-colors',
                          groupActive
                            ? 'text-[hsl(var(--sidebar-accent))]'
                            : 'text-[hsl(var(--muted-foreground))] group-hover:text-[hsl(var(--sidebar-foreground))]',
                        )} />

                        <AnimatedLabel label={item.label} collapsed={collapsed} />

                        {/* Chevron */}
                        {!collapsed && (
                          <motion.div
                            animate={{ rotate: open ? 180 : 0 }}
                            transition={{ duration: 0.2 }}
                            className="ml-auto"
                          >
                            <ChevronDown className="h-3.5 w-3.5 text-[hsl(var(--muted-foreground))]" />
                          </motion.div>
                        )}
                      </button>

                      {/* Children */}
                      <AnimatePresence initial={false}>
                        {open && !collapsed && (
                          <motion.div
                            initial={{ height: 0, opacity: 0 }}
                            animate={{ height: 'auto', opacity: 1 }}
                            exit={{ height: 0, opacity: 0 }}
                            transition={{ duration: 0.2, ease: 'easeInOut' }}
                            className="overflow-hidden"
                          >
                            <div className="ml-4 mt-0.5 space-y-0.5 border-l border-[hsl(var(--sidebar-border))] pl-3">
                              {item.children!.map((child) => {
                                const childActive =
                                  child.path === '/sales'
                                    ? location.pathname === '/sales'
                                    : location.pathname === child.path

                                return (
                                  <NavLink
                                    key={child.path}
                                    to={child.path}
                                    className={cn(
                                      'flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-[13px] font-medium transition-all duration-200',
                                      childActive
                                        ? 'bg-[hsl(var(--sidebar-accent)/0.1)] text-[hsl(var(--sidebar-accent))]'
                                        : 'text-[hsl(var(--muted-foreground))] hover:bg-[hsl(var(--sidebar-muted)/0.5)] hover:text-[hsl(var(--sidebar-foreground))]',
                                    )}
                                  >
                                    <child.icon className={cn(
                                      'h-4 w-4 shrink-0',
                                      childActive
                                        ? 'text-[hsl(var(--sidebar-accent))]'
                                        : 'text-[hsl(var(--muted-foreground))]',
                                    )} />
                                    {child.label}
                                  </NavLink>
                                )
                              })}
                            </div>
                          </motion.div>
                        )}
                      </AnimatePresence>
                    </div>
                  )
                }

                /* ── Simple item (no children) ── */
                const isActive = item.path === '/'
                  ? location.pathname === '/'
                  : location.pathname.startsWith(item.path)

                return (
                  <NavLink
                    key={item.path}
                    to={item.path}
                    className={cn(linkBase, isActive ? linkActive : linkInactive)}
                  >
                    {isActive && <ActiveBar />}

                    <item.icon className={cn(
                      'h-[18px] w-[18px] shrink-0 transition-colors',
                      isActive
                        ? 'text-[hsl(var(--sidebar-accent))]'
                        : 'text-[hsl(var(--muted-foreground))] group-hover:text-[hsl(var(--sidebar-foreground))]',
                    )} />

                    <AnimatedLabel label={item.label} collapsed={collapsed} />
                  </NavLink>
                )
              })}
            </div>
          </div>
        ))}
      </nav>

      {/* ── Collapse Button ── */}
      <div className="border-t border-[hsl(var(--sidebar-border))] p-3">
        <button
          onClick={toggleSidebar}
          className="flex w-full items-center justify-center rounded-xl p-2.5 text-[hsl(var(--muted-foreground))] transition-all duration-200 hover:bg-[hsl(var(--sidebar-muted)/0.5)] hover:text-[hsl(var(--sidebar-foreground))]"
        >
          {collapsed ? (
            <ChevronRight className="h-4 w-4" />
          ) : (
            <ChevronLeft className="h-4 w-4" />
          )}
        </button>
      </div>
    </motion.aside>
  )
}
