/**
 * CreateCampaignModal — 3-step wizard for creating WB advertising campaigns.
 *
 * Steps:
 * 1. Settings — Campaign name, bid type, payment type, budget
 * 2. Products — Split-panel: catalog (left) + selected products with bids (right)
 * 3. Confirmation — Review & submit
 *
 * On submit: save-ad → deposit budget → set initial bids → optional auto-start
 */
import React, { useState, useEffect, useMemo } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import {
  X, ChevronRight, ChevronLeft, Check, Loader2, Search, Package, Settings,
  AlertTriangle, Zap, DollarSign, Play, Plus, Trash2, ArrowRight,
} from 'lucide-react'
import {
  getCreationSubjects, getCreationProducts, createCampaign,
  type SubjectItem, type ProductItem, type CreateCampaignPayload,
} from '../api/ad-management'

// ── Types ────────────────────────────────────────────────────────

export interface QuickLaunchProduct {
  nm_id: number
  name?: string
  vendor_code?: string
  image_url?: string
}

interface Props {
  shopId: number
  balance: number
  balanceLoading?: boolean
  onClose: () => void
  onSuccess: (advertId: number, campaignStarted: boolean) => void
  /** Pre-selected product for quick launch flow (skip Step 2) */
  initialProduct?: QuickLaunchProduct
}

type Step = 1 | 2 | 3

// ── Helpers ──────────────────────────────────────────────────────

const BID_TYPE_OPTIONS = [
  { value: 'unified' as const, label: 'Единая ставка', desc: 'Автоматическое размещение в поиске и рекомендациях' },
  { value: 'manual' as const, label: 'Ручные ставки', desc: 'Управляйте ставками отдельно по каждому размещению' },
]

const PAYMENT_TYPE_OPTIONS = [
  { value: 'cpm' as const, label: 'CPM', desc: 'Оплата за 1000 показов' },
  { value: 'cpc' as const, label: 'CPC', desc: 'Оплата за клик' },
]

// ── Step Indicator ───────────────────────────────────────────────

function StepIndicator({ current, steps }: { current: Step; steps: { label: string; icon: React.ReactNode }[] }) {
  return (
    <div className="flex items-center gap-1 px-6 pt-5 pb-2">
      {steps.map((s, i) => {
        const stepNum = (i + 1) as Step
        const active = stepNum === current
        const done = stepNum < current
        return (
          <React.Fragment key={i}>
            <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${
              active
                ? 'bg-violet-600 text-white'
                : done
                  ? 'bg-emerald-500/15 text-emerald-600 dark:text-emerald-400'
                  : 'bg-[hsl(var(--muted))] text-[hsl(var(--muted-foreground))]'
            }`}>
              {done ? <Check className="w-3.5 h-3.5" /> : s.icon}
              <span className="hidden sm:inline">{s.label}</span>
            </div>
            {i < steps.length - 1 && (
              <div className={`flex-1 h-px max-w-[40px] ${
                stepNum < current ? 'bg-emerald-500' : 'bg-[hsl(var(--border))]'
              }`} />
            )}
          </React.Fragment>
        )
      })}
    </div>
  )
}

// ══════════════════════════════════════════════════════════════════
// Main Modal Component
// ══════════════════════════════════════════════════════════════════

export default function CreateCampaignModal({ shopId, balance, balanceLoading, onClose, onSuccess, initialProduct }: Props) {
  const isQuickLaunch = !!initialProduct
  const [step, setStep] = useState<Step>(1)

  // Step 1: Settings — auto-generate name for quick launch
  const defaultName = initialProduct
    ? `${initialProduct.vendor_code || initialProduct.name || `Товар ${initialProduct.nm_id}`} — Поиск`
    : ''
  const [name, setName] = useState(defaultName)
  const [bidType, setBidType] = useState<'unified' | 'manual'>('unified')
  const [paymentType, setPaymentType] = useState<'cpm' | 'cpc'>('cpm')
  const [budget, setBudget] = useState('1000')
  const [autoStart, setAutoStart] = useState(true)
  const [placementSearch, setPlacementSearch] = useState(true)
  const [placementReco, setPlacementReco] = useState(true)

  // Step 2: Products — pre-select if quick launch
  const [subjects, setSubjects] = useState<SubjectItem[]>([])
  const [loadingSubjects, setLoadingSubjects] = useState(false)
  const [selectedSubject, setSelectedSubject] = useState<number | null>(null)
  const [products, setProducts] = useState<ProductItem[]>([])
  const [loadingProducts, setLoadingProducts] = useState(false)
  const [selectedNms, setSelectedNms] = useState<Set<number>>(
    initialProduct ? new Set([initialProduct.nm_id]) : new Set()
  )
  const [productSearch, setProductSearch] = useState('')

  // Step 2: Bids per product (nm_id → bid in RUBLES, user-facing)
  const [bidPerNm, setBidPerNm] = useState<Map<number, string>>(new Map())
  const [bulkBid, setBulkBid] = useState('')

  // Step 3: Submit
  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState('')
  const [submitResult, setSubmitResult] = useState<{
    advert_id: number; budget_deposited: boolean; campaign_started: boolean; budget_message?: string; bids_applied?: boolean; start_error?: string
  } | null>(null)

  // ── Load subjects when modal opens (skip for quick launch) ─────

  useEffect(() => {
    if (isQuickLaunch) return // Skip subject loading for quick launch
    let cancelled = false
    setLoadingSubjects(true)
    getCreationSubjects(shopId, paymentType)
      .then(r => {
        if (!cancelled && r.success) {
          setSubjects(r.subjects || [])
        }
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoadingSubjects(false) })
    return () => { cancelled = true }
  }, [shopId, paymentType, isQuickLaunch])

  // ── Load products when subject changes ──────────────────────────

  useEffect(() => {
    if (selectedSubject === null) {
      setProducts([])
      return
    }
    let cancelled = false
    setLoadingProducts(true)
    getCreationProducts(shopId, [selectedSubject])
      .then(r => {
        if (!cancelled && r.success) {
          setProducts(r.products || [])
        }
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoadingProducts(false) })
    return () => { cancelled = true }
  }, [shopId, selectedSubject])

  // ── Filtered products ──────────────────────────────────────────

  const filteredProducts = useMemo(() => {
    if (!productSearch.trim()) return products
    const q = productSearch.toLowerCase()
    return products.filter(p =>
      p.title?.toLowerCase().includes(q) ||
      String(p.nm).includes(q) ||
      p.vendor_code?.toLowerCase().includes(q)
    )
  }, [products, productSearch])

  // ── Selected products data (for right panel) ───────────────────

  const selectedProductsList = useMemo(() => {
    return products.filter(p => selectedNms.has(p.nm))
  }, [products, selectedNms])

  // ── Validation ─────────────────────────────────────────────────

  const step1Valid = name.trim().length > 0
  const step2Valid = selectedNms.size > 0
  const numBudget = Number(budget) || 0

  // Quick launch: step navigation skips step 2
  const nextStep = (current: Step): Step => {
    if (current === 1 && isQuickLaunch) return 3
    return (current + 1) as Step
  }
  const prevStep = (current: Step): Step => {
    if (current === 3 && isQuickLaunch) return 1
    return (current - 1) as Step
  }

  // ── Handlers ───────────────────────────────────────────────────

  const toggleNm = (nm: number) => {
    setSelectedNms(prev => {
      const next = new Set(prev)
      if (next.has(nm)) {
        next.delete(nm)
        // Also remove bid
        setBidPerNm(prev => {
          const next = new Map(prev)
          next.delete(nm)
          return next
        })
      } else if (next.size < 50) {
        next.add(nm)
      }
      return next
    })
  }

  const removeFromSelected = (nm: number) => {
    setSelectedNms(prev => {
      const next = new Set(prev)
      next.delete(nm)
      return next
    })
    setBidPerNm(prev => {
      const next = new Map(prev)
      next.delete(nm)
      return next
    })
  }

  const selectAllVisible = () => {
    setSelectedNms(prev => {
      const next = new Set(prev)
      filteredProducts.forEach(p => {
        if (next.size < 50) next.add(p.nm)
      })
      return next
    })
  }

  const clearAll = () => {
    setSelectedNms(new Set())
    setBidPerNm(new Map())
  }

  const setBidForNm = (nm: number, value: string) => {
    setBidPerNm(prev => {
      const next = new Map(prev)
      if (value === '') {
        next.delete(nm)
      } else {
        next.set(nm, value)
      }
      return next
    })
  }

  const applyBulkBid = () => {
    const bid = bulkBid.trim()
    if (!bid || isNaN(Number(bid)) || Number(bid) <= 0) return
    setBidPerNm(prev => {
      const next = new Map(prev)
      selectedNms.forEach(nm => {
        next.set(nm, bid)
      })
      return next
    })
  }

  const handleSubmit = async () => {
    setSubmitting(true)
    setSubmitError('')
    try {
      const placement_types: string[] = []
      if (bidType === 'manual') {
        if (placementSearch) placement_types.push('search')
        if (placementReco) placement_types.push('recommendations')
      }

      // Build initial_bids from bidPerNm (convert rubles → kopecks)
      const initial_bids: { nm_id: number; bid_kopecks: number }[] = []
      bidPerNm.forEach((bidRub, nm_id) => {
        const kopecks = Math.round(Number(bidRub) * 100)
        if (kopecks > 0) {
          initial_bids.push({ nm_id, bid_kopecks: kopecks })
        }
      })

      const payload: CreateCampaignPayload = {
        shop_id: shopId,
        name: name.trim(),
        nms: Array.from(selectedNms),
        bid_type: bidType,
        payment_type: paymentType,
        placement_types: placement_types.length > 0 ? placement_types : undefined,
        budget: numBudget,
        auto_start: autoStart && numBudget >= 1000,
        initial_bids: initial_bids.length > 0 ? initial_bids : undefined,
      }

      const result = await createCampaign(payload)
      setSubmitResult(result)

      // Auto-close after 3 seconds
      setTimeout(() => {
        onSuccess(result.advert_id, result.campaign_started)
      }, 3000)
    } catch (e: any) {
      setSubmitError(e?.response?.data?.detail || 'Ошибка создания кампании')
    } finally {
      setSubmitting(false)
    }
  }

  // ── Render ─────────────────────────────────────────────────────

  const stepLabels = isQuickLaunch
    ? [
        { label: 'Настройки', icon: <Settings className="w-3.5 h-3.5" /> },
        { label: 'Запуск', icon: <Zap className="w-3.5 h-3.5" /> },
      ]
    : [
        { label: 'Настройки', icon: <Settings className="w-3.5 h-3.5" /> },
        { label: 'Товары', icon: <Package className="w-3.5 h-3.5" /> },
        { label: 'Создание', icon: <Zap className="w-3.5 h-3.5" /> },
      ]

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm"
      onClick={onClose}
    >
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: 10 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 10 }}
        onClick={e => e.stopPropagation()}
        className={`relative bg-[hsl(var(--card))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl w-full ${
          step === 2 ? 'max-w-4xl' : 'max-w-2xl'
        } max-h-[85vh] flex flex-col overflow-hidden transition-all duration-300`}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-5 pb-1">
          <h2 className="text-lg font-bold text-[hsl(var(--foreground))]">
            {isQuickLaunch ? 'Быстрый запуск кампании' : 'Создание кампании'}
          </h2>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))] transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Step Indicator */}
        <StepIndicator
          current={isQuickLaunch ? (step === 1 ? 1 : 2) as Step : step}
          steps={stepLabels}
        />

        {/* Content */}
        <div className="flex-1 overflow-y-auto px-6 py-4 min-h-0">
          <AnimatePresence mode="wait">
            {/* ─── Step 1: Settings ─────────────────────────────── */}
            {step === 1 && (
              <motion.div
                key="step1"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="space-y-5"
              >
                {/* Campaign Name */}
                <div>
                  <label className="block text-sm font-medium text-[hsl(var(--foreground))] mb-2">
                    Название кампании
                  </label>
                  <input
                    type="text"
                    value={name}
                    onChange={e => setName(e.target.value)}
                    placeholder="Например: Корма для собак — Поиск"
                    maxLength={128}
                    className="w-full px-4 py-3 rounded-xl bg-[hsl(var(--secondary))] border border-[hsl(var(--border))] text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))] focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all"
                  />
                </div>

                {/* Bid Type */}
                <div>
                  <label className="block text-sm font-medium text-[hsl(var(--foreground))] mb-2">
                    Тип ставки
                  </label>
                  <div className="grid grid-cols-2 gap-3">
                    {BID_TYPE_OPTIONS.map(opt => (
                      <button
                        key={opt.value}
                        onClick={() => setBidType(opt.value)}
                        className={`p-3 rounded-xl border text-left transition-all ${
                          bidType === opt.value
                            ? 'border-violet-500 bg-violet-500/10 ring-1 ring-violet-500/30'
                            : 'border-[hsl(var(--border))] hover:border-[hsl(var(--muted-foreground))]'
                        }`}
                      >
                        <div className="text-sm font-semibold text-[hsl(var(--foreground))]">{opt.label}</div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))] mt-1">{opt.desc}</div>
                      </button>
                    ))}
                  </div>
                </div>

                {/* Placement (manual only) */}
                {bidType === 'manual' && (
                  <div>
                    <label className="block text-sm font-medium text-[hsl(var(--foreground))] mb-2">
                      Размещение
                    </label>
                    <div className="flex gap-3">
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="checkbox" checked={placementSearch}
                          onChange={e => setPlacementSearch(e.target.checked)}
                          className="sr-only peer"
                        />
                        <div className="w-5 h-5 rounded-md border-2 border-[hsl(var(--border))] bg-[hsl(var(--secondary))] peer-checked:bg-violet-600 peer-checked:border-violet-600 transition-all flex items-center justify-center">
                          {placementSearch && <Check className="w-3.5 h-3.5 text-white" />}
                        </div>
                        <span className="text-sm text-[hsl(var(--foreground))]">Поиск</span>
                      </label>
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="checkbox" checked={placementReco}
                          onChange={e => setPlacementReco(e.target.checked)}
                          className="sr-only peer"
                        />
                        <div className="w-5 h-5 rounded-md border-2 border-[hsl(var(--border))] bg-[hsl(var(--secondary))] peer-checked:bg-violet-600 peer-checked:border-violet-600 transition-all flex items-center justify-center">
                          {placementReco && <Check className="w-3.5 h-3.5 text-white" />}
                        </div>
                        <span className="text-sm text-[hsl(var(--foreground))]">Рекомендации</span>
                      </label>
                    </div>
                  </div>
                )}

                {/* Payment Type */}
                <div>
                  <label className="block text-sm font-medium text-[hsl(var(--foreground))] mb-2">
                    Модель оплаты
                  </label>
                  <div className="grid grid-cols-2 gap-3">
                    {PAYMENT_TYPE_OPTIONS.map(opt => (
                      <button
                        key={opt.value}
                        onClick={() => setPaymentType(opt.value)}
                        className={`p-3 rounded-xl border text-left transition-all ${
                          paymentType === opt.value
                            ? 'border-violet-500 bg-violet-500/10 ring-1 ring-violet-500/30'
                            : 'border-[hsl(var(--border))] hover:border-[hsl(var(--muted-foreground))]'
                        }`}
                      >
                        <div className="text-sm font-semibold text-[hsl(var(--foreground))]">{opt.label}</div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))] mt-1">{opt.desc}</div>
                      </button>
                    ))}
                  </div>
                </div>

                {/* Budget + Balance */}
                <div>
                  <label className="block text-sm font-medium text-[hsl(var(--foreground))] mb-2">
                    Начальный бюджет (₽)
                  </label>
                  <input
                    type="number"
                    min={0}
                    step={500}
                    value={budget}
                    onChange={e => setBudget(e.target.value)}
                    placeholder="1000"
                    className="w-full px-4 py-3 rounded-xl bg-[hsl(var(--secondary))] border border-[hsl(var(--border))] text-[hsl(var(--foreground))] text-lg font-semibold focus:outline-none focus:ring-2 focus:ring-violet-500/50 transition-all"
                  />
                  <div className="flex items-center justify-between mt-1.5">
                    <p className="text-xs text-[hsl(var(--muted-foreground))]">
                      Минимум 1 000 ₽ · 0 = без пополнения
                    </p>
                    <p className="text-xs text-[hsl(var(--muted-foreground))] flex items-center gap-1">
                      Баланс:{' '}
                      {balanceLoading ? (
                        <span className="inline-flex items-center gap-1 text-[hsl(var(--muted-foreground))]">
                          <Loader2 className="w-3 h-3 animate-spin" />
                          <span className="text-[10px]">загрузка...</span>
                        </span>
                      ) : (
                        <span className="font-semibold text-[hsl(var(--foreground))]">{balance.toLocaleString('ru-RU')} ₽</span>
                      )}
                    </p>
                  </div>
                </div>

                {/* Auto-start toggle */}
                {numBudget >= 1000 && (
                  <label className="flex items-center gap-2.5 cursor-pointer select-none group">
                    <div className="relative">
                      <input
                        type="checkbox" checked={autoStart}
                        onChange={e => setAutoStart(e.target.checked)}
                        className="sr-only peer"
                      />
                      <div className="w-5 h-5 rounded-md border-2 border-[hsl(var(--border))] bg-[hsl(var(--secondary))] peer-checked:bg-violet-600 peer-checked:border-violet-600 transition-all flex items-center justify-center">
                        {autoStart && <Check className="w-3.5 h-3.5 text-white" />}
                      </div>
                    </div>
                    <span className="text-sm text-[hsl(var(--muted-foreground))] group-hover:text-[hsl(var(--foreground))] transition-colors">
                      <Play className="w-3.5 h-3.5 inline-block mr-1 text-emerald-500" />
                      Запустить кампанию сразу после создания
                    </span>
                  </label>
                )}

                {/* Quick Launch: Product Card */}
                {isQuickLaunch && initialProduct && (
                  <div className="mt-1 p-3 rounded-xl bg-[hsl(var(--secondary))] border border-[hsl(var(--border))]">
                    <label className="block text-sm font-medium text-[hsl(var(--foreground))] mb-2">
                      Товар в кампании
                    </label>
                    <div className="flex items-center gap-3">
                      {initialProduct.image_url && (
                        <img
                          src={initialProduct.image_url}
                          alt=""
                          className="w-12 h-12 rounded-lg object-cover flex-shrink-0 border border-[hsl(var(--border))]"
                        />
                      )}
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium text-[hsl(var(--foreground))] truncate">
                          {initialProduct.name || `Товар ${initialProduct.nm_id}`}
                        </p>
                        <div className="flex items-center gap-2 mt-0.5">
                          <span className="text-xs text-[hsl(var(--muted-foreground))]">nm: {initialProduct.nm_id}</span>
                          {initialProduct.vendor_code && (
                            <span className="px-1.5 py-0.5 rounded bg-[hsl(var(--muted))] text-[10px] font-mono text-[hsl(var(--muted-foreground))]">
                              {initialProduct.vendor_code}
                            </span>
                          )}
                        </div>
                      </div>
                      <div className="flex-shrink-0">
                        <div className="px-2 py-1 rounded-lg bg-violet-600/10 text-violet-600 dark:text-violet-400 text-xs font-semibold">
                          ✓ Выбран
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </motion.div>
            )}

            {/* ─── Step 2: Products (Split Panel) ─────────────── */}
            {step === 2 && (
              <motion.div
                key="step2"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="space-y-4"
              >
                {/* Subject selector */}
                <div>
                  <label className="block text-sm font-medium text-[hsl(var(--foreground))] mb-2">
                    Категория товаров
                  </label>
                  {loadingSubjects ? (
                    <div className="flex items-center gap-2 text-sm text-[hsl(var(--muted-foreground))]">
                      <Loader2 className="w-4 h-4 animate-spin" /> Загрузка категорий...
                    </div>
                  ) : (
                    <div className="grid grid-cols-1 gap-2 max-h-[120px] overflow-y-auto pr-1">
                      {subjects.map(subj => (
                        <button
                          key={subj.id}
                          onClick={() => {
                            setSelectedSubject(subj.id === selectedSubject ? null : subj.id)
                            setProductSearch('')
                          }}
                          className={`flex items-center justify-between px-3 py-2 rounded-lg border text-left text-sm transition-all ${
                            selectedSubject === subj.id
                              ? 'border-violet-500 bg-violet-500/10 text-[hsl(var(--foreground))]'
                              : 'border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))] text-[hsl(var(--foreground))]'
                          }`}
                        >
                          <span className="truncate">{subj.name}</span>
                          <span className="ml-2 px-2 py-0.5 rounded-full bg-[hsl(var(--muted))] text-xs text-[hsl(var(--muted-foreground))] flex-shrink-0">
                            {subj.count} шт
                          </span>
                        </button>
                      ))}
                      {subjects.length === 0 && (
                        <p className="text-sm text-[hsl(var(--muted-foreground))] py-4 text-center">
                          Нет доступных категорий
                        </p>
                      )}
                    </div>
                  )}
                </div>

                {/* Split panel: Catalog (left) + Selected (right) */}
                {selectedSubject !== null && (
                  <div className="flex gap-4 min-h-[360px]">
                    {/* ── Left: Product Catalog ── */}
                    <div className="flex-1 min-w-0 flex flex-col">
                      <div className="flex items-center justify-between mb-2">
                        <label className="text-sm font-medium text-[hsl(var(--foreground))]">
                          Каталог товаров
                        </label>
                        <div className="flex gap-2">
                          <button
                            onClick={selectAllVisible}
                            className="text-xs text-violet-600 dark:text-violet-400 hover:underline"
                          >
                            Выбрать все
                          </button>
                          {selectedNms.size > 0 && (
                            <button
                              onClick={clearAll}
                              className="text-xs text-red-500 hover:underline"
                            >
                              Сбросить
                            </button>
                          )}
                        </div>
                      </div>

                      {/* Search */}
                      <div className="relative mb-2">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[hsl(var(--muted-foreground))]" />
                        <input
                          type="text"
                          value={productSearch}
                          onChange={e => setProductSearch(e.target.value)}
                          placeholder="Поиск по артикулу, ID или названию..."
                          className="w-full pl-9 pr-3 py-2 rounded-lg bg-[hsl(var(--secondary))] border border-[hsl(var(--border))] text-sm text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))] focus:outline-none focus:ring-2 focus:ring-violet-500/50"
                        />
                      </div>

                      {loadingProducts ? (
                        <div className="flex items-center gap-2 text-sm text-[hsl(var(--muted-foreground))] py-6 justify-center flex-1">
                          <Loader2 className="w-4 h-4 animate-spin" /> Загрузка товаров...
                        </div>
                      ) : (
                        <div className="space-y-1 flex-1 overflow-y-auto pr-1" style={{ maxHeight: '300px' }}>
                          {filteredProducts.map(p => {
                            const checked = selectedNms.has(p.nm)
                            return (
                              <button
                                key={p.nm}
                                onClick={() => toggleNm(p.nm)}
                                className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg border text-left transition-all ${
                                  checked
                                    ? 'border-violet-500 bg-violet-500/5'
                                    : 'border-transparent hover:bg-[hsl(var(--muted))]'
                                }`}
                              >
                                <div className={`w-5 h-5 rounded-md border-2 flex-shrink-0 flex items-center justify-center transition-all ${
                                  checked
                                    ? 'bg-violet-600 border-violet-600'
                                    : 'border-[hsl(var(--border))] bg-[hsl(var(--secondary))]'
                                }`}>
                                  {checked && <Check className="w-3.5 h-3.5 text-white" />}
                                </div>
                                <div className="flex-1 min-w-0">
                                  <div className="text-sm text-[hsl(var(--foreground))] truncate">
                                    {p.title || `Товар ${p.nm}`}
                                  </div>
                                  <div className="text-xs text-[hsl(var(--muted-foreground))] flex items-center gap-2 flex-wrap">
                                    <span>nm: {p.nm}</span>
                                    {p.vendor_code && (
                                      <span className="px-1.5 py-0.5 rounded bg-[hsl(var(--muted))] text-[10px] font-mono">
                                        {p.vendor_code}
                                      </span>
                                    )}
                                  </div>
                                </div>
                                {!checked && (
                                  <Plus className="w-4 h-4 text-[hsl(var(--muted-foreground))] flex-shrink-0 opacity-0 group-hover:opacity-100" />
                                )}
                              </button>
                            )
                          })}
                          {filteredProducts.length === 0 && products.length > 0 && (
                            <p className="text-sm text-[hsl(var(--muted-foreground))] py-4 text-center">
                              Ничего не найдено
                            </p>
                          )}
                          {products.length === 0 && !loadingProducts && (
                            <p className="text-sm text-[hsl(var(--muted-foreground))] py-4 text-center">
                              Нет товаров в этой категории
                            </p>
                          )}
                        </div>
                      )}
                    </div>

                    {/* ── Divider ── */}
                    <div className="w-px bg-[hsl(var(--border))] flex-shrink-0" />

                    {/* ── Right: Selected Products + Bids ── */}
                    <div className="flex-1 min-w-0 flex flex-col">
                      <div className="flex items-center justify-between mb-2">
                        <label className="text-sm font-medium text-[hsl(var(--foreground))]">
                          В кампании
                          <span className="ml-2 px-2 py-0.5 rounded-full bg-violet-500/15 text-xs font-semibold text-violet-600 dark:text-violet-400">
                            {selectedNms.size}/50
                          </span>
                        </label>
                      </div>

                      {selectedNms.size === 0 ? (
                        <div className="flex-1 flex flex-col items-center justify-center text-center py-10">
                          <div className="w-12 h-12 rounded-full bg-[hsl(var(--muted))] flex items-center justify-center mb-3">
                            <ArrowRight className="w-5 h-5 text-[hsl(var(--muted-foreground))]" />
                          </div>
                          <p className="text-sm text-[hsl(var(--muted-foreground))]">
                            Выберите товары из каталога слева
                          </p>
                          <p className="text-xs text-[hsl(var(--muted-foreground))] mt-1">
                            Для каждого товара можно задать начальную ставку
                          </p>
                        </div>
                      ) : (
                        <>
                          {/* Selected products list with bid inputs */}
                          <div className="space-y-2 flex-1 overflow-y-auto pr-1" style={{ maxHeight: '240px' }}>
                            {selectedProductsList.map(p => (
                              <div
                                key={p.nm}
                                className="px-3 py-2.5 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--secondary))]/50 group"
                              >
                                <div className="flex items-start gap-2">
                                  <div className="flex-1 min-w-0">
                                    <div className="text-sm text-[hsl(var(--foreground))] truncate font-medium">
                                      {p.title || `Товар ${p.nm}`}
                                    </div>
                                    <div className="text-xs text-[hsl(var(--muted-foreground))] flex items-center gap-2 mt-0.5">
                                      <span>nm: {p.nm}</span>
                                      {p.vendor_code && (
                                        <span className="px-1.5 py-0.5 rounded bg-[hsl(var(--muted))] text-[10px] font-mono">
                                          {p.vendor_code}
                                        </span>
                                      )}
                                    </div>
                                  </div>
                                  <button
                                    onClick={() => removeFromSelected(p.nm)}
                                    className="p-1 rounded-md text-[hsl(var(--muted-foreground))] hover:text-red-500 hover:bg-red-500/10 transition-colors opacity-60 hover:opacity-100"
                                    title="Убрать из кампании"
                                  >
                                    <Trash2 className="w-3.5 h-3.5" />
                                  </button>
                                </div>
                                {/* Bid input */}
                                <div className="flex items-center gap-2 mt-2">
                                  <label className="text-xs text-[hsl(var(--muted-foreground))] flex-shrink-0">
                                    Ставка:
                                  </label>
                                  <div className="relative flex-1">
                                    <input
                                      type="number"
                                      min={0}
                                      step={10}
                                      value={bidPerNm.get(p.nm) || ''}
                                      onChange={e => setBidForNm(p.nm, e.target.value)}
                                      placeholder="—"
                                      className="w-full pl-3 pr-7 py-1.5 rounded-lg bg-[hsl(var(--card))] border border-[hsl(var(--border))] text-sm text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))] focus:outline-none focus:ring-2 focus:ring-violet-500/50 text-right"
                                    />
                                    <span className="absolute right-2.5 top-1/2 -translate-y-1/2 text-xs text-[hsl(var(--muted-foreground))]">₽</span>
                                  </div>
                                </div>
                              </div>
                            ))}
                          </div>

                          {/* Bulk bid */}
                          <div className="mt-3 pt-3 border-t border-[hsl(var(--border))]">
                            <div className="flex items-center gap-2">
                              <label className="text-xs text-[hsl(var(--muted-foreground))] flex-shrink-0 whitespace-nowrap">
                                Ставка для всех:
                              </label>
                              <div className="relative flex-1">
                                <input
                                  type="number"
                                  min={0}
                                  step={10}
                                  value={bulkBid}
                                  onChange={e => setBulkBid(e.target.value)}
                                  placeholder="Напр. 150"
                                  className="w-full pl-3 pr-7 py-1.5 rounded-lg bg-[hsl(var(--secondary))] border border-[hsl(var(--border))] text-sm text-[hsl(var(--foreground))] placeholder:text-[hsl(var(--muted-foreground))] focus:outline-none focus:ring-2 focus:ring-violet-500/50 text-right"
                                />
                                <span className="absolute right-2.5 top-1/2 -translate-y-1/2 text-xs text-[hsl(var(--muted-foreground))]">₽</span>
                              </div>
                              <button
                                onClick={applyBulkBid}
                                disabled={!bulkBid.trim() || isNaN(Number(bulkBid)) || Number(bulkBid) <= 0}
                                className="px-3 py-1.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-xs font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed flex-shrink-0"
                              >
                                Применить
                              </button>
                            </div>
                            <p className="text-[11px] text-[hsl(var(--muted-foreground))] mt-1.5">
                              💡 Ставки задаются в рублях за 1000 показов (CPM). Без ставки — WB установит минимальную
                            </p>
                          </div>
                        </>
                      )}
                    </div>
                  </div>
                )}
              </motion.div>
            )}

            {/* ─── Step 3: Confirmation ────────────────────────── */}
            {step === 3 && (
              <motion.div
                key="step3"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
                className="space-y-4"
              >
                {submitResult ? (
                  /* Success state */
                  <div className="flex flex-col items-center gap-4 py-6">
                    <div className="w-16 h-16 rounded-full bg-emerald-500/15 flex items-center justify-center">
                      <Check className="w-8 h-8 text-emerald-500" />
                    </div>
                    <div className="text-center space-y-2">
                      <p className="text-lg font-bold text-[hsl(var(--foreground))]">Кампания создана!</p>
                      <p className="text-sm text-[hsl(var(--muted-foreground))]">
                        ID: {submitResult.advert_id}
                      </p>
                      {submitResult.budget_deposited && (
                        <p className="text-sm text-emerald-600 dark:text-emerald-400 font-medium">
                          <DollarSign className="w-3.5 h-3.5 inline mr-1" />
                          Бюджет пополнен на {numBudget.toLocaleString('ru-RU')} ₽
                        </p>
                      )}
                      {submitResult.budget_message && (
                        <p className="text-sm text-amber-500">
                          ⚠ {submitResult.budget_message}
                        </p>
                      )}
                      {submitResult.bids_applied && (
                        <p className="text-sm text-emerald-600 dark:text-emerald-400 font-medium">
                          <DollarSign className="w-3.5 h-3.5 inline mr-1" />
                          Начальные ставки установлены
                        </p>
                      )}
                      {submitResult.bids_applied === false && bidPerNm.size > 0 && (
                        <p className="text-sm text-amber-500 font-medium">
                          <AlertTriangle className="w-3.5 h-3.5 inline mr-1" />
                          Не удалось установить начальные ставки — установите вручную в управлении кампанией
                        </p>
                      )}
                      {submitResult.campaign_started && (
                        <p className="text-sm text-emerald-600 dark:text-emerald-400 font-medium">
                          <Play className="w-3.5 h-3.5 inline mr-1" />
                          Кампания запущена
                        </p>
                      )}
                      {!submitResult.campaign_started && autoStart && numBudget >= 1000 && (
                        <p className="text-sm text-amber-500 font-medium">
                          <AlertTriangle className="w-3.5 h-3.5 inline mr-1" />
                          Автозапуск не сработал{submitResult.start_error ? `: ${submitResult.start_error}` : ''} — запустите кампанию вручную
                        </p>
                      )}
                    </div>
                  </div>
                ) : (
                  /* Review state */
                  <>
                    <h3 className="text-sm font-semibold text-[hsl(var(--foreground))] uppercase tracking-wider">
                      Проверьте данные
                    </h3>

                    <div className="space-y-3">
                      {/* Summary items */}
                      <div className="bg-[hsl(var(--secondary))] rounded-xl p-4 space-y-3">
                        <Row label="Название" value={name} />
                        <Row label="Тип ставки" value={bidType === 'unified' ? 'Единая ставка' : 'Ручные ставки'} />
                        <Row label="Модель оплаты" value={paymentType.toUpperCase()} />
                        {bidType === 'manual' && (
                          <Row
                            label="Размещение"
                            value={[
                              placementSearch && 'Поиск',
                              placementReco && 'Рекомендации',
                            ].filter(Boolean).join(', ')}
                          />
                        )}
                        <Row label="Товаров" value={`${selectedNms.size} шт`} />

                        {/* Show bids summary */}
                        {bidPerNm.size > 0 && (
                          <div className="border-t border-[hsl(var(--border))] pt-3">
                            <div className="text-xs text-[hsl(var(--muted-foreground))] mb-2">Начальные ставки:</div>
                            <div className="space-y-1">
                              {selectedProductsList.filter(p => bidPerNm.has(p.nm)).map(p => (
                                <div key={p.nm} className="flex items-center justify-between text-xs">
                                  <span className="text-[hsl(var(--muted-foreground))] truncate max-w-[200px]">
                                    {p.vendor_code || p.title || `nm: ${p.nm}`}
                                  </span>
                                  <span className="text-[hsl(var(--foreground))] font-medium">
                                    {Number(bidPerNm.get(p.nm) || 0).toLocaleString('ru-RU')} ₽
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        <div className="border-t border-[hsl(var(--border))] pt-3">
                          <Row
                            label="Начальный бюджет"
                            value={numBudget > 0 ? `${numBudget.toLocaleString('ru-RU')} ₽` : 'Без пополнения'}
                            highlight
                          />
                        </div>
                        {numBudget >= 1000 && autoStart && (
                          <Row
                            label="Автозапуск"
                            value="Да"
                            icon={<Play className="w-3.5 h-3.5 text-emerald-500" />}
                          />
                        )}
                      </div>

                      {/* Balance warning */}
                      {numBudget > balance && numBudget > 0 && (
                        <div className="flex items-center gap-2 text-sm text-amber-500 bg-amber-500/10 px-3 py-2 rounded-lg">
                          <AlertTriangle className="w-4 h-4 flex-shrink-0" />
                          Бюджет ({numBudget.toLocaleString('ru-RU')} ₽) превышает баланс ({balance.toLocaleString('ru-RU')} ₽)
                        </div>
                      )}

                      {/* Error */}
                      {submitError && (
                        <div className="flex items-center gap-2 text-sm text-red-500 bg-red-500/10 px-3 py-2 rounded-lg">
                          <AlertTriangle className="w-4 h-4 flex-shrink-0" />
                          {submitError}
                        </div>
                      )}
                    </div>
                  </>
                )}
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Footer: Navigation + Actions */}
        {!submitResult && (
          <div className="px-6 py-4 border-t border-[hsl(var(--border))] flex items-center justify-between">
            {/* Back */}
            {step > 1 ? (
              <button
                onClick={() => setStep(prevStep(step))}
                disabled={submitting}
                className="flex items-center gap-1.5 px-4 py-2.5 rounded-xl border border-[hsl(var(--border))] text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))] text-sm font-medium transition-colors disabled:opacity-50"
              >
                <ChevronLeft className="w-4 h-4" /> Назад
              </button>
            ) : (
              <div />
            )}

            {/* Next / Submit */}
            {step < 3 ? (
              <button
                onClick={() => setStep(nextStep(step))}
                disabled={step === 1 ? !step1Valid : !step2Valid}
                className="flex items-center gap-1.5 px-5 py-2.5 rounded-xl bg-violet-600 hover:bg-violet-500 text-white text-sm font-semibold transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Далее <ChevronRight className="w-4 h-4" />
              </button>
            ) : (
              <button
                onClick={handleSubmit}
                disabled={submitting}
                className="flex items-center gap-2 px-6 py-2.5 rounded-xl bg-violet-600 hover:bg-violet-500 text-white text-sm font-semibold transition-colors disabled:opacity-50"
              >
                {submitting ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Создаю кампанию...
                  </>
                ) : (
                  <>
                    <Zap className="w-4 h-4" />
                    {isQuickLaunch ? 'Запустить' : 'Создать'}
                  </>
                )}
              </button>
            )}
          </div>
        )}
      </motion.div>
    </motion.div>
  )
}

// ── Helper Components ────────────────────────────────────────────

function Row({
  label, value, highlight, icon,
}: {
  label: string; value: string; highlight?: boolean; icon?: React.ReactNode
}) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-sm text-[hsl(var(--muted-foreground))]">{label}</span>
      <span className={`text-sm flex items-center gap-1.5 ${highlight ? 'font-bold text-[hsl(var(--foreground))]' : 'text-[hsl(var(--foreground))]'}`}>
        {icon}
        {value}
      </span>
    </div>
  )
}
