import { useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import { UserPlus, Eye, EyeOff, ArrowRight } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useAuthStore } from '@/stores/authStore'
import { registerApi } from '@/api/auth'

export default function RegisterPage() {
  const [name, setName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [showPassword, setShowPassword] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const loginFromApi = useAuthStore((s) => s.loginFromApi)
  const navigate = useNavigate()

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')

    if (password !== confirmPassword) {
      setError('Пароли не совпадают')
      return
    }

    if (password.length < 6) {
      setError('Пароль должен быть минимум 6 символов')
      return
    }

    setLoading(true)

    try {
      const data = await registerApi({ email, password, name })
      loginFromApi(data)
      navigate('/', { replace: true })
    } catch (err: any) {
      const detail = err.response?.data?.detail
      if (detail === 'Пользователь с таким email уже существует') {
        setError('Этот email уже зарегистрирован')
      } else {
        setError(detail || 'Ошибка регистрации')
      }
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-[hsl(var(--background))] p-4">
      {/* Background gradient */}
      <div className="pointer-events-none fixed inset-0">
        <div className="absolute -left-40 -top-40 h-80 w-80 rounded-full bg-[hsl(var(--primary)/0.08)] blur-3xl" />
        <div className="absolute -bottom-40 -right-40 h-80 w-80 rounded-full bg-[hsl(245_80%_70%/0.06)] blur-3xl" />
      </div>

      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, ease: 'easeOut' }}
        className="w-full max-w-md"
      >
        {/* Logo */}
        <div className="mb-8 text-center">
          <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-2xl bg-[hsl(var(--primary))] text-white text-xl font-bold shadow-lg shadow-[hsl(var(--primary)/0.25)]">
            MP
          </div>
          <h1 className="text-2xl font-bold text-[hsl(var(--foreground))]">
            MP-Control
          </h1>
          <p className="mt-1 text-sm text-[hsl(var(--muted-foreground))]">
            Управление маркетплейсами
          </p>
        </div>

        {/* Card */}
        <div className="rounded-2xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-8 shadow-xl">
          <div className="mb-6">
            <h2 className="text-lg font-semibold text-[hsl(var(--foreground))]">
              Создать аккаунт
            </h2>
            <p className="mt-1 text-sm text-[hsl(var(--muted-foreground))]">
              Заполните данные для регистрации
            </p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            <Input
              label="Имя"
              type="text"
              placeholder="Ваше имя"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              autoComplete="name"
              icon={<UserPlus className="h-4 w-4" />}
            />

            <Input
              label="Email"
              type="email"
              placeholder="name@example.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              autoComplete="email"
            />

            <div className="relative">
              <Input
                label="Пароль"
                type={showPassword ? 'text' : 'password'}
                placeholder="Минимум 6 символов"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                autoComplete="new-password"
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute right-3 top-[38px] text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))] transition-colors"
              >
                {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
              </button>
            </div>

            <Input
              label="Подтвердите пароль"
              type={showPassword ? 'text' : 'password'}
              placeholder="Повторите пароль"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              required
              autoComplete="new-password"
              error={error}
            />

            <Button
              type="submit"
              className="w-full"
              size="lg"
              loading={loading}
            >
              Зарегистрироваться
              <ArrowRight className="h-4 w-4" />
            </Button>
          </form>

          <div className="mt-6 text-center">
            <p className="text-sm text-[hsl(var(--muted-foreground))]">
              Уже есть аккаунт?{' '}
              <Link to="/login" className="font-medium text-[hsl(var(--primary))] hover:underline">
                Войти
              </Link>
            </p>
          </div>
        </div>

        {/* Footer */}
        <p className="mt-6 text-center text-xs text-[hsl(var(--muted-foreground))]">
          MP-Control © 2026. Аналитика маркетплейсов.
        </p>
      </motion.div>
    </div>
  )
}
