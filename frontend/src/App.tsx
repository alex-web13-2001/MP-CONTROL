import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { AppLayout } from '@/components/layout/AppLayout'
import { AuthGuard } from '@/components/auth/AuthGuard'
import OnboardingGuard from '@/components/OnboardingGuard'
import LoginPage from '@/pages/LoginPage'
import RegisterPage from '@/pages/RegisterPage'
import OnboardingPage from '@/pages/OnboardingPage'
import DashboardPage from '@/pages/DashboardPage'
import ProductsPage from '@/pages/ProductsPage'
import SettingsPage from '@/pages/SettingsPage'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* ── Public Routes ── */}
        <Route path="/login" element={<LoginPage />} />
        <Route path="/register" element={<RegisterPage />} />

        {/* ── Onboarding (auth required, no shop guard) ── */}
        <Route
          path="/onboarding"
          element={
            <AuthGuard>
              <OnboardingPage />
            </AuthGuard>
          }
        />

        {/* ── Protected Routes (auth + shop required) ── */}
        <Route
          element={
            <AuthGuard>
              <OnboardingGuard>
                <AppLayout />
              </OnboardingGuard>
            </AuthGuard>
          }
        >
          <Route path="/" element={<DashboardPage />} />
          <Route path="/products" element={<ProductsPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          {/* Future routes will be added here:
          <Route path="/sales" element={<SalesPage />} />
          <Route path="/funnel" element={<FunnelPage />} />
          <Route path="/warehouses" element={<WarehousesPage />} />
          <Route path="/finances" element={<FinancesPage />} />
          <Route path="/advertising" element={<AdvertisingPage />} />
          <Route path="/events" element={<EventsPage />} />
          */}
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
