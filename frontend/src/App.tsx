import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { AppLayout } from '@/components/layout/AppLayout'
import { AuthGuard } from '@/components/auth/AuthGuard'
import LoginPage from '@/pages/LoginPage'
import RegisterPage from '@/pages/RegisterPage'
import DashboardPage from '@/pages/DashboardPage'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* ── Public Routes ── */}
        <Route path="/login" element={<LoginPage />} />
        <Route path="/register" element={<RegisterPage />} />

        {/* ── Protected Routes ── */}
        <Route
          element={
            <AuthGuard>
              <AppLayout />
            </AuthGuard>
          }
        >
          <Route path="/" element={<DashboardPage />} />
          {/* Future routes will be added here:
          <Route path="/sales" element={<SalesPage />} />
          <Route path="/funnel" element={<FunnelPage />} />
          <Route path="/warehouses" element={<WarehousesPage />} />
          <Route path="/finances" element={<FinancesPage />} />
          <Route path="/advertising" element={<AdvertisingPage />} />
          <Route path="/events" element={<EventsPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          */}
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
