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
import FinancesPage from '@/pages/FinancesPage'
import SalesPage from '@/pages/SalesPage'
import AbcXyzPage from '@/pages/AbcXyzPage'
import ForecastPage from '@/pages/ForecastPage'
import LtvPage from '@/pages/LtvPage'
import EventsPage from '@/pages/EventsPage'
import EventsGraphPage from '@/pages/EventsGraphPage'
import WarehouseSupplyPage from '@/pages/WarehouseSupplyPage'
import WarehouseAnalyticsPage from '@/pages/WarehouseAnalyticsPage'
import WarehousesOverviewPage from '@/pages/WarehousesOverviewPage'
import WarehousesCrossPage from '@/pages/WarehousesCrossPage'
import WarehousesStoragePage from '@/pages/WarehousesStoragePage'
import WarehousesGeographyPage from '@/pages/WarehousesGeographyPage'
import AdvertisingAnalyticsPage from '@/pages/AdvertisingAnalyticsPage'
import AdvertisingCampaignsPage from '@/pages/AdvertisingCampaignsPage'
import AdvertisingAutobidderPage from '@/pages/AdvertisingAutobidderPage'
import AdManagementPage from '@/pages/AdManagementPage'
import ProductsPricesPage from '@/pages/ProductsPricesPage'

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
          <Route path="/products/prices" element={<ProductsPricesPage />} />
          <Route path="/sales" element={<SalesPage />} />
          <Route path="/sales/abc-xyz" element={<AbcXyzPage />} />
          <Route path="/sales/forecast" element={<ForecastPage />} />
          <Route path="/customers/ltv" element={<LtvPage />} />
          <Route path="/finances" element={<FinancesPage />} />
          <Route path="/events" element={<EventsPage />} />
          <Route path="/events/graph" element={<EventsGraphPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          {/* Warehouses */}
          <Route path="/warehouses/overview" element={<WarehousesOverviewPage />} />
          <Route path="/warehouses/cross" element={<WarehousesCrossPage />} />
          <Route path="/warehouses/storage" element={<WarehousesStoragePage />} />
          <Route path="/warehouses/geography" element={<WarehousesGeographyPage />} />
          <Route path="/warehouses/supply" element={<WarehouseSupplyPage />} />
          <Route path="/warehouses/analytics" element={<WarehouseAnalyticsPage />} />
          {/* Advertising */}
          <Route path="/advertising/analytics" element={<AdvertisingAnalyticsPage />} />
          <Route path="/advertising/campaigns" element={<AdvertisingCampaignsPage />} />
          <Route path="/advertising/management" element={<AdManagementPage />} />
          <Route path="/advertising/autobidder" element={<AdvertisingAutobidderPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

