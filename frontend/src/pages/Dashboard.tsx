import { useQuery } from '@tanstack/react-query'
import { useAppStore } from '../stores/appStore'
import { apiClient } from '../api/client'

export default function Dashboard() {
  const { isLoading } = useAppStore()

  const { data: healthData, isLoading: healthLoading } = useQuery({
    queryKey: ['health'],
    queryFn: () => apiClient.get('/api/health').then(res => res.data),
    retry: false,
  })

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1 className="gradient-text">MMS Dashboard</h1>
        <p className="dashboard-subtitle">Marketplace Management System</p>
      </header>

      <main className="dashboard-content">
        <div className="card animate-fade-in">
          <h3>Статус системы</h3>
          {healthLoading || isLoading ? (
            <div className="skeleton" style={{ height: '60px', marginTop: '1rem' }} />
          ) : (
            <div className="status-info">
              <p>
                <span className="status-label">API:</span>
                <span className={`status-value ${healthData?.status === 'healthy' ? 'success' : 'error'}`}>
                  {healthData?.status || 'Недоступен'}
                </span>
              </p>
              <p>
                <span className="status-label">Версия:</span>
                <span className="status-value">{healthData?.version || '—'}</span>
              </p>
            </div>
          )}
        </div>

        <div className="card animate-fade-in" style={{ animationDelay: '0.1s' }}>
          <h3>Добро пожаловать!</h3>
          <p style={{ color: 'var(--text-secondary)', marginTop: '0.5rem' }}>
            Система автоматизации рекламы и финансовой аналитики для маркетплейсов.
          </p>
        </div>

        <div className="features-grid">
          <div className="card feature-card animate-fade-in" style={{ animationDelay: '0.2s' }}>
            <div className="feature-icon">📊</div>
            <h4>Аналитика</h4>
            <p>Глубокий анализ продаж и финансов</p>
          </div>
          <div className="card feature-card animate-fade-in" style={{ animationDelay: '0.3s' }}>
            <div className="feature-icon">🎯</div>
            <h4>Автобиддер</h4>
            <p>Автоматизация рекламных ставок</p>
          </div>
          <div className="card feature-card animate-fade-in" style={{ animationDelay: '0.4s' }}>
            <div className="feature-icon">📈</div>
            <h4>Отчеты</h4>
            <p>Детальные финансовые отчеты</p>
          </div>
        </div>
      </main>

      <style>{`
        .dashboard {
          min-height: 100vh;
          padding: 2rem;
          max-width: 1200px;
          margin: 0 auto;
        }

        .dashboard-header {
          text-align: center;
          margin-bottom: 3rem;
        }

        .dashboard-subtitle {
          color: var(--text-secondary);
          margin-top: 0.5rem;
        }

        .dashboard-content {
          display: flex;
          flex-direction: column;
          gap: 1.5rem;
        }

        .status-info {
          margin-top: 1rem;
        }

        .status-info p {
          display: flex;
          justify-content: space-between;
          padding: 0.5rem 0;
          border-bottom: 1px solid var(--border-color);
        }

        .status-label {
          color: var(--text-secondary);
        }

        .status-value {
          font-weight: 500;
        }

        .status-value.success {
          color: var(--color-success);
        }

        .status-value.error {
          color: var(--color-danger);
        }

        .features-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: 1.5rem;
          margin-top: 1rem;
        }

        .feature-card {
          text-align: center;
        }

        .feature-icon {
          font-size: 2.5rem;
          margin-bottom: 1rem;
        }

        .feature-card h4 {
          margin-bottom: 0.5rem;
        }

        .feature-card p {
          color: var(--text-secondary);
          font-size: 0.9rem;
        }
      `}</style>
    </div>
  )
}
