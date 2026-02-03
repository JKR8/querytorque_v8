import { useEffect } from 'react'
import { BrowserRouter, Routes, Route, useNavigate, useLocation, Link } from 'react-router-dom'
import { useAuth0 } from '@auth0/auth0-react'
import { config } from './config'
import { setAuthTokenProvider } from './api/client'

// Pages
import HomePage from './pages/HomePage'
import AnalyzePage from './pages/AnalyzePage'
import ModelBrowserPage from './pages/ModelBrowserPage'
import ReportsPage from './pages/ReportsPage'
import AccountPage from './pages/AccountPage'
import ToolsPage from './pages/ToolsPage'

// ============================================
// Protected Route Component
// ============================================

interface ProtectedRouteProps {
  children: React.ReactNode
}

function ProtectedRoute({ children }: ProtectedRouteProps) {
  const { isAuthenticated, isLoading, loginWithRedirect } = useAuth0()

  useEffect(() => {
    if (!isLoading && !isAuthenticated && config.features.authEnabled) {
      loginWithRedirect()
    }
  }, [isLoading, isAuthenticated, loginWithRedirect])

  if (isLoading) {
    return (
      <div className="loading-container">
        <div className="spinner" />
        <span>Loading...</span>
      </div>
    )
  }

  if (!isAuthenticated && config.features.authEnabled) {
    return null
  }

  return <>{children}</>
}

// ============================================
// App Header
// ============================================

function AppHeader() {
  const navigate = useNavigate()
  const location = useLocation()
  const { user, isAuthenticated, logout, loginWithRedirect } = useAuth0()

  const isActive = (path: string) => location.pathname === path

  return (
    <header className="header">
      <div className="brand" onClick={() => navigate('/')} style={{ cursor: 'pointer' }}>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
        </svg>
        <span>Query Torque</span>
        <span className="product-badge">DAX</span>
      </div>

      <nav className="header-nav">
        <Link to="/" className={isActive('/') ? 'active' : ''}>Home</Link>
        <Link to="/analyze" className={isActive('/analyze') ? 'active' : ''}>Analyze</Link>
        <Link to="/model-browser" className={isActive('/model-browser') ? 'active' : ''}>Model Browser</Link>
        <Link to="/tools" className={isActive('/tools') ? 'active' : ''}>Tools</Link>
        <Link to="/reports" className={isActive('/reports') ? 'active' : ''}>Reports</Link>
      </nav>

      <div className="header-spacer" />

      {config.features.authEnabled && (
        <>
          {isAuthenticated ? (
            <div className="user-menu">
              <button className="user-menu-btn" onClick={() => navigate('/account')}>
                <span className="user-avatar">
                  {user?.picture ? (
                    <img src={user.picture} alt={user.name || 'User'} />
                  ) : (
                    (user?.name || user?.email || 'U')[0].toUpperCase()
                  )}
                </span>
                <span className="user-name">{user?.name || user?.email?.split('@')[0]}</span>
              </button>
              <div className="user-dropdown">
                <button onClick={() => navigate('/account')}>Account</button>
                <hr />
                <button onClick={() => logout({ logoutParams: { returnTo: window.location.origin } })}>
                  Sign Out
                </button>
              </div>
            </div>
          ) : (
            <button className="btn btn-primary" onClick={() => loginWithRedirect()}>
              Sign In
            </button>
          )}
        </>
      )}
    </header>
  )
}

// ============================================
// App Layout
// ============================================

function AppLayout() {
  const { getAccessTokenSilently } = useAuth0()

  // Set up auth token provider for API client
  useEffect(() => {
    if (config.features.authEnabled) {
      setAuthTokenProvider(async () => {
        try {
          return await getAccessTokenSilently()
        } catch {
          return null
        }
      })
    }
  }, [getAccessTokenSilently])

  return (
    <div className="app">
      <AppHeader />
      <main className="container">
        <Routes>
          {/* Public routes */}
          <Route path="/" element={<HomePage />} />

          {/* Protected routes */}
          <Route
            path="/analyze"
            element={
              config.features.authEnabled ? (
                <ProtectedRoute>
                  <AnalyzePage />
                </ProtectedRoute>
              ) : (
                <AnalyzePage />
              )
            }
          />
          <Route
            path="/model-browser"
            element={
              config.features.authEnabled ? (
                <ProtectedRoute>
                  <ModelBrowserPage />
                </ProtectedRoute>
              ) : (
                <ModelBrowserPage />
              )
            }
          />
          <Route
            path="/tools"
            element={
              config.features.authEnabled ? (
                <ProtectedRoute>
                  <ToolsPage />
                </ProtectedRoute>
              ) : (
                <ToolsPage />
              )
            }
          />
          <Route
            path="/reports"
            element={
              config.features.authEnabled ? (
                <ProtectedRoute>
                  <ReportsPage />
                </ProtectedRoute>
              ) : (
                <ReportsPage />
              )
            }
          />
          <Route
            path="/account"
            element={
              <ProtectedRoute>
                <AccountPage />
              </ProtectedRoute>
            }
          />
        </Routes>
      </main>

      <footer className="footer">
        <p>Query Torque DAX - Power BI Performance Analysis</p>
      </footer>
    </div>
  )
}

// ============================================
// Main App
// ============================================

function App() {
  return (
    <BrowserRouter>
      <AppLayout />
    </BrowserRouter>
  )
}

export default App
