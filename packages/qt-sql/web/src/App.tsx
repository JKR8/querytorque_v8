import { useState } from 'react'
import { BrowserRouter, Routes, Route, useNavigate, useLocation } from 'react-router-dom'
import { useAuth0 } from '@auth0/auth0-react'
import { isAuthConfigured } from './config'

// Pages
import HomePage from './pages/HomePage'
import EditorPage from './pages/EditorPage'
import ReportsPage from './pages/ReportsPage'
import AccountPage from './pages/AccountPage'

// Components
import SettingsModal from './components/SettingsModal'

// Hooks
import useSettings from './hooks/useSettings'

// Protected Route wrapper
function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { isAuthenticated, isLoading, loginWithRedirect } = useAuth0()

  if (!isAuthConfigured()) {
    return <>{children}</>
  }

  if (isLoading) {
    return (
      <div className="loading-screen">
        <div className="spinner-large" />
        <p>Loading...</p>
      </div>
    )
  }

  if (!isAuthenticated) {
    loginWithRedirect()
    return null
  }

  return <>{children}</>
}

// App Header
function AppHeader({ onOpenSettings }: { onOpenSettings: () => void }) {
  const navigate = useNavigate()
  const location = useLocation()
  const authEnabled = isAuthConfigured()

  // Only use Auth0 hooks if auth is enabled
  const auth0 = authEnabled ? useAuth0() : null
  const { user, isAuthenticated, logout } = auth0 || {
    user: null,
    isAuthenticated: false,
    logout: () => {},
  }

  const isActive = (path: string) => location.pathname === path

  return (
    <header className="header">
      <div className="brand" onClick={() => navigate('/')} style={{ cursor: 'pointer' }}>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
        </svg>
        <span>Query Torque SQL</span>
      </div>

      <nav className="header-nav">
        <a
          className={isActive('/editor') ? 'active' : ''}
          onClick={() => navigate('/editor')}
        >
          Editor
        </a>
        <a
          className={isActive('/reports') ? 'active' : ''}
          onClick={() => navigate('/reports')}
        >
          Reports
        </a>
      </nav>

      <div className="header-spacer" />

      {/* Settings button */}
      <button
        className="settings-btn"
        onClick={onOpenSettings}
        title="Settings"
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="3" />
          <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
        </svg>
      </button>

      {authEnabled && isAuthenticated && user ? (
        <div className="user-menu">
          <button className="user-menu-btn" onClick={() => navigate('/account')}>
            <span className="user-avatar">
              {user.picture ? (
                <img src={user.picture} alt={user.name || user.email} />
              ) : (
                (user.name || user.email || 'U')[0].toUpperCase()
              )}
            </span>
            <span className="user-name">{user.name || user.email?.split('@')[0]}</span>
          </button>
          <div className="user-dropdown">
            <button onClick={() => navigate('/account')}>Account</button>
            <hr />
            <button onClick={() => logout({ logoutParams: { returnTo: window.location.origin } })}>
              Sign Out
            </button>
          </div>
        </div>
      ) : authEnabled ? (
        <button
          className="btn btn-primary"
          onClick={() => auth0?.loginWithRedirect()}
        >
          Sign In
        </button>
      ) : null}
    </header>
  )
}

// Main Layout
function AppLayout() {
  const location = useLocation()
  const isHome = location.pathname === '/'
  const [showSettings, setShowSettings] = useState(false)
  const { settings, setMode } = useSettings()

  // Home page has its own layout
  if (isHome) {
    return <HomePage />
  }

  return (
    <div className="app">
      <AppHeader onOpenSettings={() => setShowSettings(true)} />

      {/* Settings Modal */}
      {showSettings && (
        <SettingsModal
          settings={settings}
          onModeChange={setMode}
          onClose={() => setShowSettings(false)}
        />
      )}

      <main className="container">
        <Routes>
          <Route path="/editor" element={
            isAuthConfigured() ? (
              <ProtectedRoute>
                <EditorPage />
              </ProtectedRoute>
            ) : (
              <EditorPage />
            )
          } />

          <Route path="/reports" element={
            isAuthConfigured() ? (
              <ProtectedRoute>
                <ReportsPage />
              </ProtectedRoute>
            ) : (
              <ReportsPage />
            )
          } />

          <Route path="/account" element={
            isAuthConfigured() ? (
              <ProtectedRoute>
                <AccountPage />
              </ProtectedRoute>
            ) : (
              <AccountPage />
            )
          } />
        </Routes>
      </main>
    </div>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/*" element={<AppLayout />} />
      </Routes>
    </BrowserRouter>
  )
}
