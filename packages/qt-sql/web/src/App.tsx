import { useState } from 'react'
import { BrowserRouter, Routes, Route, useNavigate, useLocation } from 'react-router-dom'

// Pages
import HomePage from './pages/HomePage'
import EditorPage from './pages/EditorPage'
import BatchPage from './pages/BatchPage'

// Components
import SettingsModal from './components/SettingsModal'

// Hooks
import useSettings from './hooks/useSettings'

// App Header
function AppHeader({ onOpenSettings }: { onOpenSettings: () => void }) {
  const navigate = useNavigate()
  const location = useLocation()
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
          className={isActive('/batch') ? 'active' : ''}
          onClick={() => navigate('/batch')}
        >
          Batch
        </a>
      </nav>

      <div className="header-spacer" />

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
    </header>
  )
}

// Main Layout
function AppLayout() {
  const location = useLocation()
  const isHome = location.pathname === '/'
  const [showSettings, setShowSettings] = useState(false)
  const { settings } = useSettings()

  if (isHome) {
    return <HomePage />
  }

  return (
    <div className="app">
      <AppHeader onOpenSettings={() => setShowSettings(true)} />

      {showSettings && (
        <SettingsModal
          settings={settings}
          onClose={() => setShowSettings(false)}
        />
      )}

      <main className="container">
        <Routes>
          <Route path="/editor" element={<EditorPage />} />
          <Route path="/batch" element={<BatchPage />} />
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
