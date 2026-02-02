/**
 * Settings Modal Component
 * Provides mode toggle (Auto/Manual) and LLM provider configuration
 */

import { useState } from 'react'
import { getHealth } from '@/api/client'
import type { AppSettings } from '@/hooks/useSettings'
import './SettingsModal.css'

interface SettingsModalProps {
  settings: AppSettings
  onModeChange: (mode: 'auto' | 'manual') => void
  onClose: () => void
}

export default function SettingsModal({
  settings,
  onModeChange,
  onClose,
}: SettingsModalProps) {
  const [mode, setMode] = useState(settings.mode)
  const [isTesting, setIsTesting] = useState(false)
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null)

  const handleModeChange = (newMode: 'auto' | 'manual') => {
    setMode(newMode)
    onModeChange(newMode)
  }

  const handleTestConnection = async () => {
    setIsTesting(true)
    setTestResult(null)

    try {
      const health = await getHealth()
      if (health.llm_configured) {
        setTestResult({
          success: true,
          message: `Connected to ${health.llm_provider || 'LLM provider'}`,
        })
      } else {
        setTestResult({
          success: false,
          message: 'No LLM provider configured',
        })
      }
    } catch (err) {
      setTestResult({
        success: false,
        message: err instanceof Error ? err.message : 'Connection failed',
      })
    } finally {
      setIsTesting(false)
    }
  }

  return (
    <div className="settings-modal-overlay" onClick={onClose}>
      <div className="settings-modal" onClick={e => e.stopPropagation()}>
        <div className="settings-header">
          <h2>Settings</h2>
          <button className="close-btn" onClick={onClose}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>

        <div className="settings-content">
          {/* Optimization Mode */}
          <div className="settings-section">
            <h3>Optimization Mode</h3>
            <p className="settings-description">
              Choose how SQL optimization is handled
            </p>

            <div className="mode-toggle">
              <button
                className={`mode-option ${mode === 'auto' ? 'active' : ''}`}
                onClick={() => handleModeChange('auto')}
                disabled={!settings.llmConfigured}
              >
                <div className="mode-option-header">
                  <span className="mode-icon">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
                    </svg>
                  </span>
                  <span className="mode-name">Auto Mode</span>
                </div>
                <p className="mode-desc">
                  Automatically optimize SQL using configured LLM provider
                </p>
              </button>

              <button
                className={`mode-option ${mode === 'manual' ? 'active' : ''}`}
                onClick={() => handleModeChange('manual')}
              >
                <div className="mode-option-header">
                  <span className="mode-icon">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M12 20h9" />
                      <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" />
                    </svg>
                  </span>
                  <span className="mode-name">Manual Mode</span>
                </div>
                <p className="mode-desc">
                  Copy prompts to your own LLM and paste responses for validation
                </p>
              </button>
            </div>

            {!settings.llmConfigured && mode === 'manual' && (
              <div className="settings-info">
                Auto mode requires an LLM provider to be configured on the server.
              </div>
            )}
          </div>

          {/* LLM Provider Status */}
          <div className="settings-section">
            <h3>LLM Provider</h3>
            <div className="provider-status">
              <div className="provider-info">
                <span className={`status-dot ${settings.llmConfigured ? 'connected' : 'disconnected'}`} />
                <span className="provider-name">
                  {settings.llmProvider || 'Not configured'}
                </span>
              </div>
              <button
                className="test-btn"
                onClick={handleTestConnection}
                disabled={isTesting}
              >
                {isTesting ? 'Testing...' : 'Test Connection'}
              </button>
            </div>

            {testResult && (
              <div className={`test-result ${testResult.success ? 'success' : 'error'}`}>
                {testResult.message}
              </div>
            )}

            <p className="settings-hint">
              Configure LLM provider via environment variables (QT_LLM_PROVIDER, QT_*_API_KEY)
            </p>
          </div>

          {/* Calcite Status */}
          <div className="settings-section">
            <h3>Calcite Optimizer</h3>
            <div className="provider-status">
              <div className="provider-info">
                <span className={`status-dot ${settings.calciteAvailable ? 'connected' : 'disconnected'}`} />
                <span className="provider-name">
                  {settings.calciteAvailable ? 'Available' : 'Not available'}
                </span>
              </div>
            </div>
            <p className="settings-hint">
              Calcite provides algebraic SQL optimization before LLM processing
            </p>
          </div>
        </div>

        <div className="settings-footer">
          <button className="btn btn-secondary" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  )
}
