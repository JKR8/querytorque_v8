/**
 * Settings Modal Component
 * Shows LLM provider status and connection test
 */

import { useState } from 'react'
import { getHealth } from '@/api/client'
import type { AppSettings } from '@/hooks/useSettings'
import './SettingsModal.css'

interface SettingsModalProps {
  settings: AppSettings
  onClose: () => void
}

export default function SettingsModal({
  settings,
  onClose,
}: SettingsModalProps) {
  const [isTesting, setIsTesting] = useState(false)
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null)

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
              Configure LLM provider via environment variables (QT_LLM_PROVIDER, QT_*_API_KEY).
              The Optimize button requires an LLM provider. Audit mode works without one.
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
