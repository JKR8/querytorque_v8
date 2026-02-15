/**
 * Batch Settings Modal Component
 * Configure batch processing options
 */

import { useState } from 'react'
import type { BatchSettings } from '@/hooks/useBatchProcessor'
import './BatchSettingsModal.css'

interface BatchSettingsModalProps {
  settings: BatchSettings
  onSave: (settings: BatchSettings) => void
  onClose: () => void
}

export default function BatchSettingsModal({
  settings,
  onSave,
  onClose,
}: BatchSettingsModalProps) {
  const [localSettings, setLocalSettings] = useState<BatchSettings>(settings)

  const handleSave = () => {
    onSave(localSettings)
    onClose()
  }

  return (
    <div className="bs-modal-overlay" onClick={onClose}>
      <div className="bs-modal" onClick={e => e.stopPropagation()}>
        <div className="bs-header">
          <h2>Batch Settings</h2>
          <button className="close-btn" onClick={onClose}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>
        </div>

        <div className="bs-content">
          {/* Optimization Mode */}
          <div className="bs-field">
            <label>Optimization Mode</label>
            <div className="bs-mode-options">
              <button
                className="bs-mode-btn active"
              >
                <span className="bs-mode-icon">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
                  </svg>
                </span>
                <span className="bs-mode-label">Beam</span>
                <span className="bs-mode-desc">Analyst → N workers → validate → snipe</span>
              </button>
            </div>
          </div>

          {/* Max Retries */}
          <div className="bs-field">
            <label>Max Retries</label>
            <p className="bs-hint">
              Number of times to retry failed optimizations
            </p>
            <div className="bs-input-row">
              <input
                type="number"
                min="0"
                max="10"
                value={localSettings.maxRetries}
                onChange={e =>
                  setLocalSettings(s => ({
                    ...s,
                    maxRetries: Math.max(0, Math.min(10, Number(e.target.value))),
                  }))
                }
              />
              <span className="bs-input-suffix">retries</span>
            </div>
          </div>
        </div>

        <div className="bs-footer">
          <button className="btn btn-secondary" onClick={onClose}>
            Cancel
          </button>
          <button className="btn btn-primary" onClick={handleSave}>
            Apply Settings
          </button>
        </div>
      </div>
    </div>
  )
}
