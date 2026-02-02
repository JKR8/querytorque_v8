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
          {/* Auto-fix Threshold */}
          <div className="bs-field">
            <label>Auto-Fix Threshold</label>
            <p className="bs-hint">
              Queries with Torque Score below this value will be optimized
            </p>
            <div className="bs-slider-container">
              <input
                type="range"
                min="0"
                max="100"
                value={localSettings.autoFixThreshold}
                onChange={e =>
                  setLocalSettings(s => ({
                    ...s,
                    autoFixThreshold: Number(e.target.value),
                  }))
                }
              />
              <div className="bs-slider-labels">
                <span>0</span>
                <span className="bs-slider-value">{localSettings.autoFixThreshold}</span>
                <span>100</span>
              </div>
            </div>
            <div className="bs-threshold-preview">
              {localSettings.autoFixThreshold === 0 && (
                <span className="bs-preview-hint">All queries will be skipped</span>
              )}
              {localSettings.autoFixThreshold === 100 && (
                <span className="bs-preview-hint">All queries will be optimized</span>
              )}
              {localSettings.autoFixThreshold > 0 && localSettings.autoFixThreshold < 100 && (
                <span className="bs-preview-hint">
                  Optimize queries scoring below {localSettings.autoFixThreshold}
                </span>
              )}
            </div>
          </div>

          {/* Mode Selection */}
          <div className="bs-field">
            <label>Optimization Mode</label>
            <div className="bs-mode-options">
              <button
                className={`bs-mode-btn ${localSettings.mode === 'auto' ? 'active' : ''}`}
                onClick={() => setLocalSettings(s => ({ ...s, mode: 'auto' }))}
              >
                <span className="bs-mode-icon">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
                  </svg>
                </span>
                <span className="bs-mode-label">Auto</span>
                <span className="bs-mode-desc">Use configured LLM</span>
              </button>
              <button
                className={`bs-mode-btn ${localSettings.mode === 'manual' ? 'active' : ''}`}
                onClick={() => setLocalSettings(s => ({ ...s, mode: 'manual' }))}
              >
                <span className="bs-mode-icon">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M12 20h9" />
                    <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" />
                  </svg>
                </span>
                <span className="bs-mode-label">Manual</span>
                <span className="bs-mode-desc">Analyze only</span>
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
