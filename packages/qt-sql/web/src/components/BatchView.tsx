/**
 * BatchView Component
 * Displays batch processing progress and file list
 */

import { useState } from 'react'
import type { BatchFile, BatchSettings } from '@/hooks/useBatchProcessor'
import { exportBatchResults } from '@/utils/zipExport'
import BatchSettingsModal from './BatchSettingsModal'
import './BatchView.css'

interface BatchViewProps {
  files: BatchFile[]
  isProcessing: boolean
  progress: {
    total: number
    completed: number
    fixed: number
    skipped: number
    failed: number
    percent: number
  }
  settings: BatchSettings
  onStart: () => void
  onAbort: () => void
  onReset: () => void
  onRetry: (id: string) => void
  onRemoveFile: (id: string) => void
  onUpdateSettings: (settings: BatchSettings) => void
}

function getStatusIcon(status: string) {
  switch (status) {
    case 'pending':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
        </svg>
      )
    case 'analyzing':
    case 'optimizing':
      return (
        <svg className="bv-spinner" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <path d="M12 6v6l4 2" />
        </svg>
      )
    case 'fixed':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <polyline points="9,12 11,14 15,10" />
        </svg>
      )
    case 'skipped':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <line x1="8" y1="12" x2="16" y2="12" />
        </svg>
      )
    case 'failed':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <line x1="15" y1="9" x2="9" y2="15" />
          <line x1="9" y1="9" x2="15" y2="15" />
        </svg>
      )
    default:
      return null
  }
}

export default function BatchView({
  files,
  isProcessing,
  progress,
  settings,
  onStart,
  onAbort,
  onReset,
  onRetry,
  onRemoveFile,
  onUpdateSettings,
}: BatchViewProps) {
  const [showSettings, setShowSettings] = useState(false)
  const [isExporting, setIsExporting] = useState(false)

  const handleExport = async () => {
    setIsExporting(true)
    try {
      await exportBatchResults(files)
    } catch (err) {
      console.error('Export failed:', err)
    } finally {
      setIsExporting(false)
    }
  }

  const canStart = files.length > 0 && !isProcessing && files.some(f => f.status === 'pending')
  const canExport = files.some(f => f.status === 'fixed')

  return (
    <div className="bv-container">
      {/* Header */}
      <div className="bv-header">
        <div className="bv-header-left">
          <h3>Batch Processing</h3>
          <span className="bv-file-count">{files.length} files</span>
        </div>
        <div className="bv-header-actions">
          <button
            className="action-btn"
            onClick={() => setShowSettings(true)}
            disabled={isProcessing}
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <circle cx="12" cy="12" r="3" />
              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
            </svg>
            Settings
          </button>
          {isProcessing ? (
            <button className="action-btn" onClick={onAbort}>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <rect x="6" y="6" width="12" height="12" />
              </svg>
              Stop
            </button>
          ) : (
            <button
              className="action-btn primary"
              onClick={onStart}
              disabled={!canStart}
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <polygon points="5,3 19,12 5,21 5,3" />
              </svg>
              Start
            </button>
          )}
        </div>
      </div>

      {/* Progress Bar */}
      <div className="bv-progress">
        <div className="bv-progress-bar">
          <div
            className="bv-progress-fill"
            style={{ width: `${progress.percent}%` }}
          />
        </div>
        <div className="bv-progress-stats">
          <span>{progress.completed}/{progress.total} completed</span>
          <span className="bv-stat fixed">{progress.fixed} fixed</span>
          <span className="bv-stat skipped">{progress.skipped} skipped</span>
          <span className="bv-stat failed">{progress.failed} failed</span>
        </div>
      </div>

      {/* File List */}
      <div className="bv-file-list">
        {files.map(file => (
          <div key={file.id} className={`bv-file-item ${file.status}`}>
            <div className="bv-file-icon">
              {getStatusIcon(file.status)}
            </div>
            <div className="bv-file-info">
              <span className="bv-file-name">{file.name}</span>
              {file.score != null && (
                <span className="bv-file-score">Score: {file.score}</span>
              )}
              {file.error && (
                <span className="bv-file-error">{file.error}</span>
              )}
            </div>
            <div className="bv-file-status">
              <span className={`bv-status-badge ${file.status}`}>
                {file.status}
              </span>
              {file.retryCount > 0 && (
                <span className="bv-retry-count">
                  Retry {file.retryCount}/{settings.maxRetries}
                </span>
              )}
            </div>
            <div className="bv-file-actions">
              {file.status === 'failed' && file.retryCount < settings.maxRetries && (
                <button
                  className="bv-retry-btn"
                  onClick={() => onRetry(file.id)}
                  disabled={isProcessing}
                  title="Retry"
                >
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <polyline points="1,4 1,10 7,10" />
                    <path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10" />
                  </svg>
                </button>
              )}
              {!isProcessing && (
                <button
                  className="bv-remove-btn"
                  onClick={() => onRemoveFile(file.id)}
                  title="Remove"
                >
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <line x1="18" y1="6" x2="6" y2="18" />
                    <line x1="6" y1="6" x2="18" y2="18" />
                  </svg>
                </button>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Footer Actions */}
      <div className="bv-footer">
        <button
          className="action-btn"
          onClick={onReset}
          disabled={isProcessing}
        >
          Clear All
        </button>
        <button
          className="action-btn primary"
          onClick={handleExport}
          disabled={!canExport || isExporting}
        >
          {isExporting ? (
            <>
              <span className="spinner" />
              Exporting...
            </>
          ) : (
            <>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                <polyline points="7,10 12,15 17,10" />
                <line x1="12" y1="15" x2="12" y2="3" />
              </svg>
              Export ZIP
            </>
          )}
        </button>
      </div>

      {/* Settings Modal */}
      {showSettings && (
        <BatchSettingsModal
          settings={settings}
          onSave={onUpdateSettings}
          onClose={() => setShowSettings(false)}
        />
      )}
    </div>
  )
}
