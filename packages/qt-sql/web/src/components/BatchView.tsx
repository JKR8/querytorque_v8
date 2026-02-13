/**
 * BatchView Component
 * Displays batch processing progress with speedup column
 */

import { useState } from 'react'
import type { BatchFile, BatchSettings } from '@/hooks/useBatchProcessor'
import { exportBatchResults } from '@/utils/zipExport'
import './BatchView.css'

interface BatchViewProps {
  files: BatchFile[]
  isProcessing: boolean
  progress: {
    total: number
    completed: number
    optimized: number
    neutral: number
    regression: number
    failed: number
    percent: number
  }
  settings: BatchSettings
  onStart: () => void
  onAbort: () => void
  onReset: () => void
  onRetry: (id: string) => void
  onRemoveFile: (id: string) => void
  onUpdateSettings: (settings: Partial<BatchSettings>) => void
}

function getStatusIcon(status: string) {
  switch (status) {
    case 'pending':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
        </svg>
      )
    case 'running':
      return (
        <svg className="bv-spinner" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <path d="M12 6v6l4 2" />
        </svg>
      )
    case 'optimized':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <polyline points="9,12 11,14 15,10" />
        </svg>
      )
    case 'neutral':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <line x1="8" y1="12" x2="16" y2="12" />
        </svg>
      )
    case 'regression':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <polyline points="8,10 12,14 16,10" />
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

function formatSpeedup(speedup?: number): string {
  if (speedup == null) return '--'
  if (speedup >= 10) return speedup.toFixed(0) + 'x'
  return speedup.toFixed(2) + 'x'
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

  const canStart = files.length > 0 && !isProcessing && files.some(f => f.status === 'pending') && settings.dsn
  const canExport = files.some(f => f.status === 'optimized')

  return (
    <div className="bv-container">
      {/* Header */}
      <div className="bv-header">
        <div className="bv-header-left">
          <h3>Batch Processing</h3>
          <span className="bv-file-count">{files.length} files</span>
        </div>
        <div className="bv-header-actions">
          {/* DSN input */}
          <input
            type="text"
            className="bv-dsn-input"
            placeholder="Database DSN (postgres://...)"
            value={settings.dsn}
            onChange={(e) => onUpdateSettings({ dsn: e.target.value })}
            disabled={isProcessing}
          />
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
              title={!settings.dsn ? 'Enter a database DSN first' : ''}
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <polygon points="5,3 19,12 5,21 5,3" />
              </svg>
              Run All
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
          <span className="bv-stat optimized">{progress.optimized} optimized</span>
          <span className="bv-stat neutral">{progress.neutral} neutral</span>
          <span className="bv-stat regression">{progress.regression} regression</span>
          <span className="bv-stat failed">{progress.failed} failed</span>
        </div>
      </div>

      {/* File Table */}
      <div className="bv-file-list">
        {/* Table header */}
        <div className="bv-file-item bv-file-header">
          <div className="bv-file-icon" />
          <div className="bv-file-info"><span className="bv-file-name">Filename</span></div>
          <div className="bv-file-speedup">Speedup</div>
          <div className="bv-file-transforms">Transforms</div>
          <div className="bv-file-status"><span>Status</span></div>
          <div className="bv-file-actions" />
        </div>

        {files.map(file => (
          <div key={file.id} className={`bv-file-item ${file.status}`}>
            <div className="bv-file-icon">
              {getStatusIcon(file.status)}
            </div>
            <div className="bv-file-info">
              <span className="bv-file-name">{file.name}</span>
              {file.error && (
                <span className="bv-file-error">{file.error}</span>
              )}
            </div>
            <div className="bv-file-speedup">
              {file.speedup != null && (
                <span className={`bv-speedup-value ${file.status}`}>
                  {formatSpeedup(file.speedup)}
                </span>
              )}
            </div>
            <div className="bv-file-transforms">
              {file.transforms && file.transforms.length > 0 && (
                <div className="bv-transform-tags">
                  {file.transforms.slice(0, 3).map((t, i) => (
                    <span key={i} className="bv-transform-tag">{t}</span>
                  ))}
                  {file.transforms.length > 3 && (
                    <span className="bv-transform-tag">+{file.transforms.length - 3}</span>
                  )}
                </div>
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
          Clear
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
    </div>
  )
}
