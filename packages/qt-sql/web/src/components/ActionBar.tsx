/**
 * ActionBar Component
 * Bottom action bar for editor panel — Audit, Optimize, Config Boost buttons
 */

import './ActionBar.css'

interface ActionBarProps {
  dbConnected: boolean
  llmConfigured: boolean
  isAuditing: boolean
  isOptimizing: boolean
  hasOptimizeResult: boolean
  onAudit: () => void
  onOptimize: () => void
  onConfigBoost?: () => void
  disabled?: boolean
}

export default function ActionBar({
  dbConnected,
  llmConfigured,
  isAuditing,
  isOptimizing,
  hasOptimizeResult,
  onAudit,
  onOptimize,
  onConfigBoost,
  disabled,
}: ActionBarProps) {
  return (
    <div className="action-bar">
      {/* Audit — always available when DB connected */}
      <button
        className="action-bar-btn audit"
        onClick={onAudit}
        disabled={!dbConnected || isAuditing || disabled}
        title={!dbConnected ? 'Connect a database first' : 'Analyze execution plan (free, no LLM)'}
      >
        {isAuditing ? (
          <span className="action-bar-spinner" />
        ) : (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
        )}
        <span>Audit</span>
        <span className="action-bar-micro-badge free">FREE</span>
      </button>

      {/* Optimize — requires DB + LLM */}
      <button
        className="action-bar-btn optimize"
        onClick={onOptimize}
        disabled={!dbConnected || !llmConfigured || isOptimizing || disabled}
        title={
          !dbConnected
            ? 'Connect a database first'
            : !llmConfigured
            ? 'Configure LLM provider in Settings'
            : 'Run AI-powered optimization'
        }
      >
        {isOptimizing ? (
          <span className="action-bar-spinner" />
        ) : (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
            <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
          </svg>
        )}
        <span>Optimize</span>
        <span className="action-bar-micro-badge ai">AI</span>
      </button>

      {/* Config Boost — only after successful optimization */}
      {hasOptimizeResult && onConfigBoost && (
        <button
          className="action-bar-btn config-boost"
          onClick={onConfigBoost}
          disabled={disabled}
          title="Apply SET LOCAL config tuning for additional speedup"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
            <polyline points="22,12 18,12 15,21 9,3 6,12 2,12" />
          </svg>
          <span>Config Boost</span>
        </button>
      )}
    </div>
  )
}
