import { useState, useCallback } from 'react'
import {
  startOptimization,
  submitOptimizationResponse,
  acceptOptimization,
  retryOptimization,
  cancelOptimization,
  OptimizationPayload,
  ValidationResult,
} from '../api/client'

interface OptimizationPanelProps {
  measureName: string
  measureCode: string
  onComplete: (optimizedCode: string) => void
  onCancel: () => void
}

type PanelState = 'loading' | 'payload' | 'validating' | 'result' | 'error'

export default function OptimizationPanel({
  measureName,
  measureCode,
  onComplete,
  onCancel,
}: OptimizationPanelProps) {
  const [panelState, setPanelState] = useState<PanelState>('loading')
  const [payload, setPayload] = useState<OptimizationPayload | null>(null)
  const [validation, setValidation] = useState<ValidationResult | null>(null)
  const [llmResponse, setLlmResponse] = useState('')
  const [error, setError] = useState<string | null>(null)

  // Start optimization on mount
  useState(() => {
    startOptimization(measureCode, measureName)
      .then((data) => {
        setPayload(data)
        setPanelState('payload')
      })
      .catch((err) => {
        setError(err.message)
        setPanelState('error')
      })
  })

  const handleValidate = useCallback(async () => {
    if (!payload || !llmResponse.trim()) return

    setPanelState('validating')
    setError(null)

    try {
      const result = await submitOptimizationResponse(payload.session_id, llmResponse)
      setValidation(result)
      setPanelState('result')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Validation failed')
      setPanelState('payload')
    }
  }, [payload, llmResponse])

  const handleAccept = useCallback(async () => {
    if (!payload) return

    try {
      const result = await acceptOptimization(payload.session_id)
      onComplete(result.optimized_code)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to accept')
    }
  }, [payload, onComplete])

  const handleRetry = useCallback(async (feedback?: string) => {
    if (!payload) return

    setPanelState('loading')
    setError(null)

    try {
      const newPayload = await retryOptimization(payload.session_id, feedback)
      setPayload(newPayload)
      setLlmResponse('')
      setValidation(null)
      setPanelState('payload')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Retry failed')
      setPanelState('result')
    }
  }, [payload])

  const handleCancel = useCallback(async () => {
    if (payload) {
      try {
        await cancelOptimization(payload.session_id)
      } catch {
        // Ignore cancel errors
      }
    }
    onCancel()
  }, [payload, onCancel])

  const copyPayload = () => {
    if (payload) {
      navigator.clipboard.writeText(payload.prompt_markdown)
    }
  }

  if (panelState === 'loading') {
    return (
      <div className="optimization-panel">
        <div className="panel-loading">
          <div className="spinner" />
          <p>Preparing optimization payload...</p>
        </div>
        <style>{styles}</style>
      </div>
    )
  }

  if (panelState === 'error') {
    return (
      <div className="optimization-panel">
        <div className="panel-error">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="24" height="24">
            <circle cx="12" cy="12" r="10" />
            <line x1="15" y1="9" x2="9" y2="15" />
            <line x1="9" y1="9" x2="15" y2="15" />
          </svg>
          <h3>Error</h3>
          <p>{error}</p>
          <button className="btn btn-secondary" onClick={onCancel}>
            Go Back
          </button>
        </div>
        <style>{styles}</style>
      </div>
    )
  }

  if (panelState === 'validating') {
    return (
      <div className="optimization-panel">
        <div className="panel-loading">
          <div className="spinner" />
          <p>Validating LLM response...</p>
        </div>
        <style>{styles}</style>
      </div>
    )
  }

  if (panelState === 'result' && validation) {
    return (
      <div className="optimization-panel">
        <div className="panel-header">
          <h2>Validation Results</h2>
        </div>

        <div className="validation-summary">
          <div className={`validation-badge ${validation.all_passed ? 'success' : 'warning'}`}>
            {validation.all_passed ? 'All Checks Passed' : 'Some Checks Failed'}
          </div>

          <div className="validation-checks">
            <div className={`check ${validation.syntax_status}`}>
              <span className="check-label">Syntax</span>
              <span className="check-status">{validation.syntax_status.toUpperCase()}</span>
            </div>
            <div className={`check ${validation.regression_status}`}>
              <span className="check-label">Regression</span>
              <span className="check-status">{validation.regression_status.toUpperCase()}</span>
            </div>
          </div>

          {validation.syntax_errors.length > 0 && (
            <div className="error-list">
              <h4>Syntax Errors</h4>
              <ul>
                {validation.syntax_errors.map((err, i) => (
                  <li key={i}>{err}</li>
                ))}
              </ul>
            </div>
          )}

          {validation.issues_fixed.length > 0 && (
            <div className="issues-fixed">
              <h4>Issues Fixed ({validation.issues_fixed.length})</h4>
              <ul>
                {validation.issues_fixed.map((issue, i) => (
                  <li key={i}>{issue.title}</li>
                ))}
              </ul>
            </div>
          )}

          {validation.new_issues.length > 0 && (
            <div className="new-issues">
              <h4>New Issues ({validation.new_issues.length})</h4>
              <ul>
                {validation.new_issues.map((issue, i) => (
                  <li key={i}>{issue.title}</li>
                ))}
              </ul>
            </div>
          )}
        </div>

        <div className="code-diff">
          <h4>Changes</h4>
          <div
            className="diff-content"
            dangerouslySetInnerHTML={{ __html: validation.diff_html }}
          />
        </div>

        <div className="panel-actions">
          {validation.all_passed ? (
            <button className="btn btn-success" onClick={handleAccept}>
              Accept Changes
            </button>
          ) : null}

          {validation.can_retry && (
            <button className="btn btn-secondary" onClick={() => handleRetry()}>
              Retry ({validation.retry_count}/{validation.max_retries})
            </button>
          )}

          <button className="btn btn-ghost" onClick={handleCancel}>
            Cancel
          </button>
        </div>

        {error && <div className="inline-error">{error}</div>}

        <style>{styles}</style>
      </div>
    )
  }

  // Payload view (default)
  return (
    <div className="optimization-panel">
      <div className="panel-header">
        <h2>Optimize: {measureName}</h2>
      </div>

      <div className="payload-section">
        <div className="section-header">
          <h3>LLM Prompt</h3>
          <button className="btn btn-ghost btn-sm" onClick={copyPayload}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
              <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
            </svg>
            Copy
          </button>
        </div>
        <pre className="payload-content">{payload?.prompt_markdown}</pre>
      </div>

      <div className="issues-summary">
        <h3>Issues to Fix ({payload?.issues_summary.length || 0})</h3>
        <ul>
          {payload?.issues_summary.map((issue, i) => (
            <li key={i} className={`issue-item severity-${issue.severity}`}>
              <span className="issue-severity">{issue.severity}</span>
              <span className="issue-title">{issue.title}</span>
            </li>
          ))}
        </ul>
      </div>

      <div className="response-section">
        <h3>Paste LLM Response</h3>
        <textarea
          className="response-input"
          placeholder="Paste the LLM response here..."
          value={llmResponse}
          onChange={(e) => setLlmResponse(e.target.value)}
        />
      </div>

      <div className="panel-actions">
        <button
          className="btn btn-primary"
          onClick={handleValidate}
          disabled={!llmResponse.trim()}
        >
          Validate Response
        </button>
        <button className="btn btn-ghost" onClick={handleCancel}>
          Cancel
        </button>
      </div>

      {error && <div className="inline-error">{error}</div>}

      <style>{styles}</style>
    </div>
  )
}

const styles = `
  .optimization-panel {
    background: var(--qt-bg-card);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius);
    padding: var(--qt-space-lg);
  }

  .panel-header {
    margin-bottom: var(--qt-space-lg);
    padding-bottom: var(--qt-space-md);
    border-bottom: 1px solid var(--qt-border);
  }

  .panel-header h2 {
    font-size: var(--qt-text-xl);
    font-weight: var(--qt-font-semibold);
    margin: 0;
  }

  .panel-loading, .panel-error {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: var(--qt-space-2xl);
    text-align: center;
  }

  .panel-loading p, .panel-error p {
    margin-top: var(--qt-space-md);
    color: var(--qt-fg-muted);
  }

  .panel-error svg {
    color: var(--qt-critical);
  }

  .panel-error h3 {
    color: var(--qt-critical);
    margin-top: var(--qt-space-md);
    margin-bottom: var(--qt-space-sm);
  }

  .section-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: var(--qt-space-sm);
  }

  .section-header h3 {
    font-size: var(--qt-text-md);
    font-weight: var(--qt-font-semibold);
    margin: 0;
  }

  .payload-section {
    margin-bottom: var(--qt-space-lg);
  }

  .payload-content {
    background: var(--qt-bg-code);
    color: var(--qt-fg-code);
    padding: var(--qt-space-md);
    border-radius: var(--qt-radius);
    font-family: var(--qt-font-mono);
    font-size: var(--qt-text-sm);
    max-height: 300px;
    overflow: auto;
    white-space: pre-wrap;
    word-break: break-word;
  }

  .issues-summary {
    margin-bottom: var(--qt-space-lg);
  }

  .issues-summary h3 {
    font-size: var(--qt-text-md);
    font-weight: var(--qt-font-semibold);
    margin-bottom: var(--qt-space-sm);
  }

  .issues-summary ul {
    list-style: none;
    padding: 0;
    margin: 0;
  }

  .issue-item {
    display: flex;
    align-items: center;
    gap: var(--qt-space-sm);
    padding: var(--qt-space-sm);
    background: var(--qt-bg-alt);
    border-radius: var(--qt-radius-sm);
    margin-bottom: var(--qt-space-xs);
  }

  .issue-severity {
    font-size: var(--qt-text-xs);
    font-weight: var(--qt-font-semibold);
    padding: 0.125rem 0.5rem;
    border-radius: var(--qt-radius-full);
    text-transform: uppercase;
  }

  .severity-critical .issue-severity {
    background: var(--qt-critical);
    color: white;
  }

  .severity-high .issue-severity {
    background: var(--qt-high);
    color: white;
  }

  .severity-medium .issue-severity {
    background: var(--qt-medium);
    color: #422006;
  }

  .severity-low .issue-severity {
    background: var(--qt-low);
    color: white;
  }

  .issue-title {
    font-size: var(--qt-text-sm);
  }

  .response-section {
    margin-bottom: var(--qt-space-lg);
  }

  .response-section h3 {
    font-size: var(--qt-text-md);
    font-weight: var(--qt-font-semibold);
    margin-bottom: var(--qt-space-sm);
  }

  .response-input {
    width: 100%;
    min-height: 200px;
    padding: var(--qt-space-md);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius);
    font-family: var(--qt-font-mono);
    font-size: var(--qt-text-sm);
    resize: vertical;
    background: var(--qt-bg-card);
    color: var(--qt-fg);
  }

  .response-input:focus {
    outline: none;
    border-color: var(--qt-brand);
    box-shadow: 0 0 0 3px var(--qt-brand-light);
  }

  .panel-actions {
    display: flex;
    gap: var(--qt-space-md);
    padding-top: var(--qt-space-md);
    border-top: 1px solid var(--qt-border);
  }

  .inline-error {
    margin-top: var(--qt-space-md);
    padding: var(--qt-space-sm);
    background: var(--qt-critical-bg);
    border: 1px solid var(--qt-critical-border);
    border-radius: var(--qt-radius-sm);
    color: var(--qt-critical);
    font-size: var(--qt-text-sm);
  }

  .validation-summary {
    margin-bottom: var(--qt-space-lg);
  }

  .validation-badge {
    display: inline-block;
    padding: var(--qt-space-sm) var(--qt-space-md);
    border-radius: var(--qt-radius);
    font-weight: var(--qt-font-semibold);
    margin-bottom: var(--qt-space-md);
  }

  .validation-badge.success {
    background: var(--qt-low-bg);
    color: var(--qt-low);
  }

  .validation-badge.warning {
    background: var(--qt-medium-bg);
    color: var(--qt-medium);
  }

  .validation-checks {
    display: flex;
    gap: var(--qt-space-md);
    margin-bottom: var(--qt-space-md);
  }

  .check {
    display: flex;
    align-items: center;
    gap: var(--qt-space-sm);
    padding: var(--qt-space-sm) var(--qt-space-md);
    background: var(--qt-bg-alt);
    border-radius: var(--qt-radius-sm);
  }

  .check.pass .check-status {
    color: var(--qt-low);
  }

  .check.fail .check-status {
    color: var(--qt-critical);
  }

  .check.skip .check-status {
    color: var(--qt-fg-muted);
  }

  .check-label {
    font-size: var(--qt-text-sm);
    color: var(--qt-fg-muted);
  }

  .check-status {
    font-size: var(--qt-text-xs);
    font-weight: var(--qt-font-semibold);
  }

  .error-list, .issues-fixed, .new-issues {
    margin-bottom: var(--qt-space-md);
  }

  .error-list h4, .issues-fixed h4, .new-issues h4 {
    font-size: var(--qt-text-sm);
    font-weight: var(--qt-font-semibold);
    margin-bottom: var(--qt-space-sm);
  }

  .error-list ul, .issues-fixed ul, .new-issues ul {
    list-style: none;
    padding: 0;
    margin: 0;
  }

  .error-list li {
    padding: var(--qt-space-xs) var(--qt-space-sm);
    background: var(--qt-critical-bg);
    color: var(--qt-critical);
    border-radius: var(--qt-radius-sm);
    font-size: var(--qt-text-sm);
    margin-bottom: var(--qt-space-xs);
  }

  .issues-fixed li {
    padding: var(--qt-space-xs) var(--qt-space-sm);
    background: var(--qt-low-bg);
    color: var(--qt-low);
    border-radius: var(--qt-radius-sm);
    font-size: var(--qt-text-sm);
    margin-bottom: var(--qt-space-xs);
  }

  .new-issues li {
    padding: var(--qt-space-xs) var(--qt-space-sm);
    background: var(--qt-high-bg);
    color: var(--qt-high);
    border-radius: var(--qt-radius-sm);
    font-size: var(--qt-text-sm);
    margin-bottom: var(--qt-space-xs);
  }

  .code-diff {
    margin-bottom: var(--qt-space-lg);
  }

  .code-diff h4 {
    font-size: var(--qt-text-sm);
    font-weight: var(--qt-font-semibold);
    margin-bottom: var(--qt-space-sm);
  }

  .diff-content {
    background: var(--qt-bg-code);
    padding: var(--qt-space-md);
    border-radius: var(--qt-radius);
    font-family: var(--qt-font-mono);
    font-size: var(--qt-text-sm);
    overflow-x: auto;
  }

  .btn {
    display: inline-flex;
    align-items: center;
    gap: var(--qt-space-sm);
    padding: var(--qt-space-sm) var(--qt-space-md);
    font-size: var(--qt-text-sm);
    font-weight: var(--qt-font-medium);
    border: none;
    border-radius: var(--qt-radius-sm);
    cursor: pointer;
    transition: all var(--qt-transition-fast);
  }

  .btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .btn-primary {
    background: var(--qt-brand);
    color: white;
  }

  .btn-primary:hover:not(:disabled) {
    filter: brightness(1.1);
  }

  .btn-secondary {
    background: var(--qt-bg-alt);
    color: var(--qt-fg);
    border: 1px solid var(--qt-border);
  }

  .btn-secondary:hover {
    background: var(--qt-border);
  }

  .btn-success {
    background: var(--qt-low);
    color: white;
  }

  .btn-success:hover {
    filter: brightness(1.1);
  }

  .btn-ghost {
    background: transparent;
    color: var(--qt-fg-muted);
  }

  .btn-ghost:hover {
    background: var(--qt-bg-alt);
    color: var(--qt-fg);
  }

  .btn-sm {
    padding: var(--qt-space-xs) var(--qt-space-sm);
    font-size: var(--qt-text-xs);
  }
`
