import { useState, useRef, useEffect } from 'react'
import CodeEditor from '@/components/CodeEditor'
import ReportViewer from '@/components/ReportViewer'
import {
  analyzeSql,
  startOptimization,
  submitOptimizationResponse,
  acceptOptimization,
  retryOptimization,
  getHealth,
  AnalysisResult,
  OptimizationSession,
} from '@/api/client'
import './EditorPage.css'

type Status = 'idle' | 'analyzing' | 'optimizing' | 'complete' | 'error'
type OptimizeStep = 'idle' | 'validating' | 'preview'

const SQL_PLACEHOLDER = `-- Sample query with anti-patterns for optimization demo
-- Paste your SQL here or use this example

SELECT c.id, c.name, c.email, c.region, c.status,
       o.id AS order_id, o.order_date, o.total_amount,
       p.product_name, p.price, p.category
FROM customers c,
     orders o,
     order_items oi,
     products p
WHERE c.id = o.customer_id
  AND o.id = oi.order_id
  AND oi.product_id = p.id
  AND YEAR(o.order_date) = 2024
  AND UPPER(c.region) = 'NORTH AMERICA'
  AND (c.status = 'active' OR c.status = 'premium')
ORDER BY o.order_date DESC`

export default function EditorPage() {
  const [code, setCode] = useState(SQL_PLACEHOLDER)
  const [status, setStatus] = useState<Status>('idle')
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<AnalysisResult | null>(null)
  const [showResults, setShowResults] = useState(false)

  // Operational mode (auto/manual)
  const [isAutoMode, setIsAutoMode] = useState(false)

  // Optimization state
  const [optimizeStep, setOptimizeStep] = useState<OptimizeStep>('idle')
  const [optimizeSession, setOptimizeSession] = useState<OptimizationSession | null>(null)
  const [isOptimizing, setIsOptimizing] = useState(false)
  const [isAccepting, setIsAccepting] = useState(false)

  // Resizable panel state
  const [editorWidth, setEditorWidth] = useState(50)
  const [isResizing, setIsResizing] = useState(false)
  const layoutRef = useRef<HTMLDivElement>(null)

  // File input ref
  const fileInputRef = useRef<HTMLInputElement>(null)

  // Sync operational mode with backend on mount
  useEffect(() => {
    const syncMode = async () => {
      try {
        const health = await getHealth()
        setIsAutoMode(health.mode === 'auto')
      } catch {
        // Default to manual mode if health check fails
        setIsAutoMode(false)
      }
    }
    syncMode()
  }, [])

  // Handle resize
  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing || !layoutRef.current) return
      const rect = layoutRef.current.getBoundingClientRect()
      const newWidth = ((e.clientX - rect.left) / rect.width) * 100
      setEditorWidth(Math.min(75, Math.max(25, newWidth)))
    }

    const handleMouseUp = () => {
      setIsResizing(false)
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }

    if (isResizing) {
      document.body.style.cursor = 'col-resize'
      document.body.style.userSelect = 'none'
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [isResizing])

  // Listen for postMessage from iframe (manual mode paste section)
  useEffect(() => {
    const handleMessage = async (event: MessageEvent) => {
      if (event.data?.type === 'qt-llm-response' && event.data?.response) {
        await handleValidateResponse(event.data.response)
      }
    }
    window.addEventListener('message', handleMessage)
    return () => window.removeEventListener('message', handleMessage)
  }, [optimizeSession])

  // Handle file upload
  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return

    const reader = new FileReader()
    reader.onload = (event) => {
      const content = event.target?.result as string
      setCode(content)
      setResult(null)
      setShowResults(false)
    }
    reader.readAsText(file)
    e.target.value = ''
  }

  // Analyze SQL
  const handleAnalyze = async () => {
    if (!code.trim()) {
      setError('Please enter SQL code to analyze')
      return
    }

    // Check for qt:optimized marker
    if (code.startsWith('-- qt:optimized')) {
      setError('This code has already been optimized. Remove the marker to re-analyze.')
      return
    }

    setStatus('analyzing')
    setError(null)
    setOptimizeStep('idle')
    setOptimizeSession(null)

    try {
      const analysisResult = await analyzeSql(code, 'query.sql')
      setResult(analysisResult)
      setShowResults(true)
      setStatus('complete')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Analysis failed')
      setStatus('error')
    }
  }

  // Start optimization
  const handleOptimize = async () => {
    if (!result?.original_sql && !code) return

    setError(null)
    setIsOptimizing(true)

    try {
      const session = await startOptimization(
        result?.original_sql || code,
        isAutoMode ? 'auto' : 'manual',
        'query.sql'
      )
      setOptimizeSession(session)

      if (isAutoMode && session.optimized_code) {
        // Auto mode succeeded - show preview
        setOptimizeStep('preview')
      } else if (!isAutoMode) {
        // Manual mode - report includes paste section
        setError('In manual mode, use the paste section at the bottom of the report.')
      } else if (session.errors.length > 0) {
        setError(session.errors[0])
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Optimization failed')
    } finally {
      setIsOptimizing(false)
    }
  }

  // Validate LLM response (manual mode)
  const handleValidateResponse = async (llmResponse: string) => {
    if (!optimizeSession && !result) return

    setOptimizeStep('validating')
    setError(null)

    try {
      // Start a session if we don't have one
      let session = optimizeSession
      if (!session) {
        session = await startOptimization(
          result?.original_sql || code,
          'manual',
          'query.sql'
        )
        setOptimizeSession(session)
      }

      // Submit the response
      const validatedSession = await submitOptimizationResponse(
        session.session_id,
        llmResponse
      )
      setOptimizeSession(validatedSession)
      setOptimizeStep('preview')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Validation failed')
      setOptimizeStep('idle')
    }
  }

  // Accept optimization
  const handleAccept = async () => {
    if (!optimizeSession) return

    setIsAccepting(true)
    setError(null)

    try {
      await acceptOptimization(optimizeSession.session_id)

      // Update code with optimized version
      if (optimizeSession.optimized_code) {
        const marker = '-- qt:optimized\n'
        const optimizedCode = optimizeSession.optimized_code.startsWith('-- qt:')
          ? optimizeSession.optimized_code
          : marker + optimizeSession.optimized_code
        setCode(optimizedCode)
      }

      // Reset state
      setOptimizeStep('idle')
      setOptimizeSession(null)
      setShowResults(false)
      setResult(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to accept optimization')
    } finally {
      setIsAccepting(false)
    }
  }

  // Reject optimization
  const handleReject = () => {
    setOptimizeStep('idle')
    setOptimizeSession(null)
  }

  // Retry optimization
  const handleRetry = async () => {
    if (!optimizeSession || !isAutoMode) {
      setOptimizeStep('idle')
      setOptimizeSession(null)
      setError('Try again by pasting a new response in the report.')
      return
    }

    setIsOptimizing(true)
    setError(null)

    try {
      const session = await retryOptimization(
        optimizeSession.session_id,
        'Please try a different approach'
      )
      setOptimizeSession(session)

      if (session.optimized_code) {
        setOptimizeStep('preview')
      } else {
        setError('Retry failed - no optimized code returned')
        setOptimizeStep('idle')
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Retry failed')
      setOptimizeStep('idle')
    } finally {
      setIsOptimizing(false)
    }
  }

  // Cancel optimization
  const handleCancel = () => {
    setOptimizeStep('idle')
    setOptimizeSession(null)
    setError(null)
  }

  const lineCount = code.split('\n').length

  return (
    <div className="editor-page">
      {/* Mode Controls */}
      <div className="mode-controls">
        <div className="mode-indicator">
          <span className={`mode-badge ${isAutoMode ? 'auto' : 'manual'}`}>
            {isAutoMode ? 'Auto Mode' : 'Manual Mode'}
          </span>
        </div>
      </div>

      <div className="editor-layout" ref={layoutRef}>
        {/* Editor Panel */}
        <div
          className={`editor-panel ${showResults || optimizeStep !== 'idle' ? 'with-results' : ''}`}
          style={showResults || optimizeStep !== 'idle' ? { '--editor-width': `${editorWidth}%` } as React.CSSProperties : undefined}
        >
          <div className="editor-header">
            <div className="editor-title">
              <span className="dot" />
              SQL Query
            </div>
            <div className="editor-actions">
              <input
                ref={fileInputRef}
                type="file"
                accept=".sql,.txt"
                onChange={handleFileSelect}
                style={{ display: 'none' }}
              />
              <button
                className="action-btn"
                onClick={() => fileInputRef.current?.click()}
                title="Upload SQL file"
              >
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                  <polyline points="17,8 12,3 7,8" />
                  <line x1="12" y1="3" x2="12" y2="15" />
                </svg>
                Upload
              </button>
              <button
                className="action-btn primary"
                onClick={handleAnalyze}
                disabled={status === 'analyzing'}
              >
                {status === 'analyzing' ? (
                  <>
                    <span className="spinner" />
                    Analyzing...
                  </>
                ) : (
                  <>
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <polygon points="5,3 19,12 5,21 5,3" />
                    </svg>
                    Analyze
                  </>
                )}
              </button>
            </div>
          </div>

          <div className="editor-body">
            <CodeEditor
              value={code}
              onChange={setCode}
              language="sql"
              placeholder="Enter SQL code..."
            />
          </div>

          <div className="editor-footer">
            <div className="status-left">
              <span className="hint">{lineCount} lines</span>
            </div>
            <div className="status-right">
              {error && <span className="status-error">{error}</span>}
            </div>
          </div>
        </div>

        {/* Resize Handle */}
        {(showResults || optimizeStep !== 'idle') && (
          <div
            className={`resize-handle ${isResizing ? 'dragging' : ''}`}
            onMouseDown={() => setIsResizing(true)}
          />
        )}

        {/* Results Panel */}
        {showResults && result && optimizeStep === 'idle' && (
          <div className="results-panel">
            <div className="results-header">
              <span>Analysis Results</span>
              <div className="results-actions">
                {isAutoMode && (
                  <button
                    className="action-btn optimize-btn"
                    onClick={handleOptimize}
                    disabled={isOptimizing}
                    title="Optimize with AI"
                  >
                    {isOptimizing ? (
                      <>
                        <span className="spinner" />
                        Optimizing...
                      </>
                    ) : (
                      <>
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                          <path d="M12 20h9" />
                          <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" />
                        </svg>
                        Optimize
                      </>
                    )}
                  </button>
                )}
                {!isAutoMode && (
                  <span className="manual-hint">
                    Manual Mode - paste response in report
                  </span>
                )}
                <button
                  className="close-btn"
                  onClick={() => setShowResults(false)}
                  title="Close results"
                >
                  x
                </button>
              </div>
            </div>
            <ReportViewer
              html={result.html}
              fileName={result.file_name}
              score={result.score}
              status={result.status}
            />
          </div>
        )}

        {/* Optimization Loading */}
        {isOptimizing && (
          <div className="optimization-panel loading-panel">
            <div className="optimize-header">
              <h3>Optimizing with AI...</h3>
              <button className="close-btn" onClick={handleCancel}>x</button>
            </div>
            <div className="loading-content">
              <div className="spinner-large" />
              <p>Generating optimizations and validating results...</p>
              <p className="loading-hint">This may take 10-30 seconds.</p>
            </div>
          </div>
        )}

        {/* Optimization Preview */}
        {optimizeStep === 'preview' && optimizeSession && (
          <div className="optimization-panel preview-panel">
            <div className="optimize-header">
              <h3>Optimization Preview</h3>
              <button className="close-btn" onClick={handleCancel}>x</button>
            </div>
            <div className="preview-content">
              {optimizeSession.validation ? (
                <>
                  <div className="validation-summary">
                    <div className={`validation-badge ${optimizeSession.validation.success ? 'pass' : 'fail'}`}>
                      {optimizeSession.validation.success ? 'All Checks Passed' : 'Validation Issues'}
                    </div>
                    {optimizeSession.validation.issues_fixed.length > 0 && (
                      <div className="issues-fixed">
                        Fixed {optimizeSession.validation.issues_fixed.length} issue(s)
                      </div>
                    )}
                  </div>

                  <div className="code-diff">
                    <h4>Optimized SQL</h4>
                    <pre className="optimized-code">
                      {optimizeSession.optimized_code}
                    </pre>
                  </div>

                  <div className="preview-actions">
                    <button
                      className="btn btn-success"
                      onClick={handleAccept}
                      disabled={isAccepting}
                    >
                      {isAccepting ? 'Accepting...' : 'Accept'}
                    </button>
                    {optimizeSession.can_retry && (
                      <button className="btn btn-secondary" onClick={handleRetry}>
                        Retry
                      </button>
                    )}
                    <button className="btn btn-ghost" onClick={handleReject}>
                      Cancel
                    </button>
                  </div>
                </>
              ) : (
                <div className="no-validation">
                  <p>No validation results available.</p>
                  <button className="btn btn-ghost" onClick={handleCancel}>
                    Cancel
                  </button>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
