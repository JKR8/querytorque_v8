import { useState, useRef, useEffect } from 'react'
import CodeEditor from '@/components/CodeEditor'
import ReportViewer from '@/components/ReportViewer'
import ValidationReport from '@/components/ValidationReport'
import QueryResults, { QueryResultData } from '@/components/QueryResults'
import DatabaseConnection, { DatabaseConfig, ConnectionStatus } from '@/components/DatabaseConnection'
import BatchView from '@/components/BatchView'
import PlanViewer from '@/components/PlanViewer'
import useBatchProcessor from '@/hooks/useBatchProcessor'
import {
  analyzeSql,
  startOptimization,
  retryOptimization,
  getHealth,
  validateManualResponse,
  connectDuckDB,
  executeQuery,
  getExecutionPlan,
  disconnectDatabase,
  AnalysisResult,
  OptimizationSession,
  ValidationPreviewResponse,
  ExecutionPlanResponse,
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
  const [validationPreview, setValidationPreview] = useState<ValidationPreviewResponse | null>(null)

  // Database connection state
  const [dbSessionId, setDbSessionId] = useState<string | null>(null)
  const [dbStatus, setDbStatus] = useState<ConnectionStatus>({ connected: false })
  const [isConnecting, setIsConnecting] = useState(false)
  const [showDbConnect, setShowDbConnect] = useState(false)

  // Query execution state
  const [queryResult, setQueryResult] = useState<QueryResultData | null>(null)
  const [isExecuting, setIsExecuting] = useState(false)
  const [showQueryResults, setShowQueryResults] = useState(false)

  // Execution plan state
  const [planResult, setPlanResult] = useState<ExecutionPlanResponse | null>(null)
  const [isExplaining, setIsExplaining] = useState(false)
  const [showPlanResult, setShowPlanResult] = useState(false)

  // Batch processing
  const [isBatchMode, setIsBatchMode] = useState(false)
  const batch = useBatchProcessor()

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

  // Handle file upload (single or multiple)
  const handleFileSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || [])
    if (files.length === 0) return

    if (files.length === 1) {
      // Single file mode
      const reader = new FileReader()
      reader.onload = (event) => {
        const content = event.target?.result as string
        setCode(content)
        setResult(null)
        setShowResults(false)
        setIsBatchMode(false)
      }
      reader.readAsText(files[0])
    } else {
      // Multiple files - enter batch mode
      await batch.addFiles(files)
      setIsBatchMode(true)
    }

    e.target.value = ''
  }

  // Handle file drop for batch mode
  const handleFileDrop = async (e: React.DragEvent) => {
    e.preventDefault()
    const files = Array.from(e.dataTransfer.files).filter(
      f => f.name.endsWith('.sql') || f.name.endsWith('.txt')
    )

    if (files.length === 0) return

    if (files.length === 1 && !isBatchMode) {
      // Single file mode
      const reader = new FileReader()
      reader.onload = (event) => {
        const content = event.target?.result as string
        setCode(content)
        setResult(null)
        setShowResults(false)
      }
      reader.readAsText(files[0])
    } else {
      // Multiple files - enter batch mode
      await batch.addFiles(files)
      setIsBatchMode(true)
    }
  }

  // Exit batch mode
  const handleExitBatch = () => {
    batch.reset()
    setIsBatchMode(false)
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
    if (!result?.original_sql && !code) return

    setOptimizeStep('validating')
    setError(null)

    try {
      const validationResult = await validateManualResponse(
        result?.original_sql || code,
        llmResponse
      )
      setValidationPreview(validationResult)
      setOptimizeStep('preview')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Validation failed')
      setOptimizeStep('idle')
    }
  }

  // Accept optimization
  const handleAccept = async () => {
    if (!validationPreview?.optimized_code) return

    setIsAccepting(true)
    setError(null)

    try {
      const marker = '-- qt:optimized\n'
      const optimizedCode = validationPreview.optimized_code.startsWith('-- qt:')
        ? validationPreview.optimized_code
        : marker + validationPreview.optimized_code
      setCode(optimizedCode)

      // Reset state
      setOptimizeStep('idle')
      setValidationPreview(null)
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
    setValidationPreview(null)
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

  // Database connection
  const handleDbConnect = async (config: DatabaseConfig) => {
    setIsConnecting(true)
    setError(null)

    try {
      if (config.type === 'duckdb' && config.fixtureFile) {
        const response = await connectDuckDB(config.fixtureFile)

        if (response.connected) {
          setDbSessionId(response.session_id)
          setDbStatus({
            connected: true,
            type: 'duckdb',
            details: response.details,
          })
          setShowDbConnect(false)
        } else {
          setError(response.error || 'Connection failed')
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Connection failed')
    } finally {
      setIsConnecting(false)
    }
  }

  const handleDbDisconnect = async () => {
    if (dbSessionId) {
      try {
        await disconnectDatabase(dbSessionId)
      } catch {
        // Ignore disconnect errors
      }
    }
    setDbSessionId(null)
    setDbStatus({ connected: false })
    setQueryResult(null)
    setPlanResult(null)
    setShowQueryResults(false)
  }

  // Execute query
  const handleExecute = async () => {
    if (!dbSessionId || !code.trim()) return

    setIsExecuting(true)
    setError(null)
    setQueryResult(null)

    try {
      const result = await executeQuery(dbSessionId, code, 100)
      setQueryResult(result)
      setShowQueryResults(true)
      setShowResults(false) // Hide analysis results to show query results
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Execution failed')
    } finally {
      setIsExecuting(false)
    }
  }

  // Get execution plan
  const handleExplain = async () => {
    if (!dbSessionId || !code.trim()) return

    setIsExplaining(true)
    setError(null)
    setPlanResult(null)

    try {
      const result = await getExecutionPlan(dbSessionId, code, true)
      setPlanResult(result)
      setShowPlanResult(true)
      setShowResults(false)
      setShowQueryResults(false)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Explain failed')
    } finally {
      setIsExplaining(false)
    }
  }

  const lineCount = code.split('\n').length

  // Batch mode view
  if (isBatchMode) {
    return (
      <div className="editor-page batch-mode">
        <div className="mode-controls">
          <div className="mode-indicator">
            <span className="mode-badge batch">Batch Mode</span>
          </div>
          <button className="action-btn" onClick={handleExitBatch}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
            Exit Batch
          </button>
        </div>

        <div className="batch-container">
          <BatchView
            files={batch.files}
            isProcessing={batch.isProcessing}
            progress={batch.progress}
            settings={batch.settings}
            onStart={batch.start}
            onAbort={batch.abort}
            onReset={batch.reset}
            onRetry={batch.retryFile}
            onRemoveFile={batch.removeFile}
            onUpdateSettings={batch.updateSettings}
          />
        </div>
      </div>
    )
  }

  return (
    <div
      className="editor-page"
      onDragOver={e => e.preventDefault()}
      onDrop={handleFileDrop}
    >
      {/* Mode Controls */}
      <div className="mode-controls">
        <div className="mode-indicator">
          <span className={`mode-badge ${isAutoMode ? 'auto' : 'manual'}`}>
            {isAutoMode ? 'Auto Mode' : 'Manual Mode'}
          </span>
        </div>

        {/* Database Connection Status */}
        <div className="db-controls">
          {dbStatus.connected ? (
            <>
              <span className="db-status connected">
                <span className="db-dot" />
                DuckDB
              </span>
              <button
                className="action-btn db-disconnect-btn"
                onClick={handleDbDisconnect}
                title="Disconnect database"
              >
                Disconnect
              </button>
            </>
          ) : (
            <button
              className="action-btn"
              onClick={() => setShowDbConnect(true)}
              title="Connect database for query execution"
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <ellipse cx="12" cy="5" rx="9" ry="3" />
                <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
              </svg>
              Connect DB
            </button>
          )}
        </div>
      </div>

      {/* Database Connection Modal */}
      {showDbConnect && (
        <div className="modal-overlay" onClick={() => setShowDbConnect(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <DatabaseConnection
              onConnect={handleDbConnect}
              onCancel={() => setShowDbConnect(false)}
              isConnecting={isConnecting}
            />
          </div>
        </div>
      )}

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
                multiple
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

              {/* Execute Button - only when DB connected */}
              {dbStatus.connected && (
                <button
                  className="action-btn execute-btn"
                  onClick={handleExecute}
                  disabled={isExecuting || !code.trim()}
                  title="Execute query against connected database"
                >
                  {isExecuting ? (
                    <>
                      <span className="spinner" />
                      Running...
                    </>
                  ) : (
                    <>
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <polygon points="5,3 19,12 5,21 5,3" fill="currentColor" />
                      </svg>
                      Execute
                    </>
                  )}
                </button>
              )}

              {/* Explain Button - only when DB connected */}
              {dbStatus.connected && (
                <button
                  className="action-btn"
                  onClick={handleExplain}
                  disabled={isExplaining || !code.trim()}
                  title="Show execution plan"
                >
                  {isExplaining ? (
                    <>
                      <span className="spinner" />
                      Planning...
                    </>
                  ) : (
                    <>
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <path d="M9 18l6-6-6-6" />
                      </svg>
                      Explain
                    </>
                  )}
                </button>
              )}
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
        {showResults && result && optimizeStep === 'idle' && !showQueryResults && (
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

        {/* Query Results Panel */}
        {showQueryResults && (
          <div className="results-panel">
            <div className="results-header">
              <span>Query Results</span>
              <div className="results-actions">
                <button
                  className="close-btn"
                  onClick={() => {
                    setShowQueryResults(false)
                    setQueryResult(null)
                  }}
                  title="Close results"
                >
                  x
                </button>
              </div>
            </div>
            <QueryResults
              result={queryResult}
              isLoading={isExecuting}
              error={queryResult?.error}
            />
          </div>
        )}

        {/* Execution Plan Panel */}
        {showPlanResult && planResult && (
          <div className="results-panel">
            <div className="results-header">
              <span>Execution Plan</span>
              <div className="results-actions">
                <button
                  className="close-btn"
                  onClick={() => {
                    setShowPlanResult(false)
                    setPlanResult(null)
                  }}
                  title="Close plan"
                >
                  x
                </button>
              </div>
            </div>
            <div style={{ flex: 1, overflow: 'auto', padding: '1rem' }}>
              {planResult.success && planResult.plan_tree ? (
                <PlanViewer
                  planTree={planResult.plan_tree}
                  totalCost={planResult.total_cost}
                  executionTimeMs={planResult.execution_time_ms}
                  bottleneck={planResult.bottleneck}
                  warnings={planResult.warnings}
                  title="Query Plan"
                />
              ) : (
                <div className="no-validation">
                  <p>{planResult.error || 'Failed to generate execution plan'}</p>
                </div>
              )}
            </div>
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

        {/* Optimization Preview - ValidationReport */}
        {optimizeStep === 'preview' && validationPreview && (
          <div className="results-panel">
            <div className="results-header">
              <span>Validation Results</span>
              <button className="close-btn" onClick={handleCancel}>x</button>
            </div>
            <div className="validation-container">
              <ValidationReport
                result={validationPreview}
                onAccept={handleAccept}
                onReject={handleReject}
                onRetry={handleRetry}
                isAccepting={isAccepting}
              />
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
