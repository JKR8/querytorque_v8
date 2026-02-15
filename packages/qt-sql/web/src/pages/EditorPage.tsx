import { useState, useRef, useEffect } from 'react'
import CodeEditor from '@/components/CodeEditor'
import QueryResults, { QueryResultData } from '@/components/QueryResults'
import DatabaseConnection, { DatabaseConfig, ConnectionStatus } from '@/components/DatabaseConnection'
import TabBar from '@/components/TabBar'
import ActionBar from '@/components/ActionBar'
import AuditResults from '@/components/AuditResults'
import OptimizeResults from '@/components/OptimizeResults'
import PlanViewer from '@/components/PlanViewer'
import useSettings from '@/hooks/useSettings'
import {
  connectDuckDB,
  connectPostgres,
  executeQuery,
  getExecutionPlan,
  disconnectDatabase,
  auditQuery,
  optimizeQuery,
  AuditResponse,
  OptimizeResponse,
  PlanTreeNode,
} from '@/api/client'
import './EditorPage.css'

type ResultTab = 'audit' | 'optimize' | 'results' | 'plan'

const SQL_PLACEHOLDER = `-- Paste your SQL query here
-- Connect a database, then use Audit (free) or Optimize (AI)

SELECT c.id, c.name, c.email,
       o.order_date, o.total_amount
FROM customers c,
     orders o
WHERE c.id = o.customer_id
  AND YEAR(o.order_date) = 2024
  AND (c.status = 'active' OR c.status = 'premium')
ORDER BY o.order_date DESC`

export default function EditorPage() {
  // SQL state
  const [sql, setSql] = useState(SQL_PLACEHOLDER)

  // Database connection
  const [dbSessionId, setDbSessionId] = useState<string | null>(null)
  const [dbType, setDbType] = useState<'duckdb' | 'postgres' | null>(null)
  const [dbDsn, setDbDsn] = useState<string | null>(null)
  const [dbStatus, setDbStatus] = useState<ConnectionStatus>({ connected: false })
  const [showDbConnect, setShowDbConnect] = useState(false)
  const [isConnecting, setIsConnecting] = useState(false)

  // Results tabs
  const [activeTab, setActiveTab] = useState<ResultTab>('audit')
  const [showResults, setShowResults] = useState(false)

  // Audit state
  const [auditResult, setAuditResult] = useState<AuditResponse | null>(null)
  const [isAuditing, setIsAuditing] = useState(false)

  // Optimize state
  const [optimizeResult, setOptimizeResult] = useState<OptimizeResponse | null>(null)
  const [isOptimizing, setIsOptimizing] = useState(false)
  const [originalPlan, setOriginalPlan] = useState<PlanTreeNode[] | undefined>()
  const [optimizedPlan, setOptimizedPlan] = useState<PlanTreeNode[] | undefined>()
  const [originalCost, setOriginalCost] = useState<number | undefined>()
  const [optimizedCost, setOptimizedCost] = useState<number | undefined>()
  const [originalTimeMs, setOriginalTimeMs] = useState<number | undefined>()
  const [optimizedTimeMs, setOptimizedTimeMs] = useState<number | undefined>()

  // Query execution state
  const [queryResult, setQueryResult] = useState<QueryResultData | null>(null)
  const [isExecuting, setIsExecuting] = useState(false)

  // Execution plan state
  const [planTree, setPlanTree] = useState<PlanTreeNode[] | null>(null)
  const [planCost, setPlanCost] = useState<number | undefined>()
  const [planTimeMs, setPlanTimeMs] = useState<number | undefined>()
  const [planBottleneck, setPlanBottleneck] = useState<{ operator: string; cost_pct: number; suggestion?: string } | undefined>()
  const [planWarnings, setPlanWarnings] = useState<string[] | undefined>()
  const [isExplaining, setIsExplaining] = useState(false)

  // Error
  const [error, setError] = useState<string | null>(null)

  // Settings
  const { settings } = useSettings()

  // Resizable panel
  const [editorWidth, setEditorWidth] = useState(50)
  const [isResizing, setIsResizing] = useState(false)
  const layoutRef = useRef<HTMLDivElement>(null)

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

  // ---- Database Connection ----

  const handleDbConnect = async (config: DatabaseConfig) => {
    setIsConnecting(true)
    setError(null)

    try {
      if (config.type === 'duckdb' && config.fixtureFile) {
        const response = await connectDuckDB(config.fixtureFile)
        if (response.connected) {
          setDbSessionId(response.session_id)
          setDbType('duckdb')
          setDbDsn(`duckdb:///:memory:`)
          setDbStatus({ connected: true, type: 'duckdb', details: response.details })
          setShowDbConnect(false)
        } else {
          setError(response.error || 'Connection failed')
        }
      } else if (config.type === 'postgres' && config.connectionString) {
        const response = await connectPostgres(config.connectionString)
        if (response.connected) {
          setDbSessionId(response.session_id)
          setDbType('postgres')
          setDbDsn(config.connectionString)
          setDbStatus({ connected: true, type: 'postgres', details: response.details })
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
      try { await disconnectDatabase(dbSessionId) } catch { /* ignore */ }
    }
    setDbSessionId(null)
    setDbType(null)
    setDbDsn(null)
    setDbStatus({ connected: false })
    setQueryResult(null)
    setAuditResult(null)
    setOptimizeResult(null)
    setPlanTree(null)
    setShowResults(false)
  }

  // ---- Audit (free, no LLM) ----

  const handleAudit = async () => {
    if (!dbSessionId || !sql.trim()) return

    setIsAuditing(true)
    setError(null)
    setAuditResult(null)

    try {
      const result = await auditQuery(dbSessionId, sql)
      setAuditResult(result)
      setActiveTab('audit')
      setShowResults(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Audit failed')
    } finally {
      setIsAuditing(false)
    }
  }

  // ---- Optimize (LLM-backed) ----

  const handleOptimize = async () => {
    if (!dbSessionId || !dbDsn || !sql.trim()) return

    setIsOptimizing(true)
    setError(null)
    setOptimizeResult(null)
    setOriginalPlan(undefined)
    setOptimizedPlan(undefined)

    try {
      const result = await optimizeQuery({
        sql,
        dsn: dbDsn,
        mode: 'beam',
        session_id: dbSessionId || undefined,
      })
      setOptimizeResult(result)
      setActiveTab('optimize')
      setShowResults(true)

      // Fetch EXPLAIN plans for before/after comparison
      if (result.optimized_sql && dbSessionId) {
        try {
          const [origPlan, optPlan] = await Promise.all([
            getExecutionPlan(dbSessionId, sql, true),
            getExecutionPlan(dbSessionId, result.optimized_sql, true),
          ])
          if (origPlan.success && origPlan.plan_tree) {
            setOriginalPlan(origPlan.plan_tree)
            setOriginalCost(origPlan.total_cost)
            setOriginalTimeMs(origPlan.execution_time_ms)
          }
          if (optPlan.success && optPlan.plan_tree) {
            setOptimizedPlan(optPlan.plan_tree)
            setOptimizedCost(optPlan.total_cost)
            setOptimizedTimeMs(optPlan.execution_time_ms)
          }
        } catch {
          // Plan fetch is best-effort — don't fail the whole flow
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Optimization failed')
    } finally {
      setIsOptimizing(false)
    }
  }

  // ---- Execute Query ----

  const handleExecute = async () => {
    if (!dbSessionId || !sql.trim()) return

    setIsExecuting(true)
    setError(null)
    setQueryResult(null)

    try {
      const result = await executeQuery(dbSessionId, sql, 100)
      setQueryResult(result)
      setActiveTab('results')
      setShowResults(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Execution failed')
    } finally {
      setIsExecuting(false)
    }
  }

  // ---- Explain Plan ----

  const handleExplain = async () => {
    if (!dbSessionId || !sql.trim()) return

    setIsExplaining(true)
    setError(null)

    try {
      const result = await getExecutionPlan(dbSessionId, sql, true)
      if (result.success && result.plan_tree) {
        setPlanTree(result.plan_tree)
        setPlanCost(result.total_cost)
        setPlanTimeMs(result.execution_time_ms)
        setPlanBottleneck(result.bottleneck ? {
          operator: result.bottleneck.operator,
          cost_pct: result.bottleneck.cost_pct,
          suggestion: result.bottleneck.suggestion,
        } : undefined)
        setPlanWarnings(result.warnings)
        setActiveTab('plan')
        setShowResults(true)
      } else {
        setError(result.error || 'Failed to generate execution plan')
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Explain failed')
    } finally {
      setIsExplaining(false)
    }
  }

  // ---- Build tab list ----

  const tabs = [
    { id: 'audit', label: 'Audit' },
    { id: 'optimize', label: 'Optimize' },
    { id: 'results', label: 'Results' },
    { id: 'plan', label: 'Plan' },
  ]

  const lineCount = sql.split('\n').length

  return (
    <div className="editor-page">
      {/* Connection Bar */}
      <div className="mode-controls">
        <div className="db-controls">
          {dbStatus.connected ? (
            <>
              <span className="db-status connected">
                <span className="db-dot" />
                {dbType === 'postgres' ? 'PostgreSQL' : 'DuckDB'}
              </span>
              {dbStatus.details && (
                <span className="db-details">{dbStatus.details}</span>
              )}
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

        {/* Execute + Explain buttons (when connected) */}
        {dbStatus.connected && (
          <div className="db-query-actions">
            <button
              className="action-btn execute-btn"
              onClick={handleExecute}
              disabled={isExecuting || !sql.trim()}
              title="Execute query"
            >
              {isExecuting ? (
                <><span className="spinner" /> Running...</>
              ) : (
                <>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <polygon points="5,3 19,12 5,21 5,3" fill="currentColor" />
                  </svg>
                  Execute
                </>
              )}
            </button>
            <button
              className="action-btn"
              onClick={handleExplain}
              disabled={isExplaining || !sql.trim()}
              title="Show execution plan"
            >
              {isExplaining ? (
                <><span className="spinner" /> Planning...</>
              ) : (
                <>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M9 18l6-6-6-6" />
                  </svg>
                  Explain
                </>
              )}
            </button>
          </div>
        )}
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

      {/* Main Layout */}
      <div className="editor-layout" ref={layoutRef}>
        {/* Editor Panel */}
        <div
          className={`editor-panel ${showResults ? 'with-results' : ''}`}
          style={showResults ? { '--editor-width': `${editorWidth}%` } as React.CSSProperties : undefined}
        >
          <div className="editor-body">
            <CodeEditor
              value={sql}
              onChange={setSql}
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

          {/* Action Bar */}
          <ActionBar
            dbConnected={dbStatus.connected}
            llmConfigured={settings.llmConfigured}
            isAuditing={isAuditing}
            isOptimizing={isOptimizing}
            hasOptimizeResult={!!optimizeResult && optimizeResult.status !== 'ERROR'}
            onAudit={handleAudit}
            onOptimize={handleOptimize}
            disabled={!sql.trim()}
          />
        </div>

        {/* Resize Handle */}
        {showResults && (
          <div
            className={`resize-handle ${isResizing ? 'dragging' : ''}`}
            onMouseDown={() => setIsResizing(true)}
          />
        )}

        {/* Results Panel */}
        {showResults && (
          <div className="results-panel">
            <div className="results-header">
              <TabBar
                tabs={tabs}
                activeTab={activeTab}
                onTabChange={(id) => setActiveTab(id as ResultTab)}
              />
              <button
                className="close-btn"
                onClick={() => setShowResults(false)}
                title="Close results"
              >
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
                  <line x1="18" y1="6" x2="6" y2="18" />
                  <line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>

            <div className="results-body">
              {/* Audit Tab */}
              {activeTab === 'audit' && (
                auditResult ? (
                  <AuditResults result={auditResult} />
                ) : isAuditing ? (
                  <div className="results-loading">
                    <div className="spinner-large" />
                    <p>Running audit...</p>
                  </div>
                ) : (
                  <div className="results-empty">
                    <p>Click <strong>Audit</strong> to analyze your query execution plan.</p>
                    <p className="results-empty-hint">Free, no LLM required. Just needs a database connection.</p>
                  </div>
                )
              )}

              {/* Optimize Tab */}
              {activeTab === 'optimize' && (
                optimizeResult ? (
                  <OptimizeResults
                    result={optimizeResult}
                    originalPlan={originalPlan}
                    optimizedPlan={optimizedPlan}
                    originalCost={originalCost}
                    optimizedCost={optimizedCost}
                    originalTimeMs={originalTimeMs}
                    optimizedTimeMs={optimizedTimeMs}
                  />
                ) : isOptimizing ? (
                  <div className="results-loading">
                    <div className="spinner-large" />
                    <p>Running AI optimization...</p>
                    <p className="loading-hint">This may take 10-60 seconds.</p>
                  </div>
                ) : (
                  <div className="results-empty">
                    <p>Click <strong>Optimize</strong> to run AI-powered query rewriting.</p>
                    <p className="results-empty-hint">Requires database connection and LLM provider configured.</p>
                  </div>
                )
              )}

              {/* Results Tab */}
              {activeTab === 'results' && (
                <div className="results-query-tab">
                  <QueryResults
                    result={queryResult}
                    isLoading={isExecuting}
                    error={queryResult?.error}
                  />
                </div>
              )}

              {/* Plan Tab */}
              {activeTab === 'plan' && (
                <div className="results-plan-tab">
                  {planTree ? (
                    <PlanViewer
                      planTree={planTree}
                      totalCost={planCost}
                      executionTimeMs={planTimeMs}
                      bottleneck={planBottleneck}
                      warnings={planWarnings}
                      title="Query Plan"
                    />
                  ) : isExplaining ? (
                    <div className="results-loading">
                      <div className="spinner-large" />
                      <p>Generating execution plan...</p>
                    </div>
                  ) : (
                    <div className="results-empty">
                      <p>Click <strong>Explain</strong> to see the execution plan.</p>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
