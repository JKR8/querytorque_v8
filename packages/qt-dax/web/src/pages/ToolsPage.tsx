import { useCallback, useEffect, useMemo, useState } from 'react'
import OptimizationPanel from '../components/OptimizationPanel'
import {
  listPBIInstances,
  connectPBI,
  disconnectPBI,
  executeDAX,
  PBIInstance,
  PBISession,
} from '../api/client'

type ValidationStatus = 'idle' | 'running' | 'pass' | 'fail' | 'error'

interface ValidationMismatch {
  row: number
  column: number
  original: unknown
  optimized: unknown
}

export default function ToolsPage() {
  const [measureName, setMeasureName] = useState('')
  const [measureCode, setMeasureCode] = useState('')
  const [showOptimizer, setShowOptimizer] = useState(false)

  const [instances, setInstances] = useState<PBIInstance[]>([])
  const [instancesAvailable, setInstancesAvailable] = useState<boolean | null>(null)
  const [session, setSession] = useState<PBISession | null>(null)
  const [pbiError, setPbiError] = useState<string | null>(null)
  const [listLoading, setListLoading] = useState(false)
  const [connectLoading, setConnectLoading] = useState(false)

  const [query, setQuery] = useState("EVALUATE ROW('Test', 1+1)")
  const [queryResult, setQueryResult] = useState<{
    columns: string[]
    rows: unknown[][]
    rowCount: number
    ms: number
  } | null>(null)
  const [queryError, setQueryError] = useState<string | null>(null)

  const [originalDax, setOriginalDax] = useState('')
  const [optimizedDax, setOptimizedDax] = useState('')
  const [maxRows, setMaxRows] = useState(10000)
  const [sampleLimit, setSampleLimit] = useState(5)
  const [tolerance, setTolerance] = useState(1e-9)
  const [validationStatus, setValidationStatus] = useState<ValidationStatus>('idle')
  const [validationError, setValidationError] = useState<string | null>(null)
  const [validationMismatches, setValidationMismatches] = useState<ValidationMismatch[]>([])
  const [validationTiming, setValidationTiming] = useState<{
    originalMs: number
    optimizedMs: number
    speedup: number
  } | null>(null)

  const canValidate = useMemo(
    () => !!session && originalDax.trim() && optimizedDax.trim(),
    [session, originalDax, optimizedDax]
  )

  const refreshInstances = useCallback(async () => {
    setListLoading(true)
    setPbiError(null)
    try {
      const result = await listPBIInstances()
      setInstances(result.instances)
      setInstancesAvailable(result.available)
    } catch (err) {
      setPbiError(err instanceof Error ? err.message : 'Failed to list instances')
    } finally {
      setListLoading(false)
    }
  }, [])

  useEffect(() => {
    refreshInstances()
  }, [refreshInstances])

  const handleConnect = useCallback(async (port: number) => {
    setConnectLoading(true)
    setPbiError(null)
    try {
      const result = await connectPBI(port)
      setSession(result)
    } catch (err) {
      setPbiError(err instanceof Error ? err.message : 'Failed to connect')
    } finally {
      setConnectLoading(false)
    }
  }, [])

  const handleDisconnect = useCallback(async () => {
    if (!session) return
    setConnectLoading(true)
    setPbiError(null)
    try {
      await disconnectPBI(session.session_id)
      setSession(null)
    } catch (err) {
      setPbiError(err instanceof Error ? err.message : 'Failed to disconnect')
    } finally {
      setConnectLoading(false)
    }
  }, [session])

  const handleExecute = useCallback(async () => {
    if (!session) return
    setQueryError(null)
    setQueryResult(null)
    try {
      const result = await executeDAX(session.session_id, query)
      if (!result.success) {
        setQueryError(result.error || 'Query failed')
        return
      }
      setQueryResult({
        columns: result.columns,
        rows: result.rows,
        rowCount: result.row_count,
        ms: result.execution_time_ms,
      })
    } catch (err) {
      setQueryError(err instanceof Error ? err.message : 'Query failed')
    }
  }, [session, query])

  const wrapForCompare = (dax: string, limit: number): string => {
    const trimmed = dax.trim()
    if (!trimmed) return trimmed
    const upper = trimmed.toUpperCase()
    if (!upper.startsWith('EVALUATE')) {
      return `EVALUATE ROW('Result', ${trimmed})`
    }
    const rest = trimmed.slice(8).trim()
    if (!rest) return trimmed
    return `EVALUATE TOPN(${limit}, ${rest})`
  }

  const compareValues = (a: unknown, b: unknown, tol: number): boolean => {
    if (a === b) return true
    if (a == null || b == null) return a == b
    if (typeof a === 'number' && typeof b === 'number') {
      return Math.abs(a - b) <= tol
    }
    return String(a) === String(b)
  }

  const handleValidate = useCallback(async () => {
    if (!session) return
    setValidationStatus('running')
    setValidationError(null)
    setValidationMismatches([])
    setValidationTiming(null)

    try {
      const originalQuery = wrapForCompare(originalDax, maxRows)
      const optimizedQuery = wrapForCompare(optimizedDax, maxRows)

      const [origRes, optRes] = await Promise.all([
        executeDAX(session.session_id, originalQuery),
        executeDAX(session.session_id, optimizedQuery),
      ])

      if (!origRes.success) {
        setValidationStatus('error')
        setValidationError(origRes.error || 'Original query failed')
        return
      }
      if (!optRes.success) {
        setValidationStatus('error')
        setValidationError(optRes.error || 'Optimized query failed')
        return
      }

      const mismatches: ValidationMismatch[] = []
      const rowCount = Math.min(origRes.rows.length, optRes.rows.length)
      const colCount = Math.min(origRes.columns.length, optRes.columns.length)

      for (let r = 0; r < rowCount; r += 1) {
        for (let c = 0; c < colCount; c += 1) {
          if (!compareValues(origRes.rows[r][c], optRes.rows[r][c], tolerance)) {
            mismatches.push({
              row: r,
              column: c,
              original: origRes.rows[r][c],
              optimized: optRes.rows[r][c],
            })
            if (mismatches.length >= sampleLimit) break
          }
        }
        if (mismatches.length >= sampleLimit) break
      }

      const status = origRes.row_count === optRes.row_count && mismatches.length === 0 ? 'pass' : 'fail'
      setValidationStatus(status)
      setValidationMismatches(mismatches)
      const speedup = optRes.execution_time_ms > 0 ? origRes.execution_time_ms / optRes.execution_time_ms : 1
      setValidationTiming({
        originalMs: origRes.execution_time_ms,
        optimizedMs: optRes.execution_time_ms,
        speedup,
      })
    } catch (err) {
      setValidationStatus('error')
      setValidationError(err instanceof Error ? err.message : 'Validation failed')
    }
  }, [session, originalDax, optimizedDax, maxRows, sampleLimit, tolerance])

  return (
    <div className="tools-page">
      <div className="tools-header">
        <h1>DAX Tools</h1>
        <p>Same capabilities as the CLI: optimize, validate, and connect to Power BI Desktop.</p>
      </div>

      <div className="tools-grid">
        <section className="tool-card">
          <h2>Optimize DAX (LLM)</h2>
          <p className="tool-description">
            Paste a measure and walk through the optimization + validation flow.
          </p>

          <label className="field">
            <span>Measure name (optional)</span>
            <input
              type="text"
              value={measureName}
              onChange={(e) => setMeasureName(e.target.value)}
              placeholder="Total Sales"
            />
          </label>

          <label className="field">
            <span>DAX expression</span>
            <textarea
              value={measureCode}
              onChange={(e) => setMeasureCode(e.target.value)}
              placeholder="SUM('Sales'[Amount])"
            />
          </label>

          <div className="tool-actions">
            <button
              className="btn btn-primary"
              disabled={!measureCode.trim()}
              onClick={() => setShowOptimizer(true)}
            >
              Start Optimization
            </button>
            {showOptimizer && (
              <button className="btn btn-ghost" onClick={() => setShowOptimizer(false)}>
                Hide Panel
              </button>
            )}
          </div>

          {showOptimizer && (
            <div className="tool-panel">
              <OptimizationPanel
                measureName={measureName || 'Measure'}
                measureCode={measureCode}
                onComplete={() => setShowOptimizer(false)}
                onCancel={() => setShowOptimizer(false)}
              />
            </div>
          )}
        </section>

        <section className="tool-card">
          <h2>Power BI Desktop</h2>
          <p className="tool-description">
            List local instances, connect, and run ad-hoc DAX queries.
          </p>

          <div className="tool-actions">
            <button className="btn btn-secondary" onClick={refreshInstances} disabled={listLoading}>
              {listLoading ? 'Refreshing...' : 'Refresh Instances'}
            </button>
            {session ? (
              <button className="btn btn-ghost" onClick={handleDisconnect} disabled={connectLoading}>
                Disconnect
              </button>
            ) : null}
          </div>

          {instancesAvailable === false && (
            <div className="inline-error">PBI Desktop support not available on this host.</div>
          )}
          {pbiError && <div className="inline-error">{pbiError}</div>}

          <div className="instances">
            {instances.length === 0 ? (
              <p className="muted">No instances found.</p>
            ) : (
              <div className="instances-list">
                {instances.map((inst) => (
                  <div key={inst.port} className={`instance ${session?.port === inst.port ? 'active' : ''}`}>
                    <div>
                      <div className="instance-name">{inst.name}</div>
                      <div className="instance-meta">Port {inst.port}</div>
                    </div>
                    {session?.port === inst.port ? (
                      <span className="badge">Connected</span>
                    ) : (
                      <button
                        className="btn btn-sm"
                        onClick={() => handleConnect(inst.port)}
                        disabled={connectLoading}
                      >
                        Connect
                      </button>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="query-box">
            <label className="field">
              <span>Execute DAX</span>
              <textarea value={query} onChange={(e) => setQuery(e.target.value)} />
            </label>
            <div className="tool-actions">
              <button className="btn btn-primary" onClick={handleExecute} disabled={!session}>
                Run Query
              </button>
            </div>
            {queryError && <div className="inline-error">{queryError}</div>}
            {queryResult && (
              <div className="query-results">
                <div className="result-meta">
                  <span>{queryResult.rowCount} rows</span>
                  <span>{queryResult.ms.toFixed(1)}ms</span>
                </div>
                <div className="result-table">
                  <table>
                    <thead>
                      <tr>
                        {queryResult.columns.map((col) => (
                          <th key={col}>{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {queryResult.rows.slice(0, 20).map((row, idx) => (
                        <tr key={idx}>
                          {row.map((cell, cidx) => (
                            <td key={cidx}>{String(cell)}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </section>

        <section className="tool-card">
          <h2>Validate DAX (PBI Desktop)</h2>
          <p className="tool-description">
            Compare original vs optimized DAX on the connected local model.
          </p>

          <div className="validation-grid">
            <label className="field">
              <span>Original DAX</span>
              <textarea
                value={originalDax}
                onChange={(e) => setOriginalDax(e.target.value)}
                placeholder="SUM('Sales'[Amount])"
              />
            </label>
            <label className="field">
              <span>Optimized DAX</span>
              <textarea
                value={optimizedDax}
                onChange={(e) => setOptimizedDax(e.target.value)}
                placeholder="VAR s = SUM('Sales'[Amount]) RETURN s"
              />
            </label>
          </div>

          <div className="inline-settings">
            <label>
              Max rows
              <input
                type="number"
                min={1}
                value={maxRows}
                onChange={(e) => setMaxRows(Number(e.target.value))}
              />
            </label>
            <label>
              Sample mismatches
              <input
                type="number"
                min={1}
                value={sampleLimit}
                onChange={(e) => setSampleLimit(Number(e.target.value))}
              />
            </label>
            <label>
              Tolerance
              <input
                type="number"
                step="any"
                value={tolerance}
                onChange={(e) => setTolerance(Number(e.target.value))}
              />
            </label>
          </div>

          <div className="tool-actions">
            <button className="btn btn-primary" onClick={handleValidate} disabled={!canValidate}>
              {validationStatus === 'running' ? 'Validating...' : 'Validate'}
            </button>
            {!session && <span className="muted">Connect to PBI Desktop to validate.</span>}
          </div>

          {validationError && <div className="inline-error">{validationError}</div>}
          {validationStatus !== 'idle' && validationStatus !== 'running' && (
            <div className={`validation-status ${validationStatus}`}>
              {validationStatus === 'pass' ? 'Validation passed' : 'Validation failed'}
              {validationTiming && (
                <div className="timing">
                  <span>{validationTiming.originalMs.toFixed(1)}ms</span>
                  <span>{validationTiming.optimizedMs.toFixed(1)}ms</span>
                  <span>
                    {validationTiming.speedup.toFixed(2)}x{' '}
                    {validationTiming.speedup >= 1 ? 'faster' : 'slower'}
                  </span>
                </div>
              )}
            </div>
          )}

          {validationMismatches.length > 0 && (
            <div className="mismatch-list">
              <h4>Sample mismatches</h4>
              <ul>
                {validationMismatches.map((m, idx) => (
                  <li key={idx}>
                    Row {m.row + 1}, Col {m.column + 1}: {String(m.original)} vs {String(m.optimized)}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </section>
      </div>

      <style>{styles}</style>
    </div>
  )
}

const styles = `
  .tools-page {
    max-width: 1200px;
    margin: 0 auto;
    padding: var(--qt-space-lg);
  }

  .tools-header {
    margin-bottom: var(--qt-space-xl);
  }

  .tools-header h1 {
    font-size: var(--qt-text-2xl);
    font-weight: var(--qt-font-bold);
    margin-bottom: var(--qt-space-xs);
  }

  .tools-header p {
    color: var(--qt-fg-muted);
  }

  .tools-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: var(--qt-space-lg);
  }

  .tool-card {
    background: var(--qt-bg-card);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius);
    padding: var(--qt-space-lg);
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-md);
  }

  .tool-card h2 {
    font-size: var(--qt-text-lg);
    font-weight: var(--qt-font-semibold);
  }

  .tool-description {
    color: var(--qt-fg-muted);
    font-size: var(--qt-text-sm);
  }

  .field {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-xs);
  }

  .field span {
    font-size: var(--qt-text-xs);
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--qt-fg-muted);
  }

  .field input,
  .field textarea {
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius-sm);
    padding: var(--qt-space-sm);
    font-size: var(--qt-text-sm);
    background: var(--qt-bg);
    color: var(--qt-fg);
  }

  .field textarea {
    min-height: 140px;
    font-family: var(--qt-font-mono);
    resize: vertical;
  }

  .tool-actions {
    display: flex;
    gap: var(--qt-space-sm);
    align-items: center;
  }

  .tool-panel {
    margin-top: var(--qt-space-md);
  }

  .instances {
    border: 1px dashed var(--qt-border);
    border-radius: var(--qt-radius-sm);
    padding: var(--qt-space-sm);
  }

  .instances-list {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-xs);
  }

  .instance {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--qt-space-sm);
    padding: var(--qt-space-sm);
    border-radius: var(--qt-radius-sm);
    background: var(--qt-bg-alt);
  }

  .instance.active {
    border: 1px solid var(--qt-brand);
  }

  .instance-name {
    font-weight: var(--qt-font-semibold);
  }

  .instance-meta {
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
  }

  .badge {
    font-size: var(--qt-text-xs);
    background: var(--qt-brand-light);
    color: var(--qt-brand);
    padding: 0.2rem 0.5rem;
    border-radius: var(--qt-radius-full);
  }

  .query-box {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-sm);
  }

  .query-results {
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius-sm);
    padding: var(--qt-space-sm);
    background: var(--qt-bg);
  }

  .result-meta {
    display: flex;
    justify-content: space-between;
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
    margin-bottom: var(--qt-space-sm);
  }

  .result-table {
    overflow: auto;
  }

  .result-table table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--qt-text-xs);
  }

  .result-table th,
  .result-table td {
    border-bottom: 1px solid var(--qt-border);
    padding: 0.35rem 0.5rem;
    text-align: left;
    white-space: nowrap;
  }

  .validation-grid {
    display: grid;
    grid-template-columns: 1fr;
    gap: var(--qt-space-md);
  }

  .inline-settings {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: var(--qt-space-sm);
  }

  .inline-settings label {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
  }

  .inline-settings input {
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius-sm);
    padding: 0.25rem 0.5rem;
    font-size: var(--qt-text-sm);
  }

  .validation-status {
    padding: var(--qt-space-sm);
    border-radius: var(--qt-radius-sm);
    background: var(--qt-bg-alt);
    font-size: var(--qt-text-sm);
  }

  .validation-status.pass {
    border: 1px solid var(--qt-low);
    color: var(--qt-low);
  }

  .validation-status.fail {
    border: 1px solid var(--qt-critical);
    color: var(--qt-critical);
  }

  .timing {
    margin-top: var(--qt-space-xs);
    display: flex;
    gap: var(--qt-space-sm);
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
  }

  .mismatch-list {
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius-sm);
    padding: var(--qt-space-sm);
    background: var(--qt-bg-alt);
  }

  .mismatch-list h4 {
    font-size: var(--qt-text-sm);
    margin-bottom: var(--qt-space-xs);
  }

  .mismatch-list ul {
    padding-left: 1rem;
    color: var(--qt-fg-muted);
    font-size: var(--qt-text-xs);
  }

  .inline-error {
    margin-top: var(--qt-space-xs);
    padding: var(--qt-space-sm);
    background: var(--qt-critical-bg);
    border: 1px solid var(--qt-critical-border);
    border-radius: var(--qt-radius-sm);
    color: var(--qt-critical);
    font-size: var(--qt-text-sm);
  }

  .muted {
    color: var(--qt-fg-muted);
    font-size: var(--qt-text-sm);
  }

  @media (min-width: 900px) {
    .validation-grid {
      grid-template-columns: 1fr 1fr;
    }
  }
`
