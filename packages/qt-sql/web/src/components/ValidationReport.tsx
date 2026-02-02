/**
 * Validation Report Component
 * Matches audit report (sql_report.html.j2) styling exactly
 */

import { useState, useMemo } from 'react'
import type { ValidationPreviewResponse, PlanTreeNode } from '@/api/client'
import PlanViewer from './PlanViewer'
import './ValidationReport.css'

interface Props {
  result: ValidationPreviewResponse
  onAccept: () => void
  onReject: () => void
  onRetry: () => void
  isAccepting: boolean
}

interface DiffLine {
  type: 'context' | 'added' | 'removed'
  content: string
  lineNum?: number
}

/**
 * Compute a simple line-by-line diff between two SQL strings
 */
function computeDiff(original: string, optimized: string): DiffLine[] {
  const originalLines = original.split('\n')
  const optimizedLines = optimized.split('\n')
  const diff: DiffLine[] = []

  let oi = 0
  let ni = 0

  while (oi < originalLines.length || ni < optimizedLines.length) {
    if (oi >= originalLines.length) {
      diff.push({ type: 'added', content: optimizedLines[ni], lineNum: ni + 1 })
      ni++
    } else if (ni >= optimizedLines.length) {
      diff.push({ type: 'removed', content: originalLines[oi], lineNum: oi + 1 })
      oi++
    } else if (originalLines[oi].trim() === optimizedLines[ni].trim()) {
      diff.push({ type: 'context', content: originalLines[oi], lineNum: oi + 1 })
      oi++
      ni++
    } else {
      const origTrimmed = originalLines[oi].trim()
      const optTrimmed = optimizedLines[ni].trim()

      let foundInOpt = false
      for (let j = ni + 1; j < Math.min(ni + 5, optimizedLines.length); j++) {
        if (optimizedLines[j].trim() === origTrimmed) {
          foundInOpt = true
          break
        }
      }

      let foundInOrig = false
      for (let j = oi + 1; j < Math.min(oi + 5, originalLines.length); j++) {
        if (originalLines[j].trim() === optTrimmed) {
          foundInOrig = true
          break
        }
      }

      if (foundInOpt && !foundInOrig) {
        diff.push({ type: 'added', content: optimizedLines[ni], lineNum: ni + 1 })
        ni++
      } else if (foundInOrig && !foundInOpt) {
        diff.push({ type: 'removed', content: originalLines[oi], lineNum: oi + 1 })
        oi++
      } else {
        diff.push({ type: 'removed', content: originalLines[oi], lineNum: oi + 1 })
        diff.push({ type: 'added', content: optimizedLines[ni], lineNum: ni + 1 })
        oi++
        ni++
      }
    }
  }

  return diff
}

export default function ValidationReport({
  result,
  onAccept,
  onReject,
  onRetry,
  isAccepting,
}: Props) {
  const [openSections, setOpenSections] = useState<Set<number>>(new Set([1, 2]))
  const [toast, setToast] = useState<string | null>(null)

  const allPassed = result.all_passed
  const canAccept = allPassed && result.syntax_status === 'pass'
  const canRetry = result.can_retry !== false && (result.retry_count ?? 0) < (result.max_retries ?? 3)

  const toggleSection = (num: number) => {
    setOpenSections(prev => {
      const next = new Set(prev)
      if (next.has(num)) {
        next.delete(num)
      } else {
        next.add(num)
      }
      return next
    })
  }

  const showToast = (msg: string) => {
    setToast(msg)
    setTimeout(() => setToast(null), 2000)
  }

  const copySQL = () => {
    navigator.clipboard.writeText(result.optimized_code || '')
      .then(() => showToast('SQL copied!'))
  }

  const copyJSON = () => {
    navigator.clipboard.writeText(JSON.stringify(result, null, 2))
      .then(() => showToast('JSON copied!'))
  }

  // Extract metrics
  const eq = result.equivalence_details
  const planComp = result.plan_comparison
  const issuesFixed = result.issues_fixed?.length ?? 0
  const newIssues = result.new_issues?.length ?? 0

  // Prefer plan_comparison timing (from EXPLAIN ANALYZE), fall back to equivalence_details
  // Use || instead of ?? so that 0 values fall back to equivalence_details (actual execution time)
  const planCompTimeBefore = planComp?.original_execution_time_ms
  const planCompTimeAfter = planComp?.optimized_execution_time_ms
  const eqTimeBefore = eq?.original_execution_time_ms
  const eqTimeAfter = eq?.optimized_execution_time_ms

  // Use non-zero timing from either source, preferring plan_comparison
  const timeBefore = (planCompTimeBefore && planCompTimeBefore > 0) ? planCompTimeBefore : eqTimeBefore
  const timeAfter = (planCompTimeAfter && planCompTimeAfter > 0) ? planCompTimeAfter : eqTimeAfter
  const speedupRatio = eq?.speedup_ratio ?? 1

  // Calculate improvement percentage from real data
  const improvementPct = planComp?.time_improvement_pct != null && planComp.time_improvement_pct !== 0
    ? Math.round(planComp.time_improvement_pct)
    : (timeBefore && timeAfter && timeBefore > 0
        ? Math.round((timeBefore - timeAfter) / timeBefore * 100)
        : null)

  // Use real cost improvement from DuckDB plan comparison (no fake calculations)
  const costImprovementPct = planComp?.cost_improvement_pct != null
    ? Math.round(planComp.cost_improvement_pct)
    : null

  // Real cost values from DuckDB (arbitrary units, but relative comparison is meaningful)
  const costBefore = planComp?.original_total_cost
  const costAfter = planComp?.optimized_total_cost

  // Compute diff
  const diffLines = useMemo(() => {
    if (result.original_code && result.optimized_code) {
      return computeDiff(result.original_code, result.optimized_code)
    }
    return []
  }, [result.original_code, result.optimized_code])

  const addedCount = diffLines.filter(l => l.type === 'added').length
  const removedCount = diffLines.filter(l => l.type === 'removed').length

  // Patch results
  const patchResults = result.patch_result?.patch_results ?? []
  const hasPatchMode = result.patch_mode || patchResults.length > 0

  // Checks passed count
  const checksPassed = [result.syntax_status, result.schema_status, result.regression_status, result.equivalence_status]
    .filter(s => s === 'pass').length

  return (
    <div className="vr">
      {/* Header bar - matches Analysis Results header */}
      <div className="results-header">
        <span>
          Validation Results
          <span className={`vr-header-status ${allPassed ? 'pass' : 'fail'}`}>
            {allPassed ? '✓ PASS' : '✗ FAIL'}
          </span>
        </span>
        <div className="results-actions">
          <button
            className="action-btn"
            onClick={onAccept}
            disabled={!canAccept || isAccepting}
            style={{ background: 'var(--low)', color: 'white', borderColor: 'var(--low)' }}
          >
            {isAccepting ? 'Accepting...' : 'Accept'}
          </button>
          <button
            className="action-btn"
            onClick={onRetry}
            disabled={isAccepting || !canRetry}
          >
            Retry
          </button>
          <button
            className="action-btn"
            onClick={onReject}
            disabled={isAccepting}
            style={{ color: 'var(--critical)', borderColor: 'var(--critical)' }}
          >
            Cancel
          </button>
        </div>
      </div>

      {/* Scrollable light content */}
      <div className="vr-content">
        <div className="vr-page">
          {/* Header - matches audit report */}
          <header className="vr-header">
          <div className="vr-header-left">
            <div className="vr-brand">
              <div className="vr-brand-mark">
                {/* Lightning bolt icon */}
                <svg viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5">
                  <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
                </svg>
              </div>
              <span className="vr-brand-text">Query Torque</span>
              <span className="vr-brand-tag">Validation</span>
            </div>
            <div className="vr-query-id">
              {result.session_id?.slice(0, 8) || 'query'}
            </div>
          </div>
          <div className="vr-header-right">
            <div><strong>Session:</strong> {result.session_id?.slice(0, 12) || '—'}</div>
            <div><strong>Mode:</strong> {result.optimization_mode || 'manual'}</div>
          </div>
        </header>

        {/* Verdict - 3 column grid like audit report */}
        <div className="vr-verdict">
          <div className="vr-verdict-score">
            <div className={`vr-score-value ${allPassed ? 'pass' : 'fail'}`}>
              {allPassed ? '✓' : '✗'}
            </div>
            <div className="vr-score-label">{allPassed ? 'Validated' : 'Failed'}</div>
          </div>
          <div className="vr-verdict-summary">
            <div className="vr-verdict-headline">
              {allPassed
                ? 'All validation checks passed · Ready for deployment'
                : `Validation failed · ${result.errors?.length || 0} error(s) detected`}
            </div>
            <div className="vr-verdict-stats">
              <div className="vr-verdict-stat">
                <span className={`vr-verdict-stat-value ${checksPassed === 4 ? 'improved' : ''}`}>{checksPassed}/4</span>
                <span className="vr-verdict-stat-label">checks passed</span>
              </div>
              <div className="vr-verdict-stat">
                <span className={`vr-verdict-stat-value ${issuesFixed > 0 ? 'improved' : ''}`}>{issuesFixed}</span>
                <span className="vr-verdict-stat-label">issues fixed</span>
              </div>
              {newIssues > 0 && (
                <div className="vr-verdict-stat">
                  <span className="vr-verdict-stat-value bad">{newIssues}</span>
                  <span className="vr-verdict-stat-label">new issues</span>
                </div>
              )}
              {improvementPct != null && (
                <div className="vr-verdict-stat">
                  <span className="vr-verdict-stat-value improved">−{improvementPct}%</span>
                  <span className="vr-verdict-stat-label">execution time</span>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Cost Breakdown - like audit report */}
        <div className="vr-cost-breakdown">
          <div className="vr-cost-header">Optimization Impact</div>
          <div className="vr-cost-grid">
            <div className="vr-cost-item">
              <div className="vr-cost-label">Execution Time</div>
              <div className={`vr-cost-value ${improvementPct && improvementPct > 0 ? 'savings' : ''}`}>
                {improvementPct != null ? `−${improvementPct}%` : '—'}
              </div>
              <div className="vr-cost-subvalue">
                {timeBefore && timeAfter
                  ? `${(timeBefore / 1000).toFixed(1)}s → ${(timeAfter / 1000).toFixed(1)}s`
                  : 'Connect database to measure'}
              </div>
            </div>
            <div className="vr-cost-item">
              <div className="vr-cost-label">Query Cost</div>
              <div className={`vr-cost-value ${costImprovementPct && costImprovementPct > 0 ? 'savings' : ''}`}>
                {costImprovementPct != null ? `−${costImprovementPct}%` : '—'}
              </div>
              <div className="vr-cost-subvalue">
                {costBefore != null && costAfter != null
                  ? `${costBefore.toFixed(0)} → ${costAfter.toFixed(0)} units`
                  : 'Connect database to measure'}
              </div>
            </div>
            <div className="vr-cost-item divider">
              <div className="vr-cost-label">Speedup</div>
              <div className={`vr-cost-value ${speedupRatio > 1 ? 'savings' : ''}`}>
                {speedupRatio > 1 ? `${speedupRatio.toFixed(1)}x` : '—'}
              </div>
              <div className="vr-cost-subvalue">
                {speedupRatio > 1 ? 'faster execution' : 'Connect database to measure'}
              </div>
            </div>
            <div className="vr-cost-item">
              <div className="vr-cost-label">Issues Fixed</div>
              <div className={`vr-cost-value ${issuesFixed > 0 ? 'savings' : ''}`}>
                {issuesFixed > 0 ? issuesFixed : '—'}
              </div>
              <div className="vr-cost-subvalue">
                {issuesFixed > 0 ? `${newIssues > 0 ? `${newIssues} new` : 'no new issues'}` : 'anti-patterns'}
              </div>
            </div>
          </div>
        </div>

        {/* Section Divider */}
        <div className="vr-section-divider">
          <span className="vr-section-divider-label">Validation Details</span>
          <div className="vr-section-divider-title">Detailed Validation Results</div>
          <div className="vr-section-divider-desc">Click sections to expand for more information</div>
        </div>

        {/* Accordion */}
        <div className="vr-accordion">
          {/* Section 1: Validation Checks */}
          <div className={`vr-accordion-item ${openSections.has(1) ? 'open' : ''}`}>
            <div className="vr-accordion-header" onClick={() => toggleSection(1)}>
              <div className="vr-accordion-header-left">
                <span className={`vr-accordion-num ${allPassed ? 'pass' : 'fail'}`}>
                  {allPassed ? '✓' : '✗'}
                </span>
                <span className="vr-accordion-title">Validation Checks</span>
                <span className="vr-accordion-subtitle">{checksPassed}/4 passed</span>
              </div>
              <div className="vr-accordion-header-right">
                <span className={`vr-accordion-badge ${allPassed ? 'pass' : 'fail'}`}>
                  {allPassed ? 'PASS' : 'FAIL'}
                </span>
                <span className="vr-accordion-chevron">▼</span>
              </div>
            </div>
            <div className="vr-accordion-content">
              <table className="vr-data-table">
                <thead>
                  <tr><th>Check</th><th>Status</th><th>Detail</th></tr>
                </thead>
                <tbody>
                  <tr>
                    <td><strong>Syntax</strong></td>
                    <td><span className={`vr-status-badge ${result.syntax_status}`}>{result.syntax_status?.toUpperCase()}</span></td>
                    <td>{result.syntax_errors?.length ? result.syntax_errors.join(', ') : 'No syntax errors'}</td>
                  </tr>
                  <tr>
                    <td><strong>Schema</strong></td>
                    <td><span className={`vr-status-badge ${result.schema_status}`}>{result.schema_status?.toUpperCase()}</span></td>
                    <td>{result.schema_violations?.length ? result.schema_violations.join(', ') : 'All references valid'}</td>
                  </tr>
                  <tr>
                    <td><strong>Regression</strong></td>
                    <td><span className={`vr-status-badge ${result.regression_status}`}>{result.regression_status?.toUpperCase()}</span></td>
                    <td>{newIssues > 0 ? `${newIssues} new issue(s) introduced` : 'No new issues'}</td>
                  </tr>
                  <tr>
                    <td><strong>Equivalence</strong></td>
                    <td><span className={`vr-status-badge ${result.equivalence_status}`}>{result.equivalence_status?.toUpperCase()}</span></td>
                    <td>
                      {result.equivalence_status === 'pass'
                        ? `Results match (${eq?.original_row_count ?? 0} rows)`
                        : result.equivalence_status === 'skip'
                          ? 'Not tested'
                          : 'Results differ'}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {/* Section 2: SQL Diff */}
          <div className={`vr-accordion-item ${openSections.has(2) ? 'open' : ''}`}>
            <div className="vr-accordion-header" onClick={() => toggleSection(2)}>
              <div className="vr-accordion-header-left">
                <span className="vr-accordion-num info">2</span>
                <span className="vr-accordion-title">SQL Diff</span>
                <span className="vr-accordion-subtitle">
                  +{addedCount} −{removedCount} lines
                  {hasPatchMode && ` · ${patchResults.length} patches`}
                </span>
              </div>
              <div className="vr-accordion-header-right">
                <span className="vr-accordion-chevron">▼</span>
              </div>
            </div>
            <div className="vr-accordion-content">
              {/* Patch Summary */}
              {hasPatchMode && patchResults.length > 0 && (
                <div className="vr-patch-summary">
                  <div className="vr-patch-header">
                    <strong>Patches Applied</strong>
                    <span className="vr-patch-stats">
                      {result.patch_result?.applied_count ?? 0}/{result.patch_result?.total_patches ?? 0} applied
                      {result.patch_result?.success_rate != null && ` (${Math.round(result.patch_result.success_rate * 100)}%)`}
                    </span>
                  </div>
                  <div className="vr-patch-list">
                    {patchResults.map((patch, i) => (
                      <div key={i} className={`vr-patch-item ${patch.status}`}>
                        <div className="vr-patch-item-header">
                          <span className={`vr-status-badge ${patch.status === 'applied' ? 'pass' : 'fail'}`}>
                            {patch.status.toUpperCase()}
                          </span>
                          <code>{patch.issue_id}</code>
                          {patch.line_matched && <span className="vr-patch-line">Line {patch.line_matched}</span>}
                        </div>
                        {patch.description && (
                          <div className="vr-patch-description">{patch.description}</div>
                        )}
                        {patch.error && (
                          <div className="vr-patch-error">{patch.error}</div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Diff View */}
              <div className="vr-diff-block">
                <div className="vr-diff-header">
                  <span>diff --original --optimized</span>
                  <span>+{addedCount} −{removedCount}</span>
                </div>
                <div className="vr-diff-content">
                  {diffLines.map((line, i) => (
                    <div key={i} className={`vr-diff-line ${line.type}`}>
                      <span className="vr-diff-marker">
                        {line.type === 'added' ? '+' : line.type === 'removed' ? '−' : ' '}
                      </span>
                      <span className="vr-diff-text">{line.content || ' '}</span>
                    </div>
                  ))}
                  {diffLines.length === 0 && (
                    <div className="vr-diff-line context">
                      <span className="vr-diff-marker"> </span>
                      <span className="vr-diff-text" style={{ color: 'var(--fg-muted)' }}>No changes detected</span>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>

          {/* Section 3: Issues Fixed */}
          <div className={`vr-accordion-item ${openSections.has(3) ? 'open' : ''}`}>
            <div className="vr-accordion-header" onClick={() => toggleSection(3)}>
              <div className="vr-accordion-header-left">
                <span className={`vr-accordion-num ${issuesFixed > 0 ? 'pass' : 'info'}`}>{issuesFixed || '—'}</span>
                <span className="vr-accordion-title">Issues Fixed</span>
                <span className="vr-accordion-subtitle">{issuesFixed} anti-patterns resolved</span>
              </div>
              <div className="vr-accordion-header-right">
                <span className="vr-accordion-chevron">▼</span>
              </div>
            </div>
            <div className="vr-accordion-content">
              {issuesFixed > 0 ? (
                <table className="vr-data-table">
                  <thead>
                    <tr><th>Rule</th><th>Severity</th><th>Description</th></tr>
                  </thead>
                  <tbody>
                    {result.issues_fixed?.map((issue, i) => (
                      <tr key={i}>
                        <td><code>{issue.rule_id || issue.rule || 'UNKNOWN'}</code></td>
                        <td><span className={`vr-severity-badge ${issue.severity || 'medium'}`}>{(issue.severity || 'MEDIUM').toUpperCase()}</span></td>
                        <td>{issue.title || issue.description || 'Issue fixed'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <p style={{ color: 'var(--fg-muted)' }}>No issues were fixed in this optimization.</p>
              )}
            </div>
          </div>

          {/* Section 4: Execution Comparison */}
          <div className={`vr-accordion-item ${openSections.has(4) ? 'open' : ''}`}>
            <div className="vr-accordion-header" onClick={() => toggleSection(4)}>
              <div className="vr-accordion-header-left">
                <span className="vr-accordion-num info">4</span>
                <span className="vr-accordion-title">Execution Comparison</span>
                <span className="vr-accordion-subtitle">
                  {improvementPct != null ? `${improvementPct}% improvement` : 'Before vs after'}
                </span>
              </div>
              <div className="vr-accordion-header-right">
                <span className="vr-accordion-chevron">▼</span>
              </div>
            </div>
            <div className="vr-accordion-content">
              {timeBefore || timeAfter ? (
                <div>
                  <div className="vr-content-row">
                    <span className="vr-content-label">Time before:</span>
                    <span className="vr-content-value">{timeBefore?.toLocaleString() ?? '—'}ms</span>
                  </div>
                  <div className="vr-content-row">
                    <span className="vr-content-label">Time after:</span>
                    <span className="vr-content-value" style={{ color: 'var(--low)' }}>
                      {timeAfter?.toLocaleString() ?? '—'}ms
                      {improvementPct != null && ` (−${improvementPct}%)`}
                    </span>
                  </div>
                  {speedupRatio && speedupRatio > 1 && (
                    <div className="vr-content-row">
                      <span className="vr-content-label">Speedup:</span>
                      <span className="vr-content-value">{speedupRatio.toFixed(2)}x faster</span>
                    </div>
                  )}
                  {planComp?.original_bottleneck && (
                    <div className="vr-content-row">
                      <span className="vr-content-label">Original bottleneck:</span>
                      <span className="vr-content-value">{planComp.original_bottleneck}</span>
                    </div>
                  )}
                  {planComp?.optimized_bottleneck && (
                    <div className="vr-content-row">
                      <span className="vr-content-label">Optimized bottleneck:</span>
                      <span className="vr-content-value">{planComp.optimized_bottleneck}</span>
                    </div>
                  )}
                  {planComp?.original_total_cost != null && planComp?.optimized_total_cost != null && (
                    <div className="vr-content-row">
                      <span className="vr-content-label">Cost reduction:</span>
                      <span className="vr-content-value" style={{ color: 'var(--low)' }}>
                        {planComp.cost_improvement_pct != null
                          ? `${planComp.cost_improvement_pct.toFixed(1)}%`
                          : `${planComp.original_total_cost.toFixed(2)} → ${planComp.optimized_total_cost.toFixed(2)}`}
                      </span>
                    </div>
                  )}
                </div>
              ) : (
                <p style={{ color: 'var(--fg-muted)' }}>No execution metrics available. Connect a database for performance comparison.</p>
              )}

              {/* DuckDB Execution Plans from plan_comparison - using PlanViewer */}
              {planComp?.original_plan_summary?.plan_tree && (
                <div style={{ marginTop: '1rem' }}>
                  <PlanViewer
                    planTree={planComp.original_plan_summary.plan_tree as PlanTreeNode[]}
                    totalCost={planComp.original_total_cost}
                    executionTimeMs={planComp.original_execution_time_ms}
                    bottleneck={planComp.original_bottleneck ? {
                      operator: planComp.original_bottleneck,
                      cost_pct: 0,
                    } : undefined}
                    warnings={planComp.original_plan_summary.warnings as string[] | undefined}
                    title="Original Execution Plan"
                  />
                </div>
              )}

              {planComp?.optimized_plan_summary?.plan_tree && (
                <div style={{ marginTop: '1rem' }}>
                  <PlanViewer
                    planTree={planComp.optimized_plan_summary.plan_tree as PlanTreeNode[]}
                    totalCost={planComp.optimized_total_cost}
                    executionTimeMs={planComp.optimized_execution_time_ms}
                    bottleneck={planComp.optimized_bottleneck ? {
                      operator: planComp.optimized_bottleneck,
                      cost_pct: 0,
                    } : undefined}
                    warnings={planComp.optimized_plan_summary.warnings as string[] | undefined}
                    title="Optimized Execution Plan"
                  />
                </div>
              )}

              {/* Legacy sandbox_plans support */}
              {result.sandbox_plans && !planComp?.original_plan_summary?.plan_tree && (
                <div style={{ marginTop: '1rem' }}>
                  <div style={{ fontWeight: 600, marginBottom: '0.5rem' }}>DuckDB Execution Plans</div>
                  {result.sandbox_plans.original_plan && (
                    <div className="vr-sql-block">
                      <div className="vr-sql-block-header">
                        <span>original.plan</span>
                      </div>
                      <div className="vr-sql-block-content">{result.sandbox_plans.original_plan}</div>
                    </div>
                  )}
                  {result.sandbox_plans.optimized_plan && (
                    <div className="vr-sql-block" style={{ marginTop: '0.75rem' }}>
                      <div className="vr-sql-block-header">
                        <span>optimized.plan</span>
                      </div>
                      <div className="vr-sql-block-content">{result.sandbox_plans.optimized_plan}</div>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>

          {/* Section 5: New Issues (if any) */}
          {newIssues > 0 && (
            <div className={`vr-accordion-item ${openSections.has(5) ? 'open' : ''}`}>
              <div className="vr-accordion-header" onClick={() => toggleSection(5)}>
                <div className="vr-accordion-header-left">
                  <span className="vr-accordion-num fail">{newIssues}</span>
                  <span className="vr-accordion-title">New Issues</span>
                  <span className="vr-accordion-subtitle">issues introduced</span>
                </div>
                <div className="vr-accordion-header-right">
                  <span className="vr-accordion-badge fail">REVIEW</span>
                  <span className="vr-accordion-chevron">▼</span>
                </div>
              </div>
              <div className="vr-accordion-content">
                <table className="vr-data-table">
                  <thead>
                    <tr><th>Rule</th><th>Severity</th><th>Description</th></tr>
                  </thead>
                  <tbody>
                    {result.new_issues?.map((issue, i) => (
                      <tr key={i}>
                        <td><code>{issue.rule_id || issue.rule || 'UNKNOWN'}</code></td>
                        <td><span className={`vr-severity-badge ${issue.severity || 'medium'}`}>{(issue.severity || 'MEDIUM').toUpperCase()}</span></td>
                        <td>{issue.title || issue.description || 'New issue'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Section: Final SQL */}
          <div className={`vr-accordion-item ${openSections.has(6) ? 'open' : ''}`}>
            <div className="vr-accordion-header" onClick={() => toggleSection(6)}>
              <div className="vr-accordion-header-left">
                <span className="vr-accordion-num info">{newIssues > 0 ? 6 : 5}</span>
                <span className="vr-accordion-title">Final SQL</span>
                <span className="vr-accordion-subtitle">Optimized query</span>
              </div>
              <div className="vr-accordion-header-right">
                <span className="vr-accordion-chevron">▼</span>
              </div>
            </div>
            <div className="vr-accordion-content">
              <div style={{ fontWeight: 600, marginBottom: '0.5rem' }}>Optimized SQL (ready for deployment)</div>
              <div className="vr-sql-block">
                <div className="vr-sql-block-header">
                  <span>optimized.sql</span>
                  <button onClick={copySQL}>Copy</button>
                </div>
                <div className="vr-sql-block-content">{result.optimized_code}</div>
              </div>
              {result.llm_explanation && (
                <div style={{ marginTop: '1rem' }}>
                  <div style={{ fontWeight: 600, marginBottom: '0.5rem' }}>LLM Explanation</div>
                  <p style={{ color: 'var(--fg-muted)', lineHeight: 1.7 }}>{result.llm_explanation}</p>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Footer */}
        <footer className="vr-footer">
          <div><strong>Query Torque</strong> · Validation Report v1.0</div>
          <div className="vr-footer-actions">
            <button className="vr-footer-action" onClick={copySQL}>Copy SQL</button>
            <button className="vr-footer-action" onClick={copyJSON}>Copy JSON</button>
          </div>
        </footer>
        </div>
      </div>

      {/* Toast */}
      <div className={`vr-toast ${toast ? 'show' : ''}`}>{toast}</div>
    </div>
  )
}
