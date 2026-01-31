/**
 * ValidationReport Component
 * Displays validation results for optimized SQL/DAX code
 * Matches the styling of the audit report templates
 */

import { useState, useMemo, useCallback } from 'react'
import '../theme/tokens.css'

export interface ValidationIssue {
  rule_id?: string
  rule?: string
  severity?: 'critical' | 'high' | 'medium' | 'low'
  title?: string
  description?: string
}

export interface PatchResult {
  issue_id: string
  status: 'applied' | 'failed' | 'skipped'
  line_matched?: number
  description?: string
  error?: string
}

export interface EquivalenceDetails {
  original_execution_time_ms?: number
  optimized_execution_time_ms?: number
  original_row_count?: number
  speedup_ratio?: number
}

export interface PlanComparison {
  original_execution_time_ms?: number
  optimized_execution_time_ms?: number
  time_improvement_pct?: number
  cost_improvement_pct?: number
  original_total_cost?: number
  optimized_total_cost?: number
  original_bottleneck?: string
  optimized_bottleneck?: string
  original_plan_summary?: {
    plan_tree?: PlanNode[]
  }
  optimized_plan_summary?: {
    plan_tree?: PlanNode[]
  }
}

export interface PlanNode {
  indent: number
  operator: string
  details?: string
  cost_pct?: number
  rows?: number
  is_bottleneck?: boolean
}

export interface ValidationResult {
  all_passed: boolean
  syntax_status: 'pass' | 'fail' | 'skip'
  schema_status?: 'pass' | 'fail' | 'skip'
  regression_status?: 'pass' | 'fail' | 'skip'
  equivalence_status?: 'pass' | 'fail' | 'skip'
  syntax_errors?: string[]
  schema_violations?: string[]
  original_code?: string
  optimized_code?: string
  issues_fixed?: ValidationIssue[]
  new_issues?: ValidationIssue[]
  equivalence_details?: EquivalenceDetails
  plan_comparison?: PlanComparison
  patch_mode?: boolean
  patch_result?: {
    patch_results?: PatchResult[]
    applied_count?: number
    total_patches?: number
    success_rate?: number
  }
  session_id?: string
  optimization_mode?: string
  can_retry?: boolean
  retry_count?: number
  max_retries?: number
  llm_explanation?: string
  errors?: string[]
}

export interface ValidationReportProps {
  /** Validation result data */
  result: ValidationResult
  /** Callback when user accepts the optimization */
  onAccept: () => void
  /** Callback when user rejects/cancels */
  onReject: () => void
  /** Callback when user wants to retry */
  onRetry: () => void
  /** Whether accept action is in progress */
  isAccepting?: boolean
  /** Additional CSS class name */
  className?: string
}

interface DiffLine {
  type: 'context' | 'added' | 'removed'
  content: string
  lineNum?: number
}

/**
 * Compute a simple line-by-line diff between two code strings
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

const styles: Record<string, React.CSSProperties> = {
  container: {
    fontFamily: 'var(--qt-font-sans)',
    fontSize: 'var(--qt-text-base)',
    color: 'var(--qt-fg)',
    background: 'var(--qt-bg-card)',
    border: '1px solid var(--qt-border)',
    borderRadius: 'var(--qt-radius)',
    overflow: 'hidden',
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '0.75rem 1rem',
    background: 'var(--qt-bg-alt)',
    borderBottom: '1px solid var(--qt-border)',
  },
  headerTitle: {
    fontWeight: 'var(--qt-font-semibold)',
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
  },
  statusBadge: {
    fontSize: 'var(--qt-text-xs)',
    fontWeight: 'var(--qt-font-bold)',
    padding: '0.25rem 0.5rem',
    borderRadius: 'var(--qt-radius-sm)',
  },
  actions: {
    display: 'flex',
    gap: '0.5rem',
  },
  actionBtn: {
    padding: '0.5rem 1rem',
    fontSize: 'var(--qt-text-base)',
    fontWeight: 'var(--qt-font-medium)',
    borderRadius: 'var(--qt-radius-sm)',
    border: '1px solid var(--qt-border)',
    background: 'var(--qt-bg-card)',
    cursor: 'pointer',
    transition: 'all var(--qt-transition-fast)',
  },
  content: {
    padding: '1.5rem',
    maxHeight: '600px',
    overflowY: 'auto',
  },
  verdict: {
    display: 'grid',
    gridTemplateColumns: '80px 1fr',
    gap: '1.5rem',
    marginBottom: '1.5rem',
    padding: '1rem',
    background: 'var(--qt-bg-alt)',
    borderRadius: 'var(--qt-radius)',
  },
  verdictIcon: {
    width: '64px',
    height: '64px',
    borderRadius: 'var(--qt-radius)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontSize: '2rem',
    fontWeight: 'var(--qt-font-bold)',
  },
  verdictStats: {
    display: 'flex',
    gap: '1.5rem',
    marginTop: '0.5rem',
    flexWrap: 'wrap',
  },
  stat: {
    textAlign: 'center',
  },
  statValue: {
    fontSize: 'var(--qt-text-lg)',
    fontWeight: 'var(--qt-font-bold)',
  },
  statLabel: {
    fontSize: 'var(--qt-text-xs)',
    color: 'var(--qt-fg-muted)',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
  },
  accordion: {
    border: '1px solid var(--qt-border)',
    borderRadius: 'var(--qt-radius)',
    overflow: 'hidden',
    marginBottom: '0.75rem',
  },
  accordionHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '0.75rem 1rem',
    background: 'var(--qt-bg-alt)',
    cursor: 'pointer',
    userSelect: 'none',
  },
  accordionContent: {
    padding: '1rem',
    borderTop: '1px solid var(--qt-border)',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: 'var(--qt-text-sm)',
  },
  diffBlock: {
    background: 'var(--qt-bg-code)',
    borderRadius: 'var(--qt-radius-sm)',
    overflow: 'hidden',
    fontFamily: 'var(--qt-font-mono)',
    fontSize: 'var(--qt-text-sm)',
  },
  diffHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    padding: '0.5rem 1rem',
    background: '#2d2d3d',
    color: 'var(--qt-fg-code)',
    fontSize: 'var(--qt-text-xs)',
  },
  diffLine: {
    display: 'flex',
    padding: '0 1rem',
    lineHeight: '1.6',
  },
  diffMarker: {
    width: '1.5rem',
    flexShrink: 0,
    textAlign: 'center',
    userSelect: 'none',
  },
  diffText: {
    flex: 1,
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
  },
  toast: {
    position: 'fixed',
    bottom: '2rem',
    left: '50%',
    transform: 'translateX(-50%) translateY(100px)',
    background: 'var(--qt-bg-code)',
    color: 'var(--qt-fg-code)',
    padding: '0.75rem 1.5rem',
    borderRadius: 'var(--qt-radius)',
    fontWeight: 'var(--qt-font-medium)',
    boxShadow: 'var(--qt-shadow-lg)',
    opacity: 0,
    transition: 'all 0.3s',
    zIndex: 'var(--qt-z-toast)',
  },
  toastShow: {
    transform: 'translateX(-50%) translateY(0)',
    opacity: 1,
  },
}

export function ValidationReport({
  result,
  onAccept,
  onReject,
  onRetry,
  isAccepting = false,
  className,
}: ValidationReportProps) {
  const [openSections, setOpenSections] = useState<Set<number>>(new Set([1, 2]))
  const [toast, setToast] = useState<string | null>(null)

  const allPassed = result.all_passed
  const canAccept = allPassed && result.syntax_status === 'pass'
  const canRetry = result.can_retry !== false && (result.retry_count ?? 0) < (result.max_retries ?? 3)

  const toggleSection = useCallback((num: number) => {
    setOpenSections(prev => {
      const next = new Set(prev)
      if (next.has(num)) {
        next.delete(num)
      } else {
        next.add(num)
      }
      return next
    })
  }, [])

  const showToast = useCallback((msg: string) => {
    setToast(msg)
    setTimeout(() => setToast(null), 2000)
  }, [])

  const copySQL = useCallback(() => {
    navigator.clipboard.writeText(result.optimized_code || '')
      .then(() => showToast('SQL copied!'))
  }, [result.optimized_code, showToast])

  // Extract metrics
  const issuesFixed = result.issues_fixed?.length ?? 0
  const newIssues = result.new_issues?.length ?? 0
  const eq = result.equivalence_details
  const planComp = result.plan_comparison

  // Calculate timing improvements
  const timeBefore = planComp?.original_execution_time_ms || eq?.original_execution_time_ms
  const timeAfter = planComp?.optimized_execution_time_ms || eq?.optimized_execution_time_ms
  const improvementPct = planComp?.time_improvement_pct != null
    ? Math.round(planComp.time_improvement_pct)
    : (timeBefore && timeAfter && timeBefore > 0
        ? Math.round((timeBefore - timeAfter) / timeBefore * 100)
        : null)

  // Checks passed count
  const checksPassed = [result.syntax_status, result.schema_status, result.regression_status, result.equivalence_status]
    .filter(s => s === 'pass').length

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

  const getStatusBadgeStyle = (status?: string): React.CSSProperties => {
    switch (status) {
      case 'pass':
        return { background: 'var(--qt-low)', color: 'white' }
      case 'fail':
        return { background: 'var(--qt-critical)', color: 'white' }
      case 'skip':
        return { background: 'var(--qt-fg-muted)', color: 'white' }
      default:
        return { background: 'var(--qt-border)', color: 'var(--qt-fg)' }
    }
  }

  const getDiffLineStyle = (type: 'context' | 'added' | 'removed'): React.CSSProperties => {
    switch (type) {
      case 'added':
        return { background: 'rgba(34, 197, 94, 0.15)', color: '#86efac' }
      case 'removed':
        return { background: 'rgba(239, 68, 68, 0.15)', color: '#fca5a5' }
      default:
        return { color: 'var(--qt-fg-code)' }
    }
  }

  return (
    <div style={styles.container} className={className}>
      {/* Header */}
      <div style={styles.header}>
        <span style={styles.headerTitle}>
          Validation Results
          <span
            style={{
              ...styles.statusBadge,
              background: allPassed ? 'var(--qt-low)' : 'var(--qt-critical)',
              color: 'white',
            }}
          >
            {allPassed ? 'PASS' : 'FAIL'}
          </span>
        </span>
        <div style={styles.actions}>
          <button
            style={{
              ...styles.actionBtn,
              background: 'var(--qt-low)',
              color: 'white',
              borderColor: 'var(--qt-low)',
              opacity: !canAccept || isAccepting ? 0.5 : 1,
              cursor: !canAccept || isAccepting ? 'not-allowed' : 'pointer',
            }}
            onClick={onAccept}
            disabled={!canAccept || isAccepting}
          >
            {isAccepting ? 'Accepting...' : 'Accept'}
          </button>
          <button
            style={{
              ...styles.actionBtn,
              opacity: isAccepting || !canRetry ? 0.5 : 1,
              cursor: isAccepting || !canRetry ? 'not-allowed' : 'pointer',
            }}
            onClick={onRetry}
            disabled={isAccepting || !canRetry}
          >
            Retry
          </button>
          <button
            style={{
              ...styles.actionBtn,
              color: 'var(--qt-critical)',
              borderColor: 'var(--qt-critical)',
              opacity: isAccepting ? 0.5 : 1,
              cursor: isAccepting ? 'not-allowed' : 'pointer',
            }}
            onClick={onReject}
            disabled={isAccepting}
          >
            Cancel
          </button>
        </div>
      </div>

      {/* Content */}
      <div style={styles.content}>
        {/* Verdict */}
        <div style={styles.verdict}>
          <div
            style={{
              ...styles.verdictIcon,
              background: allPassed ? 'var(--qt-low-bg)' : 'var(--qt-critical-bg)',
              color: allPassed ? 'var(--qt-low)' : 'var(--qt-critical)',
            }}
          >
            {allPassed ? '\u2713' : '\u2717'}
          </div>
          <div>
            <div style={{ fontWeight: 'var(--qt-font-semibold)', marginBottom: '0.25rem' }}>
              {allPassed
                ? 'All validation checks passed - Ready for deployment'
                : `Validation failed - ${result.errors?.length || 0} error(s) detected`}
            </div>
            <div style={styles.verdictStats}>
              <div style={styles.stat}>
                <div style={{ ...styles.statValue, color: checksPassed === 4 ? 'var(--qt-low)' : 'var(--qt-fg)' }}>
                  {checksPassed}/4
                </div>
                <div style={styles.statLabel}>checks passed</div>
              </div>
              <div style={styles.stat}>
                <div style={{ ...styles.statValue, color: issuesFixed > 0 ? 'var(--qt-low)' : 'var(--qt-fg)' }}>
                  {issuesFixed}
                </div>
                <div style={styles.statLabel}>issues fixed</div>
              </div>
              {newIssues > 0 && (
                <div style={styles.stat}>
                  <div style={{ ...styles.statValue, color: 'var(--qt-critical)' }}>{newIssues}</div>
                  <div style={styles.statLabel}>new issues</div>
                </div>
              )}
              {improvementPct != null && improvementPct > 0 && (
                <div style={styles.stat}>
                  <div style={{ ...styles.statValue, color: 'var(--qt-low)' }}>-{improvementPct}%</div>
                  <div style={styles.statLabel}>execution time</div>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Validation Checks Accordion */}
        <div style={styles.accordion}>
          <div style={styles.accordionHeader} onClick={() => toggleSection(1)}>
            <span>
              <strong>Validation Checks</strong>
              <span style={{ color: 'var(--qt-fg-muted)', marginLeft: '0.5rem' }}>
                {checksPassed}/4 passed
              </span>
            </span>
            <span>{openSections.has(1) ? '\u25B2' : '\u25BC'}</span>
          </div>
          {openSections.has(1) && (
            <div style={styles.accordionContent}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    <th style={{ textAlign: 'left', padding: '0.5rem' }}>Check</th>
                    <th style={{ textAlign: 'left', padding: '0.5rem' }}>Status</th>
                    <th style={{ textAlign: 'left', padding: '0.5rem' }}>Detail</th>
                  </tr>
                </thead>
                <tbody>
                  {[
                    { name: 'Syntax', status: result.syntax_status, detail: result.syntax_errors?.join(', ') || 'No syntax errors' },
                    { name: 'Schema', status: result.schema_status, detail: result.schema_violations?.join(', ') || 'All references valid' },
                    { name: 'Regression', status: result.regression_status, detail: newIssues > 0 ? `${newIssues} new issue(s)` : 'No new issues' },
                    { name: 'Equivalence', status: result.equivalence_status, detail: result.equivalence_status === 'pass' ? `Results match` : result.equivalence_status === 'skip' ? 'Not tested' : 'Results differ' },
                  ].map(check => (
                    <tr key={check.name}>
                      <td style={{ padding: '0.5rem', fontWeight: 'var(--qt-font-medium)' }}>{check.name}</td>
                      <td style={{ padding: '0.5rem' }}>
                        <span style={{ ...styles.statusBadge, ...getStatusBadgeStyle(check.status) }}>
                          {check.status?.toUpperCase() || 'N/A'}
                        </span>
                      </td>
                      <td style={{ padding: '0.5rem', color: 'var(--qt-fg-muted)' }}>{check.detail}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* SQL Diff Accordion */}
        <div style={styles.accordion}>
          <div style={styles.accordionHeader} onClick={() => toggleSection(2)}>
            <span>
              <strong>SQL Diff</strong>
              <span style={{ color: 'var(--qt-fg-muted)', marginLeft: '0.5rem' }}>
                +{addedCount} -{removedCount} lines
                {hasPatchMode && ` | ${patchResults.length} patches`}
              </span>
            </span>
            <span>{openSections.has(2) ? '\u25B2' : '\u25BC'}</span>
          </div>
          {openSections.has(2) && (
            <div style={styles.accordionContent}>
              <div style={styles.diffBlock}>
                <div style={styles.diffHeader}>
                  <span>diff --original --optimized</span>
                  <span>+{addedCount} -{removedCount}</span>
                </div>
                <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
                  {diffLines.map((line, i) => (
                    <div key={i} style={{ ...styles.diffLine, ...getDiffLineStyle(line.type) }}>
                      <span style={styles.diffMarker}>
                        {line.type === 'added' ? '+' : line.type === 'removed' ? '-' : ' '}
                      </span>
                      <span style={styles.diffText}>{line.content || ' '}</span>
                    </div>
                  ))}
                  {diffLines.length === 0 && (
                    <div style={{ ...styles.diffLine, color: 'var(--qt-fg-muted)' }}>
                      <span style={styles.diffMarker}> </span>
                      <span style={styles.diffText}>No changes detected</span>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Final SQL Accordion */}
        <div style={styles.accordion}>
          <div style={styles.accordionHeader} onClick={() => toggleSection(3)}>
            <span>
              <strong>Final SQL</strong>
              <span style={{ color: 'var(--qt-fg-muted)', marginLeft: '0.5rem' }}>Optimized query</span>
            </span>
            <span>{openSections.has(3) ? '\u25B2' : '\u25BC'}</span>
          </div>
          {openSections.has(3) && (
            <div style={styles.accordionContent}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                <span style={{ fontWeight: 'var(--qt-font-semibold)' }}>Optimized SQL</span>
                <button
                  style={{ ...styles.actionBtn, padding: '0.25rem 0.75rem', fontSize: 'var(--qt-text-xs)' }}
                  onClick={copySQL}
                >
                  Copy
                </button>
              </div>
              <pre
                style={{
                  background: 'var(--qt-bg-code)',
                  color: 'var(--qt-fg-code)',
                  padding: '1rem',
                  borderRadius: 'var(--qt-radius-sm)',
                  fontFamily: 'var(--qt-font-mono)',
                  fontSize: 'var(--qt-text-sm)',
                  whiteSpace: 'pre-wrap',
                  overflowX: 'auto',
                  maxHeight: '300px',
                  margin: 0,
                }}
              >
                {result.optimized_code}
              </pre>
              {result.llm_explanation && (
                <div style={{ marginTop: '1rem' }}>
                  <div style={{ fontWeight: 'var(--qt-font-semibold)', marginBottom: '0.5rem' }}>Explanation</div>
                  <p style={{ color: 'var(--qt-fg-muted)', lineHeight: 1.7 }}>{result.llm_explanation}</p>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Toast */}
      <div
        style={{
          ...styles.toast,
          ...(toast ? styles.toastShow : {}),
        }}
      >
        {toast}
      </div>
    </div>
  )
}

export default ValidationReport
