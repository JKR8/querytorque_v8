/**
 * AuditResults Component
 * Free-tier result view: BottleneckCard + PlanViewer + warnings
 */

import BottleneckCard from './BottleneckCard'
import PlanViewer from './PlanViewer'
import type { AuditResponse } from '@/api/client'
import './AuditResults.css'

interface AuditResultsProps {
  result: AuditResponse
}

export default function AuditResults({ result }: AuditResultsProps) {
  if (!result.success) {
    return (
      <div className="audit-results audit-results-error">
        <p>{result.error || 'Audit failed'}</p>
      </div>
    )
  }

  return (
    <div className="audit-results">
      {/* Bottleneck Card */}
      {result.bottleneck && result.bottleneck.cost_pct >= 10 && (
        <BottleneckCard
          operator={result.bottleneck.operator}
          costPct={result.bottleneck.cost_pct}
          details={result.bottleneck.details}
          suggestion={result.bottleneck.suggestion}
          pathologyName={result.pathology_name}
        />
      )}

      {/* Warnings */}
      {result.warnings.length > 0 && (
        <div className="audit-results-warnings">
          {result.warnings.map((w, i) => (
            <div key={i} className="audit-results-warning">{w}</div>
          ))}
        </div>
      )}

      {/* Plan Tree */}
      {result.plan_tree && result.plan_tree.length > 0 && (
        <PlanViewer
          planTree={result.plan_tree}
          totalCost={result.total_cost}
          executionTimeMs={result.execution_time_ms}
          title="Execution Plan"
        />
      )}

      {!result.plan_tree && !result.bottleneck && (
        <div className="audit-results-empty">
          <p>No performance issues detected.</p>
        </div>
      )}
    </div>
  )
}
