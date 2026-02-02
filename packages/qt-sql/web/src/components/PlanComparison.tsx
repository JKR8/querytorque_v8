/**
 * PlanComparison Component
 * Side-by-side comparison of original vs optimized execution plans
 */

import PlanViewer from './PlanViewer'
import type { PlanTreeNode, ExecutionPlanComparison } from '@/api/client'
import './PlanComparison.css'

interface PlanComparisonProps {
  comparison: ExecutionPlanComparison
}

function formatImprovement(pct: number | undefined): string {
  if (pct == null) return '--'
  if (pct > 0) return `-${pct.toFixed(1)}%`
  if (pct < 0) return `+${Math.abs(pct).toFixed(1)}%`
  return '0%'
}

function formatTime(ms: number | undefined): string {
  if (ms == null) return '--'
  if (ms >= 1000) return (ms / 1000).toFixed(2) + 's'
  return ms.toFixed(2) + 'ms'
}

export default function PlanComparison({ comparison }: PlanComparisonProps) {
  const {
    original_execution_time_ms,
    optimized_execution_time_ms,
    time_improvement_pct,
    cost_improvement_pct,
    original_bottleneck,
    optimized_bottleneck,
    original_plan_summary,
    optimized_plan_summary,
    original_total_cost,
    optimized_total_cost,
  } = comparison

  const originalPlanTree = (original_plan_summary?.plan_tree || []) as PlanTreeNode[]
  const optimizedPlanTree = (optimized_plan_summary?.plan_tree || []) as PlanTreeNode[]
  const originalWarnings = original_plan_summary?.warnings as string[] | undefined
  const optimizedWarnings = optimized_plan_summary?.warnings as string[] | undefined

  const hasImprovement = (time_improvement_pct ?? 0) > 0 || (cost_improvement_pct ?? 0) > 0

  return (
    <div className="pc-container">
      {/* Summary Metrics */}
      <div className="pc-metrics">
        <div className="pc-metric-group">
          <div className="pc-metric">
            <span className="pc-metric-label">Execution Time</span>
            <div className="pc-metric-comparison">
              <span className="pc-metric-before">{formatTime(original_execution_time_ms)}</span>
              <span className="pc-metric-arrow">→</span>
              <span className={`pc-metric-after ${(time_improvement_pct ?? 0) > 0 ? 'improved' : ''}`}>
                {formatTime(optimized_execution_time_ms)}
              </span>
            </div>
            {time_improvement_pct != null && (
              <span className={`pc-metric-change ${time_improvement_pct > 0 ? 'improved' : 'regressed'}`}>
                {formatImprovement(time_improvement_pct)}
              </span>
            )}
          </div>

          <div className="pc-metric">
            <span className="pc-metric-label">Query Cost</span>
            <div className="pc-metric-comparison">
              <span className="pc-metric-before">{original_total_cost?.toFixed(0) ?? '--'}</span>
              <span className="pc-metric-arrow">→</span>
              <span className={`pc-metric-after ${(cost_improvement_pct ?? 0) > 0 ? 'improved' : ''}`}>
                {optimized_total_cost?.toFixed(0) ?? '--'}
              </span>
            </div>
            {cost_improvement_pct != null && (
              <span className={`pc-metric-change ${cost_improvement_pct > 0 ? 'improved' : 'regressed'}`}>
                {formatImprovement(cost_improvement_pct)}
              </span>
            )}
          </div>
        </div>

        {hasImprovement && (
          <div className="pc-verdict improved">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <polyline points="20,6 9,17 4,12" />
            </svg>
            Query Improved
          </div>
        )}
      </div>

      {/* Bottleneck Changes */}
      {(original_bottleneck || optimized_bottleneck) && (
        <div className="pc-bottleneck-summary">
          <div className="pc-bottleneck-item">
            <span className="pc-bottleneck-label">Original Bottleneck:</span>
            <span className="pc-bottleneck-value">{original_bottleneck || 'None'}</span>
          </div>
          <div className="pc-bottleneck-item">
            <span className="pc-bottleneck-label">Optimized Bottleneck:</span>
            <span className="pc-bottleneck-value">{optimized_bottleneck || 'None'}</span>
          </div>
        </div>
      )}

      {/* Side-by-side Plans */}
      <div className="pc-plans">
        <div className="pc-plan-column">
          <PlanViewer
            planTree={originalPlanTree}
            totalCost={original_total_cost}
            executionTimeMs={original_execution_time_ms}
            warnings={originalWarnings}
            title="Original Plan"
          />
        </div>

        <div className="pc-plan-column">
          <PlanViewer
            planTree={optimizedPlanTree}
            totalCost={optimized_total_cost}
            executionTimeMs={optimized_execution_time_ms}
            warnings={optimizedWarnings}
            title="Optimized Plan"
          />
        </div>
      </div>
    </div>
  )
}
