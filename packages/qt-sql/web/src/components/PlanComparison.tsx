/**
 * PlanComparison Component
 * Side-by-side comparison of original vs optimized execution plans
 */

import PlanViewer from './PlanViewer'
import type { PlanTreeNode } from '@/api/client'
import './PlanComparison.css'

interface PlanComparisonProps {
  originalPlanTree?: PlanTreeNode[]
  optimizedPlanTree?: PlanTreeNode[]
  originalTimeMs?: number
  optimizedTimeMs?: number
  originalCost?: number
  optimizedCost?: number
  originalWarnings?: string[]
  optimizedWarnings?: string[]
  speedup?: number
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

export default function PlanComparison({
  originalPlanTree,
  optimizedPlanTree,
  originalTimeMs,
  optimizedTimeMs,
  originalCost,
  optimizedCost,
  originalWarnings,
  optimizedWarnings,
  speedup,
}: PlanComparisonProps) {
  const timeImprovementPct = (originalTimeMs && optimizedTimeMs)
    ? ((originalTimeMs - optimizedTimeMs) / originalTimeMs) * 100
    : undefined

  const costImprovementPct = (originalCost && optimizedCost)
    ? ((originalCost - optimizedCost) / originalCost) * 100
    : undefined

  const hasImprovement = (timeImprovementPct ?? 0) > 0 || (costImprovementPct ?? 0) > 0

  return (
    <div className="pc-container">
      {/* Summary Metrics */}
      <div className="pc-metrics">
        <div className="pc-metric-group">
          {(originalTimeMs != null || optimizedTimeMs != null) && (
            <div className="pc-metric">
              <span className="pc-metric-label">Execution Time</span>
              <div className="pc-metric-comparison">
                <span className="pc-metric-before">{formatTime(originalTimeMs)}</span>
                <span className="pc-metric-arrow">&rarr;</span>
                <span className={`pc-metric-after ${(timeImprovementPct ?? 0) > 0 ? 'improved' : ''}`}>
                  {formatTime(optimizedTimeMs)}
                </span>
              </div>
              {timeImprovementPct != null && (
                <span className={`pc-metric-change ${timeImprovementPct > 0 ? 'improved' : 'regressed'}`}>
                  {formatImprovement(timeImprovementPct)}
                </span>
              )}
            </div>
          )}

          {(originalCost != null || optimizedCost != null) && (
            <div className="pc-metric">
              <span className="pc-metric-label">Query Cost</span>
              <div className="pc-metric-comparison">
                <span className="pc-metric-before">{originalCost?.toFixed(0) ?? '--'}</span>
                <span className="pc-metric-arrow">&rarr;</span>
                <span className={`pc-metric-after ${(costImprovementPct ?? 0) > 0 ? 'improved' : ''}`}>
                  {optimizedCost?.toFixed(0) ?? '--'}
                </span>
              </div>
              {costImprovementPct != null && (
                <span className={`pc-metric-change ${costImprovementPct > 0 ? 'improved' : 'regressed'}`}>
                  {formatImprovement(costImprovementPct)}
                </span>
              )}
            </div>
          )}
        </div>

        {/* Speedup display */}
        {speedup != null && speedup > 0 && (
          <div className={`pc-speedup ${speedup >= 1.05 ? 'improved' : speedup < 0.95 ? 'regressed' : ''}`}>
            <span className="pc-speedup-number">{speedup.toFixed(1)}x</span>
          </div>
        )}

        {hasImprovement && (
          <div className="pc-verdict improved">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <polyline points="20,6 9,17 4,12" />
            </svg>
            Query Improved
          </div>
        )}
      </div>

      {/* Side-by-side Plans */}
      <div className="pc-plans">
        <div className="pc-plan-column">
          <PlanViewer
            planTree={originalPlanTree || []}
            totalCost={originalCost}
            executionTimeMs={originalTimeMs}
            warnings={originalWarnings}
            title="Original Plan"
          />
        </div>

        <div className="pc-plan-column">
          <PlanViewer
            planTree={optimizedPlanTree || []}
            totalCost={optimizedCost}
            executionTimeMs={optimizedTimeMs}
            warnings={optimizedWarnings}
            title="Optimized Plan"
          />
        </div>
      </div>
    </div>
  )
}
