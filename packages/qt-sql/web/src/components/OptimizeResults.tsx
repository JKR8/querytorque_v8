/**
 * OptimizeResults Component
 * Full optimization result view: SpeedupCard + plan comparison + optimized SQL
 */

import { useState } from 'react'
import SpeedupCard from './SpeedupCard'
import PlanViewer from './PlanViewer'
import type { OptimizeResponse, PlanTreeNode } from '@/api/client'
import './OptimizeResults.css'

interface OptimizeResultsProps {
  result: OptimizeResponse
  originalPlan?: PlanTreeNode[]
  optimizedPlan?: PlanTreeNode[]
  originalCost?: number
  optimizedCost?: number
  originalTimeMs?: number
  optimizedTimeMs?: number
}

export default function OptimizeResults({
  result,
  originalPlan,
  optimizedPlan,
  originalCost,
  optimizedCost,
  originalTimeMs,
  optimizedTimeMs,
}: OptimizeResultsProps) {
  const [copied, setCopied] = useState(false)

  if (result.status === 'ERROR') {
    return (
      <div className="opt-results opt-results-error">
        <p>Optimization failed: {result.error}</p>
      </div>
    )
  }

  const handleCopy = async () => {
    if (result.optimized_sql) {
      await navigator.clipboard.writeText(result.optimized_sql)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  return (
    <div className="opt-results">
      {/* Speedup Card */}
      <SpeedupCard
        speedup={result.speedup}
        status={result.status}
        transforms={result.transforms}
      />

      {/* Plan Comparison */}
      {(originalPlan || optimizedPlan) && (
        <div className="opt-results-plans">
          <div className="opt-results-plan-col">
            {originalPlan && (
              <PlanViewer
                planTree={originalPlan}
                totalCost={originalCost}
                executionTimeMs={originalTimeMs}
                title="Original Plan"
              />
            )}
          </div>
          <div className="opt-results-plan-col">
            {optimizedPlan && (
              <PlanViewer
                planTree={optimizedPlan}
                totalCost={optimizedCost}
                executionTimeMs={optimizedTimeMs}
                title="Optimized Plan"
              />
            )}
          </div>
        </div>
      )}

      {/* Optimized SQL */}
      {result.optimized_sql && (
        <div className="opt-results-sql">
          <div className="opt-results-sql-header">
            <span>Optimized SQL</span>
            <button className="opt-results-copy-btn" onClick={handleCopy}>
              {copied ? 'Copied!' : 'Copy'}
            </button>
          </div>
          <pre className="opt-results-sql-code">
            <code>{result.optimized_sql}</code>
          </pre>
        </div>
      )}
    </div>
  )
}
