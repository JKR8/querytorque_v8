/**
 * BottleneckCard Component
 * Prominent callout card for audit bottleneck results
 */

import './BottleneckCard.css'

interface BottleneckCardProps {
  operator: string
  costPct: number
  details?: string
  suggestion?: string
  pathologyName?: string
}

export default function BottleneckCard({
  operator,
  costPct,
  details,
  suggestion,
  pathologyName,
}: BottleneckCardProps) {
  const severity = costPct >= 50 ? 'critical' : costPct >= 30 ? 'high' : 'medium'

  return (
    <div className={`bottleneck-card ${severity}`}>
      <div className="bottleneck-card-header">
        <span className="bottleneck-card-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="20" height="20">
            <path d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </span>
        <div className="bottleneck-card-title">
          <span className="bottleneck-card-operator">{operator}</span>
          <span className="bottleneck-card-cost">{costPct.toFixed(0)}% of query cost</span>
        </div>
      </div>

      {pathologyName && (
        <p className="bottleneck-card-pathology">{pathologyName}</p>
      )}

      {details && !pathologyName && (
        <p className="bottleneck-card-details">{details}</p>
      )}

      {suggestion && (
        <p className="bottleneck-card-suggestion">{suggestion}</p>
      )}
    </div>
  )
}
