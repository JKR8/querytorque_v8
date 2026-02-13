/**
 * PlanViewer Component
 * Displays execution plan as a visual tree with cost bars and operator colors
 */

import type { PlanTreeNode } from '@/api/client'
import './PlanViewer.css'

interface PlanViewerProps {
  planTree: PlanTreeNode[]
  totalCost?: number
  executionTimeMs?: number
  bottleneck?: {
    operator: string
    cost_pct: number
    suggestion?: string
  }
  warnings?: string[]
  title?: string
}

function getCostColor(costPct: number): string {
  if (costPct >= 50) return 'critical'
  if (costPct >= 30) return 'high'
  if (costPct >= 10) return 'medium'
  return 'low'
}

function getOperatorCategory(operator: string): string {
  const op = operator.toLowerCase()
  if (op.includes('scan') || op.includes('read') || op.includes('fetch')) return 'scan'
  if (op.includes('join') || op.includes('merge') || op.includes('nested loop') || op.includes('hash match')) return 'join'
  if (op.includes('aggregate') || op.includes('group') || op.includes('sum') || op.includes('count') || op.includes('window')) return 'aggregate'
  if (op.includes('sort') || op.includes('order') || op.includes('top-n')) return 'sort'
  if (op.includes('filter') || op.includes('where')) return 'filter'
  return 'other'
}

function formatRows(rows: number): string {
  if (rows >= 1_000_000) return (rows / 1_000_000).toFixed(1) + 'M'
  if (rows >= 1_000) return (rows / 1_000).toFixed(1) + 'K'
  return rows.toLocaleString()
}

function formatTime(ms: number): string {
  if (ms >= 1000) return (ms / 1000).toFixed(2) + 's'
  return ms.toFixed(2) + 'ms'
}

export default function PlanViewer({
  planTree,
  totalCost,
  executionTimeMs,
  bottleneck,
  warnings,
  title = 'Execution Plan',
}: PlanViewerProps) {
  if (!planTree || planTree.length === 0) {
    return (
      <div className="pv-container pv-empty">
        <p>No execution plan available</p>
      </div>
    )
  }

  return (
    <div className="pv-container">
      {/* Header */}
      <div className="pv-header">
        <h4>{title}</h4>
        <div className="pv-metrics">
          {executionTimeMs != null && (
            <span className="pv-metric">
              <span className="pv-metric-label">Time:</span>
              <span className="pv-metric-value">{formatTime(executionTimeMs)}</span>
            </span>
          )}
          {totalCost != null && (
            <span className="pv-metric">
              <span className="pv-metric-label">Cost:</span>
              <span className="pv-metric-value">{totalCost.toFixed(2)}</span>
            </span>
          )}
        </div>
      </div>

      {/* Warnings */}
      {warnings && warnings.length > 0 && (
        <div className="pv-warnings">
          {warnings.map((w, i) => (
            <div key={i} className="pv-warning">{w}</div>
          ))}
        </div>
      )}

      {/* Bottleneck Alert */}
      {bottleneck && bottleneck.cost_pct >= 30 && (
        <div className="pv-bottleneck">
          <div className="pv-bottleneck-header">
            <span className="pv-bottleneck-icon">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
            </span>
            <span className="pv-bottleneck-title">
              Bottleneck: {bottleneck.operator} ({bottleneck.cost_pct.toFixed(1)}% cost)
            </span>
          </div>
          {bottleneck.suggestion && (
            <p className="pv-bottleneck-suggestion">{bottleneck.suggestion}</p>
          )}
        </div>
      )}

      {/* Plan Tree */}
      <div className="pv-tree">
        {planTree.map((node, i) => {
          const category = getOperatorCategory(node.operator)
          return (
            <div
              key={i}
              className={`pv-node ${node.is_bottleneck ? 'bottleneck' : ''}`}
              style={{ paddingLeft: `${node.indent * 1.5}rem` }}
            >
              {/* Connector lines */}
              {node.indent > 0 && (
                <span className="pv-connector">
                  {i === planTree.length - 1 || planTree[i + 1]?.indent <= node.indent
                    ? '\u2514\u2500'
                    : '\u251C\u2500'}
                </span>
              )}

              {/* Operator with category color */}
              <span className={`pv-operator ${node.is_bottleneck ? 'bottleneck' : ''} pv-op-${category}`}>
                {node.operator}
              </span>

              {/* Details text */}
              {node.details && (
                <span className="pv-details">{node.details}</span>
              )}

              {/* Cost Bar */}
              {node.cost_pct > 0 && (
                <div className="pv-cost-bar-container">
                  <div
                    className={`pv-cost-bar ${getCostColor(node.cost_pct)}`}
                    style={{ width: `${Math.min(node.cost_pct, 100)}%` }}
                  />
                  <span className="pv-cost-label">{node.cost_pct.toFixed(1)}%</span>
                </div>
              )}

              {/* Row Count */}
              {node.rows != null && node.rows > 0 && (
                <span className="pv-rows">
                  {formatRows(node.rows)} rows
                  {node.estimated_rows != null && node.estimated_rows !== node.rows && (
                    <span className="pv-estimated">
                      (est. {formatRows(node.estimated_rows)})
                    </span>
                  )}
                </span>
              )}

              {/* Timing */}
              {node.timing_ms != null && node.timing_ms > 0 && (
                <span className="pv-timing">{formatTime(node.timing_ms)}</span>
              )}

              {/* Badges */}
              <div className="pv-badges">
                {node.spill && (
                  <span className="pv-badge spill">SPILL</span>
                )}
                {node.pruning_ratio != null && node.pruning_ratio > 0 && (
                  <span className="pv-badge prune">
                    {node.pruning_ratio}% pruned
                  </span>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
