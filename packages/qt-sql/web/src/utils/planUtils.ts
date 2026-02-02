/**
 * Plan utilities for execution plan analysis
 */

import type { PlanTreeNode } from '@/api/client'

/**
 * Cost level thresholds
 */
export const COST_THRESHOLDS = {
  low: 10,
  medium: 30,
  high: 50,
} as const

/**
 * Get cost severity level based on percentage
 */
export function getCostLevel(costPct: number): 'low' | 'medium' | 'high' | 'critical' {
  if (costPct >= COST_THRESHOLDS.high) return 'critical'
  if (costPct >= COST_THRESHOLDS.medium) return 'high'
  if (costPct >= COST_THRESHOLDS.low) return 'medium'
  return 'low'
}

/**
 * Format row count with K/M suffixes
 */
export function formatRowCount(rows: number): string {
  if (rows >= 1_000_000) {
    return (rows / 1_000_000).toFixed(1) + 'M'
  }
  if (rows >= 1_000) {
    return (rows / 1_000).toFixed(1) + 'K'
  }
  return rows.toLocaleString()
}

/**
 * Format execution time
 */
export function formatExecutionTime(ms: number): string {
  if (ms >= 1000) {
    return (ms / 1000).toFixed(2) + 's'
  }
  return ms.toFixed(2) + 'ms'
}

/**
 * Find the bottleneck node in a plan tree
 */
export function findBottleneck(nodes: PlanTreeNode[]): PlanTreeNode | null {
  if (!nodes || nodes.length === 0) return null

  let maxNode: PlanTreeNode | null = null
  let maxCost = 0

  for (const node of nodes) {
    if (node.cost_pct > maxCost) {
      maxCost = node.cost_pct
      maxNode = node
    }
  }

  // Only consider it a bottleneck if it's significant (>10%)
  if (maxCost >= COST_THRESHOLDS.low) {
    return maxNode
  }

  return null
}

/**
 * Calculate total cost from plan tree
 */
export function calculateTotalCost(nodes: PlanTreeNode[]): number {
  if (!nodes || nodes.length === 0) return 0

  // Sum timing from all nodes (timing is in ms)
  return nodes.reduce((sum, node) => sum + (node.timing_ms || 0), 0)
}

/**
 * Check if a cardinality estimate is significantly off
 */
export function hasCardinalityMismatch(
  estimated: number | undefined,
  actual: number,
  threshold: number = 10
): boolean {
  if (estimated == null || estimated === 0 || actual === 0) return false

  const ratio = Math.max(estimated, actual) / Math.min(estimated, actual)
  return ratio >= threshold
}

/**
 * Operator categories for styling
 */
export const OPERATOR_CATEGORIES = {
  scan: ['TABLE_SCAN', 'SEQ_SCAN', 'INDEX_SCAN', 'FILTER'],
  join: ['HASH_JOIN', 'MERGE_JOIN', 'NESTED_LOOP', 'NESTED_LOOP_JOIN', 'CROSS_PRODUCT'],
  aggregate: ['HASH_GROUP_BY', 'AGGREGATE', 'PERFECT_HASH_GROUP_BY', 'STREAMING_AGGREGATE'],
  sort: ['ORDER_BY', 'SORT', 'TOP_N'],
  project: ['PROJECTION', 'RESULT_COLLECTOR'],
} as const

/**
 * Get operator category
 */
export function getOperatorCategory(operator: string): keyof typeof OPERATOR_CATEGORIES | 'other' {
  const normalized = operator.toUpperCase()

  for (const [category, operators] of Object.entries(OPERATOR_CATEGORIES)) {
    if ((operators as readonly string[]).some(op => normalized.includes(op))) {
      return category as keyof typeof OPERATOR_CATEGORIES
    }
  }

  return 'other'
}

/**
 * Get operator improvement suggestions
 */
export const OPERATOR_SUGGESTIONS: Record<string, string> = {
  'SEQ_SCAN': 'Consider adding an index on frequently filtered columns',
  'TABLE_SCAN': 'Full table scan detected - add indexes or filter earlier',
  'NESTED_LOOP': 'Nested loop can be slow for large tables - consider hash join',
  'NESTED_LOOP_JOIN': 'Nested loop join may benefit from indexing',
  'CROSS_PRODUCT': 'Cross product detected - likely missing join condition',
  'HASH_JOIN': 'Large hash operation - ensure adequate memory available',
  'SORT': 'Sort operation - consider adding index for pre-sorted data',
  'ORDER_BY': 'Sort required - index on ORDER BY columns may help',
}

/**
 * Get suggestion for an operator
 */
export function getOperatorSuggestion(operator: string): string | null {
  const normalized = operator.toUpperCase()

  for (const [op, suggestion] of Object.entries(OPERATOR_SUGGESTIONS)) {
    if (normalized.includes(op)) {
      return suggestion
    }
  }

  return null
}
