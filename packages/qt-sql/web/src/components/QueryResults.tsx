/**
 * Query Results Component
 * Displays SQL query execution results in a data table
 */

import { useMemo } from 'react'
import './QueryResults.css'

export interface QueryResultData {
  columns: string[]
  column_types: string[]
  rows: unknown[][]
  row_count: number
  execution_time_ms: number
  truncated?: boolean
  error?: string
}

interface QueryResultsProps {
  result: QueryResultData | null
  isLoading?: boolean
  error?: string | null
}

function formatValue(value: unknown): string {
  if (value === null) {
    return 'NULL'
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false'
  }
  if (typeof value === 'number') {
    return value.toLocaleString()
  }
  if (typeof value === 'string') {
    // Truncate long strings
    if (value.length > 100) {
      return value.slice(0, 100) + '...'
    }
    return value
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

function getTypeClass(type: string): string {
  const t = type.toLowerCase()
  if (t.includes('int') || t.includes('float') || t.includes('decimal') || t.includes('numeric') || t.includes('double')) {
    return 'type-number'
  }
  if (t.includes('varchar') || t.includes('text') || t.includes('char')) {
    return 'type-string'
  }
  if (t.includes('bool')) {
    return 'type-boolean'
  }
  if (t.includes('date') || t.includes('time') || t.includes('timestamp')) {
    return 'type-date'
  }
  return 'type-other'
}

export default function QueryResults({ result, isLoading, error }: QueryResultsProps) {
  const displayError = error || result?.error

  const tableContent = useMemo(() => {
    if (!result || result.rows.length === 0) return null

    return (
      <table className="qr-table">
        <thead>
          <tr>
            {result.columns.map((col, i) => (
              <th key={i}>
                <div className="qr-header-content">
                  <span className="qr-column-name">{col}</span>
                  {result.column_types[i] && (
                    <span className={`qr-column-type ${getTypeClass(result.column_types[i])}`}>
                      {result.column_types[i]}
                    </span>
                  )}
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, rowIdx) => (
            <tr key={rowIdx}>
              {row.map((cell, cellIdx) => (
                <td
                  key={cellIdx}
                  className={cell === null ? 'qr-null' : ''}
                >
                  {formatValue(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    )
  }, [result])

  if (isLoading) {
    return (
      <div className="qr-container qr-loading">
        <div className="spinner-large" />
        <p>Executing query...</p>
      </div>
    )
  }

  if (displayError) {
    return (
      <div className="qr-container qr-error">
        <div className="qr-error-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="12" cy="12" r="10" />
            <line x1="15" y1="9" x2="9" y2="15" />
            <line x1="9" y1="9" x2="15" y2="15" />
          </svg>
        </div>
        <div className="qr-error-message">{displayError}</div>
      </div>
    )
  }

  if (!result) {
    return (
      <div className="qr-container qr-empty">
        <p>Run a query to see results</p>
      </div>
    )
  }

  if (result.rows.length === 0) {
    return (
      <div className="qr-container qr-empty">
        <p>Query returned no results</p>
        <span className="qr-time">{result.execution_time_ms.toFixed(2)}ms</span>
      </div>
    )
  }

  return (
    <div className="qr-container">
      <div className="qr-table-wrapper">
        {tableContent}
      </div>
      <div className="qr-footer">
        <span className="qr-row-count">
          {result.row_count.toLocaleString()} row{result.row_count !== 1 ? 's' : ''}
          {result.truncated && ' (truncated)'}
        </span>
        <span className="qr-time">{result.execution_time_ms.toFixed(2)}ms</span>
      </div>
    </div>
  )
}
