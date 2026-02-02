import { useState, useEffect } from 'react'
import { getDatabaseSchema, SchemaResponse } from '@/api/client'
import './DBExplorer.css'

interface TableColumn {
  name: string
  type: string
  nullable?: boolean
}

interface DBExplorerProps {
  sessionId: string
  onTableSelect?: (tableName: string) => void
}

// SVG Icons
const DatabaseIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <ellipse cx="12" cy="5" rx="9" ry="3"/>
    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
  </svg>
)

const TableIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
    <line x1="3" y1="9" x2="21" y2="9"/>
    <line x1="9" y1="21" x2="9" y2="9"/>
  </svg>
)

const ChevronIcon = ({ expanded }: { expanded: boolean }) => (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    style={{ transform: expanded ? 'rotate(90deg)' : 'rotate(0deg)', transition: 'transform 0.15s ease' }}
  >
    <polyline points="9 18 15 12 9 6"/>
  </svg>
)

const ColumnIcon = () => (
  <svg viewBox="0 0 24 24" fill="currentColor">
    <circle cx="12" cy="12" r="3"/>
  </svg>
)

const SpinnerIcon = () => (
  <svg className="db-explorer__spinner" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="12" cy="12" r="10" opacity="0.25"/>
    <path d="M12 2a10 10 0 0 1 10 10" strokeLinecap="round"/>
  </svg>
)

const AlertIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="10"/>
    <line x1="12" y1="8" x2="12" y2="12"/>
    <line x1="12" y1="16" x2="12.01" y2="16"/>
  </svg>
)

const EmptyIcon = () => (
  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M21 9v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V9"/>
    <path d="M21 9l-9-7-9 7"/>
  </svg>
)

export default function DBExplorer({ sessionId, onTableSelect }: DBExplorerProps) {
  const [schema, setSchema] = useState<SchemaResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set())

  useEffect(() => {
    async function fetchSchema() {
      try {
        setLoading(true)
        setError(null)
        const result = await getDatabaseSchema(sessionId)
        setSchema(result)
        // Auto-expand first table
        const tableNames = Object.keys(result.tables || {})
        if (tableNames.length > 0) {
          setExpandedTables(new Set([tableNames[0]]))
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load schema')
      } finally {
        setLoading(false)
      }
    }
    fetchSchema()
  }, [sessionId])

  const toggleTable = (tableName: string) => {
    setExpandedTables(prev => {
      const next = new Set(prev)
      if (next.has(tableName)) {
        next.delete(tableName)
      } else {
        next.add(tableName)
      }
      return next
    })
  }

  const handleTableClick = (tableName: string) => {
    toggleTable(tableName)
    onTableSelect?.(tableName)
  }

  if (loading) {
    return (
      <div className="db-explorer">
        <div className="db-explorer__header">
          <span className="db-explorer__header-icon"><DatabaseIcon /></span>
          <span className="db-explorer__header-text">Explorer</span>
        </div>
        <div className="db-explorer__state db-explorer__state--loading">
          <SpinnerIcon />
          <span>Loading schema...</span>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="db-explorer">
        <div className="db-explorer__header">
          <span className="db-explorer__header-icon"><DatabaseIcon /></span>
          <span className="db-explorer__header-text">Explorer</span>
        </div>
        <div className="db-explorer__state db-explorer__state--error">
          <AlertIcon />
          <span>{error}</span>
        </div>
      </div>
    )
  }

  const tables = schema?.tables || {}
  const tableNames = Object.keys(tables)

  if (tableNames.length === 0) {
    return (
      <div className="db-explorer">
        <div className="db-explorer__header">
          <span className="db-explorer__header-icon"><DatabaseIcon /></span>
          <span className="db-explorer__header-text">Explorer</span>
        </div>
        <div className="db-explorer__state db-explorer__state--empty">
          <EmptyIcon />
          <span>No tables found</span>
        </div>
      </div>
    )
  }

  return (
    <div className="db-explorer">
      <div className="db-explorer__header">
        <span className="db-explorer__header-icon"><DatabaseIcon /></span>
        <span className="db-explorer__header-text">Schema</span>
      </div>
      <div className="db-explorer__count">
        <span className="db-explorer__count-number">{tableNames.length}</span>
        <span className="db-explorer__count-label">table{tableNames.length !== 1 ? 's' : ''}</span>
      </div>
      <div className="db-explorer__list">
        {tableNames.map(tableName => {
          const columns = tables[tableName] as TableColumn[]
          const isExpanded = expandedTables.has(tableName)

          return (
            <div key={tableName} className={`db-explorer__table ${isExpanded ? 'db-explorer__table--expanded' : ''}`}>
              <div
                className="db-explorer__table-header"
                onClick={() => handleTableClick(tableName)}
              >
                <span className="db-explorer__chevron">
                  <ChevronIcon expanded={isExpanded} />
                </span>
                <span className="db-explorer__table-icon"><TableIcon /></span>
                <span className="db-explorer__table-name">{tableName}</span>
                <span className="db-explorer__column-count">
                  {Array.isArray(columns) ? columns.length : 0}
                </span>
              </div>
              {isExpanded && Array.isArray(columns) && (
                <div className="db-explorer__columns">
                  {columns.map((col, idx) => (
                    <div key={idx} className="db-explorer__column">
                      <span className="db-explorer__column-icon"><ColumnIcon /></span>
                      <span className="db-explorer__column-name">{col.name}</span>
                      <span className="db-explorer__column-type">{col.type}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
