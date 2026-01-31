import { useState, useCallback } from 'react'
import DropZone from '../components/DropZone'
import { getModelBrowserData, ModelBrowserData, Table, Measure, Relationship } from '../api/client'

type ViewMode = 'tables' | 'measures' | 'relationships'

export default function ModelBrowserPage() {
  const [modelData, setModelData] = useState<ModelBrowserData | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [viewMode, setViewMode] = useState<ViewMode>('tables')
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedMeasure, setSelectedMeasure] = useState<Measure | null>(null)

  const handleFilesSelected = useCallback(async (files: File[]) => {
    if (files.length === 0) return

    setIsLoading(true)
    setError(null)

    try {
      const data = await getModelBrowserData(files[0])
      setModelData(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load model')
    } finally {
      setIsLoading(false)
    }
  }, [])

  const filteredTables = modelData?.tables.filter(t =>
    t.name.toLowerCase().includes(searchTerm.toLowerCase())
  ) || []

  const filteredMeasures = modelData?.measures.filter(m =>
    m.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    m.table_name.toLowerCase().includes(searchTerm.toLowerCase())
  ) || []

  const filteredRelationships = modelData?.relationships.filter(r =>
    r.from_table.toLowerCase().includes(searchTerm.toLowerCase()) ||
    r.to_table.toLowerCase().includes(searchTerm.toLowerCase())
  ) || []

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]
  }

  const formatNumber = (num: number): string => {
    return new Intl.NumberFormat().format(num)
  }

  if (!modelData && !isLoading) {
    return (
      <div className="model-browser-page">
        <div className="upload-section">
          <h1>Model Browser</h1>
          <p className="upload-description">
            Upload a VPAX file to browse your Power BI model structure,
            view tables, measures, and relationships.
          </p>

          <DropZone
            accept=".vpax"
            onFilesSelected={handleFilesSelected}
          />

          {error && (
            <div className="error-message">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="20" height="20">
                <circle cx="12" cy="12" r="10" />
                <line x1="15" y1="9" x2="9" y2="15" />
                <line x1="9" y1="9" x2="15" y2="15" />
              </svg>
              {error}
            </div>
          )}
        </div>

        <style>{uploadStyles}</style>
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="model-browser-page">
        <div className="loading-section">
          <div className="spinner" />
          <h2>Loading Model</h2>
          <p>Parsing VPAX file...</p>
        </div>
        <style>{uploadStyles}</style>
      </div>
    )
  }

  return (
    <div className="model-browser-page">
      {/* Model Summary Header */}
      <div className="model-header">
        <div className="model-title">
          <h1>{modelData?.model_summary.name || 'Power BI Model'}</h1>
          <span className="model-size">{formatBytes(modelData?.model_summary.total_size_bytes || 0)}</span>
        </div>

        <div className="model-stats">
          <div className="stat">
            <span className="stat-value">{modelData?.model_summary.tables_count || 0}</span>
            <span className="stat-label">Tables</span>
          </div>
          <div className="stat">
            <span className="stat-value">{modelData?.model_summary.measures_count || 0}</span>
            <span className="stat-label">Measures</span>
          </div>
          <div className="stat">
            <span className="stat-value">{modelData?.model_summary.columns_count || 0}</span>
            <span className="stat-label">Columns</span>
          </div>
          <div className="stat">
            <span className="stat-value">{modelData?.model_summary.relationships_count || 0}</span>
            <span className="stat-label">Relationships</span>
          </div>
        </div>
      </div>

      {/* View Mode Tabs */}
      <div className="browser-toolbar">
        <div className="view-tabs">
          <button
            className={`tab ${viewMode === 'tables' ? 'active' : ''}`}
            onClick={() => setViewMode('tables')}
          >
            Tables
          </button>
          <button
            className={`tab ${viewMode === 'measures' ? 'active' : ''}`}
            onClick={() => setViewMode('measures')}
          >
            Measures
          </button>
          <button
            className={`tab ${viewMode === 'relationships' ? 'active' : ''}`}
            onClick={() => setViewMode('relationships')}
          >
            Relationships
          </button>
        </div>

        <div className="search-box">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
          <input
            type="text"
            placeholder="Search..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
      </div>

      {/* Content Area */}
      <div className="browser-content">
        {viewMode === 'tables' && (
          <div className="table-list">
            {filteredTables.map((table) => (
              <TableCard key={table.name} table={table} formatBytes={formatBytes} formatNumber={formatNumber} />
            ))}
            {filteredTables.length === 0 && (
              <p className="no-results">No tables found</p>
            )}
          </div>
        )}

        {viewMode === 'measures' && (
          <div className="measures-layout">
            <div className="measure-list">
              {filteredMeasures.map((measure) => (
                <MeasureCard
                  key={`${measure.table_name}.${measure.name}`}
                  measure={measure}
                  isSelected={selectedMeasure?.name === measure.name}
                  onClick={() => setSelectedMeasure(measure)}
                />
              ))}
              {filteredMeasures.length === 0 && (
                <p className="no-results">No measures found</p>
              )}
            </div>

            {selectedMeasure && (
              <div className="measure-detail">
                <div className="measure-detail-header">
                  <h3>{selectedMeasure.name}</h3>
                  <span className="measure-table">{selectedMeasure.table_name}</span>
                </div>
                <pre className="measure-code">{selectedMeasure.expression}</pre>
                {selectedMeasure.dependencies.length > 0 && (
                  <div className="measure-dependencies">
                    <h4>Dependencies</h4>
                    <ul>
                      {selectedMeasure.dependencies.map((dep, i) => (
                        <li key={i}>{dep}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {viewMode === 'relationships' && (
          <div className="relationships-section">
            <div className="relationship-diagram">
              <h3>Relationship Diagram (Text View)</h3>
              <div className="diagram-content">
                {filteredRelationships.map((rel, i) => (
                  <RelationshipRow key={i} relationship={rel} />
                ))}
                {filteredRelationships.length === 0 && (
                  <p className="no-results">No relationships found</p>
                )}
              </div>
            </div>
          </div>
        )}
      </div>

      <style>{browserStyles}</style>
    </div>
  )
}

// ============================================
// Sub-components
// ============================================

function TableCard({ table, formatBytes, formatNumber }: {
  table: Table
  formatBytes: (bytes: number) => string
  formatNumber: (num: number) => string
}) {
  return (
    <div className={`table-card ${table.is_hidden ? 'hidden-table' : ''}`}>
      <div className="table-header">
        <span className="table-name">{table.name}</span>
        {table.is_calculated && <span className="badge badge-calc">Calculated</span>}
        {table.is_hidden && <span className="badge badge-hidden">Hidden</span>}
      </div>
      <div className="table-stats">
        <span>{formatNumber(table.row_count)} rows</span>
        <span>{table.column_count} columns</span>
        <span>{formatBytes(table.size_bytes)}</span>
      </div>
    </div>
  )
}

function MeasureCard({ measure, isSelected, onClick }: {
  measure: Measure
  isSelected: boolean
  onClick: () => void
}) {
  return (
    <div
      className={`measure-card ${isSelected ? 'selected' : ''} ${measure.is_hidden ? 'hidden-measure' : ''}`}
      onClick={onClick}
    >
      <div className="measure-name">{measure.name}</div>
      <div className="measure-meta">
        <span className="measure-table-name">{measure.table_name}</span>
        {measure.is_hidden && <span className="badge badge-hidden">Hidden</span>}
      </div>
    </div>
  )
}

function RelationshipRow({ relationship }: { relationship: Relationship }) {
  const cardinalitySymbol = relationship.cardinality === '1:M' ? '1 -> *' :
    relationship.cardinality === 'M:1' ? '* -> 1' :
    relationship.cardinality === '1:1' ? '1 -> 1' : '* -> *'

  return (
    <div className={`relationship-row ${!relationship.is_active ? 'inactive' : ''}`}>
      <div className="rel-from">
        <span className="rel-table">{relationship.from_table}</span>
        <span className="rel-column">[{relationship.from_column}]</span>
      </div>
      <div className="rel-arrow">
        <span className="rel-cardinality">{cardinalitySymbol}</span>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="24" height="24">
          <line x1="5" y1="12" x2="19" y2="12" />
          <polyline points="12,5 19,12 12,19" />
        </svg>
      </div>
      <div className="rel-to">
        <span className="rel-table">{relationship.to_table}</span>
        <span className="rel-column">[{relationship.to_column}]</span>
      </div>
      {!relationship.is_active && <span className="badge badge-inactive">Inactive</span>}
    </div>
  )
}

// ============================================
// Styles
// ============================================

const uploadStyles = `
  .model-browser-page {
    padding: var(--qt-space-lg);
    max-width: 1200px;
    margin: 0 auto;
  }

  .upload-section {
    max-width: 700px;
    margin: 0 auto;
  }

  .upload-section h1 {
    text-align: center;
    font-size: var(--qt-text-2xl);
    font-weight: var(--qt-font-bold);
    margin-bottom: var(--qt-space-sm);
  }

  .upload-description {
    text-align: center;
    color: var(--qt-fg-muted);
    font-size: var(--qt-text-md);
    margin-bottom: var(--qt-space-xl);
  }

  .error-message {
    display: flex;
    align-items: center;
    gap: var(--qt-space-sm);
    padding: var(--qt-space-md);
    background: var(--qt-critical-bg);
    border: 1px solid var(--qt-critical-border);
    border-radius: var(--qt-radius);
    color: var(--qt-critical);
    margin-top: var(--qt-space-md);
  }

  .loading-section {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: var(--qt-space-2xl);
    text-align: center;
  }

  .loading-section h2 {
    margin-top: var(--qt-space-lg);
    margin-bottom: var(--qt-space-sm);
  }

  .loading-section p {
    color: var(--qt-fg-muted);
  }
`

const browserStyles = `
  .model-browser-page {
    padding: var(--qt-space-lg);
  }

  .model-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: var(--qt-space-lg);
    padding-bottom: var(--qt-space-lg);
    border-bottom: 1px solid var(--qt-border);
    flex-wrap: wrap;
    gap: var(--qt-space-md);
  }

  .model-title h1 {
    font-size: var(--qt-text-2xl);
    font-weight: var(--qt-font-bold);
    margin-bottom: var(--qt-space-xs);
  }

  .model-size {
    font-size: var(--qt-text-sm);
    color: var(--qt-fg-muted);
  }

  .model-stats {
    display: flex;
    gap: var(--qt-space-xl);
  }

  .stat {
    text-align: center;
  }

  .stat-value {
    display: block;
    font-size: var(--qt-text-xl);
    font-weight: var(--qt-font-bold);
    color: var(--qt-brand);
  }

  .stat-label {
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .browser-toolbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: var(--qt-space-lg);
    gap: var(--qt-space-md);
    flex-wrap: wrap;
  }

  .view-tabs {
    display: flex;
    gap: var(--qt-space-xs);
    background: var(--qt-bg-alt);
    padding: var(--qt-space-xs);
    border-radius: var(--qt-radius);
  }

  .tab {
    padding: var(--qt-space-sm) var(--qt-space-md);
    background: transparent;
    border: none;
    border-radius: var(--qt-radius-sm);
    font-size: var(--qt-text-sm);
    font-weight: var(--qt-font-medium);
    color: var(--qt-fg-muted);
    cursor: pointer;
    transition: all var(--qt-transition-fast);
  }

  .tab:hover {
    color: var(--qt-fg);
  }

  .tab.active {
    background: var(--qt-bg-card);
    color: var(--qt-fg);
    box-shadow: var(--qt-shadow-sm);
  }

  .search-box {
    display: flex;
    align-items: center;
    gap: var(--qt-space-sm);
    padding: var(--qt-space-sm) var(--qt-space-md);
    background: var(--qt-bg-card);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius);
    color: var(--qt-fg-muted);
  }

  .search-box input {
    border: none;
    background: transparent;
    font-size: var(--qt-text-sm);
    outline: none;
    width: 200px;
    color: var(--qt-fg);
  }

  .browser-content {
    min-height: 400px;
  }

  .table-list {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: var(--qt-space-md);
  }

  .table-card {
    background: var(--qt-bg-card);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius);
    padding: var(--qt-space-md);
    transition: box-shadow var(--qt-transition-fast);
  }

  .table-card:hover {
    box-shadow: var(--qt-shadow);
  }

  .table-card.hidden-table {
    opacity: 0.7;
  }

  .table-header {
    display: flex;
    align-items: center;
    gap: var(--qt-space-sm);
    margin-bottom: var(--qt-space-sm);
    flex-wrap: wrap;
  }

  .table-name {
    font-weight: var(--qt-font-semibold);
    font-size: var(--qt-text-md);
  }

  .table-stats {
    display: flex;
    gap: var(--qt-space-md);
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
  }

  .badge {
    padding: 0.125rem 0.5rem;
    border-radius: var(--qt-radius-full);
    font-size: var(--qt-text-xs);
    font-weight: var(--qt-font-medium);
  }

  .badge-calc {
    background: var(--qt-info-bg);
    color: var(--qt-info);
  }

  .badge-hidden {
    background: var(--qt-bg-alt);
    color: var(--qt-fg-muted);
  }

  .badge-inactive {
    background: var(--qt-medium-bg);
    color: var(--qt-medium);
  }

  .measures-layout {
    display: grid;
    grid-template-columns: 300px 1fr;
    gap: var(--qt-space-lg);
  }

  .measure-list {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-xs);
    max-height: 600px;
    overflow-y: auto;
  }

  .measure-card {
    padding: var(--qt-space-sm) var(--qt-space-md);
    background: var(--qt-bg-card);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius-sm);
    cursor: pointer;
    transition: all var(--qt-transition-fast);
  }

  .measure-card:hover {
    border-color: var(--qt-brand);
  }

  .measure-card.selected {
    border-color: var(--qt-brand);
    background: var(--qt-brand-light);
  }

  .measure-card.hidden-measure {
    opacity: 0.7;
  }

  .measure-name {
    font-weight: var(--qt-font-medium);
    font-size: var(--qt-text-sm);
    margin-bottom: 2px;
  }

  .measure-meta {
    display: flex;
    align-items: center;
    gap: var(--qt-space-sm);
  }

  .measure-table-name {
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
  }

  .measure-detail {
    background: var(--qt-bg-card);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius);
    padding: var(--qt-space-lg);
  }

  .measure-detail-header {
    margin-bottom: var(--qt-space-md);
  }

  .measure-detail-header h3 {
    font-size: var(--qt-text-lg);
    font-weight: var(--qt-font-semibold);
    margin-bottom: var(--qt-space-xs);
  }

  .measure-detail .measure-table {
    font-size: var(--qt-text-sm);
    color: var(--qt-fg-muted);
  }

  .measure-code {
    background: var(--qt-bg-code);
    color: var(--qt-fg-code);
    padding: var(--qt-space-md);
    border-radius: var(--qt-radius);
    font-family: var(--qt-font-mono);
    font-size: var(--qt-text-sm);
    overflow-x: auto;
    white-space: pre-wrap;
    margin-bottom: var(--qt-space-md);
  }

  .measure-dependencies h4 {
    font-size: var(--qt-text-sm);
    font-weight: var(--qt-font-semibold);
    margin-bottom: var(--qt-space-sm);
  }

  .measure-dependencies ul {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-wrap: wrap;
    gap: var(--qt-space-sm);
  }

  .measure-dependencies li {
    font-size: var(--qt-text-xs);
    padding: var(--qt-space-xs) var(--qt-space-sm);
    background: var(--qt-bg-alt);
    border-radius: var(--qt-radius-sm);
    color: var(--qt-fg-muted);
  }

  .relationships-section {
    background: var(--qt-bg-card);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius);
    padding: var(--qt-space-lg);
  }

  .relationship-diagram h3 {
    font-size: var(--qt-text-md);
    font-weight: var(--qt-font-semibold);
    margin-bottom: var(--qt-space-lg);
    color: var(--qt-fg-muted);
  }

  .diagram-content {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-md);
  }

  .relationship-row {
    display: flex;
    align-items: center;
    gap: var(--qt-space-md);
    padding: var(--qt-space-sm);
    background: var(--qt-bg-alt);
    border-radius: var(--qt-radius-sm);
  }

  .relationship-row.inactive {
    opacity: 0.6;
  }

  .rel-from, .rel-to {
    display: flex;
    flex-direction: column;
    min-width: 150px;
  }

  .rel-table {
    font-weight: var(--qt-font-medium);
    font-size: var(--qt-text-sm);
  }

  .rel-column {
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
    font-family: var(--qt-font-mono);
  }

  .rel-arrow {
    display: flex;
    flex-direction: column;
    align-items: center;
    color: var(--qt-fg-muted);
  }

  .rel-cardinality {
    font-size: var(--qt-text-xs);
    font-family: var(--qt-font-mono);
  }

  .no-results {
    text-align: center;
    color: var(--qt-fg-muted);
    padding: var(--qt-space-xl);
  }

  @media (max-width: 768px) {
    .measures-layout {
      grid-template-columns: 1fr;
    }

    .model-stats {
      width: 100%;
      justify-content: space-around;
    }

    .relationship-row {
      flex-wrap: wrap;
    }
  }
`
