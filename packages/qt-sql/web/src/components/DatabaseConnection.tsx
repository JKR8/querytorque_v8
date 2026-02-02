import { useState, useRef } from 'react'
import './DatabaseConnection.css'

interface DatabaseConnectionProps {
  onConnect: (config: DatabaseConfig) => void
  onCancel: () => void
  isConnecting?: boolean
}

export interface DatabaseConfig {
  type: 'duckdb' | 'postgres'
  // DuckDB options
  fixtureFile?: File
  // PostgreSQL options (future)
  connectionString?: string
  // Sampling options for validation
  samplePercent?: number  // 0 = no sampling, 10/25/50/100 = percentage
}

export interface ConnectionStatus {
  connected: boolean
  type?: 'duckdb' | 'postgres'
  details?: string
}

export default function DatabaseConnection({ onConnect, onCancel, isConnecting = false }: DatabaseConnectionProps) {
  const [dbType, setDbType] = useState<'duckdb' | 'postgres'>('duckdb')
  const [fixtureFile, setFixtureFile] = useState<File | null>(null)
  const [connectionString, setConnectionString] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [samplePercent, setSamplePercent] = useState<number>(0)  // 0 = no sampling
  const fileInputRef = useRef<HTMLInputElement>(null)

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      setFixtureFile(file)
      setError(null)
    }
  }

  const handleConnect = () => {
    setError(null)

    if (dbType === 'duckdb') {
      if (!fixtureFile) {
        setError('Please select a fixture file (CSV, JSON, or Parquet)')
        return
      }
      onConnect({ type: 'duckdb', fixtureFile, samplePercent: samplePercent || undefined })
    } else {
      if (!connectionString.trim()) {
        setError('Please enter a connection string')
        return
      }
      onConnect({ type: 'postgres', connectionString, samplePercent: samplePercent || undefined })
    }
  }

  return (
    <div className="database-connection">
      <div className="connection-header">
        <h2>Connect Database</h2>
        <p>Enable validated fixes by connecting to a database</p>
      </div>

      <div className="connection-tabs">
        <button
          className={`tab ${dbType === 'duckdb' ? 'active' : ''}`}
          onClick={() => setDbType('duckdb')}
        >
          DuckDB
        </button>
        <button
          className={`tab ${dbType === 'postgres' ? 'active' : ''}`}
          onClick={() => setDbType('postgres')}
        >
          PostgreSQL
        </button>
      </div>

      <div className="connection-content">
        {dbType === 'duckdb' && (
          <div className="duckdb-config">
            <div className="config-section">
              <label>Fixture File</label>
              <p className="hint">
                Upload a schema/data file. SQL files can define multiple tables with CREATE TABLE and INSERT.
              </p>
              <input
                type="file"
                ref={fileInputRef}
                accept=".csv,.json,.parquet,.sql"
                onChange={handleFileSelect}
                style={{ display: 'none' }}
              />
              <div className="file-picker">
                <button
                  className="pick-file-btn"
                  onClick={() => fileInputRef.current?.click()}
                >
                  Choose File
                </button>
                {fixtureFile && (
                  <span className="selected-file">{fixtureFile.name}</span>
                )}
              </div>
            </div>

            <div className="config-section">
              <label>Validation Sampling</label>
              <p className="hint">
                Limit rows for faster validation on large datasets
              </p>
              <select
                value={samplePercent}
                onChange={(e) => setSamplePercent(Number(e.target.value))}
                className="sample-select"
              >
                <option value={0}>Full validation (all rows)</option>
                <option value={10}>10% sample</option>
                <option value={25}>25% sample</option>
                <option value={50}>50% sample</option>
              </select>
            </div>

            <div className="info-box">
              <strong>How DuckDB validation works:</strong>
              <ul>
                <li>Your fixture file creates test tables</li>
                <li>Original SQL runs against the test data</li>
                <li>Optimized SQL runs against the same data</li>
                <li>Results are compared for equivalence</li>
              </ul>
            </div>
          </div>
        )}

        {dbType === 'postgres' && (
          <div className="postgres-config">
            <div className="config-section">
              <label>Connection String</label>
              <input
                type="text"
                placeholder="postgresql://user:pass@localhost:5432/db"
                value={connectionString}
                onChange={(e) => setConnectionString(e.target.value)}
                className="connection-input"
              />
              <p className="hint">
                Enter your PostgreSQL connection string. Data is only used for validation.
              </p>
            </div>

            <div className="config-section">
              <label>Validation Sampling</label>
              <p className="hint">
                Limit rows for faster validation on large tables
              </p>
              <select
                value={samplePercent}
                onChange={(e) => setSamplePercent(Number(e.target.value))}
                className="sample-select"
              >
                <option value={0}>Full validation (all rows)</option>
                <option value={10}>10% sample</option>
                <option value={25}>25% sample</option>
                <option value={50}>50% sample</option>
              </select>
            </div>

            <div className="warning-box">
              <strong>Security Note:</strong>
              <p>
                Connection strings are not stored. Queries are executed read-only
                with automatic rollback. Use a development/staging database for testing.
              </p>
            </div>
          </div>
        )}

        {error && (
          <div className="error-message">
            {error}
          </div>
        )}
      </div>

      <div className="connection-footer">
        <button className="btn" onClick={onCancel}>
          Cancel
        </button>
        <button
          className="btn btn-primary"
          onClick={handleConnect}
          disabled={isConnecting}
        >
          {isConnecting ? 'Connecting...' : 'Connect'}
        </button>
      </div>
    </div>
  )
}
