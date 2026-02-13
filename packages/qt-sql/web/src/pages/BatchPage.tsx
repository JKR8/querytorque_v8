/**
 * BatchPage — Full-page batch processing view
 */

import { useState, useRef } from 'react'
import BatchView from '@/components/BatchView'
import DatabaseConnection, { DatabaseConfig, ConnectionStatus } from '@/components/DatabaseConnection'
import useBatchProcessor from '@/hooks/useBatchProcessor'
import {
  connectDuckDB,
  connectPostgres,
  disconnectDatabase,
} from '@/api/client'
import './EditorPage.css'

export default function BatchPage() {
  const batch = useBatchProcessor()
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [isDragOver, setIsDragOver] = useState(false)

  // Database connection
  const [dbSessionId, setDbSessionId] = useState<string | null>(null)
  const [dbStatus, setDbStatus] = useState<ConnectionStatus>({ connected: false })
  const [dbType, setDbType] = useState<'duckdb' | 'postgres' | null>(null)
  const [showDbConnect, setShowDbConnect] = useState(false)
  const [isConnecting, setIsConnecting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleDbConnect = async (config: DatabaseConfig) => {
    setIsConnecting(true)
    setError(null)

    try {
      if (config.type === 'duckdb' && config.fixtureFile) {
        const response = await connectDuckDB(config.fixtureFile)
        if (response.connected) {
          setDbSessionId(response.session_id)
          setDbType('duckdb')
          setDbStatus({ connected: true, type: 'duckdb', details: response.details })
          batch.updateSettings({ dsn: 'duckdb:///:memory:', sessionId: response.session_id })
          setShowDbConnect(false)
        } else {
          setError(response.error || 'Connection failed')
        }
      } else if (config.type === 'postgres' && config.connectionString) {
        const response = await connectPostgres(config.connectionString)
        if (response.connected) {
          setDbSessionId(response.session_id)
          setDbType('postgres')
          setDbStatus({ connected: true, type: 'postgres', details: response.details })
          batch.updateSettings({ dsn: config.connectionString, sessionId: response.session_id })
          setShowDbConnect(false)
        } else {
          setError(response.error || 'Connection failed')
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Connection failed')
    } finally {
      setIsConnecting(false)
    }
  }

  const handleDbDisconnect = async () => {
    if (dbSessionId) {
      try { await disconnectDatabase(dbSessionId) } catch { /* ignore */ }
    }
    setDbSessionId(null)
    setDbType(null)
    setDbStatus({ connected: false })
    batch.updateSettings({ dsn: '', sessionId: undefined })
  }

  const handleFileSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []).filter(
      f => f.name.endsWith('.sql') || f.name.endsWith('.txt')
    )
    if (files.length > 0) {
      await batch.addFiles(files)
    }
    e.target.value = ''
  }

  const handleDrop = async (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
    const files = Array.from(e.dataTransfer.files).filter(
      f => f.name.endsWith('.sql') || f.name.endsWith('.txt')
    )
    if (files.length > 0) {
      await batch.addFiles(files)
    }
  }

  return (
    <div className="editor-page">
      {/* Connection Bar */}
      <div className="mode-controls">
        <div className="db-controls">
          {dbStatus.connected ? (
            <>
              <span className="db-status connected">
                <span className="db-dot" />
                {dbType === 'postgres' ? 'PostgreSQL' : 'DuckDB'}
              </span>
              {dbStatus.details && (
                <span className="db-details">{dbStatus.details}</span>
              )}
              <button
                className="action-btn db-disconnect-btn"
                onClick={handleDbDisconnect}
              >
                Disconnect
              </button>
            </>
          ) : (
            <button
              className="action-btn"
              onClick={() => setShowDbConnect(true)}
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <ellipse cx="12" cy="5" rx="9" ry="3" />
                <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
              </svg>
              Connect DB
            </button>
          )}
        </div>
        {error && <span className="status-error">{error}</span>}
      </div>

      {/* Database Connection Modal */}
      {showDbConnect && (
        <div className="modal-overlay" onClick={() => setShowDbConnect(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <DatabaseConnection
              onConnect={handleDbConnect}
              onCancel={() => setShowDbConnect(false)}
              isConnecting={isConnecting}
            />
          </div>
        </div>
      )}

      {/* Drop Zone / File Picker */}
      {batch.files.length === 0 && (
        <div
          className={`batch-drop-zone ${isDragOver ? 'drag-over' : ''}`}
          onDragOver={(e) => { e.preventDefault(); setIsDragOver(true) }}
          onDragLeave={() => setIsDragOver(false)}
          onDrop={handleDrop}
          onClick={() => fileInputRef.current?.click()}
        >
          <input
            ref={fileInputRef}
            type="file"
            accept=".sql,.txt"
            multiple
            onChange={handleFileSelect}
            style={{ display: 'none' }}
          />
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" width="48" height="48">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
            <polyline points="17,8 12,3 7,8" />
            <line x1="12" y1="3" x2="12" y2="15" />
          </svg>
          <p>Drop .sql files here or click to browse</p>
          <p className="batch-drop-hint">Multiple files supported for batch optimization</p>
        </div>
      )}

      {/* Batch View */}
      {batch.files.length > 0 && (
        <div className="batch-container">
          <div className="batch-add-bar">
            <button
              className="action-btn"
              onClick={() => fileInputRef.current?.click()}
              disabled={batch.isProcessing}
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <line x1="12" y1="5" x2="12" y2="19" />
                <line x1="5" y1="12" x2="19" y2="12" />
              </svg>
              Add Files
            </button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".sql,.txt"
              multiple
              onChange={handleFileSelect}
              style={{ display: 'none' }}
            />
          </div>
          <BatchView
            files={batch.files}
            isProcessing={batch.isProcessing}
            progress={batch.progress}
            settings={batch.settings}
            onStart={batch.start}
            onAbort={batch.abort}
            onReset={batch.reset}
            onRetry={batch.retryFile}
            onRemoveFile={batch.removeFile}
            onUpdateSettings={batch.updateSettings}
          />
        </div>
      )}
    </div>
  )
}
