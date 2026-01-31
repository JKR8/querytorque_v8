import { useState, useEffect, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { getReports, deleteReport, Report } from '../api/client'

export default function ReportsPage() {
  const navigate = useNavigate()
  const [reports, setReports] = useState<Report[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const loadReports = useCallback(async () => {
    setIsLoading(true)
    setError(null)

    try {
      const data = await getReports()
      setReports(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load reports')
    } finally {
      setIsLoading(false)
    }
  }, [])

  useEffect(() => {
    loadReports()
  }, [loadReports])

  const handleDelete = async (reportId: string) => {
    if (!confirm('Are you sure you want to delete this report?')) return

    try {
      await deleteReport(reportId)
      setReports(prev => prev.filter(r => r.id !== reportId))
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete report')
    }
  }

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  const getStatusBadge = (status: string) => {
    const statusClasses: Record<string, string> = {
      pass: 'badge-pass',
      warn: 'badge-warn',
      fail: 'badge-fail',
      deny: 'badge-deny',
    }
    return statusClasses[status] || 'badge-default'
  }

  if (isLoading) {
    return (
      <div className="reports-page">
        <div className="loading-section">
          <div className="spinner" />
          <p>Loading reports...</p>
        </div>
        <style>{styles}</style>
      </div>
    )
  }

  return (
    <div className="reports-page">
      <div className="page-header">
        <h1>Analysis Reports</h1>
        <button className="btn btn-primary" onClick={() => navigate('/analyze')}>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
            <line x1="12" y1="5" x2="12" y2="19" />
            <line x1="5" y1="12" x2="19" y2="12" />
          </svg>
          New Analysis
        </button>
      </div>

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

      {reports.length === 0 ? (
        <div className="empty-state">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="48" height="48">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
            <polyline points="14 2 14 8 20 8" />
            <line x1="16" y1="13" x2="8" y2="13" />
            <line x1="16" y1="17" x2="8" y2="17" />
            <polyline points="10 9 9 9 8 9" />
          </svg>
          <h2>No Reports Yet</h2>
          <p>Upload a VPAX file to generate your first analysis report.</p>
          <button className="btn btn-primary" onClick={() => navigate('/analyze')}>
            Start Analysis
          </button>
        </div>
      ) : (
        <div className="reports-list">
          <table>
            <thead>
              <tr>
                <th>Model Name</th>
                <th>Score</th>
                <th>Status</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {reports.map((report) => (
                <tr key={report.id}>
                  <td>
                    <div className="report-name">
                      <span className="name">{report.name}</span>
                      {report.model_name && (
                        <span className="model">{report.model_name}</span>
                      )}
                    </div>
                  </td>
                  <td>
                    <span className="score">{report.score}/100</span>
                  </td>
                  <td>
                    <span className={`badge ${getStatusBadge(report.status)}`}>
                      {report.status.toUpperCase()}
                    </span>
                  </td>
                  <td>
                    <span className="date">{formatDate(report.created_at)}</span>
                  </td>
                  <td>
                    <div className="actions">
                      <button
                        className="btn btn-ghost btn-sm"
                        onClick={() => navigate(`/reports/${report.id}`)}
                        title="View Report"
                      >
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
                          <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
                          <circle cx="12" cy="12" r="3" />
                        </svg>
                      </button>
                      <button
                        className="btn btn-ghost btn-sm"
                        onClick={() => handleDelete(report.id)}
                        title="Delete Report"
                      >
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
                          <polyline points="3 6 5 6 21 6" />
                          <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                        </svg>
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <style>{styles}</style>
    </div>
  )
}

const styles = `
  .reports-page {
    padding: var(--qt-space-lg);
    max-width: 1200px;
    margin: 0 auto;
  }

  .page-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: var(--qt-space-xl);
  }

  .page-header h1 {
    font-size: var(--qt-text-2xl);
    font-weight: var(--qt-font-bold);
  }

  .loading-section {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: var(--qt-space-2xl);
    text-align: center;
  }

  .loading-section p {
    margin-top: var(--qt-space-md);
    color: var(--qt-fg-muted);
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
    margin-bottom: var(--qt-space-lg);
  }

  .empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: var(--qt-space-2xl);
    text-align: center;
    color: var(--qt-fg-muted);
  }

  .empty-state svg {
    margin-bottom: var(--qt-space-lg);
    opacity: 0.5;
  }

  .empty-state h2 {
    font-size: var(--qt-text-xl);
    font-weight: var(--qt-font-semibold);
    color: var(--qt-fg);
    margin-bottom: var(--qt-space-sm);
  }

  .empty-state p {
    margin-bottom: var(--qt-space-lg);
  }

  .reports-list {
    background: var(--qt-bg-card);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius);
    overflow: hidden;
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }

  th, td {
    padding: var(--qt-space-md);
    text-align: left;
  }

  th {
    background: var(--qt-bg-alt);
    font-size: var(--qt-text-xs);
    font-weight: var(--qt-font-semibold);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--qt-fg-muted);
    border-bottom: 1px solid var(--qt-border);
  }

  tr:not(:last-child) td {
    border-bottom: 1px solid var(--qt-border-light);
  }

  tr:hover {
    background: var(--qt-bg-alt);
  }

  .report-name .name {
    font-weight: var(--qt-font-medium);
    display: block;
  }

  .report-name .model {
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
  }

  .score {
    font-family: var(--qt-font-mono);
    font-weight: var(--qt-font-semibold);
  }

  .badge {
    padding: 0.125rem 0.5rem;
    border-radius: var(--qt-radius-full);
    font-size: var(--qt-text-xs);
    font-weight: var(--qt-font-medium);
  }

  .badge-pass {
    background: var(--qt-low-bg);
    color: var(--qt-low);
  }

  .badge-warn {
    background: var(--qt-medium-bg);
    color: var(--qt-medium);
  }

  .badge-fail {
    background: var(--qt-high-bg);
    color: var(--qt-high);
  }

  .badge-deny {
    background: var(--qt-critical-bg);
    color: var(--qt-critical);
  }

  .badge-default {
    background: var(--qt-bg-alt);
    color: var(--qt-fg-muted);
  }

  .date {
    font-size: var(--qt-text-sm);
    color: var(--qt-fg-muted);
  }

  .actions {
    display: flex;
    gap: var(--qt-space-xs);
  }

  .btn-sm {
    padding: var(--qt-space-xs) var(--qt-space-sm);
  }

  .btn-ghost {
    background: transparent;
    border: none;
    color: var(--qt-fg-muted);
    cursor: pointer;
    border-radius: var(--qt-radius-sm);
    transition: all var(--qt-transition-fast);
  }

  .btn-ghost:hover {
    background: var(--qt-bg-alt);
    color: var(--qt-fg);
  }
`
