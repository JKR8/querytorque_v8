import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { listReports, deleteReport, Report } from '@/api/client'
import './ReportsPage.css'

export default function ReportsPage() {
  const navigate = useNavigate()
  const [reports, setReports] = useState<Report[]>([])
  const [total, setTotal] = useState(0)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    loadReports()
  }, [])

  const loadReports = async () => {
    setIsLoading(true)
    setError(null)

    try {
      const data = await listReports(50, 0)
      setReports(data.reports)
      setTotal(data.total)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load reports')
    } finally {
      setIsLoading(false)
    }
  }

  const handleDelete = async (reportId: string) => {
    if (!confirm('Are you sure you want to delete this report?')) return

    try {
      await deleteReport(reportId)
      setReports(reports.filter(r => r.id !== reportId))
      setTotal(total - 1)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete report')
    }
  }

  const getStatusClass = (status: string) => {
    switch (status) {
      case 'pass': return 'status-pass'
      case 'warn': return 'status-warn'
      case 'fail': return 'status-fail'
      case 'deny': return 'status-deny'
      default: return ''
    }
  }

  const formatDate = (dateString: string) => {
    const date = new Date(dateString)
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  return (
    <div className="reports-page">
      <div className="reports-header">
        <div className="reports-title">
          <h1>Reports</h1>
          <span className="reports-count">{total} total</span>
        </div>
        <button className="btn btn-primary" onClick={() => navigate('/editor')}>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
            <line x1="12" y1="5" x2="12" y2="19" />
            <line x1="5" y1="12" x2="19" y2="12" />
          </svg>
          New Analysis
        </button>
      </div>

      {error && (
        <div className="error-banner">
          {error}
          <button onClick={() => setError(null)}>Dismiss</button>
        </div>
      )}

      {isLoading ? (
        <div className="loading-state">
          <div className="spinner-large" />
          <p>Loading reports...</p>
        </div>
      ) : reports.length === 0 ? (
        <div className="empty-state">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" width="48" height="48">
            <path d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
          <h2>No reports yet</h2>
          <p>Analyze your first SQL query to create a report.</p>
          <button className="btn btn-primary" onClick={() => navigate('/editor')}>
            Start Analysis
          </button>
        </div>
      ) : (
        <div className="reports-list">
          <table className="reports-table">
            <thead>
              <tr>
                <th>File</th>
                <th>Score</th>
                <th>Issues</th>
                <th>Status</th>
                <th>Date</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {reports.map(report => (
                <tr key={report.id}>
                  <td className="file-name">{report.file_name}</td>
                  <td>
                    <span className={`score-badge ${getStatusClass(report.status)}`}>
                      {report.score}/100
                    </span>
                  </td>
                  <td>{report.issues_count}</td>
                  <td>
                    <span className={`status-badge ${getStatusClass(report.status)}`}>
                      {report.status}
                    </span>
                  </td>
                  <td className="date">{formatDate(report.created_at)}</td>
                  <td>
                    <div className="actions">
                      <button
                        className="action-btn"
                        onClick={() => navigate(`/editor?report=${report.id}`)}
                        title="View"
                      >
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
                          <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
                          <circle cx="12" cy="12" r="3" />
                        </svg>
                      </button>
                      <button
                        className="action-btn danger"
                        onClick={() => handleDelete(report.id)}
                        title="Delete"
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
    </div>
  )
}
