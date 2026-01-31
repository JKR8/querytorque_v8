import './ReportViewer.css'

interface ReportViewerProps {
  html: string
  fileName: string
  score: number
  status: string
}

export default function ReportViewer({ html, fileName, score, status }: ReportViewerProps) {
  const downloadReport = () => {
    const blob = new Blob([html], { type: 'text/html' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${fileName.replace(/\.\w+$/, '')}_audit.html`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  const getStatusClass = () => {
    switch (status) {
      case 'pass': return 'status-pass'
      case 'warn': return 'status-warn'
      case 'fail': return 'status-fail'
      case 'deny': return 'status-deny'
      default: return ''
    }
  }

  return (
    <div className="report-viewer">
      <div className="report-header">
        <div className="report-info">
          <span className="report-filename">{fileName}</span>
          <span className={`report-score ${getStatusClass()}`}>
            Score: {score}/100
          </span>
        </div>
        <button className="btn btn-primary" onClick={downloadReport}>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
            <polyline points="7,10 12,15 17,10" />
            <line x1="12" y1="15" x2="12" y2="3" />
          </svg>
          Download
        </button>
      </div>
      <iframe
        className="report-frame"
        srcDoc={html}
        sandbox="allow-scripts allow-same-origin"
        title="Analysis Report"
      />
    </div>
  )
}
