import { useEffect, useRef, useCallback } from 'react'

interface ReportViewerProps {
  html: string
  fileName: string
  score: number
  status: string
  onOptimize?: (measureName: string, measureCode: string) => void
}

export default function ReportViewer({ html, fileName, score, status, onOptimize }: ReportViewerProps) {
  const iframeRef = useRef<HTMLIFrameElement>(null)

  // Handle messages from iframe (for optimize button clicks)
  const handleMessage = useCallback((event: MessageEvent) => {
    if (event.data?.type === 'optimize-measure' && onOptimize) {
      onOptimize(event.data.measureName, event.data.measureCode)
    }
  }, [onOptimize])

  useEffect(() => {
    window.addEventListener('message', handleMessage)
    return () => window.removeEventListener('message', handleMessage)
  }, [handleMessage])

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

  const getStatusLabel = () => {
    switch (status) {
      case 'pass': return 'Peak Torque'
      case 'warn': return 'Power Band'
      case 'fail': return 'Stall Zone'
      case 'deny': return 'Redline'
      default: return status
    }
  }

  return (
    <>
      <div className="report-viewer">
        <div className="report-header">
          <div className="report-info">
            <span className="report-filename">{fileName}</span>
            <span className={`report-score ${getStatusClass()}`}>
              Tq {score}/100 - {getStatusLabel()}
            </span>
          </div>
          <button className="btn btn-primary" onClick={downloadReport}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
              <polyline points="7,10 12,15 17,10" />
              <line x1="12" y1="15" x2="12" y2="3" />
            </svg>
            Download Report
          </button>
        </div>
        <iframe
          ref={iframeRef}
          className="report-frame"
          srcDoc={html}
          sandbox="allow-scripts allow-same-origin"
          title="Analysis Report"
        />
      </div>

      <style>{`
        .report-viewer {
          display: flex;
          flex-direction: column;
          height: 100%;
          background: var(--qt-bg-card);
          border: 1px solid var(--qt-border);
          border-radius: var(--qt-radius);
          overflow: hidden;
        }

        .report-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: var(--qt-space-md);
          background: var(--qt-bg-alt);
          border-bottom: 1px solid var(--qt-border);
        }

        .report-info {
          display: flex;
          align-items: center;
          gap: var(--qt-space-md);
        }

        .report-filename {
          font-weight: var(--qt-font-semibold);
          font-size: var(--qt-text-md);
        }

        .report-score {
          font-family: var(--qt-font-mono);
          font-size: var(--qt-text-sm);
          font-weight: var(--qt-font-semibold);
          padding: var(--qt-space-xs) var(--qt-space-sm);
          border-radius: var(--qt-radius-sm);
        }

        .report-score.status-pass {
          background: var(--qt-low-bg);
          color: var(--qt-low);
        }

        .report-score.status-warn {
          background: var(--qt-medium-bg);
          color: var(--qt-medium);
        }

        .report-score.status-fail {
          background: var(--qt-high-bg);
          color: var(--qt-high);
        }

        .report-score.status-deny {
          background: var(--qt-critical-bg);
          color: var(--qt-critical);
        }

        .report-frame {
          flex: 1;
          width: 100%;
          border: none;
          background: white;
        }

        .btn {
          display: inline-flex;
          align-items: center;
          gap: var(--qt-space-sm);
          padding: var(--qt-space-sm) var(--qt-space-md);
          font-size: var(--qt-text-sm);
          font-weight: var(--qt-font-medium);
          border: none;
          border-radius: var(--qt-radius-sm);
          cursor: pointer;
          transition: all var(--qt-transition-fast);
        }

        .btn-primary {
          background: var(--qt-brand);
          color: white;
        }

        .btn-primary:hover {
          filter: brightness(1.1);
        }
      `}</style>
    </>
  )
}
