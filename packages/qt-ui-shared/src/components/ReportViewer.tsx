/**
 * ReportViewer Component
 * Displays HTML reports (SQL/DAX audit reports) in an iframe with download capability
 */

import { useCallback, useEffect, useRef } from 'react'
import '../theme/tokens.css'

export interface ReportViewerProps {
  /** HTML content to display in the iframe */
  html: string
  /** File name for download (without extension) */
  fileName?: string
  /** Optional score to display in header */
  score?: number
  /** Quality status: pass | warn | fail | deny */
  status?: 'pass' | 'warn' | 'fail' | 'deny'
  /** Title shown in the header */
  title?: string
  /** Height of the viewer (CSS value) */
  height?: string
  /** Show download button */
  showDownload?: boolean
  /** Callback when iframe receives a postMessage */
  onMessage?: (data: unknown) => void
  /** Additional CSS class name */
  className?: string
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
    border: '1px solid var(--qt-border)',
    borderRadius: 'var(--qt-radius)',
    overflow: 'hidden',
    background: 'var(--qt-bg-card)',
    boxShadow: 'var(--qt-shadow)',
  },
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '0.75rem 1rem',
    borderBottom: '1px solid var(--qt-border)',
    background: 'var(--qt-bg-alt)',
  },
  headerInfo: {
    display: 'flex',
    alignItems: 'center',
    gap: '1rem',
  },
  fileName: {
    fontFamily: 'var(--qt-font-mono)',
    fontSize: 'var(--qt-text-sm)',
    fontWeight: 'var(--qt-font-medium)',
    color: 'var(--qt-fg)',
  },
  score: {
    fontSize: 'var(--qt-text-xs)',
    fontWeight: 'var(--qt-font-semibold)',
    padding: '0.25rem 0.625rem',
    borderRadius: 'var(--qt-radius-full)',
  },
  downloadBtn: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: '0.5rem',
    padding: '0.5rem 1rem',
    fontSize: 'var(--qt-text-base)',
    fontWeight: 'var(--qt-font-medium)',
    borderRadius: 'var(--qt-radius-sm)',
    border: 'none',
    background: 'var(--qt-brand)',
    color: 'white',
    cursor: 'pointer',
    transition: 'background var(--qt-transition-fast)',
  },
  iframe: {
    flex: 1,
    width: '100%',
    border: 'none',
    background: 'white',
  },
}

const statusStyles: Record<string, React.CSSProperties> = {
  pass: { background: 'var(--qt-low-bg)', color: 'var(--qt-low)' },
  warn: { background: 'var(--qt-high-bg)', color: 'var(--qt-high)' },
  fail: { background: 'var(--qt-critical-bg)', color: 'var(--qt-critical)' },
  deny: { background: 'var(--qt-critical-bg)', color: 'var(--qt-critical)' },
}

export function ReportViewer({
  html,
  fileName = 'report',
  score,
  status,
  title,
  height = '600px',
  showDownload = true,
  onMessage,
  className,
}: ReportViewerProps) {
  const iframeRef = useRef<HTMLIFrameElement>(null)

  // Handle postMessage from iframe
  useEffect(() => {
    if (!onMessage) return

    const handleMessage = (event: MessageEvent) => {
      // Validate origin - accept same origin or iframe content
      if (event.source === iframeRef.current?.contentWindow) {
        onMessage(event.data)
      }
    }

    window.addEventListener('message', handleMessage)
    return () => window.removeEventListener('message', handleMessage)
  }, [onMessage])

  const handleDownload = useCallback(() => {
    const blob = new Blob([html], { type: 'text/html' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${fileName.replace(/\.\w+$/, '')}_audit.html`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }, [html, fileName])

  const displayTitle = title || fileName

  return (
    <div style={{ ...styles.container, height }} className={className}>
      <div style={styles.header}>
        <div style={styles.headerInfo}>
          <span style={styles.fileName}>{displayTitle}</span>
          {score !== undefined && status && (
            <span style={{ ...styles.score, ...statusStyles[status] }}>
              Score: {score}/100
            </span>
          )}
        </div>
        {showDownload && (
          <button
            style={styles.downloadBtn}
            onClick={handleDownload}
            onMouseOver={(e) => {
              e.currentTarget.style.background = '#5b21b6'
            }}
            onMouseOut={(e) => {
              e.currentTarget.style.background = 'var(--qt-brand)'
            }}
          >
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              width="16"
              height="16"
            >
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
              <polyline points="7,10 12,15 17,10" />
              <line x1="12" y1="15" x2="12" y2="3" />
            </svg>
            Download Report
          </button>
        )}
      </div>
      <iframe
        ref={iframeRef}
        style={styles.iframe}
        srcDoc={html}
        sandbox="allow-scripts allow-same-origin"
        title={displayTitle}
      />
    </div>
  )
}

export default ReportViewer
