import { useState, useCallback } from 'react'
import DropZone from '../components/DropZone'
import ReportViewer from '../components/ReportViewer'
import OptimizationPanel from '../components/OptimizationPanel'
import { analyzeVpaxStatic, AnalysisResult } from '../api/client'

type ViewState = 'upload' | 'analyzing' | 'report' | 'optimizing'

export default function AnalyzePage() {
  const [viewState, setViewState] = useState<ViewState>('upload')
  const [result, setResult] = useState<AnalysisResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [uploadedFile, setUploadedFile] = useState<File | null>(null)
  const [selectedMeasure, setSelectedMeasure] = useState<{ name: string; code: string } | null>(null)

  const handleFilesSelected = useCallback(async (files: File[]) => {
    if (files.length === 0) return

    const file = files[0]
    setUploadedFile(file)
    setViewState('analyzing')
    setError(null)

    try {
      const analysisResult = await analyzeVpaxStatic(file)
      setResult(analysisResult)
      setViewState('report')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Analysis failed')
      setViewState('upload')
    }
  }, [])

  const handleOptimize = useCallback((measureName: string, measureCode: string) => {
    setSelectedMeasure({ name: measureName, code: measureCode })
    setViewState('optimizing')
  }, [])

  const handleOptimizationComplete = useCallback((optimizedCode: string) => {
    console.log('Optimization complete:', optimizedCode)
    // In a real implementation, we would update the report with the optimized code
    setViewState('report')
    setSelectedMeasure(null)
  }, [])

  const handleOptimizationCancel = useCallback(() => {
    setViewState('report')
    setSelectedMeasure(null)
  }, [])

  const handleReset = useCallback(() => {
    setViewState('upload')
    setResult(null)
    setError(null)
    setUploadedFile(null)
    setSelectedMeasure(null)
  }, [])

  return (
    <div className="analyze-page">
      {viewState === 'upload' && (
        <div className="upload-section">
          <h1>Analyze Power BI Model</h1>
          <p className="upload-description">
            Upload a VPAX file to analyze your Power BI model for performance issues,
            DAX anti-patterns, and optimization opportunities.
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

          <div className="upload-help">
            <h3>How to export a VPAX file:</h3>
            <ol>
              <li>Open <strong>DAX Studio</strong> and connect to your Power BI model</li>
              <li>Go to <strong>Advanced &gt; Export Model Metrics</strong></li>
              <li>Save the .vpax file and upload it here</li>
            </ol>
            <p className="alt-method">
              Alternatively, use <strong>Tabular Editor</strong>: File &gt; Save to Folder &gt; Export as VPAX
            </p>
          </div>
        </div>
      )}

      {viewState === 'analyzing' && (
        <div className="analyzing-section">
          <div className="spinner" />
          <h2>Analyzing Model</h2>
          <p>Processing {uploadedFile?.name}...</p>
        </div>
      )}

      {viewState === 'report' && result && (
        <div className="report-section">
          <div className="report-actions">
            <button className="btn btn-secondary" onClick={handleReset}>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
                <polyline points="1,4 1,10 7,10" />
                <path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10" />
              </svg>
              Analyze Another
            </button>
          </div>

          <ReportViewer
            html={result.html}
            fileName={result.file_name}
            score={result.score}
            status={result.status}
            onOptimize={handleOptimize}
          />
        </div>
      )}

      {viewState === 'optimizing' && selectedMeasure && (
        <div className="optimization-section">
          <OptimizationPanel
            measureName={selectedMeasure.name}
            measureCode={selectedMeasure.code}
            onComplete={handleOptimizationComplete}
            onCancel={handleOptimizationCancel}
          />
        </div>
      )}

      <style>{`
        .analyze-page {
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

        .upload-help {
          margin-top: var(--qt-space-xl);
          padding: var(--qt-space-lg);
          background: var(--qt-bg-alt);
          border: 1px solid var(--qt-border);
          border-radius: var(--qt-radius);
        }

        .upload-help h3 {
          font-size: var(--qt-text-md);
          font-weight: var(--qt-font-semibold);
          margin-bottom: var(--qt-space-md);
        }

        .upload-help ol {
          padding-left: 1.5rem;
          margin-bottom: var(--qt-space-md);
        }

        .upload-help li {
          font-size: var(--qt-text-sm);
          color: var(--qt-fg-muted);
          margin-bottom: var(--qt-space-sm);
          line-height: var(--qt-leading-relaxed);
        }

        .alt-method {
          font-size: var(--qt-text-sm);
          color: var(--qt-fg-muted);
          padding-top: var(--qt-space-md);
          border-top: 1px solid var(--qt-border);
        }

        .analyzing-section {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          padding: var(--qt-space-2xl);
          text-align: center;
        }

        .analyzing-section h2 {
          font-size: var(--qt-text-xl);
          font-weight: var(--qt-font-semibold);
          margin-top: var(--qt-space-lg);
          margin-bottom: var(--qt-space-sm);
        }

        .analyzing-section p {
          color: var(--qt-fg-muted);
        }

        .report-section {
          display: flex;
          flex-direction: column;
          height: calc(100vh - 200px);
        }

        .report-actions {
          display: flex;
          justify-content: flex-end;
          margin-bottom: var(--qt-space-md);
        }

        .optimization-section {
          max-width: 900px;
          margin: 0 auto;
        }
      `}</style>
    </div>
  )
}
