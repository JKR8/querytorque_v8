/**
 * ZIP export utility for batch processing results
 */

import JSZip from 'jszip'
import { saveAs } from 'file-saver'
import type { BatchFile } from '@/hooks/useBatchProcessor'

/**
 * Generate HTML summary report for batch results
 */
function generateBatchSummary(files: BatchFile[]): string {
  const fixed = files.filter(f => f.status === 'fixed')
  const skipped = files.filter(f => f.status === 'skipped')
  const failed = files.filter(f => f.status === 'failed')

  const timestamp = new Date().toISOString()

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>QueryTorque Batch Report</title>
  <style>
    :root {
      --bg: #0a0a0f;
      --bg-card: #12121a;
      --fg: #e4e4e7;
      --fg-muted: #71717a;
      --border: #27272a;
      --low: #22c55e;
      --critical: #ef4444;
      --info: #3b82f6;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: var(--bg);
      color: var(--fg);
      line-height: 1.6;
      padding: 2rem;
    }
    .container { max-width: 900px; margin: 0 auto; }
    .header {
      display: flex;
      align-items: center;
      gap: 1rem;
      margin-bottom: 2rem;
    }
    .header h1 { font-size: 1.5rem; }
    .header .brand { color: var(--info); }
    .summary {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 1rem;
      margin-bottom: 2rem;
    }
    .summary-card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 1rem;
      text-align: center;
    }
    .summary-card .value {
      font-size: 2rem;
      font-weight: 700;
    }
    .summary-card .label {
      font-size: 0.875rem;
      color: var(--fg-muted);
    }
    .summary-card.fixed .value { color: var(--low); }
    .summary-card.failed .value { color: var(--critical); }
    .section {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 8px;
      margin-bottom: 1.5rem;
      overflow: hidden;
    }
    .section-header {
      padding: 0.75rem 1rem;
      background: rgba(255,255,255,0.02);
      border-bottom: 1px solid var(--border);
      font-weight: 600;
    }
    .file-list { padding: 0.5rem; }
    .file-item {
      display: flex;
      align-items: center;
      gap: 1rem;
      padding: 0.5rem 0.75rem;
      border-radius: 6px;
    }
    .file-item:hover { background: rgba(255,255,255,0.02); }
    .file-name { flex: 1; font-family: monospace; }
    .file-score { color: var(--fg-muted); }
    .badge {
      padding: 0.125rem 0.5rem;
      border-radius: 4px;
      font-size: 0.75rem;
      font-weight: 500;
      text-transform: uppercase;
    }
    .badge.fixed { background: rgba(34,197,94,0.1); color: var(--low); }
    .badge.skipped { background: rgba(113,113,122,0.1); color: var(--fg-muted); }
    .badge.failed { background: rgba(239,68,68,0.1); color: var(--critical); }
    .footer {
      text-align: center;
      color: var(--fg-muted);
      font-size: 0.875rem;
      padding-top: 2rem;
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <span class="brand">QueryTorque</span>
      <h1>Batch Optimization Report</h1>
    </div>

    <div class="summary">
      <div class="summary-card">
        <div class="value">${files.length}</div>
        <div class="label">Total Files</div>
      </div>
      <div class="summary-card fixed">
        <div class="value">${fixed.length}</div>
        <div class="label">Optimized</div>
      </div>
      <div class="summary-card">
        <div class="value">${skipped.length}</div>
        <div class="label">Skipped</div>
      </div>
      <div class="summary-card failed">
        <div class="value">${failed.length}</div>
        <div class="label">Failed</div>
      </div>
    </div>

    ${fixed.length > 0 ? `
    <div class="section">
      <div class="section-header">Optimized Files (${fixed.length})</div>
      <div class="file-list">
        ${fixed.map(f => `
        <div class="file-item">
          <span class="file-name">${f.name}</span>
          <span class="file-score">Score: ${f.score || '--'}</span>
          <span class="badge fixed">fixed</span>
        </div>
        `).join('')}
      </div>
    </div>
    ` : ''}

    ${skipped.length > 0 ? `
    <div class="section">
      <div class="section-header">Skipped Files (${skipped.length})</div>
      <div class="file-list">
        ${skipped.map(f => `
        <div class="file-item">
          <span class="file-name">${f.name}</span>
          <span class="file-score">Score: ${f.score || '--'}</span>
          <span class="badge skipped">skipped</span>
        </div>
        `).join('')}
      </div>
    </div>
    ` : ''}

    ${failed.length > 0 ? `
    <div class="section">
      <div class="section-header">Failed Files (${failed.length})</div>
      <div class="file-list">
        ${failed.map(f => `
        <div class="file-item">
          <span class="file-name">${f.name}</span>
          <span class="file-score">${f.error || 'Unknown error'}</span>
          <span class="badge failed">failed</span>
        </div>
        `).join('')}
      </div>
    </div>
    ` : ''}

    <div class="footer">
      Generated by QueryTorque at ${timestamp}
    </div>
  </div>
</body>
</html>`
}

/**
 * Export batch results as ZIP file
 *
 * Structure:
 * - /optimized/   - Fixed queries with optimized SQL
 * - /original/    - Original queries
 * - /reports/batch_summary.html - Summary report
 */
export async function exportBatchResults(files: BatchFile[]): Promise<void> {
  const zip = new JSZip()

  // Create folders
  const optimizedFolder = zip.folder('optimized')
  const originalFolder = zip.folder('original')
  const reportsFolder = zip.folder('reports')

  if (!optimizedFolder || !originalFolder || !reportsFolder) {
    throw new Error('Failed to create ZIP folders')
  }

  // Add files
  for (const file of files) {
    // Always add original
    originalFolder.file(file.name, file.content)

    // Add optimized version if available
    if (file.status === 'fixed' && file.optimizedContent) {
      // Add marker comment
      const markedContent = `-- qt:optimized\n-- Original file: ${file.name}\n-- Score: ${file.score || 'N/A'}\n\n${file.optimizedContent}`
      optimizedFolder.file(file.name, markedContent)
    }
  }

  // Add summary report
  const summaryHtml = generateBatchSummary(files)
  reportsFolder.file('batch_summary.html', summaryHtml)

  // Generate and download
  const blob = await zip.generateAsync({ type: 'blob' })
  const timestamp = new Date().toISOString().slice(0, 10)
  saveAs(blob, `querytorque-batch-${timestamp}.zip`)
}
