import { useState, useCallback, DragEvent, ChangeEvent } from 'react'

interface DropZoneProps {
  accept: string
  multiple?: boolean
  onFilesSelected: (files: File[]) => void
  disabled?: boolean
}

export default function DropZone({ accept, multiple = false, onFilesSelected, disabled }: DropZoneProps) {
  const [isDragging, setIsDragging] = useState(false)

  const handleDrag = useCallback((e: DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
  }, [])

  const handleDragIn = useCallback((e: DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    if (e.dataTransfer.items?.length > 0) {
      setIsDragging(true)
    }
  }, [])

  const handleDragOut = useCallback((e: DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragging(false)
  }, [])

  const handleDrop = useCallback((e: DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragging(false)

    if (disabled) return

    const files = Array.from(e.dataTransfer.files)
    const validFiles = files.filter(f => {
      const ext = '.' + f.name.split('.').pop()?.toLowerCase()
      return accept.includes(ext)
    })

    if (validFiles.length > 0) {
      onFilesSelected(multiple ? validFiles : [validFiles[0]])
    }
  }, [accept, multiple, onFilesSelected, disabled])

  const handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    if (disabled) return
    const files = Array.from(e.target.files || [])
    if (files.length > 0) {
      onFilesSelected(multiple ? files : [files[0]])
    }
  }

  const extensions = accept.split(',').map(a => a.trim()).join(' or ')

  return (
    <>
      <div
        className={`dropzone ${isDragging ? 'dragging' : ''} ${disabled ? 'disabled' : ''}`}
        onDragEnter={handleDragIn}
        onDragLeave={handleDragOut}
        onDragOver={handleDrag}
        onDrop={handleDrop}
      >
        <input
          type="file"
          accept={accept}
          multiple={multiple}
          onChange={handleChange}
          disabled={disabled}
          id="file-input"
        />
        <label htmlFor="file-input" className="dropzone-content">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="upload-icon">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
            <polyline points="17,8 12,3 7,8" />
            <line x1="12" y1="3" x2="12" y2="15" />
          </svg>
          <p className="dropzone-text">
            <strong>Drop {multiple ? 'files' : 'file'} here</strong> or click to browse
          </p>
          <p className="dropzone-hint">Accepts {extensions}</p>
        </label>
      </div>

      <style>{`
        .dropzone {
          border: 2px dashed var(--qt-border);
          border-radius: var(--qt-radius-lg);
          padding: var(--qt-space-2xl);
          text-align: center;
          cursor: pointer;
          transition: all var(--qt-transition-fast);
          background: var(--qt-bg-card);
        }

        .dropzone:hover {
          border-color: var(--qt-brand);
          background: var(--qt-brand-light);
        }

        .dropzone.dragging {
          border-color: var(--qt-brand);
          background: var(--qt-brand-light);
        }

        .dropzone.disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        .dropzone.disabled:hover {
          border-color: var(--qt-border);
          background: var(--qt-bg-card);
        }

        .dropzone input[type="file"] {
          display: none;
        }

        .dropzone-content {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: var(--qt-space-md);
          cursor: pointer;
        }

        .upload-icon {
          width: 48px;
          height: 48px;
          color: var(--qt-fg-muted);
          transition: color var(--qt-transition-fast);
        }

        .dropzone:hover .upload-icon,
        .dropzone.dragging .upload-icon {
          color: var(--qt-brand);
        }

        .dropzone-text {
          font-size: var(--qt-text-md);
          color: var(--qt-fg);
          margin: 0;
        }

        .dropzone-text strong {
          color: var(--qt-brand);
        }

        .dropzone-hint {
          font-size: var(--qt-text-sm);
          color: var(--qt-fg-muted);
          margin: 0;
        }
      `}</style>
    </>
  )
}
