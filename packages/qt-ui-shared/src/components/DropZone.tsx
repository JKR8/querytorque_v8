/**
 * DropZone Component
 * File upload drop zone with drag-and-drop support
 */

import { useState, useCallback, useRef, DragEvent, ChangeEvent } from 'react'
import '../theme/tokens.css'

export interface DropZoneProps {
  /** Accepted file extensions (e.g., ".sql,.txt") */
  accept: string
  /** Allow multiple file selection */
  multiple?: boolean
  /** Callback when files are selected */
  onFilesSelected: (files: File[]) => void
  /** Whether the drop zone is disabled */
  disabled?: boolean
  /** Custom label text */
  label?: string
  /** Custom hint text */
  hint?: string
  /** Maximum file size in bytes */
  maxSize?: number
  /** Additional CSS class name */
  className?: string
}

const styles: Record<string, React.CSSProperties> = {
  dropzone: {
    position: 'relative',
    padding: '2rem',
    border: '2px dashed var(--qt-border)',
    borderRadius: 'var(--qt-radius)',
    background: 'var(--qt-bg-alt)',
    transition: 'all var(--qt-transition-fast)',
    cursor: 'pointer',
    textAlign: 'center',
  },
  dropzoneDragging: {
    borderColor: 'var(--qt-brand)',
    background: 'var(--qt-brand-light)',
    borderStyle: 'solid',
  },
  dropzoneDisabled: {
    opacity: 0.5,
    cursor: 'not-allowed',
  },
  input: {
    position: 'absolute',
    top: 0,
    left: 0,
    width: '100%',
    height: '100%',
    opacity: 0,
    cursor: 'pointer',
  },
  content: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    gap: '0.75rem',
    pointerEvents: 'none',
  },
  icon: {
    width: '48px',
    height: '48px',
    color: 'var(--qt-fg-muted)',
  },
  iconDragging: {
    color: 'var(--qt-brand)',
  },
  text: {
    fontSize: 'var(--qt-text-md)',
    color: 'var(--qt-fg)',
  },
  strong: {
    fontWeight: 'var(--qt-font-semibold)',
    color: 'var(--qt-brand)',
  },
  hint: {
    fontSize: 'var(--qt-text-sm)',
    color: 'var(--qt-fg-muted)',
  },
  error: {
    marginTop: '0.5rem',
    fontSize: 'var(--qt-text-sm)',
    color: 'var(--qt-critical)',
  },
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

export function DropZone({
  accept,
  multiple = false,
  onFilesSelected,
  disabled = false,
  label,
  hint,
  maxSize,
  className,
}: DropZoneProps) {
  const [isDragging, setIsDragging] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  const acceptedExtensions = accept.split(',').map(ext => ext.trim().toLowerCase())

  const validateFiles = useCallback((files: File[]): File[] => {
    setError(null)
    const validFiles: File[] = []

    for (const file of files) {
      const ext = '.' + file.name.split('.').pop()?.toLowerCase()

      // Check extension
      if (!acceptedExtensions.includes(ext)) {
        setError(`Invalid file type: ${ext}. Accepted: ${accept}`)
        continue
      }

      // Check size
      if (maxSize && file.size > maxSize) {
        setError(`File too large: ${file.name} (${formatBytes(file.size)}). Max: ${formatBytes(maxSize)}`)
        continue
      }

      validFiles.push(file)
    }

    return validFiles
  }, [accept, acceptedExtensions, maxSize])

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
    const validFiles = validateFiles(files)

    if (validFiles.length > 0) {
      onFilesSelected(multiple ? validFiles : [validFiles[0]])
    }
  }, [disabled, multiple, onFilesSelected, validateFiles])

  const handleChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
    if (disabled) return

    const files = Array.from(e.target.files || [])
    const validFiles = validateFiles(files)

    if (validFiles.length > 0) {
      onFilesSelected(multiple ? validFiles : [validFiles[0]])
    }

    // Reset input so the same file can be selected again
    if (inputRef.current) {
      inputRef.current.value = ''
    }
  }, [disabled, multiple, onFilesSelected, validateFiles])

  const handleClick = useCallback(() => {
    if (!disabled) {
      inputRef.current?.click()
    }
  }, [disabled])

  const extensions = acceptedExtensions.map(ext => ext.replace('.', '')).join(' or ')
  const defaultLabel = multiple ? 'Drop files here' : 'Drop file here'
  const defaultHint = `Accepts ${extensions}${maxSize ? ` (max ${formatBytes(maxSize)})` : ''}`

  return (
    <div
      style={{
        ...styles.dropzone,
        ...(isDragging ? styles.dropzoneDragging : {}),
        ...(disabled ? styles.dropzoneDisabled : {}),
      }}
      className={className}
      onDragEnter={handleDragIn}
      onDragLeave={handleDragOut}
      onDragOver={handleDrag}
      onDrop={handleDrop}
      onClick={handleClick}
    >
      <input
        ref={inputRef}
        type="file"
        accept={accept}
        multiple={multiple}
        onChange={handleChange}
        disabled={disabled}
        style={styles.input}
        tabIndex={-1}
      />
      <div style={styles.content}>
        <svg
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          style={{
            ...styles.icon,
            ...(isDragging ? styles.iconDragging : {}),
          }}
        >
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
          <polyline points="17,8 12,3 7,8" />
          <line x1="12" y1="3" x2="12" y2="15" />
        </svg>
        <p style={styles.text}>
          <span style={styles.strong}>{label || defaultLabel}</span> or click to browse
        </p>
        <p style={styles.hint}>{hint || defaultHint}</p>
        {error && <p style={styles.error}>{error}</p>}
      </div>
    </div>
  )
}

export default DropZone
