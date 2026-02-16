/**
 * CodeEditor Component
 * Monaco editor wrapper for SQL code editing with syntax highlighting
 */

import { useRef, useCallback } from 'react'
import Editor, { OnMount, OnChange } from '@monaco-editor/react'
import type { editor } from 'monaco-editor'
import '../theme/tokens.css'

export type EditorLanguage = 'sql' | 'json' | 'javascript' | 'typescript'

export interface CodeEditorProps {
  /** Current value of the editor */
  value: string
  /** Callback when content changes */
  onChange?: (value: string) => void
  /** Programming language for syntax highlighting */
  language: EditorLanguage
  /** Whether the editor is read-only */
  readOnly?: boolean
  /** Whether the editor is disabled */
  disabled?: boolean
  /** Placeholder text when editor is empty */
  placeholder?: string
  /** Height of the editor (CSS value) */
  height?: string
  /** Show line numbers */
  lineNumbers?: boolean
  /** Show minimap */
  minimap?: boolean
  /** Word wrap mode */
  wordWrap?: 'on' | 'off' | 'wordWrapColumn' | 'bounded'
  /** Theme: 'vs' (light) | 'vs-dark' (dark) */
  theme?: 'vs' | 'vs-dark'
  /** Additional CSS class name */
  className?: string
  /** Callback when editor is mounted */
  onMount?: OnMount
}

// Map our language types to Monaco language IDs
const languageMap: Record<EditorLanguage, string> = {
  sql: 'sql',
  json: 'json',
  javascript: 'javascript',
  typescript: 'typescript',
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    border: '1px solid var(--qt-border)',
    borderRadius: 'var(--qt-radius)',
    overflow: 'hidden',
    background: 'var(--qt-bg-card)',
  },
  disabled: {
    opacity: 0.6,
    pointerEvents: 'none',
  },
  placeholder: {
    position: 'absolute',
    top: '10px',
    left: '60px',
    color: 'var(--qt-fg-muted)',
    fontFamily: 'var(--qt-font-mono)',
    fontSize: 'var(--qt-text-sm)',
    pointerEvents: 'none',
    zIndex: 1,
  },
  wrapper: {
    position: 'relative',
  },
}

export function CodeEditor({
  value,
  onChange,
  language,
  readOnly = false,
  disabled = false,
  placeholder,
  height = '400px',
  lineNumbers = true,
  minimap = false,
  wordWrap = 'on',
  theme = 'vs-dark',
  className,
  onMount,
}: CodeEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)

  const handleMount: OnMount = useCallback((editor, monaco) => {
    editorRef.current = editor

    // Call user's onMount if provided
    if (onMount) {
      onMount(editor, monaco)
    }
  }, [onMount])

  const handleChange: OnChange = useCallback((newValue) => {
    if (onChange && newValue !== undefined) {
      onChange(newValue)
    }
  }, [onChange])

  const monacoLanguage = languageMap[language]
  const showPlaceholder = !value && placeholder

  return (
    <div
      style={{
        ...styles.container,
        ...(disabled ? styles.disabled : {}),
        height,
      }}
      className={className}
    >
      <div style={styles.wrapper}>
        {showPlaceholder && (
          <div style={styles.placeholder as React.CSSProperties}>{placeholder}</div>
        )}
        <Editor
          height={height}
          language={monacoLanguage}
          value={value}
          onChange={handleChange}
          onMount={handleMount}
          theme={theme}
          options={{
            readOnly: readOnly || disabled,
            minimap: { enabled: minimap },
            lineNumbers: lineNumbers ? 'on' : 'off',
            wordWrap,
            fontSize: 13,
            fontFamily: "'JetBrains Mono', 'Fira Code', 'Monaco', Consolas, monospace",
            tabSize: 2,
            automaticLayout: true,
            scrollBeyondLastLine: false,
            folding: true,
            renderLineHighlight: 'line',
            selectOnLineNumbers: true,
            quickSuggestions: true,
            suggestOnTriggerCharacters: true,
            formatOnPaste: true,
            formatOnType: true,
            padding: { top: 10, bottom: 10 },
          }}
        />
      </div>
    </div>
  )
}

export default CodeEditor
