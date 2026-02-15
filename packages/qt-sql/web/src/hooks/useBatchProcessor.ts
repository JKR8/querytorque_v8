/**
 * Batch processor hook — manages batch SQL optimization via real pipeline API
 */

import { useReducer, useCallback, useRef } from 'react'
import { optimizeQuery, OptimizeResponse } from '@/api/client'

export type BatchFileStatus =
  | 'pending'
  | 'running'
  | 'optimized'
  | 'neutral'
  | 'regression'
  | 'failed'

export interface BatchFile {
  id: string
  name: string
  content: string
  status: BatchFileStatus
  speedup?: number
  transforms?: string[]
  optimizedContent?: string
  optimizeResult?: OptimizeResponse
  error?: string
  retryCount: number
}

export interface BatchSettings {
  maxRetries: number
  dsn: string
  mode: 'beam'
  sessionId?: string
}

interface BatchState {
  files: BatchFile[]
  isProcessing: boolean
  currentIndex: number
  settings: BatchSettings
  aborted: boolean
}

type BatchAction =
  | { type: 'ADD_FILES'; files: { name: string; content: string }[] }
  | { type: 'UPDATE_FILE'; id: string; updates: Partial<BatchFile> }
  | { type: 'SET_PROCESSING'; isProcessing: boolean }
  | { type: 'SET_CURRENT_INDEX'; index: number }
  | { type: 'SET_SETTINGS'; settings: Partial<BatchSettings> }
  | { type: 'ABORT' }
  | { type: 'RESET' }
  | { type: 'REMOVE_FILE'; id: string }

const DEFAULT_SETTINGS: BatchSettings = {
  maxRetries: 2,
  dsn: '',
  mode: 'beam',
}

function generateId(): string {
  return Math.random().toString(36).slice(2, 10)
}

function batchReducer(state: BatchState, action: BatchAction): BatchState {
  switch (action.type) {
    case 'ADD_FILES':
      return {
        ...state,
        files: [
          ...state.files,
          ...action.files.map(f => ({
            id: generateId(),
            name: f.name,
            content: f.content,
            status: 'pending' as const,
            retryCount: 0,
          })),
        ],
      }

    case 'UPDATE_FILE':
      return {
        ...state,
        files: state.files.map(f =>
          f.id === action.id ? { ...f, ...action.updates } : f
        ),
      }

    case 'SET_PROCESSING':
      return { ...state, isProcessing: action.isProcessing }

    case 'SET_CURRENT_INDEX':
      return { ...state, currentIndex: action.index }

    case 'SET_SETTINGS':
      return { ...state, settings: { ...state.settings, ...action.settings } }

    case 'ABORT':
      return { ...state, aborted: true, isProcessing: false }

    case 'RESET':
      return {
        files: [],
        isProcessing: false,
        currentIndex: 0,
        settings: state.settings, // preserve DSN
        aborted: false,
      }

    case 'REMOVE_FILE':
      return {
        ...state,
        files: state.files.filter(f => f.id !== action.id),
      }

    default:
      return state
  }
}

export interface UseBatchProcessorReturn {
  files: BatchFile[]
  isProcessing: boolean
  currentIndex: number
  settings: BatchSettings
  progress: {
    total: number
    completed: number
    optimized: number
    neutral: number
    regression: number
    failed: number
    percent: number
  }
  addFiles: (files: File[]) => Promise<void>
  removeFile: (id: string) => void
  start: () => Promise<void>
  abort: () => void
  reset: () => void
  updateSettings: (settings: Partial<BatchSettings>) => void
  retryFile: (id: string) => Promise<void>
}

export function useBatchProcessor(): UseBatchProcessorReturn {
  const [state, dispatch] = useReducer(batchReducer, {
    files: [],
    isProcessing: false,
    currentIndex: 0,
    settings: DEFAULT_SETTINGS,
    aborted: false,
  })

  const abortRef = useRef(false)

  const progress = {
    total: state.files.length,
    completed: state.files.filter(f =>
      ['optimized', 'neutral', 'regression', 'failed'].includes(f.status)
    ).length,
    optimized: state.files.filter(f => f.status === 'optimized').length,
    neutral: state.files.filter(f => f.status === 'neutral').length,
    regression: state.files.filter(f => f.status === 'regression').length,
    failed: state.files.filter(f => f.status === 'failed').length,
    percent: state.files.length > 0
      ? Math.round(
          (state.files.filter(f =>
            ['optimized', 'neutral', 'regression', 'failed'].includes(f.status)
          ).length /
            state.files.length) *
            100
        )
      : 0,
  }

  const addFiles = useCallback(async (files: File[]) => {
    const fileData = await Promise.all(
      files.map(async file => ({
        name: file.name,
        content: await file.text(),
      }))
    )
    dispatch({ type: 'ADD_FILES', files: fileData })
  }, [])

  const removeFile = useCallback((id: string) => {
    dispatch({ type: 'REMOVE_FILE', id })
  }, [])

  const processFile = useCallback(
    async (file: BatchFile): Promise<Partial<BatchFile>> => {
      if (!state.settings.dsn) {
        return { status: 'failed', error: 'No database DSN configured' }
      }

      dispatch({ type: 'UPDATE_FILE', id: file.id, updates: { status: 'running' } })

      try {
        const result = await optimizeQuery({
          sql: file.content,
          dsn: state.settings.dsn,
          mode: state.settings.mode,
          query_id: file.name.replace('.sql', ''),
          session_id: state.settings.sessionId,
        })

        const statusMap: Record<string, BatchFileStatus> = {
          'WIN': 'optimized',
          'IMPROVED': 'optimized',
          'NEUTRAL': 'neutral',
          'REGRESSION': 'regression',
          'ERROR': 'failed',
        }

        return {
          status: statusMap[result.status] || 'failed',
          speedup: result.speedup,
          transforms: result.transforms,
          optimizedContent: result.optimized_sql || undefined,
          optimizeResult: result,
          error: result.error || undefined,
        }
      } catch (err) {
        return {
          status: 'failed',
          error: err instanceof Error ? err.message : 'Optimization failed',
        }
      }
    },
    [state.settings]
  )

  const start = useCallback(async () => {
    dispatch({ type: 'SET_PROCESSING', isProcessing: true })
    abortRef.current = false

    for (let i = 0; i < state.files.length; i++) {
      if (abortRef.current) break

      const file = state.files[i]
      if (file.status !== 'pending') continue

      dispatch({ type: 'SET_CURRENT_INDEX', index: i })

      const result = await processFile(file)
      dispatch({ type: 'UPDATE_FILE', id: file.id, updates: result })
    }

    dispatch({ type: 'SET_PROCESSING', isProcessing: false })
  }, [state.files, processFile])

  const abort = useCallback(() => {
    abortRef.current = true
    dispatch({ type: 'ABORT' })
  }, [])

  const reset = useCallback(() => {
    abortRef.current = false
    dispatch({ type: 'RESET' })
  }, [])

  const updateSettings = useCallback((settings: Partial<BatchSettings>) => {
    dispatch({ type: 'SET_SETTINGS', settings })
  }, [])

  const retryFile = useCallback(
    async (id: string) => {
      const file = state.files.find(f => f.id === id)
      if (!file || file.retryCount >= state.settings.maxRetries) return

      dispatch({
        type: 'UPDATE_FILE',
        id,
        updates: {
          status: 'pending',
          retryCount: file.retryCount + 1,
          error: undefined,
        },
      })

      const result = await processFile(file)
      dispatch({ type: 'UPDATE_FILE', id, updates: result })
    },
    [state.files, state.settings.maxRetries, processFile]
  )

  return {
    files: state.files,
    isProcessing: state.isProcessing,
    currentIndex: state.currentIndex,
    settings: state.settings,
    progress,
    addFiles,
    removeFile,
    start,
    abort,
    reset,
    updateSettings,
    retryFile,
  }
}

export default useBatchProcessor
