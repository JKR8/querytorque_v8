/**
 * Batch processor hook - manages batch SQL optimization
 */

import { useReducer, useCallback, useRef } from 'react'
import { analyzeSql, startOptimization, AnalysisResult, OptimizationSession } from '@/api/client'

export type BatchFileStatus =
  | 'pending'
  | 'analyzing'
  | 'optimizing'
  | 'fixed'
  | 'skipped'
  | 'failed'

export interface BatchFile {
  id: string
  name: string
  content: string
  status: BatchFileStatus
  score?: number
  analysis?: AnalysisResult
  optimization?: OptimizationSession
  optimizedContent?: string
  error?: string
  retryCount: number
}

export interface BatchSettings {
  autoFixThreshold: number // Score below this will be optimized (0-100)
  maxRetries: number
  mode: 'auto' | 'manual'
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
  autoFixThreshold: 80, // Optimize if score < 80
  maxRetries: 3,
  mode: 'auto',
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
        settings: DEFAULT_SETTINGS,
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
    fixed: number
    skipped: number
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

  // Calculate progress
  const progress = {
    total: state.files.length,
    completed: state.files.filter(f =>
      ['fixed', 'skipped', 'failed'].includes(f.status)
    ).length,
    fixed: state.files.filter(f => f.status === 'fixed').length,
    skipped: state.files.filter(f => f.status === 'skipped').length,
    failed: state.files.filter(f => f.status === 'failed').length,
    percent: state.files.length > 0
      ? Math.round(
          (state.files.filter(f =>
            ['fixed', 'skipped', 'failed'].includes(f.status)
          ).length /
            state.files.length) *
            100
        )
      : 0,
  }

  // Add files from file input
  const addFiles = useCallback(async (files: File[]) => {
    const fileData = await Promise.all(
      files.map(async file => ({
        name: file.name,
        content: await file.text(),
      }))
    )
    dispatch({ type: 'ADD_FILES', files: fileData })
  }, [])

  // Remove a file
  const removeFile = useCallback((id: string) => {
    dispatch({ type: 'REMOVE_FILE', id })
  }, [])

  // Process a single file
  const processFile = useCallback(
    async (file: BatchFile): Promise<Partial<BatchFile>> => {
      // Step 1: Analyze
      dispatch({ type: 'UPDATE_FILE', id: file.id, updates: { status: 'analyzing' } })

      try {
        const analysis = await analyzeSql(file.content, file.name)

        // Check if optimization needed
        if (analysis.score >= state.settings.autoFixThreshold) {
          return {
            status: 'skipped',
            score: analysis.score,
            analysis,
          }
        }

        // Step 2: Optimize (if in auto mode)
        if (state.settings.mode === 'manual') {
          return {
            status: 'skipped',
            score: analysis.score,
            analysis,
            error: 'Manual mode - optimization skipped',
          }
        }

        dispatch({ type: 'UPDATE_FILE', id: file.id, updates: { status: 'optimizing' } })

        const session = await startOptimization(file.content, 'auto', file.name)

        if (session.optimized_code) {
          return {
            status: 'fixed',
            score: analysis.score,
            analysis,
            optimization: session,
            optimizedContent: session.optimized_code,
          }
        } else {
          return {
            status: 'failed',
            score: analysis.score,
            analysis,
            optimization: session,
            error: session.errors?.[0] || 'Optimization returned no result',
          }
        }
      } catch (err) {
        return {
          status: 'failed',
          error: err instanceof Error ? err.message : 'Processing failed',
        }
      }
    },
    [state.settings]
  )

  // Start batch processing
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

  // Abort processing
  const abort = useCallback(() => {
    abortRef.current = true
    dispatch({ type: 'ABORT' })
  }, [])

  // Reset everything
  const reset = useCallback(() => {
    abortRef.current = false
    dispatch({ type: 'RESET' })
  }, [])

  // Update settings
  const updateSettings = useCallback((settings: Partial<BatchSettings>) => {
    dispatch({ type: 'SET_SETTINGS', settings })
  }, [])

  // Retry a failed file
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
