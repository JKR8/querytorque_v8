/**
 * Settings hook - manages application settings with localStorage persistence
 */

import { useState, useEffect, useCallback } from 'react'
import { getHealth } from '@/api/client'

export interface AppSettings {
  mode: 'auto' | 'manual'
  llmProvider: string | null
  llmConfigured: boolean
}

interface UseSettingsReturn {
  settings: AppSettings
  isLoading: boolean
  error: string | null
  refreshSettings: () => Promise<void>
  setMode: (mode: 'auto' | 'manual') => void
}

const STORAGE_KEY = 'qt-sql-settings'

const DEFAULT_SETTINGS: AppSettings = {
  mode: 'manual',
  llmProvider: null,
  llmConfigured: false,
}

function loadFromStorage(): Partial<AppSettings> {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored) {
      return JSON.parse(stored)
    }
  } catch {
    // Ignore storage errors
  }
  return {}
}

function saveToStorage(settings: Partial<AppSettings>): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(settings))
  } catch {
    // Ignore storage errors
  }
}

export function useSettings(): UseSettingsReturn {
  const [settings, setSettings] = useState<AppSettings>(() => ({
    ...DEFAULT_SETTINGS,
    ...loadFromStorage(),
  }))
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const refreshSettings = useCallback(async () => {
    setIsLoading(true)
    setError(null)

    try {
      const health = await getHealth()
      const newSettings: AppSettings = {
        mode: health.mode,
        llmProvider: health.llm_provider,
        llmConfigured: health.llm_configured,
      }
      setSettings(newSettings)
      saveToStorage({ mode: newSettings.mode })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load settings')
      // Keep existing settings on error
    } finally {
      setIsLoading(false)
    }
  }, [])

  // Load settings on mount
  useEffect(() => {
    refreshSettings()
  }, [refreshSettings])

  const setMode = useCallback((mode: 'auto' | 'manual') => {
    setSettings(prev => {
      const next = { ...prev, mode }
      saveToStorage({ mode })
      return next
    })
  }, [])

  return {
    settings,
    isLoading,
    error,
    refreshSettings,
    setMode,
  }
}

export default useSettings
