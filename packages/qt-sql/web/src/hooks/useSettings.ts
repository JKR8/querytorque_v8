/**
 * Settings hook — fetches backend health to determine LLM availability
 */

import { useState, useEffect, useCallback } from 'react'
import { getHealth } from '@/api/client'

export interface AppSettings {
  llmProvider: string | null
  llmConfigured: boolean
}

interface UseSettingsReturn {
  settings: AppSettings
  isLoading: boolean
  error: string | null
  refreshSettings: () => Promise<void>
}

const DEFAULT_SETTINGS: AppSettings = {
  llmProvider: null,
  llmConfigured: false,
}

export function useSettings(): UseSettingsReturn {
  const [settings, setSettings] = useState<AppSettings>(DEFAULT_SETTINGS)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const refreshSettings = useCallback(async () => {
    setIsLoading(true)
    setError(null)

    try {
      const health = await getHealth()
      setSettings({
        llmProvider: health.llm_provider,
        llmConfigured: health.llm_configured,
      })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load settings')
    } finally {
      setIsLoading(false)
    }
  }, [])

  useEffect(() => {
    refreshSettings()
  }, [refreshSettings])

  return { settings, isLoading, error, refreshSettings }
}

export default useSettings
