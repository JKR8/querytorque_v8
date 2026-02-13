/**
 * Application configuration
 */

export const config = {
  api: {
    baseUrl: import.meta.env.VITE_API_BASE_URL || '/api',
  },
} as const
