/**
 * Application configuration
 * Loaded from environment variables with sensible defaults
 */

export const config = {
  // Auth0 Configuration
  auth0: {
    domain: import.meta.env.VITE_AUTH0_DOMAIN || 'dev-fnels80upiu30nf5.us.auth0.com',
    clientId: import.meta.env.VITE_AUTH0_CLIENT_ID || '',
    audience: import.meta.env.VITE_AUTH0_AUDIENCE || 'https://api.querytorque.com',
    redirectUri: window.location.origin,
  },

  // API Configuration
  api: {
    baseUrl: import.meta.env.VITE_API_BASE_URL || '/api',
  },

  // Feature flags
  features: {
    authEnabled: import.meta.env.VITE_AUTH_DISABLED !== 'true',
  },
} as const

/**
 * Check if Auth0 is configured
 */
export function isAuthConfigured(): boolean {
  if (!config.features.authEnabled) {
    return false
  }
  return Boolean(config.auth0.domain && config.auth0.clientId)
}
