/**
 * Configuration for Query Torque DAX Web App
 */

export const config = {
  // API Configuration
  apiBaseUrl: import.meta.env.VITE_API_URL || '/api',

  // Auth0 Configuration
  auth0: {
    domain: import.meta.env.VITE_AUTH0_DOMAIN || 'dev-fnels80upiu30nf5.us.auth0.com',
    clientId: import.meta.env.VITE_AUTH0_CLIENT_ID || 'EmSDrYkCM9cCjMlL9oGL35ZyKns5vZWs',
    audience: import.meta.env.VITE_AUTH0_AUDIENCE || 'https://api.querytorque.com',
    redirectUri: typeof window !== 'undefined' ? window.location.origin : '',
  },

  // Feature flags
  features: {
    authEnabled: import.meta.env.VITE_AUTH_DISABLED !== 'true',
    liveModelConnection: import.meta.env.VITE_ENABLE_LIVE_MODEL !== 'false',
  },

  // App metadata
  app: {
    name: 'Query Torque DAX',
    description: 'Power BI Performance Analysis',
    version: '1.0.0',
  },

  // File upload limits
  upload: {
    maxFileSizeMB: 50,
    acceptedFileTypes: ['.vpax', '.pbix'],
  },
} as const

export type Config = typeof config
export default config
