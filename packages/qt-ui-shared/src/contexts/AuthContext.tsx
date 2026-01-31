/**
 * AuthContext - Auth0 Context Wrapper
 * Provides authentication state and methods for QueryTorque web apps
 */

import { createContext, useContext, ReactNode, useMemo, useCallback } from 'react'
import { Auth0Provider, useAuth0 } from '@auth0/auth0-react'

/**
 * User object representing the authenticated user
 */
export interface User {
  /** Auth0 user ID (sub) */
  id: string
  /** User's email address */
  email: string
  /** User's display name */
  name?: string
  /** User's avatar URL */
  picture?: string
  /** Organization ID from Auth0 metadata */
  org_id: string
  /** User's role (admin, member, viewer) */
  role: string
  /** Subscription tier (free, pro, enterprise) */
  tier: string
}

/**
 * Authentication context type
 */
export interface AuthContextType {
  /** Current authenticated user */
  user: User | null
  /** Whether user is authenticated */
  isAuthenticated: boolean
  /** Whether auth state is loading */
  isLoading: boolean
  /** Current access token (null until fetched) */
  token: string | null
  /** Redirect to login */
  login: () => void
  /** Redirect to signup */
  signup: () => void
  /** Logout and redirect to home */
  logout: () => void
  /** Get access token silently */
  getToken: () => Promise<string | null>
}

/**
 * Auth0 configuration options
 */
export interface AuthConfig {
  /** Auth0 domain (e.g., 'your-tenant.auth0.com') */
  domain: string
  /** Auth0 client ID */
  clientId: string
  /** Auth0 API audience (optional) */
  audience?: string
  /** Redirect URI after login (defaults to current origin) */
  redirectUri?: string
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

interface AuthProviderProps {
  children: ReactNode
  /** Auth0 configuration */
  config: AuthConfig
  /** Set to true to disable auth (useful for testing) */
  disabled?: boolean
}

/**
 * Check if auth is properly configured
 */
export function isAuthConfigured(config: AuthConfig): boolean {
  return Boolean(config.domain && config.clientId)
}

/**
 * Inner provider that uses the Auth0 hook
 */
function AuthContextProvider({
  children,
  audience
}: {
  children: ReactNode
  audience?: string
}) {
  const {
    user: auth0User,
    isAuthenticated,
    isLoading,
    loginWithRedirect,
    logout: auth0Logout,
    getAccessTokenSilently,
  } = useAuth0()

  // Map Auth0 user to our User interface
  const user: User | null = useMemo(() => {
    if (!auth0User) return null

    return {
      id: auth0User.sub || '',
      email: auth0User.email || '',
      name: auth0User.name,
      picture: auth0User.picture,
      org_id: audience ? (auth0User as Record<string, unknown>)[`${audience}/org_id`] as string || '' : '',
      role: audience ? (auth0User as Record<string, unknown>)[`${audience}/role`] as string || 'member' : 'member',
      tier: audience ? (auth0User as Record<string, unknown>)[`${audience}/tier`] as string || 'free' : 'free',
    }
  }, [auth0User, audience])

  const login = useCallback(() => {
    loginWithRedirect()
  }, [loginWithRedirect])

  const signup = useCallback(() => {
    loginWithRedirect({
      authorizationParams: {
        screen_hint: 'signup',
      },
    })
  }, [loginWithRedirect])

  const logout = useCallback(() => {
    auth0Logout({
      logoutParams: {
        returnTo: window.location.origin,
      },
    })
  }, [auth0Logout])

  const getToken = useCallback(async (): Promise<string | null> => {
    try {
      const token = await getAccessTokenSilently()
      return token
    } catch (error) {
      console.error('Error getting token:', error)
      return null
    }
  }, [getAccessTokenSilently])

  const value: AuthContextType = useMemo(() => ({
    user,
    isAuthenticated,
    isLoading,
    token: null, // Token is fetched on demand via getToken()
    login,
    signup,
    logout,
    getToken,
  }), [user, isAuthenticated, isLoading, login, signup, logout, getToken])

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

/**
 * Create a stub context for when auth is disabled
 */
function createStubContext(): AuthContextType {
  return {
    user: null,
    isAuthenticated: false,
    isLoading: false,
    token: null,
    login: () => console.warn('Auth not configured'),
    signup: () => console.warn('Auth not configured'),
    logout: () => {},
    getToken: async () => null,
  }
}

/**
 * AuthProvider component that wraps your app with Auth0
 *
 * @example
 * ```tsx
 * import { AuthProvider } from '@querytorque/ui-shared'
 *
 * function App() {
 *   return (
 *     <AuthProvider config={{
 *       domain: 'your-tenant.auth0.com',
 *       clientId: 'your-client-id',
 *       audience: 'https://api.yourapp.com'
 *     }}>
 *       <YourApp />
 *     </AuthProvider>
 *   )
 * }
 * ```
 */
export function AuthProvider({
  children,
  config,
  disabled = false
}: AuthProviderProps) {
  // If auth is disabled or not configured, provide a stub context
  if (disabled || !isAuthConfigured(config)) {
    return (
      <AuthContext.Provider value={createStubContext()}>
        {children}
      </AuthContext.Provider>
    )
  }

  const redirectUri = config.redirectUri || (typeof window !== 'undefined' ? window.location.origin : '')

  return (
    <Auth0Provider
      domain={config.domain}
      clientId={config.clientId}
      authorizationParams={{
        redirect_uri: redirectUri,
        ...(config.audience && { audience: config.audience }),
      }}
    >
      <AuthContextProvider audience={config.audience}>
        {children}
      </AuthContextProvider>
    </Auth0Provider>
  )
}

/**
 * Hook to access authentication context
 *
 * @example
 * ```tsx
 * import { useAuth } from '@querytorque/ui-shared'
 *
 * function MyComponent() {
 *   const { user, isAuthenticated, login, logout } = useAuth()
 *
 *   if (!isAuthenticated) {
 *     return <button onClick={login}>Log In</button>
 *   }
 *
 *   return (
 *     <div>
 *       <p>Hello, {user?.name}</p>
 *       <button onClick={logout}>Log Out</button>
 *     </div>
 *   )
 * }
 * ```
 */
export function useAuth(): AuthContextType {
  const context = useContext(AuthContext)
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}

export default AuthContext
