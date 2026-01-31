import React from 'react'
import ReactDOM from 'react-dom/client'
import { Auth0Provider } from '@auth0/auth0-react'
import App from './App'
import { config } from './config'
import './styles/tokens.css'
import './styles/global.css'

const root = document.getElementById('root')

if (!root) {
  throw new Error('Root element not found')
}

// Wrap app with Auth0Provider if auth is enabled
const AppWithProviders = () => {
  if (config.features.authEnabled) {
    return (
      <Auth0Provider
        domain={config.auth0.domain}
        clientId={config.auth0.clientId}
        authorizationParams={{
          redirect_uri: config.auth0.redirectUri,
          audience: config.auth0.audience,
        }}
      >
        <App />
      </Auth0Provider>
    )
  }

  return <App />
}

ReactDOM.createRoot(root).render(
  <React.StrictMode>
    <AppWithProviders />
  </React.StrictMode>
)
