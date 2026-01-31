import React from 'react'
import ReactDOM from 'react-dom/client'
import { Auth0Provider } from '@auth0/auth0-react'
import App from './App'
import { config, isAuthConfigured } from './config'
import '@querytorque/ui-shared/theme/tokens.css'
import './styles/global.css'

const root = document.getElementById('root')!

// Render with or without Auth0 based on configuration
if (isAuthConfigured()) {
  ReactDOM.createRoot(root).render(
    <React.StrictMode>
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
    </React.StrictMode>
  )
} else {
  // Render without Auth0 for local development
  ReactDOM.createRoot(root).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>
  )
}
