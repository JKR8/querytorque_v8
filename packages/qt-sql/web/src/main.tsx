import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import '@querytorque/ui-shared/theme/tokens.css'
import './styles/global.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)
