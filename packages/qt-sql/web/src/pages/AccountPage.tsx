import { useAuth0 } from '@auth0/auth0-react'
import { isAuthConfigured } from '@/config'
import './AccountPage.css'

export default function AccountPage() {
  const authEnabled = isAuthConfigured()
  const auth0 = authEnabled ? useAuth0() : null
  const { user, logout } = auth0 || { user: null, logout: () => {} }

  if (!authEnabled) {
    return (
      <div className="account-page">
        <div className="auth-disabled">
          <h1>Account</h1>
          <p>Authentication is not configured for this instance.</p>
          <p className="hint">Set VITE_AUTH0_DOMAIN and VITE_AUTH0_CLIENT_ID to enable authentication.</p>
        </div>
      </div>
    )
  }

  if (!user) {
    return (
      <div className="account-page">
        <div className="loading-state">
          <div className="spinner-large" />
          <p>Loading account...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="account-page">
      <div className="account-header">
        <h1>Account Settings</h1>
      </div>

      <div className="account-content">
        <section className="account-section">
          <h2>Profile</h2>
          <div className="profile-card">
            <div className="profile-avatar">
              {user.picture ? (
                <img src={user.picture} alt={user.name || user.email} />
              ) : (
                <span>{(user.name || user.email || 'U')[0].toUpperCase()}</span>
              )}
            </div>
            <div className="profile-info">
              <div className="profile-name">{user.name || 'No name'}</div>
              <div className="profile-email">{user.email}</div>
            </div>
          </div>
        </section>

        <section className="account-section">
          <h2>Account Details</h2>
          <div className="details-grid">
            <div className="detail-item">
              <label>User ID</label>
              <code>{user.sub}</code>
            </div>
            <div className="detail-item">
              <label>Email Verified</label>
              <span className={user.email_verified ? 'verified' : 'not-verified'}>
                {user.email_verified ? 'Yes' : 'No'}
              </span>
            </div>
            <div className="detail-item">
              <label>Last Updated</label>
              <span>{user.updated_at ? new Date(user.updated_at).toLocaleDateString() : 'N/A'}</span>
            </div>
          </div>
        </section>

        <section className="account-section">
          <h2>Session</h2>
          <p className="section-desc">Sign out of your account on this device.</p>
          <button
            className="btn btn-danger"
            onClick={() => logout({ logoutParams: { returnTo: window.location.origin } })}
          >
            Sign Out
          </button>
        </section>
      </div>
    </div>
  )
}
