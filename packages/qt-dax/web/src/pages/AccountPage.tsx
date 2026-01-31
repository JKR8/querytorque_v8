import { useAuth0 } from '@auth0/auth0-react'

export default function AccountPage() {
  const { user, isLoading, logout } = useAuth0()

  if (isLoading) {
    return (
      <div className="account-page">
        <div className="loading-section">
          <div className="spinner" />
          <p>Loading account...</p>
        </div>
        <style>{styles}</style>
      </div>
    )
  }

  if (!user) {
    return (
      <div className="account-page">
        <div className="error-section">
          <p>Please sign in to view your account.</p>
        </div>
        <style>{styles}</style>
      </div>
    )
  }

  return (
    <div className="account-page">
      <h1>Account Settings</h1>

      <div className="account-sections">
        {/* Profile Section */}
        <section className="account-section">
          <h2>Profile</h2>
          <div className="profile-card">
            <div className="profile-avatar">
              {user.picture ? (
                <img src={user.picture} alt={user.name || 'User'} />
              ) : (
                <span>{(user.name || user.email || 'U')[0].toUpperCase()}</span>
              )}
            </div>
            <div className="profile-info">
              <div className="info-row">
                <label>Name</label>
                <span>{user.name || 'Not set'}</span>
              </div>
              <div className="info-row">
                <label>Email</label>
                <span>{user.email}</span>
              </div>
              <div className="info-row">
                <label>Email Verified</label>
                <span className={user.email_verified ? 'verified' : 'not-verified'}>
                  {user.email_verified ? 'Yes' : 'No'}
                </span>
              </div>
            </div>
          </div>
        </section>

        {/* Subscription Section */}
        <section className="account-section">
          <h2>Subscription</h2>
          <div className="subscription-card">
            <div className="plan-info">
              <span className="plan-name">Free Plan</span>
              <span className="plan-desc">Basic analysis features</span>
            </div>
            <div className="plan-features">
              <ul>
                <li>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
                    <polyline points="20 6 9 17 4 12" />
                  </svg>
                  5 VPAX analyses per month
                </li>
                <li>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
                    <polyline points="20 6 9 17 4 12" />
                  </svg>
                  DAX performance rules
                </li>
                <li>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16">
                    <polyline points="20 6 9 17 4 12" />
                  </svg>
                  Model structure analysis
                </li>
              </ul>
            </div>
            <button className="btn btn-primary upgrade-btn">
              Upgrade to Pro
            </button>
          </div>
        </section>

        {/* Usage Section */}
        <section className="account-section">
          <h2>Usage This Month</h2>
          <div className="usage-card">
            <div className="usage-stat">
              <div className="usage-bar">
                <div className="usage-fill" style={{ width: '60%' }} />
              </div>
              <div className="usage-labels">
                <span>3 / 5 analyses used</span>
                <span className="usage-percent">60%</span>
              </div>
            </div>
            <p className="usage-reset">Usage resets on the 1st of each month</p>
          </div>
        </section>

        {/* Danger Zone */}
        <section className="account-section danger-zone">
          <h2>Sign Out</h2>
          <p>Sign out of your account on this device.</p>
          <button
            className="btn btn-outline-danger"
            onClick={() => logout({ logoutParams: { returnTo: window.location.origin } })}
          >
            Sign Out
          </button>
        </section>
      </div>

      <style>{styles}</style>
    </div>
  )
}

const styles = `
  .account-page {
    padding: var(--qt-space-lg);
    max-width: 800px;
    margin: 0 auto;
  }

  .account-page h1 {
    font-size: var(--qt-text-2xl);
    font-weight: var(--qt-font-bold);
    margin-bottom: var(--qt-space-xl);
  }

  .loading-section, .error-section {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: var(--qt-space-2xl);
    text-align: center;
    color: var(--qt-fg-muted);
  }

  .loading-section p {
    margin-top: var(--qt-space-md);
  }

  .account-sections {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-xl);
  }

  .account-section {
    background: var(--qt-bg-card);
    border: 1px solid var(--qt-border);
    border-radius: var(--qt-radius);
    padding: var(--qt-space-lg);
  }

  .account-section h2 {
    font-size: var(--qt-text-lg);
    font-weight: var(--qt-font-semibold);
    margin-bottom: var(--qt-space-md);
  }

  .profile-card {
    display: flex;
    gap: var(--qt-space-lg);
    align-items: flex-start;
  }

  .profile-avatar {
    width: 80px;
    height: 80px;
    border-radius: 50%;
    background: var(--qt-brand);
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    flex-shrink: 0;
  }

  .profile-avatar img {
    width: 100%;
    height: 100%;
    object-fit: cover;
  }

  .profile-avatar span {
    color: white;
    font-size: var(--qt-text-2xl);
    font-weight: var(--qt-font-bold);
  }

  .profile-info {
    flex: 1;
  }

  .info-row {
    display: flex;
    justify-content: space-between;
    padding: var(--qt-space-sm) 0;
    border-bottom: 1px solid var(--qt-border-light);
  }

  .info-row:last-child {
    border-bottom: none;
  }

  .info-row label {
    font-size: var(--qt-text-sm);
    color: var(--qt-fg-muted);
  }

  .info-row span {
    font-size: var(--qt-text-sm);
    font-weight: var(--qt-font-medium);
  }

  .info-row .verified {
    color: var(--qt-low);
  }

  .info-row .not-verified {
    color: var(--qt-medium);
  }

  .subscription-card {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-md);
  }

  .plan-info {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-xs);
  }

  .plan-name {
    font-size: var(--qt-text-lg);
    font-weight: var(--qt-font-semibold);
    color: var(--qt-brand);
  }

  .plan-desc {
    font-size: var(--qt-text-sm);
    color: var(--qt-fg-muted);
  }

  .plan-features ul {
    list-style: none;
    padding: 0;
    margin: 0;
  }

  .plan-features li {
    display: flex;
    align-items: center;
    gap: var(--qt-space-sm);
    padding: var(--qt-space-xs) 0;
    font-size: var(--qt-text-sm);
    color: var(--qt-fg-muted);
  }

  .plan-features li svg {
    color: var(--qt-low);
    flex-shrink: 0;
  }

  .upgrade-btn {
    align-self: flex-start;
  }

  .usage-card {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-md);
  }

  .usage-stat {
    display: flex;
    flex-direction: column;
    gap: var(--qt-space-sm);
  }

  .usage-bar {
    height: 8px;
    background: var(--qt-bg-alt);
    border-radius: var(--qt-radius-full);
    overflow: hidden;
  }

  .usage-fill {
    height: 100%;
    background: var(--qt-brand);
    border-radius: var(--qt-radius-full);
    transition: width 0.3s ease;
  }

  .usage-labels {
    display: flex;
    justify-content: space-between;
    font-size: var(--qt-text-sm);
    color: var(--qt-fg-muted);
  }

  .usage-percent {
    font-weight: var(--qt-font-semibold);
    color: var(--qt-fg);
  }

  .usage-reset {
    font-size: var(--qt-text-xs);
    color: var(--qt-fg-muted);
  }

  .danger-zone {
    border-color: var(--qt-critical-border);
  }

  .danger-zone h2 {
    color: var(--qt-critical);
  }

  .danger-zone p {
    font-size: var(--qt-text-sm);
    color: var(--qt-fg-muted);
    margin-bottom: var(--qt-space-md);
  }

  .btn-outline-danger {
    background: transparent;
    border: 1px solid var(--qt-critical);
    color: var(--qt-critical);
    padding: var(--qt-space-sm) var(--qt-space-md);
    border-radius: var(--qt-radius-sm);
    font-size: var(--qt-text-sm);
    font-weight: var(--qt-font-medium);
    cursor: pointer;
    transition: all var(--qt-transition-fast);
  }

  .btn-outline-danger:hover {
    background: var(--qt-critical);
    color: white;
  }

  @media (max-width: 600px) {
    .profile-card {
      flex-direction: column;
      align-items: center;
      text-align: center;
    }

    .info-row {
      flex-direction: column;
      gap: var(--qt-space-xs);
      text-align: center;
    }
  }
`
