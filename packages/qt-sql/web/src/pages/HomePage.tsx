import { useNavigate } from 'react-router-dom'
import { useAuth0 } from '@auth0/auth0-react'
import { isAuthConfigured } from '@/config'

export default function HomePage() {
  const navigate = useNavigate()
  const authEnabled = isAuthConfigured()
  const auth0 = authEnabled ? useAuth0() : null
  const { isAuthenticated, loginWithRedirect } = auth0 || {
    isAuthenticated: false,
    loginWithRedirect: () => {},
  }

  const handleGetStarted = () => {
    if (authEnabled && !isAuthenticated) {
      loginWithRedirect()
    } else {
      navigate('/editor')
    }
  }

  return (
    <div className="home-page">
      <header className="home-header">
        <div className="brand">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
          </svg>
          <span>Query Torque SQL</span>
        </div>

        {authEnabled && !isAuthenticated ? (
          <button className="btn btn-primary" onClick={() => loginWithRedirect()}>
            Sign In
          </button>
        ) : authEnabled ? (
          <button className="btn btn-primary" onClick={() => navigate('/editor')}>
            Open Editor
          </button>
        ) : (
          <button className="btn btn-primary" onClick={() => navigate('/editor')}>
            Open Editor
          </button>
        )}
      </header>

      <main className="home-main">
        <section className="hero">
          <h1>SQL Performance Analysis</h1>
          <p className="hero-subtitle">
            Detect anti-patterns, optimize queries, and ship faster SQL.
            Powered by 119+ detection rules and AI-assisted optimization.
          </p>
          <button className="btn btn-primary btn-lg" onClick={handleGetStarted}>
            Get Started
          </button>
        </section>

        <section className="features">
          <div className="feature-card">
            <div className="feature-icon">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <circle cx="11" cy="11" r="8" />
                <path d="M21 21l-4.35-4.35" />
              </svg>
            </div>
            <h3>Analyze</h3>
            <p>Detect 119+ SQL anti-patterns including performance killers, security issues, and maintainability problems.</p>
          </div>

          <div className="feature-card">
            <div className="feature-icon">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M12 20h9" />
                <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" />
              </svg>
            </div>
            <h3>Optimize</h3>
            <p>Get AI-powered optimization suggestions with validated fixes. No regressions, guaranteed equivalence.</p>
          </div>

          <div className="feature-card">
            <div className="feature-icon">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
                <polyline points="22 4 12 14.01 9 11.01" />
              </svg>
            </div>
            <h3>Validate</h3>
            <p>Every optimization is validated for syntax, schema compliance, and semantic equivalence before acceptance.</p>
          </div>
        </section>

        <section className="cta">
          <h2>Ready to optimize your SQL?</h2>
          <p>Start analyzing your queries in seconds. No credit card required.</p>
          <button className="btn btn-primary btn-lg" onClick={handleGetStarted}>
            Try It Now
          </button>
        </section>
      </main>

      <footer className="home-footer">
        <p>Query Torque SQL - Part of the QueryTorque Platform</p>
      </footer>

      <style>{`
        .home-page {
          min-height: 100vh;
          display: flex;
          flex-direction: column;
          background: var(--qt-bg);
        }

        .home-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 1rem 2rem;
          border-bottom: 1px solid var(--qt-border);
        }

        .home-header .brand {
          display: flex;
          align-items: center;
          gap: 0.5rem;
          font-weight: 700;
          font-size: 1.125rem;
          color: var(--qt-fg);
        }

        .home-header .brand svg {
          width: 24px;
          height: 24px;
          color: var(--qt-info);
        }

        .home-main {
          flex: 1;
          max-width: 1200px;
          margin: 0 auto;
          padding: 2rem;
        }

        .hero {
          text-align: center;
          padding: 4rem 0;
        }

        .hero h1 {
          font-size: 3rem;
          font-weight: 800;
          color: var(--qt-fg);
          margin-bottom: 1rem;
        }

        .hero-subtitle {
          font-size: 1.25rem;
          color: var(--qt-fg-muted);
          max-width: 600px;
          margin: 0 auto 2rem;
          line-height: 1.6;
        }

        .features {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
          gap: 2rem;
          margin: 4rem 0;
        }

        .feature-card {
          background: var(--qt-bg-card);
          border: 1px solid var(--qt-border);
          border-radius: var(--qt-radius-lg);
          padding: 2rem;
          text-align: center;
        }

        .feature-icon {
          width: 48px;
          height: 48px;
          background: var(--qt-info-bg);
          border-radius: var(--qt-radius);
          display: flex;
          align-items: center;
          justify-content: center;
          margin: 0 auto 1rem;
        }

        .feature-icon svg {
          width: 24px;
          height: 24px;
          color: var(--qt-info);
        }

        .feature-card h3 {
          font-size: 1.25rem;
          font-weight: 600;
          color: var(--qt-fg);
          margin-bottom: 0.5rem;
        }

        .feature-card p {
          color: var(--qt-fg-muted);
          line-height: 1.6;
        }

        .cta {
          text-align: center;
          padding: 4rem 2rem;
          background: var(--qt-bg-alt);
          border-radius: var(--qt-radius-lg);
          margin: 2rem 0;
        }

        .cta h2 {
          font-size: 2rem;
          font-weight: 700;
          color: var(--qt-fg);
          margin-bottom: 0.5rem;
        }

        .cta p {
          color: var(--qt-fg-muted);
          margin-bottom: 1.5rem;
        }

        .home-footer {
          text-align: center;
          padding: 2rem;
          border-top: 1px solid var(--qt-border);
          color: var(--qt-fg-muted);
          font-size: 0.875rem;
        }

        .btn-lg {
          padding: 0.75rem 2rem;
          font-size: 1rem;
        }
      `}</style>
    </div>
  )
}
