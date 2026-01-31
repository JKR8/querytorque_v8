import { useNavigate } from 'react-router-dom'

export default function HomePage() {
  const navigate = useNavigate()

  return (
    <div className="home-page">
      <section className="hero">
        <h1>Power BI Performance Analysis</h1>
        <p className="hero-subtitle">
          Optimize your Power BI models and DAX code with AI-powered analysis.
          Get actionable recommendations to improve query performance and reduce costs.
        </p>
        <div className="hero-actions">
          <button className="btn btn-primary btn-lg" onClick={() => navigate('/analyze')}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="20" height="20">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
              <polyline points="17,8 12,3 7,8" />
              <line x1="12" y1="3" x2="12" y2="15" />
            </svg>
            Analyze VPAX File
          </button>
          <button className="btn btn-secondary btn-lg" onClick={() => navigate('/model-browser')}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="20" height="20">
              <rect x="3" y="3" width="7" height="7" />
              <rect x="14" y="3" width="7" height="7" />
              <rect x="14" y="14" width="7" height="7" />
              <rect x="3" y="14" width="7" height="7" />
            </svg>
            Browse Model
          </button>
        </div>
      </section>

      <section className="features">
        <h2>What We Analyze</h2>
        <div className="feature-grid">
          <div className="feature-card">
            <div className="feature-icon feature-icon-dax">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="24" height="24">
                <polyline points="16,18 22,12 16,6" />
                <polyline points="8,6 2,12 8,18" />
              </svg>
            </div>
            <h3>DAX Performance</h3>
            <p>
              Detect slow patterns like FILTER instead of CALCULATETABLE,
              excessive SUMX iterations, and missing variables.
            </p>
            <ul className="feature-list">
              <li>25 DAX anti-pattern rules</li>
              <li>Variable usage optimization</li>
              <li>Context transition detection</li>
            </ul>
          </div>

          <div className="feature-card">
            <div className="feature-icon feature-icon-model">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="24" height="24">
                <ellipse cx="12" cy="5" rx="9" ry="3" />
                <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
                <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
              </svg>
            </div>
            <h3>Model Structure</h3>
            <p>
              Analyze your model structure for high cardinality columns,
              missing relationships, and unused tables.
            </p>
            <ul className="feature-list">
              <li>15 model optimization rules</li>
              <li>Relationship analysis</li>
              <li>Storage mode recommendations</li>
            </ul>
          </div>

          <div className="feature-card">
            <div className="feature-icon feature-icon-calc">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="24" height="24">
                <rect x="4" y="4" width="16" height="16" rx="2" ry="2" />
                <line x1="4" y1="10" x2="20" y2="10" />
                <line x1="10" y1="4" x2="10" y2="20" />
              </svg>
            </div>
            <h3>Calculation Groups</h3>
            <p>
              Review calculation group design for proper precedence,
              format string handling, and best practices.
            </p>
            <ul className="feature-list">
              <li>5 calc group rules</li>
              <li>Precedence validation</li>
              <li>Format string checks</li>
            </ul>
          </div>
        </div>
      </section>

      <section className="workflow">
        <h2>How It Works</h2>
        <div className="workflow-steps">
          <div className="workflow-step">
            <div className="step-number">1</div>
            <h3>Export VPAX</h3>
            <p>Use DAX Studio or Tabular Editor to export your Power BI model as a VPAX file.</p>
          </div>
          <div className="workflow-arrow">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="24" height="24">
              <line x1="5" y1="12" x2="19" y2="12" />
              <polyline points="12,5 19,12 12,19" />
            </svg>
          </div>
          <div className="workflow-step">
            <div className="step-number">2</div>
            <h3>Upload & Analyze</h3>
            <p>Upload your VPAX file to get a comprehensive performance audit report.</p>
          </div>
          <div className="workflow-arrow">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="24" height="24">
              <line x1="5" y1="12" x2="19" y2="12" />
              <polyline points="12,5 19,12 12,19" />
            </svg>
          </div>
          <div className="workflow-step">
            <div className="step-number">3</div>
            <h3>Optimize</h3>
            <p>Use AI-powered optimization to rewrite slow DAX measures automatically.</p>
          </div>
        </div>
      </section>

      <style>{`
        .home-page {
          padding: var(--qt-space-lg);
          max-width: 1200px;
          margin: 0 auto;
        }

        .hero {
          text-align: center;
          padding: var(--qt-space-2xl) 0;
        }

        .hero h1 {
          font-size: var(--qt-text-3xl);
          font-weight: var(--qt-font-bold);
          color: var(--qt-fg);
          margin-bottom: var(--qt-space-md);
        }

        .hero-subtitle {
          font-size: var(--qt-text-lg);
          color: var(--qt-fg-muted);
          max-width: 600px;
          margin: 0 auto var(--qt-space-xl);
          line-height: var(--qt-leading-relaxed);
        }

        .hero-actions {
          display: flex;
          gap: var(--qt-space-md);
          justify-content: center;
        }

        .features {
          padding: var(--qt-space-2xl) 0;
        }

        .features h2 {
          text-align: center;
          font-size: var(--qt-text-2xl);
          font-weight: var(--qt-font-semibold);
          margin-bottom: var(--qt-space-xl);
        }

        .feature-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
          gap: var(--qt-space-lg);
        }

        .feature-card {
          background: var(--qt-bg-card);
          border: 1px solid var(--qt-border);
          border-radius: var(--qt-radius-lg);
          padding: var(--qt-space-lg);
          transition: box-shadow var(--qt-transition-normal);
        }

        .feature-card:hover {
          box-shadow: var(--qt-shadow-md);
        }

        .feature-icon {
          width: 48px;
          height: 48px;
          border-radius: var(--qt-radius);
          display: flex;
          align-items: center;
          justify-content: center;
          margin-bottom: var(--qt-space-md);
        }

        .feature-icon-dax {
          background: var(--qt-tier-1-bg);
          color: var(--qt-tier-1);
        }

        .feature-icon-model {
          background: var(--qt-info-bg);
          color: var(--qt-info);
        }

        .feature-icon-calc {
          background: var(--qt-medium-bg);
          color: var(--qt-medium);
        }

        .feature-card h3 {
          font-size: var(--qt-text-lg);
          font-weight: var(--qt-font-semibold);
          margin-bottom: var(--qt-space-sm);
        }

        .feature-card p {
          color: var(--qt-fg-muted);
          font-size: var(--qt-text-md);
          line-height: var(--qt-leading-relaxed);
          margin-bottom: var(--qt-space-md);
        }

        .feature-list {
          list-style: none;
          padding: 0;
          margin: 0;
        }

        .feature-list li {
          font-size: var(--qt-text-sm);
          color: var(--qt-fg-muted);
          padding: var(--qt-space-xs) 0;
          padding-left: 1.25rem;
          position: relative;
        }

        .feature-list li::before {
          content: "";
          position: absolute;
          left: 0;
          top: 50%;
          transform: translateY(-50%);
          width: 6px;
          height: 6px;
          border-radius: 50%;
          background: var(--qt-low);
        }

        .workflow {
          padding: var(--qt-space-2xl) 0;
          border-top: 1px solid var(--qt-border);
        }

        .workflow h2 {
          text-align: center;
          font-size: var(--qt-text-2xl);
          font-weight: var(--qt-font-semibold);
          margin-bottom: var(--qt-space-xl);
        }

        .workflow-steps {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--qt-space-md);
          flex-wrap: wrap;
        }

        .workflow-step {
          background: var(--qt-bg-card);
          border: 1px solid var(--qt-border);
          border-radius: var(--qt-radius-lg);
          padding: var(--qt-space-lg);
          text-align: center;
          max-width: 250px;
        }

        .step-number {
          width: 40px;
          height: 40px;
          border-radius: 50%;
          background: var(--qt-brand);
          color: white;
          display: flex;
          align-items: center;
          justify-content: center;
          font-weight: var(--qt-font-bold);
          font-size: var(--qt-text-lg);
          margin: 0 auto var(--qt-space-md);
        }

        .workflow-step h3 {
          font-size: var(--qt-text-md);
          font-weight: var(--qt-font-semibold);
          margin-bottom: var(--qt-space-sm);
        }

        .workflow-step p {
          font-size: var(--qt-text-sm);
          color: var(--qt-fg-muted);
          line-height: var(--qt-leading-relaxed);
        }

        .workflow-arrow {
          color: var(--qt-fg-muted);
        }

        @media (max-width: 768px) {
          .hero-actions {
            flex-direction: column;
            align-items: center;
          }

          .workflow-arrow {
            transform: rotate(90deg);
          }

          .workflow-steps {
            flex-direction: column;
          }
        }
      `}</style>
    </div>
  )
}
