/**
 * SpeedupCard Component
 * Large speedup number display with status badge
 */

import './SpeedupCard.css'

interface SpeedupCardProps {
  speedup: number
  status: string
  transforms?: string[]
}

function getSpeedupClass(status: string): string {
  switch (status) {
    case 'WIN':
    case 'IMPROVED':
      return 'win'
    case 'NEUTRAL':
      return 'neutral'
    case 'REGRESSION':
    case 'ERROR':
      return 'regression'
    default:
      return 'neutral'
  }
}

function formatSpeedup(speedup: number): string {
  if (speedup >= 10) return speedup.toFixed(0) + 'x'
  if (speedup >= 1) return speedup.toFixed(1) + 'x'
  return speedup.toFixed(2) + 'x'
}

export default function SpeedupCard({ speedup, status, transforms }: SpeedupCardProps) {
  const cls = getSpeedupClass(status)

  return (
    <div className={`speedup-card ${cls}`}>
      <div className="speedup-card-main">
        <span className="speedup-card-number">{formatSpeedup(speedup)}</span>
        <span className="speedup-card-label">
          {speedup >= 1 ? 'faster' : 'slower'}
        </span>
      </div>
      <span className={`speedup-card-badge ${cls}`}>{status}</span>
      {transforms && transforms.length > 0 && (
        <div className="speedup-card-transforms">
          {transforms.map((t, i) => (
            <span key={i} className="speedup-card-transform">{t}</span>
          ))}
        </div>
      )}
    </div>
  )
}
