import { useEffect, useRef, useState } from 'react'
import { Icon } from './icons.jsx'

export const STATUSES = ['listed', 'matched', 'accepted', 'picked_up', 'delivered']
/** Donation status colours: a one-hue ordinal ramp (light = early, dark = delivered) plus the
 *  reserved error red for expired. Mirrors the --status-* tokens in index.css; Leaflet needs hex. */
export const STATUS_COLOR = {
  listed: '#80bc9d', matched: '#67a385', accepted: '#4f8a6d', picked_up: '#377257', delivered: '#245c43', expired: '#c94c4c',
}
export const label = (s) => (s || '').replace(/_/g, ' ')
/** label() with a capital first letter, for badges, legends and table cells */
export const humanize = (s) => {
  const t = label(s)
  return t.charAt(0).toUpperCase() + t.slice(1)
}

/** One badge vocabulary for donation, match and request statuses */
const STATUS_TONE = {
  delivered: 'success', accepted: 'success', fulfilled: 'success',
  expired: 'error', declined: 'outline',
}

export function Badge({ status, tone, icon, className = '', children }) {
  const t = tone || STATUS_TONE[status] || 'neutral'
  const cls = ['badge', `badge-${t}`, status && `s-${status}`, className].filter(Boolean).join(' ')
  return (
    <span className={cls}>
      {icon ? <Icon name={icon} size={14} /> : <span className="dot" aria-hidden="true" />}
      {children ?? humanize(status)}
    </span>
  )
}

/** 5-step progress for the listed -> delivered flow */
export function Steps({ status }) {
  const idx = STATUSES.indexOf(status)
  if (status === 'expired') return <Badge status="expired" />
  return (
    <span className="steps" title={label(status)}>
      <span className="step-track" aria-hidden="true">
        {STATUSES.map((s, i) => (
          <span key={s} className={`step s-${s}${i <= idx ? ' on' : ''}`} />
        ))}
      </span>
      <span className="step-label">
        <span className="visually-hidden">Status: </span>
        {humanize(status)}
        {idx >= 0 && <span className="visually-hidden"> (step {idx + 1} of {STATUSES.length})</span>}
      </span>
    </span>
  )
}

export const fmtTime = (iso) =>
  new Date(iso).toLocaleString('en-IN', { hour: '2-digit', minute: '2-digit', day: 'numeric', month: 'short' })

export function timeLeft(iso) {
  const ms = new Date(iso) - Date.now()
  if (ms <= 0) return 'expired'
  const h = Math.floor(ms / 3.6e6), m = Math.floor((ms % 3.6e6) / 6e4)
  return h ? `${h}h ${m}m left` : `${m}m left`
}

/** Expiry countdown: amber under an hour, red once expired. `quiet` keeps it plain (finished donations). */
export function TimeLeft({ iso, quiet = false }) {
  const text = timeLeft(iso)
  const ms = new Date(iso) - Date.now()
  const tone = quiet ? null : ms <= 0 ? 'error' : ms < 3.6e6 ? 'warning' : null
  if (tone) return <Badge tone={tone} icon="clock">{text}</Badge>
  return (
    <span className="time-left">
      <Icon name="clock" size={14} />
      {text}
    </span>
  )
}

/** Poll an async loader every `ms`; returns [data, error, reload] */
export function usePolling(loader, ms = 4000, deps = []) {
  const [data, setData] = useState(null)
  const [error, setError] = useState('')
  const ref = useRef(loader)
  ref.current = loader
  const load = async () => {
    try { setData(await ref.current()); setError('') } catch (e) { setError(e.message) }
  }
  useEffect(() => {
    load()
    const t = setInterval(load, ms)
    return () => clearInterval(t)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)
  return [data, error, load]
}

export function ScoreBar({ m }) {
  return (
    <div className="scorebar" title={`distance ${m.distance_score} · urgency ${m.urgency_score} · fit ${m.fit_score}`}>
      <span className="scorebar-track" aria-hidden="true">
        <span style={{ width: `${Math.round(m.score * 100)}%` }} />
      </span>
      <span className="visually-hidden">Match score </span>
      <b className="scorebar-value">{m.score.toFixed(2)}</b>
    </div>
  )
}

/** Page title block with an optional subtitle and right-aligned actions */
export function PageHeader({ title, subtitle, actions }) {
  return (
    <header className="page-header">
      <div className="page-heading">
        <h1>{title}</h1>
        {subtitle && <p className="page-subtitle">{subtitle}</p>}
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </header>
  )
}

/** Inline feedback message; pass role="alert" where the message should be announced */
export function Alert({ tone = 'error', role, children }) {
  return (
    <div className={`alert alert-${tone}`} role={role}>
      <Icon name={tone === 'success' ? 'check-circle' : 'alert-circle'} size={18} />
      <div className="alert-body">{children}</div>
    </div>
  )
}

export function EmptyState({ icon = 'inbox', title, children }) {
  return (
    <div className="empty-state">
      <span className="empty-icon" aria-hidden="true"><Icon name={icon} size={22} /></span>
      {title && <p className="empty-title">{title}</p>}
      {children && <p className="empty-text">{children}</p>}
    </div>
  )
}

export function Loading({ label: text = 'Loading…' }) {
  return (
    <div className="loading" role="status">
      <span className="spinner" aria-hidden="true" />
      {text}
    </div>
  )
}

export const CATEGORIES = [
  ['cooked_meals', 'Cooked meals'], ['bakery', 'Bakery'], ['produce', 'Produce'],
  ['dairy', 'Dairy'], ['packaged', 'Packaged'], ['beverages', 'Beverages'],
]
export const AREAS = ['Nerul', 'Vashi', 'Belapur', 'Kharghar', 'Sanpada', 'Airoli', 'Seawoods']
