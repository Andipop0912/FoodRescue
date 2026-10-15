import { useId, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { homeFor, useAuth } from '../auth.jsx'
import { Alert } from '../components/bits.jsx'
import { Icon, Logo } from '../components/icons.jsx'

const DEMOS = [
  ['Donor', 'donor@foodrescue.demo'],
  ['NGO', 'ngo@foodrescue.demo'],
  ['Admin', 'admin@foodrescue.demo'],
]
const DEMO_ICON = { Donor: 'store', NGO: 'users', Admin: 'shield' }

const POINTS = [
  ['package', 'List surplus in a minute', 'Donors post what is left over, how much, and when it must be used.'],
  ['map-pin', 'Ranked NGO matches', 'Nearby NGOs are ranked by distance, urgency and quantity fit.'],
  ['truck', 'Tracked to delivery', 'Every donation is followed from match to pickup to delivery.'],
]

export default function Login() {
  const { login } = useAuth()
  const nav = useNavigate()
  const uid = useId()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [err, setErr] = useState('')
  const [busy, setBusy] = useState(false)

  const submit = async (e) => {
    e.preventDefault()
    setErr('')
    setBusy(true)
    try {
      const a = await login(email, password)
      nav(homeFor(a.role), { replace: true })
    } catch (ex) {
      setErr(ex.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="auth-page">
      <div className="auth-aside">
        <div className="auth-brand">
          <Logo size={44} inverted />
          <span className="brand-text">
            <span className="brand-name">FoodRescue</span>
            <span className="brand-region">Navi Mumbai</span>
          </span>
        </div>
        <div className="auth-pitch">
          <p className="auth-headline">Rescue surplus food before it goes to waste.</p>
          <p className="auth-tagline">Matching surplus food with NGOs across Navi Mumbai</p>
          <ul className="auth-points">
            {POINTS.map(([icon, title, text]) => (
              <li key={title}>
                <span className="auth-point-icon" aria-hidden="true"><Icon name={icon} size={20} /></span>
                <span className="auth-point-body">
                  <span className="auth-point-title">{title}</span>
                  <span className="auth-point-text">{text}</span>
                </span>
              </li>
            ))}
          </ul>
        </div>
        <p className="auth-foot">For restaurants, cafes, supermarkets, bakeries and the NGOs they feed.</p>
      </div>

      <main className="auth-main">
        <form className="card auth-card" onSubmit={submit}>
          <div className="auth-card-head">
            <h1>Sign in</h1>
            <p className="muted">Welcome back. Sign in to your FoodRescue account.</p>
          </div>
          <div className="form-group">
            <label className="form-label" htmlFor={`${uid}-email`}>Email</label>
            <input id={`${uid}-email`} className="input" type="email" autoComplete="username" value={email}
              onChange={(e) => setEmail(e.target.value)} required autoFocus />
          </div>
          <div className="form-group">
            <label className="form-label" htmlFor={`${uid}-password`}>Password</label>
            <input id={`${uid}-password`} className="input" type="password" autoComplete="current-password" value={password}
              onChange={(e) => setPassword(e.target.value)} required />
          </div>
          {err && <Alert tone="error" role="alert">{err}</Alert>}
          <button className="btn btn-primary btn-lg btn-block" disabled={busy} aria-busy={busy}>
            {busy ? 'Signing in…' : 'Sign in'}
          </button>
          <div className="demo-accounts">
            <div className="divider">Demo accounts</div>
            <p className="muted small">Pick a role to fill in the form (password <code>demo123</code>).</p>
            <div className="demo-grid">
              {DEMOS.map(([name, mail]) => (
                <button type="button" key={mail} className="demo-account" title={mail}
                  onClick={() => { setEmail(mail); setPassword('demo123') }}>
                  <Icon name={DEMO_ICON[name]} size={20} />
                  {name}
                </button>
              ))}
            </div>
          </div>
        </form>
      </main>
    </div>
  )
}
