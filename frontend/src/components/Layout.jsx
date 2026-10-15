import { Link, NavLink } from 'react-router-dom'
import { homeFor, useAuth } from '../auth.jsx'
import { Icon, Logo } from './icons.jsx'

const ROLE_LABEL = { donor: 'Donor', ngo: 'NGO', admin: 'Admin' }

/** Each role has its own workspace; new pages for a role get added to its list. */
const NAV = {
  donor: [{ to: '/donor', label: 'Donations', icon: 'package' }],
  ngo: [{ to: '/ngo', label: 'Matches & pickups', icon: 'truck' }],
  admin: [{ to: '/admin', label: 'Live demo', icon: 'activity' }],
}

const initials = (name) =>
  (name || '?').split(/\s+/).filter(Boolean).slice(0, 2).map((w) => w[0]).join('').toUpperCase()

export default function Layout({ children }) {
  const { auth, logout } = useAuth()
  return (
    <div className="app-shell">
      <a className="skip-link" href="#main">Skip to content</a>
      <header className="topbar">
        <div className="topbar-inner">
          <Link to={homeFor(auth.role)} className="brand">
            <Logo size={36} inverted />
            <span className="brand-text">
              <span className="brand-name">FoodRescue</span>
              <span className="brand-region">Navi Mumbai</span>
            </span>
          </Link>
          <nav className="nav" aria-label="Main">
            {(NAV[auth.role] || []).map((item) => (
              <NavLink key={item.to} to={item.to} className="nav-link">
                <Icon name={item.icon} size={18} />
                {item.label}
              </NavLink>
            ))}
          </nav>
          <div className="user-menu">
            <span className="avatar" aria-hidden="true" title={auth.name}>{initials(auth.name)}</span>
            <span className="user-meta">
              <span className="user-name">{auth.name}</span>
              <span className="user-role">{ROLE_LABEL[auth.role]}</span>
            </span>
            <button type="button" className="btn btn-sm btn-on-dark" onClick={logout}>
              <Icon name="log-out" size={16} />
              Sign out
            </button>
          </div>
        </div>
      </header>
      <main id="main" className="page-content" tabIndex={-1}>{children}</main>
    </div>
  )
}
