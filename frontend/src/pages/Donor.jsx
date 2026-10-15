import { useId, useState } from 'react'
import { api } from '../api.js'
import {
  AREAS, Alert, Badge, CATEGORIES, EmptyState, Loading, PageHeader, ScoreBar, Steps, TimeLeft, fmtTime, label, usePolling,
} from '../components/bits.jsx'
import { Icon } from '../components/icons.jsx'

const localInput = (d) => {
  const p = (n) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`
}
const blank = () => ({ food_type: '', category: 'cooked_meals', quantity_kg: '', area: 'Vashi',
  expiry: localInput(new Date(Date.now() + 4 * 3600e3)) })

export default function Donor() {
  const uid = useId()
  const [form, setForm] = useState(blank())
  const [msg, setMsg] = useState(null)
  const [busy, setBusy] = useState(false)
  const [open, setOpen] = useState(null)
  const [matches, setMatches] = useState([])
  const [rows, err, reload] = usePolling(() => api('/donations/mine'), 4000)
  const set = (k) => (e) => setForm({ ...form, [k]: e.target.value })

  const submit = async (e) => {
    e.preventDefault()
    setMsg(null)
    setBusy(true)
    try {
      await api('/donations', { method: 'POST', body: {
        food_type: form.food_type, category: form.category, quantity_kg: Number(form.quantity_kg),
        area: form.area, expiry_time: new Date(form.expiry).toISOString() } })
      setMsg({ ok: true, text: 'Donation listed. Matching NGOs are being found…' })
      setForm(blank())
      reload()
    } catch (ex) {
      setMsg({ ok: false, text: ex.message })
    } finally {
      setBusy(false)
    }
  }

  const toggle = async (id) => {
    if (open === id) return setOpen(null)
    setOpen(id)
    setMatches([])
    try { setMatches(await api(`/donations/${id}/matches`)) } catch { setMatches([]) }
  }

  return (
    <>
      <PageHeader title="Donor dashboard" subtitle="List surplus food and follow each donation from match to delivery." />

      <div className="grid-split">
        <section className="card" aria-labelledby={`${uid}-donate`}>
          <div className="card-header">
            <div>
              <h2 id={`${uid}-donate`} className="card-title">Donate surplus food</h2>
              <p className="card-subtitle">NGOs within 15 km are ranked for it as soon as it is listed.</p>
            </div>
          </div>
          <form onSubmit={submit} className="form">
            <div className="form-group">
              <label className="form-label" htmlFor={`${uid}-food`}>What food?</label>
              <input id={`${uid}-food`} className="input" value={form.food_type} onChange={set('food_type')}
                placeholder="e.g. Veg biryani" required minLength={2} />
            </div>
            <div className="form-row">
              <div className="form-group">
                <label className="form-label" htmlFor={`${uid}-category`}>Category</label>
                <select id={`${uid}-category`} className="select" value={form.category} onChange={set('category')}>
                  {CATEGORIES.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
                </select>
              </div>
              <div className="form-group">
                <label className="form-label" htmlFor={`${uid}-qty`}>Quantity (kg)</label>
                <input id={`${uid}-qty`} className="input" type="number" inputMode="decimal" min="0.5" max="5000" step="0.5"
                  value={form.quantity_kg} onChange={set('quantity_kg')} required />
              </div>
            </div>
            <div className="form-row">
              <div className="form-group">
                <label className="form-label" htmlFor={`${uid}-area`}>Area</label>
                <select id={`${uid}-area`} className="select" value={form.area} onChange={set('area')}>
                  {AREAS.map((a) => <option key={a}>{a}</option>)}
                </select>
              </div>
              <div className="form-group">
                <label className="form-label" htmlFor={`${uid}-expiry`}>Use before</label>
                <input id={`${uid}-expiry`} className="input" type="datetime-local" value={form.expiry}
                  onChange={set('expiry')} required />
              </div>
            </div>
            {msg && <Alert tone={msg.ok ? 'success' : 'error'} role="alert">{msg.text}</Alert>}
            <button className="btn btn-primary btn-lg btn-block" disabled={busy} aria-busy={busy}>
              {busy ? 'Listing…' : 'List donation'}
            </button>
          </form>
        </section>

        <section className="card" aria-labelledby={`${uid}-mine`}>
          <div className="card-header">
            <div>
              <h2 id={`${uid}-mine`} className="card-title">
                My donations {rows && <span className="count">{rows.length}</span>}
              </h2>
              <p className="card-subtitle">Select a donation to see the NGOs it was matched with.</p>
            </div>
          </div>
          {err && <Alert tone="error">{err}</Alert>}
          {!rows && !err && <Loading label="Loading your donations…" />}
          {rows && rows.length === 0 && (
            <EmptyState icon="package" title="No donations yet">List your first donation using the form.</EmptyState>
          )}
          {rows && rows.length > 0 && (
            <ul className="list">
              {rows.map((d) => {
                const isOpen = open === d.id
                const finished = d.status === 'delivered' || d.status === 'expired'
                return (
                  <li key={d.id} className={`item${isOpen ? ' is-open' : ''}`}>
                    <button type="button" className="item-head item-toggle" onClick={() => toggle(d.id)}
                      aria-expanded={isOpen} aria-controls={`${uid}-matches-${d.id}`}>
                      <span className="item-main">
                        <span className="item-title">{d.food_type}</span>
                        <span className="item-meta">{d.quantity_kg} kg · {label(d.category)} · {d.area}</span>
                        <span className="item-sub">
                          Use before {fmtTime(d.expiry_time)}
                          <TimeLeft iso={d.expiry_time} quiet={finished} />
                        </span>
                        {d.accepted_ngo_name && (
                          <span className="item-note is-success">
                            <Icon name="check-circle" size={16} />
                            <span>Accepted by <b>{d.accepted_ngo_name}</b></span>
                          </span>
                        )}
                        {d.status === 'listed' && (
                          <span className="item-note">
                            <span className="spinner spinner-sm" aria-hidden="true" />
                            Finding NGOs within 15 km…
                          </span>
                        )}
                        {!d.accepted_ngo_name && d.match_count > 0 && (
                          <span className="item-note">
                            {d.match_count} NGOs matched · {isOpen ? 'Hide' : 'View'} ranking
                          </span>
                        )}
                      </span>
                      <span className="item-side">
                        <Steps status={d.status} />
                        <Icon name="chevron-down" size={18} className="chevron" />
                      </span>
                    </button>
                    {isOpen && (
                      <div className="item-detail" id={`${uid}-matches-${d.id}`}>
                        <div className="table-wrap">
                          <table className="table table-compact table-medium">
                            <thead>
                              <tr>
                                <th scope="col" className="num">#</th>
                                <th scope="col">NGO</th>
                                <th scope="col" className="num">Distance</th>
                                <th scope="col">Score</th>
                                <th scope="col">Status</th>
                              </tr>
                            </thead>
                            <tbody>
                              {matches.map((m) => (
                                <tr key={m.id}>
                                  <td className="num">{m.rank}</td>
                                  <td>{m.ngo_name}</td>
                                  <td className="num">{m.distance_km} km</td>
                                  <td><ScoreBar m={m} /></td>
                                  <td><Badge status={m.status} /></td>
                                </tr>
                              ))}
                              {matches.length === 0 && (
                                <tr><td colSpan="5" className="table-empty">No matches yet</td></tr>
                              )}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </li>
                )
              })}
            </ul>
          )}
        </section>
      </div>
    </>
  )
}
