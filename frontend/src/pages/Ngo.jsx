import { useId, useState } from 'react'
import { api } from '../api.js'
import {
  Alert, Badge, CATEGORIES, EmptyState, Loading, PageHeader, ScoreBar, Steps, TimeLeft, fmtTime, humanize, label, usePolling,
} from '../components/bits.jsx'
import { Icon } from '../components/icons.jsx'

export default function Ngo() {
  const uid = useId()
  const [matches, merr, reloadM] = usePolling(() => api('/ngo/matches'), 4000)
  const [pickups, perr, reloadP] = usePolling(() => api('/ngo/pickups'), 4000)
  const [reqs, rerr, reloadR] = usePolling(() => api('/ngo/requests'), 6000)
  const [msg, setMsg] = useState(null)
  const [req, setReq] = useState({ category: '', quantity_kg: '', hours: 4 })

  const act = async (fn, okText) => {
    setMsg(null)
    try { await fn(); setMsg({ ok: true, text: okText }) } catch (e) { setMsg({ ok: false, text: e.message }) }
    reloadM(); reloadP(); reloadR()
  }
  const accept = (m) => act(() => api(`/matches/${m.id}/accept`, { method: 'POST' }), `Accepted donation #${m.donation.id}. Arrange pickup.`)
  const move = (d, status) => act(() => api(`/donations/${d.id}/status`, { method: 'PATCH', body: { status } }), `Donation #${d.id} marked ${label(status)}.`)
  const postReq = (e) => {
    e.preventDefault()
    act(async () => {
      await api('/ngo/requests', { method: 'POST', body: {
        category: req.category || null, quantity_kg: Number(req.quantity_kg),
        needed_by: new Date(Date.now() + req.hours * 3600e3).toISOString() } })
      setReq({ category: '', quantity_kg: '', hours: 4 })
    }, 'Need posted. Donations will be matched against it.')
  }

  return (
    <>
      <PageHeader title="NGO dashboard" subtitle="Accept ranked donations near you and track each pickup to delivery." />
      {msg && <Alert tone={msg.ok ? 'success' : 'error'} role="alert">{msg.text}</Alert>}

      <div className="grid-split reverse">
        <section className="card" aria-labelledby={`${uid}-matches`}>
          <div className="card-header">
            <div>
              <h2 id={`${uid}-matches`} className="card-title">
                Ranked matches {matches && <span className="count">{matches.length}</span>}
              </h2>
              <p className="card-subtitle">Ranked by distance, urgency and quantity fit.</p>
            </div>
          </div>
          {merr && <Alert tone="error">{merr}</Alert>}
          {!matches && !merr && <Loading label="Loading matches…" />}
          {matches && matches.length === 0 && (
            <EmptyState icon="inbox" title="No open matches right now">New donations appear here automatically.</EmptyState>
          )}
          {matches && matches.length > 0 && (
            <ul className="list">
              {matches.map((m) => (
                <li key={m.id} className="item">
                  <div className="item-head">
                    <div className="item-main">
                      <span className="item-title">{m.donation.food_type}</span>
                      <span className="item-meta">{m.donation.quantity_kg} kg · {label(m.donation.category)}</span>
                      <span className="item-sub with-icon">
                        <Icon name="map-pin" size={14} />
                        <span>{m.donation.donor_name} · {m.donation.area} · {m.distance_km} km away</span>
                      </span>
                    </div>
                    <ScoreBar m={m} />
                  </div>
                  <div className="item-body">
                    <div className="chips">
                      <span className="chip">Distance <b>{m.distance_score.toFixed(2)}</b></span>
                      <span className="chip">Urgency <b>{m.urgency_score.toFixed(2)}</b></span>
                      <span className="chip">Fit <b>{m.fit_score.toFixed(2)}</b></span>
                      <span className="chip">Rank <b>#{m.rank}</b></span>
                    </div>
                    <div className="item-actions">
                      <TimeLeft iso={m.donation.expiry_time} />
                      <button type="button" className="btn btn-primary btn-sm" onClick={() => accept(m)}>
                        <Icon name="check" size={16} />
                        Accept donation
                      </button>
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </section>

        <div className="stack">
          <section className="card" aria-labelledby={`${uid}-pickups`}>
            <div className="card-header">
              <div>
                <h2 id={`${uid}-pickups`} className="card-title">My pickups</h2>
                <p className="card-subtitle">Mark each accepted donation as it is picked up and delivered.</p>
              </div>
            </div>
            {perr && <Alert tone="error">{perr}</Alert>}
            {!pickups && !perr && <Loading label="Loading pickups…" />}
            {pickups && pickups.length === 0 && (
              <EmptyState icon="truck" title="No pickups yet">Accepted donations will be tracked here.</EmptyState>
            )}
            {pickups && pickups.length > 0 && (
              <ul className="list">
                {pickups.slice(0, 12).map((d) => (
                  <li key={d.id} className="item">
                    <div className="item-head">
                      <div className="item-main">
                        <span className="item-title">{d.food_type}</span>
                        <span className="item-meta">{d.quantity_kg} kg · {d.donor_name} ({d.area})</span>
                      </div>
                      <Steps status={d.status} />
                    </div>
                    {['accepted', 'picked_up', 'delivered'].includes(d.status) && (
                      <div className="item-body">
                        <div className="item-actions">
                          {d.status === 'accepted' && (
                            <button type="button" className="btn btn-secondary btn-sm" onClick={() => move(d, 'picked_up')}>
                              <Icon name="truck" size={16} />
                              Mark picked up
                            </button>
                          )}
                          {d.status === 'picked_up' && (
                            <button type="button" className="btn btn-primary btn-sm" onClick={() => move(d, 'delivered')}>
                              <Icon name="check" size={16} />
                              Mark delivered
                            </button>
                          )}
                          {d.status === 'delivered' && (
                            <span className="item-note is-success">
                              <Icon name="check-circle" size={16} />
                              Delivered {fmtTime(d.status_changed_at)}
                            </span>
                          )}
                        </div>
                      </div>
                    )}
                  </li>
                ))}
              </ul>
            )}
          </section>

          <section className="card" aria-labelledby={`${uid}-need`}>
            <div className="card-header">
              <div>
                <h2 id={`${uid}-need`} className="card-title">Post a need</h2>
                <p className="card-subtitle">Tell donors what you can use. New donations are matched against it.</p>
              </div>
            </div>
            <form className="form" onSubmit={postReq}>
              <div className="form-row cols-3">
                <div className="form-group">
                  <label className="form-label" htmlFor={`${uid}-category`}>Category</label>
                  <select id={`${uid}-category`} className="select" value={req.category}
                    onChange={(e) => setReq({ ...req, category: e.target.value })}>
                    <option value="">Any</option>
                    {CATEGORIES.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
                  </select>
                </div>
                <div className="form-group">
                  <label className="form-label" htmlFor={`${uid}-qty`}>Quantity (kg)</label>
                  <input id={`${uid}-qty`} className="input" type="number" inputMode="decimal" min="1" max="5000"
                    value={req.quantity_kg} required onChange={(e) => setReq({ ...req, quantity_kg: e.target.value })} />
                </div>
                <div className="form-group">
                  <label className="form-label" htmlFor={`${uid}-hours`}>Needed within</label>
                  <select id={`${uid}-hours`} className="select" value={req.hours}
                    onChange={(e) => setReq({ ...req, hours: Number(e.target.value) })}>
                    {[2, 4, 8, 24].map((h) => <option key={h} value={h}>{h} hours</option>)}
                  </select>
                </div>
              </div>
              <div className="form-actions">
                <button className="btn btn-secondary">
                  <Icon name="plus" size={18} />
                  Post need
                </button>
              </div>
            </form>
            {rerr && <div className="card-section"><Alert tone="error">{rerr}</Alert></div>}
            {reqs && reqs.length > 0 && (
              <div className="card-section">
                <h3>Your needs</h3>
                <div className="table-wrap">
                  <table className="table table-compact">
                    <thead>
                      <tr>
                        <th scope="col">Category</th>
                        <th scope="col" className="num">Needed</th>
                        <th scope="col">Filled</th>
                        <th scope="col">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {reqs.slice(0, 6).map((r) => {
                        const pct = r.quantity_kg ? Math.min(100, Math.round((r.fulfilled_kg / r.quantity_kg) * 100)) : 0
                        return (
                          <tr key={r.id}>
                            <td>
                              {r.category ? humanize(r.category) : 'Any'}
                              {r.standing && <> <Badge tone="outline">Standing</Badge></>}
                            </td>
                            <td className="num">{r.quantity_kg} kg</td>
                            <td>
                              <span className="fill">
                                <span className="meter" aria-hidden="true"><span style={{ width: `${pct}%` }} /></span>
                                {r.fulfilled_kg} kg
                              </span>
                            </td>
                            <td><Badge status={r.status} /></td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </section>
        </div>
      </div>
    </>
  )
}
