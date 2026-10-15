import { useEffect, useRef, useState } from 'react'
import { api, loadAuth, wsUrl } from '../api.js'
import MapView from '../components/MapView.jsx'
import { Alert, Badge, PageHeader, STATUSES, humanize, label } from '../components/bits.jsx'
import { Icon } from '../components/icons.jsx'

const EMPTY = {
  demo: { state: 'idle', running: false, speed: 1, batch_no: 0, total_batches: 20, next_batch_in: 0 },
  stats: { batches: 0, accepted_rows: 0, rejected_rows: 0, matches: 0, kg_rescued: 0, meals: 0, kg_in_flight: 0, donations_by_status: {}, total_donations: 0 },
  pins: [], rejected: [], events: [],
}

/** WebSocket first; if it fails or drops, fall back to polling /admin/live and retry the socket. */
function useLive() {
  const [data, setData] = useState(EMPTY)
  const [events, setEvents] = useState([])
  const [mode, setMode] = useState('connecting')
  const lastId = useRef(0)

  useEffect(() => {
    let ws, poll, retry, closed = false
    const apply = (p) => {
      lastId.current = p.last_event_id
      setData(p)
      if (p.events.length) setEvents((old) => [...old, ...p.events].slice(-300))
    }
    const startPolling = () => {
      if (poll) return
      setMode('polling')
      const tick = async () => { try { apply(await api(`/admin/live?after=${lastId.current}`)) } catch { /* retry next tick */ } }
      tick()
      poll = setInterval(tick, 2000)
    }
    const connect = () => {
      if (closed) return
      try { ws = new WebSocket(wsUrl(loadAuth().access_token)) } catch { return startPolling() }
      ws.onopen = () => { setMode('websocket'); if (poll) { clearInterval(poll); poll = null } }
      ws.onmessage = (m) => apply(JSON.parse(m.data))
      ws.onclose = () => { if (closed) return; startPolling(); retry = setTimeout(connect, 8000) }
    }
    connect()
    return () => { closed = true; clearTimeout(retry); clearInterval(poll); ws && ws.close() }
  }, [])
  return { data, events, mode }
}

const LIVE_TEXT = { websocket: 'Live (WebSocket)', polling: 'Polling fallback', connecting: 'Connecting…' }

const LiveIndicator = ({ mode }) => (
  <span className={`live-indicator live-${mode}`} role="status">
    <span className="dot" aria-hidden="true" />
    {LIVE_TEXT[mode] || mode}
  </span>
)

const Counter = ({ title, value, hint, tone }) => (
  <div className={`stat${tone ? ` stat-${tone}` : ''}`}>
    <div className="stat-label">
      {tone && <span className="dot" aria-hidden="true" />}
      {title}
    </div>
    <div className="stat-value">{typeof value === 'number' ? value.toLocaleString('en-IN') : value}</div>
    {hint && <div className="stat-hint">{hint}</div>}
  </div>
)

export default function Admin() {
  const { data, events, mode } = useLive()
  const { demo, stats, pins, rejected } = data
  const [speed, setSpeed] = useState(1)
  const [hour, setHour] = useState('')
  const [reset, setReset] = useState(true)
  const [err, setErr] = useState('')
  const logRef = useRef(null)

  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight }, [events])
  useEffect(() => { if (demo.running) setSpeed(demo.speed) }, [demo.running, demo.speed])

  const call = async (fn) => { setErr(''); try { await fn() } catch (e) { setErr(e.message) } }
  const start = () => call(() => api('/demo/start', { method: 'POST', body: { speed, reset, start_hour: hour === '' ? null : Number(hour) } }))
  const stop = () => call(() => api('/demo/stop', { method: 'POST' }))
  const changeSpeed = (s) => { setSpeed(s); if (demo.running) call(() => api('/demo/speed', { method: 'POST', body: { speed: s } })) }
  const wipe = () => window.confirm('Delete all donations, matches, batches and events?') && call(() => api('/demo/reset', { method: 'POST' }))

  const pct = demo.total_batches ? Math.round((demo.batch_no / demo.total_batches) * 100) : 0
  const by = stats.donations_by_status || {}
  const flow = [...STATUSES, 'expired']
  const tracked = flow.reduce((n, s) => n + (by[s] || 0), 0)
  const [stateLabel, stateTone] = demo.running ? ['Running', 'success']
    : demo.state === 'finished' ? ['Finished', 'neutral']
      : demo.state === 'stopped' ? ['Stopped', 'warning'] : ['Ready', 'neutral']

  return (
    <>
      <PageHeader title="Admin dashboard" subtitle="Run the live demo and watch donations move from ingestion to delivery."
        actions={<LiveIndicator mode={mode} />} />

      <section className="card demo-card" aria-labelledby="demo-title">
        <div className="demo-main">
          <div className="demo-heading">
            <div className="demo-title">
              <h2 id="demo-title">Live demo</h2>
              <Badge tone={stateTone} className={demo.running ? 'is-live' : ''}>{stateLabel}</Badge>
            </div>
            <p className="muted small">
              {demo.running
                ? `Batch ${demo.batch_no}/${demo.total_batches} · next in ${Math.ceil(demo.next_batch_in)}s · ${demo.speed}x`
                : demo.state === 'finished' ? 'Finished: all batches sent'
                  : demo.state === 'stopped' ? 'Stopped' : 'Ready: 10 minutes, one batch every 30 s (20 batches)'}
            </p>
          </div>
          <div className="controls">
            <div className="segmented" role="group" aria-label="Speed">
              {[1, 2, 5].map((s) => (
                <button key={s} type="button" aria-pressed={speed === s} onClick={() => changeSpeed(s)}>{s}x</button>
              ))}
            </div>
            {!demo.running && (
              <select className="select select-sm" value={hour} onChange={(e) => setHour(e.target.value)}
                title="Surplus volume follows the time of day" aria-label="Time of day">
                <option value="">Real clock</option><option value="13">Lunch rush</option><option value="20">Dinner rush</option><option value="4">Quiet night</option>
              </select>
            )}
            {demo.running
              ? <button type="button" className="btn btn-danger" onClick={stop}><Icon name="stop" size={16} />Stop</button>
              : <button type="button" className="btn btn-primary" onClick={start}><Icon name="play" size={16} />Start Live Demo</button>}
          </div>
        </div>
        <div className="progress-row">
          <div className="progress" role="progressbar" aria-label="Batches sent" aria-valuemin={0} aria-valuemax={100} aria-valuenow={pct}>
            <span className="progress-fill" style={{ width: `${pct}%` }} />
          </div>
          <span className="progress-pct" aria-hidden="true">{pct}%</span>
        </div>
        <div className="demo-foot">
          <label className="checkbox">
            <input type="checkbox" checked={reset} onChange={(e) => setReset(e.target.checked)} disabled={demo.running} />
            <span>Clear previous data on start</span>
          </label>
          <span className="foot-item">
            <Icon name="file" size={14} />
            CSV → <code>data/inbox/</code> {demo.last_file && <>· last: <code>{demo.last_file}</code></>}
          </span>
          <span className="foot-item">Direct ingest ok <b>{demo.direct_ok ?? 0}</b> · fallback <b>{demo.fallbacks ?? 0}</b></span>
          {!demo.running && (
            <button type="button" className="btn-link is-danger" onClick={wipe}>
              <Icon name="rotate-ccw" size={14} />
              Reset data
            </button>
          )}
        </div>
        {err && <Alert tone="error" role="alert">{err}</Alert>}
      </section>

      <section className="stats-grid" aria-label="Pipeline counters">
        <Counter title="Batches" value={stats.batches} hint={`${stats.total_donations} donations`} />
        <Counter title="Accepted rows" value={stats.accepted_rows} tone="good" />
        <Counter title="Rejected rows" value={stats.rejected_rows} tone="bad" />
        <Counter title="Matches" value={stats.matches} />
        <Counter title="Kg rescued" value={stats.kg_rescued} hint={`${stats.kg_in_flight} kg in transit`} tone="good" />
        <Counter title="Meals" value={stats.meals} hint="kg ÷ 0.4" tone="good" />
      </section>

      <section className="card" aria-labelledby="status-title">
        <div className="card-header">
          <div>
            <h2 id="status-title" className="card-title">Donations by status</h2>
            <p className="card-subtitle">From listing to delivery. Expired donations were not collected in time.</p>
          </div>
          <span className="muted small">{tracked.toLocaleString('en-IN')} tracked</span>
        </div>
        <div className={`status-bar${tracked ? '' : ' is-empty'}`} aria-hidden="true">
          {flow.filter((s) => by[s]).map((s) => (
            <span key={s} className={`status-seg s-${s}`} style={{ flexGrow: by[s] }} title={`${humanize(s)}: ${by[s]}`} />
          ))}
        </div>
        <ul className="status-legend">
          {flow.map((s) => (
            <li key={s}>
              <span className="status-count">{(by[s] || 0).toLocaleString('en-IN')}</span>
              <span className="status-name"><span className={`dot s-${s}`} aria-hidden="true" />{humanize(s)}</span>
            </li>
          ))}
        </ul>
      </section>

      <div className="grid-2 equal">
        <section className="card" aria-labelledby="map-title">
          <div className="card-header">
            <div>
              <h2 id="map-title" className="card-title">Donation map</h2>
              <p className="card-subtitle">Pins are sized by quantity and shaded by status.</p>
            </div>
          </div>
          <MapView pins={pins} />
        </section>
        <section className="card log-card" aria-labelledby="log-title">
          <div className="card-header">
            <div>
              <h2 id="log-title" className="card-title">Live event log</h2>
              <p className="card-subtitle">Ingestion, matching and delivery events as they happen.</p>
            </div>
          </div>
          <div className="log" ref={logRef} tabIndex={0} role="region" aria-label="Live event log">
            {events.length === 0 && <div className="log-empty">Waiting for events. Press “Start Live Demo”.</div>}
            {events.map((e) => (
              <div key={e.id} className={`ev ${e.kind}`}>
                <time dateTime={e.ts}>{new Date(e.ts).toLocaleTimeString('en-IN')}</time>
                <span className="tag">{label(e.kind)}</span>
                <span className="ev-msg">{e.message}</span>
              </div>
            ))}
          </div>
        </section>
      </div>

      <section className="card" aria-labelledby="rejected-title">
        <div className="card-header">
          <div>
            <h2 id="rejected-title" className="card-title">
              Rejected rows <span className="count">{rejected.length}</span>
            </h2>
            <p className="card-subtitle">The latest rows the cleaning step rejected, with the reason.</p>
          </div>
        </div>
        <div className="table-wrap table-scroll" tabIndex={0} role="region" aria-label="Rejected rows">
          <table className="table table-wide">
            <thead>
              <tr>
                <th scope="col">Batch</th>
                <th scope="col" className="num">Row</th>
                <th scope="col">Type</th>
                <th scope="col">Reason</th>
                <th scope="col">Raw data</th>
              </tr>
            </thead>
            <tbody>
              {rejected.length === 0 && <tr><td colSpan="5" className="table-empty">No rejected rows yet</td></tr>}
              {rejected.map((r) => (
                <tr key={r.id}>
                  <td>#{r.batch_id}</td>
                  <td className="num">{r.row_number}</td>
                  <td><Badge tone="outline">{humanize(r.type)}</Badge></td>
                  <td><Badge tone="error" icon="alert-circle" className="badge-wrap">{r.reason}</Badge></td>
                  <td className="raw">{[r.raw.org_name, r.raw.food_type, r.raw.category, r.raw.quantity_kg && `${r.raw.quantity_kg}kg`].filter(Boolean).join(' · ')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  )
}
