import { CircleMarker, MapContainer, Popup, TileLayer } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { Badge, STATUSES, STATUS_COLOR, humanize, label } from './bits.jsx'

/** Flow statuses are filled pins shaded light to dark; expired is a hollow red ring so it
 *  never depends on telling red from green. */
const pinStyle = (status) => (status === 'expired'
  ? { color: STATUS_COLOR.expired, weight: 2.5, fillColor: '#ffffff', fillOpacity: 0.9 }
  : { color: '#ffffff', weight: 1.5, fillColor: STATUS_COLOR[status] || '#5a6b62', fillOpacity: 0.9 })

export default function MapView({ pins }) {
  return (
    <div className="map-wrap">
      <MapContainer center={[19.06, 73.02]} zoom={12} scrollWheelZoom className="map">
        <TileLayer attribution="&copy; OpenStreetMap contributors" url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
        {pins.map((p) => (
          <CircleMarker key={p.id} center={[p.lat, p.lng]} radius={Math.min(10, 4 + p.kg / 10)} pathOptions={pinStyle(p.status)}>
            <Popup>
              <div className="map-popup">
                <strong className="map-popup-title">{p.food}</strong>
                <span>{p.kg} kg · {label(p.category)}</span>
                <span className="muted">{p.area}</span>
                <Badge status={p.status} />
              </div>
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>
      <ul className="legend" aria-label="Map legend">
        {[...STATUSES, 'expired'].map((s) => (
          <li key={s}><span className={`dot s-${s}`} aria-hidden="true" />{humanize(s)}</li>
        ))}
      </ul>
    </div>
  )
}
