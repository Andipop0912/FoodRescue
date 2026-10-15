/** Inline stroke icons on a 24px grid. They inherit currentColor and are decorative
 *  (aria-hidden) unless a `title` is passed. */
const PATHS = {
  check: <path d="M20 6 9 17l-5-5" />,
  'check-circle': (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="m8 12.5 2.8 2.8L16 9.8" />
    </>
  ),
  'alert-circle': (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7.8v4.6M12 16.2h.01" />
    </>
  ),
  'chevron-down': <path d="m6 9 6 6 6-6" />,
  clock: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7.5V12l3 2" />
    </>
  ),
  inbox: (
    <>
      <path d="M3 13h5l1.5 2.5h5L16 13h5" />
      <path d="M5.6 5h12.8L21 13v5.5a1.5 1.5 0 0 1-1.5 1.5h-15A1.5 1.5 0 0 1 3 18.5V13z" />
    </>
  ),
  package: (
    <>
      <path d="m21 7.5-9-4.5-9 4.5v9l9 4.5 9-4.5z" />
      <path d="m3 7.5 9 4.5 9-4.5M12 12v9M7.5 5.25l9 4.5" />
    </>
  ),
  truck: (
    <>
      <path d="M2.5 6h11v10h-11z" />
      <path d="M13.5 9.5h4l3 3.5V16h-7" />
      <circle cx="6.5" cy="17.5" r="1.8" />
      <circle cx="17" cy="17.5" r="1.8" />
    </>
  ),
  activity: <path d="M3 12h4l3-8 4 16 3-8h4" />,
  'map-pin': (
    <>
      <path d="M12 21s-7-6.1-7-11.5a7 7 0 0 1 14 0C19 14.9 12 21 12 21z" />
      <circle cx="12" cy="9.5" r="2.5" />
    </>
  ),
  'log-out': (
    <>
      <path d="M9 20H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h3" />
      <path d="m16 16 4-4-4-4M20 12H9" />
    </>
  ),
  play: <path d="M7 4.8v14.4L18.5 12z" fill="currentColor" stroke="none" />,
  stop: <rect x="6" y="6" width="12" height="12" rx="2" fill="currentColor" stroke="none" />,
  'rotate-ccw': (
    <>
      <path d="M3.5 12a8.5 8.5 0 1 0 2.6-6.1" />
      <path d="M3.5 4v5h5" />
    </>
  ),
  file: (
    <>
      <path d="M14 3H6.5A1.5 1.5 0 0 0 5 4.5v15A1.5 1.5 0 0 0 6.5 21h11a1.5 1.5 0 0 0 1.5-1.5V8z" />
      <path d="M14 3v5h5" />
    </>
  ),
  plus: <path d="M12 5v14M5 12h14" />,
  store: (
    <>
      <path d="M4 10.5V20h16v-9.5" />
      <path d="M3 4h18l-1.6 6.5H4.6z" />
      <path d="M9.5 20v-5h5v5" />
    </>
  ),
  users: (
    <>
      <circle cx="9" cy="8" r="3.5" />
      <path d="M2.5 20a6.5 6.5 0 0 1 13 0" />
      <path d="M16 4.6a3.5 3.5 0 0 1 0 6.8M18.5 14.6A6.5 6.5 0 0 1 21.5 20" />
    </>
  ),
  shield: <path d="M12 3 4.5 6v5.6c0 4.6 3.2 8.3 7.5 9.4 4.3-1.1 7.5-4.8 7.5-9.4V6z" />,
}

export function Icon({ name, size = 18, className = '', title, ...rest }) {
  const body = PATHS[name]
  if (!body) return null
  return (
    <svg
      className={`icon ${className}`.trim()}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden={title ? undefined : true}
      role={title ? 'img' : undefined}
      {...rest}
    >
      {title && <title>{title}</title>}
      {body}
    </svg>
  )
}

/** Brand mark: a bowl with two sprouting leaves. `inverted` is for dark (primary) surfaces. */
export function Logo({ size = 36, inverted = false }) {
  return (
    <svg className="logo" width={size} height={size} viewBox="0 0 32 32" aria-hidden="true">
      <rect width="32" height="32" rx="9" fill={inverted ? 'rgba(255,255,255,0.14)' : '#245C43'} />
      <path d="M16 15.6c-.1-4.4 2.4-7.6 7-8.3-.3 4.6-2.8 7.7-7 8.3z" fill="#E6A23C" />
      <path d="M15.4 15.6c-3.4-.3-5.5-2.6-5.8-6 3.6.3 5.6 2.5 5.8 6z" fill="#A8D5B9" />
      <path d="M6.5 17.2h19a9.5 9.5 0 0 1-19 0z" fill="#FFFFFF" />
    </svg>
  )
}
