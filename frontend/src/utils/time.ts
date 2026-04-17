/**
 * Pakistan Standard Time helpers — UTC+5, no DST.
 * Use these everywhere dates/times are displayed so the UI always shows PKT.
 */
const PKT: Intl.DateTimeFormatOptions = { timeZone: 'Asia/Karachi' }

export function fmtTime(ts: string | null | undefined): string {
  if (!ts) return '—'
  return new Date(ts).toLocaleTimeString('en-PK', {
    ...PKT,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

export function fmtDateTime(ts: string | null | undefined): string {
  if (!ts) return '—'
  return new Date(ts).toLocaleString('en-PK', {
    ...PKT,
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  })
}

export function fmtDate(ts: string | null | undefined): string {
  if (!ts) return '—'
  return new Date(ts).toLocaleDateString('en-PK', {
    ...PKT,
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  })
}
