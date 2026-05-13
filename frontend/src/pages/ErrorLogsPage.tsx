import { useState, useCallback } from 'react'
import {
  Box, Typography, Table, TableBody, TableCell, TableContainer,
  TableHead, TableRow, CircularProgress, IconButton, Tooltip,
  Dialog, DialogContent, DialogActions, Button, TextField,
  Select, MenuItem, FormControl, InputLabel, Chip,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
import AccessTimeIcon from '@mui/icons-material/AccessTime'
import FilterListIcon from '@mui/icons-material/FilterList'
import { useQuery } from '@tanstack/react-query'
import apiClient from '../api/client'
import { fmtDateTime } from '../utils/time'

// ── Types ──────────────────────────────────────────────────────────────────────

interface ErrorEntry {
  id: string
  activity_type: string
  document_id: string | null
  document_type: string | null
  timestamp: string
  status: string
  details: string | null
  duration_ms: number | null
  metadata: Record<string, unknown> | null
}

interface ErrorsResponse {
  total: number
  offset: number
  limit: number
  items: ErrorEntry[]
}

interface MetaResponse {
  activity_types: string[]
  document_types: string[]
}

// ── Helpers ────────────────────────────────────────────────────────────────────

const PAGE_SIZE = 50

/** Format a Date to the value accepted by <input type="datetime-local"> */
function toDatetimeLocal(d: Date): string {
  const pad = (n: number) => n.toString().padStart(2, '0')
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
    `T${pad(d.getHours())}:${pad(d.getMinutes())}`
  )
}

function last24h(): { from: string; to: string } {
  const now = new Date()
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000)
  return { from: toDatetimeLocal(yesterday), to: toDatetimeLocal(now) }
}

const STATUS_STYLE: Record<string, { bg: string; color: string; border: string }> = {
  error:   { bg: '#fef2f2', color: '#b91c1c', border: '#fecaca' },
  failed:  { bg: '#fef2f2', color: '#b91c1c', border: '#fecaca' },
  warning: { bg: '#fffbeb', color: '#b45309', border: '#fde68a' },
  success: { bg: '#f0fdf4', color: '#15803d', border: '#bbf7d0' },
}

function StatusBadge({ status }: { status: string }) {
  const s = STATUS_STYLE[status] ?? { bg: '#f3f4f6', color: '#374151', border: '#e5e7eb' }
  return (
    <Box component="span" sx={{
      display: 'inline-block', px: 1, py: '1px',
      fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.04em',
      borderRadius: '4px', border: `1px solid ${s.border}`,
      bgcolor: s.bg, color: s.color, whiteSpace: 'nowrap',
      textTransform: 'uppercase', lineHeight: '18px',
      minWidth: 52, textAlign: 'center',
    }}>
      {status}
    </Box>
  )
}

function ActivityTypeBadge({ value }: { value: string }) {
  return (
    <Box component="span" sx={{
      display: 'inline-block', px: 1, py: '1px',
      fontSize: '0.65rem', fontWeight: 600, letterSpacing: '0.03em',
      borderRadius: '4px', border: '1px solid #e5e7eb',
      bgcolor: '#f8fafc', color: '#374151', whiteSpace: 'nowrap',
      lineHeight: '18px',
    }}>
      {value}
    </Box>
  )
}

// ── Detail dialog ──────────────────────────────────────────────────────────────

function ErrorDetailDialog({ entry, onClose }: { entry: ErrorEntry | null; onClose: () => void }) {
  if (!entry) return null

  const rows: [string, string][] = [
    ['ID',            entry.id],
    ['Timestamp',     fmtDateTime(entry.timestamp)],
    ['Activity Type', entry.activity_type],
    ['Status',        entry.status],
    ['Document Type', entry.document_type ?? '—'],
    ['Document ID',   entry.document_id ?? '—'],
    ['Duration',      entry.duration_ms != null ? `${entry.duration_ms.toFixed(0)} ms` : '—'],
  ]

  return (
    <Dialog open onClose={onClose} maxWidth="md" fullWidth
      slotProps={{ paper: { sx: { borderRadius: '8px', overflow: 'hidden' } } }}>
      {/* Header */}
      <Box sx={{ bgcolor: '#fef2f2', borderBottom: '2px solid #fecaca', px: 3, py: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <ErrorOutlinedIcon sx={{ fontSize: 16, color: '#b91c1c' }} />
            <StatusBadge status={entry.status} />
            <ActivityTypeBadge value={entry.activity_type} />
          </Box>
          <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af', fontFamily: 'monospace' }}>
            {fmtDateTime(entry.timestamp)}
          </Typography>
        </Box>
        <Typography sx={{ fontWeight: 700, fontSize: '0.95rem', color: '#7f1d1d', mt: 0.5 }}>
          {entry.activity_type}
          {entry.document_type ? ` — ${entry.document_type}` : ''}
        </Typography>
      </Box>

      <DialogContent sx={{ p: 0, maxHeight: '70vh', overflowY: 'auto' }}>
        <Box sx={{ px: 3, py: 2 }}>

          {/* Error details */}
          {entry.details && (
            <Box sx={{ mb: 2.5 }}>
              <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#b91c1c',
                textTransform: 'uppercase', letterSpacing: '0.06em', mb: 0.75 }}>
                Error Details
              </Typography>
              <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: '6px', p: 1.5 }}>
                <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace',
                  whiteSpace: 'pre-wrap', color: '#7f1d1d', wordBreak: 'break-word', lineHeight: 1.6 }}>
                  {entry.details}
                </Typography>
              </Box>
            </Box>
          )}

          {/* Metadata */}
          {entry.metadata && Object.keys(entry.metadata).length > 0 && (
            <Box sx={{ mb: 2.5 }}>
              <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#6b7280',
                textTransform: 'uppercase', letterSpacing: '0.06em', mb: 0.75 }}>
                Metadata
              </Typography>
              <Box sx={{ bgcolor: '#f8fafc', border: '1px solid #e5e7eb', borderRadius: '6px', p: 1.5 }}>
                <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace',
                  whiteSpace: 'pre-wrap', color: '#374151', wordBreak: 'break-word', lineHeight: 1.6 }}>
                  {JSON.stringify(entry.metadata, null, 2)}
                </Typography>
              </Box>
            </Box>
          )}

          {/* Field grid */}
          <Box sx={{ border: '1px solid #f3f4f6', borderRadius: '6px', overflow: 'hidden' }}>
            {rows.map(([label, value]) => (
              <Box key={label} sx={{
                display: 'flex', gap: 1, px: 2, py: 0.9,
                borderBottom: '1px solid #f3f4f6', '&:last-child': { border: 0 },
                '&:nth-of-type(odd)': { bgcolor: '#fafafa' },
              }}>
                <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af', minWidth: 120, flexShrink: 0, fontWeight: 500 }}>
                  {label}
                </Typography>
                <Typography sx={{
                  fontSize: '0.78rem', color: '#111827', wordBreak: 'break-all',
                  fontFamily: ['ID', 'Document ID'].includes(label) ? 'monospace' : 'inherit',
                  fontWeight: ['Status', 'Activity Type'].includes(label) ? 600 : 400,
                }}>
                  {value}
                </Typography>
              </Box>
            ))}
          </Box>

        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2, pt: 1, borderTop: '1px solid #f3f4f6' }}>
        <Button size="small" variant="outlined" onClick={onClose}
          sx={{ height: 30, fontSize: '0.78rem' }}>Close</Button>
      </DialogActions>
    </Dialog>
  )
}

// ── Quick range buttons ────────────────────────────────────────────────────────

const QUICK_RANGES = [
  { label: 'Last 24h', hours: 24 },
  { label: 'Last 48h', hours: 48 },
  { label: 'Last 7d',  hours: 24 * 7 },
  { label: 'Last 30d', hours: 24 * 30 },
]

// ── Table style helpers ────────────────────────────────────────────────────────

const thSx = {
  fontSize: '0.68rem', fontWeight: 600, color: '#6b7280',
  textTransform: 'uppercase' as const, letterSpacing: '0.04em',
  py: 1, px: 1.5, borderBottom: '1px solid #e5e7eb',
}
const tdSx = {
  fontSize: '0.78rem', color: '#374151',
  py: 0.75, px: 1.5, borderBottom: '1px solid #f3f4f6',
}

// ── Page ───────────────────────────────────────────────────────────────────────

export default function ErrorLogsPage() {
  const defaults = last24h()

  const [dateFrom, setDateFrom] = useState(defaults.from)
  const [dateTo,   setDateTo]   = useState(defaults.to)
  const [activityType, setActivityType] = useState('')
  const [documentType, setDocumentType] = useState('')
  const [page, setPage] = useState(0)
  const [selected, setSelected] = useState<ErrorEntry | null>(null)
  const [activeQuick, setActiveQuick] = useState<number | null>(24)

  const offset = page * PAGE_SIZE

  const queryKey = ['error-logs', dateFrom, dateTo, activityType, documentType, page]

  const { data, isLoading, isFetching, refetch } = useQuery<ErrorsResponse>({
    queryKey,
    queryFn: () => {
      const params = new URLSearchParams()
      if (dateFrom)      params.set('date_from', dateFrom)
      if (dateTo)        params.set('date_to',   dateTo)
      if (activityType)  params.set('activity_type', activityType)
      if (documentType)  params.set('document_type', documentType)
      params.set('limit',  String(PAGE_SIZE))
      params.set('offset', String(offset))
      return apiClient.get(`/api/logs/errors?${params}`).then(r => r.data)
    },
    staleTime: 30_000,
  })

  const { data: meta } = useQuery<MetaResponse>({
    queryKey: ['error-logs-meta'],
    queryFn: () => apiClient.get('/api/logs/errors/meta').then(r => r.data),
    staleTime: 60_000,
  })

  const applyQuick = useCallback((hours: number) => {
    const now = new Date()
    const from = new Date(now.getTime() - hours * 60 * 60 * 1000)
    setDateFrom(toDatetimeLocal(from))
    setDateTo(toDatetimeLocal(now))
    setActiveQuick(hours)
    setPage(0)
  }, [])

  const handleDateChange = (field: 'from' | 'to', value: string) => {
    if (field === 'from') setDateFrom(value)
    else setDateTo(value)
    setActiveQuick(null)
    setPage(0)
  }

  const total      = data?.total ?? 0
  const items      = data?.items ?? []
  const totalPages = Math.ceil(total / PAGE_SIZE)

  return (
    <Box sx={{ p: { xs: 2, sm: 3 }, display: 'flex', flexDirection: 'column', gap: 2 }}>

      {/* Page header */}
      <Box sx={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', flexWrap: 'wrap', gap: 1 }}>
        <Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <ErrorOutlinedIcon sx={{ fontSize: 18, color: '#b91c1c' }} />
            <Typography sx={{ fontWeight: 600, fontSize: '0.875rem', color: '#111827' }}>
              Error Logs
            </Typography>
          </Box>
          <Typography sx={{ fontSize: '0.75rem', color: '#9ca3af', mt: 0.25 }}>
            All application errors — read-only view. Click a row to see full details.
          </Typography>
        </Box>

        {/* Total badge */}
        {!isLoading && (
          <Box sx={{
            display: 'flex', alignItems: 'center', gap: 0.75,
            px: 1.5, py: 0.5, border: '1px solid #fecaca', borderRadius: '6px',
            bgcolor: total > 0 ? '#fef2f2' : '#f8fafc',
          }}>
            <Box sx={{ width: 7, height: 7, borderRadius: '50%', bgcolor: total > 0 ? '#ef4444' : '#9ca3af' }} />
            <Typography sx={{ fontSize: '0.72rem', color: total > 0 ? '#b91c1c' : '#6b7280', fontWeight: 600 }}>
              {total.toLocaleString()} {total === 1 ? 'error' : 'errors'} found
            </Typography>
          </Box>
        )}
      </Box>

      {/* Filters card */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', p: 2 }}>

        {/* Quick range row */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5, flexWrap: 'wrap' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mr: 0.5 }}>
            <AccessTimeIcon sx={{ fontSize: 14, color: '#9ca3af' }} />
            <Typography sx={{ fontSize: '0.72rem', color: '#6b7280', fontWeight: 600 }}>Quick:</Typography>
          </Box>
          {QUICK_RANGES.map(r => (
            <Box key={r.hours}
              onClick={() => applyQuick(r.hours)}
              sx={{
                px: 1.5, py: 0.4, borderRadius: '5px', cursor: 'pointer',
                fontSize: '0.72rem', fontWeight: activeQuick === r.hours ? 700 : 500,
                border: '1px solid',
                borderColor: activeQuick === r.hours ? '#1a56db' : '#e5e7eb',
                bgcolor: activeQuick === r.hours ? '#eff6ff' : 'white',
                color: activeQuick === r.hours ? '#1a56db' : '#374151',
                transition: 'all 0.12s',
                '&:hover': { borderColor: '#93c5fd', bgcolor: '#f0f7ff' },
              }}>
              {r.label}
            </Box>
          ))}
        </Box>

        {/* Filter controls row */}
        <Box sx={{ display: 'flex', alignItems: 'flex-end', gap: 1.5, flexWrap: 'wrap' }}>
          <TextField
            label="From"
            type="datetime-local"
            size="small"
            value={dateFrom}
            onChange={e => handleDateChange('from', e.target.value)}
            slotProps={{ inputLabel: { shrink: true } }}
            sx={{ minWidth: 200, '& .MuiInputBase-input': { fontSize: '0.78rem' } }}
          />
          <TextField
            label="To"
            type="datetime-local"
            size="small"
            value={dateTo}
            onChange={e => handleDateChange('to', e.target.value)}
            slotProps={{ inputLabel: { shrink: true } }}
            sx={{ minWidth: 200, '& .MuiInputBase-input': { fontSize: '0.78rem' } }}
          />

          {(meta?.activity_types?.length ?? 0) > 0 && (
            <FormControl size="small" sx={{ minWidth: 170 }}>
              <InputLabel sx={{ fontSize: '0.8rem' }}>Activity Type</InputLabel>
              <Select
                value={activityType}
                label="Activity Type"
                onChange={e => { setActivityType(e.target.value); setPage(0) }}
                sx={{ fontSize: '0.78rem' }}
              >
                <MenuItem value=""><em>All types</em></MenuItem>
                {(meta?.activity_types ?? []).map(t => (
                  <MenuItem key={t} value={t} sx={{ fontSize: '0.78rem' }}>{t}</MenuItem>
                ))}
              </Select>
            </FormControl>
          )}

          {(meta?.document_types?.length ?? 0) > 0 && (
            <FormControl size="small" sx={{ minWidth: 160 }}>
              <InputLabel sx={{ fontSize: '0.8rem' }}>Document Type</InputLabel>
              <Select
                value={documentType}
                label="Document Type"
                onChange={e => { setDocumentType(e.target.value); setPage(0) }}
                sx={{ fontSize: '0.78rem' }}
              >
                <MenuItem value=""><em>All types</em></MenuItem>
                {(meta?.document_types ?? []).map(t => (
                  <MenuItem key={t} value={t} sx={{ fontSize: '0.78rem' }}>{t}</MenuItem>
                ))}
              </Select>
            </FormControl>
          )}

          {/* Active filter chips */}
          {(activityType || documentType) && (
            <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', alignItems: 'center' }}>
              <FilterListIcon sx={{ fontSize: 13, color: '#9ca3af' }} />
              {activityType && (
                <Chip label={`type: ${activityType}`} size="small"
                  onDelete={() => { setActivityType(''); setPage(0) }}
                  sx={{ fontSize: '0.68rem', height: 22, bgcolor: '#eff6ff', color: '#1d4ed8', border: '1px solid #bfdbfe' }} />
              )}
              {documentType && (
                <Chip label={`doc: ${documentType}`} size="small"
                  onDelete={() => { setDocumentType(''); setPage(0) }}
                  sx={{ fontSize: '0.68rem', height: 22, bgcolor: '#eff6ff', color: '#1d4ed8', border: '1px solid #bfdbfe' }} />
              )}
            </Box>
          )}

          <Box sx={{ flexGrow: 1 }} />

          <Tooltip title="Refresh">
            <IconButton size="small" onClick={() => refetch()}
              disabled={isFetching}
              sx={{ borderRadius: '4px', border: '1px solid #e5e7eb' }}>
              {isFetching
                ? <CircularProgress size={14} />
                : <RefreshIcon sx={{ fontSize: 16 }} />}
            </IconButton>
          </Tooltip>
        </Box>
      </Box>

      {/* Table */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <TableContainer>
          <Table size="small" sx={{ tableLayout: 'fixed', minWidth: 900 }}>
            <TableHead>
              <TableRow sx={{ bgcolor: '#f9fafb' }}>
                <TableCell sx={thSx} width={170}>Timestamp</TableCell>
                <TableCell sx={thSx} width={130}>Activity Type</TableCell>
                <TableCell sx={thSx} width={80}>Status</TableCell>
                <TableCell sx={thSx} width={120}>Document Type</TableCell>
                <TableCell sx={thSx}>Details</TableCell>
                <TableCell sx={thSx} width={90} align="right">Duration</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={6} align="center" sx={{ py: 8 }}>
                    <CircularProgress size={22} />
                  </TableCell>
                </TableRow>
              ) : items.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} align="center" sx={{ py: 8 }}>
                    <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 1 }}>
                      <ErrorOutlinedIcon sx={{ fontSize: 32, color: '#d1d5db' }} />
                      <Typography sx={{ fontSize: '0.82rem', color: '#9ca3af' }}>
                        No errors found in the selected time range.
                      </Typography>
                    </Box>
                  </TableCell>
                </TableRow>
              ) : items.map((item) => (
                <TableRow key={item.id}
                  onClick={() => setSelected(item)}
                  sx={{
                    cursor: 'pointer',
                    '&:hover': { bgcolor: '#fff5f5' },
                    transition: 'background 0.1s',
                  }}>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280', whiteSpace: 'nowrap' }}>
                    {fmtDateTime(item.timestamp)}
                  </TableCell>
                  <TableCell sx={tdSx}>
                    <ActivityTypeBadge value={item.activity_type} />
                  </TableCell>
                  <TableCell sx={tdSx}>
                    <StatusBadge status={item.status} />
                  </TableCell>
                  <TableCell sx={{ ...tdSx, color: '#9ca3af', fontSize: '0.72rem' }}>
                    {item.document_type ?? '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: '#374151', maxWidth: 0 }}>
                    {item.details
                      ? <span style={{ color: '#7f1d1d' }}>{item.details.length > 120 ? item.details.slice(0, 120) + '…' : item.details}</span>
                      : <span style={{ color: '#d1d5db' }}>—</span>}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'right', color: '#9ca3af', fontFamily: 'monospace', fontSize: '0.68rem', whiteSpace: 'nowrap' }}>
                    {item.duration_ms != null ? `${item.duration_ms.toFixed(0)} ms` : '—'}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        {/* Pagination footer */}
        {total > PAGE_SIZE && (
          <Box sx={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            px: 2, py: 1, borderTop: '1px solid #f3f4f6', bgcolor: '#fafafa',
          }}>
            <Typography sx={{ fontSize: '0.72rem', color: '#6b7280' }}>
              Showing {offset + 1}–{Math.min(offset + PAGE_SIZE, total)} of {total.toLocaleString()}
            </Typography>
            <Box sx={{ display: 'flex', gap: 0.75 }}>
              <Button size="small" variant="outlined" disabled={page === 0}
                onClick={() => setPage(p => p - 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 60 }}>
                Previous
              </Button>
              <Box sx={{ display: 'flex', alignItems: 'center', px: 1 }}>
                <Typography sx={{ fontSize: '0.72rem', color: '#374151' }}>
                  {page + 1} / {totalPages}
                </Typography>
              </Box>
              <Button size="small" variant="outlined" disabled={page >= totalPages - 1}
                onClick={() => setPage(p => p + 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 60 }}>
                Next
              </Button>
            </Box>
          </Box>
        )}
      </Box>

      {/* Detail dialog */}
      <ErrorDetailDialog entry={selected} onClose={() => setSelected(null)} />
    </Box>
  )
}
