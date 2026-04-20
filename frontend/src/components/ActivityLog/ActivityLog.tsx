import { useState, useEffect, useRef } from 'react'
import { fmtDateTime } from '../../utils/time'
import {
  Box, Typography, Chip, Select, MenuItem, FormControl, InputLabel,
  TextField, Button, IconButton, Tooltip, Stack,
  Switch, FormControlLabel, CircularProgress,
} from '@mui/material'
import DownloadIcon from '@mui/icons-material/Download'
import ClearIcon from '@mui/icons-material/Clear'
import { useSSE } from '../../hooks/useSSE'
import { useQuery } from '@tanstack/react-query'
import apiClient from '../../api/client'

interface LogEntry {
  id: string
  activity_type: string
  document_type?: string
  document_id?: string
  timestamp: string
  status: string
  details?: string
  duration_ms?: number
  metadata?: Record<string, unknown>
}

// ── Datetime-local helpers ─────────────────────────────────────────────────────

function toDatetimeLocal(d: Date): string {
  const p = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`
}

function last24hFrom(): string {
  const d = new Date()
  d.setHours(d.getHours() - 24)
  return toDatetimeLocal(d)
}

// ── Log row ────────────────────────────────────────────────────────────────────

function LogRow({ log }: { log: LogEntry }) {
  const statusColor = log.status === 'success' ? 'success' : log.status === 'failed' ? 'error' : 'warning'

  return (
    <Box sx={{ display: 'flex', gap: 1.5, alignItems: 'flex-start', py: 0.75,
      borderBottom: '1px solid', borderColor: 'divider' }}>
      <Typography variant="caption" sx={{ color: 'text.secondary', minWidth: 130,
        whiteSpace: 'nowrap', fontFamily: 'monospace' }}>
        {fmtDateTime(log.timestamp)}
      </Typography>
      <Chip label={log.activity_type} size="small" variant="outlined"
        sx={{ fontSize: 10, height: 20, minWidth: 90 }} />
      <Chip label={log.status} size="small" color={statusColor}
        sx={{ fontSize: 10, height: 20, minWidth: 60 }} />
      {log.document_type && (
        <Typography variant="caption" sx={{ color: 'text.secondary', minWidth: 100 }}>
          {log.document_type}
        </Typography>
      )}
      <Typography variant="caption" sx={{ flex: 1,
        color: log.status === 'failed' ? 'error.main' : 'text.primary' }}>
        {log.details}
      </Typography>
      {log.duration_ms != null && (
        <Typography variant="caption" sx={{ color: 'text.secondary',
          whiteSpace: 'nowrap', fontFamily: 'monospace' }}>
          {log.duration_ms}ms
        </Typography>
      )}
    </Box>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────

export default function ActivityLog() {
  const [autoScroll, setAutoScroll]     = useState(true)
  const [filterType, setFilterType]     = useState('')
  const [filterStatus, setFilterStatus] = useState('')
  const [search, setSearch]             = useState('')
  const [dateFrom, setDateFrom]         = useState<string>(last24hFrom)
  const [dateTo, setDateTo]             = useState<string>('')
  // SSE live entries that arrived after the last historical fetch
  const [extraEntries, setExtraEntries] = useState<LogEntry[]>([])

  const topRef = useRef<HTMLDivElement>(null)

  // ── Historical query (server-side type/status/date filters) ─────────────────
  const queryParams = {
    limit: 500,
    ...(dateFrom     ? { date_from: new Date(dateFrom).toISOString() } : {}),
    ...(dateTo       ? { date_to:   new Date(dateTo).toISOString()   } : {}),
    ...(filterType   ? { activity_type: filterType }                  : {}),
    ...(filterStatus ? { status: filterStatus }                       : {}),
  }

  const { data: historicalData, isLoading } = useQuery({
    queryKey: ['logs', dateFrom, dateTo, filterType, filterStatus],
    queryFn:  () => apiClient.get('/api/logs', { params: queryParams }).then(r => r.data),
    refetchInterval: 30_000,
  })

  const historicalEntries: LogEntry[] = historicalData?.items ?? []

  // Reset SSE extras whenever the date range / filters change
  useEffect(() => { setExtraEntries([]) }, [dateFrom, dateTo, filterType, filterStatus])

  // ── SSE live stream — prepend newest entries ─────────────────────────────────
  const { data: newLog } = useSSE<LogEntry>('/api/stream/logs')
  useEffect(() => {
    if (newLog?.id) {
      setExtraEntries(prev => {
        if (prev.find(l => l.id === newLog.id)) return prev
        return [newLog, ...prev].slice(0, 200)
      })
    }
  }, [newLog])

  // ── Merge: SSE extras (newest) on top, then historical (already DESC) ────────
  const historicalIds = new Set(historicalEntries.map(l => l.id))
  const allEntries = [
    ...extraEntries.filter(l => !historicalIds.has(l.id)),
    ...historicalEntries,
  ]

  const filtered = allEntries.filter(l => {
    if (search && !JSON.stringify(l).toLowerCase().includes(search.toLowerCase())) return false
    return true
  })

  // ── Auto-scroll to TOP (newest entry) on new data ───────────────────────────
  useEffect(() => {
    if (autoScroll && topRef.current) {
      topRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [filtered.length, autoScroll])

  const handleExport = () => {
    const token = localStorage.getItem('access_token')
    window.open(`/api/logs/export?fmt=csv&token=${token}`, '_blank')
  }

  const applyLast24h = () => {
    setDateFrom(last24hFrom())
    setDateTo('')
  }

  // ── Render ───────────────────────────────────────────────────────────────────
  return (
    <Box>
      {/* ── Row 1: counts · search · type · status · auto-scroll · export · clear */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1, flexWrap: 'wrap' }}>
        <Typography sx={{ fontSize: '0.8rem', color: '#6b7280' }}>{filtered.length} entries</Typography>
        {isLoading && <CircularProgress size={14} />}

        <Box sx={{ flexGrow: 1 }} />

        <TextField size="small" placeholder="Search..." value={search}
          onChange={e => setSearch(e.target.value)} sx={{ width: 160 }}
          slotProps={{
            input: {
              endAdornment: search
                ? <IconButton size="small" onClick={() => setSearch('')}><ClearIcon fontSize="small" /></IconButton>
                : null,
            },
          }}
        />

        <FormControl size="small" sx={{ minWidth: 130 }}>
          <InputLabel>Type</InputLabel>
          <Select value={filterType} label="Type" onChange={e => setFilterType(e.target.value)}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="ftp_poll">FTP Poll</MenuItem>
            <MenuItem value="csv_parse">CSV Parse</MenuItem>
            <MenuItem value="api_call">API Call</MenuItem>
            <MenuItem value="item_master">Item Master</MenuItem>
            <MenuItem value="manual_trigger">Manual</MenuItem>
            <MenuItem value="error">Error</MenuItem>
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 110 }}>
          <InputLabel>Status</InputLabel>
          <Select value={filterStatus} label="Status" onChange={e => setFilterStatus(e.target.value)}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="success">Success</MenuItem>
            <MenuItem value="failed">Failed</MenuItem>
            <MenuItem value="pending">Pending</MenuItem>
          </Select>
        </FormControl>

        <FormControlLabel
          control={<Switch checked={autoScroll} onChange={e => setAutoScroll(e.target.checked)} size="small" />}
          label={<Typography variant="caption">Auto-scroll</Typography>}
        />

        <Tooltip title="Export logs as CSV">
          <IconButton size="small" onClick={handleExport}><DownloadIcon /></IconButton>
        </Tooltip>

        <Button size="small" variant="outlined" onClick={() => setExtraEntries([])}>Clear</Button>
      </Box>

      {/* ── Row 2: date range filter ─────────────────────────────────────────── */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5, flexWrap: 'wrap' }}>
        <Typography sx={{ fontSize: '0.75rem', color: '#6b7280' }}>From</Typography>
        <TextField
          size="small"
          type="datetime-local"
          value={dateFrom}
          onChange={e => setDateFrom(e.target.value)}
          sx={{ '& input': { fontSize: '0.75rem', py: '5px' } }}
        />

        <Typography sx={{ fontSize: '0.75rem', color: '#6b7280' }}>To</Typography>
        <TextField
          size="small"
          type="datetime-local"
          value={dateTo}
          onChange={e => setDateTo(e.target.value)}
          sx={{ '& input': { fontSize: '0.75rem', py: '5px' } }}
        />

        <Button size="small" variant="outlined" onClick={applyLast24h}
          sx={{ height: 32, fontSize: '0.72rem', whiteSpace: 'nowrap' }}>
          Last 24h
        </Button>

        {dateTo && (
          <Button size="small" variant="text" onClick={() => setDateTo('')}
            sx={{ height: 32, fontSize: '0.72rem' }}>
            Clear To
          </Button>
        )}
      </Box>

      {/* ── Log list ─────────────────────────────────────────────────────────── */}
      <Box sx={{ height: 400, overflow: 'auto', p: 1, bgcolor: '#f9fafb',
        border: '1px solid #e5e7eb', borderRadius: '4px' }}>
        <div ref={topRef} />
        {filtered.length === 0 && !isLoading && (
          <Typography variant="body2" color="text.secondary" sx={{ p: 2, textAlign: 'center' }}>
            No log entries found for this date range.
          </Typography>
        )}
        <Stack spacing={0}>
          {filtered.map(log => <LogRow key={log.id} log={log} />)}
        </Stack>
      </Box>
    </Box>
  )
}
