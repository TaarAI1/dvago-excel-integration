import { useState, useEffect, useRef } from 'react'
import {
  Box, Typography, Chip, Select, MenuItem, FormControl, InputLabel,
  TextField, Button, IconButton, Tooltip, Paper, Stack,
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

function LogRow({ log }: { log: LogEntry }) {
  const statusColor = log.status === 'success' ? 'success' : log.status === 'failed' ? 'error' : 'warning'

  return (
    <Box sx={{ display: 'flex', gap: 1.5, alignItems: 'flex-start', py: 0.75, borderBottom: '1px solid', borderColor: 'divider' }}>
      <Typography variant="caption" sx={{ color: 'text.secondary', minWidth: 80, whiteSpace: 'nowrap', fontFamily: 'monospace' }}>
        {new Date(log.timestamp).toLocaleTimeString()}
      </Typography>
      <Chip label={log.activity_type} size="small" variant="outlined" sx={{ fontSize: 10, height: 20, minWidth: 90 }} />
      <Chip label={log.status} size="small" color={statusColor} sx={{ fontSize: 10, height: 20, minWidth: 60 }} />
      {log.document_type && (
        <Typography variant="caption" sx={{ color: 'text.secondary', minWidth: 120 }}>{log.document_type}</Typography>
      )}
      <Typography variant="caption" sx={{ flex: 1, color: log.status === 'failed' ? 'error.main' : 'text.primary' }}>
        {log.details}
      </Typography>
      {log.duration_ms != null && (
        <Typography variant="caption" sx={{ color: 'text.secondary', whiteSpace: 'nowrap', fontFamily: 'monospace' }}>
          {log.duration_ms}ms
        </Typography>
      )}
    </Box>
  )
}

export default function ActivityLog() {
  const [liveEntries, setLiveEntries] = useState<LogEntry[]>([])
  const [autoScroll, setAutoScroll] = useState(true)
  const [filterType, setFilterType] = useState('')
  const [filterStatus, setFilterStatus] = useState('')
  const [search, setSearch] = useState('')
  const bottomRef = useRef<HTMLDivElement>(null)

  // Initial historical logs
  const { data: historicalData, isLoading } = useQuery({
    queryKey: ['logs-initial'],
    queryFn: () => apiClient.get('/api/logs', { params: { limit: 100 } }).then((r) => r.data),
  })

  useEffect(() => {
    if (historicalData?.items) {
      setLiveEntries((prev) => {
        const existingIds = new Set(prev.map((l) => l.id))
        const newOnes = historicalData.items.filter((l: LogEntry) => !existingIds.has(l.id))
        return [...newOnes.reverse(), ...prev]
      })
    }
  }, [historicalData])

  // Live SSE stream
  const { data: newLog } = useSSE<LogEntry>('/api/stream/logs')
  useEffect(() => {
    if (newLog && newLog.id) {
      setLiveEntries((prev) => {
        if (prev.find((l) => l.id === newLog.id)) return prev
        return [newLog, ...prev].slice(0, 1000)  // cap at 1000 entries in memory
      })
    }
  }, [newLog])

  // Auto-scroll
  useEffect(() => {
    if (autoScroll) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
  }, [liveEntries, autoScroll])

  const filtered = liveEntries.filter((l) => {
    if (filterType && l.activity_type !== filterType) return false
    if (filterStatus && l.status !== filterStatus) return false
    if (search && !JSON.stringify(l).toLowerCase().includes(search.toLowerCase())) return false
    return true
  })

  const handleExport = () => {
    const token = localStorage.getItem('access_token')
    window.open(`/api/logs/export?fmt=csv&token=${token}`, '_blank')
  }

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5, flexWrap: 'wrap' }}>
        <Typography variant="h6" sx={{ fontWeight: 600 }}>Activity Log</Typography>
        <Typography variant="body2" color="text.secondary">{filtered.length} entries</Typography>
        {isLoading && <CircularProgress size={14} />}

        <Box sx={{ flexGrow: 1 }} />

        <TextField
          size="small"
          placeholder="Search..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          sx={{ width: 180 }}
          slotProps={{
            input: {
              endAdornment: search ? (
                <IconButton size="small" onClick={() => setSearch('')}><ClearIcon fontSize="small" /></IconButton>
              ) : null,
            },
          }}
        />

        <FormControl size="small" sx={{ minWidth: 130 }}>
          <InputLabel>Type</InputLabel>
          <Select value={filterType} label="Type" onChange={(e) => setFilterType(e.target.value)}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="ftp_poll">FTP Poll</MenuItem>
            <MenuItem value="csv_parse">CSV Parse</MenuItem>
            <MenuItem value="api_call">API Call</MenuItem>
            <MenuItem value="manual_trigger">Manual</MenuItem>
            <MenuItem value="error">Error</MenuItem>
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 110 }}>
          <InputLabel>Status</InputLabel>
          <Select value={filterStatus} label="Status" onChange={(e) => setFilterStatus(e.target.value)}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="success">Success</MenuItem>
            <MenuItem value="failed">Failed</MenuItem>
            <MenuItem value="pending">Pending</MenuItem>
          </Select>
        </FormControl>

        <FormControlLabel
          control={<Switch checked={autoScroll} onChange={(e) => setAutoScroll(e.target.checked)} size="small" />}
          label={<Typography variant="caption">Auto-scroll</Typography>}
        />

        <Tooltip title="Export logs as CSV">
          <IconButton size="small" onClick={handleExport}><DownloadIcon /></IconButton>
        </Tooltip>

        <Button size="small" variant="outlined" onClick={() => setLiveEntries([])}>Clear</Button>
      </Box>

      <Paper variant="outlined" sx={{ height: 400, overflow: 'auto', p: 1 }}>
        {filtered.length === 0 && !isLoading && (
          <Typography variant="body2" color="text.secondary" sx={{ p: 2, textAlign: 'center' }}>
            No log entries yet. Trigger a poll to see activity.
          </Typography>
        )}
        <Stack spacing={0}>
          {[...filtered].reverse().map((log) => (
            <LogRow key={log.id} log={log} />
          ))}
        </Stack>
        <div ref={bottomRef} />
      </Paper>
    </Box>
  )
}
