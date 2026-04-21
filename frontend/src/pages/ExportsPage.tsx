import { useState, useEffect, useRef } from 'react'
import {
  Box, Typography, Tabs, Tab, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, CircularProgress,
  Select, MenuItem, FormControl, IconButton, Tooltip, Button,
  Dialog, DialogContent, DialogActions, LinearProgress,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import FolderOutlinedIcon from '@mui/icons-material/FolderOutlined'
import StopCircleOutlinedIcon from '@mui/icons-material/StopCircleOutlined'
import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import apiClient from '../api/client'
import { fmtDateTime } from '../utils/time'

// ── Types ─────────────────────────────────────────────────────────────────────

interface ExportRun {
  run_id: string
  label: string
  triggered_by: string
  status: string
  total_stores: number
  processed_stores: number
  started_at: string
  finished_at: string | null
}

interface ExportStore {
  id: string
  run_id: string
  store_no: number | null
  store_name: string | null
  query_rows: number
  written_rows: number
  filename: string | null
  ftp_path: string | null
  status: string
  error_message: string | null
  duration_ms: number | null
  created_at: string
}

interface Progress {
  active: boolean
  run_id: string | null
  total?: number
  done?: number
  current_store?: number | null
  status?: string
}

// ── Status styles ─────────────────────────────────────────────────────────────

const STATUS: Record<string, { bg: string; color: string; border: string; label: string }> = {
  success:    { bg: '#f0fdf4', color: '#15803d', border: '#bbf7d0', label: 'Success' },
  failed:     { bg: '#fef2f2', color: '#b91c1c', border: '#fecaca', label: 'Failed' },
  skipped:    { bg: '#fffbeb', color: '#b45309', border: '#fde68a', label: 'No Data' },
  partial:    { bg: '#fff7ed', color: '#c2410c', border: '#fed7aa', label: 'Partial' },
  running:    { bg: '#eff6ff', color: '#1d4ed8', border: '#bfdbfe', label: 'Running' },
  cancelled:  { bg: '#f3f4f6', color: '#374151', border: '#e5e7eb', label: 'Cancelled' },
  processing: { bg: '#eff6ff', color: '#1d4ed8', border: '#bfdbfe', label: 'Processing' },
  pending:    { bg: '#f8fafc', color: '#64748b', border: '#e2e8f0', label: 'Pending' },
}

function StatusBadge({ status }: { status: string }) {
  const s = STATUS[status] ?? STATUS.pending
  return (
    <Box component="span" sx={{
      display: 'inline-block', px: 1, py: '1px',
      fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.04em',
      borderRadius: '4px', border: `1px solid ${s.border}`,
      bgcolor: s.bg, color: s.color, whiteSpace: 'nowrap',
      textTransform: 'uppercase', lineHeight: '18px',
      minWidth: 58, textAlign: 'center',
    }}>
      {s.label}
    </Box>
  )
}

// ── Table style helpers ───────────────────────────────────────────────────────

const thSx = {
  fontSize: '0.68rem', fontWeight: 600, color: '#6b7280',
  textTransform: 'uppercase' as const, letterSpacing: '0.04em',
  py: 1, px: 1.5, borderBottom: '1px solid #e5e7eb',
}
const tdSx = {
  fontSize: '0.78rem', color: '#374151',
  py: 0.75, px: 1.5, borderBottom: '1px solid #f3f4f6',
}

// ── Store detail popup ────────────────────────────────────────────────────────

function StoreDetailDialog({ store, onClose }: { store: ExportStore | null; onClose: () => void }) {
  if (!store) return null

  const isSkipped = store.status === 'skipped'
  const isFailed  = store.status === 'failed'
  const isSuccess = store.status === 'success'
  const s = STATUS[store.status] ?? STATUS.pending

  return (
    <Dialog open onClose={onClose} maxWidth="sm" fullWidth
      slotProps={{ paper: { sx: { borderRadius: '8px', overflow: 'hidden' } } }}>
      {/* Header */}
      <Box sx={{ bgcolor: s.bg, borderBottom: `2px solid ${s.border}`, px: 3, py: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
          <StatusBadge status={store.status} />
          <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af', fontFamily: 'monospace' }}>
            {fmtDateTime(store.created_at)}
          </Typography>
        </Box>
        <Typography sx={{ fontWeight: 700, fontSize: '1rem', color: '#111827', mt: 0.5 }}>
          Store {store.store_no ?? '—'}{store.store_name ? ` — ${store.store_name}` : ''}
        </Typography>
      </Box>

      <DialogContent sx={{ p: 0, maxHeight: '70vh', overflowY: 'auto' }}>
        <Box sx={{ px: 3, py: 2 }}>

          {/* No data message */}
          {isSkipped && (
            <Box sx={{ mb: 2, p: 1.5, bgcolor: '#fffbeb', border: '1px solid #fde68a',
              borderRadius: '6px', display: 'flex', gap: 1, alignItems: 'flex-start' }}>
              <ErrorOutlinedIcon sx={{ fontSize: 15, color: '#b45309', mt: '1px', flexShrink: 0 }} />
              <Typography sx={{ fontSize: '0.8rem', color: '#92400e', fontWeight: 500 }}>
                No data found for this store — file was not created or uploaded.
              </Typography>
            </Box>
          )}

          {/* Success summary */}
          {isSuccess && (
            <Box sx={{ mb: 2, p: 1.5, bgcolor: '#f0fdf4', border: '1px solid #bbf7d0',
              borderRadius: '6px', display: 'flex', gap: 1, alignItems: 'flex-start' }}>
              <CheckCircleOutlinedIcon sx={{ fontSize: 15, color: '#15803d', mt: '1px', flexShrink: 0 }} />
              <Typography sx={{ fontSize: '0.8rem', color: '#166534', fontWeight: 500 }}>
                Export successful — {store.written_rows} rows written and uploaded.
              </Typography>
            </Box>
          )}

          {/* Error message */}
          {isFailed && store.error_message && (
            <Box sx={{ mb: 2 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 0.75 }}>
                <ErrorOutlinedIcon sx={{ fontSize: 14, color: '#b91c1c' }} />
                <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#b91c1c',
                  textTransform: 'uppercase', letterSpacing: '0.06em' }}>Error</Typography>
              </Box>
              <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: '6px', p: 1.5 }}>
                <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace',
                  whiteSpace: 'pre-wrap', color: '#7f1d1d', wordBreak: 'break-word' }}>
                  {store.error_message}
                </Typography>
              </Box>
            </Box>
          )}

          {/* Summary grid */}
          {[
            ['Store No',      store.store_no ?? '—'],
            ['Store Name',    store.store_name ?? '—'],
            ['Query Rows',    store.query_rows],
            ['Written Rows',  store.written_rows],
            ['Filename',      store.filename ?? '—'],
            ['FTP Path',      store.ftp_path ?? '—'],
            ['Duration',      store.duration_ms != null ? `${store.duration_ms.toFixed(0)} ms` : '—'],
          ].map(([label, value]) => (
            <Box key={label as string} sx={{
              display: 'flex', gap: 1, py: 0.75,
              borderBottom: '1px solid #f3f4f6', '&:last-child': { border: 0 },
            }}>
              <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af', minWidth: 110, flexShrink: 0 }}>
                {label}
              </Typography>
              <Typography sx={{ fontSize: '0.78rem', color: '#111827', fontWeight: 500,
                wordBreak: 'break-all', fontFamily: typeof value === 'string' && (value as string).includes('.csv') ? 'monospace' : 'inherit' }}>
                {String(value)}
              </Typography>
            </Box>
          ))}
        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2, pt: 1, borderTop: '1px solid #f3f4f6' }}>
        <Button size="small" variant="outlined" onClick={onClose}
          sx={{ height: 30, fontSize: '0.78rem' }}>Close</Button>
      </DialogActions>
    </Dialog>
  )
}

// ── Progress bar ──────────────────────────────────────────────────────────────

function ProgressBanner({ progress, onKill, killing }: {
  progress: Progress; onKill: () => void; killing: boolean
}) {
  if (!progress.active) return null
  const pct = progress.total ? Math.round((progress.done ?? 0) / progress.total * 100) : 0

  return (
    <Box sx={{ mb: 2, p: 1.5, bgcolor: '#eff6ff', border: '1px solid #bfdbfe',
      borderRadius: '6px', display: 'flex', flexDirection: 'column', gap: 1 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <CircularProgress size={13} thickness={5} sx={{ color: '#1d4ed8' }} />
          <Typography sx={{ fontSize: '0.78rem', fontWeight: 600, color: '#1e40af' }}>
            Export running — Store {progress.done ?? 0} of {progress.total ?? '?'} processed
            {progress.current_store != null && ` · currently processing store ${progress.current_store}`}
          </Typography>
        </Box>
        <Tooltip title="Kill export process">
          <Button size="small" variant="outlined" color="error"
            startIcon={killing
              ? <CircularProgress size={11} />
              : <StopCircleOutlinedIcon sx={{ fontSize: 14 }} />}
            onClick={onKill} disabled={killing}
            sx={{ height: 26, fontSize: '0.72rem', borderColor: '#fecaca',
              color: '#b91c1c', '&:hover': { bgcolor: '#fef2f2', borderColor: '#f87171' } }}>
            {killing ? 'Killing…' : 'Kill Export'}
          </Button>
        </Tooltip>
      </Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <LinearProgress variant="determinate" value={pct}
          sx={{ flexGrow: 1, height: 6, borderRadius: 3,
            bgcolor: '#bfdbfe', '& .MuiLinearProgress-bar': { bgcolor: '#1d4ed8', borderRadius: 3 } }} />
        <Typography sx={{ fontSize: '0.68rem', color: '#1d4ed8', fontWeight: 600, minWidth: 30 }}>
          {pct}%
        </Typography>
      </Box>
    </Box>
  )
}

// ── Sales Export tab ──────────────────────────────────────────────────────────

function SalesExportTab() {
  const qc = useQueryClient()
  const [selectedRun, setSelectedRun] = useState('')
  const [detail, setDetail]           = useState<ExportStore | null>(null)
  const [killing, setKilling]         = useState(false)

  // Runs list (batch dropdown)
  const { data: runsData, isFetching: runsFetching } = useQuery<{ items: ExportRun[] }>({
    queryKey: ['export-runs'],
    queryFn: () => apiClient.get('/api/sales-export/runs').then(r => r.data),
    refetchInterval: 15_000,
  })
  const runs = runsData?.items ?? []

  // Auto-select newest on first load
  useEffect(() => {
    if (runs.length > 0 && !selectedRun)
      setSelectedRun(runs[0].run_id)
  }, [runs, selectedRun])

  const activeRun = runs.find(r => r.run_id === selectedRun)

  // Stores for selected run
  const { data: storesData, isLoading: storesLoading, isFetching: storesFetching } = useQuery<{
    stores: ExportStore[]
  }>({
    queryKey: ['export-stores', selectedRun],
    queryFn: () => apiClient.get(`/api/sales-export/runs/${selectedRun}/stores`).then(r => r.data),
    enabled: !!selectedRun,
    refetchInterval: 8_000,
  })
  const stores = storesData?.stores ?? []

  // Live progress
  const { data: progress } = useQuery<Progress>({
    queryKey: ['export-progress'],
    queryFn: () => apiClient.get('/api/sales-export/progress').then(r => r.data),
    refetchInterval: 2_000,
  })

  // Track previous active state to detect when an export finishes/is killed
  const prevActiveRef = useRef<boolean>(false)

  useEffect(() => {
    const isActive = !!progress?.active
    const wasActive = prevActiveRef.current
    prevActiveRef.current = isActive

    if (isActive) {
      // Export is running — keep stores + runs fresh
      qc.invalidateQueries({ queryKey: ['export-stores', selectedRun] })
      qc.invalidateQueries({ queryKey: ['export-runs'] })
    } else if (wasActive && !isActive) {
      // Export just finished or was killed — force immediate refresh so
      // the batch dropdown status badge updates straight away
      qc.invalidateQueries({ queryKey: ['export-runs'] })
      qc.invalidateQueries({ queryKey: ['export-stores', selectedRun] })
    }
  }, [progress, selectedRun, qc])

  const killMutation = useMutation({
    mutationFn: () => apiClient.post('/api/sales-export/kill'),
    onMutate: () => setKilling(true),
    onSuccess: () => {
      // Poll more aggressively for a few seconds after kill so the UI
      // picks up the cancelled status as quickly as possible
      const ids = [500, 1500, 3000, 6000]
      ids.forEach(ms => setTimeout(() => {
        qc.invalidateQueries({ queryKey: ['export-progress'] })
        qc.invalidateQueries({ queryKey: ['export-runs'] })
        qc.invalidateQueries({ queryKey: ['export-stores', selectedRun] })
      }, ms))
    },
    onSettled: () => setKilling(false),
  })

  const isRunning   = progress?.active && progress.run_id === selectedRun
  const totalStores = activeRun?.total_stores ?? stores.length
  const okCount     = stores.filter(s => s.status === 'success').length
  const skipCount   = stores.filter(s => s.status === 'skipped').length
  const failCount   = stores.filter(s => s.status === 'failed').length
  const totalRows   = stores.reduce((a, s) => a + s.written_rows, 0)

  return (
    <Box>
      {/* Live progress banner */}
      {progress?.active && (
        <ProgressBanner
          progress={progress}
          onKill={() => killMutation.mutate()}
          killing={killing}
        />
      )}

      {/* Batch / run selector */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <FolderOutlinedIcon sx={{ fontSize: 15, color: '#9ca3af' }} />
        <Typography sx={{ fontSize: '0.72rem', color: '#374151', fontWeight: 600 }}>Batch:</Typography>
        <FormControl size="small" sx={{ minWidth: 300 }}>
          <Select value={selectedRun} displayEmpty
            onChange={e => setSelectedRun(e.target.value)}
            sx={{ fontSize: '0.78rem' }}
            renderValue={v => {
              if (!v) return <em style={{ color: '#9ca3af' }}>Select an export run…</em>
              const r = runs.find(x => x.run_id === v)
              if (!r) return v
              return (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <span>{r.label}</span>
                  <StatusBadge status={r.status} />
                  <Typography component="span" sx={{ fontSize: '0.65rem', color: '#9ca3af', ml: 0.5 }}>
                    ({r.total_stores} stores)
                  </Typography>
                </Box>
              )
            }}>
            {runs.map(r => (
              <MenuItem key={r.run_id} value={r.run_id}>
                <Box sx={{ width: '100%' }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 1 }}>
                    <Typography sx={{ fontSize: '0.8rem', fontWeight: 500 }}>{r.label}</Typography>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <StatusBadge status={r.status} />
                      <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af' }}>
                        {fmtDateTime(r.started_at)}
                      </Typography>
                    </Box>
                  </Box>
                  <Box sx={{ display: 'flex', gap: 1, mt: 0.25 }}>
                    <Typography sx={{ fontSize: '0.65rem', color: '#15803d' }}>✓ {r.processed_stores}/{r.total_stores} stores</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#6b7280' }}>via {r.triggered_by}</Typography>
                  </Box>
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        {(runsFetching || storesFetching) && <CircularProgress size={12} />}

        {/* Kill button (always visible, dims if no active run) */}
        {!progress?.active && (
          <Tooltip title="No export currently running">
            <span>
              <Button size="small" variant="outlined" disabled
                startIcon={<StopCircleOutlinedIcon sx={{ fontSize: 14 }} />}
                sx={{ height: 28, fontSize: '0.72rem', ml: 1 }}>
                Kill Export
              </Button>
            </span>
          </Tooltip>
        )}

        <Box sx={{ flexGrow: 1 }} />

        <Tooltip title="Refresh">
          <IconButton size="small"
            onClick={() => {
              qc.invalidateQueries({ queryKey: ['export-runs'] })
              qc.invalidateQueries({ queryKey: ['export-stores', selectedRun] })
              qc.invalidateQueries({ queryKey: ['export-progress'] })
            }}
            sx={{ borderRadius: '4px', border: '1px solid #e5e7eb' }}>
            <RefreshIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Summary pills */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        {[
          { label: 'total stores', val: totalStores, color: '#6b7280' },
          { label: 'uploaded',     val: okCount,     color: '#22c55e' },
          { label: 'no data',      val: skipCount,   color: '#f59e0b' },
          { label: 'failed',       val: failCount,   color: '#ef4444' },
          { label: 'rows written', val: totalRows,   color: '#3b82f6' },
        ].map(p => (
          <Box key={p.label} sx={{ display: 'flex', alignItems: 'center', gap: 0.75,
            px: 1.5, py: 0.5, border: '1px solid #e5e7eb', borderRadius: '6px', bgcolor: 'white' }}>
            <Box sx={{ width: 7, height: 7, borderRadius: '50%', bgcolor: p.color }} />
            <Typography sx={{ fontSize: '0.72rem', color: '#374151' }}>
              <b>{p.val}</b> {p.label}
            </Typography>
          </Box>
        ))}
        {isRunning && <CircularProgress size={13} sx={{ ml: 0.5 }} />}
      </Box>

      {/* Stores table */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <TableContainer>
          <Table size="small" sx={{ tableLayout: 'fixed', minWidth: 860 }}>
            <TableHead>
              <TableRow sx={{ bgcolor: '#f9fafb' }}>
                <TableCell sx={thSx} width={50}>#</TableCell>
                <TableCell sx={thSx} width={80}>Store</TableCell>
                <TableCell sx={thSx}>Store Name</TableCell>
                <TableCell sx={thSx} width={90} align="center">Query Rows</TableCell>
                <TableCell sx={thSx} width={90} align="center">Written</TableCell>
                <TableCell sx={thSx} width={80}>Status</TableCell>
                <TableCell sx={thSx}>Filename</TableCell>
                <TableCell sx={thSx} width={90} align="right">Duration</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {!selectedRun ? (
                <TableRow>
                  <TableCell colSpan={8} align="center"
                    sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                    Select an export run above to view results.
                  </TableCell>
                </TableRow>
              ) : storesLoading ? (
                <TableRow>
                  <TableCell colSpan={8} align="center" sx={{ py: 6 }}>
                    <CircularProgress size={22} />
                  </TableCell>
                </TableRow>
              ) : stores.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} align="center"
                    sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                    No store records yet — export may still be running.
                  </TableCell>
                </TableRow>
              ) : stores.map((s, i) => (
                <TableRow key={s.id} onClick={() => setDetail(s)}
                  sx={{ cursor: 'pointer', '&:hover': { bgcolor: '#f0f7ff' }, transition: 'background 0.1s' }}>
                  <TableCell sx={tdSx}>{i + 1}</TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontWeight: 600 }}>
                    {s.store_no ?? '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {s.store_name || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center', fontFamily: 'monospace' }}>
                    {s.query_rows}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center', fontFamily: 'monospace',
                    color: s.written_rows > 0 ? '#15803d' : '#9ca3af', fontWeight: s.written_rows > 0 ? 600 : 400 }}>
                    {s.written_rows}
                  </TableCell>
                  <TableCell sx={tdSx}><StatusBadge status={s.status} /></TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem',
                    color: '#6b7280', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {s.filename || (s.status === 'skipped' ? 'No file — no data' : '—')}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'right', color: '#9ca3af', fontFamily: 'monospace',
                    fontSize: '0.68rem', whiteSpace: 'nowrap' }}>
                    {s.duration_ms != null ? `${s.duration_ms.toFixed(0)} ms` : '—'}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Box>

      <StoreDetailDialog store={detail} onClose={() => setDetail(null)} />
    </Box>
  )
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function ExportsPage() {
  const [tab, setTab] = useState(0)

  return (
    <Box sx={{ p: { xs: 2, sm: 3 }, display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Box>
        <Typography sx={{ fontWeight: 600, fontSize: '0.875rem', color: '#111827' }}>
          Export Records
        </Typography>
        <Typography sx={{ fontSize: '0.75rem', color: '#9ca3af' }}>
          Sales data exported per store to FTP — click a row for details.
        </Typography>
      </Box>

      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <Tabs value={tab} onChange={(_, v) => setTab(v)}
          sx={{
            borderBottom: '1px solid #f3f4f6', minHeight: 42, px: 1,
            '& .MuiTab-root': { minHeight: 42, fontSize: '0.8rem', px: 2 },
            '& .Mui-selected': { color: '#1a56db', fontWeight: 600 },
          }}>
          <Tab label="Sales Export" />
        </Tabs>
        <Box sx={{ p: { xs: 1.5, sm: 2.5 } }}>
          {tab === 0 && <SalesExportTab />}
        </Box>
      </Box>
    </Box>
  )
}
