import { useState, useEffect, useRef } from 'react'
import {
  Box, Typography, Tabs, Tab, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, Chip, CircularProgress,
  TextField, Select, MenuItem, FormControl, InputLabel,
  IconButton, Tooltip, Button, Dialog,
  DialogContent, DialogActions, Alert, Snackbar,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import StopCircleOutlinedIcon from '@mui/icons-material/StopCircleOutlined'
import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
import HourglassTopIcon from '@mui/icons-material/HourglassTop'
import FolderOutlinedIcon from '@mui/icons-material/FolderOutlined'
import FileDownloadOutlinedIcon from '@mui/icons-material/FileDownloadOutlined'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import apiClient from '../api/client'
import { fmtDateTime } from '../utils/time'

// ── Types ─────────────────────────────────────────────────────────────────────

interface DocItem {
  id: string
  document_type: string
  original_data: Record<string, unknown>
  retailprosid: string | null
  posted: boolean
  has_error: boolean
  error_message: string | null
  created_at: string
  posted_at: string | null
  source_file: string | null
}

interface DocsResponse {
  total: number
  items: DocItem[]
}

interface Batch {
  source_file: string
  count: number
  latest: string | null
  posted: number
  errors: number
}

// ── Date helper (PKT) ─────────────────────────────────────────────────────────
const fmt = fmtDateTime

// ── Cell helper ───────────────────────────────────────────────────────────────

const cell = (doc: DocItem, key: string): string =>
  String(doc.original_data?.[key] ?? '—')

// ── Status chip ───────────────────────────────────────────────────────────────

function StatusChip({ doc }: { doc: DocItem }) {
  if (doc.posted)
    return (
      <Chip icon={<CheckCircleOutlinedIcon sx={{ fontSize: '11px !important' }} />}
        label="Posted" size="small"
        sx={{ height: 20, fontSize: '0.68rem', borderRadius: '4px',
          bgcolor: '#f0fdf4', color: '#15803d', border: '1px solid #d1fae5',
          '& .MuiChip-icon': { ml: '5px' } }} />
    )
  if (doc.has_error)
    return (
      <Chip icon={<ErrorOutlinedIcon sx={{ fontSize: '11px !important' }} />}
        label="Error" size="small"
        sx={{ height: 20, fontSize: '0.68rem', borderRadius: '4px',
          bgcolor: '#fef2f2', color: '#b91c1c', border: '1px solid #fecaca',
          '& .MuiChip-icon': { ml: '5px' } }} />
    )
  return (
    <Chip icon={<HourglassTopIcon sx={{ fontSize: '11px !important' }} />}
      label="Pending" size="small"
      sx={{ height: 20, fontSize: '0.68rem', borderRadius: '4px',
        bgcolor: '#fffbeb', color: '#b45309', border: '1px solid #fde68a',
        '& .MuiChip-icon': { ml: '5px' } }} />
  )
}

// ── Info row (detail dialog) ──────────────────────────────────────────────────

function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1,
      py: 0.9, borderBottom: '1px solid #f3f4f6', '&:last-child': { border: 0 } }}>
      <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af', minWidth: 120, flexShrink: 0 }}>
        {label}
      </Typography>
      <Typography sx={{ fontSize: '0.78rem', color: '#111827', fontWeight: 500,
        wordBreak: 'break-word', flex: 1 }}>
        {value}
      </Typography>
    </Box>
  )
}

// ── Detail dialog ─────────────────────────────────────────────────────────────

function DetailDialog({ doc, onClose }: { doc: DocItem | null; onClose: () => void }) {
  if (!doc) return null

  const upc  = cell(doc, 'UPC')
  const desc = cell(doc, 'DESCRIPTION1') !== '—' ? cell(doc, 'DESCRIPTION1') : cell(doc, 'DESCRIPTION')
  const dcs  = cell(doc, 'DCS_CODE')
  const vend = cell(doc, 'VEND_CODE')
  const alu  = cell(doc, 'ALU')

  // Payload sent to RetailPro (only present on error records).
  // Stored as a pre-formatted JSON string to preserve key order from Python.
  const payloadSentRaw = doc.original_data?._payload_sent
  const payloadSentStr = typeof payloadSentRaw === 'string'
    ? payloadSentRaw
    : payloadSentRaw != null ? JSON.stringify(payloadSentRaw, null, 2) : null

  // DCS / Vendor debug blobs (always present after the fix).
  const dcsDebugRaw  = doc.original_data?._dcs_debug
  const vendDebugRaw = doc.original_data?._vend_debug
  const dcsDebugStr  = typeof dcsDebugRaw  === 'string' ? dcsDebugRaw  : dcsDebugRaw  != null ? JSON.stringify(dcsDebugRaw,  null, 2) : null
  const vendDebugStr = typeof vendDebugRaw === 'string' ? vendDebugRaw : vendDebugRaw != null ? JSON.stringify(vendDebugRaw, null, 2) : null

  // Derive a short status label for DCS / vendor from the debug blob
  const _sidLabel = (debugStr: string | null): { label: string; ok: boolean } => {
    if (!debugStr) return { label: 'no debug data', ok: false }
    try {
      const d = JSON.parse(debugStr)
      if (d.skipped)    return { label: `skipped — ${d.skipped}`, ok: false }
      if (d.source === 'cache') return { label: `cache hit · SID ${d.final_sid ?? '?'}`, ok: true }
      if (d.final_sid)  return { label: `SID ${d.final_sid}`, ok: true }
      return { label: 'SID not resolved', ok: false }
    } catch { return { label: 'parse error', ok: false } }
  }
  const dcsStatus  = _sidLabel(dcsDebugStr)
  const vendStatus = _sidLabel(vendDebugStr)

  // Pretty-print error
  let errorDisplay = doc.error_message || ''
  try { errorDisplay = JSON.stringify(JSON.parse(doc.error_message || ''), null, 2) } catch { /* raw */ }

  return (
    <Dialog open onClose={onClose} maxWidth="sm" fullWidth
      slotProps={{ paper: { sx: { borderRadius: '8px', overflow: 'hidden' } } }}>

      {/* Header */}
      <Box sx={{
        bgcolor: doc.has_error ? '#fef2f2' : doc.posted ? '#f0fdf4' : '#fffbeb',
        borderBottom: `2px solid ${doc.has_error ? '#fecaca' : doc.posted ? '#bbf7d0' : '#fde68a'}`,
        px: 3, py: 2,
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
          <StatusChip doc={doc} />
          <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af', fontFamily: 'monospace' }}>
            {fmt(doc.created_at)}
          </Typography>
        </Box>
        <Typography sx={{ fontWeight: 700, fontSize: '1rem', color: '#111827', lineHeight: 1.3, mt: 0.5 }}>
          Item Details
        </Typography>
        {desc !== '—' && (
          <Typography sx={{ fontSize: '0.78rem', color: '#6b7280', mt: 0.25 }}>{desc}</Typography>
        )}
      </Box>

      <DialogContent sx={{ p: 0, maxHeight: '72vh', overflowY: 'auto' }}>
        {/* Item details */}
        <Box sx={{ px: 3, pt: 2, pb: 1 }}>
          <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
            textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>
            Item Details
          </Typography>
          <InfoRow label="UPC"          value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.78rem', fontWeight: 500 }}>{upc}</Typography>} />
          {alu  !== '—' && <InfoRow label="ALU"           value={alu} />}
          {desc !== '—' && <InfoRow label="Description 1" value={desc} />}
          <InfoRow label="DCS Code"    value={dcs} />
          <InfoRow label="Vendor Code" value={vend} />
          <InfoRow label="Source File" value={doc.source_file || '—'} />
          {doc.retailprosid && (
            <InfoRow label="RetailPro SID"
              value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem',
                color: '#15803d' }}>{doc.retailprosid}</Typography>} />
          )}
          {doc.posted_at && <InfoRow label="Posted At" value={fmt(doc.posted_at)} />}
        </Box>

        {/* Error response */}
        {doc.error_message && (
          <Box sx={{ mx: 3, mb: 2, mt: 1 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 1 }}>
              <ErrorOutlinedIcon sx={{ fontSize: 14, color: '#b91c1c' }} />
              <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#b91c1c',
                textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                API Error Response
              </Typography>
            </Box>
            <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: '6px',
              p: 1.5, maxHeight: 200, overflow: 'auto' }}>
              <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace',
                whiteSpace: 'pre-wrap', color: '#7f1d1d', wordBreak: 'break-word',
                lineHeight: 1.6 }}>
                {errorDisplay}
              </Typography>
            </Box>
          </Box>
        )}

        {/* Payload sent */}
        {payloadSentStr && (
          <Box sx={{ mx: 3, mb: 2, mt: doc.error_message ? 0 : 1 }}>
            <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#374151',
              textTransform: 'uppercase', letterSpacing: '0.06em', mb: 1 }}>
              Payload Sent to RetailPro
            </Typography>
            <Box sx={{ bgcolor: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '6px',
              p: 1.5, maxHeight: 260, overflow: 'auto' }}>
              <Typography sx={{ fontSize: '0.7rem', fontFamily: 'monospace',
                whiteSpace: 'pre-wrap', color: '#1e293b', wordBreak: 'break-word',
                lineHeight: 1.6 }}>
                {payloadSentStr}
              </Typography>
            </Box>
          </Box>
        )}

        {/* DCS API debug */}
        {dcsDebugStr && (
          <Box sx={{ mx: 3, mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 1 }}>
              <Box sx={{ width: 7, height: 7, borderRadius: '50%',
                bgcolor: dcsStatus.ok ? '#22c55e' : '#f59e0b', flexShrink: 0 }} />
              <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#374151',
                textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                DCS API
              </Typography>
              <Typography sx={{ fontSize: '0.68rem', color: dcsStatus.ok ? '#15803d' : '#b45309',
                ml: 0.5, fontFamily: 'monospace' }}>
                {dcsStatus.label}
              </Typography>
            </Box>
            <Box sx={{ bgcolor: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '6px',
              p: 1.5, maxHeight: 220, overflow: 'auto' }}>
              <Typography sx={{ fontSize: '0.7rem', fontFamily: 'monospace',
                whiteSpace: 'pre-wrap', color: '#1e293b', wordBreak: 'break-word',
                lineHeight: 1.6 }}>
                {dcsDebugStr}
              </Typography>
            </Box>
          </Box>
        )}

        {/* Vendor API debug */}
        {vendDebugStr && (
          <Box sx={{ mx: 3, mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 1 }}>
              <Box sx={{ width: 7, height: 7, borderRadius: '50%',
                bgcolor: vendStatus.ok ? '#22c55e' : '#f59e0b', flexShrink: 0 }} />
              <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#374151',
                textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                Vendor API
              </Typography>
              <Typography sx={{ fontSize: '0.68rem', color: vendStatus.ok ? '#15803d' : '#b45309',
                ml: 0.5, fontFamily: 'monospace' }}>
                {vendStatus.label}
              </Typography>
            </Box>
            <Box sx={{ bgcolor: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '6px',
              p: 1.5, maxHeight: 220, overflow: 'auto' }}>
              <Typography sx={{ fontSize: '0.7rem', fontFamily: 'monospace',
                whiteSpace: 'pre-wrap', color: '#1e293b', wordBreak: 'break-word',
                lineHeight: 1.6 }}>
                {vendDebugStr}
              </Typography>
            </Box>
          </Box>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2, pt: 1, borderTop: '1px solid #f3f4f6' }}>
        <Button size="small" variant="outlined" onClick={onClose}
          sx={{ height: 30, fontSize: '0.78rem' }}>
          Close
        </Button>
      </DialogActions>
    </Dialog>
  )
}

// ── Stat pill ─────────────────────────────────────────────────────────────────

function Pill({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75,
      px: 1.5, py: 0.5, border: '1px solid #e5e7eb', borderRadius: '6px', bgcolor: 'white' }}>
      <Box sx={{ width: 7, height: 7, borderRadius: '50%', bgcolor: color }} />
      <Typography sx={{ fontSize: '0.72rem', color: '#374151' }}>
        <b>{value}</b> {label}
      </Typography>
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

// ── Item Master tab ───────────────────────────────────────────────────────────

function ItemMasterTab() {
  const qc = useQueryClient()
  const [page, setPage]           = useState(0)
  const pageSize                   = 100
  const [status, setStatus]       = useState('')
  const [search, setSearch]       = useState('')
  const [detail, setDetail]       = useState<DocItem | null>(null)
  const [selectedBatch, setSelectedBatch] = useState<string>('')
  const [uploading, setUploading] = useState(false)
  const [killing, setKilling]     = useState(false)
  const [toast, setToast]         = useState<{ msg: string; severity: 'success' | 'error' | 'warning' } | null>(null)
  const abortRef                  = useRef<AbortController | null>(null)

  // Poll status endpoint — catches both manual uploads AND FTP-triggered runs
  const { data: runStatus } = useQuery<{ running: boolean }>({
    queryKey: ['im-status'],
    queryFn: () => apiClient.get('/api/item-master/status').then(r => r.data),
    refetchInterval: 3_000,
  })
  const isRunning = uploading || !!runStatus?.running

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    const controller = new AbortController()
    abortRef.current = controller
    setUploading(true)
    setKilling(false)
    try {
      const form = new FormData()
      form.append('file', file)
      const res = await apiClient.post('/api/item-master/import-csv', form, {
        headers: { 'Content-Type': 'multipart/form-data' },
        signal: controller.signal,
      })
      const d = res.data
      if (d.cancelled) {
        setToast({ severity: 'warning', msg: `Stopped — ${d.total} of ${d.of_total} rows processed (${d.created} created, ${d.updated} updated, ${d.errors} errors)` })
      } else {
        setToast({ severity: 'success', msg: `Done — ${d.total} rows, ${d.created} created, ${d.updated} updated, ${d.errors} errors` })
      }
      qc.invalidateQueries({ queryKey: ['im-batches'] })
      qc.invalidateQueries({ queryKey: ['imports-im'] })
    } catch (err: unknown) {
      const isCancelled = (err as { name?: string })?.name === 'CanceledError'
                       || (err as { code?: string })?.code === 'ERR_CANCELED'
      if (!isCancelled) {
        const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || String(err)
        setToast({ severity: 'error', msg })
      }
      qc.invalidateQueries({ queryKey: ['im-batches'] })
      qc.invalidateQueries({ queryKey: ['imports-im'] })
    } finally {
      abortRef.current = null
      setUploading(false)
      setKilling(false)
    }
  }

  const handleKill = async () => {
    setKilling(true)
    try {
      await apiClient.post('/api/item-master/kill')
    } catch { /* ignore */ }
    abortRef.current?.abort()
  }

  const [downloading, setDownloading] = useState(false)

  const handleDownloadCsv = async () => {
    if (!selectedBatch) return
    setDownloading(true)
    try {
      const params = new URLSearchParams({ source_file: selectedBatch })
      const res = await apiClient.get(`/api/item-master/batch-download?${params}`, {
        responseType: 'blob',
      })
      const url = URL.createObjectURL(res.data as Blob)
      const a = document.createElement('a')
      a.href = url
      const stem = selectedBatch.split('::')[0].replace(/\.[^.]+$/, '')
      a.download = `${stem}_processed.csv`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch {
      setToast({ severity: 'error', msg: 'Failed to download processed CSV.' })
    } finally {
      setDownloading(false)
    }
  }

  // ── Batches list ─────────────────────────────────────────────────────────
  const { data: batches, isFetching: batchFetching } = useQuery<Batch[]>({
    queryKey: ['im-batches'],
    queryFn: () =>
      apiClient.get('/api/documents/batches', { params: { document_type: 'item_master' } })
        .then(r => r.data),
    refetchInterval: 30_000,
  })

  // Auto-select the most-recent batch on first load
  useEffect(() => {
    if (batches && batches.length > 0 && !selectedBatch) {
      setSelectedBatch(batches[0].source_file)
    }
  }, [batches, selectedBatch])

  // Active batch stats from the batches list
  const activeBatch = batches?.find(b => b.source_file === selectedBatch)

  // ── Documents for the selected batch ────────────────────────────────────
  const params: Record<string, string | number> = {
    document_type: 'item_master',
    limit: pageSize,
    offset: page * pageSize,
    ...(selectedBatch ? { source_file: selectedBatch } : {}),
    ...(status        ? { status }                     : {}),
  }

  const { data, isLoading, isFetching } = useQuery<DocsResponse>({
    queryKey: ['imports-im', params],
    queryFn: () => apiClient.get('/api/documents', { params }).then(r => r.data),
    refetchInterval: isRunning ? 3_000 : 30_000,
    enabled: !!selectedBatch,
  })

  const items    = data?.items ?? []
  const filtered = search
    ? items.filter(d => JSON.stringify(d).toLowerCase().includes(search.toLowerCase()))
    : items

  const totalPages = Math.ceil((data?.total ?? 0) / pageSize)

  const posted  = activeBatch?.posted  ?? 0
  const errors  = activeBatch?.errors  ?? 0
  const total   = activeBatch?.count   ?? 0
  const pending = total - posted - errors

  return (
    <Box>
      {/* Batch selector */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <FolderOutlinedIcon sx={{ fontSize: 15, color: '#9ca3af' }} />
        <Typography sx={{ fontSize: '0.72rem', color: '#374151', fontWeight: 600 }}>Batch:</Typography>
        <FormControl size="small" sx={{ minWidth: 260 }}>
          <Select value={selectedBatch} displayEmpty
            onChange={e => { setSelectedBatch(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}
            renderValue={v => {
              if (!v) return <em style={{ color: '#9ca3af' }}>Select a batch…</em>
              const b = batches?.find(x => x.source_file === v)
              return (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <span>{v}</span>
                  {b && (
                    <Typography component="span"
                      sx={{ fontSize: '0.65rem', color: '#9ca3af', ml: 0.5 }}>
                      ({b.count} rows · {fmt(b.latest)})
                    </Typography>
                  )}
                </Box>
              )
            }}>
            {(batches ?? []).map(b => (
              <MenuItem key={b.source_file} value={b.source_file}>
                <Box sx={{ width: '100%' }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Typography sx={{ fontSize: '0.8rem', fontWeight: 500 }}>{b.source_file}</Typography>
                    <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af' }}>{fmt(b.latest)}</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', gap: 1, mt: 0.25 }}>
                    <Typography sx={{ fontSize: '0.65rem', color: '#15803d' }}>✓ {b.posted} posted</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#b91c1c' }}>✗ {b.errors} errors</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#6b7280' }}>{b.count} total</Typography>
                  </Box>
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        {batchFetching && <CircularProgress size={12} />}
      </Box>

      {/* Stat pills + toolbar */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <Pill label="posted"  value={posted}  color="#22c55e" />
        <Pill label="errors"  value={errors}  color="#ef4444" />
        <Pill label="pending" value={pending} color="#f59e0b" />
        <Pill label="total"   value={total}   color="#6b7280" />
        {isFetching && <CircularProgress size={13} sx={{ ml: 0.5 }} />}

        <Box sx={{ flexGrow: 1 }} />

        <TextField size="small" placeholder="Search…" value={search}
          onChange={e => setSearch(e.target.value)}
          sx={{ width: 170, '& .MuiOutlinedInput-input': { fontSize: '0.78rem', py: '5px' } }} />

        <FormControl size="small" sx={{ minWidth: 110 }}>
          <InputLabel sx={{ fontSize: '0.78rem' }}>Status</InputLabel>
          <Select value={status} label="Status"
            onChange={e => { setStatus(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="posted">Posted</MenuItem>
            <MenuItem value="error">Error</MenuItem>
            <MenuItem value="pending">Pending</MenuItem>
          </Select>
        </FormControl>

        {/* Manual CSV upload / Kill */}
        {isRunning ? (
          <Box sx={{ display: 'flex', gap: 0.75 }}>
            <Button size="small" variant="contained" disabled
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#1a56db', pointerEvents: 'none' }}>
              <CircularProgress size={12} sx={{ color: 'white', mr: 0.75 }} />
              Processing…
            </Button>
            <Tooltip title="Stop import after current row">
              <Button size="small" variant="contained" disabled={killing}
                onClick={handleKill}
                startIcon={killing
                  ? <CircularProgress size={12} sx={{ color: 'white' }} />
                  : <StopCircleOutlinedIcon sx={{ fontSize: 15 }} />}
                sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                  bgcolor: '#dc2626', '&:hover': { bgcolor: '#b91c1c' } }}>
                {killing ? 'Stopping…' : 'Stop'}
              </Button>
            </Tooltip>
          </Box>
        ) : (
          <Tooltip title="Upload CSV manually">
            <Button component="label" size="small" variant="contained"
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#1a56db', '&:hover': { bgcolor: '#1e40af' } }}>
              Upload CSV
              <input type="file" accept=".csv" hidden onChange={handleUpload} />
            </Button>
          </Tooltip>
        )}

        <Tooltip title="Download processed CSV with RetailPro UPCs">
          <span>
            <Button
              size="small"
              variant="outlined"
              disabled={!selectedBatch || downloading}
              onClick={handleDownloadCsv}
              startIcon={downloading
                ? <CircularProgress size={12} />
                : <FileDownloadOutlinedIcon sx={{ fontSize: 15 }} />}
              sx={{
                height: 30, fontSize: '0.75rem', textTransform: 'none',
                borderColor: '#d1d5db', color: '#374151',
                '&:hover': { borderColor: '#9ca3af', bgcolor: '#f9fafb' },
              }}>
              {downloading ? 'Downloading…' : 'Download Processed CSV'}
            </Button>
          </span>
        </Tooltip>

        <Tooltip title="Refresh">
          <IconButton size="small"
            onClick={() => {
              qc.invalidateQueries({ queryKey: ['imports-im'] })
              qc.invalidateQueries({ queryKey: ['im-batches'] })
            }}
            sx={{ borderRadius: '4px', border: '1px solid #e5e7eb' }}>
            <RefreshIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Table */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px',
        overflow: 'hidden' }}>
        <TableContainer>
          <Table size="small" sx={{ tableLayout: 'fixed', minWidth: 860 }}>
            <TableHead>
              <TableRow sx={{ bgcolor: '#f9fafb' }}>
                <TableCell sx={thSx} width={44}>#</TableCell>
                <TableCell sx={thSx} width={130}>UPC</TableCell>
                <TableCell sx={thSx}>Description</TableCell>
                <TableCell sx={thSx} width={110}>DCS Code</TableCell>
                <TableCell sx={thSx} width={100}>Vendor</TableCell>
                <TableCell sx={thSx} width={80}>Status</TableCell>
                <TableCell sx={thSx} width={150}>RetailPro SID</TableCell>
                <TableCell sx={thSx} width={140}>Imported At</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {!selectedBatch ? (
                <TableRow>
                  <TableCell colSpan={8} align="center"
                    sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                    Select a batch above to view records.
                  </TableCell>
                </TableRow>
              ) : isLoading ? (
                <TableRow>
                  <TableCell colSpan={8} align="center" sx={{ py: 6 }}>
                    <CircularProgress size={22} />
                  </TableCell>
                </TableRow>
              ) : filtered.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} align="center"
                    sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                    No records found.
                  </TableCell>
                </TableRow>
              ) : filtered.map((doc, i) => (
                <TableRow key={doc.id} onClick={() => setDetail(doc)}
                  sx={{ cursor: 'pointer', '&:hover': { bgcolor: '#f0f7ff' },
                    transition: 'background 0.1s' }}>
                  <TableCell sx={tdSx}>{page * pageSize + i + 1}</TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem' }}>
                    {cell(doc, 'UPC')}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, overflow: 'hidden',
                    textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {cell(doc, 'DESCRIPTION1') !== '—'
                      ? cell(doc, 'DESCRIPTION1')
                      : cell(doc, 'DESCRIPTION')}
                  </TableCell>
                  <TableCell sx={tdSx}>{cell(doc, 'DCS_CODE')}</TableCell>
                  <TableCell sx={tdSx}>{cell(doc, 'VEND_CODE')}</TableCell>
                  <TableCell sx={tdSx}><StatusChip doc={doc} /></TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem',
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                    color: '#6b7280' }}>
                    {doc.retailprosid || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, whiteSpace: 'nowrap', color: '#6b7280' }}>
                    {fmt(doc.created_at)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        {/* Pagination */}
        {(data?.total ?? 0) > pageSize && (
          <Box sx={{ px: 2, py: 1, borderTop: '1px solid #f3f4f6',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
              Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, data!.total)} of {data!.total}
            </Typography>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button size="small" variant="outlined" disabled={page === 0}
                onClick={() => setPage(p => p - 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Prev</Button>
              <Button size="small" variant="outlined" disabled={page >= totalPages - 1}
                onClick={() => setPage(p => p + 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Next</Button>
            </Box>
          </Box>
        )}
      </Box>

      <DetailDialog doc={detail} onClose={() => setDetail(null)} />

      <Snackbar open={!!toast} autoHideDuration={5000} onClose={() => setToast(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert severity={toast?.severity ?? 'info'} onClose={() => setToast(null)}
          sx={{ fontSize: '0.8rem' }}>
          {toast?.msg}
        </Alert>
      </Snackbar>
    </Box>
  )
}

// ── QTY Adjustment types ──────────────────────────────────────────────────────

interface QtyAdjDoc {
  id: string
  source_file: string | null
  store_code: string | null
  store_name: string | null
  store_sid: string | null
  sbs_sid: string | null
  adj_sid: string | null
  note: string | null
  item_count: number
  posted_count: number
  error_count: number
  status: 'posted' | 'partial' | 'error' | 'pending'
  error_message: string | null
  api_create_payload: unknown
  api_create_response: unknown
  api_items_payload: unknown
  api_items_response: unknown
  api_get_response: unknown
  api_finalize_payload: unknown
  api_finalize_response: unknown
  api_comment_payload: unknown
  api_comment_response: unknown
  items_data: Array<{
    upc: string
    csv_delta: number | null
    current_qty: number | null
    adj_value: number
    item_sid: string | null
    ok: boolean
    error: string | null
  }> | null
  created_at: string
  posted_at: string | null
}

interface QtyAdjBatch {
  source_file: string
  doc_count: number
  total_items: number
  posted_items: number
  error_items: number
  latest: string | null
  posted_docs: number
  error_docs: number
}

// ── QTY Adj status chip ───────────────────────────────────────────────────────

function AdjStatusChip({ status }: { status: QtyAdjDoc['status'] }) {
  const map = {
    posted:  { label: 'Posted',  bg: '#f0fdf4', color: '#15803d', border: '#d1fae5' },
    partial: { label: 'Partial', bg: '#fff7ed', color: '#c2410c', border: '#fed7aa' },
    error:   { label: 'Error',   bg: '#fef2f2', color: '#b91c1c', border: '#fecaca' },
    pending: { label: 'Pending', bg: '#fffbeb', color: '#b45309', border: '#fde68a' },
  }
  const s = map[status] ?? map.pending
  return (
    <Chip label={s.label} size="small"
      sx={{ height: 20, fontSize: '0.68rem', borderRadius: '4px',
        bgcolor: s.bg, color: s.color, border: `1px solid ${s.border}` }} />
  )
}

// ── QTY Adj detail dialog ─────────────────────────────────────────────────────

function QtyAdjDetailDialog({ doc, onClose }: { doc: QtyAdjDoc | null; onClose: () => void }) {
  if (!doc) return null

  const section = (title: string, data: unknown, color = '#1e293b') => {
    if (!data) return null
    let text: string
    try { text = JSON.stringify(data, null, 2) } catch { text = String(data) }
    return (
      <Box sx={{ mx: 3, mb: 2 }}>
        <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#374151',
          textTransform: 'uppercase', letterSpacing: '0.06em', mb: 0.75 }}>
          {title}
        </Typography>
        <Box sx={{ bgcolor: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '6px',
          p: 1.5, maxHeight: 220, overflow: 'auto' }}>
          <Typography sx={{ fontSize: '0.7rem', fontFamily: 'monospace',
            whiteSpace: 'pre-wrap', color, wordBreak: 'break-word', lineHeight: 1.6 }}>
            {text}
          </Typography>
        </Box>
      </Box>
    )
  }

  return (
    <Dialog open onClose={onClose} maxWidth="md" fullWidth
      slotProps={{ paper: { sx: { borderRadius: '8px', overflow: 'hidden' } } }}>
      {/* Header */}
      <Box sx={{
        bgcolor: doc.status === 'error' ? '#fef2f2' : doc.status === 'posted' ? '#f0fdf4' : '#fff7ed',
        borderBottom: `2px solid ${doc.status === 'error' ? '#fecaca' : doc.status === 'posted' ? '#bbf7d0' : '#fed7aa'}`,
        px: 3, py: 2,
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
          <AdjStatusChip status={doc.status} />
          <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af', fontFamily: 'monospace' }}>
            {fmt(doc.created_at)}
          </Typography>
        </Box>
        <Typography sx={{ fontWeight: 700, fontSize: '1rem', color: '#111827', mt: 0.5 }}>
          Adjustment Document
        </Typography>
        <Typography sx={{ fontSize: '0.78rem', color: '#6b7280' }}>
          Store: {doc.store_name || doc.store_code || '—'} &nbsp;·&nbsp;
          {doc.item_count} items &nbsp;·&nbsp;
          {doc.posted_count} posted &nbsp;·&nbsp; {doc.error_count} errors
        </Typography>
      </Box>

      <DialogContent sx={{ p: 0, maxHeight: '76vh', overflowY: 'auto' }}>

        {/* ── Error message — shown first so it is immediately visible ── */}
        {doc.error_message && (
          <Box sx={{ mx: 3, mt: 2, mb: 1 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 0.75 }}>
              <ErrorOutlinedIcon sx={{ fontSize: 14, color: '#b91c1c' }} />
              <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#b91c1c',
                textTransform: 'uppercase', letterSpacing: '0.06em' }}>Error Details</Typography>
            </Box>
            <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: '6px', p: 1.5 }}>
              <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace', whiteSpace: 'pre-wrap',
                color: '#7f1d1d', wordBreak: 'break-word' }}>{doc.error_message}</Typography>
            </Box>
          </Box>
        )}

        {/* Summary */}
        <Box sx={{ px: 3, pt: doc.error_message ? 1 : 2, pb: 1 }}>
          <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
            textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>Summary</Typography>
          <InfoRow label="Adj SID"     value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{doc.adj_sid || '—'}</Typography>} />
          {doc.note && <InfoRow label="Note" value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{doc.note}</Typography>} />}
          <InfoRow label="Store Code"  value={doc.store_code || '—'} />
          <InfoRow label="Store SID"   value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.store_sid || '—'}</Typography>} />
          <InfoRow label="Sbs SID"     value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.sbs_sid || '—'}</Typography>} />
          <InfoRow label="Source File" value={doc.source_file || '—'} />
          {doc.posted_at && <InfoRow label="Posted At" value={fmt(doc.posted_at)} />}
        </Box>

        {/* Items table */}
        {doc.items_data && doc.items_data.length > 0 && (
          <Box sx={{ mx: 3, mb: 2 }}>
            <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
              textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>Items</Typography>
            <Box sx={{ border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ bgcolor: '#f9fafb' }}>
                    {['UPC', 'CSV Delta', 'Current Qty', 'Sent Value', 'Item SID', 'Status'].map(h => (
                      <TableCell key={h} sx={{ ...thSx, py: 0.5 }}>{h}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {doc.items_data.map((item, i) => (
                    <TableRow key={i}>
                      <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem' }}>{item.upc}</TableCell>
                      <TableCell sx={tdSx}>{item.csv_delta ?? '—'}</TableCell>
                      <TableCell sx={tdSx}>{item.current_qty ?? '—'}</TableCell>
                      <TableCell sx={{ ...tdSx, fontWeight: 600 }}>{item.adj_value}</TableCell>
                      <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem', color: '#6b7280' }}>{item.item_sid || '—'}</TableCell>
                      <TableCell sx={tdSx}>
                        {item.ok
                          ? <Chip label="OK" size="small" sx={{ height: 18, fontSize: '0.65rem', bgcolor: '#f0fdf4', color: '#15803d', border: '1px solid #d1fae5' }} />
                          : <Chip label={item.error || 'Error'} size="small" sx={{ height: 18, fontSize: '0.65rem', bgcolor: '#fef2f2', color: '#b91c1c', border: '1px solid #fecaca', maxWidth: 260 }} />
                        }
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </Box>
          </Box>
        )}

        {/* API traces */}
        {section('1. Create Adjustment — Request', doc.api_create_payload)}
        {section('1. Create Adjustment — Response', doc.api_create_response)}
        {section('2. Post Items — Request', doc.api_items_payload)}
        {section('2. Post Items — Response', doc.api_items_response)}
        {section('3. GET Rowversion — Response', doc.api_get_response)}
        {section('4. Finalize — Request', doc.api_finalize_payload)}
        {section('4. Finalize — Response', doc.api_finalize_response)}
        {section('5. Post Comment — Request', doc.api_comment_payload)}
        {section('5. Post Comment — Response', doc.api_comment_response)}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2, pt: 1, borderTop: '1px solid #f3f4f6' }}>
        <Button size="small" variant="outlined" onClick={onClose}
          sx={{ height: 30, fontSize: '0.78rem' }}>Close</Button>
      </DialogActions>
    </Dialog>
  )
}

// ── QTY Adjustment tab ────────────────────────────────────────────────────────

function QtyAdjustmentTab() {
  const qc = useQueryClient()
  const [selectedBatch, setSelectedBatch] = useState('')
  const [status, setStatus]               = useState('')
  const [detail, setDetail]               = useState<QtyAdjDoc | null>(null)
  const [page, setPage]                   = useState(0)
  const pageSize                           = 100
  const [uploading, setUploading]         = useState(false)
  const [killing, setKilling]             = useState(false)
  const [toast, setToast]                 = useState<{ msg: string; severity: 'success' | 'error' | 'warning' } | null>(null)
  const abortRef                          = useRef<AbortController | null>(null)

  const { data: runStatus } = useQuery<{ running: boolean }>({
    queryKey: ['qa-status'],
    queryFn: () => apiClient.get('/api/qty-adjustment/status').then(r => r.data),
    refetchInterval: 3_000,
  })
  const isRunning = uploading || !!runStatus?.running

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    const controller = new AbortController()
    abortRef.current = controller
    setUploading(true)
    setKilling(false)
    try {
      const form = new FormData()
      form.append('file', file)
      const res = await apiClient.post('/api/qty-adjustment/import', form, {
        headers: { 'Content-Type': 'multipart/form-data' },
        signal: controller.signal,
      })
      const d = res.data
      if (d.cancelled) {
        setToast({ severity: 'warning', msg: `Stopped — ${d.total_docs} docs processed (${d.posted_docs} posted, ${d.error_docs} errors)` })
      } else {
        setToast({ severity: 'success', msg: `Done — ${d.total_docs} docs, ${d.posted_docs} posted, ${d.error_docs} errors` })
      }
      qc.invalidateQueries({ queryKey: ['qa-batches'] })
      qc.invalidateQueries({ queryKey: ['qa-docs'] })
    } catch (err: unknown) {
      const isCancelled = (err as { name?: string })?.name === 'CanceledError'
                       || (err as { code?: string })?.code === 'ERR_CANCELED'
      if (!isCancelled) {
        const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || String(err)
        setToast({ severity: 'error', msg })
      }
      qc.invalidateQueries({ queryKey: ['qa-batches'] })
      qc.invalidateQueries({ queryKey: ['qa-docs'] })
    } finally {
      abortRef.current = null
      setUploading(false)
      setKilling(false)
    }
  }

  const handleKill = async () => {
    setKilling(true)
    try { await apiClient.post('/api/qty-adjustment/kill') } catch { /* ignore */ }
    abortRef.current?.abort()
  }

  const { data: batches, isFetching: batchFetching } = useQuery<QtyAdjBatch[]>({
    queryKey: ['qa-batches'],
    queryFn: () => apiClient.get('/api/qty-adjustment/batches').then(r => r.data),
    refetchInterval: 30_000,
  })

  useEffect(() => {
    if (batches && batches.length > 0 && !selectedBatch)
      setSelectedBatch(batches[0].source_file)
  }, [batches, selectedBatch])

  const activeBatch = batches?.find(b => b.source_file === selectedBatch)

  const params: Record<string, string | number> = {
    limit: pageSize, offset: page * pageSize,
    ...(selectedBatch ? { source_file: selectedBatch } : {}),
    ...(status        ? { status }                     : {}),
  }

  const { data, isLoading, isFetching } = useQuery<{ total: number; items: QtyAdjDoc[] }>({
    queryKey: ['qa-docs', params],
    queryFn: () => apiClient.get('/api/qty-adjustment/docs', { params }).then(r => r.data),
    refetchInterval: isRunning ? 3_000 : 30_000,
    enabled: !!selectedBatch,
  })

  const docs      = data?.items ?? []
  const totalPages = Math.ceil((data?.total ?? 0) / pageSize)

  return (
    <Box>
      {/* Batch selector */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <FolderOutlinedIcon sx={{ fontSize: 15, color: '#9ca3af' }} />
        <Typography sx={{ fontSize: '0.72rem', color: '#374151', fontWeight: 600 }}>Batch:</Typography>
        <FormControl size="small" sx={{ minWidth: 280 }}>
          <Select value={selectedBatch} displayEmpty
            onChange={e => { setSelectedBatch(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}
            renderValue={v => {
              if (!v) return <em style={{ color: '#9ca3af' }}>Select a batch…</em>
              const b = batches?.find(x => x.source_file === v)
              return (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <span>{v}</span>
                  {b && <Typography component="span" sx={{ fontSize: '0.65rem', color: '#9ca3af', ml: 0.5 }}>
                    ({b.doc_count} docs · {fmt(b.latest)})
                  </Typography>}
                </Box>
              )
            }}>
            {(batches ?? []).map(b => (
              <MenuItem key={b.source_file} value={b.source_file}>
                <Box sx={{ width: '100%' }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Typography sx={{ fontSize: '0.8rem', fontWeight: 500 }}>{b.source_file}</Typography>
                    <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af' }}>{fmt(b.latest)}</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', gap: 1, mt: 0.25 }}>
                    <Typography sx={{ fontSize: '0.65rem', color: '#15803d' }}>✓ {b.posted_docs} posted</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#b91c1c' }}>✗ {b.error_docs} errors</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#6b7280' }}>{b.doc_count} docs · {b.total_items} items</Typography>
                  </Box>
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        {batchFetching && <CircularProgress size={12} />}
      </Box>

      {/* Pills + toolbar */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <Pill label="adj docs"     value={activeBatch?.doc_count    ?? 0} color="#3b82f6" />
        <Pill label="posted docs"  value={activeBatch?.posted_docs  ?? 0} color="#22c55e" />
        <Pill label="error docs"   value={activeBatch?.error_docs   ?? 0} color="#ef4444" />
        <Pill label="total items"  value={activeBatch?.total_items  ?? 0} color="#6b7280" />
        <Pill label="posted items" value={activeBatch?.posted_items ?? 0} color="#22c55e" />
        {isFetching && <CircularProgress size={13} sx={{ ml: 0.5 }} />}
        <Box sx={{ flexGrow: 1 }} />
        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel sx={{ fontSize: '0.78rem' }}>Status</InputLabel>
          <Select value={status} label="Status"
            onChange={e => { setStatus(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="posted">Posted</MenuItem>
            <MenuItem value="partial">Partial</MenuItem>
            <MenuItem value="error">Error</MenuItem>
          </Select>
        </FormControl>
        {/* Manual CSV upload / Kill */}
        {isRunning ? (
          <Box sx={{ display: 'flex', gap: 0.75 }}>
            <Button size="small" variant="contained" disabled
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#1a56db', pointerEvents: 'none' }}>
              <CircularProgress size={12} sx={{ color: 'white', mr: 0.75 }} />
              Processing…
            </Button>
            <Tooltip title="Stop import after current store">
              <Button size="small" variant="contained" disabled={killing}
                onClick={handleKill}
                startIcon={killing
                  ? <CircularProgress size={12} sx={{ color: 'white' }} />
                  : <StopCircleOutlinedIcon sx={{ fontSize: 15 }} />}
                sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                  bgcolor: '#dc2626', '&:hover': { bgcolor: '#b91c1c' } }}>
                {killing ? 'Stopping…' : 'Stop'}
              </Button>
            </Tooltip>
          </Box>
        ) : (
          <Tooltip title="Upload CSV manually">
            <Button component="label" size="small" variant="contained"
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#1a56db', '&:hover': { bgcolor: '#1e40af' } }}>
              Upload CSV
              <input type="file" accept=".csv" hidden onChange={handleUpload} />
            </Button>
          </Tooltip>
        )}

        <Tooltip title="Refresh">
          <IconButton size="small"
            onClick={() => {
              qc.invalidateQueries({ queryKey: ['qa-docs'] })
              qc.invalidateQueries({ queryKey: ['qa-batches'] })
            }}
            sx={{ borderRadius: '4px', border: '1px solid #e5e7eb' }}>
            <RefreshIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Table — one row per adjustment document */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <TableContainer>
          <Table size="small" sx={{ tableLayout: 'fixed', minWidth: 820 }}>
            <TableHead>
              <TableRow sx={{ bgcolor: '#f9fafb' }}>
                <TableCell sx={thSx} width={44}>#</TableCell>
                <TableCell sx={thSx} width={90}>Store</TableCell>
                <TableCell sx={thSx}>Store Name</TableCell>
                <TableCell sx={thSx} width={160}>Adj SID</TableCell>
                <TableCell sx={thSx} width={70} align="center">Items</TableCell>
                <TableCell sx={thSx} width={70} align="center">Posted</TableCell>
                <TableCell sx={thSx} width={70} align="center">Errors</TableCell>
                <TableCell sx={thSx} width={85}>Status</TableCell>
                <TableCell sx={thSx} width={140}>Created At</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {!selectedBatch ? (
                <TableRow><TableCell colSpan={9} align="center"
                  sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                  Select a batch above to view records.
                </TableCell></TableRow>
              ) : isLoading ? (
                <TableRow><TableCell colSpan={9} align="center" sx={{ py: 6 }}>
                  <CircularProgress size={22} />
                </TableCell></TableRow>
              ) : docs.length === 0 ? (
                <TableRow><TableCell colSpan={9} align="center"
                  sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                  No adjustment documents found.
                </TableCell></TableRow>
              ) : docs.map((doc, i) => (
                <TableRow key={doc.id} onClick={() => setDetail(doc)}
                  sx={{ cursor: 'pointer', '&:hover': { bgcolor: '#f0f7ff' }, transition: 'background 0.1s' }}>
                  <TableCell sx={tdSx}>{page * pageSize + i + 1}</TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem' }}>
                    {doc.store_code || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.store_name || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem', color: '#6b7280',
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.adj_sid || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center' }}>{doc.item_count}</TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center', color: '#15803d', fontWeight: 600 }}>
                    {doc.posted_count}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center', color: doc.error_count > 0 ? '#b91c1c' : '#6b7280',
                    fontWeight: doc.error_count > 0 ? 600 : 400 }}>
                    {doc.error_count}
                  </TableCell>
                  <TableCell sx={tdSx}><AdjStatusChip status={doc.status} /></TableCell>
                  <TableCell sx={{ ...tdSx, whiteSpace: 'nowrap', color: '#6b7280' }}>
                    {fmt(doc.created_at)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        {/* Pagination */}
        {(data?.total ?? 0) > pageSize && (
          <Box sx={{ px: 2, py: 1, borderTop: '1px solid #f3f4f6',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
              Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, data!.total)} of {data!.total}
            </Typography>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button size="small" variant="outlined" disabled={page === 0}
                onClick={() => setPage(p => p - 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Prev</Button>
              <Button size="small" variant="outlined" disabled={page >= totalPages - 1}
                onClick={() => setPage(p => p + 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Next</Button>
            </Box>
          </Box>
        )}
      </Box>

      <QtyAdjDetailDialog doc={detail} onClose={() => setDetail(null)} />

      <Snackbar open={!!toast} autoHideDuration={5000} onClose={() => setToast(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert severity={toast?.severity ?? 'info'} onClose={() => setToast(null)}
          sx={{ fontSize: '0.8rem' }}>
          {toast?.msg}
        </Alert>
      </Snackbar>
    </Box>
  )
}

// ── Price Adjustment types ────────────────────────────────────────────────────

interface PriceAdjDoc {
  id: string
  source_file: string | null
  store_code: string | null
  store_name: string | null
  store_sid: string | null
  sbs_sid: string | null
  adj_sid: string | null
  note: string | null
  price_lvl_sid: string | null
  item_count: number
  posted_count: number
  error_count: number
  status: 'posted' | 'partial' | 'error' | 'pending'
  error_message: string | null
  api_create_payload: unknown
  api_create_response: unknown
  api_items_payload: unknown
  api_items_response: unknown
  api_get_response: unknown
  api_finalize_payload: unknown
  api_finalize_response: unknown
  api_comment_payload: unknown
  api_comment_response: unknown
  items_data: Array<{ upc: string; adj_value: number; item_sid: string | null; ok: boolean; error: string | null }> | null
  created_at: string
  posted_at: string | null
}

interface PriceAdjBatch {
  source_file: string
  doc_count: number
  total_items: number
  posted_items: number
  error_items: number
  latest: string | null
  posted_docs: number
  error_docs: number
}

// ── Price Adj detail dialog ───────────────────────────────────────────────────

function PriceAdjDetailDialog({ doc, onClose }: { doc: PriceAdjDoc | null; onClose: () => void }) {
  if (!doc) return null

  const section = (title: string, data: unknown, color = '#1e293b') => {
    const hasData = data !== null && data !== undefined
    let text: string
    if (hasData) {
      try { text = JSON.stringify(data, null, 2) } catch { text = String(data) }
    } else {
      text = '— No response recorded (step was not reached) —'
    }
    return (
      <Box sx={{ mx: 3, mb: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.75 }}>
          <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#374151',
            textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            {title}
          </Typography>
          {!hasData && (
            <Typography sx={{ fontSize: '0.62rem', color: '#9ca3af',
              bgcolor: '#f3f4f6', border: '1px solid #e5e7eb',
              borderRadius: '4px', px: 0.75, py: 0.1 }}>
              not reached
            </Typography>
          )}
        </Box>
        <Box sx={{
          bgcolor: hasData ? '#f8fafc' : '#f9fafb',
          border: `1px solid ${hasData ? '#e2e8f0' : '#e5e7eb'}`,
          borderRadius: '6px', p: 1.5, maxHeight: 260, overflow: 'auto',
        }}>
          <Typography sx={{
            fontSize: '0.7rem', fontFamily: 'monospace',
            whiteSpace: 'pre-wrap', wordBreak: 'break-word', lineHeight: 1.6,
            color: hasData ? color : '#9ca3af',
            fontStyle: hasData ? 'normal' : 'italic',
          }}>
            {text}
          </Typography>
        </Box>
      </Box>
    )
  }

  return (
    <Dialog open onClose={onClose} maxWidth="md" fullWidth
      slotProps={{ paper: { sx: { borderRadius: '8px', overflow: 'hidden' } } }}>
      <Box sx={{
        bgcolor: doc.status === 'error' ? '#fef2f2' : doc.status === 'posted' ? '#f0fdf4' : '#fff7ed',
        borderBottom: `2px solid ${doc.status === 'error' ? '#fecaca' : doc.status === 'posted' ? '#bbf7d0' : '#fed7aa'}`,
        px: 3, py: 2,
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
          <AdjStatusChip status={doc.status} />
          <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af', fontFamily: 'monospace' }}>
            {fmt(doc.created_at)}
          </Typography>
        </Box>
        <Typography sx={{ fontWeight: 700, fontSize: '1rem', color: '#111827', mt: 0.5 }}>
          Price Adjustment Document
        </Typography>
        <Typography sx={{ fontSize: '0.78rem', color: '#6b7280' }}>
          Store: {doc.store_name || doc.store_code || '—'} &nbsp;·&nbsp;
          {doc.item_count} items &nbsp;·&nbsp;
          {doc.posted_count} posted &nbsp;·&nbsp; {doc.error_count} errors
        </Typography>
      </Box>

      <DialogContent sx={{ p: 0, maxHeight: '76vh', overflowY: 'auto' }}>

        {/* ── Error message — shown first so it is immediately visible ── */}
        {doc.error_message && (
          <Box sx={{ mx: 3, mt: 2, mb: 1 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 0.75 }}>
              <ErrorOutlinedIcon sx={{ fontSize: 14, color: '#b91c1c' }} />
              <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#b91c1c',
                textTransform: 'uppercase', letterSpacing: '0.06em' }}>Error Details</Typography>
            </Box>
            <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: '6px',
              p: 1.5, maxHeight: 180, overflowY: 'auto' }}>
              <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace', whiteSpace: 'pre-wrap',
                color: '#7f1d1d', wordBreak: 'break-word' }}>{doc.error_message}</Typography>
            </Box>
          </Box>
        )}

        <Box sx={{ px: 3, pt: doc.error_message ? 1 : 2, pb: 1 }}>
          <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
            textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>Summary</Typography>
          <InfoRow label="Adj SID"        value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{doc.adj_sid || '—'}</Typography>} />
          {doc.note && <InfoRow label="Note" value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{doc.note}</Typography>} />}
          <InfoRow label="Store Code"     value={doc.store_code || '—'} />
          <InfoRow label="Store SID"      value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.store_sid || '—'}</Typography>} />
          <InfoRow label="Sbs SID"        value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.sbs_sid || '—'}</Typography>} />
          <InfoRow label="Price Level SID" value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.price_lvl_sid || '—'}</Typography>} />
          <InfoRow label="Source File"    value={doc.source_file || '—'} />
          {doc.posted_at && <InfoRow label="Posted At" value={fmt(doc.posted_at)} />}
        </Box>

        {doc.items_data && doc.items_data.length > 0 && (
          <Box sx={{ mx: 3, mb: 2 }}>
            <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
              textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>Items</Typography>
            <Box sx={{ border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ bgcolor: '#f9fafb' }}>
                    {['UPC', 'Price (adjvalue)', 'Item SID', 'Status'].map(h => (
                      <TableCell key={h} sx={{ ...thSx, py: 0.5 }}>{h}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {doc.items_data.map((item, i) => (
                    <TableRow key={i}>
                      <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem' }}>{item.upc}</TableCell>
                      <TableCell sx={tdSx}>{item.adj_value}</TableCell>
                      <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem', color: '#6b7280' }}>{item.item_sid || '—'}</TableCell>
                      <TableCell sx={tdSx}>
                        {item.ok
                          ? <Chip label="OK" size="small" sx={{ height: 18, fontSize: '0.65rem', bgcolor: '#f0fdf4', color: '#15803d', border: '1px solid #d1fae5' }} />
                          : <Chip label={item.error || 'Error'} size="small" sx={{ height: 18, fontSize: '0.65rem', bgcolor: '#fef2f2', color: '#b91c1c', border: '1px solid #fecaca', maxWidth: 260 }} />
                        }
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </Box>
          </Box>
        )}

        {/* API trace — all steps always shown; "not reached" badge marks where it stopped */}
        <Box sx={{ mx: 3, mb: 1.5, mt: 0.5, display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
            textTransform: 'uppercase', color: '#9ca3af' }}>API Trace</Typography>
          <Box sx={{ flex: 1, height: '1px', bgcolor: '#e5e7eb' }} />
        </Box>
        {section('Step 1 — Create Adjustment · Request',  doc.api_create_payload)}
        {section('Step 1 — Create Adjustment · Response', doc.api_create_response)}
        {section('Step 2 — Post Items · Request',         doc.api_items_payload)}
        {section('Step 2 — Post Items · Response',        doc.api_items_response)}
        {section('Step 3 — GET Rowversion · Response',    doc.api_get_response)}
        {section('Step 4 — Finalize · Request',           doc.api_finalize_payload)}
        {section('Step 4 — Finalize · Response',          doc.api_finalize_response)}
        {section('Step 5 — Post Comment · Request',       doc.api_comment_payload)}
        {section('Step 5 — Post Comment · Response',      doc.api_comment_response)}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2, pt: 1, borderTop: '1px solid #f3f4f6' }}>
        <Button size="small" variant="outlined" onClick={onClose}
          sx={{ height: 30, fontSize: '0.78rem' }}>Close</Button>
      </DialogActions>
    </Dialog>
  )
}

// ── Price Adjustment tab ──────────────────────────────────────────────────────

function PriceAdjustmentTab() {
  const qc = useQueryClient()
  const [selectedBatch, setSelectedBatch] = useState('')
  const [status, setStatus]               = useState('')
  const [detail, setDetail]               = useState<PriceAdjDoc | null>(null)
  const [page, setPage]                   = useState(0)
  const pageSize                           = 100
  const [uploading, setUploading]         = useState(false)
  const [killing, setKilling]             = useState(false)
  const [toast, setToast]                 = useState<{ msg: string; severity: 'success' | 'error' | 'warning' } | null>(null)
  const abortRef                          = useRef<AbortController | null>(null)

  const { data: runStatus } = useQuery<{ running: boolean }>({
    queryKey: ['pa-status'],
    queryFn: () => apiClient.get('/api/price-adjustment/status').then(r => r.data),
    refetchInterval: 3_000,
  })
  const isRunning = uploading || !!runStatus?.running

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    const controller = new AbortController()
    abortRef.current = controller
    setUploading(true)
    setKilling(false)
    try {
      const form = new FormData()
      form.append('file', file)
      const res = await apiClient.post('/api/price-adjustment/import', form, {
        headers: { 'Content-Type': 'multipart/form-data' },
        signal: controller.signal,
      })
      const d = res.data
      if (d.cancelled) {
        setToast({ severity: 'warning', msg: `Stopped — ${d.total_docs} docs processed (${d.posted_docs} posted, ${d.error_docs} errors)` })
      } else {
        setToast({ severity: 'success', msg: `Done — ${d.total_docs} docs, ${d.posted_docs} posted, ${d.error_docs} errors` })
      }
      qc.invalidateQueries({ queryKey: ['pa-batches'] })
      qc.invalidateQueries({ queryKey: ['pa-docs'] })
    } catch (err: unknown) {
      const isCancelled = (err as { name?: string })?.name === 'CanceledError'
                       || (err as { code?: string })?.code === 'ERR_CANCELED'
      if (!isCancelled) {
        const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || String(err)
        setToast({ severity: 'error', msg })
      }
      qc.invalidateQueries({ queryKey: ['pa-batches'] })
      qc.invalidateQueries({ queryKey: ['pa-docs'] })
    } finally {
      abortRef.current = null
      setUploading(false)
      setKilling(false)
    }
  }

  const handleKill = async () => {
    setKilling(true)
    try { await apiClient.post('/api/price-adjustment/kill') } catch { /* ignore */ }
    abortRef.current?.abort()
  }

  const { data: batches, isFetching: batchFetching } = useQuery<PriceAdjBatch[]>({
    queryKey: ['pa-batches'],
    queryFn: () => apiClient.get('/api/price-adjustment/batches').then(r => r.data),
    refetchInterval: isRunning ? 3_000 : 30_000,
  })

  useEffect(() => {
    if (batches && batches.length > 0 && !selectedBatch)
      setSelectedBatch(batches[0].source_file)
  }, [batches, selectedBatch])

  const activeBatch = batches?.find(b => b.source_file === selectedBatch)

  const params: Record<string, string | number> = {
    limit: pageSize, offset: page * pageSize,
    ...(selectedBatch ? { source_file: selectedBatch } : {}),
    ...(status        ? { status }                     : {}),
  }

  const { data, isLoading, isFetching } = useQuery<{ total: number; items: PriceAdjDoc[] }>({
    queryKey: ['pa-docs', params],
    queryFn: () => apiClient.get('/api/price-adjustment/docs', { params }).then(r => r.data),
    refetchInterval: isRunning ? 3_000 : 30_000,
    enabled: !!selectedBatch,
  })

  const docs       = data?.items ?? []
  const totalPages = Math.ceil((data?.total ?? 0) / pageSize)

  return (
    <Box>
      {/* Batch selector */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <FolderOutlinedIcon sx={{ fontSize: 15, color: '#9ca3af' }} />
        <Typography sx={{ fontSize: '0.72rem', color: '#374151', fontWeight: 600 }}>Batch:</Typography>
        <FormControl size="small" sx={{ minWidth: 280 }}>
          <Select value={selectedBatch} displayEmpty
            onChange={e => { setSelectedBatch(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}
            renderValue={v => {
              if (!v) return <em style={{ color: '#9ca3af' }}>Select a batch…</em>
              const b = batches?.find(x => x.source_file === v)
              return (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <span>{v}</span>
                  {b && <Typography component="span" sx={{ fontSize: '0.65rem', color: '#9ca3af', ml: 0.5 }}>
                    ({b.doc_count} docs · {fmt(b.latest)})
                  </Typography>}
                </Box>
              )
            }}>
            {(batches ?? []).map(b => (
              <MenuItem key={b.source_file} value={b.source_file}>
                <Box sx={{ width: '100%' }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Typography sx={{ fontSize: '0.8rem', fontWeight: 500 }}>{b.source_file}</Typography>
                    <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af' }}>{fmt(b.latest)}</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', gap: 1, mt: 0.25 }}>
                    <Typography sx={{ fontSize: '0.65rem', color: '#15803d' }}>✓ {b.posted_docs} posted</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#b91c1c' }}>✗ {b.error_docs} errors</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#6b7280' }}>{b.doc_count} docs · {b.total_items} items</Typography>
                  </Box>
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        {batchFetching && <CircularProgress size={12} />}
      </Box>

      {/* Pills + toolbar */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <Pill label="adj docs"     value={activeBatch?.doc_count    ?? 0} color="#3b82f6" />
        <Pill label="posted docs"  value={activeBatch?.posted_docs  ?? 0} color="#22c55e" />
        <Pill label="error docs"   value={activeBatch?.error_docs   ?? 0} color="#ef4444" />
        <Pill label="total items"  value={activeBatch?.total_items  ?? 0} color="#6b7280" />
        <Pill label="posted items" value={activeBatch?.posted_items ?? 0} color="#22c55e" />
        {isFetching && <CircularProgress size={13} sx={{ ml: 0.5 }} />}
        <Box sx={{ flexGrow: 1 }} />
        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel sx={{ fontSize: '0.78rem' }}>Status</InputLabel>
          <Select value={status} label="Status"
            onChange={e => { setStatus(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="posted">Posted</MenuItem>
            <MenuItem value="partial">Partial</MenuItem>
            <MenuItem value="error">Error</MenuItem>
          </Select>
        </FormControl>
        {/* Manual CSV upload / Kill */}
        {isRunning ? (
          <Box sx={{ display: 'flex', gap: 0.75 }}>
            <Button size="small" variant="contained" disabled
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#7c3aed', pointerEvents: 'none' }}>
              <CircularProgress size={12} sx={{ color: 'white', mr: 0.75 }} />
              Processing…
            </Button>
            <Tooltip title="Stop import after current store">
              <Button size="small" variant="contained" disabled={killing}
                onClick={handleKill}
                startIcon={killing
                  ? <CircularProgress size={12} sx={{ color: 'white' }} />
                  : <StopCircleOutlinedIcon sx={{ fontSize: 15 }} />}
                sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                  bgcolor: '#dc2626', '&:hover': { bgcolor: '#b91c1c' } }}>
                {killing ? 'Stopping…' : 'Stop'}
              </Button>
            </Tooltip>
          </Box>
        ) : (
          <Tooltip title="Upload CSV manually">
            <Button component="label" size="small" variant="contained"
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#7c3aed', '&:hover': { bgcolor: '#6d28d9' } }}>
              Upload CSV
              <input type="file" accept=".csv" hidden onChange={handleUpload} />
            </Button>
          </Tooltip>
        )}
        <Tooltip title="Refresh">
          <IconButton size="small"
            onClick={() => {
              qc.invalidateQueries({ queryKey: ['pa-docs'] })
              qc.invalidateQueries({ queryKey: ['pa-batches'] })
            }}
            sx={{ borderRadius: '4px', border: '1px solid #e5e7eb' }}>
            <RefreshIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Table */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <TableContainer>
          <Table size="small" sx={{ tableLayout: 'fixed', minWidth: 820 }}>
            <TableHead>
              <TableRow sx={{ bgcolor: '#f9fafb' }}>
                <TableCell sx={thSx} width={44}>#</TableCell>
                <TableCell sx={thSx} width={90}>Store</TableCell>
                <TableCell sx={thSx}>Store Name</TableCell>
                <TableCell sx={thSx} width={160}>Adj SID</TableCell>
                <TableCell sx={thSx} width={70} align="center">Items</TableCell>
                <TableCell sx={thSx} width={70} align="center">Posted</TableCell>
                <TableCell sx={thSx} width={70} align="center">Errors</TableCell>
                <TableCell sx={thSx} width={85}>Status</TableCell>
                <TableCell sx={thSx} width={140}>Created At</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {!selectedBatch ? (
                <TableRow><TableCell colSpan={9} align="center"
                  sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                  Select a batch above to view records.
                </TableCell></TableRow>
              ) : isLoading ? (
                <TableRow><TableCell colSpan={9} align="center" sx={{ py: 6 }}>
                  <CircularProgress size={22} />
                </TableCell></TableRow>
              ) : docs.length === 0 ? (
                <TableRow><TableCell colSpan={9} align="center"
                  sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                  No price adjustment documents found.
                </TableCell></TableRow>
              ) : docs.map((doc, i) => (
                <TableRow key={doc.id} onClick={() => setDetail(doc)}
                  sx={{ cursor: 'pointer', '&:hover': { bgcolor: '#f5f3ff' }, transition: 'background 0.1s' }}>
                  <TableCell sx={tdSx}>{page * pageSize + i + 1}</TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem' }}>
                    {doc.store_code || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.store_name || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem', color: '#6b7280',
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.adj_sid || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center' }}>{doc.item_count}</TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center', color: '#15803d', fontWeight: 600 }}>
                    {doc.posted_count}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center',
                    color: doc.error_count > 0 ? '#b91c1c' : '#6b7280',
                    fontWeight: doc.error_count > 0 ? 600 : 400 }}>
                    {doc.error_count}
                  </TableCell>
                  <TableCell sx={tdSx}><AdjStatusChip status={doc.status} /></TableCell>
                  <TableCell sx={{ ...tdSx, whiteSpace: 'nowrap', color: '#6b7280' }}>
                    {fmt(doc.created_at)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        {(data?.total ?? 0) > pageSize && (
          <Box sx={{ px: 2, py: 1, borderTop: '1px solid #f3f4f6',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
              Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, data!.total)} of {data!.total}
            </Typography>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button size="small" variant="outlined" disabled={page === 0}
                onClick={() => setPage(p => p - 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Prev</Button>
              <Button size="small" variant="outlined" disabled={page >= totalPages - 1}
                onClick={() => setPage(p => p + 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Next</Button>
            </Box>
          </Box>
        )}
      </Box>

      <PriceAdjDetailDialog doc={detail} onClose={() => setDetail(null)} />

      <Snackbar open={!!toast} autoHideDuration={5000} onClose={() => setToast(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert severity={toast?.severity ?? 'info'} onClose={() => setToast(null)}
          sx={{ fontSize: '0.8rem' }}>
          {toast?.msg}
        </Alert>
      </Snackbar>
    </Box>
  )
}

// ── Transfer Slip types ───────────────────────────────────────────────────────

interface TransferSlipDoc {
  id: string
  source_file: string | null
  note: string | null
  in_store_name: string | null
  out_store_name: string | null
  instoresid: string | null
  insbssid: string | null
  outstoresid: string | null
  outsbssid: string | null
  slip_sid: string | null
  item_count: number
  posted_count: number
  error_count: number
  status: 'posted' | 'partial' | 'error' | 'pending'
  error_message: string | null
  api_create_payload: unknown
  api_create_response: unknown
  api_items_payload: unknown
  api_items_response: unknown
  api_comment_payload: unknown
  api_comment_response: unknown
  api_get_response: unknown
  api_finalize_payload: unknown
  api_finalize_response: unknown
  items_data: Array<{ upc: string; qty: number; item_sid: string | null; ok: boolean; error: string | null }> | null
  created_at: string
  posted_at: string | null
}

interface TransferSlipBatch {
  source_file: string
  doc_count: number
  total_items: number
  posted_items: number
  error_items: number
  latest: string | null
  posted_docs: number
  error_docs: number
}

// ── Transfer Slip detail dialog ───────────────────────────────────────────────

function TransferSlipDetailDialog({ doc, onClose }: { doc: TransferSlipDoc | null; onClose: () => void }) {
  if (!doc) return null

  const section = (title: string, data: unknown) => {
    if (!data) return null
    let text: string
    try { text = JSON.stringify(data, null, 2) } catch { text = String(data) }
    return (
      <Box sx={{ mx: 3, mb: 2 }}>
        <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#374151',
          textTransform: 'uppercase', letterSpacing: '0.06em', mb: 0.75 }}>
          {title}
        </Typography>
        <Box sx={{ bgcolor: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '6px',
          p: 1.5, maxHeight: 220, overflow: 'auto' }}>
          <Typography sx={{ fontSize: '0.7rem', fontFamily: 'monospace',
            whiteSpace: 'pre-wrap', color: '#1e293b', wordBreak: 'break-word', lineHeight: 1.6 }}>
            {text}
          </Typography>
        </Box>
      </Box>
    )
  }

  return (
    <Dialog open onClose={onClose} maxWidth="md" fullWidth
      slotProps={{ paper: { sx: { borderRadius: '8px', overflow: 'hidden' } } }}>
      <Box sx={{
        bgcolor: doc.status === 'error' ? '#fef2f2' : doc.status === 'posted' ? '#f0fdf4' : '#fff7ed',
        borderBottom: `2px solid ${doc.status === 'error' ? '#fecaca' : doc.status === 'posted' ? '#bbf7d0' : '#fed7aa'}`,
        px: 3, py: 2,
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
          <AdjStatusChip status={doc.status} />
          <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af', fontFamily: 'monospace' }}>
            {fmt(doc.created_at)}
          </Typography>
        </Box>
        <Typography sx={{ fontWeight: 700, fontSize: '1rem', color: '#111827', mt: 0.5 }}>
          Transfer Slip
        </Typography>
        <Typography sx={{ fontSize: '0.78rem', color: '#6b7280' }}>
          Note: {doc.note || '—'} &nbsp;·&nbsp;
          {doc.out_store_name || '—'} → {doc.in_store_name || '—'} &nbsp;·&nbsp;
          {doc.item_count} items &nbsp;·&nbsp; {doc.posted_count} posted &nbsp;·&nbsp; {doc.error_count} errors
        </Typography>
      </Box>

      <DialogContent sx={{ p: 0, maxHeight: '76vh', overflowY: 'auto' }}>
        <Box sx={{ px: 3, pt: 2, pb: 1 }}>
          <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
            textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>Summary</Typography>
          <InfoRow label="Note"          value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{doc.note || '—'}</Typography>} />
          <InfoRow label="Slip SID"      value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{doc.slip_sid || '—'}</Typography>} />
          <InfoRow label="In Store"      value={doc.in_store_name  || '—'} />
          <InfoRow label="Out Store"     value={doc.out_store_name || '—'} />
          <InfoRow label="In Store SID"  value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.instoresid  || '—'}</Typography>} />
          <InfoRow label="In SBS SID"    value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.insbssid    || '—'}</Typography>} />
          <InfoRow label="Out Store SID" value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.outstoresid || '—'}</Typography>} />
          <InfoRow label="Out SBS SID"   value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.outsbssid   || '—'}</Typography>} />
          <InfoRow label="Source File"   value={doc.source_file || '—'} />
          {doc.posted_at && <InfoRow label="Posted At" value={fmt(doc.posted_at)} />}
        </Box>

        {doc.items_data && doc.items_data.length > 0 && (
          <Box sx={{ mx: 3, mb: 2 }}>
            <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
              textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>Items</Typography>
            <Box sx={{ border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ bgcolor: '#f9fafb' }}>
                    {['UPC', 'Qty', 'Item SID', 'Status'].map(h => (
                      <TableCell key={h} sx={{ ...thSx, py: 0.5 }}>{h}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {doc.items_data.map((item, i) => (
                    <TableRow key={i}>
                      <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem' }}>{item.upc}</TableCell>
                      <TableCell sx={tdSx}>{item.qty}</TableCell>
                      <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem', color: '#6b7280' }}>{item.item_sid || '—'}</TableCell>
                      <TableCell sx={tdSx}>
                        {item.ok
                          ? <Chip label="OK" size="small" sx={{ height: 18, fontSize: '0.65rem', bgcolor: '#f0fdf4', color: '#15803d', border: '1px solid #d1fae5' }} />
                          : <Chip label={item.error || 'Error'} size="small" sx={{ height: 18, fontSize: '0.65rem', bgcolor: '#fef2f2', color: '#b91c1c', border: '1px solid #fecaca' }} />
                        }
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </Box>
          </Box>
        )}

        {doc.error_message && (
          <Box sx={{ mx: 3, mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 0.75 }}>
              <ErrorOutlinedIcon sx={{ fontSize: 14, color: '#b91c1c' }} />
              <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#b91c1c',
                textTransform: 'uppercase', letterSpacing: '0.06em' }}>Error</Typography>
            </Box>
            <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: '6px', p: 1.5 }}>
              <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace', whiteSpace: 'pre-wrap',
                color: '#7f1d1d', wordBreak: 'break-word' }}>{doc.error_message}</Typography>
            </Box>
          </Box>
        )}

        {section('1. Create Slip — Request',     doc.api_create_payload)}
        {section('1. Create Slip — Response',    doc.api_create_response)}
        {section('2. Post Items — Request',      doc.api_items_payload)}
        {section('2. Post Items — Response',     doc.api_items_response)}
        {section('3. Post Comment — Request',    doc.api_comment_payload)}
        {section('3. Post Comment — Response',   doc.api_comment_response)}
        {section('4. GET Rowversion — Response', doc.api_get_response)}
        {section('5. Finalize — Request',        doc.api_finalize_payload)}
        {section('5. Finalize — Response',       doc.api_finalize_response)}
        {section('6. GET Updated Rowversion — Response', doc.api_verify_get_response)}
        {section('7. Verify — Request',          doc.api_verify_payload)}
        {section('7. Verify — Response',         doc.api_verify_response)}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2, pt: 1, borderTop: '1px solid #f3f4f6' }}>
        <Button size="small" variant="outlined" onClick={onClose}
          sx={{ height: 30, fontSize: '0.78rem' }}>Close</Button>
      </DialogActions>
    </Dialog>
  )
}

// ── Transfer Slip tab ─────────────────────────────────────────────────────────

function TransferSlipTab() {
  const qc = useQueryClient()
  const [selectedBatch, setSelectedBatch] = useState('')
  const [status, setStatus]               = useState('')
  const [detail, setDetail]               = useState<TransferSlipDoc | null>(null)
  const [page, setPage]                   = useState(0)
  const pageSize                           = 100
  const [uploading, setUploading]         = useState(false)
  const [killing, setKilling]             = useState(false)
  const [toast, setToast]                 = useState<{ msg: string; severity: 'success' | 'error' | 'warning' } | null>(null)
  const abortRef                          = useRef<AbortController | null>(null)

  const { data: runStatus } = useQuery<{ running: boolean }>({
    queryKey: ['ts-status'],
    queryFn: () => apiClient.get('/api/transfer-slip/status').then(r => r.data),
    refetchInterval: 3_000,
  })
  const isRunning = uploading || !!runStatus?.running

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    const controller = new AbortController()
    abortRef.current = controller
    setUploading(true)
    setKilling(false)
    try {
      const form = new FormData()
      form.append('file', file)
      const res = await apiClient.post('/api/transfer-slip/import', form, {
        headers: { 'Content-Type': 'multipart/form-data' },
        signal: controller.signal,
      })
      const d = res.data
      if (d.cancelled) {
        setToast({ severity: 'warning', msg: `Stopped — ${d.total_docs} slips processed (${d.posted_docs} posted, ${d.error_docs} errors)` })
      } else {
        setToast({ severity: 'success', msg: `Done — ${d.total_docs} slips, ${d.posted_docs} posted, ${d.error_docs} errors` })
      }
      qc.invalidateQueries({ queryKey: ['ts-batches'] })
      qc.invalidateQueries({ queryKey: ['ts-docs'] })
    } catch (err: unknown) {
      const isCancelled = (err as { name?: string })?.name === 'CanceledError'
                       || (err as { code?: string })?.code === 'ERR_CANCELED'
      if (!isCancelled) {
        const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || String(err)
        setToast({ severity: 'error', msg })
      }
      qc.invalidateQueries({ queryKey: ['ts-batches'] })
      qc.invalidateQueries({ queryKey: ['ts-docs'] })
    } finally {
      abortRef.current = null
      setUploading(false)
      setKilling(false)
    }
  }

  const handleKill = async () => {
    setKilling(true)
    try { await apiClient.post('/api/transfer-slip/kill') } catch { /* ignore */ }
    abortRef.current?.abort()
  }

  const { data: batches, isFetching: batchFetching } = useQuery<TransferSlipBatch[]>({
    queryKey: ['ts-batches'],
    queryFn: () => apiClient.get('/api/transfer-slip/batches').then(r => r.data),
    refetchInterval: 30_000,
  })

  useEffect(() => {
    if (batches && batches.length > 0 && !selectedBatch)
      setSelectedBatch(batches[0].source_file)
  }, [batches, selectedBatch])

  const activeBatch = batches?.find(b => b.source_file === selectedBatch)

  const params: Record<string, string | number> = {
    limit: pageSize, offset: page * pageSize,
    ...(selectedBatch ? { source_file: selectedBatch } : {}),
    ...(status        ? { status }                     : {}),
  }

  const { data, isLoading, isFetching } = useQuery<{ total: number; items: TransferSlipDoc[] }>({
    queryKey: ['ts-docs', params],
    queryFn: () => apiClient.get('/api/transfer-slip/docs', { params }).then(r => r.data),
    refetchInterval: isRunning ? 3_000 : 30_000,
    enabled: !!selectedBatch,
  })

  const docs       = data?.items ?? []
  const totalPages = Math.ceil((data?.total ?? 0) / pageSize)

  return (
    <Box>
      {/* Batch selector */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <FolderOutlinedIcon sx={{ fontSize: 15, color: '#9ca3af' }} />
        <Typography sx={{ fontSize: '0.72rem', color: '#374151', fontWeight: 600 }}>Batch:</Typography>
        <FormControl size="small" sx={{ minWidth: 280 }}>
          <Select value={selectedBatch} displayEmpty
            onChange={e => { setSelectedBatch(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}
            renderValue={v => {
              if (!v) return <em style={{ color: '#9ca3af' }}>Select a batch…</em>
              const b = batches?.find(x => x.source_file === v)
              return (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <span>{v}</span>
                  {b && <Typography component="span" sx={{ fontSize: '0.65rem', color: '#9ca3af', ml: 0.5 }}>
                    ({b.doc_count} slips · {fmt(b.latest)})
                  </Typography>}
                </Box>
              )
            }}>
            {(batches ?? []).map(b => (
              <MenuItem key={b.source_file} value={b.source_file}>
                <Box sx={{ width: '100%' }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Typography sx={{ fontSize: '0.8rem', fontWeight: 500 }}>{b.source_file}</Typography>
                    <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af' }}>{fmt(b.latest)}</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', gap: 1, mt: 0.25 }}>
                    <Typography sx={{ fontSize: '0.65rem', color: '#15803d' }}>✓ {b.posted_docs} posted</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#b91c1c' }}>✗ {b.error_docs} errors</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#6b7280' }}>{b.doc_count} slips · {b.total_items} items</Typography>
                  </Box>
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        {batchFetching && <CircularProgress size={12} />}
      </Box>

      {/* Pills + toolbar */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <Pill label="slips"        value={activeBatch?.doc_count    ?? 0} color="#0ea5e9" />
        <Pill label="posted slips" value={activeBatch?.posted_docs  ?? 0} color="#22c55e" />
        <Pill label="error slips"  value={activeBatch?.error_docs   ?? 0} color="#ef4444" />
        <Pill label="total items"  value={activeBatch?.total_items  ?? 0} color="#6b7280" />
        <Pill label="posted items" value={activeBatch?.posted_items ?? 0} color="#22c55e" />
        {isFetching && <CircularProgress size={13} sx={{ ml: 0.5 }} />}
        <Box sx={{ flexGrow: 1 }} />
        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel sx={{ fontSize: '0.78rem' }}>Status</InputLabel>
          <Select value={status} label="Status"
            onChange={e => { setStatus(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="posted">Posted</MenuItem>
            <MenuItem value="partial">Partial</MenuItem>
            <MenuItem value="error">Error</MenuItem>
          </Select>
        </FormControl>
        {/* Manual CSV upload / Kill */}
        {isRunning ? (
          <Box sx={{ display: 'flex', gap: 0.75 }}>
            <Button size="small" variant="contained" disabled
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#0ea5e9', pointerEvents: 'none' }}>
              <CircularProgress size={12} sx={{ color: 'white', mr: 0.75 }} />
              Processing…
            </Button>
            <Tooltip title="Stop import after current transfer slip">
              <Button size="small" variant="contained" disabled={killing}
                onClick={handleKill}
                startIcon={killing
                  ? <CircularProgress size={12} sx={{ color: 'white' }} />
                  : <StopCircleOutlinedIcon sx={{ fontSize: 15 }} />}
                sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                  bgcolor: '#dc2626', '&:hover': { bgcolor: '#b91c1c' } }}>
                {killing ? 'Stopping…' : 'Stop'}
              </Button>
            </Tooltip>
          </Box>
        ) : (
          <Tooltip title="Upload Transfer Slip CSV manually">
            <Button component="label" size="small" variant="contained"
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#0ea5e9', '&:hover': { bgcolor: '#0284c7' } }}>
              Upload CSV
              <input type="file" accept=".csv" hidden onChange={handleUpload} />
            </Button>
          </Tooltip>
        )}

        <Tooltip title="Refresh">
          <IconButton size="small"
            onClick={() => {
              qc.invalidateQueries({ queryKey: ['ts-docs'] })
              qc.invalidateQueries({ queryKey: ['ts-batches'] })
            }}
            sx={{ borderRadius: '4px', border: '1px solid #e5e7eb' }}>
            <RefreshIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Table — one row per transfer slip */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <TableContainer>
          <Table size="small" sx={{ tableLayout: 'fixed', minWidth: 900 }}>
            <TableHead>
              <TableRow sx={{ bgcolor: '#f9fafb' }}>
                <TableCell sx={thSx} width={44}>#</TableCell>
                <TableCell sx={thSx} width={130}>Note</TableCell>
                <TableCell sx={thSx} width={120}>Out Store</TableCell>
                <TableCell sx={thSx} width={120}>In Store</TableCell>
                <TableCell sx={thSx} width={160}>Slip SID</TableCell>
                <TableCell sx={thSx} width={65} align="center">Items</TableCell>
                <TableCell sx={thSx} width={65} align="center">Posted</TableCell>
                <TableCell sx={thSx} width={65} align="center">Errors</TableCell>
                <TableCell sx={thSx} width={85}>Status</TableCell>
                <TableCell sx={thSx} width={140}>Created At</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {!selectedBatch ? (
                <TableRow><TableCell colSpan={10} align="center"
                  sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                  Select a batch above to view records.
                </TableCell></TableRow>
              ) : isLoading ? (
                <TableRow><TableCell colSpan={10} align="center" sx={{ py: 6 }}>
                  <CircularProgress size={22} />
                </TableCell></TableRow>
              ) : docs.length === 0 ? (
                <TableRow><TableCell colSpan={10} align="center"
                  sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                  No transfer slip documents found.
                </TableCell></TableRow>
              ) : docs.map((doc, i) => (
                <TableRow key={doc.id} onClick={() => setDetail(doc)}
                  sx={{ cursor: 'pointer', '&:hover': { bgcolor: '#f0f9ff' }, transition: 'background 0.1s' }}>
                  <TableCell sx={tdSx}>{page * pageSize + i + 1}</TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem',
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.note || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.out_store_name || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.in_store_name || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem', color: '#6b7280',
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.slip_sid || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center' }}>{doc.item_count}</TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center', color: '#15803d', fontWeight: 600 }}>
                    {doc.posted_count}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center',
                    color: doc.error_count > 0 ? '#b91c1c' : '#6b7280',
                    fontWeight: doc.error_count > 0 ? 600 : 400 }}>
                    {doc.error_count}
                  </TableCell>
                  <TableCell sx={tdSx}><AdjStatusChip status={doc.status} /></TableCell>
                  <TableCell sx={{ ...tdSx, whiteSpace: 'nowrap', color: '#6b7280' }}>
                    {fmt(doc.created_at)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        {(data?.total ?? 0) > pageSize && (
          <Box sx={{ px: 2, py: 1, borderTop: '1px solid #f3f4f6',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
              Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, data!.total)} of {data!.total}
            </Typography>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button size="small" variant="outlined" disabled={page === 0}
                onClick={() => setPage(p => p - 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Prev</Button>
              <Button size="small" variant="outlined" disabled={page >= totalPages - 1}
                onClick={() => setPage(p => p + 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Next</Button>
            </Box>
          </Box>
        )}
      </Box>

      <TransferSlipDetailDialog doc={detail} onClose={() => setDetail(null)} />

      <Snackbar open={!!toast} autoHideDuration={5000} onClose={() => setToast(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert severity={toast?.severity ?? 'info'} onClose={() => setToast(null)}
          sx={{ fontSize: '0.8rem' }}>
          {toast?.msg}
        </Alert>
      </Snackbar>
    </Box>
  )
}

// ── GRN types ─────────────────────────────────────────────────────────────────

interface GRNDoc {
  id: string
  source_file: string | null
  note: string | null
  store_code: string | null
  store_name: string | null
  storesid: string | null
  sbssid: string | null
  vendsid: string | null
  vousid: string | null
  item_count: number
  posted_count: number
  error_count: number
  status: 'posted' | 'partial' | 'error' | 'pending'
  error_message: string | null
  api_create_payload: unknown
  api_create_response: unknown
  api_get_rowversion_response: unknown
  api_vendor_payload: unknown
  api_vendor_response: unknown
  api_items_payload: unknown
  api_items_response: unknown
  api_comment_payload: unknown
  api_comment_response: unknown
  api_get_rowversion2_response: unknown
  api_finalize_payload: unknown
  api_finalize_response: unknown
  items_data: Array<{ upc: string; qty: number; item_sid: string | null; ok: boolean; error: string | null }> | null
  created_at: string
  posted_at: string | null
}

interface GRNBatch {
  source_file: string
  doc_count: number
  total_items: number
  posted_items: number
  error_items: number
  latest: string | null
  posted_docs: number
  error_docs: number
}

// ── GRN detail dialog ─────────────────────────────────────────────────────────

function GRNDetailDialog({ doc, onClose }: { doc: GRNDoc | null; onClose: () => void }) {
  if (!doc) return null

  const section = (title: string, data: unknown) => {
    if (!data) return null
    let text: string
    try { text = JSON.stringify(data, null, 2) } catch { text = String(data) }
    return (
      <Box sx={{ mx: 3, mb: 2 }}>
        <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#374151',
          textTransform: 'uppercase', letterSpacing: '0.06em', mb: 0.75 }}>
          {title}
        </Typography>
        <Box sx={{ bgcolor: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '6px',
          p: 1.5, maxHeight: 220, overflow: 'auto' }}>
          <Typography sx={{ fontSize: '0.7rem', fontFamily: 'monospace',
            whiteSpace: 'pre-wrap', color: '#1e293b', wordBreak: 'break-word', lineHeight: 1.6 }}>
            {text}
          </Typography>
        </Box>
      </Box>
    )
  }

  return (
    <Dialog open onClose={onClose} maxWidth="md" fullWidth
      slotProps={{ paper: { sx: { borderRadius: '8px', overflow: 'hidden' } } }}>
      <Box sx={{
        bgcolor: doc.status === 'error' ? '#fef2f2' : doc.status === 'posted' ? '#f0fdf4' : '#fff7ed',
        borderBottom: `2px solid ${doc.status === 'error' ? '#fecaca' : doc.status === 'posted' ? '#bbf7d0' : '#fed7aa'}`,
        px: 3, py: 2,
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.5 }}>
          <AdjStatusChip status={doc.status} />
          <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af', fontFamily: 'monospace' }}>
            {fmt(doc.created_at)}
          </Typography>
        </Box>
        <Typography sx={{ fontWeight: 700, fontSize: '1rem', color: '#111827', mt: 0.5 }}>
          GRN Document
        </Typography>
        <Typography sx={{ fontSize: '0.78rem', color: '#6b7280' }}>
          Note: {doc.note || '—'} &nbsp;·&nbsp;
          Store: {doc.store_name || doc.store_code || '—'} &nbsp;·&nbsp;
          {doc.item_count} items &nbsp;·&nbsp; {doc.posted_count} posted &nbsp;·&nbsp; {doc.error_count} errors
        </Typography>
      </Box>

      <DialogContent sx={{ p: 0, maxHeight: '76vh', overflowY: 'auto' }}>
        <Box sx={{ px: 3, pt: 2, pb: 1 }}>
          <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
            textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>Summary</Typography>
          <InfoRow label="Note"        value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{doc.note || '—'}</Typography>} />
          <InfoRow label="Voucher SID" value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{doc.vousid || '—'}</Typography>} />
          <InfoRow label="Store Code"  value={doc.store_code || '—'} />
          <InfoRow label="Store Name"  value={doc.store_name || '—'} />
          <InfoRow label="Store SID"   value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.storesid || '—'}</Typography>} />
          <InfoRow label="SBS SID"     value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.sbssid || '—'}</Typography>} />
          <InfoRow label="Vendor SID"  value={<Typography sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>{doc.vendsid || '—'}</Typography>} />
          <InfoRow label="Source File" value={doc.source_file || '—'} />
          {doc.posted_at && <InfoRow label="Posted At" value={fmt(doc.posted_at)} />}
        </Box>

        {doc.items_data && doc.items_data.length > 0 && (
          <Box sx={{ mx: 3, mb: 2 }}>
            <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
              textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>Items</Typography>
            <Box sx={{ border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ bgcolor: '#f9fafb' }}>
                    {['UPC', 'Qty', 'Item SID', 'Status'].map(h => (
                      <TableCell key={h} sx={{ ...thSx, py: 0.5 }}>{h}</TableCell>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {doc.items_data.map((item, i) => (
                    <TableRow key={i}>
                      <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem' }}>{item.upc}</TableCell>
                      <TableCell sx={tdSx}>{item.qty}</TableCell>
                      <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem', color: '#6b7280' }}>{item.item_sid || '—'}</TableCell>
                      <TableCell sx={tdSx}>
                        {item.ok
                          ? <Chip label="OK" size="small" sx={{ height: 18, fontSize: '0.65rem', bgcolor: '#f0fdf4', color: '#15803d', border: '1px solid #d1fae5' }} />
                          : <Chip label={item.error || 'Error'} size="small" sx={{ height: 18, fontSize: '0.65rem', bgcolor: '#fef2f2', color: '#b91c1c', border: '1px solid #fecaca' }} />
                        }
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </Box>
          </Box>
        )}

        {doc.error_message && (
          <Box sx={{ mx: 3, mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 0.75 }}>
              <ErrorOutlinedIcon sx={{ fontSize: 14, color: '#b91c1c' }} />
              <Typography sx={{ fontSize: '0.7rem', fontWeight: 700, color: '#b91c1c',
                textTransform: 'uppercase', letterSpacing: '0.06em' }}>Error</Typography>
            </Box>
            <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: '6px', p: 1.5 }}>
              <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace', whiteSpace: 'pre-wrap',
                color: '#7f1d1d', wordBreak: 'break-word' }}>{doc.error_message}</Typography>
            </Box>
          </Box>
        )}

        {section('1. Create Voucher — Request',      doc.api_create_payload)}
        {section('1. Create Voucher — Response',     doc.api_create_response)}
        {section('2. GET Rowversion — Response',     doc.api_get_rowversion_response)}
        {section('3. Set Vendor — Request',          doc.api_vendor_payload)}
        {section('3. Set Vendor — Response',         doc.api_vendor_response)}
        {section('4. Post Items — Request',          doc.api_items_payload)}
        {section('4. Post Items — Response',         doc.api_items_response)}
        {section('5. Post Comment — Request',        doc.api_comment_payload)}
        {section('5. Post Comment — Response',       doc.api_comment_response)}
        {section('6. GET Rowversion (2) — Response', doc.api_get_rowversion2_response)}
        {section('7. Finalize — Request',            doc.api_finalize_payload)}
        {section('7. Finalize — Response',           doc.api_finalize_response)}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2, pt: 1, borderTop: '1px solid #f3f4f6' }}>
        <Button size="small" variant="outlined" onClick={onClose}
          sx={{ height: 30, fontSize: '0.78rem' }}>Close</Button>
      </DialogActions>
    </Dialog>
  )
}

// ── GRN tab ───────────────────────────────────────────────────────────────────

function GRNTab() {
  const qc = useQueryClient()
  const [selectedBatch, setSelectedBatch] = useState('')
  const [status, setStatus]               = useState('')
  const [detail, setDetail]               = useState<GRNDoc | null>(null)
  const [page, setPage]                   = useState(0)
  const pageSize                           = 100
  const [uploading, setUploading]         = useState(false)
  const [killing, setKilling]             = useState(false)
  const [toast, setToast]                 = useState<{ msg: string; severity: 'success' | 'error' | 'warning' } | null>(null)
  const abortRef                          = useRef<AbortController | null>(null)

  const { data: runStatus } = useQuery<{ running: boolean }>({
    queryKey: ['grn-status'],
    queryFn: () => apiClient.get('/api/grn/status').then(r => r.data),
    refetchInterval: 3_000,
  })
  const isRunning = uploading || !!runStatus?.running

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    const controller = new AbortController()
    abortRef.current = controller
    setUploading(true)
    setKilling(false)
    try {
      const form = new FormData()
      form.append('file', file)
      const res = await apiClient.post('/api/grn/import', form, {
        headers: { 'Content-Type': 'multipart/form-data' },
        signal: controller.signal,
      })
      const d = res.data
      if (d.cancelled) {
        setToast({ severity: 'warning', msg: `Stopped — ${d.total_docs} docs processed (${d.posted_docs} posted, ${d.error_docs} errors)` })
      } else {
        setToast({ severity: 'success', msg: `Done — ${d.total_docs} GRN docs, ${d.posted_docs} posted, ${d.error_docs} errors` })
      }
      qc.invalidateQueries({ queryKey: ['grn-batches'] })
      qc.invalidateQueries({ queryKey: ['grn-docs'] })
    } catch (err: unknown) {
      const isCancelled = (err as { name?: string })?.name === 'CanceledError'
                       || (err as { code?: string })?.code === 'ERR_CANCELED'
      if (!isCancelled) {
        const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || String(err)
        setToast({ severity: 'error', msg })
      }
      qc.invalidateQueries({ queryKey: ['grn-batches'] })
      qc.invalidateQueries({ queryKey: ['grn-docs'] })
    } finally {
      abortRef.current = null
      setUploading(false)
      setKilling(false)
    }
  }

  const handleKill = async () => {
    setKilling(true)
    try { await apiClient.post('/api/grn/kill') } catch { /* ignore */ }
    abortRef.current?.abort()
  }

  const { data: batches, isFetching: batchFetching } = useQuery<GRNBatch[]>({
    queryKey: ['grn-batches'],
    queryFn: () => apiClient.get('/api/grn/batches').then(r => r.data),
    refetchInterval: 30_000,
  })

  useEffect(() => {
    if (batches && batches.length > 0 && !selectedBatch)
      setSelectedBatch(batches[0].source_file)
  }, [batches, selectedBatch])

  const activeBatch = batches?.find(b => b.source_file === selectedBatch)

  const params: Record<string, string | number> = {
    limit: pageSize, offset: page * pageSize,
    ...(selectedBatch ? { source_file: selectedBatch } : {}),
    ...(status        ? { status }                     : {}),
  }

  const { data, isLoading, isFetching } = useQuery<{ total: number; items: GRNDoc[] }>({
    queryKey: ['grn-docs', params],
    queryFn: () => apiClient.get('/api/grn/docs', { params }).then(r => r.data),
    refetchInterval: isRunning ? 3_000 : 30_000,
    enabled: !!selectedBatch,
  })

  const docs       = data?.items ?? []
  const totalPages = Math.ceil((data?.total ?? 0) / pageSize)

  return (
    <Box>
      {/* Batch selector */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <FolderOutlinedIcon sx={{ fontSize: 15, color: '#9ca3af' }} />
        <Typography sx={{ fontSize: '0.72rem', color: '#374151', fontWeight: 600 }}>Batch:</Typography>
        <FormControl size="small" sx={{ minWidth: 280 }}>
          <Select value={selectedBatch} displayEmpty
            onChange={e => { setSelectedBatch(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}
            renderValue={v => {
              if (!v) return <em style={{ color: '#9ca3af' }}>Select a batch…</em>
              const b = batches?.find(x => x.source_file === v)
              return (
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <span>{v}</span>
                  {b && <Typography component="span" sx={{ fontSize: '0.65rem', color: '#9ca3af', ml: 0.5 }}>
                    ({b.doc_count} GRNs · {fmt(b.latest)})
                  </Typography>}
                </Box>
              )
            }}>
            {(batches ?? []).map(b => (
              <MenuItem key={b.source_file} value={b.source_file}>
                <Box sx={{ width: '100%' }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Typography sx={{ fontSize: '0.8rem', fontWeight: 500 }}>{b.source_file}</Typography>
                    <Typography sx={{ fontSize: '0.68rem', color: '#9ca3af' }}>{fmt(b.latest)}</Typography>
                  </Box>
                  <Box sx={{ display: 'flex', gap: 1, mt: 0.25 }}>
                    <Typography sx={{ fontSize: '0.65rem', color: '#15803d' }}>✓ {b.posted_docs} posted</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#b91c1c' }}>✗ {b.error_docs} errors</Typography>
                    <Typography sx={{ fontSize: '0.65rem', color: '#6b7280' }}>{b.doc_count} GRNs · {b.total_items} items</Typography>
                  </Box>
                </Box>
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        {batchFetching && <CircularProgress size={12} />}
      </Box>

      {/* Pills + toolbar */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <Pill label="GRN docs"     value={activeBatch?.doc_count    ?? 0} color="#f59e0b" />
        <Pill label="posted docs"  value={activeBatch?.posted_docs  ?? 0} color="#22c55e" />
        <Pill label="error docs"   value={activeBatch?.error_docs   ?? 0} color="#ef4444" />
        <Pill label="total items"  value={activeBatch?.total_items  ?? 0} color="#6b7280" />
        <Pill label="posted items" value={activeBatch?.posted_items ?? 0} color="#22c55e" />
        {isFetching && <CircularProgress size={13} sx={{ ml: 0.5 }} />}
        <Box sx={{ flexGrow: 1 }} />
        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel sx={{ fontSize: '0.78rem' }}>Status</InputLabel>
          <Select value={status} label="Status"
            onChange={e => { setStatus(e.target.value); setPage(0) }}
            sx={{ fontSize: '0.78rem' }}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="posted">Posted</MenuItem>
            <MenuItem value="partial">Partial</MenuItem>
            <MenuItem value="error">Error</MenuItem>
          </Select>
        </FormControl>

        {/* Manual CSV upload / Kill */}
        {isRunning ? (
          <Box sx={{ display: 'flex', gap: 0.75 }}>
            <Button size="small" variant="contained" disabled
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#f59e0b', pointerEvents: 'none' }}>
              <CircularProgress size={12} sx={{ color: 'white', mr: 0.75 }} />
              Processing…
            </Button>
            <Tooltip title="Stop import after current GRN document">
              <Button size="small" variant="contained" disabled={killing}
                onClick={handleKill}
                startIcon={killing
                  ? <CircularProgress size={12} sx={{ color: 'white' }} />
                  : <StopCircleOutlinedIcon sx={{ fontSize: 15 }} />}
                sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                  bgcolor: '#dc2626', '&:hover': { bgcolor: '#b91c1c' } }}>
                {killing ? 'Stopping…' : 'Stop'}
              </Button>
            </Tooltip>
          </Box>
        ) : (
          <Tooltip title="Upload GRN CSV manually">
            <Button component="label" size="small" variant="contained"
              sx={{ height: 30, fontSize: '0.75rem', textTransform: 'none',
                bgcolor: '#f59e0b', '&:hover': { bgcolor: '#d97706' } }}>
              Upload CSV
              <input type="file" accept=".csv" hidden onChange={handleUpload} />
            </Button>
          </Tooltip>
        )}

        <Tooltip title="Refresh">
          <IconButton size="small"
            onClick={() => {
              qc.invalidateQueries({ queryKey: ['grn-docs'] })
              qc.invalidateQueries({ queryKey: ['grn-batches'] })
            }}
            sx={{ borderRadius: '4px', border: '1px solid #e5e7eb' }}>
            <RefreshIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Table — one row per GRN voucher */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <TableContainer>
          <Table size="small" sx={{ tableLayout: 'fixed', minWidth: 920 }}>
            <TableHead>
              <TableRow sx={{ bgcolor: '#f9fafb' }}>
                <TableCell sx={thSx} width={44}>#</TableCell>
                <TableCell sx={thSx} width={130}>Note</TableCell>
                <TableCell sx={thSx} width={90}>Store</TableCell>
                <TableCell sx={thSx}>Store Name</TableCell>
                <TableCell sx={thSx} width={160}>Voucher SID</TableCell>
                <TableCell sx={thSx} width={65} align="center">Items</TableCell>
                <TableCell sx={thSx} width={65} align="center">Posted</TableCell>
                <TableCell sx={thSx} width={65} align="center">Errors</TableCell>
                <TableCell sx={thSx} width={85}>Status</TableCell>
                <TableCell sx={thSx} width={140}>Created At</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {!selectedBatch ? (
                <TableRow><TableCell colSpan={10} align="center"
                  sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                  Select a batch above to view records.
                </TableCell></TableRow>
              ) : isLoading ? (
                <TableRow><TableCell colSpan={10} align="center" sx={{ py: 6 }}>
                  <CircularProgress size={22} />
                </TableCell></TableRow>
              ) : docs.length === 0 ? (
                <TableRow><TableCell colSpan={10} align="center"
                  sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                  No GRN documents found.
                </TableCell></TableRow>
              ) : docs.map((doc, i) => (
                <TableRow key={doc.id} onClick={() => setDetail(doc)}
                  sx={{ cursor: 'pointer', '&:hover': { bgcolor: '#fffbeb' }, transition: 'background 0.1s' }}>
                  <TableCell sx={tdSx}>{page * pageSize + i + 1}</TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem',
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.note || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.72rem' }}>
                    {doc.store_code || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.store_name || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, fontFamily: 'monospace', fontSize: '0.68rem', color: '#6b7280',
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {doc.vousid || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center' }}>{doc.item_count}</TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center', color: '#15803d', fontWeight: 600 }}>
                    {doc.posted_count}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, textAlign: 'center',
                    color: doc.error_count > 0 ? '#b91c1c' : '#6b7280',
                    fontWeight: doc.error_count > 0 ? 600 : 400 }}>
                    {doc.error_count}
                  </TableCell>
                  <TableCell sx={tdSx}><AdjStatusChip status={doc.status} /></TableCell>
                  <TableCell sx={{ ...tdSx, whiteSpace: 'nowrap', color: '#6b7280' }}>
                    {fmt(doc.created_at)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        {(data?.total ?? 0) > pageSize && (
          <Box sx={{ px: 2, py: 1, borderTop: '1px solid #f3f4f6',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
              Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, data!.total)} of {data!.total}
            </Typography>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button size="small" variant="outlined" disabled={page === 0}
                onClick={() => setPage(p => p - 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Prev</Button>
              <Button size="small" variant="outlined" disabled={page >= totalPages - 1}
                onClick={() => setPage(p => p + 1)}
                sx={{ height: 26, fontSize: '0.72rem', minWidth: 56 }}>Next</Button>
            </Box>
          </Box>
        )}
      </Box>

      <GRNDetailDialog doc={detail} onClose={() => setDetail(null)} />

      <Snackbar open={!!toast} autoHideDuration={5000} onClose={() => setToast(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert severity={toast?.severity ?? 'info'} onClose={() => setToast(null)}
          sx={{ fontSize: '0.8rem' }}>
          {toast?.msg}
        </Alert>
      </Snackbar>
    </Box>
  )
}

// ── Module tab registry ───────────────────────────────────────────────────────

const MODULE_TABS = [
  { label: 'Item Master',       content: <ItemMasterTab /> },
  { label: 'QTY Adjustment',    content: <QtyAdjustmentTab /> },
  { label: 'Price Adjustment',  content: <PriceAdjustmentTab /> },
  { label: 'Transfer Slips',    content: <TransferSlipTab /> },
  { label: 'GRN',               content: <GRNTab /> },
]

// ── Page ──────────────────────────────────────────────────────────────────────

export default function ImportsPage() {
  const [tab, setTab] = useState(0)

  return (
    <Box sx={{ p: { xs: 2, sm: 3 }, display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Box>
        <Typography sx={{ fontWeight: 600, fontSize: '0.875rem', color: '#111827' }}>
          Import Records
        </Typography>
        <Typography sx={{ fontSize: '0.75rem', color: '#9ca3af' }}>
          All documents imported from FTP — automatically processed on schedule. Click a row for details.
        </Typography>
      </Box>

      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px',
        overflow: 'hidden' }}>
        <Tabs value={tab} onChange={(_, v) => setTab(v)}
          sx={{
            borderBottom: '1px solid #f3f4f6', minHeight: 42, px: 1,
            '& .MuiTab-root': { minHeight: 42, fontSize: '0.8rem', px: 2 },
            '& .Mui-selected': { color: '#1a56db', fontWeight: 600 },
          }}>
          {MODULE_TABS.map((t, i) => <Tab key={i} label={t.label} />)}
        </Tabs>

        <Box sx={{ p: { xs: 1.5, sm: 2.5 } }}>
          {MODULE_TABS[tab]?.content}
        </Box>
      </Box>
    </Box>
  )
}
