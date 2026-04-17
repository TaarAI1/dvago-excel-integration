import { useState, useEffect } from 'react'
import {
  Box, Typography, Tabs, Tab, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, Chip, CircularProgress,
  TextField, Select, MenuItem, FormControl, InputLabel,
  IconButton, Tooltip, Button, Dialog,
  DialogContent, DialogActions,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
import HourglassTopIcon from '@mui/icons-material/HourglassTop'
import FolderOutlinedIcon from '@mui/icons-material/FolderOutlined'
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

  const upc   = cell(doc, 'UPC')
  const desc  = cell(doc, 'DESCRIPTION1') !== '—' ? cell(doc, 'DESCRIPTION1') : cell(doc, 'DESCRIPTION')
  const desc2 = cell(doc, 'DESCRIPTION2')
  const cost  = cell(doc, 'COST')
  const dcs   = cell(doc, 'DCS_CODE')
  const vend  = cell(doc, 'VEND_CODE')
  const tax   = cell(doc, 'TAX_CODE')
  const sbs   = cell(doc, 'SBS_NO')
  const alu   = cell(doc, 'ALU')

  // Payload sent to RetailPro (only present on error records)
  const payloadSent = doc.original_data?._payload_sent as Record<string, unknown> | undefined

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
          {desc !== '—' ? desc : '(no description)'}
        </Typography>
        {desc2 !== '—' && (
          <Typography sx={{ fontSize: '0.78rem', color: '#6b7280', mt: 0.25 }}>{desc2}</Typography>
        )}
        <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af', mt: 0.5,
          fontFamily: 'monospace', letterSpacing: '0.04em' }}>
          UPC: {upc}
        </Typography>
      </Box>

      <DialogContent sx={{ p: 0, maxHeight: '72vh', overflowY: 'auto' }}>
        {/* Item details */}
        <Box sx={{ px: 3, pt: 2, pb: 1 }}>
          <Typography sx={{ fontSize: '0.65rem', fontWeight: 700, letterSpacing: '0.08em',
            textTransform: 'uppercase', color: '#9ca3af', mb: 1 }}>
            Item Details
          </Typography>
          <InfoRow label="DCS Code"    value={dcs} />
          <InfoRow label="Vendor Code" value={vend} />
          <InfoRow label="Tax Code"    value={tax} />
          {sbs  !== '—' && <InfoRow label="Subsidiary No" value={sbs} />}
          {alu  !== '—' && <InfoRow label="ALU"           value={alu} />}
          {cost !== '—' && <InfoRow label="Cost"          value={cost} />}
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
        {payloadSent && (
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
                {JSON.stringify(payloadSent, null, 2)}
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
    refetchInterval: 30_000,
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
    </Box>
  )
}

// ── Module tab registry ───────────────────────────────────────────────────────

const MODULE_TABS = [
  { label: 'Item Master', content: <ItemMasterTab /> },
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
