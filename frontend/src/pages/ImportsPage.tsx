import { useState } from 'react'
import {
  Box, Typography, Tabs, Tab, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, Chip, CircularProgress,
  TextField, Select, MenuItem, FormControl, InputLabel,
  IconButton, Tooltip, Button, Dialog, DialogTitle,
  DialogContent, DialogActions,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined'
import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
import HourglassTopIcon from '@mui/icons-material/HourglassTop'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import apiClient from '../api/client'

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

// ── Helpers ───────────────────────────────────────────────────────────────────

const cell = (doc: DocItem, key: string): string =>
  String(doc.original_data?.[key] ?? '—')

const fmt = (iso: string | null) =>
  iso ? new Date(iso).toLocaleString() : '—'

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

// ── Detail dialog ─────────────────────────────────────────────────────────────

function DetailDialog({ doc, onClose }: { doc: DocItem | null; onClose: () => void }) {
  if (!doc) return null
  const upc  = cell(doc, 'UPC')
  const desc = cell(doc, 'DESCRIPTION1') !== '—' ? cell(doc, 'DESCRIPTION1') : cell(doc, 'DESCRIPTION')

  return (
    <Dialog open onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle sx={{ pb: 0.5, fontSize: '0.95rem', fontWeight: 600 }}>
        {desc}
        <Typography component="span" sx={{ ml: 1.5, fontSize: '0.72rem',
          color: '#9ca3af', fontFamily: 'monospace' }}>
          UPC {upc}
        </Typography>
      </DialogTitle>
      <DialogContent dividers>
        {/* Summary grid */}
        <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 1.5, mb: 2 }}>
          {([
            ['Status',        <StatusChip doc={doc} />],
            ['RetailPro SID', doc.retailprosid || '—'],
            ['Source File',   doc.source_file  || '—'],
            ['DCS Code',      cell(doc, 'DCS_CODE')],
            ['Vendor Code',   cell(doc, 'VEND_CODE')],
            ['Tax Code',      cell(doc, 'TAX_CODE')],
            ['Imported At',   fmt(doc.created_at)],
            ['Posted At',     fmt(doc.posted_at)],
          ] as [string, React.ReactNode][]).map(([label, value]) => (
            <Box key={label}>
              <Typography sx={{ fontSize: '0.67rem', color: '#9ca3af', mb: 0.2 }}>{label}</Typography>
              <Typography sx={{ fontSize: '0.78rem', color: '#374151' }}>{value}</Typography>
            </Box>
          ))}
        </Box>

        {/* Error */}
        {doc.error_message && (
          <Box sx={{ mb: 2 }}>
            <Typography sx={{ fontSize: '0.7rem', fontWeight: 600, color: '#374151', mb: 0.5 }}>
              API Error Response
            </Typography>
            <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: '4px',
              p: 1.5, maxHeight: 220, overflow: 'auto' }}>
              <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace',
                whiteSpace: 'pre-wrap', color: '#7f1d1d', wordBreak: 'break-word' }}>
                {doc.error_message}
              </Typography>
            </Box>
          </Box>
        )}

        {/* Raw row */}
        <Typography sx={{ fontSize: '0.7rem', fontWeight: 600, color: '#374151', mb: 0.5 }}>
          Original Row Data
        </Typography>
        <Box sx={{ bgcolor: '#f9fafb', border: '1px solid #e5e7eb', borderRadius: '4px',
          p: 1.5, maxHeight: 260, overflow: 'auto' }}>
          <Typography sx={{ fontSize: '0.7rem', fontFamily: 'monospace',
            whiteSpace: 'pre-wrap', color: '#374151' }}>
            {JSON.stringify(doc.original_data, null, 2)}
          </Typography>
        </Box>
      </DialogContent>
      <DialogActions sx={{ px: 2.5, pb: 2 }}>
        <Button size="small" variant="outlined" onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  )
}

// ── Stat pill ─────────────────────────────────────────────────────────────────

function Pill({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75,
      px: 1.5, py: 0.5, border: '1px solid #e5e7eb', borderRadius: '6px',
      bgcolor: 'white' }}>
      <Box sx={{ width: 7, height: 7, borderRadius: '50%', bgcolor: color }} />
      <Typography sx={{ fontSize: '0.72rem', color: '#374151' }}>
        <b>{value}</b> {label}
      </Typography>
    </Box>
  )
}

// ── Item Master tab ───────────────────────────────────────────────────────────

function ItemMasterTab() {
  const qc = useQueryClient()
  const [page, setPage]     = useState(0)
  const pageSize             = 100
  const [status, setStatus] = useState('')
  const [search, setSearch] = useState('')
  const [detail, setDetail] = useState<DocItem | null>(null)

  const params: Record<string, string | number> = {
    document_type: 'item_master',
    limit: pageSize,
    offset: page * pageSize,
    ...(status ? { status } : {}),
  }

  const { data, isLoading, isFetching } = useQuery<DocsResponse>({
    queryKey: ['imports-im', params],
    queryFn: () => apiClient.get('/api/documents', { params }).then(r => r.data),
    refetchInterval: 30_000,
  })

  const items = data?.items ?? []
  const filtered = search
    ? items.filter(d => JSON.stringify(d).toLowerCase().includes(search.toLowerCase()))
    : items

  const totalPages = Math.ceil((data?.total ?? 0) / pageSize)

  const posted  = items.filter(d => d.posted).length
  const errors  = items.filter(d => d.has_error).length
  const pending = items.filter(d => !d.posted && !d.has_error).length

  return (
    <Box>
      {/* Toolbar */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2, flexWrap: 'wrap' }}>
        <Pill label="posted"  value={posted}           color="#22c55e" />
        <Pill label="errors"  value={errors}           color="#ef4444" />
        <Pill label="pending" value={pending}          color="#f59e0b" />
        <Pill label="total"   value={data?.total ?? 0} color="#6b7280" />

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
            onClick={() => qc.invalidateQueries({ queryKey: ['imports-im'] })}
            sx={{ borderRadius: '4px', border: '1px solid #e5e7eb' }}>
            <RefreshIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Table */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px',
        overflow: 'hidden' }}>
        <TableContainer>
          <Table size="small" sx={{ tableLayout: 'fixed', minWidth: 900 }}>
            <TableHead>
              <TableRow sx={{ bgcolor: '#f9fafb' }}>
                <TableCell sx={thSx} width={44}>#</TableCell>
                <TableCell sx={thSx} width={130}>UPC</TableCell>
                <TableCell sx={thSx}>Description</TableCell>
                <TableCell sx={thSx} width={110}>DCS Code</TableCell>
                <TableCell sx={thSx} width={100}>Vendor</TableCell>
                <TableCell sx={thSx} width={80}>Status</TableCell>
                <TableCell sx={thSx} width={150}>RetailPro SID</TableCell>
                <TableCell sx={thSx} width={130}>Source File</TableCell>
                <TableCell sx={thSx} width={140}>Imported At</TableCell>
                <TableCell sx={{ ...thSx, textAlign: 'right' }} width={52}></TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={10} align="center" sx={{ py: 6 }}>
                    <CircularProgress size={22} />
                  </TableCell>
                </TableRow>
              ) : filtered.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={10} align="center"
                    sx={{ py: 6, color: '#9ca3af', fontSize: '0.82rem' }}>
                    No records found.
                  </TableCell>
                </TableRow>
              ) : filtered.map((doc, i) => (
                <TableRow key={doc.id}
                  sx={{ '&:hover': { bgcolor: '#f9fafb' }, cursor: 'default' }}>
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
                  <TableCell sx={{ ...tdSx, overflow: 'hidden',
                    textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: '#9ca3af' }}>
                    {doc.source_file || '—'}
                  </TableCell>
                  <TableCell sx={{ ...tdSx, whiteSpace: 'nowrap', color: '#6b7280' }}>
                    {fmt(doc.created_at)}
                  </TableCell>
                  <TableCell align="right" sx={tdSx}>
                    <Tooltip title="View details">
                      <IconButton size="small" onClick={() => setDetail(doc)}
                        sx={{ width: 24, height: 24, borderRadius: '4px', color: '#9ca3af',
                          '&:hover': { color: '#1a56db', bgcolor: '#eff6ff' } }}>
                        <InfoOutlinedIcon sx={{ fontSize: 13 }} />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>

        {/* Pagination footer */}
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

// ── Table style helpers ───────────────────────────────────────────────────────

const thSx = {
  fontSize: '0.68rem',
  fontWeight: 600,
  color: '#6b7280',
  textTransform: 'uppercase' as const,
  letterSpacing: '0.04em',
  py: 1,
  px: 1.5,
  borderBottom: '1px solid #e5e7eb',
}

const tdSx = {
  fontSize: '0.78rem',
  color: '#374151',
  py: 0.75,
  px: 1.5,
  borderBottom: '1px solid #f3f4f6',
}

// ── Module tab registry (add new modules here) ────────────────────────────────

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
          All documents imported from FTP — automatically processed on schedule.
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
