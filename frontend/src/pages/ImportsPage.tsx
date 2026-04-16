import { useState } from 'react'
import {
  Box, Typography, Tabs, Tab, Table, TableBody, TableCell,
  TableContainer, TableHead, TableRow, Chip, CircularProgress,
  TextField, Select, MenuItem, FormControl, InputLabel,
  IconButton, Tooltip, Button, Dialog, DialogTitle,
  DialogContent, DialogActions, Alert,
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

// ── Status chip ───────────────────────────────────────────────────────────────

function StatusChip({ doc }: { doc: DocItem }) {
  if (doc.posted) {
    return (
      <Chip
        icon={<CheckCircleOutlinedIcon sx={{ fontSize: '12px !important' }} />}
        label="Posted"
        size="small"
        sx={{ height: 22, fontSize: '0.72rem', borderRadius: '4px',
          bgcolor: '#f0fdf4', color: '#15803d', border: '1px solid #d1fae5' }}
      />
    )
  }
  if (doc.has_error) {
    return (
      <Chip
        icon={<ErrorOutlinedIcon sx={{ fontSize: '12px !important' }} />}
        label="Error"
        size="small"
        sx={{ height: 22, fontSize: '0.72rem', borderRadius: '4px',
          bgcolor: '#fef2f2', color: '#b91c1c', border: '1px solid #fecaca' }}
      />
    )
  }
  return (
    <Chip
      icon={<HourglassTopIcon sx={{ fontSize: '12px !important' }} />}
      label="Pending"
      size="small"
      sx={{ height: 22, fontSize: '0.72rem', borderRadius: '4px',
        bgcolor: '#fffbeb', color: '#b45309', border: '1px solid #fde68a' }}
    />
  )
}

// ── Detail dialog ─────────────────────────────────────────────────────────────

function DetailDialog({ doc, onClose }: { doc: DocItem | null; onClose: () => void }) {
  if (!doc) return null
  const upc = String(doc.original_data?.UPC ?? '—')
  const desc = String(doc.original_data?.DESCRIPTION1 ?? doc.original_data?.DESCRIPTION ?? '—')

  return (
    <Dialog open onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle sx={{ pb: 1 }}>
        {desc}
        <Typography component="span" sx={{ ml: 1, fontSize: '0.78rem', color: '#9ca3af',
          fontFamily: 'monospace' }}>
          UPC {upc}
        </Typography>
      </DialogTitle>
      <DialogContent dividers>
        <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 1.5, mb: 2 }}>
          {[
            ['Status', <StatusChip doc={doc} />],
            ['RetailPro SID', doc.retailprosid || '—'],
            ['Source File', doc.source_file || '—'],
            ['Created', new Date(doc.created_at).toLocaleString()],
            ['Posted At', doc.posted_at ? new Date(doc.posted_at).toLocaleString() : '—'],
          ].map(([label, value]) => (
            <Box key={String(label)}>
              <Typography sx={{ fontSize: '0.7rem', color: '#9ca3af', mb: 0.25 }}>{label}</Typography>
              <Typography sx={{ fontSize: '0.82rem', color: '#374151' }}>{value as any}</Typography>
            </Box>
          ))}
        </Box>

        {doc.error_message && (
          <Box sx={{ mb: 2 }}>
            <Typography sx={{ fontSize: '0.7rem', fontWeight: 600, color: '#374151', mb: 0.5 }}>
              API Error Response
            </Typography>
            <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: '4px',
              p: 1.5, maxHeight: 240, overflow: 'auto' }}>
              <Typography sx={{ fontSize: '0.75rem', fontFamily: 'monospace',
                whiteSpace: 'pre-wrap', color: '#7f1d1d', wordBreak: 'break-word' }}>
                {doc.error_message}
              </Typography>
            </Box>
          </Box>
        )}

        <Typography sx={{ fontSize: '0.7rem', fontWeight: 600, color: '#374151', mb: 0.5 }}>
          Original Row Data
        </Typography>
        <Box sx={{ bgcolor: '#f9fafb', border: '1px solid #e5e7eb', borderRadius: '4px',
          p: 1.5, maxHeight: 280, overflow: 'auto' }}>
          <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace',
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

// ── Item Master tab content ───────────────────────────────────────────────────

function ItemMasterTab() {
  const qc = useQueryClient()
  const [page, setPage] = useState(0)
  const [pageSize] = useState(50)
  const [status, setStatus] = useState('')
  const [search, setSearch] = useState('')
  const [selectedDoc, setSelectedDoc] = useState<DocItem | null>(null)

  const params: Record<string, string | number> = {
    document_type: 'item_master',
    limit: pageSize,
    offset: page * pageSize,
  }
  if (status) params.status = status

  const { data, isLoading, isFetching } = useQuery<DocsResponse>({
    queryKey: ['imports-item-master', params],
    queryFn: () => apiClient.get('/api/documents', { params }).then((r) => r.data),
    refetchInterval: 30000,
  })

  const items = data?.items ?? []
  const filtered = search
    ? items.filter((d) =>
        JSON.stringify(d).toLowerCase().includes(search.toLowerCase())
      )
    : items

  const totalPages = Math.ceil((data?.total ?? 0) / pageSize)

  return (
    <Box>
      {/* Toolbar */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2, flexWrap: 'wrap' }}>
        <Typography sx={{ fontSize: '0.78rem', color: '#9ca3af' }}>
          {data?.total ?? 0} records
        </Typography>
        {isFetching && <CircularProgress size={13} />}

        <Box sx={{ flexGrow: 1 }} />

        <TextField size="small" placeholder="Search…" value={search}
          onChange={(e) => setSearch(e.target.value)}
          sx={{ width: 180, '& .MuiOutlinedInput-input': { fontSize: '0.8rem' } }} />

        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel>Status</InputLabel>
          <Select value={status} label="Status" onChange={(e) => setStatus(e.target.value)}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="posted">Posted</MenuItem>
            <MenuItem value="error">Error</MenuItem>
            <MenuItem value="pending">Pending</MenuItem>
          </Select>
        </FormControl>

        <Tooltip title="Refresh">
          <IconButton size="small"
            onClick={() => qc.invalidateQueries({ queryKey: ['imports-item-master'] })}
            sx={{ borderRadius: '4px' }}>
            <RefreshIcon sx={{ fontSize: 17 }} />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Table */}
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>#</TableCell>
                <TableCell>UPC</TableCell>
                <TableCell>Description</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>RetailPro SID</TableCell>
                <TableCell>Source File</TableCell>
                <TableCell>Imported At</TableCell>
                <TableCell>Posted At</TableCell>
                <TableCell align="right">Detail</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={9} align="center" sx={{ py: 5 }}>
                    <CircularProgress size={22} />
                  </TableCell>
                </TableRow>
              ) : filtered.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} align="center" sx={{ py: 5, color: '#9ca3af', fontSize: '0.82rem' }}>
                    No records found.
                  </TableCell>
                </TableRow>
              ) : filtered.map((doc, i) => {
                const upc = String(doc.original_data?.UPC ?? '—')
                const desc = String(doc.original_data?.DESCRIPTION1 ?? doc.original_data?.DESCRIPTION ?? '—')
                return (
                  <TableRow key={doc.id}>
                    <TableCell sx={{ color: '#9ca3af', fontSize: '0.72rem' }}>
                      {page * pageSize + i + 1}
                    </TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{upc}</TableCell>
                    <TableCell sx={{ maxWidth: 200, overflow: 'hidden',
                      textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: '0.78rem' }}>
                      {desc}
                    </TableCell>
                    <TableCell><StatusChip doc={doc} /></TableCell>
                    <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>
                      {doc.retailprosid || '—'}
                    </TableCell>
                    <TableCell sx={{ fontSize: '0.72rem', color: '#9ca3af', maxWidth: 160,
                      overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {doc.source_file || '—'}
                    </TableCell>
                    <TableCell sx={{ fontSize: '0.72rem', color: '#6b7280', whiteSpace: 'nowrap' }}>
                      {new Date(doc.created_at).toLocaleString()}
                    </TableCell>
                    <TableCell sx={{ fontSize: '0.72rem', color: '#6b7280', whiteSpace: 'nowrap' }}>
                      {doc.posted_at ? new Date(doc.posted_at).toLocaleString() : '—'}
                    </TableCell>
                    <TableCell align="right">
                      <Tooltip title="View details">
                        <IconButton size="small" onClick={() => setSelectedDoc(doc)}
                          sx={{ borderRadius: '4px', width: 26, height: 26, color: '#9ca3af',
                            '&:hover': { color: '#1a56db', bgcolor: '#eff6ff' } }}>
                          <InfoOutlinedIcon sx={{ fontSize: 14 }} />
                        </IconButton>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
        </TableContainer>

        {/* Pagination */}
        {(data?.total ?? 0) > pageSize && (
          <Box sx={{ px: 2, py: 1, borderTop: '1px solid #f3f4f6',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
              Page {page + 1} of {totalPages}
            </Typography>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button size="small" variant="outlined" disabled={page === 0}
                onClick={() => setPage(p => p - 1)} sx={{ height: 26, fontSize: '0.72rem', minWidth: 60 }}>
                Prev
              </Button>
              <Button size="small" variant="outlined" disabled={page >= totalPages - 1}
                onClick={() => setPage(p => p + 1)} sx={{ height: 26, fontSize: '0.72rem', minWidth: 60 }}>
                Next
              </Button>
            </Box>
          </Box>
        )}
      </Box>

      <DetailDialog doc={selectedDoc} onClose={() => setSelectedDoc(null)} />
    </Box>
  )
}

// ── Page ──────────────────────────────────────────────────────────────────────

const MODULE_TABS = [
  { label: 'Item Master', key: 'item_master' },
  // Future modules: Receiving Voucher, Inventory Adjustment, etc.
]

export default function ImportsPage() {
  const [tab, setTab] = useState(0)

  return (
    <Box sx={{ p: { xs: 2, sm: 3 }, display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Box>
        <Typography sx={{ fontWeight: 600, fontSize: '0.875rem', color: '#111827' }}>
          Import Records
        </Typography>
        <Typography sx={{ fontSize: '0.75rem', color: '#9ca3af' }}>
          Track all imported documents — posted, pending, and errors — per module.
        </Typography>
      </Box>

      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <Tabs
          value={tab}
          onChange={(_, v) => setTab(v)}
          sx={{
            borderBottom: '1px solid #f3f4f6', minHeight: 44, px: 1,
            '& .MuiTab-root': { minHeight: 44, fontSize: '0.8rem', px: 2 },
            '& .Mui-selected': { color: '#1a56db', fontWeight: 600 },
          }}
        >
          {MODULE_TABS.map((t, i) => <Tab key={i} label={t.label} />)}
        </Tabs>

        <Box sx={{ p: { xs: 1.5, sm: 2.5 } }}>
          {tab === 0 && <ItemMasterTab />}
        </Box>
      </Box>
    </Box>
  )
}
