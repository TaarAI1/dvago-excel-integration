import { useState, useCallback } from 'react'
import {
  Box, Typography, Table, TableBody, TableCell, TableContainer,
  TableHead, TableRow, CircularProgress, IconButton, Tooltip,
  Dialog, DialogContent, DialogActions, Button, TextField,
  Chip, Paper, Divider,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import AssessmentOutlinedIcon from '@mui/icons-material/AssessmentOutlined'
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline'
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline'
import PendingOutlinedIcon from '@mui/icons-material/PendingOutlined'
import { useQuery } from '@tanstack/react-query'
import apiClient from '../api/client'
import { fmtDateTime } from '../utils/time'

// ── Types ────────────────────────────────────────────────────────────────────

interface ReportRow {
  id: string
  created_at: string | null
  status: string
  sid: string | null
  note: string | null
  upc: string | null
  store: string | null
  file_type?: string
  filename?: string
  error_message: string | null
  item_count: number
  posted_count: number
  error_count: number
  source_file: string | null
  items_data: unknown[] | null
}

interface ReportSummary {
  total: number
  success: number
  error: number
  partial: number
  pending: number
}

interface ReportResponse {
  summary: ReportSummary
  rows: ReportRow[]
  total_count: number
}

// ── Constants ────────────────────────────────────────────────────────────────

const MODULES = [
  { key: 'grn',              label: 'GRN' },
  { key: 'transfer_slip',    label: 'Transfer Slip' },
  { key: 'qty_adjustment',   label: 'Qty Adjustment' },
  { key: 'price_adjustment', label: 'Price Adjustment' },
  { key: 'item_master',      label: 'Item Master' },
  { key: 'sales_export',     label: 'Sales Export' },
]

const PAGE_SIZE = 100

function toDatetimeLocal(d: Date): string {
  const pad = (n: number) => n.toString().padStart(2, '0')
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
    `T${pad(d.getHours())}:${pad(d.getMinutes())}`
  )
}

function makeRange(hoursBack: number) {
  const now = new Date()
  return {
    from: toDatetimeLocal(new Date(now.getTime() - hoursBack * 3600 * 1000)),
    to:   toDatetimeLocal(now),
  }
}

// ── Status badge ─────────────────────────────────────────────────────────────

function StatusBadge({ status }: { status: string }) {
  const map: Record<string, { color: string; bg: string; label: string }> = {
    posted:  { color: '#15803d', bg: '#f0fdf4', label: 'Posted' },
    done:    { color: '#15803d', bg: '#f0fdf4', label: 'Done' },
    success: { color: '#15803d', bg: '#f0fdf4', label: 'Success' },
    error:   { color: '#b91c1c', bg: '#fef2f2', label: 'Error' },
    partial: { color: '#b45309', bg: '#fffbeb', label: 'Partial' },
    pending: { color: '#6b7280', bg: '#f9fafb', label: 'Pending' },
  }
  const s = map[status?.toLowerCase()] ?? { color: '#6b7280', bg: '#f9fafb', label: status || '—' }
  return (
    <Box
      component="span"
      sx={{
        display: 'inline-block', px: 1, py: 0.25, borderRadius: '4px',
        fontSize: '0.72rem', fontWeight: 600,
        color: s.color, bgcolor: s.bg,
      }}
    >
      {s.label}
    </Box>
  )
}

// ── Summary card ─────────────────────────────────────────────────────────────

function SummaryCard({
  label, value, color, icon,
}: {
  label: string; value: number; color: string; icon: React.ReactNode
}) {
  return (
    <Paper variant="outlined" sx={{ p: 2, flex: 1, minWidth: 120 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
        <Box sx={{ color, display: 'flex' }}>{icon}</Box>
        <Typography sx={{ fontSize: '0.75rem', color: '#6b7280', fontWeight: 500 }}>{label}</Typography>
      </Box>
      <Typography sx={{ fontSize: '1.5rem', fontWeight: 700, color }}>{value.toLocaleString()}</Typography>
    </Paper>
  )
}

// ── Detail dialog ─────────────────────────────────────────────────────────────

function DetailDialog({ row, module, onClose }: { row: ReportRow | null; module: string; onClose: () => void }) {
  if (!row) return null
  const isSalesExport = module === 'sales_export'
  const isItemMaster  = module === 'item_master'

  return (
    <Dialog open={!!row} onClose={onClose} maxWidth="md" fullWidth>
      <Box sx={{ px: 2.5, pt: 2.5, pb: 1, borderBottom: '1px solid #e5e7eb' }}>
        <Typography sx={{ fontWeight: 600, fontSize: '0.95rem' }}>Document Detail</Typography>
      </Box>
      <DialogContent sx={{ p: 0 }}>
        <Box sx={{ p: 2.5, display: 'flex', flexDirection: 'column', gap: 1.5 }}>

          {/* Mandatory fields */}
          <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 1.5 }}>
            {!isSalesExport && (
              <Field label="SID" value={row.sid} />
            )}
            {!isItemMaster && !isSalesExport && (
              <Field label="Note" value={row.note} />
            )}
            {isItemMaster && (
              <Field label="UPC" value={row.upc} />
            )}
            {isSalesExport && (
              <>
                <Field label="Store" value={row.store} />
                <Field label="File Type" value={row.file_type} />
                <Field label="Filename" value={row.filename} />
              </>
            )}
            {!isSalesExport && (
              <Field label="Store" value={row.store} />
            )}
            <Field label="Status" value={<StatusBadge status={row.status} />} />
            <Field label="Created" value={row.created_at ? fmtDateTime(row.created_at) : '—'} />
            {!isSalesExport && (
              <>
                <Field label="Items" value={`${row.posted_count} / ${row.item_count} posted`} />
                <Field label="Errors" value={String(row.error_count)} />
              </>
            )}
            {isSalesExport && (
              <Field label="Rows" value={`${row.posted_count} / ${row.item_count} written`} />
            )}
            <Field label="Source File" value={row.source_file} />
          </Box>

          {row.error_message && (
            <>
              <Divider />
              <Box>
                <Typography sx={{ fontSize: '0.72rem', fontWeight: 600, color: '#6b7280', mb: 0.5 }}>ERROR MESSAGE</Typography>
                <Box sx={{ bgcolor: '#fef2f2', border: '1px solid #fecaca', borderRadius: 1, p: 1.5 }}>
                  <Typography sx={{ fontSize: '0.8rem', color: '#b91c1c', whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                    {row.error_message}
                  </Typography>
                </Box>
              </Box>
            </>
          )}

          {Array.isArray(row.items_data) && row.items_data.length > 0 && (
            <>
              <Divider />
              <Box>
                <Typography sx={{ fontSize: '0.72rem', fontWeight: 600, color: '#6b7280', mb: 0.5 }}>
                  ITEM DETAIL ({row.items_data.length} items)
                </Typography>
                <TableContainer sx={{ maxHeight: 300, border: '1px solid #e5e7eb', borderRadius: 1 }}>
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        <TableCell>UPC</TableCell>
                        <TableCell>Qty / Value</TableCell>
                        <TableCell>Item SID</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Error</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {(row.items_data as Record<string, unknown>[]).map((item, i) => (
                        <TableRow key={i}>
                          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>
                            {String(item.upc ?? '—')}
                          </TableCell>
                          <TableCell>{String(item.qty ?? item.adj_value ?? '—')}</TableCell>
                          <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.72rem' }}>
                            {String(item.item_sid ?? '—')}
                          </TableCell>
                          <TableCell>
                            <StatusBadge status={item.ok ? 'posted' : 'error'} />
                          </TableCell>
                          <TableCell sx={{ fontSize: '0.72rem', color: '#b91c1c', maxWidth: 200, wordBreak: 'break-word' }}>
                            {String(item.error ?? '')}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
            </>
          )}
        </Box>
      </DialogContent>
      <DialogActions sx={{ px: 2.5, py: 1.5, borderTop: '1px solid #e5e7eb' }}>
        <Button onClick={onClose} size="small" variant="outlined">Close</Button>
      </DialogActions>
    </Dialog>
  )
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <Box>
      <Typography sx={{ fontSize: '0.68rem', fontWeight: 600, color: '#9ca3af', textTransform: 'uppercase', mb: 0.25 }}>
        {label}
      </Typography>
      <Typography sx={{ fontSize: '0.82rem', color: '#111827', wordBreak: 'break-word' }}>
        {value ?? <span style={{ color: '#d1d5db' }}>—</span>}
      </Typography>
    </Box>
  )
}

// ── Table columns by module ───────────────────────────────────────────────────

function ReportTable({
  rows, module, onRowClick,
}: {
  rows: ReportRow[]; module: string; onRowClick: (r: ReportRow) => void
}) {
  const isSalesExport = module === 'sales_export'
  const isItemMaster  = module === 'item_master'

  return (
    <TableContainer>
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Timestamp</TableCell>
            <TableCell>Status</TableCell>
            {!isSalesExport && <TableCell>SID</TableCell>}
            {!isItemMaster && !isSalesExport && <TableCell>Note</TableCell>}
            {isItemMaster && <TableCell>UPC</TableCell>}
            {isSalesExport && <TableCell>Store</TableCell>}
            {isSalesExport && <TableCell>Type</TableCell>}
            {!isSalesExport && <TableCell>Store</TableCell>}
            <TableCell align="right">Items</TableCell>
            <TableCell align="right">Posted</TableCell>
            <TableCell align="right">Errors</TableCell>
            <TableCell>Error Message</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.length === 0 && (
            <TableRow>
              <TableCell colSpan={9} align="center" sx={{ py: 5, color: '#9ca3af', fontSize: '0.82rem' }}>
                No records found for the selected filters.
              </TableCell>
            </TableRow>
          )}
          {rows.map((row) => (
            <TableRow
              key={row.id}
              onClick={() => onRowClick(row)}
              sx={{ cursor: 'pointer', '&:hover': { bgcolor: '#f0f9ff' } }}
            >
              <TableCell sx={{ fontSize: '0.78rem', whiteSpace: 'nowrap', color: '#374151' }}>
                {row.created_at ? fmtDateTime(row.created_at) : '—'}
              </TableCell>
              <TableCell><StatusBadge status={row.status} /></TableCell>
              {!isSalesExport && (
                <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.72rem', maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {row.sid || '—'}
                </TableCell>
              )}
              {!isItemMaster && !isSalesExport && (
                <TableCell sx={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: '0.8rem' }}>
                  {row.note || '—'}
                </TableCell>
              )}
              {isItemMaster && (
                <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{row.upc || '—'}</TableCell>
              )}
              {isSalesExport && (
                <TableCell sx={{ fontSize: '0.8rem' }}>{row.store || '—'}</TableCell>
              )}
              {isSalesExport && (
                <TableCell sx={{ fontSize: '0.75rem', color: '#6b7280' }}>{row.file_type || '—'}</TableCell>
              )}
              {!isSalesExport && (
                <TableCell sx={{ fontSize: '0.8rem', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {row.store || '—'}
                </TableCell>
              )}
              <TableCell align="right" sx={{ fontSize: '0.8rem' }}>{row.item_count}</TableCell>
              <TableCell align="right" sx={{ fontSize: '0.8rem', color: '#15803d' }}>{row.posted_count}</TableCell>
              <TableCell align="right" sx={{ fontSize: '0.8rem', color: row.error_count > 0 ? '#b91c1c' : '#6b7280' }}>
                {row.error_count}
              </TableCell>
              <TableCell sx={{ maxWidth: 260, fontSize: '0.75rem', color: '#b91c1c', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {row.error_message || ''}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  )
}

// ── Page ─────────────────────────────────────────────────────────────────────

export default function ReportsPage() {
  const init = makeRange(24)
  const [module,     setModule]     = useState('grn')
  const [dateFrom,   setDateFrom]   = useState(init.from)
  const [dateTo,     setDateTo]     = useState(init.to)
  const [page,       setPage]       = useState(0)
  const [detailRow,  setDetailRow]  = useState<ReportRow | null>(null)
  const [tick,       setTick]       = useState(0)

  const refresh = useCallback(() => {
    setPage(0)
    setTick(t => t + 1)
  }, [])

  const applyRange = (h: number) => {
    const r = makeRange(h)
    setDateFrom(r.from)
    setDateTo(r.to)
    setPage(0)
    setTick(t => t + 1)
  }

  const { data, isFetching } = useQuery<ReportResponse>({
    queryKey: ['reports', module, dateFrom, dateTo, page, tick],
    queryFn: async () => {
      const params = new URLSearchParams({
        module,
        date_from: new Date(dateFrom).toISOString(),
        date_to:   new Date(dateTo).toISOString(),
        limit:     String(PAGE_SIZE),
        offset:    String(page * PAGE_SIZE),
      })
      const res = await apiClient.get(`/api/reports?${params}`)
      return res.data
    },
    placeholderData: (prev) => prev,
  })

  const summary     = data?.summary ?? { total: 0, success: 0, error: 0, partial: 0, pending: 0 }
  const rows        = data?.rows    ?? []
  const totalCount  = data?.total_count ?? 0
  const totalPages  = Math.max(1, Math.ceil(totalCount / PAGE_SIZE))

  return (
    <Box sx={{ p: { xs: 2, sm: 3 }, maxWidth: 1400, mx: 'auto' }}>

      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2.5 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <AssessmentOutlinedIcon sx={{ color: '#1a56db', fontSize: 22 }} />
          <Typography sx={{ fontWeight: 700, fontSize: '1.05rem', color: '#111827' }}>Reports</Typography>
          {totalCount > 0 && (
            <Chip label={`${totalCount.toLocaleString()} records`} size="small"
              sx={{ ml: 1, fontSize: '0.72rem', bgcolor: '#eff6ff', color: '#1a56db', fontWeight: 600 }} />
          )}
        </Box>
        <Tooltip title="Refresh">
          <IconButton onClick={refresh} size="small" disabled={isFetching}
            sx={{ border: '1px solid #e5e7eb', borderRadius: '6px' }}>
            {isFetching
              ? <CircularProgress size={15} />
              : <RefreshIcon sx={{ fontSize: 17, color: '#6b7280' }} />}
          </IconButton>
        </Tooltip>
      </Box>

      {/* Module selector */}
      <Box sx={{ display: 'flex', gap: 0.75, flexWrap: 'wrap', mb: 2.5 }}>
        {MODULES.map(m => (
          <Button
            key={m.key}
            size="small"
            variant={module === m.key ? 'contained' : 'outlined'}
            onClick={() => { setModule(m.key); setPage(0); setTick(t => t + 1) }}
            sx={{
              fontSize: '0.78rem', px: 1.5, py: 0.5,
              ...(module !== m.key && { color: '#374151', borderColor: '#d1d5db' }),
            }}
          >
            {m.label}
          </Button>
        ))}
      </Box>

      {/* Date filter */}
      <Paper variant="outlined" sx={{ p: 1.5, mb: 2.5 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 1 }}>
          <Typography sx={{ fontSize: '0.78rem', color: '#6b7280', fontWeight: 500, mr: 0.5 }}>Range:</Typography>
          {[
            { label: 'Last 24h', h: 24 },
            { label: 'Last 48h', h: 48 },
            { label: 'Last 7d',  h: 168 },
            { label: 'Last 30d', h: 720 },
          ].map(q => (
            <Button key={q.h} size="small" variant="outlined"
              onClick={() => applyRange(q.h)}
              sx={{ fontSize: '0.72rem', px: 1.25, py: 0.25, color: '#374151', borderColor: '#d1d5db' }}>
              {q.label}
            </Button>
          ))}
          <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
          <TextField
            type="datetime-local" size="small" label="From"
            value={dateFrom}
            onChange={e => { setDateFrom(e.target.value); setPage(0) }}
            InputLabelProps={{ shrink: true }}
            sx={{ '& input': { fontSize: '0.78rem' } }}
          />
          <TextField
            type="datetime-local" size="small" label="To"
            value={dateTo}
            onChange={e => { setDateTo(e.target.value); setPage(0) }}
            InputLabelProps={{ shrink: true }}
            sx={{ '& input': { fontSize: '0.78rem' } }}
          />
          <Button size="small" variant="contained" onClick={refresh} sx={{ fontSize: '0.78rem' }}>
            Apply
          </Button>
        </Box>
      </Paper>

      {/* Summary cards */}
      <Box sx={{ display: 'flex', gap: 1.5, mb: 2.5, flexWrap: 'wrap' }}>
        <SummaryCard label="Total"   value={summary.total}   color="#1a56db"
          icon={<AssessmentOutlinedIcon sx={{ fontSize: 18 }} />} />
        <SummaryCard label="Success" value={summary.success} color="#15803d"
          icon={<CheckCircleOutlineIcon sx={{ fontSize: 18 }} />} />
        <SummaryCard label="Error"   value={summary.error}   color="#b91c1c"
          icon={<ErrorOutlineIcon sx={{ fontSize: 18 }} />} />
        <SummaryCard label="Partial" value={summary.partial} color="#b45309"
          icon={<PendingOutlinedIcon sx={{ fontSize: 18 }} />} />
        <SummaryCard label="Pending" value={summary.pending} color="#6b7280"
          icon={<PendingOutlinedIcon sx={{ fontSize: 18 }} />} />
      </Box>

      {/* Table */}
      <Paper variant="outlined" sx={{ overflow: 'hidden' }}>
        <ReportTable rows={rows} module={module} onRowClick={setDetailRow} />

        {/* Pagination */}
        {totalPages > 1 && (
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 1, p: 1.5, borderTop: '1px solid #f3f4f6' }}>
            <Typography sx={{ fontSize: '0.78rem', color: '#6b7280' }}>
              Page {page + 1} of {totalPages} ({totalCount.toLocaleString()} total)
            </Typography>
            <Button size="small" variant="outlined" disabled={page === 0}
              onClick={() => setPage(p => p - 1)}
              sx={{ fontSize: '0.72rem', py: 0.25 }}>Prev</Button>
            <Button size="small" variant="outlined" disabled={page >= totalPages - 1}
              onClick={() => setPage(p => p + 1)}
              sx={{ fontSize: '0.72rem', py: 0.25 }}>Next</Button>
          </Box>
        )}
      </Paper>

      {/* Detail dialog */}
      <DetailDialog row={detailRow} module={module} onClose={() => setDetailRow(null)} />
    </Box>
  )
}
