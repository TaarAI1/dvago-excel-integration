import { useState } from 'react'
import {
  Box, Typography, Button, TextField, Select, MenuItem,
  FormControl, InputLabel, Chip, Dialog, DialogTitle,
  DialogContent, DialogActions, IconButton, Tooltip,
  Alert, CircularProgress, Stack,
} from '@mui/material'
import { DataGrid } from '@mui/x-data-grid'
import type { GridColDef, GridRenderCellParams } from '@mui/x-data-grid'
import RefreshIcon from '@mui/icons-material/Refresh'
import ReplayIcon from '@mui/icons-material/Replay'
import InfoIcon from '@mui/icons-material/Info'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import apiClient from '../../api/client'
import { useUIStore } from '../../stores/uiStore'

interface Document {
  id: string
  document_type: string
  posted: boolean
  has_error: boolean
  retailprosid?: string
  error_message?: string
  created_at: string
  posted_at?: string
  source_file?: string
  original_data?: Record<string, unknown>
}

interface DocsResponse {
  total: number
  items: Document[]
}

function StatusChip({ doc }: { doc: Document }) {
  if (doc.posted) return <Chip label="Posted" color="success" size="small" />
  if (doc.has_error) return <Chip label="Error" color="error" size="small" />
  return <Chip label="Pending" color="warning" size="small" />
}

export default function DocumentTable() {
  const qc = useQueryClient()
  const { filters, setFilters, resetFilters } = useUIStore()
  const [page, setPage] = useState(0)
  const [pageSize, setPageSize] = useState(25)
  const [selectedDoc, setSelectedDoc] = useState<Document | null>(null)

  const params: Record<string, string> = { limit: String(pageSize), offset: String(page * pageSize) }
  if (filters.documentType) params.document_type = filters.documentType
  if (filters.status) params.status = filters.status
  if (filters.dateFrom) params.date_from = filters.dateFrom
  if (filters.dateTo) params.date_to = filters.dateTo

  const { data, isLoading, isFetching } = useQuery<DocsResponse>({
    queryKey: ['documents', params],
    queryFn: () => apiClient.get('/api/documents', { params }).then((r) => r.data),
    refetchInterval: 10000,
  })

  const retryMutation = useMutation({
    mutationFn: (id: string) => apiClient.post(`/api/process/retry/${id}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['documents'] }),
  })

  const batchRetryMutation = useMutation({
    mutationFn: () => apiClient.post('/api/process/batch-retry'),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['documents'] }),
  })

  const columns: GridColDef[] = [
    { field: 'id', headerName: 'Document ID', width: 210, sortable: false },
    { field: 'document_type', headerName: 'Type', width: 160 },
    {
      field: 'status',
      headerName: 'Status',
      width: 110,
      renderCell: (params: GridRenderCellParams) => <StatusChip doc={params.row} />,
      sortable: false,
    },
    { field: 'retailprosid', headerName: 'RetailPro SID', width: 160 },
    { field: 'source_file', headerName: 'Source File', width: 180, sortable: false },
    {
      field: 'created_at',
      headerName: 'Created',
      width: 175,
      valueFormatter: (value: string) => value ? new Date(value).toLocaleString() : '',
    },
    {
      field: 'posted_at',
      headerName: 'Posted At',
      width: 175,
      valueFormatter: (value: string) => value ? new Date(value).toLocaleString() : '—',
    },
    {
      field: 'actions',
      headerName: 'Actions',
      width: 100,
      sortable: false,
      renderCell: (params: GridRenderCellParams) => (
        <Box sx={{ display: 'flex', gap: 0.5 }}>
          <Tooltip title="View details">
            <IconButton size="small" onClick={() => setSelectedDoc(params.row)}>
              <InfoIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          {params.row.has_error && !params.row.posted && (
            <Tooltip title="Retry">
              <IconButton
                size="small"
                onClick={() => retryMutation.mutate(params.row.id)}
                disabled={retryMutation.isPending}
              >
                <ReplayIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          )}
        </Box>
      ),
    },
  ]

  const rows = data?.items ?? []
  const errorCount = rows.filter((d) => d.has_error && !d.posted).length

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2, flexWrap: 'wrap' }}>
        <Typography variant="h6" sx={{ fontWeight: 600 }}>Documents</Typography>
        <Typography variant="body2" color="text.secondary">
          {data?.total ?? 0} total
        </Typography>
        {isFetching && <CircularProgress size={16} />}

        <Box sx={{ flexGrow: 1 }} />

        {/* Filters */}
        <FormControl size="small" sx={{ minWidth: 140 }}>
          <InputLabel>Type</InputLabel>
          <Select value={filters.documentType} label="Type" onChange={(e) => setFilters({ documentType: e.target.value })}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="item_master">Item Master</MenuItem>
            <MenuItem value="receiving_voucher">Receiving Voucher</MenuItem>
            <MenuItem value="inventory_adjustment">Inventory Adjustment</MenuItem>
          </Select>
        </FormControl>

        <FormControl size="small" sx={{ minWidth: 120 }}>
          <InputLabel>Status</InputLabel>
          <Select value={filters.status} label="Status" onChange={(e) => setFilters({ status: e.target.value })}>
            <MenuItem value="">All</MenuItem>
            <MenuItem value="posted">Posted</MenuItem>
            <MenuItem value="error">Error</MenuItem>
            <MenuItem value="pending">Pending</MenuItem>
          </Select>
        </FormControl>

        <TextField
          size="small"
          label="From"
          type="date"
          slotProps={{ inputLabel: { shrink: true } }}
          value={filters.dateFrom}
          onChange={(e) => setFilters({ dateFrom: e.target.value })}
          sx={{ width: 155 }}
        />
        <TextField
          size="small"
          label="To"
          type="date"
          slotProps={{ inputLabel: { shrink: true } }}
          value={filters.dateTo}
          onChange={(e) => setFilters({ dateTo: e.target.value })}
          sx={{ width: 155 }}
        />

        <Button size="small" onClick={resetFilters} variant="outlined">Reset</Button>

        {errorCount > 0 && (
          <Button
            size="small"
            variant="contained"
            color="error"
            startIcon={batchRetryMutation.isPending ? <CircularProgress size={14} color="inherit" /> : <ReplayIcon />}
            onClick={() => batchRetryMutation.mutate()}
            disabled={batchRetryMutation.isPending}
          >
            Retry All ({errorCount})
          </Button>
        )}

        <Tooltip title="Refresh">
          <IconButton size="small" onClick={() => qc.invalidateQueries({ queryKey: ['documents'] })}>
            <RefreshIcon />
          </IconButton>
        </Tooltip>
      </Box>

      <DataGrid
        rows={rows}
        columns={columns}
        rowCount={data?.total ?? 0}
        loading={isLoading}
        paginationMode="server"
        paginationModel={{ page, pageSize }}
        onPaginationModelChange={(m) => { setPage(m.page); setPageSize(m.pageSize) }}
        pageSizeOptions={[10, 25, 50, 100]}
        autoHeight
        disableRowSelectionOnClick
        sx={{ bgcolor: 'background.paper' }}
      />

      {/* Document detail dialog */}
      <Dialog open={!!selectedDoc} onClose={() => setSelectedDoc(null)} maxWidth="md" fullWidth>
        <DialogTitle>
          Document Details
          <Typography variant="caption" color="text.secondary" sx={{ ml: 1 }}>
            {selectedDoc?.id}
          </Typography>
        </DialogTitle>
        <DialogContent dividers>
          {selectedDoc && (
            <Stack spacing={1.5}>
              <Box sx={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                <Box><Typography variant="caption" color="text.secondary">Type</Typography><Typography>{selectedDoc.document_type}</Typography></Box>
                <Box><Typography variant="caption" color="text.secondary">Status</Typography><Box><StatusChip doc={selectedDoc} /></Box></Box>
                <Box><Typography variant="caption" color="text.secondary">RetailPro SID</Typography><Typography>{selectedDoc.retailprosid || '—'}</Typography></Box>
                <Box><Typography variant="caption" color="text.secondary">Source File</Typography><Typography>{selectedDoc.source_file || '—'}</Typography></Box>
                <Box><Typography variant="caption" color="text.secondary">Created</Typography><Typography>{selectedDoc.created_at ? new Date(selectedDoc.created_at).toLocaleString() : '—'}</Typography></Box>
                <Box><Typography variant="caption" color="text.secondary">Posted At</Typography><Typography>{selectedDoc.posted_at ? new Date(selectedDoc.posted_at).toLocaleString() : '—'}</Typography></Box>
              </Box>

              {selectedDoc.has_error && selectedDoc.error_message && (
                <Alert severity="error" sx={{ mt: 1 }}>
                  <Typography variant="subtitle2">API Error Response:</Typography>
                  <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: 12 }}>
                    {selectedDoc.error_message}
                  </Typography>
                </Alert>
              )}

              <Box>
                <Typography variant="caption" color="text.secondary">Original Data (CSV row)</Typography>
                <Box sx={{ bgcolor: 'grey.100', p: 1.5, borderRadius: 1, mt: 0.5, maxHeight: 300, overflow: 'auto' }}>
                  <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: 12, whiteSpace: 'pre-wrap' }}>
                    {JSON.stringify(selectedDoc.original_data, null, 2)}
                  </Typography>
                </Box>
              </Box>
            </Stack>
          )}
        </DialogContent>
        <DialogActions>
          {selectedDoc?.has_error && !selectedDoc.posted && (
            <Button
              onClick={() => { retryMutation.mutate(selectedDoc!.id); setSelectedDoc(null) }}
              color="warning"
              variant="contained"
              startIcon={<ReplayIcon />}
            >
              Retry
            </Button>
          )}
          <Button onClick={() => setSelectedDoc(null)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
