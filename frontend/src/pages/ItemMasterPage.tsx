import { useRef, useState } from 'react'
import {
  Box, Typography, Button, CircularProgress, Alert,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  Chip, Divider, LinearProgress,
} from '@mui/material'
import UploadFileOutlinedIcon from '@mui/icons-material/UploadFileOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
import AddCircleOutlineIcon from '@mui/icons-material/AddCircleOutline'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import apiClient from '../api/client'

// ── Types ────────────────────────────────────────────────────────────────────

interface PreviewResponse {
  total_rows: number
  preview_rows: Record<string, string | null>[]
  columns: string[]
}

interface RowResult {
  upc: string
  description: string
  action: 'created' | 'updated' | null
  sid: string | null
  ok: boolean
  error: string | null
}

interface ImportResponse {
  ok: boolean
  total: number
  created: number
  updated: number
  errors: number
  results: RowResult[]
  error?: string
}

type Step = 'idle' | 'previewing' | 'preview_done' | 'importing' | 'done'

// ── Summary stat box ─────────────────────────────────────────────────────────

function StatBox({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <Box sx={{
      flex: '1 1 100px', textAlign: 'center',
      bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', p: '12px 16px',
    }}>
      <Typography sx={{ fontSize: '1.5rem', fontWeight: 700, color, lineHeight: 1 }}>
        {value}
      </Typography>
      <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af', mt: 0.5, textTransform: 'uppercase',
        letterSpacing: '0.05em' }}>
        {label}
      </Typography>
    </Box>
  )
}

// ── Status chip ──────────────────────────────────────────────────────────────

function StatusChip({ row }: { row: RowResult }) {
  if (!row.ok) {
    return (
      <Chip label="Error" size="small" sx={{
        height: 20, fontSize: '0.7rem', borderRadius: '4px',
        bgcolor: '#fef2f2', color: '#b91c1c', border: '1px solid #fecaca',
      }} />
    )
  }
  if (row.action === 'created') {
    return (
      <Chip
        icon={<AddCircleOutlineIcon sx={{ fontSize: '11px !important' }} />}
        label="Created" size="small"
        sx={{ height: 20, fontSize: '0.7rem', borderRadius: '4px',
          bgcolor: '#f0fdf4', color: '#15803d', border: '1px solid #d1fae5' }}
      />
    )
  }
  return (
    <Chip
      icon={<EditOutlinedIcon sx={{ fontSize: '11px !important' }} />}
      label="Updated" size="small"
      sx={{ height: 20, fontSize: '0.7rem', borderRadius: '4px',
        bgcolor: '#eff6ff', color: '#1a56db', border: '1px solid #bfdbfe' }}
    />
  )
}

// ── Page ─────────────────────────────────────────────────────────────────────

export default function ItemMasterPage() {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [step, setStep] = useState<Step>('idle')
  const [file, setFile] = useState<File | null>(null)
  const [preview, setPreview] = useState<PreviewResponse | null>(null)
  const [importResult, setImportResult] = useState<ImportResponse | null>(null)
  const [error, setError] = useState('')

  // Visible columns in preview (first 8)
  const previewCols = preview?.columns.slice(0, 8) ?? []

  const handleFileChange = async (f: File) => {
    setFile(f)
    setError('')
    setPreview(null)
    setImportResult(null)
    setStep('previewing')

    const form = new FormData()
    form.append('file', f)
    try {
      const res = await apiClient.post('/api/item-master/preview', form, {
        headers: { 'Content-Type': 'multipart/form-data' },
      })
      setPreview(res.data)
      setStep('preview_done')
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Failed to parse Excel file.')
      setStep('idle')
    }
  }

  const handleImport = async () => {
    if (!file) return
    setStep('importing')
    setError('')
    const form = new FormData()
    form.append('file', file)
    try {
      const res = await apiClient.post('/api/item-master/import', form, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 300_000,   // 5 min for large batches
      })
      setImportResult(res.data)
      setStep('done')
    } catch (e: any) {
      setError(e.response?.data?.detail || 'Import failed.')
      setStep('preview_done')
    }
  }

  const handleReset = () => {
    setStep('idle')
    setFile(null)
    setPreview(null)
    setImportResult(null)
    setError('')
    if (fileInputRef.current) fileInputRef.current.value = ''
  }

  const isDragging = useRef(false)

  return (
    <Box sx={{ p: { xs: 2, sm: 3 }, display: 'flex', flexDirection: 'column', gap: 2 }}>

      {/* Page header */}
      <Box>
        <Typography sx={{ fontWeight: 600, fontSize: '0.875rem', color: '#111827' }}>
          Item Master Import
        </Typography>
        <Typography sx={{ fontSize: '0.75rem', color: '#9ca3af' }}>
          Upload an Excel file to check/create DCS, Vendor, and upsert inventory items in RetailPro.
        </Typography>
      </Box>

      {error && (
        <Alert severity="error" sx={{ py: 0.5 }} onClose={() => setError('')}>
          {error}
        </Alert>
      )}

      {/* Upload drop zone */}
      {step === 'idle' && (
        <Box
          onClick={() => fileInputRef.current?.click()}
          onDragOver={(e) => { e.preventDefault(); isDragging.current = true }}
          onDrop={(e) => {
            e.preventDefault()
            const f = e.dataTransfer.files[0]
            if (f) handleFileChange(f)
          }}
          sx={{
            bgcolor: 'white', border: '2px dashed #d1d5db', borderRadius: '8px',
            p: 5, textAlign: 'center', cursor: 'pointer',
            '&:hover': { borderColor: '#1a56db', bgcolor: '#f9fafb' },
            transition: 'all 0.15s',
          }}
        >
          <UploadFileOutlinedIcon sx={{ fontSize: 36, color: '#9ca3af', mb: 1 }} />
          <Typography sx={{ fontWeight: 500, fontSize: '0.875rem', color: '#374151' }}>
            Click to select or drag & drop your Excel file
          </Typography>
          <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af', mt: 0.5 }}>
            .xlsx — header row auto-detected (rows 1–5 scanned)
          </Typography>
          <input
            ref={fileInputRef} type="file" accept=".xlsx,.xls" hidden
            onChange={(e) => { const f = e.target.files?.[0]; if (f) handleFileChange(f) }}
          />
        </Box>
      )}

      {/* Parsing spinner */}
      {step === 'previewing' && (
        <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px',
          p: 3, display: 'flex', alignItems: 'center', gap: 2 }}>
          <CircularProgress size={18} />
          <Typography sx={{ fontSize: '0.875rem', color: '#374151' }}>
            Parsing <strong>{file?.name}</strong>…
          </Typography>
        </Box>
      )}

      {/* Preview */}
      {(step === 'preview_done' || step === 'importing' || step === 'done') && preview && (
        <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
          <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid #f3f4f6',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 1 }}>
            <Box>
              <Typography sx={{ fontWeight: 600, fontSize: '0.8rem', color: '#374151' }}>
                Preview — {file?.name}
              </Typography>
              <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
                {preview.total_rows} rows · showing first 50 · {preview.columns.length} columns detected
              </Typography>
            </Box>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button variant="outlined" size="small" onClick={handleReset}
                sx={{ height: 30, fontSize: '0.78rem' }}>
                Change File
              </Button>
              {step === 'preview_done' && (
                <Button
                  variant="contained" size="small"
                  startIcon={<PlayArrowIcon sx={{ fontSize: 15 }} />}
                  onClick={handleImport}
                  sx={{ height: 30, fontSize: '0.78rem' }}
                >
                  Import {preview.total_rows} Rows
                </Button>
              )}
              {step === 'importing' && (
                <Button variant="contained" size="small" disabled
                  startIcon={<CircularProgress size={12} color="inherit" />}
                  sx={{ height: 30, fontSize: '0.78rem' }}>
                  Importing…
                </Button>
              )}
            </Box>
          </Box>

          {step === 'importing' && <LinearProgress sx={{ height: 2 }} />}

          <TableContainer sx={{ maxHeight: 280 }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  {previewCols.map((col) => (
                    <TableCell key={col} sx={{ fontSize: '0.7rem', fontWeight: 600, whiteSpace: 'nowrap',
                      bgcolor: '#f9fafb', color: '#6b7280', textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                      {col}
                    </TableCell>
                  ))}
                  {preview.columns.length > 8 && (
                    <TableCell sx={{ fontSize: '0.7rem', color: '#9ca3af', bgcolor: '#f9fafb' }}>
                      +{preview.columns.length - 8} more
                    </TableCell>
                  )}
                </TableRow>
              </TableHead>
              <TableBody>
                {preview.preview_rows.map((row, i) => (
                  <TableRow key={i} sx={{ '&:hover': { bgcolor: '#fafafa' } }}>
                    {previewCols.map((col) => (
                      <TableCell key={col} sx={{ fontSize: '0.78rem', maxWidth: 180,
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {row[col] ?? <span style={{ color: '#d1d5db' }}>—</span>}
                      </TableCell>
                    ))}
                    {preview.columns.length > 8 && <TableCell />}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      )}

      {/* Import results */}
      {step === 'done' && importResult && (
        <>
          <Divider />

          {/* Summary stats */}
          <Box sx={{ display: 'flex', gap: 1.5, flexWrap: 'wrap' }}>
            <StatBox label="Total" value={importResult.total} color="#111827" />
            <StatBox label="Created" value={importResult.created} color="#15803d" />
            <StatBox label="Updated" value={importResult.updated} color="#1a56db" />
            <StatBox label="Errors" value={importResult.errors} color="#b91c1c" />
          </Box>

          {/* Per-row results table */}
          <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
            <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid #f3f4f6',
              display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <Typography sx={{ fontWeight: 600, fontSize: '0.8rem', color: '#374151',
                textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                Import Results
              </Typography>
              <Button variant="outlined" size="small" onClick={handleReset}
                sx={{ height: 28, fontSize: '0.78rem' }}>
                New Import
              </Button>
            </Box>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>#</TableCell>
                    <TableCell>UPC</TableCell>
                    <TableCell>Description</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>RetailPro SID</TableCell>
                    <TableCell>Error</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {importResult.results.map((row, i) => (
                    <TableRow key={i}>
                      <TableCell sx={{ color: '#9ca3af', fontSize: '0.72rem' }}>{i + 1}</TableCell>
                      <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>{row.upc}</TableCell>
                      <TableCell sx={{ maxWidth: 220, overflow: 'hidden',
                        textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: '0.78rem' }}>
                        {row.description || '—'}
                      </TableCell>
                      <TableCell><StatusChip row={row} /></TableCell>
                      <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#6b7280' }}>
                        {row.sid || '—'}
                      </TableCell>
                      <TableCell sx={{ maxWidth: 320 }}>
                        {row.error ? (
                          <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 0.5 }}>
                            <ErrorOutlinedIcon sx={{ fontSize: 13, color: '#b91c1c', mt: '2px', flexShrink: 0 }} />
                            <Typography sx={{ fontSize: '0.72rem', color: '#7f1d1d',
                              wordBreak: 'break-word', lineHeight: 1.4 }}>
                              {row.error}
                            </Typography>
                          </Box>
                        ) : (
                          <CheckCircleOutlinedIcon sx={{ fontSize: 14, color: '#d1fae5' }} />
                        )}
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
  )
}
