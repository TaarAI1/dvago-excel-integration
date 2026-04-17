import { useState, useEffect } from 'react'
import {
  Box, Typography, Tabs, Tab, TextField, Button, Alert,
  CircularProgress, InputAdornment, IconButton, Grid, Divider, Collapse,
} from '@mui/material'
import VisibilityIcon from '@mui/icons-material/Visibility'
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff'
import SaveIcon from '@mui/icons-material/Save'
import WifiIcon from '@mui/icons-material/Wifi'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import TableChartOutlinedIcon from '@mui/icons-material/TableChartOutlined'
import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
import { DataGrid } from '@mui/x-data-grid'
import type { GridColDef } from '@mui/x-data-grid'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import apiClient from '../api/client'

interface SettingMeta { value: string; label: string; is_sensitive: boolean; updated_at: string | null }
type AllSettings = Record<string, Record<string, SettingMeta>>

function TabPanel({ value, index, children }: { value: number; index: number; children: React.ReactNode }) {
  return value === index ? <Box sx={{ pt: 2.5 }}>{children}</Box> : null
}

function SettingField({ settingKey, meta, value, onChange }:
  { settingKey: string; meta: SettingMeta; value: string; onChange: (k: string, v: string) => void }) {
  const [show, setShow] = useState(false)
  const isMulti = settingKey.includes('sql') || settingKey.includes('endpoints') || settingKey.includes('field_maps')
  return (
    <TextField label={meta.label} value={value} onChange={(e) => onChange(settingKey, e.target.value)}
      fullWidth multiline={isMulti} rows={isMulti ? 5 : undefined}
      type={meta.is_sensitive && !show ? 'password' : 'text'} size="small"
      slotProps={meta.is_sensitive ? {
        input: {
          endAdornment: (
            <InputAdornment position="end">
              <IconButton size="small" onClick={() => setShow(!show)} edge="end">
                {show ? <VisibilityOffIcon sx={{ fontSize: 16 }} /> : <VisibilityIcon sx={{ fontSize: 16 }} />}
              </IconButton>
            </InputAdornment>
          ),
        },
      } : undefined}
    />
  )
}

function TestResult({ result }: { result: { ok: boolean; error: string | null } | null }) {
  if (!result) return null
  return (
    <Box sx={{
      mt: 1.5, p: 1.25, borderRadius: '6px', display: 'flex', alignItems: 'flex-start', gap: 1,
      bgcolor: result.ok ? '#f0fdf4' : '#fef2f2',
      border: '1px solid', borderColor: result.ok ? '#d1fae5' : '#fee2e2',
    }}>
      {result.ok
        ? <CheckCircleOutlinedIcon sx={{ fontSize: 15, color: '#15803d', mt: '1px', flexShrink: 0 }} />
        : <ErrorOutlinedIcon sx={{ fontSize: 15, color: '#b91c1c', mt: '1px', flexShrink: 0 }} />}
      <Typography sx={{ fontSize: '0.78rem', color: result.ok ? '#166534' : '#7f1d1d',
        wordBreak: 'break-word', lineHeight: 1.5 }}>
        {result.ok ? 'Connection successful.' : (result.error || 'Connection failed.')}
      </Typography>
    </Box>
  )
}

function TestButton({ label, onClick, result }:
  { label: string; onClick: () => Promise<void>; result: { ok: boolean; error: string | null } | null }) {
  const [loading, setLoading] = useState(false)
  const handle = async () => { setLoading(true); await onClick(); setLoading(false) }
  return (
    <Box sx={{ mt: 2 }}>
      <Button variant="outlined" size="small"
        startIcon={loading ? <CircularProgress size={12} /> : <WifiIcon sx={{ fontSize: 15 }} />}
        onClick={handle} disabled={loading} sx={{ height: 32, fontSize: '0.8rem' }}>
        {label}
      </Button>
      <TestResult result={result} />
    </Box>
  )
}

type RetailProResult = {
  ok: boolean; message?: string; session?: string; status_code?: number
  step?: number; headers?: Record<string, string>; body?: string; error?: string
} | null

function RetailProTestButton({ onClick, result }: { onClick: () => Promise<void>; result: RetailProResult }) {
  const [loading, setLoading] = useState(false)
  const handle = async () => { setLoading(true); await onClick(); setLoading(false) }

  return (
    <Box sx={{ mt: 2 }}>
      <Button variant="outlined" size="small"
        startIcon={loading ? <CircularProgress size={12} /> : <WifiIcon sx={{ fontSize: 15 }} />}
        onClick={handle} disabled={loading} sx={{ height: 32, fontSize: '0.8rem' }}>
        {loading ? 'Authenticating…' : 'Test RetailPro Connectivity'}
      </Button>

      {result && (
        <Box sx={{
          mt: 1.5, borderRadius: '6px', overflow: 'hidden',
          border: '1px solid', borderColor: result.ok ? '#d1fae5' : '#fee2e2',
        }}>
          {/* Status bar */}
          <Box sx={{
            px: 1.5, py: 1,
            bgcolor: result.ok ? '#f0fdf4' : '#fef2f2',
            display: 'flex', alignItems: 'center', gap: 1,
          }}>
            {result.ok
              ? <CheckCircleOutlinedIcon sx={{ fontSize: 15, color: '#15803d' }} />
              : <ErrorOutlinedIcon sx={{ fontSize: 15, color: '#b91c1c' }} />}
            <Typography sx={{ fontSize: '0.8rem', fontWeight: 600,
              color: result.ok ? '#166534' : '#7f1d1d' }}>
              {result.ok ? result.message : result.error}
            </Typography>
            {result.status_code && (
              <Typography sx={{ fontSize: '0.72rem', color: result.ok ? '#15803d' : '#b91c1c',
                ml: 'auto', fontFamily: 'monospace' }}>
                HTTP {result.status_code}
              </Typography>
            )}
          </Box>

          {/* Session token (success) */}
          {result.ok && result.session && (
            <Box sx={{ px: 1.5, py: 1, bgcolor: 'white', borderTop: '1px solid #d1fae5' }}>
              <Typography sx={{ fontSize: '0.7rem', color: '#6b7280', mb: 0.25 }}>Auth-Session</Typography>
              <Typography sx={{ fontSize: '0.75rem', fontFamily: 'monospace',
                color: '#166534', wordBreak: 'break-all' }}>
                {result.session}
              </Typography>
            </Box>
          )}

          {/* Failure details */}
          {!result.ok && (result.headers || result.body) && (
            <Box sx={{ px: 1.5, py: 1, bgcolor: 'white', borderTop: '1px solid #fee2e2' }}>
              {result.step && (
                <Typography sx={{ fontSize: '0.7rem', color: '#9ca3af', mb: 1 }}>
                  Failed at step {result.step}
                </Typography>
              )}
              {result.headers && Object.keys(result.headers).length > 0 && (
                <Box sx={{ mb: 1 }}>
                  <Typography sx={{ fontSize: '0.7rem', fontWeight: 600, color: '#374151', mb: 0.5 }}>
                    Response Headers
                  </Typography>
                  <Box sx={{ bgcolor: '#f9fafb', border: '1px solid #e5e7eb', borderRadius: '4px',
                    p: 1, maxHeight: 120, overflow: 'auto' }}>
                    {Object.entries(result.headers).map(([k, v]) => (
                      <Box key={k} sx={{ display: 'flex', gap: 1, fontSize: '0.72rem', fontFamily: 'monospace' }}>
                        <Typography component="span" sx={{ color: '#1a56db', fontSize: 'inherit',
                          fontFamily: 'inherit', flexShrink: 0 }}>{k}:</Typography>
                        <Typography component="span" sx={{ color: '#374151', fontSize: 'inherit',
                          fontFamily: 'inherit', wordBreak: 'break-all' }}>{v}</Typography>
                      </Box>
                    ))}
                  </Box>
                </Box>
              )}
              {result.body && (
                <Box>
                  <Typography sx={{ fontSize: '0.7rem', fontWeight: 600, color: '#374151', mb: 0.5 }}>
                    Response Body
                  </Typography>
                  <Box sx={{ bgcolor: '#f9fafb', border: '1px solid #e5e7eb', borderRadius: '4px',
                    p: 1, maxHeight: 160, overflow: 'auto' }}>
                    <Typography sx={{ fontSize: '0.72rem', fontFamily: 'monospace',
                      whiteSpace: 'pre-wrap', color: '#374151', wordBreak: 'break-word' }}>
                      {result.body}
                    </Typography>
                  </Box>
                </Box>
              )}
            </Box>
          )}
        </Box>
      )}
    </Box>
  )
}

// Inline (no top-margin) version used when paired with other buttons
function OracleTestButton({ onClick, result }:
  { onClick: () => Promise<void>; result: { ok: boolean; error: string | null } | null }) {
  const [loading, setLoading] = useState(false)
  const handle = async () => { setLoading(true); await onClick(); setLoading(false) }
  return (
    <Box>
      <Button variant="outlined" size="small"
        startIcon={loading ? <CircularProgress size={12} /> : <WifiIcon sx={{ fontSize: 15 }} />}
        onClick={handle} disabled={loading} sx={{ height: 32, fontSize: '0.8rem' }}>
        Test Connection
      </Button>
      <TestResult result={result} />
    </Box>
  )
}

const TABS = ['FTP', 'Oracle DB', 'RetailPro API', 'Scheduler', 'Sales Export', 'SMTP']

export default function SettingsPage() {
  const qc = useQueryClient()
  const [tab, setTab] = useState(0)
  const [localValues, setLocalValues] = useState<Record<string, string>>({})
  const [saveMsg, setSaveMsg] = useState('')
  const [saveError, setSaveError] = useState('')
  const [ftpResult, setFtpResult] = useState<{ ok: boolean; error: string | null } | null>(null)
  const [oracleResult, setOracleResult] = useState<{ ok: boolean; error: string | null } | null>(null)
  const [retailproResult, setRetailproResult] = useState<{
    ok: boolean
    message?: string
    session?: string
    status_code?: number
    step?: number
    headers?: Record<string, string>
    body?: string
    error?: string
  } | null>(null)

  // Oracle query panel
  const [queryOpen, setQueryOpen] = useState(false)
  const [queryText, setQueryText] = useState('')
  const [queryRunning, setQueryRunning] = useState(false)
  const [queryResult, setQueryResult] = useState<{
    ok: boolean; columns: string[]; rows: unknown[][]; row_count: number; error?: string
  } | null>(null)

  const { data: allSettings, isLoading } = useQuery<AllSettings>({
    queryKey: ['settings-raw'],
    queryFn: () => apiClient.get('/api/settings/raw').then((r) => r.data),
  })

  useEffect(() => {
    if (!allSettings) return
    const flat: Record<string, string> = {}
    for (const cat of Object.values(allSettings))
      for (const [k, m] of Object.entries(cat)) flat[k] = m.value
    setLocalValues(flat)
  }, [allSettings])

  const saveMutation = useMutation({
    mutationFn: (updates: Record<string, string>) => apiClient.put('/api/settings', { updates }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['settings-raw'] })
      setSaveMsg('Saved.'); setSaveError('')
      setTimeout(() => setSaveMsg(''), 3000)
    },
    onError: (e: any) => setSaveError(e.response?.data?.detail || 'Save failed.'),
  })

  const g = (k: string) => localValues[k] ?? ''
  const set = (k: string, v: string) => setLocalValues((p) => ({ ...p, [k]: v }))
  const save = (cat: string) => {
    const u: Record<string, string> = {}
    for (const k of Object.keys(allSettings?.[cat] ?? {})) u[k] = g(k)
    saveMutation.mutate(u)
  }

  const testFtp = async () => {
    try { const r = await apiClient.post('/api/settings/test/ftp', { host: g('ftp_host'), port: parseInt(g('ftp_port') || '21'), user: g('ftp_user'), password: g('ftp_password') }); setFtpResult(r.data) }
    catch { setFtpResult({ ok: false, error: 'Request failed' }) }
  }
  const testOracle = async () => {
    try { const r = await apiClient.post('/api/settings/test/oracle', { host: g('oracle_host'), port: parseInt(g('oracle_port') || '1521'), user: g('oracle_username'), password: g('oracle_password'), service_name: g('oracle_service_name') }); setOracleResult(r.data) }
    catch { setOracleResult({ ok: false, error: 'Request failed' }) }
  }
  const testRetailPro = async () => {
    try {
      const r = await apiClient.post('/api/settings/test/retailpro', {
        base_url: g('retailpro_base_url'),
        username: g('retailpro_username'),
        password: g('retailpro_password'),
      })
      setRetailproResult(r.data)
    } catch (e: any) {
      setRetailproResult({ ok: false, error: e.response?.data?.detail || 'Request failed' })
    }
  }

  const runOracleQuery = async () => {
    if (!queryText.trim()) return
    setQueryRunning(true)
    setQueryResult(null)
    try {
      const r = await apiClient.post('/api/settings/oracle/query', {
        host: g('oracle_host'),
        port: parseInt(g('oracle_port') || '1521'),
        user: g('oracle_username'),
        password: g('oracle_password'),
        service_name: g('oracle_service_name'),
        sql: queryText,
      })
      setQueryResult(r.data)
    } catch (e: any) {
      setQueryResult({ ok: false, columns: [], rows: [], row_count: 0, error: e.response?.data?.detail || 'Request failed' })
    } finally {
      setQueryRunning(false)
    }
  }

  const F = (key: string, label: string, sensitive = false) => (
    <SettingField settingKey={key}
      meta={{ label, value: g(key), is_sensitive: sensitive, updated_at: null }}
      value={g(key)} onChange={set} />
  )

  const SaveBtn = ({ cat }: { cat: string }) => (
    <Button variant="contained" size="small"
      startIcon={<SaveIcon sx={{ fontSize: 15 }} />}
      onClick={() => save(cat)} disabled={saveMutation.isPending}
      sx={{ height: 32, fontSize: '0.8rem' }}>
      Save
    </Button>
  )

  if (isLoading) return (
    <Box sx={{ display: 'flex', justifyContent: 'center', pt: 8 }}><CircularProgress /></Box>
  )

  return (
    <Box sx={{ p: { xs: 2, sm: 3 } }}>
      {saveMsg && <Alert severity="success" sx={{ mb: 2, py: 0.5 }} onClose={() => setSaveMsg('')}>{saveMsg}</Alert>}
      {saveError && <Alert severity="error" sx={{ mb: 2, py: 0.5 }} onClose={() => setSaveError('')}>{saveError}</Alert>}

      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
        <Tabs value={tab} onChange={(_, v) => setTab(v)} variant="scrollable"
          sx={{ borderBottom: '1px solid #f3f4f6', minHeight: 44, px: 1,
            '& .MuiTab-root': { minHeight: 44, fontSize: '0.8rem', px: 2 },
            '& .Mui-selected': { color: '#1a56db', fontWeight: 600 },
          }}>
          {TABS.map((t, i) => <Tab key={i} label={t} />)}
        </Tabs>

        <Box sx={{ p: { xs: 2, sm: 3 } }}>

          <TabPanel value={tab} index={0}>
            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 8 }}>{F('ftp_host', 'FTP Host')}</Grid>
              <Grid size={{ xs: 12, sm: 4 }}>{F('ftp_port', 'Port')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_user', 'Username')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_password', 'Password', true)}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_export_path', 'Export Path')}</Grid>
            </Grid>

            <Divider sx={{ my: 2.5 }}>
              <Typography sx={{ fontSize: '0.75rem', color: '#6b7280', px: 1 }}>Import Paths</Typography>
            </Divider>

            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_import_path', 'Item Master Import Path')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_qty_adjust_import_path', 'Quantity Adjust Import Path')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_price_adj_import_path', 'Price Adjustment Import Path')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_transfers_import_path', 'Transfers Import Path')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_grn_import_path', 'GRN Import Path')}</Grid>
            </Grid>

            <TestButton label="Test FTP" onClick={testFtp} result={ftpResult} />
            <Divider sx={{ my: 2.5 }} />
            <SaveBtn cat="ftp" />
          </TabPanel>

          <TabPanel value={tab} index={1}>
            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 8 }}>{F('oracle_host', 'Oracle Host')}</Grid>
              <Grid size={{ xs: 12, sm: 4 }}>{F('oracle_port', 'Port')}</Grid>
              <Grid size={{ xs: 12 }}>{F('oracle_service_name', 'Service Name')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('oracle_username', 'Username')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('oracle_password', 'Password', true)}</Grid>
            </Grid>

            {/* Action buttons row */}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mt: 2, flexWrap: 'wrap' }}>
              {/* Test connection */}
              <OracleTestButton onClick={testOracle} result={oracleResult} />

              {/* Run Query toggle */}
              <Button
                variant={queryOpen ? 'contained' : 'outlined'}
                size="small"
                startIcon={<TableChartOutlinedIcon sx={{ fontSize: 15 }} />}
                onClick={() => { setQueryOpen(!queryOpen); setQueryResult(null) }}
                sx={{ height: 32, fontSize: '0.8rem' }}
              >
                Run a Query
              </Button>
            </Box>

            {/* Collapsible query panel */}
            <Collapse in={queryOpen} unmountOnExit>
              <Box sx={{
                mt: 2, border: '1px solid #e5e7eb', borderRadius: '6px',
                overflow: 'hidden',
              }}>
                {/* Query editor */}
                <Box sx={{ p: 1.5, borderBottom: '1px solid #f3f4f6', bgcolor: '#fafafa',
                  display: 'flex', alignItems: 'flex-start', gap: 1.5 }}>
                  <TextField
                    fullWidth
                    multiline
                    rows={4}
                    placeholder="SELECT * FROM your_table"
                    value={queryText}
                    onChange={(e) => setQueryText(e.target.value)}
                    sx={{
                      '& .MuiOutlinedInput-root': { fontFamily: 'monospace', fontSize: '0.82rem' },
                    }}
                  />
                  <Button
                    variant="contained"
                    size="small"
                    startIcon={queryRunning
                      ? <CircularProgress size={12} color="inherit" />
                      : <PlayArrowIcon sx={{ fontSize: 15 }} />}
                    onClick={runOracleQuery}
                    disabled={queryRunning || !queryText.trim()}
                    sx={{ height: 36, fontSize: '0.8rem', flexShrink: 0, mt: 0.5 }}
                  >
                    {queryRunning ? 'Running…' : 'Run'}
                  </Button>
                </Box>

                {/* Results area */}
                {queryResult && (
                  <Box>
                    {!queryResult.ok ? (
                      <Box sx={{ p: 1.5, display: 'flex', alignItems: 'flex-start', gap: 1,
                        bgcolor: '#fef2f2' }}>
                        <ErrorOutlinedIcon sx={{ fontSize: 15, color: '#b91c1c', mt: '2px', flexShrink: 0 }} />
                        <Typography sx={{ fontSize: '0.78rem', color: '#7f1d1d',
                          wordBreak: 'break-word', fontFamily: 'monospace', lineHeight: 1.5 }}>
                          {queryResult.error}
                        </Typography>
                      </Box>
                    ) : (
                      <Box>
                        <Box sx={{ px: 1.5, py: 1, bgcolor: '#f0fdf4', borderBottom: '1px solid #d1fae5',
                          display: 'flex', alignItems: 'center', gap: 1 }}>
                          <CheckCircleOutlinedIcon sx={{ fontSize: 14, color: '#15803d' }} />
                          <Typography sx={{ fontSize: '0.75rem', color: '#15803d', fontWeight: 500 }}>
                            {queryResult.row_count} row{queryResult.row_count !== 1 ? 's' : ''} returned
                            {' (limited to 10)'}
                          </Typography>
                        </Box>
                        <DataGrid
                          rows={queryResult.rows.map((row, i) => ({
                            id: i,
                            ...Object.fromEntries(queryResult.columns.map((col, ci) => [col, row[ci]])),
                          }))}
                          columns={queryResult.columns.map((col): GridColDef => ({
                            field: col,
                            headerName: col,
                            flex: 1,
                            minWidth: 120,
                            sortable: true,
                          }))}
                          autoHeight
                          disableRowSelectionOnClick
                          pageSizeOptions={[25, 50, 100]}
                          initialState={{ pagination: { paginationModel: { pageSize: 25 } } }}
                          sx={{
                            border: 'none',
                            '& .MuiDataGrid-columnHeaders': { bgcolor: '#f9fafb', borderBottom: '1px solid #e5e7eb' },
                            '& .MuiDataGrid-cell': { fontSize: '0.8rem', borderColor: '#f3f4f6' },
                            '& .MuiDataGrid-footerContainer': { borderTop: '1px solid #e5e7eb', bgcolor: '#fafafa' },
                          }}
                        />
                      </Box>
                    )}
                  </Box>
                )}

                {/* Empty state before first run */}
                {!queryResult && !queryRunning && (
                  <Box sx={{ p: 3, textAlign: 'center' }}>
                    <Typography sx={{ fontSize: '0.8rem', color: '#9ca3af' }}>
                      Write a SELECT query above and click Run to see results.
                    </Typography>
                  </Box>
                )}
              </Box>
            </Collapse>

            <Divider sx={{ my: 2.5 }} />
            <SaveBtn cat="oracle" />
          </TabPanel>

          <TabPanel value={tab} index={2}>
            <Grid container spacing={2}>
              <Grid size={{ xs: 12 }}>{F('retailpro_base_url', 'Server URL  (e.g. http://192.168.1.100)')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('retailpro_username', 'Username')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('retailpro_password', 'Password', true)}</Grid>
            </Grid>

            {/* Test button */}
            <RetailProTestButton onClick={testRetailPro} result={retailproResult} />

            <Divider sx={{ my: 2.5 }} />
            <SaveBtn cat="retailpro" />
          </TabPanel>

          <TabPanel value={tab} index={3}>
            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 6 }}>{F('poll_cron_schedule', 'FTP Cron (e.g. */15 * * * *)')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('sales_export_cron', 'Sales Export Cron (e.g. 0 2 * * *)')}</Grid>
            </Grid>
            <Typography sx={{ fontSize: '0.75rem', color: '#9ca3af', mt: 1.5 }}>
              Format: minute · hour · day · month · weekday
            </Typography>
            <Divider sx={{ my: 2.5 }} />
            <SaveBtn cat="scheduler" />
          </TabPanel>

          <TabPanel value={tab} index={4}>
            <Grid container spacing={2}>
              <Grid size={{ xs: 12 }}>{F('sales_export_sql', 'SQL Query')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('sales_export_filename_prefix', 'Filename Prefix')}</Grid>
            </Grid>
            <Typography sx={{ fontSize: '0.75rem', color: '#9ca3af', mt: 1.5 }}>
              Output: <code style={{ background: '#f3f4f6', padding: '2px 6px', borderRadius: 3 }}>
                {'<prefix>_YYYYMMDD_HHMMSS.csv'}
              </code>
            </Typography>
            <Divider sx={{ my: 2.5 }} />
            <SaveBtn cat="sales_export" />
          </TabPanel>

          {/* ── SMTP ── */}
          <TabPanel value={tab} index={5}>
            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 8 }}>{F('smtp_host', 'SMTP Host')}</Grid>
              <Grid size={{ xs: 12, sm: 4 }}>{F('smtp_port', 'Port')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('smtp_username', 'Username')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('smtp_password', 'Password', true)}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>
                <TextField
                  select
                  label="Use TLS"
                  value={g('smtp_use_tls')}
                  onChange={(e) => set('smtp_use_tls', e.target.value)}
                  fullWidth
                  size="small"
                  slotProps={{ select: { native: true } }}
                >
                  <option value="true">Enabled</option>
                  <option value="false">Disabled</option>
                </TextField>
              </Grid>
            </Grid>

            <Divider sx={{ my: 2.5 }}>
              <Typography sx={{ fontSize: '0.75rem', color: '#6b7280', px: 1 }}>Email Addresses</Typography>
            </Divider>

            <Grid container spacing={2}>
              <Grid size={{ xs: 12, sm: 6 }}>{F('smtp_from_email', 'From Email')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('smtp_to_email', 'To Email')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('smtp_reply_to', 'Reply To')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('smtp_cc_email', 'CC Email')}</Grid>
            </Grid>

            <Divider sx={{ my: 2.5 }} />
            <SaveBtn cat="smtp" />
          </TabPanel>
        </Box>
      </Box>
    </Box>
  )
}
