import { useState, useEffect } from 'react'
import {
  Box, Typography, Tabs, Tab, TextField, Button, Alert,
  CircularProgress, InputAdornment, IconButton, Grid, Divider,
} from '@mui/material'
import VisibilityIcon from '@mui/icons-material/Visibility'
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff'
import SaveIcon from '@mui/icons-material/Save'
import WifiIcon from '@mui/icons-material/Wifi'
import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
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

const TABS = ['FTP', 'Oracle DB', 'RetailPro API', 'Scheduler', 'Sales Export']

export default function SettingsPage() {
  const qc = useQueryClient()
  const [tab, setTab] = useState(0)
  const [localValues, setLocalValues] = useState<Record<string, string>>({})
  const [saveMsg, setSaveMsg] = useState('')
  const [saveError, setSaveError] = useState('')
  const [ftpResult, setFtpResult] = useState<{ ok: boolean; error: string | null } | null>(null)
  const [oracleResult, setOracleResult] = useState<{ ok: boolean; error: string | null } | null>(null)
  const [retailproResult, setRetailproResult] = useState<{ ok: boolean; error: string | null } | null>(null)

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
    try { const r = await apiClient.post('/api/settings/test/retailpro', { base_url: g('retailpro_base_url'), api_key: g('retailpro_api_key') }); setRetailproResult(r.data) }
    catch { setRetailproResult({ ok: false, error: 'Request failed' }) }
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
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_import_path', 'Import Path')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{F('ftp_export_path', 'Export Path')}</Grid>
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
            <TestButton label="Test Oracle" onClick={testOracle} result={oracleResult} />
            <Divider sx={{ my: 2.5 }} />
            <SaveBtn cat="oracle" />
          </TabPanel>

          <TabPanel value={tab} index={2}>
            <Grid container spacing={2}>
              <Grid size={{ xs: 12 }}>{F('retailpro_base_url', 'Base URL')}</Grid>
              <Grid size={{ xs: 12, sm: 8 }}>{F('retailpro_api_key', 'API Key', true)}</Grid>
              <Grid size={{ xs: 12, sm: 4 }}>{F('retailpro_client', 'Mode (mock / real)')}</Grid>
              <Grid size={{ xs: 12 }}>{F('document_type_endpoints', 'Endpoint Map (JSON)')}</Grid>
              <Grid size={{ xs: 12 }}>{F('document_type_field_maps', 'Field Maps (JSON)')}</Grid>
            </Grid>
            <TestButton label="Test RetailPro API" onClick={testRetailPro} result={retailproResult} />
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
        </Box>
      </Box>
    </Box>
  )
}
