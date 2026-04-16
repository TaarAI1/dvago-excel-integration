import { useState, useEffect } from 'react'
import {
  Box, Typography, Tabs, Tab, TextField, Button, Alert,
  CircularProgress, InputAdornment, IconButton, Grid,
  Divider,
} from '@mui/material'
import VisibilityIcon from '@mui/icons-material/Visibility'
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff'
import WifiIcon from '@mui/icons-material/Wifi'
import SaveIcon from '@mui/icons-material/Save'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ErrorIcon from '@mui/icons-material/Error'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import apiClient from '../api/client'

interface SettingMeta {
  value: string
  label: string
  is_sensitive: boolean
  updated_at: string | null
}

type AllSettings = Record<string, Record<string, SettingMeta>>

function TabPanel({ value, index, children }: { value: number; index: number; children: React.ReactNode }) {
  if (value !== index) return null
  return <Box sx={{ pt: 3 }}>{children}</Box>
}

function SettingField({
  settingKey, meta, value, onChange,
}: {
  settingKey: string
  meta: SettingMeta
  value: string
  onChange: (key: string, val: string) => void
}) {
  const [show, setShow] = useState(false)
  const isMultiline = settingKey.includes('sql') || settingKey.includes('endpoints') || settingKey.includes('field_maps')

  return (
    <TextField
      label={meta.label}
      value={value}
      onChange={(e) => onChange(settingKey, e.target.value)}
      fullWidth
      multiline={isMultiline}
      rows={isMultiline ? 5 : undefined}
      type={meta.is_sensitive && !show ? 'password' : 'text'}
      size="small"
      slotProps={meta.is_sensitive ? {
        input: {
          endAdornment: (
            <InputAdornment position="end">
              <IconButton size="small" onClick={() => setShow(!show)} edge="end">
                {show ? <VisibilityOffIcon fontSize="small" /> : <VisibilityIcon fontSize="small" />}
              </IconButton>
            </InputAdornment>
          ),
        },
      } : undefined}
    />
  )
}

function TestButton({
  label, onClick, result,
}: {
  label: string
  onClick: () => Promise<void>
  result: { ok: boolean; error: string | null } | null
}) {
  const [loading, setLoading] = useState(false)

  const handle = async () => {
    setLoading(true)
    await onClick()
    setLoading(false)
  }

  return (
    <Box sx={{ mt: 2 }}>
      <Button
        variant="outlined"
        size="small"
        startIcon={loading ? <CircularProgress size={13} /> : <WifiIcon fontSize="small" />}
        onClick={handle}
        disabled={loading}
        sx={{ height: 34 }}
      >
        {label}
      </Button>

      {result !== null && (
        <Box
          sx={{
            mt: 1.5,
            display: 'flex',
            alignItems: 'flex-start',
            gap: 1,
            p: 1.5,
            borderRadius: '8px',
            bgcolor: result.ok ? '#f0fdf4' : '#fef2f2',
            border: '1px solid',
            borderColor: result.ok ? '#bbf7d0' : '#fecaca',
          }}
        >
          {result.ok
            ? <CheckCircleIcon sx={{ fontSize: 16, color: '#059669', mt: '1px', flexShrink: 0 }} />
            : <ErrorIcon sx={{ fontSize: 16, color: '#dc2626', mt: '1px', flexShrink: 0 }} />
          }
          <Typography
            sx={{
              fontSize: '0.8rem',
              color: result.ok ? '#065f46' : '#7f1d1d',
              wordBreak: 'break-word',
              lineHeight: 1.5,
            }}
          >
            {result.ok ? 'Connection successful.' : (result.error || 'Connection failed.')}
          </Typography>
        </Box>
      )}
    </Box>
  )
}

const TAB_LABELS = ['FTP', 'Oracle DB', 'RetailPro API', 'Scheduler', 'Sales Export']

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
    for (const cat of Object.values(allSettings)) {
      for (const [k, meta] of Object.entries(cat)) {
        flat[k] = meta.value
      }
    }
    setLocalValues(flat)
  }, [allSettings])

  const saveMutation = useMutation({
    mutationFn: (updates: Record<string, string>) =>
      apiClient.put('/api/settings', { updates }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['settings-raw'] })
      setSaveMsg('Settings saved successfully.')
      setSaveError('')
      setTimeout(() => setSaveMsg(''), 3000)
    },
    onError: (e: any) => setSaveError(e.response?.data?.detail || 'Failed to save settings.'),
  })

  const handleChange = (key: string, val: string) => setLocalValues((p) => ({ ...p, [key]: val }))
  const getVal = (key: string) => localValues[key] ?? ''

  const handleSave = (category: string) => {
    const updates: Record<string, string> = {}
    for (const k of Object.keys(allSettings?.[category] ?? {})) updates[k] = getVal(k)
    saveMutation.mutate(updates)
  }

  const testFtp = async () => {
    try {
      const res = await apiClient.post('/api/settings/test/ftp', {
        host: getVal('ftp_host'), port: parseInt(getVal('ftp_port') || '21'),
        user: getVal('ftp_user'), password: getVal('ftp_password'),
      })
      setFtpResult(res.data)
    } catch { setFtpResult({ ok: false, error: 'Request failed' }) }
  }

  const testOracle = async () => {
    try {
      const res = await apiClient.post('/api/settings/test/oracle', {
        host: getVal('oracle_host'), port: parseInt(getVal('oracle_port') || '1521'),
        user: getVal('oracle_username'), password: getVal('oracle_password'),
        service_name: getVal('oracle_service_name'),
      })
      setOracleResult(res.data)
    } catch { setOracleResult({ ok: false, error: 'Request failed' }) }
  }

  const testRetailPro = async () => {
    try {
      const res = await apiClient.post('/api/settings/test/retailpro', {
        base_url: getVal('retailpro_base_url'), api_key: getVal('retailpro_api_key'),
      })
      setRetailproResult(res.data)
    } catch { setRetailproResult({ ok: false, error: 'Request failed' }) }
  }

  const field = (key: string, label: string, sensitive = false) => (
    <SettingField
      settingKey={key}
      meta={{ label, value: getVal(key), is_sensitive: sensitive, updated_at: null }}
      value={getVal(key)}
      onChange={handleChange}
    />
  )

  if (isLoading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', pt: 8 }}>
        <CircularProgress />
      </Box>
    )
  }

  return (
    <Box sx={{ px: 2 }}>
      {saveMsg && <Alert severity="success" icon={<CheckCircleIcon fontSize="small" />} sx={{ mb: 2 }}>{saveMsg}</Alert>}
      {saveError && <Alert severity="error" sx={{ mb: 2 }}>{saveError}</Alert>}

      <Box
        sx={{
          bgcolor: 'white',
          border: '1px solid #e2e8f0',
          borderRadius: '12px',
          overflow: 'hidden',
        }}
      >
        <Tabs
          value={tab}
          onChange={(_, v) => setTab(v)}
          sx={{
            borderBottom: '1px solid #f1f5f9',
            px: 2,
            minHeight: 48,
            '& .MuiTab-root': { minHeight: 48, fontSize: '0.82rem' },
            '& .Mui-selected': { color: 'primary.main', fontWeight: 700 },
          }}
          variant="scrollable"
        >
          {TAB_LABELS.map((label, i) => <Tab key={i} label={label} />)}
        </Tabs>

        <Box sx={{ p: 3 }}>
          {/* FTP */}
          <TabPanel value={tab} index={0}>
            <Grid container spacing={2.5}>
              <Grid size={{ xs: 12, sm: 8 }}>{field('ftp_host', 'FTP Host')}</Grid>
              <Grid size={{ xs: 12, sm: 4 }}>{field('ftp_port', 'FTP Port')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{field('ftp_user', 'FTP Username')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{field('ftp_password', 'FTP Password', true)}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{field('ftp_import_path', 'Import Path')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{field('ftp_export_path', 'Export Path')}</Grid>
            </Grid>
            <TestButton label="Test FTP Connection" onClick={testFtp} result={ftpResult} />
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon fontSize="small" />}
              onClick={() => handleSave('ftp')} disabled={saveMutation.isPending}>
              Save FTP Settings
            </Button>
          </TabPanel>

          {/* Oracle */}
          <TabPanel value={tab} index={1}>
            <Grid container spacing={2.5}>
              <Grid size={{ xs: 12, sm: 8 }}>{field('oracle_host', 'Oracle Host')}</Grid>
              <Grid size={{ xs: 12, sm: 4 }}>{field('oracle_port', 'Oracle Port')}</Grid>
              <Grid size={{ xs: 12 }}>{field('oracle_service_name', 'Service Name')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{field('oracle_username', 'Oracle Username')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{field('oracle_password', 'Oracle Password', true)}</Grid>
            </Grid>
            <TestButton label="Test Oracle Connection" onClick={testOracle} result={oracleResult} />
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon fontSize="small" />}
              onClick={() => handleSave('oracle')} disabled={saveMutation.isPending}>
              Save Oracle Settings
            </Button>
          </TabPanel>

          {/* RetailPro */}
          <TabPanel value={tab} index={2}>
            <Grid container spacing={2.5}>
              <Grid size={{ xs: 12 }}>{field('retailpro_base_url', 'RetailPro Base URL')}</Grid>
              <Grid size={{ xs: 12, sm: 8 }}>{field('retailpro_api_key', 'API Key', true)}</Grid>
              <Grid size={{ xs: 12, sm: 4 }}>{field('retailpro_client', 'Mode (mock / real)')}</Grid>
              <Grid size={{ xs: 12 }}>{field('document_type_endpoints', 'Document Type → Endpoint Map (JSON)')}</Grid>
              <Grid size={{ xs: 12 }}>{field('document_type_field_maps', 'Document Field Maps (JSON)')}</Grid>
            </Grid>
            <TestButton label="Test RetailPro API" onClick={testRetailPro} result={retailproResult} />
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon fontSize="small" />}
              onClick={() => handleSave('retailpro')} disabled={saveMutation.isPending}>
              Save RetailPro Settings
            </Button>
          </TabPanel>

          {/* Scheduler */}
          <TabPanel value={tab} index={3}>
            <Grid container spacing={2.5}>
              <Grid size={{ xs: 12, sm: 6 }}>{field('poll_cron_schedule', 'FTP Import Cron (e.g. */15 * * * *)')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{field('sales_export_cron', 'Sales Export Cron (e.g. 0 2 * * *)')}</Grid>
            </Grid>
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1.5 }}>
              Standard cron: minute · hour · day · month · weekday — changes take effect immediately.
            </Typography>
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon fontSize="small" />}
              onClick={() => handleSave('scheduler')} disabled={saveMutation.isPending}>
              Save Scheduler Settings
            </Button>
          </TabPanel>

          {/* Sales Export */}
          <TabPanel value={tab} index={4}>
            <Grid container spacing={2.5}>
              <Grid size={{ xs: 12 }}>{field('sales_export_sql', 'Sales SQL Query')}</Grid>
              <Grid size={{ xs: 12, sm: 6 }}>{field('sales_export_filename_prefix', 'Output Filename Prefix')}</Grid>
            </Grid>
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1.5 }}>
              Output filename: <Box component="code" sx={{ bgcolor: '#f1f5f9', px: 0.75, py: 0.25, borderRadius: 1 }}>
                {'<prefix>_YYYYMMDD_HHMMSS.csv'}
              </Box>
            </Typography>
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon fontSize="small" />}
              onClick={() => handleSave('sales_export')} disabled={saveMutation.isPending}>
              Save Sales Export Settings
            </Button>
          </TabPanel>
        </Box>
      </Box>
    </Box>
  )
}
