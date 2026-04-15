import { useState, useEffect } from 'react'
import {
  Box, Typography, Tabs, Tab, TextField, Button, Alert,
  CircularProgress, InputAdornment, IconButton, Paper, Grid,
  Chip, Divider,
} from '@mui/material'
import VisibilityIcon from '@mui/icons-material/Visibility'
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff'
import WifiIcon from '@mui/icons-material/Wifi'
import SaveIcon from '@mui/icons-material/Save'
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
      InputProps={meta.is_sensitive ? {
        endAdornment: (
          <InputAdornment position="end">
            <IconButton size="small" onClick={() => setShow(!show)}>
              {show ? <VisibilityOffIcon fontSize="small" /> : <VisibilityIcon fontSize="small" />}
            </IconButton>
          </InputAdornment>
        ),
      } : undefined}
    />
  )
}

function TestButton({
  label, onClick, result,
}: {
  label: string
  onClick: () => void
  result: { ok: boolean; error: string | null } | null
}) {
  const [loading, setLoading] = useState(false)

  const handle = async () => {
    setLoading(true)
    await onClick()
    setLoading(false)
  }

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mt: 2 }}>
      <Button
        variant="outlined"
        size="small"
        startIcon={loading ? <CircularProgress size={14} /> : <WifiIcon />}
        onClick={handle}
        disabled={loading}
      >
        {label}
      </Button>
      {result !== null && (
        <Chip
          label={result.ok ? 'Connected' : result.error || 'Failed'}
          color={result.ok ? 'success' : 'error'}
          size="small"
        />
      )}
    </Box>
  )
}

const TABS = ['ftp', 'oracle', 'retailpro', 'scheduler', 'sales_export']
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

  // Flatten all settings into local state for editing
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

  const handleChange = (key: string, val: string) => {
    setLocalValues((prev) => ({ ...prev, [key]: val }))
  }

  const getVal = (key: string) => localValues[key] ?? ''

  const handleSave = (category: string) => {
    const catKeys = Object.keys(allSettings?.[category] ?? {})
    const updates: Record<string, string> = {}
    for (const k of catKeys) updates[k] = getVal(k)
    saveMutation.mutate(updates)
  }

  const testFtp = async () => {
    try {
      const res = await apiClient.post('/api/settings/test/ftp', {
        host: getVal('ftp_host'),
        port: parseInt(getVal('ftp_port') || '21'),
        user: getVal('ftp_user'),
        password: getVal('ftp_password'),
      })
      setFtpResult(res.data)
    } catch {
      setFtpResult({ ok: false, error: 'Request failed' })
    }
  }

  const testOracle = async () => {
    try {
      const res = await apiClient.post('/api/settings/test/oracle', {
        host: getVal('oracle_host'),
        port: parseInt(getVal('oracle_port') || '1521'),
        user: getVal('oracle_username'),
        password: getVal('oracle_password'),
        service_name: getVal('oracle_service_name'),
      })
      setOracleResult(res.data)
    } catch {
      setOracleResult({ ok: false, error: 'Request failed' })
    }
  }

  const testRetailPro = async () => {
    try {
      const res = await apiClient.post('/api/settings/test/retailpro', {
        base_url: getVal('retailpro_base_url'),
        api_key: getVal('retailpro_api_key'),
      })
      setRetailproResult(res.data)
    } catch {
      setRetailproResult({ ok: false, error: 'Request failed' })
    }
  }

  if (isLoading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', pt: 8 }}>
        <CircularProgress />
      </Box>
    )
  }

  return (
    <Box>
      <Typography variant="h5" fontWeight={700} sx={{ mb: 3 }}>Configuration</Typography>

      {saveMsg && <Alert severity="success" sx={{ mb: 2 }}>{saveMsg}</Alert>}
      {saveError && <Alert severity="error" sx={{ mb: 2 }}>{saveError}</Alert>}

      <Paper elevation={1}>
        <Tabs
          value={tab}
          onChange={(_, v) => setTab(v)}
          sx={{ borderBottom: 1, borderColor: 'divider', px: 2 }}
          variant="scrollable"
        >
          {TAB_LABELS.map((label, i) => (
            <Tab key={i} label={label} />
          ))}
        </Tabs>

        <Box sx={{ p: 3 }}>
          {/* FTP Tab */}
          <TabPanel value={tab} index={0}>
            <Grid container spacing={2}>
              <Grid item xs={12} sm={8}>
                <SettingField settingKey="ftp_host" meta={{ label: 'FTP Host', value: getVal('ftp_host'), is_sensitive: false, updated_at: null }} value={getVal('ftp_host')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={4}>
                <SettingField settingKey="ftp_port" meta={{ label: 'FTP Port', value: getVal('ftp_port'), is_sensitive: false, updated_at: null }} value={getVal('ftp_port')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={6}>
                <SettingField settingKey="ftp_user" meta={{ label: 'FTP Username', value: getVal('ftp_user'), is_sensitive: false, updated_at: null }} value={getVal('ftp_user')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={6}>
                <SettingField settingKey="ftp_password" meta={{ label: 'FTP Password', value: getVal('ftp_password'), is_sensitive: true, updated_at: null }} value={getVal('ftp_password')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={6}>
                <SettingField settingKey="ftp_import_path" meta={{ label: 'Import Path (download CSVs from here)', value: getVal('ftp_import_path'), is_sensitive: false, updated_at: null }} value={getVal('ftp_import_path')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={6}>
                <SettingField settingKey="ftp_export_path" meta={{ label: 'Export Path (upload sales CSVs here)', value: getVal('ftp_export_path'), is_sensitive: false, updated_at: null }} value={getVal('ftp_export_path')} onChange={handleChange} />
              </Grid>
            </Grid>
            <TestButton label="Test FTP Connection" onClick={testFtp} result={ftpResult} />
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon />} onClick={() => handleSave('ftp')} disabled={saveMutation.isPending}>
              Save FTP Settings
            </Button>
          </TabPanel>

          {/* Oracle Tab */}
          <TabPanel value={tab} index={1}>
            <Grid container spacing={2}>
              <Grid item xs={12} sm={8}>
                <SettingField settingKey="oracle_host" meta={{ label: 'Oracle Host', value: getVal('oracle_host'), is_sensitive: false, updated_at: null }} value={getVal('oracle_host')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={4}>
                <SettingField settingKey="oracle_port" meta={{ label: 'Oracle Port', value: getVal('oracle_port'), is_sensitive: false, updated_at: null }} value={getVal('oracle_port')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12}>
                <SettingField settingKey="oracle_service_name" meta={{ label: 'Service Name', value: getVal('oracle_service_name'), is_sensitive: false, updated_at: null }} value={getVal('oracle_service_name')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={6}>
                <SettingField settingKey="oracle_username" meta={{ label: 'Oracle Username', value: getVal('oracle_username'), is_sensitive: false, updated_at: null }} value={getVal('oracle_username')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={6}>
                <SettingField settingKey="oracle_password" meta={{ label: 'Oracle Password', value: getVal('oracle_password'), is_sensitive: true, updated_at: null }} value={getVal('oracle_password')} onChange={handleChange} />
              </Grid>
            </Grid>
            <TestButton label="Test Oracle Connection" onClick={testOracle} result={oracleResult} />
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon />} onClick={() => handleSave('oracle')} disabled={saveMutation.isPending}>
              Save Oracle Settings
            </Button>
          </TabPanel>

          {/* RetailPro Tab */}
          <TabPanel value={tab} index={2}>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <SettingField settingKey="retailpro_base_url" meta={{ label: 'RetailPro Base URL', value: getVal('retailpro_base_url'), is_sensitive: false, updated_at: null }} value={getVal('retailpro_base_url')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={8}>
                <SettingField settingKey="retailpro_api_key" meta={{ label: 'API Key', value: getVal('retailpro_api_key'), is_sensitive: true, updated_at: null }} value={getVal('retailpro_api_key')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={4}>
                <SettingField settingKey="retailpro_client" meta={{ label: 'Mode (mock / real)', value: getVal('retailpro_client'), is_sensitive: false, updated_at: null }} value={getVal('retailpro_client')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12}>
                <SettingField settingKey="document_type_endpoints" meta={{ label: 'Document Type → Endpoint Map (JSON)', value: getVal('document_type_endpoints'), is_sensitive: false, updated_at: null }} value={getVal('document_type_endpoints')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12}>
                <SettingField settingKey="document_type_field_maps" meta={{ label: 'Document Field Maps (JSON)', value: getVal('document_type_field_maps'), is_sensitive: false, updated_at: null }} value={getVal('document_type_field_maps')} onChange={handleChange} />
              </Grid>
            </Grid>
            <TestButton label="Test RetailPro API" onClick={testRetailPro} result={retailproResult} />
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon />} onClick={() => handleSave('retailpro')} disabled={saveMutation.isPending}>
              Save RetailPro Settings
            </Button>
          </TabPanel>

          {/* Scheduler Tab */}
          <TabPanel value={tab} index={3}>
            <Grid container spacing={2}>
              <Grid item xs={12} sm={6}>
                <SettingField settingKey="poll_cron_schedule" meta={{ label: 'FTP Import Cron (e.g. */15 * * * *)', value: getVal('poll_cron_schedule'), is_sensitive: false, updated_at: null }} value={getVal('poll_cron_schedule')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={6}>
                <SettingField settingKey="sales_export_cron" meta={{ label: 'Sales Export Cron (e.g. 0 2 * * *)', value: getVal('sales_export_cron'), is_sensitive: false, updated_at: null }} value={getVal('sales_export_cron')} onChange={handleChange} />
              </Grid>
            </Grid>
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
              Standard cron format: minute hour day month weekday. Changes take effect immediately.
            </Typography>
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon />} onClick={() => handleSave('scheduler')} disabled={saveMutation.isPending}>
              Save Scheduler Settings
            </Button>
          </TabPanel>

          {/* Sales Export Tab */}
          <TabPanel value={tab} index={4}>
            <Grid container spacing={2}>
              <Grid item xs={12}>
                <SettingField settingKey="sales_export_sql" meta={{ label: 'Sales SQL Query', value: getVal('sales_export_sql'), is_sensitive: false, updated_at: null }} value={getVal('sales_export_sql')} onChange={handleChange} />
              </Grid>
              <Grid item xs={12} sm={6}>
                <SettingField settingKey="sales_export_filename_prefix" meta={{ label: 'Output Filename Prefix', value: getVal('sales_export_filename_prefix'), is_sensitive: false, updated_at: null }} value={getVal('sales_export_filename_prefix')} onChange={handleChange} />
              </Grid>
            </Grid>
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>
              Output file will be named: <code>{'<prefix>_YYYYMMDD_HHMMSS.csv'}</code>
            </Typography>
            <Divider sx={{ my: 3 }} />
            <Button variant="contained" startIcon={<SaveIcon />} onClick={() => handleSave('sales_export')} disabled={saveMutation.isPending}>
              Save Sales Export Settings
            </Button>
          </TabPanel>
        </Box>
      </Paper>
    </Box>
  )
}
