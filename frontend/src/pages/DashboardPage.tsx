import { useState, useEffect } from 'react'
import {
  Box, Typography, Button, Select, MenuItem, FormControl,
  InputLabel, Chip, IconButton, Tooltip, CircularProgress,
} from '@mui/material'
import type { SelectChangeEvent } from '@mui/material'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PauseIcon from '@mui/icons-material/Pause'
import PlayCircleOutlinedIcon from '@mui/icons-material/PlayCircleOutlined'
import CloudUploadOutlinedIcon from '@mui/icons-material/CloudUploadOutlined'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import apiClient from '../api/client'
import { fmtTime } from '../utils/time'
import StatsCards from '../components/StatsCards/StatsCards'
import ActivityLog from '../components/ActivityLog/ActivityLog'

const CRON_OPTIONS = [
  { label: 'Every 5 min', value: '*/5 * * * *' },
  { label: 'Every 15 min', value: '*/15 * * * *' },
  { label: 'Every 30 min', value: '*/30 * * * *' },
  { label: 'Every hour', value: '0 * * * *' },
  { label: 'Every 6 hours', value: '0 */6 * * *' },
  { label: 'Daily midnight', value: '0 0 * * *' },
]

interface ScheduleStatus {
  running: boolean
  cron?: string
  ftp_job?: { next_run: string | null }
  sales_export_job?: { next_run: string | null }
  sales_export_job_2?: { next_run: string | null }
}

// Reusable page section wrapper
function Section({ title, children, action }: { title?: string; children: React.ReactNode; action?: React.ReactNode }) {
  return (
    <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>
      {title && (
        <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid #f3f4f6',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Typography sx={{ fontWeight: 600, fontSize: '0.8rem', color: '#374151', textTransform: 'uppercase',
            letterSpacing: '0.05em' }}>
            {title}
          </Typography>
          {action}
        </Box>
      )}
      <Box sx={{ p: 2 }}>{children}</Box>
    </Box>
  )
}

export default function DashboardPage() {
  const qc = useQueryClient()
  // Empty string until we load the real value from the backend
  const [selectedCron, setSelectedCron] = useState('')
  const [cronReady, setCronReady] = useState(false)

  const { data: scheduleStatus } = useQuery<ScheduleStatus>({
    queryKey: ['scheduleStatus'],
    queryFn: () => apiClient.get('/api/schedule/status').then((r) => r.data),
    refetchInterval: 15000,
  })

  // Initialise dropdown from server on first load only (don't override user's mid-session choice)
  useEffect(() => {
    if (!cronReady && scheduleStatus?.cron) {
      setSelectedCron(scheduleStatus.cron)
      setCronReady(true)
    }
  }, [scheduleStatus, cronReady])

  const configureMutation = useMutation({
    mutationFn: (cron: string) => apiClient.post('/api/schedule/configure', { cron }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['scheduleStatus'] }),
  })
  const pauseMutation = useMutation({
    mutationFn: () => apiClient.delete('/api/schedule/pause'),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['scheduleStatus'] }),
  })
  const resumeMutation = useMutation({
    mutationFn: () => apiClient.post('/api/schedule/resume'),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['scheduleStatus'] }),
  })
  const triggerMutation = useMutation({ mutationFn: () => apiClient.post('/api/process/trigger') })
  const salesExportMutation = useMutation({ mutationFn: () => apiClient.post('/api/sales-export/trigger') })

  const handleCronChange = (e: SelectChangeEvent) => {
    const v = e.target.value; setSelectedCron(v); configureMutation.mutate(v)
  }

  const isRunning     = scheduleStatus?.running ?? false
  const nextFtpRun    = scheduleStatus?.ftp_job?.next_run
  const nextSalesRun  = scheduleStatus?.sales_export_job?.next_run
  const nextSalesRun2 = scheduleStatus?.sales_export_job_2?.next_run

  return (
    <Box sx={{ p: { xs: 2, sm: 3 }, display: 'flex', flexDirection: 'column', gap: 2 }}>

      {/* Scheduler row */}
      <Box
        sx={{
          bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px',
          px: 2, py: 1.5,
          display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 1.5,
        }}
      >
        <Chip
          size="small"
          label={isRunning ? '● Running' : '○ Paused'}
          sx={{
            fontSize: '0.72rem', fontWeight: 600, height: 24, borderRadius: '4px',
            bgcolor: isRunning ? '#f0fdf4' : '#fef2f2',
            color: isRunning ? '#15803d' : '#b91c1c',
            border: '1px solid', borderColor: isRunning ? '#bbf7d0' : '#fecaca',
          }}
        />

        <FormControl size="small" sx={{ minWidth: 160 }}>
          <InputLabel>FTP Schedule</InputLabel>
          <Select value={selectedCron} label="FTP Schedule" onChange={handleCronChange}
            displayEmpty renderValue={(v) => {
              const match = CRON_OPTIONS.find((o) => o.value === v)
              return match ? match.label : (v || '…')
            }}>
            {CRON_OPTIONS.map((o) => <MenuItem key={o.value} value={o.value}>{o.label}</MenuItem>)}
            {/* Show current value as an option if it isn't one of the presets */}
            {selectedCron && !CRON_OPTIONS.some((o) => o.value === selectedCron) && (
              <MenuItem value={selectedCron} sx={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
                {selectedCron}
              </MenuItem>
            )}
          </Select>
        </FormControl>

        <Tooltip title={isRunning ? 'Pause' : 'Resume'}>
          <IconButton
            size="small"
            onClick={() => isRunning ? pauseMutation.mutate() : resumeMutation.mutate()}
            sx={{ borderRadius: '6px', border: '1px solid #e5e7eb', width: 30, height: 30,
              color: isRunning ? '#b45309' : '#15803d',
              '&:hover': { bgcolor: '#f9fafb' } }}
          >
            {isRunning ? <PauseIcon sx={{ fontSize: 15 }} /> : <PlayArrowIcon sx={{ fontSize: 15 }} />}
          </IconButton>
        </Tooltip>

        <Button
          variant="contained" size="small"
          startIcon={triggerMutation.isPending
            ? <CircularProgress size={12} color="inherit" />
            : <PlayCircleOutlinedIcon sx={{ fontSize: 15 }} />}
          onClick={() => triggerMutation.mutate()}
          disabled={triggerMutation.isPending}
          sx={{ height: 30, fontSize: '0.8rem' }}
        >
          {triggerMutation.isPending ? 'Polling…' : 'Poll Now'}
        </Button>

        <Button
          variant="outlined" size="small" color="secondary"
          startIcon={salesExportMutation.isPending
            ? <CircularProgress size={12} color="inherit" />
            : <CloudUploadOutlinedIcon sx={{ fontSize: 15 }} />}
          onClick={() => salesExportMutation.mutate()}
          disabled={salesExportMutation.isPending}
          sx={{ height: 30, fontSize: '0.8rem' }}
        >
          {salesExportMutation.isPending ? 'Exporting…' : 'Export Sales'}
        </Button>

        <Box sx={{ flexGrow: 1 }} />

        {(nextFtpRun || nextSalesRun || nextSalesRun2) && (
          <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
            {nextFtpRun   && `Next FTP: ${fmtTime(nextFtpRun)}`}
            {nextFtpRun   && nextSalesRun  && '  ·  '}
            {nextSalesRun && `Export 1: ${fmtTime(nextSalesRun)}`}
            {nextSalesRun2 && `  ·  Export 2: ${fmtTime(nextSalesRun2)}`}
          </Typography>
        )}
        {triggerMutation.isSuccess && (
          <Typography sx={{ fontSize: '0.72rem', color: '#15803d', fontWeight: 500 }}>✓ Triggered</Typography>
        )}
      </Box>

      {/* Stats */}
      <StatsCards />

      {/* Activity log */}
      <Section title="Activity Log">
        <ActivityLog />
      </Section>

    </Box>
  )
}
