import { useState } from 'react'
import {
  Box, Typography, Button, Select, MenuItem, FormControl,
  InputLabel, Chip, IconButton, Tooltip, CircularProgress,
} from '@mui/material'
import type { SelectChangeEvent } from '@mui/material'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PauseIcon from '@mui/icons-material/Pause'
import PlayCircleIcon from '@mui/icons-material/PlayCircle'
import CloudUploadIcon from '@mui/icons-material/CloudUpload'
import AccessTimeIcon from '@mui/icons-material/AccessTime'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import apiClient from '../api/client'
import StatsCards from '../components/StatsCards/StatsCards'
import DocumentTable from '../components/DocumentTable/DocumentTable'
import ActivityLog from '../components/ActivityLog/ActivityLog'

const CRON_OPTIONS = [
  { label: 'Every 5 minutes', value: '*/5 * * * *' },
  { label: 'Every 15 minutes', value: '*/15 * * * *' },
  { label: 'Every 30 minutes', value: '*/30 * * * *' },
  { label: 'Every hour', value: '0 * * * *' },
  { label: 'Every 6 hours', value: '0 */6 * * *' },
  { label: 'Daily at midnight', value: '0 0 * * *' },
]

interface ScheduleStatus {
  running: boolean
  ftp_job?: { next_run: string | null }
  sales_export_job?: { next_run: string | null }
}

function SectionHeader({ title }: { title: string }) {
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, px: 2, pb: 1.5 }}>
      <Box sx={{ width: 3, height: 18, bgcolor: 'primary.main', borderRadius: 2 }} />
      <Typography sx={{ fontWeight: 700, fontSize: '0.85rem', textTransform: 'uppercase',
        letterSpacing: '0.06em', color: '#475569' }}>
        {title}
      </Typography>
    </Box>
  )
}

export default function DashboardPage() {
  const qc = useQueryClient()
  const [selectedCron, setSelectedCron] = useState('*/15 * * * *')

  const { data: scheduleStatus } = useQuery<ScheduleStatus>({
    queryKey: ['scheduleStatus'],
    queryFn: () => apiClient.get('/api/schedule/status').then((r) => r.data),
    refetchInterval: 15000,
  })

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
    const cron = e.target.value
    setSelectedCron(cron)
    configureMutation.mutate(cron)
  }

  const isRunning = scheduleStatus?.running ?? false
  const nextFtpRun = scheduleStatus?.ftp_job?.next_run
  const nextSalesRun = scheduleStatus?.sales_export_job?.next_run

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0 }}>

      {/* Scheduler strip */}
      <Box sx={{ px: 2, pb: 2.5 }}>
        <Box
          sx={{
            bgcolor: 'white',
            border: '1px solid #e2e8f0',
            borderRadius: '12px',
            p: '12px 16px',
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            gap: 1.5,
          }}
        >
          {/* Status dot */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
            <Box sx={{
              width: 8, height: 8, borderRadius: '50%',
              bgcolor: isRunning ? '#10b981' : '#ef4444',
              boxShadow: isRunning ? '0 0 0 3px rgba(16,185,129,0.2)' : '0 0 0 3px rgba(239,68,68,0.2)',
            }} />
            <Chip
              label={isRunning ? 'Running' : 'Paused'}
              size="small"
              sx={{
                fontSize: '0.72rem', fontWeight: 600, height: 22,
                bgcolor: isRunning ? '#f0fdf4' : '#fef2f2',
                color: isRunning ? '#059669' : '#dc2626',
                border: 'none',
              }}
            />
          </Box>

          <FormControl size="small" sx={{ minWidth: 185 }}>
            <InputLabel>FTP Poll Schedule</InputLabel>
            <Select value={selectedCron} label="FTP Poll Schedule" onChange={handleCronChange}>
              {CRON_OPTIONS.map((o) => (
                <MenuItem key={o.value} value={o.value}>{o.label}</MenuItem>
              ))}
            </Select>
          </FormControl>

          <Tooltip title={isRunning ? 'Pause scheduler' : 'Resume scheduler'}>
            <IconButton
              onClick={() => isRunning ? pauseMutation.mutate() : resumeMutation.mutate()}
              size="small"
              sx={{
                borderRadius: '8px',
                border: '1px solid #e2e8f0',
                color: isRunning ? '#d97706' : '#059669',
                '&:hover': { bgcolor: isRunning ? '#fffbeb' : '#f0fdf4' },
              }}
            >
              {isRunning ? <PauseIcon fontSize="small" /> : <PlayArrowIcon fontSize="small" />}
            </IconButton>
          </Tooltip>

          <Button
            variant="contained"
            size="small"
            startIcon={triggerMutation.isPending ? <CircularProgress size={13} color="inherit" /> : <PlayCircleIcon fontSize="small" />}
            onClick={() => triggerMutation.mutate()}
            disabled={triggerMutation.isPending}
            sx={{ height: 32 }}
          >
            {triggerMutation.isPending ? 'Polling…' : 'Poll Now'}
          </Button>

          <Button
            variant="outlined"
            size="small"
            color="secondary"
            startIcon={salesExportMutation.isPending ? <CircularProgress size={13} color="inherit" /> : <CloudUploadIcon fontSize="small" />}
            onClick={() => salesExportMutation.mutate()}
            disabled={salesExportMutation.isPending}
            sx={{ height: 32 }}
          >
            {salesExportMutation.isPending ? 'Exporting…' : 'Export Sales'}
          </Button>

          <Box sx={{ flexGrow: 1 }} />

          {(nextFtpRun || nextSalesRun) && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <AccessTimeIcon sx={{ fontSize: 13, color: '#94a3b8' }} />
              <Typography sx={{ fontSize: '0.72rem', color: '#94a3b8' }}>
                {nextFtpRun && `FTP ${new Date(nextFtpRun).toLocaleTimeString()}`}
                {nextFtpRun && nextSalesRun && '  ·  '}
                {nextSalesRun && `Export ${new Date(nextSalesRun).toLocaleTimeString()}`}
              </Typography>
            </Box>
          )}

          {triggerMutation.isSuccess && (
            <Typography sx={{ fontSize: '0.75rem', color: 'success.main', fontWeight: 500 }}>
              ✓ Poll triggered
            </Typography>
          )}
          {salesExportMutation.isSuccess && (
            <Typography sx={{ fontSize: '0.75rem', color: 'secondary.main', fontWeight: 500 }}>
              ✓ Export triggered
            </Typography>
          )}
        </Box>
      </Box>

      {/* Stats */}
      <Box sx={{ px: 2, pb: 2.5 }}>
        <StatsCards />
      </Box>

      {/* Documents */}
      <Box
        sx={{
          mx: 2, mb: 2.5,
          bgcolor: 'white',
          border: '1px solid #e2e8f0',
          borderRadius: '12px',
          overflow: 'hidden',
        }}
      >
        <Box sx={{ px: 2, pt: 2 }}>
          <SectionHeader title="Documents" />
        </Box>
        <Box sx={{ px: 2, pb: 2 }}>
          <DocumentTable />
        </Box>
      </Box>

      {/* Activity Log */}
      <Box
        sx={{
          mx: 2, mb: 2.5,
          bgcolor: 'white',
          border: '1px solid #e2e8f0',
          borderRadius: '12px',
          overflow: 'hidden',
        }}
      >
        <Box sx={{ px: 2, pt: 2 }}>
          <SectionHeader title="Activity Log" />
        </Box>
        <Box sx={{ px: 2, pb: 2 }}>
          <ActivityLog />
        </Box>
      </Box>

    </Box>
  )
}
