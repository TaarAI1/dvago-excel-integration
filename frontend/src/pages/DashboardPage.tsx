import { useState } from 'react'
import {
  Box, Typography, Divider, Button, Select, MenuItem, FormControl,
  InputLabel, Chip, IconButton, Tooltip, CircularProgress,
} from '@mui/material'
import type { SelectChangeEvent } from '@mui/material'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PauseIcon from '@mui/icons-material/Pause'
import PlayCircleIcon from '@mui/icons-material/PlayCircle'
import CloudUploadIcon from '@mui/icons-material/CloudUpload'
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

  const triggerMutation = useMutation({
    mutationFn: () => apiClient.post('/api/process/trigger'),
  })

  const salesExportMutation = useMutation({
    mutationFn: () => apiClient.post('/api/sales-export/trigger'),
  })

  const handleCronChange = (e: SelectChangeEvent) => {
    const cron = e.target.value
    setSelectedCron(cron)
    configureMutation.mutate(cron)
  }

  const isRunning = scheduleStatus?.running ?? false
  const nextFtpRun = scheduleStatus?.ftp_job?.next_run
  const nextSalesRun = scheduleStatus?.sales_export_job?.next_run

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
      {/* Scheduler Controls */}
      <Box sx={{
        display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 2,
        bgcolor: 'white', p: 2, borderRadius: 1, boxShadow: 1,
      }}>
        <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'text.secondary', mr: 1 }}>
          Scheduler
        </Typography>

        <FormControl size="small" sx={{ minWidth: 190 }}>
          <InputLabel>FTP Poll Schedule</InputLabel>
          <Select value={selectedCron} label="FTP Poll Schedule" onChange={handleCronChange}>
            {CRON_OPTIONS.map((o) => (
              <MenuItem key={o.value} value={o.value}>{o.label}</MenuItem>
            ))}
          </Select>
        </FormControl>

        {isRunning ? (
          <Tooltip title="Pause scheduler">
            <IconButton onClick={() => pauseMutation.mutate()} size="small" color="warning">
              <PauseIcon />
            </IconButton>
          </Tooltip>
        ) : (
          <Tooltip title="Resume scheduler">
            <IconButton onClick={() => resumeMutation.mutate()} size="small" color="success">
              <PlayArrowIcon />
            </IconButton>
          </Tooltip>
        )}

        <Button
          variant="contained" size="small"
          startIcon={triggerMutation.isPending ? <CircularProgress size={14} color="inherit" /> : <PlayCircleIcon />}
          onClick={() => triggerMutation.mutate()}
          disabled={triggerMutation.isPending}
        >
          {triggerMutation.isPending ? 'Polling...' : 'Poll Now'}
        </Button>

        <Button
          variant="outlined" size="small" color="secondary"
          startIcon={salesExportMutation.isPending ? <CircularProgress size={14} color="inherit" /> : <CloudUploadIcon />}
          onClick={() => salesExportMutation.mutate()}
          disabled={salesExportMutation.isPending}
        >
          {salesExportMutation.isPending ? 'Exporting...' : 'Export Sales Now'}
        </Button>

        <Box sx={{ flexGrow: 1 }} />

        <Chip
          label={isRunning ? 'Scheduler On' : 'Scheduler Off'}
          color={isRunning ? 'success' : 'error'}
          size="small"
        />
        {nextFtpRun && (
          <Typography variant="caption" color="text.secondary">
            Next FTP: {new Date(nextFtpRun).toLocaleTimeString()}
          </Typography>
        )}
        {nextSalesRun && (
          <Typography variant="caption" color="text.secondary">
            Next Export: {new Date(nextSalesRun).toLocaleTimeString()}
          </Typography>
        )}
        {triggerMutation.isSuccess && (
          <Typography variant="caption" sx={{ color: 'success.main' }}>Poll triggered!</Typography>
        )}
        {salesExportMutation.isSuccess && (
          <Typography variant="caption" sx={{ color: 'secondary.main' }}>Export triggered!</Typography>
        )}
      </Box>

      <StatsCards />
      <Divider />
      <DocumentTable />
      <Divider />
      <ActivityLog />
    </Box>
  )
}
