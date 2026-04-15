import { useState } from 'react'
import type { SelectChangeEvent } from '@mui/material'
import {
  AppBar, Toolbar, Typography, Box, Button, Select, MenuItem,
  FormControl, InputLabel, Chip, CircularProgress, Tooltip,
  IconButton,
} from '@mui/material'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PauseIcon from '@mui/icons-material/Pause'
import PlayCircleIcon from '@mui/icons-material/PlayCircle'
import LogoutIcon from '@mui/icons-material/Logout'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import apiClient from '../../api/client'

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
}

export default function Header() {
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

  const handleCronChange = (e: SelectChangeEvent) => {
    const cron = e.target.value
    setSelectedCron(cron)
    configureMutation.mutate(cron)
  }

  const handleLogout = () => {
    localStorage.removeItem('access_token')
    window.location.href = '/login'
  }

  const nextRun = scheduleStatus?.ftp_job?.next_run
  const isRunning = scheduleStatus?.running ?? false

  return (
    <AppBar position="static" elevation={2} sx={{ bgcolor: 'primary.dark' }}>
      <Toolbar sx={{ gap: 2, flexWrap: 'wrap', py: 1 }}>
        <Typography variant="h6" sx={{ fontWeight: 700, flexGrow: 0, mr: 2 }}>
          RetailPro Integration
        </Typography>

        {/* Schedule selector */}
        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel sx={{ color: 'white' }}>Poll Schedule</InputLabel>
          <Select
            value={selectedCron}
            label="Poll Schedule"
            onChange={handleCronChange}
            sx={{ color: 'white', '& .MuiOutlinedInput-notchedOutline': { borderColor: 'rgba(255,255,255,0.5)' } }}
          >
            {CRON_OPTIONS.map((o) => (
              <MenuItem key={o.value} value={o.value}>{o.label}</MenuItem>
            ))}
          </Select>
        </FormControl>

        {/* Pause / Resume */}
        {isRunning ? (
          <Tooltip title="Pause scheduler">
            <IconButton color="inherit" onClick={() => pauseMutation.mutate()} size="small">
              <PauseIcon />
            </IconButton>
          </Tooltip>
        ) : (
          <Tooltip title="Resume scheduler">
            <IconButton color="inherit" onClick={() => resumeMutation.mutate()} size="small">
              <PlayArrowIcon />
            </IconButton>
          </Tooltip>
        )}

        {/* Manual trigger */}
        <Button
          variant="contained"
          color="secondary"
          size="small"
          startIcon={triggerMutation.isPending ? <CircularProgress size={14} color="inherit" /> : <PlayCircleIcon />}
          onClick={() => triggerMutation.mutate()}
          disabled={triggerMutation.isPending}
          sx={{ whiteSpace: 'nowrap' }}
        >
          {triggerMutation.isPending ? 'Polling...' : 'Poll Now'}
        </Button>

        <Box sx={{ flexGrow: 1 }} />

        {/* Status indicators */}
        <Chip
          label={isRunning ? 'Scheduler On' : 'Scheduler Off'}
          color={isRunning ? 'success' : 'error'}
          size="small"
        />

        {nextRun && (
          <Typography variant="caption" sx={{ color: 'rgba(255,255,255,0.7)' }}>
            Next: {new Date(nextRun).toLocaleTimeString()}
          </Typography>
        )}

        {triggerMutation.isSuccess && (
          <Typography variant="caption" sx={{ color: '#90EE90' }}>
            Poll triggered!
          </Typography>
        )}

        <Tooltip title="Logout">
          <IconButton color="inherit" onClick={handleLogout} size="small">
            <LogoutIcon />
          </IconButton>
        </Tooltip>
      </Toolbar>
    </AppBar>
  )
}
