import { Box, Card, CardContent, Typography, Skeleton, Chip } from '@mui/material'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ErrorIcon from '@mui/icons-material/Error'
import HourglassEmptyIcon from '@mui/icons-material/HourglassEmpty'
import StorageIcon from '@mui/icons-material/Storage'
import SpeedIcon from '@mui/icons-material/Speed'
import { useSSE } from '../../hooks/useSSE'

interface DashboardStats {
  total: number
  posted: number
  errors: number
  pending: number
  total_today: number
  posted_today: number
  post_rate_pct: number
  last_poll_time: string | null
  avg_api_response_ms?: number
  ts: string
}

interface StatCardProps {
  title: string
  value: string | number
  subtitle?: string
  icon: React.ReactNode
  color: string
  loading?: boolean
}

function StatCard({ title, value, subtitle, icon, color, loading }: StatCardProps) {
  return (
    <Card elevation={2} sx={{ flex: '1 1 180px', minWidth: 160 }}>
      <CardContent sx={{ pb: '12px !important' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
          <Box sx={{ color, display: 'flex' }}>{icon}</Box>
          <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 600, textTransform: 'uppercase', letterSpacing: 0.5 }}>
            {title}
          </Typography>
        </Box>
        {loading ? (
          <Skeleton variant="text" width={60} height={36} />
        ) : (
          <Typography variant="h4" sx={{ fontWeight: 700, lineHeight: 1.1 }}>
            {value}
          </Typography>
        )}
        {subtitle && (
          <Typography variant="caption" color="text.secondary">{subtitle}</Typography>
        )}
      </CardContent>
    </Card>
  )
}

export default function StatsCards() {
  const { data: stats, error } = useSSE<DashboardStats>('/api/stream/dashboard')

  const loading = !stats

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
        <Typography variant="subtitle2" color="text.secondary">Live Statistics</Typography>
        {stats && (
          <Chip
            label={`Updated ${new Date(stats.ts).toLocaleTimeString()}`}
            size="small"
            variant="outlined"
            sx={{ fontSize: 11 }}
          />
        )}
        {error && <Chip label={error} size="small" color="warning" sx={{ fontSize: 11 }} />}
      </Box>

      <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
        <StatCard
          title="Total Documents"
          value={stats?.total ?? 0}
          subtitle={`${stats?.total_today ?? 0} today`}
          icon={<StorageIcon fontSize="small" />}
          color="#1976d2"
          loading={loading}
        />
        <StatCard
          title="Posted"
          value={stats?.posted ?? 0}
          subtitle={`${stats?.post_rate_pct ?? 0}% success rate`}
          icon={<CheckCircleIcon fontSize="small" />}
          color="#2e7d32"
          loading={loading}
        />
        <StatCard
          title="Errors"
          value={stats?.errors ?? 0}
          subtitle="Require attention"
          icon={<ErrorIcon fontSize="small" />}
          color="#d32f2f"
          loading={loading}
        />
        <StatCard
          title="Pending"
          value={stats?.pending ?? 0}
          subtitle="Awaiting API call"
          icon={<HourglassEmptyIcon fontSize="small" />}
          color="#f57c00"
          loading={loading}
        />
        <StatCard
          title="Avg API Time"
          value={stats?.avg_api_response_ms != null ? `${stats.avg_api_response_ms}ms` : '—'}
          subtitle="Per document"
          icon={<SpeedIcon fontSize="small" />}
          color="#7b1fa2"
          loading={loading}
        />
      </Box>

      {stats?.last_poll_time && (
        <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
          Last successful poll: {new Date(stats.last_poll_time).toLocaleString()}
        </Typography>
      )}
    </Box>
  )
}
