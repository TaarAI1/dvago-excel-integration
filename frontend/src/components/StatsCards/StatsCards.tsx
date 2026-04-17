import { Box, Typography, Skeleton } from '@mui/material'
import { fmtTime, fmtDateTime } from '../../utils/time'
import CheckCircleOutlinedIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
import HourglassTopIcon from '@mui/icons-material/HourglassTop'
import DescriptionOutlinedIcon from '@mui/icons-material/DescriptionOutlined'
import SpeedOutlinedIcon from '@mui/icons-material/SpeedOutlined'
import { useSSE } from '../../hooks/useSSE'

interface DashboardStats {
  total: number; posted: number; errors: number; pending: number
  total_today: number; post_rate_pct: number
  last_poll_time: string | null; avg_api_response_ms?: number; ts: string
}

interface StatCardProps {
  title: string; value: string | number; subtitle?: string
  icon: React.ReactNode; color: string; loading?: boolean
}

function StatCard({ title, value, subtitle, icon, color, loading }: StatCardProps) {
  return (
    <Box
      sx={{
        flex: '1 1 160px', minWidth: 140,
        bgcolor: 'white',
        border: '1px solid #e5e7eb',
        borderRadius: '6px',
        p: '14px 16px',
      }}
    >
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1.5 }}>
        <Typography sx={{ fontSize: '0.7rem', fontWeight: 600, textTransform: 'uppercase',
          letterSpacing: '0.06em', color: '#6b7280' }}>
          {title}
        </Typography>
        <Box sx={{ color, lineHeight: 0 }}>{icon}</Box>
      </Box>
      {loading ? (
        <Skeleton variant="text" width={48} height={34} />
      ) : (
        <Typography sx={{ fontSize: '1.6rem', fontWeight: 700, lineHeight: 1, color: '#111827' }}>
          {value}
        </Typography>
      )}
      {subtitle && (
        <Typography sx={{ fontSize: '0.7rem', color: '#9ca3af', mt: 0.5 }}>{subtitle}</Typography>
      )}
    </Box>
  )
}

export default function StatsCards() {
  const { data: stats, error } = useSSE<DashboardStats>('/api/stream/dashboard')
  const loading = !stats

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
        <Typography sx={{ fontSize: '0.7rem', fontWeight: 600, textTransform: 'uppercase',
          letterSpacing: '0.06em', color: '#9ca3af' }}>
          Live Statistics
        </Typography>
        {stats && (
          <>
            <Box sx={{ width: 5, height: 5, borderRadius: '50%', bgcolor: '#15803d',
              animation: 'blink 2s infinite',
              '@keyframes blink': { '0%,100%': { opacity: 1 }, '50%': { opacity: 0.3 } },
            }} />
            <Typography sx={{ fontSize: '0.7rem', color: '#9ca3af' }}>
              {fmtTime(stats.ts)}
            </Typography>
          </>
        )}
        {error && <Typography sx={{ fontSize: '0.7rem', color: 'warning.main' }}>{error}</Typography>}
      </Box>

      <Box sx={{ display: 'flex', gap: 1.5, flexWrap: 'wrap' }}>
        <StatCard title="Total" value={stats?.total ?? 0} subtitle={`${stats?.total_today ?? 0} today`}
          icon={<DescriptionOutlinedIcon sx={{ fontSize: 15 }} />} color="#1a56db" loading={loading} />
        <StatCard title="Posted" value={stats?.posted ?? 0} subtitle={`${stats?.post_rate_pct ?? 0}% rate`}
          icon={<CheckCircleOutlinedIcon sx={{ fontSize: 15 }} />} color="#15803d" loading={loading} />
        <StatCard title="Errors" value={stats?.errors ?? 0} subtitle="Need attention"
          icon={<ErrorOutlinedIcon sx={{ fontSize: 15 }} />} color="#b91c1c" loading={loading} />
        <StatCard title="Pending" value={stats?.pending ?? 0} subtitle="Queued"
          icon={<HourglassTopIcon sx={{ fontSize: 15 }} />} color="#b45309" loading={loading} />
        <StatCard
          title="Avg API Time"
          value={stats?.avg_api_response_ms != null ? `${stats.avg_api_response_ms}ms` : '—'}
          subtitle="Per document"
          icon={<SpeedOutlinedIcon sx={{ fontSize: 15 }} />} color="#6b7280" loading={loading}
        />
      </Box>

      {stats?.last_poll_time && (
        <Typography sx={{ fontSize: '0.7rem', color: '#9ca3af', mt: 1 }}>
          Last poll: {fmtDateTime(stats.last_poll_time)}
        </Typography>
      )}
    </Box>
  )
}
