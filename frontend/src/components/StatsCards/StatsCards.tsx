import { Box, Typography, Skeleton } from '@mui/material'
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutlined'
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutlined'
import HourglassTopIcon from '@mui/icons-material/HourglassTop'
import DescriptionOutlinedIcon from '@mui/icons-material/DescriptionOutlined'
import SpeedOutlinedIcon from '@mui/icons-material/SpeedOutlined'
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
  accent: string
  bg: string
  loading?: boolean
}

function StatCard({ title, value, subtitle, icon, accent, bg, loading }: StatCardProps) {
  return (
    <Box
      sx={{
        flex: '1 1 170px',
        minWidth: 155,
        bgcolor: 'white',
        borderRadius: '12px',
        border: '1px solid #e2e8f0',
        overflow: 'hidden',
        boxShadow: '0 1px 3px 0 rgb(0 0 0 / 0.05)',
        transition: 'box-shadow 0.2s, transform 0.2s',
        '&:hover': {
          boxShadow: '0 4px 12px 0 rgb(0 0 0 / 0.08)',
          transform: 'translateY(-1px)',
        },
      }}
    >
      {/* Accent bar */}
      <Box sx={{ height: 3, bgcolor: accent, borderRadius: '12px 12px 0 0' }} />

      <Box sx={{ p: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
          <Typography
            sx={{ fontSize: '0.7rem', fontWeight: 700, textTransform: 'uppercase',
              letterSpacing: '0.07em', color: '#64748b' }}
          >
            {title}
          </Typography>
          <Box
            sx={{
              width: 32, height: 32, borderRadius: '8px',
              bgcolor: bg,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: accent,
            }}
          >
            {icon}
          </Box>
        </Box>

        {loading ? (
          <Skeleton variant="text" width={56} height={40} />
        ) : (
          <Typography sx={{ fontSize: '1.8rem', fontWeight: 700, lineHeight: 1, color: '#0f172a', mb: 0.5 }}>
            {value}
          </Typography>
        )}

        {subtitle && (
          <Typography sx={{ fontSize: '0.72rem', color: '#94a3b8' }}>
            {subtitle}
          </Typography>
        )}
      </Box>
    </Box>
  )
}

export default function StatsCards() {
  const { data: stats, error } = useSSE<DashboardStats>('/api/stream/dashboard')
  const loading = !stats

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 1.5 }}>
        <Typography sx={{ fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase',
          letterSpacing: '0.07em', color: '#64748b' }}>
          Live Statistics
        </Typography>
        {stats && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Box sx={{ width: 6, height: 6, borderRadius: '50%', bgcolor: '#10b981',
              animation: 'pulse 2s infinite',
              '@keyframes pulse': { '0%,100%': { opacity: 1 }, '50%': { opacity: 0.4 } },
            }} />
            <Typography sx={{ fontSize: '0.72rem', color: '#94a3b8' }}>
              {new Date(stats.ts).toLocaleTimeString()}
            </Typography>
          </Box>
        )}
        {error && (
          <Typography sx={{ fontSize: '0.72rem', color: 'warning.main' }}>{error}</Typography>
        )}
      </Box>

      <Box sx={{ display: 'flex', gap: 1.5, flexWrap: 'wrap' }}>
        <StatCard
          title="Total Documents"
          value={stats?.total ?? 0}
          subtitle={`${stats?.total_today ?? 0} today`}
          icon={<DescriptionOutlinedIcon sx={{ fontSize: 16 }} />}
          accent="#2563eb" bg="#eff6ff" loading={loading}
        />
        <StatCard
          title="Posted"
          value={stats?.posted ?? 0}
          subtitle={`${stats?.post_rate_pct ?? 0}% success rate`}
          icon={<CheckCircleOutlineIcon sx={{ fontSize: 16 }} />}
          accent="#059669" bg="#f0fdf4" loading={loading}
        />
        <StatCard
          title="Errors"
          value={stats?.errors ?? 0}
          subtitle="Require attention"
          icon={<ErrorOutlineIcon sx={{ fontSize: 16 }} />}
          accent="#dc2626" bg="#fef2f2" loading={loading}
        />
        <StatCard
          title="Pending"
          value={stats?.pending ?? 0}
          subtitle="Awaiting API call"
          icon={<HourglassTopIcon sx={{ fontSize: 16 }} />}
          accent="#d97706" bg="#fffbeb" loading={loading}
        />
        <StatCard
          title="Avg API Time"
          value={stats?.avg_api_response_ms != null ? `${stats.avg_api_response_ms}ms` : '—'}
          subtitle="Per document"
          icon={<SpeedOutlinedIcon sx={{ fontSize: 16 }} />}
          accent="#7c3aed" bg="#f5f3ff" loading={loading}
        />
      </Box>

      {stats?.last_poll_time && (
        <Typography sx={{ fontSize: '0.72rem', color: '#94a3b8', mt: 1.5 }}>
          Last successful poll: {new Date(stats.last_poll_time).toLocaleString()}
        </Typography>
      )}
    </Box>
  )
}
