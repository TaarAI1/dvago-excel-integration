import { useLocation, useNavigate } from 'react-router-dom'
import {
  Drawer, List, ListItemButton, ListItemIcon, ListItemText,
  Typography, Box, Divider, IconButton,
} from '@mui/material'
import DashboardIcon from '@mui/icons-material/Dashboard'
import PeopleIcon from '@mui/icons-material/People'
import SettingsIcon from '@mui/icons-material/Settings'
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft'
import IntegrationInstructionsIcon from '@mui/icons-material/IntegrationInstructions'

export const SIDEBAR_WIDTH = 220

const NAV_ITEMS = [
  { label: 'Dashboard', path: '/', icon: <DashboardIcon fontSize="small" /> },
  { label: 'User Management', path: '/users', icon: <PeopleIcon fontSize="small" /> },
  { label: 'Configuration', path: '/settings', icon: <SettingsIcon fontSize="small" /> },
]

interface SidebarProps {
  open: boolean
  onClose: () => void
}

export default function Sidebar({ open, onClose }: SidebarProps) {
  const location = useLocation()
  const navigate = useNavigate()

  return (
    <Drawer
      variant="persistent"
      open={open}
      sx={{
        width: open ? SIDEBAR_WIDTH : 0,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: SIDEBAR_WIDTH,
          boxSizing: 'border-box',
          bgcolor: '#0f172a',
          color: 'white',
          border: 'none',
          position: 'fixed',   // taken out of flex flow so main content ml is the only offset
          height: '100vh',
          overflowX: 'hidden',
          boxShadow: '4px 0 16px 0 rgb(0 0 0 / 0.15)',
        },
      }}
    >
      {/* Brand */}
      <Box sx={{ px: 2.5, pt: 2.5, pb: 2, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.2 }}>
          <Box
            sx={{
              width: 32, height: 32, borderRadius: '8px',
              background: 'linear-gradient(135deg, #2563eb 0%, #7c3aed 100%)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              flexShrink: 0,
            }}
          >
            <IntegrationInstructionsIcon sx={{ fontSize: 18, color: 'white' }} />
          </Box>
          <Box>
            <Typography sx={{ fontWeight: 700, fontSize: 12.5, lineHeight: 1.2, color: '#f8fafc' }}>
              RetailPro CSV
            </Typography>
            <Typography sx={{ fontSize: 10, color: '#94a3b8', lineHeight: 1.3 }}>
              Data Exchange
            </Typography>
          </Box>
        </Box>
        <IconButton
          onClick={onClose}
          size="small"
          sx={{ color: '#475569', '&:hover': { color: '#94a3b8', bgcolor: 'rgba(255,255,255,0.05)' }, borderRadius: '6px' }}
        >
          <ChevronLeftIcon fontSize="small" />
        </IconButton>
      </Box>

      <Divider sx={{ borderColor: 'rgba(255,255,255,0.07)', mx: 2 }} />

      <Typography
        sx={{ px: 2.5, pt: 2.5, pb: 0.75, fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
          letterSpacing: '0.08em', color: '#475569' }}
      >
        Navigation
      </Typography>

      <List sx={{ px: 1.5, pt: 0.5 }}>
        {NAV_ITEMS.map((item) => {
          const active = location.pathname === item.path
          return (
            <ListItemButton
              key={item.path}
              onClick={() => navigate(item.path)}
              sx={{
                borderRadius: '8px',
                mb: 0.5,
                px: 1.5,
                py: 1,
                minHeight: 40,
                bgcolor: active ? 'rgba(37, 99, 235, 0.18)' : 'transparent',
                '&:hover': { bgcolor: active ? 'rgba(37, 99, 235, 0.22)' : 'rgba(255,255,255,0.05)' },
                transition: 'background 0.15s',
              }}
            >
              <ListItemIcon
                sx={{
                  minWidth: 30,
                  color: active ? '#60a5fa' : '#64748b',
                  transition: 'color 0.15s',
                }}
              >
                {item.icon}
              </ListItemIcon>
              <ListItemText
                primary={item.label}
                slotProps={{
                  primary: {
                    sx: {
                      fontSize: 13.5,
                      fontWeight: active ? 600 : 400,
                      color: active ? '#e2e8f0' : '#94a3b8',
                      transition: 'color 0.15s',
                    },
                  },
                }}
              />
              {active && (
                <Box sx={{ width: 3, height: 20, borderRadius: 4, bgcolor: '#3b82f6', ml: 0.5 }} />
              )}
            </ListItemButton>
          )
        })}
      </List>

      <Box sx={{ flexGrow: 1 }} />

      <Box sx={{ px: 2, pb: 2.5 }}>
        <Divider sx={{ borderColor: 'rgba(255,255,255,0.07)', mb: 2 }} />
        <Typography sx={{ fontSize: 10, color: '#334155', textAlign: 'center' }}>
          v3.0 · RetailPro Prism
        </Typography>
      </Box>
    </Drawer>
  )
}
