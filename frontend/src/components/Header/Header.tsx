import { useLocation } from 'react-router-dom'
import { AppBar, Toolbar, Typography, Box, Tooltip, IconButton, Avatar } from '@mui/material'
import MenuIcon from '@mui/icons-material/Menu'
import LogoutIcon from '@mui/icons-material/Logout'
import { SIDEBAR_WIDTH } from '../Layout/Sidebar'

const PAGE_TITLES: Record<string, { title: string; subtitle: string }> = {
  '/': { title: 'Dashboard', subtitle: 'Live sync status and document pipeline' },
  '/users': { title: 'User Management', subtitle: 'Manage dashboard access' },
  '/settings': { title: 'Configuration', subtitle: 'FTP, Oracle DB, RetailPro API settings' },
}

interface HeaderProps {
  sidebarOpen: boolean
  onSidebarToggle: () => void
}

export default function Header({ sidebarOpen, onSidebarToggle }: HeaderProps) {
  const location = useLocation()
  const page = PAGE_TITLES[location.pathname] ?? PAGE_TITLES['/']

  const handleLogout = () => {
    localStorage.removeItem('access_token')
    window.location.href = '/'
  }

  return (
    <AppBar
      position="fixed"
      elevation={0}
      sx={{
        bgcolor: '#ffffff',
        color: 'text.primary',
        borderBottom: '1px solid #e2e8f0',
        width: sidebarOpen ? `calc(100% - ${SIDEBAR_WIDTH}px)` : '100%',
        ml: sidebarOpen ? `${SIDEBAR_WIDTH}px` : 0,
        transition: (t) =>
          t.transitions.create(['width', 'margin'], {
            easing: sidebarOpen ? t.transitions.easing.easeOut : t.transitions.easing.sharp,
            duration: sidebarOpen ? t.transitions.duration.enteringScreen : t.transitions.duration.leavingScreen,
          }),
      }}
    >
      <Toolbar sx={{ minHeight: '56px !important', px: 2.5, gap: 1.5 }}>
        {!sidebarOpen && (
          <IconButton
            edge="start"
            onClick={onSidebarToggle}
            size="small"
            sx={{ mr: 0.5, color: 'text.secondary', borderRadius: '8px' }}
          >
            <MenuIcon fontSize="small" />
          </IconButton>
        )}

        <Box sx={{ flexGrow: 1 }}>
          <Typography
            variant="subtitle1"
            sx={{ fontWeight: 700, lineHeight: 1.2, color: 'text.primary', fontSize: '0.95rem' }}
          >
            {page.title}
          </Typography>
          <Typography variant="caption" sx={{ color: 'text.secondary', lineHeight: 1 }}>
            {page.subtitle}
          </Typography>
        </Box>

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <Tooltip title="Logout">
            <IconButton
              onClick={handleLogout}
              size="small"
              sx={{
                color: 'text.secondary',
                borderRadius: '8px',
                border: '1px solid #e2e8f0',
                p: 0.75,
                '&:hover': { bgcolor: '#f8fafc', color: 'error.main', borderColor: '#fecaca' },
              }}
            >
              <LogoutIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Avatar
            sx={{
              width: 30, height: 30,
              bgcolor: '#2563eb',
              fontSize: 12, fontWeight: 700,
              ml: 0.5,
            }}
          >
            A
          </Avatar>
        </Box>
      </Toolbar>
    </AppBar>
  )
}
