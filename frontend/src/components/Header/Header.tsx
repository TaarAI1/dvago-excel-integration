import { AppBar, Toolbar, Typography, Box, Tooltip, IconButton } from '@mui/material'
import MenuIcon from '@mui/icons-material/Menu'
import LogoutIcon from '@mui/icons-material/Logout'
import { SIDEBAR_WIDTH } from '../Layout/Sidebar'

interface HeaderProps {
  sidebarOpen: boolean
  onSidebarToggle: () => void
}

export default function Header({ sidebarOpen, onSidebarToggle }: HeaderProps) {
  const handleLogout = () => {
    localStorage.removeItem('access_token')
    window.location.href = '/'
  }

  return (
    <AppBar
      position="fixed"
      elevation={1}
      sx={{
        bgcolor: 'white',
        color: 'text.primary',
        width: sidebarOpen ? `calc(100% - ${SIDEBAR_WIDTH}px)` : '100%',
        ml: sidebarOpen ? `${SIDEBAR_WIDTH}px` : 0,
        transition: (theme) =>
          theme.transitions.create(['width', 'margin'], {
            easing: sidebarOpen
              ? theme.transitions.easing.easeOut
              : theme.transitions.easing.sharp,
            duration: sidebarOpen
              ? theme.transitions.duration.enteringScreen
              : theme.transitions.duration.leavingScreen,
          }),
      }}
    >
      <Toolbar sx={{ minHeight: 56 }}>
        {/* Hamburger — only shown when sidebar is closed */}
        {!sidebarOpen && (
          <IconButton
            edge="start"
            onClick={onSidebarToggle}
            size="small"
            color="inherit"
            sx={{ mr: 1 }}
          >
            <MenuIcon />
          </IconButton>
        )}

        <Typography variant="h6" sx={{ fontWeight: 600, flexGrow: 1, color: 'primary.dark' }}>
          RetailPro Integration Dashboard
        </Typography>

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Tooltip title="Logout">
            <IconButton onClick={handleLogout} size="small" color="inherit">
              <LogoutIcon />
            </IconButton>
          </Tooltip>
        </Box>
      </Toolbar>
    </AppBar>
  )
}
