import { AppBar, Toolbar, Typography, Box, Tooltip, IconButton } from '@mui/material'
import LogoutIcon from '@mui/icons-material/Logout'
import { SIDEBAR_WIDTH } from '../Layout/Sidebar'

export default function Header() {
  const handleLogout = () => {
    localStorage.removeItem('access_token')
    window.location.href = '/'
  }

  return (
    <AppBar
      position="fixed"
      elevation={1}
      sx={{
        width: `calc(100% - ${SIDEBAR_WIDTH}px)`,
        ml: `${SIDEBAR_WIDTH}px`,
        bgcolor: 'white',
        color: 'text.primary',
      }}
    >
      <Toolbar sx={{ minHeight: 56 }}>
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
