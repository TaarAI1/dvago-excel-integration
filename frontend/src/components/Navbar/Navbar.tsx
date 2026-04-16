import { useLocation, useNavigate } from 'react-router-dom'
import {
  AppBar, Toolbar, Typography, Box, Button, Divider,
  IconButton, Tooltip, Avatar, Menu, MenuItem,
} from '@mui/material'
import { useState } from 'react'
import DashboardOutlinedIcon from '@mui/icons-material/DashboardOutlined'
import PeopleOutlinedIcon from '@mui/icons-material/PeopleOutlined'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import LogoutIcon from '@mui/icons-material/Logout'
import StorageIcon from '@mui/icons-material/Storage'

const NAV_LINKS = [
  { label: 'Dashboard', path: '/', icon: <DashboardOutlinedIcon sx={{ fontSize: 16 }} /> },
  { label: 'Users', path: '/users', icon: <PeopleOutlinedIcon sx={{ fontSize: 16 }} /> },
  { label: 'Configuration', path: '/settings', icon: <SettingsOutlinedIcon sx={{ fontSize: 16 }} /> },
]

export default function Navbar() {
  const location = useLocation()
  const navigate = useNavigate()
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null)

  const handleLogout = () => {
    localStorage.removeItem('access_token')
    window.location.href = '/'
  }

  return (
    <AppBar
      position="sticky"
      elevation={0}
      sx={{
        bgcolor: '#ffffff',
        color: 'text.primary',
        borderBottom: '1px solid #e5e7eb',
        zIndex: 100,
      }}
    >
      <Toolbar
        sx={{
          minHeight: '52px !important',
          px: { xs: 2, sm: 3 },
          gap: 0,
        }}
      >
        {/* Brand */}
        <Box
          sx={{
            display: 'flex', alignItems: 'center', gap: 1,
            mr: 4, cursor: 'pointer', flexShrink: 0,
          }}
          onClick={() => navigate('/')}
        >
          <Box
            sx={{
              width: 28, height: 28, borderRadius: '6px',
              bgcolor: '#1a56db',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}
          >
            <StorageIcon sx={{ color: 'white', fontSize: 15 }} />
          </Box>
          <Box>
            <Typography
              sx={{ fontWeight: 700, fontSize: '0.875rem', lineHeight: 1.1, color: '#111827' }}
            >
              RetailPro CSV
            </Typography>
            <Typography sx={{ fontSize: '0.65rem', color: '#9ca3af', lineHeight: 1 }}>
              Data Exchange
            </Typography>
          </Box>
        </Box>

        <Divider orientation="vertical" flexItem sx={{ mr: 3, borderColor: '#f3f4f6' }} />

        {/* Nav links */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, flexGrow: 1 }}>
          {NAV_LINKS.map((link) => {
            const active = location.pathname === link.path
            return (
              <Button
                key={link.path}
                startIcon={link.icon}
                onClick={() => navigate(link.path)}
                disableRipple
                sx={{
                  px: 1.5,
                  py: 0.75,
                  fontSize: '0.8rem',
                  fontWeight: active ? 600 : 400,
                  color: active ? '#1a56db' : '#6b7280',
                  bgcolor: active ? '#eff6ff' : 'transparent',
                  borderRadius: '6px',
                  minWidth: 0,
                  '&:hover': {
                    bgcolor: active ? '#eff6ff' : '#f9fafb',
                    color: active ? '#1a56db' : '#374151',
                  },
                }}
              >
                {link.label}
              </Button>
            )
          })}
        </Box>

        {/* Right side */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Tooltip title="Logout">
            <IconButton
              size="small"
              onClick={handleLogout}
              sx={{
                color: '#9ca3af',
                borderRadius: '6px',
                '&:hover': { color: '#b91c1c', bgcolor: '#fef2f2' },
              }}
            >
              <LogoutIcon sx={{ fontSize: 17 }} />
            </IconButton>
          </Tooltip>
          <Avatar
            onClick={(e) => setAnchorEl(e.currentTarget)}
            sx={{
              width: 28, height: 28, fontSize: 11, fontWeight: 700,
              bgcolor: '#1a56db', cursor: 'pointer',
              '&:hover': { opacity: 0.85 },
            }}
          >
            A
          </Avatar>
          <Menu
            anchorEl={anchorEl}
            open={Boolean(anchorEl)}
            onClose={() => setAnchorEl(null)}
            transformOrigin={{ horizontal: 'right', vertical: 'top' }}
            anchorOrigin={{ horizontal: 'right', vertical: 'bottom' }}
            PaperProps={{
              sx: { mt: 0.5, minWidth: 140, border: '1px solid #e5e7eb', boxShadow: '0 4px 16px rgba(0,0,0,0.08)' },
            }}
          >
            <MenuItem dense onClick={handleLogout} sx={{ fontSize: '0.8rem', color: '#b91c1c', gap: 1 }}>
              <LogoutIcon sx={{ fontSize: 15 }} /> Sign out
            </MenuItem>
          </Menu>
        </Box>
      </Toolbar>
    </AppBar>
  )
}
