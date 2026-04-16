import { useState } from 'react'
import { Box, TextField, Button, Typography, Alert, CircularProgress, InputAdornment, IconButton } from '@mui/material'
import PersonOutlineIcon from '@mui/icons-material/PersonOutline'
import LockOutlinedIcon from '@mui/icons-material/LockOutlined'
import VisibilityIcon from '@mui/icons-material/Visibility'
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff'
import IntegrationInstructionsIcon from '@mui/icons-material/IntegrationInstructions'
import axios from 'axios'

export default function Login({ onLogin }: { onLogin: () => void }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [showPwd, setShowPwd] = useState(false)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    setError('')
    try {
      const form = new URLSearchParams()
      form.append('username', username)
      form.append('password', password)
      const res = await axios.post('/api/auth/login', form, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      })
      localStorage.setItem('access_token', res.data.access_token)
      onLogin()
    } catch {
      setError('Invalid username or password.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <Box
      sx={{
        minHeight: '100vh',
        display: 'flex',
        bgcolor: '#f1f5f9',
      }}
    >
      {/* Left panel */}
      <Box
        sx={{
          display: { xs: 'none', md: 'flex' },
          flexDirection: 'column',
          justifyContent: 'center',
          alignItems: 'flex-start',
          width: '44%',
          background: 'linear-gradient(145deg, #0f172a 0%, #1e3a8a 60%, #312e81 100%)',
          px: 8,
          py: 6,
          position: 'relative',
          overflow: 'hidden',
        }}
      >
        {/* Decorative circles */}
        <Box sx={{
          position: 'absolute', top: -80, right: -80,
          width: 320, height: 320, borderRadius: '50%',
          background: 'rgba(37,99,235,0.15)',
        }} />
        <Box sx={{
          position: 'absolute', bottom: -60, left: -60,
          width: 240, height: 240, borderRadius: '50%',
          background: 'rgba(124,58,237,0.12)',
        }} />

        <Box sx={{ position: 'relative', zIndex: 1 }}>
          <Box sx={{
            width: 52, height: 52, borderRadius: 3,
            background: 'linear-gradient(135deg, #2563eb, #7c3aed)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            mb: 3,
          }}>
            <IntegrationInstructionsIcon sx={{ color: 'white', fontSize: 28 }} />
          </Box>

          <Typography sx={{ color: '#f8fafc', fontWeight: 800, fontSize: '1.9rem', lineHeight: 1.15, mb: 1.5 }}>
            RetailPro<br />Integration
          </Typography>
          <Typography sx={{ color: '#94a3b8', fontSize: '0.95rem', lineHeight: 1.6, maxWidth: 300 }}>
            Automated FTP-to-RetailPro document sync with real-time monitoring and full audit trail.
          </Typography>

          <Box sx={{ mt: 5, display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            {['FTP polling & CSV processing', 'Real-time dashboard & logs', 'Oracle DB integration'].map((f) => (
              <Box key={f} sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                <Box sx={{ width: 6, height: 6, borderRadius: '50%', bgcolor: '#3b82f6', flexShrink: 0 }} />
                <Typography sx={{ color: '#cbd5e1', fontSize: '0.875rem' }}>{f}</Typography>
              </Box>
            ))}
          </Box>
        </Box>
      </Box>

      {/* Right panel — form */}
      <Box
        sx={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          px: { xs: 3, sm: 6, md: 8 },
        }}
      >
        <Box sx={{ width: '100%', maxWidth: 380 }}>
          <Typography variant="h5" sx={{ mb: 0.75 }}>Welcome back</Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3.5 }}>
            Sign in to access your dashboard
          </Typography>

          {error && (
            <Alert severity="error" sx={{ mb: 2.5 }} onClose={() => setError('')}>
              {error}
            </Alert>
          )}

          <form onSubmit={handleSubmit}>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              <TextField
                fullWidth
                label="Username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                autoFocus
                required
                slotProps={{
                  input: {
                    startAdornment: (
                      <InputAdornment position="start">
                        <PersonOutlineIcon fontSize="small" sx={{ color: 'text.secondary' }} />
                      </InputAdornment>
                    ),
                  },
                }}
              />
              <TextField
                fullWidth
                label="Password"
                type={showPwd ? 'text' : 'password'}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                slotProps={{
                  input: {
                    startAdornment: (
                      <InputAdornment position="start">
                        <LockOutlinedIcon fontSize="small" sx={{ color: 'text.secondary' }} />
                      </InputAdornment>
                    ),
                    endAdornment: (
                      <InputAdornment position="end">
                        <IconButton size="small" onClick={() => setShowPwd(!showPwd)} edge="end">
                          {showPwd ? <VisibilityOffIcon fontSize="small" /> : <VisibilityIcon fontSize="small" />}
                        </IconButton>
                      </InputAdornment>
                    ),
                  },
                }}
              />
              <Button
                fullWidth
                type="submit"
                variant="contained"
                size="large"
                disabled={loading || !username || !password}
                sx={{
                  mt: 0.5,
                  py: 1.3,
                  fontSize: '0.95rem',
                  background: 'linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%)',
                  '&:hover': { background: 'linear-gradient(135deg, #1d4ed8 0%, #1e40af 100%)' },
                }}
                startIcon={loading ? <CircularProgress size={16} color="inherit" /> : null}
              >
                {loading ? 'Signing in…' : 'Sign In'}
              </Button>
            </Box>
          </form>
        </Box>
      </Box>
    </Box>
  )
}
