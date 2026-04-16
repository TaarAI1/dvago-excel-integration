import { useState } from 'react'
import { Box, TextField, Button, Typography, Alert, CircularProgress, InputAdornment, IconButton } from '@mui/material'
import StorageIcon from '@mui/icons-material/Storage'
import VisibilityIcon from '@mui/icons-material/Visibility'
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff'
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
        bgcolor: '#f9fafb',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        p: 3,
      }}
    >
      {/* Card */}
      <Box
        sx={{
          width: '100%',
          maxWidth: 380,
          bgcolor: 'white',
          border: '1px solid #e5e7eb',
          borderRadius: '8px',
          p: '32px 28px',
        }}
      >
        {/* Brand */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 3 }}>
          <Box
            sx={{
              width: 36, height: 36, borderRadius: '8px',
              bgcolor: '#1a56db',
              display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
            }}
          >
            <StorageIcon sx={{ color: 'white', fontSize: 18 }} />
          </Box>
          <Box>
            <Typography sx={{ fontWeight: 700, fontSize: '0.9rem', lineHeight: 1.2, color: '#111827' }}>
              RetailPro CSV Data Exchange
            </Typography>
            <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
              Sign in to your dashboard
            </Typography>
          </Box>
        </Box>

        <Box sx={{ height: '1px', bgcolor: '#f3f4f6', mb: 3 }} />

        {error && (
          <Alert severity="error" sx={{ mb: 2, py: 0.5 }} onClose={() => setError('')}>
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
                  endAdornment: (
                    <InputAdornment position="end">
                      <IconButton size="small" onClick={() => setShowPwd(!showPwd)} edge="end">
                        {showPwd
                          ? <VisibilityOffIcon sx={{ fontSize: 17 }} />
                          : <VisibilityIcon sx={{ fontSize: 17 }} />}
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
              disabled={loading || !username || !password}
              sx={{ py: 1, mt: 0.5, fontWeight: 600 }}
              startIcon={loading ? <CircularProgress size={14} color="inherit" /> : null}
            >
              {loading ? 'Signing in…' : 'Sign In'}
            </Button>
          </Box>
        </form>
      </Box>

      <Typography sx={{ mt: 3, fontSize: '0.72rem', color: '#d1d5db' }}>
        RetailPro CSV Data Exchange v3.0
      </Typography>
    </Box>
  )
}
