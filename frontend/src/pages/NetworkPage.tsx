import { useState } from 'react'
import {
  Box, Typography, Button, CircularProgress,
} from '@mui/material'
import RouterOutlinedIcon from '@mui/icons-material/RouterOutlined'
import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import CheckIcon from '@mui/icons-material/Check'
import RefreshIcon from '@mui/icons-material/Refresh'
import ErrorOutlinedIcon from '@mui/icons-material/ErrorOutlined'
import { useQuery } from '@tanstack/react-query'
import apiClient from '../api/client'

export default function NetworkPage() {
  const [copied, setCopied] = useState(false)

  const { data, isLoading, isError, error, refetch, isFetching } = useQuery<{ ip: string | null; error?: string }>({
    queryKey: ['egress-ip'],
    queryFn: () => apiClient.get('/api/network/egress-ip').then((r) => r.data),
    staleTime: 60_000,
    retry: 2,
  })

  const ip = data?.ip ?? null
  const fetchError = isError
    ? ((error as any)?.response?.data?.detail ?? (error as any)?.message ?? 'Request failed')
    : data?.error ?? null

  const handleCopy = () => {
    if (!ip) return
    navigator.clipboard.writeText(ip).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  return (
    <Box sx={{ p: { xs: 2, sm: 3 } }}>
      {/* Page header */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h6" sx={{ color: '#111827', mb: 0.5 }}>
          Egress IP Address
        </Typography>
        <Typography sx={{ fontSize: '0.85rem', color: '#6b7280' }}>
          The public IP address that outbound connections from this server originate from.
          Use this to whitelist access on your SFTP / FTP server.
        </Typography>
      </Box>

      {/* Card */}
      <Box
        sx={{
          bgcolor: 'white',
          border: '1px solid #e5e7eb',
          borderRadius: '6px',
          maxWidth: 480,
          overflow: 'hidden',
        }}
      >
        {/* Card header */}
        <Box
          sx={{
            px: 2.5, py: 1.5,
            borderBottom: '1px solid #f3f4f6',
            bgcolor: '#fafafa',
            display: 'flex', alignItems: 'center', gap: 1,
          }}
        >
          <RouterOutlinedIcon sx={{ fontSize: 16, color: '#6b7280' }} />
          <Typography sx={{ fontSize: '0.8rem', fontWeight: 600, color: '#374151' }}>
            Server outbound IP
          </Typography>
        </Box>

        {/* Card body */}
        <Box sx={{ px: 2.5, py: 2.5 }}>
          {isLoading ? (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
              <CircularProgress size={18} />
              <Typography sx={{ fontSize: '0.875rem', color: '#6b7280' }}>
                Detecting IP address…
              </Typography>
            </Box>
          ) : fetchError ? (
            <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1 }}>
              <ErrorOutlinedIcon sx={{ fontSize: 17, color: '#b91c1c', mt: '2px', flexShrink: 0 }} />
              <Typography sx={{ fontSize: '0.875rem', color: '#7f1d1d', wordBreak: 'break-word' }}>
                {fetchError}
              </Typography>
            </Box>
          ) : (
            <>
              {/* IP display */}
              <Box
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: 1.5,
                  bgcolor: '#f0f9ff',
                  border: '1px solid #bae6fd',
                  borderRadius: '6px',
                  px: 2, py: 1.25,
                  mb: 2,
                }}
              >
                <Typography
                  sx={{
                    fontFamily: 'monospace',
                    fontSize: '1.35rem',
                    fontWeight: 700,
                    color: '#0c4a6e',
                    letterSpacing: '0.02em',
                    lineHeight: 1,
                  }}
                >
                  {ip ?? '—'}
                </Typography>

                {ip && (
                  <Button
                    size="small"
                    variant="outlined"
                    onClick={handleCopy}
                    startIcon={
                      copied
                        ? <CheckIcon sx={{ fontSize: 14 }} />
                        : <ContentCopyIcon sx={{ fontSize: 14 }} />
                    }
                    sx={{
                      height: 28,
                      fontSize: '0.75rem',
                      borderColor: copied ? '#16a34a' : '#7dd3fc',
                      color: copied ? '#16a34a' : '#0369a1',
                      bgcolor: copied ? '#f0fdf4' : 'white',
                      '&:hover': {
                        borderColor: copied ? '#16a34a' : '#38bdf8',
                        bgcolor: copied ? '#f0fdf4' : '#f0f9ff',
                      },
                      minWidth: 0,
                    }}
                  >
                    {copied ? 'Copied' : 'Copy'}
                  </Button>
                )}
              </Box>

              <Typography sx={{ fontSize: '0.78rem', color: '#6b7280', lineHeight: 1.6 }}>
                Add <strong>{ip}</strong> to your SFTP server's IP allowlist / firewall rules
                so this application can connect.
              </Typography>
            </>
          )}
        </Box>

        {/* Card footer */}
        <Box
          sx={{
            px: 2.5, py: 1.25,
            borderTop: '1px solid #f3f4f6',
            bgcolor: '#fafafa',
            display: 'flex', alignItems: 'center', justifyContent: 'flex-end',
          }}
        >
          <Button
            size="small"
            variant="outlined"
            startIcon={isFetching ? <CircularProgress size={12} /> : <RefreshIcon sx={{ fontSize: 14 }} />}
            onClick={() => refetch()}
            disabled={isFetching}
            sx={{ height: 28, fontSize: '0.75rem' }}
          >
            Refresh
          </Button>
        </Box>
      </Box>
    </Box>
  )
}
