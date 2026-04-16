import { useState } from 'react'
import {
  Box, Typography, Button, Dialog, DialogTitle, DialogContent, DialogActions,
  TextField, IconButton, Chip, Tooltip, Alert, CircularProgress,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Avatar,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteIcon from '@mui/icons-material/Delete'
import KeyIcon from '@mui/icons-material/Key'
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutlined'
import DoNotDisturbAltIcon from '@mui/icons-material/DoNotDisturbAlt'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import apiClient from '../api/client'

interface User {
  id: string
  username: string
  is_active: boolean
  created_at: string
}

export default function UsersPage() {
  const qc = useQueryClient()
  const [addOpen, setAddOpen] = useState(false)
  const [pwdUser, setPwdUser] = useState<User | null>(null)
  const [deleteUser, setDeleteUser] = useState<User | null>(null)
  const [newUsername, setNewUsername] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [newPwd, setNewPwd] = useState('')
  const [error, setError] = useState('')

  const { data: users = [], isLoading } = useQuery<User[]>({
    queryKey: ['users'],
    queryFn: () => apiClient.get('/api/users').then((r) => r.data),
  })

  const createMutation = useMutation({
    mutationFn: (data: { username: string; password: string }) => apiClient.post('/api/users', data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['users'] })
      setAddOpen(false); setNewUsername(''); setNewPassword(''); setError('')
    },
    onError: (e: any) => setError(e.response?.data?.detail || 'Failed to create user.'),
  })

  const toggleActiveMutation = useMutation({
    mutationFn: ({ id, is_active }: { id: string; is_active: boolean }) =>
      apiClient.put(`/api/users/${id}`, { is_active }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['users'] }),
    onError: (e: any) => alert(e.response?.data?.detail || 'Failed to update user.'),
  })

  const changePwdMutation = useMutation({
    mutationFn: ({ id, new_password }: { id: string; new_password: string }) =>
      apiClient.put(`/api/users/${id}/password`, { new_password }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['users'] })
      setPwdUser(null); setNewPwd(''); setError('')
    },
    onError: (e: any) => setError(e.response?.data?.detail || 'Failed to change password.'),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => apiClient.delete(`/api/users/${id}`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['users'] }); setDeleteUser(null) },
    onError: (e: any) => alert(e.response?.data?.detail || 'Failed to delete user.'),
  })

  const initials = (name: string) => name.slice(0, 2).toUpperCase()
  const avatarColor = (name: string) => {
    const colors = ['#2563eb', '#7c3aed', '#059669', '#0891b2', '#d97706']
    return colors[name.charCodeAt(0) % colors.length]
  }

  return (
    <Box sx={{ px: 2 }}>
      <Box
        sx={{
          bgcolor: 'white',
          border: '1px solid #e2e8f0',
          borderRadius: '12px',
          overflow: 'hidden',
        }}
      >
        {/* Table header bar */}
        <Box sx={{
          px: 2.5, py: 2,
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          borderBottom: '1px solid #f1f5f9',
        }}>
          <Box>
            <Typography sx={{ fontWeight: 700, fontSize: '0.95rem', color: '#0f172a' }}>
              Users
            </Typography>
            <Typography sx={{ fontSize: '0.75rem', color: '#94a3b8', mt: 0.2 }}>
              {users.length} account{users.length !== 1 ? 's' : ''}
            </Typography>
          </Box>
          <Button
            variant="contained"
            size="small"
            startIcon={<AddIcon fontSize="small" />}
            onClick={() => { setAddOpen(true); setError('') }}
            sx={{ height: 34 }}
          >
            Add User
          </Button>
        </Box>

        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>User</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Created</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={4} align="center" sx={{ py: 5 }}>
                    <CircularProgress size={24} />
                  </TableCell>
                </TableRow>
              ) : users.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={4} align="center" sx={{ py: 5, color: 'text.secondary' }}>
                    No users yet. Add one to get started.
                  </TableCell>
                </TableRow>
              ) : users.map((user) => (
                <TableRow key={user.id}>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                      <Avatar
                        sx={{ width: 32, height: 32, fontSize: 12, fontWeight: 700,
                          bgcolor: avatarColor(user.username) }}
                      >
                        {initials(user.username)}
                      </Avatar>
                      <Box>
                        <Typography sx={{ fontWeight: 600, fontSize: '0.875rem', color: '#0f172a' }}>
                          {user.username}
                        </Typography>
                        <Typography sx={{ fontSize: '0.72rem', color: '#94a3b8', fontFamily: 'monospace' }}>
                          {user.id.slice(0, 8)}…
                        </Typography>
                      </Box>
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={user.is_active ? 'Active' : 'Inactive'}
                      size="small"
                      sx={{
                        fontSize: '0.72rem', fontWeight: 600, height: 22,
                        bgcolor: user.is_active ? '#f0fdf4' : '#f8fafc',
                        color: user.is_active ? '#059669' : '#94a3b8',
                        border: '1px solid',
                        borderColor: user.is_active ? '#bbf7d0' : '#e2e8f0',
                      }}
                    />
                  </TableCell>
                  <TableCell sx={{ color: '#64748b', fontSize: '0.8rem' }}>
                    {new Date(user.created_at).toLocaleDateString('en-GB', {
                      day: '2-digit', month: 'short', year: 'numeric',
                    })}
                  </TableCell>
                  <TableCell align="right">
                    <Box sx={{ display: 'flex', gap: 0.5, justifyContent: 'flex-end' }}>
                      <Tooltip title={user.is_active ? 'Deactivate' : 'Activate'}>
                        <IconButton
                          size="small"
                          onClick={() => toggleActiveMutation.mutate({ id: user.id, is_active: !user.is_active })}
                          sx={{
                            borderRadius: '8px', width: 30, height: 30,
                            color: user.is_active ? '#059669' : '#94a3b8',
                            '&:hover': { bgcolor: user.is_active ? '#f0fdf4' : '#f8fafc' },
                          }}
                        >
                          {user.is_active
                            ? <CheckCircleOutlineIcon sx={{ fontSize: 16 }} />
                            : <DoNotDisturbAltIcon sx={{ fontSize: 16 }} />
                          }
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Change Password">
                        <IconButton
                          size="small"
                          onClick={() => { setPwdUser(user); setError('') }}
                          sx={{ borderRadius: '8px', width: 30, height: 30, color: '#64748b',
                            '&:hover': { bgcolor: '#f1f5f9', color: 'primary.main' } }}
                        >
                          <KeyIcon sx={{ fontSize: 16 }} />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Delete">
                        <IconButton
                          size="small"
                          onClick={() => setDeleteUser(user)}
                          sx={{ borderRadius: '8px', width: 30, height: 30, color: '#94a3b8',
                            '&:hover': { bgcolor: '#fef2f2', color: 'error.main' } }}
                        >
                          <DeleteIcon sx={{ fontSize: 16 }} />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Box>

      {/* Add User Dialog */}
      <Dialog open={addOpen} onClose={() => setAddOpen(false)} maxWidth="xs" fullWidth>
        <DialogTitle>Add New User</DialogTitle>
        <DialogContent sx={{ pt: '12px !important' }}>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, pt: 0.5 }}>
            <TextField label="Username" fullWidth value={newUsername}
              onChange={(e) => setNewUsername(e.target.value)} autoFocus />
            <TextField label="Password" type="password" fullWidth value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              helperText="Minimum 6 characters" />
          </Box>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2.5, gap: 1 }}>
          <Button onClick={() => setAddOpen(false)} variant="outlined" size="small">Cancel</Button>
          <Button
            variant="contained" size="small"
            disabled={createMutation.isPending || !newUsername || !newPassword}
            onClick={() => createMutation.mutate({ username: newUsername, password: newPassword })}
          >
            {createMutation.isPending ? 'Creating…' : 'Create User'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Change Password Dialog */}
      <Dialog open={!!pwdUser} onClose={() => setPwdUser(null)} maxWidth="xs" fullWidth>
        <DialogTitle>Change Password</DialogTitle>
        <DialogContent sx={{ pt: '12px !important' }}>
          <Typography sx={{ fontSize: '0.85rem', color: 'text.secondary', mb: 2 }}>
            Updating password for <strong>{pwdUser?.username}</strong>
          </Typography>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          <TextField label="New Password" type="password" fullWidth value={newPwd}
            onChange={(e) => setNewPwd(e.target.value)} autoFocus
            helperText="Minimum 6 characters" />
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2.5, gap: 1 }}>
          <Button onClick={() => setPwdUser(null)} variant="outlined" size="small">Cancel</Button>
          <Button
            variant="contained" size="small"
            disabled={changePwdMutation.isPending || !newPwd}
            onClick={() => pwdUser && changePwdMutation.mutate({ id: pwdUser.id, new_password: newPwd })}
          >
            {changePwdMutation.isPending ? 'Saving…' : 'Save Password'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete Confirmation */}
      <Dialog open={!!deleteUser} onClose={() => setDeleteUser(null)} maxWidth="xs" fullWidth>
        <DialogTitle>Delete User</DialogTitle>
        <DialogContent sx={{ pt: '8px !important' }}>
          <Typography sx={{ fontSize: '0.875rem', color: 'text.secondary' }}>
            Are you sure you want to delete{' '}
            <Box component="span" sx={{ fontWeight: 700, color: 'text.primary' }}>
              {deleteUser?.username}
            </Box>
            ? This cannot be undone.
          </Typography>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2.5, gap: 1 }}>
          <Button onClick={() => setDeleteUser(null)} variant="outlined" size="small">Cancel</Button>
          <Button
            variant="contained" color="error" size="small"
            disabled={deleteMutation.isPending}
            onClick={() => deleteUser && deleteMutation.mutate(deleteUser.id)}
          >
            {deleteMutation.isPending ? 'Deleting…' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
