import { useState } from 'react'
import {
  Box, Typography, Button, Dialog, DialogTitle, DialogContent, DialogActions,
  TextField, IconButton, Chip, Tooltip, Alert, CircularProgress,
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline'
import KeyOutlinedIcon from '@mui/icons-material/KeyOutlined'
import CheckIcon from '@mui/icons-material/Check'
import BlockIcon from '@mui/icons-material/Block'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import apiClient from '../api/client'

interface User { id: string; username: string; is_active: boolean; created_at: string }

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
    mutationFn: (d: { username: string; password: string }) => apiClient.post('/api/users', d),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['users'] }); setAddOpen(false); setNewUsername(''); setNewPassword(''); setError('') },
    onError: (e: any) => setError(e.response?.data?.detail || 'Failed.'),
  })
  const toggleMutation = useMutation({
    mutationFn: ({ id, is_active }: { id: string; is_active: boolean }) => apiClient.put(`/api/users/${id}`, { is_active }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['users'] }),
  })
  const changePwdMutation = useMutation({
    mutationFn: ({ id, new_password }: { id: string; new_password: string }) => apiClient.put(`/api/users/${id}/password`, { new_password }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['users'] }); setPwdUser(null); setNewPwd(''); setError('') },
    onError: (e: any) => setError(e.response?.data?.detail || 'Failed.'),
  })
  const deleteMutation = useMutation({
    mutationFn: (id: string) => apiClient.delete(`/api/users/${id}`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['users'] }); setDeleteUser(null) },
  })

  return (
    <Box sx={{ p: { xs: 2, sm: 3 } }}>
      <Box sx={{ bgcolor: 'white', border: '1px solid #e5e7eb', borderRadius: '6px', overflow: 'hidden' }}>

        {/* Header */}
        <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid #f3f4f6',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Box>
            <Typography sx={{ fontWeight: 600, fontSize: '0.875rem', color: '#111827' }}>Users</Typography>
            <Typography sx={{ fontSize: '0.72rem', color: '#9ca3af' }}>
              {users.length} account{users.length !== 1 ? 's' : ''}
            </Typography>
          </Box>
          <Button variant="contained" size="small"
            startIcon={<AddIcon sx={{ fontSize: 15 }} />}
            onClick={() => { setAddOpen(true); setError('') }}
            sx={{ height: 32, fontSize: '0.8rem' }}>
            Add User
          </Button>
        </Box>

        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Username</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Created</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={4} align="center" sx={{ py: 5 }}>
                    <CircularProgress size={22} />
                  </TableCell>
                </TableRow>
              ) : users.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={4} align="center" sx={{ py: 5, color: '#9ca3af' }}>
                    No users found.
                  </TableCell>
                </TableRow>
              ) : users.map((u) => (
                <TableRow key={u.id}>
                  <TableCell>
                    <Typography sx={{ fontWeight: 500, fontSize: '0.875rem' }}>{u.username}</Typography>
                    <Typography sx={{ fontSize: '0.7rem', color: '#9ca3af', fontFamily: 'monospace' }}>
                      {u.id.slice(0, 8)}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Chip label={u.is_active ? 'Active' : 'Inactive'} size="small"
                      sx={{
                        height: 22, fontSize: '0.72rem', fontWeight: 500, borderRadius: '4px',
                        bgcolor: u.is_active ? '#f0fdf4' : '#f9fafb',
                        color: u.is_active ? '#15803d' : '#9ca3af',
                        border: '1px solid', borderColor: u.is_active ? '#d1fae5' : '#e5e7eb',
                      }} />
                  </TableCell>
                  <TableCell sx={{ color: '#6b7280' }}>
                    {new Date(u.created_at).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })}
                  </TableCell>
                  <TableCell align="right">
                    <Box sx={{ display: 'flex', gap: 0.5, justifyContent: 'flex-end' }}>
                      <Tooltip title={u.is_active ? 'Deactivate' : 'Activate'}>
                        <IconButton size="small"
                          onClick={() => toggleMutation.mutate({ id: u.id, is_active: !u.is_active })}
                          sx={{ borderRadius: '4px', width: 28, height: 28,
                            color: u.is_active ? '#15803d' : '#9ca3af',
                            '&:hover': { bgcolor: '#f9fafb' } }}>
                          {u.is_active ? <CheckIcon sx={{ fontSize: 14 }} /> : <BlockIcon sx={{ fontSize: 14 }} />}
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Change Password">
                        <IconButton size="small"
                          onClick={() => { setPwdUser(u); setError('') }}
                          sx={{ borderRadius: '4px', width: 28, height: 28, color: '#6b7280',
                            '&:hover': { bgcolor: '#f9fafb', color: '#1a56db' } }}>
                          <KeyOutlinedIcon sx={{ fontSize: 14 }} />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Delete">
                        <IconButton size="small" onClick={() => setDeleteUser(u)}
                          sx={{ borderRadius: '4px', width: 28, height: 28, color: '#9ca3af',
                            '&:hover': { bgcolor: '#fef2f2', color: '#b91c1c' } }}>
                          <DeleteOutlineIcon sx={{ fontSize: 14 }} />
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

      {/* Add */}
      <Dialog open={addOpen} onClose={() => setAddOpen(false)} maxWidth="xs" fullWidth>
        <DialogTitle>Add User</DialogTitle>
        <DialogContent sx={{ pt: '8px !important' }}>
          {error && <Alert severity="error" sx={{ mb: 1.5, py: 0.5 }}>{error}</Alert>}
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            <TextField label="Username" fullWidth value={newUsername}
              onChange={(e) => setNewUsername(e.target.value)} autoFocus />
            <TextField label="Password" type="password" fullWidth value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)} helperText="Min 6 chars" />
          </Box>
        </DialogContent>
        <DialogActions sx={{ px: 2.5, pb: 2 }}>
          <Button onClick={() => setAddOpen(false)} size="small" variant="outlined">Cancel</Button>
          <Button variant="contained" size="small"
            disabled={createMutation.isPending || !newUsername || !newPassword}
            onClick={() => createMutation.mutate({ username: newUsername, password: newPassword })}>
            {createMutation.isPending ? 'Creating…' : 'Create'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Change password */}
      <Dialog open={!!pwdUser} onClose={() => setPwdUser(null)} maxWidth="xs" fullWidth>
        <DialogTitle>Change Password</DialogTitle>
        <DialogContent sx={{ pt: '8px !important' }}>
          <Typography sx={{ fontSize: '0.8rem', color: '#6b7280', mb: 1.5 }}>
            For <strong>{pwdUser?.username}</strong>
          </Typography>
          {error && <Alert severity="error" sx={{ mb: 1.5, py: 0.5 }}>{error}</Alert>}
          <TextField label="New Password" type="password" fullWidth value={newPwd}
            onChange={(e) => setNewPwd(e.target.value)} autoFocus helperText="Min 6 chars" />
        </DialogContent>
        <DialogActions sx={{ px: 2.5, pb: 2 }}>
          <Button onClick={() => setPwdUser(null)} size="small" variant="outlined">Cancel</Button>
          <Button variant="contained" size="small"
            disabled={changePwdMutation.isPending || !newPwd}
            onClick={() => pwdUser && changePwdMutation.mutate({ id: pwdUser.id, new_password: newPwd })}>
            {changePwdMutation.isPending ? 'Saving…' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete */}
      <Dialog open={!!deleteUser} onClose={() => setDeleteUser(null)} maxWidth="xs" fullWidth>
        <DialogTitle>Delete User</DialogTitle>
        <DialogContent sx={{ pt: '8px !important' }}>
          <Typography sx={{ fontSize: '0.875rem', color: '#374151' }}>
            Delete <strong>{deleteUser?.username}</strong>? This cannot be undone.
          </Typography>
        </DialogContent>
        <DialogActions sx={{ px: 2.5, pb: 2 }}>
          <Button onClick={() => setDeleteUser(null)} size="small" variant="outlined">Cancel</Button>
          <Button variant="contained" color="error" size="small"
            disabled={deleteMutation.isPending}
            onClick={() => deleteUser && deleteMutation.mutate(deleteUser.id)}>
            {deleteMutation.isPending ? 'Deleting…' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
