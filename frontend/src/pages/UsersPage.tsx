import { useState } from 'react'
import {
  Box, Typography, Button, Dialog, DialogTitle, DialogContent, DialogActions,
  TextField, IconButton, Chip, Tooltip, Alert, CircularProgress,
  Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteIcon from '@mui/icons-material/Delete'
import KeyIcon from '@mui/icons-material/Key'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import CancelIcon from '@mui/icons-material/Cancel'
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
    mutationFn: (data: { username: string; password: string }) =>
      apiClient.post('/api/users', data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['users'] })
      setAddOpen(false)
      setNewUsername('')
      setNewPassword('')
      setError('')
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
      setPwdUser(null)
      setNewPwd('')
      setError('')
    },
    onError: (e: any) => setError(e.response?.data?.detail || 'Failed to change password.'),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => apiClient.delete(`/api/users/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['users'] })
      setDeleteUser(null)
    },
    onError: (e: any) => alert(e.response?.data?.detail || 'Failed to delete user.'),
  })

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h5" sx={{ fontWeight: 700 }}>User Management</Typography>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => { setAddOpen(true); setError('') }}>
          Add User
        </Button>
      </Box>

      <Paper elevation={1}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow sx={{ bgcolor: 'grey.50' }}>
                <TableCell sx={{ fontWeight: 600 }}>Username</TableCell>
                <TableCell sx={{ fontWeight: 600 }}>Status</TableCell>
                <TableCell sx={{ fontWeight: 600 }}>Created</TableCell>
                <TableCell sx={{ fontWeight: 600 }} align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {isLoading ? (
                <TableRow>
                  <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                    <CircularProgress size={28} />
                  </TableCell>
                </TableRow>
              ) : users.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                    No users found.
                  </TableCell>
                </TableRow>
              ) : users.map((user) => (
                <TableRow key={user.id} hover>
                  <TableCell sx={{ fontWeight: 500 }}>{user.username}</TableCell>
                  <TableCell>
                    <Chip
                      label={user.is_active ? 'Active' : 'Inactive'}
                      color={user.is_active ? 'success' : 'default'}
                      size="small"
                    />
                  </TableCell>
                  <TableCell sx={{ color: 'text.secondary', fontSize: 13 }}>
                    {new Date(user.created_at).toLocaleDateString()}
                  </TableCell>
                  <TableCell align="right">
                    <Tooltip title={user.is_active ? 'Deactivate' : 'Activate'}>
                      <IconButton
                        size="small"
                        onClick={() => toggleActiveMutation.mutate({ id: user.id, is_active: !user.is_active })}
                        color={user.is_active ? 'success' : 'default'}
                      >
                        {user.is_active ? <CheckCircleIcon fontSize="small" /> : <CancelIcon fontSize="small" />}
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Change Password">
                      <IconButton size="small" onClick={() => { setPwdUser(user); setError('') }}>
                        <KeyIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Delete User">
                      <IconButton size="small" color="error" onClick={() => setDeleteUser(user)}>
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>

      {/* Add User Dialog */}
      <Dialog open={addOpen} onClose={() => setAddOpen(false)} maxWidth="xs" fullWidth>
        <DialogTitle>Add New User</DialogTitle>
        <DialogContent sx={{ pt: 2 }}>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          <TextField
            label="Username" fullWidth margin="normal" value={newUsername}
            onChange={(e) => setNewUsername(e.target.value)} autoFocus
          />
          <TextField
            label="Password" type="password" fullWidth margin="normal" value={newPassword}
            onChange={(e) => setNewPassword(e.target.value)}
            helperText="Minimum 6 characters"
          />
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setAddOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={createMutation.isPending || !newUsername || !newPassword}
            onClick={() => createMutation.mutate({ username: newUsername, password: newPassword })}
          >
            {createMutation.isPending ? 'Creating...' : 'Create User'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Change Password Dialog */}
      <Dialog open={!!pwdUser} onClose={() => setPwdUser(null)} maxWidth="xs" fullWidth>
        <DialogTitle>Change Password — {pwdUser?.username}</DialogTitle>
        <DialogContent sx={{ pt: 2 }}>
          {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
          <TextField
            label="New Password" type="password" fullWidth margin="normal" value={newPwd}
            onChange={(e) => setNewPwd(e.target.value)} autoFocus
            helperText="Minimum 6 characters"
          />
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setPwdUser(null)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={changePwdMutation.isPending || !newPwd}
            onClick={() => pwdUser && changePwdMutation.mutate({ id: pwdUser.id, new_password: newPwd })}
          >
            {changePwdMutation.isPending ? 'Saving...' : 'Save Password'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={!!deleteUser} onClose={() => setDeleteUser(null)} maxWidth="xs" fullWidth>
        <DialogTitle>Delete User</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to delete <strong>{deleteUser?.username}</strong>? This cannot be undone.
          </Typography>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setDeleteUser(null)}>Cancel</Button>
          <Button
            variant="contained" color="error"
            disabled={deleteMutation.isPending}
            onClick={() => deleteUser && deleteMutation.mutate(deleteUser.id)}
          >
            {deleteMutation.isPending ? 'Deleting...' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
