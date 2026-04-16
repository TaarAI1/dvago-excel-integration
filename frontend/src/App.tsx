import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider, createTheme, CssBaseline, Box } from '@mui/material'
import { useState } from 'react'
import Navbar from './components/Navbar/Navbar'
import Login from './components/Login/Login'
import DashboardPage from './pages/DashboardPage'
import UsersPage from './pages/UsersPage'
import SettingsPage from './pages/SettingsPage'
import ItemMasterPage from './pages/ItemMasterPage'
import ImportsPage from './pages/ImportsPage'

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 10000 } },
})

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: { main: '#1a56db', dark: '#1341b0', light: '#3b76f6', contrastText: '#fff' },
    secondary: { main: '#374151' },
    success: { main: '#15803d' },
    error: { main: '#b91c1c' },
    warning: { main: '#b45309' },
    info: { main: '#1a56db' },
    background: { default: '#f9fafb', paper: '#ffffff' },
    text: { primary: '#111827', secondary: '#6b7280' },
    divider: '#e5e7eb',
  },
  typography: {
    fontFamily: '"Inter", "Roboto", system-ui, sans-serif',
    h5: { fontWeight: 600 },
    h6: { fontWeight: 600 },
    subtitle1: { fontWeight: 600 },
    subtitle2: { fontWeight: 600 },
    button: { fontWeight: 500, letterSpacing: 0, textTransform: 'none' },
  },
  shape: { borderRadius: 6 },
  // Flat — no shadows anywhere
  shadows: Array(25).fill('none') as any,
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        'html, body, #root': { height: '100%' },
        '*': { boxSizing: 'border-box' },
        body: { overflowX: 'hidden' },
      },
    },
    MuiAppBar: {
      styleOverrides: {
        root: { boxShadow: 'none' },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: { boxShadow: 'none', backgroundImage: 'none' },
        outlined: { borderColor: '#e5e7eb' },
        elevation1: { border: '1px solid #e5e7eb' },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: { boxShadow: 'none', border: '1px solid #e5e7eb' },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: {
          boxShadow: 'none',
          borderRadius: 6,
          fontWeight: 500,
          '&:hover': { boxShadow: 'none' },
          '&:active': { boxShadow: 'none' },
        },
        contained: { '&:hover': { boxShadow: 'none' } },
        outlined: { borderColor: '#d1d5db' },
      },
    },
    MuiChip: {
      styleOverrides: { root: { borderRadius: 4, fontWeight: 500, fontSize: '0.75rem' } },
    },
    MuiTab: {
      styleOverrides: {
        root: { textTransform: 'none', fontWeight: 500, fontSize: '0.875rem', minHeight: 40 },
      },
    },
    MuiTabs: {
      styleOverrides: { indicator: { height: 2 } },
    },
    MuiOutlinedInput: {
      styleOverrides: {
        root: {
          borderRadius: 6,
          '& .MuiOutlinedInput-notchedOutline': { borderColor: '#d1d5db' },
          '&:hover .MuiOutlinedInput-notchedOutline': { borderColor: '#9ca3af' },
        },
      },
    },
    MuiTableHead: {
      styleOverrides: {
        root: {
          '& .MuiTableCell-head': {
            fontWeight: 600,
            fontSize: '0.75rem',
            textTransform: 'uppercase',
            letterSpacing: '0.04em',
            color: '#6b7280',
            backgroundColor: '#f9fafb',
            borderBottom: '1px solid #e5e7eb',
          },
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: { borderColor: '#f3f4f6', padding: '10px 16px', fontSize: '0.875rem' },
      },
    },
    MuiTableRow: {
      styleOverrides: {
        root: {
          '&:hover': { backgroundColor: '#fafafa' },
          '&:last-child td': { border: 0 },
        },
      },
    },
    MuiDivider: {
      styleOverrides: { root: { borderColor: '#e5e7eb' } },
    },
    MuiAlert: {
      styleOverrides: { root: { borderRadius: 6, fontSize: '0.875rem' } },
    },
    MuiDialog: {
      styleOverrides: {
        paper: {
          boxShadow: '0 20px 60px -10px rgba(0,0,0,0.2)',
          borderRadius: 8,
          border: '1px solid #e5e7eb',
        },
      },
    },
    MuiDialogTitle: {
      styleOverrides: { root: { fontWeight: 600, fontSize: '1rem', padding: '18px 20px 10px' } },
    },
    MuiTextField: { defaultProps: { size: 'small' } },
    MuiSelect: { defaultProps: { size: 'small' } },
  },
})

function AppLayout() {
  return (
    <Box sx={{ minHeight: '100vh', bgcolor: 'background.default', display: 'flex', flexDirection: 'column' }}>
      <Navbar />
      <Box
        component="main"
        sx={{ flexGrow: 1, width: '100%', maxWidth: '100%', overflowX: 'hidden' }}
      >
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/item-master" element={<ItemMasterPage />} />
          <Route path="/imports" element={<ImportsPage />} />
          <Route path="/users" element={<UsersPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Box>
    </Box>
  )
}

function App() {
  const [authed, setAuthed] = useState(() => !!localStorage.getItem('access_token'))

  if (!authed) {
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Login onLogin={() => setAuthed(true)} />
      </ThemeProvider>
    )
  }

  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <BrowserRouter>
          <AppLayout />
        </BrowserRouter>
      </ThemeProvider>
    </QueryClientProvider>
  )
}

export default App
