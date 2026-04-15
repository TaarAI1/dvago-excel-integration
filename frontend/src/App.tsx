import { useState } from 'react'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import {
  ThemeProvider, createTheme, CssBaseline, Box, Toolbar,
} from '@mui/material'
import Header from './components/Header/Header'
import Sidebar, { SIDEBAR_WIDTH } from './components/Layout/Sidebar'
import Login from './components/Login/Login'
import DashboardPage from './pages/DashboardPage'
import UsersPage from './pages/UsersPage'
import SettingsPage from './pages/SettingsPage'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 10000,
    },
  },
})

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: { main: '#1565c0' },
    secondary: { main: '#ff6f00' },
    background: { default: '#f4f6f9' },
  },
  typography: {
    fontFamily: '"Inter", "Roboto", "Helvetica", "Arial", sans-serif',
  },
})

function AppLayout() {
  const [sidebarOpen, setSidebarOpen] = useState(true)

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      <Sidebar open={sidebarOpen} onClose={() => setSidebarOpen(false)} />

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          bgcolor: 'background.default',
          display: 'flex',
          flexDirection: 'column',
          ml: sidebarOpen ? `${SIDEBAR_WIDTH}px` : 0,
          width: sidebarOpen ? `calc(100% - ${SIDEBAR_WIDTH}px)` : '100%',
          transition: (theme) =>
            theme.transitions.create(['margin', 'width'], {
              easing: sidebarOpen
                ? theme.transitions.easing.easeOut
                : theme.transitions.easing.sharp,
              duration: sidebarOpen
                ? theme.transitions.duration.enteringScreen
                : theme.transitions.duration.leavingScreen,
            }),
        }}
      >
        <Header
          sidebarOpen={sidebarOpen}
          onSidebarToggle={() => setSidebarOpen(true)}
        />
        {/* Spacer for fixed AppBar */}
        <Toolbar sx={{ minHeight: 56 }} />
        <Box sx={{ p: 3, flexGrow: 1 }}>
          <Routes>
            <Route path="/" element={<DashboardPage />} />
            <Route path="/users" element={<UsersPage />} />
            <Route path="/settings" element={<SettingsPage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Box>
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
