import { useState } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider, createTheme, CssBaseline, Box, Container, Divider } from '@mui/material'
import Header from './components/Header/Header'
import StatsCards from './components/StatsCards/StatsCards'
import DocumentTable from './components/DocumentTable/DocumentTable'
import ActivityLog from './components/ActivityLog/ActivityLog'
import Login from './components/Login/Login'

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

function Dashboard() {
  return (
    <Box sx={{ minHeight: '100vh', bgcolor: 'background.default' }}>
      <Header />
      <Container maxWidth="xl" sx={{ py: 3 }}>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          <StatsCards />
          <Divider />
          <DocumentTable />
          <Divider />
          <ActivityLog />
        </Box>
      </Container>
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
        <Dashboard />
      </ThemeProvider>
    </QueryClientProvider>
  )
}

export default App
