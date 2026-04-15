import { useEffect, useRef, useState, useCallback } from 'react'

export function useSSE<T>(url: string, enabled = true) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const esRef = useRef<EventSource | null>(null)

  const connect = useCallback(() => {
    if (!enabled || !url) return

    const token = localStorage.getItem('access_token')
    const fullUrl = `${url}?token=${token ?? ''}`

    const es = new EventSource(fullUrl)
    esRef.current = es

    es.onmessage = (event) => {
      try {
        setData(JSON.parse(event.data))
        setError(null)
      } catch {
        // ignore parse errors
      }
    }

    es.onerror = () => {
      setError('Connection lost. Reconnecting...')
      es.close()
      // Reconnect after 5s
      setTimeout(connect, 5000)
    }
  }, [url, enabled])

  useEffect(() => {
    connect()
    return () => {
      esRef.current?.close()
    }
  }, [connect])

  return { data, error }
}
