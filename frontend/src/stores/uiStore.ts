import { create } from 'zustand'

interface Filters {
  documentType: string
  status: string
  dateFrom: string
  dateTo: string
}

interface UIState {
  filters: Filters
  setFilters: (f: Partial<Filters>) => void
  resetFilters: () => void

  selectedDocId: string | null
  setSelectedDocId: (id: string | null) => void

  triggerLoading: boolean
  setTriggerLoading: (v: boolean) => void
}

const defaultFilters: Filters = {
  documentType: '',
  status: '',
  dateFrom: '',
  dateTo: '',
}

export const useUIStore = create<UIState>((set) => ({
  filters: { ...defaultFilters },
  setFilters: (f) => set((s) => ({ filters: { ...s.filters, ...f } })),
  resetFilters: () => set({ filters: { ...defaultFilters } }),

  selectedDocId: null,
  setSelectedDocId: (id) => set({ selectedDocId: id }),

  triggerLoading: false,
  setTriggerLoading: (v) => set({ triggerLoading: v }),
}))
