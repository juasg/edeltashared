import { create } from "zustand";
import type { LatencyMetric, StreamState } from "../types/api";

interface MetricsState {
  streams: StreamState[];
  latency: LatencyMetric[];
  setStreams: (streams: StreamState[]) => void;
  setLatency: (latency: LatencyMetric[]) => void;
  updateStream: (stream: StreamState) => void;
}

export const useMetricsStore = create<MetricsState>((set) => ({
  streams: [],
  latency: [],
  setStreams: (streams) => set({ streams }),
  setLatency: (latency) => set({ latency }),
  updateStream: (stream) =>
    set((state) => ({
      streams: state.streams.map((s) =>
        s.id === stream.id ? stream : s
      ),
    })),
}));

interface NavState {
  activePage: string;
  setActivePage: (page: string) => void;
}

export const useNavStore = create<NavState>((set) => ({
  activePage: "dashboard",
  setActivePage: (page) => set({ activePage: page }),
}));
