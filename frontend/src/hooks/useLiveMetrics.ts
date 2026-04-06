import { useEffect, useRef } from "react";
import { useMetricsStore } from "../state/store";

const WS_URL = import.meta.env.VITE_WS_URL || "";

export function useLiveMetrics() {
  const wsRef = useRef<WebSocket | null>(null);
  const updateStream = useMetricsStore((s) => s.updateStream);

  useEffect(() => {
    // Skip WebSocket if no backend URL configured
    if (!WS_URL) return;

    let ws: WebSocket;
    try {
      ws = new WebSocket(`${WS_URL}/api/v1/ws/events`);
    } catch {
      return;
    }
    wsRef.current = ws;

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === "stream_update") {
          updateStream(data.payload);
        }
      } catch {
        // ignore parse errors
      }
    };

    ws.onerror = () => {
      // Silently handle connection errors when backend is unavailable
    };

    ws.onclose = () => {
      if (wsRef.current === ws) {
        wsRef.current = null;
      }
    };

    return () => {
      ws.close();
    };
  }, [updateStream]);
}
