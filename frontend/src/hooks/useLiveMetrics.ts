import { useEffect, useRef } from "react";
import { useMetricsStore } from "../state/store";

const WS_URL = import.meta.env.VITE_WS_URL || "ws://localhost:8000";

export function useLiveMetrics() {
  const wsRef = useRef<WebSocket | null>(null);
  const updateStream = useMetricsStore((s) => s.updateStream);

  useEffect(() => {
    const ws = new WebSocket(`${WS_URL}/api/v1/ws/events`);
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

    ws.onclose = () => {
      // Reconnect after 3 seconds
      setTimeout(() => {
        if (wsRef.current === ws) {
          wsRef.current = null;
        }
      }, 3000);
    };

    // Ping every 30s to keep alive
    const pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "ping" }));
      }
    }, 30000);

    return () => {
      clearInterval(pingInterval);
      ws.close();
    };
  }, [updateStream]);
}
