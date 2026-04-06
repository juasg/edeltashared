"""
Event publisher — broadcasts real-time events via Redis pub/sub.

Consumers of these events:
- WebSocket endpoint → forwards to connected React clients
- CDC Agent → listens for control commands

Event types:
- stream_update: stream status/latency changed
- latency_alert: P95 approaching or breaching 30s SLA
- hana_health: CPU/memory readings from HANA
- job_progress: initial load chunk completion
- config_change: replication created/activated/paused/deleted
"""

import json
import logging

import redis.asyncio as aioredis

logger = logging.getLogger("edeltashared.events")

CHANNEL = "edelta:events"


class EventPublisher:
    def __init__(self, redis_client: aioredis.Redis):
        self._redis = redis_client

    async def publish(self, event_type: str, payload: dict) -> None:
        message = json.dumps({"type": event_type, "payload": payload})
        await self._redis.publish(CHANNEL, message)

    async def stream_status_changed(
        self, config_id: str, status: str, latency_ms: int | None = None, error: str | None = None
    ) -> None:
        await self.publish("stream_update", {
            "config_id": config_id,
            "status": status,
            "avg_latency_ms": latency_ms,
            "error_message": error,
        })

    async def latency_alert(
        self, config_id: str, target_type: str, p95_ms: int, threshold_ms: int = 30000
    ) -> None:
        level = "breach" if p95_ms > threshold_ms else "warning"
        await self.publish("latency_alert", {
            "config_id": config_id,
            "target_type": target_type,
            "p95_latency_ms": p95_ms,
            "threshold_ms": threshold_ms,
            "level": level,
        })

    async def hana_health(self, cpu_percent: float, memory_percent: float, backed_off: bool) -> None:
        await self.publish("hana_health", {
            "cpu_percent": cpu_percent,
            "memory_percent": memory_percent,
            "backed_off": backed_off,
        })

    async def job_progress(
        self, job_id: str, config_id: str, status: str, percent: float, records_written: int
    ) -> None:
        await self.publish("job_progress", {
            "job_id": job_id,
            "config_id": config_id,
            "status": status,
            "percent": percent,
            "records_written": records_written,
        })

    async def config_changed(self, config_id: str, action: str) -> None:
        await self.publish("config_change", {
            "config_id": config_id,
            "action": action,
        })
