import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["websocket"])

logger = logging.getLogger("edeltashared.websocket")


class ConnectionManager:
    """Manages active WebSocket connections and broadcasts events."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected ({len(self.active_connections)} active)")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected ({len(self.active_connections)} active)")

    async def broadcast(self, message: dict):
        """Send a message to all connected clients."""
        dead = []
        for conn in self.active_connections:
            try:
                await conn.send_json(message)
            except Exception:
                dead.append(conn)
        for conn in dead:
            self.active_connections.remove(conn)


manager = ConnectionManager()


MAX_WS_CONNECTIONS = 100


@router.websocket("/api/v1/ws/events")
async def websocket_events(websocket: WebSocket):
    """WebSocket endpoint for live stream events.

    Requires a valid JWT token as a query parameter: ?token=<jwt>
    Subscribes to Redis pub/sub and forwards events to the client.
    """
    # Authenticate via query param token
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=4001, reason="Missing authentication token")
        return

    try:
        from app.core.auth import decode_token
        decode_token(token)
    except Exception:
        await websocket.close(code=4003, reason="Invalid or expired token")
        return

    # Enforce connection limit
    if len(manager.active_connections) >= MAX_WS_CONNECTIONS:
        await websocket.close(code=4029, reason="Too many connections")
        return

    await manager.connect(websocket)
    redis_client = websocket.app.state.redis

    try:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe("edelta:events")

        # Run two tasks: listen for Redis events and client messages
        async def redis_listener():
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        await websocket.send_json(data)
                    except (json.JSONDecodeError, Exception):
                        pass

        async def client_listener():
            while True:
                try:
                    data = await websocket.receive_text()
                    # Handle client commands (e.g., subscribe to specific streams)
                    msg = json.loads(data)
                    if msg.get("type") == "ping":
                        await websocket.send_json({"type": "pong"})
                except WebSocketDisconnect:
                    break
                except Exception:
                    break

        # Run both listeners concurrently
        redis_task = asyncio.create_task(redis_listener())
        client_task = asyncio.create_task(client_listener())

        done, pending = await asyncio.wait(
            [redis_task, client_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe("edelta:events")
        manager.disconnect(websocket)
