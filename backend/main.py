import logging
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.routers.alerts import router as alerts_router
from app.api.v1.routers.auth import router as auth_router
from app.api.v1.routers.config import router as config_router
from app.api.v1.routers.costs import router as costs_router
from app.api.v1.routers.health import router as health_router
from app.api.v1.routers.jobs import router as jobs_router
from app.api.v1.routers.logs import router as logs_router
from app.api.v1.routers.metrics import router as metrics_router
from app.api.v1.routers.orgs import router as orgs_router
from app.api.v1.routers.quality import router as quality_router
from app.api.v1.routers.schema import router as schema_router
from app.api.v1.routers.streams import router as streams_router
from app.api.websocket import router as ws_router
from app.core.config import settings
from app.core.telemetry import init_telemetry, instrument_fastapi
from app.db.session import engine

logger = logging.getLogger("edeltashared")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    # Startup
    logger.info("Starting eDeltaShared backend...")

    # OpenTelemetry
    init_telemetry("edeltashared-backend")

    # Redis connection
    app.state.redis = aioredis.from_url(
        settings.redis_url, decode_responses=True
    )
    logger.info("Redis connected")

    yield

    # Shutdown
    logger.info("Shutting down eDeltaShared backend...")
    await app.state.redis.close()
    await engine.dispose()
    logger.info("Shutdown complete")


app = FastAPI(
    title="eDeltaShared",
    description="Near real-time CDC replication engine for SAP HANA",
    version="0.1.0",
    lifespan=lifespan,
)

# OpenTelemetry instrumentation
instrument_fastapi(app)

# CORS — restricted to configured origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.cors_allowed_origins.split(",")],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers — public
app.include_router(health_router, prefix="/api/v1")
app.include_router(auth_router, prefix="/api/v1")

# Routers — authenticated
app.include_router(schema_router, prefix="/api/v1")
app.include_router(config_router, prefix="/api/v1")
app.include_router(streams_router, prefix="/api/v1")
app.include_router(jobs_router, prefix="/api/v1")
app.include_router(logs_router, prefix="/api/v1")
app.include_router(metrics_router, prefix="/api/v1")
app.include_router(quality_router, prefix="/api/v1")
app.include_router(costs_router, prefix="/api/v1")
app.include_router(alerts_router, prefix="/api/v1")
app.include_router(orgs_router, prefix="/api/v1")
app.include_router(ws_router)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
