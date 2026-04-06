from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.db.models import LatencyMetric
from app.db.session import get_session

router = APIRouter(prefix="/metrics", tags=["metrics"], dependencies=[Depends(get_current_user)])


@router.get("", response_class=PlainTextResponse)
async def prometheus_metrics():
    """Prometheus-compatible metrics endpoint."""
    return PlainTextResponse(
        content=generate_latest().decode("utf-8"),
        media_type=CONTENT_TYPE_LATEST,
    )


@router.get("/latency")
async def latency_percentiles(
    config_id: str | None = Query(None, description="Filter by replication config"),
    limit: int = Query(50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    """JSON latency percentiles for UI consumption."""
    stmt = select(LatencyMetric).order_by(LatencyMetric.window_end.desc()).limit(limit)

    if config_id:
        stmt = stmt.where(LatencyMetric.config_id == config_id)

    result = await session.execute(stmt)
    metrics = result.scalars().all()

    return [
        {
            "config_id": str(m.config_id),
            "target_type": m.target_type,
            "p50": m.p50_latency_ms,
            "p95": m.p95_latency_ms,
            "p99": m.p99_latency_ms,
            "max": m.max_latency_ms,
            "records": m.records_in_window,
            "window_start": m.window_start.isoformat() if m.window_start else None,
            "window_end": m.window_end.isoformat() if m.window_end else None,
        }
        for m in metrics
    ]
