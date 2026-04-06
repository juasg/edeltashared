import json
import uuid

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user, require_role
from app.core.exceptions import NotFoundError
from app.db.models import LatencyMetric, ReplicationConfig, StreamState
from app.db.models.enterprise import User
from app.db.session import get_session
from app.models.stream import LatencyResponse, StreamStateResponse

router = APIRouter(prefix="/streams", tags=["streams"], dependencies=[Depends(get_current_user)])


@router.get("", response_model=list[StreamStateResponse])
async def list_streams(
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(get_current_user),
):
    """List all active CDC streams."""
    result = await session.execute(
        select(StreamState).order_by(StreamState.updated_at.desc())
    )
    return list(result.scalars().all())


@router.get("/{stream_id}/status", response_model=StreamStateResponse)
async def get_stream_status(
    stream_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """Get stream health and status."""
    result = await session.execute(
        select(StreamState).where(StreamState.id == stream_id)
    )
    stream = result.scalar_one_or_none()
    if stream is None:
        raise NotFoundError(f"Stream {stream_id} not found")
    return stream


@router.get("/{stream_id}/lag")
async def get_stream_lag(
    stream_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """Get consumer lag for a stream."""
    result = await session.execute(
        select(StreamState).where(StreamState.id == stream_id)
    )
    stream = result.scalar_one_or_none()
    if stream is None:
        raise NotFoundError(f"Stream {stream_id} not found")

    # Get the associated replication config for the topic name
    config_result = await session.execute(
        select(ReplicationConfig).where(ReplicationConfig.id == stream.config_id)
    )
    config = config_result.scalar_one_or_none()

    return {
        "consumer_group": stream.consumer_group,
        "topic": config.kafka_topic if config else "unknown",
        "current_offset": stream.last_kafka_offset,
        "last_hana_seq_id": stream.last_hana_seq_id,
        "status": stream.status,
        "last_processed_at": stream.last_processed_at,
    }


@router.post("/{stream_id}/restart")
async def restart_stream(
    stream_id: uuid.UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Restart a stalled stream via Redis pub/sub command to CDC agent."""
    result = await session.execute(
        select(StreamState).where(StreamState.id == stream_id)
    )
    stream = result.scalar_one_or_none()
    if stream is None:
        raise NotFoundError(f"Stream {stream_id} not found")

    # Send restart command to CDC agent
    await request.app.state.redis.publish(
        "edelta:agent:commands",
        json.dumps({
            "action": "restart",
            "config_id": str(stream.config_id),
        }),
    )

    # Update status
    stream.status = "active"
    stream.error_message = None
    await session.commit()

    return {"message": "Restart command sent", "stream_id": str(stream_id)}


@router.get("/{stream_id}/metrics", response_model=list[LatencyResponse])
async def get_stream_metrics(
    stream_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """Get throughput and latency metrics for a stream."""
    # Get config_id from stream
    result = await session.execute(
        select(StreamState).where(StreamState.id == stream_id)
    )
    stream = result.scalar_one_or_none()
    if stream is None:
        raise NotFoundError(f"Stream {stream_id} not found")

    metrics_result = await session.execute(
        select(LatencyMetric)
        .where(LatencyMetric.config_id == stream.config_id)
        .order_by(LatencyMetric.window_end.desc())
        .limit(100)
    )
    return list(metrics_result.scalars().all())
