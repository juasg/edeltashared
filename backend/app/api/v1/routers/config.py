import uuid

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user, require_role
from app.db.models.enterprise import User
from app.db.session import get_session
from app.models.config import (
    ConnectorConfigCreate,
    ConnectorConfigResponse,
    ReplicationConfigCreate,
    ReplicationConfigResponse,
    ReplicationConfigUpdate,
)
from app.services.config_service import config_service

router = APIRouter(prefix="/config", tags=["config"])


# ===== Connector Configs =====


@router.post("/connectors", response_model=ConnectorConfigResponse, status_code=201)
async def create_connector(
    data: ConnectorConfigCreate,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_role("admin")),
):
    """Create a new target database connector."""
    return await config_service.create_connector(session, data)


@router.get("/connectors", response_model=list[ConnectorConfigResponse])
async def list_connectors(
    type: str | None = Query(None, description="Filter by type: snowflake, aurora, mongodb"),
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(get_current_user),
):
    """List all active connectors."""
    return await config_service.list_connectors(session, type)


@router.get("/connectors/{connector_id}", response_model=ConnectorConfigResponse)
async def get_connector(
    connector_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(get_current_user),
):
    """Get a connector by ID."""
    return await config_service.get_connector(session, connector_id)


# ===== Replication Configs =====


@router.post("/replications", response_model=ReplicationConfigResponse, status_code=201)
async def create_replication(
    data: ReplicationConfigCreate,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_role("operator")),
):
    """Create a new replication configuration."""
    return await config_service.create_replication(session, data)


@router.get("/replications", response_model=list[ReplicationConfigResponse])
async def list_replications(
    enabled: bool | None = Query(None, description="Filter by enabled status"),
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(get_current_user),
):
    """List all replication configurations."""
    return await config_service.list_replications(session, enabled_only=enabled or False)


@router.get("/replications/{config_id}", response_model=ReplicationConfigResponse)
async def get_replication(
    config_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(get_current_user),
):
    """Get replication config details."""
    return await config_service.get_replication(session, config_id)


@router.patch("/replications/{config_id}", response_model=ReplicationConfigResponse)
async def update_replication(
    config_id: uuid.UUID,
    data: ReplicationConfigUpdate,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_role("operator")),
):
    """Update a replication configuration."""
    return await config_service.update_replication(session, config_id, data)


@router.delete("/replications/{config_id}", status_code=204)
async def delete_replication(
    config_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_role("admin")),
):
    """Soft-delete a replication config and remove triggers."""
    await config_service.delete_replication(session, config_id)


@router.post("/replications/{config_id}/test")
async def test_replication(
    config_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_role("operator")),
):
    """Dry-run validation of a replication config."""
    return await config_service.test_replication(session, config_id)


@router.post("/replications/{config_id}/activate", response_model=ReplicationConfigResponse)
async def activate_replication(
    config_id: uuid.UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_role("operator")),
):
    """Deploy triggers and start CDC stream."""
    return await config_service.activate_replication(
        session, config_id, request.app.state.redis
    )


@router.post("/replications/{config_id}/pause", response_model=ReplicationConfigResponse)
async def pause_replication(
    config_id: uuid.UUID,
    request: Request,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_role("operator")),
):
    """Pause CDC stream (keep triggers active)."""
    return await config_service.pause_replication(
        session, config_id, request.app.state.redis
    )
