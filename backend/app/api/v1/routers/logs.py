import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.db.models import AuditLog
from app.db.session import get_session

router = APIRouter(prefix="/logs", tags=["logs"], dependencies=[Depends(get_current_user)])


@router.get("")
async def list_logs(
    entity_type: str | None = Query(None, description="Filter by entity type"),
    action: str | None = Query(None, description="Filter by action"),
    search: str | None = Query(None, description="Search in changes JSON"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    """List audit log entries with filtering and pagination."""
    stmt = select(AuditLog).order_by(desc(AuditLog.timestamp))

    if entity_type:
        stmt = stmt.where(AuditLog.entity_type == entity_type)
    if action:
        stmt = stmt.where(AuditLog.action == action)

    # Count total
    count_stmt = select(func.count()).select_from(AuditLog)
    if entity_type:
        count_stmt = count_stmt.where(AuditLog.entity_type == entity_type)
    if action:
        count_stmt = count_stmt.where(AuditLog.action == action)

    count_result = await session.execute(count_stmt)
    total = count_result.scalar() or 0

    stmt = stmt.offset(offset).limit(limit)
    result = await session.execute(stmt)
    logs = result.scalars().all()

    return {
        "items": [
            {
                "id": str(log.id),
                "entity_type": log.entity_type,
                "entity_id": str(log.entity_id) if log.entity_id else None,
                "action": log.action,
                "user_id": log.user_id,
                "changes": log.changes,
                "timestamp": log.timestamp.isoformat() if log.timestamp else None,
            }
            for log in logs
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/actions")
async def list_log_actions(
    session: AsyncSession = Depends(get_session),
):
    """List distinct audit log actions for filter dropdowns."""
    result = await session.execute(
        select(AuditLog.action).distinct().where(AuditLog.action.isnot(None))
    )
    return [row[0] for row in result.all()]
