import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user, require_role
from app.db.models.enterprise import User
from app.db.session import get_session
from app.services.cost_tracker import cost_tracker

router = APIRouter(prefix="/costs", tags=["costs"])


@router.get("/summary")
async def cost_summary(
    days: int = Query(30, ge=1, le=365),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    """Get aggregated cost summary for the user's organization."""
    org_id = str(user.org_id) if user.org_id else None
    return await cost_tracker.get_cost_summary(session, org_id, days)


@router.get("/daily")
async def daily_costs(
    config_id: uuid.UUID | None = Query(None),
    days: int = Query(30, ge=1, le=365),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    """Get daily cost breakdown for charting."""
    return await cost_tracker.get_daily_costs(
        session, str(config_id) if config_id else None, days
    )
