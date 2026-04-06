import json
import uuid

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user, require_role
from app.core.exceptions import NotFoundError
from app.core.security import encrypt_credentials
from app.db.models.enterprise import AlertHistory, AlertRule, NotificationChannel, User
from app.db.session import get_session

router = APIRouter(prefix="/alerts", tags=["alerts"])


# ===== Notification Channels =====

class ChannelCreate(BaseModel):
    channel_type: str = Field(..., pattern="^(slack|pagerduty|email|webhook)$")
    name: str = Field(..., max_length=255)
    config: dict  # Will be encrypted


@router.post("/channels", status_code=201)
async def create_channel(
    data: ChannelCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_role("admin")),
):
    channel = NotificationChannel(
        org_id=user.org_id,
        channel_type=data.channel_type,
        name=data.name,
        config_encrypted=encrypt_credentials(json.dumps(data.config)),
    )
    session.add(channel)
    await session.commit()
    await session.refresh(channel)
    return {
        "id": str(channel.id),
        "channel_type": channel.channel_type,
        "name": channel.name,
        "is_active": channel.is_active,
    }


@router.get("/channels")
async def list_channels(
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    stmt = select(NotificationChannel).order_by(NotificationChannel.name)
    if user.org_id:
        stmt = stmt.where(NotificationChannel.org_id == user.org_id)
    result = await session.execute(stmt)
    return [
        {
            "id": str(ch.id),
            "channel_type": ch.channel_type,
            "name": ch.name,
            "is_active": ch.is_active,
        }
        for ch in result.scalars().all()
    ]


# ===== Alert Rules =====

class AlertRuleCreate(BaseModel):
    name: str = Field(..., max_length=255)
    condition_type: str = Field(..., pattern="^(latency_breach|error_rate|consumer_down|hana_cpu|custom)$")
    condition_params: dict = Field(default_factory=dict)
    channel_id: uuid.UUID
    severity_filter: str | None = None
    cooldown_minutes: int = 15


@router.post("/rules", status_code=201)
async def create_alert_rule(
    data: AlertRuleCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_role("admin")),
):
    rule = AlertRule(
        org_id=user.org_id,
        name=data.name,
        condition_type=data.condition_type,
        condition_params=data.condition_params,
        channel_id=data.channel_id,
        severity_filter=data.severity_filter,
        cooldown_minutes=data.cooldown_minutes,
    )
    session.add(rule)
    await session.commit()
    await session.refresh(rule)
    return {"id": str(rule.id), "name": rule.name, "condition_type": rule.condition_type}


@router.get("/rules")
async def list_alert_rules(
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    stmt = select(AlertRule).order_by(AlertRule.name)
    if user.org_id:
        stmt = stmt.where(AlertRule.org_id == user.org_id)
    result = await session.execute(stmt)
    return [
        {
            "id": str(r.id),
            "name": r.name,
            "condition_type": r.condition_type,
            "condition_params": r.condition_params,
            "channel_id": str(r.channel_id),
            "severity_filter": r.severity_filter,
            "cooldown_minutes": r.cooldown_minutes,
            "is_enabled": r.is_enabled,
            "last_fired_at": r.last_fired_at.isoformat() if r.last_fired_at else None,
        }
        for r in result.scalars().all()
    ]


# ===== Alert History =====

@router.get("/history")
async def alert_history(
    limit: int = Query(50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user),
):
    result = await session.execute(
        select(AlertHistory).order_by(desc(AlertHistory.created_at)).limit(limit)
    )
    return [
        {
            "id": str(h.id),
            "alert_type": h.alert_type,
            "severity": h.severity,
            "message": h.message,
            "delivery_status": h.delivery_status,
            "created_at": h.created_at.isoformat() if h.created_at else None,
        }
        for h in result.scalars().all()
    ]
