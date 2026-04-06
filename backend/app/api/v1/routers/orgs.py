import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import require_role
from app.core.exceptions import ConflictError
from app.db.models.enterprise import Organization, User
from app.db.session import get_session

router = APIRouter(prefix="/orgs", tags=["organizations"])


class OrgCreate(BaseModel):
    name: str = Field(..., max_length=255)
    slug: str = Field(..., max_length=100, pattern="^[a-z0-9-]+$")


@router.post("", status_code=201)
async def create_org(
    data: OrgCreate,
    session: AsyncSession = Depends(get_session),
    admin: User = Depends(require_role("super_admin")),
):
    existing = await session.execute(
        select(Organization).where(Organization.slug == data.slug)
    )
    if existing.scalar_one_or_none():
        raise ConflictError("Organization slug already exists")

    org = Organization(name=data.name, slug=data.slug)
    session.add(org)
    await session.commit()
    await session.refresh(org)
    return {"id": str(org.id), "name": org.name, "slug": org.slug}


@router.get("")
async def list_orgs(
    session: AsyncSession = Depends(get_session),
    admin: User = Depends(require_role("super_admin")),
):
    result = await session.execute(
        select(Organization).order_by(Organization.name)
    )
    return [
        {
            "id": str(o.id),
            "name": o.name,
            "slug": o.slug,
            "is_active": o.is_active,
        }
        for o in result.scalars().all()
    ]
