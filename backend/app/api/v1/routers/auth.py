import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import (
    create_access_token,
    get_current_user,
    hash_password,
    require_role,
    verify_password,
)
from app.db.models.enterprise import Organization, User
from app.db.session import get_session

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    email: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict


class UserCreate(BaseModel):
    email: str
    password: str = Field(..., min_length=8)
    full_name: str | None = None
    role: str = Field(default="viewer", pattern="^(viewer|operator|admin)$")
    org_id: uuid.UUID | None = None


class UserResponse(BaseModel):
    id: str
    email: str
    full_name: str | None
    role: str
    org_id: str | None
    is_active: bool
    last_login_at: str | None


class SetupRequest(BaseModel):
    email: str
    password: str = Field(..., min_length=12)
    full_name: str | None = None


@router.post("/setup", status_code=201)
async def first_run_setup(data: SetupRequest, session: AsyncSession = Depends(get_session)):
    """First-run setup — creates the initial super_admin user.
    Only works if no users exist in the database."""
    existing = await session.execute(select(User).limit(1))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Setup already completed — users exist")

    default_org_id = "00000000-0000-0000-0000-000000000001"
    user = User(
        email=data.email,
        password_hash=hash_password(data.password),
        full_name=data.full_name or "System Admin",
        role="super_admin",
        org_id=default_org_id,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)

    token = create_access_token(str(user.id), user.email, user.role, default_org_id)
    return {"message": "Setup complete", "access_token": token, "user_id": str(user.id)}


@router.post("/login", response_model=LoginResponse)
async def login(data: LoginRequest, session: AsyncSession = Depends(get_session)):
    result = await session.execute(
        select(User).where(User.email == data.email, User.is_active == True)
    )
    user = result.scalar_one_or_none()

    if user is None or not verify_password(data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    user.last_login_at = datetime.now(timezone.utc)
    await session.commit()

    token = create_access_token(
        str(user.id), user.email, user.role, str(user.org_id) if user.org_id else None
    )

    return LoginResponse(
        access_token=token,
        user={
            "id": str(user.id),
            "email": user.email,
            "full_name": user.full_name,
            "role": user.role,
            "org_id": str(user.org_id) if user.org_id else None,
        },
    )


@router.get("/me")
async def get_me(user: User = Depends(get_current_user)):
    return {
        "id": str(user.id),
        "email": user.email,
        "full_name": user.full_name,
        "role": user.role,
        "org_id": str(user.org_id) if user.org_id else None,
        "is_active": user.is_active,
    }


@router.post("/users", status_code=201)
async def create_user(
    data: UserCreate,
    session: AsyncSession = Depends(get_session),
    admin: User = Depends(require_role("admin")),
):
    existing = await session.execute(select(User).where(User.email == data.email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Email already registered")

    user = User(
        email=data.email,
        password_hash=hash_password(data.password),
        full_name=data.full_name,
        role=data.role,
        org_id=data.org_id or admin.org_id,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)

    return {"id": str(user.id), "email": user.email, "role": user.role}


@router.get("/users")
async def list_users(
    session: AsyncSession = Depends(get_session),
    admin: User = Depends(require_role("admin")),
):
    stmt = select(User).order_by(User.email)
    if admin.role != "super_admin":
        stmt = stmt.where(User.org_id == admin.org_id)
    result = await session.execute(stmt)
    return [
        {
            "id": str(u.id),
            "email": u.email,
            "full_name": u.full_name,
            "role": u.role,
            "is_active": u.is_active,
            "last_login_at": u.last_login_at.isoformat() if u.last_login_at else None,
        }
        for u in result.scalars().all()
    ]
