"""
JWT authentication and RBAC middleware.

Role hierarchy (each role includes all permissions of lower roles):
  viewer < operator < admin < super_admin

Permissions by role:
  viewer:      read streams, metrics, logs, schema
  operator:    + create/activate/pause replications, run initial loads
  admin:       + manage connectors, quality rules, transforms, users in own org
  super_admin: + manage organizations, all users, system settings
"""

import logging
from datetime import datetime, timedelta, timezone
from functools import wraps

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.enterprise import User
from app.db.session import get_session

logger = logging.getLogger("edeltashared.auth")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)

ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 24

ROLE_HIERARCHY = {
    "viewer": 0,
    "operator": 1,
    "admin": 2,
    "super_admin": 3,
}


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(user_id: str, email: str, role: str, org_id: str | None = None) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "org_id": org_id,
        "exp": expire,
    }
    return jwt.encode(payload, settings.secret_key, algorithm=ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.secret_key, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
    session: AsyncSession = Depends(get_session),
) -> User:
    """Extract and validate JWT from Authorization header."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    payload = decode_token(credentials.credentials)
    user_id = payload.get("sub")

    result = await session.execute(
        select(User).where(User.id == user_id, User.is_active == True)
    )
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
        )

    # Attach user to request state for downstream use
    request.state.user = user
    return user


def require_role(min_role: str):
    """Dependency that requires a minimum role level."""
    min_level = ROLE_HIERARCHY.get(min_role, 0)

    async def check(user: User = Depends(get_current_user)):
        user_level = ROLE_HIERARCHY.get(user.role, 0)
        if user_level < min_level:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires {min_role} role or higher",
            )
        return user

    return check
