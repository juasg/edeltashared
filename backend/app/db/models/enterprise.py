from datetime import datetime

from sqlalchemy import BigInteger, Boolean, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, TimestampMixin, UUIDMixin


class Organization(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "organizations"

    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    slug: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    settings: Mapped[dict] = mapped_column(JSONB, default=dict)

    users = relationship("User", back_populates="organization")


class User(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str | None] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(20), default="viewer")
    org_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_login_at: Mapped[datetime | None] = mapped_column(nullable=True)

    organization = relationship("Organization", back_populates="users")
    api_keys = relationship("ApiKey", back_populates="user", cascade="all, delete-orphan")


class ApiKey(UUIDMixin, Base):
    __tablename__ = "api_keys"

    user_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    org_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=True
    )
    key_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[str | None] = mapped_column(String(255))
    scopes: Mapped[list] = mapped_column(JSONB, default=lambda: ["read"])
    expires_at: Mapped[datetime | None] = mapped_column(nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    user = relationship("User", back_populates="api_keys")


class StreamCost(UUIDMixin, Base):
    __tablename__ = "stream_costs"

    config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True), ForeignKey("replication_configs.id"), nullable=False
    )
    org_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=True
    )
    period_start: Mapped[datetime] = mapped_column(nullable=False)
    period_end: Mapped[datetime] = mapped_column(nullable=False)
    records_processed: Mapped[int] = mapped_column(BigInteger, default=0)
    bytes_transferred: Mapped[int] = mapped_column(BigInteger, default=0)
    compute_seconds: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    target_type: Mapped[str | None] = mapped_column(String(20))
    cost_usd: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    breakdown: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)


class NotificationChannel(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "notification_channels"

    org_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=True
    )
    channel_type: Mapped[str] = mapped_column(String(20), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    config_encrypted: Mapped[str] = mapped_column(nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    alert_rules = relationship("AlertRule", back_populates="channel")


class AlertRule(UUIDMixin, Base):
    __tablename__ = "alert_rules"

    org_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    condition_type: Mapped[str] = mapped_column(String(30), nullable=False)
    condition_params: Mapped[dict] = mapped_column(JSONB, default=dict)
    channel_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True), ForeignKey("notification_channels.id"), nullable=False
    )
    severity_filter: Mapped[str | None] = mapped_column(String(10))
    cooldown_minutes: Mapped[int] = mapped_column(Integer, default=15)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    last_fired_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    channel = relationship("NotificationChannel", back_populates="alert_rules")


class AlertHistory(UUIDMixin, Base):
    __tablename__ = "alert_history"

    rule_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("alert_rules.id"), nullable=True
    )
    channel_id: Mapped[str | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("notification_channels.id"), nullable=True
    )
    alert_type: Mapped[str | None] = mapped_column(String(30))
    severity: Mapped[str | None] = mapped_column(String(10))
    message: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict | None] = mapped_column(JSONB)
    delivery_status: Mapped[str] = mapped_column(String(20), default="pending")
    error_details: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
