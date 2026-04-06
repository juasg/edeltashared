from datetime import datetime

from sqlalchemy import BigInteger, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, UUIDMixin


class JobHistory(UUIDMixin, Base):
    __tablename__ = "job_history"

    config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True), ForeignKey("replication_configs.id"), nullable=False
    )
    job_type: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="queued")
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    records_read: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    records_written: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    records_failed: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    error_details: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        default=datetime.utcnow, server_default="now()"
    )

    config = relationship("ReplicationConfig", back_populates="jobs")
