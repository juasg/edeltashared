from datetime import datetime

from sqlalchemy import BigInteger, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, TimestampMixin, UUIDMixin


class StreamState(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "stream_state"

    config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("replication_configs.id", ondelete="CASCADE"),
        nullable=False,
    )
    consumer_group: Mapped[str] = mapped_column(String(255), nullable=False)
    last_kafka_offset: Mapped[int] = mapped_column(BigInteger, default=0)
    last_hana_seq_id: Mapped[int] = mapped_column(BigInteger, default=0)
    last_processed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="active")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    records_processed_total: Mapped[int] = mapped_column(BigInteger, default=0)
    avg_latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    config = relationship("ReplicationConfig", back_populates="stream_states")
