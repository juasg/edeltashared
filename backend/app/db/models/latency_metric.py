from datetime import datetime

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.models.base import Base, UUIDMixin


class LatencyMetric(UUIDMixin, Base):
    __tablename__ = "latency_metrics"

    config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True), ForeignKey("replication_configs.id"), nullable=False
    )
    target_type: Mapped[str] = mapped_column(String(20), nullable=False)
    p50_latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    p95_latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    p99_latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    records_in_window: Mapped[int | None] = mapped_column(Integer, nullable=True)
    window_start: Mapped[datetime | None] = mapped_column(nullable=True)
    window_end: Mapped[datetime | None] = mapped_column(nullable=True)
