from sqlalchemy import Boolean, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, TimestampMixin, UUIDMixin


class ReplicationConfig(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "replication_configs"

    source_table_schema: Mapped[str] = mapped_column(String(255), nullable=False)
    source_table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    connector_config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True), ForeignKey("connector_configs.id"), nullable=True
    )
    target_table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    replication_mode: Mapped[str] = mapped_column(String(10), default="cdc")
    field_mappings: Mapped[dict] = mapped_column(JSONB, nullable=False)
    primary_key_fields: Mapped[list] = mapped_column(JSONB, nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    trigger_deployed: Mapped[bool] = mapped_column(Boolean, default=False)
    kafka_topic: Mapped[str | None] = mapped_column(String(255), nullable=True)
    initial_load_completed: Mapped[bool] = mapped_column(Boolean, default=False)

    connector = relationship("ConnectorConfig", back_populates="replication_configs")
    stream_states = relationship(
        "StreamState", back_populates="config", cascade="all, delete-orphan"
    )
    jobs = relationship("JobHistory", back_populates="config")

    __table_args__ = (
        UniqueConstraint(
            "source_table_schema",
            "source_table_name",
            "connector_config_id",
            name="uq_source_target",
        ),
    )
