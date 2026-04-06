from datetime import datetime

from sqlalchemy import BigInteger, Boolean, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, TimestampMixin, UUIDMixin


class DataQualityRule(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "data_quality_rules"

    config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("replication_configs.id", ondelete="CASCADE"),
        nullable=False,
    )
    field_name: Mapped[str] = mapped_column(String(255), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(30), nullable=False)
    rule_params: Mapped[dict] = mapped_column(JSONB, default=dict)
    severity: Mapped[str] = mapped_column(String(10), default="warn")
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    violations = relationship("DataQualityViolation", back_populates="rule", cascade="all, delete-orphan")


class DataQualityViolation(UUIDMixin, Base):
    __tablename__ = "data_quality_violations"

    rule_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("data_quality_rules.id", ondelete="CASCADE"),
        nullable=False,
    )
    config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True), ForeignKey("replication_configs.id"), nullable=False
    )
    field_name: Mapped[str | None] = mapped_column(String(255))
    field_value: Mapped[str | None] = mapped_column(Text)
    row_key: Mapped[str | None] = mapped_column(String(500))
    violation_type: Mapped[str | None] = mapped_column(String(30))
    severity: Mapped[str | None] = mapped_column(String(10))
    message: Mapped[str | None] = mapped_column(Text)
    event_seq: Mapped[int | None] = mapped_column(BigInteger)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    rule = relationship("DataQualityRule", back_populates="violations")


class FieldTransformation(UUIDMixin, Base):
    __tablename__ = "field_transformations"

    config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("replication_configs.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_field: Mapped[str] = mapped_column(String(255), nullable=False)
    transform_type: Mapped[str] = mapped_column(String(30), nullable=False)
    transform_params: Mapped[dict] = mapped_column(JSONB, default=dict)
    execution_order: Mapped[int] = mapped_column(Integer, default=0)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)


class ColumnLineage(UUIDMixin, Base):
    __tablename__ = "column_lineage"

    config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("replication_configs.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_schema: Mapped[str | None] = mapped_column(String(255))
    source_table: Mapped[str | None] = mapped_column(String(255))
    source_column: Mapped[str | None] = mapped_column(String(255))
    target_type: Mapped[str | None] = mapped_column(String(20))
    target_table: Mapped[str | None] = mapped_column(String(255))
    target_column: Mapped[str | None] = mapped_column(String(255))
    transformations: Mapped[list] = mapped_column(JSONB, default=list)
    masking_applied: Mapped[bool] = mapped_column(Boolean, default=False)
    last_synced_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)


class SchemaChange(UUIDMixin, Base):
    __tablename__ = "schema_changes"

    config_id: Mapped[str] = mapped_column(
        UUID(as_uuid=True), ForeignKey("replication_configs.id"), nullable=False
    )
    change_type: Mapped[str] = mapped_column(String(20), nullable=False)
    column_name: Mapped[str | None] = mapped_column(String(255))
    old_definition: Mapped[dict | None] = mapped_column(JSONB)
    new_definition: Mapped[dict | None] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    auto_resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    detected_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    resolved_at: Mapped[datetime | None] = mapped_column(nullable=True)
