from datetime import datetime

from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.models.base import Base


class SchemaCache(Base):
    __tablename__ = "schema_cache"

    source_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    table_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    table_spec: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    fields: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    relationships: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    cached_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    expires_at: Mapped[datetime | None] = mapped_column(nullable=True)
