from sqlalchemy import Boolean, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base, TimestampMixin, UUIDMixin


class ConnectorConfig(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "connector_configs"

    type: Mapped[str] = mapped_column(String(20), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    credentials_encrypted: Mapped[str] = mapped_column(nullable=False)
    connection_pool_size: Mapped[int] = mapped_column(Integer, default=5)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    replication_configs = relationship("ReplicationConfig", back_populates="connector")
