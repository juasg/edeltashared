import uuid
from datetime import datetime

from pydantic import BaseModel, Field


# ===== Connector Config =====
class ConnectorConfigCreate(BaseModel):
    type: str = Field(..., pattern="^(snowflake|aurora|mongodb)$")
    name: str = Field(..., min_length=1, max_length=255)
    credentials: dict  # Will be encrypted before storage
    connection_pool_size: int = Field(default=5, ge=1, le=50)


class ConnectorConfigResponse(BaseModel):
    id: uuid.UUID
    type: str
    name: str
    connection_pool_size: int
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ===== Field Mapping =====
class FieldMapping(BaseModel):
    source: str
    target: str
    transform: str | None = None  # e.g., 'uppercase', 'trim', 'cast:int'


# ===== Replication Config =====
_SQL_IDENT = r"^[A-Za-z_][A-Za-z0-9_]{0,254}$"


class ReplicationConfigCreate(BaseModel):
    source_table_schema: str = Field(..., max_length=255, pattern=_SQL_IDENT)
    source_table_name: str = Field(..., max_length=255, pattern=_SQL_IDENT)
    connector_config_id: uuid.UUID
    target_table_name: str = Field(..., max_length=255, pattern=_SQL_IDENT)
    replication_mode: str = Field(default="cdc", pattern="^(cdc|batch)$")
    field_mappings: list[FieldMapping]
    primary_key_fields: list[str] = Field(..., min_length=1)


class ReplicationConfigUpdate(BaseModel):
    target_table_name: str | None = None
    replication_mode: str | None = Field(default=None, pattern="^(cdc|batch)$")
    field_mappings: list[FieldMapping] | None = None
    primary_key_fields: list[str] | None = None
    is_enabled: bool | None = None


class ReplicationConfigResponse(BaseModel):
    id: uuid.UUID
    source_table_schema: str
    source_table_name: str
    connector_config_id: uuid.UUID | None
    target_table_name: str
    replication_mode: str
    field_mappings: list[dict]
    primary_key_fields: list[str]
    is_enabled: bool
    trigger_deployed: bool
    kafka_topic: str | None
    initial_load_completed: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
