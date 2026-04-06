import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class InitialLoadRequest(BaseModel):
    config_id: uuid.UUID
    chunk_size: int = Field(default=10000, ge=1000, le=100000)
    max_hana_cpu: int = Field(default=60, ge=10, le=90)


class JobResponse(BaseModel):
    id: uuid.UUID
    config_id: uuid.UUID
    job_type: str
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    records_read: int | None
    records_written: int | None
    records_failed: int | None
    error_details: str | None
    metrics: dict | None
    created_at: datetime

    model_config = {"from_attributes": True}
