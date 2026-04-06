import uuid
from datetime import datetime

from pydantic import BaseModel


class StreamStateResponse(BaseModel):
    id: uuid.UUID
    config_id: uuid.UUID
    consumer_group: str
    last_kafka_offset: int
    last_hana_seq_id: int
    last_processed_at: datetime | None
    status: str
    error_message: str | None
    records_processed_total: int
    avg_latency_ms: int | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class StreamLag(BaseModel):
    consumer_group: str
    topic: str
    current_offset: int
    high_watermark: int
    lag: int


class LatencyResponse(BaseModel):
    config_id: uuid.UUID
    target_type: str
    p50_latency_ms: int | None
    p95_latency_ms: int | None
    p99_latency_ms: int | None
    max_latency_ms: int | None
    records_in_window: int | None
    window_start: datetime | None
    window_end: datetime | None
