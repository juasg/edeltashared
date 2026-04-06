from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum


class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class ApplyResult:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class LoadResult:
    records_written: int = 0
    records_failed: int = 0
    chunk_id: str = ""
    errors: list[str] = field(default_factory=list)


@dataclass
class CDCEvent:
    source: str
    schema: str
    table: str
    op: str  # 'I', 'U', 'D'
    key: dict
    before: dict | None
    after: dict | None
    ts_ms: int
    seq: int


@dataclass
class TableSchema:
    schema_name: str
    table_name: str
    columns: list[dict]  # [{name, type, nullable, is_pk}]
    primary_keys: list[str]


class DataConnector(ABC):
    """Base class for all target database connectors."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the target database."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close all connections."""

    @abstractmethod
    async def validate_connection(self) -> bool:
        """Test that the connection is valid and operational."""

    @abstractmethod
    async def ensure_table(self, schema: TableSchema) -> None:
        """Create or alter target table to match schema."""

    @abstractmethod
    async def apply_changes(self, changes: list[CDCEvent]) -> ApplyResult:
        """Apply a micro-batch of CDC events (INSERT/UPDATE/DELETE).
        Must be idempotent — safe to replay on failure."""

    @abstractmethod
    async def bulk_load(self, records: list[dict], chunk_id: str) -> LoadResult:
        """Bulk load for initial sync. Chunk-based for resumability."""

    @abstractmethod
    async def get_row_count(self, table: str) -> int:
        """Get current row count in target table."""

    @abstractmethod
    async def health_check(self) -> HealthStatus:
        """Check connection health."""
