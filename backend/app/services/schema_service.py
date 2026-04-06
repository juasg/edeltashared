import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.hana.connection_pool import HANAConnectionPool
from app.connectors.hana.schema_introspection import HANASchemaIntrospection
from app.core.config import settings
from app.models.schema import FieldInfo, RelationshipInfo, SourceInfo, TableInfo

logger = logging.getLogger("edeltashared.schema_service")


class SchemaService:
    """Service layer for HANA schema discovery.

    Manages the HANA connection pool and introspection,
    always checking cache before hitting HANA.
    """

    def __init__(self):
        self._pool: HANAConnectionPool | None = None
        self._introspection: HANASchemaIntrospection | None = None

    def _get_pool(self) -> HANAConnectionPool:
        if self._pool is None:
            self._pool = HANAConnectionPool()
        return self._pool

    def _get_introspection(self) -> HANASchemaIntrospection:
        if self._introspection is None:
            self._introspection = HANASchemaIntrospection(self._get_pool())
        return self._introspection

    async def get_sources(self) -> list[SourceInfo]:
        """List configured HANA source connections."""
        status = "disconnected"
        if settings.hana_host:
            try:
                pool = self._get_pool()
                await pool.execute_query("SELECT 1 FROM DUMMY")
                status = "connected"
            except Exception as e:
                logger.warning(f"HANA health check failed: {e}")
                status = "error"

        return [
            SourceInfo(
                id="hana-sidecar-01",
                name="HANA Sidecar",
                host=settings.hana_host or "not configured",
                port=settings.hana_port,
                status=status,
            )
        ]

    async def get_tables(
        self, session: AsyncSession, search: str | None = None
    ) -> list[TableInfo]:
        """Browse/search HANA tables. Uses 24-hour cache."""
        introspection = self._get_introspection()
        return await introspection.get_tables(session, search)

    async def get_fields(
        self, session: AsyncSession, schema_name: str, table_name: str
    ) -> list[FieldInfo]:
        """Get field details for a specific table."""
        introspection = self._get_introspection()
        return await introspection.get_fields(session, schema_name, table_name)

    async def get_relationships(
        self, session: AsyncSession, schema_name: str, table_name: str
    ) -> list[RelationshipInfo]:
        """Get FK relationships for a table."""
        introspection = self._get_introspection()
        return await introspection.get_relationships(
            session, schema_name, table_name
        )


# Singleton
schema_service = SchemaService()
