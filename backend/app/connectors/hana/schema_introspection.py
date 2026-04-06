import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.hana.connection_pool import HANAConnectionPool
from app.db.models import SchemaCache
from app.models.schema import FieldInfo, RelationshipInfo, TableInfo

logger = logging.getLogger("edeltashared.hana.schema")

CACHE_TTL_HOURS = 24


class HANASchemaIntrospection:
    """Introspects HANA schema with 24-hour caching in PostgreSQL.

    Never queries HANA during report hours if cache is valid.
    """

    def __init__(self, pool: HANAConnectionPool, source_id: str = "default"):
        self.pool = pool
        self.source_id = source_id

    async def get_tables(
        self, session: AsyncSession, search: str | None = None
    ) -> list[TableInfo]:
        """List HANA tables, optionally filtered by search term."""
        # Check cache first
        cached = await self._get_cached_tables(session)
        if cached is not None:
            tables = cached
            if search:
                search_upper = search.upper()
                tables = [
                    t for t in tables if search_upper in t.table_name.upper()
                ]
            return tables

        # Cache miss — query HANA
        sql = """
            SELECT SCHEMA_NAME, TABLE_NAME, TABLE_TYPE,
                   RECORD_COUNT
            FROM SYS.TABLES
            WHERE SCHEMA_NAME NOT LIKE 'SYS%'
              AND SCHEMA_NAME NOT LIKE '_SYS%'
            ORDER BY SCHEMA_NAME, TABLE_NAME
        """
        rows = await self.pool.execute_query(sql)

        tables = [
            TableInfo(
                schema_name=row["SCHEMA_NAME"],
                table_name=row["TABLE_NAME"],
                table_type=row.get("TABLE_TYPE", "TABLE"),
                row_count=row.get("RECORD_COUNT"),
                has_primary_key=True,
            )
            for row in rows
        ]

        # Cache results
        await self._cache_tables(session, tables)

        if search:
            search_upper = search.upper()
            tables = [t for t in tables if search_upper in t.table_name.upper()]

        return tables

    async def get_fields(
        self, session: AsyncSession, schema_name: str, table_name: str
    ) -> list[FieldInfo]:
        """Get column details for a specific table."""
        cache_key = f"{schema_name}.{table_name}"

        # Check cache
        cached = await self._get_cached_fields(session, cache_key)
        if cached is not None:
            return cached

        # Query HANA for columns
        sql = """
            SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE,
                   IS_NULLABLE, DEFAULT_VALUE
            FROM SYS.TABLE_COLUMNS
            WHERE SCHEMA_NAME = ?
              AND TABLE_NAME = ?
            ORDER BY POSITION
        """
        rows = await self.pool.execute_query(sql, (schema_name, table_name))

        # Get primary key columns
        pk_sql = """
            SELECT COLUMN_NAME
            FROM SYS.CONSTRAINTS
            WHERE SCHEMA_NAME = ?
              AND TABLE_NAME = ?
              AND IS_PRIMARY_KEY = 'TRUE'
        """
        pk_rows = await self.pool.execute_query(pk_sql, (schema_name, table_name))
        pk_columns = {row["COLUMN_NAME"] for row in pk_rows}

        fields = [
            FieldInfo(
                column_name=row["COLUMN_NAME"],
                data_type=row["DATA_TYPE_NAME"],
                length=row.get("LENGTH"),
                scale=row.get("SCALE"),
                is_nullable=row.get("IS_NULLABLE", "TRUE") == "TRUE",
                is_primary_key=row["COLUMN_NAME"] in pk_columns,
                default_value=row.get("DEFAULT_VALUE"),
            )
            for row in rows
        ]

        # Cache
        await self._cache_fields(session, cache_key, fields)
        return fields

    async def get_relationships(
        self, session: AsyncSession, schema_name: str, table_name: str
    ) -> list[RelationshipInfo]:
        """Get foreign key relationships for a table."""
        sql = """
            SELECT CONSTRAINT_NAME,
                   COLUMN_NAME,
                   REFERENCED_SCHEMA_NAME,
                   REFERENCED_TABLE_NAME,
                   REFERENCED_COLUMN_NAME
            FROM SYS.REFERENTIAL_CONSTRAINTS
            WHERE SCHEMA_NAME = ?
              AND TABLE_NAME = ?
        """
        rows = await self.pool.execute_query(sql, (schema_name, table_name))

        return [
            RelationshipInfo(
                constraint_name=row["CONSTRAINT_NAME"],
                source_column=row["COLUMN_NAME"],
                referenced_schema=row["REFERENCED_SCHEMA_NAME"],
                referenced_table=row["REFERENCED_TABLE_NAME"],
                referenced_column=row["REFERENCED_COLUMN_NAME"],
            )
            for row in rows
        ]

    # ===== Cache helpers =====

    async def _get_cached_tables(
        self, session: AsyncSession
    ) -> list[TableInfo] | None:
        result = await session.execute(
            select(SchemaCache).where(
                SchemaCache.source_id == self.source_id,
                SchemaCache.table_name == "__tables_index__",
                SchemaCache.expires_at > datetime.now(timezone.utc),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None

        return [TableInfo(**t) for t in row.table_spec]

    async def _cache_tables(
        self, session: AsyncSession, tables: list[TableInfo]
    ) -> None:
        now = datetime.now(timezone.utc)
        cache_entry = SchemaCache(
            source_id=self.source_id,
            table_name="__tables_index__",
            table_spec=[t.model_dump() for t in tables],
            cached_at=now,
            expires_at=now + timedelta(hours=CACHE_TTL_HOURS),
        )
        await session.merge(cache_entry)
        await session.commit()

    async def _get_cached_fields(
        self, session: AsyncSession, cache_key: str
    ) -> list[FieldInfo] | None:
        result = await session.execute(
            select(SchemaCache).where(
                SchemaCache.source_id == self.source_id,
                SchemaCache.table_name == cache_key,
                SchemaCache.expires_at > datetime.now(timezone.utc),
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None

        return [FieldInfo(**f) for f in row.fields]

    async def _cache_fields(
        self, session: AsyncSession, cache_key: str, fields: list[FieldInfo]
    ) -> None:
        now = datetime.now(timezone.utc)
        cache_entry = SchemaCache(
            source_id=self.source_id,
            table_name=cache_key,
            fields=[f.model_dump() for f in fields],
            cached_at=now,
            expires_at=now + timedelta(hours=CACHE_TTL_HOURS),
        )
        await session.merge(cache_entry)
        await session.commit()
