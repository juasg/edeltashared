"""Snowflake connector for the control plane.

Handles connection validation, table creation, and health checks.
Actual CDC consumption is handled by the standalone Snowflake consumer.
"""

import json
import logging

import snowflake.connector

from app.connectors.base import (
    ApplyResult,
    CDCEvent,
    DataConnector,
    HealthStatus,
    LoadResult,
    TableSchema,
)
from app.core.security import decrypt_credentials

logger = logging.getLogger("edeltashared.connectors.snowflake")


class SnowflakeConnector(DataConnector):
    def __init__(self, encrypted_credentials: str):
        creds = json.loads(decrypt_credentials(encrypted_credentials))
        self._account = creds["account"]
        self._user = creds["user"]
        self._password = creds.get("password", "")
        self._warehouse = creds.get("warehouse", "")
        self._database = creds.get("database", "")
        self._schema = creds.get("schema", "PUBLIC")
        self._conn = None

    async def connect(self) -> None:
        self._conn = snowflake.connector.connect(
            account=self._account,
            user=self._user,
            password=self._password,
            warehouse=self._warehouse,
            database=self._database,
            schema=self._schema,
        )

    async def disconnect(self) -> None:
        if self._conn:
            self._conn.close()

    async def validate_connection(self) -> bool:
        try:
            if self._conn is None:
                await self.connect()
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Snowflake validation failed: {e}")
            return False

    async def ensure_table(self, schema: TableSchema) -> None:
        columns = [f'"{col["name"]}" VARCHAR' for col in schema.columns]
        columns.extend([
            '"_edelta_op" VARCHAR(1)',
            '"_edelta_ts" TIMESTAMP_NTZ',
            '"_edelta_seq" NUMBER(19,0)',
        ])
        pk = ", ".join(f'"{k}"' for k in schema.primary_keys)
        ddl = (
            f'CREATE TABLE IF NOT EXISTS "{schema.table_name}" (\n'
            f'  {", ".join(columns)},\n'
            f'  PRIMARY KEY ({pk})\n'
            f')'
        )
        cursor = self._conn.cursor()
        cursor.execute(ddl)
        cursor.close()

    async def apply_changes(self, changes: list[CDCEvent]) -> ApplyResult:
        # Snowflake CDC is handled by the standalone consumer
        raise NotImplementedError("Use the Snowflake consumer for CDC apply")

    async def bulk_load(self, records: list[dict], chunk_id: str) -> LoadResult:
        # Used by initial load service
        if not records:
            return LoadResult(chunk_id=chunk_id)

        cursor = self._conn.cursor()
        written = 0
        try:
            for record in records:
                cols = list(record.keys())
                col_names = ", ".join(f'"{c}"' for c in cols)
                placeholders = ", ".join(["%s"] * len(cols))
                cursor.execute(
                    f'INSERT INTO "{list(record.keys())[0]}" ({col_names}) VALUES ({placeholders})',
                    list(record.values()),
                )
                written += 1
        except Exception as e:
            return LoadResult(records_written=written, records_failed=len(records) - written,
                              chunk_id=chunk_id, errors=[str(e)])
        finally:
            cursor.close()
        return LoadResult(records_written=written, chunk_id=chunk_id)

    async def get_row_count(self, table: str) -> int:
        cursor = self._conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    async def health_check(self) -> HealthStatus:
        try:
            valid = await self.validate_connection()
            return HealthStatus.HEALTHY if valid else HealthStatus.UNHEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY
