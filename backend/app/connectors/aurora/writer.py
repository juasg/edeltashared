"""Aurora PostgreSQL connector for the control plane.

Handles connection validation, table creation, and health checks
for Aurora targets configured in the control plane.
"""

import json
import logging

import psycopg2

from app.connectors.base import (
    ApplyResult,
    CDCEvent,
    DataConnector,
    HealthStatus,
    LoadResult,
    TableSchema,
)
from app.core.security import decrypt_credentials

logger = logging.getLogger("edeltashared.connectors.aurora")


class AuroraConnector(DataConnector):
    def __init__(self, encrypted_credentials: str, target_schema: str = "public"):
        creds = json.loads(decrypt_credentials(encrypted_credentials))
        self._host = creds["host"]
        self._port = creds.get("port", 5432)
        self._database = creds["database"]
        self._user = creds["user"]
        self._password = creds["password"]
        self._schema = target_schema
        self._conn = None

    async def connect(self) -> None:
        self._conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._database,
            user=self._user,
            password=self._password,
        )
        self._conn.autocommit = False

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
            logger.error(f"Aurora validation failed: {e}")
            return False

    async def ensure_table(self, schema: TableSchema) -> None:
        columns = []
        for col in schema.columns:
            nullable = "" if col.get("nullable", True) else " NOT NULL"
            columns.append(f'"{col["name"]}" VARCHAR{nullable}')

        columns.extend([
            '"_edelta_op" VARCHAR(1)',
            '"_edelta_ts" BIGINT',
            '"_edelta_seq" BIGINT',
        ])

        pk = ", ".join(f'"{k}"' for k in schema.primary_keys)

        ddl = (
            f'CREATE TABLE IF NOT EXISTS "{self._schema}"."{schema.table_name}" (\n'
            f'  {", ".join(columns)},\n'
            f'  PRIMARY KEY ({pk})\n'
            f')'
        )

        self._conn.autocommit = True
        cursor = self._conn.cursor()
        cursor.execute(ddl)
        cursor.close()
        self._conn.autocommit = False

    async def apply_changes(self, changes: list[CDCEvent]) -> ApplyResult:
        result = ApplyResult()
        cursor = self._conn.cursor()

        try:
            for event in changes:
                if event.op == "D":
                    where = " AND ".join(f'"{k}" = %s' for k in event.key)
                    cursor.execute(
                        f'DELETE FROM "{self._schema}"."{event.table}" WHERE {where}',
                        list(event.key.values()),
                    )
                    result.deleted += 1

                elif event.op in ("I", "U") and event.after:
                    cols = list(event.after.keys()) + ["_edelta_op", "_edelta_ts", "_edelta_seq"]
                    vals = list(event.after.values()) + [event.op, event.ts_ms, event.seq]
                    col_names = ", ".join(f'"{c}"' for c in cols)
                    placeholders = ", ".join(["%s"] * len(cols))
                    pk_cols = ", ".join(f'"{k}"' for k in event.key)
                    non_pk = [c for c in cols if c not in event.key]
                    update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk)

                    sql = (
                        f'INSERT INTO "{self._schema}"."{event.table}" ({col_names}) '
                        f"VALUES ({placeholders}) "
                        f"ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set}"
                    )
                    cursor.execute(sql, vals)

                    if event.op == "I":
                        result.inserted += 1
                    else:
                        result.updated += 1

            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            result.failed += 1
            result.errors.append(str(e))

        cursor.close()
        return result

    async def bulk_load(self, records: list[dict], chunk_id: str) -> LoadResult:
        if not records:
            return LoadResult(chunk_id=chunk_id)

        # Use COPY for bulk loading
        cursor = self._conn.cursor()
        written = 0
        failed = 0

        try:
            for record in records:
                cols = list(record.keys())
                col_names = ", ".join(f'"{c}"' for c in cols)
                placeholders = ", ".join(["%s"] * len(cols))
                cursor.execute(
                    f'INSERT INTO "{self._schema}"."{list(record.keys())[0]}" ({col_names}) VALUES ({placeholders})',
                    list(record.values()),
                )
                written += 1
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            failed = len(records) - written
            return LoadResult(records_written=written, records_failed=failed,
                              chunk_id=chunk_id, errors=[str(e)])

        cursor.close()
        return LoadResult(records_written=written, chunk_id=chunk_id)

    async def get_row_count(self, table: str) -> int:
        cursor = self._conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{self._schema}"."{table}"')
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    async def health_check(self) -> HealthStatus:
        try:
            valid = await self.validate_connection()
            return HealthStatus.HEALTHY if valid else HealthStatus.UNHEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY
