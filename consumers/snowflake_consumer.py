"""
Snowflake CDC consumer — applies changes via micro-batch MERGE.

Reads CDC events from Kafka, accumulates a batch, then executes
a MERGE INTO statement on Snowflake to upsert/delete rows.
"""

import asyncio
import logging
import time
from functools import partial

import snowflake.connector
from pydantic_settings import BaseSettings
from prometheus_client import start_http_server

from base_consumer import BaseCDCConsumer, BatchResult, CDCMessage

logger = logging.getLogger("consumer.snowflake")

# HANA type -> Snowflake type mapping
TYPE_MAP = {
    "INTEGER": "NUMBER(10,0)",
    "BIGINT": "NUMBER(19,0)",
    "SMALLINT": "NUMBER(5,0)",
    "TINYINT": "NUMBER(3,0)",
    "DECIMAL": "NUMBER(38,10)",
    "DOUBLE": "FLOAT",
    "REAL": "FLOAT",
    "VARCHAR": "VARCHAR",
    "NVARCHAR": "VARCHAR",
    "CHAR": "CHAR",
    "NCHAR": "CHAR",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "BOOLEAN": "BOOLEAN",
    "BLOB": "BINARY",
    "CLOB": "VARCHAR(16777216)",
    "NCLOB": "VARCHAR(16777216)",
}


class SnowflakeSettings(BaseSettings):
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_private_key_path: str = ""
    snowflake_password: str = ""
    snowflake_warehouse: str = ""
    snowflake_database: str = ""
    snowflake_schema: str = "PUBLIC"
    kafka_bootstrap_servers: str = "localhost:9092"
    consumer_topics: str = ""  # Comma-separated
    consumer_group: str = "snowflake-consumer-1"
    consumer_batch_size: int = 500
    consumer_batch_window: float = 5.0
    postgres_dsn: str = "host=localhost dbname=edeltashared user=edeltashared password=changeme_postgres"
    config_id: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


class SnowflakeCDCConsumer(BaseCDCConsumer):
    """Applies CDC events to Snowflake using MERGE INTO."""

    consumer_type = "snowflake"

    def __init__(self, sf_settings: SnowflakeSettings):
        topics = [t.strip() for t in sf_settings.consumer_topics.split(",") if t.strip()]
        super().__init__(
            kafka_servers=sf_settings.kafka_bootstrap_servers,
            topics=topics,
            group_id=sf_settings.consumer_group,
            batch_size=sf_settings.consumer_batch_size,
            batch_window_seconds=sf_settings.consumer_batch_window,
        )
        self.sf_settings = sf_settings
        self._conn = None
        self._ensured_tables: set[str] = set()

    async def connect_target(self) -> None:
        connect_args = {
            "account": self.sf_settings.snowflake_account,
            "user": self.sf_settings.snowflake_user,
            "warehouse": self.sf_settings.snowflake_warehouse,
            "database": self.sf_settings.snowflake_database,
            "schema": self.sf_settings.snowflake_schema,
        }
        if self.sf_settings.snowflake_password:
            connect_args["password"] = self.sf_settings.snowflake_password

        loop = asyncio.get_event_loop()
        self._conn = await loop.run_in_executor(
            None, partial(snowflake.connector.connect, **connect_args)
        )
        logger.info(f"Connected to Snowflake: {self.sf_settings.snowflake_database}")

    async def disconnect_target(self) -> None:
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed")

    async def ensure_target_table(
        self, schema: str, table: str, sample_msg: CDCMessage
    ) -> None:
        table_key = f"{schema}.{table}"
        if table_key in self._ensured_tables:
            return

        if sample_msg.after is None:
            return

        # Build CREATE TABLE from sample message fields
        columns = []
        for col_name in sample_msg.after.keys():
            columns.append(f'"{col_name}" VARCHAR')

        # Add edelta metadata columns
        columns.extend([
            '"_edelta_op" VARCHAR(1)',
            '"_edelta_ts" TIMESTAMP_NTZ',
            '"_edelta_seq" NUMBER(19,0)',
        ])

        # Build PK constraint
        pk_cols = ", ".join(f'"{k}"' for k in sample_msg.key.keys())

        ddl = (
            f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
            f'  {", ".join(columns)},\n'
            f'  PRIMARY KEY ({pk_cols})\n'
            f')'
        )

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._execute_ddl, ddl)
        self._ensured_tables.add(table_key)
        logger.info(f"Ensured target table: {table}")

    def _execute_ddl(self, ddl: str) -> None:
        cursor = self._conn.cursor()
        try:
            cursor.execute(ddl)
        finally:
            cursor.close()

    async def apply_batch(self, messages: list[CDCMessage]) -> BatchResult:
        """Apply a batch of CDC events using MERGE INTO."""
        if not messages:
            return BatchResult()

        # Group by table
        by_table: dict[str, list[CDCMessage]] = {}
        for msg in messages:
            by_table.setdefault(msg.table, []).append(msg)

        total_applied = 0
        total_failed = 0
        errors = []

        for table, table_msgs in by_table.items():
            # Ensure table exists
            await self.ensure_target_table(
                table_msgs[0].schema, table, table_msgs[0]
            )

            try:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, self._merge_batch, table, table_msgs
                )
                total_applied += result
            except Exception as e:
                total_failed += len(table_msgs)
                errors.append(f"{table}: {e}")
                logger.error(f"MERGE failed for {table}: {e}")

        return BatchResult(applied=total_applied, failed=total_failed, errors=errors)

    def _merge_batch(self, table: str, messages: list[CDCMessage]) -> int:
        """Execute MERGE INTO for a batch of messages targeting one table."""
        if not messages:
            return 0

        # Separate deletes from upserts
        deletes = [m for m in messages if m.op == "D"]
        upserts = [m for m in messages if m.op in ("I", "U")]

        applied = 0
        cursor = self._conn.cursor()

        try:
            # Handle deletes
            if deletes:
                for msg in deletes:
                    where_parts = []
                    values = []
                    for k, v in msg.key.items():
                        where_parts.append(f'"{k}" = %s')
                        values.append(v)

                    delete_sql = f'DELETE FROM "{table}" WHERE {" AND ".join(where_parts)}'
                    cursor.execute(delete_sql, values)
                    applied += 1

            # Handle upserts via MERGE
            if upserts:
                # Get all column names from the first message
                sample = upserts[0]
                if sample.after is None:
                    return applied

                all_cols = list(sample.after.keys())
                pk_cols = list(sample.key.keys())
                non_pk_cols = [c for c in all_cols if c not in pk_cols]

                # Add metadata columns
                meta_cols = ["_edelta_op", "_edelta_ts", "_edelta_seq"]

                # Build VALUES rows
                value_rows = []
                bind_values = []
                for msg in upserts:
                    if msg.after is None:
                        continue
                    row_vals = []
                    for col in all_cols:
                        row_vals.append(msg.after.get(col))
                        bind_values.append(msg.after.get(col))
                    # Metadata
                    bind_values.extend([msg.op, msg.ts_ms, msg.seq])
                    placeholders = ", ".join(["%s"] * (len(all_cols) + 3))
                    value_rows.append(f"({placeholders})")

                if not value_rows:
                    return applied

                src_cols = [f'"{c}"' for c in all_cols + meta_cols]
                src_cols_str = ", ".join(src_cols)

                values_clause = ", ".join(value_rows)

                # ON condition (match by PK)
                on_parts = [f'target."{pk}" = src."{pk}"' for pk in pk_cols]
                on_clause = " AND ".join(on_parts)

                # UPDATE SET for non-PK columns + metadata
                update_cols = non_pk_cols + meta_cols
                update_parts = [f'target."{c}" = src."{c}"' for c in update_cols]
                update_clause = ", ".join(update_parts)

                # INSERT columns and values
                insert_cols = ", ".join(f'"{c}"' for c in all_cols + meta_cols)
                insert_vals = ", ".join(f'src."{c}"' for c in all_cols + meta_cols)

                merge_sql = (
                    f'MERGE INTO "{table}" AS target '
                    f'USING (SELECT {src_cols_str} FROM VALUES {values_clause} '
                    f'AS t({src_cols_str})) AS src '
                    f'ON {on_clause} '
                    f'WHEN MATCHED THEN UPDATE SET {update_clause} '
                    f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
                )

                cursor.execute(merge_sql, bind_values)
                applied += len(upserts)

        finally:
            cursor.close()

        return applied


def main():
    import signal
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Start Prometheus metrics
    start_http_server(9091)

    sf_settings = SnowflakeSettings()
    consumer = SnowflakeCDCConsumer(sf_settings)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(consumer.run())
    loop.close()


if __name__ == "__main__":
    main()
