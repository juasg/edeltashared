"""
Aurora PostgreSQL CDC consumer — applies changes via INSERT ON CONFLICT (upsert).

Reads CDC events from Kafka, accumulates a batch, then executes
upsert statements on Aurora PostgreSQL.
"""

import asyncio
import logging
from functools import partial

import psycopg2
import psycopg2.extras
from pydantic_settings import BaseSettings
from prometheus_client import start_http_server

from base_consumer import BaseCDCConsumer, BatchResult, CDCMessage

logger = logging.getLogger("consumer.aurora")

# HANA type → Aurora PostgreSQL type mapping
TYPE_MAP = {
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "SMALLINT",
    "DECIMAL": "NUMERIC(38,10)",
    "DOUBLE": "DOUBLE PRECISION",
    "REAL": "REAL",
    "VARCHAR": "VARCHAR",
    "NVARCHAR": "VARCHAR",
    "CHAR": "CHAR",
    "NCHAR": "CHAR",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "BOOLEAN": "BOOLEAN",
    "BLOB": "BYTEA",
    "CLOB": "TEXT",
    "NCLOB": "TEXT",
}


class AuroraSettings(BaseSettings):
    aurora_host: str = "localhost"
    aurora_port: int = 5432
    aurora_database: str = "edeltashared_target"
    aurora_user: str = "aurora_user"
    aurora_password: str = ""
    aurora_schema: str = "public"
    kafka_bootstrap_servers: str = "localhost:9092"
    consumer_topics: str = ""
    consumer_group: str = "aurora-consumer-1"
    consumer_batch_size: int = 500
    consumer_batch_window: float = 5.0
    postgres_dsn: str = "host=localhost dbname=edeltashared user=edeltashared password=changeme_postgres"
    config_id: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


class AuroraCDCConsumer(BaseCDCConsumer):
    """Applies CDC events to Aurora PostgreSQL using INSERT ON CONFLICT."""

    consumer_type = "aurora"

    def __init__(self, aurora_settings: AuroraSettings):
        topics = [t.strip() for t in aurora_settings.consumer_topics.split(",") if t.strip()]
        super().__init__(
            kafka_servers=aurora_settings.kafka_bootstrap_servers,
            topics=topics,
            group_id=aurora_settings.consumer_group,
            batch_size=aurora_settings.consumer_batch_size,
            batch_window_seconds=aurora_settings.consumer_batch_window,
        )
        self.aurora_settings = aurora_settings
        self._conn = None
        self._ensured_tables: set[str] = set()

    async def connect_target(self) -> None:
        loop = asyncio.get_event_loop()
        self._conn = await loop.run_in_executor(None, self._connect_sync)
        logger.info(f"Connected to Aurora: {self.aurora_settings.aurora_database}")

    def _connect_sync(self):
        conn = psycopg2.connect(
            host=self.aurora_settings.aurora_host,
            port=self.aurora_settings.aurora_port,
            dbname=self.aurora_settings.aurora_database,
            user=self.aurora_settings.aurora_user,
            password=self.aurora_settings.aurora_password,
        )
        conn.autocommit = False
        return conn

    async def disconnect_target(self) -> None:
        if self._conn:
            self._conn.close()
            logger.info("Aurora connection closed")

    async def ensure_target_table(
        self, schema: str, table: str, sample_msg: CDCMessage
    ) -> None:
        table_key = f"{schema}.{table}"
        if table_key in self._ensured_tables:
            return

        if sample_msg.after is None:
            return

        columns = []
        for col_name in sample_msg.after.keys():
            columns.append(f'"{col_name}" VARCHAR')

        # Metadata columns
        columns.extend([
            '"_edelta_op" VARCHAR(1)',
            '"_edelta_ts" BIGINT',
            '"_edelta_seq" BIGINT',
        ])

        pk_cols = ", ".join(f'"{k}"' for k in sample_msg.key.keys())

        target_schema = self.aurora_settings.aurora_schema
        ddl = (
            f'CREATE TABLE IF NOT EXISTS "{target_schema}"."{table}" (\n'
            f'  {", ".join(columns)},\n'
            f'  PRIMARY KEY ({pk_cols})\n'
            f')'
        )

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._execute_ddl, ddl)
        self._ensured_tables.add(table_key)
        logger.info(f"Ensured target table: {target_schema}.{table}")

    def _execute_ddl(self, ddl: str) -> None:
        self._conn.autocommit = True
        cursor = self._conn.cursor()
        try:
            cursor.execute(ddl)
        finally:
            cursor.close()
            self._conn.autocommit = False

    async def apply_batch(self, messages: list[CDCMessage]) -> BatchResult:
        if not messages:
            return BatchResult()

        by_table: dict[str, list[CDCMessage]] = {}
        for msg in messages:
            by_table.setdefault(msg.table, []).append(msg)

        total_applied = 0
        total_failed = 0
        errors = []

        for table, table_msgs in by_table.items():
            await self.ensure_target_table(
                table_msgs[0].schema, table, table_msgs[0]
            )
            try:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, self._upsert_batch, table, table_msgs
                )
                total_applied += result
            except Exception as e:
                total_failed += len(table_msgs)
                errors.append(f"{table}: {e}")
                logger.error(f"Upsert failed for {table}: {e}")
                try:
                    self._conn.rollback()
                except Exception:
                    pass

        return BatchResult(applied=total_applied, failed=total_failed, errors=errors)

    def _upsert_batch(self, table: str, messages: list[CDCMessage]) -> int:
        if not messages:
            return 0

        deletes = [m for m in messages if m.op == "D"]
        upserts = [m for m in messages if m.op in ("I", "U")]

        applied = 0
        target_schema = self.aurora_settings.aurora_schema
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

                    cursor.execute(
                        f'DELETE FROM "{target_schema}"."{table}" WHERE {" AND ".join(where_parts)}',
                        values,
                    )
                    applied += 1

            # Handle upserts via INSERT ON CONFLICT
            if upserts:
                sample = upserts[0]
                if sample.after is None:
                    self._conn.commit()
                    return applied

                all_cols = list(sample.after.keys())
                pk_cols = list(sample.key.keys())
                non_pk_cols = [c for c in all_cols if c not in pk_cols]
                meta_cols = ["_edelta_op", "_edelta_ts", "_edelta_seq"]
                insert_cols = all_cols + meta_cols

                col_names = ", ".join(f'"{c}"' for c in insert_cols)
                placeholders = ", ".join(["%s"] * len(insert_cols))
                pk_constraint = ", ".join(f'"{c}"' for c in pk_cols)

                update_set = ", ".join(
                    f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols + meta_cols
                )

                sql = (
                    f'INSERT INTO "{target_schema}"."{table}" ({col_names}) '
                    f"VALUES ({placeholders}) "
                    f"ON CONFLICT ({pk_constraint}) "
                    f"DO UPDATE SET {update_set}"
                )

                for msg in upserts:
                    if msg.after is None:
                        continue
                    values = [msg.after.get(c) for c in all_cols]
                    values.extend([msg.op, msg.ts_ms, msg.seq])
                    cursor.execute(sql, values)
                    applied += 1

            self._conn.commit()

        except Exception:
            self._conn.rollback()
            raise
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

    start_http_server(9092)

    aurora_settings = AuroraSettings()
    consumer = AuroraCDCConsumer(aurora_settings)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(consumer.run())
    loop.close()


if __name__ == "__main__":
    main()
