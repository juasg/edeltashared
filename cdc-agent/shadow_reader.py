import json
import logging
import time
from dataclasses import dataclass

from prometheus_client import Gauge, Histogram

from config import settings
from db_connector import DBConnector

logger = logging.getLogger("cdc-agent.shadow_reader")

# Prometheus metrics
SHADOW_ROWS_PENDING = Gauge(
    "edelta_cdc_shadow_rows_pending",
    "Unconsumed rows in shadow tables",
    ["schema", "table"],
)
POLL_DURATION = Histogram(
    "edelta_cdc_agent_poll_duration_ms",
    "Time to poll shadow table in ms",
    ["schema", "table"],
)


@dataclass
class ShadowRow:
    seq_id: int
    op_type: str
    row_key: str
    changed_at: str
    changed_fields: dict | None
    ts_ms: int


class ShadowReader:
    """Reads CDC shadow tables from HANA or Postgres simulator.

    Fetches unconsumed rows in batches, converts to CDC events,
    and marks them consumed after Kafka delivery confirmation.
    """

    def __init__(self, connector: DBConnector):
        self._connector = connector
        self.batch_size = settings.cdc_batch_size

    def poll_changes(
        self,
        schema: str,
        table: str,
        last_seq_id: int = 0,
    ) -> list[ShadowRow]:
        """Read unconsumed changes from a shadow table."""
        sql = self._connector.build_poll_sql(schema, table)

        start = time.monotonic()
        conn = self._connector.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (last_seq_id, self.batch_size))
            rows = cursor.fetchall()
        finally:
            cursor.close()

        elapsed_ms = (time.monotonic() - start) * 1000
        POLL_DURATION.labels(schema=schema, table=table).observe(elapsed_ms)

        results = []
        for row in rows:
            seq_id, op_type, row_key, changed_at, changed_fields_raw = row

            changed_fields = None
            if changed_fields_raw:
                try:
                    changed_fields = json.loads(changed_fields_raw)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(
                        f"Invalid JSON in changed_fields for seq_id={seq_id}"
                    )

            ts_ms = int(changed_at.timestamp() * 1000) if changed_at else int(time.time() * 1000)

            results.append(ShadowRow(
                seq_id=seq_id,
                op_type=op_type,
                row_key=row_key,
                changed_at=str(changed_at),
                changed_fields=changed_fields,
                ts_ms=ts_ms,
            ))

        SHADOW_ROWS_PENDING.labels(schema=schema, table=table).set(len(results))
        return results

    def mark_consumed(self, schema: str, table: str, seq_ids: list[int]) -> int:
        """Mark shadow rows as consumed after successful Kafka delivery."""
        if not seq_ids:
            return 0

        sql = self._connector.build_mark_consumed_sql(schema, table, len(seq_ids))

        conn = self._connector.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(seq_ids))
            if not conn.autocommit:
                conn.commit()
            affected = cursor.rowcount
            logger.debug(f"Marked {affected} rows consumed")
            return affected
        finally:
            cursor.close()

    def to_cdc_events(
        self,
        rows: list[ShadowRow],
        source: str,
        schema: str,
        table: str,
        primary_key_fields: list[str],
    ) -> list[dict]:
        """Convert shadow rows to CDC event dicts for Kafka."""
        events = []
        for row in rows:
            # Parse composite key
            key_values = row.row_key.split("|")
            key_dict = {}
            for i, pk_field in enumerate(primary_key_fields):
                key_dict[pk_field] = key_values[i] if i < len(key_values) else None

            event = {
                "source": source,
                "schema": schema,
                "table": table,
                "op": row.op_type,
                "key": key_dict,
                "before": None,
                "after": row.changed_fields if row.op_type != "D" else None,
                "ts_ms": row.ts_ms,
                "seq": row.seq_id,
            }
            events.append(event)

        return events
