"""
Offset tracker for exactly-once semantics.

Persists Kafka offsets to the PostgreSQL metadata DB alongside
stream state. On consumer restart, seeks to the last committed offset.
"""

import logging
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

logger = logging.getLogger("consumer.offset_tracker")


class OffsetTracker:
    def __init__(self, postgres_dsn: str, config_id: str, consumer_group: str):
        self.config_id = config_id
        self.consumer_group = consumer_group
        self._conn = psycopg2.connect(postgres_dsn)
        self._conn.autocommit = True
        self._records_batch = 0
        self._latency_samples: list[int] = []

    def get_last_offset(self) -> int | None:
        """Get the last committed Kafka offset for this consumer."""
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT last_kafka_offset FROM stream_state "
            "WHERE config_id = %s AND consumer_group = %s",
            (self.config_id, self.consumer_group),
        )
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else None

    def commit_offset(self, offset: int, records_in_batch: int) -> None:
        """Persist the Kafka offset and update stream metrics."""
        self._records_batch += records_in_batch

        cursor = self._conn.cursor()
        cursor.execute(
            "UPDATE stream_state SET "
            "  last_kafka_offset = %s, "
            "  records_processed_total = records_processed_total + %s, "
            "  last_processed_at = %s, "
            "  status = 'active', "
            "  updated_at = %s "
            "WHERE config_id = %s AND consumer_group = %s",
            (
                offset,
                records_in_batch,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                self.config_id,
                self.consumer_group,
            ),
        )
        cursor.close()

    def record_latency(self, latency_ms: int) -> None:
        """Collect latency sample for periodic aggregation."""
        self._latency_samples.append(latency_ms)

    def flush_latency_metrics(self, target_type: str) -> None:
        """Aggregate and persist latency metrics to latency_metrics table."""
        if not self._latency_samples:
            return

        samples = sorted(self._latency_samples)
        n = len(samples)

        p50 = samples[int(n * 0.5)]
        p95 = samples[int(n * 0.95)]
        p99 = samples[int(n * 0.99)]
        max_val = samples[-1]

        now = datetime.now(timezone.utc)

        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT INTO latency_metrics "
            "(config_id, target_type, p50_latency_ms, p95_latency_ms, "
            " p99_latency_ms, max_latency_ms, records_in_window, "
            " window_start, window_end) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                self.config_id,
                target_type,
                p50,
                p95,
                p99,
                max_val,
                n,
                now,
                now,
            ),
        )
        cursor.close()

        logger.info(
            f"Latency metrics flushed: p50={p50}ms p95={p95}ms p99={p99}ms "
            f"max={max_val}ms ({n} samples)"
        )
        self._latency_samples.clear()

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
