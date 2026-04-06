"""
CDC event processing pipeline — wires quality validation and transformations
into the consumer flow.

Pipeline order per event:
1. Parse Kafka message → CDCMessage
2. Validate against data quality rules
3. Apply field transformations (including PII masking)
4. Pass to consumer.apply_batch()

Loads rules and transforms from metadata DB on startup and
periodically refreshes them.
"""

import logging
import time

import psycopg2
import psycopg2.extras

from quality_engine import QualityEngine, Violation
from transform_engine import TransformEngine
from base_consumer import CDCMessage

logger = logging.getLogger("consumer.pipeline")


class EventPipeline:
    """Processes CDC events through quality checks and transformations."""

    def __init__(self, postgres_dsn: str, config_id: str):
        self._dsn = postgres_dsn
        self._config_id = config_id
        self._quality: QualityEngine | None = None
        self._transform: TransformEngine | None = None
        self._last_reload = 0.0
        self._reload_interval = 60.0  # Refresh rules every 60s
        self._conn = None

    def _get_conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn)
            self._conn.autocommit = True
        return self._conn

    def reload_if_needed(self) -> None:
        """Refresh rules and transforms from metadata DB if interval elapsed."""
        now = time.monotonic()
        if now - self._last_reload < self._reload_interval:
            return
        self._last_reload = now
        self._load_rules()
        self._load_transforms()

    def _load_rules(self) -> None:
        conn = self._get_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "SELECT id::text, field_name, rule_type, rule_params, severity, is_enabled "
            "FROM data_quality_rules WHERE config_id = %s AND is_enabled = TRUE",
            (self._config_id,),
        )
        rules = [dict(r) for r in cursor.fetchall()]
        cursor.close()
        self._quality = QualityEngine(rules, self._config_id)
        logger.info(f"Loaded {len(rules)} quality rules for config {self._config_id[:8]}")

    def _load_transforms(self) -> None:
        conn = self._get_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "SELECT source_field, transform_type, transform_params, execution_order "
            "FROM field_transformations WHERE config_id = %s AND is_enabled = TRUE "
            "ORDER BY execution_order",
            (self._config_id,),
        )
        transforms = [dict(r) for r in cursor.fetchall()]
        cursor.close()
        self._transform = TransformEngine(transforms)
        logger.info(f"Loaded {len(transforms)} transforms for config {self._config_id[:8]}")

    def process(self, message: CDCMessage) -> CDCMessage | None:
        """Run a CDC message through the pipeline.

        Returns the (possibly transformed) message, or None if rejected.
        """
        self.reload_if_needed()

        # Step 1: Quality validation
        if self._quality and self._quality.has_rules:
            result = self._quality.validate(
                message.after, "|".join(str(v) for v in message.key.values()), message.seq
            )
            if result.violations:
                self._persist_violations(result.violations)
                if result.should_reject:
                    logger.warning(
                        f"Event seq={message.seq} rejected by quality rules "
                        f"({len(result.violations)} violations)"
                    )
                    return None

        # Step 2: Transformations (including PII masking)
        if self._transform and message.after:
            message.after = self._transform.apply(message.after)

        return message

    def process_batch(self, messages: list[CDCMessage]) -> list[CDCMessage]:
        """Process a batch of messages, filtering out rejected ones."""
        self.reload_if_needed()
        result = []
        for msg in messages:
            processed = self.process(msg)
            if processed is not None:
                result.append(processed)
        return result

    def _persist_violations(self, violations: list[Violation]) -> None:
        """Write violations to the database."""
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            for v in violations:
                cursor.execute(
                    "INSERT INTO data_quality_violations "
                    "(rule_id, config_id, field_name, field_value, row_key, "
                    " violation_type, severity, message, event_seq) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        v.rule_id or None,
                        v.config_id or None,
                        v.field_name,
                        v.field_value,
                        v.row_key,
                        v.violation_type,
                        v.severity,
                        v.message,
                        v.event_seq,
                    ),
                )
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to persist violations: {e}")
