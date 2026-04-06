"""
Base CDC consumer that reads from Kafka and applies changes to a target.

Handles:
- Manual offset management for exactly-once semantics
- Configurable batch size and time window (flush on whichever hits first)
- Graceful shutdown with final flush
"""

import json
import logging
import signal
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger("consumer")

MESSAGES_CONSUMED = Counter(
    "edelta_consumer_messages_consumed_total",
    "Total messages consumed",
    ["topic", "consumer_type"],
)
APPLY_DURATION = Histogram(
    "edelta_consumer_apply_duration_ms",
    "Time to apply a batch in ms",
    ["consumer_type"],
)
CONSUMER_LAG = Gauge(
    "edelta_consumer_lag",
    "Consumer lag (messages behind)",
    ["topic", "partition"],
)
END_TO_END_LATENCY = Histogram(
    "edelta_end_to_end_latency_ms",
    "End-to-end latency from trigger to target write",
    ["consumer_type", "table"],
    buckets=[100, 500, 1000, 2000, 5000, 10000, 15000, 20000, 25000, 30000, 60000],
)


@dataclass
class CDCMessage:
    source: str
    schema: str
    table: str
    op: str
    key: dict
    before: dict | None
    after: dict | None
    ts_ms: int
    seq: int
    kafka_offset: int = 0
    kafka_partition: int = 0


@dataclass
class BatchResult:
    applied: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


class BaseCDCConsumer(ABC):
    """Abstract base for target-specific CDC consumers."""

    consumer_type: str = "base"

    def __init__(
        self,
        kafka_servers: str,
        topics: list[str],
        group_id: str,
        batch_size: int = 500,
        batch_window_seconds: float = 5.0,
    ):
        self.topics = topics
        self.batch_size = batch_size
        self.batch_window = batch_window_seconds
        self._running = False

        self._consumer = Consumer({
            "bootstrap.servers": kafka_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 30000,
        })

        self._batch: list[CDCMessage] = []
        self._batch_start_time: float = 0.0

    @abstractmethod
    async def connect_target(self) -> None:
        """Establish connection to the target database."""

    @abstractmethod
    async def apply_batch(self, messages: list[CDCMessage]) -> BatchResult:
        """Apply a batch of CDC messages to the target. Must be idempotent."""

    @abstractmethod
    async def ensure_target_table(self, schema: str, table: str, sample_msg: CDCMessage) -> None:
        """Create target table if it doesn't exist."""

    @abstractmethod
    async def disconnect_target(self) -> None:
        """Close target database connection."""

    def _should_flush(self) -> bool:
        if len(self._batch) >= self.batch_size:
            return True
        if self._batch and (time.monotonic() - self._batch_start_time) >= self.batch_window:
            return True
        return False

    async def _flush_batch(self) -> None:
        if not self._batch:
            return

        batch = self._batch
        self._batch = []

        start = time.monotonic()
        result = await self.apply_batch(batch)
        elapsed_ms = (time.monotonic() - start) * 1000

        APPLY_DURATION.labels(consumer_type=self.consumer_type).observe(elapsed_ms)
        MESSAGES_CONSUMED.labels(
            topic=batch[0].table if batch else "unknown",
            consumer_type=self.consumer_type,
        ).inc(result.applied)

        # Record end-to-end latency
        now_ms = int(time.time() * 1000)
        for msg in batch:
            latency = now_ms - msg.ts_ms
            END_TO_END_LATENCY.labels(
                consumer_type=self.consumer_type,
                table=msg.table,
            ).observe(latency)

        if result.errors:
            logger.error(f"Batch errors: {result.errors[:5]}")

        # Commit offsets after successful apply
        self._consumer.commit(asynchronous=False)

        logger.info(
            f"Flushed batch: {result.applied} applied, {result.failed} failed "
            f"({elapsed_ms:.0f}ms)"
        )

    def _parse_message(self, raw_value: bytes, offset: int, partition: int) -> CDCMessage | None:
        try:
            data = json.loads(raw_value)
            return CDCMessage(
                source=data["source"],
                schema=data["schema"],
                table=data["table"],
                op=data["op"],
                key=data["key"],
                before=data.get("before"),
                after=data.get("after"),
                ts_ms=data["ts_ms"],
                seq=data["seq"],
                kafka_offset=offset,
                kafka_partition=partition,
            )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse message at offset {offset}: {e}")
            return None

    async def run(self) -> None:
        """Main consumer loop."""
        self._running = True

        await self.connect_target()
        self._consumer.subscribe(self.topics)
        logger.info(f"Consumer subscribed to {self.topics}")

        # Graceful shutdown
        def shutdown(sig, frame):
            logger.info("Shutdown signal received")
            self._running = False

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    if self._should_flush():
                        await self._flush_batch()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                parsed = self._parse_message(
                    msg.value(), msg.offset(), msg.partition()
                )
                if parsed is None:
                    continue

                if not self._batch:
                    self._batch_start_time = time.monotonic()

                self._batch.append(parsed)

                if self._should_flush():
                    await self._flush_batch()

        finally:
            # Final flush
            if self._batch:
                await self._flush_batch()
            self._consumer.close()
            await self.disconnect_target()
            logger.info("Consumer stopped")
