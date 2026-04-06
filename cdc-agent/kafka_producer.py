import json
import logging
import hashlib
from typing import Any

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import Counter, Histogram

from config import settings

logger = logging.getLogger("cdc-agent.kafka")

# Prometheus metrics
MESSAGES_PRODUCED = Counter(
    "edelta_kafka_messages_produced_total",
    "Total Kafka messages produced",
    ["topic"],
)
PRODUCE_DURATION = Histogram(
    "edelta_kafka_produce_duration_ms",
    "Time to produce a batch to Kafka in ms",
    ["topic"],
)
PRODUCE_ERRORS = Counter(
    "edelta_kafka_produce_errors_total",
    "Total Kafka produce errors",
    ["topic"],
)


class CDCKafkaProducer:
    """Publishes CDC events to Kafka with delivery guarantees.

    - Idempotent producer (exactly-once per partition)
    - Partitioned by primary key hash for row-level ordering
    - Explicit topic management (no auto-create)
    """

    def __init__(self):
        self._producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 10,
            "linger.ms": 50,
            "batch.size": 65536,
            "compression.type": "lz4",
        })
        self._admin = AdminClient({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
        })
        self._delivery_errors: list[str] = []

    def _delivery_callback(self, err, msg):
        """Called once per message to confirm delivery."""
        if err is not None:
            error_msg = f"Delivery failed for {msg.topic()}: {err}"
            logger.error(error_msg)
            self._delivery_errors.append(error_msg)
            PRODUCE_ERRORS.labels(topic=msg.topic()).inc()
        else:
            MESSAGES_PRODUCED.labels(topic=msg.topic()).inc()

    def _partition_key(self, key_dict: dict) -> bytes:
        """Generate a consistent partition key from the row's primary key."""
        key_str = "|".join(str(v) for v in sorted(key_dict.values()))
        return hashlib.md5(key_str.encode()).digest()

    async def ensure_topic(
        self, topic: str, num_partitions: int = 6, replication_factor: int = 1
    ) -> None:
        """Create a Kafka topic if it doesn't exist."""
        metadata = self._admin.list_topics(timeout=10)
        if topic in metadata.topics:
            logger.info(f"Topic {topic} already exists")
            return

        new_topic = NewTopic(
            topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config={"cleanup.policy": "compact,delete", "retention.ms": "604800000"},
        )
        futures = self._admin.create_topics([new_topic])
        for t, future in futures.items():
            try:
                future.result()
                logger.info(f"Created topic: {t}")
            except Exception as e:
                if "TopicExistsException" not in str(type(e).__name__):
                    raise

    def produce_batch(self, topic: str, events: list[dict]) -> int:
        """Produce a batch of CDC events to Kafka.

        Returns the number of successfully queued messages.
        Blocks until all delivery callbacks are received.
        """
        self._delivery_errors.clear()
        queued = 0

        with PRODUCE_DURATION.labels(topic=topic).time():
            for event in events:
                key_bytes = self._partition_key(event["key"])
                value_bytes = json.dumps(event).encode("utf-8")

                self._producer.produce(
                    topic=topic,
                    key=key_bytes,
                    value=value_bytes,
                    callback=self._delivery_callback,
                )
                queued += 1

                # Serve delivery callbacks periodically to avoid buffer overflow
                if queued % 500 == 0:
                    self._producer.poll(0)

            # Flush all pending messages and wait for delivery
            self._producer.flush(timeout=30)

        if self._delivery_errors:
            logger.error(
                f"Batch had {len(self._delivery_errors)} delivery errors"
            )

        return queued - len(self._delivery_errors)

    def flush(self):
        """Flush any remaining messages."""
        self._producer.flush(timeout=10)

    def close(self):
        """Flush and clean up."""
        self.flush()
