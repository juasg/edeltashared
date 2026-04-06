"""
MongoDB CDC consumer — applies changes via bulkWrite with upserts.

Reads CDC events from Kafka, accumulates a batch, then executes
a bulk write operation on MongoDB with upsert semantics per document.
"""

import asyncio
import logging
import time

from pymongo import MongoClient, UpdateOne, DeleteOne
from pydantic_settings import BaseSettings
from prometheus_client import start_http_server

from base_consumer import BaseCDCConsumer, BatchResult, CDCMessage

logger = logging.getLogger("consumer.mongodb")


class MongoDBSettings(BaseSettings):
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "edeltashared"
    kafka_bootstrap_servers: str = "localhost:9092"
    consumer_topics: str = ""
    consumer_group: str = "mongodb-consumer-1"
    consumer_batch_size: int = 500
    consumer_batch_window: float = 3.0
    postgres_dsn: str = "host=localhost dbname=edeltashared user=edeltashared password=changeme_postgres"
    config_id: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


class MongoDBCDCConsumer(BaseCDCConsumer):
    """Applies CDC events to MongoDB using bulkWrite with upserts."""

    consumer_type = "mongodb"

    def __init__(self, mongo_settings: MongoDBSettings):
        topics = [t.strip() for t in mongo_settings.consumer_topics.split(",") if t.strip()]
        super().__init__(
            kafka_servers=mongo_settings.kafka_bootstrap_servers,
            topics=topics,
            group_id=mongo_settings.consumer_group,
            batch_size=mongo_settings.consumer_batch_size,
            batch_window_seconds=mongo_settings.consumer_batch_window,
        )
        self.mongo_settings = mongo_settings
        self._client = None
        self._db = None

    async def connect_target(self) -> None:
        loop = asyncio.get_event_loop()
        self._client = await loop.run_in_executor(
            None, MongoClient, self.mongo_settings.mongodb_uri
        )
        self._db = self._client[self.mongo_settings.mongodb_database]
        logger.info(f"Connected to MongoDB: {self.mongo_settings.mongodb_database}")

    async def disconnect_target(self) -> None:
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")

    async def ensure_target_table(
        self, schema: str, table: str, sample_msg: CDCMessage
    ) -> None:
        # MongoDB creates collections implicitly — create an index on the PK fields
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, self._ensure_index, table, list(sample_msg.key.keys())
        )

    def _ensure_index(self, collection_name: str, pk_fields: list[str]) -> None:
        collection = self._db[collection_name]
        index_keys = [(f, 1) for f in pk_fields]
        collection.create_index(index_keys, unique=True, name="edelta_pk_idx")

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
                    None, self._bulk_write, table, table_msgs
                )
                total_applied += result
            except Exception as e:
                total_failed += len(table_msgs)
                errors.append(f"{table}: {e}")
                logger.error(f"bulkWrite failed for {table}: {e}")

        return BatchResult(applied=total_applied, failed=total_failed, errors=errors)

    def _bulk_write(self, collection_name: str, messages: list[CDCMessage]) -> int:
        collection = self._db[collection_name]
        operations = []

        for msg in messages:
            # Build filter from primary key
            pk_filter = {k: v for k, v in msg.key.items()}

            if msg.op == "D":
                operations.append(DeleteOne(pk_filter))
            elif msg.op in ("I", "U"):
                if msg.after is None:
                    continue

                # Build document with data + edelta metadata
                doc = dict(msg.after)
                doc["_edelta_op"] = msg.op
                doc["_edelta_ts"] = msg.ts_ms
                doc["_edelta_seq"] = msg.seq
                doc["_edelta_updated_at"] = int(time.time() * 1000)

                operations.append(
                    UpdateOne(
                        pk_filter,
                        {"$set": doc},
                        upsert=True,
                    )
                )

        if not operations:
            return 0

        result = collection.bulk_write(operations, ordered=False)

        applied = (
            result.inserted_count
            + result.modified_count
            + result.upserted_count
            + result.deleted_count
        )

        logger.debug(
            f"bulkWrite {collection_name}: "
            f"inserted={result.inserted_count} modified={result.modified_count} "
            f"upserted={result.upserted_count} deleted={result.deleted_count}"
        )

        return applied


def main():
    import signal
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    start_http_server(9093)

    mongo_settings = MongoDBSettings()
    consumer = MongoDBCDCConsumer(mongo_settings)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(consumer.run())
    loop.close()


if __name__ == "__main__":
    main()
