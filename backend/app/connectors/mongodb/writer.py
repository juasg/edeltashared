"""MongoDB connector for the control plane.

Handles connection validation, collection management, and health checks
for MongoDB targets configured in the control plane.
"""

import json
import logging
import time

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne, DeleteOne

from app.connectors.base import (
    ApplyResult,
    CDCEvent,
    DataConnector,
    HealthStatus,
    LoadResult,
    TableSchema,
)
from app.core.security import decrypt_credentials

logger = logging.getLogger("edeltashared.connectors.mongodb")


class MongoDBConnector(DataConnector):
    def __init__(self, encrypted_credentials: str):
        creds = json.loads(decrypt_credentials(encrypted_credentials))
        self._uri = creds["uri"]
        self._database_name = creds["database"]
        self._client = None
        self._db = None

    async def connect(self) -> None:
        self._client = AsyncIOMotorClient(self._uri)
        self._db = self._client[self._database_name]
        logger.info(f"Connected to MongoDB: {self._database_name}")

    async def disconnect(self) -> None:
        if self._client:
            self._client.close()

    async def validate_connection(self) -> bool:
        try:
            if self._client is None:
                await self.connect()
            await self._client.admin.command("ping")
            return True
        except Exception as e:
            logger.error(f"MongoDB validation failed: {e}")
            return False

    async def ensure_table(self, schema: TableSchema) -> None:
        """Create unique index on PK fields."""
        collection = self._db[schema.table_name]
        index_keys = [(pk, 1) for pk in schema.primary_keys]
        await collection.create_index(index_keys, unique=True, name="edelta_pk_idx")

    async def apply_changes(self, changes: list[CDCEvent]) -> ApplyResult:
        result = ApplyResult()

        # Group by collection (table)
        by_collection: dict[str, list] = {}
        for event in changes:
            by_collection.setdefault(event.table, []).append(event)

        for collection_name, events in by_collection.items():
            collection = self._db[collection_name]
            operations = []

            for event in events:
                pk_filter = {k: v for k, v in event.key.items()}

                if event.op == "D":
                    operations.append(DeleteOne(pk_filter))
                    result.deleted += 1
                elif event.op in ("I", "U") and event.after:
                    doc = dict(event.after)
                    doc["_edelta_op"] = event.op
                    doc["_edelta_ts"] = event.ts_ms
                    doc["_edelta_seq"] = event.seq
                    doc["_edelta_updated_at"] = int(time.time() * 1000)

                    operations.append(UpdateOne(pk_filter, {"$set": doc}, upsert=True))
                    if event.op == "I":
                        result.inserted += 1
                    else:
                        result.updated += 1

            if operations:
                try:
                    await collection.bulk_write(operations, ordered=False)
                except Exception as e:
                    result.failed += len(operations)
                    result.errors.append(str(e))
                    logger.error(f"MongoDB bulkWrite error: {e}")

        return result

    async def bulk_load(self, records: list[dict], chunk_id: str) -> LoadResult:
        if not records:
            return LoadResult(chunk_id=chunk_id)

        # Assume all records are for the same collection
        # The caller should group by collection before calling
        collection_name = chunk_id.split(":")[0] if ":" in chunk_id else "bulk_load"
        collection = self._db[collection_name]

        try:
            result = await collection.insert_many(records, ordered=False)
            return LoadResult(
                records_written=len(result.inserted_ids),
                chunk_id=chunk_id,
            )
        except Exception as e:
            return LoadResult(
                records_failed=len(records),
                chunk_id=chunk_id,
                errors=[str(e)],
            )

    async def get_row_count(self, table: str) -> int:
        collection = self._db[table]
        return await collection.count_documents({})

    async def health_check(self) -> HealthStatus:
        try:
            valid = await self.validate_connection()
            return HealthStatus.HEALTHY if valid else HealthStatus.UNHEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY
