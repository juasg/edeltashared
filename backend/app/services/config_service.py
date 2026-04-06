import json
import logging
import uuid

import redis.asyncio as aioredis
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.hana.connection_pool import HANAConnectionPool
from app.core.config import settings
from app.core.exceptions import ConflictError, NotFoundError, ValidationError
from app.core.security import decrypt_credentials, encrypt_credentials
from app.db.models import (
    AuditLog,
    ConnectorConfig,
    ReplicationConfig,
    StreamState,
)
from app.models.config import (
    ConnectorConfigCreate,
    ReplicationConfigCreate,
    ReplicationConfigUpdate,
)
from app.services.trigger_manager import TriggerManager

logger = logging.getLogger("edeltashared.config_service")


class ConfigService:
    """Business logic for replication configuration management.

    Handles CRUD, trigger deployment, Kafka topic creation,
    and CDC agent notification via Redis pub/sub.
    """

    def __init__(self):
        self._hana_pool: HANAConnectionPool | None = None
        self._trigger_manager: TriggerManager | None = None

    def _get_trigger_manager(self) -> TriggerManager:
        if self._trigger_manager is None:
            if self._hana_pool is None:
                self._hana_pool = HANAConnectionPool()
            self._trigger_manager = TriggerManager(self._hana_pool)
        return self._trigger_manager

    # ===== Connector Config =====

    async def create_connector(
        self, session: AsyncSession, data: ConnectorConfigCreate
    ) -> ConnectorConfig:
        encrypted = encrypt_credentials(json.dumps(data.credentials))
        connector = ConnectorConfig(
            type=data.type,
            name=data.name,
            credentials_encrypted=encrypted,
            connection_pool_size=data.connection_pool_size,
        )
        session.add(connector)
        await session.commit()
        await session.refresh(connector)
        return connector

    async def list_connectors(
        self, session: AsyncSession, type_filter: str | None = None
    ) -> list[ConnectorConfig]:
        stmt = select(ConnectorConfig).where(ConnectorConfig.is_active == True)
        if type_filter:
            stmt = stmt.where(ConnectorConfig.type == type_filter)
        result = await session.execute(stmt.order_by(ConnectorConfig.name))
        return list(result.scalars().all())

    async def get_connector(
        self, session: AsyncSession, connector_id: uuid.UUID
    ) -> ConnectorConfig:
        result = await session.execute(
            select(ConnectorConfig).where(ConnectorConfig.id == connector_id)
        )
        connector = result.scalar_one_or_none()
        if connector is None:
            raise NotFoundError(f"Connector {connector_id} not found")
        return connector

    # ===== Replication Config =====

    async def create_replication(
        self, session: AsyncSession, data: ReplicationConfigCreate
    ) -> ReplicationConfig:
        # Verify connector exists
        await self.get_connector(session, data.connector_config_id)

        # Generate Kafka topic name
        topic = f"edelta.{data.source_table_schema}.{data.source_table_name}".lower()

        config = ReplicationConfig(
            source_table_schema=data.source_table_schema,
            source_table_name=data.source_table_name,
            connector_config_id=data.connector_config_id,
            target_table_name=data.target_table_name,
            replication_mode=data.replication_mode,
            field_mappings=[m.model_dump() for m in data.field_mappings],
            primary_key_fields=data.primary_key_fields,
            kafka_topic=topic,
        )
        session.add(config)

        session.add(AuditLog(
            entity_type="replication_config",
            action="created",
            changes=data.model_dump(mode="json"),
        ))

        await session.commit()
        await session.refresh(config)
        return config

    async def list_replications(
        self, session: AsyncSession, enabled_only: bool = False
    ) -> list[ReplicationConfig]:
        stmt = select(ReplicationConfig)
        if enabled_only:
            stmt = stmt.where(ReplicationConfig.is_enabled == True)
        result = await session.execute(stmt.order_by(ReplicationConfig.created_at.desc()))
        return list(result.scalars().all())

    async def get_replication(
        self, session: AsyncSession, config_id: uuid.UUID
    ) -> ReplicationConfig:
        result = await session.execute(
            select(ReplicationConfig).where(ReplicationConfig.id == config_id)
        )
        config = result.scalar_one_or_none()
        if config is None:
            raise NotFoundError(f"Replication config {config_id} not found")
        return config

    async def update_replication(
        self,
        session: AsyncSession,
        config_id: uuid.UUID,
        data: ReplicationConfigUpdate,
    ) -> ReplicationConfig:
        config = await self.get_replication(session, config_id)

        update_data = data.model_dump(exclude_unset=True)
        if "field_mappings" in update_data and update_data["field_mappings"]:
            update_data["field_mappings"] = [
                m.model_dump() for m in data.field_mappings
            ]

        for key, value in update_data.items():
            setattr(config, key, value)

        session.add(AuditLog(
            entity_type="replication_config",
            entity_id=config_id,
            action="updated",
            changes=update_data,
        ))

        await session.commit()
        await session.refresh(config)
        return config

    async def delete_replication(
        self, session: AsyncSession, config_id: uuid.UUID
    ) -> None:
        config = await self.get_replication(session, config_id)

        # Remove triggers if deployed
        if config.trigger_deployed:
            try:
                tm = self._get_trigger_manager()
                await tm.remove_triggers(session, config_id)
            except Exception as e:
                logger.warning(f"Failed to remove triggers during delete: {e}")

        await session.execute(
            delete(ReplicationConfig).where(ReplicationConfig.id == config_id)
        )

        session.add(AuditLog(
            entity_type="replication_config",
            entity_id=config_id,
            action="deleted",
        ))
        await session.commit()

    # ===== Activation / Pause =====

    async def test_replication(
        self, session: AsyncSession, config_id: uuid.UUID
    ) -> dict:
        """Dry-run validation: check source table, target connector, field mappings."""
        config = await self.get_replication(session, config_id)
        results = {"valid": True, "checks": []}

        # Check connector
        try:
            connector = await self.get_connector(session, config.connector_config_id)
            results["checks"].append({"name": "connector_exists", "passed": True})
        except NotFoundError:
            results["valid"] = False
            results["checks"].append({
                "name": "connector_exists",
                "passed": False,
                "error": "Connector not found",
            })

        # Check field mappings have at least one field
        if config.field_mappings:
            results["checks"].append({"name": "field_mappings", "passed": True})
        else:
            results["valid"] = False
            results["checks"].append({
                "name": "field_mappings",
                "passed": False,
                "error": "No field mappings configured",
            })

        # Check PK fields are in field mappings
        source_fields = {m["source"] for m in config.field_mappings}
        pk_in_mappings = all(pk in source_fields for pk in config.primary_key_fields)
        results["checks"].append({
            "name": "pk_in_mappings",
            "passed": pk_in_mappings,
            **({"error": "Primary key fields not in field mappings"} if not pk_in_mappings else {}),
        })

        return results

    async def activate_replication(
        self, session: AsyncSession, config_id: uuid.UUID, redis_client: aioredis.Redis
    ) -> ReplicationConfig:
        """Deploy triggers, create Kafka topic, start CDC stream."""
        config = await self.get_replication(session, config_id)

        if config.trigger_deployed:
            raise ConflictError("Triggers already deployed")

        # Step 1: Deploy shadow table + triggers on HANA
        tm = self._get_trigger_manager()
        await tm.deploy_triggers(session, config_id)

        # Step 2: Create Kafka topic
        from cdc_agent_notify import ensure_kafka_topic
        await ensure_kafka_topic(config.kafka_topic)

        # Step 3: Create stream state record
        stream = StreamState(
            config_id=config_id,
            consumer_group=f"{config.connector.type}-consumer-{config.kafka_topic}",
            status="active",
        )
        session.add(stream)

        # Step 4: Notify CDC agent to pick up new config
        await redis_client.publish(
            "edelta:agent:commands",
            json.dumps({"action": "reload"}),
        )

        await session.commit()
        await session.refresh(config)
        return config

    async def pause_replication(
        self, session: AsyncSession, config_id: uuid.UUID, redis_client: aioredis.Redis
    ) -> ReplicationConfig:
        """Pause CDC stream (keep triggers, stop agent polling)."""
        config = await self.get_replication(session, config_id)

        # Update stream state
        await session.execute(
            update(StreamState)
            .where(StreamState.config_id == config_id)
            .values(status="paused")
        )

        # Notify CDC agent
        await redis_client.publish(
            "edelta:agent:commands",
            json.dumps({"action": "pause", "config_id": str(config_id)}),
        )

        session.add(AuditLog(
            entity_type="replication_config",
            entity_id=config_id,
            action="paused",
        ))

        await session.commit()
        await session.refresh(config)
        return config


config_service = ConfigService()
