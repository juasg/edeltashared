import logging
import uuid

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.hana.connection_pool import HANAConnectionPool
from app.connectors.hana.trigger_ddl import TriggerDDLGenerator
from app.db.models import AuditLog, ReplicationConfig

logger = logging.getLogger("edeltashared.trigger_manager")


class TriggerManager:
    """Orchestrates deployment and removal of HANA CDC triggers.

    Manages the lifecycle:
    1. Create shadow table
    2. Create INSERT/UPDATE/DELETE triggers
    3. Track deployment state in metadata DB
    4. Clean removal on deactivation
    """

    def __init__(self, pool: HANAConnectionPool):
        self.pool = pool
        self.ddl_gen = TriggerDDLGenerator()

    async def deploy_triggers(
        self,
        session: AsyncSession,
        config_id: uuid.UUID,
    ) -> None:
        """Deploy shadow table and triggers for a replication config."""
        # Load config
        result = await session.execute(
            select(ReplicationConfig).where(ReplicationConfig.id == config_id)
        )
        config = result.scalar_one_or_none()
        if config is None:
            raise ValueError(f"Replication config {config_id} not found")

        if config.trigger_deployed:
            logger.info(
                f"Triggers already deployed for {config.source_table_schema}."
                f"{config.source_table_name}"
            )
            return

        schema = config.source_table_schema
        table = config.source_table_name
        pk_fields = config.primary_key_fields
        monitored_cols = [m["source"] for m in config.field_mappings]

        logger.info(f"Deploying triggers for {schema}.{table}")

        # Step 1: Create shadow table
        shadow_ddl = self.ddl_gen.generate_shadow_table(schema, table)
        for statement in self._split_statements(shadow_ddl):
            await self.pool.execute_statement(statement)
        logger.info(f"Shadow table created: _EDELTA_CDC_{table}")

        # Step 2: Create triggers
        trigger_ddl = self.ddl_gen.generate_triggers(
            schema, table, pk_fields, monitored_cols
        )
        for statement in self._split_statements(trigger_ddl):
            await self.pool.execute_statement(statement)
        logger.info(f"Triggers created for {schema}.{table}")

        # Step 3: Update metadata
        await session.execute(
            update(ReplicationConfig)
            .where(ReplicationConfig.id == config_id)
            .values(trigger_deployed=True)
        )

        # Step 4: Audit log
        session.add(
            AuditLog(
                entity_type="replication_config",
                entity_id=config_id,
                action="triggers_deployed",
                changes={
                    "schema": schema,
                    "table": table,
                    "pk_fields": pk_fields,
                    "monitored_columns": monitored_cols,
                },
            )
        )
        await session.commit()

    async def remove_triggers(
        self,
        session: AsyncSession,
        config_id: uuid.UUID,
    ) -> None:
        """Remove triggers and shadow table for a replication config."""
        result = await session.execute(
            select(ReplicationConfig).where(ReplicationConfig.id == config_id)
        )
        config = result.scalar_one_or_none()
        if config is None:
            raise ValueError(f"Replication config {config_id} not found")

        if not config.trigger_deployed:
            logger.info("No triggers to remove")
            return

        schema = config.source_table_schema
        table = config.source_table_name

        logger.info(f"Removing triggers for {schema}.{table}")

        drop_ddl = self.ddl_gen.generate_drop(schema, table)
        for statement in self._split_statements(drop_ddl):
            try:
                await self.pool.execute_statement(statement)
            except Exception as e:
                # Log but continue — trigger may not exist
                logger.warning(f"Drop statement failed (may be expected): {e}")

        # Update metadata
        await session.execute(
            update(ReplicationConfig)
            .where(ReplicationConfig.id == config_id)
            .values(trigger_deployed=False)
        )

        session.add(
            AuditLog(
                entity_type="replication_config",
                entity_id=config_id,
                action="triggers_removed",
                changes={"schema": schema, "table": table},
            )
        )
        await session.commit()

    def _split_statements(self, ddl: str) -> list[str]:
        """Split a multi-statement DDL string into individual statements."""
        statements = []
        current = []
        for line in ddl.split("\n"):
            stripped = line.strip()
            if stripped.startswith("--") or not stripped:
                continue
            current.append(line)
            if stripped.endswith(";"):
                stmt = "\n".join(current).strip().rstrip(";")
                if stmt:
                    statements.append(stmt)
                current = []
        # Handle remaining content without trailing semicolon
        if current:
            stmt = "\n".join(current).strip().rstrip(";")
            if stmt:
                statements.append(stmt)
        return statements
