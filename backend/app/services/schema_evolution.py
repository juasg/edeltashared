"""
Schema evolution detector — compares current HANA schema against cached
field mappings to detect new columns, removed columns, and type changes.

When changes are detected:
1. Log to schema_changes table
2. For additive changes (new columns): auto-apply ALTER on target if configured
3. For breaking changes (removed columns, type changes): alert and await manual review
"""

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.hana.schema_introspection import HANASchemaIntrospection
from app.db.models import ReplicationConfig
from app.db.models.data_quality import SchemaChange
from app.models.schema import FieldInfo

logger = logging.getLogger("edeltashared.schema_evolution")


class SchemaEvolutionService:
    """Detects and manages schema changes between source and config."""

    def __init__(self, introspection: HANASchemaIntrospection):
        self.introspection = introspection

    async def detect_changes(
        self, session: AsyncSession, config_id: uuid.UUID
    ) -> list[dict]:
        """Compare current HANA schema with stored field mappings.

        Returns list of detected changes.
        """
        result = await session.execute(
            select(ReplicationConfig).where(ReplicationConfig.id == config_id)
        )
        config = result.scalar_one_or_none()
        if config is None:
            return []

        # Get current fields from HANA (or cache)
        current_fields = await self.introspection.get_fields(
            session, config.source_table_schema, config.source_table_name
        )
        current_by_name = {f.column_name: f for f in current_fields}

        # Get mapped source fields from config
        mapped_fields = {m["source"] for m in config.field_mappings}

        changes = []

        # Detect new columns (in HANA but not in mapping)
        for col_name, field in current_by_name.items():
            if col_name not in mapped_fields:
                change = {
                    "change_type": "column_added",
                    "column_name": col_name,
                    "old_definition": None,
                    "new_definition": {
                        "data_type": field.data_type,
                        "length": field.length,
                        "is_nullable": field.is_nullable,
                        "is_primary_key": field.is_primary_key,
                    },
                }
                changes.append(change)

        # Detect removed columns (in mapping but not in HANA)
        for source_field in mapped_fields:
            if source_field not in current_by_name:
                change = {
                    "change_type": "column_removed",
                    "column_name": source_field,
                    "old_definition": next(
                        (m for m in config.field_mappings if m["source"] == source_field),
                        None,
                    ),
                    "new_definition": None,
                }
                changes.append(change)

        # Detect type changes (compare with cached field info)
        # This requires the previous field definitions to be stored
        # For now, we detect based on field_mappings having transform hints

        # Persist detected changes
        for change in changes:
            schema_change = SchemaChange(
                config_id=config_id,
                change_type=change["change_type"],
                column_name=change["column_name"],
                old_definition=change["old_definition"],
                new_definition=change["new_definition"],
            )
            session.add(schema_change)

        if changes:
            await session.commit()
            logger.info(
                f"Detected {len(changes)} schema changes for "
                f"{config.source_table_schema}.{config.source_table_name}"
            )

        return changes

    async def auto_resolve_additive(
        self, session: AsyncSession, change_id: uuid.UUID
    ) -> bool:
        """Auto-resolve an additive schema change (new column).

        Adds the column to field mappings with auto-mapped target name.
        """
        result = await session.execute(
            select(SchemaChange).where(SchemaChange.id == change_id)
        )
        change = result.scalar_one_or_none()
        if change is None or change.change_type != "column_added":
            return False

        # Add to field mappings
        config_result = await session.execute(
            select(ReplicationConfig).where(ReplicationConfig.id == change.config_id)
        )
        config = config_result.scalar_one_or_none()
        if config is None:
            return False

        new_mapping = {
            "source": change.column_name,
            "target": change.column_name.lower(),
            "transform": None,
        }
        updated_mappings = list(config.field_mappings) + [new_mapping]

        await session.execute(
            update(ReplicationConfig)
            .where(ReplicationConfig.id == config.id)
            .values(field_mappings=updated_mappings)
        )

        change.status = "applied"
        change.auto_resolved = True
        change.resolved_at = datetime.now(timezone.utc)

        await session.commit()
        logger.info(f"Auto-resolved: added column {change.column_name} to mappings")
        return True

    async def get_pending_changes(
        self, session: AsyncSession, config_id: uuid.UUID | None = None
    ) -> list[SchemaChange]:
        stmt = select(SchemaChange).where(SchemaChange.status == "pending")
        if config_id:
            stmt = stmt.where(SchemaChange.config_id == config_id)
        stmt = stmt.order_by(SchemaChange.detected_at.desc())
        result = await session.execute(stmt)
        return list(result.scalars().all())
