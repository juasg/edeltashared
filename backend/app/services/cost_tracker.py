"""
Cost tracking service — aggregates stream usage metrics and computes costs.

Tracks per stream:
- Records processed
- Bytes transferred
- Compute seconds
- Cost by component (Kafka, target writes, storage)
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import StreamState
from app.db.models.enterprise import StreamCost

logger = logging.getLogger("edeltashared.costs")

# Cost per unit (configurable, example pricing)
COST_PER_MILLION_RECORDS = 0.50
COST_PER_GB_TRANSFERRED = 0.10
COST_PER_COMPUTE_HOUR = 0.05


class CostTracker:
    async def record_batch_cost(
        self,
        session: AsyncSession,
        config_id: str,
        org_id: str | None,
        records: int,
        bytes_size: int,
        compute_ms: float,
        target_type: str,
    ) -> None:
        """Record cost for a single batch operation."""
        now = datetime.now(timezone.utc)
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)

        # Check for existing hourly bucket
        result = await session.execute(
            select(StreamCost).where(
                StreamCost.config_id == config_id,
                StreamCost.period_start == hour_start,
                StreamCost.target_type == target_type,
            )
        )
        cost_record = result.scalar_one_or_none()

        compute_seconds = compute_ms / 1000.0
        records_cost = (records / 1_000_000) * COST_PER_MILLION_RECORDS
        bytes_cost = (bytes_size / (1024 ** 3)) * COST_PER_GB_TRANSFERRED
        compute_cost = (compute_seconds / 3600) * COST_PER_COMPUTE_HOUR
        total_cost = records_cost + bytes_cost + compute_cost

        if cost_record:
            cost_record.records_processed += records
            cost_record.bytes_transferred += bytes_size
            cost_record.compute_seconds = float(cost_record.compute_seconds) + compute_seconds
            cost_record.cost_usd = float(cost_record.cost_usd) + total_cost
        else:
            cost_record = StreamCost(
                config_id=config_id,
                org_id=org_id,
                period_start=hour_start,
                period_end=hour_end,
                records_processed=records,
                bytes_transferred=bytes_size,
                compute_seconds=compute_seconds,
                target_type=target_type,
                cost_usd=total_cost,
                breakdown={
                    "records": round(records_cost, 4),
                    "transfer": round(bytes_cost, 4),
                    "compute": round(compute_cost, 4),
                },
            )
            session.add(cost_record)

        await session.commit()

    async def get_cost_summary(
        self,
        session: AsyncSession,
        org_id: str | None = None,
        days: int = 30,
    ) -> dict:
        """Get aggregated cost summary."""
        since = datetime.now(timezone.utc) - timedelta(days=days)

        stmt = select(
            StreamCost.target_type,
            func.sum(StreamCost.records_processed).label("total_records"),
            func.sum(StreamCost.bytes_transferred).label("total_bytes"),
            func.sum(StreamCost.cost_usd).label("total_cost"),
        ).where(
            StreamCost.period_start >= since
        ).group_by(StreamCost.target_type)

        if org_id:
            stmt = stmt.where(StreamCost.org_id == org_id)

        result = await session.execute(stmt)
        rows = result.all()

        by_target = {}
        total_cost = 0.0

        for row in rows:
            by_target[row[0] or "unknown"] = {
                "records": row[1] or 0,
                "bytes": row[2] or 0,
                "cost_usd": round(float(row[3] or 0), 4),
            }
            total_cost += float(row[3] or 0)

        return {
            "period_days": days,
            "total_cost_usd": round(total_cost, 4),
            "by_target": by_target,
        }

    async def get_daily_costs(
        self,
        session: AsyncSession,
        config_id: str | None = None,
        days: int = 30,
    ) -> list[dict]:
        """Get daily cost breakdown for charting."""
        since = datetime.now(timezone.utc) - timedelta(days=days)

        stmt = select(
            func.date_trunc("day", StreamCost.period_start).label("day"),
            StreamCost.target_type,
            func.sum(StreamCost.cost_usd).label("cost"),
            func.sum(StreamCost.records_processed).label("records"),
        ).where(
            StreamCost.period_start >= since
        ).group_by("day", StreamCost.target_type).order_by("day")

        if config_id:
            stmt = stmt.where(StreamCost.config_id == config_id)

        result = await session.execute(stmt)
        return [
            {
                "date": row[0].isoformat() if row[0] else None,
                "target_type": row[1],
                "cost_usd": round(float(row[2] or 0), 4),
                "records": row[3] or 0,
            }
            for row in result.all()
        ]


cost_tracker = CostTracker()
