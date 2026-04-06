import uuid

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func, update, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.exceptions import NotFoundError
from app.db.models.data_quality import (
    DataQualityRule,
    DataQualityViolation,
    FieldTransformation,
    ColumnLineage,
    SchemaChange,
)
from app.db.session import get_session

router = APIRouter(prefix="/quality", tags=["data-quality"], dependencies=[Depends(get_current_user)])


# ===== Pydantic Models =====

class RuleCreate(BaseModel):
    config_id: uuid.UUID
    field_name: str = Field(..., max_length=255)
    rule_type: str = Field(..., pattern="^(not_null|range|regex|enum|length|custom_sql)$")
    rule_params: dict = Field(default_factory=dict)
    severity: str = Field(default="warn", pattern="^(warn|reject|quarantine)$")
    description: str | None = None


class TransformCreate(BaseModel):
    config_id: uuid.UUID
    source_field: str = Field(..., max_length=255)
    transform_type: str = Field(..., pattern="^(upper|lower|trim|cast|hash|mask|substr|replace|default|custom)$")
    transform_params: dict = Field(default_factory=dict)
    execution_order: int = 0


# ===== Quality Rules =====

@router.post("/rules", status_code=201)
async def create_rule(data: RuleCreate, session: AsyncSession = Depends(get_session)):
    rule = DataQualityRule(**data.model_dump())
    session.add(rule)
    await session.commit()
    await session.refresh(rule)
    return {"id": str(rule.id), **data.model_dump(mode="json")}


@router.get("/rules")
async def list_rules(
    config_id: uuid.UUID | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(DataQualityRule).order_by(DataQualityRule.field_name)
    if config_id:
        stmt = stmt.where(DataQualityRule.config_id == config_id)
    result = await session.execute(stmt)
    rules = result.scalars().all()
    return [
        {
            "id": str(r.id),
            "config_id": str(r.config_id),
            "field_name": r.field_name,
            "rule_type": r.rule_type,
            "rule_params": r.rule_params,
            "severity": r.severity,
            "is_enabled": r.is_enabled,
            "description": r.description,
        }
        for r in rules
    ]


@router.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    result = await session.execute(
        select(DataQualityRule).where(DataQualityRule.id == rule_id)
    )
    rule = result.scalar_one_or_none()
    if rule is None:
        raise NotFoundError("Rule not found")
    await session.delete(rule)
    await session.commit()


# ===== Violations =====

@router.get("/violations")
async def list_violations(
    config_id: uuid.UUID | None = Query(None),
    resolved: bool | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(DataQualityViolation).order_by(desc(DataQualityViolation.created_at))
    if config_id:
        stmt = stmt.where(DataQualityViolation.config_id == config_id)
    if resolved is not None:
        stmt = stmt.where(DataQualityViolation.resolved == resolved)

    count_result = await session.execute(
        select(func.count()).select_from(DataQualityViolation)
    )
    total = count_result.scalar() or 0

    result = await session.execute(stmt.offset(offset).limit(limit))
    violations = result.scalars().all()

    return {
        "items": [
            {
                "id": str(v.id),
                "rule_id": str(v.rule_id),
                "field_name": v.field_name,
                "field_value": v.field_value,
                "row_key": v.row_key,
                "violation_type": v.violation_type,
                "severity": v.severity,
                "message": v.message,
                "event_seq": v.event_seq,
                "resolved": v.resolved,
                "created_at": v.created_at.isoformat() if v.created_at else None,
            }
            for v in violations
        ],
        "total": total,
    }


@router.get("/violations/summary")
async def violation_summary(
    config_id: uuid.UUID | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    """Aggregate violation counts by severity and type."""
    stmt = select(
        DataQualityViolation.severity,
        DataQualityViolation.violation_type,
        func.count().label("count"),
    ).where(DataQualityViolation.resolved == False).group_by(
        DataQualityViolation.severity, DataQualityViolation.violation_type
    )
    if config_id:
        stmt = stmt.where(DataQualityViolation.config_id == config_id)

    result = await session.execute(stmt)
    return [
        {"severity": row[0], "violation_type": row[1], "count": row[2]}
        for row in result.all()
    ]


# ===== Transformations =====

@router.post("/transforms", status_code=201)
async def create_transform(data: TransformCreate, session: AsyncSession = Depends(get_session)):
    transform = FieldTransformation(**data.model_dump())
    session.add(transform)
    await session.commit()
    await session.refresh(transform)
    return {"id": str(transform.id), **data.model_dump(mode="json")}


@router.get("/transforms")
async def list_transforms(
    config_id: uuid.UUID | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(FieldTransformation).order_by(
        FieldTransformation.source_field, FieldTransformation.execution_order
    )
    if config_id:
        stmt = stmt.where(FieldTransformation.config_id == config_id)
    result = await session.execute(stmt)
    return [
        {
            "id": str(t.id),
            "config_id": str(t.config_id),
            "source_field": t.source_field,
            "transform_type": t.transform_type,
            "transform_params": t.transform_params,
            "execution_order": t.execution_order,
            "is_enabled": t.is_enabled,
        }
        for t in result.scalars().all()
    ]


@router.delete("/transforms/{transform_id}", status_code=204)
async def delete_transform(transform_id: uuid.UUID, session: AsyncSession = Depends(get_session)):
    result = await session.execute(
        select(FieldTransformation).where(FieldTransformation.id == transform_id)
    )
    t = result.scalar_one_or_none()
    if t is None:
        raise NotFoundError("Transform not found")
    await session.delete(t)
    await session.commit()


# ===== Schema Changes =====

@router.get("/schema-changes")
async def list_schema_changes(
    config_id: uuid.UUID | None = Query(None),
    status: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(SchemaChange).order_by(desc(SchemaChange.detected_at))
    if config_id:
        stmt = stmt.where(SchemaChange.config_id == config_id)
    if status:
        stmt = stmt.where(SchemaChange.status == status)
    result = await session.execute(stmt.limit(100))
    return [
        {
            "id": str(c.id),
            "config_id": str(c.config_id),
            "change_type": c.change_type,
            "column_name": c.column_name,
            "old_definition": c.old_definition,
            "new_definition": c.new_definition,
            "status": c.status,
            "auto_resolved": c.auto_resolved,
            "detected_at": c.detected_at.isoformat() if c.detected_at else None,
            "resolved_at": c.resolved_at.isoformat() if c.resolved_at else None,
        }
        for c in result.scalars().all()
    ]


@router.post("/schema-changes/{change_id}/resolve")
async def resolve_schema_change(
    change_id: uuid.UUID,
    action: str = Query(..., pattern="^(apply|ignore)$"),
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        select(SchemaChange).where(SchemaChange.id == change_id)
    )
    change = result.scalar_one_or_none()
    if change is None:
        raise NotFoundError("Schema change not found")

    from datetime import datetime, timezone
    change.status = "applied" if action == "apply" else "ignored"
    change.resolved_at = datetime.now(timezone.utc)
    await session.commit()

    return {"id": str(change_id), "status": change.status}


# ===== Lineage =====

@router.get("/lineage")
async def get_lineage(
    config_id: uuid.UUID | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(ColumnLineage).order_by(ColumnLineage.source_column)
    if config_id:
        stmt = stmt.where(ColumnLineage.config_id == config_id)
    result = await session.execute(stmt)
    return [
        {
            "id": str(l.id),
            "config_id": str(l.config_id),
            "source_schema": l.source_schema,
            "source_table": l.source_table,
            "source_column": l.source_column,
            "target_type": l.target_type,
            "target_table": l.target_table,
            "target_column": l.target_column,
            "transformations": l.transformations,
            "masking_applied": l.masking_applied,
            "last_synced_at": l.last_synced_at.isoformat() if l.last_synced_at else None,
        }
        for l in result.scalars().all()
    ]
