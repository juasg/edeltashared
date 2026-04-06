from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.db.session import get_session
from app.models.schema import FieldInfo, RelationshipInfo, SourceInfo, TableInfo
from app.services.schema_service import schema_service

router = APIRouter(prefix="/schema", tags=["schema"], dependencies=[Depends(get_current_user)])


@router.get("/sources", response_model=list[SourceInfo])
async def list_sources():
    """List configured HANA source connections."""
    return await schema_service.get_sources()


@router.get("/tables", response_model=list[TableInfo])
async def list_tables(
    search: str | None = Query(None, description="Filter tables by name"),
    session: AsyncSession = Depends(get_session),
):
    """Browse/search HANA tables. Results cached for 24 hours."""
    return await schema_service.get_tables(session, search)


@router.get("/tables/{schema_name}/{table_name}/fields", response_model=list[FieldInfo])
async def get_table_fields(
    schema_name: str,
    table_name: str,
    session: AsyncSession = Depends(get_session),
):
    """Get column details for a specific table."""
    return await schema_service.get_fields(session, schema_name, table_name)


@router.get(
    "/tables/{schema_name}/{table_name}/relationships",
    response_model=list[RelationshipInfo],
)
async def get_table_relationships(
    schema_name: str,
    table_name: str,
    session: AsyncSession = Depends(get_session),
):
    """Get foreign key relationships for a table."""
    return await schema_service.get_relationships(session, schema_name, table_name)
