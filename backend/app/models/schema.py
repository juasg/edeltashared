from pydantic import BaseModel


class SourceInfo(BaseModel):
    id: str
    name: str
    host: str
    port: int
    status: str  # 'connected' | 'disconnected' | 'error'


class TableInfo(BaseModel):
    schema_name: str
    table_name: str
    table_type: str  # 'TABLE' | 'VIEW' | 'CDS_VIEW'
    row_count: int | None = None
    has_primary_key: bool = True


class FieldInfo(BaseModel):
    column_name: str
    data_type: str
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    is_nullable: bool = True
    is_primary_key: bool = False
    default_value: str | None = None


class RelationshipInfo(BaseModel):
    constraint_name: str
    source_column: str
    referenced_schema: str
    referenced_table: str
    referenced_column: str
