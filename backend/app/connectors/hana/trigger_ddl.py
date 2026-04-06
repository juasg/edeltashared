import logging
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from app.utils.identifiers import validate_identifier

logger = logging.getLogger("edeltashared.hana.ddl")

# Templates directory
TEMPLATES_DIR = Path(__file__).resolve().parents[4] / "sql" / "hana"


def _format_column(col_name: str, prefix: str = ":new_row") -> str:
    """Jinja2 filter to format a column reference for trigger SQL."""
    return f'{prefix}."{col_name}"'


def _get_jinja_env() -> Environment:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["format_column"] = _format_column
    return env


class TriggerDDLGenerator:
    """Renders Jinja2 templates for HANA shadow tables and triggers."""

    def __init__(self):
        self.env = _get_jinja_env()

    def _validate_ids(self, schema: str, table: str, columns: list[str] | None = None) -> None:
        """Validate all identifiers before interpolating into SQL."""
        validate_identifier(schema, "schema")
        validate_identifier(table, "table")
        if columns:
            for col in columns:
                validate_identifier(col, "column")

    def generate_shadow_table(self, schema: str, table: str) -> str:
        """Generate CREATE TABLE DDL for the shadow/CDC table."""
        self._validate_ids(schema, table)
        template = self.env.get_template("create_shadow_table.sql.j2")
        return template.render(schema=schema, table=table)

    def generate_triggers(
        self,
        schema: str,
        table: str,
        primary_key_columns: list[str],
        monitored_columns: list[str],
    ) -> str:
        """Generate CREATE TRIGGER DDL for INSERT/UPDATE/DELETE."""
        self._validate_ids(schema, table, primary_key_columns + monitored_columns)
        template = self.env.get_template("create_trigger.sql.j2")
        return template.render(
            schema=schema,
            table=table,
            primary_key_columns=primary_key_columns,
            monitored_columns=monitored_columns,
        )

    def generate_drop(self, schema: str, table: str) -> str:
        """Generate DROP DDL for triggers and shadow table."""
        self._validate_ids(schema, table)
        template = self.env.get_template("drop_trigger.sql.j2")
        return template.render(schema=schema, table=table)

    def generate_purge_sql(self, schema: str, table: str) -> str:
        """Generate purge SQL for consumed shadow rows."""
        self._validate_ids(schema, table)
        return (
            f'DELETE FROM "{schema}"."_EDELTA_CDC_{table}" '
            f"WHERE is_consumed = TRUE "
            f"AND changed_at < ADD_SECONDS(CURRENT_TIMESTAMP, -3600)"
        )
