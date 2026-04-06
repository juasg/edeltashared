"""
Database connector abstraction for the CDC Agent.

Supports two modes:
- HANA mode: connects to SAP HANA via hdbcli
- Simulator mode: connects to Postgres (shadow tables mimic HANA structure)

The connector provides a unified interface so shadow_reader, health_checker,
and purge_manager work identically in both modes.
"""

import logging
import re

import psycopg2

from config import settings

logger = logging.getLogger("cdc-agent.connector")

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,254}$")


def _validate_id(name: str, label: str = "identifier") -> str:
    """Validate SQL identifier to prevent injection."""
    if not name or not _IDENT_RE.match(name):
        raise ValueError(f"Invalid {label}: '{name}'")
    return name


class DBConnector:
    """Unified connector that wraps either HANA or Postgres."""

    def __init__(self):
        self.is_simulator = settings.simulator_mode
        self._conn = None
        # HANA uses ? placeholders, Postgres uses %s
        self.placeholder = "%s" if self.is_simulator else "?"

    def get_connection(self):
        """Get or create a database connection."""
        if self._conn is not None:
            try:
                self._ping()
                return self._conn
            except Exception:
                self._conn = None

        if self.is_simulator:
            self._conn = self._connect_postgres()
        else:
            self._conn = self._connect_hana()

        return self._conn

    def _connect_postgres(self):
        """Connect to Postgres in simulator mode."""
        conn = psycopg2.connect(settings.postgres_dsn)
        conn.autocommit = True
        logger.info("Connected to Postgres (simulator mode)")
        return conn

    def _connect_hana(self):
        """Connect to SAP HANA."""
        try:
            from hdbcli import dbapi
            conn = dbapi.connect(
                address=settings.hana_host,
                port=settings.hana_port,
                user=settings.hana_user,
                password=settings.hana_password,
            )
            logger.info("Connected to HANA")
            return conn
        except ImportError:
            logger.error(
                "hdbcli not installed. Set SIMULATOR_MODE=true to use Postgres, "
                "or install hdbcli for HANA connectivity."
            )
            raise

    def _ping(self):
        if self.is_simulator:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
        else:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1 FROM DUMMY")
            cursor.close()

    def shadow_table_name(self, schema: str, table: str) -> str:
        """Get the shadow table name, format differs by mode."""
        _validate_id(schema, "schema")
        _validate_id(table, "table")
        if self.is_simulator:
            return f'"_EDELTA_CDC_{table}"'
        else:
            return f'"{schema}"."_EDELTA_CDC_{table}"'

    def build_poll_sql(self, schema: str, table: str) -> str:
        """Build the poll query with correct placeholder syntax."""
        shadow = self.shadow_table_name(schema, table)
        p = self.placeholder
        return (
            f"SELECT seq_id, op_type, row_key, changed_at, changed_fields "
            f"FROM {shadow} "
            f"WHERE is_consumed = FALSE AND seq_id > {p} "
            f"ORDER BY seq_id "
            f"LIMIT {p}"
        )

    def build_mark_consumed_sql(self, schema: str, table: str, count: int) -> str:
        """Build UPDATE for marking rows consumed."""
        shadow = self.shadow_table_name(schema, table)
        placeholders = ",".join(self.placeholder for _ in range(count))
        return (
            f"UPDATE {shadow} "
            f"SET is_consumed = TRUE "
            f"WHERE seq_id IN ({placeholders})"
        )

    def build_purge_sql(self, schema: str, table: str) -> str:
        """Build DELETE for purging consumed rows."""
        shadow = self.shadow_table_name(schema, table)
        if self.is_simulator:
            return (
                f"DELETE FROM {shadow} "
                f"WHERE is_consumed = TRUE "
                f"AND changed_at < NOW() - INTERVAL '1 hour'"
            )
        else:
            return (
                f"DELETE FROM {shadow} "
                f"WHERE is_consumed = TRUE "
                f"AND changed_at < ADD_SECONDS(CURRENT_TIMESTAMP, -3600)"
            )

    def build_health_sql(self) -> str | None:
        """Build health check query. Returns None in simulator mode."""
        if self.is_simulator:
            return None
        return (
            "SELECT "
            "  ROUND(USED_PHYSICAL_MEMORY * 100.0 / TOTAL_PHYSICAL_MEMORY, 1) AS memory_pct, "
            "  ROUND(TOTAL_CPU_USER_TIME * 100.0 / "
            "    (TOTAL_CPU_USER_TIME + TOTAL_CPU_SYSTEM_TIME + TOTAL_CPU_IDLE_TIME), 1) AS cpu_pct "
            "FROM SYS.M_HOST_RESOURCE_UTILIZATION"
        )

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
