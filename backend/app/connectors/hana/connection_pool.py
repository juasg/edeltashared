import asyncio
import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from app.core.config import settings

logger = logging.getLogger("edeltashared.hana")


class HANAConnectionPool:
    """Connection pool for SAP HANA with hard cap on concurrent connections.

    Critical constraint: HANA sidecar runs live reports.
    We enforce a semaphore-based limit (default 3) to ensure minimal load.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        max_connections: int | None = None,
        query_timeout_ms: int | None = None,
    ):
        self.host = host or settings.hana_host
        self.port = port or settings.hana_port
        self.user = user or settings.hana_user
        self.password = password or settings.hana_password
        self.max_connections = max_connections or settings.hana_max_connections
        self.query_timeout_ms = query_timeout_ms or settings.hana_query_timeout_ms
        self._semaphore = asyncio.Semaphore(self.max_connections)
        self._closed = False

    def _create_connection(self):
        """Create a new hdbcli connection. Runs in executor to avoid blocking."""
        try:
            from hdbcli import dbapi

            conn = dbapi.connect(
                address=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
            # Set statement timeout
            cursor = conn.cursor()
            cursor.execute(
                f"SET 'statement_timeout' = '{self.query_timeout_ms}'"
            )
            cursor.close()
            return conn
        except ImportError:
            logger.warning(
                "hdbcli not available — using simulator mode. "
                "Install hdbcli for real HANA connectivity."
            )
            raise
        except Exception as e:
            logger.error(f"Failed to connect to HANA: {e}")
            raise

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator:
        """Acquire a HANA connection from the pool.

        Blocks if max_connections are already in use.
        Connection is returned to the pool on exit.
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        await self._semaphore.acquire()
        conn = None
        try:
            loop = asyncio.get_event_loop()
            conn = await loop.run_in_executor(None, self._create_connection)
            yield conn
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            self._semaphore.release()

    async def execute_query(self, sql: str, params: tuple = ()) -> list[dict]:
        """Execute a query and return results as list of dicts."""
        async with self.acquire() as conn:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, self._execute_sync, conn, sql, params
            )

    def _execute_sync(self, conn, sql: str, params: tuple) -> list[dict]:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            return []
        finally:
            cursor.close()

    async def execute_statement(self, sql: str, params: tuple = ()) -> int:
        """Execute a statement (INSERT/UPDATE/DELETE) and return affected rows."""
        async with self.acquire() as conn:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, self._execute_statement_sync, conn, sql, params
            )

    def _execute_statement_sync(self, conn, sql: str, params: tuple) -> int:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
            return cursor.rowcount
        finally:
            cursor.close()

    async def close(self):
        """Mark pool as closed."""
        self._closed = True
