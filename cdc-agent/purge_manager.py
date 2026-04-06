import logging
import time

from config import settings
from db_connector import DBConnector

logger = logging.getLogger("cdc-agent.purge")


class PurgeManager:
    """Periodically purges consumed CDC shadow rows.

    Keeps shadow tables small to minimize storage impact.
    Default: purge rows consumed more than 1 hour ago, every hour.
    """

    def __init__(self, connector: DBConnector):
        self._connector = connector
        self.purge_interval = settings.cdc_purge_interval_seconds
        self._last_purge_time = 0.0

    def should_purge(self) -> bool:
        return time.monotonic() - self._last_purge_time >= self.purge_interval

    def purge_table(self, schema: str, table: str) -> int:
        """Delete consumed rows older than 1 hour from a shadow table."""
        sql = self._connector.build_purge_sql(schema, table)

        try:
            conn = self._connector.get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            if not conn.autocommit:
                conn.commit()
            deleted = cursor.rowcount
            cursor.close()

            if deleted > 0:
                logger.info(f"Purged {deleted} consumed rows from _EDELTA_CDC_{table}")
            return deleted

        except Exception as e:
            logger.error(f"Purge failed for _EDELTA_CDC_{table}: {e}")
            return 0

    def purge_all(self, tables: list[dict]) -> int:
        """Purge all monitored shadow tables."""
        if not self.should_purge():
            return 0

        self._last_purge_time = time.monotonic()
        total = 0

        for t in tables:
            total += self.purge_table(t["schema"], t["table"])

        if total > 0:
            logger.info(f"Purge cycle complete: {total} rows deleted across {len(tables)} tables")
        return total
