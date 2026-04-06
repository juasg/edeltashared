import logging
import time

from prometheus_client import Gauge

from config import settings
from db_connector import DBConnector

logger = logging.getLogger("cdc-agent.health")

# Prometheus metrics
HANA_CPU = Gauge("edelta_hana_cpu_percent", "HANA sidecar CPU usage")
HANA_MEMORY = Gauge("edelta_hana_memory_percent", "HANA sidecar memory usage")


class HANAHealthChecker:
    """Monitors HANA sidecar resource usage.

    If CPU exceeds threshold, sets a back-off flag so the
    CDC agent pauses polling to protect live reports.

    In simulator mode, always reports healthy (no back-off).
    """

    def __init__(self, connector: DBConnector):
        self._connector = connector
        self.cpu_threshold = settings.cdc_hana_cpu_threshold
        self._backed_off = False
        self._last_check_time = 0.0
        self._check_interval = 10.0

    @property
    def should_back_off(self) -> bool:
        return self._backed_off

    def check(self) -> dict:
        """Query HANA resource utilization.

        In simulator mode, returns simulated healthy values.
        """
        now = time.monotonic()
        if now - self._last_check_time < self._check_interval:
            return {"cpu_percent": -1, "memory_percent": -1, "backed_off": self._backed_off}

        self._last_check_time = now

        # Simulator mode — always healthy
        health_sql = self._connector.build_health_sql()
        if health_sql is None:
            HANA_CPU.set(15.0)
            HANA_MEMORY.set(45.0)
            self._backed_off = False
            return {"cpu_percent": 15.0, "memory_percent": 45.0, "backed_off": False}

        try:
            conn = self._connector.get_connection()
            cursor = conn.cursor()
            cursor.execute(health_sql)
            row = cursor.fetchone()
            cursor.close()

            if row:
                memory_pct = float(row[0]) if row[0] else 0.0
                cpu_pct = float(row[1]) if row[1] else 0.0
            else:
                memory_pct = 0.0
                cpu_pct = 0.0

            HANA_CPU.set(cpu_pct)
            HANA_MEMORY.set(memory_pct)

            was_backed_off = self._backed_off
            self._backed_off = cpu_pct > self.cpu_threshold

            if self._backed_off and not was_backed_off:
                logger.warning(
                    f"HANA CPU at {cpu_pct}% (threshold: {self.cpu_threshold}%) "
                    f"— backing off CDC polling"
                )
            elif not self._backed_off and was_backed_off:
                logger.info(
                    f"HANA CPU at {cpu_pct}% — resuming CDC polling"
                )

            return {
                "cpu_percent": cpu_pct,
                "memory_percent": memory_pct,
                "backed_off": self._backed_off,
            }

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"cpu_percent": -1, "memory_percent": -1, "backed_off": False}
