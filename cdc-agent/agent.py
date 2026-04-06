"""
eDeltaShared CDC Agent

Main event loop that:
1. Loads active replication configs from metadata DB
2. Polls shadow tables for changes every 2-5 seconds (HANA or Postgres simulator)
3. Publishes CDC events to Kafka
4. Marks consumed rows in shadow tables
5. Monitors HANA health and backs off if CPU > threshold
6. Purges old consumed rows hourly
7. Listens for control commands via Redis pub/sub
"""

import asyncio
import json
import logging
import signal
import threading
import time

import psycopg2
import psycopg2.extras
import redis
from prometheus_client import Gauge, start_http_server

from config import settings
from db_connector import DBConnector
from health_checker import HANAHealthChecker
from kafka_producer import CDCKafkaProducer
from purge_manager import PurgeManager
from shadow_reader import ShadowReader

logger = logging.getLogger("cdc-agent")

STREAM_STATUS = Gauge(
    "edelta_stream_status",
    "Stream status: 1=active, 0=paused, -1=error",
    ["config_id"],
)


class CDCAgent:
    def __init__(self):
        self._running = False
        self._paused_configs: set[str] = set()
        self._pg_conn = None

        # Track last seq_id per config
        self._last_seq: dict[str, int] = {}

        # Database connector (HANA or Postgres simulator)
        self._connector = DBConnector()

    # ===== Metadata DB connection =====

    def _get_pg_conn(self):
        """Get or create a PostgreSQL metadata DB connection."""
        if self._pg_conn is None or self._pg_conn.closed:
            self._pg_conn = psycopg2.connect(settings.postgres_dsn)
            self._pg_conn.autocommit = True
            logger.info("PostgreSQL metadata connection established")
        return self._pg_conn

    # ===== Config loading =====

    def load_active_configs(self) -> list[dict]:
        """Load active replication configs from metadata DB."""
        conn = self._get_pg_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(
            "SELECT id, source_table_schema, source_table_name, "
            "       primary_key_fields, kafka_topic, replication_mode "
            "FROM replication_configs "
            "WHERE is_enabled = TRUE AND trigger_deployed = TRUE "
            "  AND replication_mode = 'cdc'"
        )
        configs = cursor.fetchall()
        cursor.close()
        return [dict(c) for c in configs]

    def load_stream_state(self, config_id: str) -> int:
        """Load last processed seq_id for a config."""
        conn = self._get_pg_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT last_hana_seq_id FROM stream_state "
            "WHERE config_id = %s ORDER BY updated_at DESC LIMIT 1",
            (config_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else 0

    def update_stream_state(
        self,
        config_id: str,
        last_seq_id: int,
        records_processed: int,
        status: str = "active",
        error_message: str | None = None,
    ):
        """Persist stream state to metadata DB."""
        conn = self._get_pg_conn()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE stream_state "
            "SET last_hana_seq_id = %s, "
            "    records_processed_total = records_processed_total + %s, "
            "    last_processed_at = NOW(), "
            "    status = %s, "
            "    error_message = %s, "
            "    updated_at = NOW() "
            "WHERE config_id = %s",
            (last_seq_id, records_processed, status, error_message, config_id),
        )
        cursor.close()

    # ===== Redis command listener =====

    def _start_command_listener(self):
        """Listen for control commands via Redis pub/sub in a background thread."""
        def listener():
            try:
                r = redis.from_url(settings.redis_url)
                pubsub = r.pubsub()
                pubsub.subscribe("edelta:agent:commands")
                logger.info("Redis command listener started")

                for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    try:
                        cmd = json.loads(message["data"])
                        self._handle_command(cmd)
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Invalid command: {e}")
            except Exception as e:
                logger.error(f"Command listener error: {e}")

        thread = threading.Thread(target=listener, daemon=True)
        thread.start()

    def _handle_command(self, cmd: dict):
        """Handle a control command from the control plane."""
        action = cmd.get("action")
        config_id = cmd.get("config_id")

        if action == "pause" and config_id:
            self._paused_configs.add(config_id)
            logger.info(f"Paused config {config_id}")
        elif action == "resume" and config_id:
            self._paused_configs.discard(config_id)
            logger.info(f"Resumed config {config_id}")
        elif action == "restart" and config_id:
            self._paused_configs.discard(config_id)
            self._last_seq.pop(config_id, None)
            logger.info(f"Restarted config {config_id}")
        elif action == "reload":
            logger.info("Config reload requested")
        elif action == "shutdown":
            logger.info("Shutdown requested via command")
            self._running = False
        else:
            logger.warning(f"Unknown command: {cmd}")

    # ===== Main loop =====

    async def run(self):
        """Main CDC agent loop."""
        self._running = True

        mode = "SIMULATOR" if settings.simulator_mode else "HANA"
        logger.info(f"CDC Agent starting in {mode} mode...")

        # Start Prometheus metrics server
        start_http_server(9090)
        logger.info("Prometheus metrics on :9090")

        # Initialize components using the unified connector
        reader = ShadowReader(self._connector)
        producer = CDCKafkaProducer()
        health = HANAHealthChecker(self._connector)
        purger = PurgeManager(self._connector)

        # Start Redis command listener
        self._start_command_listener()

        # Load initial state
        configs = self.load_active_configs()
        for cfg in configs:
            cid = str(cfg["id"])
            self._last_seq[cid] = self.load_stream_state(cid)
            logger.info(
                f"Config {cid}: {cfg['source_table_schema']}.{cfg['source_table_name']} "
                f"(last_seq={self._last_seq[cid]})"
            )

        logger.info(f"CDC Agent running with {len(configs)} active configs")
        config_reload_interval = 60
        last_config_reload = time.monotonic()

        while self._running:
            try:
                # Periodically reload configs
                if time.monotonic() - last_config_reload > config_reload_interval:
                    configs = self.load_active_configs()
                    last_config_reload = time.monotonic()

                # Check HANA health
                health.check()
                if health.should_back_off:
                    logger.debug("HANA CPU high — skipping poll cycle")
                    await asyncio.sleep(settings.cdc_poll_interval_seconds * 2)
                    continue

                # Poll each config's shadow table
                for cfg in configs:
                    cid = str(cfg["id"])

                    if cid in self._paused_configs:
                        STREAM_STATUS.labels(config_id=cid).set(0)
                        continue

                    schema = cfg["source_table_schema"]
                    table = cfg["source_table_name"]
                    topic = cfg["kafka_topic"]
                    pk_fields = cfg["primary_key_fields"]
                    last_seq = self._last_seq.get(cid, 0)

                    try:
                        # Read shadow table
                        rows = reader.poll_changes(schema, table, last_seq)

                        if rows:
                            # Convert to CDC events
                            events = reader.to_cdc_events(
                                rows, "HANA_SIDECAR_01", schema, table, pk_fields
                            )

                            # Publish to Kafka
                            delivered = producer.produce_batch(topic, events)

                            if delivered > 0:
                                # Mark consumed
                                seq_ids = [r.seq_id for r in rows[:delivered]]
                                reader.mark_consumed(schema, table, seq_ids)

                                # Update tracking
                                new_last_seq = rows[delivered - 1].seq_id
                                self._last_seq[cid] = new_last_seq

                                # Persist state
                                self.update_stream_state(
                                    cid, new_last_seq, delivered
                                )

                            logger.info(
                                f"[{schema}.{table}] Processed {delivered}/{len(rows)} "
                                f"events (last_seq={self._last_seq.get(cid, 0)})"
                            )

                        STREAM_STATUS.labels(config_id=cid).set(1)

                    except Exception as e:
                        logger.error(f"Error processing {schema}.{table}: {e}")
                        STREAM_STATUS.labels(config_id=cid).set(-1)
                        self.update_stream_state(
                            cid,
                            self._last_seq.get(cid, 0),
                            0,
                            status="error",
                            error_message=str(e),
                        )

                # Purge consumed rows if interval elapsed
                purge_tables = [
                    {"schema": c["source_table_schema"], "table": c["source_table_name"]}
                    for c in configs
                ]
                purger.purge_all(purge_tables)

                # Sleep until next poll
                await asyncio.sleep(settings.cdc_poll_interval_seconds)

            except Exception as e:
                logger.error(f"Agent loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

        # Cleanup
        logger.info("CDC Agent shutting down...")
        producer.close()
        self._connector.close()
        if self._pg_conn:
            try:
                self._pg_conn.close()
            except Exception:
                pass


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    agent = CDCAgent()

    loop = asyncio.new_event_loop()

    def shutdown_handler(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        agent._running = False

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    loop.run_until_complete(agent.run())
    loop.close()


if __name__ == "__main__":
    main()
