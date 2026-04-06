"""
End-to-end integration tests for the CDC pipeline.

These tests require Docker Compose services running:
  docker compose up -d postgres redis kafka

Run with: pytest backend/tests/test_integration.py -v
"""

import asyncio
import json
import uuid

import psycopg2
import pytest

# Connection settings for test (matches docker-compose defaults)
PG_DSN = "host=localhost port=5432 dbname=edeltashared user=edeltashared password=changeme_postgres"


def get_pg_conn():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    return conn


@pytest.fixture
def pg():
    """Postgres connection fixture."""
    conn = get_pg_conn()
    yield conn
    conn.close()


# ===== Test 1: Happy path — shadow table insert triggers CDC row =====

class TestHappyPathPipeline:
    """Verify that DML on simulated source table creates shadow rows."""

    def test_insert_creates_shadow_row(self, pg):
        """INSERT into source should trigger shadow table INSERT."""
        cursor = pg.cursor()
        order_id = f"TEST-{uuid.uuid4().hex[:8]}"

        # Insert into simulated source
        cursor.execute(
            'INSERT INTO "SAPSR3"."SALES_ORDERS" '
            '("ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS") '
            "VALUES (%s, %s, %s, %s)",
            (order_id, "C-TEST", 100.00, "NEW"),
        )

        # Verify shadow row was created
        cursor.execute(
            'SELECT op_type, row_key, changed_fields '
            'FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE row_key = %s AND is_consumed = FALSE",
            (order_id,),
        )
        row = cursor.fetchone()
        assert row is not None, "Shadow row should exist after INSERT"
        assert row[0] == "I"
        assert row[1] == order_id

        # Verify JSON fields
        fields = json.loads(row[2])
        assert fields["ORDER_ID"] == order_id
        assert fields["CUSTOMER_ID"] == "C-TEST"

        # Cleanup
        cursor.execute(
            'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
            (order_id,),
        )

    def test_update_creates_shadow_row(self, pg):
        """UPDATE on source should trigger shadow table UPDATE row."""
        cursor = pg.cursor()
        order_id = f"TEST-{uuid.uuid4().hex[:8]}"

        # Insert first
        cursor.execute(
            'INSERT INTO "SAPSR3"."SALES_ORDERS" '
            '("ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS") '
            "VALUES (%s, %s, %s, %s)",
            (order_id, "C-TEST", 100.00, "NEW"),
        )

        # Update
        cursor.execute(
            'UPDATE "SAPSR3"."SALES_ORDERS" SET "STATUS" = %s WHERE "ORDER_ID" = %s',
            ("SHIPPED", order_id),
        )

        # Verify UPDATE shadow row
        cursor.execute(
            'SELECT op_type, changed_fields '
            'FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE row_key = %s AND op_type = 'U'",
            (order_id,),
        )
        row = cursor.fetchone()
        assert row is not None, "Shadow row should exist after UPDATE"
        assert row[0] == "U"

        fields = json.loads(row[1])
        assert fields["STATUS"] == "SHIPPED"

        # Cleanup
        cursor.execute(
            'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
            (order_id,),
        )

    def test_delete_creates_shadow_row(self, pg):
        """DELETE on source should trigger shadow table DELETE row."""
        cursor = pg.cursor()
        order_id = f"TEST-{uuid.uuid4().hex[:8]}"

        # Insert and then delete
        cursor.execute(
            'INSERT INTO "SAPSR3"."SALES_ORDERS" '
            '("ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS") '
            "VALUES (%s, %s, %s, %s)",
            (order_id, "C-TEST", 100.00, "NEW"),
        )
        cursor.execute(
            'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
            (order_id,),
        )

        # Verify DELETE shadow row
        cursor.execute(
            'SELECT op_type, changed_fields '
            'FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE row_key = %s AND op_type = 'D'",
            (order_id,),
        )
        row = cursor.fetchone()
        assert row is not None, "Shadow row should exist after DELETE"
        assert row[0] == "D"
        assert row[1] is None  # Deletes have no changed_fields


# ===== Test 2: Shadow table consumption and purge =====

class TestShadowTableLifecycle:
    """Verify consumed marking and purge logic."""

    def test_mark_consumed(self, pg):
        """Consumed rows should be markable."""
        cursor = pg.cursor()
        order_id = f"TEST-{uuid.uuid4().hex[:8]}"

        cursor.execute(
            'INSERT INTO "SAPSR3"."SALES_ORDERS" '
            '("ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS") '
            "VALUES (%s, %s, %s, %s)",
            (order_id, "C-TEST", 50.00, "NEW"),
        )

        # Get the shadow row seq_id
        cursor.execute(
            'SELECT seq_id FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE row_key = %s AND op_type = 'I'",
            (order_id,),
        )
        seq_id = cursor.fetchone()[0]

        # Mark consumed
        cursor.execute(
            'UPDATE "_EDELTA_CDC_SALES_ORDERS" '
            "SET is_consumed = TRUE WHERE seq_id = %s",
            (seq_id,),
        )

        # Verify
        cursor.execute(
            'SELECT is_consumed FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE seq_id = %s",
            (seq_id,),
        )
        assert cursor.fetchone()[0] is True

        # Cleanup
        cursor.execute(
            'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
            (order_id,),
        )

    def test_unconsumed_rows_appear_in_poll(self, pg):
        """Only unconsumed rows with seq_id > last should appear."""
        cursor = pg.cursor()
        ids = []
        for i in range(3):
            oid = f"TEST-POLL-{uuid.uuid4().hex[:6]}"
            ids.append(oid)
            cursor.execute(
                'INSERT INTO "SAPSR3"."SALES_ORDERS" '
                '("ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS") '
                "VALUES (%s, %s, %s, %s)",
                (oid, "C-TEST", 10.00 * (i + 1), "NEW"),
            )

        # Poll with seq_id > 0
        cursor.execute(
            'SELECT seq_id, op_type, row_key FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE is_consumed = FALSE AND seq_id > 0 ORDER BY seq_id LIMIT 1000"
        )
        rows = cursor.fetchall()
        row_keys = {r[2] for r in rows}

        for oid in ids:
            assert oid in row_keys, f"{oid} should appear in poll results"

        # Cleanup
        for oid in ids:
            cursor.execute(
                'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
                (oid,),
            )


# ===== Test 3: Metadata DB operations =====

class TestMetadataDB:
    """Verify metadata DB tables work correctly."""

    def test_create_connector_config(self, pg):
        cursor = pg.cursor()
        cid = str(uuid.uuid4())
        cursor.execute(
            "INSERT INTO connector_configs (id, type, name, credentials_encrypted) "
            "VALUES (%s, %s, %s, %s)",
            (cid, "snowflake", "Test SF", "encrypted_creds"),
        )

        cursor.execute("SELECT type, name FROM connector_configs WHERE id = %s", (cid,))
        row = cursor.fetchone()
        assert row[0] == "snowflake"
        assert row[1] == "Test SF"

        # Cleanup
        cursor.execute("DELETE FROM connector_configs WHERE id = %s", (cid,))

    def test_create_replication_config(self, pg):
        cursor = pg.cursor()
        conn_id = str(uuid.uuid4())
        config_id = str(uuid.uuid4())

        cursor.execute(
            "INSERT INTO connector_configs (id, type, name, credentials_encrypted) "
            "VALUES (%s, %s, %s, %s)",
            (conn_id, "snowflake", "Test", "enc"),
        )
        cursor.execute(
            "INSERT INTO replication_configs "
            "(id, source_table_schema, source_table_name, connector_config_id, "
            " target_table_name, field_mappings, primary_key_fields, kafka_topic) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                config_id, "SAPSR3", "SALES_ORDERS", conn_id,
                "sales_orders",
                json.dumps([{"source": "ORDER_ID", "target": "order_id"}]),
                json.dumps(["ORDER_ID"]),
                "edelta.sapsr3.sales_orders",
            ),
        )

        cursor.execute(
            "SELECT kafka_topic FROM replication_configs WHERE id = %s",
            (config_id,),
        )
        assert cursor.fetchone()[0] == "edelta.sapsr3.sales_orders"

        # Cleanup
        cursor.execute("DELETE FROM replication_configs WHERE id = %s", (config_id,))
        cursor.execute("DELETE FROM connector_configs WHERE id = %s", (conn_id,))

    def test_stream_state_tracking(self, pg):
        cursor = pg.cursor()
        conn_id = str(uuid.uuid4())
        config_id = str(uuid.uuid4())
        stream_id = str(uuid.uuid4())

        cursor.execute(
            "INSERT INTO connector_configs (id, type, name, credentials_encrypted) VALUES (%s, %s, %s, %s)",
            (conn_id, "snowflake", "T", "e"),
        )
        cursor.execute(
            "INSERT INTO replication_configs (id, source_table_schema, source_table_name, "
            "connector_config_id, target_table_name, field_mappings, primary_key_fields) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (config_id, "S", "T", conn_id, "t", "[]", '["ID"]'),
        )
        cursor.execute(
            "INSERT INTO stream_state (id, config_id, consumer_group, last_hana_seq_id) "
            "VALUES (%s, %s, %s, %s)",
            (stream_id, config_id, "sf-consumer-1", 0),
        )

        # Update stream state
        cursor.execute(
            "UPDATE stream_state SET last_hana_seq_id = 500, "
            "records_processed_total = 500 WHERE id = %s",
            (stream_id,),
        )

        cursor.execute(
            "SELECT last_hana_seq_id, records_processed_total FROM stream_state WHERE id = %s",
            (stream_id,),
        )
        row = cursor.fetchone()
        assert row[0] == 500
        assert row[1] == 500

        # Cleanup
        cursor.execute("DELETE FROM stream_state WHERE id = %s", (stream_id,))
        cursor.execute("DELETE FROM replication_configs WHERE id = %s", (config_id,))
        cursor.execute("DELETE FROM connector_configs WHERE id = %s", (conn_id,))
