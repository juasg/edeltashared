"""
End-to-end pipeline tests for the HANA simulator shadow tables.

Tests the data flow through Postgres-based HANA simulation:
  Source table INSERT/UPDATE/DELETE --> Trigger --> Shadow table rows

Requires Docker Compose Postgres running:
    docker compose up -d postgres

Run with:
    pytest backend/tests/test_e2e_pipeline.py -v
"""

import json
import time
import uuid

import psycopg2
import pytest

PG_DSN = "host=localhost port=5432 dbname=edeltashared user=edeltashared password=changeme_postgres"


def get_pg_conn():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    return conn


@pytest.fixture
def pg():
    """Postgres connection fixture with cleanup of test rows."""
    conn = get_pg_conn()
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def cleanup_test_rows(pg):
    """Clean up any test rows created during each test."""
    yield
    cursor = pg.cursor()
    # Clean up test orders (triggers will also create shadow rows, but those
    # will be cleaned when the source rows are deleted via trigger)
    try:
        cursor.execute(
            """DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" LIKE 'E2E-%%'"""
        )
    except Exception:
        pass
    # Also clean up any orphaned shadow rows from test prefix
    try:
        cursor.execute(
            """DELETE FROM "_EDELTA_CDC_SALES_ORDERS" WHERE row_key LIKE 'E2E-%%'"""
        )
    except Exception:
        pass
    cursor.close()


def _insert_order(cursor, order_id, customer_id="C-TEST", amount=100.00, status="NEW"):
    cursor.execute(
        'INSERT INTO "SAPSR3"."SALES_ORDERS" '
        '("ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS") '
        "VALUES (%s, %s, %s, %s)",
        (order_id, customer_id, amount, status),
    )


def _get_shadow_rows(cursor, order_id, op_type=None):
    sql = (
        'SELECT seq_id, op_type, row_key, changed_at, changed_fields, is_consumed '
        'FROM "_EDELTA_CDC_SALES_ORDERS" '
        "WHERE row_key = %s"
    )
    params = [order_id]
    if op_type:
        sql += " AND op_type = %s"
        params.append(op_type)
    sql += " ORDER BY seq_id"
    cursor.execute(sql, params)
    return cursor.fetchall()


# ---------------------------------------------------------------------------
# 1. INSERT triggers
# ---------------------------------------------------------------------------


class TestInsertTrigger:
    def test_insert_creates_shadow_row_with_correct_op_type(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-INS-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid, "CUST-001", 250.50, "NEW")

        rows = _get_shadow_rows(cursor, oid)
        assert len(rows) >= 1, "Shadow row should exist after INSERT"

        # Find the INSERT shadow row
        insert_rows = [r for r in rows if r[1] == "I"]
        assert len(insert_rows) == 1
        row = insert_rows[0]

        assert row[1] == "I"  # op_type
        assert row[2] == oid  # row_key
        assert row[5] is False  # is_consumed

    def test_insert_shadow_has_correct_changed_fields(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-INS-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid, "CUST-002", 999.99, "PENDING")

        rows = _get_shadow_rows(cursor, oid, "I")
        assert len(rows) == 1
        fields = json.loads(rows[0][4])

        assert fields["ORDER_ID"] == oid
        assert fields["CUSTOMER_ID"] == "CUST-002"
        # Amount may come as string or number from JSON
        assert float(fields["TOTAL_AMOUNT"]) == pytest.approx(999.99, abs=0.01)
        assert fields["STATUS"] == "PENDING"

    def test_insert_shadow_has_timestamp(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-INS-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)

        rows = _get_shadow_rows(cursor, oid, "I")
        assert rows[0][3] is not None, "changed_at should not be null"

    def test_multiple_inserts_create_separate_shadow_rows(self, pg):
        cursor = pg.cursor()
        oids = [f"E2E-MULTI-{uuid.uuid4().hex[:6]}" for _ in range(3)]

        for oid in oids:
            _insert_order(cursor, oid)

        for oid in oids:
            rows = _get_shadow_rows(cursor, oid, "I")
            assert len(rows) == 1, f"Each insert should create exactly one shadow row for {oid}"

    def test_insert_shadow_seq_ids_are_monotonic(self, pg):
        cursor = pg.cursor()
        oids = [f"E2E-SEQ-{uuid.uuid4().hex[:6]}" for _ in range(5)]

        for oid in oids:
            _insert_order(cursor, oid)

        seq_ids = []
        for oid in oids:
            rows = _get_shadow_rows(cursor, oid, "I")
            seq_ids.append(rows[0][0])

        # seq_ids should be strictly increasing
        for i in range(1, len(seq_ids)):
            assert seq_ids[i] > seq_ids[i - 1], "seq_ids should be monotonically increasing"


# ---------------------------------------------------------------------------
# 2. UPDATE triggers
# ---------------------------------------------------------------------------


class TestUpdateTrigger:
    def test_update_creates_shadow_row(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-UPD-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid, status="NEW")

        cursor.execute(
            'UPDATE "SAPSR3"."SALES_ORDERS" SET "STATUS" = %s WHERE "ORDER_ID" = %s',
            ("SHIPPED", oid),
        )

        rows = _get_shadow_rows(cursor, oid, "U")
        assert len(rows) == 1, "UPDATE should create a shadow row with op_type='U'"

    def test_update_shadow_has_correct_op_type(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-UPD-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)
        cursor.execute(
            'UPDATE "SAPSR3"."SALES_ORDERS" SET "TOTAL_AMOUNT" = %s WHERE "ORDER_ID" = %s',
            (500.00, oid),
        )

        rows = _get_shadow_rows(cursor, oid, "U")
        assert rows[0][1] == "U"

    def test_update_shadow_contains_new_values(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-UPD-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid, amount=100.00, status="NEW")
        cursor.execute(
            'UPDATE "SAPSR3"."SALES_ORDERS" SET "STATUS" = %s, "TOTAL_AMOUNT" = %s '
            'WHERE "ORDER_ID" = %s',
            ("DELIVERED", 150.75, oid),
        )

        rows = _get_shadow_rows(cursor, oid, "U")
        fields = json.loads(rows[0][4])
        assert fields["STATUS"] == "DELIVERED"
        assert float(fields["TOTAL_AMOUNT"]) == pytest.approx(150.75, abs=0.01)

    def test_multiple_updates_create_multiple_shadow_rows(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-UPD-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid, status="NEW")

        for new_status in ["PROCESSING", "SHIPPED", "DELIVERED"]:
            cursor.execute(
                'UPDATE "SAPSR3"."SALES_ORDERS" SET "STATUS" = %s WHERE "ORDER_ID" = %s',
                (new_status, oid),
            )

        rows = _get_shadow_rows(cursor, oid, "U")
        assert len(rows) == 3, "Each UPDATE should create its own shadow row"

        statuses = [json.loads(r[4])["STATUS"] for r in rows]
        assert statuses == ["PROCESSING", "SHIPPED", "DELIVERED"]

    def test_update_row_key_matches_primary_key(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-UPD-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)
        cursor.execute(
            'UPDATE "SAPSR3"."SALES_ORDERS" SET "STATUS" = %s WHERE "ORDER_ID" = %s',
            ("CANCELLED", oid),
        )

        rows = _get_shadow_rows(cursor, oid, "U")
        assert rows[0][2] == oid, "row_key should match the ORDER_ID"


# ---------------------------------------------------------------------------
# 3. DELETE triggers
# ---------------------------------------------------------------------------


class TestDeleteTrigger:
    def test_delete_creates_shadow_row(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-DEL-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)
        cursor.execute(
            'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
            (oid,),
        )

        rows = _get_shadow_rows(cursor, oid, "D")
        assert len(rows) == 1, "DELETE should create shadow row with op_type='D'"

    def test_delete_shadow_has_null_changed_fields(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-DEL-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)
        cursor.execute(
            'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
            (oid,),
        )

        rows = _get_shadow_rows(cursor, oid, "D")
        assert rows[0][4] is None, "DELETE shadow rows should have NULL changed_fields"

    def test_delete_preserves_row_key(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-DEL-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)
        cursor.execute(
            'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
            (oid,),
        )

        rows = _get_shadow_rows(cursor, oid, "D")
        assert rows[0][2] == oid

    def test_full_lifecycle_insert_update_delete(self, pg):
        """Verify the complete I -> U -> D shadow trail."""
        cursor = pg.cursor()
        oid = f"E2E-LIFE-{uuid.uuid4().hex[:8]}"

        _insert_order(cursor, oid, status="NEW")
        cursor.execute(
            'UPDATE "SAPSR3"."SALES_ORDERS" SET "STATUS" = %s WHERE "ORDER_ID" = %s',
            ("SHIPPED", oid),
        )
        cursor.execute(
            'DELETE FROM "SAPSR3"."SALES_ORDERS" WHERE "ORDER_ID" = %s',
            (oid,),
        )

        all_rows = _get_shadow_rows(cursor, oid)
        op_types = [r[1] for r in all_rows]
        assert "I" in op_types
        assert "U" in op_types
        assert "D" in op_types
        assert len(all_rows) == 3


# ---------------------------------------------------------------------------
# 4. Mark consumed
# ---------------------------------------------------------------------------


class TestMarkConsumed:
    def test_mark_rows_consumed(self, pg):
        cursor = pg.cursor()
        oid = f"E2E-CON-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)

        rows = _get_shadow_rows(cursor, oid, "I")
        seq_id = rows[0][0]

        # Mark consumed
        cursor.execute(
            'UPDATE "_EDELTA_CDC_SALES_ORDERS" SET is_consumed = TRUE WHERE seq_id = %s',
            (seq_id,),
        )

        # Verify marked
        cursor.execute(
            'SELECT is_consumed FROM "_EDELTA_CDC_SALES_ORDERS" WHERE seq_id = %s',
            (seq_id,),
        )
        assert cursor.fetchone()[0] is True

    def test_consumed_rows_excluded_from_poll(self, pg):
        """Simulates the CDC agent poll: only unconsumed rows with seq_id > last."""
        cursor = pg.cursor()
        oids = [f"E2E-POLL-{uuid.uuid4().hex[:6]}" for _ in range(3)]

        for oid in oids:
            _insert_order(cursor, oid)

        # Get all shadow rows
        all_rows = []
        for oid in oids:
            rows = _get_shadow_rows(cursor, oid, "I")
            all_rows.extend(rows)

        # Mark first one consumed
        first_seq = all_rows[0][0]
        cursor.execute(
            'UPDATE "_EDELTA_CDC_SALES_ORDERS" SET is_consumed = TRUE WHERE seq_id = %s',
            (first_seq,),
        )

        # Poll: unconsumed rows with seq_id > 0
        cursor.execute(
            'SELECT seq_id, row_key FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE is_consumed = FALSE AND seq_id > 0 ORDER BY seq_id LIMIT 1000"
        )
        polled = cursor.fetchall()
        polled_seqs = {r[0] for r in polled}

        assert first_seq not in polled_seqs, "Consumed row should not appear in poll"
        # The other two should still be there
        for row in all_rows[1:]:
            assert row[0] in polled_seqs

    def test_mark_multiple_consumed_atomically(self, pg):
        cursor = pg.cursor()
        oids = [f"E2E-BATCH-{uuid.uuid4().hex[:6]}" for _ in range(5)]
        for oid in oids:
            _insert_order(cursor, oid)

        seq_ids = []
        for oid in oids:
            rows = _get_shadow_rows(cursor, oid, "I")
            seq_ids.append(rows[0][0])

        # Mark all consumed in one UPDATE
        placeholders = ",".join(["%s"] * len(seq_ids))
        cursor.execute(
            f'UPDATE "_EDELTA_CDC_SALES_ORDERS" SET is_consumed = TRUE '
            f"WHERE seq_id IN ({placeholders})",
            tuple(seq_ids),
        )

        for sid in seq_ids:
            cursor.execute(
                'SELECT is_consumed FROM "_EDELTA_CDC_SALES_ORDERS" WHERE seq_id = %s',
                (sid,),
            )
            assert cursor.fetchone()[0] is True


# ---------------------------------------------------------------------------
# 5. Bulk insert
# ---------------------------------------------------------------------------


class TestBulkInsert:
    def test_bulk_insert_1000_rows(self, pg):
        """Insert 1000 rows and verify all appear in shadow table."""
        cursor = pg.cursor()
        prefix = f"E2E-BULK-{uuid.uuid4().hex[:6]}"
        count = 1000

        # Bulk insert using executemany
        rows = [
            (f"{prefix}-{i:04d}", f"CUST-{i}", float(i * 10), "NEW")
            for i in range(count)
        ]
        cursor.executemany(
            'INSERT INTO "SAPSR3"."SALES_ORDERS" '
            '("ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS") '
            "VALUES (%s, %s, %s, %s)",
            rows,
        )

        # Count shadow rows for our prefix
        cursor.execute(
            'SELECT COUNT(*) FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE row_key LIKE %s AND op_type = 'I'",
            (f"{prefix}-%",),
        )
        shadow_count = cursor.fetchone()[0]
        assert shadow_count == count, f"Expected {count} shadow rows, got {shadow_count}"

    def test_bulk_insert_all_unconsumed(self, pg):
        """All bulk-inserted shadow rows should start as unconsumed."""
        cursor = pg.cursor()
        prefix = f"E2E-BULKUC-{uuid.uuid4().hex[:6]}"

        for i in range(50):
            _insert_order(cursor, f"{prefix}-{i:03d}")

        cursor.execute(
            'SELECT COUNT(*) FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE row_key LIKE %s AND is_consumed = FALSE",
            (f"{prefix}-%",),
        )
        assert cursor.fetchone()[0] == 50

    def test_bulk_insert_seq_ids_unique(self, pg):
        """Each shadow row from bulk insert should have a unique seq_id."""
        cursor = pg.cursor()
        prefix = f"E2E-BULKSEQ-{uuid.uuid4().hex[:6]}"

        for i in range(100):
            _insert_order(cursor, f"{prefix}-{i:03d}")

        cursor.execute(
            'SELECT seq_id FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE row_key LIKE %s ORDER BY seq_id",
            (f"{prefix}-%",),
        )
        seq_ids = [r[0] for r in cursor.fetchall()]
        assert len(seq_ids) == len(set(seq_ids)), "All seq_ids should be unique"


# ---------------------------------------------------------------------------
# 6. Purge consumed rows
# ---------------------------------------------------------------------------


class TestPurgeConsumed:
    def test_purge_consumed_older_than_threshold(self, pg):
        """Consumed rows older than 1 hour should be purgeable."""
        cursor = pg.cursor()
        oid = f"E2E-PURGE-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)

        rows = _get_shadow_rows(cursor, oid, "I")
        seq_id = rows[0][0]

        # Mark consumed and backdate
        cursor.execute(
            'UPDATE "_EDELTA_CDC_SALES_ORDERS" '
            "SET is_consumed = TRUE, changed_at = NOW() - INTERVAL '2 hours' "
            "WHERE seq_id = %s",
            (seq_id,),
        )

        # Run purge query (same as PurgeManager would use)
        cursor.execute(
            'DELETE FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE is_consumed = TRUE "
            "AND changed_at < NOW() - INTERVAL '1 hour'"
        )
        deleted = cursor.rowcount
        assert deleted >= 1, "Should purge at least one consumed row"

        # Verify it's gone
        cursor.execute(
            'SELECT COUNT(*) FROM "_EDELTA_CDC_SALES_ORDERS" WHERE seq_id = %s',
            (seq_id,),
        )
        assert cursor.fetchone()[0] == 0

    def test_purge_does_not_affect_unconsumed(self, pg):
        """Unconsumed rows should survive purge regardless of age."""
        cursor = pg.cursor()
        oid = f"E2E-PURGE-SAFE-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)

        rows = _get_shadow_rows(cursor, oid, "I")
        seq_id = rows[0][0]

        # Backdate but keep unconsumed
        cursor.execute(
            'UPDATE "_EDELTA_CDC_SALES_ORDERS" '
            "SET changed_at = NOW() - INTERVAL '5 hours' "
            "WHERE seq_id = %s",
            (seq_id,),
        )

        # Run purge
        cursor.execute(
            'DELETE FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE is_consumed = TRUE "
            "AND changed_at < NOW() - INTERVAL '1 hour'"
        )

        # Row should still exist
        cursor.execute(
            'SELECT COUNT(*) FROM "_EDELTA_CDC_SALES_ORDERS" WHERE seq_id = %s',
            (seq_id,),
        )
        assert cursor.fetchone()[0] == 1

    def test_purge_does_not_affect_recent_consumed(self, pg):
        """Recently consumed rows (< 1 hour) should survive purge."""
        cursor = pg.cursor()
        oid = f"E2E-PURGE-RECENT-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid)

        rows = _get_shadow_rows(cursor, oid, "I")
        seq_id = rows[0][0]

        # Mark consumed but leave timestamp recent
        cursor.execute(
            'UPDATE "_EDELTA_CDC_SALES_ORDERS" SET is_consumed = TRUE WHERE seq_id = %s',
            (seq_id,),
        )

        # Run purge
        cursor.execute(
            'DELETE FROM "_EDELTA_CDC_SALES_ORDERS" '
            "WHERE is_consumed = TRUE "
            "AND changed_at < NOW() - INTERVAL '1 hour'"
        )

        # Row should still exist (consumed but recent)
        cursor.execute(
            'SELECT COUNT(*) FROM "_EDELTA_CDC_SALES_ORDERS" WHERE seq_id = %s',
            (seq_id,),
        )
        assert cursor.fetchone()[0] == 1


# ---------------------------------------------------------------------------
# 7. Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_insert_with_null_fields(self, pg):
        """INSERT with NULL optional fields should still trigger shadow row."""
        cursor = pg.cursor()
        oid = f"E2E-NULL-{uuid.uuid4().hex[:8]}"
        cursor.execute(
            'INSERT INTO "SAPSR3"."SALES_ORDERS" ("ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS") '
            "VALUES (%s, NULL, NULL, NULL)",
            (oid,),
        )

        rows = _get_shadow_rows(cursor, oid, "I")
        assert len(rows) == 1
        fields = json.loads(rows[0][4])
        assert fields["ORDER_ID"] == oid
        # NULL values should appear as JSON null
        assert fields["CUSTOMER_ID"] is None

    def test_insert_with_special_characters(self, pg):
        """ORDER_ID with special chars should be preserved in row_key."""
        cursor = pg.cursor()
        oid = f"E2E-SPEC-'\"&<>-{uuid.uuid4().hex[:6]}"
        _insert_order(cursor, oid)

        rows = _get_shadow_rows(cursor, oid, "I")
        assert len(rows) == 1
        assert rows[0][2] == oid

    def test_update_no_op_still_creates_shadow(self, pg):
        """UPDATE that sets same value should still create shadow row."""
        cursor = pg.cursor()
        oid = f"E2E-NOOP-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid, status="NEW")

        # Update to same value
        cursor.execute(
            'UPDATE "SAPSR3"."SALES_ORDERS" SET "STATUS" = %s WHERE "ORDER_ID" = %s',
            ("NEW", oid),
        )

        rows = _get_shadow_rows(cursor, oid, "U")
        assert len(rows) == 1, "Even a no-op UPDATE should fire the trigger"

    def test_large_amount_precision(self, pg):
        """Verify decimal precision is maintained through shadow table."""
        cursor = pg.cursor()
        oid = f"E2E-PREC-{uuid.uuid4().hex[:8]}"
        _insert_order(cursor, oid, amount=1234567890.12)

        rows = _get_shadow_rows(cursor, oid, "I")
        fields = json.loads(rows[0][4])
        assert float(fields["TOTAL_AMOUNT"]) == pytest.approx(1234567890.12, abs=0.01)
