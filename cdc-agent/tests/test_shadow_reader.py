"""Tests for shadow reader CDC event conversion."""

import sys
import os

# Add parent dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shadow_reader import ShadowReader, ShadowRow


class MockConnector:
    """Fake connector for unit tests."""
    placeholder = "%s"

    def get_connection(self):
        return None

    def build_poll_sql(self, schema, table):
        return "SELECT 1"

    def build_mark_consumed_sql(self, schema, table, count):
        return "UPDATE 1"


def test_to_cdc_events_insert():
    reader = ShadowReader(MockConnector())

    rows = [
        ShadowRow(
            seq_id=1,
            op_type="I",
            row_key="ORD-001",
            changed_at="2026-04-04 10:00:00",
            changed_fields={
                "ORDER_ID": "ORD-001",
                "CUSTOMER_ID": "C-100",
                "TOTAL_AMOUNT": 250.00,
                "STATUS": "NEW",
            },
            ts_ms=1743782400000,
        ),
    ]

    events = reader.to_cdc_events(rows, "HANA_SIDECAR_01", "SAPSR3", "SALES_ORDERS", ["ORDER_ID"])

    assert len(events) == 1
    e = events[0]
    assert e["source"] == "HANA_SIDECAR_01"
    assert e["schema"] == "SAPSR3"
    assert e["table"] == "SALES_ORDERS"
    assert e["op"] == "I"
    assert e["key"] == {"ORDER_ID": "ORD-001"}
    assert e["before"] is None
    assert e["after"]["CUSTOMER_ID"] == "C-100"
    assert e["ts_ms"] == 1743782400000
    assert e["seq"] == 1


def test_to_cdc_events_delete():
    reader = ShadowReader(MockConnector())

    rows = [
        ShadowRow(
            seq_id=5,
            op_type="D",
            row_key="ORD-002",
            changed_at="2026-04-04 10:00:00",
            changed_fields=None,
            ts_ms=1743782500000,
        ),
    ]

    events = reader.to_cdc_events(rows, "HANA_SIDECAR_01", "SAPSR3", "SALES_ORDERS", ["ORDER_ID"])

    assert len(events) == 1
    e = events[0]
    assert e["op"] == "D"
    assert e["key"] == {"ORDER_ID": "ORD-002"}
    assert e["after"] is None


def test_to_cdc_events_composite_key():
    reader = ShadowReader(MockConnector())

    rows = [
        ShadowRow(
            seq_id=10,
            op_type="U",
            row_key="ORD-001|ITEM-003",
            changed_at="2026-04-04 10:00:00",
            changed_fields={"QUANTITY": 5, "PRICE": 19.99},
            ts_ms=1743782600000,
        ),
    ]

    events = reader.to_cdc_events(
        rows, "HANA_SIDECAR_01", "SAPSR3", "ORDER_ITEMS", ["ORDER_ID", "ITEM_ID"]
    )

    assert len(events) == 1
    e = events[0]
    assert e["key"] == {"ORDER_ID": "ORD-001", "ITEM_ID": "ITEM-003"}
    assert e["after"]["QUANTITY"] == 5


def test_to_cdc_events_batch():
    reader = ShadowReader(MockConnector())

    rows = [
        ShadowRow(seq_id=i, op_type="I", row_key=f"ORD-{i:03d}",
                   changed_at="2026-04-04 10:00:00",
                   changed_fields={"ORDER_ID": f"ORD-{i:03d}"},
                   ts_ms=1743782400000 + i)
        for i in range(100)
    ]

    events = reader.to_cdc_events(rows, "SRC", "S", "T", ["ORDER_ID"])

    assert len(events) == 100
    # Verify ordering preserved
    for i, e in enumerate(events):
        assert e["seq"] == i
