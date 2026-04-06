"""Tests for Jinja2 DDL template rendering."""

from app.connectors.hana.trigger_ddl import TriggerDDLGenerator


def test_shadow_table_ddl():
    gen = TriggerDDLGenerator()
    ddl = gen.generate_shadow_table("SAPSR3", "SALES_ORDERS")

    assert '"SAPSR3"."_EDELTA_CDC_SALES_ORDERS"' in ddl
    assert "seq_id" in ddl
    assert "op_type" in ddl
    assert "row_key" in ddl
    assert "changed_fields" in ddl
    assert "is_consumed" in ddl
    assert "CREATE INDEX" in ddl


def test_trigger_ddl_insert():
    gen = TriggerDDLGenerator()
    ddl = gen.generate_triggers(
        schema="SAPSR3",
        table="SALES_ORDERS",
        primary_key_columns=["ORDER_ID"],
        monitored_columns=["ORDER_ID", "CUSTOMER_ID", "TOTAL_AMOUNT", "STATUS"],
    )

    assert "TR_EDELTA_CDC_SALES_ORDERS_INS" in ddl
    assert "TR_EDELTA_CDC_SALES_ORDERS_UPD" in ddl
    assert "TR_EDELTA_CDC_SALES_ORDERS_DEL" in ddl
    assert "AFTER INSERT" in ddl
    assert "AFTER UPDATE" in ddl
    assert "AFTER DELETE" in ddl
    assert "JSON_OBJECT" in ddl
    assert "'ORDER_ID'" in ddl
    assert "'CUSTOMER_ID'" in ddl


def test_trigger_ddl_composite_key():
    gen = TriggerDDLGenerator()
    ddl = gen.generate_triggers(
        schema="SAPSR3",
        table="ORDER_ITEMS",
        primary_key_columns=["ORDER_ID", "ITEM_ID"],
        monitored_columns=["ORDER_ID", "ITEM_ID", "QUANTITY", "PRICE"],
    )

    # Composite key should use || '|' || to join
    assert "|| '|' ||" in ddl


def test_drop_ddl():
    gen = TriggerDDLGenerator()
    ddl = gen.generate_drop("SAPSR3", "SALES_ORDERS")

    assert "DROP TRIGGER" in ddl
    assert "TR_EDELTA_CDC_SALES_ORDERS_INS" in ddl
    assert "TR_EDELTA_CDC_SALES_ORDERS_UPD" in ddl
    assert "TR_EDELTA_CDC_SALES_ORDERS_DEL" in ddl
    assert "DROP TABLE" in ddl
    assert "_EDELTA_CDC_SALES_ORDERS" in ddl


def test_purge_sql():
    gen = TriggerDDLGenerator()
    sql = gen.generate_purge_sql("SAPSR3", "SALES_ORDERS")

    assert "DELETE FROM" in sql
    assert "_EDELTA_CDC_SALES_ORDERS" in sql
    assert "is_consumed = TRUE" in sql
    assert "ADD_SECONDS" in sql
