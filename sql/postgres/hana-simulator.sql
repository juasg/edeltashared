-- HANA Simulator: Postgres schema that mimics HANA shadow tables
-- Use this for local development without SAP HANA access.

-- Simulated source table
CREATE TABLE IF NOT EXISTS "SAPSR3"."SALES_ORDERS" (
    "ORDER_ID" VARCHAR(50) PRIMARY KEY,
    "CUSTOMER_ID" VARCHAR(50),
    "TOTAL_AMOUNT" DECIMAL(15,2),
    "STATUS" VARCHAR(20),
    "ORDER_DATE" TIMESTAMP DEFAULT NOW(),
    "LAST_MODIFIED" TIMESTAMP DEFAULT NOW()
);

-- Shadow table (mirrors HANA CDC structure)
CREATE TABLE IF NOT EXISTS "_EDELTA_CDC_SALES_ORDERS" (
    seq_id BIGSERIAL PRIMARY KEY,
    op_type VARCHAR(1) NOT NULL,
    row_key VARCHAR(500) NOT NULL,
    changed_at TIMESTAMP DEFAULT NOW(),
    changed_fields TEXT,
    is_consumed BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_cdc_sales_consumed
    ON "_EDELTA_CDC_SALES_ORDERS" (is_consumed, seq_id);

-- Trigger function to simulate HANA CDC triggers
CREATE OR REPLACE FUNCTION fn_edelta_cdc_sales_orders_ins()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO "_EDELTA_CDC_SALES_ORDERS" (op_type, row_key, changed_fields)
    VALUES ('I', NEW."ORDER_ID",
        json_build_object(
            'ORDER_ID', NEW."ORDER_ID",
            'CUSTOMER_ID', NEW."CUSTOMER_ID",
            'TOTAL_AMOUNT', NEW."TOTAL_AMOUNT",
            'STATUS', NEW."STATUS"
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fn_edelta_cdc_sales_orders_upd()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO "_EDELTA_CDC_SALES_ORDERS" (op_type, row_key, changed_fields)
    VALUES ('U', NEW."ORDER_ID",
        json_build_object(
            'ORDER_ID', NEW."ORDER_ID",
            'CUSTOMER_ID', NEW."CUSTOMER_ID",
            'TOTAL_AMOUNT', NEW."TOTAL_AMOUNT",
            'STATUS', NEW."STATUS"
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fn_edelta_cdc_sales_orders_del()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO "_EDELTA_CDC_SALES_ORDERS" (op_type, row_key, changed_fields)
    VALUES ('D', OLD."ORDER_ID", NULL);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Create triggers on the simulated source table
DROP TRIGGER IF EXISTS tr_edelta_cdc_sales_ins ON "SAPSR3"."SALES_ORDERS";
CREATE TRIGGER tr_edelta_cdc_sales_ins
    AFTER INSERT ON "SAPSR3"."SALES_ORDERS"
    FOR EACH ROW EXECUTE FUNCTION fn_edelta_cdc_sales_orders_ins();

DROP TRIGGER IF EXISTS tr_edelta_cdc_sales_upd ON "SAPSR3"."SALES_ORDERS";
CREATE TRIGGER tr_edelta_cdc_sales_upd
    AFTER UPDATE ON "SAPSR3"."SALES_ORDERS"
    FOR EACH ROW EXECUTE FUNCTION fn_edelta_cdc_sales_orders_upd();

DROP TRIGGER IF EXISTS tr_edelta_cdc_sales_del ON "SAPSR3"."SALES_ORDERS";
CREATE TRIGGER tr_edelta_cdc_sales_del
    AFTER DELETE ON "SAPSR3"."SALES_ORDERS"
    FOR EACH ROW EXECUTE FUNCTION fn_edelta_cdc_sales_orders_del();

-- Create the schema if needed
CREATE SCHEMA IF NOT EXISTS "SAPSR3";

-- Simulated SYS views for schema introspection
CREATE TABLE IF NOT EXISTS "SYS_TABLES" (
    "SCHEMA_NAME" VARCHAR(255),
    "TABLE_NAME" VARCHAR(255),
    "TABLE_TYPE" VARCHAR(20) DEFAULT 'TABLE',
    "RECORD_COUNT" BIGINT DEFAULT 0
);

INSERT INTO "SYS_TABLES" VALUES
    ('SAPSR3', 'SALES_ORDERS', 'TABLE', 1200000),
    ('SAPSR3', 'CUSTOMER_MASTER', 'TABLE', 450000),
    ('SAPSR3', 'MATERIAL_DOC', 'TABLE', 3800000),
    ('SAPSR3', 'BILLING_DOC', 'TABLE', 890000),
    ('SAPSR3', 'PURCHASE_ORDER', 'TABLE', 620000),
    ('SAPSR3', 'INVENTORY_MGMT', 'TABLE', 2100000)
ON CONFLICT DO NOTHING;
