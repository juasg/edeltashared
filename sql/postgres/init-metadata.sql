-- eDeltaShared Metadata Database Schema
-- PostgreSQL 14+

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ===== Target database connections =====
CREATE TABLE connector_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(20) NOT NULL,              -- 'snowflake' | 'aurora' | 'mongodb'
    name VARCHAR(255) NOT NULL,
    credentials_encrypted TEXT NOT NULL,
    connection_pool_size INT DEFAULT 5,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== Replication configurations (CDC-first) =====
CREATE TABLE replication_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_table_schema VARCHAR(255) NOT NULL,
    source_table_name VARCHAR(255) NOT NULL,
    connector_config_id UUID REFERENCES connector_configs(id),
    target_table_name VARCHAR(255) NOT NULL,
    replication_mode VARCHAR(10) DEFAULT 'cdc',     -- 'cdc' (default) | 'batch'
    field_mappings JSONB NOT NULL,                   -- [{source, target, transform}]
    primary_key_fields JSONB NOT NULL,               -- ["ORDER_ID"] for CDC row identity
    is_enabled BOOLEAN DEFAULT TRUE,
    trigger_deployed BOOLEAN DEFAULT FALSE,          -- Are HANA triggers active?
    kafka_topic VARCHAR(255),                        -- Auto-generated topic name
    initial_load_completed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_table_schema, source_table_name, connector_config_id)
);

-- ===== CDC stream state (per config per target) =====
CREATE TABLE stream_state (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID REFERENCES replication_configs(id) ON DELETE CASCADE,
    consumer_group VARCHAR(255) NOT NULL,             -- e.g., 'snowflake-consumer-1'
    last_kafka_offset BIGINT DEFAULT 0,
    last_hana_seq_id BIGINT DEFAULT 0,
    last_processed_at TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'active',              -- 'active' | 'paused' | 'error'
    error_message TEXT,
    records_processed_total BIGINT DEFAULT 0,
    avg_latency_ms INT,                               -- Rolling average
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== Latency tracking (for SLA monitoring) =====
CREATE TABLE latency_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID REFERENCES replication_configs(id),
    target_type VARCHAR(20) NOT NULL,
    p50_latency_ms INT,
    p95_latency_ms INT,
    p99_latency_ms INT,
    max_latency_ms INT,
    records_in_window INT,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ
);

-- ===== Job history (initial loads + batch fallback) =====
CREATE TABLE job_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID REFERENCES replication_configs(id),
    job_type VARCHAR(20) NOT NULL,                    -- 'initial_load' | 'batch_sync'
    status VARCHAR(20) DEFAULT 'queued',              -- 'queued' | 'running' | 'completed' | 'failed' | 'cancelled'
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    records_read BIGINT,
    records_written BIGINT,
    records_failed BIGINT,
    error_details TEXT,
    metrics JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== Schema cache =====
CREATE TABLE schema_cache (
    source_id VARCHAR(255),
    table_name VARCHAR(255),
    table_spec JSONB,
    fields JSONB,
    relationships JSONB,
    cached_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (source_id, table_name)
);

-- ===== Audit log =====
CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type VARCHAR(100),
    entity_id UUID,
    action VARCHAR(50),
    user_id VARCHAR(255),
    changes JSONB,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

-- ===== Indexes =====
CREATE INDEX idx_stream_state_config ON stream_state(config_id);
CREATE INDEX idx_stream_state_status ON stream_state(status);
CREATE INDEX idx_latency_config_window ON latency_metrics(config_id, window_start);
CREATE INDEX idx_job_history_config ON job_history(config_id, created_at DESC);
CREATE INDEX idx_job_history_status ON job_history(status);
CREATE INDEX idx_audit_log_entity ON audit_log(entity_type, entity_id);
CREATE INDEX idx_audit_log_timestamp ON audit_log(timestamp DESC);
CREATE INDEX idx_replication_configs_enabled ON replication_configs(is_enabled);
CREATE INDEX idx_connector_configs_type ON connector_configs(type);
