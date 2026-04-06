-- Data quality rules and validation results

CREATE TABLE data_quality_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID REFERENCES replication_configs(id) ON DELETE CASCADE,
    field_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(30) NOT NULL,        -- 'not_null' | 'range' | 'regex' | 'enum' | 'length' | 'custom_sql'
    rule_params JSONB NOT NULL DEFAULT '{}', -- e.g. {"min": 0, "max": 9999} or {"pattern": "^[A-Z]"}
    severity VARCHAR(10) DEFAULT 'warn',    -- 'warn' | 'reject' | 'quarantine'
    is_enabled BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE data_quality_violations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id UUID REFERENCES data_quality_rules(id) ON DELETE CASCADE,
    config_id UUID REFERENCES replication_configs(id),
    field_name VARCHAR(255),
    field_value TEXT,
    row_key VARCHAR(500),
    violation_type VARCHAR(30),
    severity VARCHAR(10),
    message TEXT,
    event_seq BIGINT,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Transformation definitions per field
CREATE TABLE field_transformations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID REFERENCES replication_configs(id) ON DELETE CASCADE,
    source_field VARCHAR(255) NOT NULL,
    transform_type VARCHAR(30) NOT NULL,   -- 'upper' | 'lower' | 'trim' | 'cast' | 'hash' | 'mask' | 'custom'
    transform_params JSONB DEFAULT '{}',    -- e.g. {"type": "sha256"} or {"mask_type": "email"}
    execution_order INT DEFAULT 0,
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Lineage tracking
CREATE TABLE column_lineage (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID REFERENCES replication_configs(id) ON DELETE CASCADE,
    source_schema VARCHAR(255),
    source_table VARCHAR(255),
    source_column VARCHAR(255),
    target_type VARCHAR(20),               -- 'snowflake' | 'aurora' | 'mongodb'
    target_table VARCHAR(255),
    target_column VARCHAR(255),
    transformations JSONB DEFAULT '[]',     -- [{type, params}]
    masking_applied BOOLEAN DEFAULT FALSE,
    last_synced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Schema evolution log
CREATE TABLE schema_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID REFERENCES replication_configs(id),
    change_type VARCHAR(20) NOT NULL,      -- 'column_added' | 'column_removed' | 'type_changed' | 'pk_changed'
    column_name VARCHAR(255),
    old_definition JSONB,
    new_definition JSONB,
    status VARCHAR(20) DEFAULT 'pending',  -- 'pending' | 'applied' | 'ignored' | 'failed'
    auto_resolved BOOLEAN DEFAULT FALSE,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX idx_dq_rules_config ON data_quality_rules(config_id);
CREATE INDEX idx_dq_violations_config ON data_quality_violations(config_id, created_at DESC);
CREATE INDEX idx_dq_violations_unresolved ON data_quality_violations(resolved, created_at DESC);
CREATE INDEX idx_field_transforms_config ON field_transformations(config_id, source_field);
CREATE INDEX idx_lineage_config ON column_lineage(config_id);
CREATE INDEX idx_schema_changes_config ON schema_changes(config_id, detected_at DESC);
