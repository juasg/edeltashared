-- Enterprise features: RBAC, multi-tenancy, cost tracking, alerting

-- ===== Organizations (multi-tenancy) =====
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    slug VARCHAR(100) NOT NULL UNIQUE,
    is_active BOOLEAN DEFAULT TRUE,
    settings JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== Users =====
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    role VARCHAR(20) NOT NULL DEFAULT 'viewer',  -- 'viewer' | 'operator' | 'admin' | 'super_admin'
    org_id UUID REFERENCES organizations(id),
    is_active BOOLEAN DEFAULT TRUE,
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== API Keys =====
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    org_id UUID REFERENCES organizations(id),
    key_hash VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    scopes JSONB DEFAULT '["read"]',  -- ['read', 'write', 'admin']
    expires_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== Add org_id to existing tables =====
ALTER TABLE connector_configs ADD COLUMN IF NOT EXISTS org_id UUID REFERENCES organizations(id);
ALTER TABLE replication_configs ADD COLUMN IF NOT EXISTS org_id UUID REFERENCES organizations(id);

-- ===== Cost tracking =====
CREATE TABLE stream_costs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_id UUID REFERENCES replication_configs(id),
    org_id UUID REFERENCES organizations(id),
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    records_processed BIGINT DEFAULT 0,
    bytes_transferred BIGINT DEFAULT 0,
    compute_seconds NUMERIC(10,2) DEFAULT 0,
    target_type VARCHAR(20),
    cost_usd NUMERIC(10,4) DEFAULT 0,
    breakdown JSONB DEFAULT '{}',    -- {kafka: 0.01, target_write: 0.05, ...}
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== Notification channels =====
CREATE TABLE notification_channels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID REFERENCES organizations(id),
    channel_type VARCHAR(20) NOT NULL,  -- 'slack' | 'pagerduty' | 'email' | 'webhook'
    name VARCHAR(255) NOT NULL,
    config_encrypted TEXT NOT NULL,      -- Encrypted JSON: webhook URL, API key, etc.
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== Alert routing rules =====
CREATE TABLE alert_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID REFERENCES organizations(id),
    name VARCHAR(255) NOT NULL,
    condition_type VARCHAR(30) NOT NULL,  -- 'latency_breach' | 'error_rate' | 'consumer_down' | 'hana_cpu' | 'custom'
    condition_params JSONB DEFAULT '{}',
    channel_id UUID REFERENCES notification_channels(id),
    severity_filter VARCHAR(10),          -- Only fire for this severity or higher
    cooldown_minutes INT DEFAULT 15,
    is_enabled BOOLEAN DEFAULT TRUE,
    last_fired_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ===== Alert history =====
CREATE TABLE alert_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id UUID REFERENCES alert_rules(id),
    channel_id UUID REFERENCES notification_channels(id),
    alert_type VARCHAR(30),
    severity VARCHAR(10),
    message TEXT,
    payload JSONB,
    delivery_status VARCHAR(20) DEFAULT 'pending',  -- 'pending' | 'sent' | 'failed'
    error_details TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_users_org ON users(org_id);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_api_keys_user ON api_keys(user_id);
CREATE INDEX idx_stream_costs_config ON stream_costs(config_id, period_start);
CREATE INDEX idx_stream_costs_org ON stream_costs(org_id, period_start);
CREATE INDEX idx_notification_channels_org ON notification_channels(org_id);
CREATE INDEX idx_alert_rules_org ON alert_rules(org_id);
CREATE INDEX idx_alert_history_rule ON alert_history(rule_id, created_at DESC);

-- Seed default org only. Admin user must be created via CLI or first-run setup.
-- DO NOT seed passwords in SQL — use: POST /api/v1/auth/setup (first-run endpoint)
INSERT INTO organizations (id, name, slug) VALUES
    ('00000000-0000-0000-0000-000000000001', 'Default', 'default')
ON CONFLICT DO NOTHING;
