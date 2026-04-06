export interface ConnectorConfig {
  id: string;
  type: "snowflake" | "aurora" | "mongodb";
  name: string;
  connection_pool_size: number;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface FieldMapping {
  source: string;
  target: string;
  transform?: string | null;
}

export interface ReplicationConfig {
  id: string;
  source_table_schema: string;
  source_table_name: string;
  connector_config_id: string | null;
  target_table_name: string;
  replication_mode: "cdc" | "batch";
  field_mappings: FieldMapping[];
  primary_key_fields: string[];
  is_enabled: boolean;
  trigger_deployed: boolean;
  kafka_topic: string | null;
  initial_load_completed: boolean;
  created_at: string;
  updated_at: string;
}

export interface StreamState {
  id: string;
  config_id: string;
  consumer_group: string;
  last_kafka_offset: number;
  last_hana_seq_id: number;
  last_processed_at: string | null;
  status: "active" | "paused" | "error";
  error_message: string | null;
  records_processed_total: number;
  avg_latency_ms: number | null;
  created_at: string;
  updated_at: string;
}

export interface LatencyMetric {
  config_id: string;
  target_type: string;
  p50: number | null;
  p95: number | null;
  p99: number | null;
  max: number | null;
  records: number | null;
  window_start: string | null;
  window_end: string | null;
}

export interface JobHistory {
  id: string;
  config_id: string;
  job_type: string;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  records_read: number | null;
  records_written: number | null;
  records_failed: number | null;
  error_details: string | null;
  metrics: Record<string, unknown> | null;
  created_at: string;
}

export interface TableInfo {
  schema_name: string;
  table_name: string;
  table_type: string;
  row_count: number | null;
  has_primary_key: boolean;
}

export interface FieldInfo {
  column_name: string;
  data_type: string;
  length: number | null;
  precision: number | null;
  scale: number | null;
  is_nullable: boolean;
  is_primary_key: boolean;
  default_value: string | null;
}

export interface HealthCheck {
  status: "healthy" | "degraded";
  checks: Record<string, string>;
}
