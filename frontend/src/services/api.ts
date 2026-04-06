import axios from "axios";
import type {
  ConnectorConfig,
  FieldInfo,
  HealthCheck,
  JobHistory,
  LatencyMetric,
  ReplicationConfig,
  StreamState,
  TableInfo,
} from "../types/api";

const api = axios.create({
  baseURL: "/api/v1",
  headers: { "Content-Type": "application/json" },
});

// Attach JWT token to every request
api.interceptors.request.use((config) => {
  const token = localStorage.getItem("edelta_token");
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Redirect to login on 401
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem("edelta_token");
      if (window.location.pathname !== "/login") {
        window.location.href = "/login";
      }
    }
    return Promise.reject(error);
  }
);

// Schema
export const getSchema = {
  tables: (search?: string) =>
    api.get<TableInfo[]>("/schema/tables", { params: { search } }).then((r) => r.data),
  fields: (schema: string, table: string) =>
    api.get<FieldInfo[]>(`/schema/tables/${schema}/${table}/fields`).then((r) => r.data),
};

// Connectors
export const connectors = {
  list: (type?: string) =>
    api.get<ConnectorConfig[]>("/config/connectors", { params: { type } }).then((r) => r.data),
  create: (data: { type: string; name: string; credentials: Record<string, string>; connection_pool_size?: number }) =>
    api.post<ConnectorConfig>("/config/connectors", data).then((r) => r.data),
};

// Replications
export const replications = {
  list: (enabled?: boolean) =>
    api.get<ReplicationConfig[]>("/config/replications", { params: { enabled } }).then((r) => r.data),
  get: (id: string) =>
    api.get<ReplicationConfig>(`/config/replications/${id}`).then((r) => r.data),
  create: (data: Partial<ReplicationConfig>) =>
    api.post<ReplicationConfig>("/config/replications", data).then((r) => r.data),
  update: (id: string, data: Partial<ReplicationConfig>) =>
    api.patch<ReplicationConfig>(`/config/replications/${id}`, data).then((r) => r.data),
  delete: (id: string) =>
    api.delete(`/config/replications/${id}`),
  test: (id: string) =>
    api.post<{ valid: boolean; checks: { name: string; passed: boolean; error?: string }[] }>(`/config/replications/${id}/test`).then((r) => r.data),
  activate: (id: string) =>
    api.post<ReplicationConfig>(`/config/replications/${id}/activate`).then((r) => r.data),
  pause: (id: string) =>
    api.post<ReplicationConfig>(`/config/replications/${id}/pause`).then((r) => r.data),
};

// Streams
export const streams = {
  list: () =>
    api.get<StreamState[]>("/streams").then((r) => r.data),
  restart: (id: string) =>
    api.post(`/streams/${id}/restart`).then((r) => r.data),
};

// Metrics
export const metrics = {
  latency: (configId?: string) =>
    api.get<LatencyMetric[]>("/metrics/latency", { params: { config_id: configId } }).then((r) => r.data),
};

// Jobs
export const jobs = {
  list: (configId?: string) =>
    api.get<JobHistory[]>("/jobs", { params: { config_id: configId } }).then((r) => r.data),
  triggerInitialLoad: (configId: string, chunkSize?: number) =>
    api.post<JobHistory>("/jobs/initial-load", { config_id: configId, chunk_size: chunkSize }).then((r) => r.data),
  cancel: (id: string) =>
    api.post(`/jobs/${id}/cancel`).then((r) => r.data),
};

// Logs
export const logs = {
  list: (params?: { entity_type?: string; action?: string; limit?: number; offset?: number }) =>
    api.get<{ items: Record<string, unknown>[]; total: number }>("/logs", { params }).then((r) => r.data),
  actions: () =>
    api.get<string[]>("/logs/actions").then((r) => r.data),
};

// Health
export const health = {
  check: () =>
    api.get<HealthCheck>("/health").then((r) => r.data),
};
