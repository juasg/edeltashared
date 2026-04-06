import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Activity, Clock, Zap, AlertCircle, Cpu, HardDrive } from "lucide-react";
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Legend,
} from "recharts";
import { streams as streamsApi, metrics as metricsApi, health } from "../../services/api";
import { useMetricsStore } from "../../state/store";
import type { StreamState } from "../../types/api";

function KpiCard({
  label,
  value,
  sub,
  icon: Icon,
  color,
}: {
  label: string;
  value: string | number;
  sub?: string;
  icon: React.ElementType;
  color: string;
}) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-gray-500">{label}</p>
          <p className="text-2xl font-semibold mt-1">{value}</p>
          {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
        </div>
        <div className={`p-3 rounded-lg ${color}`}>
          <Icon className="w-5 h-5 text-white" />
        </div>
      </div>
    </div>
  );
}

const STATUS_COLORS: Record<string, string> = {
  active: "bg-emerald-100 text-emerald-700",
  paused: "bg-gray-100 text-gray-600",
  error: "bg-red-100 text-red-700",
};

const TARGET_COLORS: Record<string, string> = {
  snowflake: "#3b82f6",
  aurora: "#8b5cf6",
  mongodb: "#10b981",
};

export function Dashboard() {
  const { streams: storeStreams, setStreams, setLatency } = useMetricsStore();

  const { data: streamsData } = useQuery({
    queryKey: ["streams"],
    queryFn: streamsApi.list,
    refetchInterval: 5000,
  });

  const { data: latencyData } = useQuery({
    queryKey: ["latency"],
    queryFn: () => metricsApi.latency(),
    refetchInterval: 10000,
  });

  const { data: healthData } = useQuery({
    queryKey: ["health"],
    queryFn: health.check,
    refetchInterval: 15000,
  });

  useEffect(() => {
    if (streamsData) setStreams(streamsData);
  }, [streamsData, setStreams]);

  useEffect(() => {
    if (latencyData) setLatency(latencyData);
  }, [latencyData, setLatency]);

  const allStreams = storeStreams.length > 0 ? storeStreams : streamsData ?? [];
  const activeCount = allStreams.filter((s) => s.status === "active").length;
  const errorCount = allStreams.filter((s) => s.status === "error").length;

  const avgLatency = allStreams.length
    ? Math.round(
        allStreams.reduce((sum, s) => sum + (s.avg_latency_ms ?? 0), 0) /
          Math.max(allStreams.filter((s) => s.avg_latency_ms).length, 1)
      )
    : 0;

  const totalRecords = allStreams.reduce(
    (sum, s) => sum + s.records_processed_total,
    0
  );

  // Latency chart data
  const latencyChart = (latencyData ?? [])
    .slice(0, 30)
    .reverse()
    .map((m) => ({
      time: m.window_end
        ? new Date(m.window_end).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
        : "",
      p50: m.p50,
      p95: m.p95,
      p99: m.p99,
    }));

  // Throughput chart — group by target type
  const throughputChart = (latencyData ?? [])
    .slice(0, 30)
    .reverse()
    .map((m) => ({
      time: m.window_end
        ? new Date(m.window_end).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
        : "",
      [m.target_type]: m.records ?? 0,
    }));

  // Merge throughput entries by time
  const throughputByTime: Record<string, Record<string, number | string>> = {};
  for (const entry of throughputChart) {
    const key = entry.time;
    if (!throughputByTime[key]) throughputByTime[key] = { time: key };
    Object.assign(throughputByTime[key], entry);
  }
  const mergedThroughput = Object.values(throughputByTime);

  // Recent alerts (simulated from stream errors and latency)
  const alerts: { level: "warn" | "error" | "ok"; message: string; time: string }[] = [];
  for (const s of allStreams) {
    if (s.status === "error" && s.error_message) {
      alerts.push({
        level: "error",
        message: `Stream error: ${s.consumer_group} — ${s.error_message}`,
        time: s.updated_at ? new Date(s.updated_at).toLocaleTimeString() : "now",
      });
    }
    if (s.avg_latency_ms && s.avg_latency_ms > 25000) {
      alerts.push({
        level: "warn",
        message: `High latency: ${s.consumer_group} at ${(s.avg_latency_ms / 1000).toFixed(1)}s (approaching 30s SLA)`,
        time: "now",
      });
    }
  }

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">Dashboard</h1>

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-4 mb-6">
        <KpiCard
          label="Active Streams"
          value={activeCount}
          sub={`${allStreams.length} total`}
          icon={Activity}
          color="bg-emerald-500"
        />
        <KpiCard
          label="Avg Latency"
          value={avgLatency ? `${(avgLatency / 1000).toFixed(1)}s` : "--"}
          sub={avgLatency > 20000 ? "approaching SLA" : "within SLA"}
          icon={Clock}
          color={avgLatency > 25000 ? "bg-amber-500" : "bg-blue-500"}
        />
        <KpiCard
          label="Total Records"
          value={totalRecords.toLocaleString()}
          icon={Zap}
          color="bg-purple-500"
        />
        <KpiCard
          label="Errors"
          value={errorCount}
          icon={AlertCircle}
          color={errorCount > 0 ? "bg-red-500" : "bg-gray-400"}
        />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-2 gap-4 mb-6">
        {/* Latency Chart */}
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h2 className="text-sm font-medium text-gray-700 mb-4">
            End-to-End Latency
          </h2>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={latencyChart}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="time" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} unit="ms" />
              <Tooltip />
              <Legend iconSize={8} wrapperStyle={{ fontSize: 11 }} />
              <ReferenceLine
                y={30000}
                stroke="#ef4444"
                strokeDasharray="5 5"
                label={{ value: "30s SLA", position: "right", fill: "#ef4444", fontSize: 10 }}
              />
              <Line type="monotone" dataKey="p50" stroke="#3b82f6" strokeWidth={2} dot={false} name="p50" />
              <Line type="monotone" dataKey="p95" stroke="#f59e0b" strokeWidth={2} dot={false} name="p95" />
              <Line type="monotone" dataKey="p99" stroke="#ef4444" strokeWidth={1.5} dot={false} name="p99" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Throughput Chart */}
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h2 className="text-sm font-medium text-gray-700 mb-4">
            Throughput by Target
          </h2>
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={mergedThroughput}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="time" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend iconSize={8} wrapperStyle={{ fontSize: 11 }} />
              <Area type="monotone" dataKey="snowflake" stackId="1" stroke={TARGET_COLORS.snowflake} fill={TARGET_COLORS.snowflake} fillOpacity={0.3} name="Snowflake" />
              <Area type="monotone" dataKey="aurora" stackId="1" stroke={TARGET_COLORS.aurora} fill={TARGET_COLORS.aurora} fillOpacity={0.3} name="Aurora" />
              <Area type="monotone" dataKey="mongodb" stackId="1" stroke={TARGET_COLORS.mongodb} fill={TARGET_COLORS.mongodb} fillOpacity={0.3} name="MongoDB" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* HANA Health + Streams Table */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        {/* HANA Health */}
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h2 className="text-sm font-medium text-gray-700 mb-4">HANA Sidecar Health</h2>
          <div className="space-y-4">
            <div>
              <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span className="flex items-center gap-1"><Cpu className="w-3 h-3" /> CPU</span>
                <span>--</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2">
                <div className="bg-emerald-500 h-2 rounded-full transition-all" style={{ width: "15%" }} />
              </div>
            </div>
            <div>
              <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span className="flex items-center gap-1"><HardDrive className="w-3 h-3" /> Memory</span>
                <span>--</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2">
                <div className="bg-blue-500 h-2 rounded-full transition-all" style={{ width: "45%" }} />
              </div>
            </div>
            <div className="pt-2 border-t border-gray-100">
              <p className="text-xs text-gray-500">
                Status: <span className={`font-medium ${healthData?.status === "healthy" ? "text-emerald-600" : "text-amber-600"}`}>
                  {healthData?.status ?? "checking..."}
                </span>
              </p>
              <p className="text-xs text-gray-400 mt-1">Auto-pause threshold: 70% CPU</p>
            </div>
          </div>
        </div>

        {/* Active Streams Table */}
        <div className="col-span-2 bg-white rounded-lg border border-gray-200">
          <div className="px-5 py-4 border-b border-gray-200 flex justify-between items-center">
            <h2 className="text-sm font-medium text-gray-700">Active Streams</h2>
            <a href="/streams" className="text-xs text-blue-600 hover:underline">View All</a>
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 text-left text-gray-500">
                <th className="px-5 py-2.5 font-medium">Consumer</th>
                <th className="px-5 py-2.5 font-medium">Status</th>
                <th className="px-5 py-2.5 font-medium">Latency</th>
                <th className="px-5 py-2.5 font-medium">Records</th>
              </tr>
            </thead>
            <tbody>
              {allStreams.slice(0, 6).map((stream: StreamState) => (
                <tr key={stream.id} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="px-5 py-2.5 font-mono text-xs truncate max-w-[200px]">
                    {stream.consumer_group}
                  </td>
                  <td className="px-5 py-2.5">
                    <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-medium ${STATUS_COLORS[stream.status] ?? STATUS_COLORS.error}`}>
                      {stream.status}
                    </span>
                  </td>
                  <td className="px-5 py-2.5">
                    {stream.avg_latency_ms ? `${(stream.avg_latency_ms / 1000).toFixed(1)}s` : "--"}
                  </td>
                  <td className="px-5 py-2.5">{stream.records_processed_total.toLocaleString()}</td>
                </tr>
              ))}
              {allStreams.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-5 py-6 text-center text-gray-400 text-xs">
                    No active streams
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Recent Alerts */}
      {alerts.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h2 className="text-sm font-medium text-gray-700 mb-3">Recent Alerts</h2>
          <div className="space-y-2">
            {alerts.slice(0, 5).map((alert, i) => (
              <div key={i} className={`flex items-start gap-2 p-2.5 rounded text-xs ${
                alert.level === "error" ? "bg-red-50 text-red-700" :
                alert.level === "warn" ? "bg-amber-50 text-amber-700" :
                "bg-emerald-50 text-emerald-700"
              }`}>
                <AlertCircle className="w-3.5 h-3.5 mt-0.5 shrink-0" />
                <span className="flex-1">{alert.message}</span>
                <span className="text-gray-400 shrink-0">{alert.time}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
