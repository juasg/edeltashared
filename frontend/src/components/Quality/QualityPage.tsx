import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Shield, AlertTriangle, CheckCircle, XCircle, Filter } from "lucide-react";

const api = (path: string, params?: Record<string, string>) => {
  const url = new URL(`/api/v1${path}`, window.location.origin);
  if (params) Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  return fetch(url.toString()).then((r) => r.json());
};

const SEVERITY_STYLES: Record<string, { bg: string; icon: React.ElementType }> = {
  warn: { bg: "bg-amber-100 text-amber-700", icon: AlertTriangle },
  reject: { bg: "bg-red-100 text-red-700", icon: XCircle },
  quarantine: { bg: "bg-purple-100 text-purple-700", icon: Shield },
};

export function QualityPage() {
  const [activeTab, setActiveTab] = useState<"rules" | "violations" | "transforms" | "schema">("rules");

  const { data: rules } = useQuery({
    queryKey: ["quality-rules"],
    queryFn: () => api("/quality/rules"),
  });

  const { data: violations } = useQuery({
    queryKey: ["quality-violations"],
    queryFn: () => api("/quality/violations", { limit: "50" }),
  });

  const { data: transforms } = useQuery({
    queryKey: ["quality-transforms"],
    queryFn: () => api("/quality/transforms"),
  });

  const { data: schemaChanges } = useQuery({
    queryKey: ["schema-changes"],
    queryFn: () => api("/quality/schema-changes"),
  });

  const { data: summary } = useQuery({
    queryKey: ["violation-summary"],
    queryFn: () => api("/quality/violations/summary"),
  });

  const totalViolations = (summary ?? []).reduce((s: number, r: any) => s + r.count, 0);
  const rejectCount = (summary ?? []).filter((r: any) => r.severity === "reject").reduce((s: number, r: any) => s + r.count, 0);

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">Data Quality</h1>

      {/* Summary cards */}
      <div className="grid grid-cols-4 gap-4 mb-6">
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <p className="text-sm text-gray-500">Active Rules</p>
          <p className="text-2xl font-semibold mt-1">{(rules ?? []).length}</p>
        </div>
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <p className="text-sm text-gray-500">Open Violations</p>
          <p className="text-2xl font-semibold mt-1 text-amber-600">{totalViolations}</p>
        </div>
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <p className="text-sm text-gray-500">Rejected Events</p>
          <p className="text-2xl font-semibold mt-1 text-red-600">{rejectCount}</p>
        </div>
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <p className="text-sm text-gray-500">Transforms Active</p>
          <p className="text-2xl font-semibold mt-1">{(transforms ?? []).length}</p>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 mb-4 border-b border-gray-200">
        {(["rules", "violations", "transforms", "schema"] as const).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors capitalize ${
              activeTab === tab
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {tab === "schema" ? "Schema Changes" : tab}
          </button>
        ))}
      </div>

      {/* Rules tab */}
      {activeTab === "rules" && (
        <div className="bg-white rounded-lg border border-gray-200">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 text-left text-gray-500">
                <th className="px-5 py-3 font-medium">Field</th>
                <th className="px-5 py-3 font-medium">Rule Type</th>
                <th className="px-5 py-3 font-medium">Parameters</th>
                <th className="px-5 py-3 font-medium">Severity</th>
                <th className="px-5 py-3 font-medium">Status</th>
              </tr>
            </thead>
            <tbody>
              {(rules ?? []).map((rule: any) => {
                const style = SEVERITY_STYLES[rule.severity] ?? SEVERITY_STYLES.warn;
                return (
                  <tr key={rule.id} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="px-5 py-3 font-mono text-xs">{rule.field_name}</td>
                    <td className="px-5 py-3">
                      <span className="px-2 py-0.5 bg-gray-100 rounded text-xs">{rule.rule_type}</span>
                    </td>
                    <td className="px-5 py-3 text-xs text-gray-500 max-w-xs truncate">
                      {JSON.stringify(rule.rule_params)}
                    </td>
                    <td className="px-5 py-3">
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${style.bg}`}>
                        {rule.severity}
                      </span>
                    </td>
                    <td className="px-5 py-3">
                      {rule.is_enabled ? (
                        <CheckCircle className="w-4 h-4 text-emerald-500" />
                      ) : (
                        <XCircle className="w-4 h-4 text-gray-400" />
                      )}
                    </td>
                  </tr>
                );
              })}
              {(rules ?? []).length === 0 && (
                <tr><td colSpan={5} className="px-5 py-8 text-center text-gray-400">No rules configured</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Violations tab */}
      {activeTab === "violations" && (
        <div className="bg-white rounded-lg border border-gray-200">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 text-left text-gray-500">
                <th className="px-5 py-3 font-medium">Time</th>
                <th className="px-5 py-3 font-medium">Field</th>
                <th className="px-5 py-3 font-medium">Type</th>
                <th className="px-5 py-3 font-medium">Severity</th>
                <th className="px-5 py-3 font-medium">Message</th>
              </tr>
            </thead>
            <tbody>
              {(violations?.items ?? []).map((v: any) => (
                <tr key={v.id} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="px-5 py-2.5 text-xs text-gray-500 font-mono">
                    {v.created_at ? new Date(v.created_at).toLocaleString() : "--"}
                  </td>
                  <td className="px-5 py-2.5 font-mono text-xs">{v.field_name}</td>
                  <td className="px-5 py-2.5">
                    <span className="px-2 py-0.5 bg-gray-100 rounded text-xs">{v.violation_type}</span>
                  </td>
                  <td className="px-5 py-2.5">
                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                      (SEVERITY_STYLES[v.severity] ?? SEVERITY_STYLES.warn).bg
                    }`}>{v.severity}</span>
                  </td>
                  <td className="px-5 py-2.5 text-xs text-gray-600 max-w-md truncate">{v.message}</td>
                </tr>
              ))}
              {(violations?.items ?? []).length === 0 && (
                <tr><td colSpan={5} className="px-5 py-8 text-center text-gray-400">No violations</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Transforms tab */}
      {activeTab === "transforms" && (
        <div className="bg-white rounded-lg border border-gray-200">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 text-left text-gray-500">
                <th className="px-5 py-3 font-medium">Field</th>
                <th className="px-5 py-3 font-medium">Transform</th>
                <th className="px-5 py-3 font-medium">Parameters</th>
                <th className="px-5 py-3 font-medium">Order</th>
              </tr>
            </thead>
            <tbody>
              {(transforms ?? []).map((t: any) => (
                <tr key={t.id} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="px-5 py-3 font-mono text-xs">{t.source_field}</td>
                  <td className="px-5 py-3">
                    <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs">{t.transform_type}</span>
                  </td>
                  <td className="px-5 py-3 text-xs text-gray-500">{JSON.stringify(t.transform_params)}</td>
                  <td className="px-5 py-3 text-xs text-gray-500">{t.execution_order}</td>
                </tr>
              ))}
              {(transforms ?? []).length === 0 && (
                <tr><td colSpan={4} className="px-5 py-8 text-center text-gray-400">No transforms configured</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Schema Changes tab */}
      {activeTab === "schema" && (
        <div className="bg-white rounded-lg border border-gray-200">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 text-left text-gray-500">
                <th className="px-5 py-3 font-medium">Detected</th>
                <th className="px-5 py-3 font-medium">Type</th>
                <th className="px-5 py-3 font-medium">Column</th>
                <th className="px-5 py-3 font-medium">Status</th>
              </tr>
            </thead>
            <tbody>
              {(schemaChanges ?? []).map((c: any) => (
                <tr key={c.id} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="px-5 py-2.5 text-xs text-gray-500 font-mono">
                    {c.detected_at ? new Date(c.detected_at).toLocaleString() : "--"}
                  </td>
                  <td className="px-5 py-2.5">
                    <span className={`px-2 py-0.5 rounded text-xs ${
                      c.change_type === "column_added" ? "bg-emerald-100 text-emerald-700" :
                      c.change_type === "column_removed" ? "bg-red-100 text-red-700" :
                      "bg-amber-100 text-amber-700"
                    }`}>{c.change_type}</span>
                  </td>
                  <td className="px-5 py-2.5 font-mono text-xs">{c.column_name}</td>
                  <td className="px-5 py-2.5">
                    <span className={`px-2 py-0.5 rounded text-xs ${
                      c.status === "applied" ? "bg-emerald-100 text-emerald-700" :
                      c.status === "pending" ? "bg-amber-100 text-amber-700" :
                      "bg-gray-100 text-gray-600"
                    }`}>{c.status}</span>
                  </td>
                </tr>
              ))}
              {(schemaChanges ?? []).length === 0 && (
                <tr><td colSpan={4} className="px-5 py-8 text-center text-gray-400">No schema changes detected</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
