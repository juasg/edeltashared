import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Search, Download, ChevronLeft, ChevronRight } from "lucide-react";
import { logs } from "../../services/api";

const TABS = [
  { key: "all", label: "All" },
  { key: "replication_config", label: "CDC Agent" },
  { key: "connector", label: "Consumers" },
  { key: "initial_load", label: "Initial Loads" },
  { key: "error", label: "Errors" },
];

const LEVEL_COLORS: Record<string, string> = {
  triggers_deployed: "bg-emerald-100 text-emerald-700",
  triggers_removed: "bg-gray-100 text-gray-600",
  created: "bg-blue-100 text-blue-700",
  activated: "bg-emerald-100 text-emerald-700",
  paused: "bg-amber-100 text-amber-700",
  deleted: "bg-red-100 text-red-700",
  updated: "bg-blue-100 text-blue-700",
};

export function LogsPage() {
  const [activeTab, setActiveTab] = useState("all");
  const [searchTerm, setSearchTerm] = useState("");
  const [page, setPage] = useState(0);
  const pageSize = 50;

  const entityType = activeTab === "all" ? undefined : activeTab === "error" ? undefined : activeTab;

  const { data, isLoading } = useQuery({
    queryKey: ["logs", activeTab, page],
    queryFn: () =>
      logs.list({
        entity_type: entityType,
        limit: pageSize,
        offset: page * pageSize,
      }),
    placeholderData: (prev) => prev,
  });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Logs</h1>
        <button className="flex items-center gap-2 px-3 py-2 text-sm bg-white border border-gray-200 rounded-md hover:bg-gray-50">
          <Download className="w-4 h-4" />
          Export CSV
        </button>
      </div>

      {/* Search */}
      <div className="mb-4">
        <div className="relative w-80">
          <Search className="absolute left-3 top-2.5 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Search logs..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 mb-4 border-b border-gray-200">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => { setActiveTab(tab.key); setPage(0); }}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab.key
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Log Table */}
      <div className="bg-white rounded-lg border border-gray-200">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 text-left text-gray-500">
              <th className="px-5 py-3 font-medium w-40">Time</th>
              <th className="px-5 py-3 font-medium w-28">Action</th>
              <th className="px-5 py-3 font-medium w-36">Entity</th>
              <th className="px-5 py-3 font-medium">Details</th>
            </tr>
          </thead>
          <tbody>
            {isLoading && (
              <tr>
                <td colSpan={4} className="px-5 py-8 text-center text-gray-400">Loading...</td>
              </tr>
            )}
            {!isLoading && items.length === 0 && (
              <tr>
                <td colSpan={4} className="px-5 py-8 text-center text-gray-400">No log entries found</td>
              </tr>
            )}
            {items
              .filter((item: any) =>
                !searchTerm || JSON.stringify(item).toLowerCase().includes(searchTerm.toLowerCase())
              )
              .map((item: any) => (
                <tr key={item.id} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="px-5 py-2.5 text-gray-500 font-mono text-xs">
                    {item.timestamp ? new Date(item.timestamp).toLocaleString() : "--"}
                  </td>
                  <td className="px-5 py-2.5">
                    <span className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${
                      LEVEL_COLORS[item.action] ?? "bg-gray-100 text-gray-600"
                    }`}>
                      {item.action}
                    </span>
                  </td>
                  <td className="px-5 py-2.5 text-xs text-gray-600">
                    {item.entity_type}
                    {item.entity_id && (
                      <span className="text-gray-400 ml-1 font-mono">
                        {item.entity_id.slice(0, 8)}
                      </span>
                    )}
                  </td>
                  <td className="px-5 py-2.5 text-xs text-gray-600 max-w-md truncate">
                    {item.changes ? JSON.stringify(item.changes).slice(0, 120) : "--"}
                  </td>
                </tr>
              ))}
          </tbody>
        </table>

        {/* Pagination */}
        <div className="px-5 py-3 border-t border-gray-100 flex items-center justify-between text-xs text-gray-500">
          <span>
            Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, total)} of {total}
          </span>
          <div className="flex gap-2">
            <button
              disabled={page === 0}
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              className="flex items-center gap-1 px-2 py-1 rounded border border-gray-200 hover:bg-gray-50 disabled:opacity-40"
            >
              <ChevronLeft className="w-3.5 h-3.5" /> Prev
            </button>
            <button
              disabled={(page + 1) * pageSize >= total}
              onClick={() => setPage((p) => p + 1)}
              className="flex items-center gap-1 px-2 py-1 rounded border border-gray-200 hover:bg-gray-50 disabled:opacity-40"
            >
              Next <ChevronRight className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
