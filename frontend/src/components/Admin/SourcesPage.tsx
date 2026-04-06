import { useQuery } from "@tanstack/react-query";
import { Database, Cpu, HardDrive, RefreshCw, CheckCircle, XCircle } from "lucide-react";
import { getSchema, health } from "../../services/api";

export function SourcesPage() {
  const { data: sources, isLoading } = useQuery({
    queryKey: ["sources"],
    queryFn: () => fetch("/api/v1/schema/sources").then((r) => r.json()),
  });

  const { data: healthData } = useQuery({
    queryKey: ["health"],
    queryFn: health.check,
    refetchInterval: 15000,
  });

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Source Connections</h1>
      </div>

      <div className="space-y-4">
        {(sources ?? []).map((source: any) => (
          <div key={source.id} className="bg-white rounded-lg border border-gray-200 p-6">
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-3">
                <div className="p-2.5 bg-blue-50 rounded-lg">
                  <Database className="w-5 h-5 text-blue-600" />
                </div>
                <div>
                  <h3 className="font-semibold text-gray-900">{source.name}</h3>
                  <p className="text-sm text-gray-500">
                    {source.host}:{source.port}
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-1.5">
                {source.status === "connected" ? (
                  <CheckCircle className="w-4 h-4 text-emerald-500" />
                ) : (
                  <XCircle className="w-4 h-4 text-red-500" />
                )}
                <span className={`text-sm font-medium ${
                  source.status === "connected" ? "text-emerald-600" : "text-red-600"
                }`}>
                  {source.status}
                </span>
              </div>
            </div>

            <div className="grid grid-cols-4 gap-6 mt-5 pt-5 border-t border-gray-100">
              <div>
                <p className="text-xs text-gray-500">CDC Agent</p>
                <p className="text-sm font-medium text-emerald-600 mt-0.5">Running</p>
              </div>
              <div>
                <p className="text-xs text-gray-500 flex items-center gap-1">
                  <Cpu className="w-3 h-3" /> CPU
                </p>
                <div className="flex items-center gap-2 mt-1">
                  <div className="flex-1 bg-gray-100 rounded-full h-1.5">
                    <div className="bg-emerald-500 h-1.5 rounded-full" style={{ width: "15%" }} />
                  </div>
                  <span className="text-xs text-gray-600">15%</span>
                </div>
              </div>
              <div>
                <p className="text-xs text-gray-500 flex items-center gap-1">
                  <HardDrive className="w-3 h-3" /> Memory
                </p>
                <div className="flex items-center gap-2 mt-1">
                  <div className="flex-1 bg-gray-100 rounded-full h-1.5">
                    <div className="bg-blue-500 h-1.5 rounded-full" style={{ width: "45%" }} />
                  </div>
                  <span className="text-xs text-gray-600">45%</span>
                </div>
              </div>
              <div>
                <p className="text-xs text-gray-500">System</p>
                <p className={`text-sm font-medium mt-0.5 ${
                  healthData?.status === "healthy" ? "text-emerald-600" : "text-amber-600"
                }`}>
                  {healthData?.status ?? "checking..."}
                </p>
              </div>
            </div>

            <div className="flex gap-2 mt-4 pt-4 border-t border-gray-100">
              <button className="px-3 py-1.5 text-xs bg-white border border-gray-200 rounded-md hover:bg-gray-50">
                Test Connection
              </button>
              <button className="px-3 py-1.5 text-xs bg-white border border-gray-200 rounded-md hover:bg-gray-50">
                View Schema
              </button>
              <button className="px-3 py-1.5 text-xs bg-white border border-gray-200 rounded-md hover:bg-gray-50 flex items-center gap-1">
                <RefreshCw className="w-3 h-3" /> Refresh Cache
              </button>
            </div>
          </div>
        ))}

        {!isLoading && (!sources || sources.length === 0) && (
          <div className="text-center py-12 text-gray-400 text-sm">
            No source connections configured
          </div>
        )}
      </div>
    </div>
  );
}
