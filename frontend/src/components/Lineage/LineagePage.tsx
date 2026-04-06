import { useQuery } from "@tanstack/react-query";
import { ArrowRight, Database, Cloud, Layers, Shield, Zap } from "lucide-react";

const api = (path: string) =>
  fetch(`/api/v1${path}`).then((r) => r.json());

const TARGET_ICONS: Record<string, React.ElementType> = {
  snowflake: Cloud,
  aurora: Database,
  mongodb: Layers,
};

const TARGET_COLORS: Record<string, string> = {
  snowflake: "text-blue-600 bg-blue-50",
  aurora: "text-purple-600 bg-purple-50",
  mongodb: "text-emerald-600 bg-emerald-50",
};

export function LineagePage() {
  const { data: lineage, isLoading } = useQuery({
    queryKey: ["lineage"],
    queryFn: () => api("/quality/lineage"),
  });

  // Group by source table
  const byTable: Record<string, any[]> = {};
  for (const entry of lineage ?? []) {
    const key = `${entry.source_schema}.${entry.source_table}`;
    (byTable[key] ??= []).push(entry);
  }

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-2">Data Lineage</h1>
      <p className="text-sm text-gray-500 mb-6">
        Tracks the flow of each column from HANA source through transformations to target databases.
      </p>

      {isLoading && <p className="text-gray-400 text-sm">Loading lineage...</p>}

      {Object.entries(byTable).map(([tableName, columns]) => (
        <div key={tableName} className="bg-white rounded-lg border border-gray-200 mb-4">
          <div className="px-5 py-4 border-b border-gray-200 flex items-center gap-2">
            <Database className="w-4 h-4 text-gray-500" />
            <h2 className="font-semibold text-gray-900">{tableName}</h2>
            <span className="text-xs text-gray-400 ml-2">{columns.length} columns tracked</span>
          </div>

          <div className="divide-y divide-gray-50">
            {columns.map((col: any) => {
              const TargetIcon = TARGET_ICONS[col.target_type] ?? Database;
              const targetColor = TARGET_COLORS[col.target_type] ?? "text-gray-600 bg-gray-50";

              return (
                <div key={col.id} className="px-5 py-3 flex items-center gap-3">
                  {/* Source column */}
                  <div className="w-44">
                    <span className="font-mono text-xs text-gray-700">{col.source_column}</span>
                  </div>

                  {/* Transformations */}
                  <div className="flex items-center gap-1.5 flex-1 min-w-0">
                    {(col.transformations ?? []).length > 0 ? (
                      <>
                        <ArrowRight className="w-3.5 h-3.5 text-gray-300 shrink-0" />
                        {col.transformations.map((t: any, i: number) => (
                          <span
                            key={i}
                            className="inline-flex items-center gap-1 px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs shrink-0"
                          >
                            <Zap className="w-3 h-3" />
                            {t.type}
                          </span>
                        ))}
                      </>
                    ) : (
                      <ArrowRight className="w-3.5 h-3.5 text-gray-300 shrink-0" />
                    )}

                    {col.masking_applied && (
                      <>
                        <ArrowRight className="w-3.5 h-3.5 text-gray-300 shrink-0" />
                        <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-red-50 text-red-700 rounded text-xs shrink-0">
                          <Shield className="w-3 h-3" />
                          PII masked
                        </span>
                      </>
                    )}

                    <ArrowRight className="w-3.5 h-3.5 text-gray-300 shrink-0" />
                  </div>

                  {/* Target */}
                  <div className="flex items-center gap-2 w-52 shrink-0">
                    <div className={`p-1 rounded ${targetColor}`}>
                      <TargetIcon className="w-3.5 h-3.5" />
                    </div>
                    <div>
                      <p className="font-mono text-xs text-gray-700">{col.target_column}</p>
                      <p className="text-[10px] text-gray-400">{col.target_table}</p>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ))}

      {!isLoading && Object.keys(byTable).length === 0 && (
        <div className="text-center py-12 text-gray-400 text-sm">
          No lineage data yet. Lineage is tracked when replications are activated.
        </div>
      )}
    </div>
  );
}
