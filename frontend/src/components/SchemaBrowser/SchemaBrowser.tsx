import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Search,
  Database,
  ChevronRight,
  ChevronDown,
  Table,
  Layers,
} from "lucide-react";
import { getSchema } from "../../services/api";
import type { FieldInfo, TableInfo } from "../../types/api";

export function SchemaBrowser() {
  const [search, setSearch] = useState("");
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(
    new Set(["SAPSR3"])
  );

  const { data: tables, isLoading } = useQuery({
    queryKey: ["schema-tables", search],
    queryFn: () => getSchema.tables(search || undefined),
    placeholderData: (prev) => prev,
  });

  const { data: fields } = useQuery({
    queryKey: [
      "schema-fields",
      selectedTable?.schema_name,
      selectedTable?.table_name,
    ],
    queryFn: () =>
      getSchema.fields(selectedTable!.schema_name, selectedTable!.table_name),
    enabled: !!selectedTable,
  });

  // Group tables by schema
  const bySchema: Record<string, TableInfo[]> = {};
  for (const t of tables ?? []) {
    (bySchema[t.schema_name] ??= []).push(t);
  }

  const toggleSchema = (schema: string) => {
    setExpandedSchemas((prev) => {
      const next = new Set(prev);
      if (next.has(schema)) next.delete(schema);
      else next.add(schema);
      return next;
    });
  };

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">Schema Browser</h1>

      <div className="flex gap-6 h-[calc(100vh-160px)]">
        {/* Left panel: Table tree */}
        <div className="w-80 bg-white rounded-lg border border-gray-200 flex flex-col">
          <div className="p-3 border-b border-gray-200">
            <div className="relative">
              <Search className="absolute left-3 top-2.5 w-4 h-4 text-gray-400" />
              <input
                type="text"
                placeholder="Search tables..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>

          <div className="flex-1 overflow-auto p-2">
            {isLoading && (
              <p className="text-sm text-gray-400 p-3">Loading schema...</p>
            )}
            {Object.entries(bySchema).map(([schema, schemaTables]) => (
              <div key={schema}>
                <button
                  onClick={() => toggleSchema(schema)}
                  className="flex items-center gap-2 w-full px-2 py-1.5 text-sm font-medium text-gray-700 hover:bg-gray-50 rounded"
                >
                  {expandedSchemas.has(schema) ? (
                    <ChevronDown className="w-3.5 h-3.5" />
                  ) : (
                    <ChevronRight className="w-3.5 h-3.5" />
                  )}
                  <Database className="w-3.5 h-3.5 text-gray-500" />
                  {schema}
                  <span className="ml-auto text-xs text-gray-400">
                    {schemaTables.length}
                  </span>
                </button>

                {expandedSchemas.has(schema) && (
                  <div className="ml-4">
                    {schemaTables.map((t) => {
                      const isSelected =
                        selectedTable?.table_name === t.table_name &&
                        selectedTable?.schema_name === t.schema_name;
                      const Icon =
                        t.table_type === "VIEW" || t.table_type === "CDS_VIEW"
                          ? Layers
                          : Table;
                      return (
                        <button
                          key={`${t.schema_name}.${t.table_name}`}
                          onClick={() => setSelectedTable(t)}
                          className={`flex items-center gap-2 w-full px-2 py-1.5 text-sm rounded ${
                            isSelected
                              ? "bg-blue-50 text-blue-700"
                              : "text-gray-600 hover:bg-gray-50"
                          }`}
                        >
                          <Icon className="w-3.5 h-3.5" />
                          <span className="truncate">{t.table_name}</span>
                          {t.row_count != null && (
                            <span className="ml-auto text-xs text-gray-400">
                              {t.row_count.toLocaleString()}
                            </span>
                          )}
                        </button>
                      );
                    })}
                  </div>
                )}
              </div>
            ))}
            {!isLoading && Object.keys(bySchema).length === 0 && (
              <p className="text-sm text-gray-400 p-3 text-center">
                No tables found. Check HANA connection.
              </p>
            )}
          </div>
        </div>

        {/* Right panel: Field details */}
        <div className="flex-1 bg-white rounded-lg border border-gray-200">
          {selectedTable ? (
            <div>
              <div className="px-5 py-4 border-b border-gray-200">
                <h2 className="font-semibold text-gray-900">
                  {selectedTable.schema_name}.{selectedTable.table_name}
                </h2>
                <p className="text-sm text-gray-500 mt-0.5">
                  {selectedTable.table_type} &middot;{" "}
                  {selectedTable.row_count?.toLocaleString() ?? "?"} rows
                </p>
              </div>

              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100 text-left text-gray-500">
                    <th className="px-5 py-3 font-medium">Column</th>
                    <th className="px-5 py-3 font-medium">Type</th>
                    <th className="px-5 py-3 font-medium">Nullable</th>
                    <th className="px-5 py-3 font-medium">PK</th>
                  </tr>
                </thead>
                <tbody>
                  {(fields ?? []).map((f: FieldInfo) => (
                    <tr
                      key={f.column_name}
                      className="border-b border-gray-50 hover:bg-gray-50"
                    >
                      <td className="px-5 py-2.5 font-mono text-xs">
                        {f.column_name}
                      </td>
                      <td className="px-5 py-2.5">
                        <span className="inline-flex px-2 py-0.5 rounded bg-gray-100 text-gray-600 text-xs">
                          {f.data_type}
                          {f.length ? `(${f.length})` : ""}
                        </span>
                      </td>
                      <td className="px-5 py-2.5 text-gray-500">
                        {f.is_nullable ? "Yes" : "No"}
                      </td>
                      <td className="px-5 py-2.5">
                        {f.is_primary_key && (
                          <span className="inline-flex px-2 py-0.5 rounded bg-amber-100 text-amber-700 text-xs font-medium">
                            PK
                          </span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="flex items-center justify-center h-full text-gray-400 text-sm">
              Select a table to view its fields
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
