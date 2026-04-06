import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Plus, Trash2, CheckCircle, XCircle, Database, Cloud, Layers } from "lucide-react";
import { connectors } from "../../services/api";
import type { ConnectorConfig } from "../../types/api";

const TARGET_ICONS: Record<string, React.ElementType> = {
  snowflake: Cloud,
  aurora: Database,
  mongodb: Layers,
};

const TARGET_COLORS: Record<string, string> = {
  snowflake: "bg-blue-50 text-blue-600",
  aurora: "bg-purple-50 text-purple-600",
  mongodb: "bg-emerald-50 text-emerald-600",
};

export function TargetsPage() {
  const queryClient = useQueryClient();
  const [showForm, setShowForm] = useState(false);
  const [formType, setFormType] = useState<"snowflake" | "aurora" | "mongodb">("snowflake");
  const [formName, setFormName] = useState("");

  const { data: targets, isLoading } = useQuery({
    queryKey: ["connectors"],
    queryFn: () => connectors.list(),
  });

  const createMutation = useMutation({
    mutationFn: (data: { type: string; name: string; credentials: Record<string, string> }) =>
      connectors.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["connectors"] });
      setShowForm(false);
      setFormName("");
    },
  });

  const handleCreate = () => {
    createMutation.mutate({
      type: formType,
      name: formName,
      credentials: { placeholder: "configure_via_api" },
    });
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Target Connections</h1>
        <button
          onClick={() => setShowForm(true)}
          className="flex items-center gap-2 px-3 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          <Plus className="w-4 h-4" />
          Add Target
        </button>
      </div>

      {/* Add Form */}
      {showForm && (
        <div className="bg-white rounded-lg border border-gray-200 p-5 mb-6">
          <h3 className="font-medium text-gray-900 mb-4">New Target Connection</h3>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs text-gray-500 mb-1">Type</label>
              <select
                value={formType}
                onChange={(e) => setFormType(e.target.value as any)}
                className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md"
              >
                <option value="snowflake">Snowflake</option>
                <option value="aurora">Aurora PostgreSQL</option>
                <option value="mongodb">MongoDB</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Name</label>
              <input
                type="text"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                placeholder="e.g., Production Snowflake"
                className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md"
              />
            </div>
          </div>
          <div className="flex gap-2 mt-4">
            <button
              onClick={handleCreate}
              disabled={!formName || createMutation.isPending}
              className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
            >
              {createMutation.isPending ? "Creating..." : "Create"}
            </button>
            <button
              onClick={() => setShowForm(false)}
              className="px-4 py-2 text-sm bg-white border border-gray-200 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Target Cards */}
      <div className="space-y-3">
        {(targets ?? []).map((target: ConnectorConfig) => {
          const Icon = TARGET_ICONS[target.type] ?? Database;
          const colorClass = TARGET_COLORS[target.type] ?? "bg-gray-50 text-gray-600";

          return (
            <div key={target.id} className="bg-white rounded-lg border border-gray-200 p-5">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className={`p-2.5 rounded-lg ${colorClass}`}>
                    <Icon className="w-5 h-5" />
                  </div>
                  <div>
                    <h3 className="font-medium text-gray-900">{target.name}</h3>
                    <p className="text-xs text-gray-500 mt-0.5">
                      {target.type} &middot; Pool size: {target.connection_pool_size}
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <div className="flex items-center gap-1.5">
                    {target.is_active ? (
                      <CheckCircle className="w-4 h-4 text-emerald-500" />
                    ) : (
                      <XCircle className="w-4 h-4 text-gray-400" />
                    )}
                    <span className={`text-xs font-medium ${target.is_active ? "text-emerald-600" : "text-gray-500"}`}>
                      {target.is_active ? "Active" : "Inactive"}
                    </span>
                  </div>
                  <button className="p-1.5 text-gray-400 hover:text-red-500 rounded">
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              </div>

              <div className="flex gap-2 mt-4 pt-4 border-t border-gray-100">
                <button className="px-3 py-1.5 text-xs bg-white border border-gray-200 rounded-md hover:bg-gray-50">
                  Test Connection
                </button>
                <button className="px-3 py-1.5 text-xs bg-white border border-gray-200 rounded-md hover:bg-gray-50">
                  Edit Credentials
                </button>
              </div>
            </div>
          );
        })}

        {!isLoading && (!targets || targets.length === 0) && (
          <div className="text-center py-12 text-gray-400 text-sm">
            No target connections. Click "Add Target" to get started.
          </div>
        )}
      </div>
    </div>
  );
}
