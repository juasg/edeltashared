import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { RefreshCw, Play, AlertCircle } from "lucide-react";
import { streams as streamsApi } from "../../services/api";
import type { StreamState } from "../../types/api";

const STATUS_STYLES: Record<string, { bg: string; dot: string }> = {
  active: { bg: "bg-emerald-50 text-emerald-700", dot: "bg-emerald-500" },
  paused: { bg: "bg-gray-100 text-gray-600", dot: "bg-gray-400" },
  error: { bg: "bg-red-50 text-red-700", dot: "bg-red-500" },
};

export function StreamsPage() {
  const queryClient = useQueryClient();

  const { data: streams, isLoading } = useQuery({
    queryKey: ["streams"],
    queryFn: streamsApi.list,
    refetchInterval: 5000,
  });

  const restartMutation = useMutation({
    mutationFn: (id: string) => streamsApi.restart(id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["streams"] }),
  });

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Streams</h1>
        <button
          onClick={() => queryClient.invalidateQueries({ queryKey: ["streams"] })}
          className="flex items-center gap-2 px-3 py-2 text-sm bg-white border border-gray-200 rounded-md hover:bg-gray-50"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {isLoading ? (
        <p className="text-gray-400 text-sm">Loading streams...</p>
      ) : (
        <div className="space-y-3">
          {(streams ?? []).map((stream: StreamState) => {
            const style = STATUS_STYLES[stream.status] ?? STATUS_STYLES.error;
            return (
              <div
                key={stream.id}
                className="bg-white rounded-lg border border-gray-200 p-5"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <span
                      className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${style.bg}`}
                    >
                      <span className={`w-1.5 h-1.5 rounded-full ${style.dot}`} />
                      {stream.status}
                    </span>
                    <span className="font-mono text-sm text-gray-700">
                      {stream.consumer_group}
                    </span>
                  </div>

                  <div className="flex items-center gap-2">
                    {stream.status === "error" && (
                      <button
                        onClick={() => restartMutation.mutate(stream.id)}
                        disabled={restartMutation.isPending}
                        className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-blue-50 text-blue-700 rounded-md hover:bg-blue-100"
                      >
                        <Play className="w-3.5 h-3.5" />
                        Restart
                      </button>
                    )}
                  </div>
                </div>

                <div className="grid grid-cols-4 gap-6 mt-4 text-sm">
                  <div>
                    <p className="text-gray-500 text-xs">Latency</p>
                    <p className="font-medium">
                      {stream.avg_latency_ms
                        ? `${(stream.avg_latency_ms / 1000).toFixed(1)}s`
                        : "--"}
                    </p>
                  </div>
                  <div>
                    <p className="text-gray-500 text-xs">Records Processed</p>
                    <p className="font-medium">
                      {stream.records_processed_total.toLocaleString()}
                    </p>
                  </div>
                  <div>
                    <p className="text-gray-500 text-xs">Kafka Offset</p>
                    <p className="font-medium font-mono">
                      {stream.last_kafka_offset}
                    </p>
                  </div>
                  <div>
                    <p className="text-gray-500 text-xs">Last Processed</p>
                    <p className="font-medium">
                      {stream.last_processed_at
                        ? new Date(stream.last_processed_at).toLocaleString()
                        : "--"}
                    </p>
                  </div>
                </div>

                {stream.error_message && (
                  <div className="mt-3 flex items-start gap-2 p-3 bg-red-50 rounded text-xs text-red-700">
                    <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
                    {stream.error_message}
                  </div>
                )}
              </div>
            );
          })}
          {(streams ?? []).length === 0 && (
            <div className="text-center py-12 text-gray-400 text-sm">
              No streams yet. Activate a replication from the Schema Browser.
            </div>
          )}
        </div>
      )}
    </div>
  );
}
