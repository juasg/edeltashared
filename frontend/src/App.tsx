import { Routes, Route, Navigate } from "react-router-dom";
import { AppShell } from "./components/Layout/AppShell";
import { Dashboard } from "./components/Dashboard/Dashboard";
import { SchemaBrowser } from "./components/SchemaBrowser/SchemaBrowser";
import { StreamsPage } from "./components/Streams/StreamsPage";
import { LogsPage } from "./components/Logs/LogsPage";
import { QualityPage } from "./components/Quality/QualityPage";
import { LineagePage } from "./components/Lineage/LineagePage";
import { SourcesPage } from "./components/Admin/SourcesPage";
import { TargetsPage } from "./components/Admin/TargetsPage";
import { useLiveMetrics } from "./hooks/useLiveMetrics";

export default function App() {
  useLiveMetrics();

  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/schema" element={<SchemaBrowser />} />
        <Route path="/streams" element={<StreamsPage />} />
        <Route path="/quality" element={<QualityPage />} />
        <Route path="/lineage" element={<LineagePage />} />
        <Route path="/logs" element={<LogsPage />} />
        <Route path="/admin/sources" element={<SourcesPage />} />
        <Route path="/admin/targets" element={<TargetsPage />} />
      </Routes>
    </AppShell>
  );
}
