import type { ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";
import {
  Activity,
  Database,
  BarChart3,
  FileText,
  GitBranch,
  Layers,
  Shield,
  Target,
  Settings,
  Zap,
} from "lucide-react";

const NAV_ITEMS = [
  { path: "/dashboard", label: "Dashboard", icon: BarChart3 },
  { path: "/schema", label: "Schema Browser", icon: Database },
  { path: "/streams", label: "Streams", icon: Activity },
  { path: "/quality", label: "Data Quality", icon: Shield },
  { path: "/lineage", label: "Lineage", icon: GitBranch },
  { path: "/logs", label: "Logs", icon: FileText },
];

const ADMIN_ITEMS = [
  { path: "/admin/sources", label: "Sources", icon: Layers },
  { path: "/admin/targets", label: "Targets", icon: Target },
  { path: "/admin/settings", label: "Settings", icon: Settings },
];

export function AppShell({ children }: { children: ReactNode }) {
  const location = useLocation();

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar */}
      <aside className="w-56 bg-gray-900 text-gray-300 flex flex-col">
        <div className="px-4 py-5 flex items-center gap-2">
          <Zap className="w-6 h-6 text-emerald-400" />
          <span className="text-white font-bold text-lg">eDeltaShared</span>
        </div>

        <nav className="flex-1 px-2 py-3 space-y-0.5">
          {NAV_ITEMS.map(({ path, label, icon: Icon }) => {
            const active = location.pathname === path;
            return (
              <Link
                key={path}
                to={path}
                className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors ${
                  active
                    ? "bg-gray-800 text-white"
                    : "hover:bg-gray-800 hover:text-white"
                }`}
              >
                <Icon className="w-4 h-4" />
                {label}
              </Link>
            );
          })}

          <div className="pt-6 pb-2 px-3 text-xs uppercase tracking-wider text-gray-500">
            Admin
          </div>
          {ADMIN_ITEMS.map(({ path, label, icon: Icon }) => {
            const active = location.pathname === path;
            return (
              <Link
                key={path}
                to={path}
                className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors ${
                  active
                    ? "bg-gray-800 text-white"
                    : "hover:bg-gray-800 hover:text-white"
                }`}
              >
                <Icon className="w-4 h-4" />
                {label}
              </Link>
            );
          })}
        </nav>

        <div className="px-4 py-3 border-t border-gray-800 text-xs text-gray-500">
          v0.1.0 &middot; CDC Pipeline
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-auto">
        <div className="p-6">{children}</div>
      </main>
    </div>
  );
}
