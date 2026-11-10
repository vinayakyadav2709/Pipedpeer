import { PowerOff } from "lucide-react";
import { useOverview } from "@/lib/cluster";
import { CopyButton } from "./controls";

/** Explains an unreachable daemon and how to start it. Renders nothing while the daemon is up. */
export function DaemonBanner() {
  const { data } = useOverview();
  if (!data || data.daemon.reachable) return null;
  return (
    <div role="status" className="mb-4 flex flex-wrap items-center gap-x-4 gap-y-2 rounded-lg border border-warn/50 bg-warn/8 px-4 py-3">
      <PowerOff className="size-4 shrink-0 text-warn-fg" aria-hidden />
      <div className="min-w-0 flex-1 basis-64">
        <p className="text-sm font-medium">The Pipedpeer daemon is not running on this machine</p>
        <p className="text-sm text-fg-2">
          Live nodes, load and tasks need it. Job history and notebooks keep working, and cells run on this machine only.
          {data.daemon.error && <span className="mt-0.5 block font-mono text-xs break-words text-fg-3">{data.daemon.error}</span>}
        </p>
      </div>
      <div className="flex items-center gap-1 rounded-md border border-line-strong bg-surface py-0.5 pr-0.5 pl-2.5">
        <code className="text-sm">pipedpeer start</code>
        <CopyButton text="pipedpeer start" label="Copy command" />
      </div>
    </div>
  );
}
