import { useMemo } from "react";
import { Page } from "@/App";
import { Sparkline } from "@/charts/Sparkline";
import { api } from "@/lib/api";
import { useOverview } from "@/lib/cluster";
import { bytes, percent, plural } from "@/lib/format";
import { useNow, usePoll, whenDaemonDown } from "@/lib/poll";
import type { ClusterNode, MetricSeries } from "@/lib/types";
import { cn } from "@/ui/cn";
import { Meter } from "@/ui/controls";
import { DaemonBanner } from "@/ui/daemon";
import { Async, EmptyState, PageHeader, SkeletonRows, StaleMark } from "@/ui/panel";
import { NodeState } from "@/ui/status";

const ORDER: Record<string, number> = { healthy: 0, stale: 1, unavailable: 2 };

function LoadRow({ label, value, detail, children }: { label: string; value: string; detail?: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[4.75rem_minmax(0,1fr)] items-center gap-x-3 gap-y-0.5">
      <span className="text-xs text-fg-3">{label}</span>
      <div className="flex items-center gap-2.5">
        <div className="min-w-0 flex-1">{children}</div>
        <span className="num w-11 shrink-0 text-right text-xs font-medium">{value}</span>
      </div>
      {detail && <span className="num col-start-2 text-xs text-fg-3">{detail}</span>}
    </div>
  );
}

function Counter({ label, value, alert }: { label: string; value: number; alert?: boolean }) {
  return (
    <div>
      <dt className="text-xs text-fg-3">{label}</dt>
      <dd className={cn("num text-sm font-medium", alert && value > 0 && "text-critical-fg")}>{value}</dd>
    </div>
  );
}

function History({ series, has }: { series: MetricSeries | undefined; has: { gpu: boolean } }) {
  const times = useMemo(() => series?.points.map((p) => p.t) ?? [], [series]);
  if (!series || !times.length) return <p className="text-xs text-fg-3">No history sampled for this node yet.</p>;
  const rows = [
    { key: "cpu" as const, label: "CPU" },
    { key: "mem" as const, label: "Memory" },
    ...(has.gpu ? [{ key: "gpu_util" as const, label: "GPU" }] : []),
  ];
  return (
    <div className="flex flex-col gap-2">
      {rows.map((r) => {
        const values = series.points.map((p) => p[r.key]);
        const vals = values.filter((v): v is number => v !== null);
        return (
          <div key={r.key} className="grid grid-cols-[3.25rem_minmax(0,1fr)_4.5rem] items-center gap-x-2">
            <span className="text-xs text-fg-3">{r.label}</span>
            <Sparkline times={times} values={values} label={`${r.label} over the last 15 minutes`} />
            <span className="num text-right text-xs text-fg-3">{vals.length ? `peak ${percent(Math.max(...vals))}` : "no readings"}</span>
          </div>
        );
      })}
    </div>
  );
}

function NodeCard({ node, series }: { node: ClusterNode; series: MetricSeries | undefined }) {
  const l = node.load;
  const gpus = l.gpus ?? [];
  const gone = node.state === "unavailable";
  const stale = node.state === "stale";
  const totalMem = l.total_mem_bytes ?? 0;
  const usedMem = totalMem - (l.available_mem_bytes ?? 0);
  const caps = Object.entries(node.capabilities ?? {}).sort(([a], [b]) => a.localeCompare(b));

  return (
    <article className={cn("rounded-lg border bg-surface", gone ? "border-line" : stale ? "border-warn/50" : "border-line")} aria-label={`${node.hostname}, ${node.state}`}>
      <header className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-line px-4 py-2.5">
        <h2 className={cn("text-[0.9375rem] font-semibold", gone && "text-fg-2")}>{node.hostname}</h2>
        <NodeState state={node.state} />
        {node.is_self && <span className="rounded-sm border border-line-strong px-1.5 text-xs leading-[1.125rem] font-medium text-fg-2">this machine</span>}
        <span className="num text-xs text-fg-3" title="Scheduler health score, 0 to 1">
          health {node.health_score.toFixed(2)}
        </span>
        <span className="ml-auto flex flex-wrap items-center gap-x-3 font-mono text-xs text-fg-3">
          <span>{node.ssh_endpoint}</span>
          <span>daemon :{node.daemon_port}</span>
          {node.source && <span className="font-sans">via {node.source}</span>}
        </span>
      </header>

      {(gone || stale) && (
        <p className={cn("border-b border-line px-4 py-2 text-sm", gone ? "bg-critical/6 text-fg-2" : "bg-warn/8 text-fg-2")}>
          {gone
            ? "No heartbeat. The scheduler does not place jobs here, and the registry drops the node if it stays away. Check that the machine is on and the daemon is running."
            : "Heartbeats are late. The numbers below are the last ones this node reported; the scheduler ranks it below healthy nodes."}
        </p>
      )}

      <div className={cn("grid grid-cols-1 gap-x-8 gap-y-5 p-4 md:grid-cols-2 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)_minmax(0,1fr)]", gone && "opacity-55")}>
        <div className="flex flex-col gap-2.5">
          <LoadRow label="CPU" value={gone ? "–" : percent(l.cpu_percent)} detail={`${l.total_cpus} cores`}>
            <Meter label="CPU in use" value={gone ? 0 : l.cpu_percent} tone={gone ? "muted" : "auto"} />
          </LoadRow>
          <LoadRow label="Memory" value={gone ? "–" : percent(l.memory_percent)} detail={!totalMem ? undefined : gone ? `${bytes(totalMem)} installed` : `${bytes(usedMem)} of ${bytes(totalMem)} used${l.reserved_mem_bytes ? `, ${bytes(l.reserved_mem_bytes)} reserved for jobs` : ""}`}>
            <Meter label="Memory in use" value={gone ? 0 : l.memory_percent} reserved={totalMem ? ((l.reserved_mem_bytes ?? 0) / totalMem) * 100 : 0} tone={gone ? "muted" : "auto"} />
          </LoadRow>
          {gpus.length > 0 ? (
            gpus.map((g) => {
              const used = g.memory_total_bytes - g.memory_free_bytes;
              return (
                <div key={g.index} className="flex flex-col gap-1.5 border-t border-line pt-2.5">
                  <div className="flex items-baseline justify-between gap-2 text-xs">
                    <span className="truncate font-medium text-fg-2" title={g.name}>
                      GPU {g.index} · {g.name.replace(/^NVIDIA (GeForce )?/, "")}
                    </span>
                  </div>
                  <LoadRow label="Utilisation" value={gone ? "–" : percent(g.utilization_gpu_percent)}>
                    <Meter label={`GPU ${g.index} utilisation`} value={gone ? 0 : g.utilization_gpu_percent} tone={gone ? "muted" : "accent"} />
                  </LoadRow>
                  <LoadRow label="VRAM" value={gone || !g.memory_total_bytes ? "–" : percent((used / g.memory_total_bytes) * 100)} detail={gone ? `${bytes(g.memory_total_bytes)} installed` : `${bytes(used)} of ${bytes(g.memory_total_bytes)}`}>
                    <Meter label={`GPU ${g.index} memory in use`} value={gone ? 0 : used} max={g.memory_total_bytes} tone={gone ? "muted" : "auto"} />
                  </LoadRow>
                </div>
              );
            })
          ) : (
            <p className="border-t border-line pt-2.5 text-xs text-fg-3">No GPU on this node.</p>
          )}
        </div>

        <div className="flex flex-col gap-3">
          <h3 className="text-xs font-medium text-fg-2">Last 15 minutes</h3>
          <History series={series} has={{ gpu: gpus.length > 0 }} />
        </div>

        <div className="flex flex-col gap-3 md:col-span-2 xl:col-span-1">
          <dl className="grid grid-cols-3 gap-3">
            <Counter label="Active jobs" value={l.active_jobs} />
            <Counter label="Queued" value={l.queue_depth} />
            <Counter label="Recent failures" value={l.recent_failures} alert />
          </dl>
          {caps.length > 0 && (
            <dl className="flex flex-wrap gap-1.5 border-t border-line pt-3">
              {caps.map(([k, v]) => (
                <div key={k} className="inline-flex max-w-full items-baseline gap-1.5 rounded-sm bg-sunken px-1.5 py-0.5 text-xs">
                  <dt className="text-fg-3">{k}</dt>
                  <dd className="truncate font-mono text-fg-2" title={v}>
                    {v}
                  </dd>
                </div>
              ))}
            </dl>
          )}
        </div>
      </div>
    </article>
  );
}

export function NodesPage() {
  const overview = useOverview();
  const nodesRaw = usePoll("nodes", api.nodes, 4000);
  const metrics = usePoll("metrics:15m", (s) => api.metrics("15m", s), 5000);
  const now = useNow(1000);
  const down = overview.data ? !overview.data.daemon.reachable : false;
  const nodes = whenDaemonDown(nodesRaw, down, []);
  const sorted = useMemo(
    () => [...(nodes.data ?? [])].sort((a, b) => Number(b.is_self) - Number(a.is_self) || (ORDER[a.state] ?? 3) - (ORDER[b.state] ?? 3) || a.hostname.localeCompare(b.hostname)),
    [nodes.data],
  );
  const healthy = sorted.filter((n) => n.state === "healthy").length;
  const byId = useMemo(() => new Map((metrics.data?.series ?? []).map((s) => [s.node_id, s])), [metrics.data]);

  return (
    <Page>
      <PageHeader
        title="Nodes"
        meta={
          <span className="flex flex-wrap items-center gap-x-3">
            {nodes.data ? <span className="num">{sorted.length ? `${healthy} of ${plural(sorted.length, "node")} healthy` : "No nodes"}</span> : "Loading nodes"}
            <StaleMark polled={nodes} now={now} />
          </span>
        }
      />
      <DaemonBanner />
      <Async
        polled={nodes}
        skeleton={
          <div className="flex flex-col gap-4">
            {[0, 1, 2].map((i) => (
              <div key={i} className="rounded-lg border border-line bg-surface">
                <SkeletonRows rows={5} />
              </div>
            ))}
          </div>
        }
        isEmpty={(n) => !n.length}
        empty={
          <div className="rounded-lg border border-line bg-surface">
            <EmptyState title={down ? "No nodes while the daemon is stopped" : "No nodes discovered yet"}>
              {down ? "The node list comes from the daemon on this machine." : "Machines on the LAN appear here once their daemon announces itself. Install Pipedpeer on another machine and run pipedpeer start there."}
            </EmptyState>
          </div>
        }
      >
        {() => (
          <div className="flex flex-col gap-4">
            {sorted.map((n) => (
              <NodeCard key={n.node_id} node={n} series={byId.get(n.node_id)} />
            ))}
          </div>
        )}
      </Async>
    </Page>
  );
}
