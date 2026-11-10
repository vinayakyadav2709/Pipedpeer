import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Page } from "@/App";
import { Bars } from "@/charts/Bars";
import { bucketLabel, Columns } from "@/charts/Columns";
import { EstimateStrip, estimateTable, summariseEstimates } from "@/charts/EstimateStrip";
import { ChartEmpty, ChartFrame, Legend, type TableView } from "@/charts/frame";
import { seriesColors } from "@/charts/scale";
import { TimeLines, type LineSeries } from "@/charts/TimeLines";
import { api } from "@/lib/api";
import { useOverview } from "@/lib/cluster";
import { ago, bucketNoun, bytes, compact, day, duration, DURATION_TICKS, durationTick, gb, percent, plural, time, timeSec } from "@/lib/format";
import { useNow, usePoll, whenDaemonDown } from "@/lib/poll";
import { jobHref, useRouter } from "@/lib/router";
import type { Analytics, AnalyticsRange, ClusterNode, LiveTask, Metrics, MetricsWindow, Overview } from "@/lib/types";
import { cn } from "@/ui/cn";
import { Meter, Segmented } from "@/ui/controls";
import { DaemonBanner } from "@/ui/daemon";
import { Async, EmptyState, PageHeader, Panel, SkeletonRows, StaleMark } from "@/ui/panel";
import { JobStatus } from "@/ui/status";

const WINDOWS = ["5m", "15m", "1h", "6h"] as const;
const WINDOW_MS: Record<MetricsWindow, number> = { "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "6h": 21_600_000 };
const RANGES = ["1h", "24h", "7d", "30d", "all"] as const;
const RANGE_LABEL: Record<AnalyticsRange, string> = { "1h": "the last hour", "24h": "the last 24 hours", "7d": "the last 7 days", "30d": "the last 30 days", all: "all recorded history" };

function Figure({ label, value, unit, sub, children, className }: { label: string; value: ReactNode; unit?: string; sub?: ReactNode; children?: ReactNode; className?: string }) {
  return (
    <div className={cn("flex min-w-0 flex-col gap-0.5 px-4 py-3", className)}>
      <div className="text-xs text-fg-3">{label}</div>
      <div className="flex items-baseline gap-1.5 whitespace-nowrap">
        <span className="text-2xl font-semibold tracking-[-0.02em]">{value}</span>
        {unit && <span className="text-sm text-fg-2">{unit}</span>}
      </div>
      {children}
      {sub && <div className="num truncate text-xs text-fg-3">{sub}</div>}
    </div>
  );
}

function Summary({ o, nodes }: { o: Overview; nodes: ClusterNode[] | undefined }) {
  const c = o.cluster;
  const down = !o.daemon.reachable;
  const dash = <span className="text-fg-3">–</span>;
  const unhealthy = (nodes ?? []).filter((n) => n.state !== "healthy");
  const stale = unhealthy.filter((n) => n.state === "stale").length;
  const gone = unhealthy.length - stale;
  const memUsed = c.mem_total_bytes - c.mem_available_bytes;
  const finished = o.jobs.succeeded + o.jobs.failed;
  return (
    <div className="grid grid-cols-2 divide-line rounded-lg border border-line bg-surface sm:grid-cols-4 xl:grid-cols-7 xl:divide-x [&>*]:border-line max-xl:[&>*]:border-b max-xl:[&>*:nth-child(odd)]:border-r sm:max-xl:[&>*]:border-r">
      <Figure
        label="Healthy nodes"
        value={down ? dash : c.nodes_healthy}
        unit={down ? undefined : `of ${c.nodes_total}`}
        sub={down ? "daemon not running" : unhealthy.length ? [stale && `${stale} stale`, gone && `${gone} unreachable`].filter(Boolean).join(", ") : "all reachable"}
      />
      <Figure label="CPUs" value={down ? dash : c.cpus_total} sub={down ? " " : "cores on healthy nodes"} />
      <Figure label="Memory available" value={down ? dash : gb(c.mem_available_bytes)} unit={down ? undefined : "GB"} sub={down ? " " : `${c.mem_total_bytes ? Math.round((memUsed / c.mem_total_bytes) * 100) : 0}% of ${gb(c.mem_total_bytes)} GB in use`}>
        {!down && <Meter label="Memory in use" value={memUsed} max={c.mem_total_bytes} tone="auto" className="my-1" />}
      </Figure>
      <Figure label="GPUs" value={down ? dash : c.gpus_total} sub={down ? " " : c.gpus_total ? `${gb(c.gpu_mem_used_bytes)} of ${gb(c.gpu_mem_total_bytes)} GB VRAM` : "none detected"}>
        {!down && c.gpus_total > 0 && <Meter label="VRAM in use" value={c.gpu_mem_used_bytes} max={c.gpu_mem_total_bytes} tone="auto" className="my-1" />}
      </Figure>
      <Figure label="Active jobs" value={down ? o.jobs.running : c.active_jobs} sub={down ? "from job history" : c.queue_depth ? `${c.queue_depth} queued` : "nothing queued"} />
      <Figure label="Success rate" value={finished ? percent(o.jobs.success_rate * 100, o.jobs.failed ? 1 : 0) : dash} sub={finished ? `${compact(o.jobs.failed)} failed of ${compact(finished)}` : "no finished jobs yet"} />
      <Figure className="max-xl:!border-r-0 max-xl:!border-b-0 max-sm:col-span-2 sm:max-xl:col-span-2" label="Median duration" value={finished ? duration(o.jobs.p50_duration_ms) : dash} sub={finished ? `p95 ${duration(o.jobs.p95_duration_ms)}` : " "} />
    </div>
  );
}

const METRICS = [
  { key: "cpu", title: "CPU", noun: "CPU" },
  { key: "mem", title: "Memory", noun: "memory" },
  { key: "gpu_util", title: "GPU utilisation", noun: "GPU" },
  { key: "gpu_mem", title: "GPU memory", noun: "GPU memory" },
] as const;

function LoadCharts({ m, windowKey, now }: { m: Metrics; windowKey: MetricsWindow; now: number }) {
  const [hoverT, setHoverT] = useState<number | null>(null);
  const colors = useMemo(() => seriesColors(m.series.map((s) => s.node_id)), [m.series]);
  const times = useMemo(() => {
    const longest = m.series.reduce<Metrics["series"][number] | undefined>((a, s) => (!a || s.points.length > a.points.length ? s : a), undefined);
    return longest?.points.map((p) => p.t) ?? [];
  }, [m.series]);
  const index = useMemo(() => new Map(times.map((t, i) => [t, i])), [times]);
  const to = Math.max(times[times.length - 1] ?? now, now - m.interval_ms * 2);
  const from = to - WINDOW_MS[windowKey];

  const seriesFor = (key: (typeof METRICS)[number]["key"]): LineSeries[] =>
    m.series.map((s) => {
      const values = new Array<number | null>(times.length).fill(null);
      for (const p of s.points) {
        const i = index.get(p.t);
        if (i !== undefined) values[i] = p[key];
      }
      return { id: s.node_id, label: s.hostname, color: colors.get(s.node_id)!, values };
    });

  if (!m.series.length || !times.length) {
    return (
      <EmptyState title="No load samples yet">
        The console samples each node every {duration(m.interval_ms)} while it is running. The first points appear a few seconds after it can reach the daemon.
      </EmptyState>
    );
  }

  return (
    <div>
      <Legend className="px-4 pt-3" items={m.series.map((s) => ({ label: s.hostname, color: colors.get(s.node_id)!, shape: "line" as const }))} />
      <div className="grid grid-cols-1 gap-x-6 gap-y-4 p-4 md:grid-cols-2">
        {METRICS.map((metric) => {
          const series = seriesFor(metric.key);
          const any = series.some((s) => s.values.some((v) => v !== null));
          const table: TableView = {
            head: ["Node", "Latest", "Mean", "Peak"],
            rows: series.map((s) => {
              const vals = s.values.filter((v): v is number => v !== null);
              const latest = [...s.values].reverse().find((v) => v !== null);
              return [s.label, latest === undefined || latest === null ? "no reading" : percent(latest, 1), vals.length ? percent(vals.reduce((a, v) => a + v, 0) / vals.length, 1) : "–", vals.length ? percent(Math.max(...vals), 1) : "–"];
            }),
          };
          return (
            <ChartFrame key={metric.key} title={metric.title} table={any ? table : undefined}>
              {any ? (
                <TimeLines times={times} series={series} domain={[0, 100]} format={(v) => percent(v, 1)} axisFormat={(v) => `${v}%`} hoverT={hoverT} onHover={setHoverT} from={from} to={to} ariaLabel={`${metric.title} per node over the last ${windowKey}`} />
              ) : (
                <ChartEmpty height={132}>No node reported {metric.noun} readings in this window.</ChartEmpty>
              )}
            </ChartFrame>
          );
        })}
      </div>
    </div>
  );
}

function Tasks({ tasks }: { tasks: LiveTask[] }) {
  return (
    <ul className="divide-y divide-line">
      {tasks.map((t) => (
        <li key={`${t.node_id}/${t.lease_id}`} className="flex flex-col gap-0.5 px-4 py-2.5">
          <div className="flex items-center justify-between gap-2">
            <span className="truncate text-sm font-medium" title={t.job_name}>
              {t.job_name || t.lease_id}
            </span>
            <JobStatus status={t.state} />
          </div>
          <div className="num flex flex-wrap gap-x-2 text-xs text-fg-3">
            <span className="text-fg-2">{t.hostname}</span>
            {t.gpu_index >= 0 && <span>GPU {t.gpu_index}</span>}
            {t.mem_bytes > 0 && <span>{bytes(t.mem_bytes)}</span>}
            <span>{duration(t.age_ms)}</span>
            {t.submitter && t.submitter !== t.hostname && <span>from {t.submitter}</span>}
          </div>
        </li>
      ))}
    </ul>
  );
}

const cell = "min-w-0 p-4";

/** A share of a whole as a percentage, without printing a real but tiny share as "0.0%". */
function share(fraction: number): string {
  if (fraction > 0 && fraction < 0.001) return "<0.1%";
  return percent(fraction * 100, fraction < 0.1 ? 1 : 0);
}

/** Outcome is a status, so these series wear status colours, not categorical ones. */
const OUTCOMES = [
  { label: "succeeded", color: "var(--good)" },
  { label: "failed", color: "var(--critical)" },
  { label: "other", color: "var(--s-mute)" },
];

function JobAnalytics({ a }: { a: Analytics }) {
  const { navigate } = useRouter();
  const [hoverT, setHoverT] = useState<number | null>(null);
  const tiers = useMemo(() => summariseEstimates(a.mem_estimates ?? []), [a.mem_estimates]);
  const throughput = a.throughput ?? [];
  const durations = a.durations ?? [];
  const total = (a.by_status ?? []).reduce((n, s) => n + s.count, 0);
  const xFormat = (t: number) => (a.bucket_ms >= 86_400_000 ? day(t) : a.bucket_ms >= 6 * 3_600_000 ? day(t) : time(t));
  const durTimes = durations.map((d) => d.t);
  const stageTotal = (a.stage_time ?? []).reduce((n, s) => n + s.total_ms, 0);
  const busiest = throughput.some((b) => b.succeeded + b.failed + b.other > 0);

  return (
    <div>
      {total > 0 && (
        <p className="num border-b border-line px-4 py-2 text-sm text-fg-2">
          <span className="font-medium text-fg">{plural(total, "job")}</span> in {RANGE_LABEL[a.range as AnalyticsRange] ?? a.range}:{" "}
          {(a.by_status ?? []).map((s) => `${s.count.toLocaleString()} ${s.status}`).join(", ")}
        </p>
      )}
      <div className="grid grid-cols-1 lg:grid-cols-12 lg:[&>*]:border-line">
        <ChartFrame
          className={cn(cell, "border-b border-line lg:col-span-7 lg:border-r")}
          title="Throughput"
          hint={`jobs started per ${bucketNoun(a.bucket_ms)}, by outcome`}
          legend={busiest ? OUTCOMES : undefined}
          table={busiest ? { head: ["Bucket", "Succeeded", "Failed", "Other"], rows: throughput.map((b) => [bucketLabel(b.t, a.bucket_ms), b.succeeded, b.failed, b.other]) } : undefined}
        >
          {busiest ? (
            <Columns
              ariaLabel="Jobs started over time, split by outcome"
              bucketMs={a.bucket_ms}
              buckets={throughput.map((b) => ({ t: b.t, values: [b.succeeded, b.failed, b.other] }))}
              parts={OUTCOMES}
            />
          ) : (
            <ChartEmpty height={168}>No jobs started in {RANGE_LABEL[a.range as AnalyticsRange] ?? "this range"}. Run one with pipedpeer run and it shows up here.</ChartEmpty>
          )}
        </ChartFrame>

        <ChartFrame
          className={cn(cell, "border-b border-line lg:col-span-5")}
          title="Duration"
          hint={`per ${bucketNoun(a.bucket_ms)} with finished jobs, log scale`}
          legend={
            durations.length
              ? [
                  { label: "p50", color: "var(--s1)", shape: "line" },
                  { label: "p95", color: "var(--s1-light)", shape: "line" },
                ]
              : undefined
          }
          table={durations.length ? { head: ["Bucket", "p50", "p95", "Jobs"], rows: durations.map((d) => [bucketLabel(d.t, a.bucket_ms), duration(d.p50_ms), duration(d.p95_ms), d.count]) } : undefined}
        >
          {durations.length ? (
            <TimeLines
              ariaLabel="Job duration p50 and p95 over time"
              times={durTimes}
              height={168}
              log
              logTicks={DURATION_TICKS}
              markers={durations.length <= 48}
              format={duration}
              axisFormat={durationTick}
              from={throughput[0]?.t}
              to={throughput[throughput.length - 1]?.t}
              xFormat={xFormat}
              tipTitle={(t) => bucketLabel(t, a.bucket_ms)}
              hoverT={hoverT}
              onHover={setHoverT}
              series={[
                { id: "p95", label: "p95", color: "var(--s1-light)", values: durations.map((d) => d.p95_ms) },
                { id: "p50", label: "p50", color: "var(--s1)", values: durations.map((d) => d.p50_ms), width: 2 },
              ]}
            />
          ) : (
            <ChartEmpty height={168}>Durations appear once a job has finished in this range.</ChartEmpty>
          )}
        </ChartFrame>

        <ChartFrame className={cn(cell, "border-b border-line lg:col-span-4 lg:border-r")} title="Jobs by node" hint="where work ran">
          {a.by_node?.length ? (
            <Bars
              rows={a.by_node.map((n) => ({
                key: n.node,
                label: n.node,
                display: n.jobs.toLocaleString(),
                note: n.failed ? `${n.failed} failed` : undefined,
                segments: [
                  { label: "finished or running", value: n.jobs - n.failed, color: "var(--s1)" },
                  { label: "failed", value: n.failed, color: "var(--critical)" },
                ],
              }))}
            />
          ) : (
            <ChartEmpty height={96}>No job in this range recorded the node it ran on.</ChartEmpty>
          )}
        </ChartFrame>

        <ChartFrame className={cn(cell, "border-b border-line lg:col-span-4 lg:border-r")} title="Placement source" hint="how the scheduler found a node">
          {a.by_placement?.length ? (
            <Bars labelWidth="6rem" rows={a.by_placement.map((p) => ({ key: p.source, label: p.source, display: p.count.toLocaleString(), note: total ? percent((p.count / total) * 100) : undefined, segments: [{ label: p.source, value: p.count, color: "var(--s1)" }] }))} />
          ) : (
            <ChartEmpty height={96}>No placement decisions recorded in this range.</ChartEmpty>
          )}
        </ChartFrame>

        <ChartFrame className={cn(cell, "border-b border-line lg:col-span-4")} title="Time by stage" hint="summed across jobs">
          {a.stage_time?.length ? (
            <Bars labelWidth="7.5rem" rows={a.stage_time.map((s) => ({ key: s.stage, label: s.stage, display: duration(s.total_ms), note: stageTotal ? share(s.total_ms / stageTotal) : undefined, segments: [{ label: s.stage, value: s.total_ms, color: "var(--s1)" }] }))} />
          ) : (
            <ChartEmpty height={96}>Stage timing is not recorded for the jobs in this range.</ChartEmpty>
          )}
        </ChartFrame>

        <ChartFrame className={cn(cell, "lg:col-span-12")} title="Memory estimate against real peak" hint="One dot per job, grouped by how the scheduler estimated it. The bar marks the tier median; the shaded band is within 25% of the real peak." table={tiers.length ? estimateTable(tiers) : undefined}>
          {tiers.length ? (
            <EstimateStrip tiers={tiers} onOpen={(id) => navigate(jobHref(id))} />
          ) : (
            <ChartEmpty height={96}>No job in this range has both a memory estimate and a measured peak. Estimates are recorded when the scheduler places a job; the peak is measured when it finishes.</ChartEmpty>
          )}
        </ChartFrame>
      </div>
    </div>
  );
}

export function OverviewPage() {
  const overview = useOverview();
  const [windowKey, setWindowKey] = useState<MetricsWindow>("15m");
  const [range, setRange] = useState<AnalyticsRange>("24h");
  const nodes = usePoll("nodes", api.nodes, 5000);
  const [intervalMs, setIntervalMs] = useState(5000);
  const metrics = usePoll(`metrics:${windowKey}`, async (s) => {
    const m = await api.metrics(windowKey, s);
    if (m.interval_ms >= 1000) setIntervalMs(m.interval_ms);
    return m;
  }, intervalMs);
  const tasks = usePoll("tasks", api.tasks, 3000);
  const analytics = usePoll(`analytics:${range}`, (s) => api.analytics(range, s), 20_000);
  const rangePicked = useRef(false);
  const pickRange = (r: AnalyticsRange) => {
    rangePicked.current = true;
    setRange(r);
  };
  const historyTotal = overview.data?.jobs.total ?? 0;
  useEffect(() => {
    if (rangePicked.current || !analytics.data || analytics.data.range !== range || historyTotal === 0) return;
    const wider = RANGES[RANGES.indexOf(range) + 1];
    const empty = analytics.data.throughput.every((b) => b.succeeded + b.failed + b.other === 0);
    if (empty && wider) setRange(wider);
  }, [analytics.data, range, historyTotal]);
  const now = useNow(1000);
  const down = overview.data ? !overview.data.daemon.reachable : false;
  const tasksView = whenDaemonDown(tasks, down, []);
  const metricsView = whenDaemonDown(metrics, down, { interval_ms: intervalMs, series: [] });

  return (
    <Page>
      <PageHeader
        title="Overview"
        meta={
          <span className="flex flex-wrap items-center gap-x-3">
            {overview.updatedAt ? <span className="num">Updated {timeSec(overview.updatedAt)}</span> : "Connecting to the console server"}
            <StaleMark polled={overview} now={now} />
          </span>
        }
      />
      <DaemonBanner />
      <Async polled={overview} skeleton={<div className="skeleton h-[5.75rem] rounded-lg" />}>
        {(o) => <Summary o={o} nodes={nodes.data} />}
      </Async>

      <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_22rem]">
        <Panel
          title="Cluster load"
          hint={metrics.data ? `sampled every ${duration(metrics.data.interval_ms)}` : undefined}
          stale={<StaleMark polled={metricsView} now={now} />}
          actions={<Segmented label="Load window" value={windowKey} options={WINDOWS} onChange={setWindowKey} />}
        >
          <Async polled={metricsView} skeleton={<SkeletonRows rows={7} />}>
            {(m) =>
              down && !m.series.length ? (
                <EmptyState title="No live load while the daemon is stopped">Start the daemon and per-node CPU, memory and GPU history fills in here.</EmptyState>
              ) : (
                <LoadCharts m={m} windowKey={windowKey} now={now} />
              )
            }
          </Async>
        </Panel>

        <Panel title="Live tasks" hint={tasks.data?.length ? plural(tasks.data.length, "lease") : undefined} stale={<StaleMark polled={tasksView} now={now} />}>
          <Async
            polled={tasksView}
            isEmpty={(t) => !t.length}
            empty={
              <EmptyState title={down ? "No live tasks while the daemon is stopped" : "Nothing is running"}>
                {down ? "Leases are read from the daemon." : "Tasks appear here the moment a node reserves memory for a job, and leave when it finishes."}
              </EmptyState>
            }
          >
            {(t) => <Tasks tasks={t} />}
          </Async>
        </Panel>
      </div>

      <Panel
        className="mt-4"
        title="Job history"
        hint={analytics.updatedAt ? `as of ${ago(analytics.updatedAt, now)}` : undefined}
        stale={<StaleMark polled={analytics} now={now} />}
        actions={<Segmented label="History range" value={range} options={RANGES} onChange={pickRange} />}
      >
        <Async polled={analytics} skeleton={<SkeletonRows rows={9} />}>
          {(a) => <JobAnalytics a={a} />}
        </Async>
      </Panel>
    </Page>
  );
}
