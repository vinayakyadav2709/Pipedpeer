import type {
  Analytics,
  AnalyticsRange,
  ClusterNode,
  Job,
  JobDetail,
  JobEvent,
  LiveTask,
  MetricPoint,
  Metrics,
  MetricsWindow,
  Overview,
} from "../src/lib/types.ts";

const GB = 1024 ** 3;
const MB = 1024 ** 2;
const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;

function rng(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function hash(s: string) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619);
  return h >>> 0;
}

interface NodeSpec {
  id: string;
  host: string;
  cpus: number;
  memGB: number;
  gpus: { name: string; memGB: number }[];
  state: "healthy" | "stale" | "unavailable";
  self?: boolean;
  source: string;
  base: number;
}

const NODES: NodeSpec[] = [
  { id: "n-7f3a9c21", host: "workstation-01", cpus: 16, memGB: 64, gpus: [{ name: "NVIDIA GeForce RTX 4090", memGB: 24 }], state: "healthy", self: true, source: "self", base: 22 },
  { id: "n-b81d04e7", host: "lab-gpu-02", cpus: 32, memGB: 128, gpus: [{ name: "NVIDIA GeForce RTX 3090", memGB: 24 }, { name: "NVIDIA GeForce RTX 3090", memGB: 24 }], state: "healthy", source: "discovery", base: 48 },
  { id: "n-2c9e6f10", host: "thinkpad-x1", cpus: 8, memGB: 16, gpus: [], state: "healthy", source: "discovery", base: 12 },
  { id: "n-e4a17b93", host: "nuc-attic", cpus: 12, memGB: 32, gpus: [{ name: "NVIDIA GeForce GTX 1660 Ti", memGB: 6 }], state: "stale", source: "cache", base: 30 },
  { id: "n-51f0d8aa", host: "render-box", cpus: 24, memGB: 96, gpus: [{ name: "NVIDIA RTX A5000", memGB: 24 }], state: "unavailable", source: "cache", base: 0 },
];

const STALE_SINCE_MS = 4 * MIN;

function wave(spec: NodeSpec, t: number, salt: number, amp: number) {
  const s = t / 1000;
  const k = (hash(spec.id) % 97) + salt * 13;
  const slow = Math.sin(s / 310 + k) * 0.5 + Math.sin(s / 71 + k * 1.7) * 0.3;
  const fast = Math.sin(s / 11 + k * 0.3) * 0.12 + (rng(Math.floor(s / 5) * 31 + k)() - 0.5) * 0.16;
  return amp * (slow + fast);
}

function sample(spec: NodeSpec, t: number, now: number): MetricPoint {
  const none = { t, cpu: null, mem: null, gpu_util: null, gpu_mem: null, active_jobs: null };
  if (spec.state === "unavailable") return none;
  if (spec.state === "stale" && now - t < STALE_SINCE_MS) return none;
  const burst = Math.sin(t / 1000 / 190 + hash(spec.id)) > 0.55 ? 28 : 0;
  const cpu = clamp(spec.base + burst + wave(spec, t, 1, 26), 1, 100);
  const mem = clamp(38 + spec.base * 0.5 + wave(spec, t, 2, 9), 5, 97);
  const hasGpu = spec.gpus.length > 0;
  const gpuUtil = hasGpu ? clamp(spec.base * 1.3 + burst * 1.4 + wave(spec, t, 3, 34), 0, 100) : null;
  const gpuMem = hasGpu ? clamp(30 + spec.base * 0.7 + wave(spec, t, 4, 12), 2, 98) : null;
  return {
    t,
    cpu: round1(cpu),
    mem: round1(mem),
    gpu_util: gpuUtil === null ? null : round1(gpuUtil),
    gpu_mem: gpuMem === null ? null : round1(gpuMem),
    active_jobs: cpu > 55 ? 2 : cpu > 30 ? 1 : 0,
  };
}

const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));
const round1 = (v: number) => Math.round(v * 10) / 10;

const WINDOW_MS: Record<MetricsWindow, number> = { "5m": 5 * MIN, "15m": 15 * MIN, "1h": HOUR, "6h": 6 * HOUR };
const INTERVAL_MS = 5000;

export function metrics(window: MetricsWindow, startedAt: number): Metrics {
  const now = Math.floor(Date.now() / INTERVAL_MS) * INTERVAL_MS;
  const from = Math.max(now - WINDOW_MS[window], Math.floor(startedAt / INTERVAL_MS) * INTERVAL_MS);
  return {
    interval_ms: INTERVAL_MS,
    series: NODES.map((spec) => {
      const points: MetricPoint[] = [];
      for (let t = from; t <= now; t += INTERVAL_MS) points.push(sample(spec, t, now));
      return { node_id: spec.id, hostname: spec.host, points };
    }),
  };
}

export function nodes(): ClusterNode[] {
  const now = Date.now();
  return NODES.map((spec) => {
    const live = sample(spec, now - (spec.state === "stale" ? STALE_SINCE_MS + 5000 : 0), now);
    const totalMem = spec.memGB * GB;
    const memPct = live.mem ?? 0;
    const gpuTotal = spec.gpus.reduce((a, g) => a + g.memGB * GB, 0);
    const gpuUsedPct = live.gpu_mem ?? 0;
    const down = spec.state === "unavailable";
    return {
      node_id: spec.id,
      hostname: spec.host,
      ssh_endpoint: `pp@${spec.host}.local:22`,
      daemon_port: 7777,
      state: spec.state,
      health_score: spec.state === "healthy" ? round2(0.99 - (live.cpu ?? 0) / 900) : spec.state === "stale" ? 0.41 : 0,
      source: spec.source,
      is_self: !!spec.self,
      capabilities: {
        cpu_cores: String(spec.cpus),
        arch: "x86_64",
        os: "linux",
        ...(spec.gpus[0] ? { gpu: spec.gpus[0].name, cuda: "12.4" } : {}),
        container: "crun",
      },
      load: {
        cpu_percent: down ? 0 : (live.cpu ?? 0),
        memory_percent: down ? 0 : memPct,
        active_jobs: down ? 0 : (live.active_jobs ?? 0),
        queue_depth: spec.host === "lab-gpu-02" ? 1 : 0,
        recent_failures: spec.state === "stale" ? 3 : spec.host === "thinkpad-x1" ? 1 : 0,
        total_mem_bytes: totalMem,
        available_mem_bytes: down ? 0 : Math.round(totalMem * (1 - memPct / 100)),
        reserved_mem_bytes: down ? 0 : Math.round(totalMem * 0.08 * (live.active_jobs ?? 0)),
        total_cpus: spec.cpus,
        gpu_model: spec.gpus[0]?.name ?? "",
        gpu_mem_bytes: gpuTotal,
        gpu_mem_used_bytes: down ? 0 : Math.round((gpuTotal * gpuUsedPct) / 100),
        gpu_util_percent: down ? 0 : (live.gpu_util ?? 0),
        gpus: spec.gpus.map((g, i) => {
          const skew = i === 0 ? 1 : 0.55;
          const used = down ? 0 : (g.memGB * GB * gpuUsedPct * skew) / 100;
          return {
            index: i,
            name: g.name,
            memory_total_bytes: g.memGB * GB,
            memory_free_bytes: Math.round(g.memGB * GB - used),
            utilization_gpu_percent: down ? 0 : round1((live.gpu_util ?? 0) * skew),
          };
        }),
      },
    };
  });
}

const round2 = (v: number) => Math.round(v * 100) / 100;

const SCRIPTS = [
  ["train.py", "resnet50-finetune"],
  ["etl/ingest.py", "nightly-ingest"],
  ["sweep.py", "lr-sweep"],
  ["eval.py", "eval-holdout"],
  ["features/build.py", "feature-build"],
  ["render_frames.py", "render-frames"],
  ["tokenize_corpus.py", "tokenize-corpus"],
  ["backtest.py", "backtest-2019"],
] as const;

const TIERS = ["user", "historical", "filesize", "import"] as const;
const PLACEMENTS = ["registry", "registry", "registry", "cache", "discovery", "self", "explicit", "coordinator"] as const;
const FAILS = [
  "exit status 137: killed (out of memory)",
  "exit status 1",
  "ssh: connect to host nuc-attic.local port 22: Connection timed out",
  "lease expired before the job started",
  "exit status 1: ModuleNotFoundError: No module named 'sklearn'",
];

let JOBS: Job[] | null = null;

function buildJobs(): Job[] {
  const r = rng(424242);
  const now = Date.now();
  const out: Job[] = [];
  let t = now - 40_000;
  for (let i = 0; i < 260; i++) {
    const [script, name] = SCRIPTS[Math.floor(r() * SCRIPTS.length)]!;
    const hosts = NODES.filter((n) => n.state !== "unavailable" || r() < 0.2);
    const host = hosts[Math.floor(r() ** 1.6 * hosts.length)]!;
    const roll = r();
    let status = roll < 0.085 ? "failed" : roll < 0.105 ? "cancelled" : roll < 0.115 ? "expired" : "succeeded";
    if (i === 0) status = "running";
    if (i === 1) status = "queued";
    if (i === 2) status = "running";
    const long = r() < 0.18;
    const duration = Math.round(long ? 60_000 + r() * 900_000 : 1500 + r() ** 2 * 45_000);
    const tier = TIERS[Math.floor(r() * TIERS.length)]!;
    const peak = Math.round((0.3 + r() ** 2 * 11) * GB);
    const bias =
      tier === "user" ? 1.6 + r() * 2.8
      : tier === "historical" ? 0.86 + r() * 0.32
      : tier === "filesize" ? 0.38 + r() * 0.75
      : 1.1 + r() ** 2 * 4.5;
    const terminal = status === "succeeded" || status === "failed" || status === "cancelled" || status === "expired";
    const hasMem = status !== "queued" && status !== "expired" && r() > 0.06;
    const started = status === "queued" ? 0 : t - (terminal ? duration : Math.round(r() * 50_000 + 8000));
    const failure = status === "failed" ? FAILS[Math.floor(r() * FAILS.length)]! : status === "expired" ? FAILS[3]! : "";
    const placement = PLACEMENTS[Math.floor(r() * PLACEMENTS.length)]!;
    out.push({
      id: `job-${(hash(`${i}-${name}`) >>> 0).toString(16).padStart(8, "0")}`,
      status,
      error: failure,
      started_at: started,
      finished_at: terminal ? t : 0,
      duration_ms: terminal ? duration : 0,
      script_path: script,
      job_name: `${name}-${String(260 - i).padStart(3, "0")}`,
      job_set: r() < 0.3 ? `${name}-set` : "",
      run_host: status === "queued" ? "" : host.host,
      target_id: status === "queued" ? "" : host.id,
      placement_source: status === "queued" ? "" : placement,
      placement_reason:
        status === "queued" ? ""
        : placement === "explicit" ? `--target ${host.host}`
        : `score 0.${Math.floor(60 + r() * 39)}: ${host.cpus} cpus, ${Math.round(host.memGB * (0.3 + r() * 0.5))} GB free${host.gpus.length ? ", gpu idle" : ""}; beat ${Math.max(0, hosts.length - 1)} other candidates`,
      candidate_count: status === "queued" ? 0 : hosts.length,
      degraded_mode: r() < 0.05,
      peak_mem_bytes: hasMem && status !== "running" ? peak : 0,
      estimated_mem_bytes: hasMem ? Math.round(peak * bias) : 0,
      estimation_tier: hasMem ? tier : "",
      stage: terminal ? "executing" : status === "queued" ? "" : i === 0 ? "executing" : "uploading",
      detached: r() < 0.1,
      isolate: r() < 0.35,
    });
    const gap = r() < 0.5 ? r() * 12 * MIN : r() < 0.85 ? r() * 2.5 * HOUR : r() * 9 * HOUR;
    t -= Math.round(gap) + 20_000;
  }
  return out;
}

export function jobs(): Job[] {
  JOBS ??= buildJobs();
  return JOBS;
}

export function jobList(limit: number, status: string, q: string): Job[] {
  const needle = q.trim().toLowerCase();
  return jobs()
    .filter((j) => !status || j.status === status)
    .filter(
      (j) =>
        !needle ||
        [j.id, j.job_name, j.script_path, j.run_host, j.job_set, j.error].some((f) => f.toLowerCase().includes(needle)),
    )
    .slice(0, limit);
}

const E = "\u001b[";

function logFor(job: Job): string {
  const lines: string[] = [];
  lines.push(`${E}2m[pipedpeer]${E}0m placed on ${E}1m${job.run_host}${E}0m via ${job.placement_source}`);
  lines.push(`${E}2m[pipedpeer]${E}0m syncing workspace: 14 files, 2.3 MB`);
  lines.push(`${E}2m[pipedpeer]${E}0m environment ready ${E}32m(cached)${E}0m`);
  lines.push(`$ python ${job.script_path}`);
  for (let i = 1; i <= 40; i++) {
    const loss = (2.4 / (1 + i * 0.21)).toFixed(4);
    lines.push(
      `epoch ${String(i).padStart(2, " ")}/40  ${E}36mloss${E}0m ${loss}  ${E}36macc${E}0m ${(0.42 + 0.55 * (1 - Math.exp(-i / 9))).toFixed(4)}  ${E}2m${(1.8 + (i % 5) * 0.07).toFixed(2)}s/it${E}0m`,
    );
    if (i === 12) lines.push(`${E}33mUserWarning: The given NumPy array is not writable, and PyTorch does not support non-writable tensors. This means writing to this tensor will result in undefined behavior.${E}0m`);
  }
  if (job.status === "failed") {
    lines.push(`${E}31mTraceback (most recent call last):${E}0m`);
    lines.push(`  File "${E}32m/workspace/${job.script_path}${E}0m", line ${E}32m118${E}0m, in ${E}35m<module>${E}0m`);
    lines.push(`    main()`);
    lines.push(`  File "${E}32m/workspace/${job.script_path}${E}0m", line ${E}32m94${E}0m, in ${E}35mmain${E}0m`);
    lines.push(`    batch = torch.cat(buffers, dim=0)`);
    lines.push(`${E}1;31mRuntimeError${E}0m: ${job.error}`);
  } else if (job.status === "succeeded") {
    lines.push(`${E}32msaved checkpoint to outputs/model-final.pt${E}0m`);
    lines.push(`${E}2m[pipedpeer]${E}0m synced 3 output files back`);
  }
  return lines.join("\n") + "\n";
}

export function jobDetail(id: string): JobDetail | null {
  const job = jobs().find((j) => j.id === id);
  if (!job) return null;
  const events: JobEvent[] = [];
  if (job.started_at) {
    const q = job.started_at - 900;
    events.push({ status: "queued", prev_status: "", timestamp: q, error: "" });
    events.push({ status: "reserved", prev_status: "queued", timestamp: q + 350, error: "" });
    if (job.status !== "expired") {
      events.push({ status: "running", prev_status: "reserved", timestamp: job.started_at, error: "" });
    }
    if (job.finished_at) {
      events.push({
        status: job.status,
        prev_status: job.status === "expired" ? "reserved" : "running",
        timestamp: job.finished_at,
        error: job.error,
      });
    }
  } else {
    events.push({ status: "queued", prev_status: "", timestamp: Date.now() - 12_000, error: "" });
  }
  return { ...job, log_tail: job.status === "queued" || job.status === "expired" ? "" : logFor(job), events };
}

const RANGE_MS: Record<AnalyticsRange, number> = { "1h": HOUR, "24h": DAY, "7d": 7 * DAY, "30d": 30 * DAY, all: 0 };
const BUCKET_MS: Record<AnalyticsRange, number> = { "1h": 5 * MIN, "24h": HOUR, "7d": 6 * HOUR, "30d": DAY, all: DAY };

function pct(sorted: number[], p: number) {
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))]!;
}

export function analytics(range: AnalyticsRange): Analytics {
  const now = Date.now();
  const all = jobs();
  const oldest = all.reduce((m, j) => (j.started_at ? Math.min(m, j.started_at) : m), now);
  const from = range === "all" ? oldest : now - RANGE_MS[range];
  const bucket = BUCKET_MS[range];
  const inRange = all.filter((j) => j.started_at >= from);
  const first = Math.floor(from / bucket) * bucket;
  const throughput: Analytics["throughput"] = [];
  const durations: Analytics["durations"] = [];
  for (let t = first; t <= now; t += bucket) {
    const js = inRange.filter((j) => j.started_at >= t && j.started_at < t + bucket);
    throughput.push({
      t,
      succeeded: js.filter((j) => j.status === "succeeded").length,
      failed: js.filter((j) => j.status === "failed").length,
      other: js.filter((j) => j.status !== "succeeded" && j.status !== "failed").length,
    });
    const ds = js.filter((j) => j.duration_ms > 0).map((j) => j.duration_ms).sort((a, b) => a - b);
    if (ds.length) durations.push({ t, p50_ms: pct(ds, 0.5), p95_ms: pct(ds, 0.95), count: ds.length });
  }
  const group = <K extends string>(key: (j: Job) => K) => {
    const m = new Map<K, Job[]>();
    for (const j of inRange) {
      const k = key(j);
      if (!k) continue;
      m.set(k, [...(m.get(k) ?? []), j]);
    }
    return m;
  };
  const execMs = inRange.reduce((a, j) => a + j.duration_ms, 0);
  return {
    range,
    bucket_ms: bucket,
    throughput,
    durations,
    by_node: [...group((j) => j.run_host)]
      .map(([node, js]) => ({
        node,
        jobs: js.length,
        failed: js.filter((j) => j.status === "failed").length,
        total_duration_ms: js.reduce((a, j) => a + j.duration_ms, 0),
      }))
      .sort((a, b) => b.jobs - a.jobs),
    by_status: [...group((j) => j.status)].map(([status, js]) => ({ status, count: js.length })).sort((a, b) => b.count - a.count),
    by_placement: [...group((j) => j.placement_source)].map(([source, js]) => ({ source, count: js.length })).sort((a, b) => b.count - a.count),
    mem_estimates: inRange
      .filter((j) => j.estimated_mem_bytes > 0 && j.peak_mem_bytes > 0)
      .map((j) => ({ id: j.id, job_name: j.job_name, tier: j.estimation_tier, estimated_bytes: j.estimated_mem_bytes, peak_bytes: j.peak_mem_bytes })),
    stage_time: execMs
      ? [
          { stage: "building", total_ms: Math.round(execMs * 0.061) },
          { stage: "exporting", total_ms: Math.round(execMs * 0.018) },
          { stage: "uploading", total_ms: Math.round(execMs * 0.034) },
          { stage: "executing", total_ms: execMs },
        ]
      : [],
  };
}

export function tasks(): LiveTask[] {
  const now = Date.now();
  const phase = Math.floor(now / 20_000) % 3;
  const base: LiveTask[] = [
    { node_id: "n-b81d04e7", hostname: "lab-gpu-02", lease_id: "ls-91c2e0", job_name: "resnet50-finetune-260", submitter: "workstation-01", state: "running", mem_bytes: 9.4 * GB, gpu_index: 0, age_ms: 412_000 + (now % 60_000) },
    { node_id: "n-b81d04e7", hostname: "lab-gpu-02", lease_id: "ls-04ab77", job_name: "lr-sweep-258", submitter: "workstation-01", state: "running", mem_bytes: 3.1 * GB, gpu_index: 1, age_ms: 38_000 + (now % 60_000) },
    { node_id: "n-7f3a9c21", hostname: "workstation-01", lease_id: "ls-5d3310", job_name: "notebook: cluster-tour.ipynb", submitter: "workstation-01", state: "running", mem_bytes: 820 * MB, gpu_index: -1, age_ms: 9_000 + (now % 20_000) },
    { node_id: "n-2c9e6f10", hostname: "thinkpad-x1", lease_id: "ls-77fe19", job_name: "feature-build-259", submitter: "thinkpad-x1", state: "reserved", mem_bytes: 1.5 * GB, gpu_index: -1, age_ms: 1_400 },
  ];
  return base.slice(0, phase === 0 ? 4 : phase === 1 ? 3 : 2).map((t) => ({ ...t, mem_bytes: Math.round(t.mem_bytes) }));
}

export function overview(daemonUp: boolean): Overview {
  const ns = nodes();
  const js = jobs();
  const healthy = ns.filter((n) => n.state === "healthy");
  const done = js.filter((j) => j.status === "succeeded" || j.status === "failed");
  const ds = done.map((j) => j.duration_ms).sort((a, b) => a - b);
  const succeeded = js.filter((j) => j.status === "succeeded").length;
  const failed = js.filter((j) => j.status === "failed").length;
  const cluster = daemonUp
    ? {
        nodes_total: ns.length,
        nodes_healthy: healthy.length,
        cpus_total: healthy.reduce((a, n) => a + n.load.total_cpus, 0),
        mem_total_bytes: healthy.reduce((a, n) => a + (n.load.total_mem_bytes ?? 0), 0),
        mem_available_bytes: healthy.reduce((a, n) => a + (n.load.available_mem_bytes ?? 0), 0),
        gpus_total: healthy.reduce((a, n) => a + (n.load.gpus?.length ?? 0), 0),
        gpu_mem_total_bytes: healthy.reduce((a, n) => a + (n.load.gpu_mem_bytes ?? 0), 0),
        gpu_mem_used_bytes: healthy.reduce((a, n) => a + (n.load.gpu_mem_used_bytes ?? 0), 0),
        active_jobs: tasks().filter((t) => t.state === "running").length,
        queue_depth: 1,
      }
    : { nodes_total: 0, nodes_healthy: 0, cpus_total: 0, mem_total_bytes: 0, mem_available_bytes: 0, gpus_total: 0, gpu_mem_total_bytes: 0, gpu_mem_used_bytes: 0, active_jobs: 0, queue_depth: 0 };
  return {
    self: { node_id: "n-7f3a9c21", hostname: "workstation-01" },
    daemon: daemonUp
      ? { reachable: true, port: 7777, error: "" }
      : { reachable: false, port: 7777, error: "dial tcp 127.0.0.1:7777: connect: connection refused" },
    cluster,
    jobs: {
      total: js.length,
      running: js.filter((j) => j.status === "running").length,
      succeeded,
      failed,
      success_rate: succeeded / Math.max(1, succeeded + failed),
      p50_duration_ms: pct(ds, 0.5),
      p95_duration_ms: pct(ds, 0.95),
    },
  };
}
