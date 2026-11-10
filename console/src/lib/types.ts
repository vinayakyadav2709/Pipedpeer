export interface Overview {
  self: { node_id: string; hostname: string };
  daemon: { reachable: boolean; port: number; error: string };
  cluster: {
    nodes_total: number;
    nodes_healthy: number;
    cpus_total: number;
    mem_total_bytes: number;
    mem_available_bytes: number;
    gpus_total: number;
    gpu_mem_total_bytes: number;
    gpu_mem_used_bytes: number;
    active_jobs: number;
    queue_depth: number;
  };
  jobs: {
    total: number;
    running: number;
    succeeded: number;
    failed: number;
    success_rate: number;
    p50_duration_ms: number;
    p95_duration_ms: number;
  };
}

export interface GpuInfo {
  index: number;
  name: string;
  memory_total_bytes: number;
  memory_free_bytes: number;
  utilization_gpu_percent: number;
}

export interface NodeLoad {
  cpu_percent: number;
  memory_percent: number;
  active_jobs: number;
  queue_depth: number;
  recent_failures: number;
  total_mem_bytes?: number;
  available_mem_bytes?: number;
  reserved_mem_bytes?: number;
  total_cpus: number;
  gpu_model?: string;
  gpu_mem_bytes?: number;
  gpu_mem_used_bytes?: number;
  gpu_util_percent?: number;
  gpus?: GpuInfo[] | null;
}

export interface ClusterNode {
  node_id: string;
  hostname: string;
  ssh_endpoint: string;
  daemon_port: number;
  state: string;
  health_score: number;
  source: string;
  is_self: boolean;
  capabilities: Record<string, string> | null;
  load: NodeLoad;
}

export interface MetricPoint {
  t: number;
  cpu: number | null;
  mem: number | null;
  gpu_util: number | null;
  gpu_mem: number | null;
  active_jobs: number | null;
}

export interface MetricSeries {
  node_id: string;
  hostname: string;
  points: MetricPoint[];
}

export interface Metrics {
  interval_ms: number;
  series: MetricSeries[];
}

export type MetricsWindow = "5m" | "15m" | "1h" | "6h";

export interface LiveTask {
  node_id: string;
  hostname: string;
  lease_id: string;
  job_name: string;
  submitter: string;
  state: string;
  mem_bytes: number;
  gpu_index: number;
  age_ms: number;
}

export interface Job {
  id: string;
  status: string;
  error: string;
  started_at: number;
  finished_at: number;
  duration_ms: number;
  script_path: string;
  job_name: string;
  job_set: string;
  run_host: string;
  target_id: string;
  placement_source: string;
  placement_reason: string;
  candidate_count: number;
  degraded_mode: boolean;
  peak_mem_bytes: number;
  estimated_mem_bytes: number;
  estimation_tier: string;
  stage: string;
  detached: boolean;
  isolate: boolean;
}

export interface JobEvent {
  status: string;
  prev_status: string;
  timestamp: number;
  error: string;
}

export interface JobDetail extends Job {
  log_tail: string;
  events: JobEvent[];
}

export type AnalyticsRange = "1h" | "24h" | "7d" | "30d" | "all";

export interface Analytics {
  range: string;
  bucket_ms: number;
  throughput: { t: number; succeeded: number; failed: number; other: number }[];
  durations: { t: number; p50_ms: number; p95_ms: number; count: number }[];
  by_node: { node: string; jobs: number; failed: number; total_duration_ms: number }[];
  by_status: { status: string; count: number }[];
  by_placement: { source: string; count: number }[];
  mem_estimates: {
    id: string;
    job_name: string;
    tier: string;
    estimated_bytes: number;
    peak_bytes: number;
  }[];
  stage_time: { stage: string; total_ms: number }[];
}

export interface NotebookFile {
  path: string;
  name: string;
  modified: number;
  size: number;
}

export type MimeBundle = Record<string, unknown>;

export type KernelState = "starting" | "idle" | "busy" | "dead";
export type RunTarget = "local" | "cluster";
export type DoneStatus = "ok" | "error" | "aborted";

export interface FanoutNode {
  hostname: string;
  chunks: number;
}

export interface KernelVar {
  name: string;
  type: string;
  repr: string;
  size_bytes: number;
}

export type ClientFrame =
  | { type: "execute"; cell_id: string; code: string; target: RunTarget }
  | { type: "interrupt" }
  | { type: "restart" }
  | { type: "shutdown" }
  | { type: "complete"; req_id: string; code: string; cursor: number }
  | { type: "inspect_vars"; req_id: string };

export type ServerFrame =
  | {
      type: "status";
      state: KernelState;
      python: string;
      interception: boolean;
      execution_count: number;
      queued: string[] | null;
      running: string | null;
    }
  | { type: "stream"; cell_id: string; name: "stdout" | "stderr"; text: string }
  | { type: "display"; cell_id: string; data: MimeBundle; display_id?: string }
  | { type: "result"; cell_id: string; execution_count: number; data: MimeBundle }
  | { type: "error"; cell_id: string; ename: string; evalue: string; traceback: string[] }
  | {
      type: "done";
      cell_id: string;
      execution_count: number;
      status: DoneStatus;
      duration_ms: number;
    }
  | {
      type: "cluster";
      cell_id: string;
      event: string;
      nodes: FanoutNode[] | null;
      local_chunks: number;
    }
  | {
      type: "complete_reply";
      req_id: string;
      matches: string[] | null;
      cursor_start: number;
      cursor_end: number;
    }
  | { type: "vars_reply"; req_id: string; vars: KernelVar[] | null }
  | { type: "error_fatal"; message: string };
