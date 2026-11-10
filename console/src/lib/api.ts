import type {
  Analytics,
  AnalyticsRange,
  ClusterNode,
  Job,
  JobDetail,
  LiveTask,
  Metrics,
  MetricsWindow,
  NotebookFile,
  Overview,
} from "./types";

export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

const authListeners = new Set<() => void>();
export function onUnauthorized(fn: () => void) {
  authListeners.add(fn);
  return () => void authListeners.delete(fn);
}

async function request(path: string, init?: RequestInit): Promise<Response> {
  let res: Response;
  try {
    res = await fetch(path, { credentials: "same-origin", ...init });
  } catch {
    throw new ApiError(0, "The console server is not responding");
  }
  if (res.ok) return res;
  let message = `${res.status} ${res.statusText}`;
  try {
    const body = (await res.json()) as { error?: string };
    if (body.error) message = body.error;
  } catch {
    /* non-JSON error body: keep the status line */
  }
  if (res.status === 401) authListeners.forEach((fn) => fn());
  throw new ApiError(res.status, message);
}

async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  return (await request(path, { signal })).json() as Promise<T>;
}

const q = (params: Record<string, string | number | undefined>) => {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== "") sp.set(k, String(v));
  const s = sp.toString();
  return s ? `?${s}` : "";
};

export interface NotebookContent {
  nb: unknown;
  etag: string | null;
}

export const api = {
  overview: (s?: AbortSignal) => get<Overview>("/api/overview", s),
  nodes: (s?: AbortSignal) => get<ClusterNode[] | null>("/api/nodes", s).then((n) => n ?? []),
  metrics: (window: MetricsWindow, s?: AbortSignal) => get<Metrics>(`/api/metrics${q({ window })}`, s),
  tasks: (s?: AbortSignal) => get<LiveTask[] | null>("/api/tasks", s).then((t) => t ?? []),
  jobs: (p: { limit?: number; status?: string; q?: string }, s?: AbortSignal) =>
    get<Job[] | null>(`/api/jobs${q({ limit: p.limit ?? 200, status: p.status, q: p.q })}`, s).then((j) => j ?? []),
  job: (id: string, s?: AbortSignal) => get<JobDetail>(`/api/jobs/${encodeURIComponent(id)}`, s),
  analytics: (range: AnalyticsRange, s?: AbortSignal) => get<Analytics>(`/api/analytics${q({ range })}`, s),

  notebooks: (s?: AbortSignal) => get<NotebookFile[] | null>("/api/notebooks", s).then((n) => n ?? []),
  createNotebook: async (path?: string) => {
    const res = await request("/api/notebooks", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(path ? { path } : {}),
    });
    return (await res.json()) as { path: string };
  },
  renameNotebook: async (from: string, to: string) => {
    const res = await request("/api/notebooks/rename", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ from, to }),
    });
    return (await res.json()) as { path: string };
  },
  deleteNotebook: async (path: string) => {
    await request(`/api/notebooks/content${q({ path })}`, { method: "DELETE" });
  },
  readNotebook: async (path: string, s?: AbortSignal): Promise<NotebookContent> => {
    const res = await request(`/api/notebooks/content${q({ path })}`, { signal: s });
    return { nb: await res.json(), etag: res.headers.get("ETag") };
  },
  writeNotebook: async (path: string, nb: unknown, etag: string | null) => {
    const res = await request(`/api/notebooks/content${q({ path })}`, {
      method: "PUT",
      headers: { "content-type": "application/json", ...(etag ? { "If-Match": etag } : {}) },
      body: JSON.stringify(nb),
    });
    return (await res.json()) as { etag: string; modified: number };
  },
};

export function kernelSocketUrl(notebook: string) {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/api/kernel/ws${q({ notebook })}`;
}
