import path from "node:path";
import type { ServerWebSocket } from "bun";
import type { AnalyticsRange, ClientFrame, MetricsWindow, ServerFrame } from "../src/lib/types.ts";
import { analytics, jobDetail, jobList, metrics, nodes, overview, tasks } from "./data.ts";
import { MockKernel } from "./kernel.ts";

const PORT = Number(process.env.PORT ?? 7878);
let DAEMON_UP = process.env.MOCK_DAEMON_DOWN !== "1";
let FAIL = process.env.MOCK_FAIL ?? "";
const INTERCEPTION = process.env.MOCK_INTERCEPTION !== "0";
const DIST = path.resolve(import.meta.dir, "../../src/internal/console/web/dist");
const STARTED = Date.now() - 6 * 3600_000;

interface Stored {
  content: string;
  modified: number;
  etag: string;
}

const files = new Map<string, Stored>();
let etagSeq = 1;

function put(p: string, content: unknown, modified = Date.now()) {
  const stored = { content: JSON.stringify(content, null, 1), modified, etag: `"m${etagSeq++}"` };
  files.set(p, stored);
  return stored;
}

const src = (s: string) => s.split("\n").map((l, i, a) => (i < a.length - 1 ? l + "\n" : l));
const code = (id: string, source: string) => ({ cell_type: "code", id, metadata: {}, execution_count: null, outputs: [], source: src(source) });
const md = (id: string, source: string) => ({ cell_type: "markdown", id, metadata: {}, source: src(source) });
const notebook = (cells: unknown[]) => ({
  cells,
  metadata: { kernelspec: { display_name: "Python 3", language: "python", name: "python3" }, language_info: { name: "python" } },
  nbformat: 4,
  nbformat_minor: 5,
});

put(
  "demo/cluster-tour.ipynb",
  notebook([
    md("a1f0c3d2", "# Cluster tour\n\nOrdinary Python, no SDK. Parallel work inside a cell **fans out** to the machines on your LAN; everything else runs in the local kernel.\n\n- `multiprocessing`, `joblib` and friends are intercepted\n- a cell can also run as a standalone *cluster job*\n\n| tier | meaning |\n|---|---|\n| historical | learned from earlier runs |\n| filesize | guessed from input size |"),
    code("b27e9a10", "import time\nimport numpy as np\nimport pandas as pd\n\nrows = 1000000\nlearning_rate = 0.003\nname = \"cluster-tour\""),
    code("c3d81f44", "from multiprocessing import Pool\n\ndef score(chunk):\n    return chunk.sum()\n\nwith Pool() as pool:\n    parts = pool.map(score, np.array_split(np.arange(rows), 16))\nprint(\"done\")"),
    code("d49a02b7", "df = pd.DataFrame(parts)\ndf.head()"),
    code("e5b6c1f8", "import matplotlib.pyplot as plt\nplt.plot(history)\nplt.show()"),
    code("f6c7d2a9", "config = {\"cluster\": cluster, \"scheduler\": scheduler}\nconfig"),
    code("a7d8e3b0", "import warnings\nwarnings.warn(\"masked element\")\nfor step in range(6):\n    print(step)\n    time.sleep(0.4)"),
    code("b8e9f4c1", "frames = load(paths)\nframes[\"missing\"]  # KeyError"),
  ]),
  Date.now() - 6 * 60_000,
);
put("scratch.ipynb", notebook([code("11aa22bb", "1 + 1")]), Date.now() - 3 * 3600_000);
put("experiments/lr-sweep.ipynb", notebook([md("33cc44dd", "## Learning rate sweep"), code("55ee66ff", "for lr in range(5):\n    print(lr)")]), Date.now() - 26 * 3600_000);
put("experiments/archive/2025-benchmarks.ipynb", notebook([code("77aa88bb", "print('hello')")]), Date.now() - 19 * 24 * 3600_000);

const kernels = new Map<string, MockKernel>();
const sockets = new Set<ServerWebSocket<Sock>>();

interface Sock {
  notebook: string;
  send?: (f: ServerFrame) => void;
}

const json = (body: unknown, init: ResponseInit = {}) =>
  new Response(JSON.stringify(body), { ...init, headers: { "content-type": "application/json", "cache-control": "no-store", ...init.headers } });
const fail = (status: number, error: string) => json({ error }, { status });

function validPath(p: string | null): p is string {
  return !!p && p.endsWith(".ipynb") && !p.startsWith("/") && !p.split("/").includes("..");
}

async function api(req: Request, url: URL): Promise<Response> {
  const route = `${req.method} ${url.pathname}`;
  const qp = url.searchParams;
  if (process.env.MOCK_SLOW) await Bun.sleep(Number(process.env.MOCK_SLOW));
  if (url.pathname === "/api/__mock/set") {
    if (qp.has("fail")) FAIL = qp.get("fail")!;
    if (qp.has("daemon")) DAEMON_UP = qp.get("daemon") !== "down";
    return json({ fail: FAIL, daemon: DAEMON_UP });
  }
  if (FAIL && new RegExp(FAIL).test(url.pathname)) return fail(502, "daemon request failed: context deadline exceeded");

  switch (route) {
    case "GET /api/overview":
      return json(overview(DAEMON_UP));
    case "GET /api/nodes":
      return json(DAEMON_UP ? nodes() : []);
    case "GET /api/metrics":
      return json(DAEMON_UP ? metrics((qp.get("window") ?? "15m") as MetricsWindow, STARTED) : { interval_ms: 5000, series: [] });
    case "GET /api/tasks":
      return json(DAEMON_UP ? tasks() : []);
    case "GET /api/jobs":
      return json(jobList(Number(qp.get("limit") ?? 200), qp.get("status") ?? "", qp.get("q") ?? ""));
    case "GET /api/analytics":
      return json(analytics((qp.get("range") ?? "24h") as AnalyticsRange));
    case "GET /api/notebooks":
      return json(
        [...files].map(([p, f]) => ({ path: p, name: p.split("/").pop()!, modified: f.modified, size: f.content.length })).sort((a, b) => b.modified - a.modified),
      );
    case "POST /api/notebooks": {
      const body = (await req.json().catch(() => ({}))) as { path?: string };
      let p = body.path;
      if (p) {
        if (!validPath(p)) return fail(400, "path must be a relative .ipynb path inside the notebook directory");
        if (files.has(p)) return fail(409, `${p} already exists`);
      } else {
        for (let i = 0; ; i++) {
          p = `Untitled${i || ""}.ipynb`;
          if (!files.has(p)) break;
        }
      }
      put(p, notebook([]));
      return json({ path: p });
    }
    case "POST /api/notebooks/rename": {
      const { from, to } = (await req.json()) as { from: string; to: string };
      if (!validPath(from) || !validPath(to)) return fail(400, "path must be a relative .ipynb path inside the notebook directory");
      const f = files.get(from);
      if (!f) return fail(404, `${from} not found`);
      if (files.has(to)) return fail(409, `${to} already exists`);
      files.delete(from);
      files.set(to, f);
      return json({ path: to });
    }
    case "GET /api/notebooks/content": {
      const p = qp.get("path");
      if (!validPath(p)) return fail(400, "path must be a relative .ipynb path inside the notebook directory");
      const f = files.get(p);
      if (!f) return fail(404, `${p} not found`);
      return new Response(f.content, { headers: { "content-type": "application/json", etag: f.etag, "cache-control": "no-store" } });
    }
    case "PUT /api/notebooks/content": {
      const p = qp.get("path");
      if (!validPath(p)) return fail(400, "path must be a relative .ipynb path inside the notebook directory");
      const cur = files.get(p);
      const match = req.headers.get("if-match");
      if (match && cur && match !== cur.etag) return fail(412, "notebook changed on disk");
      const f = put(p, await req.json());
      return json({ etag: f.etag, modified: f.modified });
    }
    case "DELETE /api/notebooks/content": {
      const p = qp.get("path");
      if (!validPath(p)) return fail(400, "path must be a relative .ipynb path inside the notebook directory");
      if (!files.delete(p)) return fail(404, `${p} not found`);
      return new Response(null, { status: 204 });
    }
    case "POST /api/__mock/drop":
      for (const ws of sockets) ws.close();
      return json({ ok: true });
    case "POST /api/__mock/touch": {
      const p = qp.get("path");
      const f = p ? files.get(p) : undefined;
      if (!p || !f) return fail(404, "not found");
      put(p, JSON.parse(f.content));
      return json({ ok: true });
    }
  }
  if (req.method === "GET" && url.pathname.startsWith("/api/jobs/")) {
    const d = jobDetail(decodeURIComponent(url.pathname.slice("/api/jobs/".length)));
    return d ? json(d) : fail(404, "job not found");
  }
  return fail(404, `no route for ${route}`);
}

const server = Bun.serve<Sock>({
  hostname: "127.0.0.1",
  port: PORT,
  async fetch(req, srv) {
    const url = new URL(req.url);
    if (url.pathname === "/api/kernel/ws") {
      const nb = url.searchParams.get("notebook");
      if (!validPath(nb)) return fail(400, "notebook path required");
      return srv.upgrade(req, { data: { notebook: nb } }) ? undefined : fail(400, "websocket upgrade failed");
    }
    if (url.pathname.startsWith("/api/")) return api(req, url);
    const file = Bun.file(path.join(DIST, url.pathname));
    if (url.pathname !== "/" && (await file.exists())) return new Response(file);
    const index = Bun.file(path.join(DIST, "index.html"));
    if (await index.exists()) return new Response(index, { headers: { "cache-control": "no-store" } });
    return new Response("console assets are not built: run `bun run build` in console/", { status: 503 });
  },
  websocket: {
    open(ws: ServerWebSocket<Sock>) {
      let kernel = kernels.get(ws.data.notebook);
      if (!kernel) {
        kernel = new MockKernel(INTERCEPTION);
        kernels.set(ws.data.notebook, kernel);
      }
      sockets.add(ws);
      ws.data.send = (f) => ws.send(JSON.stringify(f));
      kernel.attach(ws.data.send);
    },
    message(ws, message) {
      const kernel = kernels.get(ws.data.notebook);
      if (!kernel || !ws.data.send) return;
      const frame = JSON.parse(String(message)) as ClientFrame;
      kernel.handle(frame, ws.data.send);
    },
    close(ws) {
      sockets.delete(ws);
      if (ws.data.send) kernels.get(ws.data.notebook)?.detach(ws.data.send);
    },
  },
});

console.log(`mock console backend on http://127.0.0.1:${server.port} (daemon ${DAEMON_UP ? "up" : "down"}, interception ${INTERCEPTION ? "on" : "off"})`);
