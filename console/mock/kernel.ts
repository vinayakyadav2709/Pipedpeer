import { deflateSync } from "node:zlib";
import type { ClientFrame, KernelState, KernelVar, RunTarget, ServerFrame } from "../src/lib/types.ts";

type Send = (frame: ServerFrame) => void;

interface Queued {
  cell_id: string;
  code: string;
  target: RunTarget;
}

interface Step {
  after: number;
  frame: (cellId: string) => ServerFrame;
}

const E = "\u001b[";
const REPLAY_LIMIT = 400;

function chunk(type: string, data: Uint8Array) {
  const out = new Uint8Array(12 + data.length);
  const view = new DataView(out.buffer);
  view.setUint32(0, data.length);
  out.set(new TextEncoder().encode(type), 4);
  out.set(data, 8);
  view.setUint32(8 + data.length, Bun.hash.crc32(out.subarray(4, 8 + data.length)));
  return out;
}

function plotPng(): string {
  const w = 640;
  const h = 300;
  const px = new Uint8Array(w * h * 3).fill(255);
  const dot = (x: number, y: number, c: [number, number, number], r = 1) => {
    for (let dy = -r; dy <= r; dy++)
      for (let dx = -r; dx <= r; dx++) {
        const xx = Math.round(x) + dx;
        const yy = Math.round(y) + dy;
        if (xx < 0 || yy < 0 || xx >= w || yy >= h) continue;
        px.set(c, (yy * w + xx) * 3);
      }
  };
  for (let x = 48; x < w - 16; x++) dot(x, h - 32, [150, 150, 150], 0);
  for (let y = 16; y < h - 32; y++) dot(48, y, [150, 150, 150], 0);
  for (let g = 1; g <= 4; g++) for (let x = 49; x < w - 16; x += 1) dot(x, h - 32 - g * 56, [232, 232, 232], 0);
  for (let x = 48; x < w - 16; x += 0.25) {
    const u = (x - 48) / (w - 64);
    dot(x, h - 40 - 220 * (1 - Math.exp(-u * 4)) * (0.92 + 0.05 * Math.sin(u * 40)), [42, 120, 214]);
    dot(x, h - 40 - 190 * (1 - Math.exp(-u * 2.6)) * (0.9 + 0.07 * Math.sin(u * 31 + 1)), [235, 104, 52]);
  }
  const raw = new Uint8Array((w * 3 + 1) * h);
  for (let y = 0; y < h; y++) raw.set(px.subarray(y * w * 3, (y + 1) * w * 3), y * (w * 3 + 1) + 1);
  const ihdr = new Uint8Array(13);
  const v = new DataView(ihdr.buffer);
  v.setUint32(0, w);
  v.setUint32(4, h);
  ihdr.set([8, 2, 0, 0, 0], 8);
  const parts = [
    new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk("IHDR", ihdr),
    chunk("IDAT", new Uint8Array(deflateSync(raw))),
    chunk("IEND", new Uint8Array(0)),
  ];
  return Buffer.concat(parts).toString("base64");
}

let png: string | null = null;

const DF_HTML = `<div>
<style scoped>
    .dataframe tbody tr th:only-of-type { vertical-align: middle; }
    .dataframe tbody tr th { vertical-align: top; }
    .dataframe thead th { text-align: right; }
</style>
<table border="1" class="dataframe">
  <thead><tr style="text-align: right;"><th></th><th>host</th><th>chunks</th><th>rows</th><th>mean_ms</th><th>p95_ms</th></tr></thead>
  <tbody>
    <tr><th>0</th><td>workstation-01</td><td>4</td><td>250000</td><td>18.42</td><td>31.07</td></tr>
    <tr><th>1</th><td>lab-gpu-02</td><td>8</td><td>500000</td><td>11.96</td><td>19.80</td></tr>
    <tr><th>2</th><td>thinkpad-x1</td><td>2</td><td>125000</td><td>27.31</td><td>44.12</td></tr>
    <tr><th>3</th><td>nuc-attic</td><td>2</td><td>125000</td><td>24.88</td><td>40.55</td></tr>
  </tbody>
</table>
<p>4 rows × 5 columns</p>
</div>`;

const DF_TEXT = `             host  chunks    rows  mean_ms  p95_ms
0  workstation-01       4  250000    18.42   31.07
1      lab-gpu-02       8  500000    11.96   19.80
2     thinkpad-x1       2  125000    27.31   44.12
3       nuc-attic       2  125000    24.88   40.55`;

const SVG = `<svg xmlns="http://www.w3.org/2000/svg" width="360" height="120" viewBox="0 0 360 120"><rect width="360" height="120" fill="#fff"/><g fill="none" stroke="#2a78d6" stroke-width="2"><circle cx="60" cy="60" r="22"/><circle cx="180" cy="34" r="16"/><circle cx="180" cy="88" r="16"/><circle cx="300" cy="60" r="22"/><path d="M82 54 164 36M82 66l82 20M196 36l82 18M196 86l82-20"/></g><g font-family="sans-serif" font-size="11" fill="#333" text-anchor="middle"><text x="60" y="64">submit</text><text x="300" y="64">gather</text></g></svg>`;

function traceback(ename: string, evalue: string, line: string, lineNo: number): string[] {
  return [
    `${E}0;31m---------------------------------------------------------------------------${E}0m`,
    `${E}0;31m${ename}${E}0m                         Traceback (most recent call last)`,
    `Cell ${E}0;32mIn[4], line ${lineNo}${E}0m`,
    `${E}1;32m      ${Math.max(1, lineNo - 1)}${E}0m ${E}38;5;66;03m# previous line${E}0m`,
    `${E}0;32m----> ${lineNo}${E}0m ${line}`,
    ``,
    `File ${E}0;32m~/project/pipeline.py:42${E}0m, in ${E}0;36mload${E}0m${E}0;34m(path)${E}0m`,
    `${E}1;32m     40${E}0m ${E}38;5;28;01mdef${E}0m ${E}38;5;21mload${E}0m(path):`,
    `${E}1;32m     41${E}0m     frames ${E}38;5;241m=${E}0m [read(p) ${E}38;5;28;01mfor${E}0m p ${E}38;5;129;01min${E}0m path]`,
    `${E}0;32m---> 42${E}0m     ${E}38;5;28;01mreturn${E}0m concat(frames)[${E}38;5;124m"${E}0m${E}38;5;124mmissing${E}0m${E}38;5;124m"${E}0m]`,
    ``,
    `${E}0;31m${ename}${E}0m: ${evalue}`,
  ];
}

function plan(code: string, target: RunTarget, vars: Map<string, KernelVar>): { steps: Step[]; ok: boolean; endless: boolean } {
  const steps: Step[] = [];
  let ok = true;
  let endless = false;
  const stream = (after: number, text: string, name: "stdout" | "stderr" = "stdout") =>
    steps.push({ after, frame: (cell_id) => ({ type: "stream", cell_id, name, text }) });
  const lines = code.split("\n").map((l) => l.trimEnd());
  const last = [...lines].reverse().find((l) => l.trim() && !l.trim().startsWith("#")) ?? "";

  for (const l of lines) {
    const m = /^([A-Za-z_]\w*)\s*=\s*(.+)$/.exec(l);
    if (!m) continue;
    const [, name, rhs] = m as unknown as [string, string, string];
    const isDf = /DataFrame|read_csv|read_parquet/.test(rhs);
    const isArr = /np\.|array|arange|linspace|rand/.test(rhs);
    const isNum = /^-?[\d.]+$/.test(rhs.trim());
    const isStr = /^["']/.test(rhs.trim());
    vars.set(name, {
      name,
      type: isDf ? "DataFrame" : isArr ? "ndarray" : isNum ? (rhs.includes(".") ? "float" : "int") : isStr ? "str" : /^\[/.test(rhs) ? "list" : /^\{/.test(rhs) ? "dict" : "object",
      repr: isDf ? "(1000000, 5)" : isArr ? "(1000, 64) float64" : rhs.length > 48 ? rhs.slice(0, 47) + "…" : rhs,
      size_bytes: isDf ? 40_000_128 : isArr ? 512_112 : isStr ? 49 + rhs.length : 28,
    });
  }

  if (target === "cluster") {
    stream(250, `${E}2m[pipedpeer]${E}0m submitted as job cell-${Date.now().toString(36)}\n`);
    stream(500, `${E}2m[pipedpeer]${E}0m placed on ${E}1mlab-gpu-02${E}0m (score 0.93, 31 cpus free)\n`);
    stream(400, `${E}2m[pipedpeer]${E}0m syncing workspace: 6 files, 48 KB\n`);
  }

  if (/while\s+True/.test(code)) {
    endless = true;
    return { steps, ok, endless };
  }

  if (/warn|stderr/.test(code)) stream(200, "UserWarning: converting a masked element to nan.\n  result = np.asarray(values, dtype=float)\n", "stderr");

  if (/Pool|joblib|Parallel|\.map\(|concurrent\.futures/.test(code)) {
    steps.push({ after: 300, frame: (cell_id) => ({ type: "cluster", cell_id, event: "fanout", nodes: [{ hostname: "lab-gpu-02", chunks: 8 }, { hostname: "thinkpad-x1", chunks: 2 }, { hostname: "nuc-attic", chunks: 2 }], local_chunks: 4 }) });
    for (let i = 1; i <= 8; i++) stream(260, `chunk ${String(i * 2).padStart(2)}/16 done  ${(i * 12.5).toFixed(0).padStart(3)}%\n`);
    stream(150, "merged 1,000,000 rows in 2.31s\n");
  }

  if (/tqdm|progress/.test(code)) {
    for (let i = 1; i <= 20; i++) {
      const bar = "█".repeat(i) + " ".repeat(20 - i);
      stream(140, `\r${String(i * 5).padStart(3)}%|${bar}| ${i * 50}/1000 [00:0${Math.floor(i / 7)}<00:0${Math.floor((20 - i) / 7)}, 355.1it/s]`);
    }
    stream(60, "\n");
  }

  const sleeps = (code.match(/sleep/g) ?? []).length;
  const prints = lines.filter((l) => /^\s*print\(/.test(l));
  const loop = /for\s+\w+\s+in\s+range\((\d+)\)/.exec(code);
  if (loop && prints.length) {
    const n = Math.min(Number(loop[1]), 400);
    for (let i = 0; i < n; i++) stream(sleeps ? 450 : 15, `step ${i}: loss=${(2.2 / (1 + i * 0.4)).toFixed(4)}\n`);
  } else {
    for (const p of prints) {
      const arg = /print\((.*)\)\s*$/.exec(p)?.[1] ?? "";
      const text = /^f?["'](.*)["']$/.exec(arg.trim())?.[1] ?? arg;
      stream(sleeps ? 500 : 60, text.replace(/\\n/g, "\n") + "\n");
    }
  }

  if (/plt\.|\.plot\(|imshow/.test(code)) {
    png ??= plotPng();
    const data = png;
    steps.push({ after: 420, frame: (cell_id) => ({ type: "display", cell_id, data: { "text/plain": "<Figure size 640x300 with 1 Axes>", "image/png": data } }) });
  }
  if (/svg/i.test(code)) {
    steps.push({ after: 200, frame: (cell_id) => ({ type: "display", cell_id, data: { "text/plain": "<IPython.core.display.SVG object>", "image/svg+xml": SVG } }) });
  }
  if (/HTML\(/.test(code)) {
    const html = `<div style="font:14px sans-serif"><b>Sandboxed HTML</b>: scripts run in an isolated frame. <button onclick="this.textContent='clicked '+(++window.n||(window.n=1))">click me</button><script>document.currentScript.parentElement.append(' (origin: '+location.origin+')')</script></div>`;
    steps.push({ after: 150, frame: (cell_id) => ({ type: "display", cell_id, data: { "text/plain": "<IPython.core.display.HTML object>", "text/html": html } }) });
  }

  if (/raise |1\s*\/\s*0|undefined_name|KeyError/.test(code)) {
    ok = false;
    const zero = /1\s*\/\s*0/.test(code);
    const ename = zero ? "ZeroDivisionError" : /KeyError/.test(code) ? "KeyError" : /raise (\w+)/.exec(code)?.[1] ?? "NameError";
    const evalue = zero ? "division by zero" : ename === "KeyError" ? "'missing'" : /raise \w+\(["'](.*)["']\)/.exec(code)?.[1] ?? "name 'undefined_name' is not defined";
    const lineNo = Math.max(1, lines.findIndex((l) => /raise |1\s*\/\s*0|undefined_name|KeyError/.test(l)) + 1);
    steps.push({ after: 180, frame: (cell_id) => ({ type: "error", cell_id, ename, evalue, traceback: traceback(ename, evalue, lines[lineNo - 1] ?? "", lineNo) }) });
    return { steps, ok, endless };
  }

  const expr = last.trim();
  const isStatement = /^(import |from |print\(|for |while |if |def |class |with |return |plt\.|display\(|HTML\(|SVG\()/.test(expr) || /^[A-Za-z_][\w.]*\s*=[^=]/.test(expr) || /^\s/.test(last) || !expr;
  if (!isStatement) {
    steps.push({
      after: 120,
      frame: (cell_id) => {
        if (/df|head\(|describe\(|DataFrame/.test(expr)) return { type: "result", cell_id, execution_count: 0, data: { "text/plain": DF_TEXT, "text/html": DF_HTML } };
        if (/json|config|\{|dict\(|vars\(/.test(expr)) {
          const obj = { cluster: { nodes: 5, healthy: 3, cpus: 56 }, scheduler: { strategy: "score", weights: { cpu: 0.4, memory: 0.35, gpu: 0.25 }, never_slower: true }, tiers: ["user", "historical", "filesize", "import"], retries: null };
          return { type: "result", cell_id, execution_count: 0, data: { "text/plain": JSON.stringify(obj), "application/json": obj } };
        }
        let text = expr;
        if (/^[\d\s+\-*/().%]+$/.test(expr)) {
          try {
            text = String(Function(`"use strict";return (${expr})`)());
          } catch {
            text = expr;
          }
        } else if (vars.has(expr)) text = vars.get(expr)!.repr;
        else if (/^["']/.test(expr)) text = expr;
        else text = `<${expr.replace(/\(.*$/, "")} object at 0x7f3a9c21d4e0>`;
        return { type: "result", cell_id, execution_count: 0, data: { "text/plain": text } };
      },
    });
  }
  if (!steps.length) steps.push({ after: sleeps ? 1500 : 90, frame: () => ({ type: "stream", cell_id: "", name: "stdout", text: "" }) });
  return { steps, ok, endless };
}

const COMPLETIONS = ["import", "in", "is", "if", "isinstance", "int", "input", "print", "pandas", "pool", "property", "range", "return", "raise", "numpy", "None", "not", "from", "for", "float", "False", "def", "del", "dict", "df", "display", "len", "list", "lambda", "map", "max", "min", "open", "os", "sum", "sorted", "str", "set", "self", "True", "try", "tuple", "type", "time", "torch", "with", "while", "zip"];
const ATTRS: Record<string, string[]> = {
  df: ["head", "tail", "describe", "groupby", "merge", "columns", "shape", "iloc", "loc", "to_parquet", "apply"],
  np: ["array", "arange", "linspace", "mean", "std", "zeros", "ones", "random", "concatenate"],
  pd: ["DataFrame", "read_csv", "read_parquet", "concat", "merge", "Series", "to_datetime"],
  plt: ["plot", "figure", "show", "subplots", "legend", "title", "xlabel", "ylabel", "imshow"],
  time: ["sleep", "time", "perf_counter", "monotonic"],
  os: ["cpu_count", "environ", "getcwd", "listdir", "path"],
};

export class MockKernel {
  private state: KernelState = "starting";
  private count = 0;
  private queue: Queued[] = [];
  private running: Queued | null = null;
  private replay: ServerFrame[] = [];
  private clients = new Set<Send>();
  private timer: ReturnType<typeof setTimeout> | null = null;
  private startedAt = 0;
  private vars = new Map<string, KernelVar>();
  private readonly interception: boolean;

  constructor(interception: boolean) {
    this.interception = interception;
    setTimeout(() => {
      this.state = "idle";
      this.status();
      this.pump();
    }, 700);
  }

  attach(send: Send) {
    this.clients.add(send);
    for (const f of this.replay) send(f);
    send(this.statusFrame());
  }

  detach(send: Send) {
    this.clients.delete(send);
  }

  private statusFrame(): ServerFrame {
    return {
      type: "status",
      state: this.state,
      python: "3.12.4",
      interception: this.interception,
      execution_count: this.count,
      queued: this.queue.map((q) => q.cell_id),
      running: this.running?.cell_id ?? null,
    };
  }

  private status() {
    this.broadcast(this.statusFrame(), false);
  }

  private broadcast(frame: ServerFrame, record = true) {
    if (record && this.running) {
      this.replay.push(frame);
      if (this.replay.length > REPLAY_LIMIT) this.replay.shift();
    }
    for (const send of this.clients) send(frame);
  }

  handle(frame: ClientFrame, reply: Send) {
    switch (frame.type) {
      case "execute":
        this.queue.push({ cell_id: frame.cell_id, code: frame.code, target: frame.target });
        this.status();
        this.pump();
        return;
      case "interrupt":
        if (this.running) this.finish(false, true);
        return;
      case "restart":
        this.restart();
        return;
      case "shutdown":
        this.abortAll();
        this.state = "dead";
        this.status();
        return;
      case "complete": {
        const before = frame.code.slice(0, frame.cursor);
        const attr = /([A-Za-z_]\w*)\.(\w*)$/.exec(before);
        const word = /[A-Za-z_]\w*$/.exec(before)?.[0] ?? "";
        const prefix = attr ? attr[2]! : word;
        const pool = attr ? (ATTRS[attr[1]!] ?? []) : [...COMPLETIONS, ...this.vars.keys()];
        const matches = [...new Set(pool)].filter((c) => c.startsWith(prefix) && c !== prefix).slice(0, 30);
        setTimeout(() => reply({ type: "complete_reply", req_id: frame.req_id, matches, cursor_start: frame.cursor - prefix.length, cursor_end: frame.cursor }), 40);
        return;
      }
      case "inspect_vars":
        reply({ type: "vars_reply", req_id: frame.req_id, vars: [...this.vars.values()] });
        return;
    }
  }

  private abortAll() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    const pending = [...(this.running ? [this.running] : []), ...this.queue];
    this.running = null;
    this.queue = [];
    this.replay = [];
    for (const q of pending) this.broadcast({ type: "done", cell_id: q.cell_id, execution_count: this.count, status: "aborted", duration_ms: 0 }, false);
  }

  private restart() {
    this.abortAll();
    this.vars.clear();
    this.count = 0;
    this.state = "starting";
    this.status();
    setTimeout(() => {
      this.state = "idle";
      this.status();
      this.pump();
    }, 900);
  }

  private pump() {
    if (this.running || this.state !== "idle") return;
    const next = this.queue.shift();
    if (!next) return;
    this.running = next;
    this.replay = [];
    this.count += 1;
    this.state = "busy";
    this.startedAt = Date.now();
    this.status();
    const { steps, ok, endless } = plan(next.code, next.target, next.target === "local" ? this.vars : new Map());
    let i = 0;
    let tick = 0;
    const step = () => {
      if (endless && i >= steps.length) {
        this.broadcast({ type: "stream", cell_id: next.cell_id, name: "stdout", text: `tick ${tick++}  ${new Date().toISOString().slice(11, 19)}\n` });
        this.timer = setTimeout(step, 500);
        return;
      }
      const s = steps[i++];
      if (!s) {
        this.finish(ok, false);
        return;
      }
      this.timer = setTimeout(() => {
        const f = s.frame(next.cell_id);
        if (f.type === "result") f.execution_count = this.count;
        if (!(f.type === "stream" && !f.text)) this.broadcast(f);
        step();
      }, s.after);
    };
    step();
  }

  private finish(ok: boolean, interrupted: boolean) {
    const cur = this.running;
    if (!cur) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    if (interrupted) {
      this.broadcast({
        type: "error",
        cell_id: cur.cell_id,
        ename: "KeyboardInterrupt",
        evalue: "",
        traceback: [
          `${E}0;31m---------------------------------------------------------------------------${E}0m`,
          `${E}0;31mKeyboardInterrupt${E}0m                         Traceback (most recent call last)`,
          `Cell ${E}0;32mIn[${this.count}], line 3${E}0m`,
          `${E}0;32m----> 3${E}0m     time${E}38;5;241m.${E}0msleep(${E}38;5;241m0.5${E}0m)`,
          ``,
          `${E}0;31mKeyboardInterrupt${E}0m: `,
        ],
      });
    }
    this.broadcast({ type: "done", cell_id: cur.cell_id, execution_count: this.count, status: ok && !interrupted ? "ok" : "error", duration_ms: Date.now() - this.startedAt });
    this.running = null;
    this.replay = [];
    this.state = "idle";
    this.status();
    this.pump();
  }
}
