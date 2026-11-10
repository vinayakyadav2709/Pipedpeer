import type { DoneStatus, FanoutNode, KernelState, KernelVar, MimeBundle, RunTarget, ServerFrame } from "@/lib/types";
import { newCell, type Cell, type CellType, type NotebookDoc, type Output } from "./nbformat";

/**
 * The whole notebook, in one value:
 *
 *   doc      cells and their persisted outputs (what goes into the .ipynb); `rev` bumps on every change to it
 *   queue    cells waiting to run, in order, with the code captured when they were queued
 *   running  the one cell the kernel is executing, tagged with the socket epoch that last confirmed it
 *   runs     how each cell's last run ended
 *   kernel   link state plus the last `status` frame
 *   save     where the on-disk copy stands relative to `rev`
 *
 * The client owns the queue and sends one `execute` at a time. That keeps Jupyter's semantics, which the
 * wire protocol has no message for: an error or an interrupt drops the cells queued behind it.
 */
export interface NotebookState {
  doc: NotebookDoc;
  rev: number;
  focus: string | null;
  mode: "command" | "edit";
  queue: QueueItem[];
  running: Running | null;
  runs: Record<string, RunRecord>;
  fanout: Record<string, Fanout>;
  kernel: KernelInfo;
  save: SaveState;
  deleted: { cell: Cell; index: number }[];
  vars: KernelVar[] | null;
  /** Bumps when cell sources were replaced wholesale, so editors re-read them. */
  sourceEpoch: number;
  /** Set between sending `restart` and the fresh kernel reporting idle; `thenRun` is queued at that point. */
  restart: { thenRun: string[] | null; sawStarting: boolean } | null;
}

export interface QueueItem {
  cellId: string;
  code: string;
  target: RunTarget;
}

export interface Running {
  cellId: string;
  startedAt: number;
  epoch: number;
  target: RunTarget;
}

export interface RunRecord {
  /** `lost`: the connection dropped mid-run and the kernel had moved on when it came back. */
  status: DoneStatus | "lost" | "skipped";
}

export interface Fanout {
  nodes: FanoutNode[];
  localChunks: number;
}

export type Link = "connecting" | "open" | "closed";

export interface KernelInfo {
  link: Link;
  /** Increments each time the socket opens. */
  epoch: number;
  state: KernelState | "unknown";
  python: string;
  interception: boolean | null;
  executionCount: number;
  serverQueued: string[];
  fatal: string | null;
}

export interface SaveState {
  status: "clean" | "dirty" | "saving" | "conflict" | "error";
  etag: string | null;
  savedRev: number;
  savedAt: number | null;
  error: string | null;
}

export type Action =
  | { type: "source"; id: string; source: string }
  | { type: "insert"; index: number; cellType: CellType; edit?: boolean }
  | { type: "delete"; id: string }
  | { type: "undoDelete" }
  | { type: "move"; id: string; index: number }
  | { type: "setType"; id: string; cellType: CellType }
  | { type: "setTarget"; id: string; target: RunTarget }
  | { type: "clearOutputs"; id: string }
  | { type: "clearAllOutputs" }
  | { type: "focus"; id: string | null; mode?: "command" | "edit" }
  | { type: "enqueue"; ids: string[] }
  | { type: "dispatched"; cellId: string; at: number }
  | { type: "interrupt" }
  | { type: "restart"; thenRun: string[] | null }
  | { type: "link"; link: Link }
  | { type: "frame"; frame: ServerFrame; at: number }
  | { type: "vars"; vars: KernelVar[] }
  | { type: "saveStart" }
  | { type: "saveOk"; etag: string; rev: number; at: number }
  | { type: "saveConflict" }
  | { type: "saveError"; message: string }
  | { type: "reloaded"; doc: NotebookDoc; etag: string | null; at: number }
  | { type: "overwrite" };

const STREAM_CAP = 400_000;

export function initialState(doc: NotebookDoc, etag: string | null, at: number): NotebookState {
  return {
    doc,
    rev: 0,
    focus: doc.cells[0]?.id ?? null,
    mode: "command",
    queue: [],
    running: null,
    runs: {},
    fanout: {},
    kernel: { link: "connecting", epoch: 0, state: "unknown", python: "", interception: null, executionCount: 0, serverQueued: [], fatal: null },
    save: { status: "clean", etag, savedRev: 0, savedAt: at, error: null },
    deleted: [],
    vars: null,
    sourceEpoch: 0,
    restart: null,
  };
}

export type CellRunState =
  | { kind: "idle" }
  | { kind: "queued" }
  | { kind: "running"; startedAt: number; target: RunTarget }
  | { kind: "done"; status: RunRecord["status"] };

export function cellRunState(s: NotebookState, id: string): CellRunState {
  if (s.running?.cellId === id) return { kind: "running", startedAt: s.running.startedAt, target: s.running.target };
  if (s.queue.some((q) => q.cellId === id) || s.kernel.serverQueued.includes(id)) return { kind: "queued" };
  const r = s.runs[id];
  return r ? { kind: "done", status: r.status } : { kind: "idle" };
}

export const isBusy = (s: NotebookState) => s.running !== null || s.queue.length > 0;

function edit(s: NotebookState, cells: Cell[]): NotebookState {
  const status = s.save.status === "conflict" || s.save.status === "saving" ? s.save.status : "dirty";
  return { ...s, doc: { ...s.doc, cells }, rev: s.rev + 1, save: { ...s.save, status } };
}

function patchCell(s: NotebookState, id: string, fn: (c: Cell) => Cell): NotebookState {
  let hit = false;
  const cells = s.doc.cells.map((c) => {
    if (c.id !== id) return c;
    hit = true;
    return fn(c);
  });
  return hit ? edit(s, cells) : s;
}

function omit<T>(rec: Record<string, T>, id: string): Record<string, T> {
  if (!(id in rec)) return rec;
  const { [id]: _drop, ...rest } = rec;
  return rest;
}

function appendStream(outputs: Output[], name: "stdout" | "stderr", text: string): Output[] {
  const last = outputs[outputs.length - 1];
  if (last?.output_type === "stream" && last.name === name) {
    let merged = last.text + text;
    if (merged.length > STREAM_CAP) merged = "[earlier output truncated]\n" + merged.slice(merged.indexOf("\n", merged.length - STREAM_CAP) + 1);
    return [...outputs.slice(0, -1), { ...last, text: merged }];
  }
  return [...outputs, { output_type: "stream", name, text }];
}

function appendDisplay(outputs: Output[], data: MimeBundle, displayId: string | undefined): Output[] {
  const next: Output = { output_type: "display_data", data, metadata: {}, ...(displayId ? { displayId } : {}) };
  if (displayId) {
    const i = outputs.findIndex((o) => o.output_type === "display_data" && o.displayId === displayId);
    if (i >= 0) return outputs.map((o, j) => (j === i ? next : o));
  }
  return [...outputs, next];
}

/** Makes `cellId` the running cell for this socket epoch. Output shown from an earlier epoch is dropped, because the server replays the run's frames after a reconnect. */
function adopt(s: NotebookState, cellId: string, at: number): NotebookState {
  const cur = s.running;
  if (cur?.cellId === cellId && cur.epoch === s.kernel.epoch) return s;
  const cell = s.doc.cells.find((c) => c.id === cellId);
  if (!cell) return s;
  const startedAt = cur?.cellId === cellId ? cur.startedAt : at;
  const cleared = patchCell(s, cellId, (c) => ({ ...c, outputs: [], durationMs: null }));
  return {
    ...cleared,
    running: { cellId, startedAt, epoch: s.kernel.epoch, target: cur?.cellId === cellId ? cur.target : cell.target },
    queue: s.queue.filter((q) => q.cellId !== cellId),
    runs: omit(s.runs, cellId),
    fanout: omit(s.fanout, cellId),
  };
}

function markSkipped(s: NotebookState, items: QueueItem[]): Record<string, RunRecord> {
  if (!items.length) return s.runs;
  const runs = { ...s.runs };
  for (const q of items) runs[q.cellId] = { status: "skipped" };
  return runs;
}

function onFrame(s: NotebookState, f: ServerFrame, at: number): NotebookState {
  switch (f.type) {
    case "status": {
      const kernel: KernelInfo = {
        ...s.kernel,
        state: f.state,
        python: f.python || s.kernel.python,
        interception: f.interception,
        executionCount: f.execution_count,
        serverQueued: f.queued ?? [],
        fatal: f.state === "dead" ? s.kernel.fatal : null,
      };
      let next: NotebookState = { ...s, kernel };
      if (next.restart) {
        const fresh = f.state === "idle" && !f.running && (next.restart.sawStarting || f.execution_count === 0);
        if (fresh) {
          const { thenRun } = next.restart;
          const stuck = next.running;
          next = { ...next, restart: null, running: null, runs: stuck ? { ...next.runs, [stuck.cellId]: { status: "aborted" } } : next.runs };
          return thenRun ? reducer(next, { type: "enqueue", ids: thenRun }) : next;
        }
        if (f.state === "starting" || f.state === "dead") next = { ...next, restart: f.state === "dead" ? null : { ...next.restart, sawStarting: true } };
      }
      if (f.running) return adopt(next, f.running, at);
      const cur = next.running;
      if (cur && (cur.epoch < kernel.epoch || f.state === "dead")) {
        next = { ...next, running: null, runs: { ...next.runs, [cur.cellId]: { status: "lost" } } };
      }
      if (f.state === "dead" && next.queue.length) next = { ...next, runs: markSkipped(next, next.queue), queue: [] };
      return next;
    }
    case "stream": {
      const a = adopt(s, f.cell_id, at);
      return patchCell(a, f.cell_id, (c) => ({ ...c, outputs: appendStream(c.outputs, f.name, f.text) }));
    }
    case "display": {
      const a = adopt(s, f.cell_id, at);
      return patchCell(a, f.cell_id, (c) => ({ ...c, outputs: appendDisplay(c.outputs, f.data, f.display_id) }));
    }
    case "result": {
      const a = adopt(s, f.cell_id, at);
      return patchCell(a, f.cell_id, (c) => ({ ...c, outputs: [...c.outputs, { output_type: "execute_result", data: f.data, metadata: {}, execution_count: f.execution_count }] }));
    }
    case "error": {
      const a = adopt(s, f.cell_id, at);
      return patchCell(a, f.cell_id, (c) => ({ ...c, outputs: [...c.outputs, { output_type: "error", ename: f.ename, evalue: f.evalue, traceback: f.traceback ?? [] }] }));
    }
    case "cluster": {
      const a = adopt(s, f.cell_id, at);
      return { ...a, fanout: { ...a.fanout, [f.cell_id]: { nodes: f.nodes ?? [], localChunks: f.local_chunks } } };
    }
    case "done": {
      const wasRunning = s.running?.cellId === f.cell_id;
      let next = patchCell(s, f.cell_id, (c) => ({
        ...c,
        executionCount: f.status === "aborted" ? c.executionCount : f.execution_count,
        durationMs: f.status === "aborted" ? null : f.duration_ms,
      }));
      next = {
        ...next,
        running: wasRunning ? null : next.running,
        queue: next.queue.filter((q) => q.cellId !== f.cell_id),
        runs: { ...next.runs, [f.cell_id]: { status: f.status } },
      };
      if (f.status === "error" && next.queue.length) next = { ...next, runs: markSkipped(next, next.queue), queue: [] };
      return next;
    }
    case "error_fatal":
      return { ...s, kernel: { ...s.kernel, fatal: f.message } };
    case "complete_reply":
    case "vars_reply":
      return s;
  }
}

export function reducer(s: NotebookState, a: Action): NotebookState {
  switch (a.type) {
    case "source":
      return patchCell(s, a.id, (c) => (c.source === a.source ? c : { ...c, source: a.source }));

    case "insert": {
      const cell = newCell(a.cellType);
      const index = Math.min(Math.max(0, a.index), s.doc.cells.length);
      const next = edit(s, [...s.doc.cells.slice(0, index), cell, ...s.doc.cells.slice(index)]);
      return { ...next, focus: cell.id, mode: a.edit ? "edit" : "command" };
    }

    case "delete": {
      const index = s.doc.cells.findIndex((c) => c.id === a.id);
      if (index < 0 || s.running?.cellId === a.id) return s;
      const cell = s.doc.cells[index]!;
      const cells = s.doc.cells.filter((c) => c.id !== a.id);
      const next = edit(s, cells);
      return {
        ...next,
        deleted: [...s.deleted, { cell, index }].slice(-50),
        focus: s.focus === a.id ? (cells[index] ?? cells[index - 1])?.id ?? null : s.focus,
        mode: "command",
        queue: s.queue.filter((q) => q.cellId !== a.id),
        runs: omit(s.runs, a.id),
        fanout: omit(s.fanout, a.id),
      };
    }

    case "undoDelete": {
      const last = s.deleted[s.deleted.length - 1];
      if (!last) return s;
      const index = Math.min(last.index, s.doc.cells.length);
      const next = edit(s, [...s.doc.cells.slice(0, index), last.cell, ...s.doc.cells.slice(index)]);
      return { ...next, deleted: s.deleted.slice(0, -1), focus: last.cell.id, mode: "command" };
    }

    case "move": {
      const from = s.doc.cells.findIndex((c) => c.id === a.id);
      if (from < 0) return s;
      const to = Math.min(Math.max(0, a.index), s.doc.cells.length - 1);
      if (to === from) return s;
      const cells = [...s.doc.cells];
      const [cell] = cells.splice(from, 1);
      cells.splice(to, 0, cell!);
      return edit(s, cells);
    }

    case "setType":
      return patchCell(s, a.id, (c) => (c.type === a.cellType ? c : { ...c, type: a.cellType, outputs: [], executionCount: null, durationMs: null }));

    case "setTarget":
      return patchCell(s, a.id, (c) => (c.target === a.target ? c : { ...c, target: a.target }));

    case "clearOutputs": {
      const next = patchCell(s, a.id, (c) => (c.outputs.length || c.executionCount !== null ? { ...c, outputs: [], executionCount: null, durationMs: null } : c));
      return { ...next, runs: s.running?.cellId === a.id ? next.runs : omit(next.runs, a.id), fanout: omit(next.fanout, a.id) };
    }

    case "clearAllOutputs": {
      const keep = s.running?.cellId;
      const next = edit(
        s,
        s.doc.cells.map((c) => (c.type === "code" && c.id !== keep ? { ...c, outputs: [], executionCount: null, durationMs: null } : c)),
      );
      return { ...next, runs: {}, fanout: keep && s.fanout[keep] ? { [keep]: s.fanout[keep] } : {} };
    }

    case "focus":
      return s.focus === a.id && (a.mode === undefined || a.mode === s.mode) ? s : { ...s, focus: a.id, mode: a.mode ?? s.mode };

    case "enqueue": {
      const taken = new Set([...s.queue.map((q) => q.cellId), ...(s.running ? [s.running.cellId] : []), ...s.kernel.serverQueued]);
      const items: QueueItem[] = [];
      for (const id of a.ids) {
        const c = s.doc.cells.find((x) => x.id === id);
        if (!c || c.type !== "code" || taken.has(id) || !c.source.trim()) continue;
        taken.add(id);
        items.push({ cellId: id, code: c.source, target: c.target });
      }
      if (!items.length) return s;
      const ids = new Set(items.map((i) => i.cellId));
      const next = edit(
        s,
        s.doc.cells.map((c) => (ids.has(c.id) ? { ...c, outputs: [], durationMs: null } : c)),
      );
      const runs = { ...next.runs };
      const fanout = { ...next.fanout };
      for (const id of ids) {
        delete runs[id];
        delete fanout[id];
      }
      return { ...next, queue: [...s.queue, ...items], runs, fanout };
    }

    case "dispatched": {
      const item = s.queue.find((q) => q.cellId === a.cellId);
      if (!item) return s;
      return { ...s, queue: s.queue.filter((q) => q !== item), running: { cellId: item.cellId, startedAt: a.at, epoch: s.kernel.epoch, target: item.target } };
    }

    case "interrupt":
      return s.queue.length ? { ...s, runs: markSkipped(s, s.queue), queue: [] } : s;

    case "restart":
      return { ...s, runs: markSkipped(s, s.queue), queue: [], vars: null, kernel: { ...s.kernel, fatal: null }, restart: { thenRun: a.thenRun, sawStarting: false } };

    case "link":
      return { ...s, kernel: { ...s.kernel, link: a.link, epoch: a.link === "open" ? s.kernel.epoch + 1 : s.kernel.epoch } };

    case "frame":
      return onFrame(s, a.frame, a.at);

    case "vars":
      return { ...s, vars: a.vars };

    case "saveStart":
      return { ...s, save: { ...s.save, status: "saving", error: null } };

    case "saveOk":
      return { ...s, save: { status: a.rev === s.rev ? "clean" : "dirty", etag: a.etag, savedRev: a.rev, savedAt: a.at, error: null } };

    case "saveConflict":
      return { ...s, save: { ...s.save, status: "conflict", error: null } };

    case "saveError":
      return { ...s, save: { ...s.save, status: "error", error: a.message } };

    case "overwrite":
      return { ...s, save: { ...s.save, status: "dirty", etag: null } };

    case "reloaded": {
      const ids = new Set(a.doc.cells.map((c) => c.id));
      const running = s.running && ids.has(s.running.cellId) ? s.running : null;
      const doc = running ? { ...a.doc, cells: a.doc.cells.map((c) => (c.id === running.cellId ? { ...c, outputs: s.doc.cells.find((x) => x.id === c.id)?.outputs ?? c.outputs } : c)) } : a.doc;
      return {
        ...s,
        doc,
        rev: s.rev + 1,
        save: { status: "clean", etag: a.etag, savedRev: s.rev + 1, savedAt: a.at, error: null },
        focus: s.focus && ids.has(s.focus) ? s.focus : (a.doc.cells[0]?.id ?? null),
        mode: "command",
        queue: [],
        running,
        runs: {},
        fanout: {},
        deleted: [],
        sourceEpoch: s.sourceEpoch + 1,
      };
    }
  }
}
