import type { MimeBundle, RunTarget } from "@/lib/types";

export type Output =
  | { output_type: "stream"; name: "stdout" | "stderr"; text: string }
  | { output_type: "display_data"; data: MimeBundle; metadata: Record<string, unknown>; displayId?: string }
  | { output_type: "execute_result"; data: MimeBundle; metadata: Record<string, unknown>; execution_count: number | null }
  | { output_type: "error"; ename: string; evalue: string; traceback: string[] };

export type CellType = "code" | "markdown" | "raw";

export interface Cell {
  id: string;
  type: CellType;
  source: string;
  outputs: Output[];
  executionCount: number | null;
  target: RunTarget;
  /** Wall time of the last run, kept in the file so it survives a reload. */
  durationMs: number | null;
  metadata: Record<string, unknown>;
}

export interface NotebookDoc {
  cells: Cell[];
  metadata: Record<string, unknown>;
}

const ID_RE = /^[a-zA-Z0-9_-]{1,64}$/;

export function newCellId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

export function newCell(type: CellType, source = ""): Cell {
  return { id: newCellId(), type, source, outputs: [], executionCount: null, target: "local", durationMs: null, metadata: {} };
}

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const text = (v: unknown): string => (Array.isArray(v) ? v.join("") : typeof v === "string" ? v : "");

function parseBundle(v: unknown): MimeBundle {
  if (!isRecord(v)) return {};
  const out: MimeBundle = {};
  for (const [mime, value] of Object.entries(v)) out[mime] = Array.isArray(value) && value.every((x) => typeof x === "string") && !mime.endsWith("json") ? value.join("") : value;
  return out;
}

function parseOutput(raw: unknown): Output | null {
  if (!isRecord(raw)) return null;
  switch (raw.output_type) {
    case "stream":
      return { output_type: "stream", name: raw.name === "stderr" ? "stderr" : "stdout", text: text(raw.text) };
    case "display_data":
      return { output_type: "display_data", data: parseBundle(raw.data), metadata: isRecord(raw.metadata) ? raw.metadata : {} };
    case "execute_result":
      return { output_type: "execute_result", data: parseBundle(raw.data), metadata: isRecord(raw.metadata) ? raw.metadata : {}, execution_count: typeof raw.execution_count === "number" ? raw.execution_count : null };
    case "error":
      return { output_type: "error", ename: text(raw.ename), evalue: text(raw.evalue), traceback: Array.isArray(raw.traceback) ? raw.traceback.map(String) : [] };
    default:
      return null;
  }
}

/** Reads an nbformat 4 document. Unknown notebook and cell metadata is carried through untouched. */
export function parseNotebook(raw: unknown): NotebookDoc {
  if (!isRecord(raw) || !Array.isArray(raw.cells)) throw new Error("This file is not an nbformat 4 notebook");
  if (typeof raw.nbformat === "number" && raw.nbformat !== 4) throw new Error(`nbformat ${raw.nbformat} is not supported; convert the notebook to nbformat 4`);
  const seen = new Set<string>();
  const cells = raw.cells.filter(isRecord).map((c): Cell => {
    let id = typeof c.id === "string" && ID_RE.test(c.id) ? c.id : newCellId();
    while (seen.has(id)) id = newCellId();
    seen.add(id);
    const metadata = isRecord(c.metadata) ? { ...c.metadata } : {};
    const ours = isRecord(metadata.pipedpeer) ? metadata.pipedpeer : {};
    delete metadata.pipedpeer;
    const type: CellType = c.cell_type === "markdown" ? "markdown" : c.cell_type === "raw" ? "raw" : "code";
    return {
      id,
      type,
      source: text(c.source),
      outputs: type === "code" && Array.isArray(c.outputs) ? c.outputs.map(parseOutput).filter((o): o is Output => o !== null) : [],
      executionCount: typeof c.execution_count === "number" ? c.execution_count : null,
      target: ours.target === "cluster" ? "cluster" : "local",
      durationMs: typeof ours.duration_ms === "number" ? ours.duration_ms : null,
      metadata,
    };
  });
  return { cells, metadata: isRecord(raw.metadata) ? raw.metadata : {} };
}

const lines = (s: string): string[] => (s === "" ? [] : (s.match(/[^\n]*\n|[^\n]+$/g) ?? []));

function serializeOutput(o: Output): Record<string, unknown> {
  switch (o.output_type) {
    case "stream":
      return { output_type: "stream", name: o.name, text: lines(o.text) };
    case "display_data":
      return { output_type: "display_data", data: o.data, metadata: o.metadata };
    case "execute_result":
      return { output_type: "execute_result", data: o.data, metadata: o.metadata, execution_count: o.execution_count };
    case "error":
      return { output_type: "error", ename: o.ename, evalue: o.evalue, traceback: o.traceback };
  }
}

export function serializeNotebook(doc: NotebookDoc): Record<string, unknown> {
  return {
    cells: doc.cells.map((c) => {
      const ours = { ...(c.target === "cluster" ? { target: "cluster" } : {}), ...(c.durationMs !== null ? { duration_ms: c.durationMs } : {}) };
      const metadata = Object.keys(ours).length ? { ...c.metadata, pipedpeer: ours } : c.metadata;
      const base = { cell_type: c.type, id: c.id, metadata, source: lines(c.source) };
      return c.type === "code" ? { ...base, execution_count: c.executionCount, outputs: c.outputs.map(serializeOutput) } : base;
    }),
    metadata: doc.metadata,
    nbformat: 4,
    nbformat_minor: 5,
  };
}
