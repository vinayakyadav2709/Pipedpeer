import { ChevronRight, CircleAlert, Eraser, FastForward, Keyboard, Laptop, Network, PanelRight, Play, Plus, RefreshCw, RotateCw, Square, Type, X } from "lucide-react";
import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import { api, ApiError } from "@/lib/api";
import { useOverview } from "@/lib/cluster";
import { bytes, gb, timeSec } from "@/lib/format";
import { usePoll, whenDaemonDown } from "@/lib/poll";
import { Link } from "@/lib/router";
import type { KernelVar } from "@/lib/types";
import { cn } from "@/ui/cn";
import { Button, IconButton, Kbd } from "@/ui/controls";
import { Menu } from "@/ui/Menu";
import { ErrorState, SkeletonRows } from "@/ui/panel";
import { CellView, type CellHandlers } from "./Cell";
import type { RunMode } from "./CodeEditor";
import { KernelClient } from "./kernel";
import { parseNotebook, serializeNotebook, type NotebookDoc } from "./nbformat";
import { cellRunState, initialState, isBusy, reducer, type KernelInfo, type SaveState } from "./state";

const AUTOSAVE_IDLE_MS = 1000;
const AUTOSAVE_MAX_WAIT_MS = 6000;

interface Loaded {
  doc: NotebookDoc;
  etag: string | null;
}

export default function NotebookPage({ path }: { path: string }) {
  const [loaded, setLoaded] = useState<Loaded | null>(null);
  const [error, setError] = useState<{ message: string; missing: boolean } | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    const ctl = new AbortController();
    setError(null);
    api
      .readNotebook(path, ctl.signal)
      .then(({ nb, etag }) => setLoaded({ doc: parseNotebook(nb), etag }))
      .catch((err: unknown) => {
        if (ctl.signal.aborted) return;
        setError({ message: err instanceof Error ? err.message : String(err), missing: err instanceof ApiError && err.status === 404 });
      });
    return () => ctl.abort();
  }, [path, attempt]);

  if (error)
    return (
      <div className="mx-auto max-w-3xl px-4 pt-8 sm:px-6">
        <h1 className="text-xl font-semibold">{error.missing ? "This notebook does not exist" : "This notebook could not be opened"}</h1>
        <p className="mt-1 font-mono text-sm text-fg-2">{path}</p>
        {!error.missing && <ErrorState className="px-0" message={error.message} onRetry={() => setAttempt((a) => a + 1)} />}
        <p className="mt-3 text-sm">
          <Link href="/notebooks" className="text-accent-fg underline">
            Back to all notebooks
          </Link>
        </p>
      </div>
    );
  if (!loaded) return <SkeletonRows rows={6} className="mx-auto max-w-4xl pt-10" />;
  return <Notebook path={path} loaded={loaded} />;
}

function KernelPill({ kernel }: { kernel: KernelInfo }) {
  const offline = kernel.link !== "open";
  const state = offline ? (kernel.link === "connecting" && kernel.epoch === 0 ? "connecting" : "reconnecting") : kernel.state === "unknown" ? "starting" : kernel.state;
  const dot = state === "idle" ? "bg-good" : state === "busy" ? "bg-accent" : state === "dead" ? "bg-critical" : "bg-warn";
  return (
    <span className="inline-flex h-6 shrink-0 items-center gap-1.5 rounded-full border border-line bg-surface px-2 text-xs" role="status" aria-label={`Kernel ${state}`}>
      <span aria-hidden className={cn("size-1.5 rounded-full", dot, state !== "idle" && state !== "dead" && "animate-[pulse-dot_1.2s_ease-in-out_infinite]")} />
      <span className="font-medium">{state}</span>
      {kernel.python && <span className="num text-fg-3 max-sm:hidden">Python {kernel.python}</span>}
    </span>
  );
}

function SaveMark({ save, onSave }: { save: SaveState; onSave: () => void }) {
  if (save.status === "error")
    return (
      <button type="button" onClick={onSave} className="inline-flex items-center gap-1 text-xs text-critical-fg underline" title={save.error ?? undefined}>
        <CircleAlert className="size-3" aria-hidden />
        Save failed. Retry
      </button>
    );
  const text = save.status === "saving" ? "Saving" : save.status === "dirty" ? "Unsaved changes" : save.status === "conflict" ? "Not saving: file changed on disk" : save.savedAt ? `Saved ${timeSec(save.savedAt)}` : "Saved";
  return (
    <span className={cn("num text-xs whitespace-nowrap", save.status === "conflict" ? "text-warn-fg" : "text-fg-3")} role="status">
      {text}
    </span>
  );
}

function ClusterStrip({ kernel }: { kernel: KernelInfo }) {
  const overview = useOverview();
  const down = overview.data ? !overview.data.daemon.reachable : false;
  const nodes = whenDaemonDown(usePoll("nodes", api.nodes, 5000), down, []);
  const healthy = (nodes.data ?? []).filter((n) => n.state === "healthy");
  const freeCpus = healthy.reduce((a, n) => a + n.load.total_cpus * (1 - n.load.cpu_percent / 100), 0);
  const totalCpus = healthy.reduce((a, n) => a + n.load.total_cpus, 0);
  const freeMem = healthy.reduce((a, n) => a + (n.load.available_mem_bytes ?? 0), 0);

  return (
    <div className="flex flex-wrap items-center gap-x-5 gap-y-1 border-b border-line bg-sunken px-4 py-1.5 text-xs sm:px-6">
      {kernel.interception === null ? (
        <span className="text-fg-3">Waiting for the kernel to report how cells run</span>
      ) : kernel.interception ? (
        <span className="inline-flex items-center gap-1.5 text-fg-2">
          <Network className="size-3.5 text-accent-fg" aria-hidden />
          <span>
            <span className="font-medium text-fg">Interception on.</span> Parallel work in a cell fans out to the cluster; the rest runs here.
          </span>
        </span>
      ) : (
        <span className="inline-flex items-center gap-1.5 text-fg-2">
          <Laptop className="size-3.5 text-warn-fg" aria-hidden />
          <span>
            <span className="font-medium text-fg">Interception off.</span> Cells run on this machine only.
          </span>
        </span>
      )}
      <Link href="/nodes" className="num ml-auto inline-flex flex-wrap items-center gap-x-3 rounded-sm text-fg-2 hover:text-fg max-sm:ml-0">
        {down ? (
          <span className="inline-flex items-center gap-1.5">
            <span aria-hidden className="size-1.5 rounded-full bg-critical" />
            Daemon not running. Start it with <code className="font-mono text-fg">pipedpeer start</code> to reach the cluster.
          </span>
        ) : nodes.data ? (
          <>
            <span className="inline-flex items-center gap-1.5">
              <span aria-hidden className={cn("size-1.5 rounded-full", healthy.length ? "bg-good" : "bg-warn")} />
              {healthy.length} of {nodes.data.length} nodes healthy
            </span>
            <span>
              {Math.round(freeCpus)} of {totalCpus} CPUs free
            </span>
            <span>{gb(freeMem)} GB memory free</span>
          </>
        ) : (
          <span className="text-fg-3">{nodes.error ? "Cluster status unavailable" : "Reading cluster status"}</span>
        )}
      </Link>
    </div>
  );
}

function VarsPanel({ vars, kernel, onRefresh, onClose }: { vars: KernelVar[] | null; kernel: KernelInfo; onRefresh: () => void; onClose: () => void }) {
  return (
    <aside aria-label="Variables" className="flex min-h-0 flex-col border-line bg-surface max-xl:fixed max-xl:inset-y-0 max-xl:right-0 max-xl:z-40 max-xl:w-[min(22rem,100vw)] max-xl:border-l max-xl:shadow-pop xl:sticky xl:top-0 xl:h-dvh xl:border-l">
      <div className="flex h-11 shrink-0 items-center justify-between border-b border-line pr-1.5 pl-4">
        <h2 className="text-sm font-semibold">
          Variables {vars && vars.length > 0 && <span className="num ml-1 text-xs font-normal text-fg-3">{vars.length}</span>}
        </h2>
        <div className="flex items-center">
          <IconButton size="sm" label="Refresh variables" onClick={onRefresh} disabled={kernel.link !== "open"}>
            <RefreshCw className="size-3.5" aria-hidden />
          </IconButton>
          <IconButton size="sm" label="Close variables" onClick={onClose}>
            <X className="size-3.5" aria-hidden />
          </IconButton>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto">
        {!vars ? (
          <p className="px-4 py-4 text-sm text-fg-3">{kernel.link === "open" ? "Reading the kernel's variables." : "Variables appear once the kernel is connected."}</p>
        ) : vars.length === 0 ? (
          <p className="px-4 py-4 text-sm text-fg-3">No variables yet. Run a cell that assigns one and it is listed here with its type and size. Cluster-job cells never add to this list.</p>
        ) : (
          <ul className="divide-y divide-line">
            {vars.map((v) => (
              <li key={v.name} className="px-4 py-2">
                <div className="flex items-baseline justify-between gap-2">
                  <span className="truncate font-mono text-sm font-medium">{v.name}</span>
                  <span className="shrink-0 text-xs text-fg-3">{v.type}</span>
                </div>
                <div className="flex items-baseline justify-between gap-2">
                  <span className="truncate font-mono text-xs text-fg-2" title={v.repr}>
                    {v.repr}
                  </span>
                  {v.size_bytes > 0 && <span className="num shrink-0 text-xs text-fg-3">{bytes(v.size_bytes)}</span>}
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </aside>
  );
}

const SHORTCUTS: { title: string; rows: [string[], string][] }[] = [
  {
    title: "Run",
    rows: [
      [["Shift", "Enter"], "Run the cell and move to the next"],
      [["Ctrl", "Enter"], "Run the cell and stay"],
      [["Alt", "Enter"], "Run the cell and insert one below"],
      [["Ctrl", "S"], "Save now"],
    ],
  },
  {
    title: "Modes",
    rows: [
      [["Esc"], "Command mode: keys act on cells"],
      [["Enter"], "Edit mode: type in the cell"],
    ],
  },
  {
    title: "Command mode",
    rows: [
      [["A"], "Insert a cell above"],
      [["B"], "Insert a cell below"],
      [["D", "D"], "Delete the cell"],
      [["Z"], "Undo the last delete"],
      [["M"], "Make it a Markdown cell"],
      [["Y"], "Make it a code cell"],
      [["J"], "Next cell (also ↓)"],
      [["K"], "Previous cell (also ↑)"],
      [["I", "I"], "Interrupt the kernel"],
      [["?"], "Show this sheet"],
    ],
  },
  {
    title: "In the editor",
    rows: [
      [["Tab"], "Complete from the kernel, or indent"],
      [["Ctrl", "F"], "Find in this cell"],
      [["Ctrl", "Z"], "Undo typing"],
    ],
  },
];

function ShortcutSheet({ open, onClose }: { open: boolean; onClose: () => void }) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const d = ref.current;
    if (!d) return;
    if (open && !d.open) d.showModal();
    if (!open && d.open) d.close();
  }, [open]);
  return (
    <dialog ref={ref} onClose={onClose} onClick={(e) => e.target === e.currentTarget && onClose()} aria-labelledby="shortcuts-title" className="m-auto w-[min(44rem,calc(100vw-2rem))] rounded-lg border border-line-strong bg-raised p-0 shadow-pop">
      <div className="flex items-center justify-between border-b border-line py-2 pr-2 pl-5">
        <h2 id="shortcuts-title" className="text-lg font-semibold">
          Keyboard shortcuts
        </h2>
        <IconButton label="Close" onClick={onClose}>
          <X className="size-4" aria-hidden />
        </IconButton>
      </div>
      <div className="grid max-h-[70vh] grid-cols-1 gap-x-8 gap-y-5 overflow-y-auto p-5 sm:grid-cols-2">
        {SHORTCUTS.map((g) => (
          <section key={g.title}>
            <h3 className="pb-1.5 text-sm font-semibold">{g.title}</h3>
            <dl className="flex flex-col gap-1.5">
              {g.rows.map(([keys, what]) => (
                <div key={what} className="flex items-center justify-between gap-3 text-sm">
                  <dd className="text-fg-2">{what}</dd>
                  <dt className="flex shrink-0 gap-1">
                    {keys.map((k, i) => (
                      <Kbd key={i}>{k}</Kbd>
                    ))}
                  </dt>
                </div>
              ))}
            </dl>
          </section>
        ))}
      </div>
      <p className="border-t border-line px-5 py-2.5 text-xs text-fg-3">On macOS, Ctrl is Cmd for run, save, find and undo.</p>
    </dialog>
  );
}

function Notebook({ path, loaded }: { path: string; loaded: Loaded }) {
  const [state, dispatch] = useReducer(reducer, loaded, (l) => initialState(l.doc, l.etag, Date.now()));
  const live = useRef(state);
  live.current = state;
  const client = useRef<KernelClient | null>(null);
  const [varsOpen, setVarsOpen] = useState(() => matchMedia("(min-width: 80rem)").matches);
  const [sheet, setSheet] = useState(false);
  const [drag, setDrag] = useState<{ id: string; over: string | null; after: boolean } | null>(null);
  const dragRef = useRef(drag);
  dragRef.current = drag;
  const chord = useRef<{ key: string; at: number } | null>(null);

  const refreshVars = useCallback(() => {
    void client.current?.inspectVars().then((vars) => vars && dispatch({ type: "vars", vars }));
  }, []);

  useEffect(() => {
    const c = new KernelClient(
      path,
      (frame) => {
        dispatch({ type: "frame", frame, at: Date.now() });
        if (frame.type === "done" || (frame.type === "status" && frame.state === "idle" && live.current.vars === null)) refreshVars();
      },
      (link) => dispatch({ type: "link", link }),
    );
    client.current = c;
    c.connect();
    return () => {
      c.close();
      client.current = null;
    };
  }, [path, refreshVars]);

  const head = state.queue[0];
  const canDispatch = state.kernel.link === "open" && state.kernel.state === "idle" && !state.running && !state.restart;
  useEffect(() => {
    if (!head || !canDispatch) return;
    if (client.current?.send({ type: "execute", cell_id: head.cellId, code: head.code, target: head.target })) dispatch({ type: "dispatched", cellId: head.cellId, at: Date.now() });
  }, [head, canDispatch]);

  const saving = useRef(false);
  const save = useCallback(async () => {
    const s = live.current;
    if (saving.current || s.save.status === "conflict" || s.rev === s.save.savedRev) return;
    saving.current = true;
    dispatch({ type: "saveStart" });
    const rev = s.rev;
    try {
      const { etag } = await api.writeNotebook(path, serializeNotebook(s.doc), s.save.etag);
      dispatch({ type: "saveOk", etag, rev, at: Date.now() });
    } catch (err) {
      if (err instanceof ApiError && err.status === 412) dispatch({ type: "saveConflict" });
      else dispatch({ type: "saveError", message: err instanceof Error ? err.message : String(err) });
    } finally {
      saving.current = false;
    }
  }, [path]);

  const firstDirtyAt = useRef<number | null>(null);
  useEffect(() => {
    if (state.save.status !== "dirty") {
      if (state.save.status === "clean") firstDirtyAt.current = null;
      return;
    }
    firstDirtyAt.current ??= Date.now();
    const wait = Math.max(0, Math.min(AUTOSAVE_IDLE_MS, firstDirtyAt.current + AUTOSAVE_MAX_WAIT_MS - Date.now()));
    const id = setTimeout(() => {
      firstDirtyAt.current = null;
      void save();
    }, wait);
    return () => clearTimeout(id);
  }, [state.rev, state.save.status, save]);

  useEffect(() => {
    const flush = () => {
      const s = live.current;
      if (s.rev === s.save.savedRev || s.save.status === "conflict") return;
      void fetch(`/api/notebooks/content?path=${encodeURIComponent(path)}`, {
        method: "PUT",
        keepalive: true,
        headers: { "content-type": "application/json", ...(s.save.etag ? { "If-Match": s.save.etag } : {}) },
        body: JSON.stringify(serializeNotebook(s.doc)),
      }).catch(() => undefined);
    };
    addEventListener("pagehide", flush);
    return () => removeEventListener("pagehide", flush);
  }, [path]);

  const reloadFromDisk = useCallback(async () => {
    try {
      const { nb, etag } = await api.readNotebook(path);
      dispatch({ type: "reloaded", doc: parseNotebook(nb), etag, at: Date.now() });
    } catch (err) {
      dispatch({ type: "saveError", message: err instanceof Error ? err.message : String(err) });
    }
  }, [path]);

  const saveCopy = useCallback(async () => {
    const copy = path.replace(/\.ipynb$/, `-copy-${new Date().toISOString().slice(11, 19).replace(/:/g, "")}.ipynb`);
    try {
      await api.createNotebook(copy);
      await api.writeNotebook(copy, serializeNotebook(live.current.doc), null);
      await reloadFromDisk();
    } catch (err) {
      dispatch({ type: "saveError", message: err instanceof Error ? err.message : String(err) });
    }
  }, [path, reloadFromDisk]);

  const cellIds = useMemo(() => state.doc.cells.map((c) => c.id), [state.doc.cells]);

  const interrupt = useCallback(() => {
    client.current?.send({ type: "interrupt" });
    dispatch({ type: "interrupt" });
  }, []);

  const restart = useCallback((thenRun: boolean) => {
    const s = live.current;
    if (!client.current?.send({ type: "restart" })) return;
    dispatch({ type: "restart", thenRun: thenRun ? s.doc.cells.filter((c) => c.type === "code").map((c) => c.id) : null });
  }, []);

  const handlers = useMemo<CellHandlers>(
    () => ({
      run: (id: string, mode: RunMode) => {
        const s = live.current;
        const index = s.doc.cells.findIndex((c) => c.id === id);
        const cell = s.doc.cells[index];
        if (!cell) return;
        if (cell.type === "code") dispatch({ type: "enqueue", ids: [id] });
        if (mode === "stay") dispatch({ type: "focus", id, mode: "command" });
        else if (mode === "insert") dispatch({ type: "insert", index: index + 1, cellType: "code", edit: true });
        else {
          const next = s.doc.cells[index + 1];
          if (next) dispatch({ type: "focus", id: next.id, mode: "command" });
          else dispatch({ type: "insert", index: index + 1, cellType: "code", edit: true });
        }
      },
      interrupt,
      leave: (id, direction) => {
        const s = live.current;
        const next = s.doc.cells[s.doc.cells.findIndex((c) => c.id === id) + direction];
        if (next) dispatch({ type: "focus", id: next.id, mode: next.type === "markdown" ? "command" : "edit" });
      },
      complete: (code, cursor) => client.current?.complete(code, cursor) ?? Promise.resolve(null),
      dragStart: (id) => setDrag({ id, over: null, after: false }),
      dragOver: (id, after) => {
        const d = dragRef.current;
        if (d && (d.over !== id || d.after !== after)) setDrag({ ...d, over: id, after });
      },
      drop: () => {
        const d = dragRef.current;
        setDrag(null);
        if (!d || !d.over || d.over === d.id) return;
        const cells = live.current.doc.cells;
        const from = cells.findIndex((c) => c.id === d.id);
        let to = cells.findIndex((c) => c.id === d.over) + (d.after ? 1 : 0);
        if (from < to) to -= 1;
        dispatch({ type: "move", id: d.id, index: to });
      },
    }),
    [interrupt],
  );

  const focusIndex = state.focus ? cellIds.indexOf(state.focus) : -1;

  const root = useRef<HTMLDivElement>(null);

  // Command-mode keys are handled for the whole document, not only while a cell holds DOM focus:
  // a button that unmounts after its click (clear output, delete) drops focus to <body>.
  useEffect(() => {
    const onKey = (e: globalThis.KeyboardEvent) => {
      const target = e.target as HTMLElement;
      const typing = !!target.closest?.(".cm-editor, input, textarea, select");
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "s") {
        e.preventDefault();
        void save();
        return;
      }
      if (typing || target.closest?.("dialog, [role=menu]")) return;
      if (target !== document.body && !root.current?.contains(target)) return;
      if (e.key === "?") {
        e.preventDefault();
        setSheet(true);
        return;
      }
      const s = live.current;
      const ids = s.doc.cells.map((c) => c.id);
      const id = s.focus;
      const at = id ? ids.indexOf(id) : -1;
      if (e.metaKey || e.ctrlKey) {
        if (e.key === "Enter" && id) (e.preventDefault(), handlers.run(id, "stay"));
        return;
      }
      const move = (delta: number) => {
        const next = ids[Math.min(ids.length - 1, Math.max(0, at + delta))];
        if (next) dispatch({ type: "focus", id: next, mode: "command" });
      };
      const now = Date.now();
      const doubled = (key: string) => {
        const hit = chord.current?.key === key && now - chord.current.at < 600;
        chord.current = hit ? null : { key, at: now };
        return hit;
      };
      const key = e.key.length === 1 ? e.key.toLowerCase() : e.key;
      if (key !== "d" && key !== "i") chord.current = null;
      switch (key) {
        case "Enter":
          if (!id) return;
          if (e.shiftKey) handlers.run(id, "advance");
          else if (e.altKey) handlers.run(id, "insert");
          else if (target.closest("button, a, summary")) return;
          else dispatch({ type: "focus", id, mode: "edit" });
          break;
        case "ArrowDown":
        case "j":
          move(1);
          break;
        case "ArrowUp":
        case "k":
          move(-1);
          break;
        case "a":
          dispatch({ type: "insert", index: Math.max(0, at), cellType: "code" });
          break;
        case "b":
          dispatch({ type: "insert", index: at + 1, cellType: "code" });
          break;
        case "d":
          if (id && doubled("d")) dispatch({ type: "delete", id });
          break;
        case "i":
          if (doubled("i")) interrupt();
          break;
        case "z":
          dispatch({ type: "undoDelete" });
          break;
        case "m":
          if (id) dispatch({ type: "setType", id, cellType: "markdown" });
          break;
        case "y":
          if (id) dispatch({ type: "setType", id, cellType: "code" });
          break;
        default:
          return;
      }
      e.preventDefault();
    };
    addEventListener("keydown", onKey);
    return () => removeEventListener("keydown", onKey);
  }, [save, handlers, interrupt]);

  const codeIds = useMemo(() => state.doc.cells.filter((c) => c.type === "code").map((c) => c.id), [state.doc.cells]);
  const codeAbove = focusIndex >= 0 ? state.doc.cells.slice(0, focusIndex).filter((c) => c.type === "code").map((c) => c.id) : [];
  const codeBelow = focusIndex >= 0 ? state.doc.cells.slice(focusIndex).filter((c) => c.type === "code").map((c) => c.id) : [];
  const busy = isBusy(state);
  const connected = state.kernel.link === "open";
  const insertAt = focusIndex >= 0 ? focusIndex + 1 : state.doc.cells.length;
  const dir = path.includes("/") ? path.slice(0, path.lastIndexOf("/") + 1) : "";
  const name = path.slice(dir.length);
  const sep = <span aria-hidden className="mx-1 h-4 w-px shrink-0 bg-line" />;

  return (
    <div className={cn("grid min-h-dvh grid-cols-1", varsOpen && "xl:grid-cols-[minmax(0,1fr)_19rem]")}>
      <div className="min-w-0">
        <div className="sticky top-0 z-20 border-b border-line bg-bg">
          <div className="flex h-11 items-center gap-3 px-4 sm:px-6">
            <nav aria-label="Breadcrumb" className="flex min-w-0 items-center gap-1 text-sm">
              <Link href="/notebooks" className="shrink-0 rounded-sm text-fg-3 hover:text-fg hover:underline">
                Notebooks
              </Link>
              <ChevronRight className="size-3.5 shrink-0 text-fg-3" aria-hidden />
              <h1 className="truncate font-medium" title={path}>
                {dir && <span className="font-normal text-fg-3">{dir}</span>}
                {name}
              </h1>
            </nav>
            <SaveMark save={state.save} onSave={() => void save()} />
            <span className="ml-auto">
              <KernelPill kernel={state.kernel} />
            </span>
          </div>
          <div role="toolbar" aria-label="Notebook" className="relative flex h-10 items-center gap-0.5 overflow-x-auto border-t border-line px-3 sm:px-5">
            <Button size="sm" variant="ghost" icon={<Plus className="size-3.5" aria-hidden />} onClick={() => dispatch({ type: "insert", index: insertAt, cellType: "code", edit: true })}>
              Code
            </Button>
            <Button size="sm" variant="ghost" icon={<Type className="size-3.5" aria-hidden />} onClick={() => dispatch({ type: "insert", index: insertAt, cellType: "markdown", edit: true })}>
              Text
            </Button>
            {sep}
            <Button size="sm" variant="ghost" icon={<Play className="size-3.5" aria-hidden />} disabled={!codeIds.length} onClick={() => dispatch({ type: "enqueue", ids: codeIds })}>
              Run all
            </Button>
            <Menu
              label="Run"
              items={[
                { label: "Run all above", hint: "Every code cell before the selected one", disabled: !codeAbove.length, onSelect: () => dispatch({ type: "enqueue", ids: codeAbove }) },
                { label: "Run selected and below", hint: "The selected cell and every one after it", disabled: !codeBelow.length, onSelect: () => dispatch({ type: "enqueue", ids: codeBelow }) },
              ]}
            />
            {sep}
            <Button size="sm" variant="ghost" icon={<Square className="size-3 fill-current" aria-hidden />} disabled={!busy || !connected} onClick={interrupt}>
              Interrupt
            </Button>
            <Menu
              label="Restart"
              icon={<RotateCw className="size-3.5" aria-hidden />}
              items={[
                { label: "Restart kernel", hint: "All variables are lost. Outputs stay.", icon: <RotateCw className="size-3.5" aria-hidden />, disabled: !connected, onSelect: () => restart(false) },
                { label: "Restart and run all", hint: "Fresh kernel, then every cell top to bottom", icon: <FastForward className="size-3.5" aria-hidden />, disabled: !connected || !codeIds.length, onSelect: () => restart(true) },
              ]}
            />
            {sep}
            <Button size="sm" variant="ghost" icon={<Eraser className="size-3.5" aria-hidden />} onClick={() => dispatch({ type: "clearAllOutputs" })}>
              <span className="max-sm:sr-only">Clear outputs</span>
            </Button>
            <span className="ml-auto flex items-center gap-0.5 pl-2">
              <IconButton size="sm" label={varsOpen ? "Hide variables" : "Show variables"} active={varsOpen} onClick={() => setVarsOpen((v) => !v)}>
                <PanelRight className="size-3.5" aria-hidden />
              </IconButton>
              <IconButton size="sm" label="Keyboard shortcuts (?)" onClick={() => setSheet(true)}>
                <Keyboard className="size-3.5" aria-hidden />
              </IconButton>
            </span>
          </div>
        </div>

        <ClusterStrip kernel={state.kernel} />

        {state.save.status === "conflict" && (
          <div role="alert" className="flex flex-wrap items-center gap-x-4 gap-y-2 border-b border-warn/50 bg-warn/10 px-4 py-2.5 sm:px-6">
            <div className="min-w-0 flex-1 basis-72">
              <p className="text-sm font-medium">This notebook changed on disk after you opened it</p>
              <p className="text-sm text-fg-2">Autosave is paused so neither version is lost. Nothing happens until you choose.</p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button size="sm" variant="solid" onClick={() => void saveCopy()}>
                Save mine as a copy
              </Button>
              <Button size="sm" onClick={() => void reloadFromDisk()}>
                Load the version on disk
              </Button>
              <Button size="sm" variant="danger" onClick={() => dispatch({ type: "overwrite" })}>
                Overwrite with mine
              </Button>
            </div>
          </div>
        )}

        {(state.kernel.fatal || (connected && state.kernel.state === "dead")) && (
          <div role="alert" className="flex flex-wrap items-center gap-x-4 gap-y-2 border-b border-critical/40 bg-critical/8 px-4 py-2.5 sm:px-6">
            <CircleAlert className="size-4 shrink-0 text-critical-fg" aria-hidden />
            <div className="min-w-0 flex-1 basis-64">
              <p className="text-sm font-medium">{state.kernel.fatal ? "The kernel could not start" : "The kernel stopped"}</p>
              <p className="font-mono text-xs break-words text-fg-2">{state.kernel.fatal ?? "Its variables are gone. Restart it to run cells again; your notebook and outputs are untouched."}</p>
            </div>
            <Button size="sm" onClick={() => restart(false)} icon={<RotateCw className="size-3.5" aria-hidden />}>
              Restart kernel
            </Button>
          </div>
        )}

        <div ref={root} className="mx-auto max-w-[58rem] px-2 pt-4 pb-40 sm:px-5" data-mode={state.mode}>
          {state.doc.cells.map((cell, index) => (
            <CellView
              key={cell.id}
              cell={cell}
              index={index}
              count={state.doc.cells.length}
              run={cellRunState(state, cell.id)}
              fanout={state.fanout[cell.id]}
              focused={state.focus === cell.id}
              editing={state.focus === cell.id && state.mode === "edit"}
              sourceEpoch={state.sourceEpoch}
              dropMark={drag?.over === cell.id && drag.id !== cell.id ? (drag.after ? "after" : "before") : null}
              dispatch={dispatch}
              handlers={handlers}
            />
          ))}
          {state.doc.cells.length === 0 ? (
            <div className="mx-auto mt-10 flex max-w-md flex-col items-start gap-3 px-3">
              <h2 className="text-lg font-semibold">An empty notebook</h2>
              <p className="text-fg-2">Add a code cell and write ordinary Python. There is nothing to import: when a cell uses multiprocessing or joblib, Pipedpeer spreads that work over the cluster for you.</p>
              <div className="flex gap-2">
                <Button variant="solid" icon={<Plus className="size-3.5" aria-hidden />} onClick={() => dispatch({ type: "insert", index: 0, cellType: "code", edit: true })}>
                  Code cell
                </Button>
                <Button icon={<Type className="size-3.5" aria-hidden />} onClick={() => dispatch({ type: "insert", index: 0, cellType: "markdown", edit: true })}>
                  Text cell
                </Button>
              </div>
            </div>
          ) : (
            <div className="flex gap-2 pt-3 pl-10">
              <Button size="sm" icon={<Plus className="size-3.5" aria-hidden />} onClick={() => dispatch({ type: "insert", index: state.doc.cells.length, cellType: "code", edit: true })}>
                Code
              </Button>
              <Button size="sm" icon={<Type className="size-3.5" aria-hidden />} onClick={() => dispatch({ type: "insert", index: state.doc.cells.length, cellType: "markdown", edit: true })}>
                Text
              </Button>
              {state.deleted.length > 0 && (
                <Button size="sm" variant="ghost" onClick={() => dispatch({ type: "undoDelete" })}>
                  Undo delete
                </Button>
              )}
            </div>
          )}
        </div>
      </div>

      {varsOpen && <VarsPanel vars={state.vars} kernel={state.kernel} onRefresh={refreshVars} onClose={() => setVarsOpen(false)} />}
      <ShortcutSheet open={sheet} onClose={() => setSheet(false)} />
    </div>
  );
}
