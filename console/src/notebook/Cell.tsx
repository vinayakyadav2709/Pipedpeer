import { ArrowDown, ArrowUp, Ban, Check, CircleAlert, Clock, Eraser, GripVertical, Laptop, LoaderCircle, Network, Play, Square, Trash2, Unplug, X } from "lucide-react";
import { memo, useEffect, useMemo, useRef, useState, type Dispatch, type DragEvent } from "react";
import { clock, duration } from "@/lib/format";
import type { RunTarget } from "@/lib/types";
import { cn } from "@/ui/cn";
import { IconButton } from "@/ui/controls";
import { CodeEditor, type RunMode } from "./CodeEditor";
import type { Completion } from "./kernel";
import { renderMarkdown } from "./markdown";
import type { Cell as CellModel } from "./nbformat";
import { OutputArea } from "./outputs";
import type { Action, CellRunState, Fanout } from "./state";

export interface CellHandlers {
  run: (id: string, mode: RunMode) => void;
  interrupt: () => void;
  leave: (id: string, direction: -1 | 1) => void;
  complete: (code: string, cursor: number) => Promise<Completion | null>;
  dragStart: (id: string) => void;
  dragOver: (id: string, after: boolean) => void;
  drop: () => void;
}

export interface CellProps {
  cell: CellModel;
  index: number;
  count: number;
  run: CellRunState;
  fanout: Fanout | undefined;
  focused: boolean;
  editing: boolean;
  sourceEpoch: number;
  dropMark: "before" | "after" | null;
  dispatch: Dispatch<Action>;
  handlers: CellHandlers;
}

let lastPointerDownAt = 0;
addEventListener("pointerdown", () => (lastPointerDownAt = Date.now()), true);

function Timer({ startedAt }: { startedAt: number }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 100);
    return () => clearInterval(id);
  }, []);
  return <span className="num">{clock(Math.max(0, now - startedAt))}</span>;
}

function RunButton({ run, onRun, onStop, disabled }: { run: CellRunState; onRun: () => void; onStop: () => void; disabled: boolean }) {
  if (run.kind === "running")
    return (
      <button type="button" aria-label="Interrupt this cell" title="Interrupt (kernel menu, or I I)" onClick={onStop} className="relative inline-flex size-7 items-center justify-center rounded-full bg-accent-soft text-accent-fg hover:bg-accent hover:text-white">
        <LoaderCircle className="absolute inset-0 size-7 animate-[spin_1.1s_linear_infinite] opacity-70" strokeWidth={1.5} aria-hidden />
        <Square className="size-2.5 fill-current" aria-hidden />
      </button>
    );
  if (run.kind === "queued")
    return (
      <span role="img" aria-label="Queued" title="Queued" className="inline-flex size-7 items-center justify-center rounded-full border border-dashed border-line-strong text-fg-3">
        <Clock className="size-3.5" aria-hidden />
      </span>
    );
  return (
    <button type="button" aria-label="Run this cell" title="Run (Ctrl+Enter)" disabled={disabled} onClick={onRun} className="inline-flex size-7 items-center justify-center rounded-full border border-line-strong bg-surface text-fg-2 transition-colors duration-150 hover:border-fg hover:bg-solid hover:text-solid-fg disabled:opacity-40">
      <Play className="ml-px size-3 fill-current" aria-hidden />
    </button>
  );
}

function TargetSwitch({ value, onChange }: { value: RunTarget; onChange: (t: RunTarget) => void }) {
  const opt = (t: RunTarget, label: string, Icon: typeof Laptop, title: string) => (
    <button
      type="button"
      role="radio"
      aria-checked={value === t}
      title={title}
      onClick={() => onChange(t)}
      className={cn("inline-flex h-6 items-center gap-1 rounded-[4px] px-1.5 text-xs font-medium whitespace-nowrap transition-colors duration-150", value === t ? "bg-surface text-fg shadow-[0_0_0_1px_var(--line)]" : "text-fg-3 hover:text-fg")}
    >
      <Icon className="size-3" aria-hidden />
      {label}
    </button>
  );
  return (
    <div role="radiogroup" aria-label="Where this cell runs" className="inline-flex rounded-md bg-sunken p-0.5">
      {opt("local", "Kernel", Laptop, "Run in the notebook kernel on this machine. Variables are shared with other cells.")}
      {opt("cluster", "Cluster job", Network, "Submit this cell as a standalone Pipedpeer job. It does not share kernel state.")}
    </div>
  );
}

function StatusLine({ cell, run, fanout }: { cell: CellModel; run: CellRunState; fanout: Fanout | undefined }) {
  const cluster = (run.kind === "running" ? run.target : cell.target) === "cluster";
  let status: React.ReactNode = null;
  if (run.kind === "running")
    status = (
      <span className="inline-flex items-center gap-1.5 text-accent-fg">
        <LoaderCircle className="size-3 animate-[spin_1.1s_linear_infinite]" aria-hidden />
        {cluster ? "Running as a cluster job" : "Running"} <Timer startedAt={run.startedAt} />
      </span>
    );
  else if (run.kind === "queued") status = <span>Queued</span>;
  else if (run.kind === "done" && run.status === "error")
    status = (
      <span className="inline-flex items-center gap-1 text-critical-fg">
        <X className="size-3" strokeWidth={2.5} aria-hidden />
        Failed{cell.durationMs !== null ? ` after ${duration(cell.durationMs)}` : ""}
      </span>
    );
  else if (run.kind === "done" && run.status === "aborted")
    status = (
      <span className="inline-flex items-center gap-1">
        <Ban className="size-3" aria-hidden />
        Stopped by a kernel restart
      </span>
    );
  else if (run.kind === "done" && run.status === "skipped")
    status = (
      <span className="inline-flex items-center gap-1">
        <Ban className="size-3" aria-hidden />
        Not run: an earlier cell failed or was interrupted
      </span>
    );
  else if (run.kind === "done" && run.status === "lost")
    status = (
      <span className="inline-flex items-center gap-1 text-warn-fg">
        <Unplug className="size-3" aria-hidden />
        The connection dropped during this run, so its output may be incomplete
      </span>
    );
  else if (cell.durationMs !== null)
    status = (
      <span className="inline-flex items-center gap-1">
        <Check className={cn("size-3", run.kind === "done" && "text-good-fg")} strokeWidth={2.5} aria-hidden />
        <span className="num">{duration(cell.durationMs)}</span>
      </span>
    );

  if (!status && !cluster && !fanout) return null;
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-0.5 pt-1.5 text-xs text-fg-3">
      {status}
      {fanout && (
        <span className="inline-flex flex-wrap items-center gap-1.5">
          <Network className="size-3" aria-hidden />
          Fanned out
          {fanout.nodes.map((n) => (
            <span key={n.hostname} className="num rounded-sm bg-sunken px-1.5 text-fg-2">
              {n.hostname} <span className="text-fg-3">{n.chunks} chunks</span>
            </span>
          ))}
          {fanout.localChunks > 0 && (
            <span className="num rounded-sm bg-sunken px-1.5 text-fg-2">
              this machine <span className="text-fg-3">{fanout.localChunks} chunks</span>
            </span>
          )}
        </span>
      )}
      {cluster && run.kind !== "running" && (
        <span className="inline-flex items-center gap-1">
          <CircleAlert className="size-3" aria-hidden />
          Cluster job: runs on its own, without the variables from other cells
        </span>
      )}
    </div>
  );
}

export const CellView = memo(function CellView({ cell, index, count, run, fanout, focused, editing, sourceEpoch, dropMark, dispatch, handlers }: CellProps) {
  const root = useRef<HTMLDivElement>(null);
  const isCode = cell.type === "code";
  const showEditor = cell.type !== "markdown" || editing;
  const html = useMemo(() => (cell.type === "markdown" && !showEditor ? renderMarkdown(cell.source) : ""), [cell.type, cell.source, showEditor]);
  const active = run.kind === "running" || run.kind === "queued";

  useEffect(() => {
    if (focused && !editing && root.current && !root.current.contains(document.activeElement)) root.current.focus({ preventScroll: true });
    // Scrolling under a pointer that is mid-click moves the target away and the click is lost.
    if (focused && Date.now() - lastPointerDownAt > 400) root.current?.scrollIntoView({ block: "nearest" });
  }, [focused, editing]);

  const onDragOver = (e: DragEvent) => {
    if (!e.dataTransfer.types.includes("application/x-pp-cell")) return;
    e.preventDefault();
    const r = e.currentTarget.getBoundingClientRect();
    handlers.dragOver(cell.id, e.clientY > r.top + r.height / 2);
  };

  return (
    <div
      ref={root}
      data-cell={cell.id}
      tabIndex={focused ? 0 : -1}
      aria-label={`${cell.type} cell ${index + 1} of ${count}`}
      onMouseDown={() => !focused && dispatch({ type: "focus", id: cell.id, mode: "command" })}
      onFocus={(e) => e.target === e.currentTarget && !focused && dispatch({ type: "focus", id: cell.id, mode: "command" })}
      onDragOver={onDragOver}
      onDrop={(e) => (e.preventDefault(), handlers.drop())}
      className="group/cell relative grid scroll-my-28 grid-cols-[2.25rem_minmax(0,1fr)] gap-x-1.5 rounded-md py-1.5 outline-none focus-visible:outline-none"
    >
      {dropMark && <div aria-hidden className={cn("absolute right-0 left-10 z-10 h-0.5 rounded-full bg-accent", dropMark === "before" ? "-top-px" : "-bottom-px")} />}

      <div className="flex flex-col items-center gap-0.5 pt-1">
        {isCode ? (
          <>
            <RunButton run={run} disabled={!cell.source.trim()} onRun={() => handlers.run(cell.id, "stay")} onStop={handlers.interrupt} />
            <span className="num font-mono text-2xs text-fg-3" aria-label={cell.executionCount !== null ? `Execution count ${cell.executionCount}` : undefined}>
              {run.kind === "running" ? "[*]" : cell.executionCount !== null ? `[${cell.executionCount}]` : ""}
            </span>
          </>
        ) : (
          <span className="pt-1.5 font-mono text-2xs text-fg-3">{cell.type === "markdown" ? "md" : "raw"}</span>
        )}
      </div>

      <div className="relative min-w-0">
        <div
          className={cn(
            "rounded-md border transition-[border-color,box-shadow] duration-150",
            showEditor ? "bg-surface" : "bg-transparent",
            focused ? (editing ? "border-accent shadow-[0_0_0_3px_color-mix(in_oklab,var(--accent)_18%,transparent)]" : "border-fg-3") : showEditor ? "border-line group-hover/cell:border-line-strong" : "border-transparent group-hover/cell:border-line",
            run.kind === "running" && !focused && "border-accent/60",
          )}
        >
          {showEditor ? (
            <CodeEditor
              value={cell.source}
              epoch={sourceEpoch}
              language={isCode ? "python" : "text"}
              focused={focused && editing}
              ariaLabel={`${cell.type} cell ${index + 1} source`}
              placeholder={isCode ? (index === 0 && count === 1 ? "Write Python here, then press Shift+Enter to run it" : undefined) : "Markdown"}
              onChange={(source) => dispatch({ type: "source", id: cell.id, source })}
              onFocus={() => dispatch({ type: "focus", id: cell.id, mode: "edit" })}
              onRun={(mode) => handlers.run(cell.id, mode)}
              onEscape={() => dispatch({ type: "focus", id: cell.id, mode: "command" })}
              onLeave={(d) => handlers.leave(cell.id, d)}
              complete={isCode && cell.target === "local" ? handlers.complete : undefined}
            />
          ) : cell.source.trim() ? (
            <div className="prose px-3 py-2" onDoubleClick={() => dispatch({ type: "focus", id: cell.id, mode: "edit" })} dangerouslySetInnerHTML={{ __html: html }} />
          ) : (
            <button type="button" className="block w-full px-3 py-2 text-left text-sm text-fg-3" onDoubleClick={() => dispatch({ type: "focus", id: cell.id, mode: "edit" })} onClick={() => dispatch({ type: "focus", id: cell.id, mode: "edit" })}>
              Empty text cell. Click to write Markdown.
            </button>
          )}
        </div>

        <div
          className={cn(
            // Narrow screens: a fixed bar, so focusing a cell never shifts layout under the pointer or covers its code.
            "z-10 w-fit items-center gap-0.5 rounded-md border border-line-strong bg-raised p-0.5 shadow-pop max-sm:fixed max-sm:bottom-[max(0.75rem,env(safe-area-inset-bottom))] max-sm:left-1/2 max-sm:z-30 max-sm:-translate-x-1/2 sm:absolute sm:-top-3.5 sm:right-2",
            focused ? "flex" : "hidden sm:group-focus-within/cell:flex sm:group-hover/cell:flex",
          )}
        >
          {isCode && <TargetSwitch value={cell.target} onChange={(target) => dispatch({ type: "setTarget", id: cell.id, target })} />}
          <IconButton size="sm" label="Move cell up" disabled={index === 0} onClick={() => dispatch({ type: "move", id: cell.id, index: index - 1 })} className="size-6">
            <ArrowUp className="size-3.5" aria-hidden />
          </IconButton>
          <IconButton size="sm" label="Move cell down" disabled={index === count - 1} onClick={() => dispatch({ type: "move", id: cell.id, index: index + 1 })} className="size-6">
            <ArrowDown className="size-3.5" aria-hidden />
          </IconButton>
          <IconButton size="sm" label="Delete cell (D D)" disabled={run.kind === "running"} onClick={() => dispatch({ type: "delete", id: cell.id })} className="size-6">
            <Trash2 className="size-3.5" aria-hidden />
          </IconButton>
          <span
            draggable
            role="img"
            aria-label="Drag to reorder"
            title="Drag to reorder"
            onDragStart={(e) => {
              e.dataTransfer.setData("application/x-pp-cell", cell.id);
              e.dataTransfer.effectAllowed = "move";
              if (root.current) e.dataTransfer.setDragImage(root.current, 24, 16);
              handlers.dragStart(cell.id);
            }}
            onDragEnd={handlers.drop}
            className="inline-flex size-6 cursor-grab items-center justify-center rounded-md text-fg-3 hover:bg-hover hover:text-fg active:cursor-grabbing max-sm:hidden"
          >
            <GripVertical className="size-3.5" aria-hidden />
          </span>
        </div>

        {isCode && <StatusLine cell={cell} run={run} fanout={fanout} />}

        {isCode && cell.outputs.length > 0 && (
          <div className="pt-2 pl-0.5">
            <OutputArea
              outputs={cell.outputs}
              live={active}
              actions={
                <button type="button" onClick={() => dispatch({ type: "clearOutputs", id: cell.id })} className={cn("inline-flex h-6 items-center gap-1 rounded-sm px-1.5 text-xs text-fg-3 hover:bg-hover hover:text-fg", !focused && "opacity-0 group-hover/cell:opacity-100 focus-visible:opacity-100")}>
                  <Eraser className="size-3.5" aria-hidden />
                  Clear output
                </button>
              }
            />
          </div>
        )}
      </div>
    </div>
  );
});
