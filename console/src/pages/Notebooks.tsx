import { FilePlus2, NotebookText, Pencil, Trash2 } from "lucide-react";
import { useState, type FormEvent } from "react";
import { Page } from "@/App";
import { api } from "@/lib/api";
import { ago, bytes, dateTime, plural } from "@/lib/format";
import { useNow, usePoll } from "@/lib/poll";
import { Link, notebookHref, useRouter } from "@/lib/router";
import type { NotebookFile } from "@/lib/types";
import { Button, IconButton } from "@/ui/controls";
import { Async, EmptyState, PageHeader, SkeletonRows, StaleMark } from "@/ui/panel";

const withExt = (name: string) => (name.trim().endsWith(".ipynb") ? name.trim() : `${name.trim()}.ipynb`);
const inputCls = "h-8 min-w-0 rounded-md border border-line-strong bg-surface px-2.5 font-mono text-sm outline-none focus-visible:border-accent focus-visible:ring-2 focus-visible:ring-accent/30";

function FileRow({ file, now, onChanged }: { file: NotebookFile; now: number; onChanged: () => void }) {
  const [mode, setMode] = useState<"view" | "rename" | "delete">("view");
  const [name, setName] = useState(file.path);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const dir = file.path.includes("/") ? file.path.slice(0, file.path.lastIndexOf("/") + 1) : "";

  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    setError(null);
    try {
      await fn();
      setMode("view");
      onChanged();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const rename = (e: FormEvent) => {
    e.preventDefault();
    const to = withExt(name);
    if (to === file.path) return setMode("view");
    void act(() => api.renameNotebook(file.path, to));
  };

  return (
    <li className="group relative flex flex-wrap items-center gap-x-3 gap-y-1 px-4 py-2.5 hover:bg-sunken">
      <NotebookText className="size-4 shrink-0 text-fg-3" aria-hidden />
      {mode === "rename" ? (
        <form onSubmit={rename} className="relative z-10 flex min-w-0 flex-1 flex-wrap items-center gap-2">
          <label className="sr-only" htmlFor={`rename-${file.path}`}>
            New path for {file.path}
          </label>
          <input id={`rename-${file.path}`} autoFocus value={name} onChange={(e) => setName(e.target.value)} onKeyDown={(e) => e.key === "Escape" && setMode("view")} className={`${inputCls} flex-1 basis-48`} />
          <Button size="sm" variant="solid" type="submit" disabled={busy || !name.trim()}>
            Rename
          </Button>
          <Button size="sm" variant="ghost" onClick={() => setMode("view")}>
            Cancel
          </Button>
        </form>
      ) : (
        <div className="min-w-0 flex-1 basis-48">
          <Link href={notebookHref(file.path)} className="block truncate text-sm font-medium after:absolute after:inset-0">
            {dir && <span className="font-normal text-fg-3">{dir}</span>}
            {file.name}
          </Link>
        </div>
      )}
      {mode === "view" && (
        <>
          <span className="num text-xs text-fg-3" title={dateTime(file.modified)}>
            {ago(file.modified, now)}
          </span>
          <span className="num w-14 text-right text-xs text-fg-3 max-sm:hidden">{bytes(file.size)}</span>
          <span className="relative z-10 flex items-center gap-0.5">
            <IconButton size="sm" label={`Rename ${file.name}`} onClick={() => (setName(file.path), setMode("rename"))}>
              <Pencil className="size-3.5" aria-hidden />
            </IconButton>
            <IconButton size="sm" label={`Delete ${file.name}`} onClick={() => setMode("delete")}>
              <Trash2 className="size-3.5" aria-hidden />
            </IconButton>
          </span>
        </>
      )}
      {mode === "delete" && (
        <span className="relative z-10 flex items-center gap-2 text-sm">
          <span className="text-fg-2">Delete this file from disk?</span>
          <Button size="sm" variant="danger" disabled={busy} onClick={() => void act(() => api.deleteNotebook(file.path))}>
            Delete
          </Button>
          <Button size="sm" variant="ghost" autoFocus onClick={() => setMode("view")}>
            Keep
          </Button>
        </span>
      )}
      {error && (
        <p role="alert" className="relative z-10 basis-full pl-7 text-xs text-critical-fg">
          {error}
        </p>
      )}
    </li>
  );
}

export function NotebooksPage() {
  const files = usePoll("notebooks", api.notebooks, 15_000);
  const { navigate } = useRouter();
  const now = useNow(10_000);
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const sorted = [...(files.data ?? [])].sort((a, b) => b.modified - a.modified);

  const create = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const { path } = await api.createNotebook(name.trim() ? withExt(name) : undefined);
      navigate(notebookHref(path));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setBusy(false);
    }
  };

  return (
    <Page>
      <PageHeader
        title="Notebooks"
        meta={
          <span className="flex flex-wrap items-center gap-x-3">
            {files.data ? <span className="num">{plural(sorted.length, "notebook")} in the console directory, most recent first</span> : "Loading notebooks"}
            <StaleMark polled={files} now={now} />
          </span>
        }
      />

      <form onSubmit={create} className="mb-3 flex flex-wrap items-center gap-2">
        <label className="sr-only" htmlFor="new-notebook">
          Name for the new notebook (optional)
        </label>
        <input id="new-notebook" value={name} onChange={(e) => setName(e.target.value)} placeholder="Untitled.ipynb" className={`${inputCls} w-full sm:w-72`} />
        <Button variant="solid" type="submit" disabled={busy} icon={<FilePlus2 className="size-3.5" aria-hidden />}>
          New notebook
        </Button>
        {error && (
          <p role="alert" className="basis-full text-sm text-critical-fg">
            {error}
          </p>
        )}
      </form>

      <div className="rounded-lg border border-line bg-surface">
        <Async
          polled={files}
          skeleton={<SkeletonRows rows={5} />}
          isEmpty={(f) => !f.length}
          empty={
            <EmptyState title="No notebooks in this directory">
              Create one above. Notebooks are plain <code className="rounded-sm border border-line bg-sunken px-1 text-xs">.ipynb</code> files, so anything you make here also opens in Jupyter and Colab, and existing notebooks in the directory show up in this list.
            </EmptyState>
          }
        >
          {() => (
            <ul className="divide-y divide-line">
              {sorted.map((f) => (
                <FileRow key={f.path} file={f} now={now} onChanged={files.refresh} />
              ))}
            </ul>
          )}
        </Async>
      </div>
    </Page>
  );
}
