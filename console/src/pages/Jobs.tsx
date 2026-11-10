import { Search, X } from "lucide-react";
import { useEffect, useState } from "react";
import { Page } from "@/App";
import { api } from "@/lib/api";
import { ago, bytes, dateTime, duration, plural, shortPath } from "@/lib/format";
import { useNow, usePoll } from "@/lib/poll";
import { jobHref, Link } from "@/lib/router";
import type { Job } from "@/lib/types";
import { cn } from "@/ui/cn";
import { Async, EmptyState, PageHeader, SkeletonRows, StaleMark } from "@/ui/panel";
import { JOB_STATUSES, JobStatus } from "@/ui/status";

const LIMIT = 200;

function useQueryState() {
  const initial = new URLSearchParams(location.search);
  const [status, setStatus] = useState(initial.get("status") ?? "");
  const [text, setText] = useState(initial.get("q") ?? "");
  const [query, setQuery] = useState(text);
  useEffect(() => {
    const id = setTimeout(() => setQuery(text.trim()), 250);
    return () => clearTimeout(id);
  }, [text]);
  useEffect(() => {
    const sp = new URLSearchParams();
    if (status) sp.set("status", status);
    if (query) sp.set("q", query);
    const s = sp.toString();
    history.replaceState(null, "", s ? `/jobs?${s}` : "/jobs");
  }, [status, query]);
  return { status, setStatus, text, setText, query };
}

function elapsed(job: Job, now: number) {
  if (job.duration_ms > 0) return duration(job.duration_ms);
  if (job.status === "running" && job.started_at) return duration(now - job.started_at);
  return "–";
}

function JobRows({ jobs, now }: { jobs: Job[]; now: number }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[44rem] border-collapse text-sm max-md:hidden">
        <thead>
          <tr className="border-b border-line text-left text-xs text-fg-3 [&>th]:px-3 [&>th]:py-2 [&>th]:font-medium [&>th:first-child]:pl-4 [&>th:last-child]:pr-4">
            <th scope="col" className="w-28">Status</th>
            <th scope="col">Job</th>
            <th scope="col">Node</th>
            <th scope="col">Started</th>
            <th scope="col" className="text-right">Duration</th>
            <th scope="col" className="text-right">Peak memory</th>
            <th scope="col">Estimate</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((j) => (
            <tr key={j.id} className="group relative border-b border-line last:border-0 hover:bg-sunken [&>td]:px-3 [&>td]:py-2 [&>td:first-child]:pl-4 [&>td:last-child]:pr-4">
              <td>
                <JobStatus status={j.status} />
              </td>
              <td className="max-w-0 w-[34%]">
                <Link href={jobHref(j.id)} className="block truncate font-medium after:absolute after:inset-0 focus-visible:outline-none focus-visible:after:rounded-sm focus-visible:after:outline-2 focus-visible:after:-outline-offset-2 focus-visible:after:outline-accent">
                  {j.job_name || j.id}
                </Link>
                <span className="block truncate font-mono text-xs text-fg-3" title={j.script_path || undefined}>{j.error && j.status !== "succeeded" ? j.error : j.script_path ? shortPath(j.script_path) : j.id}</span>
              </td>
              <td className="whitespace-nowrap text-fg-2">{j.run_host || <span className="text-fg-3">not placed</span>}</td>
              <td className="num whitespace-nowrap text-fg-2" title={dateTime(j.started_at)}>
                {ago(j.started_at, now)}
              </td>
              <td className="num text-right whitespace-nowrap">{elapsed(j, now)}</td>
              <td className="num text-right whitespace-nowrap">{j.peak_mem_bytes > 0 ? bytes(j.peak_mem_bytes) : <span className="text-fg-3">–</span>}</td>
              <td className="num whitespace-nowrap text-fg-2">
                {j.estimated_mem_bytes > 0 ? (
                  <>
                    {bytes(j.estimated_mem_bytes)} <span className="text-fg-3">{j.estimation_tier}</span>
                  </>
                ) : (
                  <span className="text-fg-3">–</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <ul className="divide-y divide-line md:hidden">
        {jobs.map((j) => (
          <li key={j.id} className="relative flex flex-col gap-1 px-4 py-2.5 active:bg-sunken">
            <div className="flex items-center justify-between gap-2">
              <Link href={jobHref(j.id)} className="truncate text-sm font-medium after:absolute after:inset-0">
                {j.job_name || j.id}
              </Link>
              <JobStatus status={j.status} />
            </div>
            <div className="num flex flex-wrap gap-x-2 text-xs text-fg-3">
              <span className="text-fg-2">{j.run_host || "not placed"}</span>
              <span>{ago(j.started_at, now)}</span>
              <span>{elapsed(j, now)}</span>
              {j.peak_mem_bytes > 0 && <span>{bytes(j.peak_mem_bytes)} peak</span>}
            </div>
            {j.error && <p className="truncate font-mono text-xs text-critical-fg">{j.error}</p>}
          </li>
        ))}
      </ul>
    </div>
  );
}

export function JobsPage() {
  const { status, setStatus, text, setText, query } = useQueryState();
  const jobs = usePoll(`jobs:${status}:${query}`, (s) => api.jobs({ limit: LIMIT, status, q: query }, s), 15_000);
  const now = useNow(1000);
  const filtered = !!status || !!query;

  return (
    <Page>
      <PageHeader
        title="Jobs"
        meta={
          <span className="flex flex-wrap items-center gap-x-3">
            {jobs.data ? (
              <span className="num">
                {jobs.data.length >= LIMIT ? `Newest ${LIMIT}` : plural(jobs.data.length, "job")}
                {filtered ? " matching" : ""}
              </span>
            ) : (
              "Loading job history"
            )}
            <StaleMark polled={jobs} now={now} />
          </span>
        }
      />

      <div className="mb-3 flex flex-wrap items-center gap-2">
        <label className="relative block w-full sm:w-72">
          <span className="sr-only">Search jobs</span>
          <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-fg-3" aria-hidden />
          <input
            type="search"
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="Search name, script, node, error"
            className="h-8 w-full rounded-md border border-line-strong bg-surface pr-8 pl-8 text-sm outline-none focus-visible:border-accent focus-visible:ring-2 focus-visible:ring-accent/30 [&::-webkit-search-cancel-button]:hidden"
          />
          {text && (
            <button type="button" aria-label="Clear search" onClick={() => setText("")} className="absolute top-1/2 right-1.5 inline-flex size-5 -translate-y-1/2 items-center justify-center rounded-sm text-fg-3 hover:bg-hover hover:text-fg">
              <X className="size-3.5" aria-hidden />
            </button>
          )}
        </label>
        <div role="radiogroup" aria-label="Filter by status" className="flex flex-wrap gap-1">
          {["", ...JOB_STATUSES].map((s) => (
            <button
              key={s || "all"}
              type="button"
              role="radio"
              aria-checked={status === s}
              onClick={() => setStatus(s)}
              className={cn(
                "h-7 rounded-md border px-2.5 text-xs font-medium transition-colors duration-150",
                status === s ? "border-solid bg-solid text-solid-fg" : "border-line bg-surface text-fg-2 hover:border-line-strong hover:text-fg",
              )}
            >
              {s || "All"}
            </button>
          ))}
        </div>
      </div>

      <div className="rounded-lg border border-line bg-surface">
        <Async
          polled={jobs}
          skeleton={<SkeletonRows rows={10} />}
          isEmpty={(j) => !j.length}
          empty={
            filtered ? (
              <EmptyState title="No jobs match these filters">
                <button type="button" className="text-accent-fg underline" onClick={() => (setStatus(""), setText(""))}>
                  Clear the filters
                </button>{" "}
                to see every job.
              </EmptyState>
            ) : (
              <EmptyState title="No jobs yet">
                Every script you start with <code className="rounded-sm border border-line bg-sunken px-1 text-xs">pipedpeer run</code>, and every notebook cell sent to the cluster, is recorded here with where it ran and how much memory it used.
              </EmptyState>
            )
          }
        >
          {(j) => <JobRows jobs={j} now={now} />}
        </Async>
      </div>
    </Page>
  );
}
