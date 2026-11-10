import { ChevronRight, CircleAlert, MoveRight } from "lucide-react";
import type { ReactNode } from "react";
import { Page } from "@/App";
import { api } from "@/lib/api";
import { bytes, dateTime, duration, timeSec } from "@/lib/format";
import { useNow, usePoll } from "@/lib/poll";
import { Link } from "@/lib/router";
import type { JobDetail } from "@/lib/types";
import { CopyButton } from "@/ui/controls";
import { LogViewer } from "@/ui/LogViewer";
import { Async, Panel, SkeletonRows, StaleMark } from "@/ui/panel";
import { JobStatus } from "@/ui/status";

function Row({ k, children }: { k: string; children: ReactNode }) {
  return (
    <div className="grid grid-cols-[6.5rem_minmax(0,1fr)] gap-x-3 px-4 py-1.5 text-sm">
      <dt className="text-fg-3">{k}</dt>
      <dd className="min-w-0 break-words">{children}</dd>
    </div>
  );
}

const none = <span className="text-fg-3">–</span>;

function Memory({ job }: { job: JobDetail }) {
  const est = job.estimated_mem_bytes;
  const peak = job.peak_mem_bytes;
  if (est <= 0 && peak <= 0) return <p className="px-4 py-3 text-sm text-fg-3">No memory estimate or measurement was recorded for this job.</p>;
  const max = Math.max(est, peak, 1);
  const ratio = est > 0 && peak > 0 ? est / peak : null;
  const bar = (v: number, color: string, label: string) => (
    <div className="grid grid-cols-[4.75rem_minmax(0,1fr)_4.5rem] items-center gap-x-3 text-xs">
      <span className="text-fg-3">{label}</span>
      <span className="block h-2">{v > 0 && <span className="block h-full min-w-0.5 rounded-r-[3px]" style={{ width: `${(v / max) * 100}%`, background: color }} />}</span>
      <span className="num text-right text-sm font-medium">{v > 0 ? bytes(v) : none}</span>
    </div>
  );
  return (
    <div className="flex flex-col gap-2 px-4 py-3">
      {bar(est, "var(--s1-light)", "Estimated")}
      {bar(peak, "var(--s1)", "Real peak")}
      <p className="pt-1 text-xs text-fg-2">
        {ratio === null
          ? peak > 0
            ? "The scheduler placed this job without a memory estimate."
            : "The peak is measured when the job finishes."
          : ratio > 1.25
            ? `Estimate was ${ratio.toFixed(1)}× the real peak: ${bytes(est - peak)} of capacity was held back.`
            : ratio < 0.8
              ? `The job used ${(1 / ratio).toFixed(1)}× its estimate: ${bytes(peak - est)} more than the scheduler reserved.`
              : "Estimate was within 25% of the real peak."}
        {job.estimation_tier && <span className="text-fg-3"> Tier: {job.estimation_tier}.</span>}
      </p>
    </div>
  );
}

function Events({ job }: { job: JobDetail }) {
  const events = job.events ?? [];
  if (!events.length) return <p className="px-4 py-3 text-sm text-fg-3">The history store has no state transitions for this job.</p>;
  return (
    <ol className="px-4 py-3">
      {events.map((e, i) => {
        const prev = events[i - 1];
        return (
          <li key={`${e.timestamp}-${e.status}`} className="relative grid grid-cols-[1rem_minmax(0,1fr)] gap-x-2 pb-3 last:pb-0">
            {i < events.length - 1 && <span aria-hidden className="absolute top-3.5 bottom-0 left-[0.4375rem] w-px bg-line-strong" />}
            <span aria-hidden className="relative mt-[0.4375rem] ml-1 size-2 rounded-full border-2 border-fg-3 bg-surface" />
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5">
                <JobStatus status={e.status} past={i < events.length - 1} />
                <span className="num text-xs text-fg-3">
                  {timeSec(e.timestamp)}
                  {prev ? `, after ${duration(e.timestamp - prev.timestamp)}` : ""}
                </span>
              </div>
              {e.error && <p className="mt-1 font-mono text-xs break-words text-critical-fg">{e.error}</p>}
            </div>
          </li>
        );
      })}
    </ol>
  );
}

function Detail({ job, now }: { job: JobDetail; now: number }) {
  const flags = [job.detached && "detached", job.isolate && "isolated", job.degraded_mode && "degraded mode"].filter(Boolean) as string[];
  const elapsed = job.duration_ms > 0 ? duration(job.duration_ms) : job.status === "running" && job.started_at ? `${duration(now - job.started_at)} so far` : null;
  return (
    <>
      <header className="flex flex-wrap items-center gap-x-3 gap-y-1.5 pb-4">
        <h1 className="min-w-0 text-xl font-semibold tracking-[-0.015em] break-words">{job.job_name || job.id}</h1>
        <JobStatus status={job.status} />
        <span className="flex items-center gap-0.5 font-mono text-xs text-fg-3">
          {job.id}
          <CopyButton text={job.id} label="Copy job id" />
        </span>
      </header>

      {job.error && (
        <div role="alert" className="mb-4 flex gap-2.5 rounded-lg border border-critical/40 bg-critical/8 px-4 py-3">
          <CircleAlert className="mt-0.5 size-4 shrink-0 text-critical-fg" aria-hidden />
          <p className="min-w-0 font-mono text-sm break-words">{job.error}</p>
        </div>
      )}

      <div className="grid grid-cols-1 items-start gap-4 xl:grid-cols-[minmax(0,1fr)_24rem]">
        <LogViewer
          text={job.log_tail ?? ""}
          empty={job.status === "queued" || job.status === "reserved" ? "The job has not started, so there is no output yet." : "No output was stored for this job."}
        />

        <div className="flex min-w-0 flex-col gap-4 xl:order-none max-xl:order-first">
          <Panel title="Run">
            <dl className="py-1.5">
              <Row k="Script">{job.script_path ? <span className="font-mono text-xs">{job.script_path}</span> : none}</Row>
              <Row k="Node">{job.run_host || <span className="text-fg-3">not placed yet</span>}</Row>
              <Row k="Started">{job.started_at ? <span className="num">{dateTime(job.started_at)}</span> : none}</Row>
              <Row k="Finished">{job.finished_at ? <span className="num">{dateTime(job.finished_at)}</span> : none}</Row>
              <Row k="Duration">{elapsed ? <span className="num">{elapsed}</span> : none}</Row>
              <Row k="Stage">{job.stage || none}</Row>
              {job.job_set && <Row k="Job set">{job.job_set}</Row>}
              {flags.length > 0 && <Row k="Flags">{flags.join(", ")}</Row>}
            </dl>
          </Panel>

          <Panel title="Placement" hint={job.placement_source ? `via ${job.placement_source}` : undefined}>
            {job.placement_source || job.placement_reason ? (
              <div className="flex flex-col gap-2 px-4 py-3 text-sm">
                <p className="flex flex-wrap items-center gap-x-1.5 text-fg-2">
                  <span className="num">{job.candidate_count > 0 ? `${job.candidate_count} candidate${job.candidate_count === 1 ? "" : "s"}` : "No candidates recorded"}</span>
                  <MoveRight className="size-3.5 text-fg-3" aria-hidden />
                  <span className="font-medium text-fg">{job.run_host || job.target_id || "unplaced"}</span>
                </p>
                {job.placement_reason && <p className="rounded-md bg-sunken px-2.5 py-2 font-mono text-xs leading-relaxed break-words text-fg-2">{job.placement_reason}</p>}
                {job.degraded_mode && <p className="text-xs text-warn-fg">Placed in degraded mode: the scheduler could not see the whole cluster.</p>}
              </div>
            ) : (
              <p className="px-4 py-3 text-sm text-fg-3">No placement decision has been recorded.</p>
            )}
          </Panel>

          <Panel title="Memory">
            <Memory job={job} />
          </Panel>

          <Panel title="Timeline">
            <Events job={job} />
          </Panel>
        </div>
      </div>
    </>
  );
}

export function JobDetailPage({ id }: { id: string }) {
  const job = usePoll(`job:${id}`, (s) => api.job(id, s), 5000);
  const live = job.data ? job.data.status === "running" || job.data.status === "queued" || job.data.status === "reserved" : true;
  const now = useNow(1000, live);
  return (
    <Page>
      <nav aria-label="Breadcrumb" className="flex items-center gap-1 pb-2 text-sm text-fg-3">
        <Link href="/jobs" className="rounded-sm hover:text-fg hover:underline">
          Jobs
        </Link>
        <ChevronRight className="size-3.5" aria-hidden />
        <span className="truncate text-fg-2">{job.data?.job_name || id}</span>
        <span className="ml-2">
          <StaleMark polled={job} now={now} />
        </span>
      </nav>
      <Async polled={job} skeleton={<div className="rounded-lg border border-line bg-surface"><SkeletonRows rows={8} /></div>}>
        {(j) => <Detail job={j} now={now} />}
      </Async>
    </Page>
  );
}
