# Execution on a node

What a daemon does with a job once it holds the lease: receive the workspace, run it in a sandbox under a memory ceiling, stream output, return results. Covers `daemonapi` (server, execution, state), `daemonctl`, `app`, `cgroups`, `userns`, `jobhistory`, `userdir`. Pool spill and DDP in this package belong to [Interception](../nixgen/CONTEXT.md); lease admission to [Nodes and placement](../coordinator/CONTEXT.md). Design detail: [system report 4.8](../../../docs/system-report.md#48-execution-isolation-and-memory).

## Language

**Daemon**:
The long-lived per-machine process (`pipedpeer __daemon__`) that serves the API on port 4242 (`daemonctl.DefaultPort`) and runs jobs; started by `pipedpeer start` or, when installed, the `pipedpeer.service` user unit; the CLI is short-lived.
_Avoid_: server, agent, worker

**Submitter**:
The CLI process that built the environment, placed the job and streams its output; it must stay alive for the job.

**Job**:
One script run on one node, with its own ID, lease and job dir.
_Avoid_: task (reserved for `pipedpeer map` units), run

**Workspace**:
The project directory minus `.pipedpeerignore` matches; only files the node's content-addressed cache lacks are sent.

**Job dir**:
The daemon's scratch directory for one job under `<data>/pipedpeer/work/`; a finished job's dir is removed once its results were fetched or after 24 h, a running job's never (`server.go` `PruneCompletedJobDirs`).
_Avoid_: history dir

**Job history**:
The submitter-side records under `<data>/pipedpeer/jobs/` that `pipedpeer jobs` reads ([ADR-0015](../../../docs/adr/0015-job-dirs-apart-from-history.md)).

**Results**:
Only the files the job created or modified, returned as a tar with a manifest; uploaded files are stamped so unchanged ones stay behind.

**Sandbox**:
The crun OCI bundle a job runs in ([ADR-0012](../../../docs/adr/0012-crun-only-runtime.md)).
_Avoid_: container (there is no image), bwrap

**Unisolated**:
A job run directly on the host because crun or user namespaces are missing; always announced in the job's output.

**Sandbox quirks**:
Sandbox features a nested or restricted host cannot provide (private procfs, hostname), detected once by probing crun and disabled one by one.

**Memory ceiling**:
The per-job `memory.max` the kernel enforces ([ADR-0016](../../../docs/adr/0016-memory-caps-in-a-user-scope.md)).

**Daemon budget**:
The memory this daemon may promise: its scope's `memory.max`, or half the machine where nothing enforces one (`cgroups/budget.go` `SelfBudget`).

**Grantable memory**:
What the daemon can still promise to a new job: its budget minus what it and its roommates have reserved.

## Invariants

- No pipedpeer file goes to `$TMPDIR`: paths come from `userdir`; `src/tmpfs_guard_test.go` fails the build otherwise.
- A job killed by its cap is reported as out-of-memory only if the cgroup's `oom_kill` counter moved during it.
- With a private nix store a job without a sandbox fails rather than runs unisolated (`execution.go` `handleJobExec`).
- Leases and the job map persist across a daemon restart (`state.go`).
- The daemon prepends `~/.local/bin` to its own PATH, since it has no login shell and a missing crun means unisolated jobs (`d8bbede`).

## Entry points

- Routes: `daemonapi/server.go` `buildRouter`; daemon start `src/daemon_cmd.go` `runDaemon`.
- Upload, exec, results: `daemonapi/execution.go` `handleJobUpload`, `handleJobExec`, `handleJobResults`.
- Sandbox decision and fallback: `handleJobExec` (`sandboxRequired`); quirks `daemonapi/sandboxquirks.go` `probeQuirks`; user namespace probe `userns/userns.go`.
- GPU: devices from `gpu.Detect()` (`internal/gpu`); driver libraries mounted via `execution.go` `gpuDriverLibDir`, `gpuDriverLibs`.
- Memory: `execution.go` `grantableMemory`, `applyMemLimit`, `oomKills`; `cgroups/cgroups.go` `Prepare`; `cgroups/budget.go` `SelfBudget`.
- Persistence: `daemonapi/server.go` `EnablePersistence`, `daemonapi/state.go`.
- Submitter side: `app/run.go` `Run`, `RunTask`; `daemonctl/execution.go` `UploadJob`, `StreamExecute`, `DownloadResults`; daemon lifecycle `daemonctl/manager.go` `Start`.
- History: `jobhistory/history.go` `BaseDir`.
- Run record: `runrec` (`Save`/`Load` steps.json beside the receipt, `WriteSimple`, `WriteDetailed`, `ExportAll`); served by `GET /v1/history?id=<job>` (`daemonapi/ui.go` `handleHistory`).
