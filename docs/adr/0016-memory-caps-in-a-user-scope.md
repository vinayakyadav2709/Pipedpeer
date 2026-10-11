# Memory caps come from cgroup v2 delegated to a user systemd scope

A per-job `memory.max` needs a cgroup the daemon may delegate from, and a daemon started from an ssh shell sits in a root-owned session scope. `pipedpeer start` launches the daemon in a transient `systemd-run --user --scope`, moves its own processes into a leaf, and caps each job below it, all without root (`bc70fbc`, `src/internal/cgroups/cgroups.go`). Machines without systemd keep the old unlimited behaviour. A job is reported as out-of-memory only when the cgroup's `oom_kill` counter moved.
