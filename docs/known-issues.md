# Known issues found while documenting (2026-10-02, commit 4c91ef3)

These were found while the report was being brought up to date. They are
recorded, not fixed, because the documentation describes the system as it
stands.

Paths are relative to `src/`. Each entry says how it was established:
- **Observed:** it happened during a run.
- **Read:** it was traced in the code but not exercised.

## Observed

3. **On a fresh lab, a job whose closure is already present never seeds its
   peers.**
   - `scripts/lab-fail.sh` placed its job on the submitting host, which said
     "nothing to send".
   - The new workers never received the closure, so `EnablePoolSpill` offered
     no peers.
   - The shim then ran all 80 items locally, and the harness failed.
   - Raw evidence: `benchmarks/raw/O4_lab_fail.txt`.
   - The cause is inferred from the log.
7. **The idle-interception benchmark no longer measures idle interception.**
   - `bench.py` N7 is labelled "the cost model declines", but at this commit
     the shim distributed 18 of 48 items in every enabled run.

## Read in the code, not exercised

| Area | Issue | Where |
|---|---|---|
| Pool | The forwarded body lacks `no_fanout`, so the peer can fan out again | `daemonapi/pool.go:983-1001, 894` |
| Pool | `imap_unordered` returns results in order | `nixgen/shim.go:264-265` |
| numpy | The `@` operator is not intercepted (only module functions are patched) | `nixgen/shim.go:1159-1163` |
| Security | Trusted Nix keys are collected from every node in the node store, and are not bound to an authenticated identity | `daemonapi/server.go:1578-1615` |
| Security | The legacy closure import uses `--no-check-sigs`, and the submitter's own upload uses that format | `daemonapi/execution.go:1029-1066` |
| Setup | `nix-imports` aborts when the keys file is empty, which happens before the daemon that writes it has started (fresh multi-user Nix) | `setup/setup.go:206-215, 420-454` |
| Setup | Keys appended to `nix.conf` are a snapshot; peers added later are not trusted until setup runs again | `setup/setup.go:250-253` |
| Private store | `pathExists` checks the host `/nix/store`, not the private store | `daemonapi/execution.go:191-194` |
| Temp files | `closurediff.go` still uses `os.CreateTemp("", …)` | `daemonctl/closurediff.go:61` |

## Limitations by design

- **One introducer per cluster.** There is no failover. Joining across networks
  needs it up, although links that are already live survive it going away.
- **No relay.** A pair of peers with no direct path is not offered work
  (`95e0712`); it would carry every byte of every job.
- **Two peers both behind per-destination (symmetric) NATs cannot connect.**
