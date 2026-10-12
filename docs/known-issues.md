# Known issues found while documenting (2026-10-02, commit 4c91ef3)

These were found while the report was being brought up to date. They are
recorded, not fixed, because the documentation describes the system as it
stands.

Paths are relative to `src/`. Each entry says how it was established:
- **Observed:** it happened during a run.
- **Read:** it was traced in the code but not exercised.

## Resolved since

| Issue | Fixed by |
|---|---|
| Observed 1: the build hangs when `$HOME` is a git repository | The flake is built as `path:.`, so Nix never treats the scratch directory as part of an enclosing repository. A failed build, or a script that exits with an error, is no longer rescheduled forever: both would fail the same way on any node (`app.IsPermanent`, `coordinator.Permanent`) |
| Observed 4: the cluster's signing keys could not be recorded | `WriteTrustedKeys` writes through a uniquely named temporary file |
| Observed 5: daemon start-up prunes job history | Each pruner only removes its own entries: the daemon only UUID-named job directories, the history only directories holding a record |
| Observed 6: a job's output stream closes early | a7bb9a5 waits for both output readers before reaping the job |
| Pool: `items_zlib` is dropped when the daemon splits work | 049a1aa |
| Internet: unreachable reasons never reach `pipedpeer nodes` | They are kept by the daemon, served by `/v1/peers/unreachable`, and listed under `nodes` |
| `--remote` gives up after one look (demo run sheet) | It waits up to 10 min for a node (`PIPEDPEER_REMOTE_WAIT`), says every 30 s why each node is busy, and fails at once when no node could ever hold the job |
| Observed 2: three tests fail, not skip, without torch | The shim test gate counts the weighted sampler as needing torch |
| Pool: `runMicroChunks` answers in JSON; the shim reads frames | Micro-chunked requests answer in the format they were asked in, with a receipt |
| Pool: forwarded work is recorded as `local` | The forwarding node relabels the peer's `local` parts as `peer:<addr>` before passing the answer on |
| Internet: `RemovePeer` removes every `127.0.0.1` forwarder | Removal matches host and port |
| Pool sizing reads `os.cpu_count()` (demo run sheet) | `_usable_cores()` honours CPU affinity and the cgroup `cpu.max` quota |
| A third-party import missing from the resolver table is silently dropped (report) | The build warns and names the imports, and `--pkg` silences it. The table also covered `import PIL` with a package name nixpkgs lacks (`Pillow`, now `pillow`), and listed pip names (`opencv`) where scripts use import names (`cv2`, `bs4`, `skimage`) |
| Data races in the `nattype` and `portmap` tests | Fixed in the tests; `go test -race ./...` is clean |
| Discovery probes carry the shared secret to every host on the subnet (Security) | A host must answer a challenge (HMAC over a random challenge, bound to the TLS session) before the token is sent to it |
| Firewall commands run without `sudo` (Setup) | Changes go through `sudo` for a non-root user; checks stay unprivileged |
| `nix-imports` aborts when the keys file is empty (Setup) | With no keys yet, setup installs the key helper and the daemon adds keys as peers join |
| The punch nonce is identical every time (Internet) | Drawn from `crypto/rand` |
| `closurediff.go` writes to `$TMPDIR` (Temp files) | Writes to the on-disk scratch directory |
| `pathExists` checks the host `/nix/store`, not the private store | `/nix` paths are mapped into the private store (`nixstore.HostPath`) |
| Observed 8: `SECURITY.md` is out of date | Rewritten to describe the secret, pinned TLS, signed closures and what they do not cover |
| Found on the rig (Phase 4): `RandomForest(n_jobs=-1)` is 6–7× slower on the cluster than on one machine | joblib calls take `Pool.map`'s path: a measured head, the dispatch floor, then a race in multi-task chunks. The old backend sent one task per round trip and waited for each |
| Found on the rig (Phase 4): `.pipedpeerignore` lines ending in `/` exclude nothing | Trailing slashes are dropped before tar sees the pattern, and a leading slash anchors it at the project root. Every root demo was uploading the 1.1 GB pandas CSV |
| Found on the rig (Phase 4): a memory-bound pool shrinks itself between maps | The pool's own workers count as reclaimable when it re-sizes |
| Found on the rig (Phase 4): more workers do not make a pool map faster | Local and cluster pull from one queue instead of splitting the items in half up front |
| Found on the rig (Phase 4): sklearn ships the training set once per tree | Arrays the tasks in a chunk share travel once per chunk |
| Found on the rig (Phase 4): a run straight after another is refused for memory | A run on this machine waits out a memory refusal for up to the placement window (10 min) |

## Not bugs, recorded so they are not reported again

- **`TestLeaseExpiryRedirectsTask` can fail under `go test -race ./...`.** It passes alone and as a package, before and after the fixes above. It fails only when every package runs at once under the race detector: heartbeats 30 ms apart are starved against a 400 ms lease. The test already skips when it can see the starvation, but there is a window it cannot see.

- **`imap_unordered` returns results in order.** That is allowed: "unordered" promises no order, and input order is one of them. The cost is that a slow item holds back the rest of its batch.
- **numpy's `@` operator is not intercepted.** It cannot be: `numpy.ndarray` is an immutable C type, and `ndarray.__matmul__` cannot be replaced (`TypeError: cannot set '__matmul__' attribute of immutable type`). Write `np.matmul(a, b)` or `a.dot(b)` for a product that should be distributed.
- **A forwarded request can fan out again.** It is meant to: the origin forwarded because it lacked the memory to run the work, and the peer spreading it further is how the work gets done. A sub-chunk landing back on the origin carries no forwarding mark, so it can be forwarded once more, but never in a loop.

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
| Setup | Keys appended to `nix.conf` are a snapshot; peers added later are not trusted until setup runs again | `setup/setup.go:250-253` |
| Private store | `pathExists` checks the host `/nix/store`, not the private store | `daemonapi/execution.go:191-194` |
| Temp files | `closurediff.go` still uses `os.CreateTemp("", …)` | `daemonctl/closurediff.go:61` |

## Limitations by design

- **One introducer per cluster.** There is no failover. Joining across networks
  needs it up, although links that are already live survive it going away.
- **No relay.** A pair of peers with no direct path is not offered work
  (`95e0712`); it would carry every byte of every job.
- **Two peers both behind per-destination (symmetric) NATs cannot connect.**
