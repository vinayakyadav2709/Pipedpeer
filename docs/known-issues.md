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
| Internet: unreachable reasons never reach `pipedpeer nodes` | They are kept by the daemon, served by `/v1/nodes`, and listed under the `nodes` table |
| `--remote` gives up after one look (demo run sheet) | It waits up to 15 s for a node (`PIPEDPEER_REMOTE_WAIT`), and says why each node was turned down |
| Observed 2: three tests fail, not skip, without torch | The shim test gate counts the weighted sampler as needing torch |
| Pool: `runMicroChunks` answers in JSON; the shim reads frames | Micro-chunked requests answer in the format they were asked in, with a receipt |
| Pool: forwarded work is recorded as `local` | The forwarding node relabels the peer's `local` parts as `peer:<addr>` before passing the answer on |
| Internet: `RemovePeer` removes every `127.0.0.1` forwarder | Removal matches host and port |
| Pool sizing reads `os.cpu_count()` (demo run sheet) | `_usable_cores()` honours CPU affinity and the cgroup `cpu.max` quota |
| A third-party import missing from the resolver table is silently dropped (report) | The build warns and names the imports, and `--pkg` silences it. The table also covered `import PIL` with a package name nixpkgs lacks (`Pillow`, now `pillow`), and listed pip names (`opencv`) where scripts use import names (`cv2`, `bs4`, `skimage`) |
| Data races in the `nattype` and `portmap` tests | Fixed in the tests; `go test -race ./...` is clean |

## Not bugs, recorded so they are not reported again

- **`imap_unordered` returns results in order.** That is allowed: "unordered" promises no order, and input order is one of them. The cost is that a slow item holds back the rest of its batch.
- **numpy's `@` operator is not intercepted.** It cannot be: `numpy.ndarray` is an immutable C type, and `ndarray.__matmul__` cannot be replaced (`TypeError: cannot set '__matmul__' attribute of immutable type`). Write `np.matmul(a, b)` or `a.dot(b)` for a product that should be distributed.
- **A forwarded request can fan out again.** It is meant to: the origin forwarded because it lacked the memory to run the work, and the peer spreading it further is how the work gets done. A sub-chunk landing back on the origin carries no forwarding mark, so it can be forwarded once more, but never in a loop.

## Observed

1. **The build hangs forever when `$HOME` is a git repository.**
   - What happens:
     - The build directory moved from `$TMPDIR` to `~/.cache/pipedpeer/scratch`
       (`userdir.Scratch`).
     - When the home directory is itself a git work tree, `nix build` treats
       the generated `flake.nix` as part of that repository and refuses it:
       `Path '.cache/pipedpeer/scratch/build-…/flake.nix' … is not tracked by Git`.
     - The coordinator then retries the job with back-off indefinitely.
   - Workaround used for the measurements: `XDG_CACHE_HOME=/var/tmp/...`.
2. **Three tests fail, not skip, without torch.**
   - The tests are `TestWeightedShardsPartitionTheDataset`,
     `TestEqualStepsAcrossUnevenShares` and `TestShuffledShardsStayDisjoint`.
   - Each fails with `ModuleNotFoundError: torch`.
   - Raw evidence: `benchmarks/raw/T1b_go_test_without_torch.txt`.
3. **On a fresh lab, a job whose closure is already present never seeds its
   peers.**
   - `scripts/lab-fail.sh` placed its job on the submitting host, which said
     "nothing to send".
   - The new workers never received the closure, so `EnablePoolSpill` offered
     no peers.
   - The shim then ran all 80 items locally, and the harness failed.
   - Raw evidence: `benchmarks/raw/O4_lab_fail.txt`.
   - The cause is inferred from the log.
4. **The daemon could not record the cluster's signing keys.**
   - At start-up it logged:

     ```
     could not record the cluster's signing keys: rename …/trusted-public-keys.tmp …/trusted-public-keys: no such file or directory
     ```

   - It may be a race between two writers of the same temporary file. Not
     investigated.
5. **Daemon start-up prunes job history.**
   - It logged `pruned stale job dir(s) n=14`.
   - Job history and daemon job directories share
     `~/.local/share/pipedpeer/jobs`, so either pruner can delete the other's
     entries.
6. **A job's output stream sometimes closes early, and the run is rescheduled.**
   - Two of five `PIPEDPEER_SHIM=0` runs in the lab failed with `execution
     failed: remote: stdout scanner: read |0: file already closed`.
   - The coordinator rescheduled them, giving 9.66 s and 3.25 s.
   - Raw evidence: `benchmarks/raw/N7_shim_off_run1.log`, `run4.log`.
   - Not investigated.
7. **The idle-interception benchmark no longer measures idle interception.**
   - `bench.py` N7 is labelled "the cost model declines", but at this commit
     the shim distributed 18 of 48 items in every enabled run.
8. **`SECURITY.md` is out of date.** It still says there is no authentication,
   authorisation or transport encryption, which is no longer accurate (see
   `system-report.md` §4.3).

## Read in the code, not exercised

| Area | Issue | Where |
|---|---|---|
| Pool | `items_zlib` is set by the shim, but `poolRequest` has no such field and the header is rebuilt without it, so compressed items would reach `pickle.loads` | `nixgen/shim.go:1278`; `daemonapi/pool.go:487-539, 1381, 1531, 1667` |
| Pool | `runMicroChunks` always answers in JSON; the shim reads frames | `daemonapi/pool.go:977`; `nixgen/shim.go:1303-1313` |
| Pool | Forwarded work is recorded as `local` by the peer, so receipts undercount remote items. The forwarded body lacks `no_fanout`, so it can fan out again | `daemonapi/pool.go:983-1001, 894` |
| Pool | `imap_unordered` returns results in order | `nixgen/shim.go:264-265` |
| numpy | The `@` operator is not intercepted (only module functions are patched) | `nixgen/shim.go:1159-1163` |
| Internet | `RemovePeer` deletes manual nodes by host only, so one departing internet peer removes every `127.0.0.1` forwarder | `daemonapi/server.go:1380-1385`; `nodestore/store.go:239-240` |
| Internet | Unreachable reasons are only logged and never reach `pipedpeer nodes`, contrary to the comment | `main.go:2217-2223`; `internet/internet.go:26-30` |
| Internet | The punch nonce comes from an all-zero-seeded generator, so it is identical every time (it is not an authentication value) | `internet/internet.go:384-385` |
| Security | The shared secret is attached to TCP sweep probes sent to every host on the local /24 | `discovery/tcp.go:82-83`; `authtoken/token.go:166-175` |
| Security | Trusted Nix keys are collected from every node in the node store, and are not bound to an authenticated identity | `daemonapi/server.go:1578-1615` |
| Security | The legacy closure import uses `--no-check-sigs`, and the submitter's own upload uses that format | `daemonapi/execution.go:1029-1066` |
| Security | The QUIC hello nonce is chosen by the signer and not bound to the session | `direct/transport.go:61-77` |
| Setup | The firewall commands run without `sudo` | `setup/network.go:47-51, 100-124` |
| Setup | `nix-imports` aborts when the keys file is empty, which happens before the daemon that writes it has started (fresh multi-user Nix) | `setup/setup.go:206-215, 420-454` |
| Setup | Keys appended to `nix.conf` are a snapshot; peers added later are not trusted until setup runs again | `setup/setup.go:250-253` |
| Private store | `pathExists` checks the host `/nix/store`, not the private store | `daemonapi/execution.go:191-194` |
| Temp files | `closurediff.go` still uses `os.CreateTemp("", …)` | `daemonctl/closurediff.go:61` |
