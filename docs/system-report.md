# PipedPeer: system report

Source material for the major project report and the conference paper. Written
against commit `4c91ef3` (2026-09-01) and checked against the code on
2026-10-02; extended on 2026-10-12 for the leftovers and demo rounds
(`d2139f2`..`c4f7800`), marked [Oct-12] where a result is new; extended on
2026-10-15 for the address-book phase after the college demo (4.15, 5.12,
Appendix B), where numbers still to be measured read **TODO(verify)**. Every
non-obvious claim carries a file reference relative to `src/`.
Every number in Chapter 5 traces to a file in `docs/benchmarks/raw/`, or to a
commit whose message recorded the measurement. `raw/C_commit_measurements.md`
quotes those messages verbatim.

**Read this first.** Two earlier descriptions exist, and both are now wrong in
places:
- **The May 2026 report** (`Major_Project-4-1.pdf`) describes a system that no
  longer exists.
- **The August 2026 version of this document** (commit `b774de8`) predates:
  - the shared secret, pinned TLS and node keys;
  - joining across networks by hole punching;
  - signed closure transfer;
  - cloudpickle shipping for Pool kernels;
  - throughput-based work sharing;
  - daemon-side gradient averaging;
  - memory budgets;
  - rootless operation.

  It also described the trusted-LAN, no-authentication design that those
  changes replaced.

Appendix B tabulates both sets of differences, and those since October 11.

---

## Front matter

| | |
|---|---|
| Title | PipedPeer: Zero-Configuration Distributed Computing via Recursive, Decentralised Coordination |
| Authors | Yateen Vaviya (2023300251), Vinayak Yadav (2023300264), Sahil Gupta (2023300072) |
| Guide | Prof. Rupali Sawant |
| Department | Computer Engineering |
| Institute | Bharatiya Vidya Bhavan's Sardar Patel Institute of Technology, Munshi Nagar, Andheri-West, Mumbai 400058 |
| University | University of Mumbai |
| Year | 2026-2027 |

**Keywords.** Distributed computing, peer-to-peer, NAT traversal, transparent
interception, Nix, content-addressed caching, OCI sandboxing, cost-based
scheduling, Python.

---

## Chapter 1  Introduction

### 1.1 Problem statement

A developer with a laptop, a lab workstation and a desktop at home has plenty of
idle compute and no practical way to use it together. The frameworks built for
this (Spark, Ray, Dask) all assume someone has already configured a cluster:
- a head node;
- matching software on every worker;
- a scheduler process;
- a network on which every machine can reach every other.

The last requirement is the one ordinary machines fail. Each sits behind its own
home or campus router, so none of them accepts an inbound connection.

The usual fallback is `ssh` plus `rsync`. It means reproducing the environment
by hand on every machine, a VPN or port forwarding to reach them, and no
scheduling, isolation or fault tolerance.

Three problems sit underneath this, and they are usually solved separately:

1. **The environment problem.** A script needs an interpreter and a specific set
   of libraries. Making the remote machine match the local one is the bulk of
   the work.
2. **The code problem.** Using more than one machine normally means rewriting
   the program against a cluster API, even when the environment matches.
3. **The reachability problem.** Machines on different networks cannot connect
   to each other without a VPN, port forwarding or a relay that carries every
   byte.

PipedPeer addresses all three without asking the user to change the program:
- **Environment:** reproduced exactly by shipping a Nix closure.
- **Code:** distributed without modification by intercepting the parallel
  primitives the program already uses.
- **Reachability:** machines on different networks connect directly. A small
  introducer tells each where the others are, and then each punches a path
  through its own router.

### 1.2 Objectives

1. Run an unmodified Python script on the best available machine, with results
   appearing in the user's working directory as if it had run locally.
2. Reproduce the script's environment exactly on that machine, and send only
   the store paths that machine lacks.
3. Choose the machine from live capacity: architecture and GPU as hard
   requirements, then load, memory and hardware as a ranking.
4. Distribute the parallel work inside the script across the cluster with no
   code changes, only when doing so is expected to be faster, and in proportion
   to how fast each machine has been measured to be.
5. Join machines on different networks into one cluster with one command, with
   no VPN, no port forwarding and no relay in the data path.
6. Protect a node's API with a shared secret and its traffic with encryption,
   and survive the loss of any worker without losing the run.
7. Require no manual installation on a new machine: one binary, whose `setup`
   installs the rest, without root if necessary.

### 1.3 Scope

In scope and implemented:
- the daemon and CLI;
- LAN discovery;
- joining across networks:
  - an introducer;
  - NAT classification;
  - router port mapping;
  - UDP hole punching;
  - direct QUIC;
- the shared secret, pinned TLS and node identity keys;
- signed closure transfer;
- placement, leasing and measured work sharing;
- closure build and transfer with path-level diffs;
- sandboxed execution with per-job memory limits;
- result sync;
- the interception shim:
  - `multiprocessing.Pool` and `ProcessPoolExecutor`;
  - pandas;
  - numpy;
  - joblib and scikit-learn;
  - torch data-parallel training;
- GPU-aware placement;
- a live TUI.

Out of scope in the current implementation:
- per-user authorisation (one shared secret per cluster);
- runtimes other than Python;
- a relay for peers no direct path can reach (such a peer is reported and
  routed around);
- any incentive mechanism.

### 1.4 Technologies

| Technology | Role |
|---|---|
| Go 1.25 | Daemon, CLI and coordinator, in one static binary |
| `go-chi/chi` | HTTP routing for the daemon API |
| `gorilla/websocket` | Streaming a job's stdout and stderr back live |
| `quic-go` | Encrypted, multiplexed peer links over the punched UDP socket |
| Go `crypto/tls`, `crypto/ed25519` | Pinned TLS on the daemon port; node identity keys |
| Nix (pinned nixpkgs) | Reproducible, content-addressed environments; closure signing |
| `crun` | OCI sandbox for each job (installed into the user's `~/.local/bin` if absent) |
| cgroup v2, `systemd-run --user` | Memory ceilings for the daemon and for each job |
| zstd (`klauspost/compress`) | Workspace and results archives |
| `modernc.org/sqlite` | Node store; pure Go, so the binary stays static |
| `gopsutil` | CPU, memory and process metrics |
| Cobra | CLI structure |
| Bubble Tea, Lip Gloss | Dashboard TUI |
| `zerolog` | Structured logging |
| Python (embedded shim) | The interception layer: Python modules under `nixgen/shim/`, embedded with `go:embed` |
| cloudpickle | Added to every generated environment, to ship lambdas and closures by value |

### 1.5 Assumptions and constraints

- **One binary.** Every participating machine runs the same binary. `pipedpeer
  setup` installs Nix and `crun` if they are missing, and it can do so without
  root, using a private Nix store (4.7).
- **Linux workers.** Workers must run Linux, because `crun`, cgroups and the
  Nix store are Linux-specific. The submitting side builds for macOS as a
  client (the CI build matrix covers linux and darwin, amd64 and arm64).
- **Shared secret.** Machines in a cluster share one secret. The first
  machine's `setup` creates it before the daemon first starts, and
  `pipedpeer connect <code>` copies it to the others (4.3, 4.15).
- **Introducer.** Joining across networks needs one reachable introducer.
  Every daemon is one for its own cluster, so any member that the joining
  machine can reach will do; a public server can run as an introducer that
  takes no jobs. It never carries job data.
- **A direct path must exist.** When two machines have no possible direct path,
  for example two carrier-grade NATs that both allocate a new port per
  destination, they cannot work together. The system says so and routes around
  that peer. It does not relay.
- **The submitter must stay alive.** The submitting process must stay alive for
  the duration of a job, because it holds the lease and receives the stream.

---

## Chapter 2  Literature survey

The survey covers six areas:
- sandboxing and environment portability at the edge;
- messaging and actor-style fault tolerance;
- failure-transparent programming models;
- Python portability;
- scheduling;
- collaborative offloading.

Sixteen papers are reviewed in the report proper. Two changes since the August
version affect the "bearing on this work" paragraphs:
- **Authentication.** The system now has a shared secret and encrypted links,
  so the paragraph saying it "has no authentication at all" is rewritten to
  describe what it has and what it lacks.
- **Measured placement.** Placement of parallel work is now measured rather
  than estimated.

The transport choices in Chapter 4 also rest on standards and reference works
that the report now cites directly:
- QUIC (RFC 9000) and TLS 1.3 (RFC 8446);
- STUN (RFC 8489) and NAT behaviour discovery (RFC 5780);
- NAT-PMP (RFC 6886), PCP (RFC 6887) and UPnP IGD;
- Ford, Srisuresh and Kegel on UDP hole punching;
- Ed25519;
- zstd (RFC 8878).

---

## Chapter 3  Analysis

### 3.1 Architecture

Every machine runs one long-lived **daemon**. The CLI is short-lived and holds
no peer-to-peer logic of its own. The **coordinator is a library linked into the
CLI**, not a service, so there is no scheduler process and no head node anywhere
in the system. A machine is a submitter and a worker at the same time, and the
same binary does both. Every daemon is also an **introducer** for its own
cluster (4.2). Two optional roles break the symmetry: `pool-only` (a phone that
takes Pool chunks but no whole jobs) and `introducer-only` (a hosted server
that introduces and runs nothing).

See `fig_3.1_system_design.png`.

The subsystems, all under `src/internal/` (plus `main`):

| Package | Responsibility |
|---|---|
| `coordinator` | Filter, score and choose a node; take a lease; retry |
| `daemonapi` | HTTP and WebSocket surface, leases, sandbox, pool spill, DDP reduction and the one-GPU-or-split decision, closure caches, node poller and address gossip |
| `schedule` | Makespan-optimal division of work across measured devices |
| `ddpplace` | Choosing the ranks of a training ring by measured throughput |
| `discovery` | LAN: UDP multicast and broadcast, TCP `/health` sweep fallback |
| `internet` | Peer manager for joining across networks: register with every introducer, punch, link, migrate, forward; serves the in-daemon introducer |
| `direct` | Shared UDP socket, candidate race, birthday-collision fallback, QUIC links |
| `rendezvous` | Introducer address book and punch-request forwarding |
| `nattype` | STUN client and NAT mapping and filtering classification (diagnostics) |
| `portmap` | PCP, NAT-PMP and UPnP-IGD port mapping |
| `clustercfg` | `cluster.json`: token, introducers, roles |
| `invite` | The `pp1-` invite code (secret + introducer addresses) |
| `authtoken` | Shared-secret middleware and outbound injection |
| `tlsid` | Daemon certificate made from the node key, pins keyed by node id, TLS and HTTP on one port |
| `identity` | ed25519 node key; node id = its fingerprint |
| `nixsign` | Signing closures with the node key; the trusted-key list |
| `narpack` | Signed closure archives; LRU pruning of closure caches |
| `nixstore` | System or private (rootless) Nix store |
| `nodestore` | SQLite address book: every path per node id, last observed capacity |
| `nixgen` | Flake generation, wheel pins, and the interception shim (`shim/*.py`) as embedded files |
| `pythondeps` | Import scanning and import-to-package mapping |
| `resourceest` | Tiered memory estimate |
| `cgroups` | Daemon and per-job memory ceilings; the daemon's memory budget |
| `tarcodec` | zstd, gzip or plain archive sniffing |
| `heartbeat`, `gpu` | Capability and load reporting; vendor GPU detection |
| `jobhistory`, `runrec` | Per-job records; the run record (timed steps, simple and detailed views, export) |
| `service`, `warmup`, `doctor`, `phone` | Autostart user unit; background build of common environments; diagnostics with fixes; Termux pool worker |
| `daemonctl`, `setup`, `userdir`, `userns`, `netaddr`, `logging`, `say`, `term`, `app` | Supporting |

Large packages are split by responsibility; one line per file:

| File (under `src/`) | Responsibility |
|---|---|
| `ddp_cmd.go`, `ddp_plan.go` | `runDDP` (the CLI side of a training run); rank weights, uneven splits and the lead choice |
| `internal/coordinator/coordinator.go`, `find.go`, `nodes.go`, `place.go`, `execute.go` | the `Coordinator` type and `FindNodeWithin`; filtering and rejection reasons; node list, `scoreNode` and `scoreGPUNode`; lease request, commit and `PlaceWithRetry`; `ExecuteWithRetry` and its back-off |
| `internal/daemonctl/execution.go`, `upload.go`, `workspace.go` | streaming an execution and fetching results; job upload and closure send; workspace tar and NAR export |
| `internal/internet/internet.go`, `connect.go`, `hints.go`, `link.go`, `introducer.go` | the `Manager` and its loop; interface watching, redial and the connect race; candidate hints and the router-mapping check; link lifecycle, forwarders and no-path back-off; the in-daemon introducer |
| `internal/daemonapi/server.go`, `serverhttp.go`, `serverlease.go`, `serverclosure.go` | `Server` construction and constants; router, admission and trusted-key refresh; leases, persistence and the sweeper; closure broadcast to peers |
| `internal/daemonapi/execrun.go`, `execsupervise.go`, `execupload.go`, `execresults.go`, `execution.go` | `handleJobExec` and the OCI bundle; memory cap, output streaming and cancel; workspace upload; results archive and NAR import; shared helpers and the GPU driver directory |
| `internal/daemonapi/pool.go`, `poolhandlers.go`, `poolrun.go`, `poolremote.go`, `poolworker.go`, `poolwire.go`, `poolcompress.go`, `poolscripts.go` | the pool manager state; `/v1/pool/*` handlers and body limit; chunk placement and peer choice; remote runs and racing; local worker processes; frame wire format; per-link compression; the embedded worker Python |
| `internal/daemonapi/ddpsync.go`, `ddpboard.go`, `ddprequest.go`, `ddplocal.go`, `ddppacing.go`, `ddpreduce.go`, `ddprebalance.go` | one DDP sync round trip; the per-run board (expected ranks, drops); request parsing; the single-machine path; pacing and per-rank times; the weighted reduction; rebalance and the DiLoCo plan |
| `internal/daemonapi/nodes.go`, `nodespeers.go`, `nodespoll.go`, `narcache.go`, `narimport.go`, `narsubstitute.go`, `cas.go` | `/v1/nodes` and staleness; peer add, remove and path verification; the poller; the signed-archive cache; store import and missing-path checks; substitution from cache.nixos.org; content-addressed workspace files |
| `internal/nixgen/shim/pipedpeer_shim/pool.py`, `pool_func.py`, `pool_pacing.py`, `pool_race.py`, `pool_resources.py` | the `Pool` drop-in; function payloads and chunking; pacing and cancellation; racing local against remote (and the joblib backend); pool width and memory probes |
| `.../pandas_ops.py`, `pandas_frame.py`, `pandas_combine.py`, `pandas_ooc.py` | install hooks and shuffles; the partitioned frame; combining partial aggregates; out-of-core reads |
| `.../ddp.py`, `ddp_state.py`, `ddp_sync.py`, `ddp_ring.py`, `ddp_loader.py`, `ddp_fast.py`, `ddp_wire.py`, `ddp_diloco.py`, `ddp_sampler.py` | the DDP interception and install; the run-state object; every sync with the lead daemon; ring changes and releases; DataLoader wiring; the fast-sync driver; wire pieces (exit codes, int8 codec, frames); DiLoCo schedule and outer step; weighted batch split and shard sampler |

The relay code, the NATS transport, the standalone registry and the ping
package were deleted in the address-book phase. No job traffic is relayed
(4.2).

### 3.2 Data flow

**Level 0** (`fig_3.2_dfd_level0.png`) has four external entities:
- the user;
- peer nodes;
- introducers (other members, or an optional hosted one).

**Level 1** (`fig_3.3_dfd_level1.png`) decomposes the daemon into seven
processes:
- resolve environment;
- find and link peers;
- place task;
- transfer;
- execute;
- distribute parallel work;
- return results.

They work over four stores:
- the node store;
- the closure cache;
- job history;
- the identity and trust store (node key, pinned certificates, trusted Nix
  keys).

### 3.3 Use cases

See `fig_3.4_use_case.png`.

- **User:** runs a script, asks for a GPU or a memory floor, trains with data
  parallelism, and inspects jobs and nodes.
- **Operator:** runs the installer (which runs `setup`), prints an invite on the
  first machine and runs `connect` with it on the others; `doctor` when
  something is wrong, `uninstall` to clean a machine.
- **Peer daemon:** accepts and leases work, serves closures to other peers, and
  executes parallel chunks.
- **Introducer:** registers nodes and forwards punch requests.

### 3.4 Sequence

See `fig_3.5_sequence.png`. The full path for one job:

1. The CLI scans the script's imports and builds a Nix closure locally.
2. It estimates the job's memory requirement.
3. It asks its own daemon for the healthy peer list. That list includes peers
   linked across networks, which appear as local forwarders (4.2).
4. The coordinator filters and scores, then sends `accept` to the best node,
   which checks capacity and reserves it.
5. On acceptance it sends `commit`, which turns the reservation into a running
   lease.
6. It asks the worker whether the closure is runnable there. If not, it asks
   which store paths are missing and sends only those. If that diff is declined,
   it sends the whole closure.
7. It uploads the workspace.
8. The worker pushes the closure to peers that lack it, so they become eligible
   to receive spilled work. Peers that advertise signed closures receive a
   signed archive.
9. Execution runs in a `crun` sandbox under a per-job memory ceiling. stdout and
   stderr stream back per line over a WebSocket.
10. The shim inside the job distributes parallel work if the cost model agrees.
11. On exit, changed files and a deletion manifest come back as a zstd archive.

Every request in this sequence carries the shared secret when one is set. It
travels over pinned TLS on a LAN, or inside a QUIC link across networks.

---

## Chapter 4  Design and methodology

### 4.1 Discovery on a LAN

Three sources feed one table, and none of them blocks the others.

**UDP, not mDNS.**
- `discovery/discovery.go` opens two sockets on port **4242**/udp:
  - one joined to multicast group **239.255.0.99** (:46);
  - one bound for broadcast.
- Both use `SO_REUSEADDR` and `SO_REUSEPORT`.
- The probe is the literal `PIPEDPEER_DISC_V1` (:47). The reply is that header
  followed by a JSON descriptor.
- The multicast socket exists because a UDP broadcast to a shared port is
  delivered to only one socket. Without it, two daemons on the same host would
  never see each other.
- This is not mDNS: there is no DNS-SD record. The dashboard now labels the
  source "broadcast".

**TCP fallback.** Access points with client isolation drop broadcast without
error. When the UDP probe returns nothing, `discovery/tcp.go` sweeps each
local interface's prefix from its network address (docker, veth, bridge and
tailscale interfaces skipped), with 64 probes in flight and a 700 ms
`GET /health` per address, and stops issuing probes once any daemon answers.

**The host is the packet's source.** A reply's advertised address was fixed
when the peer's daemon started, so a peer that moved was reported at its old
address forever (experiment E4). Discovery now takes the host from the UDP
source of the reply.

**Address book.** `nodestore/store.go` keeps, in SQLite, every node by node id
(the fingerprint of its key) and every **path** to it: kind (`lan`, `internet`,
`gossip`, `self`), host, port, last seen, last success, last error
([ADR-0019](adr/0019-address-book.md)). A sighting adds a candidate and never
moves the active path; a path becomes active only after the poller reached it
and the certificate's key matched the node id, and an active path that still
answers is kept (a verified LAN path beats a verified internet path). This
stops the overwrite experiment E5e found, where LAN discovery replaced a
working internet link with an unreachable LAN address. The poller runs every
10 s with a 3 s timeout and tries the active path, then the other candidates,
LAN first. Pruning: a node with no verified path for 15 min goes, a
never-verified discovery or gossip node after 2 min, LAN paths unseen for
10 min and internet or gossip paths unseen for 1 h; a sighting alone never
refreshes a node, so the stale rows of the demo can no longer live forever.

**Signed address gossip.** Each member serves its own addresses at
`GET /v1/addresses`, signed with its node key and numbered; the poller pulls
the records from healthy peers at most once a minute. A record is only a
candidate (and a hole-punching hint), the highest number per node wins, and a
bad signature is dropped with a log line.

### 4.2 Joining across networks

A LAN broadcast cannot reach a machine behind another router, and that machine
will not accept an inbound connection. `fig_4.1_nat_traversal.png` shows how
two such machines come to share a direct, encrypted link.

**One command per machine.**
- `pipedpeer invite` prints one line, `pipedpeer connect <code>`. The code is
  `pp1-` + base64url of the cluster secret and up to six introducer addresses
  (this machine's LAN addresses, known introducers, and the public address
  given with `setup --public-address`, if any) + a CRC-32, so a mistyped code is refused as damaged
  (`invite/invite.go`).
- `pipedpeer connect <code>` saves the secret and introducers to
  `cluster.json` (`clustercfg`), restarts the daemon once, waits up to 60 s on
  `/v1/introducers` and `/v1/nodes`, and ends with one of three results:
  connected (with the machines it sees, exit 0), partly (per machine, the
  reason and the fix, exit 2), or failed (code damaged, no introducer answered,
  or secret rejected, each with what to do, exit 1).
- The daemon reads `cluster.json` itself, so a daemon restarted by `run` after
  a reboot rejoins (audit B4), and it remembers verified member addresses as
  further introducers (at most 8).

**Introducers are address books, not relays, and every daemon is one.**
- Every daemon with a token serves the introducer on UDP **4244**, answering
  only its own cluster id (`rendezvous/rendezvous.go`, `ServeIntroducer` in
  `internet`). A node registers with every introducer it knows every ~30 s and
  at once after a local address change, and keeps per introducer the last
  reply, last error and member count (`GET /v1/introducers`); one that has not
  answered for 60 s is logged once. Before this, one standalone introducer was
  the cluster's single point of failure: when its IP changed, the cluster never
  recovered and nothing said why (experiment E3b).
- A public server can join as `pipedpeer start --introducer-only`: it
  introduces and takes no jobs ([hosted-introducer.md](hosted-introducer.md)).
- A node registers its candidate addresses under a cluster ID derived from the
  secret: `sha256("pipedpeer-rendezvous\0" + token)`, first 8 bytes. The
  introducer therefore never holds the secret.
- A `connect` request makes the introducer forward one punch datagram to the
  target. That datagram carries only the addresses the caller itself
  registered. `connect` is rate-limited per source (4 per second, burst 8).
- The introducer never carries job data. Over a whole verification run the
  introducer host sent 0.2 MB, while about thirty megabytes crossed between the
  peers (commit `42c2426`).

**Candidates.** Each node offers three kinds of candidate address
(`direct/direct.go:48-69`):
- `lan`: its LAN addresses;
- `mapped`: a port its router agreed to forward;
- `reflex`: the address the introducer saw.

The router is asked through PCP, then NAT-PMP, then UPnP-IGD
(`portmap/portmap.go`), for a 2 h lease that is renewed at half the lifetime
actually granted. A mapping is withheld if its external IP differs from the one
the introducer saw. Only the IP is compared, and on the first poll, before any
reflexive address is known, the mapping is published unchecked
(`internet/hints.go:121-147, 299-316`). The check exists because a mapping on
a router behind a second NAT, such as a phone tether, would otherwise be
published as reachable when it is not.

**One socket.**
- Punch probes, introducer messages and QUIC all share one UDP socket on port
  **4243** (`direct/sharedconn.go`).
- A router's mapping belongs to the socket that opened it, so the connection
  must arrive on the same socket the probes left from.
- Datagrams are told apart on arrival:
  - probes by four magic bytes;
  - introducer messages by a leading `{`;
  - everything else is QUIC.

**The race.**
- Both sides probe every pairing of their candidates every 250 ms, for a 15 s
  window (`direct/direct.go:386-433`; `internet/connect.go:238-239`).
- Trying candidates one at a time would add a timeout per address that cannot
  work.
- When the race fails because one side's NAT allocates a new port per
  destination, the **birthday collision** fallback runs (`direct/birthday.go`):
  - that side opens 256 sockets toward the peer;
  - the other sends 600 probes to random ports between 1024 and 65535;
  - the socket that is hit is the one used.

**The link.**
- The winning path carries QUIC (ALPN `pipedpeer-direct/3` since the address-book phase, TLS 1.3).
- Both ends then exchange a hello signed with their ed25519 node key, and each
  checks:
  - that the peer's key fingerprint is the node it meant to reach;
  - that the cluster ID matches (`direct/transport.go`).
- TLS on this link skips certificate checks, so a node's identity rests on the
  hello. Until `b5b943c` the signature covered only the cluster and a nonce the
  signer chose, so a peer we dialled could replay our hello to a third node and
  be taken for us. The hello now signs the TLS exporter value of the session it
  travels on, and proves the cluster secret with an HMAC over the same value:
  it is good for one session and one cluster only. A node on an older build is
  refused and told why (ADR-0006). The punch nonce, which came from a
  zero-seeded stream and was the same eight bytes every time, now comes from
  `crypto/rand` (`edba934`).
- The link is then exposed to the rest of the daemon as a TCP forwarder on
  `127.0.0.1:<port>`, recorded as the `internet` path of that node id.
  Each local connection becomes one QUIC stream to the peer's daemon. The
  scheduler, the closure transfer and the gradient sync therefore all work over
  the link without knowing it exists.
- `pipedpeer status` shows how each peer is reached in a PATH column (`self`,
  `lan`, `internet`).

**Roaming** (experiments E3a, E3b). A peer that changed IP used to come back
only after about 3 minutes, because the other side kept the dead link and
refused the new one as "already connected". Now a hello from a node key that
already has a link replaces the old link at once. The daemon polls its
interfaces every 5 s; on a change it migrates the QUIC connections it dialled
to the new local address (quic-go path probing) and redials the ones it
accepted, and re-registers with every introducer.

**No relay.**
- A peer with no direct path is not relayed. It is logged with the reason, never
  handed to the scheduler, and retried after a back-off (`internet/link.go:137-160`).
- A relay was built (commit `a6e0723`) and then removed from the data path
  (`95e0712`), because it would carry every byte of every job.
- A live link is never expired because the introducer went away. A peer was
  reached in 0.03 s with the introducer suspended (`42c2426`).
- `pipedpeer status` lists each peer with no direct path under the table, with
  its reason (`8b13704`), including when no peer at all is reachable
  (`36d4923`); the reason lives on the node's row.

**What the NAT lab found** (October 12). Until `lab/nat` (4.13) the punching
code was exercised only at live demos. The lab showed peers behind NAT rarely
linking, for four reasons, all fixed:
- The socket a birthday collision found listened under the collision's
  context, which ends when the socket is adopted, so the peer's QUIC dial met a
  closed listener (40 refusals on one node, 77 on another). It now accepts dials
  for 30 s and stays open while any link it accepted lives (`8944270`).
- The same socket kept the search's 10 s read deadline, so after adoption every
  read failed: dials timed out and links died about 90 s later. The deadline is
  cleared on adoption (`bb007c8`).
- A node's first registration went out before it knew its public address, so a
  woken peer punched at a private address on another network. A node now
  registers again as soon as it learns its public address, before it asks
  anyone to punch (`8944270`).
- The address the race returned was IPv4-mapped and never equalled a
  candidate, so an ordinary punch was labelled a collision; and a dead link's
  teardown could remove the link that had replaced it (`bb007c8`).

### 4.3 Security and identity

The August design assumed a trusted LAN and had no authentication. The current
one has three layers. `fig_4.2_trust.png` shows them.

**Shared secret** (`authtoken/token.go`).
- The first machine's `setup` (or `invite`) generates 32 random bytes before
  the daemon first starts, and `connect <code>` stores the code's secret; both
  go into `cluster.json` with mode 0600 (`clustercfg`, `authtoken.Set`). An old
  `auth_token` file migrates on first read.
- Every route on the daemon, `/health` included, requires it in
  `X-Pipedpeer-Token` or as a bearer token. The comparison is constant-time and
  a mismatch is a 401 (`authtoken/token.go:164-167`).
- Outbound requests to `/health` and `/v1/*` carry it automatically, but only
  to a host that has proved it holds it (`4885a42`). The LAN sweep used to send
  the token in plain HTTP to every host on the /24 that answered on the port.
  The token transport now challenges a remote host first and attaches the token
  only after an HMAC over the challenge comes back; a proof is remembered for
  the life of the process, and loopback is exempt. Older builds cannot answer
  the challenge.
- Jobs receive it as `PIPEDPEER_TOKEN`, so that the shim can reach its own
  daemon. A consequence is that any job's code can read the secret.
- The CLI never starts a daemon without a secret, so the old open behaviour no
  longer happens in practice.
- `pipedpeer ui` opens the page with the token in the URL fragment
  (`#token=…`), which browsers never send; the page stores it and removes it
  from the address bar. There is no loopback exemption, because internet peers
  reach the daemon through loopback forwarders.

**Pinned TLS on the daemon port** (`tlsid/`).
- Each daemon's self-signed certificate is made from its ed25519 node key
  (it was a separate ECDSA P-256 key before the address-book phase).
- The same port serves TLS and plain HTTP: the listener peeks at the first byte
  and treats `0x16` as a TLS handshake (`tlsid/listener.go`).
- Outbound requests to another daemon try TLS first. Pins are keyed by node id
  (`tlsid.Expect(addr, nodeID)`): a certificate whose key matches the expected
  node id is accepted at any address, and a changed key for a known node id is
  refused outright. Pins used to be keyed by `host:port`, so a different
  machine that later got a peer's old IP was refused
  ([ADR-0008](adr/0008-pinned-tls-and-key-addressed-nodes.md)).
- This is trust on first use, and it is **opportunistic**: if TLS fails for any
  reason other than a pin mismatch, the client remembers that peer as plain HTTP
  and falls back. The code says so: an attacker who can break the handshake can
  force plain HTTP (`tlsid/dial.go:16-21`).
- Loopback is never upgraded. Peers linked across networks are reached through
  loopback forwarders, so their encryption comes from QUIC, not from this layer.

**Node identity and signed closures** (`identity/key.go`, `nixsign/`).
- Each node has an ed25519 key with mode 0600. Its fingerprint (first 8 bytes of
  the SHA-256 of the public key, 16 hex) is the node id everywhere: node store,
  direct links, pins. Before the address-book phase the node store keyed rows by
  a random UUID while internet links used the fingerprint, which produced
  phantom duplicate rows (audit B3).
- Signatures are domain-separated by a context string.
- The same key signs closures as a Nix signing key named `pipedpeer-<fp>`.
- Nodes publish the public half in their heartbeat. Each daemon writes the keys
  of the nodes it knows to a `trusted-public-keys` file.
- `setup` adds those keys to the system Nix configuration as
  `extra-trusted-public-keys`, with consent, and **never** writes
  `require-sigs = false` (`setup/setup.go:153-295`; commit `cf2e3ae`).
- A closure sent between daemons in the signed format is imported with `nix copy
  --from`, so the receiving Nix applies its own signature policy. This was
  verified on a machine with `require-sigs = true`: a closure signed by a
  trusted peer imported, and the same closure was refused once the sender's key
  was removed from the trust list (`cf2e3ae`).

**Getting peer keys to Nix** (`8484714`, `983ae2a`, `454754a`, `eeacb55`,
`ee8a1ee`). On a fresh cluster the first job never left the submitter: every
worker refused the closure with "lacks a signature by a trusted key", because
the worker knew the key only in its own file and Nix trusts only what
`nix.conf` lists. Setup had skipped the trust step on single-user Nix in the
belief that it does not check signatures; it does.
- Import passes the cluster's keys to that one `nix copy` with `--option
  extra-trusted-public-keys`. Signatures stay required and no machine
  configuration is touched.
- An archive's manifest names its signer and key. The receiver checks the key
  against the signer's published identity (`nixsign.Verify`, on a request that
  is already token-authenticated) and trusts it for that import, so a job sent
  right after joining is not refused for want of a poll.
- A multi-user nix-daemon ignores keys passed per command by a user it does not
  trust. For that case `setup`, with its one sudo, installs a root-owned copy of
  the binary at `/usr/local/libexec/pipedpeer-nix-trust` and a sudoers rule that
  lets the user run only that file (the **key helper**). Run as that file,
  checked by inode before anything else in `main`, the binary only adds or
  removes keys of the form `pipedpeer-<16 hex>:<ed25519 key>`, the name derived
  from the key, each in its own include file, and restarts nix-daemon once per
  change. The daemon calls it with `sudo -n` for a key system Nix does not yet
  trust, before the import, and retries a failure every 15 minutes. `setup`
  and `connect` take the same path; `uninstall` removes it. Which Nix needs what is decided in one place, `nixsign.SystemUntrusted`.
- A refused import is returned to the submitter and printed.
- Setup prefers the reliable Nix: one already installed, else the Determinate
  installer when it can get root, else the private store, which fails on arm64
  and under Ubuntu's user-namespace restrictions (`454754a`). Its firewall
  changes run through sudo when the process is not root (`9b68937`).

**What this does not cover**, stated plainly:
- **One shared secret.** There is no per-user or per-node authorisation, and
  every holder can run code on every node.
- **The trusted-key list is not tied to an authenticated identity.** Any node
  in the node store contributes its key, including one found by unauthenticated
  LAN discovery (`daemonapi/serverhttp.go:160-209`).
- **The key helper trusts on the token's word.** With the helper installed,
  anyone holding the cluster token can get signed paths trusted by system Nix
  on every machine with no human in the loop. That was already true after the
  old manual trust step; SECURITY.md and ADR-0018 say so.
- **One import path skips signature checking.** The older transfer format, which
  the submitter still uses for its own upload, imports with `--no-check-sigs`
  (`daemonapi/execresults.go:173`). Single-user Nix does check signatures on the
  signed path; the belief that it did not was corrected in `8484714`.
- **LAN discovery is unauthenticated.** Discovery accepts any responder as a
  candidate, but the token goes only to hosts that prove they hold it, and a
  path is used only once the peer's key matches its node id.

### 4.4 Placement and scoring

Candidates are merged from:
- the node store;
- live discovery;
- internet-linked peers;
- self.

A node with a `role` (pool-only, introducer-only) is never offered a job.

They are deduplicated by node ID. Then five hard filters run in order
(`fig_4.3_placement_flow.png`):
1. the architecture must match the closure;
2. a GPU must exist if one was required;
3. free memory must meet the request;
4. free cores must meet the request;
5. free VRAM must meet the request.

Each rejection records a reason, which surfaces in `pipedpeer jobs <id> --explain`.

Survivors are scored by `coordinator.scoreNode` (`coordinator/nodes.go:126-158`),
unchanged since August:

```go
score := 1.0
score -= n.Load.CPUPercent / 200.0          // up to -0.50
score -= n.Load.MemPercent / 200.0          // up to -0.50
score -= float64(n.Load.ActiveJobs) * 0.05  // per running job

cores   := normalized(float64(totalCores(n)), 32.0)
mhz     := normalized(capFloat(n, "cpu_mhz"), 4000.0)
freeRAM := normalized(float64(n.Load.AvailableMemBytes)/(1<<30), 32.0)

score += (cores         - 0.5) * 0.10
score += (mhz           - 0.5) * 0.06
score += (freeRAM       - 0.5) * 0.10
score += (freeCoreRatio - 0.5) * 0.10

score *= n.HealthScore
```

Two properties matter:
- **Load dominates.** The two load terms can remove 1.0 between them, while the
  four hardware terms move the score by at most 0.36.
- **A missing value is neutral.** `normalized` returns 0.5 for a missing value,
  so uneven telemetry neither rewards nor punishes a node.

See `fig_4.4_scoring_weights.png`. When a GPU is wanted, `scoreGPUNode` adds up
to 0.8 more from VRAM, device count, free VRAM, compute capability and a
utilisation penalty (`coordinator/nodes.go:171-209`).

One thing did change: what a node reports as free memory. `/health` now
advertises what the node can actually *grant*, which is its memory budget less
existing reservations (4.8), rather than the machine's free memory.

**Placement within one run** (`1e07d56`).
- A node that refuses the job's signed closure is left out for the rest of the
  run, naming the node; the run fails only when no
  node is left. A refusal is that node's missing trust, not every node's, so it
  does not count as a failed attempt. A DDP ring still treats it as permanent.
- This machine's memory ceiling comes from its own daemon's `/health`
  (`mem_ceiling_bytes`), read once per run, instead of being unknown.
- `--remote` waits up to 10 minutes for a node with room (it was 15 s). Every
  30 s it says why each node is busy (under `-v`). A job that can never fit any
  node, or whose script does not exist, fails at once: the script check now
  runs first on every path, after a mistyped name was placed on three nodes at
  the demo.

### 4.5 Leases

Placement is a three-phase handshake. Its state machine
(`fig_4.5_lease_fsm.png`) is where crash recovery lives.

```
accept  -> Reserved   (capacity held, expires after 30 s + 5 s grace)
commit  -> Running    (renewed every 30 s, reaped after 5 min without renewal)
complete/cancel -> released
```

The constants are `DefaultLeaseDuration = 30s`, `DefaultGracePeriod = 5s`,
`DefaultSweepInterval = 2s` and `DefaultRunningLeaseTTL = 5m`
(`daemonapi/server.go:28-35`).

The concurrency and memory checks and the lease insertion happen under one
lock, so two racing submitters cannot both take the last slot. The cores check
runs just before the lock is taken (`serverhttp.go:309-319`).

`/v1/accept` checks, in order:
1. cores;
2. the concurrency limit, counting reserved and running leases together;
3. memory against the daemon's budget;
4. GPU devices.

Recovery follows from the design:
- **A dead submitter** stops renewing, and its slot is reclaimed.
- **A dead worker** is marked unreachable at the next failed poll. The in-flight read
  fails, and the coordinator reschedules with back-off doubling from 2 s to a
  2 min ceiling (`coordinator/execute.go:141, 205-224`). Waiting for room never drops a
  task, but a job that *fails* is run at most three times, on nodes not yet
  tried first, and then returns its last error; it used to retry a signal,
  exit 126/127 or a sandbox failure forever (`1e07d56`).

### 4.6 Reproducing the environment

**Import scanning is a line-oriented parser.**
- The August version used a single regular expression, which kept only the
  first name of `import a, b`.
- `pythondeps/deps.go:177-199` now parses each line by hand:
  - `#` comments are stripped;
  - `from X import ...` yields `X`;
  - `import a, b as c` yields both `a` and `b`;
  - relative imports are ignored.
- It is still line-oriented. It does not see:
  - imports split across continuation lines;
  - `__import__` or `importlib` calls.
- It can also pick up import-like text inside a multi-line string.

**Classification.** Each name is classified as:
- a local file in the script's directory;
- a standard-library module (a list of 124);
- an external dependency, mapped to a nixpkgs attribute through a 40-entry
  table (`sklearn` to `scikit-learn`, and so on; tqdm, seaborn, sympy, networkx
  and statsmodels were added in `15d37b2`).

**Unknown imports are looked up, not dropped** (`2c91f14`).
- An import the table does not know is tried as the `python3Packages`
  attribute of the same name in the pinned nixpkgs, with one `nix eval` for all
  of them, and added when it exists.
- Only the imports still without a package are reported, before anything runs:
  `Not packaged (the job will fail to import it): X — add it with --pkg
  <nixpkgs name>` (`app/env.go:149-151`). A failed eval says so and falls back to
  the warning. Until `15d37b2` such an import was dropped silently and the job
  died on `ImportError` on the node.
- A `requirements.txt` beside the script is read as if it had been passed with
  `--pkg`, and the run says so in one line (`app/env.go:134-136`).
- `uv.lock`, whose names replace detection, and `--pkg` remain the escape
  hatches.

**The scan follows the script's own modules** (`f3b564d`). Local modules were
always bundled, but until this commit only the main script's imports were read,
so a `helper.py` importing seaborn left the environment without it. The scan now
walks every local module, resolving each against its own directory and then the
script's, with a visited set so that import cycles end. An imported local
package has all of its `.py` files scanned; a module that cannot be read is
reported and skipped.

**Common packages built ahead of time** (`fbc0418`). There is no layered base
environment: what is shared between environments is Nix's own store paths, and a
transfer sends only the paths a peer lacks (4.7). The demo's
`prep-4-common.sh` uses that. It builds one environment that imports the usual
set (numpy, pandas, scikit-learn, scipy, matplotlib, seaborn, tqdm, and CUDA
torch) through `--check-only` on every machine, so a script seen for the first
time shares those store paths and fetches only what it adds. Before and after:
the environment of a teacher-style script built in 54 s without it and 4 s with
it, on a container of the rehearsal lab <!-- evidence v1/15-pc3-pkgprep-before-after.txt (8e14b9b), dry-run build -->.

**The store path is the cache key.** `nixgen/flake.go:35` pins an exact nixpkgs
revision (`56c02bc00adcf003215cc4bd996d6efaf4cff188`). A branch would resolve
differently on different days, so the same script would get a different store
path and the cache would miss. A fixed revision makes the store path a stable
name for an environment across the cluster.

Every generated environment also contains `cloudpickle`, so that Pool kernels
with no source can be shipped by value (4.9). Two pinned exceptions remain, both
for x86_64-linux:
- **scikit-learn** comes from a specific wheel (1.9.0).
- **torch with CUDA** comes from a fixed-output derivation of pinned wheels
  (torch 2.13.0+cu126). This is used only when a GPU is requested. GPU is
  preferred by default when the script imports torch, unless `--gpu off`.
  Otherwise nixpkgs' CPU torch is used.

### 4.7 Closure transfer and caches

See `fig_4.6_closure_cache.png`.

**Two predicates.** A node can be asked two different questions:
- is the archive in its cache?
- is the closure materialised, meaning `<storePath>/bin/run` exists?

The second (`/v1/store?runnable=1`) exists because a shared-store rig has the
closure without a cached archive, and a per-node store has `bin/run` only after
an import.

**Path-level diff.**
- If the target cannot run the closure, the submitter lists the closure's store
  paths and asks `POST /v1/store/missing` which ones the target lacks.
- If some but not all are missing, it sends just those, as a partial import that
  is never cached as the closure (`daemonctl/closurediff.go`).
- Measured results:
  - adding one package to an environment the peer already held sent 7 of 43
    paths, 206.0 MB down to 28.6 MB (`193e1bf`);
  - adding cloudpickle to an existing environment cost 3 of 39 paths, 0.43 MiB
    against a 444 MiB closure (`837f9c2`).

**Signed transfer between daemons.**
- After an upload, the worker pushes the closure to peers that cannot run it,
  because spilled Pool work is offered only to peers that can.
- Peers that advertise the `narpack1` format receive a signed archive
  (`daemonapi/signedclosure.go`, `narpack/`). The worker:
  1. signs the paths with its node key;
  2. publishes them with `nix copy --to file://…?compression=zstd`;
  3. packs every narinfo, plus the NARs for the paths the peer lacks, into one
     tar.
- The receiver recognises the format from the bytes, and imports with `nix copy
  --from` under its own signature policy.
- Peers that do not advertise the format get the older gzip `nix-store
  --export`.
- The submitter's own upload still uses that older format.

**Bounded caches.**
- Both closure caches (the signed binary cache and the archive cache) are pruned
  least-recently-used to `PIPEDPEER_CLOSURE_CACHE_MAX`, which defaults to 8 GiB
  **each** (`narpack/prune.go:32`).
- Pruning runs after every send and every store, and at daemon startup.
- With the cap set to 50 MiB, the caches went from 109 MiB to 28 MiB and from
  768 MiB to 4 KiB, and the next job still ran on the peer (`b19f7fc`).

**Archives.**
- Workspace archives above 1 MiB are zstd-compressed.
- Results archives always are.
- Readers sniff zstd, gzip or plain by magic bytes (`tarcodec/tarcodec.go`).

**Transfer you can see** (`906f707`).
- The submitter times packing the closure and sending it. On a terminal the
  send draws a progress bar (`schollz/progressbar`); otherwise it prints one
  line after the transfer, such as `Sent 7 of 43 store paths: 28.6 MB in 0.6 s`.
- `--cold` sends the whole closure as if the node had none, skipping both the
  cache check and the path diff, so a first run can be timed on a warm cluster.
  It does not undo the submitter's own Nix build or the node's earlier import,
  for which no endpoint exists, and the run summary says `sent (cold)`. True
  first-run cost therefore comes from fresh machines.
- A Nix signature refusal on import is no longer swallowed: the upload returns
  it, and the CLI prints why the peer took no work (`8484714`).

### 4.8 Execution, isolation and memory

**The sandbox.** Jobs run under `crun` in an OCI bundle
(`fig_4.7_sandbox.png`; `daemonapi/execrun.go:401-434`).
- The root filesystem is read-only and minimal.
- Mounts:
  - `/nix` read-only;
  - the workspace read-write at `/work`;
  - a private home read-write;
  - tmpfs for `/dev`, `/tmp` and `/dev/shm`.
- `/dev/shm` is mode 1777 because Python's `multiprocessing` needs it.
- Namespaces: pid, ipc and mount, plus uts where the kernel allows it
  (`sandboxquirks.go:141-147`). When `crun` runs rootless it adds a user
  namespace itself.
- GPU jobs get the device nodes. The host driver libraries are bound **file by
  file** into `/run/opengl-driver/lib`, so the host C library never shadows the
  closure's.

**A memory ceiling per job** (`cgroups/`).
- Each job gets its own cgroup with `memory.max` (and an equal swap limit) of
  twice its estimate, with a 512 MiB floor. The limit is clamped to what the
  node can grant, but never below what admission approved.
- A job with no estimate used to run with no cap at all. It is now capped at
  4 GiB (`app/run.go:311-313, 569`, `acbbfa2`), with a note pointing to `--mem`; the node
  still lowers that to what it can spare.
- If the job is killed and the cgroup's `oom_kill` counter rose, the client is
  told the job ran out of memory and how to raise `--mem`.

**A memory ceiling for the daemon itself.**
- Where a user systemd is available, `pipedpeer start` launches the daemon in
  a `systemd-run --user` scope with the settings below. A daemon without one,
  such as one in a container, gets a watchdog instead (`acbbfa2`, started by the
  daemon since `8e14b9b`): every second it sums the memory of the daemon's
  children and, past half the machine or `PIPEDPEER_MEMORY_MAX`, kills the
  largest child and logs it, never the daemon. It stands down where a cgroup
  already sets a limit. An allocation fast enough to outrun a one-second poll
  is not caught; a real scope is the fix for that. The scope settings:
  - `MemoryMax` at half the machine's RAM;
  - `MemorySwapMax=0`;
  - no `MemoryHigh`.
- `PIPEDPEER_MEMORY_MAX` overrides the ceiling.
- `MemoryHigh` was rejected by measurement. A 3 GiB allocation under a 1 GiB
  `MemoryHigh` stalled at 998 MiB with 7,381 throttle events and no progress for
  minutes (`96c7f35`).
- Daemons on the same machine, identified by the kernel boot ID, subtract each
  other's reservations through `/v1/reserved`. Without that, two daemons on one
  host would each promise the same memory. Measured on one machine:
  - before: 7.24 GiB capped plus one unbounded container daemon on 13.49 GiB;
  - after: 6.74 GiB plus 1.97 GiB (`15b98b2`).

**Rootless operation.**
- If there is no system Nix, `setup` seeds a private store under
  `~/.local/share/pipedpeer/nix` from the Nix 2.31.3 release. That store is
  bound at `/nix` inside a user and mount namespace (`nixstore/`).
- `crun` is installed into `~/.local/bin` if no package manager supplies it.
- Working files live under XDG directories rather than `$TMPDIR`, which is often
  RAM-backed (`userdir/`).

**What the sandbox does not do**, stated plainly:
- The job runs as uid 0 inside the container.
- It shares the host's network: there is no network namespace.
- The spec sets no seccomp filter and no capability list.
- Isolation is the default (`--isolate`), but it can be turned off:
  - the `python` shorthand turns it off;
  - a node without `crun` or user namespaces runs the job **unisolated** with a
    warning, unless it uses a private store, in which case it refuses.

**Results** are diffed: only files created or modified during the run return,
with deletions carried in a manifest. On extraction:
- the submitter refuses any path that escapes its directory;
- the worker skips such paths in an incoming workspace.

**Output is forwarded raw** (`d961dbb`). The daemon used to split a job's output
into lines, so a progress bar that redraws one line with `\r` showed only its
final state. It now forwards each read of up to 4 KB unchanged, carrying an
incomplete trailing UTF-8 character to the next chunk, and the submitter prints
the bytes as they come. A tqdm bar on a remote node animates on the submitter's
terminal.

**Stopping a job** (`d961dbb`, `dcfa6f5`, `d28c69e`, `d188565`). Until this round
nothing could stop a running job: the daemon never watched its client, and
Ctrl+C left the script, its Pool chunks on peers and every DDP rank running.
- Ctrl+C, SIGTERM or a closed terminal (SIGHUP) makes the client send one
  `{"cancel":true}` frame on the exec websocket. The daemon sends SIGTERM to the
  job's process group, or `crun kill` for a sandboxed job, then SIGKILL two
  seconds later; the final frame carries `cancelled: true`. A second Ctrl+C
  exits the client at once.
- The cancel reaches `--remote` runs, `map` and every DDP rank. Pool chunks that
  a killed script asked for stop too: the chunk carries the request's context,
  so when the request ends the local workers and their children are killed by
  process group and forwards to peers are dropped, with nothing re-run.
- The coordinator completes the lease as `cancelled` and returns at once, with
  no back-off and no retry.
- A connection that ends **without** the cancel frame (a crash, a lost network,
  a detach) never kills the job. That is what background mode builds on.
- Ctrl+C also stops the submitter's own Nix work (`99cf0ad`). The closure export,
  the signed pack and the seed used to run with no context, so nix-store kept
  packing a multi-GB closure until the CLI exited. `nixstore.CmdContext` gives
  Nix its own process group and on cancel sends it SIGTERM, then SIGKILL 5 s
  later. SIGTERM comes first because a private store runs Nix inside crun in its
  own session: crun passes SIGTERM on, and killing crun outright would leave Nix
  running. A cancelled seed stops the upload instead of falling back to a whole
  resend. In the lab, Ctrl+C during an upload left no nix-store, `nix copy` or
  pack process on any container 1 s after the CLI exited <!-- evidence v5/22-item2-ctrlc-pack-pc2.txt, v5/22-item2-ctrlc-pack-ps.txt; the pack itself (0.1 s, and 4.3 s in v5/21) finished too fast to interrupt -->.
- Every exit, early failures included, now goes through one finish step that
  sends the done frame, writes the log, records the end and releases the lease.
- Time from Ctrl+C to idle peers: under 2 s in the container lab; the peers'
  `/v1/pool/stats` showed the cancel at the next 1.3 s sample <!-- evidence v1/31-ctrlc-poolwatch.txt (8e14b9b) -->.

**Background mode** (`ca853bb`, `b06d91a`, `272ca83`, `d188565`, `7939f84`). The
SSH-era detach was deleted with the move to the daemon API (`f46d354`); this is
its replacement.
- The daemon keeps every frame of a job's output in `<jobDir>/output.jsonl`, in
  the order a live client saw it.
- `--detach`, or Ctrl+Z during a foreground run, closes the websocket normally
  (close code 4001) without a cancel frame. The client prints
  `detached — job <id> keeps running` and the `pipedpeer attach <id>` command,
  and the prompt returns. `jobs` shows the run as `bg`.
- `pipedpeer attach <id>` replays the log, follows live, downloads the results
  at the end and prints the run summary. An attacher that falls behind catches
  up from the file without gaps or repeats. Ctrl+C during attach cancels the
  job. Job ids are short, and `jobs`, `job` and `attach` take a full id, a
  prefix or a suffix.
- Attach works only from the machine that submitted the job, whose local
  history holds the id.
- **A lease follows its job.** The execution request carries the lease id, and
  the sweeper keeps a lease bound to a running job past its TTL without
  renewals, because a detached or crashed submitter renews nothing; the job's
  end releases it. Otherwise a detached job would run on a node whose admission
  thought it free.
- A `--remote` job survives its submitter vanishing without a signal and can be
  attached when the submitter returns. A Pool job cannot: its script runs on
  the submitter.
- Detach is offered for single runs only; under `--ddp` or `map`, Ctrl+Z prints
  that detach is not supported and Ctrl+C stops the run.
- The job directory of a detached job is pruned 24 hours after the job ended;
  other job directories keep the seven-day retention.
- `output.jsonl` is capped at 50 MB. Past that it keeps its newest half behind
  a line saying how much was dropped, and never drops the final frame, so a
  background job that prints without end cannot fill a disk.
- A job still marked running when the daemon starts is failed with a final
  frame, `job lost: the daemon restarted while it ran`, so an attach ends
  instead of waiting; its lease is freed and a leftover crun container is
  removed. A job run without crun keeps no process id, so a leftover process of
  one is not stopped.
- `GET /v1/jobs/{id}/attach` (websocket) and `GET /v1/jobs/{id}/status` are the
  two new routes. Both are additive, so older clients are unaffected
  (ADR-0006).

**Whose network went away** (`879b86c`). When the exec connection drops, the
CLI must tell a dead node from its own lost network: the first means run the job
elsewhere, the second means detach and wait. It used to ask only another peer,
so with two machines a silent node always looked like this machine being
offline, the job was never rerun, and the watcher (which also needed another
peer) never failed a detached one.
- `networkUp` (`app/lostconn.go`) now runs three probes at once, 2 s each, none
  needing root: another peer's `/health`, a registration with the introducer,
  or a TCP connect to the default gateway on 53, 80 or 443 (accepted or refused
  both count).
- If any answers, the node is the one gone: a run fails over, and a detached
  job is failed after `goneAfter`. For an internet node reached through the local
  forwarder, only the introducer counts when one is set.
- In the lab, with two machines, the CLI named the node gone 16 s after it was
  killed ("while the default gateway 10.88.0.1 answers") and queued the job to
  run elsewhere <!-- evidence v5/11-item1-kill-node-pc1.txt, v5/11-item1-kill-node-drill.txt (kill 13:31:55.63, verdict 13:32:11.67) -->.

### 4.9 Transparent interception

**Delivery.**
- The shim is 3,822 lines of Python embedded in the Go binary
  (`nixgen/shim/`).
- It is appended to the workspace archive at
  `.pipedpeer/shim/sitecustomize.py` by a tar transform, so the user's project
  is never touched.
- The worker points `PYTHONPATH` at it, and CPython imports it before the user's
  first line.
- It replays the work of Nix's own `sitecustomize` (`NIX_PYTHONPATH`), which it
  shadows.

**Patching.**
- `multiprocessing.Pool` and `ProcessPoolExecutor` are patched at startup.
- numpy, pandas, torch and joblib are patched on first import, through a
  `sys.meta_path` finder, so a job that never imports torch never pays for it.

**Pool, in detail.**
- **What it intercepts.**
  - `map`, `starmap`, `imap` and `imap_unordered` go to the cluster. `imap` is
    lazy, in batches.
  - `imap_unordered` currently returns results in order.
  - `apply` and `apply_async` stay local.
- **Pool width.** The local pool's width is chosen from free memory and cgroup
  headroom (256 MB per worker, using at most 40%). The user's `processes=` is
  ignored unless `PIPEDPEER_RESPECT_POOL_SIZE=1`.
- **Measuring first.** A head of items runs locally to measure cost per item.
  Work below 0.5 s in total stays local.
- **Two ways to ship the kernel:**
  - **Source** (`_func_payload`). A plain module-level function ships as source,
    with its helpers and a pickle of the module data it reads. This covers
    functions defined in `__main__`, which the August version could not ship.
  - **By value** (`_func_pickle`). Lambdas, closures, bound methods, partials
    and decorated functions are serialised by cloudpickle.
  - If neither works, the call is counted `unshippable` and runs locally.
- **The race.** Chunks alternate between local and remote, with up to four
  remote chunks in flight. Local workers re-run any remote chunk that has not
  come back, and a slot table takes the first result. The call returns once the
  local side has covered every item (`fig_4.12_race_timeline.png`).
- **A slow peer is raced, not discarded** (`8541355`). On the daemon, a peer
  that ran past the 5-minute part budget used to be marked dead and its part
  restarted locally from scratch, throwing away however far it had got. Now the
  peer's copy keeps running, the same part starts locally, and the first to
  finish is used. The loser is cancelled: a local loser's worker process group is
  killed, a remote loser's connection closed. If both fail, the part fails with
  both errors. A late-started local win is not counted in the speed
  measurements.
- **The receipt.** Every run can write a receipt (`PIPEDPEER_RECEIPT`) counting:
  - items run remotely and locally;
  - items dispatched;
  - remote failures;
  - unshippable calls;
  - by-value shipments;
  - per-part provenance.

  A claim about where work ran can therefore be checked.

**Sharing work by measured speed.**
- On the daemon, a chunk is split across peers by `schedule.Compute`
  (`schedule/schedule.go:129-300`). Each device is modelled as a setup time plus
  a rate, fitted from its last eight observed parts (`daemonapi/poolrates.go`).
- The solver chooses the makespan *T* that finishes all the work. It drops any
  device whose setup time alone exceeds *T*, because that device "cannot start
  before the rest of the cluster finishes".
- A chunk posted by a shim on the same machine keeps no local share, because the
  local pool is already using those cores.
- On a peer, a chunk fans out across its cores.
- Measured on a lab cluster with 8, 2 and 1 CPUs:
  - splitting by headcount was 1.6× slower than not distributing at all;
  - splitting by measured throughput declined the 1-CPU worker and took a
    median 2.87 s, against 3.61 s by headcount (`8c9284c`).

**Keeping this machine free: `run --offload`** (`ef68f02`). `--remote` moves the
whole script, but its node could still spread pieces back to the sender, and
nothing kept a local script's own spread work off this machine.
- `--offload` passes this machine's id to the job as `PIPEDPEER_OFFLOAD`. The id
  is the kernel boot ID that `heartbeat.Machine` uses, so every daemon on this
  physical machine is excluded; it is never an address.
- The script and its non-distributed code still run here. Pool, joblib and
  executor chunks, pandas and numpy offloads and DDP ranks go only to other
  machines, shipped as under `--distribute force` (no cost model).
- With `--remote`, the chosen node's spreading skips this machine, which also
  keeps this machine out of a `--remote` DDP ring.
- A piece no other machine can take runs here with a one-line warning; the run
  never fails for it. The summary's spread line always names this machine,
  `0 on <host>` included. It is opt-in; defaults are unchanged.
- The container lab cannot show it: its containers share the host's kernel boot
  ID and so count as one machine.

**What is intercepted, and how.**

| Primitive | Strategy | Gate |
|---|---|---|
| `multiprocessing.Pool`, `ProcessPoolExecutor.map` | Measure locally, then race local against remote chunks shared by measured speed | 0.5 s of work |
| `numpy.matmul`, `dot`, `tensordot` (2-D) | Row blocks, concatenated | cost model (4.10) |
| `numpy.linalg.svd`, `eig` | Whole matrix to one worker | cost model, unsplit form |
| pandas `groupby().agg()` and named aggregations, `merge`, `join` | Hash shuffle on the key; bucket 0 local | cost model; above 512 MB unless forced |
| `pandas.read_csv`, `read_parquet` | Out-of-core, in 64 MB chunks at record boundaries | file larger than half of free memory |
| `joblib.Parallel`, scikit-learn `n_jobs` | Same as `Pool.map`: measure a head on joblib's own backend, then race local against multi-task remote chunks | 0.5 s of work; batches holding a lock stay local |
| torch `matmul`, `mm` | Row blocks | size only (≥ 32 MB) |
| torch training (`--ddp`) | Gradients averaged by the lead daemon | opt-in |

The numpy patch replaces module functions, so the `A @ B` operator is not
intercepted.

**Why the shuffle is exact in value.**
- Rows are bucketed by a hash of the key, so every key is complete on one node.
- Each node's aggregation is therefore complete, and partials combine by
  concatenation, with no combiners. That is why any aggregation specification
  works.
- Values are exact, but row order is not: a concatenation of buckets does not
  reproduce pandas' join order. `TestShimHashShuffleMatchesLocal` compares joins
  after sorting (`fig_4.9_hash_shuffle.png`).

**Data-parallel training without a socket mesh** (`fig_4.10_ddp.png`).
- Each rank posts its gradients to the lead node's ordinary daemon port
  (`/v1/ddp/sync`), rather than letting `torch.distributed` build a rank-to-rank
  mesh. That port is the only one the rest of the system already needs.
- In August the daemon was a blackboard that returned every rank's blob. On two
  machines that made training 21× slower than one: 62.9 s against 2.9 s
  (`145db30`).
- Now the daemon averages (`daemonapi/ddpreduce.go`):
  - gradients arrive as raw little-endian frames;
  - they are summed in float64, weighted by the samples behind each one, as
    Σ n_i g_i / Σ n_i;
  - one model is returned.
- **Encoding.** Gradients travel as int8 by default since the address-book
  phase, with one scale per 4096-value block and the rounding error carried
  into the next step on both the uplink and the downlink (error feedback); it
  was float16, with int8 opt-in.
- **Rank placement.** Ranks are chosen by `ddpplace.Select`:
  - it probes each node's throughput (`/v1/bench`);
  - it admits the prefix that maximises *k·rate_k*;
  - it uses one device kind only: GPUs if any are wanted and present.
  
  The lead is a GPU node whenever any candidate has a GPU, so a CPU-only node
  never builds the reference model (at the demo a CPU lead built a smaller
  network and every GPU rank's weights were refused). The shim then sizes each
  rank's shard and batch slice by its share; since the address-book phase the
  slices sum to the script's own batch, where each rank used to run the full
  batch, so the learning rate and the maths stay the script's.
- **Shape check.** Every rank sends a parameter manifest (names, shapes, dtypes,
  count) with the opening barrier; a mismatch aborts every rank with one
  message naming ranks, devices and counts (exit 76).
- **One GPU or a split** (address-book phase). After warm-up the lead daemon
  compares the measured split step time with the fastest rank alone, and either
  keeps the ring or releases every rank but the fastest (exit 77, not a
  failure), with one plain line in rank 0's output. At the demo a small model
  split over two Wi-Fi laptops took about 9 minutes against about 20 s on one
  GPU, because 92% of each step was sync. `--ddp N` (N ≥ 2) fixes the ring and
  turns the decision off; a ring of one stops syncing entirely.
- **`--ddp-sync fast` (DiLoCo).** Opt-in: exact per-step sync during warm-up
  (post-local SGD), then an adaptive interval H in [1, 64] driven by the drift
  between ranks measured at each sync (AdaComm-style), tightening back toward
  exact over the last 10% of the steps when the total is known; pseudo-gradients
  averaged by the samples each rank processed, an outer Nesterov step (learning
  rate 0.7, momentum 0.9), and the Adam moments averaged every 10 outer steps.
  It never adds epochs or changes the batch or learning rate. Automatic mode may
  choose it only once its validation loss is within 1% of one GPU on every test
  model (5.12).
- **Refitting.** Every 20 syncs the daemon refits shares from the observed step
  rates, if any share moved by 10% or more. It can drop the slowest rank (never
  rank 0, and never below two) when that rank adds more sync than compute. A
  round completes without a straggler after 45 s.
- **A start barrier** (`c567369`, `9b47502`). On the friend's two-laptop ring,
  rank 0 trained while rank 1 was still receiving its closure, so every sync
  waited about 45 s for it. The first sync is now a barrier: every rank must
  reach the opening weight broadcast (closure imported, script at its first
  optimizer step) within `PIPEDPEER_DDP_START_WAIT` (default 5 min).
  - Ranks still missing are dropped and rank 0 names them; no step waits on
    them. After 2 s at the barrier rank 0 says whom it waits for ("waiting for
    rank 1 on gpu1 to be ready … up to 5 min") and repeats every 30 s, reading
    arrivals from an additive `GET /v1/ddp/sync` and host names from
    `PIPEDPEER_DDP_HOSTS`.
  - The barrier's time goes to `barrier_sec`, not the sync statistics, and
    rank 0 prints it as not counted as sync. Before that, a 2.5 min wait at the
    barrier was reported as sync: "36202 ms per sync", and 146.0 s of sync in a
    148.7 s run whose training took 4.0 s on the other rank <!-- evidence v4/80-item1-ddp-start-barrier-default.txt, v4/80-item1-ddp-start-barrier-default-drill.txt (rank 1 throttled to 1 Mbit) -->.
  - A late rank is refused (410); its shim exits 75 and the CLI prints a plain
    line without failing the run or stopping the rejoin watcher, so a DataLoader
    script can still take it back. A rank still running 60 s after another
    finishes is stopped. With `PIPEDPEER_DDP_START_WAIT=30` and rank 1's link
    throttled to 1 Mbit/s, rank 0 trained alone after 30 s and the CLI stopped
    rank 1 61 s after the ring finished <!-- evidence v4/82-item1-ddp-start-wait-30s.txt, v4/82-item1-ddp-start-wait-30s-drill.txt -->.
  - The SAME-gradients check marked a round duplicate on its first contribution,
    and nothing undid it when the round closed on rank 0 alone. It now needs two
    matching contributions.
- **A dead rank no longer ends the run** (`8e14b9b`). Until this commit one
  failed rank failed the whole run, although the sync already closed without it.
  This holds for a lost *machine*; since the address-book phase a rank whose
  *script* fails stops every rank at once, and a lost lead stops the survivors
  within about 30 s (exit 76) instead of leaving the submitter waiting, as it
  did at the demo.
  Now the CLI prints which rank died and on which host, tells the lead daemon,
  which stops waiting for it and finishes any round held on it, and the
  survivors take new shares at the next epoch boundary. The run fails only if
  no rank finishes, or on Ctrl+C, and the error names the real rank (it used to
  name the loop count).
- **Shards by weighted rendezvous hash.** The shim assigns samples to ranks with
  a weighted rendezvous hash. The pool's hash-mod-peers routing would have moved
  most samples when one rank left; this keeps each survivor's share exact and
  over 95% of its samples in place. Rings of equal shares with a resizable
  batch use the same sampler.
- **Limits of the above.** Losing rank 0's machine still ends the run, because
  its daemon holds the sync. A script that slices its own tensors instead of
  using a DataLoader is not re-spread.
- **A machine that comes back rejoins** (`c4f7800`). A rank whose machine
  returns, or a new eligible node, joins the ring at the next epoch boundary
  instead of sitting idle (`src/ddpup.go`, `daemonapi/ddpup.go`):
  - runDDP watches the nodes and starts a rank there;
  - the joiner loads the lead's model and optimizer snapshot through the lead
    daemon;
  - for each epoch it missed it runs one real batch from the unsharded loader
    (`82596fe`), so a script's loop variables exist (empty passes left
    `print(loss)` after the loop raising NameError). During these batches
    `backward()`, `optimizer.step()`, GradScaler's step, update and unscale_,
    the sync and the step count do nothing, so nothing trains or is counted
    twice, and the rank's output is prefixed `[rank N catch-up, not trained]`.
    The snapshot now carries buffers too, copied in after the catch-up batches,
    so BatchNorm running statistics come from the lead;
  - the shim's world size and rendezvous-hash shards grow to include it, so
    about 1/n of the data moves, and the daemon's averaging group grows with it.
  - Only DataLoader scripts can grow; a script that slices its own tensors gets
    one line saying so. `04_torch_ddp.py` now uses a DataLoader (with an
    optional epoch count), so the GPU demo can show a rank dying and coming
    back.
  - `rebalanceFor` now runs once per sync and reaches every rank; it used to
    hand new shares to one rank per cadence.
  - Rejoin time in the rehearsal (two containers sharing one GPU): 4 s from the
    node's daemon starting to the rank joining the ring <!-- evidence v2/23b-ddp-down-up.txt + gpulab/20261012-064736-logs (1519952): daemon starting 05:37:47, rank joined 05:37:51 -->.
- **The verdict.** At exit, each run prints whether the ring paid for itself,
  counting distinct machines rather than ranks.

### 4.10 The cost model

Every intercepted numpy call is gated (`nixgen/shim/numpy_ops.py`,
`_numpy_should_offload`). The checks, in order:

1. At least two nodes and at least 32 MB of operands, unless forced.
2. **If the working set does not fit in local memory, offload.** If the cluster
   cannot take it either, a matmul raises with the size and what to do, rather
   than falling back to a local run that would take the machine down. This was
   added after an 8192-cube product drove a 14 GB machine to 194 MB free
   (`ba14ac0`).
3. If a peer reports a GPU, remote throughput is multiplied by 6. That factor is
   the toy-batch measurement, not the larger saturating one.
4. Evaluate at an optimistic 10 Gbit/s. If offloading loses even there, stay
   local without probing.
5. Otherwise, measure bandwidth with a 64 MB probe through the real dispatch
   path, cached for 300 s.

Split work then offloads when *est_local > est_transfer + est_local / K × 1.3*,
using about 200 GFLOP/s for matmul. Unsplit work (svd, eig, at about
1.5 GFLOP/s) offloads when the transfer costs less than half the local time.
`fig_4.11_cost_model.png` plots the regions.

**Dense matmul is no longer forbidden from shipping.** In August it was a rule,
and `docker/demo-test.sh` failed if `matmul: sending` appeared. That rule was a
fact about one rig written down as a law. The check now times the same product
locally and asserts that the choice made was not slower (`2fecfe1`). Measured on
a 4-core container peer, local won by 25× to 100× across four shapes, and the
model kept every one local (`ec5d9fc`).

`--distribute force` overrides the model.

### 4.11 Fault tolerance

The invariant is that using the cluster is never worse than not using it. Six
layers hold it up:
1. **The race** (4.9), on the daemon too: a peer past its part budget is raced
   by a local copy rather than discarded.
2. **Per-primitive fallback.** Any exception in an intercepted call falls back to
   the original implementation. The one exception is an out-of-memory matmul,
   which raises (4.10).
3. **Peer fallback.** A failed part tries the next peer, then runs locally.
4. **Worker respawn.** A dead warm worker is restarted, and a one-shot process
   is the fallback.
5. **Rescheduling.** A failed job is placed again with back-off, on an untried
   node first, at most three times; a node that refused its closure is skipped
   (4.4, 4.5). A dead node is told apart from this machine's own lost network
   before the job is rerun (4.8). A cancelled one is not retried (4.8).
6. **Surviving ranks.** A DDP run continues on the ranks that are left when one
   dies, and starts without a rank that misses the start barrier (4.9).

Memory is bounded at the same time:
- A pool request larger than the node's grantable memory is forwarded to a
  healthier peer.
- A request needing more than 40% of it runs in sequential micro-chunks.
- The `/v1/pool/map` body is capped at the smaller of 2 GiB and half the
  daemon's budget (`daemonapi/poolhandlers.go:58-71`).

### 4.12 Resource estimation

The first tier that applies wins (`resourceest/estimator.go`):
1. an explicit `--mem`;
2. the largest peak RSS of the last five runs of the script, plus 20%;
3. per-library baselines plus 100 MB.

The estimator also has a tier based on input file sizes (× 3, plus baselines),
but every caller passes `nil` data files (the run, DDP and map paths), so that
tier is never reached.

With nothing known, the estimate is 512 MB. The worker samples the job's process
tree every 500 ms and records the peak for the next run.

### 4.13 Test workloads and harnesses

- **Workloads:**
  - ten dispatch scripts in `test_project/`;
  - seven demo workloads in `pipedpeer-demo/` (`00_pool` to `06_pool_lambda`);
  - a three-network walkthrough, `pipedpeer-demo/DEMO.md`.
- **Lab rigs:**
  - a three-node container lab, `scripts/lab-up.sh`;
  - an uneven-CPU lab, `scripts/lab-hetero.sh`.
- **Benchmark harnesses** (`scripts/`): `bench-pool`, `bench-split`, `bench-ddp`,
  `bench-ddp-hetero`, `bench-matmul`, `bench-nar`, `bench-shim-d2`.
- **Verification scripts:**
  - `net-verify.sh`, the cross-network acceptance check;
  - `audit-tests.sh`, the mutation audit;
  - `check-skips.sh`, which fails if a distribution test skipped.
- **Demo rehearsal rigs** (October 12):
  - `lab/nat` with `scripts/lab-nat.sh` (`2e3e344`): each node on its own
    private network behind a router container (two cone NATs, two symmetric
    ones) and the introducer on a public network. It asserts that cone pairs
    punch and take work, that cone-symmetric pairs get there by the birthday
    path, that the symmetric pair is listed unreachable with its reason, and
    (`lab-nat.sh trust`) that a multi-user Nix node set up once takes work from
    a node that joins later with no prompt;
  - `lab/lan` with `scripts/lab-lan.sh` (`0fe2644`): the venue's shape, one
    bridge network as the college LAN, this laptop's own daemon as the GPU
    machine and introducer, three containers on single-user Nix and one on
    Determinate multi-user Nix as the Ubuntu stand-in;
  - `07_everything.py`, one script with every kind of intercepted work, and
    five student-style scripts in `pipedpeer-demo/students/` (`bb9380c`).

### 4.14 Seeing what the cluster does

The October demo work made visible what the daemons had done only in their
logs. Nothing here adds a daemon endpoint except the two attach routes (4.8).

**`pipedpeer events`** (`4bd368a`). A plain-language feed of what this machine's
daemon has done: peers joining and leaving, links and the reason a peer has no
path, leases, jobs started, finished and cancelled, closures sent and imported,
keys trusted, and Pool work received and forwarded. Node ids are replaced by
host names, repeats are collapsed, and `-f` follows live.
- It reads `daemon.log`, which mixes three formats (zerolog console lines,
  zerolog JSON from the API package, and stdlib `log` lines from the pool), so
  the parser is the project's own. The daemon gained one log line for each
  event that had none: peer up and down, lease granted, released and expired,
  job started, finished and cancelled, closure imported (`b06d91a`). Log times
  now carry seconds, and colour is dropped when stderr is not a terminal.
- The feed is per machine: a sender sees what it sent and to whom, a receiver
  who sent what. There is no cluster-wide merge.

**`status` and `dashboard`** (`10afd3d`; `nodes` folded into `status` in the
address-book phase). The node rows always carried the load the heartbeat
reports; `status` prints cores and CPU use, free memory, and the
GPU's short name with its utilisation and memory in use. The dashboard has the same columns, the
last five events, and a status line with the newest one. Peers with no direct
path are listed under the table with their reason (`8b13704`, `36d4923`), even
when no peer is reachable.

**Watching a connect.** `pipedpeer connect` waits on the introducers and the
node list, then prints connected, partly or failed with the reason per machine
and the fix (4.2); `pipedpeer doctor` checks the rest (4.15).

**`--check-only` is a dry run** (`c8eb327`). It used to take a lease and cancel
it. It now builds the environment when Nix is present, lists imports with no
package, names the parallel calls in the source that can spread (a regular
expression over the script, since a Python parser would need a Python on the
submitter), gives the memory estimate, the GPU and DDP choice and where the job
would go, says how much of the closure that node already holds, and ends
`checks complete` or `NOT READY` with the reasons. Without Nix it reports the
build as skipped, not failed.

**A run summary** (`fbbaaf4`, `906f707`, `272ca83`). Every run except a detached
one ends with a summary of at most eight lines: the host it ran on, the
environment build time, what happened to the closure (already on the node, a
partial send, or a full or cold send with size and time), the script's time and
peak memory, how its items spread across machines, the files that came back,
and the total, or the time at which it was cancelled. The spread comes from a
receipt the shim writes to a file at exit (`PIPEDPEER_RECEIPT`), with
items counted by where they ran. The daemon returns it on the done frame and
the client stores it in the job history. Since `5b89f8c` the receipt also counts the items this machine
runs itself, including a chunk it re-ran after a peer failed.

**Usage lines** (`ca70da4`, `98e8684`). The claim that the sender stays free had
no proof on screen. The summary gains two lines, sampled once a second from
`/proc` and from the executing node's `/health` (`app/selfuse.go`):

```
  this machine CPU 22% avg · RAM +69 MB (peak +249 MB) · no GPU
  on pc3       CPU 31% avg
```

<!-- evidence v5/41-item10-usage-pool-pc2.txt (00_pool.py --remote from pc2, lab) -->
A GPU node adds GPU utilisation and peak VRAM. Any sampling failure prints a
warning and drops the line; it never fails a run. A `--ddp` run samples this
machine and each rank's node, and prints one `on <node>` line per machine under
a `=== Usage ===` header (two ranks on one node share a line; a rank that joins
mid-run gets none). Attach and collect summaries have no usage lines, since no
CLI was there to take samples. For the same demo point, `04_torch_ddp.py` now
trains a wider model in bigger batches, long enough for `nvidia-smi` to show the
load.

### 4.15 After the demo: setup, output and speed

The college demo of 2026-10-14 ran on two laptops, two lab PCs on routed
subnets and a phone hotspot. It found the address problems of 4.1-4.2, the
training problems of 4.9, and a list of annoyances. This section covers the
rest of what changed in answer.

**Commands.** Twenty-eight commands became thirteen: `setup`, `invite`,
`connect`, `run`, `status`, `dashboard`, `ui`, `jobs`, `attach`, `start`,
`stop`, `doctor`, `uninstall` (`map` and `events` remain, hidden). Bare
`pipedpeer` prints help. Configuration is one file, `cluster.json` (token,
introducers, roles), and the user-facing environment variables shrank to a
handful.

**Ports.** One block, one hard cut ([ADR-0020](adr/0020-ports-block-hard-cut.md)):
4242/tcp daemon API, 4242/udp discovery, 4243/udp direct links, 4244/udp
introducer. The gloo training ports are gone with the gloo backend.

**Install and setup.** Every nightly build is published under its own version
(`nightly-<version>`), and `install-pipedpeer.sh --version <v>` gives every
machine the cluster's exact build; the installer verifies the checksum, puts
the binary in `/usr/local/bin`, and runs `setup`. `setup` creates the secret
before the daemon first starts, opens the four ports, asks whether to start
pipedpeer at boot (a systemd user unit plus `loginctl enable-linger`; default
yes; `start --boot` / `stop --boot` later), and prints colour-coded results
with explicit ACTION NEEDED lines. After `connect`, the common environments
build in the background so the first run starts warm; `status` shows the
warm-up's progress. `pipedpeer doctor` checks the daemon, the token, each
introducer, each peer (with TCP and UDP reachability told apart, since a router
that passes UDP but not TCP was the demo's unexplained `unknown` row), the
firewall, user namespaces and AppArmor, crun, Nix, the NVIDIA driver, disk and
clock skew, each with its fix. `pipedpeer uninstall` removes the binary, data,
service, firewall rules and root-owned key helper, and keeps Nix.

**Runs like Python.** Plain `run` prints only the script's own output: stages,
placement, lease and shim lines appear with `-v` (the shim reads
`PIPEDPEER_VERBOSE`), and warnings that mean a wrong result or an action print
always. Flags after the script name go to the script (`run t.py --epochs 3`),
and the script's exit code is pipedpeer's. When work left this machine, one
line follows: total time, the script's own time, pipedpeer's overhead and
where it ran (5.12).

**Web page.** `pipedpeer ui` opens `http://127.0.0.1:4242/ui#token=…` (4.3).
The page shows nodes, recent runs from every machine (merged through each
node's `/v1/history`), the run timeline, where the work ran, and a run view
with every step of every chunk.

**Phones.** An Android phone joins as a `pool-only` worker in Termux: Termux's
own Python and numpy run Pool chunks, whole jobs are refused, and no Nix or
crun is needed ([phone.md](phone.md)).

**Speed** (each with a before/after figure in 5.12).
- *Pool scheduling.* Each node's in-flight depth comes from its measured round
  trip (at most 3) instead of a global 4; the losing copy of a race is
  cancelled, and a peer that lost its last 3 races stops being raced (at the
  demo the fast laptop won all 300 races, wasting the peer's work); chunk size
  follows the measured transfer cost and round trip and shrinks toward the end;
  at most one tail backup.
- *Transfer.* The pool request body was built by repeated concatenation,
  quadratic in its size; it is built once. Functions and globals of 1 MiB or
  more go once per peer by hash; requests are zstd-compressed at a level chosen
  by the measured link speed; numpy buffers travel out of band (pickle 5); the
  link speed is measured passively from real transfers and feeds the cost
  models (estimates were 7-37x too high).
- *Environments.* Only workspace files the node lacks are sent (a content
  cache on each node); nodes fetch paths cache.nixos.org serves themselves;
  the per-script environment wrapper is skipped when an existing environment
  covers the imports; a closure seed to a node that already holds every path
  takes milliseconds.
- *Training.* int8 gradients with error feedback by default; gradient upload
  overlapped with the next step's compute; a butterfly reduce instead of one
  hub at three ranks or more; ranks on one machine average locally first.
- *Stats across runs.* Per machine and function, rates and overheads persist
  across runs (24 h half-life), so the first chunks of a run are sized from
  history rather than a cold guess.

---

## Chapter 5  Results

Raw output for everything here is in `docs/benchmarks/raw/`, and
`docs/benchmarks/RESULTS.md` collects it. There are three sources, and every
number says which one it comes from:

- **[Oct]** Run for this report on 2026-10-02 at commit `4c91ef3`. Host:
  - Fedora 43, Linux 6.19.13;
  - Intel i5-13420H, 12 threads, 15 GiB;
  - Go 1.25.9, Python 3.14.4, with torch 2.14.1+cpu in a virtualenv (as CI
    installs it);
  - lab workers as podman containers on that host.
- **[Aug]** The August pass (2026-08-25, Fedora 44, 20 logical cores),
  for measurements whose code has not changed since and which were not
  repeated.
- **[commit]** Multi-machine measurements recorded in commit messages between
  2026-08-26 and 2026-08-29, quoted in `raw/C_commit_measurements.md`. They were
  taken by hand on the development machine, the second machine "yeet" and a GCP
  introducer. They were not repeated for this report.
- **[Oct-12]** Run on 2026-10-12 on a second host (Fedora 44, 16 threads,
  RTX 3060 Laptop): the N7 re-run at `08ce1d4`, and the demo rehearsal (5.11)
  in the `lab/lan` containers at `31b5364` (first-pass figures at `8e14b9b`)
  and a two-container GPU lab at `1519952`.

**The caveat that governs this chapter.** The [Oct] and [Aug] lab workers are
containers on one host, so they measure correctness, overhead, caching and fault
tolerance, not multi-machine speedup. The only speedup figures quoted are
[commit] measurements between real machines. Each is labelled as such, and none
was repeated here.

### 5.1 Functional verification [Oct]

Commands: `go test -v -count=1 ./...` in `src/`, `scripts/check-skips.sh` and
`scripts/audit-tests.sh`. Raw output: `raw/T1_go_test_verbose.txt`,
`raw/T1b_go_test_without_torch.txt`, `raw/T2_check_skips.txt` and
`raw/T3_audit_tests.txt`.

| Run | Passed | Failed | Skipped |
|---|---|---|---|
| With torch (CPU wheel, as CI installs) | 615 | 0 | 2 (integration opt-in) |
| Same host, no torch | 607 | 3 | 7 |
| [Oct-12] after the demo round, with torch | 822 | 0 | 7 (2 integration opt-in, 5 depend on the host's Nix install) |

<!-- evidence fix5/go-test-json.jsonl (cc56753, go test -count=1 -p 1 -json ./..., top-level tests) -->

- **Packages:** all 36 packages that have tests pass. After the demo round:
  38 of 38 packages (`cc56753`, `go test -count=1 ./...`; 2 more have no test
  files) <!-- evidence fix5/go-test-full-2.txt -->.
- **Failures without torch:** the three are `TestWeightedShardsPartitionTheDataset`,
  `TestEqualStepsAcrossUnevenShares` and `TestShuffledShardsStayDisjoint`. They
  raise `ModuleNotFoundError` instead of skipping, which is a defect of the
  tests.
- **Skip check:** all 15 named distribution tests ran.
- **Mutation audit:** all 95 mutations were caught ("did NOT fail: 0"). The
  script restores the source from its own copies.

### 5.2 Environment cache [Oct]

Harness: `docs/benchmarks/bench.py`, N1. Raw: `raw/O1_bench_py.txt`,
`raw/N1_trivial_run*.log`, `results.json`.

| Run | Seconds | Placed on | Sent |
|---|---|---|---|
| 1 | 23.41 | worker A | whole closure (first build) |
| 2 | 8.08 | worker B | whole closure (B did not yet hold it) |
| 3 | 0.86 | worker A | nothing |
| 4 | 0.83 | worker A | nothing |
| 5 | 0.82 | worker A | nothing |

- **Warm cost:** the median of the runs that sent nothing is 0.83 s.
- **August, for comparison:** the same harness on the other host gave 12.00 s
  cold and 0.65 s warm.

See `fig_5.1_closure_cache.png`.

### 5.3 Cost of interception [Oct]

**Whole job.** These are the N7 runs of `bench.py`, a 48-item parallel map.

| | Seconds | Notes |
|---|---|---|
| Interception off | 0.94, 0.66, 0.65 | three clean runs; two others failed once and were rescheduled (9.66, 3.25) |
| Interception on | 0.97 to 1.10, median 0.98 | every run sent 18 of 48 items to two other daemons |

- **Not a declined workload.** The harness was meant to measure interception on
  a workload the cost model declines. At this commit the shim distributed in
  every enabled run, which on one host adds cost: about 0.3 s over the fastest
  disabled runs, which ran on a different worker.
- **The slow disabled runs were failures.** They hit `stdout scanner: read |0:
  file already closed` and were rescheduled (`raw/N7_shim_off_run1.log`,
  `run4.log`). That is a defect, not variation.
- **What this does not measure.** The idle cost of interception; the start-up
  figure below measures it directly, and the [Oct-12] re-run below measures it
  on a whole job.

August measured 0.85 s off against 0.93 s on.

**Whole job, declined [Oct-12].** Re-run on 2026-10-12 at `08ce1d4` on a second
host (Fedora 44, 16 threads, Python 3.14.8, docker lab), with a workload small
enough that the cost model declines it: `workloads/pool_idle.py`, a 16-item
`Pool.map` (`8abb031`). Five runs each.

| | Median | Range | Notes |
|---|---|---|---|
| `PIPEDPEER_SHIM=0` | 0.678 s | 0.671–0.852 s | |
| shim on | 0.796 s | 0.747–0.797 s, plus one cold 7.03 s first run | every receipt `dispatched_items 0` |

- Interception that declines costs about **0.12 s** per job: the measurement
  head plus the decision.
- `bench.py` now runs N7 with `PIPEDPEER_RECEIPT` and fails any run whose
  receipt shows a dispatched item. A forced run of the same workload does
  dispatch, so the decline is the cost model's choice, not a dead cluster.
- Raw: `raw/N7_*`, the `n7_*` entries of `results.json`.

**Start-up.** Raw: `raw/O2_shim_startup.txt`, from `scripts/bench-shim-d2.sh`.
- Now: **1.24×**, against the 3× budget.
- August, before patching was deferred: 20.0× (`raw/B3_shim_overhead.txt`).
- `bench-shim-d2.sh` now runs in CI.

See `fig_5.2_shim_overhead.png`.

### 5.4 Distribution happens [Oct]

**Forced `Pool.map`** (N3).
- Every run's receipt shows 18 of 48 items completed on three other daemons.
- The checksum is identical across all five runs.

**numpy** (N5).
- The 34 MB SVD was offloaded to one worker.
- The workload's `a @ b` is not intercepted (4.9), so it ran locally without
  the cost model being asked.

**`scripts/bench-pool.sh` against the lab** (`raw/O3_bench_pool_lab.txt`).
- Timings: plain 0.17 s, tuned 0.17 s, pipedpeer 0.24 s.
- 12 of 48 items ran on the cluster.
- This is slower on one host, as it must be, but within the 3× never-slower
  budget.

### 5.5 Fault tolerance [Oct]: the harness failed

`scripts/lab-fail.sh` (`raw/O4_lab_fail.txt`) **failed**:
- The job was placed on the submitting host, which already held its closure
  ("nothing to send").
- The freshly created workers never received the closure.
- The shim ran all 80 items locally: `remote_items 0`, `local_items 80`.
- No worker held a chunk to kill.

The likely cause is that the closure is pushed to peers only after an upload.
That cause is inferred from the log and has not been verified.

The August pass of the older harness printed `POOL-OK sum=2470`. That harness
was later shown (commit `fca0107`) to have distributed nothing, so it is not
evidence either. What remains is test evidence: `TestShimRaceCorrectWithDeadRemote`.

### 5.6 Sandbox cost [Aug]

Not repeated. Median of 20 iterations: crun costs 15 to 20 ms more than bwrap
(`raw/B2_sandbox_bench.*`). See `fig_5.3_sandbox_overhead.png`.

### 5.7 Measurements between real machines [commit]

Quoted from commit messages; the verbatim text is in
`raw/C_commit_measurements.md`.

| Measurement | Result | Commit |
|---|---|---|
| Pool map, 20-core + 16-core peer over Tailscale, plain vs PipedPeer | 5.8-6.4 s vs 4.9-5.4 s; 116/256 items on peer | fca0107 |
| Same, before fan-out across peer cores | 6.4 s vs 8.5 s | fca0107 |
| Split on 8:2:1 lab, headcount vs measured | 3.61 s vs 2.87 s (median of 5) | 8c9284c |
| Training, two machines, daemon as blackboard | 62.9 s vs 2.9 s one machine | 145db30 |
| Training: one machine / 2 ranks fp16 / 2 ranks int8 | 54.9 / 69.4 / 56.1 s; loss .087495 / .087497 / .087688 | 364b3d9 |
| Ring 16/8/2 cores: equal vs measured batches | 25.4 s vs 23.6 s | 2fc3914 |
| Three-rank ring vs the one machine admission picks | 36.4 s vs 6.9 s | 2fc3914 |
| GPU vs CPU, same model and batch | 1.5 s vs 55.0 s | 2da0857 |
| Shares after throttling a rank | 64:36, then 84:16, then 70:30 | ac19310 |
| Closure diff, one package added | 7/43 paths, 206.0 MB to 28.6 MB | 193e1bf |
| Peer reached with introducer suspended | 0.03 s | 42c2426 |
| Introducer traffic | 0.2 MB vs ~30 MB between peers | 42c2426 |
| Signed closure under require-sigs | imported when trusted, refused when untrusted | cf2e3ae |

**Not verified:** two GPUs in one machine (2fc8766).

### 5.8 Scale [Oct]

| | |
|---|---|
| Go, non-test | 32,049 lines across 83 files (including the 3,822-line shim) |
| Go tests | 23,100 lines across 99 files |
| Packages | 38 (37 internal, plus `main`) |
| HTTP routes | 22 |
| Tests run | 617 (615 pass, 2 skip) |
| Mutation audit | 95 cases, all caught |
| [Oct-12] Go, non-test / tests / tests run | 42,424 lines (114 files) / 30,767 lines (154 files) / 829 (822 pass, 7 skip) |
| [Oct-12] HTTP routes | 31 (the round adds `/v1/jobs/{id}/attach` and `/v1/jobs/{id}/status`) |

<!-- lines: wc -l over the tracked src/**/*.go at cc56753 (the same method gives the [Oct] row's 23,100 test lines at 5bcf140); routes: r.Get/Post/Delete in daemonapi/server.go (22 at 5bcf140). tests: evidence fix5/go-test-json.jsonl (cc56753, go test -count=1 -p 1 -json ./..., top-level tests) -->


### 5.9 Comparison

See `fig_5.4_comparison.png`.

| | PipedPeer | Spark | Ray | Dask | BOINC | ssh |
|---|---|---|---|---|---|---|
| Runs unmodified Python | yes | no | partial | partial | no | yes |
| No cluster to configure | yes | no | no | no | no | partial |
| No head or master node | yes | no | no | no | no | yes |
| Reproduces the environment | yes | partial | partial | partial | partial | no |
| Joins NATed machines, no relay | yes | no | no | no | no | no |
| Survives losing a node | yes | yes | yes | yes | yes | no |
| Authenticated and encrypted | partial | yes | partial | partial | yes | yes |
| Sandboxes each job | partial | no | no | no | partial | no |
| Runtimes other than Python | no | yes | partial | no | yes | yes |
| Proven in production use | no | yes | yes | yes | yes | yes |

### 5.10 Not measured

| Wanted | Why absent |
|---|---|
| Controlled, repeated multi-machine benchmark | one machine available for this report |
| Fault injection that distributes | the October run did not distribute (5.5) |
| Hole-punch success rate across many NATs | links observed one at a time |
| Two GPUs in one machine | no such machine available |
| Discovery latency | containers share loopback |
| Seven demo workloads end to end | not run for the [Oct] pass; run in the [Oct-12] rehearsal (5.11) |
| Real speedup, Wi-Fi, two physical GPUs, Ubuntu AppArmor, college firewall | the [Oct-12] rehearsal is containers on one laptop; these come only from the venue |

### 5.11 Demo rehearsal [Oct-12]

The whole demo was rehearsed on one laptop before the venue: `lab/lan` (4.13)
for the CPU steps, and two containers sharing the laptop's one RTX 3060 through
CDI for the GPU steps and the DDP ring. Containers prove the logic; they share
one CPU, so no figure here is a speedup.

| Step | First run (cold) | Cached |
|---|---|---|
| Live join (the old join command) until "connected" | 1.8 s | — |
| Headline `Pool` script (`00_pool.py`) | 43.5 s | 17.0 s |
| `06_pool_lambda.py --remote --cold`, then without `--cold` | 20.5 s | 4.8 s |
| GPU step from a GPU-less node, `04_torch_ddp.py --remote --gpu force` | 264.8 s | 7.6 s |
| `--ddp 2 --gpu force` on two containers sharing one GPU | 630.3 s | 9.5 s |
| `07_everything.py` | 664.6 s | 26.7 s |
| Student scripts (five), first run after `prep-4-common` | 35–126 s each | 19.6–69.5 s each |
| Teacher-style script's environment, without and with `prep-4-common` | 54 s | 4 s |
| Finale, every script at once | 412.0 s, all 8 jobs ok | — |

The first runs of the headline script and the GPU step are from the first
pass (`8e14b9b`), since the second pass found those steps warm; every other
figure is from the second pass (`31b5364`), the cached student runs and the
ring's cached run from a re-check at `cc56753`, and the ring's first run,
`07_everything` and the finale from a last pass at `9949ad0` (the ring's first
run took 635 s and `07_everything`'s 700.6 s at `cc56753`). A join has no
cached form, and the finale ran once, after the other steps. It ran with the
demo's `PIPEDPEER_REMOTE_WAIT=10m` and with the lab's containers resized to
6.0 GB of memory in total; at 1.2 GiB each, three of its eight jobs fitted no
node. The `--cold` row without network
loss; with 0.5 % emulated loss in the first pass it was 169.1 s / 5.8 s.

<!-- evidence (~/Downloads/pipedpeer-work/evidence): v2/56-join-pc3.txt (V1 5.2 s: v1/20-join-pc3.txt); v1/30-host-00pool-first.txt; v2/71-step-2-headline.txt;
v2/73-step-2b-cold-pc2.txt; v1/14-pc3-truecold-2b.txt; v1/13-gpu-step.txt; v2/74b-gpu-pc1-direct.txt; v2/112-c-ddp-ring-cold.txt; v2/113-c-ddp-ring-cached.txt; v2/98-c-everything-pc3-cold.txt; v3/43-ddp-ring-cold.txt; v3/21-item2-everything-pc3-cold.txt; v3/22-item2-everything-pc3-cached.txt; v3/33-item3-finale-run3.txt; v3/30-finale-lab-resize.txt; v2/75-teacher-*.txt; v2/100-c-teacher-cached-*.txt (primes 39.3, monte_carlo_pi 35.9, word_count 42.5, grid_search 69.5, image_blur 19.6 s); v1/15-pc3-pkgprep-before-after.txt. -->

| Drill | Result |
|---|---|
| Ctrl+C mid-Pool: seconds until peers stop | under 2 s (next 1.3 s sample) |
| Closed terminal (SIGHUP) | stopped everywhere, as Ctrl+C |
| Ctrl+Z, then `attach` while running and after it finished | followed live to the end; replayed in 1.0 s once finished |
| Submitter's network pulled during a `--remote` job, then `attach` | 200 s outage: the CLI detached, the job ran once, results back 15 s after reconnect |
| A node stopped mid-run (failover) | same checksum; 16.5 s against 17.0 s undisturbed |
| A DDP rank killed mid-run (survivors finish) | the survivor finished all 80 epochs, 90.5 s |
| A DDP rank's machine back mid-run (rejoins) | rejoined at the next epoch, 4 s after its daemon started |

<!-- evidence: v1/31-ctrlc-poolwatch.txt, v1/32-sighup*.txt, v1/34-attach.txt, v1/35-detach-attach-live.txt (8e14b9b); v2/64-senderdeath-long-*.txt, v2/65-senderdeath-collected.txt, v2/72-step-9-failover.txt (31b5364); v2/23b-ddp-down-up.txt (GPU lab, 1519952) -->

### 5.12 Measurement method and the address-book phase

**One record, two views.** Every run keeps a record (`internal/runrec`,
`steps.json` beside the receipt): timed steps (pickle, compress, send, queue,
compute, receive, unpickle, environment build and send, DDP sync rounds, and
the script itself) per machine and chunk, written by the shim
(`shim/record.py`), the daemon and the CLI. Shim work is never folded into the
script's time; it used to be, which is how the quadratic request body hid.
- *Simple view*, printed after every run that used another machine:
  `pipedpeer: 41.2 s total — script 38.0 s, pipedpeer 3.2 s — ran on yeet 60%,
  fedora 40%`.
- *Detailed view*: `run -v`, `jobs <id> --steps` (`jobs last --steps` for the latest run), the web run view, and CSV or
  JSON (`jobs <id> --export csv|json`, `jobs --export-all <dir>` for every run).

**Emulated lab.** The plain lab (three containers on one host) proves that
work crosses nodes, never speedup. Speedup figures come from the emulated lab
(`scripts/lab-emu.sh`): N containers with equal fixed quotas (2 CPUs, a hard
memory cap each, the total within the host), links shaped by tc netem to LAN,
Wi-Fi and 5G profiles plus 0-5% loss; speedup = 1 container against N of the
same size. Every such figure is labelled *emulated*. Training scaling uses
CPU-training stand-ins in those containers plus the sync cost measured under
netem; the one real GPU gives single-GPU baselines only.

**Scaling map.** One variable at a time with the rest fixed: payload 1 KB to
2 GB, node count 1 to 8, item count and cost per item, bandwidth and latency
profiles, loss, node speed spread up to 10x, cold and warm environment; for
training, model size, batch, sync interval, int8, ranks per node. Each step's
growth is fitted on a log-log scale from the run records, and a step whose
exponent beats the expected one by more than 0.15 is flagged as a bug.

**Real points.** Two laptops over Tailscale (the real internet worst case) and
every demo-day run, exported with `pipedpeer jobs --export-all`.

| Result | Value | Source |
|---|---|---|
| One GPU vs split: cell where split wins / where solo wins | TODO(verify) | `docs/benchmarks/ddp-switch.md` (emulated) |
| DiLoCo validation loss gap vs one GPU, per model (mean of 3 seeds, spread) | TODO(verify) | `docs/benchmarks/diloco.md` (emulated) |
| Pool request body: before / after, 100 MB payload | TODO(verify) | `docs/benchmarks/w10.md` (emulated) |
| Workspace re-upload after editing one file | TODO(verify) | `docs/benchmarks/w10.md` (emulated) |
| Closure seed to a node holding every path | TODO(verify) | `docs/benchmarks/w10.md` (emulated) |
| Pool estimate error, before (7-37x) / after | TODO(verify) | `docs/benchmarks/w10.md` (emulated) |
| Scaling map: superlinear steps found | TODO(verify) | `docs/benchmarks/scaling.md`, `scaling-train.md` (emulated) |
| Peer IP change recovered / introducer IP change recovered | TODO(verify) (targets < 30 s / < 60 s) | `make test-net` |
| Tailscale, two laptops: cpu.py, gpu.py, both.py | TODO(verify) | manual checklist |
| Demo-day runs | TODO(verify) | `jobs --export-all` |

---

## Chapter 6  Conclusion, limitations, future work

### 6.1 Conclusion

PipedPeer runs an unmodified Python script on other machines with no cluster to
configure. It:
- reproduces the script's environment exactly, and sends only the store paths
  a machine lacks;
- distributes the parallel work inside the script by measured speed, without
  touching the source;
- joins machines on different networks with one command and no relay, and
  keeps them joined when their addresses change.

The project's recurring discipline is refusing to ship a feature in a form that
measured worse than doing nothing:
- the DDP blackboard was replaced by daemon-side reduction;
- infrequent sync stays opt-in: the old auto interval trained a worse model,
  but that was measured under fp16 averaging and full-batch ranks, so DiLoCo is
  promoted only if its own benchmark passes (5.12);
- work is divided by measured speed, because headcount was slower than local;
- the matmul "never ship" rule was replaced by a check that the choice made was
  not the slower one.

### 6.2 Limitations

**Of the system.**
- **No relay.** Peer pairs with no direct path cannot cooperate.
- **Python only.**
- **Linux workers.** The private-store seed is x86_64 only.
- **A Pool script needs its submitter.** A `--remote` job survives the submitter
  vanishing and can be attached later (4.8), but a Pool script runs on the
  submitter, so its machine must survive the job.
- **DDP needs rank 0's machine.** A run continues without any other rank, but
  rank 0's daemon holds the sync; losing it stops the run with a plain reason.
  Re-spreading needs a DataLoader.
- **GPUs are NVIDIA only in practice.** AMD is detected but has no ROCm torch;
  Intel integrated graphics never count.
- **Phones run Pool work only**, through Termux, CPU only.
- **`--offload` cannot be shown in the container lab.** Containers share the
  host's kernel boot ID, so they count as one machine.
- **Imports outside the table and nixpkgs fail.** An unknown import is looked up
  in nixpkgs and reported before the run if it is not there (4.6); it is no
  longer dropped silently.
- **Only `ProcessPoolExecutor.map` is intercepted**, not `submit`; pandas
  groupby on categorical keys is never split; numpy work under 32 MB never
  ships, by design.
- **The events feed is per machine.** The web page's recent runs merge every machine's history, but the event feed does not.
- **A partial sandbox.** It runs as uid 0 inside, has no network namespace and
  no seccomp, and is skipped on nodes without crun or userns unless the store
  is private.

**Of the evaluation.** One host for the lab; the multi-machine figures are hand
measurements from commit messages.

**Defects found while preparing this report**, as recorded on 2026-10-11, with
what has happened to each since (details in `known-issues.md`):
1. **Home directory under git.** When `$HOME` is a git repository, the build
   directory (`~/.cache/pipedpeer/scratch`) lies inside it. Nix refuses the
   untracked `flake.nix`, and the coordinator retries forever. *Fixed:* builds
   run outside the user's git tree (`35a0050`).
2. **Tests that fail without torch.** Three tests fail rather than skip.
   *Fixed:* they skip where torch is missing (`f82ef2c`).
3. **Fresh lab never distributes.** A job placed on a node that already holds
   its closure never seeds its peers, so the fault-injection harness could not
   run (inferred). *Cause found and fixed:* every worker refused the broadcast
   closure for want of a trusted key (4.3; `8484714`, `eeacb55`, `ee8a1ee`).
4. **Suspected defects found by reading the code, not exercised.**
   - Compressed pool items: the daemon drops the `items_zlib` flag. *Fixed in
     `049a1aa`.*
   - Micro-chunk path: it replies in JSON when the shim expects frames.
     *Fixed in `0cffbbb`.*
   - Forwarded work: it is counted as local in receipts, and forwarding is not
     strictly one hop. *The receipt now counts the items this machine runs
     (`5b89f8c`).*
   - Internet peers: `RemovePeer` deletes every loopback forwarder. *Fixed in
     `024bb6e`.*
   - LAN discovery: the shared secret is sent in TCP sweep probes. *Fixed in
     `4885a42`.*
   - Firewall: the commands run without sudo. *Fixed in `9b68937`.*
   - Nix trust: trusted keys are not bound to an authenticated identity.
     *A closure's signer key is now checked against its published identity
     (`eeacb55`); the key list itself is still built from the node store.*
   - Private store: path checks look at the host `/nix/store`.
   - Setup: the nix-imports step aborts before the daemon starts on a fresh
     multi-user Nix.
   - Trusted keys file: the daemon logged a failed rename of
     `trusted-public-keys` during this run. *Each write now has its own temp
     file (`202fef6`).*

### 6.3 Security

See 4.3. In summary:
- **What exists.**
  - A shared secret on every route, created before the first daemon start and
    carried by the invite code.
  - One identity per node (key fingerprint), with TLS pinned by node id.
  - Signed address gossip; introducers answer only their own cluster.
  - Pinned, opportunistic TLS on a LAN.
  - Authenticated QUIC across networks, with the hello bound to its TLS session
    and the cluster secret.
  - The token sent only to hosts that prove they hold it.
  - Signed closures, with setup never disabling `require-sigs`, and peer keys
    reaching system Nix through the root-owned key helper.
- **What does not.**
  - Per-user authority.
  - Mandatory TLS.
  - Identity-bound signing trust for the key list as a whole.
  - Signature checks on the legacy upload path.
  - Authentication on LAN discovery (its answers are only candidates).
- **`SECURITY.md`** describes the system as it is (`8c83185`), including the key
  helper (ADR-0018).

### 6.4 Future work

- **Close the security gaps listed in 6.3.**
- **A relay of last resort** (still rejected; Tailscale covers the gap).
- **Placement.** Make it locality-aware and topology-aware.
- **Submitter fault tolerance** for Pool scripts, and DDP that survives losing
  rank 0's machine.
- **A cluster-wide events view.**
- **Other runtimes.**
- **A controlled multi-machine evaluation.** Include the fault-injection run and
  a two-GPU machine.

---

## Appendix A  Traceability

| Claim | Evidence |
|---|---|
| Store path is the cache key | `nixgen/flake.go:35`; 5.2 |
| Path-level closure diff | `daemonctl/closurediff.go`; `/v1/store/missing`; commit 193e1bf |
| Signed transfer | `daemonapi/signedclosure.go`, `narpack/`, `nixsign/`; commit cf2e3ae |
| Setup never writes require-sigs=false | `setup/setup.go:153-295`; mutation audit case |
| Shared secret on every route | `authtoken/token.go`; `authtoken/token.go:136-171` |
| Pinned TLS, opportunistic | `tlsid/dial.go:16-21, 106-110`; `tlsid/listener.go` |
| Hole punching, birthday collision | `direct/direct.go`, `direct/birthday.go`, `internet/connect.go` |
| No relay in the data path | `internal/relay` deleted; commit 95e0712 removed it from the daemon |
| Router mapping published only if it matches | `internet/hints.go:121-147` |
| Placement scoring | `coordinator/nodes.go:126-158` |
| Lease constants | `daemonapi/server.go:28-35` |
| Staleness thresholds | `daemonapi/nodes.go:199-210`; `nodestore/store.go:122-125` |
| Discovery is UDP, not mDNS | `discovery/discovery.go:39-47` |
| Import scan is a line parser | `pythondeps/deps.go:177-199` |
| Makespan work sharing | `schedule/schedule.go:129-300`; `daemonapi/poolrates.go` |
| Daemon-side weighted gradient average | `daemonapi/ddpreduce.go`; mutation audit case |
| Per-job and daemon memory ceilings | `cgroups/`; `daemonapi/execrun.go:436-461` |
| Sandbox namespaces, no netns or seccomp | `daemonapi/execrun.go:401-434`; `sandboxquirks.go:141-147` |
| Hash shuffle exact in value | `TestShimHashShuffleMatchesLocal` |
| Remote failure degrades to local | `TestShimRaceCorrectWithDeadRemote` |
| Unknown import resolved or reported | `pythondeps/`; `app/env.go:90-106`; commits 15d37b2, 2c91f14, f3b564d |
| Cancel stops the job everywhere | `daemonapi/execsupervise.go`, `daemonapi/poolhandlers.go`, `coordinator/execute.go`; commits d961dbb, dcfa6f5, d28c69e |
| Background mode and attach | `daemonapi/joblog.go`, `daemonapi/attach.go`, `daemonctl/attach.go`, `app/attach.go`; commits ca853bb, 272ca83 |
| DDP survives a dead rank | `ddp_cmd.go` runDDP; shim sampler; commit 8e14b9b |
| Key helper | `setup/`, `nixsign/`; ADR-0018; commit 454754a |
| `--offload` keeps spread work off this machine | `run_cmd.go`, `daemonapi/poolhandlers.go`, shim `PIPEDPEER_OFFLOAD`; `offload_test.go`; commit ef68f02 |
| Usage lines in the summary | `app/selfuse.go`; commits ca70da4, 98e8684 |
| Closure refusal skips one node; 3 failed runs at most | `coordinator/execute.go` `ExecuteWithRetry`; `fxplace_test.go`; commit 1e07d56 |
| Slow peer raced, not discarded | `daemonapi/poolremote.go`; commit 8541355 |
| Dead node told from a lost network | `app/lostconn.go` `networkUp`; commit 879b86c |
| Ctrl+C stops nix export and pack | `nixstore/nixstore.go` `CmdContext`; `nixstore/cancel_test.go`; commit 99cf0ad |
| DDP start barrier | `daemonapi/ddpsync.go`; `ddpstart_test.go`, `ddpstart_shim_test.go`; commits c567369, 9b47502 |
| Rejoin catch-up batch, buffers in the snapshot | shim; `ddpup_shim_test.go`; commit 82596fe |

## Appendix B  What changed

**Since the May 2026 report.**

| Previous report | Current implementation |
|---|---|
| bubblewrap sandbox | `crun` OCI bundle |
| Push dispatch over a message bus | HTTP and WebSocket (the embedded bus was deleted in the address-book phase) |
| `internal/taskqueue/`, JetStream | Deleted; retry is a loop in the submitting CLI |
| mDNS discovery | UDP multicast and broadcast, TCP sweep fallback |
| Python AST import scanner | A hand-written line parser |
| HealthScore with three terms | Eight terms, plus a five-term GPU score |
| "Parallel job splitting is not implemented" | The interception shim |
| Placeholder results | Measured results |

**Since the August 2026 version of this document (`b774de8`).**

| August | Now |
|---|---|
| Trusted LAN, no authentication or encryption | Optional shared secret on every route; pinned TLS; authenticated QUIC across networks |
| Discovery beyond the LAN only via registry or manual peers; NAT traversal as future work | Joining through an introducer; PCP, NAT-PMP and UPnP; hole punching with birthday fallback; direct QUIC; no relay |
| Closure sent whole, unsigned; setup could write `require-sigs = false` | Path-level diff; signed binary-cache transfer between daemons; never disables signature checking |
| Pool kernels by reference (never worked remotely for `__main__`) | Source tier plus cloudpickle by-value tier; receipts |
| Even split across peers | Makespan solver over measured rates; slow peers declined |
| DDP daemon a blackboard; averaging in the shim | Daemon reduces in float64, sample-weighted; fp16 default, int8 opt-in; measured rank placement; mid-run refit |
| No cgroup limits | Per-job `memory.max`; daemon scope at half of RAM; roommate accounting |
| Root needed for `/nix` and crun | Private store in a user namespace; crun in `~/.local/bin` |
| gzip archives | zstd |
| Unbounded caches | LRU, 8 GiB each by default |
| Regex import scan | Line parser handling `import a, b` |
| "Dense matmul never ships" as a rule | Cost model decides; ships when it does not fit or a GPU peer favours it |
| `bench-shim-d2.sh` not in CI | In CI, with `check-skips.sh` and `bench-pool.sh` |
| 249 tests run (246 passed, 3 skipped) | 617 tests run (615 passed, 2 skipped); 95-case mutation audit |

**Since the October 11 version (`5bcf140`): the leftovers and demo rounds.**

| October 11 | Now |
|---|---|
| Direct hello signs a signer-chosen nonce; replayable to a third node | Hello signs the TLS exporter value and proves the secret by HMAC (4.2) |
| Token sent to any host the LAN sweep found | Token sent only after the host proves it holds it (4.3) |
| A peer with no path only logged | Listed with its reason under the node table (4.2) |
| First closure on a fresh cluster refused by every worker | Keys passed per import; signer named in the archive; root-owned key helper on multi-user Nix (4.3) |
| Punching exercised only at demos | `lab/nat`; four NAT-link defects found and fixed (4.2) |
| Unknown import dropped silently; main script scanned only | nixpkgs lookup, `requirements.txt`, local modules followed (4.6) |
| No way to stop a running job | Ctrl+C or SIGHUP stops it everywhere, Pool chunks and DDP ranks included (4.8) |
| No background mode since `f46d354` | `--detach`, Ctrl+Z, `attach`; lease follows the job; 24 h retention (4.8) |
| Output split into lines | Raw chunks; `\r` progress bars animate (4.8) |
| Job with no estimate uncapped; no ceiling without systemd | 4 GiB default cap; memory watchdog (4.8) |
| One dead DDP rank ends the run | Survivors continue on rendezvous-hash shards; a returning machine rejoins at the next epoch (4.9) |
| DDP rank 0 trained while others still received the closure | Start barrier with named waits; late ranks refused; a rejoiner runs one catch-up batch per missed epoch (4.9) |
| Sender's own spread work could land on it | `run --offload`; usage lines for this machine and each node (4.9, 4.14) |
| Retried non-permanent failures forever; one refusing node failed the run; `--remote` waited 15 s | 3 failed runs at most; refusing node skipped; 10 min wait with reasons every 30 s (4.4, 4.5) |
| With two machines, a dead node looked like this machine offline | Gateway and introducer probes decide (4.8) |
| Ctrl+C left nix-store packing | Nix runs in its own process group and is stopped (4.8) |
| Daemon activity only in `daemon.log` | `events`, CPU and GPU in the node table and `dashboard`, join watch, run summary, real `--check-only` (4.14) |
| N7 measured distribution, not idle cost | Declined workload: about 0.12 s per job (5.3) |
| Tests: 617 run | 829 run (822 pass, 7 skip) <!-- evidence fix5/go-test-json.jsonl (cc56753, go test -count=1 -p 1 -json ./..., top-level tests) --> |

**Since the October 12 version: the address-book phase (after the 2026-10-14 demo).**

| Before | Now |
|---|---|
| One standalone introducer; its IP change broke the cluster | An introducer in every daemon; a node registers with all it knows (4.2) |
| One address per node, overwritten by every source | Every path per node id; switch only to a verified path; signed gossip (4.1) |
| Three identities (UUID, key fingerprint, separate TLS key); pins by `host:port` | Node id = key fingerprint; TLS from the node key; pins by node id (4.3) |
| A moved peer back after ~3 min; manual re-join on every machine | Link replaced on reconnect; QUIC migration; re-register on address change (4.2) |
| Secret and introducer typed by hand; seven prep scripts | `invite` prints one `connect` line; connect says connected, partly or failed (4.2, 4.15) |
| Ports 38xxx plus gloo's range | 4242-4244 (4.15) |
| 28 commands, several config files | 13 commands, `cluster.json` (4.15) |
| Stage, coordinator and shim lines on every run | The script's output only; `-v` for details; simple view of the run record (4.15, 5.12) |
| `--epochs` after the script refused as an unknown flag; exit code always 1 | Script flags pass through; the script's exit code (4.15) |
| Each rank ran the full batch; fp16 gradients; split always kept | Batch split to the script's batch; int8 with error feedback; one GPU or split decided from measured steps (4.9) |
| Lead could be a CPU node; mismatched models trained on alone | GPU lead when one exists; manifest check aborts every rank (4.9) |
| Daemon down after a reboot | Optional systemd user unit (4.15) |
| UI asked for the token | Token in the URL fragment from `pipedpeer ui` (4.3) |
| Workspace sent whole, job dirs kept 7 days | Only missing files; job dirs removed after results are fetched or 24 h (4.15) |
| Relay diagnostics, NATS, registry, gloo, torch matmul offload | Deleted |
