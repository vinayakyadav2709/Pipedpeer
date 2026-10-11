# PipedPeer: system report

Source material for the major project report and the conference paper. Written
against commit `4c91ef3` (2026-09-01) and checked against the code on
2026-10-02. Every non-obvious claim carries a file reference relative to `src/`.
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

Appendix B tabulates both sets of differences.

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
| Cobra and Viper | CLI structure and configuration |
| Bubble Tea, Lip Gloss | Dashboard TUI |
| NATS (embedded) | Optional alternate transport for lease messages |
| `zerolog` | Structured logging |
| Python (embedded shim) | The interception layer, 3,822 lines shipped inside the binary |
| cloudpickle | Added to every generated environment, to ship lambdas and closures by value |

### 1.5 Assumptions and constraints

- **One binary.** Every participating machine runs the same binary. `pipedpeer
  setup` installs Nix and `crun` if they are missing, and it can do so without
  root, using a private Nix store (4.7).
- **Linux workers.** Workers must run Linux, because `crun`, cgroups and the
  Nix store are Linux-specific. The submitting side builds for macOS as a
  client (the CI build matrix covers linux and darwin, amd64 and arm64).
- **Shared secret.** Machines in a cluster share one secret (`pipedpeer auth
  set`). It is **optional**: a daemon with no secret configured accepts every
  request, as earlier versions did (4.3).
- **Introducer.** Joining across networks needs one machine with a public
  address to act as introducer. It never carries job data.
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
same binary does both. The **introducer** (`pipedpeer rendezvous`) is the one
role a machine can take that is not symmetric. It is optional: a LAN cluster
needs none.

See `fig_3.1_system_design.png`.

The subsystems, all under `src/internal/` (37 packages, plus `main`):

| Package | Responsibility |
|---|---|
| `coordinator` | Filter, score and choose a node; take a lease; retry |
| `daemonapi` | HTTP and WebSocket surface (22 routes), leases, sandbox, pool spill, DDP reduction, closure caches |
| `schedule` | Makespan-optimal division of work across measured devices |
| `ddpplace` | Choosing the ranks of a training ring by measured throughput |
| `discovery` | LAN: UDP multicast and broadcast, TCP `/health` sweep fallback |
| `internet` | Peer manager for joining across networks: publish, punch, link, forward |
| `direct` | Shared UDP socket, candidate race, birthday-collision fallback, QUIC links |
| `rendezvous` | Introducer address book and punch-request forwarding |
| `nattype` | STUN client and NAT mapping and filtering classification (diagnostics) |
| `portmap` | PCP, NAT-PMP and UPnP-IGD port mapping |
| `clustercfg` | The remembered introducer address |
| `authtoken` | Shared-secret middleware and outbound injection |
| `tlsid` | Self-signed daemon certificate, pin-on-first-use, TLS and HTTP on one port |
| `identity` | Node UUID and ed25519 node key |
| `nixsign` | Signing closures with the node key; the trusted-key list |
| `narpack` | Signed closure archives; LRU pruning of closure caches |
| `nixstore` | System or private (rootless) Nix store |
| `nodestore` | SQLite table of known peers and their last observed capacity |
| `nixgen` | Flake generation, wheel pins, and the interception shim as an embedded asset |
| `pythondeps` | Import scanning and import-to-package mapping |
| `resourceest` | Tiered memory estimate |
| `cgroups` | Daemon and per-job memory ceilings; the daemon's memory budget |
| `tarcodec` | zstd, gzip or plain archive sniffing |
| `heartbeat`, `gpu` | Capability and load reporting; vendor GPU detection |
| `jobhistory`, `registry` | Per-job records; optional standalone directory |
| `daemonctl`, `setup`, `userdir`, `userns`, `netaddr`, `relay`, `natsbus`, `logging`, `ping`, `app` | Supporting |

`relay` still exists, and so do the `relay-test` and `relay-connect`
diagnostics, but the daemon does not import it. No job traffic is relayed
(4.2).

### 3.2 Data flow

**Level 0** (`fig_3.2_dfd_level0.png`) has four external entities:
- the user;
- peer nodes;
- the introducer (optional);
- the registry (optional).

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
- **Operator:** installs prerequisites, sets the cluster secret, and joins the
  machine to a cluster.
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
- `discovery/discovery.go` opens two sockets on port **38099** (:39):
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
error. When the UDP probe returns nothing, `discovery/tcp.go` sweeps the first
254 addresses of each local subnet, with 64 probes in flight and a 700 ms
`GET /health` per address, and stops issuing probes once any daemon answers
(:14-20, :34, :46, :60-70, :82).

**Node store.** `nodestore/store.go` keeps peers in SQLite, and a peer poller
runs every 10 s with a 3 s timeout. A single failed poll marks a node
unreachable at once (`MarkUnreachable`, `nodestore/store.go:322-324`). Two
further thresholds apply:
- a row not refreshed for **45 s**, for example while the local daemon itself
  was down, is not reported healthy (`daemonapi/server.go:1287`);
- after **5 min**, an auto-discovered row is deleted (`server.go:1620`). Manual
  rows, which include peers linked across networks, are never deleted by this
  rule.

### 4.2 Joining across networks

A LAN broadcast cannot reach a machine behind another router, and that machine
will not accept an inbound connection. `fig_4.1_nat_traversal.png` shows how
two such machines come to share a direct, encrypted link.

**One command.**
- `pipedpeer join <introducer>` does the following (`main.go:244-310`):
  - normalises the address (default port 38445);
  - saves it to `cluster.json` (`clustercfg/clustercfg.go`);
  - restarts the daemon in internet mode;
  - waits up to 90 s for peers, then prints the node table.
- `pipedpeer start --rendezvous` and `PIPEDPEER_RENDEZVOUS` do the same without
  waiting.
- A later plain `start` rejoins from the saved address.

**The introducer is an address book, not a relay.**
- `pipedpeer rendezvous` runs:
  - two STUN-style reflectors (UDP 38443, 38444);
  - an address book on UDP 38445 (`rendezvous/rendezvous.go`).
- A node registers its candidate addresses under a cluster ID derived from the
  secret: `sha256("pipedpeer-rendezvous\0" + token)`, first 8 bytes. The
  introducer therefore never holds the secret.
- A `connect` request makes the introducer forward one punch datagram to the
  target. That datagram carries only the addresses the caller itself
  registered. `connect` is rate-limited per source (4 per second, burst 8).
- The introducer never carries job data. Over a whole verification run the
  introducer host sent 0.2 MB, while about thirty megabytes crossed between the
  peers (commit `42c2426`).
- The same command can also serve a relay on 38446. The daemon never uses it.

**Candidates.** Each node offers three kinds of candidate address
(`direct/direct.go:58-89`):
- `lan`: its LAN addresses;
- `mapped`: a port its router agreed to forward;
- `reflex`: the address the introducer saw.

The router is asked through PCP, then NAT-PMP, then UPnP-IGD
(`portmap/portmap.go`), for a 2 h lease that is renewed at half the lifetime
actually granted. A mapping is withheld if its external IP differs from the one
the introducer saw. Only the IP is compared, and on the first poll, before any
reflexive address is known, the mapping is published unchecked
(`internet/internet.go:340-358, 801-809`). The check exists because a mapping on
a router behind a second NAT, such as a phone tether, would otherwise be
published as reachable when it is not.

**One socket.**
- Punch probes, introducer messages and QUIC all share one UDP socket on port
  **38447** (`direct/sharedconn.go`).
- A router's mapping belongs to the socket that opened it, so the connection
  must arrive on the same socket the probes left from.
- Datagrams are told apart on arrival:
  - probes by four magic bytes;
  - introducer messages by a leading `{`;
  - everything else is QUIC.

**The race.**
- Both sides probe every pairing of their candidates every 250 ms, for a 15 s
  window (`direct/direct.go:374-414`; `internet.go:441`).
- Trying candidates one at a time would add a timeout per address that cannot
  work.
- When the race fails because one side's NAT allocates a new port per
  destination, the **birthday collision** fallback runs (`direct/birthday.go`):
  - that side opens 256 sockets toward the peer;
  - the other sends 600 probes to random ports between 1024 and 65535;
  - the socket that is hit is the one used.

**The link.**
- The winning path carries QUIC (ALPN `pipedpeer-direct/1`, TLS 1.3).
- Both ends then exchange a hello signed with their ed25519 node key, and each
  checks:
  - that the peer's key fingerprint is the node it meant to reach;
  - that the cluster ID matches (`direct/transport.go`).
- The link is then exposed to the rest of the daemon as a TCP forwarder on
  `127.0.0.1:<port>`, registered as an ordinary manual node (`internet.go:541`).
  Each local connection becomes one QUIC stream to the peer's daemon. The
  scheduler, the closure transfer and the gradient sync therefore all work over
  the link without knowing it exists.
- `pipedpeer nodes` shows how each peer is reached in a PATH column:
  - `self`;
  - `lan`;
  - `mapped` or `reflex`, for the candidate kind that answered;
  - `punched`, for a collision hit;
  - `punched-in`, when the peer dialled us.

**No relay.**
- A peer with no direct path is not relayed. It is logged with the reason, never
  handed to the scheduler, and retried after a back-off (`internet.go:597-607`).
- A relay was built (commit `a6e0723`) and then removed from the data path
  (`95e0712`), because it would carry every byte of every job.
- A live link is never expired because the introducer went away. A peer was
  reached in 0.03 s with the introducer suspended (`42c2426`).

### 4.3 Security and identity

The August design assumed a trusted LAN and had no authentication. The current
one has three layers, all of them optional in the sense that a node with none
configured still runs. `fig_4.2_trust.png` shows them.

**Shared secret** (`authtoken/token.go`).
- `pipedpeer auth set` generates 32 random bytes, or stores a given secret, at
  `~/.local/share/pipedpeer/auth_token` with mode 0600.
- Every route on the daemon, `/health` included, requires it in
  `X-Pipedpeer-Token` or as a bearer token. The comparison is constant-time and
  a mismatch is a 401 (`daemonapi/server.go:1070`).
- Outbound requests to `/health` and `/v1/*` carry it automatically.
- Jobs receive it as `PIPEDPEER_TOKEN`, so that the shim can reach its own
  daemon. A consequence is that any job's code can read the secret.
- **With no secret configured the middleware passes every request**, which is
  the old open behaviour.

**Pinned TLS on the daemon port** (`tlsid/`).
- Each daemon creates a self-signed ECDSA P-256 certificate on first start.
- The same port serves TLS and plain HTTP: the listener peeks at the first byte
  and treats `0x16` as a TLS handshake (`tlsid/listener.go`).
- Outbound requests to another daemon try TLS first, and pin the peer's
  certificate fingerprint on first contact in `known_peers.json`.
- A changed certificate is refused outright, with the command that clears the
  pin (`pipedpeer auth forget`).
- This is trust on first use, and it is **opportunistic**: if TLS fails for any
  reason other than a pin mismatch, the client remembers that peer as plain HTTP
  and falls back. The code says so: an attacker who can break the handshake can
  force plain HTTP (`tlsid/dial.go:16-21`).
- Loopback is never upgraded. Peers linked across networks are reached through
  loopback forwarders, so their encryption comes from QUIC, not from this layer.

**Node identity and signed closures** (`identity/key.go`, `nixsign/`).
- Each node has an ed25519 key with mode 0600. Its fingerprint (first 8 bytes of
  the SHA-256 of the public key) is the node's address in internet mode.
- Signatures are domain-separated by a context string.
- The same key signs closures as a Nix signing key named `pipedpeer-<fp>`.
- Nodes publish the public half in their heartbeat. Each daemon writes the keys
  of the nodes it knows to a `trusted-public-keys` file.
- `setup` adds those keys to the system Nix configuration as
  `extra-trusted-public-keys`, with consent, and **never** writes
  `require-sigs = false` (`setup/setup.go:184-253`; commit `cf2e3ae`).
- A closure sent between daemons in the signed format is imported with `nix copy
  --from`, so the receiving Nix applies its own signature policy. This was
  verified on a machine with `require-sigs = true`: a closure signed by a
  trusted peer imported, and the same closure was refused once the sender's key
  was removed from the trust list (`cf2e3ae`).

**What this does not cover**, stated plainly:
- **One shared secret.** There is no per-user or per-node authorisation, and
  every holder can run code on every node.
- **The trusted-key list is not tied to an authenticated identity.** Any node
  in the node store contributes its key, including one found by unauthenticated
  LAN discovery (`daemonapi/server.go:1578-1615`).
- **Some imports skip signature checking.** No check happens on a private store
  or single-user Nix. The older transfer format, which the submitter still uses
  for its own upload, imports with `--no-check-sigs`
  (`daemonapi/execution.go:1029-1066`).
- **The registry and LAN discovery are unauthenticated.** The optional
  standalone registry has no authentication. Discovery accepts any responder, but
  the token now goes only to hosts that prove they hold it.

### 4.4 Placement and scoring

Candidates are merged from:
- the node store;
- live discovery;
- internet-linked peers;
- the optional registry;
- self.

They are deduplicated by node ID. Then five hard filters run in order
(`fig_4.3_placement_flow.png`):
1. the architecture must match the closure;
2. a GPU must exist if one was required;
3. free memory must meet the request;
4. free cores must meet the request;
5. free VRAM must meet the request.

Each rejection records a reason, which surfaces in `pipedpeer job`.

Survivors are scored by `coordinator.scoreNode` (`coordinator.go:942-976`),
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
utilisation penalty (`coordinator.go:987-1024`).

One thing did change: what a node reports as free memory. `/health` now
advertises what the node can actually *grant*, which is its memory budget less
existing reservations (4.8), rather than the machine's free memory.

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
(`daemonapi/server.go:44-51`).

The concurrency and memory checks and the lease insertion happen under one
lock, so two racing submitters cannot both take the last slot. The cores check
runs just before the lock is taken (`server.go:1757-1767`).

`/v1/accept` checks, in order:
1. cores;
2. the concurrency limit, counting reserved and running leases together;
3. memory against the daemon's budget;
4. GPU devices.

Recovery follows from the design:
- **A dead submitter** stops renewing, and its slot is reclaimed.
- **A dead worker** is marked unreachable at the next failed poll. The in-flight read
  fails, and the coordinator reschedules with back-off doubling from 2 s to a
  2 min ceiling. Tasks are never dropped (`coordinator.go:764-799`).

### 4.6 Reproducing the environment

**Import scanning is a line-oriented parser.**
- The August version used a single regular expression, which kept only the
  first name of `import a, b`.
- `pythondeps/deps.go:155-177` now parses each line by hand:
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
- an external dependency, mapped to a nixpkgs attribute through a 35-entry
  table (`sklearn` to `scikit-learn`, and so on).

An unmapped third-party import is **silently dropped**. There is no package
index lookup. `uv.lock`, whose names replace detection, and `--pkg` are the
escape hatches.

**The store path is the cache key.** `nixgen/flake.go:34` pins an exact nixpkgs
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

### 4.8 Execution, isolation and memory

**The sandbox.** Jobs run under `crun` in an OCI bundle
(`fig_4.7_sandbox.png`; `daemonapi/execution.go:703-737`).
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
- If the job is killed and the cgroup's `oom_kill` counter rose, the client is
  told the job ran out of memory and how to raise `--mem`.

**A memory ceiling for the daemon itself.**
- Where a user systemd is available, `pipedpeer start` launches the daemon in
  a `systemd-run --user` scope with the settings below. A daemon without one,
  such as one in a container, has no enforced ceiling and relies on the
  admission budget alone (`cgroups/cgroups.go:174-269`). The settings:
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

### 4.9 Transparent interception

**Delivery.**
- The shim is 3,822 lines of Python embedded in the Go binary
  (`nixgen/shim.go`).
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
  (`schedule/schedule.go:129-281`). Each device is modelled as a setup time plus
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

**What is intercepted, and how.**

| Primitive | Strategy | Gate |
|---|---|---|
| `multiprocessing.Pool`, `ProcessPoolExecutor.map` | Measure locally, then race local against remote chunks shared by measured speed | 0.5 s of work |
| `numpy.matmul`, `dot`, `tensordot` (2-D) | Row blocks, concatenated | cost model (4.10) |
| `numpy.linalg.svd`, `eig` | Whole matrix to one worker | cost model, unsplit form |
| pandas `groupby().agg()` and named aggregations, `merge`, `join` | Hash shuffle on the key; bucket 0 local | cost model; above 512 MB unless forced |
| `pandas.read_csv`, `read_parquet` | Out-of-core, in 64 MB chunks at record boundaries | file larger than half of free memory |
| `joblib.Parallel`, scikit-learn `n_jobs` | One batch per request | any cluster; no cost model |
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
- **Encoding.** Gradients travel as float16 by default. int8, with one scale per
  tensor and the rounding error carried into the next step, is opt-in with
  `PIPEDPEER_DDP_INT8=1`.
- **Rank placement.** Ranks are chosen by `ddpplace.Select`:
  - it probes each node's throughput (`/v1/bench`);
  - it admits the prefix that maximises *k·rate_k*;
  - it uses one device kind only: GPUs if any are wanted and present.
  
  The shim then sizes each rank's shard and batch by its share.
- **Refitting.** Every 20 syncs the daemon refits shares from the observed step
  rates, if any share moved by 10% or more. It can drop the slowest rank (never
  rank 0, and never below two) when that rank adds more sync than compute. A
  round completes without a straggler after 45 s.
- **The verdict.** At exit, each run prints whether the ring paid for itself,
  counting distinct machines rather than ranks.

### 4.10 The cost model

Every intercepted numpy call is gated (`nixgen/shim.go`,
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

The invariant is that using the cluster is never worse than not using it. Five
layers hold it up:
1. **The race** (4.9).
2. **Per-primitive fallback.** Any exception in an intercepted call falls back to
   the original implementation. The one exception is an out-of-memory matmul,
   which raises (4.10).
3. **Peer fallback.** A failed part tries the next peer, then runs locally.
4. **Worker respawn.** A dead warm worker is restarted, and a one-shot process
   is the fallback.
5. **Rescheduling.** A failed job is placed again with back-off; tasks are never
   dropped.

Memory is bounded at the same time:
- A pool request larger than the node's grantable memory is forwarded to a
  healthier peer.
- A request needing more than 40% of it runs in sequential micro-chunks.
- The `/v1/pool/map` body is capped at the smaller of 2 GiB and half the
  daemon's budget (`daemonapi/pool.go:738-752`).

### 4.12 Resource estimation

The first tier that applies wins (`resourceest/estimator.go`):
1. an explicit `--mem`;
2. the largest peak RSS of the last five runs of the script, plus 20%;
3. per-library baselines plus 100 MB.

The estimator also has a tier based on input file sizes (× 3, plus baselines),
but every caller passes `nil` data files (`main.go:468, 755, 1434`), so that
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

- **Packages:** all 36 packages that have tests pass.
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
  figure below measures it directly.

August measured 0.85 s off against 0.93 s on.

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
| Seven demo workloads end to end | not run for this report |

---

## Chapter 6  Conclusion, limitations, future work

### 6.1 Conclusion

PipedPeer runs an unmodified Python script on other machines with no cluster to
configure. It:
- reproduces the script's environment exactly, and sends only the store paths
  a machine lacks;
- distributes the parallel work inside the script by measured speed, without
  touching the source;
- joins machines on different networks with one command and no relay.

The project's recurring discipline is refusing to ship a feature in a form that
measured worse than doing nothing:
- the DDP blackboard was replaced by daemon-side reduction;
- auto-tuned sync intervals stay opt-in because they trained a worse model;
- work is divided by measured speed, because headcount was slower than local;
- the matmul "never ship" rule was replaced by a check that the choice made was
  not the slower one.

### 6.2 Limitations

**Of the system.**
- **No relay.** Peer pairs with no direct path cannot cooperate.
- **Python only.**
- **Linux workers.** The private-store seed is x86_64 only.
- **The submitter must survive the job.**
- **Unmapped imports are dropped silently** (35-entry table).
- **A partial sandbox.** It runs as uid 0 inside, has no network namespace and
  no seccomp, and is skipped on nodes without crun or userns unless the store
  is private.

**Of the evaluation.** One host for the lab; the multi-machine figures are hand
measurements from commit messages.

**Defects found while preparing this report**, recorded and not fixed (details
in `known-issues.md`):
1. **Home directory under git.** When `$HOME` is a git repository, the build
   directory (`~/.cache/pipedpeer/scratch`) lies inside it. Nix refuses the
   untracked `flake.nix`, and the coordinator retries forever.
2. **Tests that fail without torch.** Three tests fail rather than skip.
3. **Fresh lab never distributes.** A job placed on a node that already holds
   its closure never seeds its peers, so the fault-injection harness could not
   run (inferred).
4. **Suspected defects found by reading the code, not exercised.**
   - Compressed pool items: the daemon drops the `items_zlib` flag.
   - Micro-chunk path: it replies in JSON when the shim expects frames.
   - Forwarded work: it is counted as local in receipts, and forwarding is not
     strictly one hop.
   - Internet peers: `RemovePeer` deletes every loopback forwarder.
   - LAN discovery: the shared secret is sent in TCP sweep probes.
   - Firewall: the commands run without sudo.
   - Nix trust: trusted keys are not bound to an authenticated identity.
   - Private store: path checks look at the host `/nix/store`.
   - Setup: the nix-imports step aborts before the daemon starts on a fresh
     multi-user Nix.
   - Trusted keys file: the daemon logged a failed rename of
     `trusted-public-keys` during this run.

### 6.3 Security

See 4.3. In summary:
- **What exists.**
  - An optional shared secret on every route.
  - Pinned, opportunistic TLS on a LAN.
  - Authenticated QUIC across networks.
  - Signed closures, with setup never disabling `require-sigs`.
- **What does not.**
  - Per-user authority.
  - Mandatory TLS.
  - Identity-bound signing trust.
  - Signature checks on the legacy and private-store paths.
  - Authentication on the registry and discovery.
- **`SECURITY.md`** still describes the earlier no-authentication design and
  should be updated.

### 6.4 Future work

- **Close the security gaps listed in 6.3.**
- **A relay of last resort.**
- **Placement.** Make it locality-aware and topology-aware.
- **Submitter fault tolerance.**
- **Other runtimes.**
- **A controlled multi-machine evaluation.** Include the fault-injection run and
  a two-GPU machine.

---

## Appendix A  Traceability

| Claim | Evidence |
|---|---|
| Store path is the cache key | `nixgen/flake.go:34`; 5.2 |
| Path-level closure diff | `daemonctl/closurediff.go`; `/v1/store/missing`; commit 193e1bf |
| Signed transfer | `daemonapi/signedclosure.go`, `narpack/`, `nixsign/`; commit cf2e3ae |
| Setup never writes require-sigs=false | `setup/setup.go:184-253`; mutation audit case |
| Shared secret on every route | `authtoken/token.go`; `daemonapi/server.go:1070-1093` |
| Pinned TLS, opportunistic | `tlsid/dial.go:16-21, 107-114`; `tlsid/listener.go` |
| Hole punching, birthday collision | `direct/direct.go`, `direct/birthday.go`, `internet/internet.go` |
| No relay in the data path | `relay` imported only by `netcheck.go` (diagnostic commands), not by the daemon path in `main.go` or `internet/`; commit 95e0712 |
| Router mapping published only if it matches | `internet/internet.go:340-358` |
| Placement scoring | `coordinator/coordinator.go:942-976` |
| Lease constants | `daemonapi/server.go:44-51` |
| Staleness thresholds | `daemonapi/server.go:1287,1620` |
| Discovery is UDP, not mDNS | `discovery/discovery.go:39-47` |
| Import scan is a line parser | `pythondeps/deps.go:155-177` |
| Makespan work sharing | `schedule/schedule.go:129-281`; `daemonapi/poolrates.go` |
| Daemon-side weighted gradient average | `daemonapi/ddpreduce.go`; mutation audit case |
| Per-job and daemon memory ceilings | `cgroups/`; `daemonapi/execution.go:745-764` |
| Sandbox namespaces, no netns or seccomp | `daemonapi/execution.go:703-737`; `sandboxquirks.go:141-147` |
| Hash shuffle exact in value | `TestShimHashShuffleMatchesLocal` |
| Remote failure degrades to local | `TestShimRaceCorrectWithDeadRemote` |

## Appendix B  What changed

**Since the May 2026 report.**

| Previous report | Current implementation |
|---|---|
| bubblewrap sandbox | `crun` OCI bundle |
| Push dispatch over NATS | HTTP and WebSocket; NATS embedded on loopback as an optional lease transport |
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
| Discovery beyond the LAN only via registry or manual peers; NAT traversal as future work | `join` through an introducer; PCP, NAT-PMP and UPnP; hole punching with birthday fallback; direct QUIC; no relay |
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
