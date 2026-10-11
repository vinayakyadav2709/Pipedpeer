# Security

## Threat model, stated plainly

Pipedpeer runs code on other machines. A daemon that accepts a job runs that
job's code. Everything below is about **who** can submit one, and what an
attacker on the path can do. It is not about limiting what an accepted job can
do: an accepted job runs code on that machine.

Every protection is optional in the sense that a node with none configured
still runs, and **a node with no shared secret accepts work from anyone who can
reach its port**. Configure one (`pipedpeer auth set`) on every machine in the
cluster before using it on any network you do not fully control.

## What is in place

**A shared cluster secret** (`src/internal/authtoken`).
- `pipedpeer auth set` generates a 32-byte secret, or stores one you give it.
  It is stored with mode 0600.
- With a secret configured, every daemon route requires it, `/health`
  included. The comparison is constant-time.
- Pipedpeer's own requests to `/health` and `/v1/*` carry it. Requests to
  anything else never do.
- LAN discovery probes every address on the local subnet. They carry an HMAC
  proof of the secret, bound to the address probed and to the time, and never
  the secret itself. The proof only opens `GET /health`, and only for two
  minutes.
- Jobs receive the secret as `PIPEDPEER_TOKEN`, so the shim can reach its own
  daemon. **Any job's code can therefore read it.**

**Pinned TLS on the daemon port** (`src/internal/tlsid`).
- Each daemon has a self-signed certificate.
- Peers pin its fingerprint on first contact, and refuse a changed one.
- This is trust on first use, and it is opportunistic: a peer whose TLS
  handshake fails for any reason other than a pin mismatch falls back to plain
  HTTP. An attacker who can break the handshake can force that.
- Peers across networks are reached over QUIC, which is encrypted.

**Node identity and signed closures** (`src/internal/identity`,
`src/internal/nixsign`).
- Each node has an ed25519 key. The same key signs the closures (the Nix
  environments) it sends.
- `pipedpeer setup` adds the cluster's keys to the system Nix configuration as
  `extra-trusted-public-keys`, with consent. It never turns signature checking
  off.

**Job sandbox.**
- `pipedpeer run` runs a job under `crun` with a cgroup memory limit by
  default (`--isolate`), so a runaway job cannot take its node down.
- The `pipedpeer python` shorthand turns the sandbox off (`--isolate=false`).

## What is not covered

- **One shared secret.** There is no per-user or per-node authorisation. Every
  holder can run code on every node, and so can every job.
- **The sandbox is partial.** A job is root inside its container, shares the
  host network, and has no seccomp filter. Do not run a daemon on a machine
  holding secrets you would not hand to every other member of the cluster.
- **The trusted-key list is not tied to an authenticated identity.** Any node
  in the node store contributes its key.
- **Some imports skip signature checking.** There is no check on a private
  store or a single-user Nix. The older transfer format, still used for a
  submitter's own upload, imports with `--no-check-sigs`.
- **The optional standalone registry has no authentication.**
- **The QUIC hello nonce is chosen by the signer and not bound to the
  session.**

## What you should do

- Set a shared secret on every machine: `pipedpeer auth set`.
- Expose the daemon port (38080) only to the machines in the cluster.
- Treat every cluster member, and every job submitted to it, as able to run
  code on every node.

## Reporting a vulnerability

Report suspected vulnerabilities privately by opening a
[GitHub security advisory](https://github.com/vinayakyadav2709/Pipedpeer/security/advisories/new).
Please do not open a public issue for a vulnerability.

Reports that the items under "What is not covered" exist are already known.
Reports about their scope, or anything that reaches beyond them, are very
welcome.
