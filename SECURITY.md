# Security

## Threat model, stated plainly

Pipedpeer runs code, supplied by whoever is in the cluster, on every machine in
it. **Cluster membership is the trust boundary.** Admit only machines and people
you would let run code on your own.

What protects the boundary:

- **A shared token on every route.** The first machine's `pipedpeer setup`
  creates it, before the daemon first starts, and `pipedpeer connect <code>`
  copies it to the others; the daemon refuses any request without it (`/health`
  included). The invite code contains the token: share it like a password.
- **One identity per machine.** The node id is the fingerprint of the machine's
  ed25519 node key, and the daemon's TLS certificate is made from that key.
- **Pinned, opportunistic TLS** on the daemon port, pinned by node id, not by
  address: a certificate whose key does not match the expected node id is
  refused, and a different machine that later takes over an old peer's IP is
  simply a new node. It is opportunistic: an attacker who can break
  the handshake can force plain HTTP, so the token check, not TLS, is what
  keeps strangers out.
- **The QUIC hello between internet peers is bound to the session** and proves
  the holder knows the cluster token, so a hello cannot be replayed on another
  connection and knowing the cluster ID alone is not enough.
- **The token is sent only to hosts that prove they hold it.** Before a token
  goes to a non-loopback host, that host answers a challenge on `/health`, so a
  look-alike on the network does not receive it.
- **The cluster page at `/ui` loads without the token** (a browser cannot send the header). `pipedpeer ui` opens it with the token in the URL fragment (`#token=…`), which the browser never sends to a server; the page stores it and drops it from the address bar. Its data stays behind the token. There is no loopback exemption: internet peers reach the daemon through 127.0.0.1 forwarders, so loopback proves nothing.
- **Introducers answer only their own cluster.** Every daemon is an introducer for its cluster; a request for another cluster id is refused.
- **Address gossip is signed** by each member's node key and only ever yields candidate addresses, which are used after they answer and prove the node id.
- **Closures are signed** with the node identity key and imported with the
  receiving Nix's own signature policy.

What does not protect you:

- The `crun` sandbox is **not** a security boundary against a hostile peer: no
  seccomp profile, no capability dropping.
- Any job's code can read the token (it is passed as `PIPEDPEER_TOKEN`).
- One token for everyone: there is no per-user or per-node authorisation.
- LAN discovery is unauthenticated (it only yields candidate addresses; the link still needs the token and the pinned key).
- **On a multi-user nix, cluster keys reach system nix automatically.** Setup
  installs a root-owned helper and a sudoers rule letting your user run it
  without a password ([ADR-0018](docs/adr/0018-root-owned-nix-key-helper.md));
  the daemon uses it to make nix trust each peer's signing key. So anyone with
  the token, and any process running as your user, can get signed store paths
  into the system store that every user of that nix shares. That is the same
  exposure a manual trust step would give, now without a prompt. The helper
  accepts only well-formed pipedpeer keys and writes only its own include
  file. `pipedpeer uninstall` removes it and the keys.

## What you must do

- Share invite codes only with people you would let run code on your machine.
- Do not expose the daemon port (4242/tcp) to the internet; members reach each
  other over the direct QUIC links (4243/udp) and the introducer (4244/udp).
- Use firewall rules to restrict the daemon port to known peer addresses.
- Do not run a daemon on a machine holding secrets you would not hand to any
  other member of the cluster.

## Reporting a vulnerability

Report suspected vulnerabilities privately by opening a
[GitHub security advisory](https://github.com/vinayakyadav2709/Pipedpeer/security/advisories/new).
Please do not open a public issue for a vulnerability.

Findings that amount to "a cluster member can run code" are
the documented design above rather than a new vulnerability — but reports about
the *scope* of that exposure (for example, reachability beyond the intended
network, or a path that escapes the sandbox) are very welcome.
