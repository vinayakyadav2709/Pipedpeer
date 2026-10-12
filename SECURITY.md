# Security

## Threat model, stated plainly

Pipedpeer runs code, supplied by whoever is in the cluster, on every machine in
it. **Cluster membership is the trust boundary.** Admit only machines and people
you would let run code on your own.

What protects the boundary:

- **A shared token on every route.** `pipedpeer auth set` defines the cluster;
  the daemon refuses any request without it (`/health` included). With no token
  set the daemon is open to anyone who can reach the port.
- **Pinned, opportunistic TLS** on the daemon port (trust on first use). A
  changed certificate is refused. It is opportunistic: an attacker who can break
  the handshake can force plain HTTP, so the token check, not TLS, is what
  keeps strangers out.
- **The QUIC hello between internet peers is bound to the session** and proves
  the holder knows the cluster token, so a hello cannot be replayed on another
  connection and knowing the cluster ID alone is not enough.
- **The token is sent only to hosts that prove they hold it.** Before a token
  goes to a non-loopback host, that host answers a challenge on `/health`, so a
  look-alike on the network does not receive it.
- **The cluster page at `/ui` loads without the token** (a browser cannot send the header); it asks for the token, and its data, `/v1/stats`, stays behind it.
- **Closures are signed** with the node identity key and imported with the
  receiving Nix's own signature policy.

What does not protect you:

- The `crun` sandbox is **not** a security boundary against a hostile peer: no
  seccomp profile, no capability dropping.
- Any job's code can read the token (it is passed as `PIPEDPEER_TOKEN`).
- One token for everyone: there is no per-user or per-node authorisation.
- The standalone registry and LAN discovery are unauthenticated.
- **On a multi-user nix, cluster keys reach system nix automatically.** Setup
  installs a root-owned helper and a sudoers rule letting your user run it
  without a password ([ADR-0018](docs/adr/0018-root-owned-nix-key-helper.md));
  the daemon uses it to make nix trust each peer's signing key. So anyone with
  the token, and any process running as your user, can get signed store paths
  into the system store that every user of that nix shares. That is the same
  exposure a manual `pipedpeer trust` gives, now without a prompt. The helper
  accepts only well-formed pipedpeer keys and writes only its own include
  file. `pipedpeer setup --uninstall` removes it and the keys.

## What you must do

- Set a token (`pipedpeer auth set`) on every machine before it joins anything
  but a network you fully control.
- Never expose the daemon port (`38080`) to the internet without a token.
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
