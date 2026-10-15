# Ports move to the 4242 block in one hard cut

The daemon API, discovery, direct links and the introducer each had a five-digit port in the 38xxx range, plus gloo's ten-port training range, three reflector and relay-diagnostic ports and a registry port: more numbers than anyone could remember to open on every firewall and to remember on demo day. Now:

| Port | Use |
|---|---|
| 4242/tcp | Daemon API (token-gated) |
| 4242/udp | LAN discovery (multicast group 239.255.0.99 unchanged) |
| 4243/udp | Direct QUIC links |
| 4244/udp | Introducer |

There is no training port: gloo was removed ([ADR-0010](0010-gradient-sync-through-the-daemon.md)); the reflectors, relay diagnostics and registry were deleted.

The move ships in the same release as the one-identity change ([ADR-0008](0008-pinned-tls-and-key-addressed-nodes.md)) and signed address gossip, with the direct hello at ALPN `pipedpeer-direct/3`: one break instead of three, following [ADR-0006](0006-hard-cuts-for-security-wire-changes.md).

## Consequences

- Older builds are refused; upgrade every machine of a cluster together (`install-pipedpeer.sh --version <v>` gives every machine the same build).
- The constants live in one place each: `daemonctl.DefaultPort`, the discovery port, `internet.DefaultPort`, `clustercfg.DefaultPort`.
