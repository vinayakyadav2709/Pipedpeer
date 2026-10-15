# No relay in the data path

A relay was built (`a6e0723`) and removed from the daemon (`95e0712`): it carried every byte of every closure over somebody else's uplink and egress bill, even for two laptops on one Wi-Fi. A peer with no direct path is recorded with its reason and not offered work; the scheduler routes around it as it does an unhealthy node, which works because a job needs *a* machine rather than *that* machine. The remaining relay code (`src/internal/relay`, the `relay-test` / `relay-connect` diagnostics and the reflector commands) was deleted in the address-book phase, so no relay exists anywhere in the binary.

## Consequences

- Two peers both behind per-destination (symmetric) NATs cannot cooperate (`ReasonBothSymmetric`, `src/internal/direct/direct.go`).
- Retries to an unreachable peer back off, capped at five minutes, and reset when the peer publishes new addresses (`src/internal/internet/internet.go`: cap in `noPath`, reset rule on `backoffEntry`, check in `shouldTry`).
- Where no direct path exists (split college subnets, UDP-only routers), the answer is a different network or Tailscale, never a relay; `pipedpeer doctor` names the case.
