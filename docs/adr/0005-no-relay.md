# No relay in the data path

A relay was built (`a6e0723`) and removed (`95e0712`): it carried every byte of every closure over somebody else's uplink and egress bill, even for two laptops on one Wi-Fi. A peer with no direct path is recorded with its reason and not offered work; the scheduler routes around it as it does an unhealthy node, which works because a job needs *a* machine rather than *that* machine.

## Consequences

- Two peers both behind per-destination (symmetric) NATs cannot cooperate (`ReasonBothSymmetric`, `src/internal/direct/direct.go`).
- `src/internal/relay` survives only for the `relay-test` / `relay-connect` diagnostics in `src/netcheck.go`; the daemon does not import it.
- Retries to an unreachable peer back off, capped at five minutes, and reset when the peer publishes new addresses (`src/internal/internet/internet.go`: cap in `noPath`, reset rule on `backoffEntry`, check in `shouldTry`).
