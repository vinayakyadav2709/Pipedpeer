# Pipedpeer Future Plans

This file tracks post-v1 additions that are valuable but intentionally deferred.

## 1. Trust and Verification Framework

### Goal

Support execution on partially trusted or untrusted nodes while maintaining correctness guarantees.

### Verification tiers

Use policy-driven verification levels:

- Consensus mode: run the same task on multiple nodes and compare results.
- Spot-check mode: run single execution with periodic verification re-runs.
- Optimistic mode: accept result immediately with occasional background checks.

### Suggested trust model

- Maintain per-node trust score.
- Increase trust on validated correct results.
- Decrease trust on timeout, mismatch, or repeated bad behavior.
- Apply stricter verification for low-trust nodes.

### Result comparison strategy

- Prefer deterministic outputs for direct hash comparison.
- For floating-point outputs, allow tolerance-based comparison where needed.
- Keep verification metadata alongside task records.

### Failure and abuse handling

- Requeue tasks on verification disagreement.
- Quarantine repeatedly failing nodes.
- Add rate limits for suspicious peers.

## 2. Data Swarming and P2P Artifact Distribution

### Goal

Reduce bottlenecks and transfer cost for large datasets/models by allowing nodes to fetch from peers, not only from the origin.

### Core approach

- Keep artifacts content-addressed (hash/CID style).
- Split large files into chunks.
- Let nodes advertise chunk availability.
- Fetch chunks from any available peer.

### Expected benefits

- Lower load on original submitter/uploader.
- Faster warm-up when multiple workers need the same input.
- Better scalability for large model and dataset workflows.

### Integrity and correctness

- Verify every chunk by hash before use.
- Discard and re-fetch corrupt or mismatched chunks.
- Track source reliability for chunk providers.

### Integration with scheduler

- Include data availability in scheduling score.
- Prefer nodes with local chunks/artifacts already present.
- Trigger replication for hot artifacts.

### Suggested rollout

- Phase A: single-source artifact fetch with local cache.
- Phase B: peer-assisted chunk fetch for large artifacts only.
- Phase C: full swarming with adaptive replication policies.
