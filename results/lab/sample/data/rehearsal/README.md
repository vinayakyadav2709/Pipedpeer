# Headline rehearsal lines

The E12 join, E13 churn, E14 curl-to-cluster, E15 network-delay and W4 (E1) lines from the
headline rehearsal of 2026-11-03 (three 1 GB containers, `PL_NPROC=2`; see REHEARSAL.md), so
the story page (`scripts/campaign/story.py --sample`) has data for every headline section.

- `runs.jsonl`: the 33 lines, unchanged except that each `receipt` keeps only its summary
  keys (`local_items`, `remote_items`, `by_where`, `remote_failures`, `mapped_items`,
  `dispatched_items`, `wasted_peer_s`) and `record_path` is dropped. The full receipts were
  about 9 MB.
- `logs/<run_id>/chunks.json`: timeline.py's per-chunk records for the E12 and E13 runs.
- `timeline.jsonl`: E14's events. Only the job phase was rehearsed, so the setup events
  (head_start, labpc, ssh_ok, install_end, join_end) are absent.

Emulated, container lab, sample sizes; not a result.
