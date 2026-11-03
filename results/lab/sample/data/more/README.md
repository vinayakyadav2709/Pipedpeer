# E16-E20 rehearsal lines

The 64 E16 (energy), E17 (idle footprint and cluster start), E18 (noisy neighbour),
E19 (cold vs warm ship) and E20 (unmodified pandas and scikit-learn, W5 and W6) lines from
the more-bench rehearsal of 2026-11-03 (three 2 GB containers, `PL_NPROC=2`, commit
b0dd3d41), so `scripts/campaign/paper_figures.py --sample` can draw every figure.

- `runs.jsonl`: the lines unchanged except that each `receipt` keeps only its summary keys
  (`local_items`, `remote_items`, `by_where`, `remote_failures`, `mapped_items`,
  `dispatched_items`, `wasted_peer_s`) and `record_path` is dropped. The receipts held the
  per-step lists and were about 1 MB.
- `logs/<run_id>/chunks.json` and `record.json`: timeline.py's per-chunk records and pipedpeer's
  job record, where the run had them (E18's swimlanes and throughput read them).
- E16 has no joules: the containers cannot read RAPL (`energy_unavailable` gives the reason
  per PC). The E16 figure in the draft preview is a synthetic example, marked as such, and
  the final export skips it until the lab day's `results/lab/energy/` exists.
- E19's first attempt (8 lines) failed its result check: the phases' scripts changed the
  result. `analyse.usable()` drops them; the later attempts are complete.

Emulated, container lab, sample sizes; not a result.
