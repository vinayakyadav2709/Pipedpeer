# Rehearsal 2026-11-03T18:57:12+05:30

Commit 332ad015; lab pipedpeer-lab-headline (3 x 1g); PL_NPROC=2; PL_SYSTEMS=all.

| experiment | result |
|---|---|
| e14 | rc=0 runs=1 failed=0 |
| e12 | rc=0 runs=3 failed=0 |
| e13 | rc=0 runs=3 failed=2 |
| e15 | rc=0 runs=12 failed=0 |

All runs in runs.jsonl, by experiment:

| experiment | runs | failed | warm-ups |
|---|---|---|---|
| e12 | 3 | 0 | 0 |
| e13 | 3 | 2 | 0 |
| e14 | 1 | 0 | 0 |
| e15 | 12 | 0 | 3 |

Failed runs (from runs.jsonl):

- e13 W1 ray n=2 on pc00: exit 1:     [2026-11-03 13:10:20,684 I 3854 3877] (raylet) object_store.cc:37: Object store current usage 8e-09 / 0.2 GB.
- e13 W1 dask n=2 on pc00: exit 143: [runner] stopped after 600 s
