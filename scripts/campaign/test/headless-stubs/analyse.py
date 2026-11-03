#!/usr/bin/env python3
"""Rehearsal stand-in for scripts/campaign/analyse.py (lab/runner): one CSV per
experiment from results/lab/runs.jsonl, and a figures directory."""
import collections, csv, json, os

runs = [json.loads(l) for l in open("results/lab/runs.jsonl")]
by = collections.defaultdict(list)
for r in runs:
    by[r["experiment"]].append(r)
os.makedirs("results/lab/figures", exist_ok=True)
for exp, rows in by.items():
    with open(f"results/lab/{exp}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["system", "n", "repeat", "wall_s"])
        w.writerows([r["system"], r["n"], r["repeat"], r["wall_s"]] for r in rows)
open("results/lab/figures/README.txt", "w").write("rehearsal: no figures\n")
print(f"analysed {len(runs)} runs in {len(by)} experiments")
