"""The shim's receipt, bounded for runs.jsonl.

A receipt carries one timed step per item (pickle and unpickle) with no cap:
at lab sizes one weak-scaling run's receipt is about 167 MB, which the
driver collector truncates (file_log max_log_size) and which would make
runs.jsonl about 1 GB over the day (results/lab/sample/REPORT.md, gap 1).
So the runner keeps the whole receipt as logs/<run_id>/receipt.json.gz and
puts summary(receipt) inline: every small field as it is, and the steps
aggregated per (step, node).
"""
import json
from collections import defaultdict

INLINE_FIELD_BYTES = 4096   # a field larger than this stays in the file only


def summary(receipt):
    out, big = {}, {}
    for k, v in receipt.items():
        if k == "steps":
            continue
        size = len(json.dumps(v))
        if size <= INLINE_FIELD_BYTES:
            out[k] = v
        else:
            big[k] = size
    steps = receipt.get("steps") or []
    agg = defaultdict(lambda: {"count": 0, "seconds": 0.0, "bytes": 0})
    for s in steps:
        a = agg[(s.get("step", "?"), s.get("node") or s.get("machine") or "")]
        a["count"] += 1
        a["seconds"] += (s.get("dur_ns") or 0) / 1e9
        a["bytes"] += s.get("bytes") or 0
    out["steps_total"] = len(steps)
    out["steps_by"] = [dict(step=k[0], node=k[1], count=v["count"], seconds=round(v["seconds"], 6), bytes=v["bytes"])
                       for k, v in sorted(agg.items())]
    if big:
        out["fields_in_file"] = big
    return out
