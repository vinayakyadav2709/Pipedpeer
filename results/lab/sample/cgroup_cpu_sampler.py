"""Per-container CPU for the sample (container lab only).

In a container, the node collector's host_metrics cpu scraper reads the
host's /proc/stat, so every "PC" reports the laptop's CPU. This samples the
container's own cgroup (cpu.stat) every second and writes OTLP JSON lines with
the same metric name and shape the node collector sends (system.cpu.time,
state=user|system, resource lab.pc_id), so sample_figures.py reads both the
same way. Not used on real lab PCs, where host_metrics is per PC.

  python3 cgroup_cpu_sampler.py <pc_id> <out.jsonl>
"""
import json
import sys
import time

pc, out = sys.argv[1], sys.argv[2]
with open(out, "a") as f:
    while True:
        st = dict(line.split() for line in open("/sys/fs/cgroup/cpu.stat"))
        t = str(time.time_ns())
        dps = [{"attributes": [{"key": "state", "value": {"stringValue": s}}], "timeUnixNano": t,
                "asDouble": int(st[s + "_usec"]) / 1e6} for s in ("user", "system")]
        rec = {"resourceMetrics": [{"resource": {"attributes": [
            {"key": "lab.pc_id", "value": {"stringValue": pc}},
            {"key": "lab.cpu_source", "value": {"stringValue": "cgroup"}}]},
            "scopeMetrics": [{"scope": {"name": "sample/cgroup_cpu_sampler"}, "metrics": [
                {"name": "system.cpu.time", "unit": "s", "sum": {"dataPoints": dps, "aggregationTemporality": 2}}]}]}]}
        f.write(json.dumps(rec) + "\n")
        f.flush()
        time.sleep(1.0 - (time.time() % 1.0))
