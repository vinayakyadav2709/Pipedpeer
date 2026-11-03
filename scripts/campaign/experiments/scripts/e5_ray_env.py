"""E5's Ray baseline: ship the pins with runtime_env={"pip": [...]} and
import them on every node. E5_PIPS is the pip list, comma-separated."""
import os
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@ray.remote(num_cpus=1)
def probe():
    import joblib, numpy, pandas, sklearn, torch  # noqa: F401,E401
    extra = os.environ.get("E5_EXTRA", "")
    if extra:
        __import__(extra)
    return ray.get_runtime_context().get_node_id()


if __name__ == "__main__":
    pips = [p for p in os.environ["E5_PIPS"].split(",") if p.strip()]
    t0 = time.perf_counter()
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"),
             runtime_env={"pip": pips, "env_vars": {"E5_EXTRA": os.environ.get("E5_EXTRA", "")}})
    nodes = [n["NodeID"] for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU", 0) > 0]
    got = ray.get([probe.options(scheduling_strategy=NodeAffinitySchedulingStrategy(n, soft=False)).remote()
                   for n in nodes])
    print("E5-RAY-OK nodes=%d seconds=%.2f" % (len(set(got)), time.perf_counter() - t0))
