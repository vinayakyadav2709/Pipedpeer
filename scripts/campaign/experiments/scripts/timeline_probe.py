"""Per-task records from Dask's scheduler and Ray's state API, for the
throughput timelines of E12 and E13. lib/campaign/timeline.py stages this
file on pc00 and runs it with the campaign env's python3.

  timeline_probe.py dask-start <scheduler address>
      put a scheduler plugin in that counts, per second and per worker, the
      tasks that finished (processing -> memory), the tasks taken back from
      a worker that left (processing -> released/waiting/no-worker: each is
      run again), and logs every worker that joins or leaves
  timeline_probe.py dask-dump <scheduler address>
      print what the plugin counted since dask-start
  timeline_probe.py ray-dump <ray address> <since, unix ms>
      print every PoolActor.run_batch that ran since then, from Ray's
      timeline: start and end (unix ms), the node's IP, the task id. Only
      batches that finished running appear; Ray's Pool cuts the map into
      batches of ceil(items / (4 x actors)) items.

Each prints one line "PLTL <json>". Times are the scheduler's (Dask, pc00)
or the raylet's (Ray, the worker PC's own clock), unix seconds or ms.
"""
import json
import sys
import time


def dask_start(addr):
    from distributed import Client
    from distributed.diagnostics.plugin import SchedulerPlugin

    class Timeline(SchedulerPlugin):
        # Defined here, so the scheduler gets it by value (cloudpickle) and
        # needs nothing installed.
        name = "pl-timeline"

        def __init__(self):
            self.done = {}      # "<unix second> <worker address>" -> tasks finished
            self.lost = 0       # tasks taken back from a worker: each runs again
            self.erred = 0
            self.workers = []   # [unix s, "add"|"remove", worker address]
            self.scheduler = None

        async def start(self, scheduler):
            self.scheduler = scheduler

        def transition(self, key, start, finish, *args, **kwargs):
            if start != "processing":
                return
            if finish == "memory":
                w = kwargs.get("worker")
                if not w and self.scheduler is not None:
                    ts = self.scheduler.tasks.get(key)
                    w = next((x.address for x in (ts.who_has or ())), "?") if ts else "?"
                k = "%d %s" % (int(time.time()), w)
                self.done[k] = self.done.get(k, 0) + 1
            elif finish in ("released", "waiting", "no-worker"):
                self.lost += 1
            elif finish == "erred":
                self.erred += 1

        def add_worker(self, scheduler=None, worker=None, **kwargs):
            self.workers.append([time.time(), "add", worker])

        def remove_worker(self, scheduler=None, worker=None, **kwargs):
            self.workers.append([time.time(), "remove", worker])

        def dump(self):
            return {"done": self.done, "lost": self.lost, "erred": self.erred,
                    "workers": self.workers, "now": time.time()}

    with Client(addr, timeout=30) as c:
        c.register_plugin(Timeline(), name="pl-timeline")
        print("PLTL " + json.dumps({"started": time.time()}))


def dask_dump(addr):
    from distributed import Client
    with Client(addr, timeout=30) as c:
        out = c.run_on_scheduler(lambda dask_scheduler: dask_scheduler.plugins["pl-timeline"].dump())
        print("PLTL " + json.dumps(out))


def ray_dump(addr, since_ms):
    # ray.timeline() reads the task events from the GCS; the state API
    # (ray.util.state) would need the dashboard, which the runner turns off.
    import ray
    ray.init(address=addr, logging_level="ERROR")
    out = []
    for e in ray.timeline():
        if e.get("cat") != "task::PoolActor.run_batch" or e.get("ph") != "X":
            continue
        start_ms = e["ts"] / 1000.0
        if start_ms < since_ms:
            continue
        out.append([round(start_ms, 1), round(start_ms + e.get("dur", 0) / 1000.0, 1), e.get("pid"),
                    (e.get("args") or {}).get("task_id")])
    print("PLTL " + json.dumps({"tasks": out, "now": time.time()}))


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "dask-start":
        dask_start(sys.argv[2])
    elif cmd == "dask-dump":
        dask_dump(sys.argv[2])
    elif cmd == "ray-dump":
        ray_dump(sys.argv[2], int(sys.argv[3]))
    else:
        sys.exit("usage: timeline_probe.py dask-start|dask-dump ADDR | ray-dump ADDR SINCE_MS")
