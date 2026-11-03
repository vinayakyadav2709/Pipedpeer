"""A fake lab behind the Remote interface: it reads the shell commands the
runner sends and keeps the state a real cluster would (which daemons run,
which Ray and Dask processes are up), so tests assert on behaviour, not on
command strings."""
import json
import re
import time

from campaign.inventory import parse
from campaign.remote import Timed

INVENTORY = """
[head]
pc00 ansible_host=10.0.0.10 machine_id=m0
[workers]
pc01 ansible_host=10.0.0.11 machine_id=m1
pc02 ansible_host=10.0.0.12 machine_id=m2
pc03 ansible_host=10.0.0.13 machine_id=m3
pc04 ansible_host=10.0.0.14 machine_id=m4
[all:vars]
ansible_user=lab
"""


class FakeCluster:
    def __init__(self, inventory=INVENTORY, result="checksum=1.5", fail=None):
        self.inv = parse(inventory)
        self.ids = [h.id for h in self.inv.all]
        self.daemons = set(self.ids)
        self.daemon_env = {}
        self.ray = {}       # host -> cpus ("head" for the head)
        self.dask = {}      # host -> processes ("scheduler")
        self.calls = []
        self.result = result
        self.fail = fail or (lambda host, cmd: None)
        self.clock = 1_000_000_000

    def hosts(self, pattern):
        return [h for h in pattern.split(":") if h in self.ids]

    def _one(self, host, cmd):
        self.calls.append((host, cmd))
        forced = self.fail(host, cmd)
        if forced is not None:
            return forced
        if "pipedpeer stop" in cmd and "pipedpeer start" not in cmd:
            self.daemons.discard(host)
            return 0, "stopped daemon\n"
        if "pipedpeer start" in cmd:
            self.daemons.add(host)
            m = re.findall(r"env ((?:\w+=\S+ )*)", cmd)
            self.daemon_env[host] = m[0].strip() if m else ""
            self.daemon_caps = getattr(self, "daemon_caps", {})
            c = re.search(r"taskset -c (\S+) pipedpeer start", cmd)
            self.daemon_caps[host] = c.group(1) if c else None
            return 0, "daemon running\n"
        if "/health" in cmd and "/v1/nodes" in cmd:
            if host not in self.daemons:
                return 7, ""
            nodes = [{"node_id": h, "state": "healthy"} for h in sorted(self.daemons) if h != host]
            return 0, json.dumps({"node_id": host}) + "\n" + json.dumps(nodes) + "\n"
        if " stop --force" in cmd:
            self.ray.pop(host, None)
            return 0, ""
        if " start --head" in cmd:
            self.ray[host] = "head:" + re.search(r"--num-cpus=(\S+)", cmd).group(1)
            return 0, "ray head started\n"
        if " start --address=" in cmd:
            self.ray[host] = re.search(r"--num-cpus=(\S+)", cmd).group(1)
            return 0, ""
        if "ray.nodes()" in cmd:
            n = sum(1 for v in self.ray.values() if not str(v).startswith("head"))
            return 0, "PLN %d\n" % n
        if "PL_CAMPAIGN_DASK=1' $d/environ" in cmd:
            self.dask.pop(host, None)
            return 0, ""
        if "scheduler_info" in cmd:
            ws = {h: v for h, v in self.dask.items() if v != "scheduler"}
            return 0, "PLN %d %d\n" % (len(ws), sum(ws.values()))
        if cmd.strip() == "nproc":
            return 0, "4\n"
        if "base64 -d" in cmd or "rm -rf" in cmd:
            return 0, ""
        if "cat " in cmd and "receipt.json" in cmd:
            return 0, json.dumps({"remote_items": 3, "local_items": 5})
        if "jobs last --export json" in cmd:
            return 0, json.dumps({"steps": []})
        if "pipedpeer run" in cmd or "python3" in cmd:
            items = re.search(r"BENCH_ITEMS=(\d+)", cmd)
            return 0, "POOLMAP-OK items=%s seconds=0.5 %s\n" % (items.group(1) if items else "?", self.result)
        if "PLNET" in cmd or "/proc/net/dev" in cmd:
            return 0, "PLNET 1000 2000\n"
        if "--version" in cmd:
            return 0, "pipedpeer version test\n"
        return 0, ""


class FakeRemote:
    def __init__(self, cluster):
        self.c = cluster

    def sh(self, pattern, cmd, timeout=600):
        hosts = self.c.hosts(pattern)
        if not hosts:
            return 2, "no host matched"
        outs, rc = [], 0
        for h in hosts:
            r, o = self.c._one(h, cmd)
            rc = rc or r
            outs.append(o if len(hosts) == 1 else "".join("%s: %s\n" % (h, ln) for ln in o.splitlines()))
        return rc, "".join(outs)

    def timed(self, host, cmd, timeout=1800):
        rc, out = self.sh(host, cmd)
        self.c.clock += 2_000_000_000
        return Timed(rc, out, self.c.clock - 2_000_000_000, self.c.clock)

    def timed_all(self, hosts, cmd, timeout=3600):
        return {h.id: self.timed(h.id, cmd) for h in hosts}

    def bg(self, host, log, cmd):
        self.c.calls.append((host, "bg: " + cmd))
        if " scheduler " in cmd:
            self.c.dask[host] = "scheduler"
        elif " worker " in cmd:
            n = re.search(r"--nworkers (\S+)", cmd).group(1)
            self.c.dask[host] = int(n)
        return 0, "123\n"

    def put(self, pattern, src, dest):
        return 0, ""


def no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)
