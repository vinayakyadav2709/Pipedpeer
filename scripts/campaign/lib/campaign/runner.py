"""run.sh's body: parse arguments, build the context, run one experiment.

  run.sh <experiment> [--rehearsal] [--inventory FILE] [--n 1,2] [--repeats K]
                      [--seed S] [--size W1:BENCH_ITEMS=48] [--workloads W1,W2]
"""
import argparse
import base64
import hashlib
import importlib.util
import json
import os
import random
import shlex
import subprocess
import sys
import time

from . import inventory as inventory_mod
from . import record, systems, workloads as workloads_mod
from .remote import Remote

HERE = os.path.dirname(os.path.abspath(__file__))
CAMPAIGN = os.path.normpath(os.path.join(HERE, "..", ".."))
REPO = os.path.normpath(os.path.join(CAMPAIGN, "..", ".."))
REMOTE_HOME = "~/.local/share/pipedpeer-lab"


class Context:
    def __init__(self, args, remote=None, inv=None, out_dir=None):
        self.args = args
        self.experiment = args.experiment
        self.rehearsal = args.rehearsal
        self.out_dir = out_dir or os.environ.get("PL_RESULTS") or os.path.join(REPO, "results", "lab")
        os.makedirs(self.out_dir, exist_ok=True)
        inv_path = args.inventory or os.path.join(
            self.out_dir, "inventory-rehearsal.ini" if args.rehearsal else "inventory.ini")
        self.inventory_path = inv_path
        self.inv = inv or inventory_mod.load(inv_path)
        self.remote = remote or Remote(inv_path, log=self.log)
        self.env_dir = os.environ.get("PL_REMOTE_ENV", "$HOME/.local/share/pipedpeer-lab/env")
        self.run_timeout = float(os.environ.get("PL_RUN_TIMEOUT", "3600"))
        self.wait_timeout = float(os.environ.get("PL_WAIT_TIMEOUT", "300"))
        self.ray_start_args = os.environ.get(
            "PL_RAY_START_ARGS", "--object-store-memory=200000000" if args.rehearsal else "")
        self.dask_worker_args = os.environ.get(
            "PL_DASK_WORKER_ARGS", "--memory-limit 0" if args.rehearsal else "")
        # PL_NPROC: the cores each PC offers Ray and Dask, and the {nproc_*}
        # placeholders (the rehearsal's containers see all of the host's).
        self.nproc_override = int(os.environ["PL_NPROC"]) if os.environ.get("PL_NPROC") else None
        # PL_SYSTEMS: only these system kinds run (the rest are skipped and
        # logged), for a rehearsal before the Ray/Dask env exists.
        self.only_systems = [x for x in os.environ.get("PL_SYSTEMS", "").split(",") if x]
        self.seed = args.seed if args.seed is not None else int(time.time())
        self.rng = random.Random(self.seed)
        self.workloads, self.have_workloads = workloads_mod.load(REPO, os.environ.get("PL_WORKLOADS"))
        self.size_override = parse_sizes(args.size or [])
        self.daemons = {}
        self.backends = {}
        self._staged = set()
        self._nproc = {}
        self.recorder = None
        self.run_id = None    # the run in flight, for the per-run log names on the PCs
        self._marker = None   # the Ansible call writing current-run, still running
        self.logfile = os.path.join(self.out_dir, "logs", "%s.log" % self.experiment)
        os.makedirs(os.path.dirname(self.logfile), exist_ok=True)

    # --- plumbing -----------------------------------------------------------

    def log(self, msg):
        line = "[%s %s] %s" % (time.strftime("%H:%M:%S"), self.experiment, msg)
        print(line, file=sys.stderr, flush=True)
        with open(self.logfile, "a") as f:
            f.write(line + "\n")

    def pipedpeer_version(self):
        rc, out = self.remote.sh(self.inv.head.id, "pipedpeer --version 2>/dev/null || pipedpeer version", timeout=30)
        return out.strip().splitlines()[-1] if rc == 0 and out.strip() else "unknown"

    def start(self):
        self.recorder = record.Recorder(self.out_dir, self.experiment, REPO, self.pipedpeer_version(),
                                        rehearsal=self.rehearsal)
        self.log("inventory %s: head %s, %d workers; seed %d; results %s" % (
            self.inventory_path, self.inv.head.id, len(self.inv.workers), self.seed, self.recorder.path))
        if not self.have_workloads:
            self.log("warning: scripts/campaign/workloads.json missing; using the built-in W1 only")

    def backend(self, name):
        if name not in self.backends:
            if name == "plain":
                b = systems.Plain(self)
            elif name == "pipedpeer":
                b = systems.PipedPeer(self)
            elif name == "pipedpeer-headcount":
                b = systems.PipedPeer(self, name=name, daemon_env={"PIPEDPEER_POOL_SPLIT": "headcount"})
            elif name == "ray":
                b = systems.Ray(self)
            elif name == "dask":
                b = systems.Dask(self)
            else:
                raise KeyError("no system %r" % name)
            self.backends[name] = b
        return self.backends[name]

    def add_backend(self, name, b):
        self.backends[name] = b
        return b

    def only(self, active):
        """Stop the Ray or Dask cluster that is not about to be measured."""
        for b in list(self.backends.values()):
            if b is not active and b.kind in ("ray", "dask") and b.kind != active.kind:
                b.deactivate()

    def enabled(self, system):
        ok = not self.only_systems or system.split("-")[0] in self.only_systems
        if not ok:
            self.log("%s skipped: PL_SYSTEMS=%s" % (system, ",".join(self.only_systems)))
        return ok

    def nproc(self, host):
        if self.nproc_override:
            return self.nproc_override
        if host.id not in self._nproc:
            rc, out = self.remote.sh(host.id, "nproc", timeout=30)
            self._nproc[host.id] = int(out.strip().splitlines()[-1]) if rc == 0 else 1
        return self._nproc[host.id]

    def stage(self, host, wl, system):
        """Put the script a system runs (and wl.files) in its own directory on
        host; returns that directory. A pipedpeer workspace holds nothing else."""
        script = wl.script_for(system)
        d = "%s/work/%s-%s" % (REMOTE_HOME, wl.id, system)
        key = (host.id, d)
        if key in self._staged:
            return d
        files = [script] + list(wl.files)
        parts = ["rm -rf %s && mkdir -p %s && : > %s/.pipedpeerignore" % (d, d, d)]
        for rel in files:
            with open(os.path.join(REPO, rel), "rb") as f:
                data = f.read()
            if len(data) > 60_000:
                raise systems.StepError("%s is too big to stage through the shell (%d bytes)" % (rel, len(data)))
            parts.append("printf %%s %s | base64 -d > %s/%s" % (
                base64.b64encode(data).decode(), d, shlex.quote(os.path.basename(rel))))
        rc, out = self.remote.sh(host.id, " && ".join(parts), timeout=120)
        if rc != 0:
            raise systems.StepError("staging %s failed: %s" % (script, out.strip()[-300:]), host=host.id)
        self._staged.add(key)
        return d

    def release_all(self):
        for b in self.backends.values():
            try:
                if hasattr(b, "release"):
                    b.release()
            except systems.StepError as e:
                self.log("warning: release of %s failed: %s" % (b.name, e))

    def mark(self, value):
        """Write value (a run id, or "idle") to $LAB_HOME/current-run on every
        PC, so pp-poll.py stamps its lines with the run (otel/README.md,
        "Runner integration"). One Ansible fan-out in the background: the
        previous one is waited for first, so the writes land in order."""
        self.mark_wait()
        if not isinstance(self.remote, Remote):
            return  # a test double: nothing to reach
        cmd = "mkdir -p %s && printf '%%s\\n' %s > %s/current-run" % (REMOTE_HOME, shlex.quote(value), REMOTE_HOME)
        env = dict(os.environ, PL_INVENTORY=self.inventory_path)
        self._marker = subprocess.Popen(["bash", os.path.join(CAMPAIGN, "lib", "ansible.sh"), "sh", "all", cmd],
                                        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def mark_wait(self, timeout=60):
        if self._marker is not None:
            try:
                self._marker.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self._marker.kill()
                self.log("warning: writing current-run on the PCs took over %ds" % timeout)
            self._marker = None

    def sync(self):
        """Upload results/lab with sync-r2.sh, only for a real campaign run.

        A rehearsal (--rehearsal, or results anywhere but results/lab) never
        uploads: with ingest.env on the machine, sync-r2.sh would send it to
        the campaign's real bucket (the lab/sample rehearsal did, 239 files).
        PL_SYNC=1 forces the upload, for a harness that points the
        credentials (PIPEDPEER_LAB_INGEST_ENV) at a local stand-in; PL_SYNC=0
        leaves it to the caller (the lab day syncs after attempts.jsonl)."""
        script = os.path.join(CAMPAIGN, "sync-r2.sh")
        want = os.environ.get("PL_SYNC", "")
        campaign = os.path.realpath(self.out_dir) == os.path.realpath(os.path.join(REPO, "results", "lab"))
        if want == "0":
            self.log("PL_SYNC=0: the caller syncs to R2")
        elif want != "1" and (self.rehearsal or not campaign):
            self.log("not uploading: a rehearsal (or results outside results/lab) never syncs to R2; PL_SYNC=1 forces it")
        elif os.path.exists(script):
            rc = subprocess.call(["bash", script], env=dict(os.environ, LAB_RESULTS=self.out_dir))
            self.log("sync-r2.sh exited %d" % rc)
        else:
            self.log("scripts/campaign/sync-r2.sh not present; nothing uploaded")

    # --- one run --------------------------------------------------------------

    def system_env(self, wl, system, n, hosts, caps=None):
        """workloads.json system_env with the core counts filled in: pc00's,
        and the sum over the PCs that do the work (pc00 too for pipedpeer and
        plain; only the workers for ray and dask, whose pc00 does none)."""
        kind = system.split("-")[0]
        workers = hosts if kind in ("pipedpeer", "plain") else [h for h in hosts if h is not self.inv.head]
        cores = [caps[i] if caps and i < len(caps) else self.nproc(h) for i, h in enumerate(workers)]
        sub = caps[0] if caps and kind == "pipedpeer" else self.nproc(self.inv.head)
        env = wl.system_env(system, sub, sum(cores) or 1)
        if self.nproc_override:
            # PL_NPROC makes every PC an N-core PC; Python sees all the
            # host's CPUs otherwise (os.cpu_count: multiprocessing, loky, the
            # shim), also inside pipedpeer's sandbox, where this goes as -e.
            env["PYTHON_CPU_COUNT"] = str(self.nproc_override)
        return env

    def size_for(self, wl, mode, n):
        size = wl.size(mode, n, rehearsal=self.rehearsal, override=self.size_override.get(wl.id))
        if not size or any(v in ("0", "") for v in size.values()):
            # workloads.json ships "0" until session 1 calibrates the sizes.
            raise systems.StepError("%s has no calibrated %s size in workloads.json (%s); "
                                    "calibrate it or pass --size" % (wl.id, mode, size or "none"))
        return size

    def check(self, wl, size, result, system, reference):
        """result_ok: the result equals the plain run's at the same size."""
        if result is None:
            return False, None
        if wl.result_compare == "present":
            return True, "present"
        ref = self.recorder.cache.get(wl.id, size)
        if ref is None:
            if system == "plain":
                self.recorder.cache.put(wl.id, size, result, "plain")
                return True, "plain"
            if reference == "plain":
                ref = self.reference_run(wl, size)
            if ref is None:
                if reference == "first":
                    self.recorder.cache.put(wl.id, size, result, system)
                    return True, system
                return None, None
        return wl.compare(result, ref["result"]), ref["source"]

    def reference_run(self, wl, size):
        """A plain run at this size, unrecorded, to have something to compare."""
        tag = hashlib.sha1(json.dumps(size, sort_keys=True).encode()).hexdigest()[:8]
        logdir = self.recorder.log_dir("ref-%s-%s" % (wl.id, tag))
        self.log("no plain result for %s at %s yet: running one as the reference" % (wl.id, size))
        try:
            env = dict(self.system_env(wl, "plain", 1, [self.inv.head]), **size)
            res = self.backend("plain").run(wl, env, logdir, 1)
        except systems.StepError as e:
            self.log("reference run failed: %s" % e)
            return None
        with open(os.path.join(logdir, "output.log"), "w") as f:
            f.write(res["out"])
        result = workloads_mod.extract(wl.result_regex, res["out"])
        if res["rc"] != 0 or result is None:
            self.log("reference run gave no result (rc %d); result_ok stays null" % res["rc"])
            return None
        self.recorder.cache.put(wl.id, size, result, "plain")
        return self.recorder.cache.get(wl.id, size)

    def one_run(self, wl, system, n, repeat, warmup=False, mode="strong", caps=None,
                reference="plain", extra=None, run=None):
        """Activate system at N, run wl once, append one line. Never raises."""
        try:
            return self._one_run(wl, system, n, repeat, warmup, mode, caps, reference, extra, run)
        finally:
            self.run_id = None
            self.mark("idle")

    def _one_run(self, wl, system, n, repeat, warmup, mode, caps, reference, extra, run):
        rec = self.recorder
        run_id = rec.new_run_id()
        logdir = rec.log_dir(run_id)
        line = {"run_id": run_id, "workload": wl.id, "system": system, "n": n, "repeat": repeat,
                "warmup": warmup, "mode": mode, "log_path": os.path.relpath(logdir, self.out_dir),
                "seed": self.seed}
        if caps:
            line["caps"] = caps
        line.update(extra or {})
        b = self.backend(system)
        self.log("%s %s n=%d repeat=%d%s" % (wl.id, system, n, repeat, " (warm-up)" if warmup else ""))
        self.run_id = run_id
        self.mark(run_id)
        try:
            size = line["size"] = self.size_for(wl, mode, n)
            hosts = b.activate(n, caps)
            line["hosts"] = [h.id for h in hosts]
            env = dict(self.system_env(wl, system, n, hosts, caps), **size)
            self.mark_wait()
            res = run(b, wl, env, logdir, n) if run else b.run(wl, env, logdir, n)
        except systems.StepError as e:
            line.update({"error": str(e), "failing_pc": e.host or self.inv.head.id, "result_ok": False})
            self.log("FAILED: %s" % e)
            return rec.write(line)
        except LookupError as e:
            line.update({"error": str(e), "failing_pc": None, "result_ok": False})
            self.log("FAILED: %s" % e)
            return rec.write(line)
        out = res.pop("out", "")
        with open(os.path.join(logdir, "output.log"), "w") as f:
            f.write(out)
        rc = res.pop("rc")
        line.update(res)
        line["rc"] = rc
        result = workloads_mod.extract(wl.result_regex, out)
        line["result"] = result
        st = workloads_mod.extract(wl.timing_regex, out)
        try:
            line["script_s"] = float(st.split(" | ")[-1]) if st else None
        except ValueError:
            line["script_s"] = None
        if rc != 0:
            last = [x for x in out.strip().splitlines() if x.strip()][-1:] or [""]
            line.update({"error": "exit %d: %s" % (rc, last[0][:300]), "failing_pc": self.inv.head.id,
                         "result_ok": False})
            self.log("FAILED: exit %d (%s)" % (rc, line["log_path"]))
        else:
            ok, src = self.check(wl, size, result, system, reference)
            line["result_ok"], line["result_ref"] = ok, src
            if ok is False:
                line["error"] = "result %r differs from the reference" % result
                self.log("FAILED: result %r differs from the reference" % result)
        return rec.write(line)

    # --- the scaling loop -------------------------------------------------------

    def scaling(self, wl_ids, systems_, ns, repeats, mode="strong", caps=None, reference="plain"):
        """For each workload and N: one warm-up per system (plain's first, so
        its result is the reference), then `repeats` rounds, each in a fresh
        random order of the systems."""
        for wid in wl_ids:
            if wid not in self.workloads:
                self.log("workload %s not in workloads.json; skipped" % wid)
                continue
            wl = self.workloads[wid]
            systems_ = [s for s in systems_ if self.enabled(s)]
            for n in ns:
                here = [s for s in systems_ if s != "plain" or n == 1]
                if not here:
                    continue
                rest = [s for s in here if s != "plain"]
                warm = (["plain"] if "plain" in here else []) + self.rng.sample(rest, len(rest))
                for s in warm:
                    self.one_run(wl, s, n, 0, warmup=True, mode=mode, caps=caps, reference=reference)
                for r in range(1, repeats + 1):
                    for s in self.rng.sample(here, len(here)):
                        self.one_run(wl, s, n, r, mode=mode, caps=caps, reference=reference)

    def ns(self, default):
        if self.args.n:
            return self.args.n
        if self.rehearsal:
            return [1, 2]
        return default

    def repeats(self, default):
        if self.args.repeats is not None:
            return self.args.repeats
        return 1 if self.rehearsal else default

    def wl_ids(self, default):
        return self.args.workloads or default


def parse_sizes(items):
    out = {}
    for it in items:
        wid, _, kv = it.partition(":")
        k, _, v = kv.partition("=")
        if not (wid and k and v):
            raise SystemExit("--size wants W1:ENV=VALUE, got %r" % it)
        out.setdefault(wid, {})[k] = v
    return out


def parse_args(argv):
    p = argparse.ArgumentParser(prog="run.sh", description="Run one lab campaign experiment.")
    p.add_argument("experiment")
    p.add_argument("--rehearsal", action="store_true",
                   help="container lab: inventory-rehearsal.ini, N=1,2, one repeat, small sizes")
    p.add_argument("--inventory")
    p.add_argument("--n", type=lambda s: [int(x) for x in s.split(",") if x])
    p.add_argument("--repeats", type=int)
    p.add_argument("--seed", type=int)
    p.add_argument("--size", action="append", help="W1:ENV=VALUE, overrides workloads.json")
    p.add_argument("--workloads", type=lambda s: [x for x in s.split(",") if x])
    return p.parse_args(argv)


def load_experiment(name):
    path = os.path.join(CAMPAIGN, "experiments", "%s.py" % name)
    if not os.path.exists(path):
        have = sorted(f[:-3] for f in os.listdir(os.path.join(CAMPAIGN, "experiments"))
                      if f.endswith(".py") and not f.startswith("_"))
        raise SystemExit("no experiment %r; have: %s" % (name, " ".join(have)))
    sys.path.insert(0, os.path.dirname(path))
    spec = importlib.util.spec_from_file_location("experiment_" + name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    mod = load_experiment(args.experiment)
    if getattr(mod, "LOCAL_ONLY", False):
        # The guided timers (E3, E10) touch no PC.
        return mod.main(args, os.environ.get("PL_RESULTS") or os.path.join(REPO, "results", "lab"))
    ctx = Context(args)
    ctx.start()
    status = 0
    try:
        mod.main(ctx)
    except KeyboardInterrupt:
        ctx.log("interrupted")
        status = 130
    finally:
        ctx.mark_wait()
        ctx.release_all()
        ctx.log("%d run(s) recorded, %d failed -> %s" % (ctx.recorder.count, ctx.recorder.failed, ctx.recorder.path))
        ctx.sync()
    return status
