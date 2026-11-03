"""The four systems and how each is restricted to N worker PCs.

  pipedpeer  daemons run on pc00..pc(N-1) and are stopped (`pipedpeer stop`)
             everywhere else; pc00 submits and counts as one of the N. Ready
             when pc00's API lists exactly N-1 other healthy nodes.
  ray        head on pc00 with --num-cpus=0 (submits, does no work) plus
             workers on pc01..pcN, each with --num-cpus = its core count.
  dask       scheduler on pc00 plus `dask worker` on pc01..pcN with
             --nworkers <cores> --nthreads 1: the workloads are pure-Python
             CPU loops that hold the GIL, so one single-threaded process per
             core is the configuration that can use every core (threads would
             serialise on the GIL).
  plain      the Nix env's stock python3 on pc00, N=1 only.

Core caps (E4) are CPU affinity, `taskset -c 0-<c-1>`, for every system: the
PipedPeer daemon starts itself in a `systemd-run --user --scope`, which would
leave any root scope's AllowedCPUs behind, while affinity is inherited by
every child (crun, Ray's raylet and workers, Dask's nanny processes). A
daemon installed as a systemd user service gets the same cap as a drop-in
with CPUAffinity=.

Ray and Dask clusters are started for a run and stopped before the other one
starts, so neither idles on the PCs while the other is measured. PipedPeer's
daemons stay up through an N block, as they would in use.
"""
import json
import shlex
import time

STATE = "$HOME/.local/share/pipedpeer-lab"
WORK = STATE + "/work"
RAY_PORT = 6379
DASK_PORT = 8786

# The daemon's API on this PC, with the token from the environment or the
# one config file (the lab containers have PIPEDPEER_TOKEN set).
API = ('tok="${PIPEDPEER_TOKEN:-$(sed -n \'s/.*"token" *: *"\\([^"]*\\)".*/\\1/p\' '
       '"${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/cluster.json" 2>/dev/null)}"; '
       'curl -sf --max-time 5 -H "X-Pipedpeer-Token: $tok" http://127.0.0.1:4242')


class StepError(Exception):
    """A step that failed on a named PC; the run is recorded as failed."""

    def __init__(self, msg, host=None, log=None):
        super().__init__(msg)
        self.host = host
        self.log = log


def cpus(c):
    return "0" if c <= 1 else "0-%d" % (c - 1)


def pattern(hosts):
    return ":".join(h.id for h in hosts)


class Backend:
    name = "?"
    kind = "?"

    def __init__(self, ctx, **opts):
        self.ctx = ctx
        self.opts = opts
        self.up_n = None
        self.up_caps = None

    @property
    def inv(self):
        return self.ctx.inv

    @property
    def remote(self):
        return self.ctx.remote

    def env_bin(self, b):
        return self.ctx.env_dir + "/bin/" + b

    def hosts_for(self, n):
        raise NotImplementedError

    def activate(self, n, caps=None):
        """Bring the system up on exactly N workers; returns the hosts used."""
        raise NotImplementedError

    def deactivate(self):
        pass

    def run(self, wl, size, logdir, n):
        raise NotImplementedError

    def sh_ok(self, pat, cmd, what, timeout=600):
        rc, out = self.remote.sh(pat, cmd, timeout=timeout)
        if rc != 0:
            host = pat if ":" not in pat else _failing_host(out, pat)
            raise StepError("%s failed (rc %d): %s" % (what, rc, _tail(out)), host=host)
        return out

    def stage(self, wl, system):
        return self.ctx.stage(self.inv.head, wl, system)

    def envs(self, size):
        return " ".join("%s=%s" % (k, shlex.quote(str(v))) for k, v in sorted(size.items()))


def _tail(out, n=3):
    lines = [x for x in out.strip().splitlines() if x.strip()]
    return " | ".join(lines[-n:])


def _failing_host(out, pat):
    for line in out.splitlines():
        if ":" in line:
            h = line.split(":", 1)[0]
            if h in pat.split(":") and ("rror" in line or "fail" in line.lower()):
                return h
    return pat.split(":")[0]


class Plain(Backend):
    name = kind = "plain"

    def hosts_for(self, n):
        return [self.inv.head]

    def activate(self, n, caps=None):
        if n != 1:
            raise StepError("plain runs at N=1 only")
        self.ctx.only(self)
        return self.hosts_for(1)

    def run(self, wl, size, logdir, n):
        ws = self.stage(wl, "plain")
        args = " ".join(shlex.quote(a) for a in wl.args)
        cmd = "cd %s && env %s %s %s %s" % (ws, self.envs(size), self.env_bin("python3"),
                                            shlex.quote(wl.script.rsplit("/", 1)[-1]), args)
        t = self.remote.timed(self.inv.head.id, cmd, timeout=self.ctx.run_timeout)
        return {"rc": t.rc, "out": t.out, "start_ns": t.start_ns, "end_ns": t.end_ns}


class PipedPeer(Backend):
    """opts: daemon_env {K: V} set on every daemon (PIPEDPEER_POOL_SPLIT),
    run_env {K: V} passed with -e, run_flags [..] for `pipedpeer run`."""
    name = kind = "pipedpeer"

    def __init__(self, ctx, name="pipedpeer", **opts):
        super().__init__(ctx, **opts)
        self.name = name

    @property
    def daemons(self):
        """host id -> (cap, env) its daemon was started with, None if stopped.
        On the context: every pipedpeer variant drives the same daemons."""
        return self.ctx.daemons

    def hosts_for(self, n):
        return self.inv.all[:n]

    def _start_cmd(self, cap, denv):
        envs = " ".join("%s=%s" % kv for kv in sorted(denv.items()))
        affinity = cpus(cap) if cap else ""
        return (
            'unit="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/pipedpeer.service"; '
            'drop="$unit.d/pplab.conf"; pipedpeer stop >/dev/null 2>&1 || true; '
            'if [ -f "$unit" ]; then '
            '  if [ -n "%(aff)s%(envs)s" ]; then mkdir -p "$unit.d"; '
            '    { echo "[Service]"; [ -n "%(aff)s" ] && echo "CPUAffinity=%(aff)s"; '
            '      for e in %(envs)s; do echo "Environment=$e"; done; } > "$drop"; '
            '  else rm -f "$drop"; fi; '
            '  systemctl --user daemon-reload; pipedpeer start; '
            'else env %(envs)s %(taskset)s pipedpeer start; fi'
        ) % {"aff": affinity, "envs": envs,
             "taskset": ("taskset -c " + affinity) if affinity else ""}

    def activate(self, n, caps=None):
        self.ctx.only(self)
        want = self.hosts_for(n)
        if len(want) < n:
            raise StepError("inventory has %d PCs, N=%d asked" % (len(self.inv.all), n))
        rest = [h for h in self.inv.all if h not in want]
        denv = self.opts.get("daemon_env") or {}
        stop = [h for h in rest if self.daemons.get(h.id) is not None or h.id not in self.daemons]
        if stop:
            self.sh_ok(pattern(stop), "pipedpeer stop >/dev/null 2>&1 || true; "
                       "pkill -f '[p]ipedpeer __daemon__' 2>/dev/null; true", "pipedpeer stop")
            for h in stop:
                self.daemons[h.id] = None
        for i, h in enumerate(want):
            cap = caps[i] if caps else None
            mode = (cap, tuple(sorted(denv.items())))
            if self.daemons.get(h.id) != mode:
                self.sh_ok(h.id, self._start_cmd(cap, denv), "pipedpeer start", timeout=120)
                self.daemons[h.id] = mode
        self.wait_nodes(n)
        self.up_n, self.up_caps = n, caps
        return want

    def node_view(self):
        """(own id, {node id: state}) as pc00's daemon sees the cluster."""
        rc, out = self.remote.sh(self.inv.head.id, "%s/health; echo; %s/v1/nodes" % (API, API), timeout=30)
        if rc != 0:
            return None, {}
        parts = [p for p in out.strip().split("\n") if p.strip()]
        try:
            own = json.loads(parts[0])["node_id"]
            nodes = json.loads(parts[1]) if len(parts) > 1 else []
        except (ValueError, KeyError, IndexError):
            return None, {}
        return own, {x.get("node_id"): x.get("state") for x in nodes or []}

    def healthy_others(self):
        own, view = self.node_view()
        return sum(1 for k, v in view.items() if k != own and v == "healthy"), own

    def wait_nodes(self, n, timeout=None):
        timeout = timeout or self.ctx.wait_timeout
        deadline = time.time() + timeout
        got = -1
        while time.time() < deadline:
            got, own = self.healthy_others()
            if own and got == n - 1:
                return
            time.sleep(3)
        raise StepError("pc00 sees %d healthy peers, want exactly %d after %ds" % (got, n - 1, timeout),
                        host=self.inv.head.id)

    def deactivate(self):
        pass  # daemons stay up through the N block; release() restores all

    def release(self):
        """Every daemon back up, uncapped, without benchmark env."""
        todo = [h for h in self.inv.all if self.daemons.get(h.id, (None, ())) != (None, ())]
        for h in todo:
            rc, out = self.remote.sh(h.id, self._start_cmd(None, {}), timeout=120)
            if rc != 0:
                self.ctx.log("warning: restarting the daemon on %s failed: %s" % (h.id, _tail(out)))
            self.daemons[h.id] = (None, ())

    def run(self, wl, size, logdir, n, extra_flags=None, timeout=None):
        ws = self.stage(wl, "pipedpeer")
        run_env = dict(self.opts.get("run_env") or {}, **size)
        flags = list(self.opts.get("run_flags") or []) + list(extra_flags or [])
        es = " ".join("-e %s" % shlex.quote("%s=%s" % kv) for kv in sorted(run_env.items()))
        args = " ".join(shlex.quote(a) for a in wl.args)
        script = shlex.quote(wl.script.rsplit("/", 1)[-1])
        cmd = "cd %s && rm -f receipt.json && pipedpeer run %s -e PIPEDPEER_RECEIPT=receipt.json %s %s %s" % (
            ws, " ".join(shlex.quote(f) for f in flags), es, script, args)
        t = self.remote.timed(self.inv.head.id, cmd, timeout=timeout or self.ctx.run_timeout)
        res = {"rc": t.rc, "out": t.out, "start_ns": t.start_ns, "end_ns": t.end_ns}
        res.update(self.collect(ws, logdir))
        return res

    def collect(self, ws, logdir):
        """The shim's receipt (PIPEDPEER_RECEIPT) inline, and the run's step
        record (`pipedpeer jobs last --export json`) as a file in logdir."""
        out = {}
        rc, text = self.remote.sh(self.inv.head.id, "cat %s/receipt.json" % ws, timeout=30)
        if rc == 0:
            try:
                out["receipt"] = json.loads(text)
            except ValueError:
                out["receipt_raw"] = text[:2000]
        rc, text = self.remote.sh(self.inv.head.id, "pipedpeer jobs last --export json", timeout=60)
        if rc == 0 and text.strip():
            path = logdir + "/record.json"
            with open(path, "w") as f:
                f.write(text)
            out["record_path"] = path
        return out


class Ray(Backend):
    name = kind = "ray"

    def hosts_for(self, n):
        return [self.inv.head] + self.inv.workers[:n]

    def stop_all(self):
        self.sh_ok(pattern(self.inv.all), "%s stop --force >/dev/null 2>&1; true" % self.env_bin("ray"),
                   "ray stop", timeout=180)
        self.up_n = None

    def activate(self, n, caps=None):
        self.ctx.only(self)
        if self.up_n == n and self.up_caps == caps:
            return self.hosts_for(n)
        if len(self.inv.workers) < n:
            raise StepError("inventory has %d workers, N=%d asked" % (len(self.inv.workers), n))
        self.stop_all()
        head = self.inv.head
        extra = self.ctx.ray_start_args
        self.sh_ok(head.id, "%s start --head --port=%d --num-cpus=0 --disable-usage-stats "
                   "--include-dashboard=false %s" % (self.env_bin("ray"), RAY_PORT, extra),
                   "ray start --head", timeout=180)
        for i, w in enumerate(self.inv.workers[:n]):
            c = caps[i] if caps else None
            ncpu = str(c or self.ctx.nproc(w))
            pre = ("taskset -c %s " % cpus(c)) if c else ""
            self.sh_ok(w.id, "%s%s start --address=%s:%d --num-cpus=%s --disable-usage-stats %s" % (
                pre, self.env_bin("ray"), head.ip, RAY_PORT, ncpu, extra), "ray start (worker)", timeout=180)
        probe = ("RAY_ADDRESS=%s:%d %s -c \"import ray; ray.init(address='auto', logging_level='ERROR'); "
                 "print('PLN', sum(1 for x in ray.nodes() if x['Alive'] and x['Resources'].get('CPU', 0) > 0))\""
                 % (head.ip, RAY_PORT, self.env_bin("python3")))
        deadline, got = time.time() + self.ctx.wait_timeout, "?"
        while time.time() < deadline:
            rc, out = self.remote.sh(head.id, probe, timeout=90)
            got = next((ln.split()[1] for ln in out.splitlines() if ln.startswith("PLN ")), "?")
            if got == str(n):
                self.up_n, self.up_caps = n, caps
                return self.hosts_for(n)
            time.sleep(3)
        raise StepError("ray head sees %s worker nodes with CPUs, want %d" % (got, n), host=head.id)

    def deactivate(self):
        if self.up_n is not None:
            self.stop_all()

    def release(self):
        self.stop_all()

    def run(self, wl, size, logdir, n):
        ws = self.stage(wl, "ray")
        script = shlex.quote(wl.script_for("ray").rsplit("/", 1)[-1])
        args = " ".join(shlex.quote(a) for a in wl.args)
        cmd = "cd %s && env RAY_ADDRESS=%s:%d %s %s %s %s" % (
            ws, self.inv.head.ip, RAY_PORT, self.envs(size), self.env_bin("python3"), script, args)
        t = self.remote.timed(self.inv.head.id, cmd, timeout=self.ctx.run_timeout)
        return {"rc": t.rc, "out": t.out, "start_ns": t.start_ns, "end_ns": t.end_ns}


DASK_PROCS = "dask[a-z-]* (scheduler|worker)"


class Dask(Backend):
    name = kind = "dask"

    def hosts_for(self, n):
        return [self.inv.head] + self.inv.workers[:n]

    def stop_all(self):
        cmd = ("pkill -f -- '%s' 2>/dev/null; for i in 1 2 3 4 5 6 7 8 9 10; do "
               "pgrep -f -- '%s' >/dev/null || exit 0; sleep 1; done; "
               "pkill -9 -f -- '%s' 2>/dev/null; true") % (DASK_PROCS, DASK_PROCS, DASK_PROCS)
        self.sh_ok(pattern(self.inv.all), cmd, "dask stop", timeout=120)
        self.up_n = None

    def activate(self, n, caps=None):
        self.ctx.only(self)
        if self.up_n == n and self.up_caps == caps:
            return self.hosts_for(n)
        if len(self.inv.workers) < n:
            raise StepError("inventory has %d workers, N=%d asked" % (len(self.inv.workers), n))
        self.stop_all()
        head = self.inv.head
        addr = "tcp://%s:%d" % (head.ip, DASK_PORT)
        self.remote.bg(head.id, STATE + "/logs/dask-scheduler.log", "%s scheduler --port %d --dashboard-address :8787" % (
            self.env_bin("dask"), DASK_PORT))
        expect = 0
        for i, w in enumerate(self.inv.workers[:n]):
            c = caps[i] if caps else None
            nw = c or self.ctx.nproc(w)
            pre = ("taskset -c %s " % cpus(c)) if c else ""
            expect += nw
            self.remote.bg(w.id, STATE + "/logs/dask-worker.log",
                           "%s%s worker %s --nworkers %d --nthreads 1 --no-dashboard %s" % (
                               pre, self.env_bin("dask"), addr, nw, self.ctx.dask_worker_args))
        probe = ("%s -c \"from distributed import Client; c = Client('%s', timeout=20); "
                 "w = c.scheduler_info()['workers'].values(); "
                 "print('PLN', len({x['host'] for x in w}), len(w))\"" % (self.env_bin("python3"), addr))
        deadline, got = time.time() + self.ctx.wait_timeout, "?"
        while time.time() < deadline:
            rc, out = self.remote.sh(head.id, probe, timeout=90)
            line = next((ln.split() for ln in out.splitlines() if ln.startswith("PLN ")), None)
            if line:
                got = "%s hosts / %s processes" % (line[1], line[2])
                if int(line[1]) == n and int(line[2]) >= expect:
                    self.up_n, self.up_caps = n, caps
                    return self.hosts_for(n)
            time.sleep(3)
        raise StepError("dask scheduler sees %s, want %d hosts / %d processes" % (got, n, expect), host=head.id)

    def deactivate(self):
        if self.up_n is not None:
            self.stop_all()

    def release(self):
        self.stop_all()

    def run(self, wl, size, logdir, n):
        ws = self.stage(wl, "dask")
        script = shlex.quote(wl.script_for("dask").rsplit("/", 1)[-1])
        args = " ".join(shlex.quote(a) for a in wl.args)
        cmd = "cd %s && env DASK_SCHEDULER=tcp://%s:%d %s %s %s %s" % (
            ws, self.inv.head.ip, DASK_PORT, self.envs(size), self.env_bin("python3"), script, args)
        t = self.remote.timed(self.inv.head.id, cmd, timeout=self.ctx.run_timeout)
        return {"rc": t.rc, "out": t.out, "start_ns": t.start_ns, "end_ns": t.end_ns}


class Torchrun(Backend):
    """torchrun DDP (gloo) on pc00..pc(N-1), one rank per PC, the same PCs
    PipedPeer's --ddp N uses; c10d rendezvous on pc00, so one command fans
    out to every PC and each takes the next node rank."""
    name = kind = "torchrun"
    PORT = 29500

    def hosts_for(self, n):
        return self.inv.all[:n]

    def activate(self, n, caps=None):
        self.ctx.only(self)
        if len(self.inv.all) < n:
            raise StepError("inventory has %d PCs, N=%d asked" % (len(self.inv.all), n))
        return self.hosts_for(n)

    def release(self):
        self.remote.sh(pattern(self.inv.all), "pkill -f '[t]orchrun' 2>/dev/null; true", timeout=60)

    def run(self, wl, size, logdir, n):
        hosts = self.hosts_for(n)
        ws = None
        for h in hosts:
            ws = self.ctx.stage(h, wl, "torchrun")
        script = shlex.quote(wl.script_for("torchrun").rsplit("/", 1)[-1])
        rdzv = "e8-%d" % time.time_ns()
        cmd = ("cd %s && env %s %s --nnodes %d --nproc-per-node 1 --rdzv-backend c10d "
               "--rdzv-endpoint %s:%d --rdzv-id %s %s") % (
            ws, self.envs(size), self.env_bin("torchrun"), n, self.inv.head.ip, self.PORT, rdzv, script)
        timed = self.remote.timed_all(hosts, cmd, timeout=self.ctx.run_timeout)
        out = "\n".join("== %s rc=%d\n%s" % (h, t.rc, t.out) for h, t in timed.items())
        bad = [h for h, t in timed.items() if t.rc != 0]
        return {"rc": timed[bad[0]].rc if bad else 0, "out": out,
                "start_ns": min(t.start_ns for t in timed.values()),
                "end_ns": max(t.end_ns for t in timed.values())}
