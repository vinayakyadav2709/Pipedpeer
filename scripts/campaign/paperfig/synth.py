#!/usr/bin/env python3
"""A made-up results directory at lab-day scale (17 PCs, N = 1, 4, 16,
every experiment), for checking figure layouts before the lab day:

  python3 scripts/campaign/paperfig/synth.py OUT_DIR
  python3 scripts/campaign/paper_figures.py --results OUT_DIR --draft --out OUT_DIR/figures

Layout only: every number is invented. Never draw paper figures from it;
write it outside the repository (a cache directory).
"""
import json
import os
import random
import sys

R = random.Random(7)
PCS = ["pc%02d" % i for i in range(17)]
NS = [1, 4, 16]
T0 = 1_800_000_000 * 10**9
EFF = {"pipedpeer": 0.88, "ray": 0.7, "dask": 0.62}
LINES = {"W1": (6, 6), "W2": (12, 10), "W3": (8, 6), "W4": (8, 6)}
BASE = {"W1": 600.0, "W2": 900.0, "W3": 420.0, "W4": 300.0}


class W:
    def __init__(self, out):
        self.out, self.t, self.k = out, T0, 0
        self.rows = []

    def run(self, **r):
        self.k += 1
        wall = r["wall_s"]
        rid = "%s-%05d" % (r["experiment"], self.k)
        r.setdefault("result_ok", True)
        r.update(run_id=rid, start_ns=self.t, end_ns=self.t + int(wall * 1e9), log_path="logs/" + rid)
        r.setdefault("hosts", PCS[:max(1, r.get("n", 1)) + (0 if r["system"] in ("pipedpeer", "plain") else 1)])
        self.t += int((wall + 20) * 1e9)
        self.rows.append(r)  # written by close(), so later edits to r land
        return r

    def close(self):
        with open(os.path.join(self.out, "runs.jsonl"), "w") as f:
            for r in self.rows:
                f.write(json.dumps(r) + "\n")

    def file(self, r, name, obj):
        d = os.path.join(self.out, r["log_path"])
        os.makedirs(d, exist_ok=True)
        json.dump(obj, open(os.path.join(d, name), "w"))


def noisy(x, sd=0.04):
    return x * (1 + R.gauss(0, sd))


def e1(w):
    for wl, base in BASE.items():
        for rep in range(-1, 5):
            w.run(experiment="e1", workload=wl, system="plain", n=1, repeat=max(rep, 0), warmup=rep < 0,
                  mode="strong", wall_s=noisy(base))
        for s, eff in EFF.items():
            for n in NS:
                for rep in range(-1, 5):
                    wall = noisy(base / (n * eff * (0.97 if n > 1 else 1.0)) + (2 if s != "pipedpeer" else 0.6))
                    r = w.run(experiment="e1", workload=wl, system=s, n=n, repeat=max(rep, 0), warmup=rep < 0,
                              mode="strong", wall_s=wall)
                    if s == "pipedpeer":
                        loc = int(10000 / n * noisy(1.1, 0.05))
                        r["receipt"] = {"local_items": loc, "remote_items": 10000 - loc}
                        w.file(r, "record.json", {"steps": [
                            {"step": "env_build", "chunk": -1, "dur_ns": int(noisy(0.012) * 1e9)},
                            {"step": "env_send", "chunk": -1, "dur_ns": int(noisy(0.004 * n ** 0.5) * 1e9)},
                            {"step": "script", "chunk": -1, "dur_ns": int(wall * 0.98 * 1e9)}]})
    for s in ["plain"] + list(EFF):
        for n in ([1] if s == "plain" else NS):
            for rep in range(5):
                e = EFF.get(s, 1.0)
                w.run(experiment="e1b", workload="W1", system=s, n=n, repeat=rep, mode="weak",
                      wall_s=noisy(BASE["W1"] / 4 / (e ** (0.25 * NS.index(n)) if n > 1 else 1)))


def faults(w):
    for s in ("pipedpeer", "pipedpeer-headcount", "ray", "dask"):
        for rep in range(5):
            r = w.run(experiment="e4", workload="W1", system=s, n=2, repeat=rep, caps=[8, 4],
                      hosts=["pc00", "pc01"], wall_s=noisy({"pipedpeer": 52, "pipedpeer-headcount": 74,
                                                            "ray": 70, "dask": 61}[s]))
            if s.startswith("pipedpeer"):
                a = 0.66 if s == "pipedpeer" else 0.5
                r["receipt"] = {"by_where": {"local": int(9600 * a), "peer:10.0.0.12:4242": int(9600 * (1 - a))}}
    for s in EFF:
        for rep in range(5):
            w.run(experiment="e6", workload="W1", system=s, n=4, repeat=rep, fault="none", wall_s=noisy(40))
            w.run(experiment="e6", workload="W1", system=s, n=4, repeat=rep, fault="kill -9", killed_pc="pc02",
                  kill_at_s=12, wall_s=noisy({"pipedpeer": 46, "ray": 41, "dask": 55}[s]),
                  result_ok=s != "ray" or rep % 2 == 0)
    w.run(experiment="e6", workload="W1", system="pipedpeer-ctrlc", n=4, ctrlc_latency_s=0.42, wall_s=3.1)
    for rep in range(10):
        w.run(experiment="e7", workload="E7-trivial", system="pipedpeer-noshim", n=2, repeat=rep, wall_s=noisy(0.31, 0.06))
        w.run(experiment="e7", workload="E7-trivial", system="pipedpeer-shim", n=2, repeat=rep, wall_s=noisy(0.36, 0.06))
    for s, wall, b, loss in (("torchrun", 312, 2.9e9, 0.0812), ("pipedpeer-exact", 305, 2.9e9, 0.0812),
                          ("pipedpeer-fast", 221, 0.8e9, 0.0838)):
        for rep in range(3):
            w.run(experiment="e8", workload="T1", system=s, n=4, repeat=rep, variant="mlp-c", wall_s=noisy(wall),
                  bytes_per_rank=int(noisy(b, 0.01)), result="BENCH_LOSS %.6f" % noisy(loss, 0.004))


def timeline(per_pc_rate, wall, bin_s=1):
    n = int(wall / bin_s) + 1
    return {"bin_s": bin_s, "per_pc": {pc: [max(0.0, noisy(f(t * bin_s), 0.08)) for t in range(n)]
                                       for pc, f in per_pc_rate.items()}}


def headline(w):
    for s, wall, uses_new in (("pipedpeer", 160, True), ("ray", 260, False), ("dask", 190, True)):
        start = PCS[1:5]
        joins = [PCS[5 + 4 * k: 9 + 4 * k] for k in range(3)]
        rate = {pc: (lambda t: 30.0) for pc in start}
        for k, g in enumerate(joins):
            for pc in g:
                rate[pc] = (lambda t, at=20 * (k + 1) + 3: 30.0 if uses_new and t > at else 0.0)
        r = w.run(experiment="e12", workload="W1", system=s, n=4, hosts=["pc00"] + start, wall_s=wall,
                  timeline=timeline(rate, wall))
        r["events"] = [{"kind": "join", "t_ns": r["start_ns"] + int(20e9 * (k + 1)), "pcs": g,
                        "seen_ns": r["start_ns"] + int((20 * (k + 1) + 2.5) * 1e9)} for k, g in enumerate(joins)]
    for s, wall, ok in (("pipedpeer", 70, True), ("ray", 44, False), ("dask", 96, True)):
        rate = {pc: (lambda t, pc=pc: 0.0 if pc == "pc03" and (t % 20) < 6 else 25.0) for pc in PCS[1:5]}
        r = w.run(experiment="e13", workload="W1", system=s, n=4, wall_s=wall, timeline=timeline(rate, wall),
                  result_ok=ok, error=None if ok else "exit 1: actor died", kills=wall // 20, items_rerun=40 * (s != "pipedpeer"),
                  churn_every_s=20, churn_down_s=6, fault="churn", size={"BENCH_ITEMS": "8000"})
        r["events"] = []
        for k in range(int(wall // 20)):
            r["events"] += [{"kind": "kill", "pc": "pc03", "t_ns": r["start_ns"] + int((20 * k + 1) * 1e9)},
                            {"kind": "restart", "pc": "pc03", "t_ns": r["start_ns"] + int((20 * k + 7) * 1e9)}]
    for s in EFF:
        for rtt in (0, 20, 80):
            for rep in range(5):
                w.run(experiment="e15", workload="W1", system=s, n=4, repeat=rep, rtt_added_ms=rtt,
                      wall_s=noisy(30 * (1 + rtt / 400 * {"pipedpeer": 0.5, "ray": 1.0, "dask": 2.2}[s])))
    with open(os.path.join(w.out, "timeline.jsonl"), "w") as f:
        head = T0 - int(900e9)

        def ev(pc, e, t):
            f.write(json.dumps({"pc": pc, "event": e, "t_ns": head + int(t * 1e9), "machine_id": "m" + pc}) + "\n")
        ev("pc00", "head_start", 0)
        for i, pc in enumerate(PCS):
            t = -120 + i * 18 + R.uniform(0, 30)
            ev(pc, "setup_start", t)
            ev(pc, "labpc", t + 5)
            ev(pc, "ssh_ok", t + 40)
            f.write(json.dumps({"pc": pc, "event": "pc_map", "machine_id": "m" + pc}) + "\n")
            ev(pc, "install_end", t + 140 + R.uniform(0, 40))
            ev(pc, "join_end", t + 190 + R.uniform(0, 30))
            ev(pc, "first_work", 560 + R.uniform(0, 4))
        ev("pc00", "cluster_ready", 520)
        ev("pc00", "first_job_start", 555)
        ev("pc00", "first_job_end", 600)


def more(w):
    for s, cpu, pss, rss, ready, cmds in (("pipedpeer", 0.3, 15, 26, 2.1, 0), ("ray", 2.5, 590, 820, 14.0, 17),
                                          ("dask", 4.8, 75, 80, 6.2, 17)):
        w.run(experiment="e17", workload="E17-idle", system=s, n=16, wall_s=80, footprint={"pc00": {}},
              cpu_pct_mean_per_pc=cpu, pss_mb_total=pss * 17, rss_mb_total=rss * 17, ready_s=ready,
              ready_after_s=ready - 1.0, commands_typed=cmds)
    for s in EFF:
        for fault in ("none", "hog"):
            wall = 40 * ({"pipedpeer": 1.08, "ray": 1.45, "dask": 1.15}[s] if fault == "hog" else 1)
            work = PCS[:4] if s == "pipedpeer" else PCS[1:5]
            rate = {pc: (lambda t, pc=pc: 12.0 if (fault == "hog" and pc == "pc01" and t > 5 and s != "ray")
                         else 25.0) for pc in work}
            r = w.run(experiment="e18", workload="W1", system=s, n=4, fault=fault, hog_pcs=["pc01"] if fault == "hog" else [],
                      wall_s=wall, timeline=timeline(rate, wall), fair_share=0.25,
                      hog_share=(0.16 if s != "ray" else 0.25) if fault == "hog" else 0.25)
            r["events"] = [{"kind": "hog", "t_ns": r["start_ns"] + int(5e9), "pcs": ["pc01"]}] if fault == "hog" else []
            r["chunks_path"] = r["log_path"] + "/chunks.json"
            chunks = []
            for pc in work:
                t = 0.5
                while t < wall - 1:
                    d = R.uniform(2, 5) if s != "dask" else 0
                    chunks.append({"pc": pc, "t0": t, "t1": t + d, "items": 40})
                    t += d if d else 1
            w.file(r, "chunks.json", {"chunks": chunks})
    for s in ("pipedpeer", "ray"):
        for ph, t in (("cold", 48 if s == "pipedpeer" else 95), ("warm", 3.1 if s == "pipedpeer" else 11),
                      ("edit", 3.3 if s == "pipedpeer" else 12), ("newdep", 21 if s == "pipedpeer" else 60)):
            for rep in range(3):
                extra = {"env_build_s": noisy(t * 0.6) if ph in ("cold", "newdep") else 0.01,
                         "env_send_s": noisy(t * 0.25) if ph in ("cold", "newdep") else 0.05} if s == "pipedpeer" else {}
                w.run(experiment="e19", workload="E19", system=s, n=16, phase=ph, variant=ph, ttfr_s=noisy(t),
                      wall_s=t, **extra)
    for wl, base in (("W5", 240.0), ("W6", 380.0)):
        for rep in range(2):
            w.run(experiment="e20", workload=wl, system="plain", n=1, repeat=rep, wall_s=noisy(base),
                  script_lines_changed=0)
            for s, sp, lines in (("pipedpeer", 11.0, 0), ("ray", 7.5, 9), ("dask", 8.2, 7)):
                w.run(experiment="e20", workload=wl, system=s, n=16, repeat=rep, wall_s=noisy(base / sp),
                      script_lines_changed=lines, result_ok=not (s == "ray" and wl == "W6"))


def main(out):
    os.makedirs(out, exist_ok=True)
    w = W(out)
    e1(w)
    faults(w)
    headline(w)
    more(w)
    w.close()
    with open(os.path.join(out, "machines.csv"), "w") as f:
        f.write("pc_id,ip,hostname\n")
        for i, pc in enumerate(PCS):
            f.write("%s,10.0.0.%d,lab-%d\n" % (pc, 11 + i, i))
    with open(os.path.join(out, "port-lines.csv"), "w") as f:
        f.write("workload,system,changed_lines\n")
        for wl, (ray, dask) in LINES.items():
            f.write("%s,plain,0\n%s,pipedpeer,0\n%s,ray,%d\n%s,dask,%d\n" % (wl, wl, wl, ray, wl, dask))


if __name__ == "__main__":
    main(sys.argv[1])
