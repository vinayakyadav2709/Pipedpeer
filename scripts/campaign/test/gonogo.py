"""The lab day's go/no-go numbers, from a rehearsal's runs.jsonl and
attempts.jsonl (headless-test.sh):

  python3 gonogo.py <dir with runs.jsonl, attempts.jsonl> <out.json> [pc ...]

  E5   pipedpeer's cold ship ran before the campaign env (prep.e5cold),
       once, right, and no worker held any package E5 ships (numpy, joblib,
       pandas, scikit-learn, torch); the interpreter, system libraries and
       the shim's own cloudpickle come with the first job and may be there
  E14  every PC computed part of the first job; share and first work per PC
  E12  pipedpeer's joiner did more than 5 % of the items and worked for at
       least half the run; share, first work and wall for every system
  E6, E13  every pipedpeer run gave the right result
  E16-E20  ran

Writes out.json (the numbers) and out.txt (one line each, NO-GO lines last),
exits 1 on any NO-GO. Lines of interrupted attempts are left out.
"""
import collections
import json
import sys

# What E5's job imports (experiments/e5.py FULL; the rehearsal ships SMALL).
SHIPPED = {"numpy", "joblib", "pandas", "scikit-learn", "torch"}


def load(d):
    runs = [json.loads(ln) for ln in open(d + "/runs.jsonl")]
    att = [json.loads(ln) for ln in open(d + "/attempts.jsonl")]
    cut = [(a["runs_from_line"], a["runs_to_line"]) for a in att if a["status"] == "interrupted"]
    return [r for i, r in enumerate(runs) if not any(f <= i < t for f, t in cut)]


def e5(runs, out, notes, bad):
    rs = [r for r in runs if r["experiment"] == "e5"]
    cold = [r for r in rs if r["system"] == "pipedpeer" and r.get("phase") == "cold"]
    keys = ("before_campaign_env", "result_ok", "wall_s", "all_have_s", "mb_down", "closure_paths",
            "present_before", "closure_on_pc00")
    out["e5_cold"] = [{k: r.get(k) for k in keys} for r in rs if r.get("phase") == "cold"]
    for r in rs:
        if not r.get("result_ok"):
            notes.append("E5 %s %s failed: %s" % (r["system"], r.get("phase"), (r.get("error") or "")[:120]))
    if len(cold) != 1 or not cold[0].get("before_campaign_env") or not cold[0].get("result_ok"):
        bad.append("e5: want one pipedpeer cold line, before the env and right; got %s" % out["e5_cold"])
        return
    c = cold[0]
    pres = {pc: v for pc, v in (c.get("present_before") or {}).items() if pc != "pc00"}
    if not c.get("closure_paths") or not pres:
        bad.append("e5: no closure presence recorded: %s" % c)
        return
    notes.append("E5 cold: closure %s paths, %s MB; workers held before: %s; wall %s s, all PCs had it at %s s, "
                 "%s MB received" % (c["closure_paths"], c.get("closure_mb"), pres, c.get("wall_s"),
                                     c.get("all_have_s"), c.get("mb_down")))
    warm = {pc: [p for p in v["python_packages"] if p.split("-", 1)[-1].rsplit("-", 1)[0] in SHIPPED]
            for pc, v in pres.items() if isinstance(v, dict)}
    warm = {pc: p for pc, p in warm.items() if p}
    if warm:
        bad.append("e5: workers held packages E5 ships before the cold ship: %s" % warm)
    if not all(isinstance(v, dict) for v in pres.values()):
        notes.append("E5 cold: present_before has counts only (an older build); check the bytes by hand")


def e14(runs, out, notes, bad, pcs):
    rs = [r for r in runs if r["experiment"] == "e14"]
    if not rs:
        bad.append("e14: no line")
        return
    r = rs[-1]
    ip = r.get("items_per_pc") or {}
    tot = sum(ip.values()) or 1
    out["e14_share"] = {pc: round(v / tot, 4) for pc, v in sorted(ip.items())}
    out["e14_wall_s"] = r.get("wall_s")
    out["e14_first_work_s"] = {pc: round((t - r["start_ns"]) / 1e9, 2)
                               for pc, t in sorted((r.get("first_work") or {}).items())}
    notes.append("E14 first job: share %s; first work %s s; wall %s s" % (
        out["e14_share"], out["e14_first_work_s"], r.get("wall_s")))
    idle = [pc for pc in pcs if ip.get(pc, 0) <= 0]
    if idle:
        bad.append("e14: no work on %s (%s)" % (" ".join(idle), ip))
    if not r.get("result_ok"):
        bad.append("e14: wrong result or failed: %s" % (r.get("error") or "")[:120])


def e12(runs, out, notes, bad):
    for r in (r for r in runs if r["experiment"] == "e12"):
        ev = r.get("events") or []
        k = {x: r.get(x) for x in ("wall_s", "result_ok", "joiner_share", "joiner_first_work_s",
                                    "joiner_present_frac", "items_per_pc", "error")}
        k["join_at_s"] = [round((e["t_ns"] - r["start_ns"]) / 1e9, 2) for e in ev]
        k["seen_s"] = [round((e["seen_ns"] - r["start_ns"]) / 1e9, 2) if e.get("seen_ns") else None for e in ev]
        out["e12_" + r["system"]] = k
        notes.append("E12 %s: wall %s s, join at %s s, seen at %s s, joiner share %s, first work %s s after "
                     "its join, joiners present for %s of the run" % (
                         r["system"], r.get("wall_s"), k["join_at_s"], k["seen_s"], r.get("joiner_share"),
                         r.get("joiner_first_work_s"), r.get("joiner_present_frac")))
    pp = out.get("e12_pipedpeer")
    if not pp or not pp["result_ok"] or (pp["joiner_share"] or 0) <= 0.05 or (pp["joiner_present_frac"] or 0) < 0.5:
        bad.append("e12 pipedpeer: the joiner did not work for most of the run: %s" % pp)


def faults(runs, out, notes, bad):
    for e in ("e6", "e13"):
        rs = [r for r in runs if r["experiment"] == e and not r.get("warmup")]
        tab = collections.Counter((r["system"], r.get("fault") or r.get("variant") or "-", bool(r.get("result_ok")))
                                  for r in rs)
        out[e] = {"%s %s ok=%s" % k: v for k, v in sorted(tab.items())}
        notes.append("%s: %s" % (e.upper(), out[e]))
        if not rs or any(not r.get("result_ok") for r in rs if r["system"] == "pipedpeer"):
            bad.append("%s: a pipedpeer run gave a wrong result or failed (or none ran)" % e)


def more(runs, out, notes, bad):
    for e in ("e16", "e17", "e18", "e19", "e20"):
        rs = [r for r in runs if r["experiment"] == e]
        out[e] = {"runs": len(rs), "failed": sum(1 for r in rs if r.get("error")),
                  "errors": sorted({"%s: %s" % (r["system"], (r.get("error") or "")[:100]) for r in rs if r.get("error")})}
        notes.append("%s: %d runs, %d failed %s" % (e.upper(), out[e]["runs"], out[e]["failed"], out[e]["errors"]))
        if not rs:
            bad.append("%s: no runs" % e)


def main(argv):
    d, outp = argv[1], argv[2]
    pcs = argv[3:] or ["pc00", "pc01", "pc02", "pc03"]
    runs = load(d)
    out, notes, bad = {}, [], []
    e5(runs, out, notes, bad)
    e14(runs, out, notes, bad, pcs)
    e12(runs, out, notes, bad)
    faults(runs, out, notes, bad)
    more(runs, out, notes, bad)
    out["no_go"] = bad
    with open(outp, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    with open(outp.rsplit(".", 1)[0] + ".txt", "w") as f:
        f.write("\n".join(notes + ["NO-GO: " + b for b in bad]) + "\n")
    print("\n".join(notes + ["NO-GO: " + b for b in bad]))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
