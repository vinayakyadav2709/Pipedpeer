#!/usr/bin/env python3
"""Workload sizes for the lab day (lab-day.sh).

  sizes.py apply <repo> <calibration dir> <pc>   calibrate.sh's results into
        scripts/campaign/workloads.json and results/lab/workloads.md
  sizes.py weak <repo> <workload> <n>            the weak size env at N, as
        NAME=value words (what run.sh uses for e1b)
  sizes.py have <repo> <workload> <NAME=value...> exit 0 if run.sh's result
        cache already has a reference at that size
  sizes.py put <repo> <workload> <NAME=value...> <source>   the plain run's
        stdout (stdin) into run.sh's result cache, as the reference

The cache key and the result string are run.sh's own (lib/campaign/record.py,
workloads.py), imported from there when present, so e1b finds these entries.
"""
import datetime
import json
import os
import re
import sys


def runner(repo):
    lib = os.path.join(repo, "scripts", "campaign", "lib")
    if os.path.isdir(os.path.join(lib, "campaign")):
        sys.path.insert(0, lib)
        try:
            from campaign import record, workloads  # noqa: PLC0415
            return record, workloads
        except ImportError:
            pass
    return None, None


def spec(repo):
    with open(os.path.join(repo, "scripts", "campaign", "workloads.json")) as f:
        return json.load(f)


def workload(repo, wid):
    return next(w for w in spec(repo)["workloads"] if w["id"] == wid)


def cache_key(repo, wid, size):
    record, _ = runner(repo)
    if record:
        return record.ResultCache.key(wid, size)
    return wid + "|" + ",".join("%s=%s" % kv for kv in sorted(size.items()))


def extract(repo, regex, text):
    _, wl = runner(repo)
    if wl:
        return wl.extract(regex, text)
    found = [m.group(1) if m.groups() else m.group(0) for m in re.finditer(regex, text)]
    return " | ".join(found) if found else None


def parse_size(words):
    return dict(w.split("=", 1) for w in words)


def cache_path(repo):
    return os.path.join(repo, "results", "lab", "result-cache.json")


def load_cache(repo):
    p = cache_path(repo)
    return json.load(open(p)) if os.path.exists(p) else {}


def weak(repo, wid, n):
    w = workload(repo, wid)
    per = w.get("weak_per_pc") or {}
    if not per:
        sys.exit(f"{wid} has no weak size")
    return {k: str(int(v) * n) if v.isdigit() else v for k, v in per.items()}


def apply(repo, caldir, pc):
    path = os.path.join(repo, "scripts", "campaign", "workloads.json")
    doc = spec(repo)
    cals = {}
    for w in doc["workloads"]:
        p = os.path.join(caldir, w["id"] + ".json")
        if os.path.exists(p):
            cals[w["id"]] = c = json.load(open(p))
            w["strong"] = c["strong"]
            if "weak_per_pc" in c:
                w["weak_per_pc"] = c["weak_per_pc"]
    if not cals:
        sys.exit("no calibration results")
    any_c = next(iter(cals.values()))
    when = datetime.date.today().isoformat()
    doc["sizes_status"] = (f"calibrated on {pc} ({any_c['host']}, {any_c['nproc']} cpus) on {when} "
                           f"by lab-day.sh: {', '.join(sorted(cals))}")
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)

    md = os.path.join(repo, "results", "lab", "workloads.md")
    text = open(md).read() if os.path.exists(md) else "# Lab campaign workloads\n"
    rows = [f"| Workload | Strong size | Weak size per PC | Fit on {pc} | Strong size measured on {pc} |",
            "|---|---|---|---|---|"]
    for wid, c in sorted(cals.items()):
        var = c["var"]
        weak_s = f"`{var}={c['weak_per_pc'][var]}` × N" if "weak_per_pc" in c else "none"
        unit = {"BENCH_ITEMS": "items", "CPU_ITEMS": "items", "RF_TREES": "trees", "GS_SAMPLES": "rows"}.get(var, var)
        verify = f"{c['verify_s']:.1f} s" if "verify_s" in c else "not run"
        rows.append(f"| {wid} | `{var}={c['strong'][var]}` | {weak_s} | "
                    f"{c['c0_s']:.2f} s + {c['c1_s']:.3g} s × {unit} | {verify} |")
    table = "\n".join(rows)
    note = (f"Calibrated on {pc} ({any_c['host']}, {any_c['nproc']} cpus) on {when} by lab-day.sh with "
            "`CAL_VERIFY=1 scripts/campaign/calibrate.sh`; the raw output is in "
            "`results/lab/calibration/`. These sizes hold for every system and every run of the day.")
    sec = re.search(r"^## Sizes\n.*?(?=^## )", text, flags=re.S | re.M)
    if sec:
        body = sec.group(0)
        body = re.sub(r"^\| Workload \| Strong size.*?\n(?:\|.*\n)+", table + "\n", body, count=1, flags=re.M)
        body = re.sub(r"^Provisional, laptop; recalibrate on pc01 in session 1\.", note, body, count=1, flags=re.M)
        if note not in body:
            body = body.replace(table, table + "\n\n" + note, 1)
        text = text[:sec.start()] + body + text[sec.end():]
    else:
        text += "\n## Sizes\n\n" + table + "\n\n" + note + "\n"
    text = re.sub(r"^\*\*Sizes are provisional \(laptop\)\. Recalibrate on pc01 in session 1\*\*.*$",
                  f"**Sizes calibrated on {pc} on {when}** (see Sizes).", text, count=1, flags=re.M)
    with open(md + ".tmp", "w") as f:
        f.write(text)
    os.replace(md + ".tmp", md)
    print(table)


def main():
    cmd, repo = sys.argv[1], sys.argv[2]
    if cmd == "apply":
        apply(repo, sys.argv[3], sys.argv[4])
    elif cmd == "weak":
        size = weak(repo, sys.argv[3], int(sys.argv[4]))
        print(" ".join(f"{k}={v}" for k, v in size.items()))
    elif cmd == "have":
        size = parse_size(sys.argv[4].split())
        sys.exit(0 if cache_key(repo, sys.argv[3], size) in load_cache(repo) else 1)
    elif cmd == "put":
        wid, size, source = sys.argv[3], parse_size(sys.argv[4].split()), sys.argv[5]
        result = extract(repo, workload(repo, wid)["result_regex"], sys.stdin.read())
        if not result:
            sys.exit(f"{wid} at {size}: no result in the output")
        data = load_cache(repo)
        data[cache_key(repo, wid, size)] = {"result": result, "source": source}
        tmp = cache_path(repo) + ".tmp"
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with open(tmp, "w") as f:
            json.dump(data, f, indent=1, sort_keys=True)
        os.replace(tmp, cache_path(repo))
        print(f"{wid} {size}: {result} ({source})")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
