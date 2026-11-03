#!/usr/bin/env python3
"""Tables and the summary of the lab day (lab-day.sh). Stdlib only.

  tables.py machines <out dir> <inventory> <csv>   results/lab/machines.csv
  tables.py network <out root> <inventory> <csv>   results/lab/network.csv
      (iperf3 from day/prepare.sh, plus ping RTT from here, pc00)
  tables.py summary <results> <day dir> <expected pcs> [note]
"""
import csv
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time


def inventory(path):
    hosts, group = [], None
    for line in open(path):
        line = line.strip()
        if line.startswith("["):
            group = line.strip("[]")
        elif group in ("head", "workers") and re.match(r"pc\d+ ", line):
            f = line.split()
            v = dict(p.split("=", 1) for p in f[1:] if "=" in p)
            hosts.append((f[0], v.get("ansible_host", ""), group))
    return hosts


def write_csv(path, header, rows):
    with open(path + ".new", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    os.replace(path + ".new", path)


def machines(outdir, inv, out):
    cols = ["hostname", "cpu_model", "cores", "threads_per_core", "ram_gib", "os", "kernel",
            "nic", "nic_mbps", "python", "machine_id"]
    rows = []
    for pc, ip, _ in inventory(inv):
        p = os.path.join(outdir, pc + ".out")
        kv = {}
        if os.path.exists(p):
            for line in open(p):
                k, _, v = line.rstrip("\n").partition("=")
                if k in cols:
                    kv[k] = v
        rows.append([pc, ip] + [kv.get(c, "") for c in cols])
    write_csv(out, ["pc_id", "ip"] + cols, rows)


def iperf_mbps(text):
    try:
        doc = json.loads(text)
        return round(doc["end"]["sum_received"]["bits_per_second"] / 1e6, 1)
    except (ValueError, KeyError, TypeError):
        return ""


def rtt(ip):
    """Average and mdev RTT in ms: ping when present, else TCP connects to 22."""
    if shutil.which("ping"):
        p = subprocess.run(["ping", "-n", "-q", "-c", "10", "-i", "0.2", ip], capture_output=True, text=True)
        m = re.search(r"= [\d.]+/([\d.]+)/[\d.]+/([\d.]+) ms", p.stdout)
        if m:
            return m.group(1), m.group(2), "icmp"
    times = []
    for _ in range(10):
        t0 = time.perf_counter()
        try:
            socket.create_connection((ip, 22), 2).close()
        except OSError:
            continue
        times.append((time.perf_counter() - t0) * 1000)
    if not times:
        return "", "", "none"
    avg = sum(times) / len(times)
    mdev = (sum((t - avg) ** 2 for t in times) / len(times)) ** 0.5
    return "%.3f" % avg, "%.3f" % mdev, "tcp22-connect"


def network(outroot, inv, out):
    rows = []
    for pc, ip, group in inventory(inv):
        if group != "workers":
            continue
        p = os.path.join(outroot, "iperf." + pc, pc + ".out")
        up = down = ""
        if os.path.exists(p):
            fwd, _, rev = open(p).read().partition("@@REVERSE@@")
            up, down = iperf_mbps(fwd), iperf_mbps(rev)
        avg, mdev, method = rtt(ip)
        rows.append([pc, ip, avg, mdev, method, up, down])
    write_csv(out, ["pc_id", "ip", "rtt_avg_ms", "rtt_mdev_ms", "rtt_method",
                    "to_pc00_mbps", "from_pc00_mbps"], rows)


def jsonl(path):
    if not os.path.exists(path):
        return []
    out = []
    for line in open(path):
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def summary(results, day, expected, note=""):
    width = 70
    lines = ["=" * width, "  PipedPeer lab day: summary (%s)" % time.strftime("%Y-%m-%d %H:%M"), ""]
    inv = os.path.join(day, "inventory-all.ini")
    found = inventory(inv) if os.path.exists(inv) else []
    lines.append("  PCs: %d found of %s expected (pc00 included)" % (len(found), expected))
    dropped = list(csv.DictReader(open(os.path.join(results, "dropped.csv")))) if os.path.exists(
        os.path.join(results, "dropped.csv")) else []
    for d in dropped:
        lines.append("    dropped %s during %s: %s" % (d["pc_id"], d["step"], d["reason"][:90]))
    ledger = open(os.path.join(day, "done")).read().splitlines() if os.path.exists(os.path.join(day, "done")) else []
    for line in ledger:
        if line.startswith("aborted: "):
            lines.append("  STOPPED EARLY: " + line[len("aborted: "):])

    attempts = jsonl(os.path.join(results, "attempts.jsonl"))
    # The lines of cut-off attempts do not count (analyse.py drops them too).
    cut = set()
    for a in attempts:
        if a["status"] == "interrupted":
            cut.update(range(a["runs_from_line"], a["runs_to_line"]))
    runs = []
    rpath = os.path.join(results, "runs.jsonl")
    for i, line in enumerate(open(rpath) if os.path.exists(rpath) else []):
        try:
            if i not in cut:
                runs.append(json.loads(line))
        except ValueError:
            pass
    last = {}
    for a in attempts:
        last[a["experiment"]] = a
    lines += ["", "  Experiments:"]
    if not last:
        lines.append("    none ran")
    for exp, a in last.items():
        mine = [r for r in runs if str(r.get("experiment", "")).lower() == exp.lower()]
        bad = [r for r in mine if r.get("error") or r.get("result_ok") is False]
        status = "ok" if a["status"] == "ok" else a["status"].upper()
        lines.append("    %-5s %-8s %4d runs, %d failed" % (exp, status, len(mine), len(bad)))
    interrupted = [a for a in attempts if a["status"] == "interrupted"]
    if interrupted:
        lines.append("    (%d cut-off attempts: their lines are marked in results/lab/attempts.jsonl)" % len(interrupted))
    blockers = os.path.join(results, "BLOCKERS.md")
    if os.path.exists(blockers):
        n = sum(1 for line in open(blockers) if line.startswith("- "))
        lines.append("    %d problems recorded in results/lab/BLOCKERS.md (in R2 too)" % n)

    lines += ["", "  Results in Cloudflare R2 (bucket pipedpeer-lab):",
              "    raw/        results/lab: runs.jsonl, CSVs, figures, logs, pcap/",
              "    raw/otel/   telemetry (host metrics, traces, daemon logs)"]
    fc = os.path.join(day, "final-count.txt")
    if os.path.exists(fc):
        lines += ["    " + line.strip() for line in open(fc) if line.strip()]

    problems = os.path.join(day, "cleanup-problems")
    probs = [line.rstrip("\n") for line in open(problems)] if os.path.exists(problems) else []
    lines.append("")
    if note:
        lines.append("  " + note)
    elif probs:
        lines.append("  DONE, but these PCs are not fully back as they were:")
        for p in probs:
            pc, _, what = p.partition(" ")
            lines.append("    %s: %s" % (pc, what))
    else:
        lines += ["  ALL DONE, the lab PCs are back as they were. You can leave."]
    lines.append("=" * width)
    print("\n".join(lines))


def main():
    cmd = sys.argv[1]
    if cmd == "machines":
        machines(*sys.argv[2:5])
    elif cmd == "network":
        network(*sys.argv[2:5])
    elif cmd == "summary":
        summary(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5] if len(sys.argv) > 5 else "")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
