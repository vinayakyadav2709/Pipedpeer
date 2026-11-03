#!/usr/bin/env python3
"""Minutes per step of a lab day, from its log (results/lab/logs/day/lab-day.log):

  day-timings.py <lab-day.log>

A step runs from its "[day HH:MM:SS] == <step>" line to the next such line,
or to the last line of the same run of lab-day.sh. A step cut off by a kill
is marked "cut off" and its time is not added to the phase totals; the
rerun's time is.
"""
import re
import sys
from collections import OrderedDict

LINE = re.compile(r"^\[day (\d\d):(\d\d):(\d\d)\] (.*)$")
PHASE = {"bootstrap": "setup", "discover": "discover", "calibrate": "calibrate", "weakrefs": "calibrate",
         "analyse": "analyse"}


def phase(step):
    if step.startswith("prep."):
        return "prepare"
    if step.startswith("exp."):
        return "run"
    if step in ("clocks.end", "pull.pcap", "clean.workers", "undo.workers", "final.sync", "clean.pc00"):
        return "cleanup"
    return PHASE.get(step, "other")


def main(path):
    events, day = [], 0
    last = None
    for raw in open(path, errors="replace"):
        m = LINE.match(raw.rstrip("\n"))
        if not m:
            continue
        t = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
        if last is not None and t < last - 3600:
            day += 86400
        last = t
        events.append((t + day, m.group(4)))
    # A rerun of lab-day.sh starts with "lab day on <host>".
    steps, cur = [], None
    for t, msg in events:
        if msg.startswith("lab day on ") or msg.startswith("== "):
            if cur:
                cur["end"] = t if not msg.startswith("lab day on ") else cur["last"]
                cur["cut"] = msg.startswith("lab day on ")
                steps.append(cur)
            cur = None
            if msg.startswith("== "):
                name = msg[3:].split(":")[0].split()[0]
                cur = {"step": name, "start": t, "last": t}
        elif cur:
            cur["last"] = t
    if cur:
        cur["end"], cur["cut"] = cur["last"], False
        steps.append(cur)
    totals = OrderedDict()
    print("%-16s %-10s %8s" % ("step", "phase", "minutes"))
    for s in steps:
        mins = (s["end"] - s["start"]) / 60
        print("%-16s %-10s %8.1f%s" % (s["step"], phase(s["step"]), mins, "  (cut off)" if s["cut"] else ""))
        if not s["cut"]:
            totals[phase(s["step"])] = totals.get(phase(s["step"]), 0) + mins
    print()
    for p, m in totals.items():
        print("%-27s %8.1f" % ("phase " + p, m))
    print("%-27s %8.1f" % ("all (without cut-off steps)", sum(totals.values())))


if __name__ == "__main__":
    main(sys.argv[1])
