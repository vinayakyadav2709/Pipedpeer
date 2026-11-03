"""What one system's processes cost on this PC while it idles, for E17.
experiments/e17.py stages this file on every PC and runs it with the
campaign env's python3:

  footprint_probe.py <pipedpeer|ray|dask> <seconds>

The processes: pipedpeer, every `pipedpeer __daemon__` and its
descendants; ray and dask, every process whose environment carries the
runner's mark (PL_CAMPAIGN_RAY=1, PL_CAMPAIGN_DASK=1), which every child
inherits (raylet, gcs_server, the idle workers, Dask's nanny and its
worker). Once a second it reads /proc/<pid>/stat (CPU ticks), statm (RSS)
and smaps_rollup (PSS, which splits shared pages among the processes that
share them). Prints one line "PLFP <json>": cpu_s over the window, cpu_pct
(of one core), RSS and PSS sums (MB, mean and max over the samples), the
process count and their names.
"""
import json
import os
import sys
import time

MARK = {"ray": b"PL_CAMPAIGN_RAY=1", "dask": b"PL_CAMPAIGN_DASK=1"}
TICK = os.sysconf("SC_CLK_TCK")
PAGE = os.sysconf("SC_PAGE_SIZE")


def _read(path, mode="r"):
    try:
        with open(path, mode) as f:
            return f.read()
    except OSError:
        return None


def _stat(pid):
    """(ppid, comm, cpu ticks) or None."""
    s = _read("/proc/%d/stat" % pid)
    if not s:
        return None
    comm = s[s.index("(") + 1:s.rindex(")")]
    f = s[s.rindex(")") + 2:].split()
    return int(f[1]), comm, int(f[11]) + int(f[12])


def _roots(kind, me):
    out = set()
    for d in os.listdir("/proc"):
        if not d.isdigit() or int(d) == me:
            continue
        pid = int(d)
        if kind == "pipedpeer":
            cmd = (_read("/proc/%d/cmdline" % pid, "rb") or b"").split(b"\0")
            if len(cmd) > 1 and os.path.basename(cmd[0]) == b"pipedpeer" and cmd[1] == b"__daemon__":
                out.add(pid)
        elif MARK[kind] in (_read("/proc/%d/environ" % pid, "rb") or b""):
            out.add(pid)
    return out


def processes(kind, me):
    """{pid: (comm, ticks)} for the system's processes and their children."""
    stats = {}
    for d in os.listdir("/proc"):
        if d.isdigit() and int(d) != me:
            st = _stat(int(d))
            if st:
                stats[int(d)] = st
    keep = _roots(kind, me) & set(stats)
    grew = True
    while grew:
        grew = False
        for pid, (ppid, _, _) in stats.items():
            if ppid in keep and pid not in keep:
                keep.add(pid)
                grew = True
    return {pid: (stats[pid][1], stats[pid][2]) for pid in keep}


def _mem(pid):
    rss = pss = 0
    m = _read("/proc/%d/statm" % pid)
    if m:
        rss = int(m.split()[1]) * PAGE
    r = _read("/proc/%d/smaps_rollup" % pid)
    for line in (r or "").splitlines():
        if line.startswith("Pss:"):
            pss = int(line.split()[1]) * 1024
    return rss, pss or rss


def main():
    kind, seconds = sys.argv[1], float(sys.argv[2])
    me = os.getpid()
    first, last, names = {}, {}, {}
    rss, pss = [], []
    t0 = time.time()
    n = 0
    while True:
        procs = processes(kind, me)
        r = p = 0
        for pid, (comm, ticks) in procs.items():
            first.setdefault(pid, ticks if n == 0 else 0)  # a process born mid-window counts from 0
            last[pid] = ticks
            names[pid] = comm
            a, b = _mem(pid)
            r += a
            p += b
        rss.append(r)
        pss.append(p)
        n += 1
        if time.time() - t0 >= seconds:
            break
        time.sleep(1)
    dt = time.time() - t0
    cpu = sum(last[p] - first[p] for p in last) / TICK
    count = {}
    for c in names.values():
        count[c] = count.get(c, 0) + 1
    mb = 1e6
    print("PLFP " + json.dumps({
        "kind": kind, "seconds": round(dt, 2), "samples": n, "procs": len(names), "names": count,
        "cpu_s": round(cpu, 3), "cpu_pct": round(100 * cpu / dt, 2) if dt else None,
        "rss_mb_avg": round(sum(rss) / n / mb, 1), "rss_mb_max": round(max(rss) / mb, 1),
        "pss_mb_avg": round(sum(pss) / n / mb, 1), "pss_mb_max": round(max(pss) / mb, 1)}, sort_keys=True))


if __name__ == "__main__":
    main()
