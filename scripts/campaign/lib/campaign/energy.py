"""Package energy (RAPL) per run, from otel/energy.sh's 1 s samples (E16).

energy.sh runs on every PC (node.sh starts it with the collector; ensure()
starts it where it is not running) and appends to
$STATE/logs/energy.jsonl: a header naming each zone with its counter range,
then one line per second with every zone's energy_uj. A run's joules on a
PC are the package zones' counter growth between the run's start_ns and
end_ns, each end interpolated between the samples around it, with the
counter's wrap (max_energy_range_uj) undone. Samples carry the PC's own
clock and runs pc00's; with chrony (clocks.txt, under 10 ms) that is far
below the 1 s period.

Only zones named package-* (powercap) or Esocket* (hwmon amd_energy) are
summed: psys covers the package too, and the subzones (core, uncore, dram)
lie inside it.
"""
import base64
import json
import os
import time

from . import systems

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.normpath(os.path.join(HERE, "..", "..", "otel", "energy.sh"))
REMOTE_SCRIPT = systems.STATE + "/energy.sh"
FILE = systems.STATE + "/logs/energy.jsonl"
PACKAGE = ("package", "Esocket")


def parse(text):
    """{"available", "reason", "source", "zones": {id: {name, max_uj}},
    "samples": [(t_ns, {id: uj})]} from energy.jsonl text. available is
    None when the text has no header line at all."""
    st = {"available": None, "reason": None, "source": None, "zones": {}, "samples": []}
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            d = json.loads(line)
        except ValueError:
            continue
        if d.get("kind") == "energy-start":
            st["available"] = bool(d.get("available"))
            st["reason"] = d.get("reason")
            st["source"] = d.get("source") or st["source"]
            st["zones"].update(d.get("zones") or {})
        elif d.get("kind") == "energy" and isinstance(d.get("ts_ns"), int):
            st["samples"].append((d["ts_ns"], {k: v for k, v in (d.get("uj") or {}).items() if isinstance(v, int)}))
            if st["available"] is None:
                st["available"] = True
    st["samples"].sort(key=lambda s: s[0])
    return st


def _series(samples, zone, max_uj):
    """[(t_ns, cumulative uJ)] for one zone, the counter's wraps undone."""
    out, last, acc = [], None, 0
    for t, uj in samples:
        v = uj.get(zone)
        if v is None:
            continue
        if last is not None:
            d = v - last
            if d < 0:
                d += (max_uj + 1) if max_uj else 0
                d = max(d, 0)
            acc += d
        last = v
        out.append((t, acc))
    return out


def _at(series, t):
    """Cumulative uJ at t, linear between the samples around it; None when
    t is outside the samples."""
    if not series or t < series[0][0] or t > series[-1][0]:
        return None
    lo = series[0]
    for hi in series:
        if hi[0] >= t:
            if hi[0] == lo[0]:
                return float(hi[1])
            return lo[1] + (hi[1] - lo[1]) * (t - lo[0]) / (hi[0] - lo[0])
        lo = hi
    return None


def package_zones(st):
    return sorted(z for z, info in st["zones"].items() if str(info.get("name", "")).startswith(PACKAGE))


def joules(st, t0_ns, t1_ns):
    """(joules over the package zones in [t0, t1], None) or (None, why)."""
    if not st.get("available"):
        return None, st.get("reason") or "no energy samples"
    zones = package_zones(st)
    if not zones:
        return None, "no package zone (zones: %s)" % (", ".join(sorted(st["zones"])) or "none")
    total = 0.0
    for z in zones:
        s = _series(st["samples"], z, int(st["zones"][z].get("max_uj") or 0))
        a, b = _at(s, t0_ns), _at(s, t1_ns)
        if a is None or b is None:
            return None, "the samples do not cover the run"
        total += b - a
    return round(total / 1e6, 3), None


def per_host(out, hosts):
    """Split an Ansible fan-out's output ("pcNN: line") into {pc: text}."""
    ids = [h.id for h in hosts]
    if len(ids) == 1:
        return {ids[0]: out}
    res = {i: [] for i in ids}
    for line in out.splitlines():
        h, sep, rest = line.partition(": ")
        if sep and h in res:
            res[h].append(rest)
    return {h: "\n".join(v) for h, v in res.items()}


def stage_cmd():
    with open(SCRIPT, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    return "mkdir -p %s && printf %%s %s | base64 -d > %s" % (systems.STATE + "/logs", data, REMOTE_SCRIPT)


def ensure(ctx, hosts):
    """Start energy.sh on every host where it is not running. Returns
    {pc: "running"|"started"|"unavailable: <reason>"}."""
    rc, out = ctx.remote.sh(systems.pattern(hosts), "%s && sh %s start" % (stage_cmd(), REMOTE_SCRIPT),
                            timeout=60)
    res = {}
    for pc, text in per_host(out, hosts).items():
        line = next((ln for ln in text.splitlines() if ln.startswith("PLENERGY ")), "PLENERGY unavailable no answer")
        word, _, rest = line[len("PLENERGY "):].partition(" ")
        res[pc] = "unavailable: " + rest if word == "unavailable" else word
    ctx.log("energy sampler: %s" % ", ".join("%s %s" % kv for kv in sorted(res.items())))
    return res


def fetch(ctx, hosts, lines=None):
    """{pc: parsed energy.jsonl}; lines keeps only the last that many
    sample lines (plus the headers)."""
    if lines:
        cmd = "grep -a energy-start %s 2>/dev/null; tail -n %d %s 2>/dev/null; true" % (FILE, int(lines), FILE)
    else:
        cmd = "cat %s 2>/dev/null; true" % FILE
    rc, out = ctx.remote.sh(systems.pattern(hosts), cmd, timeout=120)
    return {pc: parse(text) for pc, text in per_host(out, hosts).items()}


def fields(per_pc, ids, start_ns, end_ns, all_ids=None):
    """The energy fields of a run line, from {pc: parsed} samples: ids are
    the PCs the run used, all_ids every PC (for energy_all_pcs_j)."""
    if not start_ns or not end_ns:
        return {"energy_available": False, "energy_j": None}
    per, why = {}, {}
    for pc in all_ids or ids:
        j, reason = joules(per_pc.get(pc) or parse(""), start_ns, end_ns)
        per[pc] = j
        if j is None:
            why[pc] = reason
    used = [per.get(pc) for pc in ids]
    ok = all(j is not None for j in used)
    out = {"energy_available": ok, "energy_j": round(sum(used), 3) if ok else None,
           "energy_per_pc_j": per,
           "energy_source": next((st["source"] for st in per_pc.values() if st.get("source")), None)}
    if all_ids:
        every = list(per.values())
        out["energy_all_pcs_j"] = round(sum(every), 3) if all(j is not None for j in every) else None
    if why:
        out["energy_unavailable"] = why
    return out


def measure(ctx, hosts, start_ns, end_ns, all_hosts=None, settle_s=1.5):
    """Fetch the samples around [start_ns, end_ns] and return the line's
    energy fields. Waits settle_s first, so a sample after end_ns exists."""
    time.sleep(settle_s)
    lines = int((end_ns - start_ns) / 1e9) + 20 if start_ns and end_ns else 20
    per = fetch(ctx, all_hosts or hosts, lines=lines)
    return fields(per, [h.id for h in hosts], start_ns, end_ns, [h.id for h in all_hosts] if all_hosts else None)


def archive(ctx, hosts, dest):
    """Every host's whole energy.jsonl into dest/<pc>.jsonl, for analyse
    to put joules on every run of the day."""
    os.makedirs(dest, exist_ok=True)
    got = {}
    for h in hosts:
        rc, out = ctx.remote.sh(h.id, "cat %s 2>/dev/null; true" % FILE, timeout=300)
        with open(os.path.join(dest, "%s.jsonl" % h.id), "w") as f:
            f.write(out if out.endswith("\n") or not out else out + "\n")
        got[h.id] = len(parse(out)["samples"])
    ctx.log("energy samples archived to %s: %s" % (dest, ", ".join("%s %d" % kv for kv in sorted(got.items()))))
    return got
