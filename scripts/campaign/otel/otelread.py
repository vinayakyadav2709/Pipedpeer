#!/usr/bin/env python3
"""Read the driver collector's files in results/lab/otel/.

    otelread.py check <dir> [seconds]   counts per lab.pc_id and source (telemetry.sh check)
    otelread.py cat <file>...           one OTLP JSON document per line, to stdout

Two formats, by extension:
  *.zjsonl  the file exporter with compression: zstd. Repeated: a 4-byte
            big-endian length, then one zstd frame holding one OTLP JSON
            document (resourceMetrics or resourceLogs). Not a plain .zst file.
  *.jsonl   one OTLP JSON document per line (no compression; older runs).
The exporter rotates at 256 MB to <name>-<timestamp>.<ext> beside the live file.

Library use (analysis): `for doc in otelread.docs(path): ...`, or
`otelread.records(dir, "logs")` / `otelread.points(dir, "metrics")`, which
flatten to dicts with the resource attributes merged in.

zstd needs Python 3.14 (compression.zstd) or the zstandard package.
"""

import collections
import glob
import json
import os
import struct
import sys
import time

try:
    from compression import zstd as _zstd  # Python 3.14+

    def _unzstd(b):
        return _zstd.decompress(b)
except ImportError:  # pragma: no cover - depends on the interpreter
    try:
        import zstandard as _zs

        def _unzstd(b):
            return _zs.ZstdDecompressor().decompress(b, max_output_size=1 << 30)
    except ImportError:
        _unzstd = None


def frame_offsets(path):
    """(offset, length) of every frame in a .zjsonl file; a partial last frame is skipped."""
    out = []
    with open(path, "rb") as f:
        size = f.seek(0, 2)
        pos = 0
        while pos + 4 <= size:
            f.seek(pos)
            (n,) = struct.unpack(">I", f.read(4))
            if pos + 4 + n > size:
                break  # still being written
            out.append((pos + 4, n))
            pos += 4 + n
    return out


def docs(path, tail_bytes=None):
    """OTLP JSON documents in a file. tail_bytes: only about the last that many bytes."""
    if path.endswith(".zjsonl"):
        if _unzstd is None:
            raise SystemExit("reading .zjsonl needs Python 3.14 (compression.zstd) or the zstandard package")
        frames = frame_offsets(path)
        if tail_bytes is not None:
            keep, total = [], 0
            for off, n in reversed(frames):
                if total > tail_bytes:
                    break
                keep.append((off, n))
                total += n
            frames = list(reversed(keep))
        with open(path, "rb") as f:
            for off, n in frames:
                f.seek(off)
                try:
                    yield json.loads(_unzstd(f.read(n)))
                except ValueError:
                    pass
        return
    with open(path, "rb") as f:
        if tail_bytes is not None:
            size = f.seek(0, 2)
            f.seek(max(0, size - tail_bytes))
            if size > tail_bytes:
                f.readline()  # partial line
        for line in f:
            if line.strip():
                try:
                    yield json.loads(line)
                except ValueError:
                    pass  # a line still being written


def files(d, signal):
    """Every file of one signal ("metrics" or "logs"), oldest first."""
    found = glob.glob(os.path.join(d, f"{signal}*.zjsonl")) + glob.glob(os.path.join(d, f"{signal}*.jsonl"))
    return sorted(found, key=os.path.getmtime)


def attrs(obj):
    out = {}
    for a in obj.get("attributes", []) or []:
        v = a.get("value", {})
        out[a["key"]] = next(iter(v.values()), None) if v else None
    return out


KINDS = ("gauge", "sum", "histogram", "summary", "exponentialHistogram")


def points(d, signal="metrics", tail_bytes=None):
    """Every datapoint as {name, t_ns, value, resource attrs..., attrs}."""
    for path in files(d, signal):
        for doc in docs(path, tail_bytes):
            for rm in doc.get("resourceMetrics", []):
                res = attrs(rm.get("resource", {}))
                for sm in rm.get("scopeMetrics", []):
                    scope = sm.get("scope", {}).get("name", "")
                    for m in sm.get("metrics", []):
                        for kind in KINDS:
                            for dp in m.get(kind, {}).get("dataPoints", []):
                                yield dict(res, _scope=scope, name=m["name"], t_ns=int(dp.get("timeUnixNano", 0)),
                                           value=dp.get("asDouble", dp.get("asInt")), attrs=attrs(dp))


def records(d, signal="logs", tail_bytes=None):
    """Every log record as {t_ns, body, resource attrs..., attrs}."""
    for path in files(d, signal):
        for doc in docs(path, tail_bytes):
            for rl in doc.get("resourceLogs", []):
                res = attrs(rl.get("resource", {}))
                for sl in rl.get("scopeLogs", []):
                    for r in sl.get("logRecords", []):
                        t = int(r.get("timeUnixNano") or r.get("observedTimeUnixNano") or 0)
                        body = r.get("body", {})
                        yield dict(res, t_ns=t, observed_ns=int(r.get("observedTimeUnixNano") or 0),
                                   body=next(iter(body.values()), None) if body else None, attrs=attrs(r))


def metric_source(p):
    """host.<scraper> for host metrics, prom.<job> for scraped ones."""
    src = p.get("lab.source", "?")
    if src == "host-metrics":
        scope = p["_scope"].rsplit("/", 1)[-1]
        return "host." + scope.removesuffix("scraper")
    if src == "prometheus":
        return "prom." + str(p.get("service.name", "?"))
    return src


def log_source(r):
    src = r.get("lab.source") or f"({r.get('service.name', '?')})"
    kind = r["attrs"].get("lab.kind")
    return f"{src}/{kind}" if kind else src


def check(d, window):
    since = time.time_ns() - window * 10**9
    tail = 64 << 20  # the last minute is near the end; do not read gigabytes
    counts = collections.Counter()
    run_ids = collections.defaultdict(set)
    for p in points(d, "metrics", tail):
        if p["t_ns"] >= since:
            counts[(p.get("lab.pc_id", "(none)"), "metrics", metric_source(p))] += 1
    for r in records(d, "logs", tail):
        if (r["observed_ns"] or r["t_ns"]) >= since:
            who = r.get("lab.pc_id", "(none)")
            counts[(who, "logs", log_source(r))] += 1
            if r["attrs"].get("lab.run_id"):
                run_ids[who].add(r["attrs"]["lab.run_id"])
    print(f"last {window} s, from {d}")
    print(f"{'lab.pc_id':<14} {'signal':<8} {'source':<28} {'count':>9}")
    for (who, sig, src), n in sorted(counts.items()):
        print(f"{who:<14} {sig:<8} {src:<28} {n:>9}")
    for who, ids in sorted(run_ids.items()):
        print(f"{who:<14} run ids seen: {' '.join(sorted(ids)[:8])}{' ...' if len(ids) > 8 else ''}")
    if not counts:
        print("nothing arrived")
        return 1
    return 0


def main(argv):
    if len(argv) >= 2 and argv[0] == "check":
        return check(argv[1], int(argv[2]) if len(argv) > 2 else 60)
    if len(argv) >= 2 and argv[0] == "cat":
        for path in argv[1:]:
            for doc in docs(path):
                sys.stdout.write(json.dumps(doc, separators=(",", ":")) + "\n")
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
