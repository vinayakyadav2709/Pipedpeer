#!/usr/bin/env bash
# clocks.sh [--exec '<prefix>'] [host...]: each lab PC's clock offset from
# this machine (the driver), flagged over 10 ms, written to
# results/lab/clocks.txt.
#
# Hosts: the arguments, else results/lab/inventory.ini (groups head and
# workers; connects to ansible_host, as ansible_user when set). Each host is
# reached as `<prefix> <host> <command>`; the prefix defaults to
# `ssh -o BatchMode=yes -o ConnectTimeout=5`. For containers:
#   clocks.sh --exec 'docker exec -i' pipedpeer-lab-otel-1 pipedpeer-lab-otel-2
#
# The offset is measured, not read from NTP: over one open connection the
# driver sends a line, the PC answers with its clock, the driver notes send
# and receive times; offset = remote - midpoint, uncertainty half the round
# trip, best of 10 rounds. chronyc tracking (or timedatectl) on each PC is
# recorded beside it, to show what keeps the clock in sync.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
inventory="$repo/results/lab/inventory.ini"
out="$repo/results/lab/clocks.txt"
prefix="ssh -o BatchMode=yes -o ConnectTimeout=5"
limit_ms="${CLOCK_LIMIT_MS:-10}"

if [[ "${1:-}" == --exec ]]; then
	prefix="$2"
	shift 2
fi

# label<TAB>target per host
hosts=()
if (($#)); then
	for h in "$@"; do hosts+=("$h	$h"); done
elif [[ -f "$inventory" ]]; then
	while IFS= read -r line; do hosts+=("$line"); done < <(awk '
		/^\[/ { g = $0; gsub(/[][]/, "", g); next }
		/^[[:space:]]*($|[#;])/ { next }
		g == "head" || g == "workers" {
			name = $1; host = $1; user = ""
			for (i = 2; i <= NF; i++) {
				if ($i ~ /^ansible_host=/) host = substr($i, 14)
				if ($i ~ /^ansible_user=/) user = substr($i, 14)
			}
			if (user != "") host = user "@" host
			print name "\t" host
		}' "$inventory")
else
	echo "usage: $0 [--exec '<prefix>'] host... (or create $inventory)" >&2
	exit 2
fi
((${#hosts[@]})) || { echo "no hosts in $inventory (groups head, workers)" >&2; exit 2; }

mkdir -p "$(dirname "$out")"
python3 -I - "$prefix" "$limit_ms" "$out" "${hosts[@]}" <<'PY'
import concurrent.futures, shlex, subprocess, sys, time

prefix, limit_ms, out, hosts = shlex.split(sys.argv[1]), float(sys.argv[2]), sys.argv[3], sys.argv[4:]
SYNC = "chronyc tracking 2>/dev/null || timedatectl show 2>/dev/null || timedatectl status 2>/dev/null || echo 'no chronyc or timedatectl'"
ECHO = 'while read -r _; do date +%s%N; done'


def remote(target, cmd):
    # ssh joins its arguments into one remote shell line; docker exec does not.
    argv = ["sh", "-c", cmd]
    return prefix + [target] + ([shlex.join(argv)] if prefix[0].endswith("ssh") else argv)


def sync_summary(text):
    keep = ("Reference ID", "System time", "Last offset", "Leap status",
            "NTPSynchronized", "NTP service", "System clock synchronized", "no chronyc")
    return "; ".join(" ".join(l.split()) for l in text.splitlines() if l.strip().startswith(keep)) or text.strip()[:120]


def measure(entry):
    label, target = entry.split("\t", 1)
    try:
        p = subprocess.Popen(remote(target, ECHO), stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        best = None
        for _ in range(10):
            t0 = time.time_ns()
            p.stdin.write("x\n")
            p.stdin.flush()
            line = p.stdout.readline()
            t1 = time.time_ns()
            if not line.strip().isdigit():
                raise RuntimeError((line + p.stderr.read()).strip() or "no answer")
            rtt = t1 - t0
            off = int(line) - (t0 + t1) // 2
            if best is None or rtt < best[1]:
                best = (off, rtt)
        p.stdin.close()
        p.wait(timeout=10)
        sync = subprocess.run(remote(target, SYNC), capture_output=True, text=True, timeout=30).stdout
        return label, best[0] / 1e6, best[1] / 2e6, sync_summary(sync), None
    except Exception as e:  # one unreachable PC must not hide the others
        return label, None, None, "", str(e).splitlines()[0] if str(e) else type(e).__name__


with concurrent.futures.ThreadPoolExecutor(16) as pool:
    rows = list(pool.map(measure, hosts))

driver_sync = sync_summary(subprocess.run(["sh", "-c", SYNC], capture_output=True, text=True).stdout)
lines = [f"# clocks.sh {time.strftime('%Y-%m-%dT%H:%M:%S%z')}; offset = PC clock - driver clock; limit {limit_ms:g} ms",
         f"# driver sync: {driver_sync}",
         f"{'pc':<24} {'offset_ms':>10} {'+-ms':>7}  status  sync"]
bad = 0
for label, off, err, sync, fail in rows:
    if fail:
        bad += 1
        lines.append(f"{label:<24} {'-':>10} {'-':>7}  ERROR   {fail}")
        continue
    flag = "OVER" if abs(off) > limit_ms else "ok"
    bad += flag != "ok"
    lines.append(f"{label:<24} {off:>10.3f} {err:>7.3f}  {flag:<6}  {sync}")
lines.append(f"# {len(rows) - bad}/{len(rows)} within {limit_ms:g} ms" + ("" if not bad else "; fix the others with chrony (point it at the driver or a common NTP server), then rerun"))
text = "\n".join(lines) + "\n"
open(out, "w").write(text)
sys.stdout.write(text)
print(f"wrote {out}")
sys.exit(1 if bad else 0)
PY
