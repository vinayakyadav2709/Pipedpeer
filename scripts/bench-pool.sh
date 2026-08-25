#!/usr/bin/env bash
# Hand-tuned parallelism versus leaving the code alone.
#
# The pitch is that a user should not have to pick a worker count, split their
# data, or think about which machine has spare memory. That is only worth
# claiming if the untouched script is at least as fast as the version they
# would have tuned themselves, so this measures both and prints the receipt
# that says where the work actually ran.
#
# Three columns:
#   plain      stock python3, multiprocessing.Pool(cpu_count) - what you write today
#   tuned      the same work, hand-split into one contiguous slice per worker
#   pipedpeer  the identical untouched script, run through the cluster
#
# Results are only meaningful with peers: with none, pipedpeer should match
# plain rather than beat it, and the receipt will say remote_items 0.
set -euo pipefail

cd "$(dirname "$0")/.."
repo_root="$(pwd)"
cli="$repo_root/bin/pipedpeer"
items="${BENCH_ITEMS:-48}"
spin="${BENCH_SPIN:-120000}"

command -v python3 >/dev/null || { echo "python3 required"; exit 1; }
[[ -x "$cli" ]] || "$repo_root/scripts/build.sh"

state_dir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$state_dir"
workdir="$(mktemp -d "$state_dir/bench-pool.XXXXXX")"
trap 'rm -rf "$workdir"' EXIT
: > "$workdir/.pipedpeerignore"

# One kernel, three drivers. The kernel is defined in each script's __main__
# on purpose: that is the shape real scripts have, and the shape that could
# never be shipped before kernels travelled as source.
kernel='
def work(x):
    total = 0
    for i in range(SPIN):
        total += (i ^ x) % 7
    return total
'

cat > "$workdir/plain.py" <<EOF
import multiprocessing, os, time
SPIN = $spin
$kernel

if __name__ == "__main__":
    items = list(range($items))
    t0 = time.time()
    with multiprocessing.Pool(os.cpu_count()) as pool:
        out = pool.map(work, items)
    print("%.2f %d" % (time.time() - t0, sum(out)))
EOF

# What a careful user writes by hand: one contiguous slice per worker, sized
# to the local core count, reduced at the end.
cat > "$workdir/tuned.py" <<EOF
import multiprocessing, os, time
SPIN = $spin
$kernel

def run_slice(chunk):
    return [work(x) for x in chunk]

if __name__ == "__main__":
    items = list(range($items))
    n = os.cpu_count()
    size = (len(items) + n - 1) // n
    slices = [items[i:i + size] for i in range(0, len(items), size)]
    t0 = time.time()
    with multiprocessing.Pool(n) as pool:
        out = [v for part in pool.map(run_slice, slices) for v in part]
    print("%.2f %d" % (time.time() - t0, sum(out)))
EOF

cp "$workdir/plain.py" "$workdir/auto.py"

run_plain() { python3 "$1" ; }

echo "workload: $items items x $spin spins"
echo

read -r plain_t plain_sum < <(run_plain "$workdir/plain.py")
echo "plain      ${plain_t}s"

read -r tuned_t tuned_sum < <(run_plain "$workdir/tuned.py")
echo "tuned      ${tuned_t}s"

if [[ "$tuned_sum" != "$plain_sum" ]]; then
	echo "FAIL: tuned baseline computed a different answer ($tuned_sum vs $plain_sum)"
	exit 1
fi

auto_out="$workdir/auto.log"
if ! "$cli" run "$workdir/auto.py" -e PIPEDPEER_RECEIPT=receipt.json > "$auto_out" 2>&1; then
	echo "FAIL: pipedpeer run failed"
	tail -20 "$auto_out"
	exit 1
fi

auto_line="$(grep -Eo '^[0-9]+\.[0-9]+ [0-9]+$' "$auto_out" | tail -1)"
if [[ -z "$auto_line" ]]; then
	echo "FAIL: no timing line in pipedpeer output"
	tail -20 "$auto_out"
	exit 1
fi
read -r auto_t auto_sum <<< "$auto_line"
echo "pipedpeer  ${auto_t}s"

if [[ "$auto_sum" != "$plain_sum" ]]; then
	echo "FAIL: pipedpeer computed a different answer ($auto_sum vs $plain_sum)"
	exit 1
fi

echo
if [[ -f "$workdir/receipt.json" ]]; then
	python3 - "$workdir/receipt.json" <<'PYEOF'
import json, sys
r = json.load(open(sys.argv[1]))
print("receipt: %d items on the cluster, %d local, %d failure(s), %d declined" % (
    r.get("remote_items", 0), r.get("local_items", 0),
    r.get("remote_failures", 0), r.get("unshippable", 0)))
PYEOF
else
	echo "receipt: none returned"
fi

# The never-slower invariant is the one worth guarding. Beating the tuned
# baseline needs peers and is not asserted here; losing badly to plain python
# means interception cost more than it saved, which is a regression whether or
# not a cluster was available.
python3 - "$plain_t" "$auto_t" <<'PYEOF'
import sys
plain, auto = float(sys.argv[1]), float(sys.argv[2])
budget = plain * 3
print()
if auto > budget:
    print("FAIL: %.2fs against a %.2fs plain baseline (>3x)" % (auto, budget / 3))
    sys.exit(1)
print("PASS: %.2fs vs %.2fs plain (within the 3x never-slower budget)" % (auto, plain))
PYEOF
