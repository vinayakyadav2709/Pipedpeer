#!/usr/bin/env bash
# Does an uneven ring split the work by what each rank can actually do?
#
# With equal batches a step costs what the slowest rank costs, so a ring of
# unlike machines runs at its worst member's pace and the fast ones idle.
# Measured shares fix that by giving each rank a shard and a batch sized to
# what it was measured at — but only if two invariants hold, and both fail
# silently:
#
#   the shards must be disjoint and cover all but a remainder smaller than one
#     global batch — a sample trained on twice per epoch is a quietly different
#     dataset and the loss still looks fine, while the sub-batch remainder is
#     what drop_last discards in any DDP setup;
#   every rank must run the SAME NUMBER of steps — each step ends at a
#     barrier, so a rank with three times the data and the same batch size
#     runs three times the steps and the others wait at a sync that never
#     completes.
#
# This asserts both from the run's own receipts, rather than trusting that a
# run which finished must have been correct.
#
#   scripts/bench-ddp-hetero.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cli="${PIPEDPEER:-$repo_root/bin/pipedpeer}"
port="${PIPEDPEER_PORT:-38080}"
ranks="${RANKS:-3}"

[[ -x "$cli" ]] || { echo "no binary at $cli" >&2; exit 2; }

tok="$("$cli" auth show 2>/dev/null | head -1)"
case "$tok" in *"no token set"*) tok="" ;; esac
pp_curl() {
	if [[ -n "$tok" ]]; then curl -H "X-Pipedpeer-Token: $tok" "$@"; else curl "$@"; fi
}

nodes_json="$(pp_curl -sf --max-time 5 "http://127.0.0.1:$port/v1/nodes" 2>/dev/null || true)"
[[ -n "$nodes_json" ]] || { echo "FAIL: no answer from the daemon API on :$port"; exit 1; }
healthy="$(printf '%s' "$nodes_json" | python3 -c 'import json,sys
print(sum(1 for n in json.load(sys.stdin) if n.get("state") == "healthy"))')"
if [[ "$healthy" -lt "$ranks" ]]; then
	echo "FAIL: $healthy healthy node(s), need $ranks. This measures how an"
	echo "      uneven ring divides work; with fewer nodes there is no ring."
	exit 1
fi

driver="$here/bench-ddp-hetero.py"
[[ -f "$driver" ]] || driver="$repo_root/scripts/bench-ddp-hetero.py"
[[ -f "$driver" ]] || { echo "cannot find bench-ddp-hetero.py" >&2; exit 2; }

work="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/bench-ddp-hetero"
rm -rf "$work"; mkdir -p "$work"; : > "$work/.pipedpeerignore"
cp "$driver" "$work/train.py"

# The control: the same ring with every rank given the same batch, which is
# what it did before shares existed. Without it "shares made this faster" is
# an assertion with nothing to stand against.
control_log="$work/equal.log"
echo "running $ranks ranks, equal batches (control) ..."
cstart="$(date +%s.%N)"
(cd "$work" && PIPEDPEER_DDP_EQUAL_BATCHES=1 "$cli" run --ddp "$ranks" --gpu off train.py) \
	> "$control_log" 2>&1 || {
	echo "FAIL: the control run did not finish"; tail -25 "$control_log"; exit 1; }
cend="$(date +%s.%N)"
control_secs="$(echo "$cend - $cstart" | bc)"

log="$work/run.log"
echo "running $ranks ranks, measured shares ..."
start="$(date +%s.%N)"
(cd "$work" && "$cli" run --ddp "$ranks" --gpu off train.py) > "$log" 2>&1 || {
	echo "FAIL: the run did not finish"; tail -25 "$log"; exit 1; }
end="$(date +%s.%N)"

echo
grep -a "^shares:" "$log" || echo "(shares were even — nothing to reweigh)"
echo

# The shim logs one line per rank naming its shard and batch. That is the
# receipt: it is what the rank actually did, not what was planned for it.
python3 - "$log" "$(echo "$end - $start" | bc)" "$control_secs" <<'PY'
import re, sys

log = open(sys.argv[1], errors="replace").read()
elapsed = float(sys.argv[2])
control = float(sys.argv[3])

rows = re.findall(
    r"rank (\d+) takes (\d+) of (\d+) samples \(share (\d+)%\), batch (\d+)", log)
if not rows:
    if "measured unequal shares" in log:
        print("FAIL: shares were measured but the script splits its own data,")
        print("      so they were never applied. This benchmark needs a script")
        print("      whose DataLoader the shim can reshard.")
        raise SystemExit(1)
    print("FAIL: no rank reported its shard. Either the ring was even, or the")
    print("      weighted path did not run — both mean nothing was measured.")
    raise SystemExit(1)

rows = sorted({int(r[0]): r for r in rows}.values(), key=lambda r: int(r[0]))
total = int(rows[0][2])

print("%-8s %10s %8s %10s %8s" % ("rank", "samples", "share", "batch", "steps"))
shard_sum = 0
steps = []
global_batch = 0
for rank, took, whole, share, batch in rows:
    took, batch = int(took), int(batch)
    shard_sum += took
    global_batch += batch
    per_epoch = took // batch
    steps.append(per_epoch)
    print("%-8s %10d %7s%% %10d %8d" % (rank, took, share, batch, per_epoch))

fail = False

# Invariant 1: the shards are disjoint and cover the dataset bar a remainder
# below one global batch — which is exactly what drop_last discards, and which
# shuffling makes a different remainder each epoch.
dropped = total - shard_sum
if dropped < 0:
    print()
    print("FAIL: the shards cover %d of %d samples — %d more than exist, so"
          % (shard_sum, total, -dropped))
    print("      samples are being trained on twice per epoch and the loss")
    print("      would look reasonable anyway.")
    fail = True
elif dropped >= global_batch:
    print()
    print("FAIL: %d of %d samples go untrained, which is more than the one"
          % (dropped, total))
    print("      global batch of %d that drop_last accounts for." % global_batch)
    fail = True

# Invariant 2: equal step counts, or the ring deadlocks at a barrier.
if len(set(steps)) != 1:
    print()
    print("FAIL: ranks run %s steps per epoch." % steps)
    print("      Every step ends at a barrier, so unequal counts mean the ranks")
    print("      that finish early wait forever on ranks that are still going.")
    print("      Shard and batch must scale together, not shard alone.")
    fail = True

# Worth reporting even when it passes: this is the number the feature exists
# for, and a share that matches the hardware is the evidence it was measured
# rather than assumed.
if not fail:
    spread = max(int(r[1]) for r in rows) / min(int(r[1]) for r in rows)
    print()
    print("shards are disjoint and cover %d of %d samples (%d dropped, under one "
          "global batch of %d); every rank runs %d steps an epoch"
          % (shard_sum, total, dropped, global_batch, steps[0]))
    print("fastest rank took %.1fx the slowest rank's share" % spread)
    print()
    print("%-22s %8.1fs" % ("equal batches", control))
    print("%-22s %8.1fs" % ("measured shares", elapsed))
    if elapsed > control:
        print()
        print("FAIL: weighting the shares made this ring SLOWER (%.1fs against"
              % elapsed)
        print("      %.1fs). Giving a slow rank less work is meant to stop it"
              % control)
        print("      setting the pace; if it does not, the shares are wrong or")
        print("      the run is dominated by something else - the sync-bound")
        print("      line in the log says which.")
        raise SystemExit(1)
    print("shares beat equal batches by %.0f%%" % (100 * (control - elapsed) / control))

raise SystemExit(1 if fail else 0)
PY
rc=$?

echo
echo "logs in $work"
exit $rc
