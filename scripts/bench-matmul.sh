#!/usr/bin/env bash
# Does the matmul cost model choose the faster path?
#
# The shim ships A @ B to the cluster when its model says remote beats local.
# Until now the only check on that was a tripwire asserting it never ships,
# which cannot tell a correct refusal from a missed opportunity - and the model
# is refusing on a rule of thumb: it charges five times A's bytes for the round
# trip whatever B's shape, when what actually crosses is A, the product C, and
# B replicated to every worker. Those are the same number only for square
# operands.
#
# So: for each shape, run it both ways and score the choice. The measurement is
# the arbiter, not the model's own arithmetic.
#
#   scripts/bench-matmul.sh
#   BENCH_SHAPES="4096 4096 4096" scripts/bench-matmul.sh   # one shape
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
port="${PIPEDPEER_PORT:-38080}"

# Best-effort: an absent token is a normal answer. Reading it inline used to
# be a pipeline under `set -o pipefail`, so a binary that did not know the
# `auth` command killed the script here - exit 1, zero bytes of output, before
# the first echo.
lib="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/cli.sh"
[[ -f "$lib" ]] || { echo "missing $lib - copy scripts/ whole, not this file alone" >&2; exit 2; }
source "$lib"
pp_resolve_cli "$repo_root" || exit 2
cli="$PP_CLI"
pp_read_token

# "How many peers" and "could I ask" are different questions, and answering
# the second with 0 is how this benchmark once ran with no peer at all: the
# API call failed, the count came back empty, and the arithmetic test that was
# meant to stop the run produced a syntax error and let it through.
nodes_json="$(pp_curl -sf --max-time 5 "http://127.0.0.1:$port/v1/nodes" 2>/dev/null || true)"
if [[ -z "$nodes_json" ]]; then
	echo "FAIL: no answer from the daemon API on :$port. Not the same as having"
	echo "      no peers, and the difference decides whether these numbers mean"
	echo "      anything."
	exit 1
fi
peers="$(printf '%s' "$nodes_json" | python3 -c 'import json,sys
nodes = json.load(sys.stdin)
print(sum(1 for n in nodes if n.get("state") == "healthy" and n.get("source") != "self"))')"
if [[ "$peers" -lt 1 ]]; then
	echo "FAIL: no healthy peer. With none the model refuses everything and the"
	echo "      benchmark would score a decision that was never made."
	exit 1
fi
echo "peers: $peers"

work="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/bench-matmul"
rm -rf "$work"; mkdir -p "$work"; : > "$work/.pipedpeerignore"
# Beside this script normally; under scripts/ when run from a checkout.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
driver="$here/bench-matmul.py"
[[ -f "$driver" ]] || driver="$repo_root/scripts/bench-matmul.py"
[[ -f "$driver" ]] || { echo "cannot find bench-matmul.py beside $here" >&2; exit 2; }
cp "$driver" "$work/mm.py"

# Shapes chosen to separate the model's rule of thumb from what crosses:
#
#   square      A, B and C all the same size - the case the flat multiplier
#               was calibrated on.
#   tall-skinny A is large, B is tiny. Replicating B costs almost nothing, so
#               the true round trip is about 2x A, not 5x.
#   wide        A is small, B and C are large. Replicating B to every worker
#               is the whole cost, and it is not proportional to A at all.
#
# Sizes are bounded by what the machine can survive, not by what would show
# the effect best. Forcing an 8192-cube spill on a 14 GB box drove it to
# 194 MB free and the kernel killed the desktop: the submitting daemon and the
# worker are separate processes that each read the host's free memory and each
# concluded it had 11 GB to play with. On separate machines that reading is
# right; on one machine they oversubscribe it between them.
shapes="${BENCH_SHAPES_ALL:-"2048 2048 2048|4096 4096 4096|16384 1024 1024|1024 4096 4096"}"
[[ -n "${BENCH_SHAPES:-}" ]] && shapes="$BENCH_SHAPES"

# One shape per run: the decision is made once per call, and a merged log makes
# it guesswork which line belonged to which shape.
# The estimator sizes a job from its imports, which is a guess about a script
# it has not run. These shapes are known: A, B and C are held at once, the
# cluster path holds pickled copies of what it ships, and being killed at an
# 855 MB default is not a result about the cost model. So the size is stated.
mem_for() {
	# shellcheck disable=SC2086
	set -- $1
	python3 -c "
m, k, n = $1, $2, $3
arrays = (m * k + k * n + m * n) * 8
print('%dM' % max(1024, int(arrays * 2.5 / 1e6)))"
}

# fits reports whether forcing this shape is safe here. The forced path holds
# the three arrays plus the pickled payload, a compression attempt on it, and
# the returned blocks; measured at roughly eight times the arrays. Refusing to
# measure a shape is a worse benchmark than measuring it, and a far better one
# than an OOM that takes the machine's desktop with it.
fits() {
	# shellcheck disable=SC2086
	set -- $1
	local avail_mb
	avail_mb="$(free -m | awk '/^Mem:/{print $7}')"
	python3 -c "
m, k, n = $1, $2, $3
arrays = (m * k + k * n + m * n) * 8
factor = float('${BENCH_MEM_FACTOR:-8}')
need_mb = arrays * factor / 1e6
avail_mb = $avail_mb * 0.6
print('yes' if need_mb <= avail_mb else 'no %d %d' % (need_mb, avail_mb))"
}

one() {
	local shape="$1" mode="$2" log="$3"
	# shellcheck disable=SC2086
	(cd "$work" && "$cli" run --mem "$(mem_for "$shape")" --distribute "$mode" mm.py -- $shape) > "$log" 2>&1 ||
		{ echo "FAIL running $shape ($mode)"; tail -20 "$log"; exit 1; }
}

result_json() { grep -a '^BENCH ' "$1" | tail -1 | cut -d' ' -f2-; }
dispatched() { grep -aq 'matmul: sending' "$1" && echo yes || echo no; }

printf '\n%-22s %9s %8s %9s %9s %8s   %s\n' \
	"shape" "GFLOP" "local" "cluster" "chose" "regret" "verdict"

fail=0
IFS='|' read -ra shape_list <<< "$shapes"
for shape in "${shape_list[@]}"; do
	shape="$(echo "$shape" | xargs)"
	label="$(echo "$shape" | tr ' ' 'x')"

	auto_log="$work/$label.auto.log"
	force_log="$work/$label.force.log"

	verdict="$(fits "$shape")"
	if [[ "$verdict" != "yes" ]]; then
		read -r _ need avail <<< "$verdict"
		printf '%-22s %9s %8s %9s %9s %8s   not measured: needs ~%s MB, %s MB safely free\n' \
			"$label" "-" "-" "-" "-" "-" "$need" "$avail"
		continue
	fi

	one "$shape" auto "$auto_log"
	one "$shape" force "$force_log"

	auto_json="$(result_json "$auto_log")"
	force_json="$(result_json "$force_log")"
	[[ -n "$auto_json" && -n "$force_json" ]] || { echo "FAIL: $label produced no measurement"; exit 1; }

	line="$(AUTO="$auto_json" FORCE="$force_json" \
		CHOSE="$(dispatched "$auto_log")" LABEL="$label" python3 - <<'PY'
import json, os

auto = json.loads(os.environ["AUTO"])
force = json.loads(os.environ["FORCE"])
chose_cluster = os.environ["CHOSE"] == "yes"

if not auto["correct"] or not force["correct"]:
    print("%-22s  WRONG ANSWER - the offload did not compute A @ B" % os.environ["LABEL"])
    raise SystemExit(3)

# local: the untouched BLAS path, measured in-process both times; take the
# faster of the two readings, since either is the same computation.
local = min(auto["local_s"], force["local_s"])
# cluster: the forced run is the only honest reading of what shipping costs.
cluster = force["configured_s"]
# What the auto run actually spent is what the user would have paid.
paid = auto["configured_s"]

best = min(local, cluster)
regret = paid / best - 1.0
chose = "cluster" if chose_cluster else "local"
right = (cluster < local) == chose_cluster

# A choice is only wrong if the other path was meaningfully faster. Below
# that the two are the same answer and picking either is defensible.
MARGIN = 0.15
if not right and abs(cluster - local) / best <= MARGIN:
    right, note = True, "tie"
else:
    note = "ok" if right else "MISSED" if not chose_cluster else "SHOULD NOT HAVE SHIPPED"

print("%-22s %9.1f %8.2fs %9.2fs %9s %7.0f%%   %s" % (
    os.environ["LABEL"], auto["gflop"], local, cluster, chose, 100 * regret, note))
raise SystemExit(0 if right else 1)
PY
	)" || fail=1
	echo "$line"
done

echo
if (( fail )); then
	echo "The model chose the slower path on at least one shape. That is the"
	echo "D2 promise - never slower than local - not holding, or an offload"
	echo "worth taking that it refused."
	echo
	echo "logs in $work"
	exit 1
fi
echo "the model picked the faster path on every shape measured"
echo
echo "logs in $work"
