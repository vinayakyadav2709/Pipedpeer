#!/usr/bin/env bash
# run.sh (brief S9): the campaign matrix driver.
#
#   run.sh <experiment> [--sizes 1,2] [--repeats k] [--workloads W1,..]
#                       [--systems ..] [--dry-run]
#
# Walks the experiment's rows in experiments.tsv: for every (workload, N) it
# brings the cluster up, starts host metrics, runs the warm-up and then the
# shuffled repeats, fetching each run's outcome into runs.jsonl (one JSON
# line, Contracts). Repeats get a seeded shuffle (<exp>-<w>-<n>-<rep>) so a
# resumed campaign reproduces its order; a run_id already in runs.jsonl is
# skipped, so Ctrl-C mid-campaign resumes cleanly. A run that times out is
# recorded with exit_code 124 / note "timeout" and the campaign moves on.
#
# run.sh re-executes itself under systemd-inhibit so a laptop sleep cannot
# kill a night of runs (LAB_INHIBITED=1 guards the loop). cluster.sh (S7),
# metrics.sh and upload.sh (S10) are siblings; tests point CLUSTER_SH,
# METRICS_SH and UPLOAD_SH at stubs.
#
# Sourced without executing: campaign_run_one is the per-run function
# e4/e5/e6 reuse (it only records; it does not call cluster/metrics), and
# run_shuffle is the repeat-order shuffle.
#
# E8's repeats are seeds: run.sh exports BENCH_SEED = 10 + repeat (11,12,13)
# for them; every other experiment leaves the seed alone.

# shellcheck disable=SC2034  # some locals are assigned and read only inside helpers/tests
set -euo pipefail

REPO="${REPO:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}"
# shellcheck source=/dev/null
source "$REPO/scripts/campaign/lib.sh"
# shellcheck source=/dev/null
source "$REPO/scripts/campaign/systems.sh"

EXPERIMENTS="$REPO/scripts/campaign/experiments.tsv"
CLUSTER_SH="${CLUSTER_SH:-$REPO/scripts/campaign/cluster.sh}"
METRICS_SH="${METRICS_SH:-$REPO/scripts/campaign/metrics.sh}"
UPLOAD_SH="${UPLOAD_SH:-$REPO/scripts/campaign/upload.sh}"
# The size spec "scaling" as the comma list the size parsing splits on.
SCALING_SIZES="1,2,4,8,16,24,32"

RUNS_DONE=0
RUNS_SKIPPED=0

run_usage() {
	cat <<'EOF'
usage: run.sh <experiment> [--sizes 1,2] [--repeats k] [--workloads W1,..]
                           [--systems ..] [--dry-run]

Runs one experiment from scripts/campaign/experiments.tsv on the lab. Needs
$LAB/sizes.env (calibrate.sh) and the scan/install/venv steps before it.
Environment: LAB_RESULTS (results dir, default <repo>/results/lab);
CLUSTER_SH / METRICS_SH / UPLOAD_SH (sibling scripts, stubbable in tests).
EOF
}

# _load_sizes: sizes.env exists and exports its names.
_load_sizes() {
	if [[ ! -s "$LAB/sizes.env" ]]; then
		lab_log "no $LAB/sizes.env: run calibrate.sh first"
		echo "no $LAB/sizes.env: run calibrate.sh first" >&2
		return 1
	fi
	set -a
	# shellcheck source=/dev/null
	. "$LAB/sizes.env"
	set +a
}

# run_shuffle <seed> <system...>: the repeat's system order.
run_shuffle() {
	local seed="$1"
	shift
	# shellcheck disable=SC2015  # python3 is on every lab machine
	python3 -c "import random,sys; r=random.Random(sys.argv[1]); s=sys.argv[2:]; r.shuffle(s); print(*s)" \
		"$seed" "$@"
}

# _run_id <exp> <w> <system> <n> <repeat> <warmup 0|1>
_run_id() {
	if [[ "$6" == 1 ]]; then
		printf '%s-%s-%s-n%s-r0\n' "$1" "$2" "$3" "$4"
	else
		printf '%s-%s-%s-n%s-r%s\n' "$1" "$2" "$3" "$4" "$5"
	fi
}

_run_id_in_runs() {
	grep -qF -- "\"run_id\":\"$1\"" "$LAB/runs.jsonl" 2>/dev/null
}

# _get_version: the daemon version on the submit PC (the driver has no daemon).
# `)` ends the field. A failure is logged and leaves it empty.
_get_version() {
	local pc="${CAMPAIGN_SUBMIT_PC:-pc00}" v rc=0
	v="$(lab_ssh "$pc" "pipedpeer status 2>/dev/null | head -1 | grep -o 'version=[^ )]*' | cut -d= -f2")" || rc=$?
	if ((rc != 0)); then
		lab_log "version: pipedpeer status on $pc failed (exit $rc); the version stays empty"
		return 0
	fi
	printf '%s\n' "$v"
}

# _run_size_json <w> <n>: the size object of the runs.jsonl line.
_run_size_json() {
	local w="$1" n="$2" items ms payload params
	case "$w" in
		W1 | idle)
			if [[ "${BENCH_WEAK:-}" == 1 ]]; then
				items=$((W1_WEAK_PER_PC * n))
			else
				items="${W1_ITEMS:?W1_ITEMS not in sizes.env}"
			fi
			printf '{"BENCH_ITEMS":%s}' "$items"
			;;
		W2) printf '{"W2_ITEMS":%s}' "${W2_ITEMS:?W2_ITEMS not in sizes.env}" ;;
		W3) printf '{"BENCH_TREES":%s}' "${W3_TREES:?W3_TREES not in sizes.env}" ;;
		T1) printf '{"BENCH_EPOCHS":%s}' "${T1_EPOCHS:?T1_EPOCHS not in sizes.env}" ;;
		trivial) printf '{}' ;;
		TS1 | TS10 | TS100 | TS1000 | DS10K | DS1M | DS20M | SB | EL)
			params="$(_sys_sweep_params "$w")" || return 1
			read -r items ms payload <<<"$params"
			printf '{"items":%s,"task_ms":%s,"payload_bytes":%s}' "$items" "$ms" "$payload"
			;;
		noop) printf '{"items":%s}' "${NOOP_ITEMS:?NOOP_ITEMS not in sizes.env}" ;;
		lat) printf '{"calls":%s}' "${LAT_CALLS:?LAT_CALLS not in sizes.env}" ;;
		fileio) printf '{"files":%s,"file_kb":%s}' "${FILEIO_FILES:?FILEIO_FILES not in sizes.env}" "${FILEIO_FILE_KB:?FILEIO_FILE_KB not in sizes.env}" ;;
		*) echo "run: unknown workload $w" >&2; return 1 ;;
	esac
}

# _run_record <run_id> <exp> <w> <system> <n> <repeat> <warmup> <rc> <note> <hosts joined>
# Reads the fetched run dir, judges the result against the reference (rules
# in workloads.md), appends the contract's runs.jsonl line (python
# json.dumps, never string concatenation) and prints
# `RESULT <run_id> ok=<bool|null> remote_items=<k|->`.
# PYRUN_MODE chooses the reference (brief sys_sizekey); empty means strong.
_run_record() {
	local run_id="$1" exp="$2" w="$3" sys="$4" n="$5" rep="$6" warm="$7"
	local rc="$8" note="$9" hosts="${10}"
	local dir="$LAB/hosts/${CAMPAIGN_SUBMIT_PC:-pc00}/runs/$run_id"
	local sizekey="strong" reffile="" refout=""
	[[ -n "${PYRUN_MODE:-}" ]] && sizekey="$PYRUN_MODE"
	if [[ "$sizekey" == weak || "$sizekey" == n* ]]; then
		sizekey="n$n"   # weak sizes have their own per-N references (majority pass)
	fi
	# trivial, noop, lat, fileio and EL have no reference file: their verdict is
	# the result line count (or none at all for EL).
	case "$w" in trivial | noop | lat | fileio | EL) ;; *)
		reffile="$LAB/reference/${w}-${sizekey}.txt"
		refout="$reffile"   # a successful REF run writes this file (below)
		[[ -s "$reffile" ]] || reffile=""
		;;
	esac
	mkdir -p "$(dirname "$LAB/runs.jsonl")"
	PYRUN_RUN_ID="$run_id" PYRUN_EXP="$exp" PYRUN_WORKLOAD="$w" PYRUN_SYSTEM="$sys" \
		PYRUN_N="$n" PYRUN_REPEAT="$rep" PYRUN_WARMUP="$warm" PYRUN_RC="$rc" \
		PYRUN_NOTE="$note" PYRUN_DIR="$dir" PYRUN_REFFILE="$reffile" PYRUN_REFOUT="$refout" \
		PYRUN_HOSTS="$hosts" PYRUN_RUNSLOG="$LAB/runs.jsonl" \
		PYRUN_SYSTEMS_REGEX="$(sys_result_regex "$w")" \
		PYRUN_SIZEJSON="$(_run_size_json "$w" "$n")" \
		python3 - <<'RUNREC_PY'
import json
import os
import re
import sys

env = os.environ
d = env["PYRUN_DIR"]


def num(path, default=0):
    try:
        with open(path) as fh:
            return int(fh.read().strip())
    except (OSError, ValueError):
        return default


def maybe_int(path):
    """The integer in the file, or None when the file is missing or garbled."""
    try:
        with open(path) as fh:
            return int(fh.read().strip())
    except (OSError, ValueError):
        return None


def maybe_json(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


t0, t1 = num(os.path.join(d, "t0")), num(os.path.join(d, "t1"))
# The exit file is the truth. Without it the code is unknown (None -> null),
# except a timeout, where the driver's own 124 is the answer. Never 0.
exit_code = maybe_int(os.path.join(d, "exit"))
if exit_code is None and env["PYRUN_RC"].strip() == "124":
    exit_code = 124

out = os.path.join(d, "out")
texts = []
if os.path.isfile(out):
    with open(out, encoding="utf-8") as fh:
        texts = [l.rstrip("\n") for l in fh if l.strip()]
rx = re.compile(env["PYRUN_SYSTEMS_REGEX"])
result_lines = [l for l in texts if rx.match(l)]

ref = "none"
result_ok = None
w = env["PYRUN_WORKLOAD"]
reffile = env["PYRUN_REFFILE"]


def tail(line):
    return float(line.rsplit(" ", 1)[1])


if w == "trivial":
    result_ok = any(l.strip() == "TRIVIAL-OK" for l in result_lines)
elif w in ("noop", "lat", "fileio"):
    result_ok = len(result_lines) == 1
elif w == "EL":
    result_ok = None  # no reference: the join and the wall time are the finding
elif reffile:
    ref = "reference"
    with open(reffile, encoding="utf-8") as fh:
        ref_lines = [l.rstrip("\n") for l in fh if l.strip()]
    if (w in ("W1", "idle") or w.startswith(("TS", "DS")) or w == "SB") and ref_lines and result_lines:
        def _ck(s):
            for tok in s.split():
                if tok.startswith("checksum="):
                    return tok
            return None
        result_ok = _ck(ref_lines[0]) is not None and _ck(ref_lines[0]) == _ck(result_lines[0])
    elif w == "W2":
        result_ok = len(result_lines) == 4 and sorted(result_lines) == sorted(ref_lines)
    elif w == "W3" and ref_lines and result_lines:
        result_ok = ref_lines[0].strip() == result_lines[0].strip()
        if not result_ok and env["PYRUN_SYSTEM"] == "ssh":
            # workloads.md: the ssh port trains disjoint tree subsets, so its
            # accuracy may differ by up to 0.005 (the others stay exact)
            try:
                result_ok = abs(tail(ref_lines[0]) - tail(result_lines[0])) <= 0.005 + 1e-9
            except (ValueError, IndexError):
                result_ok = False
    elif w == "T1":
        fl = [l for l in ref_lines if l.startswith("final loss")]
        fg = [l for l in result_lines if l.startswith("final loss")]
        base = tail(fl[0]) if fl else None
        mine = tail(fg[0]) if fg else None
        result_ok = base is not None and mine is not None and \
            abs(mine - base) <= 0.01 * abs(base)

# REF is the plain single-node run: its result lines become the reference
# every later run is judged against (workloads.md). Never overwritten, and
# never taken from a failed run.
refout = env.get("PYRUN_REFOUT", "")
if env["PYRUN_EXP"] == "REF" and refout and not reffile and exit_code == 0 \
        and result_lines and w not in ("trivial", "noop", "lat", "fileio", "EL"):
    os.makedirs(os.path.dirname(refout), exist_ok=True)
    with open(refout, "w", encoding="utf-8") as fh:
        fh.write("\n".join(result_lines) + "\n")
    sys.stderr.write("reference written: %s\n" % refout)

if exit_code != 0:
    result_ok = False  # a job that failed or whose end is unknown is not a result

receipt = maybe_json(os.path.join(d, "receipt.json"))
remote_items = receipt.get("remote_items") if isinstance(receipt, dict) else None

rec = {
    "run_id": env["PYRUN_RUN_ID"],
    "experiment": env["PYRUN_EXP"],
    "workload": w,
    "system": env["PYRUN_SYSTEM"],
    "n": int(env["PYRUN_N"] or 0),
    "repeat": int(env["PYRUN_REPEAT"] or 0),
    "warmup": env["PYRUN_WARMUP"] == "1",
    "size": json.loads(env.get("PYRUN_SIZEJSON") or "{}"),
    "start_ns": t0,
    "end_ns": t1,
    "first_out_ns": num(os.path.join(d, "first_out_ns")),
    "wall_s": round((t1 - t0) / 1e9, 3) if t0 and t1 else None,
    "exit_code": exit_code,
    "note": env["PYRUN_NOTE"],
    "result_lines": result_lines,
    "result_ok": result_ok,
    "ref": ref,
    "hosts": env["PYRUN_HOSTS"].split(),
    "git_commit": env.get("PYRUN_GIT_COMMIT") or None,
    "pipedpeer_version": env.get("PIPEDPEER_VERSION") or None,
    "receipt": receipt,
    "timeline": maybe_json(os.path.join(d, "timeline.json")),
}
with open(env["PYRUN_RUNSLOG"], "a") as fh:
    fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
oktxt = "null" if result_ok is None else ("true" if result_ok else "false")
ritxt = "-" if remote_items is None else str(remote_items)
print("RESULT %s ok=%s remote_items=%s" % (env["PYRUN_RUN_ID"], oktxt, ritxt))
sys.stderr.write("recorded %s ok=%s ref=%s\n" % (env["PYRUN_RUN_ID"], oktxt, ref))
RUNREC_PY
}

# campaign_run_one <exp> <w> <system> <n> <repeat> <warmup 0|1> [timeout_s] [mode]
# One run: detach on the submit PC (CAMPAIGN_SUBMIT_PC, default pc00), wait,
# fetch, record. A timeout stops the run's unit and returns 124, a transport
# failure its own code; every completed run yields a runs.jsonl line. Default
# timeout: CAMPAIGN_DEFAULT_TIMEOUT_S or 3600. Needs sizes.env loaded (run_all does it; load it
# before calling this directly) and $LAB/hosts.tsv from scan.sh.
campaign_run_one() {
	local exp="$1" w="$2" sys="$3" n="$4" rep="$5" warm="$6"
	local timeout_s="${7:-${CAMPAIGN_DEFAULT_TIMEOUT_S:-3600}}"
	local pc="${CAMPAIGN_SUBMIT_PC:-pc00}"
	PYRUN_MODE="${8:-strong}"
	_load_sizes >/dev/null 2>&1 || true
	local run_id hfile dir rc=0 code cmd prefix envs rec_rc="" note=""
	local -a hosts=()

	run_id="$(_run_id "$exp" "$w" "$sys" "$n" "$rep" "$warm")"
	if _run_id_in_runs "$run_id"; then
		printf 'skipped %s\n' "$run_id"
		return 0
	fi

	hfile="$(lab_hosts "$n")" || return 1
	mapfile -t hosts <"$hfile"

	envs="$(_sys_sizeenv "$w" "$n")"
	prefix=""
	if [[ -n "$envs" ]]; then
		prefix="export $envs; "
	fi
	cmd="$(sys_cmd "$sys" "$w" "$n" "$run_id")"
	lab_detach "$pc" "$run_id" "$prefix$cmd" || return 1
	code="$(lab_wait "$pc" "$run_id" "$timeout_s")" || rc=$?
	if ((rc == 124)); then
		note="timeout"
		rec_rc=124
	elif ((rc != 0)); then
		note="lost-connection"
		lab_log "run $run_id: lab_wait returned $rc; recording the run with exit_code null"
	fi

	dir="$LAB/hosts/$pc/runs/$run_id"
	lab_fetch "$pc" "$run_id" "$dir" ||
		lab_log "run $run_id: fetch failed after wait; recording what $pc kept"
	if ((rc == 124)); then
		# A hung run would load every later one. A nohup run has no unit: the
		# stop then fails, which is logged and does not stop the campaign.
		lab_ssh "$pc" "systemctl --user stop lab-$run_id" >/dev/null 2>&1 ||
			lab_log "run $run_id: could not stop unit lab-$run_id on $pc (a nohup run has no unit)"
	fi

	PYRUN_GIT_COMMIT="${PYRUN_GIT_COMMIT:-}"
	PIPEDPEER_VERSION="${PIPEDPEER_VERSION:-}"
	PYRUN_RUNSLOG="$LAB/runs.jsonl" _run_record "$run_id" "$exp" "$w" "$sys" \
		"$n" "$rep" "$warm" "$rec_rc" "$note" "${hosts[*]}"
	if ((rc == 124)); then
		printf 'TIMEOUT %s\n' "$run_id" >&2
		return 124
	fi
	return 0
}

# _run_rows <exp>: experiments.tsv rows for the experiment, as
# workload<TAB>systems<TAB>sizes<TAB>repeats<TAB>warmup<TAB>timeout<TAB>mode.
_run_rows() {
	[[ -s "$EXPERIMENTS" ]] || { echo "no $EXPERIMENTS" >&2; return 1; }
	awk -F'\t' -v e="$1" '!/^#/ && NF >= 8 && $1 == e { for (i=2;i<=NF;i++) printf "%s%s", $i, (i<NF?"\t":"\n") }' "$EXPERIMENTS"
}

# _intersect <a-list> <b-list>: the common csv items, in a's order.
_intersect() {
	awk -v a="$1" -v b="$2" 'BEGIN{
		n=split(a,x,","); m=split(b,y,",");
		for(i=1;i<=n;i++) for(j=1;j<=m;j++) if(x[i]==y[j]){printf "%s%s",(c?",":""),x[i]; c=1}
	}'
}

# _run_result_line <run_id>: the `RESULT <id> ok=.. remote_items=..` line,
# rebuilt from the run's runs.jsonl line (empty when there is none).
_run_result_line() {
	python3 - "$LAB/runs.jsonl" "$1" <<'PY'
import json
import sys

try:
    with open(sys.argv[1]) as fh:
        for ln in fh:
            r = json.loads(ln)
            if r["run_id"] == sys.argv[2]:
                ok = "null" if r["result_ok"] is None else str(bool(r["result_ok"])).lower()
                rec = r.get("receipt")
                ri = rec.get("remote_items") if isinstance(rec, dict) else None
                print("RESULT %s ok=%s remote_items=%s" % (r["run_id"], ok, "-" if ri is None else ri))
                break
except FileNotFoundError:
    pass
PY
}

# _run_blocker <line>: append to BLOCKERS.md unless it is already there, so a
# resumed campaign does not repeat itself.
_run_blocker() {
	if ! grep -qxF -- "$1" "$LAB/BLOCKERS.md" 2>/dev/null; then
		echo "$1" >>"$LAB/BLOCKERS.md"
	fi
	lab_log "$1"
}

# _run_gates <exp> <w> <sys> <n> <run_id> <RESULT line>: SMOKE gates.
_run_gates() {
	local exp="$1" w="$2" sys="$3" n="$4" run_id="$5" result="$6"
	local ok ritm
	[[ "$exp" == SMOKE ]] || return 0
	ok="$(printf '%s\n' "$result" | sed -n 's/^RESULT [^ ]* ok=\([a-z\-]*\).*/\1/p')"
	ritm="$(printf '%s\n' "$result" | sed -n 's/^.*remote_items=\(.*\)$/\1/p')"
	printf 'SMOKE %s %s n=%s result_ok=%s remote_items=%s\n' "$w" "$sys" "$n" "$ok" "$ritm"
	if [[ "$ok" == false ]]; then
		_run_blocker "SMOKE gate: result_ok=false $run_id"
	fi
	if [[ "$sys" == pipedpeer && "$n" -gt 1 && "$ritm" == 0 ]]; then
		_run_blocker "SMOKE gate: remote_items=0 $run_id"
	fi
}

# _run_majority <exp>: rewrites result_ok for weak-mode runs with no
# reference: for that (workload, N) the majority value across systems is the
# truth (brief Decisions: sizes without a reference compare against the
# majority, flagged ref=majority). Twice rules out strong-mode runs; warm-ups
# are skipped. Numbers match exactly (checksums carry the same digits).
_run_majority() {
	[[ "$1" == E1b ]] || return 0
	[[ -s "$LAB/runs.jsonl" ]] || return 0
	PYRUN_RUNSLOG="$LAB/runs.jsonl" python3 - "$1" <<'MAJ_PY'
import json
import os
import sys

exp = sys.argv[1]
path = os.environ["PYRUN_RUNSLOG"]
rows = []
with open(path) as fh:
    for ln in fh:
        rows.append(json.loads(ln))

groups = {}
for i, r in enumerate(rows):
    if r["experiment"] != exp or r.get("ref") != "none" or r["warmup"]:
        continue
    if r.get("exit_code") != 0:
        continue  # a failed run stays result_ok false
    groups.setdefault((r["workload"], r["n"]), []).append(i)


def val(r):
    lines = r["result_lines"]
    if not lines:
        return None
    w = r["workload"]
    if w in ("W1", "idle"):
        return float(lines[0].rsplit("=", 1)[1])
    if w == "W2":
        return "|".join(sorted(lines))
    if w == "W3":
        return lines[0].strip()
    return None  # noop, lat, fileio, sweeps: no majority vote


changed = 0
for key, idxs in groups.items():
    vals = [val(rows[i]) for i in idxs]
    if any(v is None for v in vals):
        continue
    dist = {}
    for v in vals:
        dist[v] = dist.get(v, 0) + 1
    winner, count = max(dist.items(), key=lambda kv: kv[1])
    if count > len(vals) / 2:
        for i, v in zip(idxs, vals):
            rows[i]["ref"] = "majority"
            rows[i]["result_ok"] = v == winner
            changed += 1
    else:
        sys.stderr.write("WARN majority-missing %s %s n=%s\n" % (exp, key[0], key[1]))

if changed:
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        for r in rows:
            fh.write(json.dumps(r, separators=(",", ":")) + "\n")
    os.replace(tmp, path)
    sys.stderr.write("majority: updated %d rows\n" % changed)
MAJ_PY
}

# run_all <exp> --workloads/--systems/--sizes/--repeats: the matrix walker.
run_all() {
	local exp="$1"
	shift
	local wl="" sy="" sizes="" reps="" arg
	while (($#)); do
		arg="$1"
		case "$arg" in
			--workloads) wl="${2:-}"; shift 2 ;;
			--systems) sy="${2:-}"; shift 2 ;;
			--sizes) sizes="${2:-}"; shift 2 ;;
			--repeats) reps="${2:-}"; shift 2 ;;
			*) run_usage >&2; echo "unknown argument $arg" >&2; return 2 ;;
		esac
	done

	if [[ ! -x "$CLUSTER_SH" ]]; then
		echo "missing $CLUSTER_SH (written by S7; not here yet)" >&2
		return 1
	fi
	_run_rows "$exp" >/dev/null || return 1
	_load_sizes || return 1
	PYRUN_GIT_COMMIT="$(git -C "$REPO" rev-parse --short=7 HEAD 2>/dev/null || true)"
	PIPEDPEER_VERSION="$(_get_version)"

	local rows row_w row_sys row_sizes row_reps row_warm row_to row_mode
	rows="$(_run_rows "$exp")"
	local w n sys rep rcg result run_id resline
	while IFS=$'\t' read -r row_w row_sys row_sizes row_reps row_warm row_to row_mode; do
		[[ -n "$row_w" ]] || continue
		if [[ "$row_mode" == weak ]]; then export BENCH_WEAK=1; else unset BENCH_WEAK; fi
		if [[ -n "$wl" ]]; then
			row_w="$(_intersect "$wl" "$row_w")"
			[[ -n "$row_w" ]] || continue
		fi
		if [[ -n "$sy" ]]; then
			row_sys="$(_intersect "$sy" "$row_sys")"
			[[ -n "$row_sys" ]] || continue
		fi
		[[ -n "$sizes" ]] && row_sizes="$sizes"
		[[ -n "$reps" ]] && row_reps="$reps"
		# CAMPAIGN_TIMEOUT_CAP_S: a verifier's shorter pass; never raises a timeout
		if [[ -n "${CAMPAIGN_TIMEOUT_CAP_S:-}" ]] && ((row_to > CAMPAIGN_TIMEOUT_CAP_S)); then
			row_to="$CAMPAIGN_TIMEOUT_CAP_S"
		fi
		if [[ "$row_sizes" == "scaling" ]]; then
			row_sizes="$SCALING_SIZES"
		fi
		local -a workloads_list=() sizes_list=() row_systems=() sys_at_n=() order=()
		IFS=',' read -r -a workloads_list <<<"$row_w"
		IFS=',' read -r -a sizes_list <<<"$row_sizes"
		IFS=',' read -r -a row_systems <<<"$row_sys"
		for w in "${workloads_list[@]}"; do
			for n in "${sizes_list[@]}"; do
				sys_at_n=()
				for sys in "${row_systems[@]}"; do
					if [[ "$sys" == plain && "$n" -gt 1 ]]; then
						continue
					fi
					sys_at_n+=("$sys")
				done
				((${#sys_at_n[@]})) || continue

				# Every system at this N shares these machines: a start that
				# fails voids the comparison here, never a smaller cluster
				# reported as N. The next N starts its clusters afresh.
				if ! "$CLUSTER_SH" up "$n"; then
					lab_log "run_all: cluster up $n failed for $exp $w; skipping this N, see logs/cluster-reset and NOT-READY lines"
					_run_blocker "cluster up failed: $exp $w n=$n"
					continue
				fi
				# one tag per workload: a second workload at the same N would
				# otherwise meet the first one's ALREADY-METRICS
				"$METRICS_SH" start "$exp-$w-n$n" "$LAB/hosts-$n.txt" ||
					lab_log "metrics start $exp-$w-n$n failed; continuing without it"
				if ((row_warm > 0)); then
					for sys in "${sys_at_n[@]}"; do
						BENCH_SEED=""
						rcg=0
						result="$(campaign_run_one "$exp" "$w" "$sys" "$n" 0 1 "$row_to" "$row_mode")" || rcg=$?
						_run_classify "$result" "$rcg" "${exp}-${w}-${sys}-n${n}-r0"
					done
				fi
				for ((rep = 1; rep <= row_reps; rep++)); do
					order=()
					read -ra order <<< "$(run_shuffle "$exp-$w-$n-$rep" "${sys_at_n[@]}")"
					for sys in "${order[@]}"; do
						run_id="$(_run_id "$exp" "$w" "$sys" "$n" "$rep" 0)"
						if [[ "$exp" == E8 ]]; then
							BENCH_SEED=$((10 + rep))
						else
							BENCH_SEED=""
						fi
						rcg=0
						result="$(campaign_run_one "$exp" "$w" "$sys" "$n" "$rep" 0 "$row_to" "$row_mode")" || rcg=$?
						_run_classify "$result" "$rcg" "$run_id"
						resline="$(grep -m1 '^RESULT ' <<<"$result" || true)"
						if [[ "$exp" == SMOKE && -n "$resline" ]]; then
							PYRUN_MODE="$row_mode" _run_gates "$exp" "$w" "$sys" "$n" "$run_id" "$resline"
						elif [[ "$exp" == SMOKE && "$result" == skipped\ * ]]; then
							# Resumed run: the earlier pass may have died before the
							# gate ran, so judge it again from runs.jsonl.
							resline="$(_run_result_line "$run_id")"
							if [[ -n "$resline" ]]; then
								PYRUN_MODE="$row_mode" _run_gates "$exp" "$w" "$sys" "$n" "$run_id" "$resline"
							fi
						fi
					done
				done
				"$METRICS_SH" stop "$exp-$w-n$n" "$LAB/hosts-$n.txt" ||
					lab_log "metrics stop $exp-$w-n$n failed"
				"$UPLOAD_SH" || lab_log "upload failed; runs continue (S10 handles the retry)"
			done
		done
	done <<<"$rows"
	_run_majority "$exp"
	printf 'RUNS done=%d skipped=%d\n' "$RUNS_DONE" "$RUNS_SKIPPED"
}

# _run_classify <captured stdout of campaign_run_one> <rc> <run_id>: pass
# through the skip marker, warn on a failed run; a skip advances only the
# skipped counter.
_run_classify() {
	local out="$1" rc="$2" run_id="$3"
	if [[ "$out" == skipped\ * ]]; then
		printf '%s\n' "$out"
		RUNS_SKIPPED=$((RUNS_SKIPPED + 1))
		return 0
	fi
	RUNS_DONE=$((RUNS_DONE + 1))
	if [[ "$out" == *"ok=false"* ]]; then
		lab_log "WARN result_ok=false $run_id"
	fi
	if ((rc == 124)); then
		lab_log "run $run_id: TIMEOUT; its unit was stopped"
		return 0
	fi
	if ((rc != 0)); then
		lab_log "run $run_id failed (exit $rc); continuing"
	fi
	return 0
}

# run_dry_run <exp> [opts]: every run_id with its command, nothing runs.
run_dry_run() {
	local exp="$1"
	shift
	local wl="" sy="" sizes="" reps="" arg
	local -a rows_list=()
	while (($#)); do
		arg="$1"
		case "$arg" in
			--workloads) wl="${2:-}"; shift 2 ;;
			--systems) sy="${2:-}"; shift 2 ;;
			--sizes) sizes="${2:-}"; shift 2 ;;
			--repeats) reps="${2:-}"; shift 2 ;;
			*) run_usage >&2; echo "unknown argument $arg" >&2; return 2 ;;
		esac
	done
	_load_sizes || return 1
	local rows row_w row_sys row_sizes row_reps row_warm row_to row_mode
	rows="$(_run_rows "$exp")"
	while IFS=$'\t' read -r row_w row_sys row_sizes row_reps row_warm row_to row_mode; do
		[[ -n "$row_w" ]] || continue
		if [[ "$row_mode" == weak ]]; then export BENCH_WEAK=1; else unset BENCH_WEAK; fi
		if [[ -n "$wl" ]]; then
			row_w="$(_intersect "$wl" "$row_w")"
			[[ -n "$row_w" ]] || continue
		fi
		if [[ -n "$sy" ]]; then
			row_sys="$(_intersect "$sy" "$row_sys")"
			[[ -n "$row_sys" ]] || continue
		fi
		[[ -n "$sizes" ]] && row_sizes="$sizes"
		[[ -n "$reps" ]] && row_reps="$reps"
		if [[ "$row_sizes" == "scaling" ]]; then
			row_sizes="$SCALING_SIZES"
		fi
		local -a workloads_list=() sizes_list=() row_systems=()
		IFS=',' read -r -a workloads_list <<<"$row_w"
		IFS=',' read -r -a sizes_list <<<"$row_sizes"
		IFS=',' read -r -a row_systems <<<"$row_sys"
		local w n sys rep run_id cmd warm seed
		for w in "${workloads_list[@]}"; do
			for n in "${sizes_list[@]}"; do
				for sys in "${row_systems[@]}"; do
					if [[ "$sys" == plain && "$n" -gt 1 ]]; then
						continue
					fi
					for rep in $(seq 0 "$row_reps"); do
						warm=0
						if ((rep == 0)); then
							((row_warm > 0)) || continue
							warm=1
						fi
						run_id="$(_run_id "$exp" "$w" "$sys" "$n" "$rep" "$warm")"
						seed=""
						if [[ "$exp" == E8 && "$rep" -gt 0 ]]; then
							seed=$((10 + rep))
						fi
						cmd="$(BENCH_SEED="$seed" sys_cmd "$sys" "$w" "$n" "$run_id")"
						printf '%s\t%s\n' "$run_id" "$cmd"
					done
				done
			done
		done
	done <<<"$rows"
}

main() {
	local -a sub_args=()
	local arg dry=0
	local -a dry_arg=()
	if (($# == 0)); then
		run_usage >&2
		return 2
	fi
	if [[ "$1" == -h || "$1" == --help ]]; then
		run_usage
		return 0
	fi
	local exp="$1"
	shift
	while (($#)); do
		arg="$1"
		case "$arg" in
			--dry-run) dry=1; dry_arg=(--dry-run); shift ;;
			*) sub_args+=("$arg"); shift ;;
		esac
	done
	if [[ -z "${LAB_INHIBITED:-}" ]] && command -v systemd-inhibit >/dev/null 2>&1; then
		# macOS would get caffeinate -dimsu in the same place; the lab is
		# Linux-driven and there is no mac driver machine (Facts).
		exec systemd-inhibit --what=sleep:idle:handle-lid-switch --why=pipedpeer-lab \
			env LAB_INHIBITED=1 bash "$0" "$exp" "${dry_arg[@]}" "${sub_args[@]}"
	fi
	if ((dry)); then
		run_dry_run "$exp" "${sub_args[@]}"
	else
		run_all "$exp" "${sub_args[@]}"
	fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	main "$@"
fi
