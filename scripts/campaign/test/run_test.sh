#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2317,SC2034,SC2015,SC2016,SC2002,SC2143  # rows run in subshells
# The S9 case table: one row per behaviour scripts/campaign/run.sh promises
# (brief S9), driven through the fake lab (test/fakelab.sh) with stub
# pipedpeer/python that print the real result line formats. Nothing here
# touches a real results dir, real pipedpeer, or a real PC: each row runs in
# its own subshell with its own HOME, PATH and LAB_RESULTS.
#
# One ok/FAIL line per row; the run ends "PASS <k>/<k>" and exits 0 only when
# every row passed. Set LAB_TEST_KEEP=1 to keep a failing row's scratch dir.

set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
RUN_SH="$CAMP/run.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"

# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

# Rows run in ( ... ) subshells, so a counter would be lost: every verdict is
# one line in $RESULTS, and the final count is read back from it.
RESULTS="$(mktemp)"
problems=()
dirs=()

cleanup() {
	local d
	if [[ "${LAB_TEST_KEEP:-0}" == 1 ]]; then
		printf 'kept: %s\n' "${dirs[*]}" >&2
		return
	fi
	for d in "${dirs[@]}"; do
		rm -rf "$d"
	done
	rm -f "$RESULTS"
}
trap cleanup EXIT

ok() { echo ok >>"$RESULTS"; printf 'ok   %s\n' "$1"; }
bad() { echo bad >>"$RESULTS"; printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }
verdict() {
	if ((${#problems[@]} == 0)); then
		ok "$1"
	else
		bad "$1" "${problems[*]}"
	fi
	problems=()
}

# RUN_TEST_FORCE_FAIL=1: the table is one row that fails on purpose, so the
# summary below can be seen to say FAIL and exit non-zero.
FORCE_FAIL="${RUN_TEST_FORCE_FAIL:-0}"

newdir() {
	local d
	d="$(mktemp -d "$STATE/campaign-run-test.XXXXXX")"
	mkdir -p "$d/home" "$d/bin"
	dirs+=("$d")
	printf '%s\n' "$d"
}

# row_setup <dir>: fake lab (3 PCs), sizes.env, references, stub pipedpeer
# and cluster/metrics/upload stubs. After this the row sources run.sh itself
# (source, so campaign_run_one and run_shuffle are callable).
row_setup() {
	local dir="$1"
	cd "$REPO"
	fakelab_init "$dir" 3 || exit 90
	export HOME="$dir/home"
	export LAB_RESULTS="$dir"
	export LAB_RETRY_SLEEP=0.2
	export LAB_POLL_S=1
	export LAB_INHIBITED=1
	export PATH="$CAMP:$PATH"
	# sizes.env with the stub's own numbers.
	cat >"$dir/sizes.env" <<'SIZES'
BENCH_PROCS=8
W1_ITEMS=4800
W1_WEAK_PER_PC=48
W2_ITEMS=128
W3_TREES=150
T1_EPOCHS=30
SIZES

	# Reference files the stub outputs satisfy (S8's rules copied here).
	mkdir -p "$dir/reference"
	cat >"$dir/reference/W1-strong.txt" <<'REF'
POOLMAP-OK items=4800 seconds=1.0000 checksum=42.0
REF
	cat >"$dir/reference/idle-strong.txt" <<'REF'
POOLIDLE-OK items=4800 seconds=1.5000 checksum=42.0
REF
	cat >"$dir/reference/W2-strong.txt" <<'REF'
Pool: 128 items, checksum 100.0
joblib: 64 Mandelbrot tiles, 24 points inside
numpy: largest singular value 1.2346
pandas: 5 rows -> 2 cities, total rain 100.1
REF
	cat >"$dir/reference/W3-strong.txt" <<'REF'
accuracy: 0.9400
REF
	cat >"$dir/reference/T1-strong.txt" <<'REF'
final loss: 0.490000
weight checksum: 1.234567890
REF
	printf 'TRIVIAL-OK\n' >"$dir/reference/trivial-strong.txt"

	# The venv python: prints the port/original result lines per script arg.
	mkdir -p "$dir/pc00/pipedpeer-lab/venv/bin"
	cat >"$dir/pc00/pipedpeer-lab/venv/bin/python" <<'PYSTUB'
#!/usr/bin/env bash
[[ -n "${CAMP_STUB_SLEEP:-}" ]] && sleep "$CAMP_STUB_SLEEP"
case "$*" in
	*pool_map.py*|*w1_*) echo "POOLMAP-OK items=${BENCH_ITEMS:-4800} seconds=1.0000 checksum=${BENCH_STUB_BAD:-42.0}" ;;
	*pool_idle.py*) echo "POOLIDLE-OK items=4800 seconds=1.5000 checksum=${BENCH_STUB_BAD:-42.0}" ;;
	*trivial.py*) echo "TRIVIAL-OK" ;;
	*cpu.py*|*w2_*) echo "Pool: 128 items, checksum 100.0"; echo "joblib: 64 Mandelbrot tiles, 24 points inside"; echo "numpy: largest singular value 1.2346"; echo "pandas: 5 rows -> 2 cities, total rain 100.1" ;;
	*w3_*|*w3_rf.py*) echo "accuracy: 0.9400" ;;
	*t1_*|*t1_mlp.py*) echo "final loss: 0.490000"; echo "weight checksum: 1.234567890" ;;
	*) echo "python-stub: no result lines for [$*]" >&2; exit 3 ;;
esac
PYSTUB
	chmod +x "$dir/pc00/pipedpeer-lab/venv/bin/python"

	# ssh/torchrun baseline shells: the same lines.
	mkdir -p "$dir/pc00/pipedpeer-lab/work/baselines"
	local w sh lines
	for w in w1 w2 w3 trivial; do
		case "$w" in
			w1) lines='echo "POOLMAP-OK items=${BENCH_ITEMS:-4800} seconds=1.0000 checksum=42.0"' ;;
			w2) lines='echo "Pool: 128 items, checksum 100.0"; echo "joblib: 64 Mandelbrot tiles, 24 points inside"; echo "numpy: largest singular value 1.2346"; echo "pandas: 5 rows -> 2 cities, total rain 100.1"' ;;
			w3) lines='echo "accuracy: 0.9400"' ;;
			trivial) lines='echo "TRIVIAL-OK"' ;;
		esac
		sh="$dir/pc00/pipedpeer-lab/work/baselines/${w}_ssh.sh"
		printf '#!/usr/bin/env bash\n%s\n' "$lines" >"$sh"
		chmod +x "$sh"
	done
	sh="$dir/pc00/pipedpeer-lab/work/baselines/t1_torchrun.sh"
	printf '#!/usr/bin/env bash\necho "final loss: 0.490000"\necho "weight checksum: 1.234567890"\n' >"$sh"
	chmod +x "$sh"

	# The pipedpeer stub (status/run/jobs), on PATH ahead of everything.
	cat >"$dir/bin/pipedpeer" <<'PPSTUB'
#!/usr/bin/env bash
sub="${1:-}"; shift
case "$sub" in
	status) echo "daemon running (pid=1600 node-id=9078b729c31d42b5 port=4242 version=0.0.1-nightly.20261104-teststub)"; echo; echo "NAME STATE PATH CPU MEM FREE GPU JOBS NOTE"; echo "pc00 healthy self 8c 100% - - 0 -" ;;
	jobs)
		shift  # last
		echo '{"run_id":"stub","total_ns":1000000,"script_ns":900000,"steps":[]}' ;;
	run)
		env_extra=()
		while (( $# )); do
			case "$1" in
				-e) env_extra+=("$2"); shift 2 ;;
				-e*) env_extra+=("${1#-e}"); shift ;;
				--gpu|--distribute|--ddp|--ddp-sync) shift 2 ;;
				--gpu*|--distribute*|--ddp*|--ddp-sync*) shift ;;
				-*) shift ;;
				*) break ;;
			esac
		done
		script="$1"
		for kv in "${env_extra[@]}"; do export "${kv%%=*}"="${kv#*=}"; done
		[[ -n "${CAMP_STUB_SLEEP:-}" ]] && sleep "$CAMP_STUB_SLEEP"
		if [[ -n "${PIPEDPEER_RECEIPT:-}" ]]; then
			mkdir -p "$(dirname "$PIPEDPEER_RECEIPT")"
			printf '{"remote_items":%s,"local_items":0,"dispatched_items":1,"mapped_items":1,"remote_failures":0,"remote_used":true,"steps":[]}\n' "${STUB_REMOTE_ITEMS:-10}" >"$PIPEDPEER_RECEIPT"
		fi
		case "$script" in
			*pool_map.py*|*w1_*) echo "POOLMAP-OK items=${BENCH_ITEMS:-4800} seconds=1.0000 checksum=${BENCH_STUB_BAD:-42.0}" ;;
			*pool_idle.py*) echo "POOLIDLE-OK items=4800 seconds=1.5000 checksum=${BENCH_STUB_BAD:-42.0}" ;;
			*trivial.py*) echo "TRIVIAL-OK" ;;
			*cpu.py*) echo "Pool: 128 items, checksum 100.0"; echo "joblib: 64 Mandelbrot tiles, 24 points inside"; echo "numpy: largest singular value ${STUB_W2_SV:-1.2346}"; echo "pandas: 5 rows -> 2 cities, total rain 100.1" ;;
			*w3_rf.py*) echo "accuracy: ${STUB_W3_ACC:-0.9400}" ;;
			*t1_mlp.py*) echo "final loss: ${STUB_T1_LOSS:-0.490000}"; echo "weight checksum: 1.234567890" ;;
			*) echo "pipedpeer-stub: no result lines for [$script]" >&2; exit 3 ;;
		esac
		[[ -n "${CAMP_STUB_EXIT:-}" ]] && exit "$CAMP_STUB_EXIT"
		exit 0
		;;
	connect) exit 0 ;;
	stop|start) ;;
	*) echo "pipedpeer-stub: unknown subcommand $sub" >&2; exit 2 ;;
esac
PPSTUB
	chmod +x "$dir/bin/pipedpeer"

	# cluster/metrics/upload stubs that log their calls.
	export CLUSTER_SH="$dir/bin/cluster.sh" METRICS_SH="$dir/bin/metrics.sh" UPLOAD_SH="$dir/bin/upload.sh"
	printf '#!/usr/bin/env bash\necho "cluster $*" >>"%s/stubs.log"\n' "$d" >"$CLUSTER_SH"
	chmod +x "$CLUSTER_SH"
	printf '#!/usr/bin/env bash\necho "metrics $*" >>"%s/stubs.log"\n' "$d" >"$METRICS_SH"
	chmod +x "$METRICS_SH"
	printf '#!/usr/bin/env bash\necho "upload" >>"%s/stubs.log"\n' "$d" >"$UPLOAD_SH"
	chmod +x "$UPLOAD_SH"

	# shellcheck source=/dev/null
	source "$RUN_SH"
	set +e
}

# expected_e1_runs: the E1 run count for --sizes 1,2 --repeats 2, computed from
# the E1 row of experiments.tsv (plain only at n=1), not typed in.
expected_e1_runs() {
	python3 - "$CAMP/experiments.tsv" <<'PY'
import sys

for ln in open(sys.argv[1]):
    f = ln.rstrip("\n").split("\t")
    if len(f) >= 8 and f[0] == "E1":
        wl, systems, warm = f[1].split(","), f[2].split(","), int(f[5])
        runs = 0
        for n in (1, 2):
            at_n = [s for s in systems if not (s == "plain" and n > 1)]
            runs += len(wl) * len(at_n) * (2 + (1 if warm > 0 else 0))
        print(runs)
        break
else:
    print(0)
PY
}

# rec_field <run_id> <field>: that field of the run's runs.jsonl line, as JSON.
rec_field() {
	python3 - "$LAB/runs.jsonl" "$1" "$2" <<'PY'
import json
import sys

for ln in open(sys.argv[1]):
    r = json.loads(ln)
    if r["run_id"] == sys.argv[2]:
        print(json.dumps(r[sys.argv[3]]))
        break
else:
    print("MISSING")
PY
}

json_count() { # json_count <exp-filter> — rows in runs.jsonl
	python3 - "$LAB/runs.jsonl" "$1" <<'PY'
import json
import sys

filter_exp = sys.argv[2]
n = 0
try:
    with open(sys.argv[1]) as fh:
        for ln in fh:
            if not ln.strip():
                continue
            if not filter_exp or json.loads(ln)["experiment"] == filter_exp:
                n += 1
except FileNotFoundError:
    pass
print(n)
PY
}

dup_ids() {
	python3 - "$LAB/runs.jsonl" <<'PY'
import json
import sys

ids = []
try:
    with open(sys.argv[1]) as fh:
        for ln in fh:
            if ln.strip():
                ids.append(json.loads(ln)["run_id"])
except FileNotFoundError:
    pass
dups = sorted({i for i in ids if ids.count(i) > 1})
if dups:
    print("duplicates: %s" % dups)
    raise SystemExit(1)
print("ok")
PY
}

if [[ "$FORCE_FAIL" == 1 ]]; then
	pnote "failing on purpose"
	verdict "forced failure"
	nrows=$(wc -l <"$RESULTS")
	nfail=$(grep -c '^bad$' "$RESULTS" || true)
	if ((nfail == 0)); then printf "PASS %d/%d\n" "$((nrows - nfail))" "$nrows"; else printf "FAIL %d/%d passed\n" "$((nrows - nfail))" "$nrows"; fi
	((nrows > 0 && nfail == 0))
	exit
fi

# --- row 1: SMOKE --dry-run lists 27 run_ids, each once, no plain at 2 ------
(d=$(newdir)
row_setup "$d"
out="$(run.sh SMOKE --dry-run 2>"$d/err")"
ids="$(cut -f1 <<<"$out")"
n_total="$(wc -l <<<"$ids")"
n_uniq="$(sort -u <<<"$ids" | wc -l)"
[[ "$n_total" == 27 && "$n_uniq" == 27 ]] || pnote "expected 27 unique run_ids, got $n_total/$n_uniq"
[[ "$(grep -c 'plain' <<<"$ids")" == 3 ]] || pnote "plain should appear 3 times (n=1 only), got $(grep -c "plain" <<<"$ids")"
[[ -z "$(grep -E '\-plain-n2-|plain.*-n2-' <<<"$out")" ]] || pnote "plain must not run at n=2"
[[ ! -f "$d/runs.jsonl" && ! -f "$LAB/runs.jsonl" ]] || pnote "dry-run must not touch runs.jsonl"
[[ "$(grep -c 'cluster\|metrics\|upload' "$d/stubs.log" 2>/dev/null || echo 0)" == 0 || ! -f "$d/stubs.log" ]] || pnote "dry-run must not call cluster/metrics/upload"
disp="$(grep -c 'systemd-inhibit\|inhibit' "$d/err" || true)"
verdict "dry-run SMOKE prints 27 run_ids once each")

# --- row 2: E1 --sizes 1,2 --repeats 2: warm-ups r0 + 2 repeats -------------
(d=$(newdir)
row_setup "$d"
out="$(run.sh E1 --sizes 1,2 --repeats 2 2>"$d/err")"
n="$(json_count E1)"
want81="$(expected_e1_runs)"
[[ "$n" == "$want81" && "$want81" -gt 0 ]] || pnote "expected $want81 runs derived from the E1 row of experiments.tsv (workloads x systems-at-n x (repeats+warm-up)), got $n"
warm="$(grep -c '"warmup":true' "$LAB/runs.jsonl")"
[[ "$warm" == 27 ]] || pnote "expected 27 warm-up rows, got $warm"
plain_n2="$(grep -c '"system":"plain","n":2' "$LAB/runs.jsonl")"
[[ "$plain_n2" == 0 ]] || pnote "plain ran at n=2 ($plain_n2 rows)"
dup_ids >/dev/null 2>&1 || pnote "duplicate run_ids in runs.jsonl"
grep -q 'cluster up 1' "$d/stubs.log" && grep -q 'cluster up 2' "$d/stubs.log" || pnote "cluster.sh up not called for both sizes"
grep -q 'metrics start E1-n1' "$d/stubs.log" || pnote "metrics.sh start missing for n=1"
grep -q 'metrics stop E1-n2' "$d/stubs.log" || pnote "metrics.sh stop missing for n=2"
grep -q '^upload' "$d/stubs.log" || pnote "upload.sh not called"
verdict "E1 sizes 1,2 repeats 2 runs every row with warm-ups and order"
)

# --- row 3: same command again: nothing runs, skipped <k> -------------------
(d=$(newdir)
row_setup "$d"
out1="$(run.sh E1 --sizes 1,2 --repeats 2 2>/dev/null)"
before="$(json_count E1)"
out2="$(run.sh E1 --sizes 1,2 --repeats 2 2>"$d/err")"
skips="$(grep -c '^skipped ' <<<"$out2")"
after="$(json_count E1)"
[[ "$skips" == "$(expected_e1_runs)" ]] || pnote "expected $(expected_e1_runs) skipped lines, got $skips"
[[ "$before" == "$after" ]] || pnote "runs.jsonl grew on rerun ($before -> $after)"
[[ "$out2" == *"RUNS done=0 skipped=$(expected_e1_runs)"* ]] || pnote "summary should say done=0 skipped=$(expected_e1_runs), got: $(tail -1 <<<"$out2")"
verdict "rerun of a finished experiment skips every run")

# --- row 4: kill mid-way, rerun completes the rest, no run_id twice ---------
(d=$(newdir)
row_setup "$d"
# setsid + a group kill: killing only the bash would orphan the running
# campaign_run_one subshell, which would still record its run.
CAMP_STUB_SLEEP=0.6 setsid run.sh E1 --workloads W1 --sizes 1,2 --repeats 2 --systems plain,pipedpeer,ray 2>"$d/err1" &
bgpid=$!
sleep 2.2
kill -- "-$bgpid" 2>/dev/null || true
wait "$bgpid" 2>/dev/null || true
mid="$(json_count E1)"
out2="$(run.sh E1 --workloads W1 --sizes 1,2 --repeats 2 --systems plain,pipedpeer,ray 2>"$d/err2")"
skips="$(grep -c '^skipped ' <<<"$out2")"
[[ "$skips" -ge 1 ]] || pnote "rerun should skip the pre-kill rows, skipped $skips"
final="$(json_count E1)"
[[ "$final" == 15 ]] || pnote "expected 15 rows total, got $final"
dup_ids >/dev/null 2>&1 || pnote "duplicate run_ids after kill+rerun"
verdict "kill mid-way: rerun completes, no run_id twice")

# --- row 5: stub output with wrong checksum -> result_ok:false + warn -------
(d=$(newdir)
row_setup "$d"
out="$(BENCH_STUB_BAD=1 run.sh E1 --workloads W1 --systems pipedpeer --sizes 1 --repeats 1 2>"$d/err")"
n_false="$(grep -c '"result_ok":false' "$LAB/runs.jsonl")"
[[ "$n_false" == 2 ]] || pnote "expected 2 false rows (r0 + r1), got $n_false"
grep -q 'WARN result_ok=false' <<EOF || pnote "no WARN line: $(cat "$d/err" | tail -3)"
$(grep . "$d/err" | tr '\n' ' ' 2>/dev/null || true)
EOF
grep -q 'WARN result_ok=false' "$LAB/campaign.log" || pnote "no WARN in campaign.log"
verdict "wrong checksum recorded result_ok:false with WARN")

# --- row 6: no reference file -> ref:"none", result_ok null ------------------
(d=$(newdir)
row_setup "$d"
rm -f "$LAB/reference/W3-strong.txt"
out="$(run.sh E1 --workloads W3 --systems ray --sizes 1 --repeats 1 2>"$d/err")"
row="$(grep '"system":"ray","n":1' "$LAB/runs.jsonl" | tail -1)"
grep -q '"ref":"none"\|"ref":"none"' <<<"$row" || pnote "ref not none: $row"
grep -q '"result_ok":null,"ref":"none"' <<<"$row" || pnote "result_ok not null: $row"
verdict "missing reference -> ref none, result_ok null")

# --- row 7: run exceeding timeout -> exit_code 124, note timeout ------------
(d=$(newdir)
row_setup "$d"
rc=0
CAMP_STUB_SLEEP=3 campaign_run_one T W1 pipedpeer 1 1 0 1 strong >/dev/null 2>&1 || rc=$?
[[ "$rc" == 124 ]] || pnote "campaign_run_one should return 124 on timeout, got $rc"
row="$(grep '"experiment":"T"' "$LAB/runs.jsonl" | head -1)"
[[ -n "$row" ]] || pnote "no runs.jsonl line after timeout"
grep -q '"exit_code":124' <<<"$row" || pnote "exit_code not 124: $row"
grep -q '"note":"timeout"' <<<"$row" || pnote "note not timeout: $row"
verdict "timeout recorded exit_code 124 note timeout, campaign continued")

# --- row 8: pc00 ssh drop for 2 polls -> run finishes, one line -------------
(d=$(newdir)
row_setup "$d"
# the drop starts 0.4s in and clears 1.3s later: two lab_wait polls fail
( sleep 0.4; echo pc00 >"$FAKELAB_DIR/fail"; sleep 2.0; rm -f "$FAKELAB_DIR/fail" ) &
CAMP_STUB_SLEEP=3 rc=0
campaign_run_one T W1 pipedpeer 1 1 0 30 strong >/dev/null 2>&1 || rc=$?
wait $! 2>/dev/null || true
[[ "$rc" == 0 ]] || pnote "campaign_run_one should succeed through the drop, rc=$rc"
n="$(json_count T)"
[[ "$n" == 1 ]] || pnote "expected exactly 1 recorded run, got $n"
grep -q 'still waiting' "$LAB/campaign.log" || pnote "ssh drop not handled as not-yet (campaign.log)"
verdict "ssh drop mid-wait: run finishes and records exactly once")

# --- row 9: shuffle for the same seed -> same order -------------------------
(d=$(newdir)
row_setup "$d"
o1="$(run_shuffle seed-x plain pipedpeer ray dask ssh)"
o2="$(run_shuffle seed-x plain pipedpeer ray dask ssh)"
[[ "$o1" == "$o2" && -n "$o1" ]] || pnote "same seed produced different order: [$o1] vs [$o2]"
nwords="$(wc -w <<<"$o1")"
[[ "$nwords" == 5 ]] || pnote "shuffle lost systems: $nwords"
o3="$(run_shuffle seed-y plain pipedpeer ray dask ssh)"
[[ "$o1" != "$o3" ]] || pnote "different seeds gave the same order (suspicious but not fatal)" || true
verdict "reruns reproduce the seeded system order")

# --- row 10: pipedpeer SMOKE n=2 receipt remote_items 0 -> BLOCKERS line ----
(d=$(newdir)
row_setup "$d"
out="$(STUB_REMOTE_ITEMS=0 run.sh SMOKE --workloads W2 --systems pipedpeer --sizes 2 --repeats 1 2>"$d/err")"
grep -q 'SMOKE W2 pipedpeer n=2 result_ok=' <<<"$out" || pnote "no SMOKE summary line: $(head -2 <<<"$out")"
grep 'SMOKE W2 pipedpeer n=2' <<<"$out" | grep -q 'remote_items=0' || pnote "remote_items not 0 in the summary"
grep -q 'remote_items=0' "$LAB/BLOCKERS.md" || pnote "BLOCKERS.md missing the receipt gate line"
grep -q 'result_ok=false' "$LAB/BLOCKERS.md" && pnote "result_ok gate fired wrongly (W2 receipt zero does not imply wrong result)" || true
grep -q 'remote_items=0' "$LAB/campaign.log" || pnote "gate not logged"
verdict "SMOKE receipt remote_items=0 at n=2 lands in BLOCKERS.md")

# --- row 11 (extra): weak mode (E1b) gets the majority override -------------
(d=$(newdir)
row_setup "$d"
out="$(run.sh E1b --workloads W1 --sizes 2 --repeats 1 2>"$d/err")"
n="$(json_count E1b)"
[[ "$n" == 4 ]] || pnote "expected 4 rows (4 systems, no warm-up), got $n"
maj="$(grep -c '"ref":"majority"' "$LAB/runs.jsonl")"
okc="$(grep -c '"result_ok":true' "$LAB/runs.jsonl")"
[[ "$maj" == 4 && "$okc" == 4 ]] || pnote "majority pass: ref=majority x$maj / ok x$okc (want 4/4)"
verdict "E1b weak: no reference -> rows flipped to ref=majority")


# --- C1: the real path when systemd-inhibit is present ---------------------
inhibit_stub() { # inhibit_stub <dir>: systemd-inhibit that drops its options and runs the command
	cat >"$1/bin/systemd-inhibit" <<'INH'
#!/usr/bin/env bash
while [[ "${1:-}" == --* ]]; do shift; done
exec "$@"
INH
	chmod +x "$1/bin/systemd-inhibit"
}

(d=$(newdir)
row_setup "$d"
inhibit_stub "$d"
env -u LAB_INHIBITED "$RUN_SH" SMOKE --workloads W1 --systems plain --sizes 1 >"$d/out" 2>"$d/err"
[[ "$(json_count SMOKE)" == 1 ]] || pnote "real run under systemd-inhibit recorded $(json_count SMOKE) runs, want 1: $(head -3 "$d/out")"
grep -q 'cluster up 1' "$d/stubs.log" 2>/dev/null || pnote "cluster.sh was not called: the dry list ran"
verdict "run.sh SMOKE without --dry-run under systemd-inhibit takes the real path")

(d=$(newdir)
row_setup "$d"
inhibit_stub "$d"
env -u LAB_INHIBITED "$RUN_SH" SMOKE --workloads W1 --systems plain --sizes 1 --dry-run >"$d/out" 2>"$d/err"
[[ ! -e "$d/runs.jsonl" && ! -e "$d/stubs.log" ]] || pnote "dry-run under systemd-inhibit ran something"
grep -q 'SMOKE-W1-plain-n1-r1' "$d/out" || pnote "dry list missing: $(head -2 "$d/out")"
verdict "run.sh SMOKE --dry-run under systemd-inhibit prints the dry list only")

# --- C2: size spec scaling ---------------------------------------------------
(d=$(newdir)
row_setup "$d"
sizes="$(run.sh E1 --workloads W1 --systems pipedpeer --repeats 1 --dry-run 2>"$d/err" | sed -n 's/.*-n\([0-9]*\)-r.*/\1/p' | sort -un | tr '\n' ' ')"
[[ "$sizes" == "1 2 4 8 16 24 32 " || "$sizes" == "1 2 4 8 16 24 32" ]] || pnote "dry sizes [$sizes], want 1 2 4 8 16 24 32"
run.sh E1 --workloads W1 --systems pipedpeer --repeats 1 >"$d/out" 2>"$d/err" || true
ups="$(sed -n 's/^cluster up //p' "$d/stubs.log" | tr '\n' ' ')"
[[ "$ups" == "1 2 4 8 16 24 32 " ]] || pnote "real run brought up [$ups], want 1 2 4 8 16 24 32"
verdict "size spec scaling is exactly 1,2,4,8,16,24,32, dry and real")

(d=$(newdir)
row_setup "$d"
sizes="$(run.sh SMOKE --sizes 2,4 --dry-run 2>"$d/err" | sed -n 's/.*-n\([0-9]*\)-r.*/\1/p' | sort -un | tr '\n' ' ')"
[[ "$sizes" == "2 4 " ]] || pnote "--sizes 2,4 gave [$sizes]"
verdict "--sizes 2,4 runs sizes 2 and 4")

# --- H3 / M2: the exit code in the record ------------------------------------
(d=$(newdir)
row_setup "$d"
lab_wait() { return 255; }
CAMP_STUB_SLEEP=2 campaign_run_one T W1 pipedpeer 1 1 0 30 strong >/dev/null 2>&1 || true
id=T-W1-pipedpeer-n1-r1
[[ "$(rec_field $id exit_code)" == null ]] || pnote "exit_code $(rec_field $id exit_code), want null"
[[ "$(rec_field $id note)" == '"lost-connection"' ]] || pnote "note $(rec_field $id note), want lost-connection"
[[ "$(rec_field $id result_ok)" == false ]] || pnote "result_ok $(rec_field $id result_ok), want false"
verdict "lab_wait 255 with no exit file: exit_code null, lost-connection, result_ok false")

(d=$(newdir)
row_setup "$d"
CAMP_STUB_EXIT=3 campaign_run_one T W1 pipedpeer 1 1 0 30 strong >/dev/null 2>&1 || true
id=T-W1-pipedpeer-n1-r1
[[ "$(rec_field $id exit_code)" == 3 ]] || pnote "exit_code $(rec_field $id exit_code), want 3"
[[ "$(rec_field $id result_ok)" == false ]] || pnote "result_ok $(rec_field $id result_ok), want false (matching output, failed job)"
verdict "job exits 3: exit_code 3, result_ok false")

(d=$(newdir)
row_setup "$d"
campaign_run_one T W1 pipedpeer 1 1 0 30 strong >/dev/null 2>&1 || true
id=T-W1-pipedpeer-n1-r1
[[ "$(rec_field $id exit_code)" == 0 ]] || pnote "exit_code $(rec_field $id exit_code), want 0"
[[ "$(rec_field $id result_ok)" == true ]] || pnote "result_ok $(rec_field $id result_ok), want true"
verdict "job exits 0: exit_code 0, result_ok true")

(d=$(newdir)
row_setup "$d"
lab_wait() { return 0; }
CAMP_STUB_SLEEP=2 campaign_run_one T W1 pipedpeer 1 1 0 30 strong >/dev/null 2>&1 || true
id=T-W1-pipedpeer-n1-r1
[[ "$(rec_field $id exit_code)" == null ]] || pnote "exit_code $(rec_field $id exit_code), want null"
[[ "$(rec_field $id result_ok)" == false ]] || pnote "result_ok $(rec_field $id result_ok), want false"
verdict "exit file absent with rc 0: exit_code null, never 0, result_ok false")

# --- T1: result_ok judges every workload on wrong and right values -----------
(d=$(newdir)
row_setup "$d"
STUB_W2_SV=9.9999 campaign_run_one T W2 pipedpeer 1 1 0 30 strong >/dev/null 2>&1 || true
STUB_W3_ACC=0.5000 campaign_run_one T W3 pipedpeer 1 1 0 30 strong >/dev/null 2>&1 || true
STUB_T1_LOSS=0.500000 campaign_run_one T T1 pipedpeer 1 1 0 30 strong >/dev/null 2>&1 || true
STUB_T1_LOSS=0.491000 campaign_run_one T2 T1 pipedpeer 1 1 0 30 strong >/dev/null 2>&1 || true
for w in W2 W3 T1; do
	[[ "$(rec_field "T-$w-pipedpeer-n1-r1" result_ok)" == false ]] || pnote "$w wrong value judged $(rec_field "T-$w-pipedpeer-n1-r1" result_ok), want false"
done
[[ "$(rec_field T2-T1-pipedpeer-n1-r1 result_ok)" == true ]] || pnote "T1 loss within 1% judged $(rec_field T2-T1-pipedpeer-n1-r1 result_ok), want true"
verdict "wrong W2/W3/T1 values are result_ok false; T1 within 1% is true")

# --- M7: SMOKE gate on a resumed (skipped) run -------------------------------
(d=$(newdir)
row_setup "$d"
STUB_REMOTE_ITEMS=0 campaign_run_one SMOKE W2 pipedpeer 2 1 0 30 strong >/dev/null 2>&1 || true
run.sh SMOKE --workloads W2 --systems pipedpeer --sizes 2 --repeats 1 >"$d/out" 2>"$d/err"
grep -q '^skipped ' "$d/out" || pnote "the run was not skipped: $(head -2 "$d/out")"
grep -q 'remote_items=0' "$LAB/BLOCKERS.md" 2>/dev/null || pnote "skipped SMOKE run did not reach the gate (no BLOCKERS line)"
run.sh SMOKE --workloads W2 --systems pipedpeer --sizes 2 --repeats 1 >/dev/null 2>&1
[[ "$(grep -c 'remote_items=0' "$LAB/BLOCKERS.md")" == 1 ]] || pnote "a second resume duplicated the BLOCKERS line"
verdict "SMOKE gate fires for a skipped run, once")

# --- F1: the summary fails when a row fails -----------------------------------
(d=$(newdir)
rc=0
RUN_TEST_FORCE_FAIL=1 bash "${BASH_SOURCE[0]}" >"$d/out" 2>&1 || rc=$?
((rc != 0)) || pnote "exit 0 although a row failed"
grep -qx 'FAIL 0/1 passed' "$d/out" || pnote "summary not FAIL 0/1: $(tail -2 "$d/out" | tr '\n' ' ')"
verdict "a failing row prints FAIL p/n and exits non-zero")

nrows=$(wc -l <"$RESULTS")
nfail=$(grep -c '^bad$' "$RESULTS" || true)
npass=$((nrows - nfail))
if ((nfail == 0)); then printf "PASS %d/%d\n" "$npass" "$nrows"; else printf "FAIL %d/%d passed\n" "$npass" "$nrows"; fi
((nrows > 0 && nfail == 0))
