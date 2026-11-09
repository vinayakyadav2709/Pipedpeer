#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2317,SC2034,SC2015,SC2016,SC2002,SC2143  # rows run in subshells
# Case table for the batch-5 runner changes (brief step 5, unit RN1):
# the new workloads and systems in systems.sh, the run record's new branches,
# the timeout rules, CAMPAIGN_SUBMIT_PC, the per-workload metrics tag, the
# version lookup and the torchrun port. Fake lab only (test/fakelab.sh), each
# row in its own subshell with its own HOME; a fake systemctl guards the unit
# stop, so no row can reach a real service manager.
#
# One ok/FAIL line per row; "PASS k/k" and exit 0 only when every row passed.
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
RUN_SH="$CAMP/run.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
# shellcheck source=/dev/null
source "$CAMP/test/fakelab.sh"

RESULTS="$(mktemp)"
DIRS_FILE="$RESULTS.dirs"
problems=()

kill_dir_procs() {
	local e p
	for e in /proc/[0-9]*/environ; do
		p="${e#/proc/}"; p="${p%/environ}"
		[[ "$p" == "$$" || "$p" == "$BASHPID" ]] && continue
		if tr '\0' '\n' 2>/dev/null <"$e" | grep -qE -- "=$1([/:]|\$)"; then
			kill -KILL "$p" 2>/dev/null || true
		fi
	done
}
cleanup() {
	local d
	if [[ "${LAB_TEST_KEEP:-0}" == 1 ]]; then
		printf 'kept: %s\n' "$(tr '\n' ' ' <"$DIRS_FILE" 2>/dev/null)" >&2
		return
	fi
	while IFS= read -r d; do
		[[ -n "$d" ]] || continue
		kill_dir_procs "$d"
		rm -rf "$d"
	done <"$DIRS_FILE" 2>/dev/null
	rm -f "$RESULTS" "$DIRS_FILE"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

ok() { echo ok >>"$RESULTS"; printf 'ok   %s\n' "$1"; }
bad() { echo bad >>"$RESULTS"; printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }
verdict() {
	if ((${#problems[@]} == 0)); then ok "$1"; else bad "$1" "${problems[*]}"; fi
	problems=()
}
newdir() {
	local d
	d="$(mktemp -d "$STATE/campaign-batch-test.XXXXXX")"
	mkdir -p "$d/home" "$d/bin"
	printf '%s\n' "$d" >>"$DIRS_FILE"
	printf '%s\n' "$d"
}

# row_setup <dir>: fake lab (3 PCs), sizes.env with the batch keys, stubs for
# cluster/metrics/upload, a fake systemctl, a pipedpeer stub that prints
# "(version=abc)", a venv python that sleeps CAMP_STUB_SLEEP seconds. Then
# the row sources run.sh.
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
	unset CAMPAIGN_TIMEOUT_CAP_S CAMPAIGN_DEFAULT_TIMEOUT_S CAMPAIGN_SUBMIT_PC
	cat >"$dir/sizes.env" <<'SIZES'
BENCH_PROCS=8
W1_ITEMS=4800
W1_WEAK_PER_PC=48
W2_ITEMS=128
W3_TREES=150
T1_EPOCHS=30
SWEEP_ITERS_PER_MS=40
SWEEP_TOTAL_MS=960000
SWEEP_DATA_ITEMS=64
SWEEP_DATA_TASK_MS=2000
NOOP_ITEMS=50000
LAT_CALLS=1000
ELASTIC_TASK_MS=100
ELASTIC_ITEMS=19200
FILEIO_FILES=4096
FILEIO_FILE_KB=64
SIZES
	mkdir -p "$dir/reference"
	printf 'SWEEP-OK items=960000 seconds=1.0000 checksum=42.000000\n' >"$dir/reference/TS1-strong.txt"
	# fake systemctl: logs, fails when $dir/systemctl.fail exists
	cat >"$dir/bin/systemctl" <<'SYSCTL'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "fake systemctl: FAKELAB_DIR unset" >&2; exit 97; }
printf 'systemctl %s\n' "$*" >>"$FAKELAB_DIR/systemctl.log"
[[ -e "$FAKELAB_DIR/systemctl.fail" ]] && { echo "Failed to stop unit" >&2; exit 5; }
exit 0
SYSCTL
	cat >"$dir/bin/pipedpeer" <<'PPSTUB'
#!/usr/bin/env bash
case "${1:-}" in
	status) echo "daemon running (pid=1600 node-id=9078 port=4242 version=${STUB_VERSION:-abc})"; echo ;;
	*) exit 2 ;;
esac
PPSTUB
	chmod +x "$dir/bin/systemctl" "$dir/bin/pipedpeer"
	mkdir -p "$dir/pc00/pipedpeer-lab/venv/bin"
	cat >"$dir/pc00/pipedpeer-lab/venv/bin/python" <<'PYSTUB'
#!/usr/bin/env bash
[[ -n "${CAMP_STUB_SLEEP:-}" ]] && sleep "$CAMP_STUB_SLEEP"
echo "POOLMAP-OK items=4800 seconds=1.0000 checksum=42.0"
PYSTUB
	chmod +x "$dir/pc00/pipedpeer-lab/venv/bin/python"
	export CLUSTER_SH="$dir/bin/cluster.sh" METRICS_SH="$dir/bin/metrics.sh" UPLOAD_SH="$dir/bin/upload.sh"
	printf '#!/usr/bin/env bash\necho "cluster $*" >>"%s/stubs.log"\n' "$dir" >"$CLUSTER_SH"
	printf '#!/usr/bin/env bash\necho "metrics $*" >>"%s/stubs.log"\n' "$dir" >"$METRICS_SH"
	printf '#!/usr/bin/env bash\necho "upload" >>"%s/stubs.log"\n' "$dir" >"$UPLOAD_SH"
	chmod +x "$CLUSTER_SH" "$METRICS_SH" "$UPLOAD_SH"
	# shellcheck source=/dev/null
	source "$RUN_SH"
	set +e
	set -a
	# shellcheck source=/dev/null
	. "$dir/sizes.env"
	set +a
}

# mkrun <dir> <run_id> <exit> [result line...]: a fetched run dir on pc00.
mkrun() {
	local dir="$1" id="$2" code="$3" r
	shift 3
	r="$dir/hosts/pc00/runs/$id"
	mkdir -p "$r"
	printf '%s\n' "$code" >"$r/exit"
	echo 1000000000 >"$r/t0"
	echo 2000000000 >"$r/t1"
	: >"$r/out"
	for l in "$@"; do printf '%s\n' "$l" >>"$r/out"; done
}

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

record() { # record <id> <w> <system> [exp]
	_run_record "$1" "${4:-X}" "$2" "$3" 8 1 0 "" "" "pc00" >/dev/null 2>&1
}

SWEEP_OK='SWEEP-OK items=960000 seconds=2.5000 checksum=42.000000'

# --- sys_cmd rows ---------------------------------------------------------
(d=$(newdir); row_setup "$d"
c="$(sys_cmd pipedpeer TS1 8 r1 2>&1)"
[[ "$c" == *"-e BENCH_TASK_MS=1 "* ]] || pnote "no -e BENCH_TASK_MS=1: $c"
[[ "$c" == *"-e BENCH_ITEMS=960000"* ]] || pnote "items not TOTAL/ms: $c"
[[ "$c" == *"sweep_map.py"* ]] || pnote "script not sweep_map.py: $c"
[[ "$c" != *--distribute* ]] || pnote "TS1 must not force distribution: $c"
verdict "sys_cmd pipedpeer TS1: task_ms env, no --distribute")

(d=$(newdir); row_setup "$d"
c="$(sys_cmd pipedpeer noop 8 r1 2>&1)"
[[ "$c" == *"--distribute force"* ]] || pnote "noop lacks --distribute force: $c"
c2="$(sys_cmd pipedpeer lat 8 r1 2>&1)"
[[ "$c2" == *"--distribute force"* ]] || pnote "lat lacks --distribute force: $c2"
verdict "sys_cmd pipedpeer noop/lat: --distribute force")

(d=$(newdir); row_setup "$d"
c="$(sys_cmd pipedpeer-bare SB 1 r1 2>&1)"
[[ "$c" == *"pipedpeer run --isolate=false "* ]] || pnote "no run --isolate=false: $c"
[[ "$c" == *"sweep_map.py"* ]] || pnote "script not sweep_map.py: $c"
c2="$(sys_cmd pipedpeer-bare fileio 1 r1 2>&1)"
[[ "$c2" == *"--isolate=false"* && "$c2" == *"fileio.py"* ]] || pnote "fileio bare: $c2"
verdict "sys_cmd pipedpeer-bare SB/fileio: --isolate=false and the script")

(d=$(newdir); row_setup "$d"
c="$(sys_cmd pipedpeer W1 4 r1 2>&1)"
[[ "$c" != *--isolate* && "$c" != *--distribute* ]] || pnote "W1 changed: $c"
[[ "$c" == "pipedpeer run  -e BENCH_ITEMS=4800 -e BENCH_PROCS=8  pool_map.py && "* ]] || pnote "W1 text changed: $c"
verdict "sys_cmd pipedpeer W1: unchanged text")

(d=$(newdir); row_setup "$d"
c="$(sys_cmd ray idle 2 r1 2>&1)"
[[ "$c" == *"baselines/idle_ray.py"* ]] || pnote "idle ray: $c"
verdict "sys_cmd ray idle: idle_ray.py")

(d=$(newdir); row_setup "$d"
c="$(sys_cmd ray-pool W1 2 r1 2>&1)"
[[ "$c" == *"baselines/w1_ray_pool.py"* && "$c" == *RAY_ADDRESS=* ]] || pnote "ray-pool W1: $c"
verdict "sys_cmd ray-pool W1: w1_ray_pool.py")

(d=$(newdir); row_setup "$d"
rc=0; c="$(sys_cmd ray-pool W2 2 r1 2>&1)" || rc=$?
((rc != 0)) || pnote "ray-pool W2 should fail: $c"
verdict "sys_cmd ray-pool W2: error")

(d=$(newdir); row_setup "$d"
c="$(sys_cmd torchrun T1 2 r1 2>&1)"
[[ "$c" == *"t1_torchrun.sh 2"* && "$c" == *BENCH_SEED=* && "$c" == *hosts-2.txt* ]] || pnote "torchrun: $c"
[[ "$c" == *BENCH_EPOCHS=30* ]] || pnote "no epochs: $c"
verdict "sys_cmd torchrun T1: script, seed, hosts file")

(d=$(newdir); row_setup "$d"
s="$(_sys_sizeenv DS20M 8 2>&1)"
[[ "$s" == *"BENCH_PAYLOAD_BYTES=20000000"* && "$s" == *"BENCH_ITEMS=64"* && "$s" == *"BENCH_TASK_MS=2000"* ]] || pnote "DS20M: $s"
verdict "_sys_sizeenv DS20M: payload 20000000")

(d=$(newdir); row_setup "$d"
sed -i '/^SWEEP_TOTAL_MS/d' "$d/sizes.env"; unset SWEEP_TOTAL_MS
rc=0; s="$(_sys_sizeenv TS10 8 2>&1)" || rc=$?
((rc != 0)) || pnote "should fail without SWEEP_TOTAL_MS"
[[ "$s" == *SWEEP_TOTAL_MS* ]] || pnote "error does not name the key: $s"
verdict "sizes.env lacking SWEEP_TOTAL_MS: error names the key")

# --- run record rows ------------------------------------------------------
(d=$(newdir); row_setup "$d"
mkrun "$d" a-noop 0 'NOOP-OK items=50000 seconds=2.0000 tasks_per_s=25000.0'
mkrun "$d" b-noop 0
mkrun "$d" c-noop 7 'NOOP-OK items=50000 seconds=2.0000 tasks_per_s=25000.0'
record a-noop noop pipedpeer; record b-noop noop pipedpeer; record c-noop noop pipedpeer
[[ "$(rec_field a-noop result_ok)" == true ]] || pnote "noop 1 line: $(rec_field a-noop result_ok)"
[[ "$(rec_field b-noop result_ok)" == false ]] || pnote "noop 0 lines: $(rec_field b-noop result_ok)"
[[ "$(rec_field b-noop exit_code)" == 0 ]] || pnote "exit code lost"
[[ "$(rec_field c-noop result_ok)" == false && "$(rec_field c-noop exit_code)" == 7 ]] || pnote "noop failed run kept ok"
verdict "noop record: 1 line true, 0 lines false (exit kept), failed false")

(d=$(newdir); row_setup "$d"
mkrun "$d" l-lat 0 'LAT-OK n=1000 p50_ms=1.0 p90_ms=2.0 p99_ms=3.0'
mkrun "$d" f-io 0 'FILEIO-OK files=4096 bytes=268435456 seconds=3.0000'
record l-lat lat ray; record f-io fileio pipedpeer-bare
[[ "$(rec_field l-lat result_ok)" == true && "$(rec_field f-io result_ok)" == true ]] || pnote "lat/fileio not ok"
verdict "lat and fileio records: 1 line true")

(d=$(newdir); row_setup "$d"
mkrun "$d" t-ok 0 "$SWEEP_OK"
mkrun "$d" t-bad 0 'SWEEP-OK items=960000 seconds=2.5000 checksum=43.000000'
record t-ok TS1 ray; record t-bad TS1 ray
[[ "$(rec_field t-ok result_ok)" == true ]] || pnote "TS1 equal: $(rec_field t-ok result_ok)"
[[ "$(rec_field t-bad result_ok)" == false ]] || pnote "TS1 differs: $(rec_field t-bad result_ok)"
[[ "$(rec_field t-ok ref)" == '"reference"' ]] || pnote "ref not reference"
verdict "TS1 checksum equals REF true, differs false")

(d=$(newdir); row_setup "$d"
mkrun "$d" el 0 "$SWEEP_OK"
record el EL pipedpeer REF
[[ "$(rec_field el result_ok)" == null ]] || pnote "EL result_ok: $(rec_field el result_ok)"
[[ ! -e "$LAB/reference/EL-strong.txt" ]] || pnote "EL wrote a reference"
mkrun "$d" nr 0 'NOOP-OK items=5 seconds=1.0000 tasks_per_s=5.0'
record nr noop plain REF
[[ ! -e "$LAB/reference/noop-strong.txt" ]] || pnote "noop wrote a reference"
mkrun "$d" sr 0 "$SWEEP_OK"
record sr TS10 plain REF
[[ -s "$LAB/reference/TS10-strong.txt" ]] || pnote "a TS10 REF run wrote no reference"
verdict "EL: result_ok null, no reference; noop no reference; TS10 REF writes one")

(d=$(newdir); row_setup "$d"
j="$(_run_size_json TS10 8 2>&1)"
[[ "$j" == '{"items":96000,"task_ms":10,"payload_bytes":0}' ]] || pnote "TS10 json: $j"
j="$(_run_size_json DS1M 8 2>&1)"
[[ "$j" == '{"items":64,"task_ms":2000,"payload_bytes":1000000}' ]] || pnote "DS1M json: $j"
verdict "_run_size_json: sweep keys items, task_ms, payload_bytes")

# --- timeouts, cap, submit PC --------------------------------------------
(d=$(newdir); row_setup "$d"
lab_wait() { echo "$3" >>"$d/wait.args"; echo 0; }
campaign_run_one T W1 plain 1 1 0 >/dev/null 2>&1
[[ "$(cat "$d/wait.args" 2>/dev/null)" == 3600 ]] || pnote "default timeout: $(cat "$d/wait.args" 2>/dev/null)"
CAMPAIGN_DEFAULT_TIMEOUT_S=77 campaign_run_one T2 W1 plain 1 1 0 >/dev/null 2>&1
[[ "$(tail -1 "$d/wait.args")" == 77 ]] || pnote "env default timeout: $(tail -1 "$d/wait.args")"
verdict "campaign_run_one without a timeout uses 3600 (or the env default)")

(d=$(newdir); row_setup "$d"
rc=0
CAMP_STUB_SLEEP=5 campaign_run_one T W1 plain 1 1 0 1 strong >/dev/null 2>&1 || rc=$?
[[ "$rc" == 124 ]] || pnote "rc $rc, want 124"
row="$(cat "$LAB/runs.jsonl")"
grep -q '"exit_code":124' <<<"$row" && grep -q '"note":"timeout"' <<<"$row" || pnote "not recorded as timeout: $row"
grep -q 'systemctl --user stop lab-T-W1-plain-n1-r1' "$d/systemctl.log" 2>/dev/null || pnote "no stop for the unit: $(cat "$d/systemctl.log" 2>/dev/null)"
verdict "run past its timeout: 124, note timeout, stop reached the unit")

(d=$(newdir); row_setup "$d"
touch "$d/systemctl.fail"
rc=0
CAMP_STUB_SLEEP=5 campaign_run_one T W1 plain 1 1 0 1 strong >/dev/null 2>"$d/err" || rc=$?
[[ "$rc" == 124 ]] || pnote "rc $rc, want 124 (campaign continues)"
grep -q '"note":"timeout"' "$LAB/runs.jsonl" || pnote "run not recorded"
grep -qi 'stop' "$LAB/campaign.log" 2>/dev/null || grep -rqi 'stop' "$d"/*.log 2>/dev/null || grep -qi 'stop' "$d/err" || pnote "failed stop not logged"
verdict "timeout, stop fails: logged, the campaign continues")

(d=$(newdir); row_setup "$d"
printf 'experiment\tworkload\tsystems\tsizes\trepeats\twarmup\ttimeout_s\tsize_mode\nQ1\tW1\tplain\t1\t1\t0\t3600\tstrong\nQ2\tW1\tplain\t1\t1\t0\t60\tstrong\n' >"$d/x.tsv"
EXPERIMENTS="$d/x.tsv"
campaign_run_one() { echo "$1 $7" >>"$d/to.args"; echo "RESULT $1 ok=true remote_items=-"; }
CAMPAIGN_TIMEOUT_CAP_S=300 run_all Q1 >/dev/null 2>&1
CAMPAIGN_TIMEOUT_CAP_S=300 run_all Q2 >/dev/null 2>&1
grep -q '^Q1 300$' "$d/to.args" || pnote "cap 300, tsv 3600: $(cat "$d/to.args")"
grep -q '^Q2 60$' "$d/to.args" || pnote "cap 300, tsv 60: $(cat "$d/to.args")"
run_all Q1 >/dev/null 2>&1
[[ "$(grep -c '^Q1 3600$' "$d/to.args")" == 1 ]] || pnote "no cap should wait the tsv value"
verdict "CAMPAIGN_TIMEOUT_CAP_S: min(tsv, cap), unset keeps tsv")

(d=$(newdir); row_setup "$d"
lab_detach() { echo "detach $1" >>"$d/pcs"; }
lab_wait() { echo "wait $1" >>"$d/pcs"; echo 0; }
lab_fetch() { echo "fetch $1 $3" >>"$d/pcs"; mkdir -p "$3"; }
CAMPAIGN_SUBMIT_PC=pc01 campaign_run_one S W1 plain 1 1 0 30 >/dev/null 2>&1
grep -q '^detach pc01$' "$d/pcs" && grep -q '^wait pc01$' "$d/pcs" && grep -q "^fetch pc01 $LAB/hosts/pc01/runs/S-W1-plain-n1-r1\$" "$d/pcs" || pnote "pc01: $(cat "$d/pcs")"
! grep -q 'pc00' "$d/pcs" || pnote "pc00 touched: $(cat "$d/pcs")"
: >"$d/pcs"
campaign_run_one S2 W1 plain 1 1 0 30 >/dev/null 2>&1
grep -q '^detach pc00$' "$d/pcs" && ! grep -q 'pc01' "$d/pcs" || pnote "default not pc00: $(cat "$d/pcs")"
verdict "CAMPAIGN_SUBMIT_PC: pc01 only when set, pc00 by default")

# --- metrics tag, version -------------------------------------------------
(d=$(newdir); row_setup "$d"
campaign_run_one() { echo "RESULT $1 ok=true remote_items=-"; }
run_all E1 --workloads W1,W2 --systems plain --sizes 1 --repeats 1 >/dev/null 2>&1
grep -q 'metrics start E1-W1-n1' "$d/stubs.log" && grep -q 'metrics start E1-W2-n1' "$d/stubs.log" || pnote "tags: $(grep metrics "$d/stubs.log")"
grep -q 'metrics stop E1-W2-n1' "$d/stubs.log" || pnote "stop tag missing"
! grep -qE 'metrics (start|stop) E1-n1' "$d/stubs.log" || pnote "old tag still used"
verdict "E1 W1 then W2: one metrics tag per workload")

(d=$(newdir); row_setup "$d"
v="$(_get_version 2>/dev/null)"
[[ "$v" == abc ]] || pnote "version [$v], want abc"
verdict "pipedpeer status on pc00 prints (version=abc): abc, no )")

# --- torchrun port --------------------------------------------------------
TR="$REPO/docs/benchmarks/workloads/baselines/t1_torchrun.sh"
torch_setup() {
	local d="$1"
	mkdir -p "$d/tbin" "$d/xdg/pipedpeer"
	printf '10.0.0.1\n10.0.0.2\n' >"$d/hosts-2.txt"
	# fake ssh: the last argument is the remote command. Rank 1 (node-rank=1)
	# exits with ${FAKE_RANK1_RC}; rank 0 records its pid and runs a long sleep.
	cat >"$d/tbin/ssh" <<'FSSH'
#!/usr/bin/env bash
cmd="${*: -1}"
printf '%s\n' "$*" >>"$FAKE_SSH_LOG"
if [[ "$cmd" == *node-rank=1* ]]; then
	sleep 1
	[[ "${FAKE_RANK1_RC:-0}" == 0 ]] && echo "rank1 out"
	exit "${FAKE_RANK1_RC:-0}"
fi
echo $$ >"$FAKE_RANK0_PID"
if [[ "${FAKE_RANK1_RC:-0}" != 0 ]]; then exec sleep 30; fi
sleep 1
echo "final loss: 0.49"
FSSH
	chmod +x "$d/tbin/ssh"
}
torch_run() {
	local d="$1"
	PATH="$d/tbin:$PATH" XDG_STATE_HOME="$d/xdg" FAKE_SSH_LOG="$d/ssh.log" FAKE_RANK0_PID="$d/r0.pid" \
		LAB_HOSTS_FILE="$d/hosts-2.txt" LAB_USER=lab bash "$TR" 2 >"$d/tout" 2>"$d/terr"
}

(d=$(newdir); torch_setup "$d"
s0=$SECONDS; rc=0
FAKE_RANK1_RC=3 torch_run "$d" || rc=$?
((rc != 0)) || pnote "should exit non-zero"
((SECONDS - s0 <= 5)) || pnote "took $((SECONDS - s0)) s"
pid="$(cat "$d/r0.pid" 2>/dev/null || true)"
[[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null || pnote "rank 0 (pid ${pid:-none}) still alive"
[[ -n "$pid" ]] && kill -KILL "$pid" 2>/dev/null || true
verdict "torchrun fake rank 1 exits 3: non-zero within 5 s, rank 0 killed")

(d=$(newdir); torch_setup "$d"
rc=0
FAKE_RANK1_RC=0 torch_run "$d" || rc=$?
((rc == 0)) || pnote "rc $rc: $(cat "$d/terr")"
grep -q 'final loss' "$d/tout" || pnote "rank 0 output not printed"
verdict "torchrun both ranks 0: exit 0")

(d=$(newdir); torch_setup "$d"
FAKE_RANK1_RC=0 BENCH_SEED=13 BENCH_EPOCHS=7 torch_run "$d" || true
l="$(cat "$d/ssh.log")"
[[ "$l" == *"-i "*pipedpeer-lab-pc00* && "$l" == *BatchMode=yes* && "$l" == *StrictHostKeyChecking=accept-new* ]] || pnote "ssh options: $l"
[[ "$l" == *BENCH_SEED=13* && "$l" == *BENCH_EPOCHS=7* && "$l" == *"timeout 3000"* ]] || pnote "remote env/timeout: $l"
verdict "torchrun ssh: pc00 key, no ssh config, seed and epochs inlined, rank timeout")

nrows=$(wc -l <"$RESULTS")
nfail=$(grep -c '^bad$' "$RESULTS" || true)
npass=$((nrows - nfail))
if ((nfail == 0)); then printf 'PASS %d/%d\n' "$npass" "$nrows"; else printf 'FAIL %d/%d passed\n' "$npass" "$nrows"; fi
((nrows > 0 && nfail == 0))
