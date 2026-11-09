#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2034,SC2317
# X17 case table (E17 introducer loss, E18 failure detection) on the fake lab.
# Nothing touches a real PC or systemd manager: the fake lab's ssh/rsync/pssh
# stubs run everything under <dir>/pcNN, and pipedpeer, systemctl and the venv
# python are stubs first on PATH. Both scripts are driven as they are run on
# the lab; E17's rows call e17_main with campaign_run_one replaced by a copy
# that honours CAMPAIGN_SUBMIT_PC (RN1's contract), because the checked-out
# run.sh still submits on pc00 and E17 needs the job on the surviving member.
# That substitution is named here and nowhere else: everything else in e17.sh
# is the real code.
set -euo pipefail
REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
BASE_PATH="$PATH"
pass=0
failed=0
DIRS_FILE="$(mktemp)"
cleanup() {
	local d e p
	while IFS= read -r d; do
		[[ -n "$d" ]] || continue
		pkill -f -- "$d/" 2>/dev/null || true
		for e in /proc/[0-9]*/environ; do
			p="${e#/proc/}"
			p="${p%/environ}"
			[[ "$p" == "$$" || "$p" == "$BASHPID" ]] && continue
			if tr '\0' '\n' 2>/dev/null <"$e" | grep -qE -- "=$d([/:]|$)"; then kill -KILL "$p" 2>/dev/null || true; fi
		done
		rm -rf "$d"
	done <"$DIRS_FILE"
	rm -f "$DIRS_FILE"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
ok() { pass=$((pass + 1)); echo "ok   $1"; }
bad() { failed=$((failed + 1)); echo "FAIL $1 -- $2"; }
newdir() { local d; d="$(mktemp -d "$STATE/e-e17-18-test.XXXXXX")"; mkdir -p "$d/home"; echo "$d" >>"$DIRS_FILE"; echo "$d"; }
rbad() { echo "FAIL $*"; }
verdict_row() { # <name> <captured output> <stderr file>
	if [[ "$2" == *ROW-OK* && "$2" != *FAIL* ]]; then
		ok "$1"
	else
		bad "$1" "$(printf '%s' "$2" | tr '\n' ' ') $(tail -2 "$3" 2>/dev/null | tr '\n' ' ')"
	fi
}

# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

# setup_lab <dir> <n>: fake lab, sizes/reference, and the stubs both scripts
# reach through ssh: pipedpeer, systemctl, cluster.sh and the venv python.
setup_lab() {
	local dir="$1" n="$2" i pc v
	v="$dir/verif"
	fakelab_init "$dir" "$n" || exit 90
	mkdir -p "$v"
	# lab_wait does integer arithmetic on LAB_POLL_S; the scripts' own polls
	# (E17_POLL_S, E18_POLL_S) are the sub-second ones.
	export HOME="$dir/home" LAB_RESULTS="$dir" LAB_RETRY_SLEEP=0.1 LAB_POLL_S=1
	export CLUSTER_SH="$v/cluster.sh" UPLOAD_SH="$v/upload.sh"
	cat >"$v/pipedpeer" <<'PP'
#!/usr/bin/env bash
# test stub pipedpeer: start/stop/status/invite/connect/run/jobs
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "pipedpeer stub: no FAKELAB_DIR" >&2; exit 97; }
me="$(basename "$HOME")"
printf '%s pipedpeer %s\n' "$me" "$*" >>"$FAKELAB_DIR/pipedpeer.log"
case "${1:-}" in
	start) printf '%s\n' "${PIPEDPEER_NO_LAN_DISCOVERY-unset}" >"$HOME/.no_lan_env" ;;
	stop) rm -f "$HOME/.no_lan_env" ;;
	status)
		calls=0
		[[ -f "$HOME/.status_calls" ]] && calls="$(cat "$HOME/.status_calls")"
		calls=$((calls + 1))
		echo "$calls" >"$HOME/.status_calls"
		f="$FAKELAB_DIR/status.$me"
		[[ -f "$f" ]] || f="$FAKELAB_DIR/status.txt"
		if [[ -f "$f" ]]; then cat "$f"; exit 0; fi
		[[ "${FAKE_E18_GEN:-0}" == 1 ]] || { echo "pipedpeer stub: no status file" >&2; exit 1; }
		printf 'NAME STATE PATH CPU MEM FREE GPU JOBS NOTE\n'
		while IFS=$'\t' read -r id _ip _user host _nproc _ram; do
			[[ -z "$id" || "$id" == \#* ]] && continue
			st=healthy
			if [[ "$id" == "${FAKE_TARGET_PC:-}" ]]; then
				if [[ -n "${FAKE_TARGET_HEALTHY_POLLS:-}" ]] && ((calls > FAKE_TARGET_HEALTHY_POLLS)); then st=unreachable; fi
				[[ "${FAKE_TARGET_NEVER:-0}" == 1 ]] && st=healthy
			fi
			if [[ "${FAKE_OTHER_UNHEALTHY:-0}" == 1 && "$id" == "${FAKE_OTHER_PC:-pc02}" ]]; then st=unreachable; fi
			printf '%-10s%-12s%-6s%-8s%-10s%-5s%-3s%s\n' "$host" "$st" self '8c 1%' '7.2 GiB' none 0 -
		done <"$FAKELAB_DIR/hosts.tsv" ;;
	invite)
		if [[ "$me" == pc00 ]]; then echo "pipedpeer connect OLD-CODE-$me"; else echo "pipedpeer connect NEW-CODE-$me"; fi ;;
	connect)
		[[ -n "${FAKE_CONNECT_SLEEP:-}" ]] && sleep "$FAKE_CONNECT_SLEEP"
		code="${2:-}"
		if [[ "${FAKE_OLD_JOINS:-0}" == 1 && "$code" == *OLD* ]]; then echo connected >"$HOME/.joined"; exit 0; fi
		[[ "${FAKE_CONNECT_FAIL:-0}" == 1 ]] && exit 1
		[[ "$code" == *OLD* ]] && { echo "old invite: no route" >&2; exit 1; }
		echo connected >"$HOME/.joined"
		if [[ -n "${FAKE_JOINED_ROWS:-}" ]]; then
			{ printf 'NAME STATE PATH CPU MEM FREE GPU JOBS NOTE\n'
				for ((k = 0; k < FAKE_JOINED_ROWS; k++)); do
					printf '%-10s%-12s%-6s%-8s%-10s%-5s%-3s%s\n' "pc0$k.lab" healthy self '8c 1%' '7.2 GiB' none 0 -
				done
				printf '\n'; } >"$FAKELAB_DIR/status.$me"
		fi
		exit 0 ;;
	run)
		small=0
		for a in "$@"; do [[ "$a" == BENCH_ITEMS=64 ]] && small=1; done
		sleep_s="${FAKE_RUN_SLEEP:-0}"
		((small)) && sleep_s="${FAKE_SMALL_SLEEP:-0.2}"
		[[ "$sleep_s" == 0 ]] || sleep "$sleep_s"
		[[ "${FAKE_RUN_FAIL:-0}" == 1 ]] && { echo boom >&2; exit 1; }
		echo "POOLMAP-OK items=10 seconds=1.0000 checksum=5.000000"
		if [[ -f "$FAKELAB_DIR/receipt.json" ]]; then
			jd="$HOME/.local/share/pipedpeer/jobs/$(date +%s%N)"
			mkdir -p "$jd"
			cp "$FAKELAB_DIR/receipt.json" "$jd/receipt.json"
		fi ;;
	jobs) printf '{"run_id":"x","steps":[]}\n' ;;
	*) echo "pipedpeer stub: unknown call: $*" >&2; exit 64 ;;
esac
PP
	cat >"$v/systemctl" <<'SC'
#!/usr/bin/env bash
# test stub systemctl: log; FAKE_KILL_FAIL fails a SIGKILL
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "systemctl stub: no FAKELAB_DIR" >&2; exit 97; }
printf '%s systemctl %s\n' "$(basename "$HOME")" "$*" >>"$FAKELAB_DIR/systemctl.log"
if [[ "${FAKE_KILL_FAIL:-0}" == 1 && "$*" == *"kill -s SIGKILL"* ]]; then exit 1; fi
exit 0
SC
	cat >"$v/cluster.sh" <<'CL'
#!/usr/bin/env bash
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "cluster stub: no FAKELAB_DIR" >&2; exit 97; }
printf 'cluster %s\n' "$*" >>"$FAKELAB_DIR/cluster.log"
case "${1:-}" in
	up) echo "CLUSTER-UP n=$2" ;;
	down) echo CLUSTER-DOWN ;;
esac
exit 0
CL
	cat >"$v/upload.sh" <<'UP'
#!/usr/bin/env bash
printf 'upload %s\n' "$*" >>"$FAKELAB_DIR/upload.log"
exit 0
UP
	for ((i = 0; i < n; i++)); do
		pc="$(printf 'pc%02d' "$i")"
		mkdir -p "$dir/$pc/pipedpeer-lab/venv/bin"
		cat >"$dir/$pc/pipedpeer-lab/venv/bin/python" <<'PY'
#!/usr/bin/env bash
# test stub venv python: answers E18's Ray/Dask state queries from counters
[[ -n "${FAKELAB_DIR:-}" ]] || { echo "venv python stub: no FAKELAB_DIR" >&2; exit 97; }
code=" $* "
case "$code" in
	*ray.nodes*)
		n=0; [[ -f "$HOME/.ray_calls" ]] && n="$(cat "$HOME/.ray_calls")"
		n=$((n + 1)); echo "$n" >"$HOME/.ray_calls"
		if ((n > ${FAKE_RAY_ALIVE_AFTER:-999999})); then echo 0; else echo 1; fi ;;
	*scheduler_info*)
		n=0; [[ -f "$HOME/.dask_calls" ]] && n="$(cat "$HOME/.dask_calls")"
		n=$((n + 1)); echo "$n" >"$HOME/.dask_calls"
		if ((n > ${FAKE_DASK_ALIVE_AFTER:-999999})); then echo 0; else echo 1; fi ;;
	*) echo 1 ;;
esac
PY
		chmod +x "$dir/$pc/pipedpeer-lab/venv/bin/python"
	done
	chmod +x "$v"/*
	export PATH="$v:$dir/bin:$BASE_PATH"
	printf 'W1_ITEMS=10\nW1_WEAK_PER_PC=5\nBENCH_PROCS=0\n' >"$dir/sizes.env"
	mkdir -p "$dir/reference"
	echo "POOLMAP-OK items=10 seconds=1.0000 checksum=5.000000" >"$dir/reference/W1-strong.txt"
	printf '%s' '{"remote_items":64,"local_items":0}' >"$dir/receipt.json"
	: >"$dir/campaign.log"
}

# write_status <dir> <pc> <rows>: a healthy-table fixture for that PC.
write_status() {
	local dir="$1" pc="$2" rows="$3" i
	{
		printf 'NAME STATE PATH CPU MEM FREE GPU JOBS NOTE\n'
		for ((i = 0; i < rows; i++)); do
			printf '%-10s%-12s%-6s%-8s%-10s%-5s%-3s%s\n' "pc0$i.lab" healthy self '8c 1%' '7.2 GiB' none 0 -
		done
		printf '\n'
	} >"$dir/status.$pc"
}

# campaign_run_one with CAMPAIGN_SUBMIT_PC, as RN1's contract has it. The rest
# is run.sh's own sequence, copied line for line from campaign_run_one.
e17_test_campaign_run_one() {
	local exp="$1" w="$2" sys="$3" n="$4" rep="$5" warm="$6"
	local timeout_s="${7:-0}"
	PYRUN_MODE="${8:-strong}"
	_load_sizes >/dev/null 2>&1 || true
	local run_id hfile dir rc=0 code cmd prefix envs rec_rc="" note=""
	local -a hosts=()
	local pc="${CAMPAIGN_SUBMIT_PC:-pc00}"
	run_id="$(_run_id "$exp" "$w" "$sys" "$n" "$rep" "$warm")"
	if _run_id_in_runs "$run_id"; then
		printf 'skipped %s\n' "$run_id"
		return 0
	fi
	hfile="$(lab_hosts "$n")" || return 1
	mapfile -t hosts <"$hfile"
	envs="$(_sys_sizeenv "$w" "$n")"
	prefix=""
	[[ -n "$envs" ]] && prefix="export $envs; "
	cmd="$(sys_cmd "$sys" "$w" "$n" "$run_id")"
	lab_detach "$pc" "$run_id" "$prefix$cmd" || return 1
	code="$(lab_wait "$pc" "$run_id" "$timeout_s")" || rc=$?
	if ((rc == 124)); then
		note="timeout"
		rec_rc=124
	elif ((rc != 0)); then
		note="lost-connection"
		lab_log "test run $run_id: lab_wait returned $rc"
	fi
	dir="$LAB/hosts/$pc/runs/$run_id"
	lab_fetch "$pc" "$run_id" "$dir" || lab_log "test run $run_id: fetch failed"
	# run.sh's _run_record in this tree still reads pc00's run dir; the fetched
	# dir of the submit PC is mirrored there so the record is made from it.
	if [[ "$pc" != pc00 && -d "$dir" ]]; then
		mkdir -p "$LAB/hosts/pc00/runs"
		cp -a "$dir" "$LAB/hosts/pc00/runs/$run_id" || lab_log "test run $run_id: could not mirror the run dir for _run_record"
	fi
	PYRUN_GIT_COMMIT="" PIPEDPEER_VERSION="" PYRUN_RUNSLOG="$LAB/runs.jsonl" \
		_run_record "$run_id" "$exp" "$w" "$sys" "$n" "$rep" "$warm" "$rec_rc" "$note" "${hosts[*]}"
	((rc == 124)) && return 124
	return 0
}

# e17_inproc <args...>: run e17.sh in this shell with the campaign_run_one
# above. Sourced so the test's substitution applies; e17.sh's own main guard
# keeps it from running twice.
e17_inproc() {
	# shellcheck source=/dev/null
	source "$CAMP/e17.sh"
	campaign_run_one() { e17_test_campaign_run_one "$@"; }
	e17_main "$@"
}
# ---------------------------------------------------------------- E17 rows
# dry-run: the plan, and nothing else
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	out="$(bash "$CAMP/e17.sh" --dry-run --n 3 --joiners 2 2>&1)"
	echo "$out" | grep -q 'E17-DRY-RUN' || rbad "no E17-DRY-RUN marker"
	echo "$out" | grep -q 'JOINERS pc03 pc04' || rbad "joiners not in the plan"
	echo "$out" | grep -q 'SUBMIT_PC pc01' || rbad "submit PC not in the plan"
	[[ ! -f "$d/pipedpeer.log" ]] || rbad "dry-run called pipedpeer"
	[[ ! -f "$d/systemctl.log" ]] || rbad "dry-run called systemctl"
	[[ ! -f "$d/hosts-5.txt" ]] || rbad "dry-run wrote a host file"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e17 --dry-run prints the plan and touches nothing" "$res" "$d/err"

# a true result after the kill: wall and result_ok from runs.jsonl, join
# through the survivor, the forced job's remote_items from its receipt
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	write_status "$d" pc00 3
	write_status "$d" pc01 3
	export FAKE_RUN_SLEEP=4 FAKE_SMALL_SLEEP=0.3 FAKE_CONNECT_SLEEP=0.5 FAKE_JOINED_ROWS=3
	export E17_START_WAIT_S=5 E17_JOIN_S=5 E17_POLL_S=0.2
	e17_inproc --n 3 --joiners 2 --repeats 1
	csv="$d/analysis-input/e17.csv"
	[[ -f "$csv" ]] || rbad "no e17.csv"
	row="$(sed -n 2p "$csv" 2>/dev/null || true)"
	IFS=, read -r i kat wall okv join_s join_ok ritm old <<<"$row"
	[[ "$i" == 1 ]] || rbad "row i [$i] ($row)"
	awk -v a="${kat:-x}" 'BEGIN{exit !(a >= 2 && a <= 4.5)}' || rbad "kill_at_s [$kat] not ~3"
	awk -v a="${wall:-x}" 'BEGIN{exit !(a >= 3 && a < 12)}' || rbad "job_wall_s [$wall] not from the record"
	[[ "$okv" == 1 ]] || rbad "job_result_ok [$okv] for a true record"
	[[ "$join_ok" == 1 ]] || rbad "new_join_ok [$join_ok]"
	awk -v a="${join_s:-x}" 'BEGIN{exit !(a > 0 && a < 5)}' || rbad "new_join_s [$join_s] not measured"
	[[ "$ritm" == 64 ]] || rbad "new_remote_items [$ritm] not from the receipt"
	[[ "$old" == 0 ]] || rbad "old_invite_joined [$old] for a dead introducer"
	grep -q 'pc00 systemctl --user kill -s SIGKILL pipedpeer.service' "$d/systemctl.log" 2>/dev/null || rbad "no SIGKILL on pc00"
	grep -q 'pc00 systemctl --user stop pipedpeer.service' "$d/systemctl.log" 2>/dev/null || rbad "no stop after the SIGKILL"
	grep -q 'pc00 systemctl --user set-environment PIPEDPEER_NO_LAN_DISCOVERY=1' "$d/systemctl.log" || rbad "no env on the members"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e17 a true result after the kill: wall/result from runs.jsonl, join and remote_items" "$res" "$d/err"

# job ended before the kill: kill_at_s empty, row kept; an old invite that
# still joins is recorded and logged as unexpected
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	write_status "$d" pc00 3
	write_status "$d" pc01 3
	export FAKE_RUN_SLEEP=1 FAKE_SMALL_SLEEP=0.2 FAKE_CONNECT_SLEEP=0.1 FAKE_JOINED_ROWS=3
	export FAKE_OLD_JOINS=1 E17_START_WAIT_S=5 E17_JOIN_S=5 E17_POLL_S=0.2
	e17_inproc --n 3 --joiners 2 --repeats 1
	csv="$d/analysis-input/e17.csv"
	row="$(sed -n 2p "$csv" 2>/dev/null || true)"
	IFS=, read -r i kat wall okv join_s join_ok ritm old <<<"$row"
	[[ -n "$row" ]] || rbad "no row"
	[[ -z "$kat" ]] || rbad "kill_at_s [$kat] for a job that had ended"
	awk -v a="${wall:-x}" 'BEGIN{exit !(a >= 0.5 && a < 6)}' || rbad "job_wall_s [$wall] not kept"
	[[ "$okv" == 1 ]] || rbad "job_result_ok [$okv]"
	[[ "$old" == 1 ]] || rbad "old_invite_joined [$old] for a joining old code"
	grep -q 'already ended before the kill' "$d/campaign.log" || rbad "the early end was not logged"
	grep -q 'UNEXPECTED' "$d/campaign.log" || rbad "old invite joining was not logged unexpected"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e17 job ended before the kill: row kept, kill_at_s empty; old invite joins -> 1 + log" "$res" "$d/err"

# connect hits its timeout: new_join_ok=0, new_join_s empty, next repeat runs
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	write_status "$d" pc00 3
	write_status "$d" pc01 3
	export FAKE_RUN_SLEEP=0.4 FAKE_CONNECT_SLEEP=5
	export E17_START_WAIT_S=3 E17_JOIN_S=2 E17_CONNECT_TIMEOUT_S=1 E17_OLD_INVITE_TIMEOUT_S=1 E17_POLL_S=0.2
	e17_inproc --n 3 --joiners 2 --repeats 2
	csv="$d/analysis-input/e17.csv"
	[[ "$(($(wc -l <"$csv") - 1))" == 2 ]] || rbad "rows $(($(wc -l <"$csv") - 1)) != 2 (the next repeat did not run)"
	for r in 2 3; do
		row="$(sed -n "${r}p" "$csv")"
		IFS=, read -r i kat wall okv join_s join_ok ritm old <<<"$row"
		[[ "$i" == "$((r - 1))" ]] || rbad "row $r i [$i]"
		[[ -z "$join_s" ]] || rbad "row $r new_join_s [$join_s] after a connect timeout"
		[[ "$join_ok" == 0 ]] || rbad "row $r new_join_ok [$join_ok]"
		[[ -z "$ritm" ]] || rbad "row $r new_remote_items [$ritm] without a join"
	done
	grep -q 'connect on pc03 failed (exit 124' "$d/campaign.log" || rbad "connect timeout not logged as 124"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e17 connect timeout: new_join_ok 0, new_join_s empty, next repeat runs" "$res" "$d/err"

# trap on Ctrl-C: moved files restored, env cleared
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	write_status "$d" pc00 3
	write_status "$d" pc01 3
	export FAKE_RUN_SLEEP=0.5 FAKE_CONNECT_SLEEP=30 E17_START_WAIT_S=1 E17_CONNECT_TIMEOUT_S=30 E17_POLL_S=0.2
	mkdir -p "$d/pc03/.local/share/pipedpeer"
	printf ORIG >"$d/pc03/.local/share/pipedpeer/nodes.db"
	printf ORIGJSON >"$d/pc03/.local/share/pipedpeer/cluster.json"
	# setsid makes a new process group; bash ignores SIGINT in an async job, so
	# perl puts the default back before exec (the exp_test.sh pattern).
	# shellcheck disable=SC2016  # perl source, not shell
	setsid perl -e '$SIG{INT}="DEFAULT"; exec @ARGV' bash "$CAMP/e17.sh" --n 3 --joiners 2 --repeats 1 >"$d/e17.out" 2>&1 &
	pid=$!
	backup="$d/pc03/.local/share/pipedpeer/nodes.db.e17-backup"
	for ((k = 0; k < 50; k++)); do [[ -f "$backup" ]] && break; sleep 0.2; done
	[[ -f "$backup" ]] || rbad "nodes.db was not moved aside before the INT"
	kill -INT -- -"$pid" 2>/dev/null || kill -INT "$pid" 2>/dev/null || true
	wait "$pid" || true
	[[ "$(cat "$d/pc03/.local/share/pipedpeer/nodes.db" 2>/dev/null)" == ORIG ]] || rbad "nodes.db not restored"
	[[ "$(cat "$d/pc03/.local/share/pipedpeer/cluster.json" 2>/dev/null)" == ORIGJSON ]] || rbad "cluster.json not restored"
	[[ ! -e "$backup" ]] || rbad "the backup file was left behind"
	grep -q 'pc00 systemctl --user unset-environment PIPEDPEER_NO_LAN_DISCOVERY' "$d/systemctl.log" || rbad "env not cleared on pc00"
	[[ "$(cat "$d/pc00/.no_lan_env" 2>/dev/null)" == unset ]] || rbad "pc00 restarted with the env still set"
	[[ "$(cat "$d/pc01/.no_lan_env" 2>/dev/null)" == unset ]] || rbad "pc01 restarted with the env still set"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e17 trap on Ctrl-C restores moved files and clears the env" "$res" "$d/err"
# ---------------------------------------------------------------- E18 rows
# detection at the 12th poll: about 12 s, only the target's row counts
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	export FAKE_E18_GEN=1 FAKE_TARGET_PC=pc01 FAKE_TARGET_HEALTHY_POLLS=11
	export E18_POLL_S=1 E18_DETECT_TIMEOUT_S=30
	bash "$CAMP/e18.sh" --n 3 --systems pipedpeer --repeats 1 >"$d/e18.out" 2>&1 || rbad "e18 exit $?"
	csv="$d/analysis-input/e18.csv"
	[[ -f "$csv" ]] || rbad "no e18.csv"
	row="$(sed -n 2p "$csv" 2>/dev/null || true)"
	IFS=, read -r sys i pc det to <<<"$row"
	[[ "$sys,$i,$pc" == "pipedpeer,1,pc01" ]] || rbad "row head [$row]"
	awk -v a="${det:-x}" 'BEGIN{exit !(a >= 10 && a <= 14)}' || rbad "detect_s [$det] not ~12"
	[[ "$to" == 0 ]] || rbad "timed_out [$to]"
	grep -q 'pc01 systemctl --user kill -s SIGKILL pipedpeer.service' "$d/systemctl.log" 2>/dev/null || rbad "no SIGKILL on the target"
	grep -q 'pc01 systemctl --user stop pipedpeer.service' "$d/systemctl.log" 2>/dev/null || rbad "no stop on the target"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e18 detection after 12 polls: detect_s ~12, timed_out 0" "$res" "$d/err"

# never detected: timed_out=1, detect_s empty at the limit
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	export FAKE_E18_GEN=1 FAKE_TARGET_PC=pc01 FAKE_TARGET_NEVER=1
	export E18_POLL_S=0.2 E18_DETECT_TIMEOUT_S=1.5
	bash "$CAMP/e18.sh" --n 3 --systems pipedpeer --repeats 1 >"$d/e18.out" 2>&1 || rbad "e18 exit $?"
	row="$(sed -n 2p "$d/analysis-input/e18.csv" 2>/dev/null || true)"
	IFS=, read -r sys i pc det to <<<"$row"
	[[ "$to" == 1 ]] || rbad "timed_out [$to] for a target that never went"
	[[ -z "$det" ]] || rbad "detect_s [$det] for a timeout"
	grep -q 'not detected within' "$d/campaign.log" || rbad "the limit was not logged"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e18 never detected: timed_out 1, detect_s empty at the limit" "$res" "$d/err"

# near-miss: another node is unhealthy the whole time; the target stays
# healthy, so there is no detection
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	export FAKE_E18_GEN=1 FAKE_TARGET_PC=pc01 FAKE_TARGET_NEVER=1
	export FAKE_OTHER_UNHEALTHY=1 FAKE_OTHER_PC=pc02
	export E18_POLL_S=0.2 E18_DETECT_TIMEOUT_S=1.5
	bash "$CAMP/e18.sh" --n 3 --systems pipedpeer --repeats 1 >"$d/e18.out" 2>&1 || rbad "e18 exit $?"
	row="$(sed -n 2p "$d/analysis-input/e18.csv" 2>/dev/null || true)"
	IFS=, read -r sys i pc det to <<<"$row"
	[[ "$to" == 1 ]] || rbad "another node's state was taken as a detection (timed_out [$to])"
	[[ -z "$det" ]] || rbad "detect_s [$det] from another node's row"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e18 near-miss: another node unhealthy does not count as detection" "$res" "$d/err"

# unit kill fails: the row is empty, logged, and no poll runs
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	export FAKE_E18_GEN=1 FAKE_TARGET_PC=pc01 FAKE_KILL_FAIL=1
	export E18_POLL_S=0.2 E18_DETECT_TIMEOUT_S=5
	bash "$CAMP/e18.sh" --n 3 --systems pipedpeer --repeats 1 >"$d/e18.out" 2>&1 || rbad "e18 exit $?"
	row="$(sed -n 2p "$d/analysis-input/e18.csv" 2>/dev/null || true)"
	[[ "$row" == "pipedpeer,1,pc01,," ]] || rbad "row [$row] not empty after a failed kill"
	[[ ! -f "$d/pc00/.status_calls" ]] || rbad "status was polled after a failed kill"
	grep -q 'SIGKILL+stop of pipedpeer on pc01 failed' "$d/campaign.log" || rbad "the failed kill was not logged"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e18 unit kill fails: row empty, logged, no wait" "$res" "$d/err"

# Ray detection: the target's node stops being Alive
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	export FAKE_E18_GEN=1 FAKE_TARGET_PC=pc01 FAKE_RAY_ALIVE_AFTER=2
	export E18_POLL_S=0.2 E18_DETECT_TIMEOUT_S=5
	bash "$CAMP/e18.sh" --n 3 --systems ray --repeats 1 >"$d/e18.out" 2>&1 || rbad "e18 exit $?"
	row="$(sed -n 2p "$d/analysis-input/e18.csv" 2>/dev/null || true)"
	IFS=, read -r sys i pc det to <<<"$row"
	[[ "$sys,$i,$pc" == "ray,1,pc01" ]] || rbad "row head [$row]"
	[[ -n "$det" ]] || rbad "no detect_s"
	[[ "$to" == 0 ]] || rbad "timed_out [$to]"
	grep -q 'pc01 systemctl --user kill -s SIGKILL lab-ray' "$d/systemctl.log" 2>/dev/null || rbad "no SIGKILL on lab-ray"
	grep -q 'pc01 systemctl --user stop lab-ray' "$d/systemctl.log" 2>/dev/null || rbad "no stop on lab-ray"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e18 Ray: Alive false for the target is a detection" "$res" "$d/err"

# Dask detection: the target's host leaves scheduler_info()
d="$(newdir)"
res="$(
	setup_lab "$d" 5
	export FAKE_E18_GEN=1 FAKE_TARGET_PC=pc01 FAKE_DASK_ALIVE_AFTER=2
	export E18_POLL_S=0.2 E18_DETECT_TIMEOUT_S=5
	bash "$CAMP/e18.sh" --n 3 --systems dask --repeats 1 >"$d/e18.out" 2>&1 || rbad "e18 exit $?"
	row="$(sed -n 2p "$d/analysis-input/e18.csv" 2>/dev/null || true)"
	IFS=, read -r sys i pc det to <<<"$row"
	[[ "$sys,$i,$pc" == "dask,1,pc01" ]] || rbad "row head [$row]"
	[[ -n "$det" ]] || rbad "no detect_s"
	[[ "$to" == 0 ]] || rbad "timed_out [$to]"
	grep -q 'pc01 systemctl --user kill -s SIGKILL lab-dask-worker' "$d/systemctl.log" 2>/dev/null || rbad "no SIGKILL on lab-dask-worker"
	grep -q 'pc01 systemctl --user stop lab-dask-worker' "$d/systemctl.log" 2>/dev/null || rbad "no stop on lab-dask-worker"
	echo ROW-OK
)" 2>"$d/err" || true
verdict_row "e18 Dask: the host absent from scheduler_info() is a detection" "$res" "$d/err"

echo "e17_18_test: $pass passed, $failed failed"
((failed == 0))



