#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2317  # rows run in subshells; helpers are called via variables
# The S7 case table (brief): scripts/campaign/cluster.sh's status parsing on
# saved text and its up/down/cap flow on the fake lab (test/fakelab.sh).
#
# The rows that drive up/down run cluster.sh as a child script with its own
# LAB_RESULTS, PATH (stubs first) and CLUSTER_READY_S, so the real results
# dir and no real systemctl/ssh/pipedpeer are ever reached; the fake lab
# refuses without FAKELAB_DIR. One ok/FAIL line per row; "PASS <k>/<k>" and
# exit 0 only when every row passed. LAB_TEST_KEEP=1 keeps failing scratch.
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CLUSTER_SH="$REPO/scripts/campaign/cluster.sh"
FAKELAB_LIB="$REPO/scripts/campaign/test/fakelab.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"

# shellcheck source=/dev/null
source "$FAKELAB_LIB"

pass=0
failed=0
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
}
trap cleanup EXIT

ok() { pass=$((pass + 1)); printf 'ok   %s\n' "$1"; }
bad() { failed=$((failed + 1)); printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }
verdict() {
	if ((${#problems[@]} == 0)); then
		ok "$1"
	else
		bad "$1" "${problems[*]}"
	fi
	problems=()
}

newdir() {
	local d
	d="$(mktemp -d "$STATE/cluster-test.XXXXXXX")"
	mkdir -p "$d/home"
	dirs+=("$d")
	printf '%s\n' "$d"
}

count() { # count <file> <pattern>
	if [[ -f "$1" ]]; then
		grep -c "$2" "$1" || true
	else
		echo 0
	fi
}

val() { # val <log> <key>: the last "<key>=<value>" line's value
	[[ -f "$1" ]] || return 0
	sed -n "s/^$2=//p" "$1" | tail -1
}

want_val() { # want_val <log> <key> <want> <what>
	local got
	got="$(val "$1" "$2")"
	[[ "$got" == "$3" ]] || pnote "$4: got [$got], want [$3]"
}

line() { # line <file> <exact line>
	[[ -f "$1" ]] || return 1
	grep -qxF -- "$2" "$1"
}

# The fake lab's ssh stub needs PATH with the stub bin first; the test's own
# command stubs go on top of that, so `command -v pipedpeer` inside a fake PC
# resolves to the stub, never a real binary.

_verif_install() { # _verif_install <dir>: pipedpeer + systemctl stubs
	local d="$1"
	mkdir -p "$d/verif"
	cat >"$d/verif/pipedpeer" <<'EOF'
#!/usr/bin/env bash
# test stub pipedpeer: log calls; `status` prints the saved text.
if [[ -z "${FAKELAB_DIR:-}" ]]; then echo "pipedpeer stub: no FAKELAB_DIR" >&2; exit 97; fi
printf 'pipedpeer %s\n' "$*" >>"$FAKELAB_DIR/pipedpeer.log"
# host, verb and how many sudo lines existed by then (a restart after a cap has >= 1)
printf '%s %s sudo=%s\n' "$(basename "$HOME")" "${1:-}" "$(wc -l <"$FAKELAB_DIR/sudo.log" 2>/dev/null || echo 0)" >>"$FAKELAB_DIR/pipedpeer.order"
case "${1:-}" in
	status) exec cat -- "${PIPEDPEER_STATUS_FILE:?no PIPEDPEER_STATUS_FILE}" ;;
	start | stop | setup | invite | jobs | doctor) exit 0 ;;
	*) echo "pipedpeer stub: unknown call: $*" >&2; exit 64 ;;
esac
EOF
	cat >"$d/verif/systemctl" <<'EOF'
#!/usr/bin/env bash
# test stub systemctl: log the call and succeed.
if [[ -z "${FAKELAB_DIR:-}" ]]; then echo "systemctl stub: no FAKELAB_DIR" >&2; exit 97; fi
printf 'systemctl %s\n' "$*" >>"$FAKELAB_DIR/systemctl.log"
exit 0
EOF
	chmod +x "$d/verif/pipedpeer" "$d/verif/systemctl"
}

_verif_python() { # _verif_python <dir> <pc>: a venv python that prints "$PY_FAKE_OUT".
	local d="$1" pc="$2"
	mkdir -p "$d/$pc/pipedpeer-lab/venv/bin"
	cat >"$d/$pc/pipedpeer-lab/venv/bin/python" <<'EOF'
#!/usr/bin/env bash
# test stub venv python: whatever the integer-bearing answer is today.
# the node listing (ip cpus per line) is PY_FAKE_NODES when set
if [[ "$*" == *NodeManagerAddress* && -n "${PY_FAKE_NODES:-}" ]]; then printf '%s\n' "$PY_FAKE_NODES"; exit 0; fi
printf '%s\n' "${PY_FAKE_OUT:?PY_FAKE_OUT unset}"
EOF
	chmod +x "$d/$pc/pipedpeer-lab/venv/bin/python"
}

# ss7_setup <dir> <n>: fake lab + stubs + speed. Must be called in a
# subshell; it exports the environment the row needs.
ss7_setup() {
	local dir="$1" n="${2:-4}"
	cd "$REPO"
	fakelab_init "$dir" "$n" || exit 90
	_verif_install "$dir"
	_verif_python "$dir" pc00
	export PATH="$dir/verif:$PATH"
	export HOME="$dir/home"
	export LAB_RESULTS="$dir"
	export LAB_RETRY_SLEEP=1
	export LAB_POLL_S=1
	export CLUSTER_READY_S=2
	if [[ "$(command -v pipedpeer || true)" != "$dir/verif/pipedpeer" ]]; then
		echo "ss7_setup: the stub pipedpeer is not first on PATH" >&2
		exit 91
	fi
	set +e
}

fx() { # fx <file> <<'EOF' ...: write a saved status text
	cat >"$1"
}

# --- the table ------------------------------------------------------------

# status text with 3 healthy + 1 stale, n=3 -> ready (count 3)
t1() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d"
		# shellcheck source=/dev/null
		source "$CLUSTER_SH"
		fx "$d/status.txt" <<'EOF'
daemon running (pid=1600 node-id=9078b729c31d42b5 port=4242 version=0.0.1-nightly.20261104)

NAME  STATE    PATH  CPU     MEM FREE  GPU                    JOBS  NOTE
pc00  healthy  self  8c 1%   6.7 GiB   RTX 3060 0% 41M/6.0G   0     primary laptop tonight
pc01  healthy  pc01  8c 1%   7.2 GiB   none                   0     -
pc02  healthy  pc02  8c 1%   7.2 GiB   none                   0     second shelf machine
pc03  stale    pc03  8c 1%   7.2 GiB   none                   0     was asleep

Recent runs:
EOF
		status_healthy_count "$d/status.txt" >"$d/cnt"
		printf 'cnt=%s\n' "$(tail -1 "$d/cnt")"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" cnt 3 "3 healthy + 1 stale counted"
	verdict "status text with 3 healthy + 1 stale -> count 3 (n=3 ready)"
}

# 2 healthy, n=3 -> NOT-READY pipedpeer want=3 have=2, exit 1
t2() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d" 4
		fx "$d/status.txt" <<'EOF'
daemon running (pid=1600 node-id=9078b729c31d42b5 port=4242)

NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -
pc02  stale    pc02  8c 1%   7.2 GiB   none 0     -
pc03  stale    pc03  8c 1%   7.2 GiB   none 0     -

Recent runs:
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		bash "$REPO/scripts/campaign/cluster.sh" up 3 --systems pipedpeer
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	want_val "$d/log" rc 1 "up with 2 of 3 healthy"
	grep -q 'NOT-READY pipedpeer want=3 have=2' "$d/log" || pnote "no NOT-READY pipedpeer want=3 have=2 line"
	grep -q 'NOT-READY pipedpeer want=3 have=2' "$d/campaign.log" || pnote "NOT-READY is not in campaign.log"
	verdict "2 healthy, n=3 -> NOT-READY pipedpeer want=3 have=2, exit 1"
}

# the self row counts -> 1
t3() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d" 1
		# shellcheck source=/dev/null
		source "$CLUSTER_SH"
		fx "$d/status.txt" <<'EOF'
daemon running (pid=1600 node-id=9078b729c31d42b5 port=4242)

NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     running alone

Recent runs:
EOF
		status_healthy_count "$d/status.txt" >"$d/cnt"
		printf 'cnt=%s\n' "$(tail -1 "$d/cnt")"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" cnt 1 "self row counted"
	verdict "self row counts -> 1"
}

# a NOTE containing spaces -> the row is still parsed (STATE is column 2)
t4() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d"
		# shellcheck source=/dev/null
		source "$CLUSTER_SH"
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  4c 1%   7.2 GiB   none 0     primary laptop home of the whole driver campaign
EOF
		status_healthy_count "$d/status.txt" >"$d/cnt"
		printf 'cnt=%s\n' "$(tail -1 "$d/cnt")"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" cnt 1 "row with a spaced NOTE counted"
	verdict "NOTE containing spaces -> row still parsed"
}

# no header line -> error, never a count of 0
t5() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d"
		# shellcheck source=/dev/null
		source "$CLUSTER_SH"
		fx "$d/status.txt" <<'EOF'
daemon not running
no table here at all
EOF
		status_healthy_count "$d/status.txt"
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	want_val "$d/log" rc 1 "missing status table"
	grep -q 'status table not found' "$d/log" || pnote "no 'status table not found' error"
	if grep -qx '0' "$d/log"; then
		pnote "a count of 0 was printed for a missing table"
	fi
	verdict "no header line -> 'status table not found', never a count of 0"
}

# --cap pc01=2 on a clean up -> the set-property line is recorded and caps
# survive the exit (e4 measures under them) until `down` resets
t6() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d" 4
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -

Recent runs:
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems pipedpeer --cap pc01=2
		printf 'up_rc=%s\n' "$?"
		bash "$REPO/scripts/campaign/cluster.sh" down
		printf 'down_rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" up_rc 0 "up 2 with --cap pc01=2"
	want_val "$d/log" down_rc 0 "down"
	local sets resets
	sets="$(count "$d/sudo.log" 'AllowedCPUs=0-1')"
	[[ "$sets" == 1 ]] || pnote "one AllowedCPUs=0-1 set-property expected, found $sets"
	resets="$(grep 'AllowedCPUs=' "$d/sudo.log" 2>/dev/null | grep -vc '0-1' || true)"
	[[ "$resets" == 1 ]] || pnote "one AllowedCPUs= reset expected, found $resets"
	grep -q 'set-property --runtime user-' "$d/sudo.log" || pnote "no set-property --runtime user- slice call"
	grep -q 'CLUSTER-DOWN' "$d/log" || pnote "down did not print CLUSTER-DOWN"
	[[ ! -f "$d/caps.txt" ]] || pnote "caps.txt survived down"
	# the daemon on the capped PC restarts after the cap (reads the capped cores)
	# and again after the reset; the uncapped PC is never restarted by the cap
	local order="$d/pipedpeer.order" first_stop
	first_stop="$(grep '^pc01 stop' "$order" 2>/dev/null | head -1 || true)"
	[[ "$first_stop" =~ sudo=([0-9]+) && ${BASH_REMATCH[1]} -ge 1 ]] || pnote "pc01 daemon not restarted after the cap: [$first_stop]"
	[[ "$(grep -c '^pc01 stop' "$order" 2>/dev/null || true)" -ge 2 ]] || pnote "pc01 daemon not restarted after the reset"
	# pc00 is in the 2-node cluster and uncapped: the cap must not stop it
	# (pc03 never took part, so a check on it could not fail)
	! grep -q '^pc00 stop' "$order" 2>/dev/null || pnote "uncapped pc00 was stopped by the cap: $(grep '^pc00' "$order" | tr '\n' ' ')"
	verdict "--cap pc01=2 -> set-property logged, daemon restarted after the cap and the reset, kept on clean exit, reset by down"
}

# --cap on a failed up -> the EXIT trap resets it
t7() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d" 4
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -
pc02  stale    pc02  8c 1%   7.2 GiB   none 0     -

Recent runs:
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		bash "$REPO/scripts/campaign/cluster.sh" up 3 --systems pipedpeer --cap pc01=2
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	want_val "$d/log" rc 1 "failed up with a cap"
	local resets
	resets="$(grep 'AllowedCPUs=' "$d/sudo.log" 2>/dev/null | grep -vc '0-1' || true)"
	[[ "$resets" -ge 1 ]] || pnote "the cap was not reset on the failed exit"
	[[ ! -f "$d/caps.txt" ]] || pnote "caps.txt survived the failed up"
	grep -q 'NOT-READY pipedpeer want=3 have=2' "$d/log" || pnote "no NOT-READY line"
	verdict "--cap on a NOT-READY exit -> AllowedCPUs= reset, caps.txt gone"
}

# up 2 with all three systems -> units started, ready, cores recorded, ips pushed
t8() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d" 4
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -

Recent runs:
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=2
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems pipedpeer,ray,dask
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" rc 0 "up 2 with all systems"
	grep -q 'CLUSTER-UP n=2 systems=pipedpeer ray dask' "$d/log" || pnote "no CLUSTER-UP line"
	local ray_units sched_units workers
	ray_units="$(count "$d/units.log" 'systemd-run lab-ray')"
	sched_units="$(count "$d/units.log" 'systemd-run lab-dask-scheduler')"
	workers="$(count "$d/units.log" 'systemd-run lab-dask-worker')"
	[[ "$ray_units" == 2 ]] || pnote "$ray_units lab-ray unit starts, want 2 (head + 1 worker)"
	[[ "$sched_units" == 1 ]] || pnote "$sched_units lab-dask-scheduler starts, want 1"
	[[ "$workers" == 2 ]] || pnote "$workers lab-dask-worker starts, want 2 (pc00 included)"
	grep -q 'pipedpeer=16' "$d/logs/cluster-2/cores.txt" 2>/dev/null || pnote "pipedpeer cores not 16"
	grep -q 'ray=2' "$d/logs/cluster-2/cores.txt" 2>/dev/null || pnote "cores.txt has no ray=2"
	grep -q 'dask=2' "$d/logs/cluster-2/cores.txt" 2>/dev/null || pnote "cores.txt has no dask=2"
	line "$d/pc00/pipedpeer-lab/hosts-2.txt" '10.99.0.10' || pnote "pc00 has no ip 10.99.0.10 in hosts-2.txt"
	line "$d/pc01/pipedpeer-lab/hosts-2.txt" '10.99.0.11' || pnote "pc01 has no ip 10.99.0.11 in hosts-2.txt"
	verdict "up 2, all systems -> 5 units started, cores.txt, ips pushed to every PC"
}

# down stops the service units everywhere and never touches pipedpeer
t9() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d" 4
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -

Recent runs:
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems pipedpeer >/dev/null 2>&1
		bash "$REPO/scripts/campaign/cluster.sh" down
		printf 'down_rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" down_rc 0 "down"
	local stops starts
	stops="$(count "$d/pipedpeer.log" '^pipedpeer stop$')"
	starts="$(count "$d/pipedpeer.log" '^pipedpeer start$')"
	[[ "$stops" == 2 ]] || pnote "$stops pipedpeer stop calls, want 2 (the 2 PCs outside the cluster)"
	[[ "$starts" == 2 ]] || pnote "$starts pipedpeer start calls, want 2 (pc00, pc01)"
	grep -q 'stop lab-ray lab-dask-worker lab-dask-scheduler' "$d/systemctl.log" || pnote "down did not stop the three service units"
	grep -q 'CLUSTER-DOWN' "$d/log" || pnote "no CLUSTER-DOWN line"
	verdict "down -> lab-ray/lab-dask units stopped everywhere, pipedpeer daemons untouched"
}

# usage on -h
t10() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d"
		bash "$REPO/scripts/campaign/cluster.sh" -h
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" rc 0 "-h"
	grep -q 'usage: cluster.sh' "$d/log" || pnote "no usage on -h"
	verdict "-h prints usage and exits 0"
}

# a capped PC (the head too) starts Ray with --num-cpus=<cap>; an uncapped one with the probed count
t11() {
	local d rc=0
	d="$(newdir)"
	(
		ss7_setup "$d" 4
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -

Recent runs:
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=2
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems ray --cap pc00=4 --cap pc01=2
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	grep -q 'ray start --head --port=6379 --num-cpus=4' "$d/units.log" || pnote "capped head not started with --num-cpus=4: $(grep lab-ray "$d/units.log" | tr '\n' '|')"
	grep -q 'ray start --address=[0-9.]*:6379 --num-cpus=2' "$d/units.log" || pnote "capped worker not started with --num-cpus=2"
	d="$(newdir)"
	(
		ss7_setup "$d" 4
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -

Recent runs:
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=2
		# the probe runs on the "PC" (this host): fixture values, no quota found
		mkdir -p "$d/cg"
		printf '0::/none\n' >"$d/proc_cgroup"
		export PP_CG_ROOT="$d/cg" PP_PROC_CGROUP="$d/proc_cgroup" PP_NPROC=5
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems ray
	) >"$d/log" 2>&1 || true
	grep -q 'ray start --head --port=6379 --num-cpus=5' "$d/units.log" || pnote "uncapped head not started with the probed --num-cpus=5: $(grep lab-ray "$d/units.log" | tr '\n' '|')"
	grep -q 'ray start --address=[0-9.]*:6379 --num-cpus=5' "$d/units.log" || pnote "uncapped worker not started with the probed --num-cpus=5"
	verdict "ray --cap: head and worker started with --num-cpus; no cap, the probed count"
}

# a head PC with too little memory for Ray: up refuses before starting it
t11m() {
	local d
	d="$(newdir)"
	(
		ss7_setup "$d" 4
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -

Recent runs:
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt" PY_FAKE_OUT=2
		mkdir -p "$d/cg"
		printf '0::/none\n' >"$d/proc_cgroup"
		export PP_CG_ROOT="$d/cg" PP_PROC_CGROUP="$d/proc_cgroup" PP_NPROC=4 PP_MEMTOTAL_KIB=1048576
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems ray
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || true
	grep -q '^rc=1$' "$d/log" || pnote "up did not fail: $(tail -3 "$d/log" | tr '\n' '|')"
	grep -q 'NOT-READY ray pc=pc00' "$d/log" || pnote "no NOT-READY ray pc=pc00"
	! grep -q 'ray start --head' "$d/units.log" 2>/dev/null || pnote "the Ray head was started anyway"
	verdict "ray: a 1 GiB head is refused before it starts (NOT-READY ray pc=pc00)"
}

# R2 M3: a capped head whose Ray CPUs differ from the cap is logged (not restarted); one that agrees is not
t12() {
	local d out want
	for want in 8 4; do
		d="$(newdir)"
		(
			ss7_setup "$d" 4
			fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -

Recent runs:
EOF
			export PIPEDPEER_STATUS_FILE="$d/status.txt"
			export PY_FAKE_OUT=2
			export PY_FAKE_NODES="10.99.0.10 $want
10.99.0.11 2"
			bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems ray --cap pc00=4 --cap pc01=2
		) >"$d/log" 2>&1 || true
		out="$(cat "$d/log" "$d/campaign.log" 2>/dev/null)"
		if [[ $want == 8 ]]; then
			grep -q 'cap pc00: Ray HEAD reports 8 CPUs' <<<"$out" || pnote "head mismatch not logged: $(grep -v "resetting\|sudo.log" "$d/log" | tail -8 | tr "\n" "|")"
		else
			! grep -q 'Ray HEAD reports' <<<"$out" || pnote "head at the cap logged as a mismatch"
		fi
	done
	verdict "ray cap: head CPUs off the cap logged; head at the cap not"
}

printf 'scripts/campaign/cluster.sh case table\n\n'
t1
t2
t3
t4
t5
t6
t7
t8
t9
t10
t11
t11m
t12
printf '\n'

total=$((pass + failed))
if ((failed == 0)); then
	printf 'PASS %d/%d\n' "$pass" "$total"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$failed" "$total"
exit 1
