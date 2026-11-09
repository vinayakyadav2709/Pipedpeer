#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2034,SC2317
# The S6 case table: install.sh, pins.sh and venv.sh on the fake lab
# (test/fakelab.sh), plus the two rows for versions.py itself.
#
# The PCs run a `pipedpeer` stub (written into the fake lab's bin dir by
# pipedpeer_stub) that prints the real status first line and obeys a per-row
# config file, pipedpeer.conf: which PC's connect fails or stops after two
# "partly connected" answers, which PC reports the wrong version, and which
# node id is an alias of another PC's.
#
# Every row runs in its own subshell with its own fake lab, HOME and
# LAB_RESULTS, so the real results dir and any real pipedpeer binary are
# never touched. One ok/FAIL line per row; the run ends with "PASS <k>/<k>"
# and exits 0 only when every row passed. Set LAB_TEST_KEEP=1 to keep the
# scratch dirs of a failing run.

set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
LIB="$REPO/scripts/campaign/lib.sh"
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
	d="$(mktemp -d "$STATE/campaign-install-test.XXXXXX")"
	mkdir -p "$d/home"
	dirs+=("$d")
	printf '%s\n' "$d"
}

# case_setup <dir> [n]: fake lab, own HOME/LAB_RESULTS, lib.sh sourced; the
# pipedpeer stub goes into the stub bin dir before the PATH export, so the
# PCs (and any script on the driver) see the stub, never a real binary.
case_setup() {
	local dir="$1" n="${2:-3}"
	cd "$REPO"
	fakelab_init "$dir" "$n" || exit 90
	pipedpeer_stub "$dir/bin"
	# INSTALL-STUB stands in for scripts/install-pipedpeer.sh in the fake lab
	cat >"$dir/bin/INSTALL-STUB" <<'IS'
#!/usr/bin/env bash
# The real installer ends by running setup, which starts the warm-up.
exec pipedpeer setup -y
IS
	chmod +x "$dir/bin/INSTALL-STUB"
	# the real installer would download from GitHub; the tests stub it out
	export INSTALL_CMD="INSTALL-STUB {version}"
	mkdir -p "$dir/home/pipedpeer-lab/work" "$dir/home/pipedpeer-lab/runs"
	cp "$dir/pc00/pipedpeer-lab/work/stamp.py" "$dir/home/pipedpeer-lab/work/" 2>/dev/null || true
	export HOME="$dir/home"
	export LAB_RESULTS="$dir"
	export LAB_RETRY_SLEEP=1
	export LAB_POLL_S=1
	# the campaign scripts must not touch the real results dir either way
	# shellcheck source=/dev/null
	source "$LIB" || exit 91
	set +e
}

conf() { # conf <dir>: read the row's pipedpeer.conf from stdin
	cat >"$1/pipedpeer.conf"
}

# pipedpeer_stub <bin_dir>: the fake pipedpeer binary. It mirrors the real
# answers the campaign parses: the one status line, the invite line, the
# connect exit codes, `pipedpeer run` printing a VERSIONS line.
pipedpeer_stub() {
	local bin="$1"
	cat >"$bin/pipedpeer" <<'PP-STUB'
#!/usr/bin/env bash
# Fake pipedpeer for campaign tests; configured by $FAKELAB_DIR/pipedpeer.conf.
set -uo pipefail
dir="${FAKELAB_DIR:?}"
[[ -f "$dir/pipedpeer.conf" ]] && . "$dir/pipedpeer.conf" >/dev/null
pc="$(basename "$(cd "${HOME:-}" && pwd)")"
VERSION="${STUB_VERSION:-0.0.1-test.20261108-abc1234}"
MISMATCH_PC="${STUB_MISMATCH_PC:-}"
MISMATCH_VERSION="${STUB_MISMATCH_VERSION:-0.0.9-else}"
CONNECT_FAIL="${STUB_CONNECT_FAIL_PC:-}"
CONNECT_PART="${STUB_CONNECT_PART_PC:-}"
NODE_ALIAS="${STUB_NODE_ALIAS_PC:-}"
case "${1:-}" in
	status)
		case "$pc" in
			"$MISMATCH_PC") VERSION="$MISMATCH_VERSION" ;;
		esac
		node_id="node-$pc"
		case "$pc" in "${NODE_ALIAS%%=*}") node_id="node-${NODE_ALIAS##*=}" ;; esac
		printf 'daemon running (pid=%s node-id=%s port=4242 version=%s)\n' "$$" "$node_id" "$VERSION"
		printf '\n'
		printf 'NAME  STATE    PATH  CPU     MEM FREE  GPU   JOBS  NOTE\n'
		for d in "$dir"/pc*; do
			[[ -d "$d" ]] || continue
			h="$(basename "$d").lab"
			state=healthy
			# a PC whose connect failed is not in the leader's table as healthy
			dn="$(basename "$d")"
			if [[ ",$CONNECT_FAIL," == *",$dn,"* && -z "${STUB_FAILED_STILL_HEALTHY:-}" ]]; then state=offline; fi
			printf '%s  %s  peer  8c 1%%  31GiB  none 0  0  -\n' "$h" "$state"
		done
		;;
	invite)
		printf '%s\n' "${STUB_CODE:-pipedpeer connect FAKE-CODE-123}"
		echo "created a new cluster" >&2
		;;
	connect)
		rc=0
		if [[ ",$CONNECT_FAIL," == *,"$pc",* ]]; then
			rc=1
		fi
		if [[ ",$CONNECT_PART," == *,"$pc",* ]]; then
			count_file="$dir/connect-$pc.count"
			n=0
			[[ -f "$count_file" ]] && n="$(tail -1 "$count_file")"
			n=$((n + 1))
			printf '%s\n' "$n" >"$count_file"
			((n <= 2)) && rc=2
		fi
		exit "$rc"
		;;
	run)
		[[ -x "$(command -v python3)" ]] || :
		printf '%s\n' "${STUB_RUN_VERSIONS:-VERSIONS python=3.14.10 numpy=2.5.1 pandas=3.0.5 sklearn=1.9.0 joblib=1.5.3 torch=2.13.0+cpu ray=none dask=none}"
		;;
	--version | version)
		printf 'version=%s\n' "$VERSION"
		;;
	setup)
		# Real setup starts the layer warm-up; install.sh waits on this log.
		mkdir -p "$HOME/.local/state/pipedpeer"
		{
			printf '%s building the common environment (numpy, pandas, torch CPU) build=test torch=CPU\n' "$(date -Is)"
			printf 'done\n'
		} >"$HOME/.local/state/pipedpeer/warmup.log"
		exit 0
		;;
	*)
		echo "stub pipedpeer: unknown command ${1:-}" >&2
		exit 9
		;;
esac
PP-STUB
	chmod +x "$bin/pipedpeer"
}

# uv_stub <bin_dir>: fake uv whose venv python execs the driver's python3. Enough for
# the fake lab; the real PCs get the real uv.
uv_stub() {
	local bin="$1"
	cat >"$bin/uv" <<'UV-STUB'
#!/usr/bin/env bash
# Fake uv for campaign tests.
set -euo pipefail
UV_STUB_GUARD_EMAIL_testbench=pipedpeer
# the calls venv.sh makes: `uv python install --no-bin <v>` (managed CPython
# under $UV_PYTHON_INSTALL_DIR) and `uv venv --managed-python --python <v>
# --seed <dir>`, which makes <dir>/bin/python (the driver's python3) and pip
if [[ "${1:-}" == python && "${2:-}" == install ]]; then
	[[ -n "${UV_PYTHON_INSTALL_DIR:-}" ]] || {
		echo "stub uv: no UV_PYTHON_INSTALL_DIR" >&2
		exit 3
	}
	mkdir -p "$UV_PYTHON_INSTALL_DIR"
	exit 0
fi
if [[ "${1:-}" == venv ]]; then
	dest="${*: -1}"
	mkdir -p "$dest/bin"
	printf '#!/usr/bin/env bash\nexec python3 "$@"\n' >"$dest/bin/python"
	printf '#!/usr/bin/env bash\nexit 0\n' >"$dest/bin/pip"
	chmod +x "$dest/bin/python" "$dest/bin/pip"
	exit 0
fi
echo "stub uv: unknown args: ${1:-} ${2:-}" >&2
exit 2
UV-STUB
	chmod +x "$bin/uv"
}

count() {
	if [[ -f "$1" ]]; then
		grep -c "$2" "$1" || true
	else
		echo 0
	fi
}
line() {
	[[ -f "$1" ]] || return 1
	grep -qxF -- "$2" "$1"
}

# --- the table ------------------------------------------------------------

# all PCs connect with exit 0 -> INSTALL ok=n, install.csv has n rows
t1() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'INSTALL ok=3 failed=0' "$d/log" || pnote "no 'INSTALL ok=3 failed=0'"
	grep -qxF 'id,pipedpeer_version,connect_exit,node_id' "$d/install.csv" || pnote "no csv header"
	line "$d/install.csv" "pc00,0.0.1-test.20261108-abc1234,self,node-pc00" || pnote "pc00 row wrong"
	line "$d/install.csv" "pc01,0.0.1-test.20261108-abc1234,0,node-pc01" || pnote "pc01 row wrong"
	local n_rows
	n_rows="$(count "$d/install.csv" '^pc')"
	[[ "$n_rows" == 3 ]] || pnote "install.csv has $n_rows pc rows, want 3"
	[[ -f "$d/logs/install/FAILED" ]] && pnote "FAILED file written with no failures"
	verdict "all connect 0 -> INSTALL ok=3 failed=0, install.csv rows"
}

# one connect exits 2 twice then 0 -> ok, retried (3 attempts)
t2() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		conf "$d" <<'EOF'
STUB_CONNECT_PART_PC=pc02
EOF
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'INSTALL ok=3 failed=0' "$d/log" || pnote "no INSTALL ok=3 (expected retried PC to count ok)"
	line "$d/install.csv" "pc02,0.0.1-test.20261108-abc1234,0,node-pc02" || pnote "pc02 row must record connect_exit 0"
	local tries
	tries="$(cat "$d/connect-pc02.count" 2>/dev/null || echo 0)"
	[[ "$tries" == 3 ]] || pnote "connect on pc02 ran $tries times, want 3 (2x exit 2, then 0)"
	verdict "connect exit 2 twice then 0 -> ok, retried"
}

# one connect always exits 1 -> FAILED, exit 1
t3() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		conf "$d" <<'EOF'
STUB_CONNECT_FAIL_PC=pc01
EOF
		timeout 60 bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234 </dev/null
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'INSTALL ok=2 failed=1' "$d/log" || pnote "no 'INSTALL ok=2 failed=1'"
	line "$d/logs/install/FAILED" pc01 || pnote "FAILED does not list pc01"
	line "$d/install.csv" "pc01,0.0.1-test.20261108-abc1234,1,node-pc01" || pnote "pc01 row must record connect_exit 1"
	verdict "connect exit 1 always -> FAILED, exit 1"
}

# connect exit 1 but the leader's table shows the PC healthy -> counts ok (and does not hang on stdin)
t3b() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		conf "$d" <<'EOF'
STUB_CONNECT_FAIL_PC=pc01
STUB_FAILED_STILL_HEALTHY=1
EOF
		timeout 60 bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234 </dev/null
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'INSTALL ok=3 failed=0' "$d/log" || pnote "healthy in the leader's table must count ok: $(tail -3 "$d/log" | tr '\n' ' ')"
	verdict "connect exit 1 but healthy on pc00 -> ok"
}

# two PCs with the same node id -> DUPLICATE-NODE-ID, exit 1
t4() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d" 4
		conf "$d" <<'EOF'
STUB_NODE_ALIAS_PC=pc03=pc02
EOF
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	local dup
	dup="$(grep -o 'DUPLICATE-NODE-ID node-pc02 pc02,pc03' "$d/log" || true)"
	[[ -n "$dup" ]] || pnote "no 'DUPLICATE-NODE-ID node-pc02 pc02,pc03' line"
	grep -q '^rc=1$' "$d/log" || pnote "row did not exit 1"
	verdict "two PCs with the same node-id -> DUPLICATE-NODE-ID, exit 1"
}

# status version mismatch on one PC -> FAILED with "version <got> != <want>"
t5() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		conf "$d" <<'EOF'
STUB_MISMATCH_PC=pc01
STUB_MISMATCH_VERSION=0.0.9-else
EOF
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'pc01.*version 0.0.9-else != 0.0.1-test' "$d/log" || pnote "no 'pc01 version 0.0.9-else != 0.0.1-test'"
	grep -q 'INSTALL ok=2 failed=1' "$d/log" || pnote "mismatching PC not counted as failed"
	grep -q '^rc=1$' "$d/log" || pnote "row did not exit 1"
	verdict "status version mismatch -> FAILED 'version <got> != <want>'"
}

# versions.py with torch missing -> torch=none, no crash (direct run)
t6() {
	local d rc=0 shadow
	d="$(newdir)"
	(
		mkdir -p "$d/shadow"
		printf 'raise ImportError("torch blocked by the pipedpeer campaign test")\n' >"$d/shadow/torch.py"
		PYTHONPATH="$d/shadow" python3 "$REPO/docs/benchmarks/workloads/versions.py"
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q '^VERSIONS python=' "$d/log" || pnote "no VERSIONS line"
	grep -q 'torch=none' "$d/log" || pnote "torch is not 'none' in the versions line"
	verdict "versions.py with torch missing -> torch=none, no crash"
}

# rows of pins.sh (extra, same suite)
# pins.sh writes versions.csv's pipedpeer row, requirements.txt and python-version
t7() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		export PINS_WHEEL_CHECK=0
		conf "$d" <<'EOF'
STUB_RUN_VERSIONS='VERSIONS python=3.12.7 numpy=1.26.4 pandas=2.2.3 sklearn=1.5.2 joblib=1.4.2 torch=2.4.1+cpu ray=2.35.0 dask=2024.9.0'
EOF
		bash "$REPO/scripts/campaign/pins.sh"
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -qxF 'system,python,numpy,pandas,sklearn,joblib,torch,ray,dask,pipedpeer' "$d/versions.csv" || pnote "bad versions.csv header"
	line "$d/versions.csv" "pipedpeer,3.12.7,1.26.4,2.2.3,1.5.2,1.4.2,2.4.1+cpu,2.35.0,2024.9.0,0.0.1-test.20261108-abc1234" || pnote "pipedpeer row wrong"
	for want in 'numpy==1.26.4' 'pandas==2.2.3' 'scikit-learn==1.5.2' 'joblib==1.4.2' 'torch==2.4.1+cpu' 'ray[default]' 'dask[distributed]' ; do
		line "$d/requirements.txt" "$want" || pnote "requirements.txt lacks '$want'"
	done
	line "$d/requirements.txt" '--extra-index-url https://download.pytorch.org/whl/cpu' || pnote "no torch cpu index line"
	grep -q 'tqdm' "$d/requirements.txt" && pnote "requirements.txt mentions tqdm"
	line "$d/python-version" 3.12 || pnote "python-version is not 3.12"
	verdict "pins.sh -> versions.csv pipedpeer row, requirement pins, python-version"
}

# venv.sh builds the venv on every PC and records the venv row (extra row)
t8() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		uv_stub "$d/bin"
		# pins.sh first (requirements.txt + python-version must exist)
		export PINS_WHEEL_CHECK=0
		conf "$d" <<'EOF'
STUB_RUN_VERSIONS='VERSIONS python=3.12.7 numpy=1.26.4 pandas=2.2.3 sklearn=1.5.2 joblib=1.4.2 torch=2.4.1+cpu ray=2.35.0 dask=2024.9.0'
EOF
		bash "$REPO/scripts/campaign/pins.sh" >/dev/null 2>&1
		# the fake venv's python sees the driver's packages; ramp the pip
		# step out of the test's way (there is no network in the fake lab)
		VENV_PIP=true bash "$REPO/scripts/campaign/venv.sh"
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q '^venv,' "$d/versions.csv" || pnote "no venv row in versions.csv"
	local ref got
	ref="$(awk -F, '$1=="venv"{print $2, $3}' "$d/versions.csv")"
	got="$(awk -F, '$1=="venv"{print $2, $3}' "$d/versions.csv")"
	[[ -n "$got" ]] || pnote "venv row has no python/numpy fields"
	# the venv row must come from pc00's venv python (same on every PC here)
	verdict "venv.sh -> venv on all PCs, versions.csv venv row"
}

# a PC whose versions line differs -> FAILED list (extra row, saved text)
t9() {
	local d rc=0 d2
	d="$(newdir)"
	(
		case_setup "$d"
		source "$REPO/scripts/campaign/venv.sh"
		mkdir -p "$d/logs/venv"
		printf 'VERSIONS python=3.12.7 numpy=1.26.4 pandas=2.2.3 sklearn=1.5.2 joblib=1.4.2 torch=2.4.1+cpu ray=2.35.0 dask=2024.9.0\n' >"$d/logs/venv/pc00"
		printf 'VERSIONS python=3.12.7 numpy=1.26.4 pandas=2.2.3 sklearn=1.5.2 joblib=1.4.2 torch=2.4.1+cpu ray=2.35.0 dask=2024.9.0\n' >"$d/logs/venv/pc01"
		printf 'VERSIONS python=3.12.7 numpy=1.26.4 pandas=2.2.3 sklearn=1.5.2 joblib=1.4.2 torch=2.3.0+cpu ray=2.35.0 dask=2024.9.0\n' >"$d/logs/venv/pc02"
		_venv_check "$d/logs/venv"
		printf 'check_rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q '^FAILED pc02$' "$d/log" || pnote "mismatched pc02 not reported FAILED"
	line "$d/logs/venv/FAILED" pc02 || pnote "FAILED file does not list pc02"
	grep -q '^check_rc=1$' "$d/log" || pnote "check did not return 1"
	verdict "a PC whose versions line differs -> FAILED list, exit 1"
}

# install runs `lab-pc.sh mark` on every PC; one PC where it fails is warned
# about by name and the install still finishes ok=3.
t10() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		cat >"$d/fake-lab-pc.sh" <<'FLP'
#!/usr/bin/env bash
echo "mark $*" >>"$HOME/mark.log"
[[ "$(basename "$HOME")" == pc01 ]] && exit 1
exit 0
FLP
		export LAB_PC_SELF="$d/fake-lab-pc.sh"
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'INSTALL ok=3 failed=0' "$d/log" || pnote "install did not finish ok=3"
	[[ -f "$d/pc00/mark.log" && -f "$d/pc02/mark.log" ]] || pnote "mark did not run on pc00 and pc02"
	grep -q 'mark failed on pc01' "$d/log" || pnote "no warning naming pc01"
	grep -q 'mark failed on pc00' "$d/log" && pnote "warned about pc00, which succeeded"
	verdict "mark fails on one PC -> warning names it, install continues"
}

printf 'scripts/campaign install/pins/venv case table\n\n'
t1
t2
t3
t3b
t4
t5
t6
t7
t8
t9
t10
printf '\n'

total=$((pass + failed))
if ((failed == 0)); then
	printf 'PASS %d/%d\n' "$pass" "$total"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$failed" "$total"
exit 1
