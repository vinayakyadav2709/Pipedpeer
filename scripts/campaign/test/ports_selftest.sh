#!/usr/bin/env bash
# Rows for ports_test.sh itself, run with fake python/ray/dask in a private HOME
# (never the real venv or a real cluster): the runner must finish, take a
# failed cluster start as "not up", and leave no cluster, state dir or temp
# dir behind even when killed.
# Output: ok/FAIL per row, then PASS <k>/<k>; exit 0 only when all pass.
set -u
unset PORTS_KEEP START_TRIES START_WAIT # the rows set their own; the caller's knobs must not leak in
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
runner="$here/ports_test.sh"
t=$(mktemp -d)
pass=0
total=0
cleanup() {
	alive_pids | xargs -r kill >/dev/null 2>&1
	rm -rf "$t"
}
trap cleanup EXIT

# every fake long-running process appends its pid to $t/pids
alive_pids() {
	local p
	while read -r p; do kill -0 "$p" 2>/dev/null && echo "$p"; done <"$t/pids" 2>/dev/null
}

touch "$t/pids"
mkdir -p "$t/bin" "$t/home" "$t/raytmp"
cat >"$t/bin/python" <<'PYEOF'
#!/usr/bin/env bash
# fake venv python: the readiness probes exit 0, or hang when FAKE_HANG=1
if [ "${FAKE_HANG:-0}" = 1 ]; then echo $$ >>"$FAKE_PIDS"; exec sleep 6001; fi
exit 0
PYEOF
cat >"$t/bin/ray" <<'RAYEOF'
#!/usr/bin/env bash
case $1 in
start)
	[ "${FAKE_RAY_FAIL:-0}" = 1 ] && { echo "ray: fake start failure" >&2; exit 1; }
	echo started >>"$FAKE_LOG"
	setsid sleep 6002 >/dev/null 2>&1 &
	echo $! >>"$FAKE_PIDS"
	echo $! >"$FAKE_PIDS.ray"
	;;
stop)
	echo stopped >>"$FAKE_LOG"
	[ -f "$FAKE_PIDS.ray" ] && kill "$(cat "$FAKE_PIDS.ray")"
	;;
esac
exit 0
RAYEOF
cat >"$t/bin/dask" <<'DEOF'
#!/usr/bin/env bash
case $1 in
scheduler) echo $$ >>"$FAKE_PIDS"; exec sleep 6003 ;;
*) echo $$ >>"$FAKE_PIDS"; exec sleep 6004 ;;
esac
DEOF
chmod +x "$t/bin/"*

# run_runner <timeout s> <args...>: output in $t/out, rc in $rc.
run_runner() {
	local tmo=$1
	shift
	: >"$t/ray.log"
	HOME="$t/home" FAKE_LOG="$t/ray.log" FAKE_PIDS="$t/pids" PORTS_DEV_PY="$t/bin/python" \
		PORTS_RAYROOT="$t/raytmp" START_TRIES=2 START_WAIT=0.2 \
		RAY_PORT=${RAY_PORT:-47379} DASK_PORT=${DASK_PORT:-47786} PORTS_IN_SELFTEST=1 \
		timeout "$tmo" bash "$runner" "$@" >"$t/out" 2>&1
	rc=$?
}
row() { # <name> <ok 0|1> <why>
	total=$((total + 1))
	if [ "$2" = 0 ]; then
		pass=$((pass + 1))
		echo "ok $1"
	else
		echo "FAIL $1: $3"
	fi
}
leftovers() { # processes and dirs a finished run must not leave
	local n=0
	[ -n "$(alive_pids)" ] && n=$((n + 1))
	compgen -G "$t/home/.local/state/pipedpeer/ports-test.*" >/dev/null && n=$((n + 1))
	compgen -G "$t/raytmp/ports-ray.*" >/dev/null && n=$((n + 1))
	echo "$n"
}

# 1. a normal run ends, prints one PORTS total and starts and stops what it started
rc=
run_runner 60 -k zz-no-row
n=$(grep -c '^PORTS rows=' "$t/out")
ok=1
[ "$rc" = 0 ] && [ "$n" = 1 ] && [ "$(grep -c started "$t/ray.log")" = 1 ] &&
	[ "$(grep -c stopped "$t/ray.log")" = 1 ] && [ "$(leftovers)" = 0 ] && ok=0
row "runner ends, one total, ray stopped, nothing left" $ok "rc=$rc totals=$n ray.log=$(tr '\n' ' ' <"$t/ray.log") left=$(leftovers) pids=$(alive_pids | tr "\n" " ") dirs=$(find "$t/home/.local/state/pipedpeer" "$t/raytmp" -mindepth 1 -maxdepth 1 2>&1 | tr "\n" " ")"

# 2. a ray head that does not start is "not up": its row is skipped
FAKE_RAY_FAIL=1 run_runner 60 -k w1-ray
grep -q '^SKIP w1-ray: local ray cluster not up' "$t/out"
row "failed ray start -> row skipped" $? "$(head -3 "$t/out" | tr '\n' '|')"

# 3. the opposite: a head that starts is used, the row is not skipped
run_runner 60 -k w1-ray
grep -Eq "^(PASS|FAIL) w1-ray" "$t/out"
row "started ray -> row runs" $? "$(head -3 "$t/out" | tr '\n' '|')"

# 4. killed while waiting for the head: head stopped, no dirs, no processes
# row 3 failed rows on purpose and kept its dirs; start clean
rm -rf "$t/home/.local/state/pipedpeer" "$t/raytmp"/*
FAKE_HANG=1 run_runner 3 -k zz-no-row
sleep 1
ok=1
[ "$(grep -c stopped "$t/ray.log")" -ge 1 ] && [ "$(leftovers)" = 0 ] && ok=0
row "killed mid-start: ray stopped, nothing left" $ok "rc=$rc ray.log=$(tr '\n' ' ' <"$t/ray.log") left=$(leftovers) pids=$(alive_pids | tr "\n" " ") dirs=$(find "$t/home/.local/state/pipedpeer" "$t/raytmp" -mindepth 1 -maxdepth 1 2>&1 | tr "\n" " ")"

# 5. the opposite: no row file is the runner itself (no recursion into ports_test.sh)
[ "$(grep -c 'PORTS rows=' "$t/out")" -le 1 ] && ! grep -q 'ports_test.sh' <(ls "$here"/ports_w*.sh)
row "row files are ports_w*.sh only" $? "glob matches the runner"

# --- t1_torchrun.sh / .py (D1): the launcher must see a failed rank, and a
# lost WORLD_SIZE must stop the run instead of training on one node ----------
BAS="$here/../../../docs/benchmarks/workloads/baselines"
mkdir -p "$t/fakessh"
cat >"$t/fakessh/ssh" <<'SSHEOF'
#!/usr/bin/env bash
# fake ssh: answers as rank <node-rank> of the torchrun command it was given
for a in "$@"; do case $a in *--node-rank=*) r=${a##*--node-rank=}; r=${r%% *} ;; esac; done
echo "out of rank $r"
[ "$r" = "${FAKE_FAIL_RANK:-none}" ] && { echo "rank $r exploded" >&2; exit 1; }
exit 0
SSHEOF
chmod +x "$t/fakessh/ssh"
printf '10.0.0.1\n10.0.0.2\n' >"$t/hosts"
run_t1() {
	HOME="$t/home" PATH="$t/fakessh:$PATH" LAB_HOSTS_FILE="$t/hosts" LAB_USER=u \
		SSH_CONFIG=/dev/null XDG_STATE_HOME="$t/home/.local/state" "$@" \
		bash "$BAS/t1_torchrun.sh" 2 >"$t/t1.out" 2>"$t/t1.err"
	rc=$?
}
mkdir -p "$t/home/.local/state/pipedpeer"
run_t1 env FAKE_FAIL_RANK=1
ok=1
[ "$rc" != 0 ] && grep -q 'rank 1 failed' "$t/t1.err" && ok=0
row "t1_torchrun.sh: rank 1 fails -> non-zero, named" $ok "rc=$rc err=$(head -2 "$t/t1.err" | tr '\n' '|')"
run_t1 env FAKE_FAIL_RANK=none
ok=1
[ "$rc" = 0 ] && grep -q 'out of rank 0' "$t/t1.out" && ok=0
row "t1_torchrun.sh: all ranks ok -> exit 0, rank 0 output" $ok "rc=$rc out=$(head -2 "$t/t1.out" | tr '\n' '|')"

BPY="${PORTS_DEV_PY:-${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer-lab-dev/venv/bin/python}"
BENCH_EXPECT_WORLD=2 "${BPY:-python3}" "$BAS/t1_torchrun.py" >"$t/py.out" 2>"$t/py.err"
rc=$?
ok=1
[ "$rc" != 0 ] && grep -q 'expected WORLD_SIZE=2' "$t/py.err" && ok=0
row "t1_torchrun.py: WORLD_SIZE lost -> stops, says why" $ok "rc=$rc err=$(tail -1 "$t/py.err")"
if [ -x "$BPY" ] && "$BPY" -c 'import torch' 2>/dev/null; then
	BENCH_EXPECT_WORLD=1 BENCH_EPOCHS=0 "$BPY" "$BAS/t1_torchrun.py" >"$t/py.out" 2>"$t/py.err"
	rc=$?
	ok=1
	[ "$rc" = 0 ] && grep -q 'final loss' "$t/py.out" && ok=0
	row "t1_torchrun.py: expected world 1 and none set -> runs" $ok "rc=$rc err=$(tail -1 "$t/py.err")"
else
	echo "SKIP t1_torchrun.py single-rank row: no torch in the dev venv"
fi

echo "PASS $pass/$total"
[ "$pass" = "$total" ]
