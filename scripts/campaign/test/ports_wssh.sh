# test/ports_wssh.sh — SS1's rows (the ssh ports' env forwarding, W2's --items,
# W3's tree count), sourced by test/ports_test.sh, which owns the row DSL and
# the shared PORTS_* paths. Rows run in the runner's work dir.
# shellcheck shell=bash
#
# Every row name starts with `wssh-` so a subset run is `ports_test.sh -k wssh`.
#
# Cases (unit SS1):
#   1 w2_ssh.sh --items 16            -> parts sum to 16, join says Pool: 16 items
#   2 w2_ssh.sh without --items       -> default 128 (ordinary use)
#   3 BENCH_ITEMS=40 on 2 hosts       -> parts see 40, checksum = pool_map.py's
#   4 BENCH_PROCS=0                   -> part uses its own PC's cpu_count
#   5 BENCH_PROCS=3                   -> part uses 3
#   6 W3, a part returns too few trees-> join exits non-zero, names the part
#   7 W3, every part complete         -> accuracy line unchanged
PY=${PORTS_PY:?ports_test.sh exports PORTS_PY}
hosts2=${PORTS_HOSTS2:?}
workdir=${PORTS_WORKDIR:?}
REPO=${REPO:?ports_test.sh sets REPO}
logs=${logs:?ports_test.sh sets logs}
statedir=${statedir:?ports_test.sh sets statedir}

# 1. --items N sets the size for the parts and the join (H4).
port_ssh_case wssh-w2-items16 \
	"'$PY' $REPO/pipedpeer-demo/cpu.py --items 16" \
	"LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' LAB_PY='$PY' \
	 LAB_WORKDIR='$workdir' bash baselines/w2_ssh.sh --items 16" \
	'Pool:'

# 2. Ordinary use without the argument keeps cpu.py's default of 128.
port_ssh_case wssh-w2-default \
	"'$PY' $REPO/pipedpeer-demo/cpu.py --items 128" \
	"LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' LAB_PY='$PY' \
	 LAB_WORKDIR='$workdir' bash baselines/w2_ssh.sh" \
	'Pool:'

# 3. BENCH_ITEMS reaches the remote parts (H3): 40 items in, 40 out, and the
# join's checksum equals pool_map.py's, so the item order is the original's.
port_ssh_case wssh-w1-items40 \
	"BENCH_ITEMS=40 BENCH_PROCS=8 '$PY' pool_map.py" \
	"BENCH_ITEMS=40 BENCH_PROCS=8 LAB_HOSTS_FILE='$hosts2' LAB_N=2 \
	 LAB_USER='$(id -un)' LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/w1_ssh.sh" \
	POOLMAP-OK

# 4/5. The part reports the Pool size it used, so 0 means "this PC's cpu_count"
# (H3) and a number is that number. The part runs directly: no ssh involved.
wssh_part_procs_row() { # <name> <BENCH_PROCS> <expected>
	local name=$1 procs=$2 want=$3 out got rc
	FILTERED "$name" || return 0
	out=$(_run "$name" "BENCH_ITEMS=2 BENCH_PROCS=$procs '$PY' baselines/w1_ssh_part.py --part 0 --parts 1")
	rc=$?
	got=$(printf '%s' "$out" | "$PY" -c 'import json, sys; print(json.load(sys.stdin)["procs"])' 2>>"$logs/$name.err") || got=
	if [ "$rc" = 0 ] && [ "$got" = "$want" ]; then
		row_pass "$name"
	else
		row_fail "$name" "exit $rc, part used BENCH_PROCS=${got:-<none>}, expected $want"
		_errtail "$name"
	fi
}
want_cpu=$("$PY" -c 'import os; print(os.cpu_count() or 1)')
wssh_part_procs_row wssh-w1-procs0 0 "$want_cpu"
wssh_part_procs_row wssh-w1-procs3 3 3

# 6. A part that trained fewer trees than its share must stop the join and be
# named. A fake LAB_PY answers the part call and hands the join to the real
# python, so no real forest is trained.
_wssh_fake_py() {
	local fake="$statedir/wssh-fake-python"
	cat >"$fake" <<FAKE
#!/usr/bin/env bash
# Fake part runner for wssh-w3-short-part: the join still runs the real python.
set -uo pipefail
if [ "\${1:-}" = "-" ]; then exec '$PY' "\$@"; fi
case "\${*}" in
*w3_ssh_part.py*)
	part=0 parts=1 prev=
	for a in "\$@"; do
		case "\$prev" in
		--part) part=\$a ;;
		--parts) parts=\$a ;;
		esac
		prev=\$a
	done
	trees=\${BENCH_TREES:-150}
	exp=\$((trees / parts))
	if [ "\$part" -lt \$((trees % parts)) ]; then exp=\$((exp + 1)); fi
	if [ "\$part" -eq 1 ]; then exp=\$((exp - 1)); fi
	printf '{"part":%d,"trees":%d,"proba":[[0.5,0.5]],"seconds":0.01}\\n' "\$part" "\$exp"
	exit 0
	;;
esac
exec '$PY' "\$@"
FAKE
	chmod +x "$fake"
	printf '%s\n' "$fake"
}
wssh_w3_short_part_row() {
	local name=wssh-w3-short-part fake rc
	FILTERED "$name" || return 0
	if [ "$WORKS_SSH" != 1 ]; then
		row_skip "$name" "$SSH_SKIP_WHY"
		return 0
	fi
	fake=$(_wssh_fake_py)
	_run "$name" "BENCH_TREES=20 LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' LAB_PY='$fake' LAB_WORKDIR='$workdir' bash baselines/w3_ssh.sh" >/dev/null
	rc=$?
	if [ "$rc" != 0 ] && grep -q 'part 1' "$logs/$name.err"; then
		row_pass "$name"
	else
		row_fail "$name" "exit $rc, the short part is not named"
		_errtail "$name"
	fi
}
wssh_w3_short_part_row

# 7. With every part complete the accuracy line still matches the original.
PORT_TOL=0.005 port_ssh_case wssh-w3-complete \
	"BENCH_TREES=20 '$PY' w3_rf.py" \
	"BENCH_TREES=20 LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' \
	 LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/w3_ssh.sh" \
	'accuracy:'
