# test/ports_wray.sh — RY1's rows (Ray ports), sourced by test/ports_test.sh.
# shellcheck shell=bash
PY=${PORTS_PY:?ports_test.sh exports PORTS_PY}
RAY_ADDR=${PORTS_RAY_ADDR:?}
hosts2=${PORTS_HOSTS2:?}
workdir=${PORTS_WORKDIR:?}
REPO=${REPO:?ports_test.sh sets REPO}
logs=${logs:?ports_test.sh sets logs}

ORIG_W1_40="BENCH_ITEMS=40 BENCH_PROCS=8 '$PY' pool_map.py"
port_ray_case wray-w1-40 \
	"$ORIG_W1_40" \
	"BENCH_ITEMS=40 BENCH_PROCS=8 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w1_ray.py" \
	POOLMAP-OK

port_ray_case wray-w1-pool-40 \
	"$ORIG_W1_40" \
	"BENCH_ITEMS=40 BENCH_PROCS=8 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w1_ray_pool.py" \
	POOLMAP-OK

ORIG_W2_8="'$PY' $REPO/pipedpeer-demo/cpu.py --items 8"
port_ray_case wray-w2-pool \
	"$ORIG_W2_8" \
	"BENCH_ITEMS=8 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w2_ray.py --items 8" \
	'Pool:'
port_ray_case wray-w2-joblib \
	"$ORIG_W2_8" \
	"BENCH_ITEMS=8 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w2_ray.py --items 8" \
	'joblib:'
port_ray_case wray-w2-numpy \
	"$ORIG_W2_8" \
	"BENCH_ITEMS=8 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w2_ray.py --items 8" \
	'numpy:'
port_ray_case wray-w2-pandas \
	"$ORIG_W2_8" \
	"BENCH_ITEMS=8 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w2_ray.py --items 8" \
	'pandas:'

port_fail_ray_case wray-w1-bad-address \
	"RAY_ADDRESS='127.0.0.1:59999' '$PY' baselines/w1_ray.py"

wray_source_check() {
	local name=wray-source-check
	FILTERED "$name" || return 0
	local bad=0 msg=""
	for f in baselines/w1_ray.py baselines/idle_ray.py; do
		if ! grep -q '@ray.remote' "$f"; then
			bad=1
			msg="$msg $f missing @ray.remote;"
		fi
		if grep -q 'ray.util.multiprocessing' "$f"; then
			bad=1
			msg="$msg $f still uses ray.util.multiprocessing;"
		fi
	done
	if [ "$bad" = 0 ]; then
		row_pass "$name"
	else
		row_fail "$name" "$msg"
	fi
}
wray_source_check
