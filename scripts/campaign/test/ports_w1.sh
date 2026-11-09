# test/ports_w1.sh — S8a's rows (W1, idle, trivial ports), sourced by
# test/ports_test.sh, which owns the row DSL, the local clusters and these
# variables (exports below); rows run in the runner's work dir.
# shellcheck shell=bash
# Rows (brief S8a case table): every port's result line matches the original's
# at the small size (W1 items=48, idle items=16, trivial exact), and a port
# given an unreachable address exits non-zero fast. The ssh rows also prove the
# two-PC split equals the original's checksum bit for bit.
PY=${PORTS_PY:?ports_test.sh exports PORTS_PY}
RAY_ADDR=${PORTS_RAY_ADDR:?}
DASK_ADDR=${PORTS_DASK_ADDR:?}
hosts2=${PORTS_HOSTS2:?}
workdir=${PORTS_WORKDIR:?}

ORIG_W1="BENCH_ITEMS=48 BENCH_PROCS=8 '$PY' pool_map.py"
ORIG_IDLE="BENCH_ITEMS=16 BENCH_PROCS=8 '$PY' pool_idle.py"
ORIG_TRIVIAL="'$PY' trivial.py"

port_ray_case w1-ray \
	"$ORIG_W1" \
	"BENCH_ITEMS=48 BENCH_PROCS=8 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w1_ray.py" \
	POOLMAP-OK
port_dask_case w1-dask \
	"$ORIG_W1" \
	"BENCH_ITEMS=48 BENCH_PROCS=8 DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/w1_dask.py" \
	POOLMAP-OK
port_ssh_case w1-ssh \
	"$ORIG_W1" \
	"BENCH_ITEMS=48 BENCH_PROCS=8 LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' \
	 LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/w1_ssh.sh" \
	POOLMAP-OK

port_ray_case idle-ray \
	"$ORIG_IDLE" \
	"BENCH_ITEMS=16 BENCH_PROCS=8 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/idle_ray.py" \
	POOLIDLE-OK
port_dask_case idle-dask \
	"$ORIG_IDLE" \
	"BENCH_ITEMS=16 BENCH_PROCS=8 DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/idle_dask.py" \
	POOLIDLE-OK
port_ssh_case idle-ssh \
	"$ORIG_IDLE" \
	"BENCH_ITEMS=16 BENCH_PROCS=8 LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' \
	 LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/idle_ssh.sh" \
	POOLIDLE-OK

port_ray_case trivial-ray \
	"$ORIG_TRIVIAL" \
	"RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/trivial_ray.py" \
	'TRIVIAL-OK'
port_dask_case trivial-dask \
	"$ORIG_TRIVIAL" \
	"DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/trivial_dask.py" \
	'TRIVIAL-OK'
port_exact_ssh_case trivial-ssh \
	"$ORIG_TRIVIAL" \
	"LAB_HOSTS_FILE='$hosts2' LAB_USER='$(id -un)' bash baselines/trivial_ssh.sh" \
	'TRIVIAL-OK'

# Unreachable addresses: exit non-zero fast, never hang.
port_fail_ray_case w1-ray-bad-address \
	"RAY_ADDRESS='127.0.0.1:59999' '$PY' baselines/w1_ray.py"
port_fail_dask_case w1-dask-bad-address \
	"DASK_SCHEDULER='tcp://127.0.0.1:59999' '$PY' baselines/w1_dask.py"
