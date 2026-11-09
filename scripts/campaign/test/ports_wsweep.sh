# test/ports_wsweep.sh — SW1's rows (sweep ports), sourced by
# test/ports_test.sh, which owns the row DSL, the local clusters and these
# variables (exports below); rows run in the runner's work dir.
# shellcheck shell=bash
# Rows (step 5, SW1 case table): the Ray, Dask and plain-ssh ports of
# sweep_map.py print the same checksum as the original at the same small size,
# with and without a payload (the payload rows also prove the ssh part read
# the file it was sent; a missing file is a direct part test in
# test_sweep.py).
PY=${PORTS_PY:?ports_test.sh exports PORTS_PY}
RAY_ADDR=${PORTS_RAY_ADDR:?}
DASK_ADDR=${PORTS_DASK_ADDR:?}
hosts2=${PORTS_HOSTS2:?}
workdir=${PORTS_WORKDIR:?}

# 4 items x 1 arithmetic term: small enough that the checksum is the whole
# point, as the other port rows are.
SIZE_PLAIN="BENCH_ITEMS=4 BENCH_TASK_MS=1 BENCH_ITERS_PER_MS=1 BENCH_PROCS=1 BENCH_PAYLOAD_BYTES=0"
SIZE_PAYLOAD="BENCH_ITEMS=2 BENCH_TASK_MS=1 BENCH_ITERS_PER_MS=1 BENCH_PROCS=1 BENCH_PAYLOAD_BYTES=1000"
ORIG_SWEEP="$SIZE_PLAIN '$PY' sweep_map.py"
ORIG_PAYLOAD="$SIZE_PAYLOAD '$PY' sweep_map.py"

port_ray_case sweep-ray \
	"$ORIG_SWEEP" \
	"$SIZE_PLAIN RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/sweep_ray.py" \
	SWEEP-OK
port_dask_case sweep-dask \
	"$ORIG_SWEEP" \
	"$SIZE_PLAIN DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/sweep_dask.py" \
	SWEEP-OK
port_ssh_case sweep-ssh \
	"$ORIG_SWEEP" \
	"$SIZE_PLAIN LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' \
	 LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/sweep_ssh.sh" \
	SWEEP-OK

port_ray_case sweep-payload-ray \
	"$ORIG_PAYLOAD" \
	"$SIZE_PAYLOAD RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/sweep_ray.py" \
	SWEEP-OK
port_dask_case sweep-payload-dask \
	"$ORIG_PAYLOAD" \
	"$SIZE_PAYLOAD DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/sweep_dask.py" \
	SWEEP-OK
port_ssh_case sweep-payload-ssh \
	"$ORIG_PAYLOAD" \
	"$SIZE_PAYLOAD LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' \
	 LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/sweep_ssh.sh" \
	SWEEP-OK
