# test/ports_w3.sh — S8c's rows (W3 ports), sourced by test/ports_test.sh.
# shellcheck shell=bash
PY=${PORTS_PY:?}
RAY_ADDR=${PORTS_RAY_ADDR:?}
DASK_ADDR=${PORTS_DASK_ADDR:?}
hosts2=${PORTS_HOSTS2:?}
workdir=${PORTS_WORKDIR:?}

ORIG_W3="BENCH_TREES=20 '$PY' w3_rf.py"

port_ray_case w3-ray-accuracy \
	"$ORIG_W3" \
	"BENCH_TREES=20 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w3_ray.py" \
	'accuracy:'
port_dask_case w3-dask-accuracy \
	"$ORIG_W3" \
	"BENCH_TREES=20 DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/w3_dask.py" \
	'accuracy:'
# The ssh port trains disjoint tree subsets (random_state 7+part), so its forest
# is not the original's: workloads.md allows |delta| <= 0.005 on accuracy for it
# only (ray and dask stay exact). 0.9719 vs 0.9727 (0.0008) is that difference,
# not a fault.
PORT_TOL=0.005 port_ssh_case w3-ssh-accuracy \
	"$ORIG_W3" \
	"BENCH_TREES=20 LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/w3_ssh.sh" \
	'accuracy:'

port_fail_ray_case w3-ray-bad-address \
	"RAY_ADDRESS='127.0.0.1:59999' BENCH_TREES=20 '$PY' baselines/w3_ray.py"
port_fail_dask_case w3-dask-bad-address \
	"DASK_SCHEDULER='tcp://127.0.0.1:59999' BENCH_TREES=20 '$PY' baselines/w3_dask.py"
