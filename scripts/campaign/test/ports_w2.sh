# test/ports_w2.sh — S8b's rows (W2 ports), sourced by test/ports_test.sh.
# shellcheck shell=bash
PY=${PORTS_PY:?}
RAY_ADDR=${PORTS_RAY_ADDR:?}
DASK_ADDR=${PORTS_DASK_ADDR:?}
hosts2=${PORTS_HOSTS2:?}
workdir=${PORTS_WORKDIR:?}

ORIG_W2="BENCH_ITEMS=48 '$PY' $REPO/pipedpeer-demo/cpu.py --items 48"
# The original is cpu.py; the campaign runs it via the demo path. Use the repo copy.
ORIG_W2="'$PY' $REPO/pipedpeer-demo/cpu.py --items 48"

port_ray_case w2-ray-pool \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w2_ray.py --items 48" \
	'Pool:'
port_ray_case w2-ray-joblib \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w2_ray.py --items 48" \
	'joblib:'
port_ray_case w2-ray-numpy \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w2_ray.py --items 48" \
	'numpy:'
port_ray_case w2-ray-pandas \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 RAY_ADDRESS='$RAY_ADDR' '$PY' baselines/w2_ray.py --items 48" \
	'pandas:'

port_dask_case w2-dask-pool \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/w2_dask.py --items 48" \
	'Pool:'
port_dask_case w2-dask-joblib \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/w2_dask.py --items 48" \
	'joblib:'
port_dask_case w2-dask-numpy \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/w2_dask.py --items 48" \
	'numpy:'
port_dask_case w2-dask-pandas \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 DASK_SCHEDULER='$DASK_ADDR' '$PY' baselines/w2_dask.py --items 48" \
	'pandas:'

port_ssh_case w2-ssh-pool \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/w2_ssh.sh" \
	'Pool:'
port_ssh_case w2-ssh-joblib \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/w2_ssh.sh" \
	'joblib:'
port_ssh_case w2-ssh-numpy \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/w2_ssh.sh" \
	'numpy:'
port_ssh_case w2-ssh-pandas \
	"$ORIG_W2" \
	"BENCH_ITEMS=48 LAB_HOSTS_FILE='$hosts2' LAB_N=2 LAB_USER='$(id -un)' LAB_PY='$PY' LAB_WORKDIR='$workdir' bash baselines/w2_ssh.sh" \
	'pandas:'

port_fail_ray_case w2-ray-bad-address \
	"RAY_ADDRESS='127.0.0.1:59999' '$PY' baselines/w2_ray.py --items 48"
port_fail_dask_case w2-dask-bad-address \
	"DASK_SCHEDULER='tcp://127.0.0.1:59999' '$PY' baselines/w2_dask.py --items 48"
