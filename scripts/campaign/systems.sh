#!/usr/bin/env bash
# systems.sh (brief S9): per-system commands, result regexes and size keys.
#
# Sourced by run.sh after lib.sh. `sys_cmd <system> <workload> <n>` prints the
# command line executed in ~/pipedpeer-lab/work on pc00, with the workload's
# size values from sizes.env baked in (run.sh exports them first). An optional
# 4th argument names the run's dir on the PC, so the receipt/timeline land in
# runs/<run_id>/ (the campaign layout) instead of the shared work dir.
#
# pipedpeer has no sync details here: pipedpeer-headcount is the same command
# (E4 sets PIPEDPEER_POOL_SPLIT on pc00's daemon before it runs), and
# pipedpeer-noshim only adds -e PIPEDPEER_SHIM=0.

# shellcheck disable=SC2088  # the tildes expand on the remote shell, not here
set -euo pipefail
# _sys_script <w> [args]: workload -> script file in the work dir plus args.
_sys_script() {
	local w="$1"
	case "$w" in
		W1) printf '%s' 'pool_map.py' ;;
		W2) printf '%s' "cpu.py --items ${W2_ITEMS:?W2_ITEMS not in sizes.env}" ;;
		W3) printf '%s' 'w3_rf.py' ;;
		T1) printf '%s' 't1_mlp.py' ;;
		idle) printf '%s' 'pool_idle.py' ;;
		trivial) printf '%s' 'trivial.py' ;;
		*) echo "sys: unknown workload $w" >&2; return 1 ;;
	esac
}

# _sys_port <w>: the baselines/ file name stem (w1, w2, w3, t1, trivial).
_sys_port() {
	local w="$1"
	case "$w" in
		W1) printf '%s' 'w1' ;;
		W2) printf '%s' 'w2' ;;
		W3) printf '%s' 'w3' ;;
		T1) printf '%s' 't1' ;;
		trivial) printf '%s' 'trivial' ;;
		*) echo "sys: no port for workload $w" >&2; return 1 ;;
	esac
}

# _sys_sizeenv <w> <n>: `K=V K=V` workload size assignments (values from
# sizes.env, which run.sh exports). Emptied when the workload's size is an
# argument, not an env var. Weak mode (BENCH_WEAK=1, set by run.sh) replaces
# W1's fixed item count with the per-PC count times n.
_sys_sizeenv() {
	local w="$1" n="$2" items
	case "$w" in
		W1 | idle)
			if [[ "${BENCH_WEAK:-}" == 1 ]]; then
				items=$((W1_WEAK_PER_PC * n))
			else
				items="${W1_ITEMS:?W1_ITEMS not in sizes.env}"
			fi
			printf 'BENCH_ITEMS=%s BENCH_PROCS=%s' "$items" "${BENCH_PROCS:-0}"
			;;
		W3) printf 'BENCH_TREES=%s' "${W3_TREES:?W3_TREES not in sizes.env}" ;;
		T1) printf 'BENCH_EPOCHS=%s' "${T1_EPOCHS:?T1_EPOCHS not in sizes.env}" ;;
		W2 | trivial) ;;
		*) echo "sys: unknown workload $w" >&2; return 1 ;;
	esac
}

# _pp_size_args [K=V...]: "-e K=V" for every assignment on stdin-ish input.
_pp_size_args() {
	local kv args=""
	for kv in $1; do
		args+=" -e $kv"
	done
	printf '%s' "$args"
}

# sys_cmd <system> <workload> <n> [run_id]
sys_cmd() {
	local sys="$1" w="$2" n="$3" run_id="${4:-}"
	local script args="" sizeenv stem pairs="" sync
	script="$(_sys_script "$w")"
	sizeenv="$(_sys_sizeenv "$w" "$n")"
	pairs="$(_pp_size_args "$sizeenv")"
	stem="$(_sys_port "$w")"
	# $script already carries W2's --items (an extra $args there doubled it); the
	# baselines (ray, dask, ssh) take it from $args.
	case "$w" in W2) args="--items ${W2_ITEMS:?W2_ITEMS not in sizes.env}" ;; esac

	case "$sys" in
		plain)
			printf '%s\n' "~/pipedpeer-lab/venv/bin/python $script"
			;;
		pipedpeer | pipedpeer-headcount | pipedpeer-noshim)
			local shim="" grab
			[[ "$sys" == pipedpeer-noshim ]] && shim="-e PIPEDPEER_SHIM=0"
			# No `-e PIPEDPEER_RECEIPT=`: a path the user names replaces the daemon's own
			# receipt, and the run record then loses every other machine's steps. The
			# daemon keeps the receipt in the run's job dir; copy the newest one out.
			# A run without one (shim off) is recorded as none, with a line on stderr.
			# shellcheck disable=SC2016 # expands on the PC
			grab='jd="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/jobs"; r="$jd/$(ls -t "$jd" | head -1)/receipt.json"; if [ -f "$r" ]; then cp "$r" "$DEST" || echo "receipt copy failed: remote work not recorded" >&2; else echo "no receipt in the job dir: remote work not recorded" >&2; fi'
			if [[ -n "$run_id" ]]; then
				printf '%s\n' "pipedpeer run $pairs $shim $script && pipedpeer jobs last --export json >~/pipedpeer-lab/runs/$run_id/timeline.json && { DEST=\"\$HOME/pipedpeer-lab/runs/$run_id/receipt.json\"; $grab; }"
			else
				printf '%s\n' "pipedpeer run $pairs $shim $script && pipedpeer jobs last --export json >timeline.json && { DEST=receipt.json; $grab; }"
			fi
			;;
		pipedpeer-exact | pipedpeer-fast)
			sync="exact"
			[[ "$sys" == pipedpeer-fast ]] && sync="fast"
			printf '%s\n' "pipedpeer run --gpu off --ddp $n --ddp-sync $sync -e BENCH_SEED=${BENCH_SEED:-11} -e BENCH_EPOCHS=${T1_EPOCHS:?T1_EPOCHS not in sizes.env} t1_mlp.py"
			;;
		ray)
			printf '%s\n' "RAY_ADDRESS=$(lab_ip pc00):6379 ~/pipedpeer-lab/venv/bin/python baselines/${stem}_ray.py $args"
			;;
		dask)
			printf '%s\n' "DASK_SCHEDULER=tcp://$(lab_ip pc00):8786 ~/pipedpeer-lab/venv/bin/python baselines/${stem}_dask.py $args"
			;;
		ssh)
			# pc00's own key (install.sh) and a first-contact host key: parallel's
			# ssh is told so, no ~/.ssh/config is left on the PC.
			printf '%s\n' "PARALLEL_SSH='ssh -i ~/.ssh/pipedpeer-lab-pc00 -o BatchMode=yes -o StrictHostKeyChecking=accept-new' LAB_N=$n LAB_HOSTS_FILE=~/pipedpeer-lab/hosts-$n.txt $sizeenv bash baselines/${stem}_ssh.sh $args"
			;;
		torchrun)
			[[ "$w" == T1 ]] || { echo "sys: torchrun runs only T1" >&2; return 1; }
			printf '%s\n' "~/pipedpeer-lab/venv/bin/torchrun --nnodes=$n --nproc-per-node=1 --master-addr=$(lab_ip pc00) --master-port=29500 baselines/t1_torchrun.py"
			;;
		*) echo "sys: unknown system $sys" >&2; return 1 ;;
	esac
}

# sys_result_regex <w>: the lines taken as the run's result.
sys_result_regex() {
	local w="$1"
	case "$w" in
		W1) printf '%s\n' '^POOLMAP-OK items=[0-9]+ seconds=[0-9.]+ checksum=[0-9.]+$' ;;
		idle) printf '%s\n' '^POOLIDLE-OK items=[0-9]+ seconds=[0-9.]+ checksum=[0-9.]+$' ;;
		W2) printf '%s\n' '^(Pool: [0-9]+ items, checksum [0-9.]+|joblib: [0-9]+ Mandelbrot tiles, [0-9]+ points inside|numpy: largest singular value [0-9.]+|pandas: [0-9,]+ rows -> [0-9]+ cities, total rain [0-9.]+)$' ;;
		W3) printf '%s\n' '^accuracy: [0-9.]+$' ;;
		T1) printf '%s\n' '^(final loss|weight checksum): [0-9.]+$' ;;
		trivial) printf '%s\n' '^TRIVIAL-OK$' ;;
		*) echo "sys: unknown workload $w" >&2; return 1 ;;
	esac
}

# sys_sizekey <w> <n> <mode>: the size key in reference file names.
# Strong mode has one calibrated size per workload (compare every N against
# it); weak mode's size grows with N, so its key is the N.
sys_sizekey() {
	local mode="$3"
	case "$mode" in
		strong) printf '%s\n' 'strong' ;;
		weak) printf 'n%s\n' "$2" ;;
		*) echo "sys: unknown size mode $mode" >&2; return 1 ;;
	esac
}
