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
		TS1 | TS10 | TS100 | TS1000 | DS10K | DS1M | DS20M | SB | EL) printf '%s' 'sweep_map.py' ;;
		noop) printf '%s' 'noop_map.py' ;;
		lat) printf '%s' 'lat_map.py' ;;
		fileio) printf '%s' 'fileio.py' ;;
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
		idle) printf '%s' 'idle' ;;
		TS1 | TS10 | TS100 | TS1000 | DS10K | DS1M | DS20M | EL) printf '%s' 'sweep' ;;
		noop) printf '%s' 'noop' ;;
		lat) printf '%s' 'lat' ;;
		SB | fileio) return 1 ;; # E20 runs on Pipedpeer only: no baseline port
		*) echo "sys: no port for workload $w" >&2; return 1 ;;
	esac
}

# _sys_sweep_params <w>: `<items> <task_ms> <payload_bytes>` of a sweep
# workload (sizes.env keys, Contracts in the step-5 brief); fails loud on an
# older sizes.env. Callers run it in $(...), so a missing key ends only that.
_sys_sweep_params() {
	local w="$1" ms
	case "$w" in
		TS1 | TS10 | TS100 | TS1000)
			ms="${w#TS}"
			printf '%s %s 0' "$((${SWEEP_TOTAL_MS:?SWEEP_TOTAL_MS not in sizes.env} / ms))" "$ms"
			;;
		DS10K) printf '%s %s 10000' "${SWEEP_DATA_ITEMS:?SWEEP_DATA_ITEMS not in sizes.env}" "${SWEEP_DATA_TASK_MS:?SWEEP_DATA_TASK_MS not in sizes.env}" ;;
		DS1M) printf '%s %s 1000000' "${SWEEP_DATA_ITEMS:?SWEEP_DATA_ITEMS not in sizes.env}" "${SWEEP_DATA_TASK_MS:?SWEEP_DATA_TASK_MS not in sizes.env}" ;;
		DS20M) printf '%s %s 20000000' "${SWEEP_DATA_ITEMS:?SWEEP_DATA_ITEMS not in sizes.env}" "${SWEEP_DATA_TASK_MS:?SWEEP_DATA_TASK_MS not in sizes.env}" ;;
		SB) printf '%s 100 0' "$((${SWEEP_TOTAL_MS:?SWEEP_TOTAL_MS not in sizes.env} / 4 / 100))" ;;
		EL) printf '%s %s 0' "${ELASTIC_ITEMS:?ELASTIC_ITEMS not in sizes.env}" "${ELASTIC_TASK_MS:?ELASTIC_TASK_MS not in sizes.env}" ;;
		*) echo "sys: not a sweep workload $w" >&2; return 1 ;;
	esac
}

# _sys_sizeenv <w> <n>: `K=V K=V` workload size assignments (values from
# sizes.env, which run.sh exports). Emptied when the workload's size is an
# argument, not an env var. Weak mode (BENCH_WEAK=1, set by run.sh) replaces
# W1's fixed item count with the per-PC count times n.
_sys_sizeenv() {
	local w="$1" n="$2" items ms payload params
	case "$w" in
		TS1 | TS10 | TS100 | TS1000 | DS10K | DS1M | DS20M | SB | EL)
			params="$(_sys_sweep_params "$w")" || return 1
			read -r items ms payload <<<"$params"
			printf 'BENCH_ITEMS=%s BENCH_TASK_MS=%s BENCH_ITERS_PER_MS=%s BENCH_PROCS=%s BENCH_PAYLOAD_BYTES=%s' \
				"$items" "$ms" "${SWEEP_ITERS_PER_MS:?SWEEP_ITERS_PER_MS not in sizes.env}" "${BENCH_PROCS:-0}" "$payload"
			;;
		noop) printf 'BENCH_ITEMS=%s BENCH_PROCS=%s' "${NOOP_ITEMS:?NOOP_ITEMS not in sizes.env}" "${BENCH_PROCS:-0}" ;;
		lat) printf 'BENCH_CALLS=%s' "${LAT_CALLS:?LAT_CALLS not in sizes.env}" ;;
		fileio) printf 'BENCH_FILES=%s BENCH_FILE_KB=%s' "${FILEIO_FILES:?FILEIO_FILES not in sizes.env}" "${FILEIO_FILE_KB:?FILEIO_FILE_KB not in sizes.env}" ;;
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
	# SB and fileio have no baseline port: stem stays empty, and only the
	# ray/dask/ssh branches below refuse that.
	stem="$(_sys_port "$w" 2>/dev/null)" || stem=""
	# $script already carries W2's --items (an extra $args there doubled it); the
	# baselines (ray, dask, ssh) take it from $args.
	case "$w" in W2) args="--items ${W2_ITEMS:?W2_ITEMS not in sizes.env}" ;; esac

	case "$sys" in
		plain)
			printf '%s\n' "~/pipedpeer-lab/venv/bin/python $script"
			;;
		pipedpeer | pipedpeer-headcount | pipedpeer-noshim | pipedpeer-bare)
			local shim="" grab flags=""
			[[ "$sys" == pipedpeer-bare ]] && flags=" --isolate=false"
			# E15/E16: work must leave the machine, or a no-op never measures the network
			case "$w" in noop | lat) flags+=" --distribute force" ;; esac
			[[ "$sys" == pipedpeer-noshim ]] && shim="-e PIPEDPEER_SHIM=0"
			# No `-e PIPEDPEER_RECEIPT=`: a path the user names replaces the daemon's own
			# receipt, and the run record then loses every other machine's steps. The
			# daemon keeps the receipt in the run's job dir; copy the newest one out.
			# A run without one (shim off) is recorded as none, with a line on stderr.
			# shellcheck disable=SC2016 # expands on the PC
			grab='jd="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/jobs"; r="$jd/$(ls -t "$jd" | head -1)/receipt.json"; if [ -f "$r" ]; then cp "$r" "$DEST" || echo "receipt copy failed: remote work not recorded" >&2; else echo "no receipt in the job dir: remote work not recorded" >&2; fi'
			if [[ -n "$run_id" ]]; then
				printf '%s\n' "pipedpeer run$flags $pairs $shim $script && pipedpeer jobs last --export json >~/pipedpeer-lab/runs/$run_id/timeline.json && { DEST=\"\$HOME/pipedpeer-lab/runs/$run_id/receipt.json\"; $grab; }"
			else
				printf '%s\n' "pipedpeer run$flags $pairs $shim $script && pipedpeer jobs last --export json >timeline.json && { DEST=receipt.json; $grab; }"
			fi
			;;
		pipedpeer-exact | pipedpeer-fast)
			sync="exact"
			[[ "$sys" == pipedpeer-fast ]] && sync="fast"
			printf '%s\n' "pipedpeer run --gpu off --ddp $n --ddp-sync $sync -e BENCH_SEED=${BENCH_SEED:-11} -e BENCH_EPOCHS=${T1_EPOCHS:?T1_EPOCHS not in sizes.env} t1_mlp.py"
			;;
		ray | ray-pool)
			[[ -n "$stem" ]] || { echo "sys: $sys has no port for $w" >&2; return 1; }
			if [[ "$sys" == ray-pool ]]; then
				# E6's labelled arm: the old multiprocessing.Pool code on Ray, W1 only
				[[ "$w" == W1 ]] || { echo "sys: ray-pool runs only W1" >&2; return 1; }
				stem="w1"
				printf '%s\n' "RAY_ADDRESS=$(lab_ip pc00):6379 ~/pipedpeer-lab/venv/bin/python baselines/w1_ray_pool.py $args"
				return 0
			fi
			printf '%s\n' "RAY_ADDRESS=$(lab_ip pc00):6379 ~/pipedpeer-lab/venv/bin/python baselines/${stem}_ray.py $args"
			;;
		dask)
			[[ -n "$stem" ]] || { echo "sys: dask has no port for $w" >&2; return 1; }
			printf '%s\n' "DASK_SCHEDULER=tcp://$(lab_ip pc00):8786 ~/pipedpeer-lab/venv/bin/python baselines/${stem}_dask.py $args"
			;;
		ssh)
			[[ -n "$stem" ]] || { echo "sys: ssh has no port for $w" >&2; return 1; }
			# pc00's own key (install.sh) and a first-contact host key: parallel's
			# ssh is told so, no ~/.ssh/config is left on the PC.
			printf '%s\n' "PARALLEL_SSH='ssh -i ~/.ssh/pipedpeer-lab-pc00 -o BatchMode=yes -o StrictHostKeyChecking=accept-new' LAB_N=$n LAB_HOSTS_FILE=~/pipedpeer-lab/hosts-$n.txt $sizeenv bash baselines/${stem}_ssh.sh $args"
			;;
		torchrun)
			[[ "$w" == T1 ]] || { echo "sys: torchrun runs only T1" >&2; return 1; }
			# one rank per PC over ssh (the script reads the hosts file)
			printf '%s\n' "LAB_HOSTS_FILE=~/pipedpeer-lab/hosts-$n.txt BENCH_SEED=${BENCH_SEED:-11} BENCH_EPOCHS=${T1_EPOCHS:?T1_EPOCHS not in sizes.env} bash baselines/t1_torchrun.sh $n"
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
		TS1 | TS10 | TS100 | TS1000 | DS10K | DS1M | DS20M | SB | EL) printf '%s\n' '^SWEEP-OK items=[0-9]+ seconds=[0-9.]+ checksum=[0-9.]+$' ;;
		noop) printf '%s\n' '^NOOP-OK items=[0-9]+ seconds=[0-9.]+ tasks_per_s=[0-9.]+$' ;;
		lat) printf '%s\n' '^LAT-OK n=[0-9]+ p50_ms=[0-9.]+ p90_ms=[0-9.]+ p99_ms=[0-9.]+$' ;;
		fileio) printf '%s\n' '^FILEIO-OK files=[0-9]+ bytes=[0-9]+ seconds=[0-9.]+$' ;;
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
