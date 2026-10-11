#!/usr/bin/env bash
# Run on LAPTOP-A, in the lab, at the setup visit AND ~30 minutes before.
#
#   ./demo/prep-2-rehearse.sh
#   DEMO_CSV_ROWS=3000000 ./demo/prep-2-rehearse.sh    # smaller pandas file
#
# Two jobs:
#  1. warm everything (setup.sh): data.csv, every environment built and on
#     every machine, so nothing on stage waits on a Nix build;
#  2. time every step as it will run live, and write the numbers to
#     demo/rehearsal-results.txt:
#       cold     wall time of 'pipedpeer run ... --cold' (environment resent)
#       warm     wall time of the same run again (only the script travels)
#       python3  the script's own TOTAL under plain python3 (compare.sh)
#       cluster  the script's own TOTAL under pipedpeer (compare.sh)
#
# Whether a job is faster across machines than on one depends on how much
# data it ships and how fast the links are - only measuring here tells.
# Present only the steps that win; the rest go in the "if asked" pile.
source "$(dirname "$0")/_lib.sh"

out="$DEMO_DIR/demo/rehearsal-results.txt"

banner "1/2  Warm-up: data, environments, closures (setup.sh)"
./setup.sh || { echo "${R}setup.sh failed - fix that before anything else${N}"; exit 1; }

banner "2/2  Timed rehearsal"
{
	echo "Rehearsal on $(date '+%Y-%m-%d %H:%M'), $(peer_count) peer(s), $(gpu_nodes) GPU machine(s)"
	echo
	printf '%-18s %-8s %-8s %-9s %-9s %s\n' step cold warm python3 cluster "speedup / note"
} >"$out"

took() { grep -Eo '^TOTAL [0-9.]+s' "$1" | tail -1 | awk '{print $2}'; }

# timed <log> <run args...>: wall seconds of one pipedpeer run, "fail" on error.
timed() {
	local log="$1" t0=$SECONDS
	shift
	if "$PIPE" run "$@" 2>&1 | tee "$log"; [[ "${PIPESTATUS[0]}" -eq 0 ]]; then
		echo "$((SECONDS - t0))s" >"$log.t"
	else
		echo "fail" >"$log.t"
	fi
}

# cold_warm <args...>: sets $cold and $warm for one run command.
cold_warm() {
	local log
	log="$(mktemp)"
	timed "$log" "$@" --cold
	cold=$(<"$log.t")
	timed "$log" "$@"
	warm=$(<"$log.t")
	last_log="$log"
}

for s in 00_pool.py 06_pool_lambda.py 01_sklearn_rf.py 02_numpy_heavy.py; do
	cold_warm "$s"
	rm -f "$last_log" "$last_log.t"
	log="$(mktemp)"
	./compare.sh "$s" 2>&1 | tee "$log"
	loc=$(grep -E '^ this machine only:' "$log" | sed 's/^ this machine only: *//')
	clu=$(grep -E '^ across the cluster:' "$log" | sed 's/^ across the cluster: *//')
	spd=$(grep -E '^ speedup:' "$log" | awk '{print $2}')
	printf '%-18s %-8s %-8s %-9s %-9s %s\n' "$s" "$cold" "$warm" "${loc:-?}" "${clu:-?}" "${spd:--}" >>"$out"
	rm -f "$log"
done

# The pandas step has no fair local baseline on a big-RAM laptop (it fits in
# memory there); what matters is that it went out-of-core and how long it took.
cd 03_pandas_ooc
cold_warm 03_pandas_ooc.py -e PIPEDPEER_OOC_MIN=1e8
cd ..
if grep -q 'ooc csv fallback' "$last_log"; then mode="FELL BACK to one machine"
elif grep -q 'combining .* chunk partials' "$last_log"; then mode="out-of-core across cluster"
else mode="?"; fi
printf '%-18s %-8s %-8s %-9s %-9s %s\n' "03_pandas_ooc.py" "$cold" "$warm" "-" "$(took "$last_log")" "$mode" >>"$out"
rm -f "$last_log" "$last_log.t"

# GPU: as on stage, but from here. Cold resends several GB - expect minutes.
g=$(gpu_nodes)
if [[ "$g" -ge 1 ]]; then
	if [[ "$g" -ge 2 ]]; then args=(--ddp 2 --gpu force); else args=(--gpu force); fi
	cold_warm 04_torch_ddp.py "${args[@]}"
	gpu=$(grep -Eo 'using GPU: .*' "$last_log" | head -1)
	printf '%-18s %-8s %-8s %-9s %-9s %s\n' "04_torch_ddp.py" "$cold" "$warm" "-" "$(took "$last_log")" "${args[*]} ${gpu:-NO GPU LINE}" >>"$out"
	rm -f "$last_log" "$last_log.t"
fi

if [[ -f 07_everything.py ]]; then
	cold_warm 07_everything.py
	printf '%-18s %-8s %-8s %-9s %-9s %s\n' "07_everything.py" "$cold" "$warm" "-" "$(took "$last_log")" "" >>"$out"
	rm -f "$last_log" "$last_log.t"
fi

# Finale: everything at once, warm only.
t0=$SECONDS
if ./demo/13-finale.sh </dev/null; then res="all ok"; else res="SOME FAILED - see demo/finale-logs/"; fi
printf '%-18s %-8s %-8s %-9s %-9s %s\n' "finale (all)" "-" "$((SECONDS - t0))s" "-" "-" "$res" >>"$out"

banner "Rehearsal results  (saved to demo/rehearsal-results.txt)"
cat "$out"
echo
note "A speedup under ~1.2x on the real links: move that step to 'if asked'."
note "pandas 'FELL BACK': the binary on some machine differs (compare prep-0 hashes)."
note "cold much bigger than warm is the point of the cold/cached step; warm near cold = environment resent every time."
