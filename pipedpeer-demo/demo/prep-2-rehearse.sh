#!/usr/bin/env bash
# Run on box-a, on the real networks, the day before AND ~30 minutes before.
#
#   ./demo/prep-2-rehearse.sh
#   DEMO_CSV_ROWS=3000000 ./demo/prep-2-rehearse.sh    # smaller pandas file
#
# Two jobs:
#  1. warm everything (setup.sh): data.csv, every environment built and on
#     every machine, so nothing on stage waits on a Nix build;
#  2. time the comparison steps exactly as they will run live, and write the
#     numbers to demo/rehearsal-results.txt.
#
# Step 2 is the point. Whether a job is faster on three machines than on one
# depends on how much data it ships and how fast the links between your
# networks are - and that is only known by measuring on those links. Present
# only the steps that win here; the rest go in the "if asked" pile.
source "$(dirname "$0")/_lib.sh"

out="$DEMO_DIR/demo/rehearsal-results.txt"

banner "1/2  Warm-up: data, environments, closures (setup.sh)"
./setup.sh || { echo "${R}setup.sh failed - fix that before anything else${N}"; exit 1; }

banner "2/2  Timed rehearsal (warm, as it will be on stage)"
{
	echo "Rehearsal on $(date '+%Y-%m-%d %H:%M'), $(peer_count) peer(s), $(gpu_nodes) GPU machine(s)"
	echo
	printf '%-22s %-14s %-14s %s\n' step "this machine" cluster speedup
} >"$out"

for s in 00_pool.py 01_sklearn_rf.py 02_numpy_heavy.py; do
	log="$(mktemp)"
	./compare.sh "$s" 2>&1 | tee "$log"
	loc=$(grep -E '^ this machine only:' "$log" | sed 's/^ this machine only: *//')
	clu=$(grep -E '^ across the cluster:' "$log" | sed 's/^ across the cluster: *//')
	spd=$(grep -E '^ speedup:' "$log" | awk '{print $2}')
	printf '%-22s %-14s %-14s %s\n' "$s" "${loc:-?}" "${clu:-?}" "${spd:--}" >>"$out"
	rm -f "$log"
done

# The pandas step has no fair local baseline on a big-RAM laptop (it fits in
# memory there); what matters is that it went out-of-core and how long it took.
log="$(mktemp)"
(cd 03_pandas_ooc && "$PIPE" run 03_pandas_ooc.py -e PIPEDPEER_OOC_MIN=1e8) 2>&1 | tee "$log"
t=$(grep -Eo '^TOTAL [0-9.]+s' "$log" | tail -1 | awk '{print $2}')
if grep -q 'ooc csv fallback' "$log"; then mode="FELL BACK to one machine"
elif grep -q 'combining .* chunk partials' "$log"; then mode="out-of-core across cluster"
else mode="?"; fi
printf '%-22s %-14s %-14s %s\n' "03_pandas_ooc.py" "-" "${t:-failed}" "$mode" >>"$out"
rm -f "$log"

g=$(gpu_nodes)
if [[ "$g" -ge 1 ]]; then
	ddp=()
	[[ "$g" -ge 2 ]] && ddp=(--ddp 2)
	log="$(mktemp)"
	"$PIPE" run 04_torch_ddp.py "${ddp[@]}" 2>&1 | tee "$log"
	t=$(grep -Eo '^TOTAL [0-9.]+s' "$log" | tail -1 | awk '{print $2}')
	printf '%-22s %-14s %-14s %s\n' "04_torch_ddp.py" "-" "${t:-failed}" "${ddp[*]:-1 GPU}" >>"$out"
	rm -f "$log"
fi

banner "Rehearsal results  (saved to demo/rehearsal-results.txt)"
cat "$out"
echo
note "A speedup under ~1.2x on the real links: move that step to 'if asked'."
note "pandas 'FELL BACK': the binary on some machine lacks the items_zlib fix."
