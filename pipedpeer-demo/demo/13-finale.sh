#!/usr/bin/env bash
# Finale - every demo script at once, spread over the cluster (LAPTOP-A).
#
#   ./demo/13-finale.sh                 # all of them
#   ./demo/13-finale.sh pool numpy      # a subset, by name (see the list below)
#
# Each job goes out with 'pipedpeer python' (round-robin, --remote) and writes
# to demo/finale-logs/<name>.log. Ctrl+C stops every job everywhere.
source "$(dirname "$0")/_lib.sh"

# name  dir  script  extra args
all=(
	"pool        .              00_pool.py"
	"sklearn     .              01_sklearn_rf.py"
	"numpy       .              02_numpy_heavy.py"
	"pandas      03_pandas_ooc  03_pandas_ooc.py   -e PIPEDPEER_OOC_MIN=1e8"
	"torch       .              04_torch_ddp.py"
	"files       .              05_file_sync.py"
	"lambda      .              06_pool_lambda.py"
	"everything  .              07_everything.py"
)

pick=()
for row in "${all[@]}"; do
	read -r name _ <<<"$row"
	if [[ $# -eq 0 ]]; then pick+=("$row"); continue; fi
	for want in "$@"; do [[ "$want" == "$name" ]] && pick+=("$row"); done
done
if [[ ${#pick[@]} -eq 0 ]]; then
	echo "no job matches: $* (names: pool sklearn numpy pandas torch files lambda everything)" >&2
	exit 2
fi

logs="$DEMO_DIR/demo/finale-logs"
mkdir -p "$logs"

banner "Finale - ${#pick[@]} jobs at once"
note "Watch the dashboard: every row busy."
pause

pids=() names=()
for row in "${pick[@]}"; do
	read -r name dir script extra <<<"$row"
	if [[ ! -f "$dir/$script" ]]; then
		echo "${Y}  skipping $name: $dir/$script not found${N}"
		continue
	fi
	# shellcheck disable=SC2086 # extra is a word list on purpose
	(cd "$dir" && exec "$PIPE" python "$script" --job-name "finale-$name" $extra) \
		</dev/null >"$logs/$name.log" 2>&1 &
	pids+=($!) names+=("$name")
	echo "  started $name  (log: demo/finale-logs/$name.log)"
done
if [[ ${#pids[@]} -eq 0 ]]; then
	echo "${R}  nothing started${N}"
	exit 1
fi

# SIGTERM is what pipedpeer treats as Ctrl+C; background jobs of a script
# ignore SIGINT, so it is passed on as TERM.
stop() {
	echo
	echo "${R}  stopping every job${N}"
	# The watcher too: left running, the 'wait' below never returns.
	kill -TERM "${pids[@]}" ${watcher:+"$watcher"} 2>/dev/null
	wait
	exit 130
}
trap stop INT TERM

echo
"$PIPE" tasks --watch &
watcher=$!
rcs=()
for i in "${!pids[@]}"; do
	wait "${pids[$i]}"
	rcs[i]=$?
done
kill "$watcher" 2>/dev/null
wait "$watcher" 2>/dev/null
trap - INT TERM

strip() { sed 's/\x1b\[[0-9;]*m//g' "$1"; }
banner "Finale results"
printf '  %-12s %-22s %-10s %s\n' JOB "RAN ON" TOTAL RESULT
fail=0
for i in "${!names[@]}"; do
	log="$logs/${names[$i]}.log"
	on=$(strip "$log" | grep -E '^ *ran on ' | tail -1 | awk '{print $3}')
	t=$(strip "$log" | grep -Eo '^TOTAL [0-9.]+s' | tail -1 | awk '{print $2}')
	if [[ "${rcs[$i]}" -eq 0 ]]; then res="${G}ok${N}"; else res="${R}fail (exit ${rcs[$i]})${N}"; fail=1; fi
	printf '  %-12s %-22s %-10s %s\n' "${names[$i]}" "${on:-?}" "${t:-?}" "$res"
done
echo
[[ "$fail" -eq 0 ]] || note "A failed job: read its log in demo/finale-logs/. 'no eligible node' = max-concurrent or memory; rerun that one alone."
note "RAN ON '?' = that job printed no summary; its log says where it went."
exit "$fail"
