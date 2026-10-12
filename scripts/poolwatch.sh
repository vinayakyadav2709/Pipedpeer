#!/usr/bin/env bash
# Watch where pool work runs in the lab-lan containers, once a second.
#
#   scripts/poolwatch.sh [seconds] [pc...]      # default: 45 s, pc1-pc4
#
# One line per tick, one column per pc:
#   ex=   items executed on that pc so far        (/v1/pool/stats items_executed_here)
#   can=  pool chunks cancelled on that pc so far (/v1/pool/stats chunks_cancelled)
#   py=   python processes running in that pc right now
# Run it in a second terminal while a pool job runs: ex= grows on the pcs that
# take work; after Ctrl+C every pc's ex= stops and its can= goes up (VERIFY B3).
# It runs on the host and reads each pc's daemon over the lab bridge, so the
# containers need neither curl nor python. The lab's cluster secret is the
# host's (`pipedpeer auth show`), sent as X-Pipedpeer-Token.
set -euo pipefail

secs="${1:-45}"
shift || true
pcs=("$@")
((${#pcs[@]})) || pcs=(pc1 pc2 pc3 pc4)

token="$(pipedpeer auth show 2>/dev/null || true)"
declare -A ip
for pc in "${pcs[@]}"; do
	ip[$pc]="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "pp-lan-$pc" 2>/dev/null || true)"
	[[ -n "${ip[$pc]}" ]] || echo "warn: no container pp-lan-$pc (is the lab up?)" >&2
done

stats() { # ip -> "ex=N can=N", or "ex=? can=?" with a reason on stderr
	local body
	if ! body="$(curl -fsS --max-time 2 -H "X-Pipedpeer-Token: $token" "http://$1:38080/v1/pool/stats" 2>&1)"; then
		echo "warn: $1: $body" >&2
		echo "ex=? can=?"
		return
	fi
	python3 -I -c 'import json,sys; s=json.loads(sys.argv[1]); print("ex=%d can=%d" % (s.get("items_executed_here",0), s.get("chunks_cancelled",0)))' "$body"
}

end=$((SECONDS + secs))
while ((SECONDS < end)); do
	line="$(date +%H:%M:%S.%N | cut -c1-10)"
	for pc in "${pcs[@]}"; do
		[[ -n "${ip[$pc]}" ]] || continue
		py="$(docker exec "pp-lan-$pc" sh -c 'grep -l python /proc/[0-9]*/comm 2>/dev/null | wc -l' 2>/dev/null || echo '?')"
		line+="  $pc $(stats "${ip[$pc]}") py=$py"
	done
	echo "$line"
	sleep 1
done
