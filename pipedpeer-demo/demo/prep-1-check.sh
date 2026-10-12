#!/usr/bin/env bash
# Run on LAPTOP-A once every machine (except the live-join PC-3) has joined. Read-only: it changes
# nothing, it only says whether the cluster is in the shape the demo needs.
#
#   ./demo/prep-1-check.sh
source "$(dirname "$0")/_lib.sh"

fail=0
ok() { echo "  ${G}ok${N}    $*"; }
bad() { echo "  ${R}FAIL${N}  $*"; fail=1; }
warn() { echo "  ${Y}warn${N}  $*"; }

banner "Is the cluster ready for the demo?"

if "$PIPE" status 2>/dev/null | grep -q running; then ok "daemon running here"
else bad "daemon not running here -> pipedpeer start"; fi

echo
if ! table="$("$PIPE" nodes 2>/dev/null)" || ! grep -qE '^NODE(_ID)?[[:space:]]' <<<"$table"; then
	bad "no node table - the daemon is not answering here, so nothing else can be checked"
	echo; echo "${R}${B}NOT READY${N}"; exit 1
fi
echo "$table"
echo

rows() { awk 'NR>1 && NF>=4 && $NF != "self"' <<<"$table"; }
peers=$(peer_count)
if [[ "$peers" -ge 2 ]]; then ok "$peers healthy peers besides this machine"
elif [[ "$peers" -eq 1 ]]; then warn "only 1 peer - the demo works, but say 'two machines'"
else bad "no peers - on LAPTOP-A check 'systemctl --user status pipedpeer-rendezvous', then pipedpeer join <LAPTOP-A IP> on each machine"; fi

# A '-' in PATH means the machines know of each other but have no direct link.
nopath=$(rows | awk '$(NF-1) == "-"' | wc -l)
if [[ "$peers" -eq 0 ]]; then :
elif [[ "$nopath" -eq 0 ]]; then ok "every peer has a direct path (punched / punched-in / lan)"
else bad "$nopath peer(s) with no path - run 'pipedpeer net-check' on them"; fi

# Each peer's daemon port must take a TCP connection from here: Wi-Fi client
# isolation lets machines see each other in the table yet blocks the jobs.
source "$DEMO_DIR/demo/_checks.sh"
while read -r name host port; do
	if [[ "$host" != "-" ]] && reach "$host" "$port"; then ok "$name reachable at $host:$port"
	else bad "$name unreachable at $host:$port"; reach_fail "$name" "$host" "$port"; fi
done < <(peer_addrs)

gpus=$(gpu_nodes)
case "$gpus" in
0) warn "no GPU machine - the GPU step will be skipped" ;;
1) warn "one GPU machine - the GPU step runs on one GPU, without --ddp 2" ;;
*) ok "$gpus GPU machines - the GPU step also runs as 2-rank DDP" ;;
esac

if [[ -f 03_pandas_ooc/data.csv ]]; then ok "03_pandas_ooc/data.csv present ($(du -h 03_pandas_ooc/data.csv | cut -f1))"
else bad "no data.csv - run ./demo/prep-2-rehearse.sh"; fi

if [[ -f 07_everything.py ]]; then ok "07_everything.py present"
else warn "no 07_everything.py - the everything step and its finale job are skipped"; fi

for mod in numpy pandas sklearn; do
	if python3 -c "import $mod" 2>/dev/null; then ok "python3 here has $mod (needed for the local baseline only)"
	else warn "python3 here has no $mod - compare.sh will show 'cannot run' for the local half"; fi
done

echo
echo "this binary: $(sha256sum "$PIPE" | cut -c1-16)  - compare with prep-0 output on every machine"

echo
if [[ "$fail" -eq 0 ]]; then echo "${G}${B}READY${N}"; else echo "${R}${B}NOT READY - fix the FAIL lines above${N}"; exit 1; fi
