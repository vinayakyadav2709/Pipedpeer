#!/usr/bin/env bash
# The internet path, in containers: sites behind real NATs meeting through an
# introducer (lab/nat/compose.yml has the map).
#
#   scripts/lab-nat.sh [nat]   four nodes, two cone sites and two symmetric
#   scripts/lab-nat.sh trust   a multi-user nix machine trusts a later joiner
#
# nat asserts, from the daemons' own answers:
#   - cone<->cone links by an ordinary punch (no collision, no LAN shortcut)
#   - cone<->symmetric links, and one side of each pair says `punched`: the
#     birthday collision is what connected it
#   - symmetric<->symmetric does not link, and both ends list the other as
#     unreachable with the both-symmetric reason
#   - a Pool job from a cone site runs on the other cone site and on a
#     symmetric one (receipt + each worker's /v1/pool/stats)
# trust asserts the sudo helper (FIX-SUDOHELPER): `pp` runs setup once with
# sudo; afterwards pp has no blanket sudo, a new node joins, and a job it
# submits runs on the multi-user machine, which had to trust the new node's
# key with nobody there to type a password.
#
# Every run starts from fresh containers and tears them down (LAB_NAT_KEEP=1
# keeps them). Logs: ~/.local/state/pipedpeer/lab-nat/<time>-<mode>/.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-nat}"
case "$mode" in nat | trust) ;; *) echo "usage: $0 [nat|trust]" >&2; exit 2 ;; esac

source "$repo_root/scripts/lib/runtime.sh"
pp_pick_runtime || exit 1
rt="$PP_RUNTIME"
compose() { (cd "$repo_root/lab/nat" && pp_compose "$@"); }

logdir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/lab-nat/$(date +%Y%m%d-%H%M%S)-$mode"
mkdir -p "$logdir"
echo "logs: $logdir"

# Static, so the alpine introducer runs the same binary as the nodes.
if [[ -d "$repo_root/src" ]]; then
	(cd "$repo_root/src" && CGO_ENABLED=0 go build -o "$repo_root/lab/pipedpeer" .)
elif [[ -x "${PIPEDPEER:-}" ]]; then
	cp "$PIPEDPEER" "$repo_root/lab/pipedpeer"
else
	echo "no source to build from; set PIPEDPEER to a static binary" >&2
	exit 2
fi

LAB_TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
export LAB_TOKEN
rv=198.51.100.10:38445

if [[ "$mode" == nat ]]; then
	nodes=(a b c d)
	services=(rendezvous router-a router-b router-c router-d node-a node-b node-c node-d)
else
	nodes=(b)
	services=(rendezvous router-a router-b mu)
fi

ctr() { [[ "$1" == mu ]] && echo pp-nat-mu || echo "pp-nat-node-$1"; }
x() { local c; c="$(ctr "$1")"; shift; "$rt" exec "$c" "$@"; }
api() { x "$1" curl -sf --max-time 5 -H "X-Pipedpeer-Token: $LAB_TOKEN" "http://127.0.0.1:38080$2"; }
fail() { echo "FAIL: $*"; exit 1; }
# wait_for <seconds> <command...>: poll every 3 s until it succeeds.
wait_for() {
	local secs="$1"; shift
	local end=$((SECONDS + secs))
	until "$@" >/dev/null 2>&1; do
		((SECONDS < end)) || return 1
		sleep 3
	done
}

collect() {
	set +e
	local n
	for n in "${nodes[@]}" $([[ "$mode" == trust ]] && echo mu); do
		x "$n" sh -c 'cat $(find /var/lib/pipedpeer-state /root/.local/state /home/pp/.local/state \
			-name daemon.log 2>/dev/null)' >"$logdir/$n.daemon.log" 2>&1
		"$rt" logs "$(ctr "$n")" >"$logdir/$n.out" 2>&1
	done
	"$rt" logs pp-nat-rendezvous >"$logdir/rendezvous.out" 2>&1
	if [[ "${LAB_NAT_KEEP:-0}" == 1 ]]; then
		echo "LAB_NAT_KEEP=1: containers left running; tear down with: cd lab/nat && $rt compose down"
	else
		compose down --remove-orphans >/dev/null 2>&1
	fi
}

compose down --remove-orphans >/dev/null 2>&1 || true
trap collect EXIT
echo "bringing up: ${services[*]}"
compose up -d --build "${services[@]}" >"$logdir/compose-up.log" 2>&1 ||
	{ tail -30 "$logdir/compose-up.log"; fail "compose up"; }

node_id() { api "$1" /health | python3 -c 'import json,sys; print(json.load(sys.stdin)["node_id"])'; }

# peers <node>: "<node_id> <path>" for every healthy row other than itself.
peers() {
	local self
	self="$(node_id "$1")"
	api "$1" /v1/nodes | python3 -c 'import json,sys
for n in json.load(sys.stdin) or []:
    if n.get("state") == "healthy" and n.get("node_id") != sys.argv[1]:
        print(n["node_id"], (n.get("capabilities") or {}).get("path", "-"))' "$self"
}
path_to() { peers "$1" | awk -v id="$2" '$1 == id { print $2 }'; }
links() { [[ -n "$(path_to "$1" "$2")" ]]; }

# A Pool job from <submitter>; afterwards the caller reads the workers' tallies.
run_pool() {
	local sub="$1"
	# -i: without it exec drops stdin and the task arrives empty (and "passes").
	"$rt" exec -i "$(ctr "$sub")" sh -c 'mkdir -p /work && : > /work/.pipedpeerignore && cat > /work/task.py' <<'EOF'
import os, time
from multiprocessing import Pool

def work(i):
    time.sleep(1 + (i % 3))
    return i * i

if __name__ == "__main__":
    n = int(os.environ.get("LAB_ITEMS", "120"))
    with Pool(4) as p:
        out = p.map(work, range(n))
    assert out == [i * i for i in range(n)], out
    print("POOL-OK sum=%d" % sum(out))
EOF
	echo "running a Pool job from $sub (the first run builds its closure; minutes)"
	if ! x "$sub" sh -c 'cd /work && timeout 1500 pipedpeer run task.py -e PIPEDPEER_RECEIPT=receipt.json' \
		>"$logdir/run-$sub.log" 2>&1; then
		tail -30 "$logdir/run-$sub.log"
		fail "the job on $sub did not finish"
	fi
	grep -q POOL-OK "$logdir/run-$sub.log" || fail "the job on $sub printed no correct result"
	x "$sub" cat /work/receipt.json >"$logdir/receipt-$sub.json" ||
		fail "no receipt on $sub; cannot tell whether anything ran remotely"
	local remote
	remote="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("remote_items", 0))' \
		"$logdir/receipt-$sub.json")"
	((remote > 0)) || fail "receipt shows no item ran off $sub (see $logdir/receipt-$sub.json)"
	echo "  $remote items ran on other nodes"
}
chunks() {
	api "$1" /v1/pool/stats | python3 -c 'import json,sys; print(json.load(sys.stdin).get("chunks_received", 0))' ||
		echo 0
}

if [[ "$mode" == nat ]]; then
	for n in "${nodes[@]}"; do
		wait_for 240 api "$n" /health || fail "node-$n's daemon never answered"
	done
	declare -A id
	for n in "${nodes[@]}"; do id[$n]="$(node_id "$n")"; done
	echo "nodes up: $(for n in "${nodes[@]}"; do printf '%s=%.8s ' "$n" "${id[$n]}"; done)"

	reason_both='fresh port per destination' # direct.ReasonBothSymmetric
	unreachable_both() { api "$1" /v1/peers/unreachable | grep -q "$reason_both"; }
	converged() {
		links a "${id[b]}" && links b "${id[a]}" &&
			for s in c d; do
				for k in a b; do links "$k" "${id[$s]}" && links "$s" "${id[$k]}" || return 1; done
			done &&
			unreachable_both c && unreachable_both d
	}
	# A punch takes up to 15 s and a collision 12 s more, with backoff between
	# attempts; five minutes is several rounds for every pair.
	echo "waiting for every pair to link or give up..."
	if ! wait_for 300 converged; then
		for n in "${nodes[@]}"; do
			echo "node-$n sees:"; peers "$n" | sed 's/^/  /'
			echo "  unreachable: $(api "$n" /v1/peers/unreachable)"
		done
		fail "the cluster did not settle into the expected links within 5 minutes"
	fi

	echo "paths (as seen from each end):"
	pair() { # pair <x> <y> -> "pathXsees pathYsees"
		echo "$(path_to "$1" "${id[$2]}") $(path_to "$2" "${id[$1]}")"
	}
	read -r p1 p2 < <(pair a b)
	echo "  a<->b cone/cone: $p1 / $p2"
	for p in $p1 $p2; do
		[[ "$p" == punched || "$p" == lan ]] &&
			fail "cone<->cone took '$p'; an ordinary punch should have linked it"
	done
	for k in a b; do
		for s in c d; do
			read -r p1 p2 < <(pair "$k" "$s")
			echo "  $k<->$s cone/symmetric: $p1 / $p2"
			[[ "$p1" == punched || "$p2" == punched ]] ||
				fail "$k<->$s linked as '$p1'/'$p2', not by collision; is router-$s really symmetric?"
		done
	done
	for s in c d; do
		o=$([[ $s == c ]] && echo d || echo c)
		links "$s" "${id[$o]}" && fail "symmetric<->symmetric $s<->$o linked; the NATs are not what they claim"
	done
	x c pipedpeer nodes >"$logdir/nodes-c.txt" 2>&1 || true
	grep -q 'Peers with no direct path' "$logdir/nodes-c.txt" ||
		fail "\`pipedpeer nodes\` on c does not list d as unreachable (see $logdir/nodes-c.txt)"
	echo "  c<->d symmetric/symmetric: unreachable, '$reason_both' on both ends"

	run_pool a
	n_b="$(chunks b)" n_c="$(chunks c)" n_d="$(chunks d)"
	echo "  chunks received: b=$n_b c=$n_c d=$n_d"
	((n_b > 0)) || fail "the cone peer b took no work from a"
	((n_c + n_d > 0)) || fail "no symmetric-site peer took work from a"
	echo "PASS: cone<->cone punches, cone<->symmetric collides, symmetric<->symmetric is reported, and work crosses both"
	exit 0
fi

# trust mode.
mu() { "$rt" exec pp-nat-mu "$@"; }
as_pp() {
	mu runuser -u pp -- env HOME=/home/pp XDG_RUNTIME_DIR=/run/user/1000 \
		DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus \
		PATH=/nix/var/nix/profiles/default/bin:/usr/local/bin:/usr/bin:/bin \
		PIPEDPEER_TOKEN="$LAB_TOKEN" PIPEDPEER_RENDEZVOUS="$rv" "$@"
}

wait_for 120 mu systemctl is-active dbus || fail "systemd never came up in mu"
mu ip route replace default via 10.77.1.254
echo "installing multi-user nix in mu (Determinate installer)..."
mu sh -c "curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix |
	sh -s -- install linux --no-confirm" >"$logdir/mu-nix-install.log" 2>&1 ||
	{ tail -20 "$logdir/mu-nix-install.log"; fail "nix install in mu"; }
wait_for 60 as_pp nix store info || fail "pp cannot reach the nix-daemon in mu"
mu loginctl enable-linger pp
wait_for 30 mu test -S /run/user/1000/bus || fail "no user session for pp in mu"

# The one sudo. A temporary blanket rule stands in for the user typing the
# password; it is removed before anything the claim depends on.
mu sh -c "echo 'pp ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/zz-lab-setup && chmod 0440 /etc/sudoers.d/zz-lab-setup"
as_pp pipedpeer setup -y >"$logdir/mu-setup.log" 2>&1 ||
	{ mu rm -f /etc/sudoers.d/zz-lab-setup; tail -20 "$logdir/mu-setup.log"; fail "setup as pp in mu"; }
mu rm -f /etc/sudoers.d/zz-lab-setup
mu test -f /etc/sudoers.d/pipedpeer || fail "setup installed no /etc/sudoers.d/pipedpeer"
as_pp sudo -n true 2>/dev/null && fail "pp still has blanket sudo; the check below would prove nothing"
mu cat /etc/sudoers.d/pipedpeer >"$logdir/mu-sudoers.txt"
wait_for 60 api mu /health || fail "mu's daemon never answered"

echo "a new node joins after setup..."
compose up -d node-b >>"$logdir/compose-up.log" 2>&1 || fail "compose up node-b"
wait_for 240 api b /health || fail "node-b's daemon never answered"
mu_id="$(node_id mu)" b_id="$(node_id b)"
wait_for 300 links mu "$b_id" || fail "mu never linked to the new node b"
wait_for 120 links b "$mu_id" || fail "b never linked to mu"
echo "  mu<->b: $(path_to mu "$b_id") / $(path_to b "$mu_id")"

run_pool b
c_mu="$(chunks mu)"
echo "  chunks received by mu: $c_mu"
((c_mu > 0)) || fail "mu took no work from b: it did not trust b's key (see $logdir/mu.daemon.log)"
echo "PASS: after one sudo at setup, mu trusted a node that joined later, with no prompt"
