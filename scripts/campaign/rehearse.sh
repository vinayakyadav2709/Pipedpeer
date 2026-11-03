#!/usr/bin/env bash
# The runner's rehearsal on the container lab (plan 1.6): bring up three
# containers (LAB_NAME=runner, LAB_MEM=2g), map pc00..pc02 onto them, put
# the campaign env in each, run every experiment at N=1 and 2 with one
# repeat through the real run.sh, run analyse.py, and tear the lab down.
#
#   scripts/campaign/rehearse.sh [--keep] [--reuse] [--only "e1 e4"]
#
# --keep leaves the lab up; --reuse runs on a lab that is already up with the
# env in place (a previous --keep), appending to the same results.
#
# Results go to results/lab/rehearsal/ (never the campaign's runs.jsonl):
# runs.jsonl, logs/, csv/, figures/, REHEARSAL.md (pass/fail per experiment).
#
# The campaign env (scripts/campaign/env, lab/baselines) reaches the
# containers one of three ways, first that works:
#   1. copied from a container that already built it (PL_ENV_FROM, default
#      ppl-baselines): nix-store --export | --import, no download;
#   2. built in each container by scripts/campaign/env/install.sh;
#   3. neither: python3 with numpy, pandas, joblib, scikit-learn from
#      nixpkgs, and only plain and pipedpeer run (PL_SYSTEMS).
#
# What a 3-container lab cannot do, and what the rehearsal does instead, is
# in README.md ("Rehearsal limits").
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
keep=0
reuse=0
only=""
while (($#)); do
	case "$1" in
	--keep) keep=1 ;;
	--reuse) reuse=1 ;;
	--only) only="$2"; shift ;;
	*) echo "usage: rehearse.sh [--keep] [--reuse] [--only \"e1 e4\"]" >&2; exit 2 ;;
	esac
	shift
done

export LAB_NAME="${LAB_NAME:-runner}" LAB_MEM="${LAB_MEM:-2g}"
source "$repo/scripts/lib/runtime.sh"
source "$repo/scripts/lib/lab.sh"
pp_plain_lab_vars || exit 2
pp_pick_runtime || exit 1
rt="$PP_RUNTIME"
out="$repo/results/lab/rehearsal"
inv="$repo/results/lab/inventory-rehearsal.ini"
mkdir -p "$out"
log() { echo "[rehearse $(date +%T)] $*" | tee -a "$out/rehearse.log" >&2; }

if [[ $keep -eq 0 ]]; then
	trap 'log "tearing the lab down"; "$repo/scripts/lab-down.sh" >/dev/null 2>&1 || true' EXIT
fi

w() { "$rt" exec "$LAB_CTR-$1" sh -c "$2"; }
envlink=/root/.local/share/pipedpeer-lab/env
if [[ $reuse -eq 1 ]]; then
	for i in 1 2 3; do
		w "$i" "test -x $envlink/bin/python3" || { log "ERROR: --reuse but $LAB_CTR-$i has no env"; exit 1; }
	done
	log "reusing $LAB_CTR-1..3 and their env"
else
	log "bringing up $LAB_CTR-1..3 ($LAB_MEM each)"
	"$repo/scripts/lab-up.sh" >>"$out/rehearse.log" 2>&1
fi

for i in 1 2 3; do
	for _ in $(seq 1 90); do
		w "$i" 'curl -sf --max-time 3 -H "X-Pipedpeer-Token: $LAB_TOKEN" http://127.0.0.1:4242/health' >/dev/null 2>&1 && break
		sleep 2
	done
done

{
	echo "# Written by scripts/campaign/rehearse.sh: pc00..pc02 are the $LAB_CTR containers."
	echo "[head]"
	echo "pc00 ansible_host=$LAB_CTR-1 lab_ip=$LAB_BASE.11 machine_id=$LAB_CTR-1"
	echo "[workers]"
	echo "pc01 ansible_host=$LAB_CTR-2 lab_ip=$LAB_BASE.12 machine_id=$LAB_CTR-2"
	echo "pc02 ansible_host=$LAB_CTR-3 lab_ip=$LAB_BASE.13 machine_id=$LAB_CTR-3"
	echo "[all:vars]"
	echo "ansible_connection=community.docker.docker"
	echo "ansible_python_interpreter=auto_silent"
} >"$inv"
log "inventory: $inv"

bash "$here/lib/ansible.sh" install >>"$out/rehearse.log" 2>&1
PL_INVENTORY="$inv" bash "$here/lib/ansible.sh" sh all 'cat /etc/hostname' | tee -a "$out/rehearse.log" >&2 ||
	{ log "ERROR: Ansible cannot reach the containers"; exit 1; }

src="${PL_ENV_FROM:-ppl-baselines}"
srcpath="$("$rt" exec "$src" readlink -f "$envlink" 2>/dev/null || true)"
if [[ $reuse -eq 1 ]]; then
	[[ -x $here/env/install.sh || -n $srcpath ]] || export PL_SYSTEMS="${PL_SYSTEMS:-plain,pipedpeer}"
elif [[ -n $srcpath ]] && "$rt" exec "$src" test -x "$srcpath/bin/ray"; then
	log "copying the campaign env $srcpath from $src into each container"
	for i in 1 2 3; do
		"$rt" exec "$src" sh -c "nix-store --export \$(nix-store -qR $srcpath)" |
			"$rt" exec -i "$LAB_CTR-$i" sh -c "nix-store --import >/dev/null && mkdir -p ${envlink%/*} &&
				nix-store --add-root $envlink --indirect -r $srcpath >/dev/null && $envlink/bin/python3 -c 'import ray, dask'" ||
			{ log "ERROR: env copy into $LAB_CTR-$i failed"; exit 1; }
	done
	export PL_SYSTEMS="${PL_SYSTEMS:-}"
elif [[ -x $here/env/install.sh ]]; then
	log "building the campaign env in each container (scripts/campaign/env/install.sh)"
	for i in 1 2 3; do
		"$rt" cp "$here/env" "$LAB_CTR-$i:/root/pplab-env"
		w "$i" 'bash /root/pplab-env/install.sh' >>"$out/rehearse.log" 2>&1 &
	done
	wait
else
	log "no campaign env: python3 from nixpkgs, plain and pipedpeer only"
	for i in 1 2 3; do
		w "$i" "nix-env -iE '_: (import <nixpkgs> {}).python3.withPackages (p: [p.numpy p.pandas p.joblib p.scikit-learn])' >/dev/null 2>&1 &&
			mkdir -p ${envlink%/*} && ln -sfn /root/.nix-profile $envlink" &
	done
	wait
	export PL_SYSTEMS="${PL_SYSTEMS:-plain,pipedpeer}"
fi

# E4's caps need taskset (util-linux), which every lab PC has and the lab
# image does not.
for i in 1 2 3; do
	w "$i" 'command -v taskset >/dev/null || nix-env -iA nixpkgs.util-linux >/dev/null 2>&1' ||
		log "warning: no taskset in $LAB_CTR-$i; E4 will fail there"
done
# E15 needs tc (iproute2) and E13 pkill (procps); every lab PC has both.
for i in 1 2 3; do
	w "$i" 'command -v tc >/dev/null || nix-env -iA nixpkgs.iproute2 >/dev/null 2>&1; command -v pkill >/dev/null || nix-env -iA nixpkgs.procps >/dev/null 2>&1' ||
		log "warning: no tc or pkill in $LAB_CTR-$i; E13 or E15 will fail there"
done

export PL_RESULTS="$out" PL_NPROC="${PL_NPROC:-2}" PL_E11_IDLE_S="${PL_E11_IDLE_S:-30}"
experiments="${only:-e5 e14 e1 e1b e16 e4 e6 e12 e13 e15 e17 e18 e19 e20 e7 e8 e9 e11 e3 e10}"
declare -A status
for e in $experiments; do
	log "=== $e"
	touch "$out/runs.jsonl"
	before=$(wc -l <"$out/runs.jsonl")
	case "$e" in
	e3) feed=$'\n\n3\n0\nrehearsal\n\n\n4\n5\nrehearsal\n\n\n4\n5\nrehearsal\n' ;;
	e10) feed=$'1\ny\n\n\n3\n0\nrehearsal\n\nfail\n5\n6\nrehearsal\n' ;;
	*) feed=$'\n' ;;
	esac
	# The headline experiments need jobs that outlast a join or a kill
	# (README.md, "Rehearsal limits"); E14 runs on all three containers.
	# E6 kills at 30% of the run: the smoke size (about a second) ends
	# before Ansible can deliver the kill, so E6 gets ten times the work.
	case "$e" in
	e6) extra=(--size W1:BENCH_ITEMS=4000) ;;
	e12) extra=(--size W1:BENCH_ITEMS=20000) ;;
	e13) extra=(--size W1:BENCH_ITEMS=10000) ;;   # per PC: weak size x N
	e14) extra=(--n 3 --size W1:BENCH_ITEMS=20000) ;;
	e15) extra=(--size W1:BENCH_ITEMS=2000) ;;
	e18) extra=(--size W1:BENCH_ITEMS=3000) ;;   # outlasts the hog's 3 s start
	*) extra=() ;;
	esac
	set +e
	printf '%s' "$feed" | "$here/run.sh" "$e" --rehearsal --inventory "$inv" "${extra[@]}" >>"$out/rehearse.log" 2>&1
	rc=$?
	set -e
	after=$(wc -l <"$out/runs.jsonl")
	failed=$(tail -n $((after - before)) "$out/runs.jsonl" |
		python3 -c 'import json,sys; print(sum(1 for l in sys.stdin if json.loads(l).get("error")))')
	status[$e]="rc=$rc runs=$((after - before)) failed=$failed"
	log "$e: ${status[$e]}"
done

log "analyse.py"
"${PL_VENV:-$HOME/.local/share/pipedpeer-lab/ansible}/bin/python" "$here/analyse.py" --runs "$out/runs.jsonl" \
	>>"$out/rehearse.log" 2>&1 || log "analyse.py failed"

{
	echo "# Rehearsal $(date -Is)"
	echo
	echo "Commit $(git -C "$repo" rev-parse --short HEAD); lab $LAB_CTR (3 x $LAB_MEM); PL_NPROC=$PL_NPROC; PL_SYSTEMS=${PL_SYSTEMS:-all}."
	echo
	echo "| experiment | result |"
	echo "|---|---|"
	for e in $experiments; do echo "| $e | ${status[$e]} |"; done
	echo
	echo "All runs in runs.jsonl, by experiment:"
	echo
	python3 -c 'import json,sys,collections
c = collections.defaultdict(lambda: [0, 0, 0])
for l in open(sys.argv[1]):
    r = json.loads(l)
    k = c[r["experiment"]]
    k[0] += 1; k[1] += bool(r.get("error")); k[2] += bool(r.get("warmup"))
print("| experiment | runs | failed | warm-ups |\n|---|---|---|---|")
for e, (n, f, w) in sorted(c.items()):
    print("| %s | %d | %d | %d |" % (e, n, f, w))' "$out/runs.jsonl"
	echo
	echo "Failed runs (from runs.jsonl):"
	echo
	python3 -c 'import json,sys
for l in open(sys.argv[1]):
    r = json.loads(l)
    if not r.get("error"):
        continue
    print("- %s %s %s n=%s on %s: %s" % (r["experiment"], r.get("workload"), r.get("system"), r.get("n"), r.get("failing_pc"), r["error"][:200]))' "$out/runs.jsonl" || true
} >"$out/REHEARSAL.md"
log "wrote $out/REHEARSAL.md"
cat "$out/REHEARSAL.md"
