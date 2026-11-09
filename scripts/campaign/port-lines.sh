#!/usr/bin/env bash
# port-lines.sh: writes $LAB/port-lines.csv (contracts) — for every baseline
# port in docs/benchmarks/workloads/baselines/, how many lines it changes from
# its original (plan §1.3). For a .py port: `diff -u orig port | grep -c
# '^[+-][^+-]'`. For a `<w>_ssh.sh` with a part: that part's diff count plus
# the non-blank, non-comment lines of the .sh itself (the join glue); without
# a part (trivial) just those lines. PipedPeer's own row is 0, by design: it
# runs the original scripts unchanged.
# Usage: port-lines.sh            # writes $LAB/port-lines.csv
#        port-lines.sh -h
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")/.." rev-parse --show-toplevel)"
# shellcheck source=lib.sh source-path=SCRIPTDIR
source "$REPO/scripts/campaign/lib.sh"
usage() {
	echo "usage: ${0##*/}   # writes \$LAB/port-lines.csv" >&2
	exit 1
}
case "${1:-}" in
-h | --help) usage ;;
'') ;;
*) usage ;;
esac

BAS="$REPO/docs/benchmarks/workloads/baselines"

# original_for <w>: the script a <w>_* port covers. Ports of workloads whose
# files a later unit ships (w2 demo, w3/t1 copies) resolve the same way, so
# their rows appear as their files land.
original_for() {
	case "$1" in
	w1) printf '%s' "$REPO/docs/benchmarks/workloads/pool_map.py" ;;
	w2) printf '%s' "$REPO/pipedpeer-demo/cpu.py" ;;
	w3) printf '%s' "$REPO/docs/benchmarks/workloads/w3_rf.py" ;;
	t1) printf '%s' "$REPO/docs/benchmarks/workloads/t1_mlp.py" ;;
	idle) printf '%s' "$REPO/docs/benchmarks/workloads/pool_idle.py" ;;
	trivial) printf '%s' "$REPO/docs/benchmarks/workloads/trivial.py" ;;
	sweep) printf '%s' "$REPO/docs/benchmarks/workloads/sweep_map.py" ;;
	noop) printf '%s' "$REPO/docs/benchmarks/workloads/noop_map.py" ;;
	lat) printf '%s' "$REPO/docs/benchmarks/workloads/lat_map.py" ;;
	*) return 1 ;;
	esac
}

# diff_lines <orig> <changed>: the changed-line metric of the plan.
diff_lines() {
	local text n
	# diff exits 1 when the files differ; only its output matters here.
	text=$(diff -u "$1" "$2" || true)
	n=$(printf '%s\n' "$text" | grep -c '^[+-][^+-]' || true)
	printf '%s\n' "$n"
}

# sh_lines <file>: non-blank, non-comment lines (the glue that is new code).
sh_lines() {
	grep -Evc '^[[:space:]]*(#|$)' "$1" || true
}

row() { # <port_path> <w>
	local port="$1" w="$2" orig name lines glue
	orig=$(original_for "$w" || true)
	if [ ! -f "$orig" ]; then
		lab_log "port-lines: no original for $w; skipping ${port##*/}"
		return 0
	fi
	name=${port##*/}
	case $name in
	*_ssh.sh) lines=$(sh_lines "$port") ;; # no part: only the glue is new code
	*) lines=$(diff_lines "$orig" "$port") ;;
	esac
	# An ssh baseline has two files: the part (a real diff of the original)
	# and the glue (counted whole, it has no common ancestor with any file).
	glue=""
	case $name in
	*_ssh_part.py) glue=${name%_ssh_part.py}_ssh.sh ;;
	*_torchrun.py) glue=${name%_torchrun.py}_torchrun.sh ;; # torchrun launcher
	esac
	if [ -n "$glue" ] && [ -f "$BAS/$glue" ]; then
		lines=$((lines + $(sh_lines "$BAS/$glue")))
	fi
	printf '%s,%s,%s\n' "baselines/$name" "${orig#"$REPO"/}" "$lines" >>"$tmp_out"
}

mkdir -p "$LAB"
out="$LAB/port-lines.csv"
tmp_out="$(mktemp "$out.XXXXXX")"
trap 'rm -f "$tmp_out"' EXIT
printf 'port,original,changed_lines\n' >>"$tmp_out"
printf 'pipedpeer,,0\n' >>"$tmp_out"

for port in "$BAS"/*_ray.py "$BAS"/*_ray_pool.py "$BAS"/*_dask.py "$BAS"/*_ssh_part.py "$BAS"/*_torchrun.py "$BAS"/*_ssh.sh; do
	[ -f "$port" ] || continue
	case ${port##*/} in
	*_ssh.sh) continue ;; # counted with its part; trivial_ssh.sh alone has no part
	esac
	w=${port##*/}
	w=${w%_ray.py}
	w=${w%_ray_pool.py} # the labelled Pool arm of E6; its name is its label
	w=${w%_dask.py}
	w=${w%_ssh_part.py}
	w=${w%_torchrun.py}
	w=${w%_ssh_part.sh}
	row "$port" "$w"
done
# An ssh glue script with no matching part (trivial) is its own row.
for port in "$BAS"/*_ssh.sh; do
	[ -f "$port" ] || continue
	name=${port##*/}
	w=${name%_ssh.sh}
	[ -f "$BAS/${w}_ssh_part.py" ] && continue
	row "$port" "$w"
done

mv "$tmp_out" "$out"
trap - EXIT
lab_log "port-lines: wrote $out ($(($(wc -l <"$out") - 1)) rows)"
