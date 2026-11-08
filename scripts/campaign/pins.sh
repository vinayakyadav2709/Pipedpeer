#!/usr/bin/env bash
# pins.sh: take the pipedpeer closure's Python stack as the campaign's
# reference versions (brief S6).
#
# Runs `pipedpeer run versions.py` on pc00 (so the versions are the ones the
# pipedpeer closure ships, not the driver's), holds them in versions.csv as
# the `pipedpeer` row, and writes the per-PC pins:
#
#   requirements.txt   numpy==, pandas==, scikit-learn==, joblib== (exact
#                      closure versions), torch==<closure torch>+cpu with the
#                      PyTorch CPU wheel index, ray[default], dask[distributed]
#                      (no versions: the closure carries neither by default),
#                      no tqdm (E5 adds it)
#   python-version     the closure's Python `x.y` (venv.sh installs it with uv)
#
# When PyPI (or the PyTorch CPU index) has no wheel for the exact pin, the
# nearest lower version is pinned instead, with a
# `# pinned nearest: <wanted> -> <got>` comment on that line. The check needs
# pip on the driver and a network; set PINS_WHEEL_CHECK=0 to skip it (tests)
# and keep the exact pins — venv.sh's pip install fails loudly later if a pin
# cannot be had.

set -euo pipefail

# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'EOF'
usage: pins.sh

Run versions.py in the pipedpeer closure on pc00 and write, from its
VERSIONS line, $LAB/versions.csv (pipedpeer row), $LAB/requirements.txt
(numpy, pandas, scikit-learn, joblib, torch+cpu, ray[default],
dask[distributed]) and $LAB/python-version (the closure's Python x.y).

Environment:
  PINS_WHEEL_CHECK  0 skips the "does the index have that exact pin" check
EOF
}

# _pin_of <line> <name>: the value of `name=value` on that VERSIONS line.
_pin_of() {
	local -a parts=()
	read -ra parts <<<"$1"
	local f name="$2"
	for f in "${parts[@]}"; do
		case "$f" in
			"$name="*) printf '%s\n' "${f#*=}"; return 0 ;;
		esac
	done
	return 1
}

# _pin_nearest <pkg> <wanted> [pip index args...]: that exact version's
# availability on the index; when pip has no wheel for it, the nearest lower
# version. Prints the version to pin ("" when pip itself failed).
_pin_nearest() {
	local pkg="$1" wanted="$2"
	shift 2
	local out avail nearest
	out="$(python3 -m pip index versions "$pkg" "$@" 2>/dev/null || true)"
	# shellcheck disable=SC2020
	avail="$(printf '%s\n' "$out" | sed -n 's/^Available versions:[[:space:]]*//p' | head -1 | tr ', ' '\n\n' || true)"
	if [[ -z "$avail" ]]; then
		lab_log "pins: pip index versions $pkg gave nothing; keeping the exact pin $wanted"
		return 1
	fi
	# The PyTorch CPU index lists "2.5.1+cpu": compare without the local tag.
	avail="$(printf '%s\n' "$avail" | sed 's/+.*//')"
	if printf '%s\n' "$avail" | grep -qxF -- "$wanted"; then
		printf '%s\n' "$wanted"
		return 0
	fi
	nearest="$(AVAIL="$avail" python3 - "$wanted" <<'PYEOF'
import os
import sys

w = sys.argv[1]
avail = os.environ.get("AVAIL", "").split()


def key(v):
    try:
        return tuple(int(p) for p in v.split("+")[0].split("."))
    except (ValueError, TypeError):
        return None


kw = key(w)
best = None
for v in avail:
    k = key(v)
    if k is None or kw is None or k >= kw:
        continue
    if best is None or k > best[0]:
        best = (k, v)
print(best[1] if best else "")
PYEOF
)"
	if [[ -z "$nearest" ]]; then
		lab_log "pins: no lower version of $pkg than $wanted; keeping the exact pin"
	fi
	printf '%s\n' "$nearest"
}

# _status_version <pc>: pipedpeer's version from that PC's status first line.
_status_version() {
	local pc="$1" out
	out="$(lab_ssh "$pc" 'pipedpeer status')" || {
		lab_log "pins: cannot reach pc00's status (exit $?)"
		return 1
	}
	printf '%s\n' "$out" | head -1 | sed -n 's/.*version=\([^ )]*\).*/\1/p'
}

main() {
	case "${1:-}" in
		"" ) ;;
		-h | --help) usage; exit 0 ;;
		*) usage >&2; exit 2 ;;
	esac
	[[ -f "$LAB/hosts.tsv" ]] || {
		lab_log "pins: no hosts.tsv"
		echo "no $LAB/hosts.tsv: run scan.sh first" >&2
		exit 1
	}

	# versions.py inside the closure: rsync it into the PC's work dir first.
	local rc
	rc=0
	lab_rsync "$REPO/docs/benchmarks/workloads/versions.py" pc00:pipedpeer-lab/work/versions.py || rc=$?
	if ((rc != 0)); then
		lab_log "pins: could not rsync versions.py to pc00"
		echo "cannot rsync versions.py to pc00" >&2
		exit 1
	fi

	local out line ppver rc2
	out="$(lab_ssh pc00 'cd ~/pipedpeer-lab/work && pipedpeer run versions.py')" || rc2=$?
	if [[ -n "${rc2:-}" && "${rc2:-0}" != 0 ]]; then
		lab_log "pins: pipedpeer run versions.py on pc00 exited with $rc2"
		echo "pipedpeer run versions.py on pc00 failed (exit $rc2)" >&2
		exit 1
	fi
	line="$(printf '%s\n' "$out" | grep -m1 '^VERSIONS ' || true)"
	[[ -n "$line" ]] || {
		lab_log "pins: no VERSIONS line in the pipedpeer run output"
		echo "no VERSIONS line in the output of pipedpeer run versions.py" >&2
		exit 1
	}

	local py np pd sk jb tr ra da
	py="$(_pin_of "$line" python)" np="$(_pin_of "$line" numpy)"
	pd="$(_pin_of "$line" pandas)" sk="$(_pin_of "$line" sklearn)"
	jb="$(_pin_of "$line" joblib)" tr="$(_pin_of "$line" torch)"
	ra="$(_pin_of "$line" ray)" da="$(_pin_of "$line" dask)"

	local missing=0 v
	for f in python numpy pandas sklearn joblib torch; do
		case "$f" in
			python) v="$py" ;;
			numpy) v="$np" ;;
			pandas) v="$pd" ;;
			sklearn) v="$sk" ;;
			joblib) v="$jb" ;;
			torch) v="$tr" ;;
		esac
		if [[ -z "$v" || "$v" == "none" ]]; then
			lab_log "pins: the closure has no $f"
			echo "the pipedpeer closure has no $f; pins.sh cannot pin it" >&2
			missing=1
		fi
	done
	((missing == 0)) || exit 1
	ppver="$(_status_version pc00)" || ppver=""

	# requirements.txt
	local tr_base
	tr_base="${tr%%+*}"
	local req="$LAB/requirements.txt"
	{
		printf 'numpy==%s\n' "$np"
		printf 'pandas==%s\n' "$pd"
		printf 'scikit-learn==%s\n' "$sk"
		printf 'joblib==%s\n' "$jb"
		printf -- '--extra-index-url https://download.pytorch.org/whl/cpu\n'
		printf 'torch==%s+cpu\n' "$tr_base"
		printf 'ray[default]\n'
		printf 'dask[distributed]\n'
		printf 'pyarrow\n' # dask.dataframe (w2_dask.py) imports it
	} >"$req"

	# wheel availability; a missing pin falls back to the nearest lower one
	if [[ "${PINS_WHEEL_CHECK:-1}" != 0 ]]; then
		local chosen pkg wanted
		for pkg in numpy pandas scikit-learn joblib; do
			wanted="$(_pin_of "$line" "$pkg")"
			[[ -n "$wanted" ]] || continue
			chosen="$(_pin_nearest "$pkg" "$wanted")"
			if [[ -n "$chosen" && "$chosen" != "$wanted" ]]; then
				sed -i "s|^${pkg}==.*|${pkg}==${chosen}  # pinned nearest: ${wanted} -> ${chosen}|" "$req"
				lab_log "pins: ${pkg} ${wanted} -> ${chosen} (nearest lower on PyPI)"
			fi
		done
		chosen="$(_pin_nearest torch "$tr_base" --index-url https://download.pytorch.org/whl/cpu)"
		if [[ -n "$chosen" && "$chosen" != "$tr_base" ]]; then
			sed -i "s|^torch==.*|torch==${chosen}+cpu  # pinned nearest: ${tr_base} -> ${chosen}|" "$req"
			lab_log "pins: torch ${tr_base} -> ${chosen} (nearest lower in the CPU index)"
		fi
	fi

	# python-version: the closure's Python `x.y`
	printf '%s\n' "${py%.*}" >"$LAB/python-version"

	# versions.csv: the pipedpeer row (own row replaced on a re-run)
	local tmp="$LAB/versions.csv.tmp"
	{
		[[ -f "$LAB/versions.csv" ]] || printf 'system,python,numpy,pandas,sklearn,joblib,torch,ray,dask,pipedpeer\n'
		if [[ -f "$LAB/versions.csv" ]]; then
			awk -F, 'NR == 1 || $1 != "pipedpeer"' "$LAB/versions.csv"
		fi
	} >"$tmp"
	printf 'pipedpeer,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$py" "$np" "$pd" "$sk" "$jb" "$tr" "$ra" "$da" "${ppver:-none}" >>"$tmp"
	mv "$tmp" "$LAB/versions.csv"
	lab_log "pins: python=$py numpy=$np pandas=$pd sklearn=$sk joblib=$jb torch=$tr ray=$ra dask=$da pipedpeer=${ppver:-none}"
	printf 'PINS python=%s torch=%s ray=%s dask=%s\n' "$py" "$tr" "$ra" "$da"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	main "$@"
fi
