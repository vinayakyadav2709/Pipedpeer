#!/usr/bin/env bash
# venv.sh: one uv-managed CPython + virtualenv on every PC (brief S6).
#
# The venvs should match the closure (versions.py in it is the reference, see
# pins.sh), but Ubuntu desktop 22.04/24.04 ships Python 3.10/3.12 and may
# lack python3-venv, so uv installs the single CPython into
# ~/pipedpeer-lab/py, and the campaign's venv is `~/pipedpeer-lab/venv`
# made by `uv venv --seed` from `$LAB/python-version` (x.y, from pins.sh),
# then `pip install -q -r requirements.txt` with the venv's own pip.
#
# After the fan-out, each PC runs versions.py with its fresh venv; pc00's
# line is the reference, and any PC whose line differs is listed in
# logs/venv/FAILED (and exit 1). versions.csv gets the `venv` row from
# pc00's line. VENV_PIP overrides the pip step (tests run `true`).

set -euo pipefail

# shellcheck source=/dev/null
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
	cat <<'EOF'
usage: venv.sh

Build one venv per PC ~/pipedpeer-lab/venv from $LAB/python-version and
$LAB/requirements.txt with uv and pip; versions.csv gets the `venv` row
from pc00's venv python, and every PC whose versions line differs from
pc00's is FAILED (exit 1).

Environment:
  VENV_PIP  override the pip step inside the venv (tests run `true`)
EOF
}

# _venv_check <dir>: pc00 logs/<step>/pc00's VERSIONS line vs the other PCs'.
# Prints `FAILED <ids...>` on stdout, writes the ids one per line into
# <dir>/FAILED, and returns 1 when any differ, 2 when pc00's line is missing.
_venv_check() {
	local dir="$1" ref pc_line pc bad=0 bad_ids=""
	ref="$(grep -h '^VERSIONS ' "$dir/pc00" 2>/dev/null | head -1 || true)"
	if [[ -z "$ref" ]]; then
		echo "venv: no VERSIONS line from pc00's venv python in $dir/pc00" >&2
		return 2
	fi
	for pc in "$dir"/pc*; do
		[[ -f "$pc" ]] || continue
		pc="$(basename "$pc")"
		[[ "$pc" == pc00 ]] && continue
		pc_line="$(grep -h '^VERSIONS ' "$dir/$pc" 2>/dev/null | head -1 || true)"
		if [[ "$pc_line" != "$ref" ]]; then
			bad=1
			bad_ids+="$pc "
		fi
	done
	rm -f "$dir/FAILED"
	if [[ "$bad" == 1 ]]; then
		printf '%s\n' "${bad_ids% }" | tr ' ' '\n' >"$dir/FAILED"
		printf 'FAILED %s\n' "${bad_ids% }"
	fi
	return "$bad"
}

# _pin_of <line> <name>: the value of `name=value` on a VERSIONS line
# (duplicated from pins.sh; lib.sh is S1's file, these stay local).
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

# venv_command <x.y>: the remote command that builds the venv. uv with an
# install dir keeps CPython under py/ in its own directory and creates no
# py/bin/python3.x, so the venv comes from `uv venv` (it finds the managed
# interpreter through UV_PYTHON_INSTALL_DIR) with pip seeded.
# shellcheck disable=SC2088
venv_command() {
	printf '%s' "export UV_PYTHON_INSTALL_DIR=~/pipedpeer-lab/py && ~/pipedpeer-lab/bin/uv python install --no-bin $1 && ~/pipedpeer-lab/bin/uv venv --managed-python --python $1 --seed ~/pipedpeer-lab/venv && ${VENV_PIP:-~/pipedpeer-lab/venv/bin/pip install -q -r ~/pipedpeer-lab/requirements.txt} && cd ~/pipedpeer-lab/work && ~/pipedpeer-lab/venv/bin/python versions.py"
}

main() {
	case "${1:-}" in
		"") ;;
		-h | --help) usage; exit 0 ;;
		*) usage >&2; exit 2 ;;
	esac
	local uv
	uv="$(command -v uv)" || {
		echo "MISSING: uv" >&2
		lab_log "venv: MISSING: uv"
		exit 1
	}
	[[ -f "$LAB/python-version" ]] || {
		lab_log "venv: no $LAB/python-version"
		echo "no $LAB/python-version: run pins.sh first" >&2
		exit 1
	}
	local pyver
	pyver="$(cat "$LAB/python-version")"
	[[ "$pyver" =~ ^3\.[0-9]+$ ]] || {
		lab_log "venv: python-version has '$pyver', want an x.y"
		echo "$LAB/python-version is '$pyver', want e.g. 3.12" >&2
		exit 2
	}
	[[ -f "$LAB/requirements.txt" ]] || {
		lab_log "venv: no $LAB/requirements.txt"
		echo "no $LAB/requirements.txt: run pins.sh first" >&2
		exit 1
	}
	[[ -f "$LAB/hosts.tsv" ]] || {
		lab_log "venv: no hosts.tsv"
		echo "no $LAB/hosts.tsv: run scan.sh first" >&2
		exit 1
	}
	local count
	count="$(lab_available_count)"
	[[ "$count" -gt 0 ]] || {
		echo "no PC rows in $LAB/hosts.tsv" >&2
		exit 1
	}

	local hostfile
	hostfile="$(lab_hosts "$count")"
	local -a ids=()
	mapfile -t ids <"$hostfile"

	lab_fanout venv-dirs "$hostfile" 'mkdir -p ~/pipedpeer-lab/bin ~/pipedpeer-lab/py ~/pipedpeer-lab/work' ||
		lab_log "venv: mkdir failed on some PC(s); see logs/venv-dirs/FAILED"

	local pc
	for pc in "${ids[@]}"; do
		lab_rsync "$uv" "$pc:pipedpeer-lab/bin/uv"
		lab_rsync "$LAB/requirements.txt" "$pc:pipedpeer-lab/requirements.txt"
		lab_rsync "$REPO/docs/benchmarks/workloads/versions.py" "$pc:pipedpeer-lab/work/versions.py"
	done

	local venvcmd
	venvcmd="$(venv_command "$pyver")"
	lab_log "venv: building $pyver venvs on ${#ids[@]} PCs"
	if ! lab_fanout venv "$hostfile" "$venvcmd"; then
		lab_log "venv: the venv build step failed on every PC"
	fi

	local mism rc_check
	mism="$(_venv_check "$LAB/logs/venv")" || rc_check=$?
	rc_check="${rc_check:-0}"
	if ((rc_check == 2)); then
		lab_log "venv: pc00's venv python produced no VERSIONS line"
		exit 1
	fi
	if ((rc_check != 0)); then
		lab_log "venv: version lines differ from pc00's: ${mism#FAILED }"
	fi

	# versions.csv: the venv row from pc00's line (own row replaced on a
	# re-run), the pipedpeer version from pc00's status
	local line ppver tmp="$LAB/versions.csv.tmp"
	line="$(grep -h '^VERSIONS ' "$LAB/logs/venv/pc00" | head -1)"
	ppver="$(_pp_status_version pc00 || true)"
	{
		[[ -f "$LAB/versions.csv" ]] || printf 'system,python,numpy,pandas,sklearn,joblib,torch,ray,dask,pipedpeer\n'
		if [[ -f "$LAB/versions.csv" ]]; then
			awk -F, 'NR == 1 || $1 != "venv"' "$LAB/versions.csv"
		fi
	} >"$tmp"
	printf 'venv,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
		"$(_pin_of "$line" python)" "$(_pin_of "$line" numpy)" "$(_pin_of "$line" pandas)" \
		"$(_pin_of "$line" sklearn)" "$(_pin_of "$line" joblib)" "$(_pin_of "$line" torch)" \
		"$(_pin_of "$line" ray)" "$(_pin_of "$line" dask)" "${ppver:-none}" >>"$tmp"
	mv "$tmp" "$LAB/versions.csv"
	lab_log "venv: venv row written from pc00's $(_pin_of "$line" python)"
	((rc_check == 0)) || exit 1
	printf 'VENV %s on %d PCs\n' "$pyver" "${#ids[@]}"
}

# _pp_status_version <pc>: pipedpeer's version from that PC's status line.
_pp_status_version() {
	local pc="$1" out
	out="$(lab_ssh "$pc" 'pipedpeer status')" || return 1
	printf '%s\n' "$out" | head -1 | sed -n 's/.*version=\([^ )]*\).*/\1/p'
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	main "$@"
fi
