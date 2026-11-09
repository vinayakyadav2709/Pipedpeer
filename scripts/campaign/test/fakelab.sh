# shellcheck shell=bash
# The fake lab: a handful of "PCs" that are directories, and stubs for the
# five system tools the campaign drives them through (brief S1, tests only).
#
# Why the stubs exist: the campaign's helpers are thin wrappers around ssh,
# rsync, pssh and systemd-run, and every bug they ever grow is about retries,
# exit codes and output files. A fake lab exercises exactly that on one
# laptop, with no cluster and no waiting for a network to agree.
#
# Shape: `fakelab_init <dir> <n>` makes `<dir>/pc00` .. `<dir>/pcNN` as the
# PCs' HOMEs, writes `<dir>/hosts.tsv` in the campaign's format, and puts a
# stub bin dir first on PATH.
#
#   ssh -F x pcNN -- cmd   -> HOME=<dir>/pcNN bash -c "$cmd"
#   rsync ... pcNN:path    -> the same path under <dir>/pcNN (real rsync runs)
#   pssh -h file -o/-e ... -> loops the ssh stub, one output file per host,
#                             printing pssh 2.3.1's report lines,
#                             "[k] HH:MM:SS [SUCCESS] <id>" / "[k] ... [FAILURE] <id> <error>"
#   systemd-run --unit=U   -> setsid bash -c "$cmd" &
#   sudo ...               -> appends to <dir>/sudo.log, runs the command
#   nmap ...               -> runs $SCAN_NMAP_CMD, which a test supplies
#
# FAKELAB_FAIL=pc01[,pc02] makes ssh to those hosts exit 255 (a connection
# failure, the only kind the helpers retry). The file `<dir>/fail` holds more
# ids, read at each call, so a test can drop a connection in the middle of a
# wait and restore it later without touching a running process's environment.
#
# Every stub refuses to run unless FAKELAB_DIR is set: a stub that has been
# copied or reached outside the lab would otherwise stand in for a real
# system binary, and a test would pass for the wrong reason.
#
# Source this file, then call fakelab_init in the shell (or subshell) that
# uses the lab; it exports FAKELAB_DIR, FAKELAB_RSYNC and the new PATH.

FAKELAB_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# fakelab_init <dir> <n>: build n PCs (pc00..) and the stubs in <dir>.
fakelab_init() {
	local dir="${1:?fakelab_init: dir}" n="${2:?fakelab_init: number of PCs}"
	local i pc real_rsync stamp

	mkdir -p "$dir" "$dir/bin" "$dir/home"
	real_rsync="$(command -v rsync || true)"
	if [[ -z "$real_rsync" ]]; then
		echo "fakelab: rsync is not on PATH; the stub needs the real one for local copies" >&2
		return 1
	fi
	stamp="$FAKELAB_LIB_DIR/../stamp.py"
	if [[ ! -f "$stamp" ]]; then
		echo "fakelab: no stamp.py next to the lib at $stamp" >&2
		return 1
	fi

	export FAKELAB_DIR="$dir"
	export FAKELAB_RSYNC="$real_rsync"

	{
		printf '#id\tip\tuser\thostname\tnproc\tram_gib\n'
		for ((i = 0; i < n; i++)); do
			pc="$(printf 'pc%02d' "$i")"
			printf '%s\t10.99.0.%d\tlab\t%s.lab\t8\t31\n' "$pc" "$((10 + i))" "$pc"
		done
	} >"$dir/hosts.tsv"

	for ((i = 0; i < n; i++)); do
		pc="$(printf 'pc%02d' "$i")"
		mkdir -p "$dir/$pc/pipedpeer-lab/work" "$dir/$pc/pipedpeer-lab/runs"
		# Tests that exercise staging (stage_test.sh) set FAKELAB_NO_STAMP=1 and
		# start from an empty work dir.
		if [[ "${FAKELAB_NO_STAMP:-0}" != 1 ]]; then
			cp "$stamp" "$dir/$pc/pipedpeer-lab/work/stamp.py"
		fi
	done

	_fakelab_write_stubs "$dir"
	export PATH="$dir/bin:$PATH"

	if [[ "$(command -v ssh || true)" != "$dir/bin/ssh" ]]; then
		echo "fakelab: the stub ssh is not first on PATH ($(command -v ssh || true))" >&2
		return 1
	fi
	return 0
}

_fakelab_write_stubs() {
	local dir="$1"

	cat >"$dir/bin/ssh" <<'FAKELAB-SSH'
#!/usr/bin/env bash
# Fake-lab ssh: HOME=<FAKELAB_DIR>/<host> bash -c "<command>".
set -uo pipefail
if [[ -z "${FAKELAB_DIR:-}" || ! -d "${FAKELAB_DIR:-}" ]]; then
	echo "fakelab ssh: FAKELAB_DIR is unset; refusing to stand in for a real ssh" >&2
	exit 97
fi
while (( $# )); do
	case "$1" in
		-F | -o | -p | -l | -i | -e | -c | -b | -D | -J | -L | -R | -W) shift 2 ;;
		--) shift && break ;;
		-*) shift ;;
		*) break ;;
	esac
done
host="${1:-}"
if [[ -z "$host" ]]; then
	echo "fakelab ssh: no host given" >&2
	exit 2
fi
shift || true
if [[ "${1:-}" == "--" ]]; then shift; fi
cmd="$*"
printf 'ssh %s\n' "$host" >>"$FAKELAB_DIR/calls.log"
home="$FAKELAB_DIR/$host"
if [[ ! -d "$home" ]]; then
	echo "fakelab ssh: no such host $host (no $home)" >&2
	exit 255
fi
fail="${FAKELAB_FAIL:-}"
if [[ -f "$FAKELAB_DIR/fail" ]]; then
	fail="$(cat "$FAKELAB_DIR/fail")${fail:+,$fail}"
fi
if [[ -n "$fail" ]]; then
	IFS=',' read -r -a failing <<<"$fail"
	for p in "${failing[@]}"; do
		[[ "${p// /}" == "$host" ]] || continue
		echo "fakelab ssh: $host is down (FAKELAB_FAIL)" >&2
		exit 255
	done
fi
HOME="$home" bash -c "$cmd"
FAKELAB-SSH

	cat >"$dir/bin/rsync" <<'FAKELAB-RSYNC'
#!/usr/bin/env bash
# Fake-lab rsync: "<host>:<path>" becomes "<FAKELAB_DIR>/<host>/<path>",
# then the real rsync does the copy. Flags pass through untouched.
set -uo pipefail
if [[ -z "${FAKELAB_DIR:-}" || ! -d "${FAKELAB_DIR:-}" ]]; then
	echo "fakelab rsync: FAKELAB_DIR is unset; refusing to stand in for a real rsync" >&2
	exit 97
fi
if [[ -z "${FAKELAB_RSYNC:-}" || ! -x "${FAKELAB_RSYNC:-}" ]]; then
	echo "fakelab rsync: FAKELAB_RSYNC is not set to the real rsync" >&2
	exit 97
fi
args=()
for arg in "$@"; do
	if [[ "$arg" =~ ^(pc[0-9]+):(.*)$ ]]; then
		pc="${BASH_REMATCH[1]}"
		rest="${BASH_REMATCH[2]#\~/}"
		if [[ ! -d "$FAKELAB_DIR/$pc" ]]; then
			echo "fakelab rsync: no such host $pc" >&2
			exit 255
		fi
		arg="$FAKELAB_DIR/$pc/$rest"
	fi
	args+=("$arg")
done
exec "$FAKELAB_RSYNC" "${args[@]}"
FAKELAB-RSYNC

	cat >"$dir/bin/pssh" <<'FAKELAB-PSSH'
#!/usr/bin/env bash
# Fake-lab pssh: one ssh stub per host, -o/-e files per host, and the
# report lines the campaign's fan-out parses, in pssh 2.3.1's own format
# (psshlib/task.py): "[1] 12:34:56 [SUCCESS] host" and
# "[2] 12:34:56 [FAILURE] host Exited with error code 255".
set -uo pipefail
if [[ -z "${FAKELAB_DIR:-}" || ! -d "${FAKELAB_DIR:-}" ]]; then
	echo "fakelab pssh: FAKELAB_DIR is unset; refusing to stand in for a real pssh" >&2
	exit 97
fi
hosts=""
out=""
err=""
rest=()
while (( $# )); do
	case "$1" in
		-h) hosts="${2:-}"; shift 2 ;;
		-o) out="${2:-}"; shift 2 ;;
		-e) err="${2:-}"; shift 2 ;;
		-x | -t | -p | -l | -A | -O) shift 2 ;;
		-i | -v | -P | -q) shift ;;
		-*) shift ;;
		*) rest+=("$1"); shift ;;
	esac
done
if [[ -z "$hosts" || ${#rest[@]} -eq 0 ]]; then
	echo "fakelab pssh: need -h <file> and a command" >&2
	exit 2
fi
cmd="${rest[*]}"
[[ -n "$out" ]] || out=.
[[ -n "$err" ]] || err=.
mkdir -p "$out" "$err"
rc_all=0
k=0
while IFS= read -r host; do
	[[ -z "$host" || "$host" == \#* ]] && continue
	k=$((k + 1))
	rc=0
	"$FAKELAB_DIR/bin/ssh" "$host" -- "$cmd" >"$out/$host" 2>"$err/$host" || rc=$?
	if ((rc == 0)); then
		printf '[%d] %s [SUCCESS] %s\n' "$k" "$(date +%H:%M:%S)" "$host"
	else
		printf '[%d] %s [FAILURE] %s Exited with error code %d\n' "$k" "$(date +%H:%M:%S)" "$host" "$rc"
		rc_all=1
	fi
done <"$hosts"
exit "$rc_all"
FAKELAB-PSSH

	cat >"$dir/bin/systemd-run" <<'FAKELAB-SYSTEMD-RUN'
#!/usr/bin/env bash
# Fake-lab systemd-run: --user --unit=U --collect <cmd...> runs <cmd>
# detached, as setsid does, so it outlives the ssh that started it.
set -uo pipefail
if [[ -z "${FAKELAB_DIR:-}" || ! -d "${FAKELAB_DIR:-}" ]]; then
	echo "fakelab systemd-run: FAKELAB_DIR is unset" >&2
	exit 97
fi
unit=""
rest=()
while (( $# )); do
	# once the command started, every further word (flags too) is its argument
	if [[ ${#rest[@]} -gt 0 ]]; then rest+=("$1"); shift; continue; fi
	case "$1" in
		--unit=*) unit="${1#--unit=}"; shift ;;
		--unit) unit="${2:-}"; shift 2 ;;
		--user | --collect | --no-block | --quiet | -q) shift ;;
		-*) shift ;;
		*) rest+=("$1"); shift ;;
	esac
done
if [[ ${#rest[@]} -eq 0 ]]; then
	echo "fakelab systemd-run: no command" >&2
	exit 2
fi
printf 'systemd-run %s: %s\n' "${unit:-?}" "${rest[*]}" >>"$FAKELAB_DIR/units.log"
# Wrapper output is kept (units.out), not discarded, so a failure before
# cmd.sh's own redirections can be read.
setsid bash -c "${rest[*]}" >>"$FAKELAB_DIR/units.out" 2>&1 &
exit 0
FAKELAB-SYSTEMD-RUN

	cat >"$dir/bin/sudo" <<'FAKELAB-SUDO'
#!/usr/bin/env bash
# Fake-lab sudo: record the call, then run it. Nothing here is privileged.
set -uo pipefail
if [[ -z "${FAKELAB_DIR:-}" || ! -d "${FAKELAB_DIR:-}" ]]; then
	echo "fakelab sudo: FAKELAB_DIR is unset" >&2
	exit 97
fi
printf 'sudo %s\n' "$*" >>"$FAKELAB_DIR/sudo.log"
if [[ ${#} -eq 0 ]]; then
	echo "fakelab sudo: no command" >&2
	exit 2
fi
exec "$@"
FAKELAB-SUDO

	# Commands sent to a fake PC run here, so systemctl must not reach this
	# machine's service manager. Tests that assert on calls install their own.
	cat >"$dir/bin/systemctl" <<'FAKELAB-SYSTEMCTL'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKELAB_DIR/fakelab-systemctl.log"
FAKELAB-SYSTEMCTL
	chmod +x "$dir/bin/systemctl"

	cat >"$dir/bin/nmap" <<'FAKELAB-NMAP'
#!/usr/bin/env bash
# Fake-lab nmap: whatever $SCAN_NMAP_CMD prints, with nmap's own arguments
# passed through as "$@" so a test can decide by -p/-oG if it wants.
set -uo pipefail
if [[ -z "${FAKELAB_DIR:-}" || ! -d "${FAKELAB_DIR:-}" ]]; then
	echo "fakelab nmap: FAKELAB_DIR is unset" >&2
	exit 97
fi
if [[ -z "${SCAN_NMAP_CMD:-}" ]]; then
	echo "fakelab nmap: set SCAN_NMAP_CMD to stub the scan" >&2
	exit 1
fi
exec bash -c "$SCAN_NMAP_CMD" fakelab-nmap "$@"
FAKELAB-NMAP

	chmod +x "$dir/bin/ssh" "$dir/bin/rsync" "$dir/bin/pssh" \
		"$dir/bin/systemd-run" "$dir/bin/sudo" "$dir/bin/nmap"
}
