#!/usr/bin/env bash
# Somebody else's script: check it, then run it (any machine).
#
#   ./demo/11-teacher.sh <script.py> [data files the script opens ...]
#
# The script (and any files named after it) is copied into a fresh folder, so
# the upload is exactly those files and not whatever else sits next to them on
# a USB stick or in Downloads. Results land in that folder; its path is printed.
set -uo pipefail
script="${1:-}"
if [[ -z "$script" || ! -f "$script" ]]; then
	echo "usage: ./demo/11-teacher.sh <script.py> [data files ...]" >&2
	exit 2
fi
work="$(mktemp -d "${TMPDIR:-/tmp}/pipedpeer-teacher.XXXXXX")"
for f in "$@"; do
	cp -r -- "$f" "$work/" || { echo "could not copy $f into $work" >&2; exit 1; }
done
: >"$work/.pipedpeerignore"   # marks the folder as the job's whole workspace
name="$(basename "$script")"

source "$(dirname "$0")/_lib.sh"
cd "$work"

banner "Your script: $name"
note "Nothing edited. First a dry run: what it imports, what it would spread, where it would go."
failed=0
show "$PIPE" run "$name" --check-only
rc=$?
echo
if [[ $rc -ne 0 ]]; then
	echo "${R}  the dry run failed (exit $rc): read the Error line above; running it anyway${N}" >&2
	failed=1
fi
note "'not packaged: X' -> rerun with --pkg <nixpkgs name>, or pick a script from students/."
note "No 'spreads:' line -> nothing parallel in it; it runs on one machine. Say so."
pause
show "$PIPE" run "$name"
rc=$?
echo
if [[ $rc -ne 0 ]]; then
	echo "${R}  the run failed (exit $rc): read the Error line above${N}" >&2
	failed=1
fi
note "Files it wrote are in: $work"
exit "$failed"
