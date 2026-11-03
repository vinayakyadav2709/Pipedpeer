#!/usr/bin/env bash
# sync-r2.sh [count]: mirror results/lab to r2:pipedpeer-lab/raw with the
# pinned rclone (otel/lib.sh), after every experiment. results/lab/otel/ (the
# collector uploads that itself) and results/lab/serve/ are left out.
# rclone is configured from ~/.config/pipedpeer-lab/r2.env through
# RCLONE_CONFIG_R2_* variables only; no rclone.conf holds the keys.
# Without r2.env it warns and exits 0, so the runner carries on.
#
# count: local and remote object counts (they match after a sync).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
source "$here/otel/lib.sh"

results="$repo/results/lab"
remote="r2:$R2_BUCKET/raw"
filters=(--exclude "/otel/**" --exclude "/serve/**")

if ! lab_r2_load; then
	echo "WARNING: sync-r2: skipped, R2 not configured; results stay local only" >&2
	exit 0
fi
lab_install_rclone
lab_rclone_env
rclone=("$LAB_BIN/rclone")

case "${1:-sync}" in
sync)
	[[ -d "$results" ]] || { echo "sync-r2: no $results; nothing to sync" >&2; exit 0; }
	"${rclone[@]}" sync "$results" "$remote" "${filters[@]}" --stats-one-line --stats 0 -v 2>&1 | tail -3
	;;
count)
	echo "local:  $("${rclone[@]}" lsf -R --files-only "${filters[@]}" "$results" | wc -l) objects in $results (otel/, serve/ excluded)"
	echo "remote: $("${rclone[@]}" lsf -R --files-only "$remote" | wc -l) objects in $remote"
	;;
*)
	echo "usage: $0 [sync|count]" >&2
	exit 2
	;;
esac
