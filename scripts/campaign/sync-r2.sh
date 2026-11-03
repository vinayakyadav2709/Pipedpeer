#!/usr/bin/env bash
# sync-r2.sh [sync|final|count]: copy results/lab to the pipedpeer-lab R2
# bucket after every experiment.
#
# Two ways in, whichever is configured:
#   ingest (default): ~/.config/pipedpeer-lab/ingest.env, through the Worker in
#     ingest/, which enforces the free tier. The driver collector then only
#     writes files, so results/lab/otel/ is uploaded from here too, under otel/.
#   S3 keys: ~/.config/pipedpeer-lab/r2.env, rclone to r2:pipedpeer-lab/raw
#     (the collector uploads otel/ itself in that mode). No cap is enforced.
# Without either it warns and exits 0, so the runner carries on.
#
# sync: files changed in the last 2 min are left for the next sync, so files
#   still being written are not stored twice. final: sends everything.
# count: local vs uploaded (they match after `final`).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
results="$repo/results/lab"
ingest="$here/ingest/ingest.sh"
ingest_env="${PIPEDPEER_LAB_INGEST_ENV:-$HOME/.config/pipedpeer-lab/ingest.env}"
# On pc00 the decrypted bundle's r2.env (PIPEDPEER_LAB_R2_ENV) carries the
# ingest credentials when publish.sh was given ingest.env.
if [[ ! -r "$ingest_env" && -n "${PIPEDPEER_LAB_R2_ENV:-}" ]] && grep -q '^INGEST_TOKEN=.' "$PIPEDPEER_LAB_R2_ENV" 2>/dev/null; then
	ingest_env="$PIPEDPEER_LAB_R2_ENV"
fi
export PIPEDPEER_LAB_INGEST_ENV="$ingest_env"
mode="${1:-sync}"

if [[ -r "$ingest_env" || -n "${INGEST_TOKEN:-}" ]]; then
	[[ -d "$results" ]] || { echo "sync-r2: no $results; nothing to sync" >&2; exit 0; }
	case "$mode" in
	sync) exec "$ingest" sync "$results" raw ;;
	final) exec "$ingest" sync --final "$results" raw ;;
	count) exec "$ingest" count "$results" ;;
	*) echo "usage: $0 [sync|final|count]" >&2; exit 2 ;;
	esac
fi

source "$here/otel/lib.sh"
remote="r2:$R2_BUCKET/raw"
filters=(--exclude "/otel/**" --exclude "/serve/**")

if ! lab_r2_load; then
	echo "WARNING: sync-r2: skipped, neither ingest.env nor r2.env; results stay local only" >&2
	exit 0
fi
lab_install_rclone
lab_rclone_env
rclone=("$LAB_BIN/rclone")

case "$mode" in
sync | final)
	[[ -d "$results" ]] || { echo "sync-r2: no $results; nothing to sync" >&2; exit 0; }
	"${rclone[@]}" sync "$results" "$remote" "${filters[@]}" --stats-one-line --stats 0 -v 2>&1 | tail -3
	;;
count)
	echo "local:  $("${rclone[@]}" lsf -R --files-only "${filters[@]}" "$results" | wc -l) objects in $results (otel/, serve/ excluded)"
	echo "remote: $("${rclone[@]}" lsf -R --files-only "$remote" | wc -l) objects in $remote"
	;;
*)
	echo "usage: $0 [sync|final|count]" >&2
	exit 2
	;;
esac
