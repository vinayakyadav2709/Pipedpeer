#!/usr/bin/env bash
# Upload the results dir to Cloudflare R2 (brief S10): rclone copy (never
# sync — nothing on the remote is ever deleted), then rclone check --one-way
# to confirm every file landed. Credentials come from $R2_ENV as env-var
# config (RCLONE_CONFIG_R2_*); the file's values are never printed.
# A failed upload prints UPLOAD-FAILED and exits 1; run.sh logs it and the
# campaign continues.
set -euo pipefail

_D="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
. "$_D/lib.sh"

upload_usage() {
	cat <<'EOF'
usage: upload.sh

Uploads $LAB (default <repo>/results/lab, override with LAB_RESULTS) to
  r2:pipedpeer-lab/raw/$(cat $LAB/campaign_id)/
with rclone copy, then verifies with rclone check --one-way. Excluded:
serve/** (the setup files each PC downloads) and known_hosts.

Credentials: ${R2_ENV:-~/.config/pipedpeer-lab/r2.env}, sourced with
set -a so rclone reads the remote from RCLONE_CONFIG_R2_* env vars.

Prints UPLOAD-OK files=<k> or UPLOAD-FAILED <why> and exits 1.
EOF
}

upload_main() {
	local r2_env="${R2_ENV:-$HOME/.config/pipedpeer-lab/r2.env}"
	local cid remote files msg out
	if ! command -v rclone >/dev/null 2>&1; then
		echo "UPLOAD-FAILED no rclone on PATH" >&2
		lab_log "upload: no rclone on PATH"
		return 1
	fi
	if [[ ! -f "$r2_env" ]]; then
		echo "UPLOAD-FAILED no r2.env (looked at $r2_env)" >&2
		lab_log "upload: no r2.env at $r2_env"
		return 1
	fi
	if [[ ! -f "$LAB/campaign_id" ]]; then
		echo "UPLOAD-FAILED no campaign_id in $LAB (run serve.sh first)" >&2
		lab_log "upload: no campaign_id in $LAB"
		return 1
	fi
	cid="$(cat "$LAB/campaign_id")"
	remote="r2:pipedpeer-lab/raw/$cid/"
	files="$(find "$LAB" -type f ! -path "$LAB/serve/*" ! -name known_hosts | wc -l)"
	set -a
	# shellcheck source=/dev/null
	. "$r2_env"
	set +a
	if out="$(rclone copy "$LAB" "$remote" --exclude 'serve/**' --exclude 'known_hosts' 2>&1)"; then
		:
	else
		msg="$(printf '%s\n' "$out" | sed '/^[[:space:]]*$/d' | tail -1)"
		echo "UPLOAD-FAILED ${msg:-rclone copy failed}" >&2
		lab_log "upload: rclone copy failed: ${msg:-<no message>}"
		return 1
	fi
	if out="$(rclone check "$LAB" "$remote" --one-way --exclude 'serve/**' --exclude 'known_hosts' 2>&1)"; then
		echo "UPLOAD-OK files=$files"
		lab_log "upload: ok files=$files remote=$remote"
	else
		msg="$(printf '%s\n' "$out" | sed '/^[[:space:]]*$/d' | tail -1)"
		echo "UPLOAD-FAILED ${msg:-rclone check failed}" >&2
		lab_log "upload: rclone check failed: ${msg:-<no message>}"
		return 1
	fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	case "${1:-}" in
		-h | --help)
			upload_usage
			exit 0
			;;
		"") upload_main ;;
		*)
			upload_usage >&2
			exit 2
			;;
	esac
fi
