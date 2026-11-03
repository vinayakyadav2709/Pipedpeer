#!/usr/bin/env bash
# Client for the lab campaign's ingest Worker (worker.js beside this file).
#
#   ingest.sh put <file> <key>        upload one file (split above 90 MB)
#   ingest.sh sync [--final] <dir> <prefix>
#                                     upload every new or changed file under dir
#   ingest.sh usage                   bytes, uploads and caps, as the Worker counts them
#   ingest.sh count <dir>             local files synced vs objects the Worker holds
#   ingest.sh selftest                put, read back and compare one small object
#
# Credentials: INGEST_URL and INGEST_TOKEN from the environment or from
# ${PIPEDPEER_LAB_INGEST_ENV:-~/.config/pipedpeer-lab/ingest.env}. Without them
# every command warns and exits 0, so a run is never stopped by its upload.
#
# Text files (.jsonl .json .log .csv .txt .md .yaml) are gzipped on the way up
# and stored as <key>.gz; everything else goes as it is. The Worker refuses
# anything that would cross the free-tier caps (HTTP 507); this script checks
# the same caps first so it does not spend a request on a sure refusal.
set -euo pipefail

part_bytes=$((90 * 1000 * 1000))
env_file="${PIPEDPEER_LAB_INGEST_ENV:-$HOME/.config/pipedpeer-lab/ingest.env}"

warn() { echo "[ingest] $*" >&2; }

load_env() {
	if [[ -z "${INGEST_URL:-}" || -z "${INGEST_TOKEN:-}" ]] && [[ -r "$env_file" ]]; then
		local k v
		while IFS='=' read -r k v; do
			case "$k" in
			INGEST_URL) INGEST_URL="$v" ;;
			INGEST_TOKEN) INGEST_TOKEN="$v" ;;
			esac
		done <"$env_file"
	fi
	if [[ -z "${INGEST_URL:-}" || -z "${INGEST_TOKEN:-}" ]]; then
		warn "WARNING: no ingest credentials ($env_file); nothing uploaded"
		exit 0
	fi
}

# The token goes in a header file, never on a command line other users can read.
hdr_file=""
cleanup() { if [[ -n "$hdr_file" ]]; then rm -f "$hdr_file"; fi; }
trap cleanup EXIT
auth_header() {
	if [[ -z "$hdr_file" ]]; then
		hdr_file="$(mktemp)"
		chmod 600 "$hdr_file"
		printf 'Authorization: Bearer %s\n' "$INGEST_TOKEN" >"$hdr_file"
	fi
}

api() { # method path [curl args...]
	local m="$1" p="$2"
	shift 2
	auth_header
	curl -sS --retry 4 --retry-all-errors --retry-delay 3 --max-time 600 -X "$m" -H "@$hdr_file" "$@" "$INGEST_URL$p"
}

usage_json() { api GET /usage; }

# Refuse locally when the upload would cross a cap, so a sure refusal costs
# no request. The Worker checks again; this is only the cheap first look.
budget_ok() { # bytes
	usage_json | python3 -c '
import json, sys
d = json.load(sys.stdin); u, c = d["usage"], d["caps"]
n = int(sys.argv[1])
if u["bytes"] + n > c["bytes"]:
    sys.exit("storage cap: %d + %d > %d bytes" % (u["bytes"], n, c["bytes"]))
if u["puts"] + 1 > c["puts"]:
    sys.exit("upload cap reached (%d)" % c["puts"])
' "$1"
}

put_raw() { # file key
	local f="$1" key="$2" size code out
	size="$(stat -c %s "$f")"
	if ! budget_ok "$size"; then
		warn "REFUSED $key: free-tier cap; the file stays local"
		return 3
	fi
	out="$(mktemp)"
	code="$(api PUT "/o/$key" --data-binary "@$f" -H 'content-type: application/octet-stream' -o "$out" -w '%{http_code}')" || code=000
	if [[ "$code" != 200 ]]; then
		warn "FAILED $key: HTTP $code $(head -c 300 "$out")"
		rm -f "$out"
		[[ "$code" == 507 ]] && return 3
		return 1
	fi
	rm -f "$out"
}

is_text() {
	case "$1" in
	*.jsonl | *.json | *.log | *.csv | *.txt | *.md | *.yaml | *.yml | *.ini) return 0 ;;
	*) return 1 ;;
	esac
}

put_file() { # file key
	local f="$1" key="$2" src="$1" tmp="" rc=0
	if is_text "$f"; then
		tmp="$(mktemp)"
		gzip -c -6 "$f" >"$tmp"
		src="$tmp"
		key="$key.gz"
	fi
	local size
	size="$(stat -c %s "$src")"
	if ((size <= part_bytes)); then
		put_raw "$src" "$key" || rc=$?
	else
		local dir i=0 p
		dir="$(mktemp -d)"
		split -b "$part_bytes" -d -a 4 "$src" "$dir/part"
		for p in "$dir"/part*; do
			put_raw "$p" "$key.part$(printf %04d $i)" || { rc=$?; break; }
			i=$((i + 1))
		done
		rm -rf "$dir"
	fi
	[[ -n "$tmp" ]] && rm -f "$tmp"
	return $rc
}

# sync remembers what it sent (path, size, mtime) in a manifest beside the
# data, so a rerun after a crash sends only what changed.
#
# A file still being written (changed in the last SETTLE seconds, default 120)
# is left for a later sync: re-sending a growing file every minute would store
# it many times over and eat the storage cap. `sync --final` sends everything.
cmd_sync() {
	local settle="${INGEST_SETTLE:-120}"
	if [[ "${1:-}" == --final ]]; then
		settle=0
		shift
	fi
	local dir="${1%/}" prefix="${2%/}" manifest rel stamp now sent=0 skipped=0 waiting=0 refused=0 failed=0 rc
	now="$(date +%s)"
	[[ -d "$dir" ]] || { warn "no directory $dir"; return 0; }
	manifest="$dir/.ingest-manifest"
	touch "$manifest"
	while IFS= read -r -d '' f; do
		rel="${f#"$dir"/}"
		case "$rel" in
		.ingest-manifest | serve/* | */.ingest-manifest) continue ;;
		esac
		stamp="$rel	$(stat -c '%s %Y' "$f")"
		if ((now - $(stat -c %Y "$f") < settle)); then
			waiting=$((waiting + 1))
			continue
		fi
		if grep -qxF "$stamp" "$manifest"; then
			skipped=$((skipped + 1))
			continue
		fi
		rc=0
		put_file "$f" "$prefix/$rel" || rc=$?
		case $rc in
		0)
			awk -F'\t' -v r="$rel" '$1 != r' "$manifest" >"$manifest.new"
			echo "$stamp" >>"$manifest.new"
			mv "$manifest.new" "$manifest"
			sent=$((sent + 1))
			;;
		3) refused=$((refused + 1)) ;;
		*) failed=$((failed + 1)) ;;
		esac
	done < <(find "$dir" -type f -print0 | sort -z)
	echo "[ingest] sync $dir -> $prefix: $sent sent, $skipped unchanged, $waiting still being written, $refused refused (cap), $failed failed"
	((failed == 0))
}

# Same two lines as the rclone mode of sync-r2.sh ("local: N ..." and
# "remote: N ..."), which day/finish.sh parses. "remote" counts the files the
# Worker accepted (HTTP 200), from the manifest.
cmd_count() {
	local dir="$1" local_n synced=0
	local_n="$(find "$dir" -type f ! -name .ingest-manifest ! -path "$dir/serve/*" | wc -l)"
	[[ -f "$dir/.ingest-manifest" ]] && synced="$(wc -l <"$dir/.ingest-manifest")"
	echo "local:  $local_n objects in $dir (serve/ excluded)"
	echo "remote: $synced objects accepted by the ingest Worker"
	usage_json | python3 -c 'import json,sys; d=json.load(sys.stdin); u=d["usage"]; print("worker: %d objects, %.3f GB of %.1f GB cap, %d of %d uploads" % (u["objects"], u["bytes"]/1e9, d["caps"]["bytes"]/1e9, u["puts"], d["caps"]["puts"]))'
	[[ "$local_n" == "$synced" ]]
}

cmd_selftest() {
	local f back key
	f="$(mktemp)"
	back="$(mktemp)"
	key="selftest/$(hostname)-$(date +%s).bin"
	head -c 4096 /dev/urandom >"$f"
	put_raw "$f" "$key"
	api GET "/o/$key" -o "$back"
	if cmp -s "$f" "$back"; then
		echo "[ingest] selftest ok: $key round-tripped"
	else
		echo "[ingest] selftest FAILED: $key came back different" >&2
		rm -f "$f" "$back"
		return 1
	fi
	rm -f "$f" "$back"
}

main() {
	local cmd="${1:-}"
	shift || true
	load_env
	case "$cmd" in
	put) put_file "$1" "$2" ;;
	sync) cmd_sync "$@" ;;
	usage) usage_json ;;
	count) cmd_count "${1:-results/lab}" ;;
	selftest) cmd_selftest ;;
	*)
		sed -n '2,9p' "$0" >&2
		exit 2
		;;
	esac
}

main "$@"
