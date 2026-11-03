# shellcheck shell=bash disable=SC2034
# Shared by the campaign telemetry scripts (install-otelcol.sh, node.sh,
# telemetry.sh, sync-r2.sh). Source it; do not execute it.
#
# Pinned releases. To move a pin, change the version and both checksums
# together; the checksums are the release's own published sha256 values.
OTELCOL_VERSION=0.164.0
OTELCOL_SHA256_amd64=f0c1ce9f661399beb4a58b711924716e08ea70c3802db0715fcfde3cbbada2d4
OTELCOL_SHA256_arm64=8bd4a7f443405522a2175cc1a8675f9e99cc047c7181b73461efd08efbba38d3
RCLONE_VERSION=1.75.2
RCLONE_SHA256_amd64=349ac8fba6ff65d6247043f1750cdcb518ec5d500ef91463a10d37c0ccdf3702
RCLONE_SHA256_arm64=7e1e8d69654941b7b7df84ee74c5f7fb09cce7d5496947fb7bf16b55ff427d10

# Everything the campaign installs on a machine lives under LAB_HOME, so undo
# is one rm -rf. Nothing here needs root.
LAB_HOME="${PIPEDPEER_LAB_HOME:-$HOME/.local/share/pipedpeer-lab}"
LAB_BIN="$LAB_HOME/bin"
LAB_OTEL="$LAB_HOME/otel"
# Logs the node collector reads (README, Runner integration): ray/, dask/,
# pipedpeer-run/<run_id>.log, and pp-poll.py's JSON lines.
LAB_LOGS="$LAB_HOME/logs"
R2_ENV="${PIPEDPEER_LAB_R2_ENV:-$HOME/.config/pipedpeer-lab/r2.env}"
R2_BUCKET=pipedpeer-lab

lab_arch() {
	case "$(uname -m)" in
	x86_64 | amd64) echo amd64 ;;
	aarch64 | arm64) echo arm64 ;;
	*)
		echo "unsupported architecture $(uname -m); pins exist for amd64 and arm64" >&2
		return 1
		;;
	esac
}

# lab_fetch <url> <sha256> <dest>: download (or take $4, a local copy) into
# dest and refuse it unless the checksum matches.
lab_fetch() {
	local url="$1" want="$2" dest="$3" local_copy="${4:-}" got
	mkdir -p "$(dirname "$dest")"
	if [[ -n "$local_copy" ]]; then
		cp "$local_copy" "$dest.part"
	else
		curl -fsSL --retry 3 -o "$dest.part" "$url"
	fi
	got="$(sha256sum "$dest.part" | cut -d' ' -f1)"
	if [[ "$got" != "$want" ]]; then
		rm -f "$dest.part"
		echo "checksum mismatch for $url: got $got, want $want" >&2
		return 1
	fi
	mv -f "$dest.part" "$dest"
}

# lab_install_otelcol: the pinned otelcol-contrib into LAB_BIN, idempotent.
# OTELCOL_TARBALL points at an already downloaded release tarball (it is
# still checksum-verified).
lab_install_otelcol() {
	local arch bin="$LAB_BIN/otelcol-contrib" sum tarball
	if [[ -x "$bin" ]] && "$bin" --version 2>/dev/null | grep -q "version $OTELCOL_VERSION\b"; then
		return 0
	fi
	arch="$(lab_arch)" || return 1
	sum="OTELCOL_SHA256_$arch"
	tarball="$LAB_HOME/dl/otelcol-contrib_${OTELCOL_VERSION}_linux_$arch.tar.gz"
	lab_fetch "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v$OTELCOL_VERSION/otelcol-contrib_${OTELCOL_VERSION}_linux_$arch.tar.gz" \
		"${!sum}" "$tarball" "${OTELCOL_TARBALL:-}" || return 1
	mkdir -p "$LAB_BIN"
	tar -xzf "$tarball" -C "$LAB_BIN" otelcol-contrib
	chmod 0755 "$bin"
	rm -rf "$LAB_HOME/dl"
	echo "installed otelcol-contrib $OTELCOL_VERSION at $bin"
}

# lab_install_rclone: the pinned rclone into LAB_BIN, idempotent.
lab_install_rclone() {
	local arch bin="$LAB_BIN/rclone" sum zip dir
	if [[ -x "$bin" ]] && "$bin" version 2>/dev/null | head -1 | grep -q "v$RCLONE_VERSION\$"; then
		return 0
	fi
	arch="$(lab_arch)" || return 1
	sum="RCLONE_SHA256_$arch"
	dir="rclone-v$RCLONE_VERSION-linux-$arch"
	zip="$LAB_HOME/dl/$dir.zip"
	lab_fetch "https://github.com/rclone/rclone/releases/download/v$RCLONE_VERSION/$dir.zip" "${!sum}" "$zip" "${RCLONE_ZIP:-}" || return 1
	mkdir -p "$LAB_BIN"
	# python3's zipfile when unzip is missing: lab PCs are not guaranteed it.
	if command -v unzip >/dev/null; then
		unzip -p "$zip" "$dir/rclone" >"$bin.part"
	else
		python3 -c 'import sys, zipfile; sys.stdout.buffer.write(zipfile.ZipFile(sys.argv[1]).read(sys.argv[2]))' "$zip" "$dir/rclone" >"$bin.part"
	fi
	chmod 0755 "$bin.part"
	mv -f "$bin.part" "$bin"
	rm -rf "$LAB_HOME/dl"
	echo "installed rclone $RCLONE_VERSION at $bin" >&2
}

# lab_r2_load: read R2_ACCOUNT_ID, R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY
# from r2.env without sourcing it (only those three keys, nothing executed).
# Returns 1, printing why, when the file is missing or incomplete; a file
# others can read is tightened to 600 first.
lab_r2_load() {
	local line key val
	R2_ACCOUNT_ID="" R2_ACCESS_KEY_ID="" R2_SECRET_ACCESS_KEY=""
	if [[ ! -f "$R2_ENV" ]]; then
		echo "warning: $R2_ENV not found; R2 is off (put R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY there, mode 600)" >&2
		return 1
	fi
	if [[ "$(stat -c %a "$R2_ENV")" != 600 ]]; then
		echo "warning: $R2_ENV was mode $(stat -c %a "$R2_ENV"); setting 600" >&2
		chmod 600 "$R2_ENV"
	fi
	while IFS= read -r line || [[ -n "$line" ]]; do
		line="${line#export }"
		[[ "$line" =~ ^[[:space:]]*(R2_ACCOUNT_ID|R2_ACCESS_KEY_ID|R2_SECRET_ACCESS_KEY)=(.*)$ ]] || continue
		key="${BASH_REMATCH[1]}" val="${BASH_REMATCH[2]}"
		val="${val%\"}" val="${val#\"}" val="${val%\'}" val="${val#\'}"
		printf -v "$key" '%s' "$val"
	done <"$R2_ENV"
	if [[ -z "$R2_ACCOUNT_ID" || -z "$R2_ACCESS_KEY_ID" || -z "$R2_SECRET_ACCESS_KEY" ]]; then
		echo "warning: $R2_ENV lacks one of R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY; R2 is off" >&2
		return 1
	fi
}

# lab_rclone_env: rclone's remote "r2" from environment variables only, so no
# config file ever holds the keys. Call after lab_r2_load.
lab_rclone_env() {
	export RCLONE_CONFIG_R2_TYPE=s3
	export RCLONE_CONFIG_R2_PROVIDER=Cloudflare
	export RCLONE_CONFIG_R2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
	export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
	export RCLONE_CONFIG_R2_ENDPOINT="https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com"
	export RCLONE_CONFIG_R2_REGION=auto
	# A bucket-scoped R2 token cannot create or list buckets.
	export RCLONE_CONFIG_R2_NO_CHECK_BUCKET=true
	# An empty config path: rclone neither reads nor writes rclone.conf.
	export RCLONE_CONFIG=/dev/null
	if [[ -n "${PIPEDPEER_LAB_S3_ENDPOINT:-}" ]]; then
		# Tests only: an S3 stand-in (rclone serve s3) instead of R2.
		export RCLONE_CONFIG_R2_PROVIDER=Other RCLONE_CONFIG_R2_ENDPOINT="$PIPEDPEER_LAB_S3_ENDPOINT"
	fi
}

# lab_stop_pid <pidfile>: stop the process (TERM, then KILL after 10 s) and
# remove the pid file. Succeeds when nothing was running.
lab_stop_pid() {
	local pidfile="$1" pid i
	[[ -f "$pidfile" ]] || return 0
	pid="$(cat "$pidfile")"
	if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
		for i in $(seq 1 100); do
			kill -0 "$pid" 2>/dev/null || break
			sleep 0.1
		done
		kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
	fi
	rm -f "$pidfile"
}

# lab_running <pidfile>: is the recorded process alive?
lab_running() {
	[[ -f "$1" ]] && kill -0 "$(cat "$1")" 2>/dev/null
}
