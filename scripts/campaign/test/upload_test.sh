#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2015,SC2317  # prose rows; cleanup runs from a trap
# The S10 case table for upload.sh (brief S10): upload lands on an rclone
# serve s3 server on loopback (bucket root = a scratch dir, never the real
# R2), exceeds/nothing with the excludes, and a missing r2.env fails with a
# message. One ok/FAIL line per row; ends with "PASS <k>/<k>".
#
# The serve s3 server here is always 127.0.0.1 and always short-lived: it is
# started by this test, with key test:test, and killed on exit.

set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
UPLOAD="$REPO/scripts/campaign/upload.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"

pass=0
failed=0
problems=()
dirs=()
server_pids=()

cleanup() {
	local d p
	for p in "${server_pids[@]}"; do
		kill "$p" 2>/dev/null || true
	done
	if [[ "${LAB_TEST_KEEP:-0}" == 1 ]]; then
		printf 'kept: %s\n' "${dirs[*]}" >&2
		return
	fi
	for d in "${dirs[@]}"; do
		rm -rf "$d"
	done
}
trap cleanup EXIT

ok() { pass=$((pass + 1)); printf 'ok   %s\n' "$1"; }
bad() { failed=$((failed + 1)); printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }
verdict() {
	if ((${#problems[@]} == 0)); then
		ok "$1"
	else
		bad "$1" "${problems[*]}"
	fi
	problems=()
}

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# free_port: 8333 when free, else a high port nobody is listening on.
free_port() {
	local p
	for p in 8333 $((20000 + RANDOM % 20000)) $((40000 + RANDOM % 20000)); do
		if ! port_open "$p"; then
			printf '%s\n' "$p"
			return 0
		fi
	done
	return 1
}

# lab_tree <dir>: a small $LAB with nested dirs, serve/ and known_hosts.
lab_tree() {
	local d="$1" cid
	cid="lab-$(date +%F)"
	mkdir -p "$d/lab/hosts/pc00/metrics" "$d/lab/serve"
	printf '%s\n' "$cid" >"$d/lab/campaign_id"
	printf 'campaign log line\n' >"$d/lab/campaign.log"
	printf '{"run_id":"x"}\n' >"$d/lab/runs.jsonl"
	printf 'vmstat data\n' >"$d/lab/hosts/pc00/metrics/E1-n8.vmstat"
	printf 'setup line\n' >"$d/lab/serve/SETUP-LINE.txt"
	printf 'hosts\n' >"$d/lab/known_hosts"
}

# r2env <file> <port>: the test r2.env (endpoint on loopback, never the real
# bucket; PROVIDER=Other per the brief's Decisions).
r2env() {
	local f="$1" port="$2"
	{
		printf 'RCLONE_CONFIG_R2_TYPE=s3\n'
		printf 'RCLONE_CONFIG_R2_PROVIDER=Other\n'
		printf 'RCLONE_CONFIG_R2_ACCESS_KEY_ID=test\n'
		printf 'RCLONE_CONFIG_R2_SECRET_ACCESS_KEY=test\n'
		printf 'RCLONE_CONFIG_R2_ENDPOINT=http://127.0.0.1:%s\n' "$port"
	} >"$f"
}

# serve_s3 <dir> <port>: start the test S3 server (bucket root = <dir>).
serve_s3() {
	local dir="$1" port="$2" log="$3"
	rclone serve s3 --auth-key test,test "$dir" --addr "127.0.0.1:$port" \
		>"$log" 2>&1 &
	server_pids+=("$!")
}

# --- the table -------------------------------------------------------------

some_files() { # some_files <lab dir>: what upload.sh may copy
	find "$1" -type f ! -path "$1/serve/*" ! -name known_hosts | wc -l
}

# upload against rclone serve s3 -> files appear, excludes stay behind
t1() {
	local d rc=0 port kwant cid
	d="$(mktemp -d "$STATE/campaign-upload-test.XXXXXX")"
	dirs+=("$d")
	port="$(free_port)" || {
		pnote "no free lo port found"
		verdict "upload against serve s3 -> files appear"
		return
	}
	lab_tree "$d"
	cid="$(cat "$d/lab/campaign_id")"
	kwant="$(some_files "$d/lab")"
	r2env "$d/r2.env" "$port"
	serve_s3 "$d/srv" "$port" "$d/s3.log"
	until port_open "$port"; do
		kill -0 "${server_pids[-1]}" 2>/dev/null || {
			pnote "serve s3 exited: $(tail -1 "$d/s3.log" 2>/dev/null)"
			verdict "upload against serve s3 -> files appear"
			return
		}
		sleep 0.3
	done
	LAB_RESULTS="$d/lab" R2_ENV="$d/r2.env" bash "$UPLOAD" \
		>"$d/up.log" 2>&1 || rc=$?
	((rc == 0)) || pnote "upload exited $rc (log: $(tail -2 "$d/up.log" 2>/dev/null))"
	grep -q "UPLOAD-OK files=$kwant" "$d/up.log" 2>/dev/null ||
		pnote "no UPLOAD-OK files=$kwant in: $(head -1 "$d/up.log" 2>/dev/null)"
	[[ -f "$d/srv/pipedpeer-lab/raw/$cid/campaign.log" ]] ||
		pnote "campaign.log not on the server"
	[[ -f "$d/srv/pipedpeer-lab/raw/$cid/hosts/pc00/metrics/E1-n8.vmstat" ]] ||
		pnote "nested metrics file not on the server"
	[[ ! -d "$d/srv/pipedpeer-lab/raw/$cid/serve" ]] || pnote "serve/ was uploaded"
	[[ ! -e "$d/srv/pipedpeer-lab/raw/$cid/known_hosts" ]] || pnote "known_hosts was uploaded"
	grep -q 'SECRET_ACCESS_KEY' "$d/up.log" 2>/dev/null &&
		pnote "uploaded printed credentials" || true
	verdict "upload against serve s3 -> files appear, excludes kept"
}

# R2_ENV missing -> UPLOAD-FAILED no r2.env, exit 1
t2() {
	local d rc=0
	d="$(mktemp -d "$STATE/campaign-upload-test.XXXXXX")"
	dirs+=("$d")
	lab_tree "$d"
	LAB_RESULTS="$d/lab" R2_ENV="$d/nope.env" bash "$UPLOAD" \
		>"$d/up.log" 2>&1 || rc=$?
	((rc == 1)) || pnote "missing r2.env exited $rc, want 1"
	grep -q 'UPLOAD-FAILED no r2.env' "$d/up.log" 2>/dev/null ||
		pnote "no UPLOAD-FAILED no r2.env line"
	verdict "R2_ENV missing -> UPLOAD-FAILED no r2.env, exit 1"
}

printf 'scripts/campaign/upload.sh case table\n\n'
t1
t2

total=$((pass + failed))
if ((failed == 0)); then
	printf 'PASS %d/%d\n' "$pass" "$total"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$failed" "$total"
exit 1
