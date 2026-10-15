# Find a pipedpeer binary that matches the code being tested, and read its
# token without killing the caller.
#
# Two failures this exists to prevent, both seen for real:
#
# The harnesses treated "bin/pipedpeer exists" as "bin/pipedpeer is current".
# On the test machine that file was four days stale and predated the `auth`
# command entirely, so every harness ran a build of the code from before the
# thing it was meant to verify. A checkout is the authority on what the binary
# should be; only when there is no source does an existing binary win.
#
# And the old token probe, `tok=$(pipedpeer auth show | head -1)`, was a pipeline
# under `set -o pipefail`. When the binary does not know the `auth` command
# the pipeline fails, the assignment fails, `set -e` fires, and the script
# dies before its first echo - exit 1, zero bytes of output, on a run that
# takes twenty minutes to get to. The probe is best-effort by design: an
# absent token is a normal answer, not an error.
#
# Sets PP_CLI and PP_TOKEN, defines pp_curl. Source it; do not execute it.

# pp_resolve_cli <repo_root>
pp_resolve_cli() {
	local repo_root="$1"
	if [[ -n "${PIPEDPEER:-}" ]]; then
		# Explicitly pointed at a binary: that is the user's call, use it.
		PP_CLI="$PIPEDPEER"
	elif [[ -f "$repo_root/src/go.mod" ]]; then
		echo "building binary from $repo_root/src..." >&2
		(cd "$repo_root/src" && CGO_ENABLED=0 go build -o "$repo_root/bin/pipedpeer" .) >&2 || return 1
		PP_CLI="$repo_root/bin/pipedpeer"
	else
		PP_CLI="$repo_root/bin/pipedpeer"
	fi
	if [[ ! -x "$PP_CLI" ]]; then
		echo "no binary at $PP_CLI, and no checkout here to build one from." >&2
		echo "Set PIPEDPEER to a built binary." >&2
		return 1
	fi
	return 0
}

# pp_read_token - never fails. PIPEDPEER_TOKEN wins, else the token in
# cluster.json (the one config file; the `auth` command is gone).
pp_read_token() {
	local cfg="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/cluster.json"
	PP_TOKEN="${PIPEDPEER_TOKEN:-}"
	if [[ -z "$PP_TOKEN" && -r "$cfg" ]]; then
		PP_TOKEN="$(grep -o '"token" *: *"[^"]*"' "$cfg" 2>/dev/null | sed 's/.*"\([^"]*\)"$/\1/' || true)"
	fi
}

pp_curl() {
	if [[ -n "${PP_TOKEN:-}" ]]; then
		curl -H "X-Pipedpeer-Token: $PP_TOKEN" "$@"
	else
		curl "$@"
	fi
}
