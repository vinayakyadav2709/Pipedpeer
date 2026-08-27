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
# And the token probe, `tok=$(pipedpeer auth show | head -1)`, is a pipeline
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
	elif [[ -d "$repo_root/src" && -x "$repo_root/scripts/build.sh" ]]; then
		echo "building binary from $repo_root/src..." >&2
		"$repo_root/scripts/build.sh" >&2 || return 1
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

# pp_read_token [cli] - never fails, whatever the binary does.
pp_read_token() {
	local cli="${1:-$PP_CLI}" out
	out="$("$cli" auth show 2>/dev/null | head -1 || true)"
	case "$out" in
	*"no token set"* | *"unknown command"* | *Error*) out="" ;;
	esac
	PP_TOKEN="$out"
}

pp_curl() {
	if [[ -n "${PP_TOKEN:-}" ]]; then
		curl -H "X-Pipedpeer-Token: $PP_TOKEN" "$@"
	else
		curl "$@"
	fi
}
