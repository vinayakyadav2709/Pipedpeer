#!/usr/bin/env bash
# Runs the test suite on a machine that has the optional Python dependencies.
#
# Six of the shim tests skip without pandas, scikit-learn or joblib, and those
# six are the ones that exercise the interception this project exists for. A
# developer machine without them reports "ok" for the whole package, which is
# how a genuinely failing test stayed green: the fake daemon in shim_test.go
# stopped decompressing payloads when compression landed, and every test that
# would have caught it was skipping.
#
# So: build the test binaries here, run them there. Go test binaries are
# self-contained, so the remote host needs the Python packages and nothing else
# - no Go, no checkout, no build.
#
#   scripts/test-on-host.sh yeet
#   scripts/test-on-host.sh yeet ./internal/nixgen/
set -euo pipefail

host="${1:-}"
if [[ -z "$host" ]]; then
	echo "usage: $0 <ssh-host> [package ...]" >&2
	echo "  e.g. $0 yeet ./internal/nixgen/" >&2
	exit 2
fi
shift

packages=("$@")
if (( ${#packages[@]} == 0 )); then
	# The packages whose tests have optional Python dependencies. Everything
	# else runs the same everywhere and is covered by `go test ./...`.
	packages=(./internal/nixgen/ ./internal/daemonapi/)
fi

cd "$(dirname "$0")/../src"

# Not $TMPDIR: /tmp is a tmpfs of limited size on these machines and a test
# binary is tens of megabytes.
scratch="${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer/testbins"
mkdir -p "$scratch"

remote_dir=".pipedpeer-tests"
ssh "$host" "mkdir -p $remote_dir"

failed=0
for pkg in "${packages[@]}"; do
	name="$(basename "$pkg")"
	bin="$scratch/$name.test"
	echo "building $pkg ..."
	# Static, because the remote machine's libc is not this one's.
	CGO_ENABLED=0 go test -c -o "$bin" "$pkg"
	scp -q "$bin" "$host:$remote_dir/$name.test"

	echo "running $name on $host ..."
	# -test.v so the skip lines are visible: a skip here is the thing this
	# script exists to surface, not something to let past quietly.
	if ! ssh "$host" "cd $remote_dir && ./$name.test -test.v" > "$scratch/$name.out" 2>&1; then
		failed=1
		echo "FAIL: $name"
		grep -E "^--- (FAIL|SKIP)" "$scratch/$name.out" | head -20 || true
		tail -30 "$scratch/$name.out"
		continue
	fi

	skipped="$(grep -cE "^--- SKIP" "$scratch/$name.out" || true)"
	passed="$(grep -cE "^--- PASS" "$scratch/$name.out" || true)"
	echo "  $passed passed, $skipped skipped"
	if [[ "${skipped:-0}" -gt 0 ]]; then
		grep -E "^--- SKIP" "$scratch/$name.out" | sed 's/^/    /'
		echo "    ^ still skipping on $host; install what they need or they are not being run anywhere"
	fi
done

if (( failed )); then
	echo
	echo "Tests that pass here and fail there are the ones worth having. Output is"
	echo "in $scratch/*.out"
	exit 1
fi
echo "all packages passed on $host"
