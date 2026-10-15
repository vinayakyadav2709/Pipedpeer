#!/usr/bin/env bash
# Unit tests for the Go module, with the log kept and every skip named.
# Integration tests are opt-in: export PIPEDPEER_INTEGRATION=1 (or run
# scripts/test-integration.sh, which runs both of them).
#
# The old version ran `go test > log` under `set -e`, so a failing suite
# exited before the log was printed and `make test` failed with no output.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log_dir="$repo_root/test_results"
log="$log_dir/unit_tests.log"
mkdir -p "$log_dir"

# Builds and tests run in a capped slice so a runaway test is OOM-killed, not
# the laptop. Fall back to a plain run where systemd-run is missing (CI).
capped=()
if command -v systemd-run >/dev/null 2>&1 && systemctl --user is-system-running >/dev/null 2>&1; then
	capped=(systemd-run --user --scope -q --slice=pipedpeer-agents.slice)
fi

echo "==================== Running Pipedpeer Test Suite ===================="
cd "$repo_root/src"
set +e
"${capped[@]}" go test -v -count=1 ./... >"$log" 2>&1
test_exit=$?
set -e

# A skip is not a pass. Name them: a suite that quietly skipped the tests
# carrying its central claim still prints ok.
skipped="$(grep -cE '^\s*--- SKIP' "$log" || true)"
if [[ "${skipped:-0}" -gt 0 ]]; then
	echo "Skipped $skipped test(s):"
	grep -E '^\s*--- SKIP' "$log" | sed -E 's/.*SKIP: ([^ ]+).*/  \1/' | sort -u
	echo "Run scripts/check-skips.sh to fail on a skipped distribution test."
fi

if [[ $test_exit -ne 0 ]]; then
	echo "Failures:"
	grep -E '^\s*--- FAIL|^FAIL|panic:' "$log" || tail -50 "$log"
fi

echo
echo "==================== Test Summary ===================="
if [[ $test_exit -eq 0 ]]; then
	echo "Result:        PASS"
else
	echo "Result:        FAIL"
fi
echo "Log: $log"
echo "======================================================"
exit "$test_exit"
