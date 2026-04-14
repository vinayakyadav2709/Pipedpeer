#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root/cmd/pipedpeer"

tmp_output="$(mktemp)"
trap 'rm -f "$tmp_output"' EXIT

set +e
go test -v ./... 2>&1 | tee "$tmp_output"
test_exit=${PIPESTATUS[0]}
set -e

total_tests=$(grep -c '^=== RUN   ' "$tmp_output" || true)
passed_tests=$(grep -c '^--- PASS: ' "$tmp_output" || true)
failed_tests=$(grep -c '^--- FAIL: ' "$tmp_output" || true)
skipped_tests=$(grep -c '^--- SKIP: ' "$tmp_output" || true)
package_failures=$(grep -Ec '^FAIL\s+github\.com/pipedpeer/pipedpeer' "$tmp_output" || true)
no_test_pkgs=$(grep -Ec '\[no test files\]|\[no tests to run\]' "$tmp_output" || true)

echo
echo "==================== Test Summary ===================="
echo "Total tests:   $total_tests"
echo "Passed:        $passed_tests"
echo "Failed:        $failed_tests"
echo "Skipped:       $skipped_tests"
echo "Pkg failures:  $package_failures"
echo "No-test pkgs:  $no_test_pkgs"
if [[ $test_exit -eq 0 && $failed_tests -eq 0 && $package_failures -eq 0 ]]; then
	echo "Result:        PASS"
else
	echo "Result:        FAIL"
fi
echo "======================================================"

exit "$test_exit"
