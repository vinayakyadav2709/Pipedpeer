#!/usr/bin/env bash
# Fails when a test that is supposed to prove distribution quietly skipped.
#
# A green suite that skipped every test touching the cluster is not evidence
# of anything, and that is not hypothetical: six of sixteen shim tests skip
# without pandas installed, and those six are precisely the ones that exercise
# a remote path. The suite still printed ok. So the tests that carry the
# project's central claim are named here, and their absence is a failure
# rather than a silence.
set -euo pipefail

cd "$(dirname "$0")/../src"

must_run=(
	TestShimPoolDistributesMainModuleFunc
	TestShimPoolDistributesAdjacentModuleFunc
	TestShimPoolStarmapDistributes
	TestShimPoolUnshippableStaysLocalQuietly
	TestShimExecutorAPISurvivesInterception
	TestShimPoolSingleNodeDoesNotDialItself
	TestShimImapStaysLazy
	TestShimRaceCorrectWithRemote
	TestShimRaceCorrectWithDeadRemote
	TestMultiNodeSpill
	TestPoolStatsRecordsChunkOrigin
	TestPoolMapNoSplitRoutesPerPeer
	TestFanWidthRespectsFreeMemory
	TestShimDoesNotImportHeavyLibsAtStartup
	TestShimPatchesOnFirstImport
)

log="$(mktemp "${TMPDIR:-/var/tmp}/pipedpeer-skipcheck.XXXXXX")"
trap 'rm -f "$log"' EXIT

go test -v -count=1 ./internal/nixgen/ ./internal/daemonapi/ > "$log" 2>&1 || {
	echo "FAIL: tests did not pass"
	tail -40 "$log"
	exit 1
}

skipped="$(grep -E '^\s*--- SKIP' "$log" | sed -E 's/.*SKIP: ([^ ]+).*/\1/' || true)"
if [[ -n "$skipped" ]]; then
	echo "skipped this run:"
	echo "$skipped" | sed 's/^/  /'
fi

missing=()
for t in "${must_run[@]}"; do
	if ! grep -qE "^\s*--- PASS: $t\b" "$log"; then
		missing+=("$t")
	fi
done

if (( ${#missing[@]} )); then
	echo
	echo "FAIL: these tests carry the distribution claim and did not run:"
	printf '  %s\n' "${missing[@]}"
	echo
	echo "A suite that skips them and reports ok is how a completely broken"
	echo "dispatch path stayed invisible for months. Install the missing"
	echo "dependency (python3, pandas) rather than accepting the skip."
	exit 1
fi

echo "all ${#must_run[@]} distribution tests ran"
