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

# Not $TMPDIR: /tmp is a tmpfs of limited size on these machines, and a -v
# run of the whole suite is not small.
scratch="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$scratch"
log="$(mktemp "$scratch/pipedpeer-skipcheck.XXXXXX")"
trap 'rm -f "$log"' EXIT

# Everything, not a chosen few. The point is to see what did not run, and a
# package left off the list is a package whose skips are invisible - which is
# how two integration tests gated on PIPEDPEER_INTEGRATION sat unrun in CI
# without appearing anywhere. must_run below is still the set that must not be
# among them.
go test -v -count=1 ./... > "$log" 2>&1 || {
	echo "FAIL: tests did not pass"
	tail -40 "$log"
	exit 1
}

skipped="$(grep -E '^\s*--- SKIP' "$log" | sed -E 's/.*SKIP: ([^ ]+).*/\1/' | sort -u || true)"
if [[ -n "$skipped" ]]; then
	echo "skipped this run ($(printf '%s\n' "$skipped" | wc -l) test(s)):"
	# With the reason, because "skipped" on its own does not say whether the
	# machine could have run it. Most of these are a missing python package,
	# which is a thing somebody can fix.
	grep -B1 -E '^\s*--- SKIP' "$log" |
		grep -E '^\s+[a-z_]+\.go:[0-9]+:' | sed 's/^ */    /' | sort -u | head -20 || true
	echo "$skipped" | sed 's/^/  /'
	echo
	echo "  scripts/test-on-host.sh <host> runs these where their dependencies are."
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
