#!/usr/bin/env bash
# Can these tests fail?
#
# The failure this project exists not to repeat was a suite whose names
# promised what its assertions never checked: Pool distribution had never
# worked and every test passed anyway, because a three-layer fallback ladder
# made correctness unconditionally true. A test that cannot fail is worse than
# no test, because it is counted as evidence.
#
# So: break each feature on purpose, run the tests named for it, and require
# red. Anything that stays green is not testing what its name says.
#
#   scripts/audit-tests.sh              # every case
#   scripts/audit-tests.sh weighted     # cases whose name matches
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
filter="${1:-}"

work="$(mktemp -d)"

# Restore from the copies this script made, never `git checkout`. An earlier
# version cleaned up with `git checkout -- src`, which does not restore what
# was mutated - it discards every uncommitted change in the tree, and it threw
# away three unrelated fixes that were in progress when it ran. A tool that
# edits source must put back exactly what it took and nothing else.
declare -A restore_map=()
restore_all() {
	local f
	for f in "${!restore_map[@]}"; do
		[[ -f "${restore_map[$f]}" ]] && cp "${restore_map[$f]}" "$f"
	done
	rm -rf "$work"
}
trap restore_all EXIT

pass=0
fail=0
declare -a failed

# case <name> <file> <python-mutation> <package> <test-regex>
#
# The mutation is a Python snippet with `s` bound to the file's text; it must
# return the broken version. Anything that does not change the file is itself
# a failure — a mutation that silently no-ops would report the test as strong
# when nothing was broken.
case_() {
	local name="$1" file="$2" mutation="$3" pkg="$4" tests="$5"

	if [[ -n "$filter" && "$name" != *"$filter"* ]]; then
		return
	fi

	local backup="$work/$(echo "$name" | tr -c 'a-zA-Z0-9' '_').bak"
	cp "$file" "$backup"
	restore_map["$file"]="$backup"

	if ! python3 - "$file" <<PY
import sys
p = sys.argv[1]
s = open(p).read()
before = s
$mutation
if s == before:
    sys.stderr.write("mutation changed nothing\n")
    sys.exit(3)
open(p, "w").write(s)
PY
	then
		printf '%-46s %s\n' "$name" "BROKEN CASE: the mutation did not apply"
		failed+=("$name (mutation did not apply)")
		fail=$((fail + 1))
		cp "$backup" "$file"
		return
	fi

	local out
	out="$(cd src && go test "$pkg" -run "$tests" -count=1 2>&1)"
	local rc=$?
	cp "$backup" "$file"

	if [[ $rc -ne 0 ]]; then
		printf '%-46s %s\n' "$name" "red — the test catches it"
		pass=$((pass + 1))
	else
		printf '%-46s %s\n' "$name" "STILL GREEN"
		echo "$out" | tail -3 | sed 's/^/    /'
		failed+=("$name")
		fail=$((fail + 1))
	fi
}

echo "Breaking each feature and requiring its tests to notice."
echo

# ---- this batch -------------------------------------------------------------
case_ "weighted gradient fold" src/internal/daemonapi/ddpreduce.go \
	's = s.replace("a.sum[i] += float64(math.Float32frombits(binary.LittleEndian.Uint32(payload[i*4:]))) * weight", "a.sum[i] += float64(math.Float32frombits(binary.LittleEndian.Uint32(payload[i*4:])))", 1)
s = s.replace("\ta.weight += weight", "\ta.weight += 1")' \
	./internal/daemonapi/ "TestUnequalBatchesAverageBySampleCount"

case_ "equal step counts across shares" src/internal/nixgen/shim.go \
	's = s.replace("        self.steps = n // sum(self.batches)", "        self.steps = -(-n // (sum(self.batches) + 1))", 1)' \
	./internal/nixgen/ "TestEqualStepsAcrossUnevenShares"

case_ "contiguous shards" src/internal/nixgen/shim.go \
	's = s.replace("        start, end = self._bounds()\n        return iter(order[start:end])", "        return iter(order[self.rank::len(self.batches)])", 1)' \
	./internal/nixgen/ "TestWeightedShardsPartitionTheDataset"

case_ "one shuffle seed for every rank" src/internal/nixgen/shim.go \
	's = s.replace("g.manual_seed(self.epoch)", "g.manual_seed(self.epoch * 100 + self.rank)", 1)' \
	./internal/nixgen/ "TestShuffledShardsStayDisjoint"

case_ "rebalance hysteresis" src/internal/daemonapi/ddprebalance.go \
	's = s.replace("const rebalanceMinShift = 0.10", "const rebalanceMinShift = 0.0", 1)' \
	./internal/daemonapi/ "TestSmallDriftIsLeftAlone"

case_ "drop weighs sync against compute" src/internal/daemonapi/ddprebalance.go \
	's = s.replace("\tif added >= syncSec {", "\tif false {", 1)' \
	./internal/daemonapi/ "TestARankThatEarnsItsPlaceIsKept"

case_ "dropped rank is not waited for" src/internal/daemonapi/ddpsync.go \
	's = s.replace("\tif e.expected > 0 && e.expected <= e.world {", "\tif false {", 1)' \
	./internal/daemonapi/ "TestADroppedRankIsNotWaitedFor"

case_ "archive codec reads gzip" src/internal/tarcodec/tarcodec.go \
	's = s.replace("\tif m, err := br.Peek(2); err == nil && string(m) == string(gzipMagic) {\n\t\treturn Gzip\n\t}\n", "", 1)' \
	./internal/tarcodec/ "TestEveryEncodingIsReadable"

case_ "sniffing does not consume" src/internal/tarcodec/tarcodec.go \
	's = s.replace("func Sniff(br *bufio.Reader) Encoding {", "func Sniff(br *bufio.Reader) Encoding {\n\tbr.Discard(1)", 1)' \
	./internal/tarcodec/ "TestSniffNamesTheEncoding"

case_ "out-of-memory branch" src/internal/nixgen/shim.go \
	's = s.replace("    if not _fits_locally(working_set):", "    if False:", 1)' \
	./internal/nixgen/ "TestWorkThatDoesNotFitGoesToTheClusterWhateverItCosts"

case_ "GPU peer priced above CPU" src/internal/nixgen/shim.go \
	's = s.replace("_GPU_SPEEDUP = 6.0", "_GPU_SPEEDUP = 1.0", 1)' \
	./internal/nixgen/ "TestAGPUPeerIsNotPricedAsACPU"

case_ "per-peer local address" src/main.go \
	's = s.replace("func routeSourceIP(host string) (net.IP, bool) {", "func routeSourceIP(host string) (net.IP, bool) {\n\tif true {\n\t\treturn net.ParseIP(detectLocalIP()), true\n\t}", 1)' \
	. "TestTheRouteDecidesTheAddress"

case_ "queue depth is counted" src/internal/daemonapi/server.go \
	's = s.replace("func (s *Server) QueueDepth() int {", "func (s *Server) QueueDepth() int {\n\tif true {\n\t\treturn 0\n\t}", 1)' \
	./internal/daemonapi/ "TestQueueDepthCountsPromisedWork|TestHealthReportsBothFields"

case_ "recent failures are counted" src/internal/daemonapi/server.go \
	's = s.replace("func (s *Server) RecentFailures() int {", "func (s *Server) RecentFailures() int {\n\tif true {\n\t\treturn 0\n\t}", 1)' \
	./internal/daemonapi/ "TestRecentFailuresCountsRecentOnes|TestHealthReportsBothFields"

# ---- the previous batch, never adversarially checked -------------------------
case_ "every module on an import line" src/internal/pythondeps/deps.go \
	's = s.replace("\t\tfor _, part := range strings.Split(rest, \",\") {", "\t\tfor _, part := range strings.Split(rest, \",\")[:1] {", 1)' \
	./internal/pythondeps/ "TestCommaSeparatedImportsAreAllDetected"

case_ "nix found in installer locations" src/internal/nixstore/nixstore.go \
	's = s.replace("\tdirs := systemNixDirs", "\tdirs := []string{}\n\t_ = systemNixDirs", 1)' \
	./internal/nixstore/ "TestTheInstallerLocationsAreSearched"

case_ "nix found in the home profile" src/internal/nixstore/nixstore.go \
	's = s.replace("\t\tdirs = append(append([]string{}, dirs...), filepath.Join(home, \".nix-profile\", \"bin\"))", "", 1)' \
	./internal/nixstore/ "TestSystemNixIsFoundOffPath"

case_ "closure fragment marked partial" src/internal/daemonctl/closurediff.go \
	's = s.replace("\t\tmp.WriteField(\"partial\", \"1\")", "", 1)' \
	./internal/daemonctl/ "TestFragmentIsMarkedPartial"

case_ "bandwidth probe is skipped" src/internal/nixgen/shim.go \
	's = s.replace("    if not _offload_wins(nbytes, flops_per_byte, round_trip, split,\n                         flops_per_sec, K, _OPTIMISTIC_BW, remote_flops):\n        return False\n", "", 1)' \
	./internal/nixgen/ "TestShimDoesNotProbeWhenNoLinkWouldChangeTheAnswer"

case_ "a second GPU gets a rank" src/internal/ddpplace/ddpplace.go \
	's = s.replace("\t\tslots := c.Slots", "\t\tslots := 1\n\t\t_ = c.Slots", 1)' \
	./internal/ddpplace/ "TestSecondGPUOnANodeGetsARank"

case_ "co-located ranks split node memory" src/internal/ddpplace/ddpplace.go \
	's = s.replace("MemBytes: c.MemBytes / int64(slots),", "MemBytes: c.MemBytes,", 1)' \
	./internal/ddpplace/ "TestRanksOnOneNodeShareItsMemory"

case_ "roommate reservations subtracted" src/internal/daemonapi/server.go \
	's = s.replace("func (s *Server) reservedOnThisMachine() int64 {", "func (s *Server) reservedOnThisMachine() int64 {\n\tif true {\n\t\treturn 0\n\t}", 1)' \
	./internal/daemonapi/ "TestAPeerSharingThisMachineIsSubtracted"

case_ "roommate asked live, not cached" src/internal/daemonapi/server.go \
	's = s.replace("\t\tif live, ok := peerReservedNow(ph); ok {", "\t\tif live, ok := int64(0), false; ok {\n\t\t\t_ = live\n\t\t}\n\t\tif live, ok := int64(0), false; ok {", 1)' \
	./internal/daemonapi/ "TestTheLiveFigureBeatsTheCachedOne"

case_ "page cache is not spent budget" src/internal/cgroups/budget.go \
	's = s.replace("\treclaimable := stat[\"inactive_file\"] + stat[\"slab_reclaimable\"]", "\treclaimable := int64(0)\n\t_ = stat", 1)' \
	./internal/cgroups/ "TestPageCacheIsNotSpentBudget"

case_ "state is snapshotted before marshal" src/internal/daemonapi/server.go \
	's = s.replace("\ts.mu.Lock()\n\tleases := make(map[string]*Lease, len(s.leases))", "\tif true {\n\t\ts.state.save(s.leases, s.jobs)\n\t\treturn\n\t}\n\ts.mu.Lock()\n\tleases := make(map[string]*Lease, len(s.leases))", 1)' \
	./internal/daemonapi/ "TestPersistUnderConcurrentUploads"

case_ "prefer-GPU excludes CPU nodes" src/main.go \
	's = s.replace("func ddpOneDeviceKind(cands []ddpplace.Candidate, preferGPU bool) []ddpplace.Candidate {", "func ddpOneDeviceKind(cands []ddpplace.Candidate, preferGPU bool) []ddpplace.Candidate {\n\tif true {\n\t\treturn cands\n\t}", 1)' \
	. "TestPreferGPUKeepsCPUsOutOfTheRing"

case_ "a straggler is named" src/internal/daemonapi/ddpsync.go \
	's = s.replace("\tif slow <= 0 || fast <= 0 || slow/fast < pacedRatio {", "\tif true {", 1)' \
	./internal/daemonapi/ "TestOneSlowRankIsNamed"

case_ "dead env vars are refused" src/internal/nixgen/shim.go \
	's = s.replace("_BW_TTL = 300.0", "_BW_TTL = 300.0\n_DEAD = \"PIPEDPEER_NEVER_READ_BY_ANYTHING\"", 1)' \
	./internal/nixgen/ "TestEveryEnvVarTheShimNamesIsRead"

# ---- found by the acceptance audit, 2026-08-28 -------------------------------
# All three were checks that read the machine they ran on rather than the
# situation they described, and all three had tests that skipped rather than
# failed. A skip is not a pass, and these make sure it stays that way.

case_ "setup finds nix off PATH" src/internal/setup/setup.go \
	's = s.replace("func nixInstalled() bool {\n\t_, err := nixstore.SystemNix()\n\treturn err == nil\n}", "func nixInstalled() bool {\n\treturn binaryCheck(\"nix\")()\n}", 1)' \
	./internal/setup/ "TestSetupFindsNixOffPath"

case_ "the userns diagnosis names a fix" src/internal/userns/userns.go \
	's = s.replace("func diagnose() string {", "func diagnose() string {\n\tif true {\n\t\treturn \"the kernel said no\"\n\t}", 1)' \
	./internal/userns/ "TestDiagnosisNamesAFix"

case_ "the missing-nix error names where it looked" src/internal/nixstore/nixstore.go \
	's = s.replace("\treturn \"\", fmt.Errorf(\"nix not found on PATH or in %s\", strings.Join(dirs, \", \"))", "\t_ = dirs\n\treturn \"\", fmt.Errorf(\"nix not found\")", 1)' \
	./internal/nixstore/ "TestMissingNixSaysWhereItLooked"

case_ "one address holds one node" src/internal/nodestore/store.go \
	's = s.replace("\tif err := s.forgetOthersAt(host, port, nodeID); err != nil {\n\t\treturn err\n\t}\n", "", 1)' \
	./internal/nodestore/ "TestReaddingAnAddressReplacesTheOldIdentity"

echo
echo "======================================================"
printf 'caught by their tests: %d\n' "$pass"
printf 'did NOT fail:          %d\n' "$fail"
if (( fail )); then
	echo
	echo "These features can be deleted and their tests still pass:"
	printf '  %s\n' "${failed[@]}"
	echo
	echo "A test that cannot fail is counted as evidence and is not any."
	exit 1
fi
echo "Every feature above has a test that notices when it is gone."
exit 0
