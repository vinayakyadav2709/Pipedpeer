package nixgen

import "testing"

// TestWorkThatDoesNotFitGoesToTheClusterWhateverItCosts.
//
// Every other branch of the cost model compares how long local takes against
// how long remote takes, which quietly assumes local is possible. When the
// working set does not fit, local is not slower — it is the OOM killer, or
// hours of swap. Measured: forcing an 8192-cube product on a 14 GB machine
// drove it to 194 MB free and the kernel killed the desktop shell.
//
// Driven by an env override rather than by actually running a machine out of
// memory, which is a test that takes the machine with it.
func TestWorkThatDoesNotFitGoesToTheClusterWhateverItCosts(t *testing.T) {
	runShimPython(t, "oom", shimFakeDaemon+`

# A product whose working set is far more than this machine claims to have.
# The shape is small so nothing large is allocated; only the decision is under
# test.
assert shim._numpy_should_offload(
    32 * 1024 * 1024, 256, 5, True, 200e9, working_set=64 * 1024**3), (
    "a working set larger than memory was kept local, which is not a slower "
    "answer - it is the OOM killer")

# And the same call without the working set stays local, as it always has:
# local BLAS beats the transport at this size.
assert not shim._numpy_should_offload(32 * 1024 * 1024, 256, 5, True, 200e9)
print("OOM-OK")
`, "OOM-OK")
}

// TestFitsLocallyReadsTheMachine, and treats "cannot tell" as "fits" rather
// than inventing a refusal on a machine whose meminfo it could not read.
func TestFitsLocallyReadsTheMachine(t *testing.T) {
	runShimPythonEnv(t, "fits", `
import sitecustomize as shim

# The override says one gigabyte is free.
assert shim._local_free_bytes() == 1000000000, shim._local_free_bytes()
assert shim._fits_locally(500 * 1000 * 1000), "500 MB did not fit in 1 GB"
assert not shim._fits_locally(2 * 1000 * 1000 * 1000), "2 GB fitted in 1 GB"
# Nine tenths, not all: an allocation that exactly fills memory leaves the
# machine thrashing rather than working.
assert not shim._fits_locally(950 * 1000 * 1000), "950 MB of 1 GB was accepted"
# Nothing asked for always fits.
assert shim._fits_locally(0)
print("FITS-OK")
`, "FITS-OK", "PIPEDPEER_TEST_FREE_MEM=1000000000")
}

// TestAGPUPeerIsNotPricedAsACPU.
//
// The remote estimate used this cluster's CPU BLAS rate for every peer.
// Measured on the hardware here, the same model and batch took 1.5s on the
// GPU against 56.2s on the CPU — so charging CPU rates for a GPU peer refuses
// offloads that would have paid for themselves many times over.
func TestAGPUPeerIsNotPricedAsACPU(t *testing.T) {
	runShimPython(t, "gputerm", shimFakeDaemon+`
K, bw = 4, 1e9
assert shim._GPU_SPEEDUP > 1, "the GPU term does not make remote any faster"

# Real square-matmul shapes: nbytes and flops-per-byte both follow n, which
# is what lets compute outgrow transfer. Holding flops-per-byte fixed makes
# both sides scale linearly in n and the decision size-independent - the first
# version of this test did that and could never have failed.
def shape(n):
    return (8 * n * n, max(8, n // 8), 5, True, 200e9)

# A GPU peer must never be judged worse than a CPU one for the same work.
for n in (1024, 4096, 8192, 16384):
    cpu = shim._offload_wins(*shape(n), K, bw)
    gpu = shim._offload_wins(*shape(n), K, bw, 200e9 * shim._GPU_SPEEDUP)
    assert gpu or not cpu, "n=%d: a GPU peer scored worse than a CPU one" % n

# And somewhere it changes the answer, or it is arithmetic with no effect.
found = [n for n in range(2048, 40961, 2048)
         if not shim._offload_wins(*shape(n), K, bw)
         and shim._offload_wins(*shape(n), K, bw, 200e9 * shim._GPU_SPEEDUP)]
assert found, "no size exists where the GPU term changes the answer"
print("GPUTERM-OK n=%d" % found[0])
`, "GPUTERM-OK")
}

// TestSingleWorkerOffloadIsNotARace. svd and eig relocate work so a weak
// orchestrator is freed entirely; they are worth it when shipping the matrix
// costs far less than computing it here, not when the peer is faster.
// Charging remote compute against that made the condition unsatisfiable.
func TestSingleWorkerOffloadIsNotARace(t *testing.T) {
	runShimPython(t, "svdcrit", shimFakeDaemon+`

# LAPACK speed: local compute dwarfs the transfer, so this must ship even
# though the peer is no faster than we are.
assert shim._offload_wins(32 * 1024 * 1024, 512, 3, False, 1.5e9, 1, 1e9), (
    "a single-worker offload was refused though shipping costs far less than "
    "computing it here")
print("SVDCRIT-OK")
`, "SVDCRIT-OK")
}
