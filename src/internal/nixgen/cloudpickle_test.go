package nixgen

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Pool kernels that have no source to send.
//
// The shim ships a kernel to a worker as source, which exists only for a
// plain named function. A lambda, a closure over a local, a bound method, a
// decorated function or a partial has no name a worker could resolve, so
// every one of them was counted "unshippable" and the tail ran at home - and
// nothing failed, which is how it stayed that way. cloudpickle carries those
// by value.
//
// The rules of pooldist_test.go apply and are why these tests are shaped as
// they are: the daemon runs OUT OF PROCESS with no access to __main__ or the
// workspace, and what is asserted is the daemon's own count of what it
// executed plus the shim's receipt. A correct result proves nothing - the
// local fallback ladder guarantees correct results even when nothing ever
// leaves the machine.

var cloudpickleEnv struct {
	sync.Once
	python string
	why    string
}

// pythonWithCloudpickle finds an interpreter that can serialise by value.
//
// A developer machine's python3 usually cannot: cloudpickle is a dependency
// pipedpeer puts into the environments it BUILDS, not one it expects to find
// lying around. So the fallback is to build the environment the daemon would
// really ship, which is also the more faithful test - that is the
// interpreter this code runs under in the field.
func pythonWithCloudpickle(t *testing.T) string {
	t.Helper()
	cloudpickleEnv.Do(func() {
		has := func(py string) bool {
			return exec.Command(py, "-c", "import cloudpickle").Run() == nil
		}
		if py := os.Getenv("PIPEDPEER_TEST_PYTHON"); py != "" {
			if has(py) {
				cloudpickleEnv.python = py
			} else {
				cloudpickleEnv.why = "PIPEDPEER_TEST_PYTHON=" + py + " cannot import cloudpickle"
			}
			return
		}
		if py, err := exec.LookPath("python3"); err == nil && has(py) {
			cloudpickleEnv.python = py
			return
		}
		nix, err := exec.LookPath("nix")
		if err != nil {
			cloudpickleEnv.why = "no python3 with cloudpickle, and no nix to build one"
			return
		}
		dir, err := os.MkdirTemp("", "pp-cpkenv-")
		if err != nil {
			cloudpickleEnv.why = err.Error()
			return
		}
		defer os.RemoveAll(dir)
		flake := GenerateFlakeForArch(nil, "python3", NixArch(), false)
		if err := os.WriteFile(filepath.Join(dir, "flake.nix"), []byte(flake), 0o644); err != nil {
			cloudpickleEnv.why = err.Error()
			return
		}
		cmd := exec.Command(nix, "build", "--extra-experimental-features",
			"nix-command flakes", "--no-link", "--print-out-paths")
		cmd.Dir = dir
		out, err := cmd.Output()
		if err != nil {
			cloudpickleEnv.why = fmt.Sprintf("building a cloudpickle env: %v", err)
			return
		}
		fields := strings.Fields(strings.TrimSpace(string(out)))
		if len(fields) == 0 {
			cloudpickleEnv.why = "nix build printed no store path"
			return
		}
		py := filepath.Join(fields[len(fields)-1], "bin", "run")
		if !has(py) {
			// The generated environment is meant to carry cloudpickle. If it
			// does not, that is this package's bug rather than a missing
			// local dependency, and skipping would hide exactly that.
			cloudpickleEnv.why = "BUG: the generated environment " + py +
				" cannot import cloudpickle"
			return
		}
		cloudpickleEnv.python = py
	})
	if cloudpickleEnv.python == "" {
		if strings.HasPrefix(cloudpickleEnv.why, "BUG:") {
			t.Fatal(cloudpickleEnv.why)
		}
		t.Skipf("no interpreter with cloudpickle: %s", cloudpickleEnv.why)
	}
	return cloudpickleEnv.python
}

// byValueJob wraps a kernel definition in a Pool.map workload whose per-item
// cost is high enough that the adaptive chunker produces several chunks; a
// single chunk is dealt to the local side and nothing ever ships.
func byValueJob(kernelDef, expr string) string {
	return kernelDef + `

import multiprocessing

if __name__ == "__main__":
    with multiprocessing.Pool(4) as pool:
        got = pool.map(KERNEL, list(range(64)))
    want = [` + expr + ` for x in range(64)]
    assert got == want, "wrong results: %r" % (got[:8],)
    print("POOL-OK")
`
}

// TestKernelsWithoutSourceStillReachAPeer is the point of the whole change.
func TestKernelsWithoutSourceStillReachAPeer(t *testing.T) {
	python := pythonWithCloudpickle(t)

	cases := []struct {
		name   string
		kernel string
		expr   string
	}{
		{"lambda", `
import time
KERNEL = lambda x: (time.sleep(0.02), x * 3)[1]`, "x * 3"},

		{"closure over a local", `
import time
def _outer(k):
    def inner(x):
        time.sleep(0.02)
        return x * k
    return inner
KERNEL = _outer(7)`, "x * 7"},

		{"bound method", `
import time
class Work:
    def __init__(self, off):
        self.off = off
    def run(self, x):
        time.sleep(0.02)
        return x + self.off
KERNEL = Work(100).run`, "x + 100"},

		{"decorated function", `
import functools, time
def deco(f):
    @functools.wraps(f)
    def w(x):
        time.sleep(0.02)
        return f(x) + 1
    return w
@deco
def KERNEL(x):
    return x * 2`, "x * 2 + 1"},

		{"partial", `
import functools, time
def _add(a, b):
    time.sleep(0.02)
    return a + b
KERNEL = functools.partial(_add, 10)`, "x + 10"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := runPoolScriptWith(t, python,
				map[string]string{"job.py": byValueJob(tc.kernel, tc.expr)}, "job.py")
			assertDistributed(t, res, "POOL-OK")

			if res.stats.ByRef == 0 {
				t.Errorf("no chunk shipped by value; the daemon saw %d source chunks", res.stats.BySrc)
			}
			if res.stats.BySrc > 0 {
				t.Errorf("%d chunks shipped as source for a callable that has none", res.stats.BySrc)
			}
			if res.receipt.ShippedPickled == 0 {
				t.Errorf("receipt does not record any by-value shipping: %+v", res.receipt)
			}
			if res.receipt.Unshippable > 0 {
				t.Errorf("receipt still counts %d items unshippable", res.receipt.Unshippable)
			}
			// By-value items are a subset of what was dispatched, so the
			// count cannot exceed it. Counting the tail where the tier is
			// chosen rather than what was sent made it larger - the race
			// keeps half the tail local - and a subset exceeding its
			// superset is the kind of flattering number the receipt exists
			// to prevent.
			if res.receipt.ShippedPickled > res.receipt.DispatchedItems {
				t.Errorf("receipt claims %d items shipped by value but only %d dispatched",
					res.receipt.ShippedPickled, res.receipt.DispatchedItems)
			}
		})
	}
}

// TestNamedFunctionStillShipsAsSource pins the tier order.
//
// Source is the cheaper and older path - a few hundred bytes of text, no
// serialisation compatibility surface at all - so it has to stay the first
// choice. Nothing about a by-value payload's RESULT would reveal a swapped
// order, which is why this asserts which form the daemon received.
func TestNamedFunctionStillShipsAsSource(t *testing.T) {
	python := pythonWithCloudpickle(t)
	res := runPoolScriptWith(t, python, map[string]string{"job.py": poolScript}, "job.py")
	assertDistributed(t, res, "POOL-OK")

	if res.stats.BySrc == 0 {
		t.Errorf("a named function did not ship as source (%d by value)", res.stats.ByRef)
	}
	if res.stats.ByRef > 0 {
		t.Errorf("a named function shipped by value %d times; source is the cheaper tier", res.stats.ByRef)
	}
	if res.receipt.ShippedPickled > 0 {
		t.Errorf("receipt claims %d by-value items for a plain named function", res.receipt.ShippedPickled)
	}
}

// TestHeaderCarriesOneKernelFormNotBoth.
//
// The runner prefers func_src and ignores a callable sent beside it. A
// header carrying both would therefore run the source while the test
// believed it was exercising the by-value path - green, and measuring the
// wrong thing.
func TestHeaderCarriesOneKernelFormNotBoth(t *testing.T) {
	python := pythonWithCloudpickle(t)
	job := byValueJob(`
import time
KERNEL = lambda x: (time.sleep(0.02), x + 1)[1]`, "x + 1")

	res := runPoolScriptWith(t, python, map[string]string{"job.py": job}, "job.py")
	assertDistributed(t, res, "POOL-OK")
	if res.stats.Both > 0 {
		t.Fatalf("%d requests carried func_src and func together", res.stats.Both)
	}
	if res.stats.BySrc+res.stats.ByRef != res.stats.Requests {
		t.Fatalf("requests=%d but by_src=%d + by_ref=%d",
			res.stats.Requests, res.stats.BySrc, res.stats.ByRef)
	}
}

// TestKernelThatCannotTravelAtAllStaysLocal.
//
// cloudpickle widens what can ship; it does not make everything shippable. A
// kernel holding a lock, a socket or a live generator still has to stay
// local, and has to do it the way it always did: correct results, a log
// line, a receipt that says so, and no traceback. The promise interception
// makes is that it never breaks a working script.
func TestKernelThatCannotTravelAtAllStaysLocal(t *testing.T) {
	python := pythonWithCloudpickle(t)
	job := `
import multiprocessing, threading, time

LOCK = threading.Lock()

def _make():
    lk = LOCK
    def kernel(x):
        time.sleep(0.02)
        with lk:
            return x * 2
    return kernel

KERNEL = _make()

if __name__ == "__main__":
    with multiprocessing.Pool(4) as pool:
        got = pool.map(KERNEL, list(range(64)))
    assert got == [x * 2 for x in range(64)], "wrong results: %r" % (got[:8],)
    print("LOCAL-OK")
`
	res := runPoolScriptWith(t, python, map[string]string{"job.py": job}, "job.py")
	if !strings.Contains(res.stdout, "LOCAL-OK") {
		t.Fatalf("script did not finish:\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
	if strings.Contains(res.stderr, "Traceback") {
		t.Errorf("a kernel that cannot ship produced a traceback:\n%s", res.stderr)
	}
	if res.receipt.Unshippable == 0 {
		t.Errorf("receipt does not count the work as unshippable: %+v", res.receipt)
	}
	if res.receipt.ShippedPickled > 0 {
		t.Errorf("receipt claims %d by-value items for a kernel holding a lock", res.receipt.ShippedPickled)
	}
	if res.stats.Items > 0 {
		t.Errorf("the daemon executed %d items of a kernel that cannot be serialised", res.stats.Items)
	}
}

// TestByValueKernelCarriesTheWorkspaceItReads.
//
// cloudpickle pickles a module-level function BY REFERENCE by default: it
// stores the module name and expects the far side to import it. The job's
// own files never travel to a worker, so a lambda calling a helper from a
// sibling module would produce a payload naming a module the worker has
// never heard of - and it would fail there, in the daemon, rather than here.
//
// The daemon in this test runs out of process from its own directory with no
// access to the workspace, so a by-reference payload cannot accidentally
// resolve.
func TestByValueKernelCarriesTheWorkspaceItReads(t *testing.T) {
	python := pythonWithCloudpickle(t)
	files := map[string]string{
		"kernel.py": `
import time

SCALE = 9

def helper(x):
    time.sleep(0.02)
    return x * SCALE
`,
		"job.py": `
import multiprocessing
import kernel

KERNEL = lambda x: kernel.helper(x) + 1

if __name__ == "__main__":
    with multiprocessing.Pool(4) as pool:
        got = pool.map(KERNEL, list(range(64)))
    assert got == [x * 9 + 1 for x in range(64)], "wrong results: %r" % (got[:8],)
    print("POOL-OK")
`,
	}
	res := runPoolScriptWith(t, python, files, "job.py")
	assertDistributed(t, res, "POOL-OK")
	if res.stats.ByRef == 0 {
		t.Errorf("the lambda never shipped by value")
	}
	if res.receipt.ShippedPickled == 0 {
		t.Errorf("receipt does not record by-value shipping: %+v", res.receipt)
	}
}

// TestEveryPoolEntryPointTakesAKernelWithoutSource.
//
// map and imap reach the local-pool routing through _run; apply and
// apply_async did not, so a lambda through those raised PicklingError from
// inside multiprocessing while the same lambda through map worked. A drop-in
// replacement that works for three of five entry points is a trap: the
// failure appears only when someone changes which one they call.
func TestEveryPoolEntryPointTakesAKernelWithoutSource(t *testing.T) {
	python := pythonWithCloudpickle(t)
	job := `
import multiprocessing

KERNEL = lambda x: x * 3

if __name__ == "__main__":
    with multiprocessing.Pool(4) as pool:
        assert pool.apply(KERNEL, (5,)) == 15, "apply"
        assert pool.apply_async(KERNEL, (6,)).get(30) == 18, "apply_async"
        assert pool.map(KERNEL, [1, 2]) == [3, 6], "map"
        assert list(pool.imap(KERNEL, [1, 2])) == [3, 6], "imap"
        assert sorted(pool.imap_unordered(KERNEL, [1, 2])) == [3, 6], "imap_unordered"
        assert pool.starmap(lambda a, b: a + b, [(1, 2)]) == [3], "starmap"
    print("ENTRYPOINTS-OK")
`
	res := runPoolScriptWith(t, python, map[string]string{"job.py": job}, "job.py")
	if !strings.Contains(res.stdout, "ENTRYPOINTS-OK") {
		t.Fatalf("a Pool entry point rejected a lambda:\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
}

// TestApplyWorksWithoutKeywordArguments.
//
// multiprocessing.Pool.apply defaults kwds to {}; the shim defaulted it to
// None and passed it through to func(*args, **kwds). So the ordinary call -
// pool.apply(f, (x,)), no keyword arguments - raised "argument after ** must
// be a mapping, not NoneType" from inside a worker, which reads as a fault in
// the user's own function rather than in the drop-in replacement.
func TestApplyWorksWithoutKeywordArguments(t *testing.T) {
	python := pythonWithCloudpickle(t)
	job := `
import multiprocessing

def kernel(x, bonus=0):
    return x * 2 + bonus

if __name__ == "__main__":
    with multiprocessing.Pool(4) as pool:
        assert pool.apply(kernel, (5,)) == 10, "apply without kwds"
        assert pool.apply(kernel, (5,), {"bonus": 1}) == 11, "apply with kwds"
        assert pool.apply_async(kernel, (5,)).get(30) == 10, "apply_async without kwds"
        assert pool.apply_async(kernel, (5,), {"bonus": 1}).get(30) == 11, "apply_async with kwds"
    print("APPLY-OK")
`
	res := runPoolScriptWith(t, python, map[string]string{"job.py": job}, "job.py")
	if !strings.Contains(res.stdout, "APPLY-OK") {
		t.Fatalf("apply mishandled its arguments:\nstdout:\n%s\nstderr:\n%s", res.stdout, res.stderr)
	}
}
