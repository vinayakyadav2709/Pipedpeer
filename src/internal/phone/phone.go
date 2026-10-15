// Package phone makes an Android phone in Termux a pool-only worker.
//
// A phone has no Nix and no crun (no user namespaces for app processes), and
// an arm64 phone never holds the laptops' x86 store paths anyway. So it takes
// only Pool/joblib chunks, run with Termux's own python (`pkg install python
// python-numpy`), and refuses whole jobs through the pool-only role in
// cluster.json. The pool runner scripts use the standard library only. A chunk
// that ships func_src is re-compiled there and runs on any host python; a
// by-value pickled function carries bytecode, so it needs the submitter's
// minor version.
package phone

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/term"
)

// PythonHeader carries the submitter's python version ("3.12" or "3.12.4") on
// a /v1/store?runnable=1 request; only major.minor is compared.
const PythonHeader = "X-Pipedpeer-Python"

// FuncSrcHeader is "1" on a /v1/store?runnable=1 request whose chunk ships
// func_src: then any host python will do (Decisions: func_src mismatch rule).
const FuncSrcHeader = "X-Pipedpeer-Func-Src"

// IsTermux reports whether this process runs inside Termux.
func IsTermux() bool {
	return os.Getenv("TERMUX_VERSION") != "" || strings.Contains(os.Getenv("PREFIX"), "com.termux")
}

// poolOnly reports whether this node is configured pool-only. A variable so
// tests need no cluster.json.
var poolOnly = func() bool {
	c, err := clustercfg.Load()
	if err != nil {
		log.Warn().Err(err).Msg("phone: cannot read cluster.json; treating this node as a full node")
		return false
	}
	return c.PoolOnly
}

// hostPython finds the host python and its "major.minor". Variables so tests
// can stub them; probed once because every chunk asks.
var (
	lookPython  = func() (string, error) { return exec.LookPath("python3") }
	pyVersionOf = func(py string) (string, error) {
		out, err := exec.Command(py, "-c", "import sys; print('%d.%d' % sys.version_info[:2])").Output()
		return strings.TrimSpace(string(out)), err
	}
	pyOnce            sync.Once
	pyPath, pyVersion string
	pyErr             error
)

func hostPython() (string, string, error) {
	pyOnce.Do(func() {
		pyPath, pyErr = lookPython()
		if pyErr != nil {
			pyErr = fmt.Errorf("no python3 on PATH (in Termux: pkg install python python-numpy): %w", pyErr)
			return
		}
		pyVersion, pyErr = pyVersionOf(pyPath)
		if pyErr != nil {
			pyErr = fmt.Errorf("%s does not report its version: %w", pyPath, pyErr)
		}
	})
	return pyPath, pyVersion, pyErr
}

// PoolRunPath is the executable a pool chunk runs under: `<store>/bin/run`
// when the closure is here, else on a pool-only node the host python (the
// runner is invoked as `<runPath> <script> <args>`, which python accepts as
// is).
func PoolRunPath(store string) (string, error) {
	if store != "" {
		if run := filepath.Join(store, "bin", "run"); exists(run) {
			return run, nil
		}
	}
	if !poolOnly() {
		return "", fmt.Errorf("closure %s is not materialised here", store)
	}
	py, _, err := hostPython()
	if err != nil {
		log.Warn().Err(err).Str("store", store).Msg("phone: pool-only node cannot run a chunk")
		return "", err
	}
	return py, nil
}

// CanRun answers /v1/store?runnable=1: the closure is materialised, or this
// is a pool-only node with a host python that is either any version (the
// chunk ships func_src) or the submitter's minor version (by-value bytecode).
func CanRun(store string, h http.Header) bool {
	if store != "" && exists(filepath.Join(store, "bin", "run")) {
		return true
	}
	if !poolOnly() {
		return false
	}
	funcSrc := h.Get(FuncSrcHeader) == "1"
	want := minor(h.Get(PythonHeader))
	if want == "" && !funcSrc {
		return false
	}
	py, have, err := hostPython()
	if err != nil {
		log.Warn().Err(err).Msg("phone: pool-only node answers not runnable")
		return false
	}
	if !funcSrc && minor(have) != want {
		return false
	}
	if miss := missingModules(py, strings.Split(h.Get(ImportsHeader), ",")); len(miss) > 0 {
		log.Warn().Strs("missing", miss).Str("python", py).
			Msg("phone: refusing a chunk whose modules this phone's python lacks (pkg install python-<name> or pip install <name>)")
		return false
	}
	return true
}

// importable caches modules python can import: a real import, because a
// package can be installed and still fail to load (Termux torch dlopens the
// system libvulkan.so). A missing one is asked again, so a later pkg install
// counts without a restart.
var (
	importMu   sync.Mutex
	importable = map[string]bool{}
	tryImports = func(py string, mods []string) (map[string]bool, error) {
		var stderr strings.Builder
		cmd := exec.Command(py, append([]string{"-c",
			"import importlib, sys\nfor m in sys.argv[1:]:\n    try:\n        importlib.import_module(m)\n        print(m, True)\n    except Exception as e:\n        print(m, False)\n        print('pipedpeer: import %s failed: %r' % (m, e), file=sys.stderr)"},
			mods...)...)
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			log.Warn().Str("python", py).Str("stderr", msg).Msg("phone: module check")
		}
		if err != nil {
			return nil, err
		}
		got := map[string]bool{}
		for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if f := strings.Fields(l); len(f) == 2 {
				got[f[0]] = f[1] == "True"
			}
		}
		return got, nil
	}
)

// missingModules returns the names in mods that py cannot import.
func missingModules(py string, mods []string) []string {
	importMu.Lock()
	defer importMu.Unlock()
	var ask []string
	for _, m := range mods {
		if m = strings.TrimSpace(m); m == "" {
			continue
		}
		if _, ok := importable[m]; !ok {
			ask = append(ask, m)
		}
	}
	if len(ask) > 0 {
		got, err := tryImports(py, ask)
		if err != nil {
			log.Warn().Err(err).Strs("modules", ask).Msg("phone: cannot ask python which modules it has; treating them as missing")
			return ask
		}
		var miss []string
		for _, m := range ask {
			if got[m] {
				importable[m] = true
			} else {
				miss = append(miss, m)
			}
		}
		return miss
	}
	return nil
}

// Need is the header a runnable check sends for a chunk: the submitter's
// python, whether the chunk ships func_src, and the modules it imports
// (Modules).
func Need(python string, funcSrc bool, modules ...string) http.Header {
	h := http.Header{}
	if len(modules) > 0 {
		h.Set(ImportsHeader, strings.Join(modules, ","))
	}
	if python != "" {
		h.Set(PythonHeader, python)
	}
	if funcSrc {
		h.Set(FuncSrcHeader, "1")
	}
	return h
}

// minor trims "3.12.4" to "3.12"; anything without a dot is "".
func minor(v string) string {
	p := strings.SplitN(strings.TrimSpace(v), ".", 3)
	if len(p) < 2 || p[0] == "" || p[1] == "" {
		return ""
	}
	return p[0] + "." + p[1]
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// Termux packages Setup installs. Required: the pool runner and numpy work.
// ML: torch and scipy chunks (about 350 MB more, docs/phone.md); a failure
// there leaves a numpy-only worker. cloudpickle comes from pip (no Termux
// package): by-value functions reference it when they unpickle.
var (
	requiredPkgs = []string{"python", "python-numpy"}
	mlPkgs       = []string{"python-torch", "python-scipy"}
	runCmd       = func(name string, args ...string) ([]byte, error) {
		return exec.Command(name, args...).CombinedOutput()
	}
)

// Setup makes this node pool-only (no nix, crun or firewall on a phone),
// installs Termux python with numpy, torch and scipy, and says what is still
// missing.
func Setup(w io.Writer) error {
	c, err := clustercfg.Load()
	if err != nil {
		return err
	}
	if !c.PoolOnly {
		c.PoolOnly = true
		if err := clustercfg.Save(c); err != nil {
			return err
		}
	}
	term.OK(w, "this phone takes Pool/joblib work only (pool_only in %s)", clustercfg.Path())
	install := func(what string, pkgs ...string) bool {
		fmt.Fprintf(w, "installing %s (pkg install %s), this can take a few minutes...\n", what, strings.Join(pkgs, " "))
		out, err := runCmd("pkg", append([]string{"install", "-y"}, pkgs...)...)
		if err != nil {
			log.Warn().Err(err).Strs("packages", pkgs).Str("output", tail(out)).Msg("phone: pkg install failed")
			term.Warn(w, "pkg install %s failed: %v", strings.Join(pkgs, " "), err)
			return false
		}
		return true
	}
	if !install("python and numpy", requiredPkgs...) {
		term.Action(w, "install python by hand: pkg install %s", strings.Join(requiredPkgs, " "))
	}
	if !install("torch and scipy for ML chunks (~350 MB)", mlPkgs...) {
		term.Action(w, "for ML chunks: pkg install %s (numpy chunks work without it)", strings.Join(mlPkgs, " "))
	}
	pyOnce = sync.Once{} // python may be new since the last probe
	py, v, err := hostPython()
	if err != nil {
		term.Fail(w, "%v", err)
		return nil
	}
	if out, err := runCmd(py, "-m", "pip", "install", "-q", "cloudpickle"); err != nil {
		log.Warn().Err(err).Str("output", tail(out)).Msg("phone: pip install cloudpickle failed")
		term.Action(w, "for by-value functions: %s -m pip install cloudpickle", py)
	}
	have := []string{}
	for _, m := range []string{"numpy", "torch", "scipy", "cloudpickle"} {
		if len(missingModules(py, []string{m})) == 0 {
			have = append(have, m)
		}
	}
	term.OK(w, "python %s at %s with %s", v, py, strings.Join(have, ", "))
	term.Action(w, "keep the worker awake: termux-wake-lock (and keep the phone charging)")
	return nil
}

// tail is the last 400 bytes of a command's output, for a log line.
func tail(b []byte) string {
	if len(b) > 400 {
		b = b[len(b)-400:]
	}
	return strings.TrimSpace(string(b))
}
