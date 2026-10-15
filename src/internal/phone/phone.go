// Package phone makes an Android phone in Termux a pool-only worker.
//
// A phone has no Nix and no crun (no user namespaces for app processes), and
// an arm64 phone never holds the laptops' x86 store paths anyway. So it takes
// only Pool/joblib chunks, run with Termux's own python (`pkg install python
// python-numpy`), and refuses whole jobs through the pool-only role in
// cluster.json. The pool runner scripts use the standard library only, so a
// chunk runs on any python whose minor version matches the submitter's
// (pickle protocol and bytecode for func_src are minor-version bound).
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
// is a pool-only node whose host python has the submitter's minor version.
func CanRun(store string, h http.Header) bool {
	if store != "" && exists(filepath.Join(store, "bin", "run")) {
		return true
	}
	if !poolOnly() {
		return false
	}
	want := minor(h.Get(PythonHeader))
	if want == "" {
		return false
	}
	_, have, err := hostPython()
	if err != nil {
		log.Warn().Err(err).Msg("phone: pool-only node answers not runnable")
		return false
	}
	return minor(have) == want
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

// Setup makes this node pool-only (no nix, crun or firewall on a phone) and
// says what is still missing.
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
	py, v, err := hostPython()
	if err != nil {
		term.Action(w, "install python: pkg install python python-numpy")
		return nil
	}
	term.OK(w, "python %s at %s", v, py)
	if err := exec.Command(py, "-c", "import numpy").Run(); err != nil {
		log.Warn().Err(err).Msg("phone: numpy import failed")
		term.Action(w, "install numpy: pkg install python-numpy")
	}
	term.Action(w, "keep the worker awake: termux-wake-lock (and keep the phone charging)")
	return nil
}
