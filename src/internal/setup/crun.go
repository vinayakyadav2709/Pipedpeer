package setup

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// crunVersion is pinned rather than resolved from "latest": a setup step that
// silently installs a different runtime on different days is not reproducible,
// and the whole design rests on two nodes agreeing about how a job runs.
const crunVersion = "1.29.1"

// installCrunRelease fetches the upstream static crun binary into the user's
// own bin directory.
//
// This exists so that providing a container runtime never needs root. The
// alternatives all do: `apt-get install crun` and `dnf install crun` are
// privileged, and asking a user for sudo to join a cluster is a real barrier
// on machines they do not administer. crun publishes statically linked
// per-architecture binaries, which run on any glibc or musl distribution
// without unpacking anything.
//
// Verified on a stock Ubuntu 26.04 box with no root: downloads, runs, and
// reports its version.
func installCrunRelease() error {
	arch, ok := crunArch()
	if !ok {
		return fmt.Errorf("no published crun build for %s", runtime.GOARCH)
	}
	url := fmt.Sprintf("https://github.com/containers/crun/releases/download/%s/crun-%s-linux-%s",
		crunVersion, crunVersion, arch)

	binDir := userdir.Bin()
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", binDir, err)
	}
	dest := filepath.Join(binDir, "crun")

	fmt.Printf("    → Fetching crun %s into %s (no root needed)...\n", crunVersion, binDir)

	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("downloading crun: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("downloading crun: %s returned %s", url, resp.Status)
	}

	// Write beside the target and rename: a half-downloaded file left at
	// crun's own name would satisfy every later "is crun installed" check
	// while failing every job.
	tmp := dest + ".partial"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return fmt.Errorf("writing crun: %w", err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("writing crun: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("writing crun: %w", err)
	}

	// Run it before accepting it. There is no published checksum for these
	// assets, so the check that matters is that the bytes are an executable
	// that identifies itself as the version asked for - which a truncated
	// download, an HTML error page, or a wrong-architecture build all fail.
	out, err := exec.Command(tmp, "--version").Output()
	if err != nil {
		os.Remove(tmp)
		return fmt.Errorf("downloaded crun does not run: %w", err)
	}
	if !strings.Contains(string(out), crunVersion) {
		os.Remove(tmp)
		return fmt.Errorf("downloaded crun reports %q, want version %s",
			strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0]), crunVersion)
	}
	if err := os.Rename(tmp, dest); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("installing crun: %w", err)
	}

	// setup's own later checks, and anything it runs, look crun up on PATH.
	// The directory is on it for login shells everywhere, but this process
	// was not necessarily started from one.
	if !onPath(binDir) {
		os.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
		fmt.Printf("    → Note: add %s to PATH to use crun outside pipedpeer\n", binDir)
	}
	return nil
}

func crunArch() (string, bool) {
	switch runtime.GOARCH {
	case "amd64":
		return "amd64", true
	case "arm64":
		return "arm64", true
	case "ppc64le":
		return "ppc64le", true
	case "riscv64":
		return "riscv64", true
	case "s390x":
		return "s390x", true
	}
	return "", false
}

func onPath(dir string) bool {
	for _, p := range filepath.SplitList(os.Getenv("PATH")) {
		if p == dir {
			return true
		}
	}
	return false
}
