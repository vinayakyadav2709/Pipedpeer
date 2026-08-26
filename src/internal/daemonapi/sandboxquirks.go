package daemonapi

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"github.com/rs/zerolog/log"
)

// quirks records which parts of a job sandbox this machine will actually
// allow. Both default to true and are only ever turned off by a refusal
// observed from crun.
type quirks struct {
	// freshProc: the job gets its own procfs, so it cannot see the node's
	// other processes. Refused when the daemon is itself running unprivileged
	// inside a container, because the kernel will not mount a new procfs
	// instance while the visible one carries masked submounts.
	freshProc bool
	// hostname: the job gets its own UTS namespace and a hostname. Refused in
	// the same nested case, where sethostname is not permitted.
	hostname bool
}

// sandboxQuirks settles, once, what this machine permits.
//
// Determined by asking crun rather than by inferring it: "am I inside a
// container" has no reliable answer, and the cost of guessing wrong is every
// job on the node failing before it starts. Each refusal is matched by the
// message crun prints for it, so one unsupported feature disables that
// feature and not the others - an earlier version treated any failure as "no
// private /proc" and got the right sandbox for the wrong reason, which would
// have silently dropped isolation the moment the reason changed.
var sandboxQuirks = sync.OnceValue(probeQuirks)

func probeQuirks() quirks {
	q := quirks{freshProc: true, hostname: true}

	crun, err := exec.LookPath("crun")
	if err != nil {
		return q // no crun: the question never comes up
	}
	trueBin := ""
	for _, p := range []string{"/bin/true", "/usr/bin/true"} {
		if _, err := os.Stat(p); err == nil {
			trueBin = p
			break
		}
	}
	if trueBin == "" {
		return q
	}

	// One pass per feature that can be refused, plus one to confirm.
	for i := 0; i < 3; i++ {
		out, err := runProbe(crun, trueBin, q)
		if err == nil {
			return q
		}
		msg := string(out)
		switch {
		case q.freshProc && strings.Contains(msg, "mount `proc`"):
			q.freshProc = false
			log.Warn().Msg("cannot mount a private /proc for jobs; binding the host's instead. " +
				"Jobs stay sandboxed but can see this machine's process list")
		case q.hostname && strings.Contains(msg, "sethostname"):
			q.hostname = false
			log.Warn().Msg("cannot give jobs their own hostname; sharing this machine's instead")
		default:
			// Something else is wrong. Leaving the remaining features on is
			// deliberate: turning them off would not fix this and would
			// weaken every sandbox on the node for no reason.
			log.Warn().Str("crun", strings.TrimSpace(msg)).
				Msg("sandbox probe failed for an unrecognised reason; using full isolation anyway")
			return q
		}
	}
	return q
}

func runProbe(crun, trueBin string, q quirks) ([]byte, error) {
	bundle, err := userdir.Scratch("sandboxprobe-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(bundle)
	rootfs := filepath.Join(bundle, "rootfs")
	// dev included because crun populates it whether or not the spec asks:
	// without the directory the probe dies with "openat2 `dev`: No such file
	// or directory" before it reaches the question being asked.
	for _, d := range []string{"proc", "dev", "usr", "bin", "lib", "lib64"} {
		_ = os.MkdirAll(filepath.Join(rootfs, d), 0o755)
	}

	mounts := []ociMount{
		procMountFor(q),
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs",
			Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
	}
	for _, d := range []string{"/usr", "/bin", "/lib", "/lib64"} {
		if _, err := os.Stat(d); err == nil {
			mounts = append(mounts, ociMount{
				Destination: d, Type: "bind", Source: d, Options: []string{"rbind", "ro"},
			})
		}
	}

	// The namespace set must match what a job bundle asks for, or the probe
	// answers a different question than the one being asked. crun adds the
	// user namespace itself when running rootless.
	cfg := ociConfig{
		OciVersion: "1.0.2",
		Process:    ociProcess{Args: []string{trueBin}, Cwd: "/", Env: []string{"PATH=/usr/bin:/bin"}},
		Root:       ociRoot{Path: "rootfs"},
		Mounts:     mounts,
		Linux:      &ociLinux{Namespaces: namespacesFor(q)},
	}
	if q.hostname {
		cfg.Hostname = "pipedpeer-probe"
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), data, 0o644); err != nil {
		return nil, err
	}

	id := "pp-probe-" + filepath.Base(bundle)
	out, runErr := exec.Command(crun, "run", "--bundle", bundle, id).CombinedOutput()
	_ = exec.Command(crun, "delete", "-f", id).Run()
	return out, runErr
}

// namespacesFor lists the namespaces a job sandbox should ask for.
func namespacesFor(q quirks) []ociNamespace {
	ns := []ociNamespace{{Type: "pid"}, {Type: "ipc"}, {Type: "mount"}}
	if q.hostname {
		ns = append(ns, ociNamespace{Type: "uts"})
	}
	return ns
}

func procMountFor(q quirks) ociMount {
	if q.freshProc {
		return ociMount{Destination: "/proc", Type: "proc", Source: "proc"}
	}
	return ociMount{Destination: "/proc", Type: "bind", Source: "/proc", Options: []string{"rbind"}}
}

// hostnameFor is the sandbox hostname, empty when this machine will not let a
// job have its own.
func hostnameFor(q quirks) string {
	if q.hostname {
		return "pipedpeer"
	}
	return ""
}
