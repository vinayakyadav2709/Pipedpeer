// Package clustercfg remembers which cluster this machine belongs to.
//
// Joining a cluster over the internet needs one piece of information the
// machine cannot work out for itself: the address of the introducer that
// holds the address book. It used to be passed on every start, through a flag
// or an environment variable, which meant a daemon restarted by hand - or by
// a reboot, or by anything that did not know to repeat the flag - came back
// alone and silently stopped taking work. The machine had joined a cluster;
// the fact just did not survive.
//
// So it is written down once and read every time. A flag still wins when it
// is given, because overriding what was saved is exactly what a flag is for.
package clustercfg

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// DefaultPort is the introducer's port when an address does not name one.
//
// The demo, the docs and every example use it, so typing a bare host is the
// common case and requiring ":38445" would be noise.
const DefaultPort = 38445

// EnvVar is the environment variable the daemon reads.
const EnvVar = "PIPEDPEER_RENDEZVOUS"

type config struct {
	Rendezvous string `json:"rendezvous,omitempty"`
}

func path() string {
	return filepath.Join(userdir.Data(), "cluster.json")
}

func load() config {
	var c config
	b, err := os.ReadFile(path())
	if err != nil {
		return c
	}
	_ = json.Unmarshal(b, &c)
	return c
}

// Rendezvous is the introducer this machine joins through, or "".
func Rendezvous() string { return strings.TrimSpace(load().Rendezvous) }

// SetRendezvous records the introducer, so later starts need no flag.
//
// Written via a temporary file and renamed: a half-written config read by the
// next start would look like a machine that belongs to no cluster, which is
// indistinguishable from one that never joined.
func SetRendezvous(addr string) error {
	addr = strings.TrimSpace(addr)
	dir := userdir.Data()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	c := load()
	c.Rendezvous = addr
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := path() + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path())
}

// Normalize turns what a person types into host:port.
//
// A bare host gets the default port, because "the address of the introducer"
// is how people think of it and the port is an implementation detail they
// were told once. An address that already names a port is left alone.
func Normalize(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", fmt.Errorf("no address given")
	}
	// Strip a scheme if someone pasted a URL.
	for _, p := range []string{"http://", "https://", "udp://"} {
		addr = strings.TrimPrefix(addr, p)
	}
	addr = strings.TrimSuffix(addr, "/")

	// A bracketed IPv6 literal, with or without a port.
	if strings.HasPrefix(addr, "[") {
		if i := strings.LastIndex(addr, "]"); i >= 0 {
			if rest := addr[i+1:]; strings.HasPrefix(rest, ":") {
				if _, err := strconv.Atoi(rest[1:]); err != nil {
					return "", fmt.Errorf("%q does not end in a port number", addr)
				}
				return addr, nil
			}
			return addr + ":" + strconv.Itoa(DefaultPort), nil
		}
		return "", fmt.Errorf("%q has no closing bracket", addr)
	}

	host, port, found := strings.Cut(addr, ":")
	if !found {
		return addr + ":" + strconv.Itoa(DefaultPort), nil
	}
	// More than one colon and no brackets: a bare IPv6 address, which cannot
	// carry a port without them.
	if strings.Contains(port, ":") {
		return "[" + addr + "]:" + strconv.Itoa(DefaultPort), nil
	}
	if host == "" {
		return "", fmt.Errorf("%q names a port but no host", addr)
	}
	if _, err := strconv.Atoi(port); err != nil {
		return "", fmt.Errorf("%q does not end in a port number", addr)
	}
	return addr, nil
}

// Effective is the introducer to use, and where the answer came from.
//
// Precedence is flag, then environment, then what was saved. A flag is how
// somebody overrides a saved cluster for one run, so it has to win; the saved
// value exists so that nothing has to be typed at all.
func Effective(flag string) (addr, source string) {
	if s := strings.TrimSpace(flag); s != "" {
		return s, "flag"
	}
	if s := strings.TrimSpace(os.Getenv(EnvVar)); s != "" {
		return s, "environment"
	}
	if s := Rendezvous(); s != "" {
		return s, "saved"
	}
	return "", ""
}
