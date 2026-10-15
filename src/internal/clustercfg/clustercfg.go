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
	"sync"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"github.com/rs/zerolog/log"
)

// DefaultPort is the introducer's port when an address does not name one.
//
// The demo, the docs and every example use it, so typing a bare host is the
// common case and requiring ":4244" would be noise.
const DefaultPort = 4244

// maxIntroducers is how many introducer addresses are remembered.
const maxIntroducers = 8

// Config is everything this machine remembers about its cluster, in one file.
type Config struct {
	Token       string   `json:"token"`
	Introducers []string `json:"introducers"`
	NoJobs      bool     `json:"no_jobs,omitempty"`
	PoolOnly    bool     `json:"pool_only,omitempty"`
	// PublicAddress is this machine's address as peers off its network see
	// it (host or host:port; a 1:1-NAT cloud VM). Invites list it first.
	PublicAddress string `json:"public_address,omitempty"`
}

// disk is Config plus the field older builds wrote, read only to migrate it.
type disk struct {
	Config
	Rendezvous string `json:"rendezvous,omitempty"`
}

var mu sync.Mutex

// Path is the config file, for messages to the operator.
func Path() string {
	return filepath.Join(userdir.Data(), "cluster.json")
}

func legacyTokenPath() string {
	return filepath.Join(userdir.Data(), "auth_token")
}

// Load reads the config. A missing file is a zero Config, not an error.
//
// Two older layouts are migrated on the way: {"rendezvous": addr} becomes one
// introducer, and a separate auth_token file becomes Token. The result is
// saved, and auth_token is deleted only after that save succeeded, so a
// failed migration loses nothing.
func Load() (Config, error) {
	mu.Lock()
	defer mu.Unlock()
	return load()
}

func load() (Config, error) {
	var d disk
	b, err := os.ReadFile(Path())
	if err != nil && !os.IsNotExist(err) {
		return Config{}, err
	}
	if err == nil {
		if err := json.Unmarshal(b, &d); err != nil {
			return Config{}, fmt.Errorf("%s: %w", Path(), err)
		}
	}
	migrated := false
	if r := strings.TrimSpace(d.Rendezvous); r != "" {
		d.Introducers = mergeAddrs(d.Introducers, r)
		migrated = true
	}
	legacy := false
	if d.Token == "" {
		if tb, err := os.ReadFile(legacyTokenPath()); err == nil {
			if t := strings.TrimSpace(string(tb)); t != "" {
				d.Token, legacy, migrated = t, true, true
			}
		}
	}
	if migrated {
		log.Warn().Str("file", Path()).Msg("clustercfg: migrating the old rendezvous/auth_token files into cluster.json")
		if err := save(d.Config); err != nil {
			log.Warn().Err(err).Msg("clustercfg: could not save the migrated config; the old files are kept")
		} else if legacy {
			if err := os.Remove(legacyTokenPath()); err != nil && !os.IsNotExist(err) {
				log.Warn().Err(err).Msg("clustercfg: could not remove the old auth_token file")
			}
		}
	}
	return d.Config, nil
}

// Save writes the config atomically with mode 0600.
//
// Written via a temporary file and renamed: a half-written config read by the
// next start would look like a machine that belongs to no cluster, which is
// indistinguishable from one that never joined.
func Save(c Config) error {
	mu.Lock()
	defer mu.Unlock()
	return save(c)
}

func save(c Config) error {
	if err := os.MkdirAll(userdir.Data(), 0o700); err != nil {
		return err
	}
	if c.Introducers == nil {
		c.Introducers = []string{}
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := Path() + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, Path())
}

// mergeAddrs appends add to have, dropping duplicates (the newest position
// wins) and keeping the maxIntroducers most recent.
func mergeAddrs(have []string, add ...string) []string {
	var out []string
	for _, a := range append(append([]string{}, have...), add...) {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		for i, o := range out {
			if o == a {
				out = append(out[:i], out[i+1:]...)
				break
			}
		}
		out = append(out, a)
	}
	if len(out) > maxIntroducers {
		out = out[len(out)-maxIntroducers:]
	}
	return out
}

// RememberIntroducers adds introducer addresses (host:port) to the config.
func RememberIntroducers(addrs ...string) error {
	mu.Lock()
	defer mu.Unlock()
	c, err := load()
	if err != nil {
		return err
	}
	c.Introducers = mergeAddrs(c.Introducers, addrs...)
	return save(c)
}

// Rendezvous is the most recently remembered introducer, or "".
func Rendezvous() string {
	c, err := Load()
	if err != nil || len(c.Introducers) == 0 {
		if err != nil {
			log.Warn().Err(err).Msg("clustercfg: cannot read the config; treating this machine as in no cluster")
		}
		return ""
	}
	return c.Introducers[len(c.Introducers)-1]
}

// SetRendezvous remembers one introducer.
func SetRendezvous(addr string) error { return RememberIntroducers(addr) }

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

// Effective is the introducer to use, and where the answer came from: a flag
// beats what was saved.
func Effective(flag string) (addr, source string) {
	if s := strings.TrimSpace(flag); s != "" {
		return s, "flag"
	}
	if s := Rendezvous(); s != "" {
		return s, "saved"
	}
	return "", ""
}
