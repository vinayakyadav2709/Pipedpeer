package peercache

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixtrust"
)

// Seams for tests.
var (
	multiUser         = nixsign.MultiUser
	substituterListed = nixtrust.SubstituterListed
	clusterKeys       = func() string {
		body, err := os.ReadFile(keysFile())
		if err != nil {
			return ""
		}
		return strings.Join(strings.Fields(string(body)), " ")
	}
	// probeBase is where the daemon's cache answers on this machine.
	probeBase = func(port int) string { return "http://127.0.0.1:" + strconv.Itoa(port) }
)

// NixArgs returns the `--option` arguments that make one nix build command
// ask this machine's daemon cache for what cache.nixos.org lacks. It returns
// nil, and logs why, when there is no port, no daemon answering, or (on a
// multi-user nix-daemon) the trusted-substituter line is not yet in place.
// The user's own nix settings are never touched.
func NixArgs(port int) []string {
	if port == 0 {
		return nil
	}
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, Route)
	multi := multiUser()
	if multi {
		if url != nixtrust.SubstituterURL {
			log.Warn().Int("port", port).Msg("peer cache off: a multi-user nix only trusts the cache on the default daemon port; run `pipedpeer setup` after switching it")
			return nil
		}
		if !substituterListed() {
			log.Warn().Msg("peer cache off: this machine's nix-daemon does not list the cache yet; run `pipedpeer setup`")
			return nil
		}
	}
	if err := probe(port); err != nil {
		return nil
	}
	// connect-timeout: with cache.nixos.org unreachable, nix otherwise waits
	// out the OS connect timeout per attempt before it reaches the peer
	// (lab: 79.8 s without, 29.2 s with 5 s).
	args := []string{"--option", "connect-timeout", "5", "--option", "extra-substituters", url}
	if !multi {
		if keys := clusterKeys(); keys != "" {
			args = append(args, "--option", "extra-trusted-public-keys", keys)
		}
		args = append(args, "--option", "narinfo-cache-negative-ttl", "0")
	}
	return append(args, "--option", "fallback", "true")
}

var probeClient = &http.Client{Timeout: time.Second}

// probe checks that the daemon's cache answers; a stopped daemon is normal.
func probe(port int) error {
	resp, err := probeClient.Get(probeBase(port) + Route + "/nix-cache-info")
	if err != nil {
		log.Info().Err(err).Msg("peer cache off: daemon not answering")
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Info().Int("status", resp.StatusCode).Msg("peer cache off: daemon has no cache route")
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	head, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	if !strings.HasPrefix(string(head), "StoreDir: /nix/store") {
		log.Warn().Int("port", port).Msg("peer cache off: the answer on this port is not a nix cache")
		return fmt.Errorf("not a nix cache")
	}
	return nil
}

// RunWithFallback runs a nix build with the peer cache options (argv) and, if
// that fails while the cache was on, once more without them (plain). A
// multi-user nix-daemon ignores a client's `fallback`, so a peer that vanishes
// between narinfo and NAR would otherwise fail a build that a local build can
// do. The first error is logged in full (run should put nix's output tail in
// it); the second is returned. A cancelled ctx is not retried.
func RunWithFallback(ctx context.Context, argv, plain []string, run func(argv []string) error) error {
	err := run(argv)
	if err == nil || slices.Equal(argv, plain) || ctx.Err() != nil {
		return err
	}
	log.Warn().Err(err).Msg("nix build with the peer cache failed; retrying without it")
	return run(plain)
}
