package daemonapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/nixstore"
)

// invalidStorePaths returns which of these on-disk store paths nix does not
// hold as valid.
func invalidStorePaths(paths []string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd, cleanup, err := nixstore.CmdContext(ctx, "", append([]string{"nix-store", "--check-validity", "--print-invalid"}, paths...)...)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nix-store --check-validity: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return strings.Fields(string(out)), nil
}

// binaryCache is where substitutable paths are looked up.
var binaryCache = "https://cache.nixos.org"

// substitutable reports which paths binaryCache serves; a variable so tests
// need no network.
var substitutable = narinfoOnCache

// realise fetches paths from the node's substituters; a variable for tests.
var realise = func(ctx context.Context, paths []string) error {
	cmd, cleanup, err := nixstore.CmdContext(ctx, "", append([]string{"nix-store", "-r"}, paths...)...)
	if err != nil {
		return err
	}
	defer cleanup()
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// fetches are this node's binary-cache downloads in flight; an import waits
// for them (awaitFetches), at most substituteWait once its own archive is in.
var fetches struct {
	sync.Mutex
	inflight map[chan struct{}]context.CancelFunc
	offUntil time.Time // no substitution before this (the cache was too slow)
}

// substituteWait bounds how long an import waits for this node's own fetches
// after the sender's archive has arrived. The sender holds every path and is
// usually the faster source (a LAN peer): a cold lab node downloading at
// 130 KB/s held a 3-node broadcast past the sender's 10-minute timeout (M2
// cold sweep). substituteOff is how long substitution then stays off.
// ponytail: fixed bound, not a rate race; compare fetch and link rates if
// slow-uplink internet peers lose by it.
var substituteWait, substituteOff = 30 * time.Second, 30 * time.Minute

// errFetchSlow: the import gave up on this node's fetches; answered as 409 so
// the sender asks again which paths are missing and sends them itself.
var errFetchSlow = errors.New("fetching from the binary cache was slower than the sender")

// awaitFetches waits for the fetches in flight. Past substituteWait it cancels
// them (a running fetch holds nix's lock on its paths, so the sender's copies
// could not be imported), turns substitution off for substituteOff, and
// returns errFetchSlow.
func awaitFetches(ctx context.Context) error {
	fetches.Lock()
	var dones []chan struct{}
	for d := range fetches.inflight {
		dones = append(dones, d)
	}
	fetches.Unlock()
	timer := time.NewTimer(substituteWait)
	defer timer.Stop()
	for i, d := range dones {
		select {
		case <-d:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		fetches.Lock()
		for _, d := range dones[i:] {
			if cancel, ok := fetches.inflight[d]; ok {
				cancel()
			}
		}
		fetches.offUntil = time.Now().Add(substituteOff)
		fetches.Unlock()
		for _, d := range dones[i:] {
			<-d
		}
		log.Warn().Dur("waited", substituteWait).Dur("off_for", substituteOff).Str("cache", binaryCache).
			Msg("binary cache fetch still running; cancelled it and asking the sender for those paths instead")
		return fmt.Errorf("%w: still downloading from %s after %s; send every missing path",
			errFetchSlow, binaryCache, substituteWait)
	}
	return nil
}

// narinfoOnCache asks binaryCache for each path's narinfo, 16 at a time, all
// within 2 s so an offline node answers the missing question promptly.
func narinfoOnCache(paths []string) map[string]bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	found := make(map[string]bool)
	var mu sync.Mutex
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var failures atomic.Int64
	for _, p := range paths {
		hashPart, _, ok := strings.Cut(filepath.Base(p), "-")
		if !ok || !strings.HasPrefix(p, "/nix/store/") {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			req, err := http.NewRequestWithContext(ctx, http.MethodHead, binaryCache+"/"+hashPart+".narinfo", nil)
			if err != nil {
				failures.Add(1)
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				failures.Add(1)
				return
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				mu.Lock()
				found[p] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if n := failures.Load(); n > 0 {
		log.Warn().Int64("paths", n).Str("cache", binaryCache).
			Msg("binary cache lookup failed; those paths are asked of the sender")
	}
	return found
}

// substituteMissing removes from missing the paths binaryCache serves and
// starts fetching them, returning what the sender must still send.
func substituteMissing(missing []string) []string {
	if len(missing) == 0 {
		return missing
	}
	fetches.Lock()
	off := time.Now().Before(fetches.offUntil)
	fetches.Unlock()
	if off {
		log.Info().Int("paths", len(missing)).Str("cache", binaryCache).
			Msg("binary cache was slower than the sender recently; asking the sender for every missing path")
		return missing
	}
	found := substitutable(missing)
	if len(found) == 0 {
		return missing
	}
	rest := make([]string, 0, len(missing)-len(found))
	var fetch []string
	for _, p := range missing {
		if found[p] {
			fetch = append(fetch, p)
		} else {
			rest = append(rest, p)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	done := make(chan struct{})
	fetches.Lock()
	if fetches.inflight == nil {
		fetches.inflight = make(map[chan struct{}]context.CancelFunc)
	}
	fetches.inflight[done] = cancel
	fetches.Unlock()
	go func() {
		defer func() {
			cancel()
			fetches.Lock()
			delete(fetches.inflight, done)
			fetches.Unlock()
			close(done)
		}()
		start := time.Now()
		if err := realise(ctx, fetch); err != nil {
			log.Warn().Err(err).Int("paths", len(fetch)).
				Msg("fetching paths from the binary cache failed; the import will ask for the whole closure")
			return
		}
		log.Info().Int("paths", len(fetch)).Float64("secs", time.Since(start).Seconds()).
			Msg("fetched paths from the binary cache instead of the sender")
	}()
	return rest
}
