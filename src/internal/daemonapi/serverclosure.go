package daemonapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/narpack"
)

// peerHasStore asks a peer whether it can actually run the given closure: the
// store must be materialised (<storePath>/bin/run exists), not merely have its
// NAR cached — a cached but un-imported closure would fail at pool-map time.
// need (phone.Need, may be nil) lets a pool-only phone answer for its host
// python instead of a closure.
func (s *Server) peerHasStore(ph *PeerHealth, storePath string, need http.Header) bool {
	runnable, _ := s.peerStoreState(ph, storePath, need)
	return runnable
}

// peerStoreState is peerHasStore plus whether the peer is importing the
// closure right now (an older peer never says so).
func (s *Server) peerStoreState(ph *PeerHealth, storePath string, need http.Header) (runnable, importing bool) {
	u := fmt.Sprintf("http://%s:%d/v1/store?path=%s&runnable=1", ph.Host, ph.Port, url.QueryEscape(storePath))
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		log.Warn().Err(err).Str("peer", ph.Host).Msg("cannot build the runnable check")
		return false, false
	}
	for k, v := range need {
		req.Header[k] = v
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, false
	}
	defer resp.Body.Close()
	var r struct {
		Cached    bool `json:"cached"`
		Importing bool `json:"importing"`
	}
	if json.NewDecoder(resp.Body).Decode(&r) != nil {
		return false, false
	}
	return r.Cached, r.Importing
}

// healthyPeers returns the peers currently known to be up that take pool
// work (introducer-only peers are left out), and how many peers
// the poller has any opinion about at all. The second number distinguishes
// "everyone is down" from "we have not looked yet".
func (s *Server) healthyPeers() ([]PeerHealth, int) {
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	var targets []PeerHealth
	for _, ph := range s.peerHealths {
		if ph.Status == "healthy" && ph.Host != "" && ph.Port != 0 && ph.takesPoolWork() {
			targets = append(targets, *ph)
		}
	}
	return targets, len(s.peerHealths)
}

// broadcastClosure pushes a cached closure NAR to every healthy peer that does
// not have it yet, so pool spill can fan a single task out to multiple nodes.
// Best-effort and parallel: a peer that fails to import is simply not offered
// spill work (runChunk always keeps the local part, so nothing is lost).
//
// It returns one line per peer that refused the closure, for the submitter.
// Logged here alone, a refusal left the user with "local 80 items" and no
// reason: the shim only sees that no peer holds the closure.
//
// It waits at most broadcastWait. The upload (and so the run) used to wait
// for the slowest peer's import: ~600 s per run on a fresh lab with one slow
// link (M2). A peer still importing then keeps going in the background and
// joins the pool when it is done, because runChunk asks peerFn for the peers
// holding the closure on every chunk.
func (s *Server) broadcastClosure(storePath string) []string {
	targets, known := s.healthyPeers()
	if len(targets) == 0 && known == 0 {
		// The poller has not finished its first cycle. A daemon that has just
		// started still answers uploads, and /v1/nodes serves peer states
		// persisted from a previous run, so both the submitter and the
		// operator can see a healthy cluster while this map is still empty —
		// and the closure then reaches nobody. Poll once, synchronously,
		// rather than silently declining to broadcast.
		s.pollAllNodes()
		targets, known = s.healthyPeers()
	}
	// A pool-only phone runs chunks on its host python and has no Nix to
	// import a closure into.
	// A peer of another architecture (a phone among laptops) cannot run this
	// closure; it runs chunks on its own pinned python instead (phone.CanRun).
	// A full node of another system gets the job's recipe instead and builds
	// its own env (offerRecipes); older ones run chunks on their pinned python.
	var foreign []PeerHealth
	targets = slices.DeleteFunc(targets, func(ph PeerHealth) bool {
		other := ph.Arch != "" && ph.Arch != identity.System()
		if other && ph.Role != heartbeat.RolePoolOnly {
			foreign = append(foreign, ph)
		}
		return ph.Role == heartbeat.RolePoolOnly || other
	})
	s.offerRecipes(storePath, foreign, false)
	if len(targets) == 0 {
		// Usually a timing problem: the health poller has not run since the
		// peers were added, so they are known but not yet marked healthy.
		log.Warn().Int("known_peers", known).
			Msg("no healthy peers to broadcast the closure to; pool work will stay local")
		return nil
	}
	// A CUDA closure is several GB and only a GPU node can use it. The
	// broadcast is a warm-up for pool spill, and the node a GPU job is placed
	// on gets the closure with the upload, so pushing it to CPU-only peers
	// only costs minutes of transfer and their memory (rehearsal: 260 s and
	// three OOM-killed receivers for a 4 s GPU script).
	if paths, err := closurePaths(storePath); err != nil {
		log.Warn().Err(err).Str("store", storePath).
			Msg("cannot list the closure to tell whether it needs a GPU; broadcasting to every peer")
	} else if closureNeedsGPU(paths) {
		var cpuPeers []PeerHealth
		targets, cpuPeers = gpuPeersOnly(targets)
		if len(cpuPeers) > 0 {
			log.Info().Int("skipped", len(cpuPeers)).Str("store", storePath).
				Msg("not broadcasting a GPU closure to peers without a GPU; offering them the job's CPU build instead")
			// They build the CPU variant of the same torch from the recipe
			// (~1.3 GiB, mostly substituted) and join the pool when done.
			s.offerRecipes(storePath, cpuPeers, true)
		}
		if len(targets) == 0 {
			return nil
		}
	}
	// Who gets it is settled before anything is packed. Packing first cost
	// every GPU run 3m24s (V2 95): the whole CUDA closure was exported and
	// then sent to nobody, because no peer had a GPU.
	//
	// A peer this node is already sending the closure to is waited on, not
	// sent it again; a peer importing it from another node is left to finish.
	var lines []string
	var waitOn []PeerHealth
	var waits []*seed
	var fresh []int
	for i := range targets {
		ph := targets[i]
		if sd := s.seedFor(ph, storePath); sd != nil {
			log.Info().Str("peer", ph.addr()).Str("store", storePath).
				Msg("peer is already importing this closure from us; waiting on that send")
			waitOn, waits = append(waitOn, ph), append(waits, sd)
			continue
		}
		runnable, importing := s.peerStoreState(&ph, storePath, nil)
		switch {
		case runnable:
		case importing:
			log.Info().Str("peer", ph.addr()).Str("store", storePath).
				Msg("peer is already importing this closure from another node; not sending it again")
			lines = append(lines, fmt.Sprintf("peer %s is still importing this environment; it joins the pool when the import finishes", ph.addr()))
		default:
			sd, started := s.startSeed(ph, storePath)
			if started {
				fresh = append(fresh, len(waits))
			}
			waitOn, waits = append(waitOn, ph), append(waits, sd)
		}
	}
	if len(waits) == 0 {
		if len(lines) == 0 {
			log.Info().Int("peers", len(targets)).Str("store", storePath).
				Msg("every peer already holds the closure; nothing to broadcast")
		}
		return lines
	}
	log.Info().Int("peers", len(fresh)).Int("joined", len(waits)-len(fresh)).Str("store", storePath).
		Msg("broadcasting closure to peers")

	// The old-format archive, exported from our own store when no upload
	// filled the cache (a job on the machine that built its closure uploads
	// none). Only a peer that cannot take the signed format needs it, so it
	// is made at most once and only when such a peer asks.
	legacy := sync.OnceValues(func() (string, bool) { return localArchive(s.narCache, storePath) })

	send := func(i int) {
		ph := waitOn[i]
		start := time.Now()
		err := s.importStoreOnPeer(&ph, storePath, legacy)
		s.finishSeed(ph, storePath, waits[i], err)
		if err != nil {
			log.Warn().Err(err).Str("peer", ph.addr()).Str("store", storePath).Msg("closure broadcast failed")
			return
		}
		log.Info().Str("peer", ph.addr()).Str("store", storePath).
			Float64("secs", time.Since(start).Round(10*time.Millisecond).Seconds()).Msg("peer took the closure")
	}
	// Serial mode imports one peer at a time (shared-RAM rig); default parallel.
	if s.serialImports.Load() {
		go func() {
			for _, i := range fresh {
				send(i)
			}
		}()
	} else {
		for _, i := range fresh {
			go send(i)
		}
	}

	timer := time.NewTimer(broadcastWait)
	defer timer.Stop()
	expired := false
	for i, sd := range waits {
		if !expired {
			select {
			case <-sd.done:
			case <-timer.C:
				expired = true
			}
		}
		select {
		case <-sd.done:
			if sd.err != nil {
				lines = append(lines, fmt.Sprintf("peer %s did not take this environment, so it gets no pool work: %v", waitOn[i].addr(), sd.err))
			}
		default:
			log.Warn().Str("peer", waitOn[i].addr()).Str("store", storePath).Dur("waited", broadcastWait).
				Msg("peer is still importing the closure; starting without it, it joins the pool when done")
			lines = append(lines, fmt.Sprintf("peer %s is still importing this environment after %s; it joins the pool when the import finishes", waitOn[i].addr(), broadcastWait))
		}
	}
	sort.Strings(lines)
	return lines
}

// broadcastWait bounds how long an upload waits for peers to import its
// closure. On a LAN a CPU closure lands well inside it; a peer behind a slow
// link (800 kbit: minutes) no longer holds every run hostage.
var broadcastWait = 60 * time.Second

// seed is one closure send to one peer, shared by every broadcast that
// wants it while it runs. err is set before done closes.
type seed struct {
	done chan struct{}
	err  error
}

func seedKey(ph PeerHealth, storePath string) string { return ph.addr() + " " + storePath }

func (ph *PeerHealth) addr() string { return fmt.Sprintf("%s:%d", ph.Host, ph.Port) }

// seedFor returns the send of storePath to ph already in flight, or nil.
func (s *Server) seedFor(ph PeerHealth, storePath string) *seed {
	s.seedsMu.Lock()
	defer s.seedsMu.Unlock()
	return s.seeds[seedKey(ph, storePath)]
}

// startSeed registers a send of storePath to ph; started is false when
// another broadcast registered one since seedFor looked.
func (s *Server) startSeed(ph PeerHealth, storePath string) (sd *seed, started bool) {
	s.seedsMu.Lock()
	defer s.seedsMu.Unlock()
	key := seedKey(ph, storePath)
	if sd := s.seeds[key]; sd != nil {
		return sd, false
	}
	if s.seeds == nil {
		s.seeds = make(map[string]*seed)
	}
	sd = &seed{done: make(chan struct{})}
	s.seeds[key] = sd
	return sd, true
}

func (s *Server) finishSeed(ph PeerHealth, storePath string, sd *seed, err error) {
	s.seedsMu.Lock()
	delete(s.seeds, seedKey(ph, storePath))
	s.seedsMu.Unlock()
	sd.err = err
	close(sd.done)
}

// markImporting records that this node is receiving storePath until the
// returned func runs.
func (s *Server) markImporting(storePath string) func() {
	s.importingMu.Lock()
	if s.importing == nil {
		s.importing = make(map[string]int)
	}
	s.importing[storePath]++
	s.importingMu.Unlock()
	return func() {
		s.importingMu.Lock()
		if s.importing[storePath]--; s.importing[storePath] <= 0 {
			delete(s.importing, storePath)
		}
		s.importingMu.Unlock()
	}
}

func (s *Server) isImporting(storePath string) bool {
	s.importingMu.Lock()
	defer s.importingMu.Unlock()
	return s.importing[storePath] > 0
}

// gpuPeersOnly keeps the peers that report an NVIDIA GPU (the closure is CUDA
// torch; an AMD card cannot use it), and returns the others.
func gpuPeersOnly(peers []PeerHealth) (keep, dropped []PeerHealth) {
	for _, ph := range peers {
		if ph.GPU == string(gpu.VendorNVIDIA) {
			keep = append(keep, ph)
		} else {
			dropped = append(dropped, ph)
		}
	}
	return keep, dropped
}

// localArchive is narCache.ensureLocal, as a variable so a test can see
// whether a broadcast packed anything.
var localArchive = (*narCache).ensureLocal

// importStoreOnPeer sends a closure NAR to a peer's /v1/store/import. legacy
// yields the whole old-format archive, made only if this peer needs it.
// A peer whose own binary-cache fetch lost to us (409) has cancelled it and
// stopped substituting, so one more send carries every path it lacks.
func (srv *Server) importStoreOnPeer(ph *PeerHealth, storePath string, legacy func() (string, bool)) error {
	err := srv.sendClosureOnce(ph, storePath, legacy)
	if errors.Is(err, errFetchSlow) {
		log.Warn().Err(err).Str("peer", fmt.Sprintf("%s:%d", ph.Host, ph.Port)).
			Msg("peer's binary cache fetch was too slow; sending it every missing path")
		err = srv.sendClosureOnce(ph, storePath, legacy)
	}
	return err
}

func (srv *Server) sendClosureOnce(ph *PeerHealth, storePath string, legacy func() (string, bool)) error {
	// Send only what this peer lacks. The whole-closure archive is keyed on
	// the top-level store path, so two environments differing by one package
	// share none of it and every byte of python, numpy and the interpreter
	// tree crosses the wire again. Store paths are content-addressed, which
	// makes them exactly the units it is safe to skip.
	//
	// Best-effort throughout: any failure here falls back to the whole
	// archive, which is correct, just larger.
	partial := false
	signed := false
	var narPath string
	if ph.TakesSignedClosures() {
		// The form that keeps signatures, so the peer can require them
		// instead of being configured to stop asking. It carries its own
		// per-path metadata, so there is no "partial" flag to set: the
		// archive says what it contains.
		if pack, cleanup, ok := buildSignedClosure(srv, ph, storePath); ok {
			defer cleanup()
			narPath, signed = pack, true
		}
	}
	if !signed {
		if diff, ok := srv.diffNARFor(ph, storePath); ok {
			defer os.RemoveAll(filepath.Dir(diff))
			narPath, partial = diff, true
		} else if whole, ok := legacy(); ok {
			narPath = whole
		} else {
			// Nothing to send means this peer can never be offered spill
			// work, and the run would look healthy while staying local.
			return fmt.Errorf("no closure archive for %s: it is in neither this node's store nor its cache", storePath)
		}
	}

	f, err := os.Open(narPath)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch := newSendWatch(cancel)
	defer watch.stop()

	pr, pw := io.Pipe()
	mp := multipart.NewWriter(pw)
	go func() {
		defer pw.Close()
		if err := mp.WriteField("store_path", storePath); err != nil {
			return
		}
		if partial {
			// The receiver must import this without caching it as the
			// closure, or it would forward a fragment to the next node.
			if err := mp.WriteField("partial", "1"); err != nil {
				return
			}
		}
		if signed {
			// Said as well as sniffed. The receiver decides by looking at
			// the bytes, because that is what stays right when the two ends
			// are different versions; this field is what makes a mismatch
			// legible in a log rather than mysterious.
			if err := mp.WriteField("format", narpack.FormatName); err != nil {
				return
			}
		}
		w, err := mp.CreateFormFile("nar", "closure.nar")
		if err != nil {
			return
		}
		if _, err := io.Copy(w, watch.reader(f)); err != nil {
			pw.CloseWithError(err)
			return
		}
		mp.Close()
		watch.sent()
	}()

	url := fmt.Sprintf("http://%s:%d/v1/store/import", ph.Host, ph.Port)
	req, err := http.NewRequestWithContext(ctx, "POST", url, pr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mp.FormDataContentType())
	// No total timeout: a big closure on a slow link takes as long as it
	// takes, and a 10-minute cap failed it on every run. The watch cuts a
	// send that stops moving instead.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if why := watch.cut(); why != "" {
			return fmt.Errorf("import %s: %s", url, why)
		}
		return fmt.Errorf("import %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		// The peer answers {"error": ...}; the bare text reads better on the
		// submitter's terminal than escaped JSON.
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			if resp.StatusCode == http.StatusConflict {
				return fmt.Errorf("%w (peer said: %s)", errFetchSlow, e.Error)
			}
			return fmt.Errorf("import returned %d: %s", resp.StatusCode, e.Error)
		}
		return fmt.Errorf("import %s returned %d: %s", url, resp.StatusCode, string(body))
	}
	return nil
}
