package daemonapi

// Pool speed stats kept across runs (C22; U18 calls from pool.go). Keyed by
// machine key and function fingerprint, never by address: an internet peer's
// address is a forwarder port that changes. Design and constants: research R8
// "Recommended design" steps 1-3.
//
// DECIDED: hand-rolled. Dask's and Ray's runtime stats live inside their
// schedulers and are not usable as libraries (R8 "Reuse comparison").

import (
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

const (
	poolStatsAlpha    = 0.22           // 2/(N+1), N = 8: today's obsWindow
	poolStatsBeta     = 0.25           // deviation gain (RFC 6298 RTTVAR)
	poolStatsHalfLife = 24 * time.Hour // laptop load regimes change daily (R8 step 3)
	poolStatsMaxKeys  = 1000
	poolStatsOverhead = 0.05 // assumed dispatch floor until o is measured
	// A run is the stretch of parts with no gap longer than this; the next
	// part after a longer gap starts a new run and folds the last one in.
	poolStatsRunGap  = 5 * time.Minute
	poolStatsSaveLag = 2 * time.Second
	poolStatsAnyFn   = "*" // per-machine entry holding the speed index
)

// poolHW is what invalidates an entry: a different CPU makes every measured
// rate meaningless.
type poolHW struct {
	Model string  `json:"cpu_model"`
	Cores int     `json:"cpu_cores"`
	MHz   float64 `json:"cpu_mhz"`
}

func (h poolHW) score() float64 { return float64(h.Cores) * h.MHz }

// same ignores clock jitter: some kernels report the current, not the
// rated, frequency.
func (h poolHW) same(o poolHW) bool {
	if h.Model != o.Model || h.Cores != o.Cores {
		return false
	}
	if h.MHz == 0 || o.MHz == 0 {
		return h.MHz == o.MHz
	}
	return math.Abs(h.MHz-o.MHz) <= 0.1*math.Max(h.MHz, o.MHz)
}

type poolStatEntry struct {
	R     float64 `json:"r"`     // items/s, persisted across runs
	Dev   float64 `json:"dev_r"` // mean deviation of r
	O     float64 `json:"o"`     // warm per-part overhead, s
	N     int     `json:"n"`
	TLast int64   `json:"t_last"` // unix s
	HW    poolHW  `json:"hw"`
	// The run in progress; folded into R/O when the next run starts.
	RunR float64 `json:"run_r,omitempty"`
	RunO float64 `json:"run_o,omitempty"`
	RunN int     `json:"run_n,omitempty"`
	// S is the machine's speed index (only on the poolStatsAnyFn entry):
	// measured rate over what cores x MHz predicts from other machines.
	S float64 `json:"s,omitempty"`
}

func (e *poolStatEntry) live(now time.Time) bool {
	return e.RunN > 0 && now.Unix()-e.TLast <= int64(poolStatsRunGap/time.Second)
}

// fold ends the last run: one odd run moves the persisted value halfway (Dask's 0.5).
func (e *poolStatEntry) fold() {
	if e.RunN == 0 {
		return
	}
	if e.R == 0 {
		e.R, e.O = e.RunR, e.RunO
	} else {
		e.R = 0.5*e.R + 0.5*e.RunR
		e.O = 0.5*e.O + 0.5*e.RunO
	}
	e.RunR, e.RunO, e.RunN = 0, 0, 0
}

// rate is the best current estimate: the live run's, else the persisted one.
func (e *poolStatEntry) rate(now time.Time) float64 {
	if e.live(now) {
		return e.RunR
	}
	return e.R
}

func poolStatKey(machine, fn string) string { return machine + "\x00" + fn }

type poolStatsStore struct {
	path    string
	mu      sync.Mutex
	entries map[string]*poolStatEntry
	timer   *time.Timer
}

// One store per file: tests run several Servers in one process.
var poolStatsStores sync.Map // path -> *poolStatsStore

func openPoolStats(path string) *poolStatsStore {
	if v, ok := poolStatsStores.Load(path); ok {
		return v.(*poolStatsStore)
	}
	st := &poolStatsStore{path: path, entries: map[string]*poolStatEntry{}}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		log.Warn().Err(err).Str("path", path).Msg("pool stats unreadable; starting without priors")
	default:
		if err := json.Unmarshal(b, &st.entries); err != nil {
			log.Warn().Err(err).Str("path", path).Msg("pool stats corrupt; starting without priors")
			st.entries = map[string]*poolStatEntry{}
		}
	}
	v, _ := poolStatsStores.LoadOrStore(path, st)
	return v.(*poolStatsStore)
}

func (st *poolStatsStore) save() error {
	st.mu.Lock()
	b, err := json.Marshal(st.entries)
	st.mu.Unlock()
	if err != nil {
		return err
	}
	dir := filepath.Dir(st.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".poolstats-*")
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return err
	}
	return os.Rename(f.Name(), st.path)
}

// saveSoon batches the writes of one map's parts into one; st.mu held.
func (st *poolStatsStore) saveSoon() {
	if st.timer != nil {
		return
	}
	st.timer = time.AfterFunc(poolStatsSaveLag, func() {
		st.mu.Lock()
		st.timer = nil
		st.mu.Unlock()
		if err := st.save(); err != nil {
			log.Warn().Err(err).Str("path", st.path).Msg("could not save pool stats")
		}
	})
}

// get returns the entry, dropping it when the machine's hardware changed; st.mu held.
func (st *poolStatsStore) get(machine, fn string, cur poolHW, curOK bool) *poolStatEntry {
	k := poolStatKey(machine, fn)
	e := st.entries[k]
	if e != nil && curOK && !e.HW.same(cur) {
		log.Info().Str("machine", machine).Str("fn", fn).Str("was", e.HW.Model).Str("now", cur.Model).
			Msg("pool stats: hardware changed, dropping measured rate")
		delete(st.entries, k)
		return nil
	}
	return e
}

// put inserts e, evicting the least recently used entry over the bound; st.mu held.
func (st *poolStatsStore) put(machine, fn string, e *poolStatEntry) {
	k := poolStatKey(machine, fn)
	if _, ok := st.entries[k]; !ok && len(st.entries) >= poolStatsMaxKeys {
		// ponytail: O(n) scan per insert at 1000 keys; a heap if the bound grows.
		var oldK string
		var oldT int64 = math.MaxInt64
		for kk, ee := range st.entries {
			if ee.TLast < oldT {
				oldK, oldT = kk, ee.TLast
			}
		}
		delete(st.entries, oldK)
	}
	st.entries[k] = e
}

// hwPrior is the cold-start rate for fn on machine: the same function's
// rate on another machine, scaled by cores x MHz and both speed indexes.
// st.mu held.
func (st *poolStatsStore) hwPrior(machine, fn string, cur poolHW, now time.Time, hwOf func(string) (poolHW, bool)) (float64, bool) {
	if cur.score() <= 0 {
		return 0, false
	}
	var best *poolStatEntry
	var bestM string
	for k, e := range st.entries {
		m, f := splitPoolStatKey(k)
		if f != fn || m == machine || e.HW.score() <= 0 || e.rate(now) <= 0 {
			continue
		}
		if h, ok := hwOf(m); ok && !e.HW.same(h) {
			continue // that machine changed; its entry goes on its next use
		}
		if best == nil || e.TLast > best.TLast {
			best, bestM = e, m
		}
	}
	if best == nil {
		return 0, false
	}
	return best.rate(now) * cur.score() / best.HW.score() * st.speed(machine) / st.speed(bestM), true
}

func (st *poolStatsStore) speed(machine string) float64 {
	if e := st.entries[poolStatKey(machine, poolStatsAnyFn)]; e != nil && e.S > 0 {
		return e.S
	}
	return 1
}

func splitPoolStatKey(k string) (machine, fn string) {
	for i := 0; i < len(k); i++ {
		if k[i] == 0 {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}

func (st *poolStatsStore) prior(machine, fn string, now time.Time, hwOf func(string) (poolHW, bool)) (float64, float64, bool) {
	cur, curOK := hwOf(machine)
	st.mu.Lock()
	defer st.mu.Unlock()
	cold, coldOK := st.hwPrior(machine, fn, cur, now, hwOf)
	e := st.get(machine, fn, cur, curOK)
	if e == nil {
		if !coldOK {
			return 0, 0, false
		}
		return cold, poolStatsOverhead, true
	}
	if e.live(now) {
		return e.RunR, e.RunO, true
	}
	e.fold()
	o := e.O
	if o <= 0 {
		o = poolStatsOverhead
	}
	if !coldOK {
		// Nothing to decay toward: the old rate beats no prior at all.
		return e.R, o, true
	}
	age := now.Sub(time.Unix(e.TLast, 0))
	w := math.Exp2(-age.Hours() / poolStatsHalfLife.Hours())
	return w*e.R + (1-w)*cold, o, true
}

func (st *poolStatsStore) observe(machine, fn string, items int, computeSec, elapsedSec float64, now time.Time, hwOf func(string) (poolHW, bool)) {
	if items <= 0 || computeSec <= 0 {
		return
	}
	x := float64(items) / computeSec
	ov := math.Max(0, elapsedSec-computeSec)
	cur, curOK := hwOf(machine)
	st.mu.Lock()
	defer st.mu.Unlock()
	cold, coldOK := st.hwPrior(machine, fn, cur, now, hwOf)
	e := st.get(machine, fn, cur, curOK)
	if e == nil {
		e = &poolStatEntry{}
		st.put(machine, fn, e)
	}
	if !e.live(now) {
		e.fold()
		// Seed the run with the prior as one pseudo-observation.
		e.RunR, e.RunO = e.R, e.O
		if e.RunR == 0 && coldOK {
			e.RunR = cold
		}
	}
	seeded := e.RunR > 0
	if e.Dev == 0 {
		e.Dev = x / 2 // RFC 6298 initial RTTVAR
	}
	switch {
	case !seeded, e.RunN == 0 && (x > 2*e.RunR || x < e.RunR/2):
		// No prior, or the run's first part is 2x off it (Dask's reset rule).
		e.RunR, e.RunO = x, ov
	case e.RunN >= 3 && math.Abs(x-e.RunR) > 3*e.Dev:
		// Level shift (throttling, a co-tenant): jump, don't crawl.
		e.Dev = (1-poolStatsBeta)*e.Dev + poolStatsBeta*math.Abs(x-e.RunR)
		e.RunR = x
		e.RunO = (1-poolStatsAlpha)*e.RunO + poolStatsAlpha*ov
	default:
		// Running mean while few samples, then the EWMA (pseudo-obs + RunN+1 real).
		a := math.Max(poolStatsAlpha, 1/float64(e.RunN+2))
		e.Dev = (1-poolStatsBeta)*e.Dev + poolStatsBeta*math.Abs(x-e.RunR)
		e.RunR = (1-a)*e.RunR + a*x
		e.RunO = (1-a)*e.RunO + a*ov
	}
	e.RunN++
	e.N++
	e.TLast = now.Unix()
	if curOK {
		e.HW = cur
	}
	if coldOK && cold > 0 {
		// Speed index: what this machine does beyond its cores x MHz.
		ratio := x / (cold / st.speed(machine))
		se := st.get(machine, poolStatsAnyFn, cur, curOK)
		if se == nil {
			se = &poolStatEntry{S: 1}
			st.put(machine, poolStatsAnyFn, se)
		}
		se.S = (1-poolStatsAlpha)*se.S + poolStatsAlpha*ratio
		se.TLast = now.Unix()
		if curOK {
			se.HW = cur
		}
	}
	st.saveSoon()
}

func (s *Server) poolStats() *poolStatsStore {
	return openPoolStats(filepath.Join(userdir.Data(), "poolstats.json"))
}

// poolHWOf reads a machine's CPU from its heartbeat capabilities: this
// daemon's own, else the node store's copy. machine is a node id or a
// heartbeat machine key.
func (s *Server) poolHWOf(machine string) (poolHW, bool) {
	s.selfMu.RLock()
	caps := s.selfCaps
	s.selfMu.RUnlock()
	if machine == s.nodeID || (caps != nil && caps["machine"] == machine) {
		if h, ok := capsHW(caps); ok {
			return h, true
		}
	}
	if s.store == nil {
		return poolHW{}, false
	}
	nodes, err := s.store.ListAll()
	if err != nil {
		log.Warn().Err(err).Str("machine", machine).Msg("pool stats: node store unreadable; no hardware check")
		return poolHW{}, false
	}
	for _, n := range nodes {
		if n.CapsJSON == "" {
			continue
		}
		var c map[string]string
		if err := json.Unmarshal([]byte(n.CapsJSON), &c); err != nil {
			log.Warn().Err(err).Str("peer", n.NodeID).Msg("pool stats: stored capabilities unreadable")
			continue
		}
		if n.NodeID == machine || c["machine"] == machine {
			return capsHW(c)
		}
	}
	return poolHW{}, false
}

func capsHW(c map[string]string) (poolHW, bool) {
	cores, err := strconv.Atoi(c["cpu_cores"])
	if err != nil || cores <= 0 {
		return poolHW{}, false
	}
	// Absent on some VMs: 0 disables the cores x MHz prior.
	var mhz float64
	if v := c["cpu_mhz"]; v != "" {
		if mhz, err = strconv.ParseFloat(v, 64); err != nil {
			log.Warn().Err(err).Str("cpu_mhz", v).Msg("pool stats: unreadable cpu_mhz; no cores x MHz prior")
			mhz = 0
		}
	}
	return poolHW{Model: c["cpu_model"], Cores: cores, MHz: mhz}, true
}

// poolPrior returns what earlier parts and runs measured for fn on machine,
// or a cores x MHz estimate from another machine that ran fn.
func (s *Server) poolPrior(machine, fn string) (itemsPerSec, overheadSec float64, ok bool) {
	return s.poolStats().prior(machine, fn, time.Now(), s.poolHWOf)
}

// poolObserve records one completed warm part (the first part per node and
// function is cold and must not be passed in).
func (s *Server) poolObserve(machine, fn string, items int, computeSec, elapsedSec float64) {
	s.poolStats().observe(machine, fn, items, computeSec, elapsedSec, time.Now(), s.poolHWOf)
}
