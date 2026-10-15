package daemonapi

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/runrec"
	"github.com/pipedpeer/pipedpeer/internal/schedule"
)

// rateModel learns how fast each peer actually is, from the parts already
// dispatched to it.
//
// Nothing else in the system knows this. A peer's advertised core count says
// nothing about how loaded it is, how fast the link to it is, or how much of
// a chunk's cost is per-item work rather than a fixed round trip - and those
// are exactly the numbers that decide whether sending it work helps. The
// dispatch loop already times every part, so the measurements are free.
//
// Two observations of different sizes separate the fixed cost from the
// per-item one: elapsed = setup + items/rate is a straight line in items, and
// its intercept is the round trip. That distinction matters because it is the
// difference between "this peer is slow, give it less" and "this peer cannot
// start before we finish, do not use it".
type rateModel struct {
	mu     sync.Mutex
	byPeer map[string]*peerObs
}

// obsWindow is small on purpose. A peer's speed changes when something else
// starts running on it, and a long memory would keep sending work to a
// machine that was fast ten minutes ago.
const obsWindow = 8

type obs struct {
	items float64
	sec   float64
}

type peerObs struct {
	ring []obs
	next int
}

func newRateModel() *rateModel {
	return &rateModel{byPeer: map[string]*peerObs{}}
}

// observe records that a peer took d to handle items.
func (m *rateModel) observe(peer string, items int, d time.Duration) {
	if items <= 0 || d <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.byPeer[peer]
	if p == nil {
		p = &peerObs{ring: make([]obs, 0, obsWindow)}
		m.byPeer[peer] = p
	}
	o := obs{items: float64(items), sec: d.Seconds()}
	if len(p.ring) < obsWindow {
		p.ring = append(p.ring, o)
		return
	}
	p.ring[p.next] = o
	p.next = (p.next + 1) % obsWindow
}

// fit returns the measured per-item rate and fixed cost for a peer, and
// whether there is enough evidence to use them.
func (m *rateModel) fit(peer string) (rate, setup float64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.byPeer[peer]
	if p == nil || len(p.ring) == 0 {
		return 0, 0, false
	}
	return fitLine(p.ring)
}

// fitLine solves sec = setup + items/rate by least squares.
//
// Falls back to a rate-only estimate when the observations do not separate
// the two - all the same size, or a fit that comes out non-physical. A
// negative intercept is not evidence of a negative round trip, it is noise,
// and clamping it to zero is better than either believing it or throwing the
// measurement away.
func fitLine(os []obs) (rate, setup float64, ok bool) {
	if len(os) == 0 {
		return 0, 0, false
	}
	var n, sx, sy, sxx, sxy float64
	for _, o := range os {
		n++
		sx += o.items
		sy += o.sec
		sxx += o.items * o.items
		sxy += o.items * o.sec
	}
	mean := func() (float64, float64, bool) {
		// items per second over the whole sample, treating the round trip as
		// part of the per-item cost. Less precise, never wrong by much.
		if sy <= 0 {
			return 0, 0, false
		}
		return sx / sy, 0, true
	}
	if n < 2 {
		return mean()
	}
	denom := n*sxx - sx*sx
	if denom == 0 { // every observation the same size: no spread to fit
		return mean()
	}
	slope := (n*sxy - sx*sy) / denom
	intercept := (sy - slope*sx) / n
	if slope <= 0 {
		return mean()
	}
	if intercept < 0 {
		intercept = 0
	}
	return 1 / slope, intercept, true
}

// devices builds the scheduler's view of a set of peers, plus this node when
// it is taking a share. Peers with no measurement yet are given a prior from
// their advertised core count, so a cluster does not have to be warmed up
// before it can be used - the prior is replaced by measurement as soon as the
// first part comes back.
func (m *rateModel) devices(peers []string, cores func(string) int, localRate float64, includeLocal bool) []schedule.Device {
	var out []schedule.Device
	if includeLocal && localRate > 0 {
		out = append(out, schedule.Device{
			ID: "local", Node: "local", Kind: schedule.CPU,
			Rate: localRate, MemBytes: 1 << 62,
		})
	}
	for _, p := range peers {
		rate, setup, ok := m.fit(p)
		if !ok {
			// No history. Assume the peer is worth roughly its core count
			// relative to ours, and a round trip that is small but not free,
			// so a first dispatch is neither refused nor oversized.
			//
			// An unknown core count means "assume it is like us", not "assume
			// it is a single core". The pessimistic reading is
			// self-fulfilling: it sizes the peer's share below the floor, the
			// share is dropped, nothing is ever sent there, and so nothing is
			// ever measured. A peer that is in fact slower is corrected after
			// its first part comes back.
			c := cores(p)
			if c <= 0 {
				c = localCores()
			}
			rate = localRate * float64(c) / float64(max(1, localCores()))
			if rate <= 0 {
				rate = float64(c)
			}
			setup = 0.05
		}
		out = append(out, schedule.Device{
			ID: p, Node: p, Kind: schedule.CPU,
			Rate: rate, SetupSec: setup, MemBytes: 1 << 62,
		})
	}
	return out
}

// localCores is this node's core count, the denominator for the core-count
// prior above.
func localCores() int { return runtime.NumCPU() }

// localDeviceID marks the scheduler's entry for this node.
const localDeviceID = "local"

// planShares decides how a chunk's items are divided between this node and
// its peers.
//
// There is deliberately no minimum share here. An absolute floor looks
// prudent and is not: on a 16-item chunk it dropped a peer owed 7 items,
// which the measurements said would nearly halve the time. The cost of asking
// a peer at all is already priced, as that peer's fitted setup term, so the
// model refuses the shares that are not worth a round trip and keeps the ones
// that are.

func (pm *poolManager) planShares(ch *chunk, peers []string, includeLocal bool) []schedule.Share {
	devices := pm.devicesFor(peers, ch.fnFP, includeLocal)
	plan := schedule.Compute(schedule.Options{Items: len(ch.items)}, devices)

	for _, r := range plan.Rejected {
		// Loud: a peer silently missing from a plan looks exactly like a peer
		// the daemon never knew about, and telling those two apart after the
		// fact is what cost this project months once already.
		log.Printf("[pool] not using %s: %s", r.Device.ID, r.Reason)
	}
	if len(plan.Shares) > 0 {
		log.Printf("[pool] plan: %s (predicted %.2fs, best single device %.2fs)",
			describeShares(plan.Shares), plan.Makespan, plan.Alone)
	}
	return plan.Shares
}

func describeShares(shares []schedule.Share) string {
	var b strings.Builder
	for i, s := range shares {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%d", s.Device.ID, s.Items)
	}
	return b.String()
}

// localRate is this node's measured items-per-second, falling back to a
// core-count-shaped guess until something has actually run here. The absolute
// value matters less than its ratio to the peers', since only relative speed
// decides the split.
func (pm *poolManager) localRate() float64 {
	if rate, _, ok := pm.rates.fit(localDeviceID); ok {
		return rate
	}
	return float64(localCores())
}

// devicesFor is rateModel.devices for a map of function fn, with what the
// plain model lacks (R5 step 2, R8 step 3): each peer is measured under its
// node id, never its address; an unmeasured peer starts from what earlier
// runs saw for fn on that node (poolPrior) before falling back to the
// core-count guess; and the work a node already holds is added to its setup,
// so the split equalises real finish times across chunks in flight instead of
// treating every node as idle.
func (pm *poolManager) devicesFor(peers []string, fn string, includeLocal bool) []schedule.Device {
	devs := pm.rates.devices(nil, pm.peerCores, pm.localRate(), includeLocal)
	now := time.Now()
	for _, p := range peers {
		rate, setup := pm.peerRate(p, fn)
		devs = append(devs, schedule.Device{
			ID: p, Node: p, Kind: schedule.CPU,
			Rate: rate, SetupSec: setup + pm.book.backlog(pm.key(p), now), MemBytes: 1 << 62,
		})
	}
	return devs
}

// peerRate is a peer's items/s and fixed seconds per request for fn: this
// run's measurements, else an earlier run's (poolPrior), else the core-count
// prior rateModel.devices uses.
func (pm *poolManager) peerRate(peer, fn string) (rate, setup float64) {
	key := pm.key(peer)
	if r, o, ok := pm.rates.fit(key); ok {
		return r, o
	}
	if pm.priorFn != nil && fn != "" {
		if r, o, ok := pm.priorFn(key, fn); ok && r > 0 {
			return r, o
		}
	}
	d := pm.rates.devices([]string{peer}, pm.peerCores, pm.localRate(), false)
	return d[0].Rate, d[0].SetupSec
}

// expectedSec predicts how long n items of fn take on peer ("" = this node),
// fixed cost included; 0 when nothing is known.
func (pm *poolManager) expectedSec(peer, fn string, n int) float64 {
	if peer == "" {
		if r, o, ok := pm.rates.fit(localDeviceID); ok && r > 0 {
			return o + float64(n)/r
		}
		if pm.priorFn != nil && fn != "" && pm.selfID != "" {
			if r, o, ok := pm.priorFn(pm.selfID, fn); ok && r > 0 {
				return o + float64(n)/r
			}
		}
		return 0
	}
	r, o := pm.peerRate(peer, fn)
	if r <= 0 {
		return 0
	}
	return o + float64(n)/r
}

// depth is how many parts peer should hold at once: one running plus enough
// to cover its lead time (its fixed cost per request and, when measured, a
// part's payload over the link), against parts sized so that cost is under
// 10% (schedule.PartSec). 2 on any LAN or Wi-Fi link, 3 at most.
func (pm *poolManager) depth(peer, fn string) int {
	_, setup := pm.peerRate(peer, fn)
	return schedule.Depth(setup, schedule.PartSec(setup, 0))
}

// observeStats feeds one completed part to the cross-run stats, skipping each
// (node, fn)'s first part since this daemon started: it carries closure fetch
// and worker spawn, not the function's speed.
func (pm *poolManager) observeStats(node, fn string, items int, computeSec, elapsedSec float64) {
	if pm.observeFn == nil || fn == "" || node == "" || items <= 0 {
		return
	}
	if pm.firstPart(node, fn) {
		return
	}
	pm.observeFn(node, fn, items, computeSec, elapsedSec)
}

// step is one daemon-side run-record step of the node that ran the part (a
// peer's node id for a remote part, this node's for a local one), for the
// part's receipt: "where it ran" in every view is read from Machine.
func (pm *poolManager) step(node, name string, start time.Time, d time.Duration, part int) runrec.Step {
	return runrec.Step{Machine: node, Node: node, Chunk: part, Name: name,
		StartNS: start.UnixNano(), DurNS: d.Nanoseconds()}
}

// pacing answers how the shim should pace a map on storePath (the additive
// /v1/pool/stats fields): the sum of the peers' depths, the target length of
// one part and the peers' cores. With no peer the
// shim keeps one chunk in flight.
func (pm *poolManager) pacing(storePath, submitter string) map[string]int64 {
	pm.mu.Lock()
	var peers []string
	if pm.peerFn != nil {
		peers = pm.peerFn(storePath, submitter, nil)
	}
	pm.mu.Unlock()
	var inflight, cores int64
	part := 0.25
	for _, p := range peers {
		_, setup := pm.peerRate(p, "")
		inflight += int64(schedule.Depth(setup, schedule.PartSec(setup, 0)))
		part = max(part, schedule.PartSec(setup, 0))
		c := pm.peerCores(p)
		if c <= 0 {
			c = localCores()
		}
		cores += int64(c)
	}
	return map[string]int64{
		"peers":    int64(len(peers)),
		"inflight": max(inflight, 1),
		"part_ms":  int64(part * 1000),
		"cores":    cores,
	}
}

// flightBook is what every node holds right now: each part running on it and
// how long it was predicted to take. The split reads it as backlog (a busy
// node starts late); the gate reads it as depth.
type flightBook struct {
	mu    sync.Mutex
	next  int64
	nodes map[string]*nodeFlights
}

type nodeFlights struct {
	busy map[int64]flight
	wake chan struct{} // closed and replaced whenever a part leaves
}

type flight struct {
	start time.Time
	pred  float64
}

func (b *flightBook) node(key string) *nodeFlights {
	if b.nodes == nil {
		b.nodes = map[string]*nodeFlights{}
	}
	n := b.nodes[key]
	if n == nil {
		n = &nodeFlights{busy: map[int64]flight{}, wake: make(chan struct{})}
		b.nodes[key] = n
	}
	return n
}

// backlog is the predicted seconds of work key still holds.
func (b *flightBook) backlog(key string, now time.Time) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.nodes[key]
	if n == nil {
		return 0
	}
	var pred, el []float64
	for _, f := range n.busy {
		pred = append(pred, f.pred)
		el = append(el, now.Sub(f.start).Seconds())
	}
	return schedule.Backlog(pred, el)
}

// held is how many parts key holds right now.
func (b *flightBook) held(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n := b.nodes[key]; n != nil {
		return len(n.busy)
	}
	return 0
}

// acquire waits until key holds fewer than depth parts (or ctx ends), then
// books one predicted to take pred seconds. release frees it; calling it
// again is harmless.
func (b *flightBook) acquire(ctx context.Context, key string, depth int, pred float64) (release func(), err error) {
	for {
		b.mu.Lock()
		n := b.node(key)
		if len(n.busy) < max(depth, 1) {
			b.next++
			id := b.next
			n.busy[id] = flight{start: time.Now(), pred: pred}
			b.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					b.mu.Lock()
					delete(n.busy, id)
					close(n.wake)
					n.wake = make(chan struct{})
					b.mu.Unlock()
				})
			}, nil
		}
		wake := n.wake
		b.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
