package daemonapi

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

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

func (pm *poolManager) planShares(items []json.RawMessage, peers []string, includeLocal bool) []schedule.Share {
	devices := pm.rates.devices(peers, pm.peerCores, pm.localRate(), includeLocal)
	plan := schedule.Compute(schedule.Options{Items: len(items)}, devices)

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
