package daemonapi

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/schedule"
)

// A part is bound to a node when it starts, not when the chunk is split. The
// split is made from estimates, and against a peer nobody has measured yet
// (a phone, a fresh cluster) the estimate is a guess: a 10x-slow peer got an
// equal share, its parts queued behind its own late ones, and every chunk's
// reply waited for them, so the first run took as long as one node alone.
// Two moves let a run learn inside itself:
//   - a part waiting for its node's depth gate starts on another candidate
//     that has a free slot once that is predicted to finish it sooner than
//     waiting (acquireFirst), and
//   - a running part past its prediction gets a copy on the fastest peer with
//     a free slot when that copy should save time; first to finish wins
//     (backupPeer, used by runRemoteRacing).
// Either way the slow peer's lost part is counted as at least its elapsed
// time (observeLost), so the next split already shrinks its share.

// slot is one candidate node for a part: its gate key, depth and the part's
// predicted seconds there.
type slot struct {
	key   string
	depth int
	pred  float64
}

// acquireFirst waits until the part can start on one of cands and books it
// there. cands[0] is the planned node and is taken whenever it has room;
// another candidate only when it has room now and starting there is predicted
// to finish sooner than waiting for cands[0] (stealable). Returns the index
// taken.
func (b *flightBook) acquireFirst(ctx context.Context, cands []slot) (int, func(), error) {
	for {
		b.mu.Lock()
		if b.wake == nil {
			b.wake = make(chan struct{})
		}
		now := time.Now()
		pick := -1
		if b.free(cands[0]) {
			pick = 0
		} else if len(cands) > 1 {
			wait := b.earliestFree(cands[0].key, now)
			for i, c := range cands[1:] {
				if b.free(c) && c.pred < wait+cands[0].pred && (pick < 0 || c.pred < cands[pick].pred) {
					pick = i + 1
				}
			}
		}
		if pick >= 0 {
			release := b.bookLocked(cands[pick], now)
			b.mu.Unlock()
			return pick, release, nil
		}
		wake := b.wake
		b.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
			// No part left, but the planned node's parts aged past their
			// prediction: re-judge whether another candidate is now worth it.
		}
	}
}

// tryAcquire books c only if it has room now.
func (b *flightBook) tryAcquire(c slot) (func(), bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.wake == nil {
		b.wake = make(chan struct{})
	}
	if !b.free(c) {
		return nil, false
	}
	return b.bookLocked(c, time.Now()), true
}

// free reports whether c's node holds fewer than its depth. Caller holds b.mu.
func (b *flightBook) free(c slot) bool {
	return len(b.node(c.key).busy) < max(c.depth, 1)
}

// bookLocked records a part on c's node; the returned release frees it and
// wakes every waiter. Caller holds b.mu.
func (b *flightBook) bookLocked(c slot, now time.Time) func() {
	n := b.node(c.key)
	b.next++
	id := b.next
	n.busy[id] = flight{start: now, pred: c.pred}
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			delete(n.busy, id)
			close(b.wake)
			b.wake = make(chan struct{})
			b.mu.Unlock()
		})
	}
}

// earliestFree predicts the seconds until key frees a slot: the soonest of
// its parts to end. A part past its prediction is assumed to need as long
// again as it was expected to take, or as long as it has overrun, whichever
// is more (schedule.BackupWorth's reading). Caller holds b.mu.
func (b *flightBook) earliestFree(key string, now time.Time) float64 {
	n := b.nodes[key]
	if n == nil || len(n.busy) == 0 {
		return 0
	}
	soon := math.Inf(1)
	for _, f := range n.busy {
		el := now.Sub(f.start).Seconds()
		rem := f.pred - el
		if el > f.pred {
			rem = max(el-f.pred, f.pred)
		}
		soon = min(soon, rem)
	}
	return soon
}

// partSlots is the gate view of a part of n items of fn on each candidate.
func (pm *poolManager) partSlots(cands []string, fn string, n int) []slot {
	out := make([]slot, len(cands))
	for i, c := range cands {
		out[i] = slot{key: pm.key(c), depth: pm.depth(c, fn), pred: pm.expectedSec(c, fn, n)}
	}
	return out
}

// backupPeer picks the peer among others with a free slot whose copy of a
// late part (expected seconds on its node, elapsed so far) is predicted to
// save the most, and books it. "" when none is worth it.
func (pm *poolManager) backupPeer(others []string, fn string, n int, expected, elapsed float64) (string, func(), float64) {
	best, bestGain := "", 0.0
	var bestSlot slot
	for i, c := range pm.partSlots(others, fn, n) {
		if pm.peerDead(others[i]) || pm.book.held(c.key) >= max(c.depth, 1) {
			continue
		}
		if ok, gain := schedule.BackupWorth(expected, elapsed, c.pred, 0); ok && gain > bestGain {
			best, bestGain, bestSlot = others[i], gain, c
		}
	}
	if best == "" {
		return "", nil, 0
	}
	release, ok := pm.book.tryAcquire(bestSlot)
	if !ok {
		log.Printf("[pool] backup peer %s filled up before its copy started; not copying", best)
		return "", nil, 0
	}
	return best, release, bestGain
}

// soonestPeer is the index of the peer predicted to finish n items of fn
// first, counting the work it already holds. Ties (every peer unmeasured and
// idle) go round-robin from rotate, so successive one-task dispatches still
// spread; plain rotation alone gave a 10x-slow peer every other small chunk.
func (pm *poolManager) soonestPeer(peers []string, rotate int, fn string, n int) int {
	now := time.Now()
	best, bestSec := 0, math.Inf(1)
	for k := range peers {
		j := (rotate + k) % len(peers)
		sec := pm.book.backlog(pm.key(peers[j]), now) + pm.expectedSec(peers[j], fn, n)
		if sec < bestSec*0.9 { // within 10% counts as a tie
			best, bestSec = j, sec
		}
	}
	return best
}
