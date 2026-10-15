package daemonapi

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/phone"
	"github.com/pipedpeer/pipedpeer/internal/runrec"
)

// runChunk dispatches one chunk, preferring a warm worker and falling back to
// the cold per-request spawn when the warm worker dies or cannot start.
// When healthy peers share the store, items are split across local + peers and
// results are merged in input order. Peers are tried best-first; a peer that
// fails is skipped for the rest of the chunk and the next best takes its part.
// Local is always the last resort, so remote nodes add capacity but never
// remove it (D2: never slower).
//
// With noSplit, items are not re-split: each item is one part routed to its
// own preferred peer (part i prefers peers[i], wrapping past the peer count to
// local), so a pre-partitioned payload (hash-shuffle buckets) lands one item
// per node. Results are still returned in input order.
func (pm *poolManager) runChunk(runPath, storePath string, ch *chunk, submitter string) ([]any, error) {
	ctx := ch.context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Peel off peers before taking the local worker lock so each local submit
	// serialises only its own sub-chunk.
	var peers []string
	pm.mu.Lock()
	if pm.peerFn != nil {
		peers = pm.peerFn(storePath, submitter, phone.Need(ch.python, ch.funcSrc != "", ch.modules...))
	}
	pm.mu.Unlock()
	// --offload: none of the parts below may land on the kept-off machine,
	// and when that is this node, it takes no share of its own.
	peers, offHere := pm.offloadPeers(peers, ch.offload)
	peers = pm.dropForwarder(peers, ch.forwardedBy)
	if offHere && len(peers) == 0 {
		log.Printf("[pool] --offload: no other machine shares this closure; running %d items on this machine", len(ch.items))
	}

	pm.deadMu.Lock()
	pm.deadPeers = make(map[string]bool)
	pm.deadMu.Unlock()

	// One worker per participating node. Local is always included.
	type part struct {
		// c is the chunk with items narrowed to this part's share. Globals and
		// mode flags travel with it so every worker gets the full context.
		c *chunk
		// run is set only for the local part; remote parts POST instead.
		runPath string
		// peers is the candidate list for this part, its own primary peer
		// first (part i prefers peers[i]), the rest as fallback in ranked
		// order. The part tries each in order, skipping dead ones, before
		// going local.
		peers []string
		// steal lets the part start on another of peers than the planned
		// one (a split share; never an affinity-routed item).
		steal bool
	}
	var parts []part
	items := ch.items
	noSplit := ch.noSplit

	switch {
	case noSplit:
		// One part per item. Item i's peer is chosen by its affinity key when
		// one is present (stable hash over the key, so the same chunk always
		// lands on the same node and later cache_keys fetches resolve there),
		// otherwise positionally (peer i, rotating back through the list when
		// there are more items than peers). Items past the peer count fall
		// through to local via the empty runPath branch below.
		//
		// The affinity index must be computed over a STABLE peer order
		// (lexicographic), not the load-ranked order: the load ranking shifts
		// between the parse and the combine requests, so hash % len would land
		// the same key on different nodes and every cache_keys fetch would miss.
		// The load order is still used for fallback ranking after the stable
		// affinity target.
		stablePeers := append([]string{}, peers...)
		sort.Strings(stablePeers)
		for i := range items {
			sub := *ch
			sub.items = items[i : i+1]
			// Narrow the keys to this part's single item: the remote worker
			// resolves cache_keys from its own _CACHE, so broadcasting the full
			// list makes it demand chunks that live on other peers (miss).
			if len(sub.cacheKeys) > 0 {
				sub.cacheKeys = sub.cacheKeys[i : i+1]
			}
			if len(sub.itemKeys) > 0 {
				sub.itemKeys = sub.itemKeys[i : i+1]
			}
			p := part{c: &sub}
			if len(peers) > 0 {
				target := ""
				if key := affinityKey(ch, i); key != "" {
					target = stablePeers[int(affinityHash(key)%uint32(len(stablePeers)))]
				} else {
					target = peers[i%len(peers)]
				}
				for j, peer := range peers {
					if peer == target {
						ordered := append(append([]string{}, peers[j:]...), peers[:j]...)
						p.peers = ordered
						break
					}
				}
				if p.peers == nil {
					p.peers = []string{target}
				}
			} else {
				p.runPath = runPath
			}
			parts = append(parts, p)
		}
	case len(peers) == 0:
		parts = []part{{c: ch, runPath: runPath}}
	default:
		// Split evenly across local + peers. A small chunk stays local (splitting
		// would cost more than it saves); only fan out once there is real work.
		// force (PIPEDPEER_DISTRIBUTE=force) lowers the floor to a pair so a
		// demo-sized chunk still visibly fans out, and a chunk too small even
		// for that goes to a peer whole — rotated so successive one-task
		// dispatches (joblib) spread across the cluster instead of pinning
		// the first peer.
		minSplit := 8
		if ch.force {
			minSplit = 2
		}
		if len(items) < minSplit {
			// Too small to divide. Send it whole to a peer anyway when it came
			// from a shim on this machine: that shim already kept its own half
			// and handed us this one to place elsewhere, so running it here
			// competes with the pool that is mid-race, and the round trip
			// bought nothing. Rotated so successive small dispatches spread
			// instead of pinning the first peer.
			if ch.force || ch.originLocal || offHere {
				j := pm.soonestPeer(peers, int(forceRotate.Add(1)), ch.fnFP, len(items))
				ordered := append(append([]string{}, peers[j:]...), peers[:j]...)
				parts = []part{{c: ch, peers: ordered, steal: true}}
			} else {
				parts = []part{{c: ch, runPath: runPath}}
			}
		} else {
			// Keep a share for this node only when the work came from
			// somewhere else. A chunk posted by a shim on this machine is the
			// half of a race the local pool already declined to run: taking a
			// share of it here would put the daemon's own worker processes on
			// the same cores that pool is saturating, so the machine does the
			// work twice over, oversubscribed, and spilling measures slower
			// than never spilling at all. Every part still falls back to local
			// if its peers fail, so nothing is stranded.
			// Sizes follow measured speed, not headcount. An equal split
			// across a 20-core box and a 4-core box makes the fast one finish
			// and wait, so the chunk takes as long as the slowest
			// participant - which is how adding a machine used to make a job
			// slower. The scheduler equalises finish times instead, and
			// declines any peer that cannot start before the rest are done.
			shares := pm.planShares(ch, peers, !ch.originLocal && !offHere)
			idx := 0
			for _, sh := range shares {
				if sh.Items == 0 || idx >= len(items) {
					continue
				}
				end := min(idx+sh.Items, len(items))
				sub := *ch
				sub.items = items[idx:end]
				idx = end
				if sh.Device.ID == localDeviceID {
					parts = append(parts, part{c: &sub, runPath: runPath})
					continue
				}
				// Preferred peer first, then the rest as fallbacks: a part
				// whose peer dies still runs somewhere rather than failing.
				ordered := []string{sh.Device.ID}
				for _, p := range peers {
					if p != sh.Device.ID {
						ordered = append(ordered, p)
					}
				}
				parts = append(parts, part{c: &sub, peers: ordered, steal: true})
			}
			// The plan can refuse every peer; under --offload there is then no
			// local share to carry the items, so they go to a peer whole.
			if len(parts) == 0 && offHere {
				parts = []part{{c: ch, peers: peers}}
				idx = len(items)
			}
			// Anything rounding left over goes to the first part rather than
			// being dropped; losing items here is silent and corrupts results.
			if idx < len(items) && len(parts) > 0 {
				last := parts[len(parts)-1]
				extra := *last.c
				extra.items = append(append([]json.RawMessage{}, last.c.items...), items[idx:]...)
				parts[len(parts)-1].c = &extra
			}
		}
	}

	remote := 0
	for _, pt := range parts {
		if len(pt.peers) > 0 {
			remote++
		}
	}
	log.Printf("[pool] chunk: %d items -> %d part(s), %d remote, %d local", len(items), len(parts), remote, len(parts)-remote)
	pm.stats.add(func(p *poolStats) { p.PartsToPeers += int64(remote) })

	// Results are stored per part and concatenated in part order after the
	// wait, so the response is always in input order regardless of which part
	// finished first. The shim maps results back by index, so this order is
	// load-bearing.
	partResults := make([][]any, len(parts))
	errs := make([]error, len(parts))
	var wg sync.WaitGroup

	for i, p := range parts {
		wg.Add(1)
		go func(i int, p part) {
			defer wg.Done()
			var r []any
			var e error
			started := time.Now()
			if p.runPath != "" {
				r, e = pm.runLocal(runPath, storePath, p.c)
				if e == nil {
					d := time.Since(started)
					ch.prov.record("local", len(p.c.items), d.Milliseconds(),
						pm.step(pm.selfID, "compute", started, d, i))
					pm.rates.observe(localDeviceID, len(p.c.items), d)
					pm.observeStats(pm.selfID, ch.fnFP, len(p.c.items), d.Seconds(), d.Seconds())
				}
			} else {
				// Walk the ranked peer list; a failure falls through to the
				// next best candidate. If every peer fails, the work is ours.
				//
				// One time budget for the whole walk. A peer that runs out of
				// it is slow, not broken: its copy keeps running, a second copy
				// starts here, and the first to finish wins. Handing the part
				// to the next peer would only start it again from scratch (a
				// one-item svd part tried four peers at 300 s each and hung for
				// over 15 minutes), and dropping the peer's copy threw away
				// minutes of nearly finished work.
				deadline := started.Add(remotePartBudget)
				raced := false
				for pi := 0; pi < len(p.peers); pi++ {
					peer := p.peers[pi]
					if ctx.Err() != nil {
						break
					}
					if pm.peerDead(peer) {
						continue
					}
					left := time.Until(deadline)
					if left <= 0 {
						log.Printf("[pool] part %d: no time left of %s for another peer; running its %d items here", i, remotePartBudget, len(p.c.items))
						break
					}
					// The node's depth gate: parts beyond what it should hold
					// wait here, in the daemon's queue for that node, not on
					// the wire. The wait is a "queue" step of the record.
					// A split part may start on another live candidate
					// instead, when that one has room and its planned node is
					// predicted to stay busy longer (poolsteal.go).
					cands := []string{peer}
					if p.steal {
						for _, q := range p.peers[pi+1:] {
							if !pm.peerDead(q) {
								cands = append(cands, q)
							}
						}
					}
					slots := pm.partSlots(cands, ch.fnFP, len(p.c.items))
					qStart := time.Now()
					k, release, gerr := pm.book.acquireFirst(ctx, slots)
					if gerr != nil {
						e = gerr
						break
					}
					if k > 0 {
						log.Printf("[pool] part %d: %s is still busy (%d parts); starting its %d items on %s instead",
							i, peer, pm.book.held(slots[0].key), len(p.c.items), cands[k])
						peer = cands[k]
						pi-- // should the stand-in fail, the planned peer is next
					}
					key, expected := slots[k].key, slots[k].pred
					var others []string
					if p.steal {
						for _, q := range cands {
							if q != peer {
								others = append(others, q)
							}
						}
					}
					waited := time.Since(qStart)
					sent := time.Now()
					var where string
					var compute float64
					if offHere {
						// No local copy races a slow peer: this machine is
						// kept off. A peer that fails hands on to the next.
						where = "peer:" + peer
						r, compute, e = pm.runRemote(peer, storePath, p.c, 0)
					} else {
						fresh := pm.expectedSec("", ch.fnFP, len(p.c.items))
						r, compute, where, raced, e = pm.runRemoteRacing(i, peer, runPath, storePath, p.c, left-waited, expected, fresh, others...)
					}
					release()
					if e == nil {
						d := time.Since(sent)
						key = pm.key(peer) // the reply may have named the node
						won := where == "peer:"+peer
						switch {
						case where == "local":
							key = pm.selfID // the backup copy here won
						case !won:
							key = pm.key(strings.TrimPrefix(where, "peer:")) // a backup peer's copy won
						}
						steps := []runrec.Step{pm.step(key, "queue", qStart, waited, i)}
						if compute > 0 && won {
							steps = append(steps, pm.step(key, "compute", sent, time.Duration(compute*float64(time.Second)), i))
						}
						ch.prov.record(where, len(p.c.items), time.Since(started).Milliseconds(), steps...)
						if won {
							// Timed end to end, so the round trip and the closure
							// transfer are in the number rather than assumed away.
							// A backup's win is not measured: it started late, so
							// d is not its time. The queue wait is not the peer's
							// time, so it is left out.
							pm.rates.observe(key, len(p.c.items), d)
							if compute <= 0 {
								compute = d.Seconds()
							}
							pm.observeStats(key, ch.fnFP, len(p.c.items), compute, d.Seconds())
						} else {
							pm.observeLost(peer, ch.fnFP, len(p.c.items), d, expected)
						}
						break
					}
					if ctx.Err() != nil {
						pm.observeLost(peer, ch.fnFP, len(p.c.items), time.Since(sent), expected)
						break // the requester left; the peer did nothing wrong
					}
					if raced {
						break // both copies failed; the local one was the fallback
					}
					pm.markPeerDead(peer)
				}
				if e != nil && ctx.Err() == nil && !raced {
					// D2/D3 — a remote node adds capacity, never subtracts.
					if offHere {
						log.Printf("[pool] --offload: every peer failed part %d (%v); running its %d items on this machine", i, e, len(p.c.items))
					}
					r, e = pm.runLocal(runPath, storePath, p.c)
					if e == nil {
						// Deliberately not measured: this ran locally after a
						// peer failed, so the elapsed time includes the failed
						// attempt and would libel this node's own speed.
						ch.prov.record("local", len(p.c.items), time.Since(started).Milliseconds())
					}
				}
			}
			if err := ctx.Err(); err != nil {
				// Cancelled: no local fallback, no "part failed" noise. The
				// request logs the cancel once.
				errs[i] = err
				return
			}
			if e != nil {
				errs[i] = e
				pm.stats.add(func(p *poolStats) { p.PartsFailed++ })
				log.Printf("[pool] part %d failed: %v", i, e)
				return
			}
			partResults[i] = r
		}(i, p)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	results := make([]any, 0, len(items))
	for _, r := range partResults {
		results = append(results, r...)
	}
	return results, nil
}

// affinityKey returns the routing key for noSplit item i: the cache key when
// the request resolves cached chunks, else the item's own affinity key, else
// "" for positional routing.
func affinityKey(ch *chunk, i int) string {
	if len(ch.cacheKeys) > 0 && i < len(ch.cacheKeys) {
		return ch.cacheKeys[i]
	}
	if len(ch.itemKeys) > 0 && i < len(ch.itemKeys) {
		return ch.itemKeys[i]
	}
	return ""
}

// affinityHash is a stable FNV-1a hash (NOT Go's randomized maphash): the
// same key must route to the same peer across daemon restarts and requests.
func affinityHash(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// sinkPeerLast moves the submitting machine's endpoint to the tail of the
// spill order (soft exclusion): an idle orchestrator otherwise outranks busy
// workers and eats the compute --remote was supposed to keep off it. It stays
// eligible so it still helps when no worker can take a chunk.
func sinkPeerLast(peers []string, submitter string) []string {
	if submitter == "" {
		return peers
	}
	for i, p := range peers {
		if p == submitter && i < len(peers)-1 {
			out := append(peers[:i:i], peers[i+1:]...)
			return append(out, submitter)
		}
	}
	return peers
}

// bestPeer returns the top-ranked healthy peer sharing a store ("" when none),
// used to forward requests the local node cannot admit (asymmetric
// orchestrator: 0% heavy compute runs locally when peers exist).
func (pm *poolManager) bestPeer(storePath, submitter, offload string, need http.Header) string {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.peerFn == nil {
		return ""
	}
	ps, _ := pm.offloadPeers(pm.peerFn(storePath, submitter, need), offload)
	for _, p := range ps {
		if p != submitter {
			return p
		}
	}
	if len(ps) > 0 {
		return ps[0] // only the submitter shares this store — D2 fallback
	}
	return ""
}

// peerDead reports whether a peer failed earlier in this chunk.
func (pm *poolManager) peerDead(peer string) bool {
	pm.deadMu.Lock()
	defer pm.deadMu.Unlock()
	return pm.deadPeers[peer]
}

// markPeerDead records that a peer failed so later parts skip it.
func (pm *poolManager) markPeerDead(peer string) {
	pm.deadMu.Lock()
	defer pm.deadMu.Unlock()
	pm.deadPeers[peer] = true
}

// runLocal dispatches a sub-chunk to this node's warm worker (or cold path).
func (pm *poolManager) runLocal(runPath, storePath string, ch *chunk) ([]any, error) {
	ctx := ch.context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pm.stats.add(func(p *poolStats) { p.ItemsExecuted += int64(len(ch.items)) })
	pm.mu.Lock()
	worker, ok := pm.workers[storePath]
	if !ok || worker.dead() {
		if ok {
			log.Printf("[pool] warm worker for store %s was dead; respawning", storePath)
			pm.workers[storePath] = nil
		}
		log.Printf("[pool] spawning warm worker for store %s", storePath)
		w, err := pm.spawn(runPath, storePath)
		if err != nil {
			pm.mu.Unlock()
			log.Printf("[pool] warm worker spawn failed: %v", err)
			// Cold fallback: one-off spawn keeps the node functional even when
			// persistent workers are unavailable.
			return runPoolChunk(runPath, ch)
		}
		worker = w
		pm.workers[storePath] = worker
	}
	pm.mu.Unlock()

	results, err := worker.submit(ch)
	if err != nil && ctx.Err() != nil {
		// The request was cancelled and submit killed the worker mid-chunk:
		// forget it (the next request re-warms) and run nothing in its place.
		pm.dropWorker(storePath, worker)
		return nil, ctx.Err()
	}
	if err != nil {
		// Worker died mid-flight (or could not take the task) — drop it, stop
		// it if it still runs, and fall back to a cold run for this chunk; the
		// next request will re-warm.
		log.Printf("[pool] warm worker submit failed for store %s: %v", storePath, err)
		pm.dropWorker(storePath, worker)
		if worker.killOnce() {
			go func() {
				err := worker.cmd.Wait()
				log.Printf("[pool] warm worker pid %d stopped after a failed submit: %v", worker.cmd.Process.Pid, err)
			}()
		}
		return runPoolChunk(runPath, ch)
	}
	return results, nil
}

// dropWorker forgets a failed warm worker, but only while the map still holds
// that worker: a concurrent request may already have spawned a fresh one for
// the store, and dropping it would leak its process past stopAll.
func (pm *poolManager) dropWorker(storePath string, w *poolWorker) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.workers[storePath] == w {
		delete(pm.workers, storePath)
	}
}
