package daemonapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/schedule"
)

// runRemote forwards a sub-chunk to a peer daemon's /v1/pool/map. The peer must
// have the same closure already (peerFn filters for that). Failures are fatal
// for the chunk: the caller returns them rather than silently dropping work.
// remotePartBudget is how long one part may spend on peers, across every peer
// it tries, before it runs locally. A variable so tests can shorten it.
var remotePartBudget = 5 * time.Minute

// partMem is the required_mem a part sends: the shim's per-node estimate
// (transfer.py _pool_required: 2 x (globals + callable + largest item)) taken
// over this part's own items, never more than the chunk's. Sending the whole
// chunk's figure made a peer refuse a part sized by items it never received.
// Compressed or out-of-band items are not their real size, so those parts keep
// the chunk's figure.
func (ch *chunk) partMem() int64 {
	if ch.requiredMem <= 0 || len(ch.items) == 0 || ch.itemsZlib || ch.oob {
		return ch.requiredMem
	}
	big := 0
	for _, it := range ch.items {
		big = max(big, len(it))
	}
	return min(ch.requiredMem, 2*int64(len(ch.globals)+len(ch.pickledFunc)+big))
}

// runRemote posts a part to one peer and waits up to timeout for its results.
// computeSec is the peer's own account of its compute time (its receipt), 0
// when it sent none.
func (pm *poolManager) runRemote(peer, storePath string, ch *chunk, timeout time.Duration) (results []any, computeSec float64, err error) {
	// no_fanout: the origin splits exactly once; a peer must never re-split and
	// forward again (one-hop tree, results flow straight back to the origin).
	var body []byte
	if ch.frames {
		hdr, _ := json.Marshal(map[string]any{
			"func_src": ch.funcSrc, "func_name": ch.funcName, "func": ch.pickledFunc,
			"starmap": ch.starmap, "items_frames": len(ch.items), "globals": ch.globals != nil,
			"required_mem": ch.partMem(), "no_fanout": true,
			"cache_keys": ch.cacheKeys, "item_keys": ch.itemKeys, "chunk_dir": ch.chunkDir,
			"items_zlib": ch.itemsZlib, "fn_fp": ch.fnFP, "oob": ch.oob,
		})
		body = buildFrames(hdr, ch.globals, ch.items)
	} else {
		payload := map[string]any{"func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName, "extra_b64": ch.extraB64, "items": ch.items, "starmap": ch.starmap, "items_b64": ch.itemsB64, "no_fanout": true, "chunk_dir": ch.chunkDir}
		if len(ch.cacheKeys) > 0 {
			payload["cache_keys"] = ch.cacheKeys
		}
		if len(ch.itemKeys) > 0 {
			payload["item_keys"] = ch.itemKeys
		}
		if ch.fnFP != "" {
			payload["fn_fp"] = ch.fnFP
		}
		body, _ = json.Marshal(payload)
	}
	hdr := http.Header{}
	if ch.frames {
		hdr.Set("Content-Type", framesContentType)
	} else {
		hdr.Set("Content-Type", "application/json")
	}
	hdr.Set("X-Pipedpeer-Store", storePath)
	if ch.python != "" {
		hdr.Set("X-Pipedpeer-Python", ch.python)
	}
	// Tied to the request this chunk serves: a cancel closes the connection,
	// and the peer stops its work when its own request context ends.
	status, _, body, err := pm.postPeer(ch.context(), peer, body, hdr, timeout)
	if err != nil {
		return nil, 0, fmt.Errorf("peer %s: %w", peer, err)
	}
	if status != http.StatusOK {
		return nil, 0, fmt.Errorf("peer %s returned %d", peer, status)
	}
	if ch.frames {
		// Frames response: header line + result frames.
		nl := bytes.IndexByte(body, '\n')
		if nl < 0 {
			return nil, 0, fmt.Errorf("peer %s: bad frames response", peer)
		}
		var out struct {
			ResultsFrames int `json:"results_frames"`
			Receipt       struct {
				Node  string     `json:"node"`
				Parts []provPart `json:"parts"`
			} `json:"receipt"`
		}
		if err := json.Unmarshal(body[:nl], &out); err != nil || out.ResultsFrames <= 0 {
			return nil, 0, fmt.Errorf("peer %s: bad frames response: %v", peer, err)
		}
		pm.sawNode(peer, out.Receipt.Node)
		for _, p := range out.Receipt.Parts {
			computeSec += float64(p.Ms) / 1000
		}
		var frames [][]byte
		rest := body[nl+1:]
		for i := 0; i < out.ResultsFrames; i++ {
			var f []byte
			f, rest, err = readFrame(rest)
			if err != nil {
				return nil, 0, fmt.Errorf("peer %s: %v", peer, err)
			}
			frames = append(frames, f)
		}
		results := make([]any, 0, len(frames))
		for _, f := range frames {
			results = append(results, map[string]string{"pickle": string(f)})
		}
		return results, computeSec, nil
	}
	var out struct {
		Results []map[string]string `json:"results"`
	}
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&out); err != nil {
		return nil, 0, err
	}
	results = make([]any, 0, len(out.Results))
	for _, r := range out.Results {
		if _, err := base64.StdEncoding.DecodeString(r["pickle"]); err != nil {
			return nil, 0, err
		}
		results = append(results, r)
	}
	return results, 0, nil
}

// runRemoteRacing posts part i to peer. Once the part is late (past twice its
// expected time on the peer, at least 1 s) a second copy starts, first to
// finish wins and the other is cancelled (a cancelled local copy kills its
// warm worker, a cancelled remote copy closes the connection and the peer
// stops):
//   - on the peer among others with a free slot whose copy should save the
//     most (backupPeer), re-checked every so often while the part stays late:
//     this node's cores are often busy with the script's own pool;
//   - else here, the tail backup (R8 step 7), when a copy here is expected to
//     save more than its own setup and no other local backup is running on
//     this daemon (at most one at a time);
//   - here once the peer has not answered within budget at all. This is the
//     never-stuck guarantee and ignores the one-backup cap.
//
// expected is the part's predicted seconds on the peer and fresh the predicted
// seconds of a copy here, both 0 when unknown (then only the budget applies).
// where is "peer:<id>" (the copy that won) or "local"; raced reports that the
// local copy was started, so the caller must not run the part locally again.
func (pm *poolManager) runRemoteRacing(i int, peer, runPath, storePath string, ch *chunk, budget time.Duration, expected, fresh float64, others ...string) (results []any, computeSec float64, where string, raced bool, err error) {
	type outcome struct {
		r       []any
		compute float64
		err     error
		where   string
	}
	out := make(chan outcome, 3) // every sender finishes even after a winner returns
	parent := ch.context()
	started := time.Now()
	running := 0
	remote := func(to string, release func()) {
		rctx, rcancel := context.WithCancel(parent)
		rc := *ch
		rc.ctx = rctx
		running++
		go func() {
			defer rcancel()
			if release != nil {
				defer release()
			}
			r, c, e := pm.runRemote(to, storePath, &rc, 0)
			out <- outcome{r, c, e, "peer:" + to}
		}()
	}
	// The losers are cancelled through parent's children: cancel them all
	// when this returns.
	stop, cancelAll := context.WithCancel(parent)
	defer cancelAll()
	parent = stop
	remote(peer, nil)

	hard := time.NewTimer(budget)
	defer hard.Stop()
	// A peer copy is judged from the predicted time on (a free peer is
	// otherwise idle, and its copy wins only by being faster), a local one
	// from twice that: it takes cores this node's own work may be using.
	sec := func(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }
	late := sec(max(2*expected, 1))
	var lateC <-chan time.Time
	arm := func(d time.Duration) {
		t := time.NewTimer(d)
		lateC = t.C
	}
	localChecked, peerCopy, backup := fresh <= 0 || late >= budget, false, false
	if expected > 0 && len(others) > 0 {
		arm(sec(max(expected, 0.5)))
	} else if expected > 0 && !localChecked {
		arm(late)
	}
	var errs []error
wait:
	for {
		select {
		case o := <-out:
			running--
			if o.err == nil {
				if peerCopy {
					log.Printf("[pool] part %d: %s finished first; cancelling the other copy", i, o.where)
				}
				return o.r, o.compute, o.where, false, nil
			}
			if parent.Err() != nil {
				return nil, 0, "", false, parent.Err()
			}
			errs = append(errs, o.err)
			if running == 0 {
				return nil, 0, "", false, errors.Join(errs...)
			}
			log.Printf("[pool] part %d: %s copy failed: %v; waiting for the other", i, o.where, o.err)
		case <-parent.Done():
			return nil, 0, "", false, parent.Err()
		case <-lateC:
			lateC = nil
			elapsed := time.Since(started).Seconds()
			if !peerCopy {
				if alt, release, gain := pm.backupPeer(others, ch.fnFP, len(ch.items), expected, elapsed); alt != "" {
					peerCopy = true
					log.Printf("[pool] part %d: late on %s (expected %.1fs, now %.1fs); a copy on %s should save %.1fs, starting it, first to finish wins",
						i, peer, expected, elapsed, alt, gain)
					remote(alt, release)
					continue
				}
			}
			if !localChecked && !peerCopy && time.Since(started) >= late {
				localChecked = true
				ok, gain := schedule.BackupWorth(expected, elapsed, fresh, 0)
				if ok && pm.backups.CompareAndSwap(0, 1) {
					backup = true
					log.Printf("[pool] part %d: late on %s (expected %.1fs, now %.1fs); a copy here should save %.1fs, starting it, first to finish wins",
						i, peer, expected, elapsed, gain)
					break wait
				}
				if ok {
					log.Printf("[pool] part %d: late on %s but another backup is running; waiting", i, peer)
				}
			}
			switch {
			case len(others) > 0:
				// No peer had room yet; one may free up while this part is late.
				arm(sec(max(expected/2, 0.25)))
			case !localChecked:
				arm(late - time.Since(started))
			}
		case <-hard.C:
			log.Printf("[pool] part %d: peer %s has not finished %d items in %s; starting them here too, first to finish wins", i, peer, len(ch.items), budget.Round(100*time.Millisecond))
			break wait
		}
	}
	if backup {
		defer pm.backups.Store(0)
	}

	lc := *ch
	lc.ctx = parent
	running++
	go func() {
		r, e := pm.runLocal(runPath, storePath, &lc)
		out <- outcome{r, 0, e, "local"}
	}()

	for running > 0 {
		o := <-out
		running--
		if o.err == nil {
			log.Printf("[pool] part %d: %s finished first; cancelling the other copy", i, o.where)
			return o.r, o.compute, o.where, true, nil // the deferred cancel stops the losers
		}
		if parent.Err() != nil {
			return nil, 0, "", true, parent.Err()
		}
		log.Printf("[pool] part %d: %s copy failed: %v; waiting for the other", i, o.where, o.err)
		errs = append(errs, o.err)
	}
	return nil, 0, "", true, errors.Join(errs...)
}
