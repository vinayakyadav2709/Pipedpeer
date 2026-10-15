package daemonapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/phone"
)

// handlePoolMap executes a pickled function over a batch of items using the
// local closure, returning per-item results. It is the worker side of the
// sitecustomize cluster pool (see nixgen/shim.go). Each request is one chunk.
//
// It runs in a subprocess of <storePath>/bin/run so it executes in exactly the
// environment the user's script runs in — no SDK, no shared state. Requests to
// a store that already has a warm worker reuse it instead of re-spawning.
// handlePoolStats reports what pool work this node has actually done, so a
// test or a demo can check where the work went without shelling in to grep a
// log file.
//
// With ?store=<path> it also answers how the shim should pace a map on that
// closure (additive, all integers): inflight = the sum over peers of each
// node's depth (the shim keeps that many chunks in flight, not a global 4),
// part_ms = the target length of one part, cores = the peers' cores (a chunk
// must fill them). link_bytes_per_sec (always, when measured) is the sum of
// the fresh per-peer link speeds (U27's cost models divide it per peer).
func (s *Server) handlePoolStats(w http.ResponseWriter, r *http.Request) {
	s.bindPool()
	out := map[string]int64{}
	for k, v := range s.pool.stats.snapshot() {
		out[k] = v
	}
	if bw, ok := clusterLinkSpeed(); ok {
		out["link_bytes_per_sec"] = int64(bw)
	}
	if store := r.URL.Query().Get("store"); store != "" {
		for k, v := range s.pool.pacing(store, s.poolSubmitter(r)) {
			out[k] = v
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// maxPoolBody bounds a single /v1/pool/map request. The body is read whole
// into memory before anything looks at it, so without a ceiling a peer -
// or a bug in chunk sizing - can push this daemon into the OOM killer with
// one request, taking every other job on the node with it. The shim's own
// chunks are bounded by the 40%-of-free-RAM rule long before this; the
// ceiling is here for everything that is not the shim behaving.
const maxPoolBody = 2 << 30 // 2 GiB

// poolBodyLimit is the ceiling for this daemon, which is not the same number
// on every node. A fixed 2 GiB is most of a small worker's entire allowance -
// the container beside its host in the lab is capped at exactly that - so one
// request sized to the constant would consume everything the daemon is
// allowed before a single item had been looked at. Half the budget, so there
// is room left to do something with what arrives.
func (s *Server) poolBodyLimit() int64 {
	limit := int64(maxPoolBody)
	if b := cgroups.SelfBudget(); b.Total > 0 && b.Total/2 < limit {
		limit = b.Total / 2
	}
	return limit
}

// poolCancelled reports whether the requester of a pool request went away: the
// shim's job was killed, or the origin dropped the forward it sent here. The
// workers and forwards tied to the request have already been stopped through
// its context by then; this logs it once and counts it.
func (s *Server) poolCancelled(r *http.Request, items int, started time.Time) bool {
	if r.Context().Err() == nil {
		return false
	}
	log.Printf("[pool] cancelled: %d items from %s after %.1fs; requester went away, local workers stopped, peer forwards dropped",
		items, r.RemoteAddr, time.Since(started).Seconds())
	s.pool.stats.add(func(p *poolStats) { p.ChunksCancelled++ })
	return true
}

func (s *Server) handlePoolMap(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	limit := s.poolBodyLimit()
	rawBody, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{
			"error": fmt.Sprintf("pool request body exceeds %d bytes", limit),
		})
		return
	}
	defer releaseHeapAfterWork()
	s.bindPool()
	// A peer may have compressed the body for the link (C21).
	if rawBody, err = decodePoolBody(r.Header.Get("Content-Encoding"), rawBody); err != nil {
		code, v := poolDecodeStatus(err)
		writeJSON(w, code, v)
		return
	}
	var req poolRequest
	// Frames bodies: the header is the first line and the frames follow it.
	// Legacy bodies are compact JSON with no literal newline, so the first
	// newline split is a safe discriminator.
	var items []json.RawMessage
	var globals []byte
	frames := false
	if nl := bytes.IndexByte(rawBody, '\n'); nl > 0 {
		var probe struct {
			ItemsFrames int  `json:"items_frames"`
			Globals     bool `json:"globals"`
		}
		if err := json.Unmarshal(rawBody[:nl], &probe); err == nil && probe.ItemsFrames > 0 {
			frames = true
			if err := json.Unmarshal(rawBody[:nl], &req); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request: " + err.Error()})
				return
			}
			var err error
			globals, items, err = parseFrames(rawBody[nl+1:], probe.Globals, probe.ItemsFrames)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad frames: " + err.Error()})
				return
			}
		}
	}
	if !frames {
		if err := json.NewDecoder(bytes.NewReader(rawBody)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request: " + err.Error()})
			return
		}
		if err := json.Unmarshal(req.Items, &items); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad items: " + err.Error()})
			return
		}
	}

	// The store path / run wrapper comes from the request environment. The
	// submitter embeds it so we need no job context.
	storePath := r.Header.Get("X-Pipedpeer-Store")
	// Where the requesting job was submitted from ("host:port"); sunk to the
	// tail of spill order so an idle orchestrator never outranks workers.
	submitter := s.poolSubmitter(r)
	offload := r.Header.Get("X-Pipedpeer-Offload")
	python := r.Header.Get("X-Pipedpeer-Python")
	if storePath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-Pipedpeer-Store header"})
		return
	}
	// <store>/bin/run, or a pool-only phone's host python; a by-value
	// function from another python minor is refused (Decisions: func_src rule).
	runPath, err := phone.PoolRunPath(storePath)
	modules := chunkModules(&req, globals, items, frames)
	if err == nil && !phone.CanRun(storePath, phone.Need(python, req.FuncSrc != "", modules...)) {
		err = fmt.Errorf("this node's python cannot run the chunk (python %q, modules %v; see the daemon log)", python, modules)
	}
	if err != nil {
		log.Printf("[pool] refusing pool/map for %s: %v", storePath, err)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cannot run this chunk here: " + err.Error()})
		return
	}

	if req.FuncSrc != "" && req.FuncName == "" {
		req.FuncName = "run" // mirror the runner's default: func_src without func_name
	}
	log.Printf("[pool] received pool/map from %s: items=%d b64=%v cache_keys=%d required_mem=%d forwarded=%q",
		r.RemoteAddr, len(items), req.ItemsB64, len(req.CacheKeys), req.RequiredMemBytes,
		r.Header.Get("X-Pipedpeer-Forwarded"))
	fromLocalShim := isLoopback(r.RemoteAddr) && r.Header.Get("X-Pipedpeer-Forwarded") == ""
	s.pool.stats.add(func(p *poolStats) {
		p.ChunksReceived++
		if fromLocalShim {
			p.ChunksLocal++
		} else {
			p.ChunksPeer++
		}
	})

	// Memory bounding for horizontal scaling on constrained nodes. The safe
	// working set is 40% of free RAM (max_safe_chunk_size); anything larger
	// runs as sequential micro-chunks so an 8-16GB node never OOMs mid-chunk.
	// When even one micro-chunk cannot fit (asymmetric orchestrator: weak
	// local, strong peers), the whole request is forwarded to the best peer —
	// the orchestrator executes 0% heavy compute. 503 only when no peer can
	// take it.
	//
	// Forwarding happens at most once: a request that was itself forwarded,
	// or is a part (no_fanout) another node already split, is refused here
	// instead, and its sender falls back to its next peer or runs it.
	forwardedBy := r.Header.Get("X-Pipedpeer-Forwarded")
	if req.RequiredMemBytes > 0 {
		available := s.AvailableForJob()
		if fromLocalShim {
			// The job asking is running here under its own lease; its pool
			// work is that job's memory, not a new promise on top of it.
			available = s.availableForOwnJob()
		}
		if available < req.RequiredMemBytes {
			if forwardedBy == "" && !req.NoFanout {
				if peer := s.pool.bestPeer(storePath, submitter, offload, phone.Need(python, req.FuncSrc != "", modules...)); peer != "" {
					log.Printf("[pool] forwarding %d items (required_mem=%d) to peer %s", len(items), req.RequiredMemBytes, peer)
					if s.forwardPoolMap(r.Context(), w, rawBody, peer, storePath, offload, python) {
						return
					}
					if s.poolCancelled(r, len(items), started) {
						return
					}
				}
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": fmt.Sprintf("insufficient memory: need %d bytes, available %d bytes", req.RequiredMemBytes, available),
			})
			return
		}
		safe := int64(float64(available) * 0.4)
		if safe > 0 && req.RequiredMemBytes > safe && len(items) > 1 {
			ch := &chunk{
				pickledFunc: req.Func, funcSrc: req.FuncSrc, funcName: req.FuncName,
				extraB64: req.ExtraB64, items: items, globals: globals, frames: frames,
				starmap: req.Starmap, itemsB64: req.ItemsB64, itemsZlib: req.ItemsZlib, noSplit: req.NoSplit,
				noFanout: req.NoFanout, force: req.Force, requiredMem: req.RequiredMemBytes,
				cacheKeys: req.CacheKeys, itemKeys: req.ItemKeys,
				chunkDir:    chunkDirFor(storePath),
				offload:     offload,
				fnFP:        req.FnFP,
				python:      python,
				modules:     modules,
				oob:         req.Oob,
				forwardedBy: forwardedBy,
				// originLocal stays unset: setting it here would change
				// placement, which is out of this path's remit.
				prov: &provenance{},
				ctx:  r.Context(),
			}
			results, err := s.runMicroChunks(runPath, storePath, ch, safe, submitter)
			if err != nil {
				if s.poolCancelled(r, len(items), started) {
					return
				}
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			s.writePoolReply(w, ch.frames, results, ch.prov)
			return
		}
	}

	var results []any
	ch := &chunk{
		pickledFunc: req.Func, funcSrc: req.FuncSrc, funcName: req.FuncName,
		extraB64: req.ExtraB64, items: items, globals: globals, frames: frames,
		starmap: req.Starmap, itemsB64: req.ItemsB64, itemsZlib: req.ItemsZlib, noSplit: req.NoSplit,
		noFanout: req.NoFanout, force: req.Force, requiredMem: req.RequiredMemBytes,
		cacheKeys: req.CacheKeys, itemKeys: req.ItemKeys,
		chunkDir:    chunkDirFor(storePath),
		offload:     offload,
		fnFP:        req.FnFP,
		python:      python,
		modules:     modules,
		oob:         req.Oob,
		originLocal: fromLocalShim,
		forwardedBy: forwardedBy,
		prov:        &provenance{},
		ctx:         r.Context(),
	}
	if err := os.MkdirAll(ch.chunkDir, 0755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	switch {
	case ch.noSplit:
		// Hash-shuffle routing: the origin pre-partitioned items into buckets
		// (one per node); each item must land on exactly one node, so bypass
		// the minSplit gate and route per item.
		results, err = s.pool.runChunk(runPath, storePath, ch, submitter)
	case req.NoFanout:
		// One-hop fan-out: the origin already split; this chunk is terminal.
		// Timed, so the origin learns the compute time apart from the trip.
		t0 := time.Now()
		results, err = s.pool.runLocal(runPath, storePath, ch)
		if err == nil {
			d := time.Since(t0)
			ch.prov.record("local", len(ch.items), d.Milliseconds(),
				s.pool.step(s.pool.selfID, "compute", t0, d, 0))
		}
	default:
		results, err = s.pool.runChunk(runPath, storePath, ch, submitter)
	}
	if err != nil {
		if s.poolCancelled(r, len(items), started) {
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.writePoolReply(w, frames, results, ch.prov)
}

// writePoolReply answers a pool request in the shape it was asked in: frames
// for a frames request (the shim reads only results_frames and the frames that
// follow, so a JSON reply to it would look like zero results), JSON otherwise.
func (s *Server) writePoolReply(w http.ResponseWriter, frames bool, results []any, prov *provenance) {
	if !frames {
		writeJSON(w, http.StatusOK, map[string]any{"results": results})
		return
	}
	// Frames response: small header line + raw pickle result frames. The
	// receipt rides in the header so the submitter learns where the work
	// ran, not merely that it was accepted.
	w.Header().Set("Content-Type", framesContentType)
	w.WriteHeader(http.StatusOK)
	body := make([]byte, 0, 1024)
	body = append(body, poolFramesHeader(len(results), s.nodeID, prov.snapshot())...)
	for _, r := range results {
		body = putFrame(body, resultPickle(r))
	}
	w.Write(body)
}

// runMicroChunks splits a too-large request into sequential sub-batches sized
// to max_safe_chunk_size (40% of free RAM) and concatenates results in input
// order. Each sub-batch still fans out to peers; only the orchestrator's own
// memory share is bounded, and the worker's explicit GC keeps RSS flat across
// the pipeline.
func (s *Server) runMicroChunks(runPath, storePath string, ch *chunk, safe int64, submitter string) ([]any, error) {
	n := int((ch.requiredMem + safe - 1) / safe)
	if n > len(ch.items) {
		n = len(ch.items)
	}
	total := int64(len(ch.items))
	var results []any
	for i := 0; i < n; i++ {
		lo := total * int64(i) / int64(n)
		hi := total * int64(i+1) / int64(n)
		if lo == hi {
			continue
		}
		if err := ch.context().Err(); err != nil {
			return nil, err
		}
		sub := *ch
		sub.items = ch.items[lo:hi]
		var keys []string
		if len(ch.cacheKeys) > 0 {
			keys = ch.cacheKeys[lo:hi]
		}
		sub.cacheKeys = keys
		var ikeys []string
		if len(ch.itemKeys) > 0 {
			ikeys = ch.itemKeys[lo:hi]
		}
		sub.itemKeys = ikeys
		var part []any
		var err error
		switch {
		case sub.noSplit:
			part, err = s.pool.runChunk(runPath, storePath, &sub, submitter)
		case sub.noFanout:
			part, err = s.pool.runLocal(runPath, storePath, &sub)
			if err == nil {
				ch.prov.record("local", len(sub.items), 0)
			}
		default:
			part, err = s.pool.runChunk(runPath, storePath, &sub, submitter)
		}
		if err != nil {
			return nil, err
		}
		results = append(results, part...)
	}
	return results, nil
}

// forwardPoolMap relays the raw request to a peer (one hop) so a weak
// orchestrator forwards heavy work instead of refusing it. Returns true when
// the peer answered. The status and body are copied, except that a frames
// receipt is relabelled so the work the peer ran itself reads as "peer:<peer>":
// to the submitter it ran on another machine, not on this one.
//
// ctx is the incoming request's: when its requester goes away the forward is
// dropped, and the peer stops the work through its own request context.
func (s *Server) forwardPoolMap(ctx context.Context, w http.ResponseWriter, rawBody []byte, peer, storePath, offload, python string) bool {
	hdr := http.Header{}
	hdr.Set("Content-Type", "application/json")
	hdr.Set("X-Pipedpeer-Store", storePath)
	// Our node id, so the peer keeps every part of it off this node; any
	// non-empty value reads as forwarded (older builds send "1").
	fwd := s.nodeID
	if fwd == "" {
		fwd = "1"
	}
	hdr.Set("X-Pipedpeer-Forwarded", fwd)
	if python != "" {
		hdr.Set("X-Pipedpeer-Python", python)
	}
	if offload != "" {
		// The peer may split it again; it must keep off that machine too.
		hdr.Set("X-Pipedpeer-Offload", offload)
	}
	status, ct, body, err := s.pool.postPeer(ctx, peer, rawBody, hdr, 5*time.Minute)
	if err != nil {
		if ctx.Err() == nil { // a cancel is logged once by the caller
			log.Printf("[pool] forward to %s failed: %v", peer, err)
		}
		return false
	}
	if status == http.StatusOK {
		body = relabelForwardedReceipt(body, peer)
	}
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(status)
	w.Write(body)
	return true
}

// postPeer POSTs a pool body to a peer's /v1/pool/map through the link
// encoding (C21: compression, globals the peer already holds) and returns the
// reply. A peer that answers 409 {"missing_globals": sha} lacked a globals
// blob it was assumed to hold: that is forgotten and the body resent once.
func (pm *poolManager) postPeer(ctx context.Context, peer string, raw []byte, hdr http.Header, timeout time.Duration) (status int, contentType string, body []byte, err error) {
	key := pm.key(peer)
	client := &http.Client{Timeout: timeout}
	for attempt := 0; ; attempt++ {
		enc, ce := encodePeerBody(key, raw)
		req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/v1/pool/map", peer), bytes.NewReader(enc))
		if err != nil {
			return 0, "", nil, err
		}
		req.Header = hdr.Clone()
		if ce != "" {
			req.Header.Set("Content-Encoding", ce)
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, "", nil, err
		}
		body, err = timedRead(key, resp.Body)
		resp.Body.Close()
		if err != nil {
			return 0, "", nil, fmt.Errorf("reading reply: %w", err)
		}
		if sha, miss := missingGlobalsIn(resp.StatusCode, body); miss && attempt == 0 {
			forgetPeerGlobals(key, sha) // logs the resend
			continue
		}
		return resp.StatusCode, resp.Header.Get("Content-Type"), body, nil
	}
}

// relabelForwardedReceipt rewrites a frames reply's receipt so every part the
// peer ran itself ("local") reads "peer:<peer>". The node and the frames are
// kept. A JSON reply carries no receipt and passes through untouched.
func relabelForwardedReceipt(body []byte, peer string) []byte {
	nl := bytes.IndexByte(body, '\n')
	if nl < 0 {
		return body
	}
	var hdr struct {
		ResultsFrames *int `json:"results_frames"`
		Receipt       struct {
			Node  string     `json:"node"`
			Parts []provPart `json:"parts"`
		} `json:"receipt"`
	}
	if err := json.Unmarshal(body[:nl], &hdr); err != nil {
		log.Printf("[pool] forwarded reply from %s has an unreadable first line; receipt left as sent: %v", peer, err)
		return body
	}
	if hdr.ResultsFrames == nil {
		return body // JSON reply
	}
	for i := range hdr.Receipt.Parts {
		if hdr.Receipt.Parts[i].Where == "local" {
			hdr.Receipt.Parts[i].Where = "peer:" + peer
		}
	}
	out := poolFramesHeader(*hdr.ResultsFrames, hdr.Receipt.Node, hdr.Receipt.Parts)
	return append(out, body[nl+1:]...)
}

// poolSubmitter is the requesting job's submitting machine as this daemon's
// peer list names it ("host:port"), so spill order can sink it.
//
// By node id first: the submitter stamps the address it reaches its LAN
// from, which never matches on an internet peer, where it is a node at
// 127.0.0.1:<forwarder port> - and the orchestrator got sent work. The
// address header stays for submitters too old to send the id.
func (s *Server) poolSubmitter(r *http.Request) string {
	addr := r.Header.Get("X-Pipedpeer-Submitter")
	id := r.Header.Get("X-Pipedpeer-Submitter-Node")
	if id == "" {
		return addr
	}
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	for _, ph := range s.peerHealths {
		if ph.NodeID == id && ph.Host != "" && ph.Port != 0 {
			return fmt.Sprintf("%s:%d", ph.Host, ph.Port)
		}
	}
	return addr
}
