package daemonapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// DDP sync over the daemon's own channel. Ranks never open ports of their
// own: every rank POSTs its blob for (group, seq) to the lead rank's daemon
// on the one port that already works between every pair of nodes, and the
// call returns once all world blobs are present. Averaging happens in the
// shim — the daemon is a blackboard, not a math engine.
//
// This replaces torch.distributed's socket mesh, which needs direct
// rank-to-rank TCP on its own ports — a requirement that holds on a
// datacenter LAN and on nothing pipedpeer actually targets.

// DDPFramesContentType marks the binary form of a sync: a JSON header line
// followed by one length-prefixed payload frame. Base64 inflates a gradient
// by a third both on the wire and in this daemon's memory, and it buys
// nothing here — the daemon never looks inside the payload.
const DDPFramesContentType = "application/vnd.pipedpeer.ddp"

// DDPReduceContentType marks a sync the daemon averages itself. The reply is
// one model rather than one per rank, which is the whole point: on two
// machines the old reply was already twice the model per step, and it grows
// with the ring. See ddpreduce.go for why that mattered enough to give up the
// daemon's ignorance of the payload.
const DDPReduceContentType = "application/vnd.pipedpeer.ddp.reduce"

type ddpEntry struct {
	blobs   map[int][]byte // rank -> opaque payload (blackboard mode)
	fetched map[int]bool   // ranks that have taken the result home
	done    chan struct{}  // closed when all world contributions are present
	world   int
	created time.Time

	// Reduce mode: contributions are folded into acc as they arrive and the
	// individual payloads are dropped, so the lead daemon holds one buffer
	// instead of one model per rank. result is computed once, when the last
	// rank arrives, and served to every waiter.
	acc         *ddpAccumulator
	result      []byte
	resultScale float64
	// seen tracks which ranks have contributed, since acc keeps only a count
	// and a rank that retried must not be folded in twice.
	seen map[int]bool
	// err is set when a contribution could not be folded in - a rank that
	// disagrees about the model's shape, say. Held rather than returned to
	// that one rank, because the others are blocked waiting for a result that
	// is never coming.
	err error
	// syncEvery is the agreed averaging interval: the largest any rank
	// proposed. Largest rather than smallest because the proposal is a rank's
	// estimate of how much of its run sync is eating, and the rank suffering
	// most is the one setting the pace for everybody.
	syncEvery int

	// fingerprint detects ranks doing identical work. Sharding only happens
	// automatically for DataLoader-based code; a script that slices its
	// tensors by hand - which both bundled training demos do - leaves every
	// rank computing the same batch. The gradients are then identical, the
	// average equals the single-process gradient exactly, and the run is
	// correct, slower, and silent about it. This is the only place holding
	// every rank's contribution, so it is the only place that can notice.
	fingerprint uint64
	sameWork    bool
	checked     bool
}

type ddpBoard struct {
	mu      sync.Mutex
	entries map[string]*ddpEntry // key: group + "/" + seq
}

func newDDPBoard() *ddpBoard {
	return &ddpBoard{entries: make(map[string]*ddpEntry)}
}

// sweep drops entries older than maxAge: a rank that died mid-run must not
// pin its group's blobs (a full model per rank) in memory forever.
func (b *ddpBoard) sweep(maxAge time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for k, e := range b.entries {
		if time.Since(e.created) > maxAge {
			delete(b.entries, k)
		}
	}
}

type ddpSyncRequest struct {
	Group string `json:"group"`
	Seq   int64  `json:"seq"`
	Rank  int    `json:"rank"`
	World int    `json:"world"`
	Data  string `json:"data"` // b64, opaque to the daemon

	// Reduce mode only.
	DType ddpDType `json:"dtype,omitempty"`
	Count int      `json:"count,omitempty"`
	// Scale is the int8 quantisation step this rank used. Each rank picks its
	// own from its own values, so it travels with the payload.
	Scale float64 `json:"scale,omitempty"`
	// Kind is "grads" or "weights". Only gradients are checked for ranks
	// duplicating work: the run opens by broadcasting the initial weights,
	// and those are identical by construction whenever the script seeds its
	// generator - which every sane training script does. Fingerprinting that
	// sync reported duplicated work on a correctly sharded run.
	Kind string `json:"kind,omitempty"`
	// SyncEvery is this rank's proposal for how often to average. Ranks
	// measure their own link and compute independently and so arrive at
	// different numbers; averaging at different points in each rank's step
	// sequence is not local SGD, it is nonsense, and it showed up as a
	// measurably worse final loss. The daemon hands everyone one answer.
	SyncEvery int `json:"sync_every,omitempty"`
}

// ddpMode is how a request wants its payload handled.
type ddpMode int

const (
	ddpModeJSON   ddpMode = iota // base64 envelope, blackboard
	ddpModeFrames                // binary frames, blackboard
	ddpModeReduce                // binary frames, averaged here
)

// readDDPRequest accepts all three forms.
func readDDPRequest(r *http.Request) (ddpSyncRequest, []byte, ddpMode, error) {
	ct := r.Header.Get("Content-Type")
	if ct != DDPFramesContentType && ct != DDPReduceContentType {
		var req ddpSyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return req, nil, ddpModeJSON, err
		}
		// Deliberately not decoded. In blackboard mode whatever a rank
		// deposits is what its peers take home, and validating the encoding
		// would reject payloads the daemon has no business reading.
		return req, []byte(req.Data), ddpModeJSON, nil
	}
	mode := ddpModeFrames
	if ct == DDPReduceContentType {
		mode = ddpModeReduce
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return ddpSyncRequest{}, nil, mode, err
	}
	nl := bytes.IndexByte(body, '\n')
	if nl < 0 {
		return ddpSyncRequest{}, nil, mode, fmt.Errorf("ddp frames body has no header line")
	}
	var req ddpSyncRequest
	if err := json.Unmarshal(body[:nl], &req); err != nil {
		return req, nil, mode, err
	}
	blob, _, err := readFrame(body[nl+1:])
	return req, blob, mode, err
}

// handleDDPSync is one round trip per rank per sync: deposit our blob, block
// until the set is complete, take every rank's blob home.
func (s *Server) handleDDPSync(w http.ResponseWriter, r *http.Request) {
	req, blob, mode, err := readDDPRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Group == "" || req.World < 1 || req.Rank < 0 || req.Rank >= req.World {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "group, world and rank in [0,world) required"})
		return
	}

	key := req.Group + "/" + jsonInt(req.Seq)

	s.ddp.mu.Lock()
	e, ok := s.ddp.entries[key]
	if !ok {
		e = &ddpEntry{
			blobs:   make(map[int][]byte),
			fetched: make(map[int]bool),
			seen:    make(map[int]bool),
			done:    make(chan struct{}),
			world:   req.World,
			created: time.Now(),
		}
		if mode == ddpModeReduce {
			if req.Count <= 0 || req.DType.size() == 0 {
				s.ddp.mu.Unlock()
				writeJSON(w, http.StatusBadRequest,
					map[string]string{"error": "reduce mode needs a positive count and a known dtype"})
				return
			}
			e.acc = newDDPAccumulator(req.DType, req.Count)
		}
		s.ddp.entries[key] = e
	}
	if e.world != req.World {
		s.ddp.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "world size mismatch within group"})
		return
	}
	if req.SyncEvery > e.syncEvery {
		e.syncEvery = req.SyncEvery
	}
	var complete bool
	if e.acc != nil {
		// Folded in on arrival and the payload dropped: holding every rank's
		// model until the last one shows up is what made the lead daemon's
		// memory scale with the ring.
		if !e.seen[req.Rank] {
			// Only worth doing early: ranks that duplicate work do it from
			// the first step, and hashing every payload forever would put a
			// pass over the model into every sync.
			if req.Kind == "grads" && req.Seq <= ddpFingerprintSeqs {
				h := fnv1a(blob)
				if len(e.seen) == 0 {
					e.fingerprint, e.sameWork, e.checked = h, true, true
				} else if h != e.fingerprint {
					e.sameWork = false
				}
			}
			if addErr := e.acc.add(blob, req.Scale); addErr != nil {
				// Recorded rather than returned to this rank alone: the
				// others are blocked on a result that is now never coming,
				// and a timeout three minutes later would name nothing.
				e.err = fmt.Errorf("rank %d: %w", req.Rank, addErr)
			} else {
				e.seen[req.Rank] = true
			}
		}
		complete = e.err != nil || len(e.seen) == e.world
		if complete && e.err == nil && e.result == nil {
			e.result, e.resultScale, e.err = e.acc.mean()
			e.acc = nil // the sum is no longer needed; let it go now
		}
	} else {
		e.blobs[req.Rank] = blob
		complete = len(e.blobs) == e.world
	}
	if complete {
		select {
		case <-e.done:
		default:
			close(e.done)
		}
	}
	s.ddp.mu.Unlock()

	if !complete {
		select {
		case <-e.done:
		case <-r.Context().Done():
			return
		case <-time.After(3 * time.Minute):
			writeJSON(w, http.StatusGatewayTimeout,
				map[string]string{"error": "ddp sync timed out waiting for peer ranks"})
			return
		}
	}

	s.ddp.mu.Lock()
	if e.acc != nil || e.result != nil || e.err != nil {
		result, rerr, agreedEvery, rscale := e.result, e.err, e.syncEvery, e.resultScale
		sameWork := e.checked && e.sameWork && e.world > 1
		e.fetched[req.Rank] = true
		if len(e.fetched) == e.world || rerr != nil {
			delete(s.ddp.entries, key)
		}
		s.ddp.mu.Unlock()
		if rerr != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": rerr.Error()})
			return
		}
		body := []byte(fmt.Sprintf(
			"{\"reduced\": true, \"sync_every\": %d, \"scale\": %v, \"same_work\": %t}\n",
			agreedEvery, rscale, sameWork))
		body = putFrame(body, result)
		w.Header().Set("Content-Type", DDPReduceContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}
	ordered := make([][]byte, e.world)
	for rank, b := range e.blobs {
		ordered[rank] = b
	}
	e.fetched[req.Rank] = true
	if len(e.fetched) == e.world {
		delete(s.ddp.entries, key)
	}
	s.ddp.mu.Unlock()

	if mode == ddpModeFrames {
		body := []byte(fmt.Sprintf("{\"blob_frames\": %d}\n", len(ordered)))
		for _, b := range ordered {
			body = putFrame(body, b)
		}
		w.Header().Set("Content-Type", DDPFramesContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}
	asStrings := make([]string, len(ordered))
	for i, b := range ordered {
		asStrings[i] = string(b)
	}
	writeJSON(w, http.StatusOK, map[string]any{"blobs": asStrings})
}

func jsonInt(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// ddpFingerprintSeqs is how many early syncs are checked for ranks doing
// identical work.
const ddpFingerprintSeqs = 3

// fnv1a is FNV-1a 64. Enough to tell "these two models are the same" from
// "these two models differ", which is the only question being asked, and fast
// enough to run over a model without showing up beside a network round trip.
func fnv1a(b []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}
