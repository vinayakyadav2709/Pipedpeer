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

type ddpEntry struct {
	blobs   map[int][]byte // rank -> opaque payload
	fetched map[int]bool   // ranks that have taken the full set home
	done    chan struct{}  // closed when all world blobs are present
	world   int
	created time.Time
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
}

// readDDPRequest accepts either form: the binary frame or the JSON envelope.
func readDDPRequest(r *http.Request) (ddpSyncRequest, []byte, bool, error) {
	if r.Header.Get("Content-Type") != DDPFramesContentType {
		var req ddpSyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return req, nil, false, err
		}
		// Deliberately not decoded. The daemon is a blackboard: whatever a
		// rank deposits is what its peers take home, and validating the
		// encoding would reject payloads it has no business reading.
		return req, []byte(req.Data), false, nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return ddpSyncRequest{}, nil, true, err
	}
	nl := bytes.IndexByte(body, '\n')
	if nl < 0 {
		return ddpSyncRequest{}, nil, true, fmt.Errorf("ddp frames body has no header line")
	}
	var req ddpSyncRequest
	if err := json.Unmarshal(body[:nl], &req); err != nil {
		return req, nil, true, err
	}
	blob, _, err := readFrame(body[nl+1:])
	return req, blob, true, err
}

// handleDDPSync is one round trip per rank per sync: deposit our blob, block
// until the set is complete, take every rank's blob home.
func (s *Server) handleDDPSync(w http.ResponseWriter, r *http.Request) {
	req, blob, frames, err := readDDPRequest(r)
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
			done:    make(chan struct{}),
			world:   req.World,
			created: time.Now(),
		}
		s.ddp.entries[key] = e
	}
	if e.world != req.World {
		s.ddp.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "world size mismatch within group"})
		return
	}
	e.blobs[req.Rank] = blob
	complete := len(e.blobs) == e.world
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
	ordered := make([][]byte, e.world)
	for rank, b := range e.blobs {
		ordered[rank] = b
	}
	e.fetched[req.Rank] = true
	if len(e.fetched) == e.world {
		delete(s.ddp.entries, key)
	}
	s.ddp.mu.Unlock()

	if frames {
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
