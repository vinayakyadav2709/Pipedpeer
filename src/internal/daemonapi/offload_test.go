package daemonapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestOffloadNeverPicksTheExcludedMachine: a chunk carrying --offload's
// machine key runs no part on a peer with that key, and none on this node
// when this node is that machine. All peers are 127.0.0.1 addresses, as
// internet peers are, so only the key separates them.
func TestOffloadNeverPicksTheExcludedMachine(t *testing.T) {
	var hits [3]atomic.Int64
	peer := func(i int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits[i].Add(1)
			var req struct {
				Items []json.RawMessage `json:"items"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			res := make([]map[string]string, len(req.Items))
			for j := range res {
				res[j] = map[string]string{"pickle": base64.StdEncoding.EncodeToString([]byte("ok"))}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"results": res})
		}))
	}
	keys := []string{"boot-me", "boot-b", "boot-c"} // peer 0 is a second daemon on this machine
	machineOf := map[string]string{}
	var peers []string
	for i := range keys {
		s := peer(i)
		defer s.Close()
		addr := strings.TrimPrefix(s.URL, "http://")
		peers = append(peers, addr)
		machineOf[addr] = keys[i]
	}

	old := selfMachine
	selfMachine = func() string { return "boot-me" }
	defer func() { selfMachine = old }()

	pm := newPoolManager()
	pm.SetPeerFn(func(_, _ string) []string { return peers })
	pm.machineFn = func(p string) string { return machineOf[p] }

	for _, tc := range []struct {
		name  string
		items int
	}{{"split chunk", 32}, {"chunk too small to split", 3}} {
		t.Run(tc.name, func(t *testing.T) {
			items := make([]json.RawMessage, tc.items)
			for i := range items {
				items[i] = json.RawMessage(`"aXRlbQ=="`)
			}
			ch := &chunk{funcSrc: "def run(x): return x", funcName: "run", items: items,
				itemsB64: true, originLocal: true, offload: "boot-me", prov: &provenance{}}
			// A part run here would need this run path; it does not exist,
			// so any local part fails the chunk.
			res, err := pm.runChunk("/nonexistent/bin/run", "/nonexistent", ch, "")
			if err != nil {
				t.Fatalf("runChunk: %v", err)
			}
			if len(res) != tc.items {
				t.Fatalf("got %d results, want %d", len(res), tc.items)
			}
			if n := hits[0].Load(); n != 0 {
				t.Errorf("the excluded machine's peer got %d requests", n)
			}
			for _, p := range ch.prov.snapshot() {
				if !strings.HasPrefix(p.Where, "peer:") || machineOf[strings.TrimPrefix(p.Where, "peer:")] == "boot-me" {
					t.Errorf("part ran at %q under --offload", p.Where)
				}
			}
		})
	}
	if hits[1].Load()+hits[2].Load() == 0 {
		t.Errorf("no peer got any work")
	}
}
