package daemonapi

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func ddpPost(t *testing.T, ts *httptest.Server, group string, seq int64, rank, world int, data string) (int, []string) {
	t.Helper()
	body, _ := json.Marshal(ddpSyncRequest{Group: group, Seq: seq, Rank: rank, World: world, Data: data})
	resp, err := ts.Client().Post(ts.URL+"/v1/ddp/sync", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("rank %d: %v", rank, err)
	}
	defer resp.Body.Close()
	var out struct {
		Blobs []string `json:"blobs"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out.Blobs
}

// Every rank deposits its blob and blocks until the full set exists — one
// round trip per rank per sync, all on the daemon's single port. This is the
// transport that replaced torch.distributed's own socket mesh, which needed
// direct rank-to-rank TCP that pipedpeer's target networks do not offer.
func TestDDPSyncExchangesAllRanks(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	const world = 3
	results := make([][]string, world)
	var wg sync.WaitGroup
	for r := 0; r < world; r++ {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			// Stagger arrivals: late ranks must not miss the exchange.
			time.Sleep(time.Duration(rank) * 50 * time.Millisecond)
			code, blobs := ddpPost(t, ts, "g1", 1, rank, world, string(rune('a'+rank)))
			if code != 200 {
				t.Errorf("rank %d got status %d", rank, code)
				return
			}
			results[rank] = blobs
		}(r)
	}
	wg.Wait()

	for rank, blobs := range results {
		if len(blobs) != world {
			t.Fatalf("rank %d received %d blobs, want %d", rank, len(blobs), world)
		}
		for i, b := range blobs {
			if b != string(rune('a'+i)) {
				t.Fatalf("rank %d: blob[%d] = %q, want %q", rank, i, b, string(rune('a'+i)))
			}
		}
	}

	// The entry must be gone once every rank has taken the set home:
	// each sync is a full model per rank, and a training run does thousands.
	s.ddp.mu.Lock()
	n := len(s.ddp.entries)
	s.ddp.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d entries left on the board after a complete exchange", n)
	}
}

func TestDDPSyncSequencesAreIndependent(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	var wg sync.WaitGroup
	for seq := int64(1); seq <= 2; seq++ {
		for r := 0; r < 2; r++ {
			wg.Add(1)
			go func(seq int64, rank int) {
				defer wg.Done()
				code, blobs := ddpPost(t, ts, "g", seq, rank, 2, "d")
				if code != 200 || len(blobs) != 2 {
					t.Errorf("seq %d rank %d: status %d, %d blobs", seq, rank, code, len(blobs))
				}
			}(seq, r)
		}
	}
	wg.Wait()
}

func TestDDPSyncRejectsBadRequests(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	if code, _ := ddpPost(t, ts, "", 1, 0, 1, "x"); code != 400 {
		t.Fatalf("empty group accepted: %d", code)
	}
	if code, _ := ddpPost(t, ts, "g", 1, 5, 2, "x"); code != 400 {
		t.Fatalf("rank outside world accepted: %d", code)
	}
}

func TestDDPSyncSweepDropsStaleEntries(t *testing.T) {
	b := newDDPBoard()
	b.entries["old/1"] = &ddpEntry{created: time.Now().Add(-time.Hour), done: make(chan struct{})}
	b.entries["new/1"] = &ddpEntry{created: time.Now(), done: make(chan struct{})}
	b.sweep(10 * time.Minute)
	if _, ok := b.entries["old/1"]; ok {
		t.Fatal("stale entry survived the sweep")
	}
	if _, ok := b.entries["new/1"]; !ok {
		t.Fatal("fresh entry was swept")
	}
}
