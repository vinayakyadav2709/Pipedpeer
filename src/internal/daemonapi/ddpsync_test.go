package daemonapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
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

// TestDDPSyncBinaryFramesRoundTrip covers the wire format gradients actually
// use. A gradient is the largest thing this system sends per step, and it is
// sent every step; base64 inflated it by a third both on the wire and in this
// daemon's memory, where a full model per rank is already held until the
// slowest rank arrives. The daemon never reads the payload, so the encoding
// bought nothing — and binary payloads must survive bytes that are not valid
// UTF-8, which a JSON string cannot carry.
func TestDDPSyncBinaryFramesRoundTrip(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	const world = 2
	// Deliberately not text: real payloads are pickled float arrays.
	payloads := [][]byte{
		{0x00, 0xff, 0x80, 0x01, 0xfe},
		{0xde, 0xad, 0xbe, 0xef, 0x00},
	}

	post := func(rank int) ([][]byte, error) {
		hdr, _ := json.Marshal(map[string]any{
			"group": "bin", "seq": 1, "rank": rank, "world": world,
		})
		body := append(hdr, '\n')
		body = putFrame(body, payloads[rank])
		resp, err := http.Post(ts.URL+"/v1/ddp/sync", DDPFramesContentType, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("status %d", resp.StatusCode)
		}
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		nl := bytes.IndexByte(raw, '\n')
		if nl < 0 {
			return nil, fmt.Errorf("no header line")
		}
		var head struct {
			Frames int `json:"blob_frames"`
		}
		if err := json.Unmarshal(raw[:nl], &head); err != nil {
			return nil, err
		}
		rest := raw[nl+1:]
		var out [][]byte
		for i := 0; i < head.Frames; i++ {
			var f []byte
			if f, rest, err = readFrame(rest); err != nil {
				return nil, err
			}
			out = append(out, f)
		}
		return out, nil
	}

	results := make([][][]byte, world)
	errs := make([]error, world)
	var wg sync.WaitGroup
	for r := 0; r < world; r++ {
		wg.Add(1)
		go func(rank int) {
			defer wg.Done()
			results[rank], errs[rank] = post(rank)
		}(r)
	}
	wg.Wait()

	for rank := range results {
		if errs[rank] != nil {
			t.Fatalf("rank %d: %v", rank, errs[rank])
		}
		if len(results[rank]) != world {
			t.Fatalf("rank %d got %d blobs, want %d", rank, len(results[rank]), world)
		}
		for i, got := range results[rank] {
			if !bytes.Equal(got, payloads[i]) {
				t.Errorf("rank %d blob %d = %x, want %x", rank, i, got, payloads[i])
			}
		}
	}
}

// TestDDPSyncNeedsATokenWhenOneIsSet pins down the failure mode rather than
// the fix, because the fix lives in the launcher and the symptom lived here.
//
// Every rank posts its gradients to the lead rank's daemon. When that daemon
// requires a token and the rank has not been given one, the request is
// refused - and the refusal arrives while the body is still uploading, so the
// client sees a BrokenPipeError from inside urllib rather than anything about
// authentication. DDP was broken on every cluster with a token set, and the
// error named neither the daemon nor the credential.
func TestDDPSyncNeedsATokenWhenOneIsSet(t *testing.T) {
	if err := authtoken.Set("test-token-for-ddp"); err != nil {
		t.Skipf("NOT VERIFIED: cannot set a token here: %v", err)
	}
	defer authtoken.Set("")

	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	body := []byte(`{"group":"g","seq":1,"rank":0,"world":1}` + "\n")

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ddp/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", DDPFramesContentType)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated sync returned %d, want 401 — this test cannot "+
			"be guarding anything if the endpoint is open", resp.StatusCode)
	}

	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ddp/sync", bytes.NewReader(body))
	req2.Header.Set("Content-Type", DDPFramesContentType)
	req2.Header.Set(authtoken.Header, "test-token-for-ddp")
	resp2, err := ts.Client().Do(req2)
	if err != nil {
		t.Fatalf("authenticated request failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode == http.StatusUnauthorized {
		t.Error("the token was rejected; ranks carrying it still could not sync")
	}
}
