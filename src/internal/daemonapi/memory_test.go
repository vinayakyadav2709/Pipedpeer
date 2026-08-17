package daemonapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func memAvailBytes(t *testing.T) int64 {
	t.Helper()
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		t.Skipf("/proc/meminfo unavailable: %v", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return kb * 1024
		}
	}
	t.Fatal("MemAvailable not found in /proc/meminfo")
	return 0
}

func postPoolMap(t *testing.T, url, storePath string, body map[string]any) (int, []int64) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest("POST", url+"/v1/pool/map", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Pipedpeer-Store", storePath)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("pool/map: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Results []struct {
			Pickle string `json:"pickle"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return resp.StatusCode, nil
	}
	// The worker returns pickled values; small ints pickle as
	// [header] K <value> . — verify the trailer rather than the whole blob.
	vals := make([]int64, 0, len(out.Results))
	for _, r := range out.Results {
		raw, err := base64.StdEncoding.DecodeString(r.Pickle)
		if err != nil || len(raw) < 3 || raw[len(raw)-3] != 'K' || raw[len(raw)-1] != '.' {
			t.Fatalf("unexpected pickle payload %q", r.Pickle)
		}
		vals = append(vals, int64(raw[len(raw)-2]))
	}
	return resp.StatusCode, vals
}

// TestPoolMapMicroChunks (T10) posts a payload whose required_mem exceeds 40%
// of the node's free RAM and requires the daemon to pipeline it as sequential
// micro-chunks, returning every result in input order (horizontal scaling on
// constrained nodes: no OOM, no reordering).
func TestPoolMapMicroChunks(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	fakeRun(t, storePath)

	s := New("node-" + strings.Repeat("x", 8))
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	defer s.StopWarmWorkers()

	avail := memAvailBytes(t)
	required := avail * 6 / 10 // 60% of free RAM: > 40% (micro-split) and < 100% (admit)
	var items []any
	for i := 1; i <= 60; i++ {
		items = append(items, i)
	}
	status, results := postPoolMap(t, srv.URL, storePath, map[string]any{
		"func_src":     "def run(x):\n    return x * 2\n",
		"items":        items,
		"starmap":      false,
		"required_mem": required,
	})
	if status != http.StatusOK {
		t.Fatalf("micro-chunk pipeline: status %d, want 200", status)
	}
	if len(results) != 60 {
		t.Fatalf("want 60 results, got %d", len(results))
	}
	for i, want := range results {
		if want != int64((i+1)*2) {
			t.Fatalf("result %d: got %v, want %v (order broken)", i, want, (i+1)*2)
		}
	}
}

// TestPoolMapForwardToPeer (T11) simulates an asymmetric orchestrator: the
// local node's free memory cannot admit the payload, a healthy peer can. The
// request must be forwarded whole to the peer (0% heavy compute locally) and
// the results must come back unchanged.
func TestPoolMapForwardToPeer(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	fakeRun(t, storePath)

	s1 := New("node-" + strings.Repeat("a", 8))
	s2 := New("node-" + strings.Repeat("b", 8))

	// s1 reserves nearly all its RAM so it can never admit the payload.
	avail := memAvailBytes(t)
	s1.mu.Lock()
	s1.leases["reserve"] = &Lease{LeaseID: "reserve", State: LeaseRunning, MemBytes: avail - 100*1024*1024}
	s1.mu.Unlock()

	var mu sync.Mutex
	hits := 0
	srv1 := httptest.NewServer(s1.Handler())
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/pool/map" {
			mu.Lock()
			hits++
			mu.Unlock()
		}
		s2.Handler().ServeHTTP(w, r)
	}))
	defer srv1.Close()
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()

	peerHost := strings.TrimPrefix(srv2.URL, "http://")
	s1.pool.SetPeerFn(func(_ string) []string { return []string{peerHost} })

	var items []any
	for i := 1; i <= 40; i++ {
		items = append(items, i)
	}
	status, results := postPoolMap(t, srv1.URL, storePath, map[string]any{
		"func_src":     "def run(x):\n    return x * 2\n",
		"items":        items,
		"starmap":      false,
		"required_mem": 300 * 1024 * 1024, // > s1's ~100MB free, < s2's
	})
	if status != http.StatusOK {
		t.Fatalf("forward: status %d, want 200", status)
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Fatalf("peer must receive exactly 1 pool/map request, got %d", hits)
	}
	if len(results) != 40 {
		t.Fatalf("want 40 results, got %d", len(results))
	}
	for i, want := range results {
		if want != int64((i+1)*2) {
			t.Fatalf("result %d: got %v, want %v", i, want, (i+1)*2)
		}
	}
}
