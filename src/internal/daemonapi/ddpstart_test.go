package daemonapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func ddpPostKind(t *testing.T, ts *httptest.Server, seq int64, rank int, kind string) (int, map[string]any) {
	t.Helper()
	hdr, _ := json.Marshal(map[string]any{
		"group": "g", "seq": seq, "rank": rank, "world": 2,
		"dtype": "float32", "count": 2, "kind": kind,
	})
	body := putFrame(append(hdr, '\n'), encodeF32([]float32{1, 2}))
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ddp/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", DDPReduceContentType)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("rank %d seq %d: %v", rank, seq, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var head map[string]any
	if nl := bytes.IndexByte(raw, '\n'); nl >= 0 {
		_ = json.Unmarshal(raw[:nl], &head)
	}
	return resp.StatusCode, head
}

// TestARankMissingTheStartIsDroppedNotWaitedFor is V3 53: rank 1 was still
// receiving its closure when rank 0 began, so every step waited the 45 s
// partial timeout for it, and the rounds rank 0 closed alone said every rank
// produced the SAME gradients. The opening round is the start barrier: a rank
// missing at its end is dropped, later rounds do not wait for it, a round of
// one rank is no evidence of duplicated work, and the late rank is turned away.
func TestARankMissingTheStartIsDroppedNotWaitedFor(t *testing.T) {
	sw, pa := ddpStartWait, ddpPartialAfter
	ddpStartWait, ddpPartialAfter = 200*time.Millisecond, time.Minute
	t.Cleanup(func() { ddpStartWait, ddpPartialAfter = sw, pa })

	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	if code, _ := ddpPostKind(t, ts, 1, 0, "weights"); code != http.StatusOK {
		t.Fatalf("opening round: status %d, want it to close on the ranks that came", code)
	}

	start := time.Now()
	code, head := ddpPostKind(t, ts, 2, 0, "grads")
	if code != http.StatusOK {
		t.Fatalf("first step: status %d", code)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("first step took %v: it waited for the rank that missed the start", d)
	}
	if head["down"] != float64(1) {
		t.Errorf("reply %v: want a note that rank 1 left the ring", head)
	}
	if head["same_work"] == true {
		t.Error("a round of one rank was reported as every rank doing the same work")
	}

	if code, _ := ddpPostKind(t, ts, 1, 1, "weights"); code != http.StatusGone {
		t.Errorf("the late rank got status %d, want 410 so its shim stops", code)
	}
}
