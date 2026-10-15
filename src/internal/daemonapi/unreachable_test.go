package daemonapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func getJSON(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d, want 200", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

// /v1/peers/unreachable is served from the rows themselves, so a reason lands
// on the node it belongs to (B3: the old report was keyed by fingerprint and
// the rows by UUID, which made phantom rows). Healthy peers are not listed.
func TestUnreachablePeersComeFromTheirRows(t *testing.T) {
	s := nodeTestServer(t, "me")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	var empty []unreachablePeer
	getJSON(t, ts.URL+"/v1/peers/unreachable", &empty)
	if empty == nil || len(empty) != 0 {
		t.Fatalf("no peers: got %#v, want []", empty)
	}

	okHost, okPort := fakePeer(t, "alive")
	if err := s.AddPeer(okHost, okPort); err != nil {
		t.Fatal(err)
	}
	deadHost, deadPort := deadAddr(t)
	if err := s.store.SeePath("gone", "lan", deadHost, deadPort); err != nil {
		t.Fatal(err)
	}
	s.pollAllNodes()

	var got []unreachablePeer
	getJSON(t, ts.URL+"/v1/peers/unreachable", &got)
	if len(got) != 1 || got[0].Peer != "gone" || !strings.Contains(got[0].Reason, "refused") {
		t.Fatalf("got %+v, want only 'gone' with its connection error", got)
	}

	var nodes []nodeResp
	getJSON(t, ts.URL+"/v1/nodes", &nodes)
	byID := map[string]nodeResp{}
	for _, n := range nodes {
		byID[n.NodeID] = n
	}
	if a := byID["alive"]; a.State != "healthy" || a.Reason != "" || a.Path != "internet" || a.Host != okHost {
		t.Fatalf("alive row %+v: want healthy, internet path at %s, no reason", a, okHost)
	}
	if g := byID["gone"]; g.State != "unreachable" || g.Reason == "" || g.Capabilities[ReasonCapability] != g.Reason {
		t.Fatalf("gone row %+v: want unreachable with its reason (top level and capability)", g)
	}
}
