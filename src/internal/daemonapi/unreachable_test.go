package daemonapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// An internet peer with no direct path has no forwarder, so /v1/nodes never
// lists it. The daemon must say it is there and why it gets no work.
func TestUnreachablePeersAreServed(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	get := func() []unreachablePeer {
		t.Helper()
		resp, err := http.Get(ts.URL + "/v1/peers/unreachable")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d, want 200", resp.StatusCode)
		}
		var out []unreachablePeer
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		if out == nil {
			t.Fatal("body decoded to null, want [] so clients need no nil case")
		}
		return out
	}

	if got := get(); len(got) != 0 {
		t.Fatalf("no reporter: got %+v, want []", got)
	}

	s.SetUnreachableReporter(func() map[string]string {
		return map[string]string{"bbbb": "both behind symmetric NAT", "aaaa": "punch timed out"}
	})
	want := []unreachablePeer{{"aaaa", "punch timed out"}, {"bbbb", "both behind symmetric NAT"}}
	if got := get(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v (sorted by peer)", got, want)
	}

	s.SetUnreachableReporter(func() map[string]string { return nil })
	if got := get(); len(got) != 0 {
		t.Fatalf("empty reporter: got %+v, want []", got)
	}
}

// The /ui page shows unreachable peers as rows: a known node is marked with
// the reason, a peer the store never held is appended, the rest are untouched.
func TestWithUnreachableMarksRows(t *testing.T) {
	s := New("test-node")
	nodes := []nodeResp{
		{NodeID: "known", State: "healthy", Capabilities: map[string]string{"cpu": "4"}},
		{NodeID: "other", State: "healthy"},
	}
	if got := s.withUnreachable(nodes); !reflect.DeepEqual(got, nodes) {
		t.Fatalf("no reporter: got %+v, want rows unchanged", got)
	}

	s.SetUnreachableReporter(func() map[string]string {
		return map[string]string{"known": "punch timed out", "stranger": "both behind symmetric NAT"}
	})
	got := s.withUnreachable([]nodeResp{
		{NodeID: "known", State: "healthy", Capabilities: map[string]string{"cpu": "4"}},
		{NodeID: "other", State: "healthy"},
	})
	want := []nodeResp{
		{NodeID: "known", State: "unreachable", Capabilities: map[string]string{"cpu": "4", ReasonCapability: "punch timed out"}},
		{NodeID: "other", State: "healthy"},
		{NodeID: "stranger", State: "unreachable", Source: "internet",
			Capabilities: map[string]string{ReasonCapability: "both behind symmetric NAT"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}
