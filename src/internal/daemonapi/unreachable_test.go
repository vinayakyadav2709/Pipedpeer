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
