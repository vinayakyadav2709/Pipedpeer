package daemonctl

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMaxConcurrentAt(t *testing.T) {
	for body, want := range map[string]int{`{"max_concurrent":3}`: 3, `{"status":"ok"}`: 0, `x`: 0} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(body)) }))
		if got := maxConcurrentAt(s.URL); got != want {
			t.Errorf("%s: got %d want %d", body, got, want)
		}
		s.Close()
	}
}
