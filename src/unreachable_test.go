package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

func portOf(t *testing.T, srv *httptest.Server) int {
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFetchUnreachableReadsTheDaemon(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/peers/unreachable" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Write([]byte(`[{"peer":"abc","reason":"punch timed out"}]`))
	}))
	defer srv.Close()
	got, err := fetchUnreachable(portOf(t, srv))
	if err != nil || len(got) != 1 || got[0].Peer != "abc" || got[0].Reason != "punch timed out" {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestFetchUnreachableOldDaemon404(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	got, err := fetchUnreachable(portOf(t, srv))
	if err != nil || got != nil {
		t.Fatalf("got %v, %v", got, err)
	}
}
