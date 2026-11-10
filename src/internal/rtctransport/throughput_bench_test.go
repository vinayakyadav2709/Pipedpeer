package rtctransport

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

// One 8 MiB POST echoed back over a warm peer connection: 16 MiB through the
// transport per iteration, on loopback.
func BenchmarkEcho8MiB(b *testing.B) {
	p := newPair(b)
	go func() {
		_ = http.Serve(p.listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(w, r.Body)
		}))
	}()
	tr := NewHTTPTransport(p.dialer)
	b.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr}
	body := bytes.Repeat([]byte("pipedpeer"), (8<<20)/9+1)[:8<<20]
	url := "http://" + p.addr + "/echo"

	echo := func() {
		resp, err := client.Post(url, "application/octet-stream", bytes.NewReader(body))
		if err != nil {
			b.Fatal(err)
		}
		n, err := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if err != nil || n != int64(len(body)) {
			b.Fatalf("echoed %d of %d bytes: %v", n, len(body), err)
		}
	}
	echo()
	b.SetBytes(2 * int64(len(body)))
	b.ReportAllocs()
	for b.Loop() {
		echo()
	}
}
