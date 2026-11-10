package rtctransport

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPOverDataChannels(t *testing.T) {
	l := NewListener(Config{})
	t.Cleanup(func() { _ = l.Close() })

	var offers atomic.Int32
	mux := http.NewServeMux()
	mux.Handle(SignalPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offers.Add(1)
		l.SignalHandler().ServeHTTP(w, r)
	}))
	signal := httptest.NewServer(mux)
	t.Cleanup(signal.Close)

	go func() {
		_ = http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("X-Echo", "rtc")
			_, _ = w.Write(body)
		}))
	}()

	d := NewDialer(Config{})
	t.Cleanup(func() { _ = d.Close() })
	tr := NewHTTPTransport(d)
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: 60 * time.Second}
	url := "http://" + strings.TrimPrefix(signal.URL, "http://") + "/echo"

	echo := func(body []byte) error {
		resp, err := client.Post(url, "application/octet-stream", bytes.NewReader(body))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		got, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status %s", resp.Status)
		}
		if resp.Header.Get("X-Echo") != "rtc" {
			return fmt.Errorf("response did not come from the data-channel server")
		}
		if !bytes.Equal(got, body) {
			return fmt.Errorf("echo mismatch: sent %d bytes, got %d", len(body), len(got))
		}
		return nil
	}

	if err := echo([]byte("hello over webrtc")); err != nil {
		t.Fatalf("small POST: %v", err)
	}
	if err := echo([]byte("second request")); err != nil {
		t.Fatalf("second sequential POST: %v", err)
	}
	if n := offers.Load(); n != 1 {
		t.Fatalf("offers after two sequential requests = %d, want 1", n)
	}

	big := make([]byte, 8<<20)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	if err := echo(big); err != nil {
		t.Fatalf("8 MiB POST: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- echo(bytes.Repeat([]byte{byte(i)}, 4096+i))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent POST: %v", err)
		}
	}
	if n := offers.Load(); n != 1 {
		t.Errorf("offers after all requests = %d, want 1", n)
	}
}
