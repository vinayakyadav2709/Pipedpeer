package daemonctl

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeExec is a daemon exec socket: it reads the config, sends one output
// frame, then reports every later client frame (or the close) on got.
func fakeExec(t *testing.T, onCancel func(*websocket.Conn)) (host string, port int, got chan string) {
	t.Helper()
	got = make(chan string, 4)
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		var cfg ExecConfig
		if err := c.ReadJSON(&cfg); err != nil {
			t.Errorf("config: %v", err)
			return
		}
		got <- "lease=" + cfg.LeaseID
		_ = c.WriteJSON(outputMessage{O: "started\n"})
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				var ce *websocket.CloseError
				if errors.As(err, &ce) {
					got <- "close " + strconv.Itoa(ce.Code)
				} else {
					got <- "read error"
				}
				return
			}
			got <- string(msg)
			if onCancel != nil {
				onCancel(c)
			}
		}
	}))
	t.Cleanup(srv.Close)
	h, p, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ = strconv.Atoi(p)
	return h, port, got
}

func next(t *testing.T, got chan string) string {
	t.Helper()
	select {
	case s := <-got:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("server saw nothing")
		return ""
	}
}

func TestDetachClosesWith4001AndNoCancel(t *testing.T) {
	host, port, got := fakeExec(t, nil)
	detach := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := StreamExecute(context.Background(), host, port, "job1", ExecConfig{LeaseID: "L1"}, detach)
		done <- err
	}()
	if s := next(t, got); s != "lease=L1" {
		t.Fatalf("config frame: %q", s)
	}
	close(detach)
	if s := next(t, got); s != "close 4001" {
		t.Fatalf("server saw %q, want close 4001 and no cancel frame", s)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrDetached) {
			t.Fatalf("err = %v, want ErrDetached", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StreamExecute did not return after detach")
	}
}

func TestCancelSendsCancelFrameAndWaitsForDone(t *testing.T) {
	host, port, got := fakeExec(t, func(c *websocket.Conn) {
		_ = c.WriteJSON(outputMessage{Done: true, ExitCode: 143, Cancelled: true})
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := StreamExecute(ctx, host, port, "job2", ExecConfig{}, nil)
		done <- err
	}()
	next(t, got) // config
	cancel()
	if s := strings.TrimSpace(next(t, got)); s != `{"cancel":true}` {
		t.Fatalf("server saw %q, want the cancel frame", s)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StreamExecute did not return after cancel")
	}
}
