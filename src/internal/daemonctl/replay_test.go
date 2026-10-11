package daemonctl

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestReplayOutputSeparatesJobOutcomeFromFileProblems(t *testing.T) {
	var out strings.Builder
	saved := `{"o":"a\r","exit_code":0}` + "\n" + `{"o":"b\n","exit_code":0}` + "\n" + `{"done":true,"exit_code":3,"peak_mem_bytes":9}` + "\n"
	res, jobErr, err := ReplayOutput(strings.NewReader(saved), &out, io.Discard)
	var rexit *RemoteExitError
	if err != nil || !errors.As(jobErr, &rexit) || rexit.Code != 3 || res.PeakBytes != 9 || out.String() != "a\rb\n" {
		t.Fatalf("res=%+v jobErr=%v err=%v out=%q", res, jobErr, err, out.String())
	}
	if _, jobErr, err := ReplayOutput(strings.NewReader(`{"o":"x","exit_code":0}`+"\n"), io.Discard, io.Discard); err == nil || jobErr != nil {
		t.Fatalf("truncated file: jobErr=%v err=%v", jobErr, err)
	}
}

// A connection that breaks after the job started is ErrConnectionLost, so a
// single run can treat it as a detach instead of running the job again.
func TestStreamExecuteLostConnection(t *testing.T) {
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		if _, _, err := c.ReadMessage(); err != nil { // config frame
			t.Errorf("config: %v", err)
		}
		_ = c.WriteMessage(websocket.TextMessage, []byte(`{"o":"tick\n","exit_code":0}`))
		c.UnderlyingConn().Close() // no close frame: the network went away
	}))
	defer srv.Close()
	host, portStr, _ := strings.Cut(strings.TrimPrefix(srv.URL, "http://"), ":")
	port, _ := strconv.Atoi(portStr)
	_, err := StreamExecute(context.Background(), host, port, "j", ExecConfig{}, nil)
	if !errors.Is(err, ErrConnectionLost) || errors.Is(err, ErrDetached) {
		t.Fatalf("err = %v, want ErrConnectionLost", err)
	}
}
