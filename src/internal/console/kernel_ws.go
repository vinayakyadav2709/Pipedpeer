package console

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsWriteTimeout = 10 * time.Second
	wsPingInterval = 30 * time.Second
	wsPongTimeout  = 75 * time.Second
	// Largest client frame: a cell's source.
	wsMaxClientFrame = 16 << 20
)

// The guard middleware has already vetted the Origin, including the dev
// origin, which gorilla's same-host default would refuse.
var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  16 << 10,
	WriteBufferSize: 16 << 10,
	CheckOrigin:     func(*http.Request) bool { return true },
}

func (s *Server) handleKernelWS(w http.ResponseWriter, r *http.Request) {
	abs, err := s.resolveNotebook(r.URL.Query().Get("notebook"))
	if err != nil {
		writePathError(w, err)
		return
	}
	dir := filepath.Dir(abs)
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		dir = s.root
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	sess := s.kernels.session(s.relPath(abs), dir)
	c := newClient()
	sess.attach(c)
	defer sess.detach(c)

	go writeFrames(conn, c)
	readFrames(conn, c, sess)
}

func writeFrames(conn *websocket.Conn, c *client) {
	ping := time.NewTicker(wsPingInterval)
	defer ping.Stop()
	defer conn.Close()
	for {
		select {
		case <-c.gone:
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, ""), time.Now().Add(time.Second))
			return
		case frame := <-c.out:
			_ = conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				c.drop()
				return
			}
		case <-ping.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteTimeout)); err != nil {
				c.drop()
				return
			}
		}
	}
}

func readFrames(conn *websocket.Conn, c *client, sess *session) {
	defer c.drop()
	conn.SetReadLimit(wsMaxClientFrame)
	_ = conn.SetReadDeadline(time.Now().Add(wsPongTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsPongTimeout))
	})
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(wsPongTimeout))
		var f clientFrame
		if json.Unmarshal(raw, &f) != nil {
			continue
		}
		switch f.Type {
		case "execute":
			target := f.Target
			if target != targetCluster {
				target = targetLocal
			}
			sess.execute(f.CellID, f.Code, target)
		case "interrupt":
			sess.interrupt()
		case "restart":
			sess.restart()
		case "shutdown":
			sess.stop()
		case "complete":
			sess.complete(f.ReqID, f.Code, f.Cursor)
		case "inspect_vars":
			sess.inspectVars(f.ReqID)
		}
	}
}
