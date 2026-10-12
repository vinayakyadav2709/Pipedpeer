package app

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
	"github.com/rs/zerolog"
)

// fakeNode is a daemon holding one detached job, reporting status, its
// recorded output over the attach socket, and a results tar with one file.
func fakeNode(t *testing.T, jobID string, status *atomic.Value) *httptest.Server {
	t.Helper()
	up := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs/"+jobID+"/status", func(w http.ResponseWriter, r *http.Request) {
		st := status.Load().(string)
		fin := ""
		if st != "running" {
			fin = "2026-10-12T10:00:00Z"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"job_id": jobID, "status": st, "exit_code": 0, "detached": true, "finished_at": fin})
	})
	mux.HandleFunc("/v1/jobs/"+jobID+"/attach", func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		for _, f := range []string{`{"o":"tick 0\n","exit_code":0}`, `{"e":"warn\n","exit_code":0}`,
			`{"done":true,"exit_code":0,"peak_mem_bytes":1234,"timing":{"import_ms":1,"setup_ms":2,"run_ms":30}}`} {
			if err := c.WriteMessage(websocket.TextMessage, []byte(f)); err != nil {
				t.Errorf("write frame: %v", err)
				return
			}
		}
		_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"))
	})
	mux.HandleFunc("/v1/jobs/"+jobID+"/results", func(w http.ResponseWriter, r *http.Request) {
		tw := tar.NewWriter(w)
		body := []byte("result\n")
		if err := tw.WriteHeader(&tar.Header{Name: "out/result.txt", Mode: 0644, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Errorf("tar header: %v", err)
		}
		_, _ = tw.Write(body)
		_ = tw.Close()
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// detachedRecord writes a history record for a detached job on endpoint and
// returns its dir and the project folder results go to.
func detachedRecord(t *testing.T, jobID, endpoint string) (string, string) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	project := t.TempDir()
	rec, dir, err := jobhistory.NewRecord(filepath.Join(project, "main.py"), "", "", true, false)
	if err != nil {
		t.Fatal(err)
	}
	rec.RemoteJobID, rec.DaemonEndpoint, rec.LocalSyncRoot = jobID, endpoint, project
	if err := jobhistory.SaveRecord(dir, rec); err != nil {
		t.Fatal(err)
	}
	return dir, project
}

func TestWatcherCollectsFinishedDetachedJob(t *testing.T) {
	var status atomic.Value
	status.Store("running")
	srv := fakeNode(t, "j1", &status)
	dir, project := detachedRecord(t, "j1", strings.TrimPrefix(srv.URL, "http://"))
	log := zerolog.Nop()
	warned := map[string]bool{}

	collectDetached(context.Background(), log, warned, deadPort(t))
	if r, _ := jobhistory.ReadRecordAt(dir); r.Collected || r.Status != jobhistory.StateRunning {
		t.Fatalf("running job collected early: %+v", r)
	}

	status.Store("done")
	collectDetached(context.Background(), log, warned, deadPort(t))
	r, err := jobhistory.ReadRecordAt(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Collected || r.Status != jobhistory.StateSucceeded || r.PeakMemBytes != 1234 || r.ReceivedFiles != 1 {
		t.Fatalf("record after collection: %+v", r)
	}
	if b, err := os.ReadFile(filepath.Join(project, "out", "result.txt")); err != nil || string(b) != "result\n" {
		t.Fatalf("results not in the project folder: %q %v", b, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "stdout.log")); string(b) != "tick 0\n" {
		t.Fatalf("stdout.log = %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, jobhistory.CollectedOutput)); strings.Count(string(b), "\n") != 3 {
		t.Fatalf("output.jsonl = %q", b)
	}

	// Idempotent: a second pass and attach never sync again.
	if err := os.Remove(filepath.Join(project, "out", "result.txt")); err != nil {
		t.Fatal(err)
	}
	collectDetached(context.Background(), log, warned, deadPort(t))

	// Attach replays from disk with the node gone.
	srv.Close()
	if err := Attach(context.Background(), r.ID); err != nil {
		t.Fatalf("attach of a collected job: %v", err)
	}
	if _, err := os.Stat(filepath.Join(project, "out", "result.txt")); !os.IsNotExist(err) {
		t.Fatalf("results applied a second time (stat err %v)", err)
	}
}

func TestWatcherRetriesUnreachableNode(t *testing.T) {
	noNetwork(t)
	dir, _ := detachedRecord(t, "j2", deadAddr(t))
	warned := map[string]bool{}
	for i := 0; i < 2; i++ {
		collectDetached(context.Background(), zerolog.Nop(), warned, deadPort(t))
	}
	r, err := jobhistory.ReadRecordAt(dir)
	if err != nil {
		t.Fatal(err)
	}
	if r.Collected || r.Status != jobhistory.StateRunning {
		t.Fatalf("unreachable job marked: %+v", r)
	}
	if !warned[r.ID] {
		t.Fatal("no warning recorded for the unreachable node")
	}
}

// deadAddr is a local address nothing listens on.
func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func deadPort(t *testing.T) int {
	_, p, _ := net.SplitHostPort(deadAddr(t))
	n, _ := strconv.Atoi(p)
	return n
}

// localDaemon fakes this machine's daemon listing a self node and one peer at
// peerAddr; it returns the daemon's port.
func localDaemon(t *testing.T, peerAddr string) int {
	t.Helper()
	ph, pp, _ := net.SplitHostPort(peerAddr)
	peerPort, _ := strconv.Atoi(pp)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"ssh_endpoint": "me@127.0.0.1:22", "daemon_port": deadPort(t), "source": "self"},
			{"ssh_endpoint": "user@" + ph + ":22", "daemon_port": peerPort, "source": "discovery"},
		})
	}))
	t.Cleanup(srv.Close)
	_, p, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	n, _ := strconv.Atoi(p)
	return n
}

// livePeer is a peer that answers /health.
func livePeer(t *testing.T) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func fastProbes(t *testing.T) {
	tries, gap := lostProbeTries, lostProbeGap
	lostProbeTries, lostProbeGap = 2, 10*time.Millisecond
	t.Cleanup(func() { lostProbeTries, lostProbeGap = tries, gap })
	noNetwork(t)
}

// noNetwork stands in for a machine with no default route and no
// introducer, so only peers can show it is online.
func noNetwork(t *testing.T) {
	gw, ports, intro := gatewayAddr, gatewayPorts, introducerAddr
	gatewayAddr = func() (netip.Addr, error) { return netip.Addr{}, errors.New("no default route") }
	introducerAddr = func() string { return "" }
	t.Cleanup(func() { gatewayAddr, gatewayPorts, introducerAddr = gw, ports, intro })
}

// gatewayUp makes 127.0.0.1 the default gateway, answering by refusing a
// connect, as most routers do on a closed port.
func gatewayUp(t *testing.T) {
	gatewayAddr = func() (netip.Addr, error) { return netip.MustParseAddr("127.0.0.1"), nil }
	gatewayPorts = []string{strconv.Itoa(deadPort(t))}
}

// fakeIntroducer runs a rendezvous server on a local UDP port.
func fakeIntroducer(t *testing.T) string {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go rendezvous.NewServer(time.Minute).Serve(pc, stop)
	t.Cleanup(func() { close(stop) })
	return pc.LocalAddr().String()
}

// Two machines: the only other one is the node, so no peer can show this
// machine is online; the gateway or the introducer must.
func TestJudgeLostConnectionTwoMachines(t *testing.T) {
	fastProbes(t)
	dh, dp, _ := net.SplitHostPort(deadAddr(t))
	deadNodePort, _ := strconv.Atoi(dp)
	self := localDaemon(t, net.JoinHostPort(dh, dp)) // the node is the only peer

	if v, why := judgeLostConnection(dh, deadNodePort, "j", self); v != lostDetach {
		t.Fatalf("no route: verdict %d (%s), want detach", v, why)
	}
	gatewayUp(t)
	if v, why := judgeLostConnection(dh, deadNodePort, "j", self); v != lostRerun || !strings.Contains(why, "gateway") {
		t.Fatalf("gateway up: verdict %d (%s), want rerun", v, why)
	}
	// An internet node (loopback forwarder) with an introducer set: only
	// the introducer speaks for the uplink.
	introducerAddr = func() string { return deadAddr(t) }
	if v, why := judgeLostConnection(dh, deadNodePort, "j", self); v != lostDetach {
		t.Fatalf("introducer silent: verdict %d (%s), want detach", v, why)
	}
	intro := fakeIntroducer(t)
	introducerAddr = func() string { return intro }
	if v, why := judgeLostConnection(dh, deadNodePort, "j", self); v != lostRerun || !strings.Contains(why, "introducer") {
		t.Fatalf("introducer answers: verdict %d (%s), want rerun", v, why)
	}
}

func TestJudgeLostConnection(t *testing.T) {
	fastProbes(t)
	dead := deadAddr(t)
	dh, dp, _ := net.SplitHostPort(dead)
	deadNodePort, _ := strconv.Atoi(dp)

	// The node died while a peer still answers: the failover reruns it.
	if v, why := judgeLostConnection(dh, deadNodePort, "j", localDaemon(t, livePeer(t))); v != lostRerun {
		t.Fatalf("node dead, peer up: verdict %d (%s), want rerun", v, why)
	}
	// Nothing answers: this machine is offline, the job runs on.
	if v, why := judgeLostConnection(dh, deadNodePort, "j", localDaemon(t, deadAddr(t))); v != lostDetach {
		t.Fatalf("offline: verdict %d (%s), want detach", v, why)
	}
	// The node answers: running → detach, ended → collect.
	var status atomic.Value
	status.Store("running")
	node := fakeNode(t, "j3", &status)
	nh, np, _ := net.SplitHostPort(strings.TrimPrefix(node.URL, "http://"))
	nodePort, _ := strconv.Atoi(np)
	if v, _ := judgeLostConnection(nh, nodePort, "j3", deadPort(t)); v != lostDetach {
		t.Fatalf("node runs the job: verdict %d, want detach", v)
	}
	status.Store("done")
	if v, _ := judgeLostConnection(nh, nodePort, "j3", deadPort(t)); v != lostEnded {
		t.Fatalf("job ended: verdict %d, want ended", v)
	}
	// The node answers but lost the job: rerun.
	if v, _ := judgeLostConnection(nh, nodePort, "other", deadPort(t)); v != lostRerun {
		t.Fatalf("unknown job: verdict %d, want rerun", v)
	}
}

func TestWatcherFailsJobWhoseNodeIsGone(t *testing.T) {
	saved := goneAfter
	goneAfter = 0
	t.Cleanup(func() { goneAfter = saved })
	noNetwork(t)
	dir, _ := detachedRecord(t, "j4", deadAddr(t))
	self := localDaemon(t, livePeer(t))
	warned := map[string]bool{}

	collectDetached(context.Background(), zerolog.Nop(), warned, self)
	r, _ := jobhistory.ReadRecordAt(dir)
	if r.UnreachableSince == "" || r.Status != jobhistory.StateRunning {
		t.Fatalf("first unreachable tick: %+v", r)
	}
	collectDetached(context.Background(), zerolog.Nop(), warned, self)
	r, _ = jobhistory.ReadRecordAt(dir)
	if r.Status != jobhistory.StateFailed || !strings.Contains(r.Error, "gone") || !strings.Contains(r.Error, "pipedpeer run") {
		t.Fatalf("node gone past goneAfter: %+v", r)
	}
	if err := Attach(context.Background(), r.ID); err == nil || !strings.Contains(err.Error(), "gone") {
		t.Fatalf("attach of a lost job: %v", err)
	}
}

// Two machines: the node is the only peer, so the gateway shows this
// machine is online and the job fails rather than waiting forever.
func TestWatcherFailsJobWhoseNodeIsGoneTwoMachines(t *testing.T) {
	saved := goneAfter
	goneAfter = 0
	t.Cleanup(func() { goneAfter = saved })
	noNetwork(t)
	node := deadAddr(t)
	dir, _ := detachedRecord(t, "j5", node)
	self := localDaemon(t, node)
	warned := map[string]bool{}

	collectDetached(context.Background(), zerolog.Nop(), warned, self)
	if r, _ := jobhistory.ReadRecordAt(dir); r.UnreachableSince != "" {
		t.Fatalf("offline machine started the gone clock: %+v", r)
	}
	gatewayUp(t)
	collectDetached(context.Background(), zerolog.Nop(), warned, self)
	collectDetached(context.Background(), zerolog.Nop(), warned, self)
	if r, _ := jobhistory.ReadRecordAt(dir); r.Status != jobhistory.StateFailed || !strings.Contains(r.Error, "gone") {
		t.Fatalf("node gone, gateway up: %+v", r)
	}
}
