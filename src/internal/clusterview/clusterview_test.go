package clusterview

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func serve(t *testing.T, mux *http.ServeMux) int {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().(*net.TCPAddr).Port
}

func TestFetchNodesDecodesDaemonReply(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nodes", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `[{"node_id":"n1","ssh_endpoint":"root@10.0.0.5:22","daemon_port":38080,
			"capabilities":{"hostname":"box","arch":"x86_64"},
			"load":{"cpu_percent":12.5,"memory_percent":40,"active_jobs":1,"queue_depth":0,"recent_failures":0,"total_cpus":16},
			"state":"healthy","health_score":0.98,"source":"discovery"}]`)
	})
	nodes, err := FetchNodes(context.Background(), serve(t, mux))
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes", len(nodes))
	}
	n := nodes[0]
	if n.NodeID != "n1" || n.DaemonPort != 38080 || n.State != "healthy" || n.HealthScore != 0.98 || n.Source != "discovery" {
		t.Errorf("node = %+v", n)
	}
	if n.Load.CPUPercent != 12.5 || n.Load.TotalCPUs != 16 || n.Load.ActiveJobs != 1 {
		t.Errorf("load = %+v", n.Load)
	}
	if !n.Healthy() || n.Address() != "10.0.0.5" || n.Hostname() != "box" {
		t.Errorf("Healthy=%v Address=%q Hostname=%q", n.Healthy(), n.Address(), n.Hostname())
	}
}

func TestFetchNodesDaemonDown(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	_, err = FetchNodes(context.Background(), port)
	if err == nil || !strings.HasPrefix(err.Error(), "daemon unreachable: ") {
		t.Errorf("err = %v, want one starting \"daemon unreachable: \"", err)
	}
	if _, err := FetchTasks(context.Background(), port); err == nil {
		t.Error("FetchTasks succeeded with no daemon")
	}
}

func TestFetchNodesRejectedReplyNamesStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nodes", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"missing or invalid X-Pipedpeer-Token"}`)
	})
	_, err := FetchNodes(context.Background(), serve(t, mux))
	want := "daemon answered 401 Unauthorized: missing or invalid X-Pipedpeer-Token"
	if err == nil || err.Error() != want {
		t.Errorf("err = %v, want %q", err, want)
	}
}

func TestFetchNodesBadReplyIsNotUnreachable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nodes", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `not json`) })
	_, err := FetchNodes(context.Background(), serve(t, mux))
	if err == nil || strings.Contains(err.Error(), "unreachable") {
		t.Errorf("err = %v, want a decode error that does not claim the daemon is down", err)
	}
}

func TestFetchTasksUnionsHealthyNodes(t *testing.T) {
	now := time.Now()
	lease := func(id string, age time.Duration) map[string]any {
		return map[string]any{
			"lease_id": id, "job_name": "job-" + id, "submitter_node": "sub", "state": "running",
			"mem_bytes": 2048, "gpu_index": 1, "created_at": now.Add(-age).UTC().Format(time.RFC3339Nano),
		}
	}
	worker := func(leases ...map[string]any) (int, *atomic.Int32) {
		var hits atomic.Int32
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/jobs", func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"node_id": "ignored", "jobs": leases})
		})
		return serve(t, mux), &hits
	}
	portA, _ := worker(lease("new", 10*time.Second), lease("old", 5*time.Minute))
	portB, _ := worker(lease("mid", time.Minute))
	portStale, staleHits := worker(lease("never", time.Hour))
	portBroken := serveBroken(t)

	nodes := []Node{
		{NodeID: "a", SSHEndpoint: "u@127.0.0.1:22", DaemonPort: portA, State: "healthy", Capabilities: map[string]string{"hostname": "host-a"}},
		{NodeID: "b", SSHEndpoint: "u@127.0.0.1:22", DaemonPort: portB, State: "healthy"},
		{NodeID: "s", SSHEndpoint: "u@127.0.0.1:22", DaemonPort: portStale, State: "stale"},
		{NodeID: "x", SSHEndpoint: "u@127.0.0.1:22", DaemonPort: portBroken, State: "healthy"},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nodes", func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(nodes) })

	tasks, err := FetchTasks(context.Background(), serve(t, mux))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, task := range tasks {
		got = append(got, task.NodeID+"/"+task.LeaseID)
	}
	if want := "a/old b/mid a/new"; strings.Join(got, " ") != want {
		t.Fatalf("tasks = %v, want %q (oldest first, stale and failing nodes skipped)", got, want)
	}
	if staleHits.Load() != 0 {
		t.Error("a stale node was queried for its jobs")
	}

	old := tasks[0]
	if old.Hostname != "host-a" || old.JobName != "job-old" || old.Submitter != "sub" || old.State != "running" ||
		old.MemBytes != 2048 || old.GPUIndex != 1 {
		t.Errorf("task = %+v", old)
	}
	if old.Age < 5*time.Minute || old.Age > 6*time.Minute {
		t.Errorf("age = %v, want about 5m", old.Age)
	}
	if tasks[1].Hostname != "127.0.0.1" {
		t.Errorf("hostname without a capability = %q, want the endpoint's host", tasks[1].Hostname)
	}
}

func serveBroken(t *testing.T) int {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"jobs": "nope"`) })
	return serve(t, mux)
}

func TestHostFromEndpoint(t *testing.T) {
	for endpoint, want := range map[string]string{
		"user@host:22":     "host",
		"root@10.0.0.5:22": "10.0.0.5",
		"host:2222":        "host",
		"host":             "host",
		"user@host":        "host",
		"":                 "127.0.0.1",
		"user@:22":         "127.0.0.1",
	} {
		if got := HostFromEndpoint(endpoint); got != want {
			t.Errorf("HostFromEndpoint(%q) = %q, want %q", endpoint, got, want)
		}
	}
}

func TestAddressPrefersActiveHost(t *testing.T) {
	n := Node{SSHEndpoint: "root@10.0.0.5:22", Host: "203.0.113.7", Path: "internet"}
	if got := n.Address(); got != "203.0.113.7" {
		t.Errorf("Address() = %q, want the active host", got)
	}
	n.Capabilities = map[string]string{}
	if got := n.Hostname(); got != "203.0.113.7" {
		t.Errorf("Hostname() = %q, want the active host when none is reported", got)
	}
}
