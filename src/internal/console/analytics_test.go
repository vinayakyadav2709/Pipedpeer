package console

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clusterview"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// fixtureJob describes one stored job relative to now.
type fixtureJob struct {
	id       string
	status   string
	ago      time.Duration
	duration time.Duration
	host     string
	edit     func(*jobRecord)
}

func writeHistory(t *testing.T, dir string, now time.Time, jobs []fixtureJob) {
	t.Helper()
	for _, j := range jobs {
		started := now.Add(-j.ago)
		rec := jobRecord{Record: jobhistory.Record{
			ID:         j.id,
			Status:     j.status,
			StartedAt:  started.UTC().Format(time.RFC3339Nano),
			ScriptPath: "/work/" + j.id + ".py",
			RunHost:    j.host,
		}}
		if jobhistory.IsTerminal(j.status) {
			rec.FinishedAt = started.Add(j.duration).UTC().Format(time.RFC3339Nano)
			rec.DurationMs = j.duration.Milliseconds()
		}
		if j.edit != nil {
			j.edit(&rec)
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		jobDir := filepath.Join(dir, j.id)
		if err := os.MkdirAll(jobDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(jobDir, metadataFile), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// historyFixture is six jobs: four in the last hour, one yesterday-ish, one
// last week. Durations are chosen so the percentiles are easy to check.
func historyFixture(t *testing.T) (dir string, now time.Time) {
	t.Helper()
	dir, now = t.TempDir(), time.Now()
	writeHistory(t, dir, now, []fixtureJob{
		{id: "j1", status: jobhistory.StateSucceeded, ago: 10 * time.Minute, duration: 1 * time.Second, host: "host-a",
			edit: func(r *jobRecord) {
				r.PlacementSource = "registry"
				r.EstimatedMemBytes, r.PeakMemBytes, r.EstimationTier = 200, 150, "historical"
				r.JobName = "train"
				r.Stages = []jobhistory.Stage{{Name: "nix build", Ms: 40}, {Name: "script", Ms: 900}}
			}},
		{id: "j2", status: jobhistory.StateSucceeded, ago: 20 * time.Minute, duration: 2 * time.Second, host: "host-a",
			edit: func(r *jobRecord) {
				r.PlacementSource = "registry"
				r.Stages = []jobhistory.Stage{{Name: "nix build", Ms: 60}, {Name: "script", Ms: 1900}}
			}},
		{id: "j3", status: jobhistory.StateFailed, ago: 30 * time.Minute, duration: 3 * time.Second, host: "host-b",
			edit: func(r *jobRecord) { r.PlacementSource = "self"; r.Error = "exit status 1" }},
		{id: "j4", status: jobhistory.StateRunning, ago: 5 * time.Minute, host: "host-b"},
		{id: "j5", status: jobhistory.StateSucceeded, ago: 20 * time.Hour, duration: 10 * time.Second, host: "host-a"},
		{id: "j6", status: jobhistory.StateCancelled, ago: 5 * 24 * time.Hour, duration: 4 * time.Second},
	})
	// Things in a real history directory that are not jobs.
	if err := os.MkdirAll(filepath.Join(dir, "nar-cache"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "broken"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken", metadataFile), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, now
}

func TestJobsListAndFilters(t *testing.T) {
	dir, _ := historyFixture(t)
	ts := newTestServer(t, Options{HistoryDir: dir})

	ids := func(query string) []string {
		var jobs []JobView
		ts.getJSON(t, "/api/jobs"+query, &jobs)
		out := []string{}
		for _, j := range jobs {
			out = append(out, j.ID)
		}
		return out
	}
	check := func(query string, want ...string) {
		t.Helper()
		if got := ids(query); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("GET /api/jobs%s = %v, want %v", query, got, want)
		}
	}
	check("", "j4", "j1", "j2", "j3", "j5", "j6")
	check("?limit=2", "j4", "j1")
	check("?status=succeeded", "j1", "j2", "j5")
	check("?status=failed", "j3")
	check("?q=HOST-B", "j4", "j3")
	check("?q=exit+status", "j3")
	check("?status=succeeded&q=j5", "j5")
	check("?q=nothing-matches")

	resp, body := ts.do(t, http.MethodGet, "/api/jobs?limit=0", "", nil)
	wantStatus(t, resp, body, http.StatusBadRequest)

	var jobs []JobView
	ts.getJSON(t, "/api/jobs?q=j3", &jobs)
	j := jobs[0]
	if j.DurationMs != 3000 || j.FinishedAt-j.StartedAt != 3000 || j.Error != "exit status 1" || j.RunHost != "host-b" {
		t.Errorf("j3 = %+v", j)
	}
	ts.getJSON(t, "/api/jobs?q=j4", &jobs)
	if jobs[0].FinishedAt != 0 || jobs[0].DurationMs != 0 {
		t.Errorf("running job reports finished_at=%d duration_ms=%d, want 0 for both", jobs[0].FinishedAt, jobs[0].DurationMs)
	}
}

func TestJobsEmptyHistoryIsEmptyArray(t *testing.T) {
	ts := newTestServer(t, Options{HistoryDir: filepath.Join(t.TempDir(), "never-created")})
	resp, body := ts.do(t, http.MethodGet, "/api/jobs", "", nil)
	wantStatus(t, resp, body, http.StatusOK)
	if string(body) != "[]\n" {
		t.Errorf("empty history = %q, want []", body)
	}
}

func TestJobDetail(t *testing.T) {
	dir, _ := historyFixture(t)
	big := strings.Repeat("0123456789abcde\n", 8<<10) // 128 KiB of 16-byte lines
	for name, content := range map[string]string{
		"stdout.log": big + "last stdout line\n",
		"stderr.log": "a warning\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, "j3", name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ts := newTestServer(t, Options{HistoryDir: dir})

	resp, body := ts.do(t, http.MethodGet, "/api/jobs/j3", "", nil)
	wantStatus(t, resp, body, http.StatusOK)
	var detail JobDetail
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatal(err)
	}
	if detail.ID != "j3" || detail.Status != jobhistory.StateFailed {
		t.Errorf("detail = %+v", detail.JobView)
	}
	if len(detail.LogTail) > logTailBytes {
		t.Errorf("log_tail is %d bytes, want at most %d", len(detail.LogTail), logTailBytes)
	}
	if !strings.HasSuffix(detail.LogTail, "last stdout line\n--- stderr ---\na warning\n") {
		t.Errorf("log_tail ends %q", detail.LogTail[len(detail.LogTail)-80:])
	}
	if !strings.HasPrefix(detail.LogTail, "0123456789abcde\n") {
		t.Errorf("log_tail starts mid-line: %q", detail.LogTail[:32])
	}
	if !strings.Contains(string(body), `"events":[]`) {
		t.Errorf("events should be an empty array, body: %.200s", body)
	}

	var quiet JobDetail
	ts.getJSON(t, "/api/jobs/j1", &quiet)
	if quiet.LogTail != "" {
		t.Errorf("job with no stored output: log_tail = %q, want empty", quiet.LogTail)
	}

	for _, id := range []string{"nope", "nar-cache", "broken", "..", "%2e%2e%2fj1"} {
		resp, body := ts.do(t, http.MethodGet, "/api/jobs/"+id, "", nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET /api/jobs/%s: status %d, want 404 (body %.100s)", id, resp.StatusCode, body)
		}
	}
}

func TestAnalyticsAggregation(t *testing.T) {
	dir, now := historyFixture(t)
	ts := newTestServer(t, Options{HistoryDir: dir})

	var a Analytics
	ts.getJSON(t, "/api/analytics?range=1h", &a)
	if a.Range != "1h" || a.BucketMs != (5*time.Minute).Milliseconds() {
		t.Errorf("range/bucket = %s/%d", a.Range, a.BucketMs)
	}
	var succeeded, failed, other int
	for i, p := range a.Throughput {
		succeeded, failed, other = succeeded+p.Succeeded, failed+p.Failed, other+p.Other
		if p.T%a.BucketMs != 0 {
			t.Errorf("throughput bucket %d is not aligned to bucket_ms", p.T)
		}
		if i > 0 && p.T-a.Throughput[i-1].T != a.BucketMs {
			t.Errorf("throughput has a gap between %d and %d", a.Throughput[i-1].T, p.T)
		}
	}
	if succeeded != 2 || failed != 1 || other != 1 {
		t.Errorf("1h throughput totals succeeded/failed/other = %d/%d/%d, want 2/1/1", succeeded, failed, other)
	}
	if n := len(a.Throughput); n < 12 || n > 13 {
		t.Errorf("1h throughput has %d buckets, want the whole hour (12 or 13)", n)
	}
	// j1, j2 and j3 finished, each in its own 5-minute bucket; the running j4
	// has no duration and must not appear.
	if len(a.Durations) != 3 {
		t.Fatalf("durations = %+v, want 3 buckets", a.Durations)
	}
	wantP50 := map[int64]int64{}
	for id, ms := range map[time.Duration]int64{10 * time.Minute: 1000, 20 * time.Minute: 2000, 30 * time.Minute: 3000} {
		wantP50[now.Add(-id).UnixMilli()/a.BucketMs*a.BucketMs] = ms
	}
	for _, d := range a.Durations {
		if d.Count != 1 || d.P50Ms != wantP50[d.T] || d.P95Ms != wantP50[d.T] {
			t.Errorf("duration bucket %+v, want count 1 and p50=p95=%d", d, wantP50[d.T])
		}
	}

	wantNodes := []NodeJobs{
		{Node: "host-a", Jobs: 2, Failed: 0, TotalDurationMs: 3000},
		{Node: "host-b", Jobs: 2, Failed: 1, TotalDurationMs: 3000},
	}
	if fmt.Sprint(a.ByNode) != fmt.Sprint(wantNodes) {
		t.Errorf("by_node = %+v, want %+v", a.ByNode, wantNodes)
	}
	wantStatuses := []StatusCount{{"succeeded", 2}, {"failed", 1}, {"running", 1}}
	if fmt.Sprint(a.ByStatus) != fmt.Sprint(wantStatuses) {
		t.Errorf("by_status = %+v, want %+v", a.ByStatus, wantStatuses)
	}
	wantPlacement := []PlacementCount{{"registry", 2}, {"self", 1}}
	if fmt.Sprint(a.ByPlacement) != fmt.Sprint(wantPlacement) {
		t.Errorf("by_placement = %+v, want %+v (jobs with no recorded source are not counted)", a.ByPlacement, wantPlacement)
	}
	wantMem := []MemEstimate{{ID: "j1", JobName: "train", Tier: "historical", EstimatedBytes: 200, PeakBytes: 150}}
	if fmt.Sprint(a.MemEstimates) != fmt.Sprint(wantMem) {
		t.Errorf("mem_estimates = %+v, want %+v", a.MemEstimates, wantMem)
	}
	wantStages := []StageTime{{"nix build", 100}, {"script", 2800}}
	if fmt.Sprint(a.StageTime) != fmt.Sprint(wantStages) {
		t.Errorf("stage_time = %+v, want %+v", a.StageTime, wantStages)
	}

	count := func(rangeName string) int {
		var a Analytics
		ts.getJSON(t, "/api/analytics?range="+rangeName, &a)
		n := 0
		for _, s := range a.ByStatus {
			n += s.Count
		}
		return n
	}
	for rangeName, want := range map[string]int{"1h": 4, "24h": 5, "7d": 6, "30d": 6, "all": 6} {
		if got := count(rangeName); got != want {
			t.Errorf("range %s covers %d jobs, want %d", rangeName, got, want)
		}
	}

	resp, body := ts.do(t, http.MethodGet, "/api/analytics?range=2h", "", nil)
	wantStatus(t, resp, body, http.StatusBadRequest)
}

func TestAnalyticsEmptySectionsAreArrays(t *testing.T) {
	ts := newTestServer(t, Options{})
	resp, body := ts.do(t, http.MethodGet, "/api/analytics", "", nil)
	wantStatus(t, resp, body, http.StatusOK)
	for _, section := range []string{"throughput", "durations", "by_node", "by_status", "by_placement", "mem_estimates", "stage_time"} {
		if !strings.Contains(string(body), `"`+section+`":[]`) {
			t.Errorf("section %s is not an empty array in %s", section, body)
		}
	}
	if !strings.Contains(string(body), `"range":"24h"`) {
		t.Errorf("default range is not 24h: %s", body)
	}
}

func TestPercentileNearestRank(t *testing.T) {
	hundred := make([]int64, 100)
	for i := range hundred {
		hundred[i] = int64(i + 1)
	}
	cases := []struct {
		sorted []int64
		p      float64
		want   int64
	}{
		{nil, 50, 0},
		{[]int64{7}, 50, 7},
		{[]int64{7}, 95, 7},
		{[]int64{1, 2}, 50, 1},
		{[]int64{1, 2, 3, 4}, 50, 2},
		{[]int64{1, 2, 3, 4}, 95, 4},
		{hundred, 50, 50},
		{hundred, 95, 95},
	}
	for _, c := range cases {
		if got := percentile(c.sorted, c.p); got != c.want {
			t.Errorf("percentile(%d values, %v) = %d, want %d", len(c.sorted), c.p, got, c.want)
		}
	}
}

// fakeDaemon serves /v1/nodes and /v1/jobs the way a daemon does and returns
// the port it listens on.
func fakeDaemon(t *testing.T, nodes func(port int) []clusterview.Node, jobs string) int {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	port := srv.Listener.Addr().(*net.TCPAddr).Port
	mux.HandleFunc("/v1/nodes", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(nodes(port))
	})
	mux.HandleFunc("/v1/jobs", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(jobs))
	})
	return port
}

func clusterFixture(port int) []clusterview.Node {
	return []clusterview.Node{
		{
			NodeID: "node-self", SSHEndpoint: "me@127.0.0.1:22", DaemonPort: port, State: "healthy",
			HealthScore: 0.9, Source: "self", Capabilities: map[string]string{"hostname": "alpha"},
			Load: registry.LoadInfo{
				CPUPercent: 25, MemPercent: 50, ActiveJobs: 2, QueueDepth: 1, TotalCPUs: 8,
				TotalMemBytes: 16 << 30, AvailableMemBytes: 8 << 30,
				GPUs: []registry.PerGPUInfo{
					{Index: 0, MemoryTotalBytes: 8 << 30, MemoryFreeBytes: 6 << 30, UtilizationGPU: 40},
					{Index: 1, MemoryTotalBytes: 8 << 30, MemoryFreeBytes: 8 << 30, UtilizationGPU: 0},
				},
			},
		},
		{
			NodeID: "node-cpu", SSHEndpoint: "me@127.0.0.1:22", DaemonPort: port, State: "healthy",
			Source: "discovery", Capabilities: map[string]string{"hostname": "beta"},
			Load: registry.LoadInfo{CPUPercent: 75, MemPercent: 10, TotalCPUs: 4, TotalMemBytes: 8 << 30, AvailableMemBytes: 7 << 30},
		},
		{
			NodeID: "node-gone", SSHEndpoint: "me@192.0.2.1:22", DaemonPort: 1, State: "stale",
			Source: "discovery", Capabilities: map[string]string{"hostname": "gamma"},
			Load: registry.LoadInfo{CPUPercent: 99, TotalCPUs: 64, TotalMemBytes: 256 << 30, ActiveJobs: 9},
		},
	}
}

func TestOverviewNodesAndTasks(t *testing.T) {
	dir, _ := historyFixture(t)
	created := time.Now().Add(-90 * time.Second).UTC().Format(time.RFC3339Nano)
	port := fakeDaemon(t, clusterFixture, `{"node_id":"x","jobs":[{"lease_id":"L1","job_name":"train","submitter_node":"node-self","state":"running","mem_bytes":1024,"gpu_index":-1,"created_at":"`+created+`"}]}`)
	ts := newTestServer(t, Options{HistoryDir: dir, DaemonPort: port, SelfNodeID: "node-self", SelfHostname: "alpha"})

	var o Overview
	ts.getJSON(t, "/api/overview", &o)
	if o.Self != (OverviewSelf{NodeID: "node-self", Hostname: "alpha"}) {
		t.Errorf("self = %+v", o.Self)
	}
	if o.Daemon != (OverviewDaemon{Reachable: true, Port: port}) {
		t.Errorf("daemon = %+v", o.Daemon)
	}
	wantCluster := OverviewCluster{
		NodesTotal: 3, NodesHealthy: 2, CPUsTotal: 12,
		MemTotalBytes: 24 << 30, MemAvailableBytes: 15 << 30,
		GPUsTotal: 2, GPUMemTotalBytes: 16 << 30, GPUMemUsedBytes: 2 << 30,
		ActiveJobs: 2, QueueDepth: 1,
	}
	if o.Cluster != wantCluster {
		t.Errorf("cluster = %+v\n       want %+v (the stale node's capacity must not count)", o.Cluster, wantCluster)
	}
	// Finished durations are 1s, 2s, 3s, 4s, 10s.
	wantJobs := OverviewJobs{Total: 6, Running: 1, Succeeded: 3, Failed: 1, SuccessRate: 0.75, P50DurationMs: 3000, P95DurationMs: 10000}
	if o.Jobs != wantJobs {
		t.Errorf("jobs = %+v, want %+v", o.Jobs, wantJobs)
	}

	var nodes []NodeView
	ts.getJSON(t, "/api/nodes", &nodes)
	if len(nodes) != 3 {
		t.Fatalf("got %d nodes, want 3", len(nodes))
	}
	if n := nodes[0]; !n.IsSelf || n.Hostname != "alpha" || n.HealthScore != 0.9 || n.Load.TotalCPUs != 8 || len(n.Load.GPUs) != 2 {
		t.Errorf("self node = %+v", n)
	}
	if nodes[1].IsSelf || nodes[2].State != "stale" {
		t.Errorf("other nodes = %+v", nodes[1:])
	}

	// Both healthy nodes point at the fake daemon, so its one lease is
	// reported once per node; the stale node is never asked.
	var tasks []TaskView
	ts.getJSON(t, "/api/tasks", &tasks)
	if len(tasks) != 2 {
		t.Fatalf("got %d tasks, want 2: %+v", len(tasks), tasks)
	}
	for _, task := range tasks {
		if task.LeaseID != "L1" || task.JobName != "train" || task.Submitter != "node-self" || task.State != "running" ||
			task.MemBytes != 1024 || task.GPUIndex != -1 {
			t.Errorf("task = %+v", task)
		}
		if task.AgeMs < 90_000 || task.AgeMs > 150_000 {
			t.Errorf("age_ms = %d, want about 90000", task.AgeMs)
		}
	}
}

func TestDaemonDownIsReportedNotInvented(t *testing.T) {
	// A port nothing listens on.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	ts := newTestServer(t, Options{DaemonPort: port})

	var o Overview
	ts.getJSON(t, "/api/overview", &o)
	if o.Daemon.Reachable || o.Daemon.Error == "" || o.Daemon.Port != port {
		t.Errorf("daemon = %+v, want unreachable with an error", o.Daemon)
	}
	if o.Cluster != (OverviewCluster{}) {
		t.Errorf("cluster = %+v, want all zero with no daemon", o.Cluster)
	}
	for _, path := range []string{"/api/nodes", "/api/tasks"} {
		resp, body := ts.do(t, http.MethodGet, path, "", nil)
		wantStatus(t, resp, body, http.StatusBadGateway)
		wantErrorBody(t, body)
	}
}

func TestMetricsNullMeansNoReading(t *testing.T) {
	b := newMetricsBuffer(5 * time.Second)
	now := time.Now()
	nodes := clusterFixture(1)

	b.record(now.Add(-10*time.Second), nodes, true)
	b.record(now.Add(-5*time.Second), nil, false) // the daemon did not answer
	b.record(now, nodes, true)

	raw, err := json.Marshal(b.snapshot(now, 5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		IntervalMs int64 `json:"interval_ms"`
		Series     []struct {
			NodeID   string           `json:"node_id"`
			Hostname string           `json:"hostname"`
			Points   []map[string]any `json:"points"`
		} `json:"series"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.IntervalMs != 5000 || len(m.Series) != 3 {
		t.Fatalf("interval %d, %d series; want 5000 and 3: %s", m.IntervalMs, len(m.Series), raw)
	}
	alpha, beta, gamma := m.Series[0], m.Series[1], m.Series[2]
	if alpha.Hostname != "alpha" || beta.Hostname != "beta" || gamma.Hostname != "gamma" {
		t.Fatalf("series order = %s, %s, %s", alpha.Hostname, beta.Hostname, gamma.Hostname)
	}

	// GPU 0 uses 2 of 8 GiB and GPU 1 none: 2/16 of memory, 20% mean util.
	first := alpha.Points[0]
	for key, want := range map[string]any{"cpu": 25.0, "mem": 50.0, "gpu_util": 20.0, "gpu_mem": 12.5, "active_jobs": 2.0} {
		if first[key] != want {
			t.Errorf("alpha %s = %v, want %v", key, first[key], want)
		}
	}
	if p := beta.Points[0]; p["cpu"] != 75.0 || p["gpu_util"] != nil || p["gpu_mem"] != nil {
		t.Errorf("node without a GPU: point = %v, want cpu 75 and null GPU readings", p)
	}
	for _, key := range []string{"cpu", "mem", "gpu_util", "gpu_mem", "active_jobs"} {
		if v, present := alpha.Points[1][key]; !present || v != nil {
			t.Errorf("while the daemon was down, alpha %s = %v (present=%v), want null", key, v, present)
		}
		if v := gamma.Points[0][key]; v != nil {
			t.Errorf("stale node %s = %v, want null: its load is not a current reading", key, v)
		}
	}
	if len(alpha.Points) != 3 {
		t.Errorf("alpha has %d points, want 3", len(alpha.Points))
	}

	if got := b.snapshot(now, 7*time.Second).Series[0].Points; len(got) != 2 {
		t.Errorf("7s window holds %d points, want the 2 newest", len(got))
	}
}

func TestMetricsRingKeepsNewest(t *testing.T) {
	b := newMetricsBuffer(time.Hour) // six hours of retention is six points
	now := time.Now()
	nodes := clusterFixture(1)[:1]
	for i := 9; i >= 0; i-- {
		b.record(now.Add(-time.Duration(i)*time.Minute), nodes, true)
	}
	points := b.snapshot(now, metricsRetention).Series[0].Points
	if len(points) != 6 {
		t.Fatalf("ring holds %d points, want 6", len(points))
	}
	for i, p := range points {
		want := now.Add(-time.Duration(5-i) * time.Minute).UnixMilli()
		if p.T != want {
			t.Errorf("point %d has t=%d, want %d (oldest first, newest kept)", i, p.T, want)
		}
	}
}

func TestMetricsEndpointSamplesDaemon(t *testing.T) {
	port := fakeDaemon(t, clusterFixture, `{"jobs":[]}`)
	ts := newTestServer(t, Options{DaemonPort: port, SampleInterval: 20 * time.Millisecond})
	go ts.sampleLoop(t.Context())

	deadline := time.Now().Add(testTimeout)
	for {
		var m Metrics
		ts.getJSON(t, "/api/metrics?window=5m", &m)
		if len(m.Series) == 3 && len(m.Series[0].Points) >= 2 {
			if m.IntervalMs != 20 {
				t.Errorf("interval_ms = %d, want 20", m.IntervalMs)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sampler produced no series: %+v", m)
		}
		time.Sleep(10 * time.Millisecond)
	}

	resp, body := ts.do(t, http.MethodGet, "/api/metrics?window=2h", "", nil)
	wantStatus(t, resp, body, http.StatusBadRequest)
}
