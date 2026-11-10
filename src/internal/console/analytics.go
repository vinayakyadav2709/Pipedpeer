package console

import (
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/pipedpeer/pipedpeer/internal/clusterview"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

const (
	defaultJobsLimit = 200
	maxJobsLimit     = 5000
	memEstimatesCap  = 200
	defaultRange     = "24h"
	rangeAll         = "all"
	// The `all` range uses daily buckets up to this span, weekly beyond it.
	dailyBucketSpan = 90 * 24 * time.Hour
)

// analyticsRange is one accepted value of ?range=: how far back it looks and
// how wide its buckets are.
type analyticsRange struct {
	span   time.Duration
	bucket time.Duration
}

var analyticsRanges = map[string]analyticsRange{
	"1h":  {time.Hour, 5 * time.Minute},
	"24h": {24 * time.Hour, time.Hour},
	"7d":  {7 * 24 * time.Hour, 6 * time.Hour},
	"30d": {30 * 24 * time.Hour, 24 * time.Hour},
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	out := Overview{
		Self:   OverviewSelf{NodeID: s.opts.SelfNodeID, Hostname: s.opts.SelfHostname},
		Daemon: OverviewDaemon{Port: s.opts.DaemonPort},
	}
	nodes, err := clusterview.FetchNodes(r.Context(), s.opts.DaemonPort)
	if err != nil {
		out.Daemon.Error = err.Error()
	} else {
		out.Daemon.Reachable = true
		out.Cluster = summariseCluster(nodes)
	}
	records, err := s.history.load()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "job history: "+err.Error())
		return
	}
	out.Jobs = summariseJobs(records)
	writeJSON(w, http.StatusOK, out)
}

func summariseCluster(nodes []clusterview.Node) OverviewCluster {
	c := OverviewCluster{NodesTotal: len(nodes)}
	for _, n := range nodes {
		if !n.Healthy() {
			continue
		}
		c.NodesHealthy++
		c.CPUsTotal += n.Load.TotalCPUs
		c.MemTotalBytes += n.Load.TotalMemBytes
		c.MemAvailableBytes += n.Load.AvailableMemBytes
		c.ActiveJobs += n.Load.ActiveJobs
		c.QueueDepth += n.Load.QueueDepth
		gpu := gpuTotals(n)
		c.GPUsTotal += gpu.count
		c.GPUMemTotalBytes += gpu.memTotal
		c.GPUMemUsedBytes += gpu.memUsed
	}
	return c
}

func summariseJobs(records []jobRecord) OverviewJobs {
	j := OverviewJobs{Total: len(records)}
	var durations []int64
	for _, r := range records {
		switch r.Status {
		case jobhistory.StateRunning:
			j.Running++
		case jobhistory.StateSucceeded:
			j.Succeeded++
		case jobhistory.StateFailed:
			j.Failed++
		}
		if d, ok := r.finishedDuration(); ok {
			durations = append(durations, d)
		}
	}
	if finished := j.Succeeded + j.Failed; finished > 0 {
		j.SuccessRate = float64(j.Succeeded) / float64(finished)
	}
	sort.Slice(durations, func(a, b int) bool { return durations[a] < durations[b] })
	j.P50DurationMs = percentile(durations, 50)
	j.P95DurationMs = percentile(durations, 95)
	return j
}

// finishedDuration is how long a finished job took. Jobs still running, or
// that never recorded an end, have none.
func (r jobRecord) finishedDuration() (int64, bool) {
	if !jobhistory.IsTerminal(r.Status) || r.finished.IsZero() {
		return 0, false
	}
	return r.DurationMs, true
}

// percentile returns the p-th percentile of sorted by the nearest-rank
// method, so the answer is always a value that was observed. Empty input
// gives 0.
func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := clusterview.FetchNodes(r.Context(), s.opts.DaemonPort)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	out := make([]NodeView, 0, len(nodes))
	for _, n := range nodes {
		caps := n.Capabilities
		if caps == nil {
			caps = map[string]string{}
		}
		out = append(out, NodeView{
			NodeID:       n.NodeID,
			Hostname:     n.Hostname(),
			SSHEndpoint:  n.SSHEndpoint,
			DaemonPort:   n.DaemonPort,
			State:        n.State,
			HealthScore:  n.HealthScore,
			Source:       n.Source,
			IsSelf:       n.NodeID == s.opts.SelfNodeID,
			Capabilities: caps,
			Load:         n.Load,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("window")
	if name == "" {
		name = defaultMetricsWindow
	}
	window, ok := metricsWindows[name]
	if !ok {
		writeError(w, http.StatusBadRequest, "window must be one of 5m, 15m, 1h, 6h")
		return
	}
	writeJSON(w, http.StatusOK, s.metrics.snapshot(time.Now(), window))
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := clusterview.FetchTasks(r.Context(), s.opts.DaemonPort)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	out := make([]TaskView, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, TaskView{
			NodeID:    t.NodeID,
			Hostname:  t.Hostname,
			LeaseID:   t.LeaseID,
			JobName:   t.JobName,
			Submitter: t.Submitter,
			State:     t.State,
			MemBytes:  t.MemBytes,
			GPUIndex:  t.GPUIndex,
			AgeMs:     t.Age.Milliseconds(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := defaultJobsLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = min(n, maxJobsLimit)
	}
	status, text := q.Get("status"), q.Get("q")

	records, err := s.history.load()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "job history: "+err.Error())
		return
	}
	out := []JobView{}
	for _, rec := range records {
		if status != "" && rec.Status != status {
			continue
		}
		if !rec.matches(text) {
			continue
		}
		out = append(out, rec.view())
		if len(out) == limit {
			break
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	rec, dir, ok := s.history.get(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, JobDetail{
		JobView: rec.view(),
		LogTail: logTail(dir),
		// The history store keeps a job's latest status, not its
		// transitions, so there are none to return.
		Events: []JobEvent{},
	})
}

func (s *Server) handleAnalytics(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("range")
	if name == "" {
		name = defaultRange
	}
	if _, ok := analyticsRanges[name]; !ok && name != rangeAll {
		writeError(w, http.StatusBadRequest, "range must be one of 1h, 24h, 7d, 30d, all")
		return
	}
	records, err := s.history.load()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "job history: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, analyse(records, name, time.Now()))
}

// analyse aggregates the jobs that started inside the named range. records
// are newest first.
func analyse(records []jobRecord, rangeName string, now time.Time) Analytics {
	var from time.Time
	var bucket time.Duration
	if rangeName == rangeAll {
		bucket = 24 * time.Hour
		if n := len(records); n > 0 {
			from = records[n-1].started
			if now.Sub(from) > dailyBucketSpan {
				bucket = 7 * 24 * time.Hour
			}
		}
	} else {
		rng := analyticsRanges[rangeName]
		from, bucket = now.Add(-rng.span), rng.bucket
	}

	out := Analytics{
		Range:        rangeName,
		BucketMs:     bucket.Milliseconds(),
		Throughput:   []ThroughputPoint{},
		Durations:    []DurationPoint{},
		ByNode:       []NodeJobs{},
		ByStatus:     []StatusCount{},
		ByPlacement:  []PlacementCount{},
		MemEstimates: []MemEstimate{},
		StageTime:    []StageTime{},
	}

	var inRange []jobRecord
	for _, r := range records {
		if r.started.IsZero() || r.started.Before(from) || r.started.After(now) {
			continue
		}
		inRange = append(inRange, r)
	}
	if len(inRange) == 0 {
		return out
	}

	bucketMs := bucket.Milliseconds()
	bucketOf := func(t time.Time) int64 { return t.UnixMilli() / bucketMs * bucketMs }

	throughput := map[int64]*ThroughputPoint{}
	durations := map[int64][]int64{}
	byNode := map[string]*NodeJobs{}
	byStatus := map[string]int{}
	byPlacement := map[string]int{}
	stageTime := map[string]int64{}
	var stageOrder []string

	for _, r := range inRange {
		t := bucketOf(r.started)
		tp := throughput[t]
		if tp == nil {
			tp = &ThroughputPoint{T: t}
			throughput[t] = tp
		}
		switch r.Status {
		case jobhistory.StateSucceeded:
			tp.Succeeded++
		case jobhistory.StateFailed:
			tp.Failed++
		default:
			tp.Other++
		}

		d, finished := r.finishedDuration()
		if finished {
			durations[t] = append(durations[t], d)
		}

		byStatus[r.Status]++
		if r.PlacementSource != "" {
			byPlacement[r.PlacementSource]++
		}
		if r.RunHost != "" {
			nj := byNode[r.RunHost]
			if nj == nil {
				nj = &NodeJobs{Node: r.RunHost}
				byNode[r.RunHost] = nj
			}
			nj.Jobs++
			if r.Status == jobhistory.StateFailed {
				nj.Failed++
			}
			if finished {
				nj.TotalDurationMs += d
			}
		}
		if r.EstimatedMemBytes > 0 && r.PeakMemBytes > 0 && len(out.MemEstimates) < memEstimatesCap {
			out.MemEstimates = append(out.MemEstimates, MemEstimate{
				ID: r.ID, JobName: r.JobName, Tier: r.EstimationTier,
				EstimatedBytes: r.EstimatedMemBytes, PeakBytes: r.PeakMemBytes,
			})
		}
		for _, st := range r.Stages {
			if _, seen := stageTime[st.Name]; !seen {
				stageOrder = append(stageOrder, st.Name)
			}
			stageTime[st.Name] += st.Ms
		}
	}

	// Throughput is a count, so a bucket with no jobs is a real zero and the
	// series is continuous. A percentile of no jobs is not a number, so
	// duration buckets exist only where jobs finished.
	first := bucketOf(from)
	if rangeName == rangeAll {
		first = bucketOf(inRange[len(inRange)-1].started)
	}
	for t := first; t <= bucketOf(now); t += bucketMs {
		if tp := throughput[t]; tp != nil {
			out.Throughput = append(out.Throughput, *tp)
		} else {
			out.Throughput = append(out.Throughput, ThroughputPoint{T: t})
		}
		if ds := durations[t]; len(ds) > 0 {
			sort.Slice(ds, func(a, b int) bool { return ds[a] < ds[b] })
			out.Durations = append(out.Durations, DurationPoint{
				T: t, P50Ms: percentile(ds, 50), P95Ms: percentile(ds, 95), Count: len(ds),
			})
		}
	}

	for _, nj := range byNode {
		out.ByNode = append(out.ByNode, *nj)
	}
	sort.Slice(out.ByNode, func(a, b int) bool {
		x, y := out.ByNode[a], out.ByNode[b]
		if x.Jobs != y.Jobs {
			return x.Jobs > y.Jobs
		}
		return x.Node < y.Node
	})

	for status, count := range byStatus {
		out.ByStatus = append(out.ByStatus, StatusCount{Status: status, Count: count})
	}
	sort.Slice(out.ByStatus, func(a, b int) bool {
		x, y := out.ByStatus[a], out.ByStatus[b]
		if x.Count != y.Count {
			return x.Count > y.Count
		}
		return x.Status < y.Status
	})

	for source, count := range byPlacement {
		out.ByPlacement = append(out.ByPlacement, PlacementCount{Source: source, Count: count})
	}
	sort.Slice(out.ByPlacement, func(a, b int) bool {
		x, y := out.ByPlacement[a], out.ByPlacement[b]
		if x.Count != y.Count {
			return x.Count > y.Count
		}
		return x.Source < y.Source
	})

	// First-seen order: the stages in the order a run goes through them.
	for _, name := range stageOrder {
		out.StageTime = append(out.StageTime, StageTime{Stage: name, TotalMs: stageTime[name]})
	}
	return out
}
