package console

import "github.com/pipedpeer/pipedpeer/internal/registry"

// The types in this file are the analytics API's wire shapes.

// Overview is GET /api/overview.
type Overview struct {
	Self    OverviewSelf    `json:"self"`
	Daemon  OverviewDaemon  `json:"daemon"`
	Cluster OverviewCluster `json:"cluster"`
	Jobs    OverviewJobs    `json:"jobs"`
}

type OverviewSelf struct {
	NodeID   string `json:"node_id"`
	Hostname string `json:"hostname"`
}

type OverviewDaemon struct {
	Reachable bool   `json:"reachable"`
	Port      int    `json:"port"`
	Error     string `json:"error"`
}

// OverviewCluster sums capacity and load over the healthy nodes; nodes_total
// counts every node the daemon knows.
type OverviewCluster struct {
	NodesTotal        int   `json:"nodes_total"`
	NodesHealthy      int   `json:"nodes_healthy"`
	CPUsTotal         int   `json:"cpus_total"`
	MemTotalBytes     int64 `json:"mem_total_bytes"`
	MemAvailableBytes int64 `json:"mem_available_bytes"`
	GPUsTotal         int   `json:"gpus_total"`
	GPUMemTotalBytes  int64 `json:"gpu_mem_total_bytes"`
	GPUMemUsedBytes   int64 `json:"gpu_mem_used_bytes"`
	ActiveJobs        int   `json:"active_jobs"`
	QueueDepth        int   `json:"queue_depth"`
}

// OverviewJobs summarises the whole job history. The rate and percentiles
// cover finished jobs only and are 0 when there are none.
type OverviewJobs struct {
	Total         int     `json:"total"`
	Running       int     `json:"running"`
	Succeeded     int     `json:"succeeded"`
	Failed        int     `json:"failed"`
	SuccessRate   float64 `json:"success_rate"`
	P50DurationMs int64   `json:"p50_duration_ms"`
	P95DurationMs int64   `json:"p95_duration_ms"`
}

// NodeView is one element of GET /api/nodes.
type NodeView struct {
	NodeID       string            `json:"node_id"`
	Hostname     string            `json:"hostname"`
	SSHEndpoint  string            `json:"ssh_endpoint"`
	DaemonPort   int               `json:"daemon_port"`
	State        string            `json:"state"`
	HealthScore  float64           `json:"health_score"`
	Source       string            `json:"source"`
	IsSelf       bool              `json:"is_self"`
	Capabilities map[string]string `json:"capabilities"`
	Load         registry.LoadInfo `json:"load"`
}

// Metrics is GET /api/metrics.
type Metrics struct {
	IntervalMs int64          `json:"interval_ms"`
	Series     []MetricSeries `json:"series"`
}

type MetricSeries struct {
	NodeID   string        `json:"node_id"`
	Hostname string        `json:"hostname"`
	Points   []MetricPoint `json:"points"`
}

// MetricPoint is one sample of one node. A nil field means there was no
// reading: the node has no GPU, or it could not be reached.
type MetricPoint struct {
	T          int64    `json:"t"`
	CPU        *float64 `json:"cpu"`
	Mem        *float64 `json:"mem"`
	GPUUtil    *float64 `json:"gpu_util"`
	GPUMem     *float64 `json:"gpu_mem"`
	ActiveJobs *int     `json:"active_jobs"`
}

// TaskView is one element of GET /api/tasks.
type TaskView struct {
	NodeID    string `json:"node_id"`
	Hostname  string `json:"hostname"`
	LeaseID   string `json:"lease_id"`
	JobName   string `json:"job_name"`
	Submitter string `json:"submitter"`
	State     string `json:"state"`
	MemBytes  int64  `json:"mem_bytes"`
	GPUIndex  int    `json:"gpu_index"`
	AgeMs     int64  `json:"age_ms"`
}

// JobView is one element of GET /api/jobs. finished_at and duration_ms are 0
// for a job that has not finished.
type JobView struct {
	ID                string `json:"id"`
	Status            string `json:"status"`
	Error             string `json:"error"`
	StartedAt         int64  `json:"started_at"`
	FinishedAt        int64  `json:"finished_at"`
	DurationMs        int64  `json:"duration_ms"`
	ScriptPath        string `json:"script_path"`
	JobName           string `json:"job_name"`
	JobSet            string `json:"job_set"`
	RunHost           string `json:"run_host"`
	TargetID          string `json:"target_id"`
	PlacementSource   string `json:"placement_source"`
	PlacementReason   string `json:"placement_reason"`
	CandidateCount    int    `json:"candidate_count"`
	DegradedMode      bool   `json:"degraded_mode"`
	PeakMemBytes      int64  `json:"peak_mem_bytes"`
	EstimatedMemBytes int64  `json:"estimated_mem_bytes"`
	EstimationTier    string `json:"estimation_tier"`
	Stage             string `json:"stage"`
	Detached          bool   `json:"detached"`
	Isolate           bool   `json:"isolate"`
}

// JobDetail is GET /api/jobs/{id}.
type JobDetail struct {
	JobView
	LogTail string     `json:"log_tail"`
	Events  []JobEvent `json:"events"`
}

type JobEvent struct {
	Status     string `json:"status"`
	PrevStatus string `json:"prev_status"`
	Timestamp  int64  `json:"timestamp"`
	Error      string `json:"error"`
}

// Analytics is GET /api/analytics.
type Analytics struct {
	Range        string            `json:"range"`
	BucketMs     int64             `json:"bucket_ms"`
	Throughput   []ThroughputPoint `json:"throughput"`
	Durations    []DurationPoint   `json:"durations"`
	ByNode       []NodeJobs        `json:"by_node"`
	ByStatus     []StatusCount     `json:"by_status"`
	ByPlacement  []PlacementCount  `json:"by_placement"`
	MemEstimates []MemEstimate     `json:"mem_estimates"`
	StageTime    []StageTime       `json:"stage_time"`
}

type ThroughputPoint struct {
	T         int64 `json:"t"`
	Succeeded int   `json:"succeeded"`
	Failed    int   `json:"failed"`
	Other     int   `json:"other"`
}

type DurationPoint struct {
	T     int64 `json:"t"`
	P50Ms int64 `json:"p50_ms"`
	P95Ms int64 `json:"p95_ms"`
	Count int   `json:"count"`
}

type NodeJobs struct {
	Node            string `json:"node"`
	Jobs            int    `json:"jobs"`
	Failed          int    `json:"failed"`
	TotalDurationMs int64  `json:"total_duration_ms"`
}

type StatusCount struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
}

type PlacementCount struct {
	Source string `json:"source"`
	Count  int    `json:"count"`
}

type MemEstimate struct {
	ID             string `json:"id"`
	JobName        string `json:"job_name"`
	Tier           string `json:"tier"`
	EstimatedBytes int64  `json:"estimated_bytes"`
	PeakBytes      int64  `json:"peak_bytes"`
}

type StageTime struct {
	Stage   string `json:"stage"`
	TotalMs int64  `json:"total_ms"`
}
