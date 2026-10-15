// Package registry holds the node record types the daemon serves on /v1/nodes
// and the scheduler ranks. The registry server that once stored them is gone:
// daemons learn about each other through discovery and the introducer.
package registry

import "time"

// NodeRecord represents a registered node in the cluster.
type NodeRecord struct {
	NodeID        string            `json:"node_id"`
	SSHEndpoint   string            `json:"ssh_endpoint"`
	DaemonPort    int               `json:"daemon_port"`
	Capabilities  map[string]string `json:"capabilities,omitempty"`
	Load          LoadInfo          `json:"load"`
	HealthScore   float64           `json:"health_score"`
	State         string            `json:"state"` // "healthy", "stale", "unavailable"
	RegisteredAt  time.Time         `json:"registered_at"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	LeaseExpiry   time.Time         `json:"lease_expiry"`
}

// PerGPUInfo carries live stats for a single GPU device on this node.
type PerGPUInfo struct {
	Index            int     `json:"index"`
	Name             string  `json:"name"`
	MemoryTotalBytes int64   `json:"memory_total_bytes"`
	MemoryFreeBytes  int64   `json:"memory_free_bytes"`
	UtilizationGPU   float64 `json:"utilization_gpu_percent"`
}

// LoadInfo carries current resource utilization.
type LoadInfo struct {
	CPUPercent        float64 `json:"cpu_percent"`
	MemPercent        float64 `json:"memory_percent"`
	ActiveJobs        int     `json:"active_jobs"`
	QueueDepth        int     `json:"queue_depth"`
	RecentFailures    int     `json:"recent_failures"`
	TotalMemBytes     int64   `json:"total_mem_bytes,omitempty"`
	AvailableMemBytes int64   `json:"available_mem_bytes,omitempty"`
	ReservedMemBytes  int64   `json:"reserved_mem_bytes,omitempty"`
	// MemCeilingBytes is the most this node's daemon will ever grant one
	// job (its cgroup memory.max, or its share of the machine); 0 when
	// unknown. Placement fails fast on a job bigger than every ceiling,
	// where available memory alone would leave it queued forever.
	MemCeilingBytes int64        `json:"mem_ceiling_bytes,omitempty"`
	TotalCPUs       int          `json:"total_cpus"`
	GPUModel        string       `json:"gpu_model,omitempty"`
	GPUMemBytes     int64        `json:"gpu_mem_bytes,omitempty"`
	GPUMemUsedBytes int64        `json:"gpu_mem_used_bytes,omitempty"`
	GPUUtilPercent  float64      `json:"gpu_util_percent,omitempty"`
	GPUs            []PerGPUInfo `json:"gpus,omitempty"`
}
