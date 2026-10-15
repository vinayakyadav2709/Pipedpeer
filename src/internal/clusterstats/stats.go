// Package clusterstats gathers one snapshot of the cluster: every node with
// its load and pool counters, the leases running on it, and this machine's
// recent runs with their timelines and receipts. The CLI's `stats` command
// and the daemon's web page both show it, so both build it here.
package clusterstats

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// Node is a cluster member as the local daemon lists it.
type Node struct {
	NodeID       string            `json:"node_id"`
	Host         string            `json:"host"`
	Port         int               `json:"port"`
	State        string            `json:"state"`
	Source       string            `json:"source"`
	Capabilities map[string]string `json:"capabilities,omitempty"`
	Load         registry.LoadInfo `json:"load"`
}

// NodeFromAPI is a row of the daemon's /v1/nodes as a Node. The CLI's `stats`
// and the daemon's /ui both build their rows with it, so both ask a node's
// daemon at the same address.
func NodeFromAPI(nodeID, sshEndpoint string, daemonPort int, state, source string,
	caps map[string]string, load registry.LoadInfo) Node {
	return Node{NodeID: nodeID, Host: hostOf(sshEndpoint), Port: daemonPort,
		State: state, Source: source, Capabilities: caps, Load: load}
}

// hostOf pulls the host out of "user@host:port", keeping an IPv6 address in
// its brackets so it drops straight into a URL. An empty endpoint is this
// machine.
func hostOf(endpoint string) string {
	if i := strings.Index(endpoint, "@"); i >= 0 {
		endpoint = endpoint[i+1:]
	}
	if i := strings.LastIndex(endpoint, ":"); i >= 0 && !strings.HasSuffix(endpoint, "]") {
		endpoint = endpoint[:i]
	}
	if endpoint == "" {
		return "127.0.0.1"
	}
	return endpoint
}

// PoolCounters is a node's tally of pool work since its daemon started.
type PoolCounters struct {
	ChunksReceived int64 `json:"chunks_received"`
	ChunksLocal    int64 `json:"chunks_from_local_shim"`
	ChunksPeer     int64 `json:"chunks_from_peers"`
	ItemsExecuted  int64 `json:"items_executed_here"`
	PartsToPeers   int64 `json:"parts_sent_to_peers"`
	PartsFailed    int64 `json:"parts_failed"`
}

// Lease is one task running on a node right now.
type Lease struct {
	LeaseID   string    `json:"lease_id"`
	JobName   string    `json:"job_name"`
	Submitter string    `json:"submitter_node"`
	State     string    `json:"state"`
	MemBytes  int64     `json:"mem_bytes"`
	GPUIndex  int       `json:"gpu_index"`
	CreatedAt time.Time `json:"created_at"`
}

// NodeStats is a node with what it said when asked.
type NodeStats struct {
	Node
	Hostname  string        `json:"hostname"`
	Reachable bool          `json:"reachable"`
	Pool      *PoolCounters `json:"pool,omitempty"`
	Leases    []Lease       `json:"leases,omitempty"`
}

// JobStats is one of this machine's recent runs.
type JobStats struct {
	ID        string `json:"id"`
	Script    string `json:"script"`
	Status    string `json:"status"`
	Stage     string `json:"stage,omitempty"`
	RunHost   string `json:"run_host,omitempty"`
	TargetID  string `json:"target_id,omitempty"`
	StartedAt string `json:"started_at"`
	// Origin is the hostname of the machine the run was started on.
	Origin     string                 `json:"origin,omitempty"`
	TotalMs    int64                  `json:"total_ms"`
	Stages     []jobhistory.Stage     `json:"stages,omitempty"`
	Receipt    *jobhistory.Receipt    `json:"receipt,omitempty"`
	Candidates []jobhistory.Candidate `json:"candidates,omitempty"`
	Reason     string                 `json:"placement_reason,omitempty"`
	// Source is how the node was chosen: "self" for this machine, a
	// coordinator source such as "discovery", or "explicit" for --host.
	Source string `json:"placement_source,omitempty"`
}

// Snapshot is the whole picture at one moment.
type Snapshot struct {
	At    time.Time   `json:"at"`
	Nodes []NodeStats `json:"nodes"`
	Jobs  []JobStats  `json:"jobs"`
}

// History is a node's answer to GET /v1/history?limit=N: its own recent runs.
type History struct {
	Origin string     `json:"origin"`
	Jobs   []JobStats `json:"jobs"`
}

// Collect asks every listed node for its pool counters and running leases,
// concurrently, and reads this machine's last jobLimit runs. A node that does
// not answer is kept and marked unreachable: a partial picture beats none,
// and a missing node is itself the news.
func Collect(nodes []Node, jobLimit int, client *http.Client) Snapshot {
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Second}
	}
	out := make([]NodeStats, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		out[i] = NodeStats{Node: n, Hostname: n.Capabilities["hostname"]}
		if out[i].Hostname == "" {
			out[i].Hostname = n.Host
		}
		if n.Host == "" || n.Port == 0 || n.State == "unreachable" {
			continue
		}
		wg.Add(1)
		go func(ns *NodeStats) {
			defer wg.Done()
			base := fmt.Sprintf("http://%s:%d", ns.Host, ns.Port)
			var pc PoolCounters
			if getJSON(client, base+"/v1/pool/stats", &pc) == nil {
				ns.Reachable = true
				ns.Pool = &pc
			}
			var jobs struct {
				Jobs []Lease `json:"jobs"`
			}
			if getJSON(client, base+"/v1/jobs", &jobs) == nil {
				ns.Reachable = true
				ns.Leases = jobs.Jobs
			}
		}(&out[i])
	}
	wg.Wait() // reachability is known before the history round
	hist := make([]History, len(out))
	for i := range out {
		ns := &out[i]
		if ns.Source == "self" || !ns.Reachable {
			continue
		}
		wg.Add(1)
		go func(i int, ns *NodeStats) {
			defer wg.Done()
			url := fmt.Sprintf("http://%s:%d/v1/history?limit=%d", ns.Host, ns.Port, jobLimit)
			if err := getJSON(client, url, &hist[i]); err != nil {
				warnHistoryOnce(ns.Host, err)
				hist[i] = History{}
			}
		}(i, ns)
	}
	wg.Wait()
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Source == "self" || out[j].Source == "self" {
			return out[i].Source == "self"
		}
		return out[i].Hostname < out[j].Hostname
	})
	jobs := RecentJobs(jobLimit)
	for i := range out {
		if out[i].Source == "self" {
			for j := range jobs {
				jobs[j].Origin = out[i].Hostname
			}
		}
	}
	for _, h := range hist {
		for _, j := range h.Jobs {
			j.Origin = h.Origin
			jobs = append(jobs, j)
		}
	}
	return Snapshot{At: time.Now(), Nodes: out, Jobs: MergeJobs(jobs, jobLimit)}
}

// MergeJobs orders runs from several machines by start time, newest first,
// and keeps the newest limit.
func MergeJobs(jobs []JobStats, limit int) []JobStats {
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].StartedAt > jobs[j].StartedAt })
	if limit > 0 && len(jobs) > limit {
		jobs = jobs[:limit]
	}
	return jobs
}

// warned remembers which hosts already got a history warning: Collect runs
// about once a second, and a peer on an older build would fill the log.
var warned sync.Map

func warnHistoryOnce(host string, err error) {
	if _, dup := warned.LoadOrStore(host, true); !dup {
		log.Printf("clusterstats: no run history from %s (its runs are left out): %v", host, err)
	}
}

// RecentJobs reads this machine's newest runs, with their timelines and
// receipts.
func RecentJobs(limit int) []JobStats {
	list, err := jobhistory.List(limit)
	if err != nil {
		return nil
	}
	out := make([]JobStats, 0, len(list))
	for _, s := range list {
		r, dir, err := jobhistory.ReadRecord(s.ID)
		if err != nil {
			continue
		}
		js := JobStats{
			ID: r.ID, Script: filepath.Base(r.ScriptPath), Status: r.Status, Stage: r.Stage,
			RunHost: r.RunHost, TargetID: r.TargetID, StartedAt: r.StartedAt,
			TotalMs: jobhistory.Total(r.Stages).Milliseconds(), Stages: r.Stages,
			Candidates: r.Candidates, Reason: r.PlacementReason, Source: r.PlacementSource,
		}
		if js.TotalMs == 0 {
			js.TotalMs = r.DurationMs
		}
		if rc, ok, _ := jobhistory.ReadReceipt(dir); ok {
			js.Receipt = &rc
		}
		out = append(out, js)
	}
	return out
}

func getJSON(client *http.Client, url string, v any) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}
