package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// localDaemonPort is the port this machine's daemon listens on: the one it
// recorded at start, else the default.
func localDaemonPort() int {
	if p := daemonctl.Status().Port; p > 0 {
		return p
	}
	return daemonctl.DefaultPort
}

// clusterTask is one live lease, tagged with the node running it.
type clusterTask struct {
	NodeID    string
	Hostname  string
	LeaseID   string
	JobName   string
	Submitter string
	State     string
	MemBytes  int64
	GPUIndex  int
	Age       time.Duration
}

// clusterNode is a node as reported by the daemon's /v1/nodes.
type clusterNode struct {
	NodeID      string `json:"node_id"`
	SSHEndpoint string `json:"ssh_endpoint"`
	DaemonPort  int    `json:"daemon_port"`
	State       string `json:"state"`
	Source      string `json:"source"`
	// Host, Path and Reason are additive /v1/nodes fields: the active
	// address, how it is reached (lan|internet|self) and why a row is not
	// healthy. Older daemons leave them empty.
	Host         string            `json:"host,omitempty"`
	Path         string            `json:"path,omitempty"`
	Reason       string            `json:"reason,omitempty"`
	Capabilities map[string]string `json:"capabilities"`
	Load         registry.LoadInfo `json:"load"`
}

// fetchClusterNodes asks the local daemon for everything it knows about
// cluster membership. The daemon is the single source of truth here — the CLI
// deliberately does no discovery of its own.
func fetchClusterNodes(daemonPort int) ([]clusterNode, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", daemonPort))
	if err != nil {
		return nil, fmt.Errorf("daemon unreachable: %w", err)
	}
	defer resp.Body.Close()

	var nodes []clusterNode
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

type unreachablePeer struct {
	Peer   string `json:"peer"`
	Reason string `json:"reason"`
}

// fetchUnreachable lists internet peers the daemon has no direct path to.
// A daemon without the route (404) yields nil, nil.
func fetchUnreachable(daemonPort int) ([]unreachablePeer, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/peers/unreachable", daemonPort))
	if err != nil {
		return nil, fmt.Errorf("daemon unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}
	var out []unreachablePeer
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// fetchClusterTasks queries every healthy node's /v1/jobs concurrently and
// returns the union, newest last. Nodes that fail to answer are skipped rather
// than failing the whole view: a partial picture beats no picture.
func fetchClusterTasks(daemonPort int) ([]clusterTask, error) {
	nodes, err := fetchClusterNodes(daemonPort)
	if err != nil {
		return nil, err
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		tasks []clusterTask
	)
	client := &http.Client{Timeout: 3 * time.Second}

	for _, n := range nodes {
		if n.State != "healthy" {
			continue
		}
		wg.Add(1)
		go func(n clusterNode) {
			defer wg.Done()

			host := n.address()
			resp, err := client.Get(fmt.Sprintf("http://%s:%d/v1/jobs", host, n.DaemonPort))
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var payload struct {
				NodeID string `json:"node_id"`
				Jobs   []struct {
					LeaseID       string    `json:"lease_id"`
					JobName       string    `json:"job_name"`
					SubmitterNode string    `json:"submitter_node"`
					State         string    `json:"state"`
					MemBytes      int64     `json:"mem_bytes"`
					GPUIndex      int       `json:"gpu_index"`
					CreatedAt     time.Time `json:"created_at"`
				} `json:"jobs"`
			}
			if json.NewDecoder(resp.Body).Decode(&payload) != nil {
				return
			}

			hostname := n.Capabilities["hostname"]
			if hostname == "" {
				hostname = host
			}

			mu.Lock()
			for _, j := range payload.Jobs {
				tasks = append(tasks, clusterTask{
					NodeID:    n.NodeID,
					Hostname:  hostname,
					LeaseID:   j.LeaseID,
					JobName:   j.JobName,
					Submitter: j.SubmitterNode,
					State:     j.State,
					MemBytes:  j.MemBytes,
					GPUIndex:  j.GPUIndex,
					Age:       time.Since(j.CreatedAt),
				})
			}
			mu.Unlock()
		}(n)
	}
	wg.Wait()

	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Age > tasks[j].Age })
	return tasks, nil
}

// address is the node's active host, falling back to its ssh endpoint.
func (n clusterNode) address() string {
	if n.Host != "" {
		return n.Host
	}
	return hostFromEndpoint(n.SSHEndpoint)
}

// hostFromEndpoint pulls "host" out of "user@host:port".
func hostFromEndpoint(endpoint string) string {
	s := endpoint
	if idx := strings.Index(s, "@"); idx != -1 {
		s = s[idx+1:]
	}
	if idx := strings.LastIndex(s, ":"); idx != -1 {
		s = s[:idx]
	}
	if s == "" {
		return "127.0.0.1"
	}
	return s
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	if id == "" {
		return "-"
	}
	return id
}
