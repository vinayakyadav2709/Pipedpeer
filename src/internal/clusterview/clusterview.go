// Package clusterview reads cluster membership and live work from the local
// daemon. The daemon is the single source of truth: callers do no discovery of
// their own.
package clusterview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

const (
	requestTimeout = 3 * time.Second
	stateHealthy   = "healthy"
)

// Node is a cluster member as reported by the daemon's /v1/nodes.
type Node struct {
	NodeID      string  `json:"node_id"`
	SSHEndpoint string  `json:"ssh_endpoint"`
	DaemonPort  int     `json:"daemon_port"`
	State       string  `json:"state"`
	HealthScore float64 `json:"health_score"`
	Source      string  `json:"source"`
	// Host is the address the daemon currently reaches the node on, and Path
	// how (lan, internet or self). Reason says why a row is not healthy.
	Host         string            `json:"host,omitempty"`
	Path         string            `json:"path,omitempty"`
	Reason       string            `json:"reason,omitempty"`
	Capabilities map[string]string `json:"capabilities"`
	Load         registry.LoadInfo `json:"load"`
}

// Healthy reports whether the daemon currently considers the node usable.
func (n Node) Healthy() bool { return n.State == stateHealthy }

// Address is the host the node's daemon is reached on: the daemon's active
// path, else the host of its ssh endpoint.
func (n Node) Address() string {
	if n.Host != "" {
		return n.Host
	}
	return HostFromEndpoint(n.SSHEndpoint)
}

// Hostname is the node's self-reported name, falling back to its address.
func (n Node) Hostname() string {
	if h := n.Capabilities["hostname"]; h != "" {
		return h
	}
	return n.Address()
}

// Task is one live lease, tagged with the node running it.
type Task struct {
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

// FetchNodes asks the local daemon for everything it knows about cluster
// membership.
func FetchNodes(ctx context.Context, daemonPort int) ([]Node, error) {
	var nodes []Node
	url := fmt.Sprintf("http://127.0.0.1:%d/v1/nodes", daemonPort)
	if err := getJSON(ctx, url, &nodes); err != nil {
		var decodeErr *decodeError
		if errors.As(err, &decodeErr) {
			return nil, decodeErr.err
		}
		return nil, fmt.Errorf("daemon unreachable: %w", err)
	}
	return nodes, nil
}

// FetchTasks returns the live leases of every healthy node, oldest first.
func FetchTasks(ctx context.Context, daemonPort int) ([]Task, error) {
	nodes, err := FetchNodes(ctx, daemonPort)
	if err != nil {
		return nil, err
	}
	return TasksOf(ctx, nodes), nil
}

// TasksOf queries each healthy node's /v1/jobs concurrently and returns the
// union, oldest first. Nodes that fail to answer are skipped rather than
// failing the whole view: a partial picture beats no picture.
func TasksOf(ctx context.Context, nodes []Node) []Task {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		tasks []Task
	)
	for _, n := range nodes {
		if !n.Healthy() {
			continue
		}
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			leases, err := fetchLeases(ctx, n)
			if err != nil {
				return
			}
			mu.Lock()
			tasks = append(tasks, leases...)
			mu.Unlock()
		}(n)
	}
	wg.Wait()

	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Age > tasks[j].Age })
	return tasks
}

func fetchLeases(ctx context.Context, n Node) ([]Task, error) {
	var payload struct {
		Jobs []struct {
			LeaseID       string    `json:"lease_id"`
			JobName       string    `json:"job_name"`
			SubmitterNode string    `json:"submitter_node"`
			State         string    `json:"state"`
			MemBytes      int64     `json:"mem_bytes"`
			GPUIndex      int       `json:"gpu_index"`
			CreatedAt     time.Time `json:"created_at"`
		} `json:"jobs"`
	}
	url := fmt.Sprintf("http://%s:%d/v1/jobs", n.Address(), n.DaemonPort)
	if err := getJSON(ctx, url, &payload); err != nil {
		return nil, err
	}

	hostname := n.Hostname()
	tasks := make([]Task, 0, len(payload.Jobs))
	for _, j := range payload.Jobs {
		tasks = append(tasks, Task{
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
	return tasks, nil
}

func getJSON(ctx context.Context, url string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var body struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 4<<10)).Decode(&body)
		if body.Error == "" {
			return &decodeError{fmt.Errorf("daemon answered %s", resp.Status)}
		}
		return &decodeError{fmt.Errorf("daemon answered %s: %s", resp.Status, body.Error)}
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return &decodeError{err}
	}
	return nil
}

// decodeError marks a response that arrived but could not be used, as
// opposed to a daemon that did not answer.
type decodeError struct{ err error }

func (e *decodeError) Error() string { return e.err.Error() }

// HostFromEndpoint pulls "host" out of "user@host:port".
func HostFromEndpoint(endpoint string) string {
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
