package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// Ranks joining a running ring: a machine whose rank died comes back, or a
// new eligible machine appears. runDDP watches the daemon's node list and
// starts a rank there; the rank's shim does the rest with the lead daemon
// (internal/daemonapi/ddpup.go).

// ddpRejoinAfter is how long a machine whose rank died must wait before it
// is given a new one, unless it was seen leaving the cluster first. A rank
// that died on a healthy machine (out of memory, say) would otherwise be
// restarted straight into the same death.
const ddpRejoinAfter = 30 * time.Second

// ddpMaxRejoins bounds how often one machine is given a new rank in a run.
const ddpMaxRejoins = 2

// ddpWatchEvery is how often the node list is read for machines to add.
var ddpWatchEvery = 5 * time.Second

// ddpJoinGrace is how long a rank still waiting to join may outlive the
// rest of the ring. It joins only at an epoch boundary, so one started
// during the last epoch never gets in.
// ponytail: a joined rank still training alone after the others ended (they
// all died) is also stopped after this; ask the lead daemon if that matters.
var ddpJoinGrace = 60 * time.Second

// ddpUpState decides which machines may take a new rank mid-run.
type ddpUpState struct {
	mu       sync.Mutex
	known    map[string]bool      // healthy at the start: placement already judged them
	running  map[string]int       // node -> live ranks
	deadAt   map[string]time.Time // node -> when its rank died
	seenDown map[string]bool      // node left the cluster after its rank died
	joins    map[string]int       // ranks added on this node
}

func newDDPUpState(nodes []registry.NodeRecord) *ddpUpState {
	s := &ddpUpState{known: map[string]bool{}, running: map[string]int{},
		deadAt: map[string]time.Time{}, seenDown: map[string]bool{}, joins: map[string]int{}}
	for _, n := range nodes {
		if n.State == "healthy" {
			s.known[n.NodeID] = true
		}
	}
	return s
}

func (s *ddpUpState) started(node string) {
	s.mu.Lock()
	s.running[node]++
	s.known[node] = true
	s.mu.Unlock()
}

func (s *ddpUpState) ended(node string, died bool, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running[node] > 0 {
		s.running[node]--
	}
	if died {
		s.deadAt[node] = now
		delete(s.seenDown, node)
	}
}

func (s *ddpUpState) live() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.running {
		n += c
	}
	return n
}

// pick returns a machine that should take a new rank: healthy, wanted, not
// already running one, and either back after its rank died or new to the
// cluster since the run started.
func (s *ddpUpState) pick(nodes []registry.NodeRecord, now time.Time, want func(registry.NodeRecord) bool) (registry.NodeRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	healthy := map[string]bool{}
	for _, n := range nodes {
		if n.State == "healthy" {
			healthy[n.NodeID] = true
		}
	}
	for id := range s.deadAt {
		if !healthy[id] {
			s.seenDown[id] = true
		}
	}
	sorted := append([]registry.NodeRecord(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].NodeID < sorted[j].NodeID })
	for _, n := range sorted {
		id := n.NodeID
		if !healthy[id] || s.running[id] > 0 || s.joins[id] >= ddpMaxRejoins || !want(n) {
			continue
		}
		if died, ok := s.deadAt[id]; ok {
			if s.seenDown[id] || now.Sub(died) >= ddpRejoinAfter {
				return n, true
			}
			continue
		}
		if !s.known[id] {
			return n, true
		}
	}
	return registry.NodeRecord{}, false
}

// took records that a machine was given a rank.
func (s *ddpUpState) took(node string) {
	s.mu.Lock()
	s.joins[node]++
	delete(s.deadAt, node)
	delete(s.seenDown, node)
	s.mu.Unlock()
}

// ddpJoinRefused is the lead daemon declining a joiner. Final means asking
// again is pointless (the script slices its own tensors).
type ddpJoinRefused struct {
	msg   string
	final bool
}

func (e *ddpJoinRefused) Error() string { return e.msg }

// ddpJoinCheck asks the lead daemon whether rank could join now.
func ddpJoinCheck(host string, port int, group string, rank int) error {
	body, _ := json.Marshal(map[string]any{"group": group, "rank": rank, "world": rank + 1, "check": true})
	u := "http://" + net.JoinHostPort(host, strconv.Itoa(port)) + "/v1/ddp/join"
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok := authtoken.Current(); tok != "" {
		req.Header.Set(authtoken.Header, tok)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	var out struct {
		Error string `json:"error"`
		Final bool   `json:"final"`
	}
	if resp.StatusCode == http.StatusConflict && json.Unmarshal(raw, &out) == nil && out.Error != "" {
		return &ddpJoinRefused{msg: out.Error, final: out.Final}
	}
	// A lead daemon from before joins existed: nothing to retry.
	return &ddpJoinRefused{msg: fmt.Sprintf("the lead daemon cannot take ranks mid-run (HTTP %d)", resp.StatusCode),
		final: resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed}
}

// ddpFetchNodes reads the daemon's node list.
func ddpFetchNodes(daemonPort int) ([]registry.NodeRecord, error) {
	return ddpFetchNodesAt("127.0.0.1", daemonPort)
}

// ddpFetchNodesAt reads the node list of the daemon at host:port.
func ddpFetchNodesAt(host string, port int) ([]registry.NodeRecord, error) {
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(
		fmt.Sprintf("http://%s/v1/nodes", net.JoinHostPort(host, strconv.Itoa(port))))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var all []registry.NodeRecord
	if err := json.NewDecoder(resp.Body).Decode(&all); err != nil {
		return nil, err
	}
	return all, nil
}

// ddpGrower watches for machines to add to a running ring.
type ddpGrower struct {
	st         *ddpUpState
	daemonPort int
	leadHost   string
	leadPort   int
	group      string
	want       func(registry.NodeRecord) bool
	maxRanks   int        // 0: no cap
	nextRank   func() int // the index the next rank takes
	// start launches a rank that joins; false when the run is ending.
	start func(rank int, n registry.NodeRecord) bool
	out   io.Writer // progress lines (ddpProgressOut); warnings go to stderr
	fetch func(int) ([]registry.NodeRecord, error)
	check func(host string, port int, group string, rank int) error
}

// run polls until ctx ends, or until the ring says it cannot grow.
func (g *ddpGrower) run(ctx context.Context) {
	t := time.NewTicker(ddpWatchEvery)
	defer t.Stop()
	lastWhy := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if g.maxRanks > 0 && g.st.live() >= g.maxRanks {
			continue
		}
		nodes, err := g.fetch(g.daemonPort)
		if err != nil {
			if why := err.Error(); why != lastWhy {
				lastWhy = why
				fmt.Fprintf(os.Stderr, "[ddp] cannot read the node list to look for machines to add: %v\n", err)
			}
			continue
		}
		n, ok := g.st.pick(nodes, time.Now(), g.want)
		if !ok {
			continue
		}
		rank := g.nextRank()
		if err := g.check(g.leadHost, g.leadPort, g.group, rank); err != nil {
			var refused *ddpJoinRefused
			if errors.As(err, &refused) && refused.final {
				fmt.Fprintf(g.out, "[ddp] %s is free, but %s; the ring carries on as it is\n", ddpHostName(n), refused.msg)
				return
			}
			if why := err.Error(); why != lastWhy {
				lastWhy = why
				fmt.Fprintf(g.out, "[ddp] not adding %s yet: %v\n", ddpHostName(n), err)
			}
			continue
		}
		lastWhy = ""
		g.st.took(n.NodeID)
		if !g.start(rank, n) {
			return
		}
	}
}

// ddpAdded announces a rank started mid-run to ddpAwaitRanks.
type ddpAdded struct {
	rank   int
	host   string
	cancel context.CancelFunc
}
