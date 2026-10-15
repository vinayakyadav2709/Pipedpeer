package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/term"
)

// What `connect` reads from the local daemon to decide how the join went:
// /v1/nodes (C11 adds host/path/reason), /v1/introducers (C10/C11) and
// /v1/peers/unreachable.
type connNode struct {
	NodeID       string            `json:"node_id"`
	State        string            `json:"state"`
	Source       string            `json:"source"`
	Path         string            `json:"path"`
	Reason       string            `json:"reason"`
	Capabilities map[string]string `json:"capabilities"`
}

type connIntro struct {
	Addr      string    `json:"addr"`
	LastReply time.Time `json:"last_reply"`
	LastErr   string    `json:"last_err"`
	Members   int       `json:"members"`
}

type connUnreach struct {
	Peer   string `json:"peer"`
	Reason string `json:"reason"`
}

type connSnapshot struct {
	nodes       []connNode
	intros      []connIntro
	unreachable []connUnreach
}

// connOutcome is also connect's exit code (Decisions: 0 / 2 / 1).
type connOutcome int

const (
	connConnected connOutcome = 0
	connFailed    connOutcome = 1
	connPartly    connOutcome = 2
)

type connResult struct {
	outcome  connOutcome
	healthy  []string // machine names
	problems []string // "<machine>: <reason> — fix: <what to do>"
	why, fix string   // failed only
}

func getJSON(client *http.Client, url string, into any) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil // an older daemon without the route: nothing to read
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		return fmt.Errorf("%s: HTTP %d %s", url, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

// snapshot reads the three lists from the daemon at base (http://host:port).
func snapshot(base string) (connSnapshot, error) {
	c := &http.Client{Timeout: 3 * time.Second}
	var s connSnapshot
	if err := getJSON(c, base+"/v1/nodes", &s.nodes); err != nil {
		return s, err
	}
	if err := getJSON(c, base+"/v1/introducers", &s.intros); err != nil {
		return s, err
	}
	if err := getJSON(c, base+"/v1/peers/unreachable", &s.unreachable); err != nil {
		return s, err
	}
	return s, nil
}

func short8(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// fixFor turns a peer's reason into what to do about it.
func fixFor(reason string) string {
	r := strings.ToLower(reason)
	switch {
	case strings.Contains(r, "token") || strings.Contains(r, "401") || strings.Contains(r, "secret"):
		return "this machine and that one hold different secrets: run `pipedpeer invite` on a connected machine and `pipedpeer connect` again here"
	case strings.Contains(r, "deadline") || strings.Contains(r, "timeout") || strings.Contains(r, "no route") ||
		strings.Contains(r, "refused"):
		return "the machines cannot reach each other on 4242/tcp: run `pipedpeer doctor` on both (router or firewall between them)"
	}
	return "run `pipedpeer doctor` on both machines"
}

func secretRejected(reason string) bool {
	r := strings.ToLower(reason)
	return strings.Contains(r, "token") || strings.Contains(r, "401") || strings.Contains(r, "secret")
}

// classify decides connected / partly / failed from one snapshot.
func classify(s connSnapshot) connResult {
	var res connResult
	names := map[string]int{}
	for _, n := range s.nodes {
		names[n.Capabilities["hostname"]]++
	}
	name := func(n connNode) string {
		h := n.Capabilities["hostname"]
		switch {
		case h == "":
			return short8(n.NodeID)
		case names[h] > 1: // cloned lab PCs share a hostname (F17)
			return h + " (" + short8(n.NodeID) + ")"
		}
		return h
	}
	healthyIDs := map[string]bool{}
	rejected := false
	for _, n := range s.nodes {
		if n.Source == "self" || n.Path == "self" {
			continue
		}
		if n.State == "healthy" {
			res.healthy = append(res.healthy, name(n))
			healthyIDs[short8(n.NodeID)] = true
			continue
		}
		why := n.Reason
		if why == "" {
			why = n.State
		}
		rejected = rejected || secretRejected(why)
		res.problems = append(res.problems, name(n)+": "+why+" — fix: "+fixFor(why))
	}
	for _, u := range s.unreachable {
		if healthyIDs[short8(u.Peer)] {
			continue
		}
		rejected = rejected || secretRejected(u.Reason)
		res.problems = append(res.problems, short8(u.Peer)+": "+u.Reason+" — fix: "+fixFor(u.Reason))
	}
	sort.Strings(res.healthy)
	switch {
	case len(res.healthy) > 0 && len(res.problems) == 0:
		res.outcome = connConnected
		return res
	case len(res.healthy) > 0:
		res.outcome = connPartly
		return res
	}
	res.outcome = connFailed
	replied, members := false, 0
	var addrs, errs []string
	for _, in := range s.intros {
		addrs = append(addrs, in.Addr)
		if !in.LastReply.IsZero() {
			replied = true
			members = max(members, in.Members)
		} else if in.LastErr != "" {
			errs = append(errs, in.Addr+": "+in.LastErr)
		}
	}
	switch {
	case rejected:
		res.why = "the cluster rejected this machine's secret"
		res.fix = "run `pipedpeer invite` on a connected machine and `pipedpeer connect <new code>` here"
	case !replied:
		res.why = "nobody answered: no introducer replied (" + strings.Join(addrs, ", ") + ")"
		if len(errs) > 0 {
			res.why += "; " + strings.Join(errs, "; ")
		}
		res.fix = "check the inviting machine is on and its daemon runs (`pipedpeer status` there), " +
			"that both are on a network that lets 4244/udp through, then run connect again"
	case members <= 1 && len(res.problems) == 0:
		res.why = "the introducer answered, but no other machine has joined it yet"
		res.fix = "run `pipedpeer status` on the inviting machine; if it is down, `pipedpeer start` there"
	default:
		res.why = "machines were found but none could be reached"
		res.fix = "see each machine's fix above"
	}
	return res
}

// waitConnected polls the daemon at base until connected or wait runs out,
// with a countdown line on a terminal.
func waitConnected(base string, wait time.Duration) connResult {
	fi, err := os.Stdout.Stat()
	tty := err == nil && fi.Mode()&os.ModeCharDevice != 0
	deadline := time.Now().Add(wait)
	var res connResult
	lastErr := ""
	for {
		s, err := snapshot(base)
		if err != nil {
			if e := err.Error(); e != lastErr {
				fmt.Fprintf(os.Stderr, "pipedpeer: asking the local daemon: %v\n", err)
				lastErr = e
			}
			res = connResult{outcome: connFailed, why: "the local daemon did not answer: " + err.Error(),
				fix: "run `pipedpeer start`, then `pipedpeer connect` again"}
		} else {
			res = classify(s)
		}
		left := time.Until(deadline)
		if res.outcome == connConnected || left <= 0 {
			break
		}
		if tty {
			fmt.Printf("\r\033[K  waiting for the cluster · %d s left · %d found", int(left.Seconds()+0.5), len(res.healthy))
		}
		time.Sleep(time.Second)
	}
	if tty {
		fmt.Print("\r\033[K")
	}
	return res
}

// printConnResult is connect's last word.
func printConnResult(w io.Writer, r connResult) {
	switch r.outcome {
	case connConnected:
		term.OK(w, "connected: %d other machine(s): %s", len(r.healthy), strings.Join(r.healthy, ", "))
	case connPartly:
		term.Warn(w, "partly connected: %d machine(s) reachable (%s), %d not", len(r.healthy),
			strings.Join(r.healthy, ", "), len(r.problems))
		for _, p := range r.problems {
			term.Fail(w, "%s", p)
		}
	default:
		term.Fail(w, "not connected: %s", r.why)
		for _, p := range r.problems {
			term.Fail(w, "%s", p)
		}
		term.Action(w, "%s", r.fix)
	}
}
