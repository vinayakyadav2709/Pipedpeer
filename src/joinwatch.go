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
	// refusedOnly: every problem is a refused connection (a machine that is
	// reachable with no daemon listening), so waiting longer will not help.
	refusedOnly bool
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
	switch {
	case refused(reason):
		return "that machine is reachable but its pipedpeer daemon is not running: run `pipedpeer status` there, and `pipedpeer start` if it is down"
	case noAnswer(reason):
		return "the machines cannot reach each other on 4242/tcp: run `pipedpeer doctor` on both (router or firewall between them)"
	case secretRejected(reason):
		return "this machine and that one hold different secrets: run `pipedpeer invite` on a connected machine and `pipedpeer connect` again here"
	}
	return "run `pipedpeer doctor` on both machines"
}

// refused: the connection was actively refused, not left unanswered.
func refused(reason string) bool {
	return strings.Contains(strings.ToLower(reason), "connection refused")
}

// noAnswer: the reason says nothing answered. A timeout inside the token
// check is one too: its text names the token, but no one refused anything
// (the real laptops: no introducer reachable, connect blamed the secret).
func noAnswer(reason string) bool {
	r := strings.ToLower(reason)
	for _, s := range []string{"deadline", "timeout", "timed out", "no route", "refused", "did not answer",
		"unreachable", "no reply", "no answer", "network is down"} {
		if strings.Contains(r, s) {
			return true
		}
	}
	return false
}

// secretRejected: something answered and refused this machine's secret (401)
// or could not prove it holds it. Never when nothing answered.
func secretRejected(reason string) bool {
	if noAnswer(reason) {
		return false
	}
	r := strings.ToLower(reason)
	for _, s := range []string{"401", "could not prove", "proof does not match", "carried no proof", "secret"} {
		if strings.Contains(r, s) {
			return true
		}
	}
	return false
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
	// One line per machine: a peer can be both an unhealthy node and an
	// unreachable entry, or unreachable more than once (once per path), and
	// was counted and printed each time ("4 not" for two machines).
	healthyIDs, reported := map[string]bool{}, map[string]bool{}
	rejected, allRefused := false, true
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
		allRefused = allRefused && refused(why)
		reported[short8(n.NodeID)] = true
		res.problems = append(res.problems, name(n)+": "+why+" — fix: "+fixFor(why))
	}
	for _, u := range s.unreachable {
		id := short8(u.Peer)
		if healthyIDs[id] || reported[id] {
			continue
		}
		reported[id] = true
		rejected = rejected || secretRejected(u.Reason)
		allRefused = allRefused && refused(u.Reason)
		res.problems = append(res.problems, short8(u.Peer)+": "+u.Reason+" — fix: "+fixFor(u.Reason))
	}
	sort.Strings(res.healthy)
	switch {
	case len(res.healthy) > 0 && len(res.problems) == 0:
		res.outcome = connConnected
		return res
	case len(res.healthy) > 0:
		res.outcome = connPartly
		res.refusedOnly = allRefused
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
		res.why = "no introducer answered (tried " + strings.Join(addrs, ", ") + ")"
		if len(errs) > 0 {
			res.why += "; " + strings.Join(errs, "; ")
		}
		res.fix = "check the inviting machine is on and its daemon runs (`pipedpeer status` there). " +
			"If either machine is behind a NAT that cannot be opened (UPnP failing, double NAT), give one machine " +
			"a public address or forward 4244/udp to it (`pipedpeer setup --public-address <addr>`), " +
			"or put both on Tailscale; `pipedpeer doctor` names which. Then run connect again"
	case members <= 1 && len(res.problems) == 0:
		res.why = "the introducer answered, but no other machine has joined it yet"
		res.fix = "run `pipedpeer status` on the inviting machine; if it is down, `pipedpeer start` there"
	default:
		res.why = "machines were found but none could be reached"
		res.fix = "see each machine's fix above"
	}
	return res
}

// connSettle is how long the picture must stay the same (a machine healthy,
// every problem a refused connection) before waitConnected stops early; connPoll
// is the polling step. Vars so tests can shorten them.
var (
	connSettle = 10 * time.Second
	connPoll   = time.Second
)

// waitConnected polls the daemon at base until connected or wait runs out,
// with a countdown line on a terminal.
func waitConnected(base string, wait time.Duration) connResult {
	fi, err := os.Stdout.Stat()
	tty := err == nil && fi.Mode()&os.ModeCharDevice != 0
	deadline := time.Now().Add(wait)
	var res connResult
	lastErr := ""
	var settledSince time.Time // since when the refused-only picture has been the same
	settledKey := ""
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
		// A machine is up and the rest only refuse: nothing a longer wait fixes.
		if res.outcome == connPartly && res.refusedOnly {
			if key := strings.Join(res.problems, "\n"); key != settledKey || settledSince.IsZero() {
				settledKey, settledSince = key, time.Now()
			} else if time.Since(settledSince) >= connSettle {
				break
			}
		} else {
			settledSince, settledKey = time.Time{}, ""
		}
		if tty {
			fmt.Printf("\r\033[K  waiting for the cluster · %d s left · %d found", int(left.Seconds()+0.5), len(res.healthy))
		}
		time.Sleep(connPoll)
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
