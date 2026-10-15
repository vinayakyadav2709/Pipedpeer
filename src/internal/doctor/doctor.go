// Package doctor answers "why doesn't this machine work with the others?" in
// one screen: every check prints what it found and, when something is wrong,
// the command or step that fixes it.
package doctor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/say"
	"github.com/pipedpeer/pipedpeer/internal/term"
)

// Level is how bad a finding is.
type Level int

const (
	OK   Level = iota // works
	Warn              // works, but worse than it should
	Fail              // broken; doctor exits 1
)

// Check is one line of the report.
type Check struct {
	Level Level
	What  string
	Fix   string // empty for OK lines
}

// Print writes the report and returns the exit code: 1 when any check failed.
func Print(w io.Writer, checks []Check) int {
	code := 0
	for _, c := range checks {
		msg := c.What
		if c.Fix != "" {
			msg += " — fix: " + c.Fix
		}
		switch c.Level {
		case OK:
			term.OK(w, "%s", msg)
		case Warn:
			term.Warn(w, "%s", msg)
		default:
			term.Fail(w, "%s", msg)
			code = 1
		}
	}
	return code
}

// Cluster is everything the cluster checks need, injected so tests can fake
// the daemon and the network.
type Cluster struct {
	// API is the local daemon's base URL ("http://127.0.0.1:4242"); empty
	// when no daemon is running.
	API string
	// HTTP talks to the local daemon and carries the token.
	HTTP *http.Client
	// Token is this machine's cluster secret ("" = none).
	Token string
	// Introducers are the configured introducer addresses.
	Introducers []string
	// Dial opens TCP to a peer; PeerHTTP reads a peer's clock (Date header).
	Dial     func(ctx context.Context, network, addr string) (net.Conn, error)
	PeerHTTP *http.Client
	Now      func() time.Time
}

// introducerState is daemonapi's GET /v1/introducers row (contract C11).
type introducerState struct {
	Addr      string    `json:"addr"`
	LastReply time.Time `json:"last_reply"`
	LastErr   string    `json:"last_err,omitempty"`
	Members   int       `json:"members"`
}

// node is the part of a GET /v1/nodes row doctor reads.
type node struct {
	NodeID       string            `json:"node_id"`
	SSHEndpoint  string            `json:"ssh_endpoint"`
	Host         string            `json:"host"`
	DaemonPort   int               `json:"daemon_port"`
	State        string            `json:"state"`
	Source       string            `json:"source"`
	Path         string            `json:"path"`
	Reason       string            `json:"reason"`
	Capabilities map[string]string `json:"capabilities"`
	Paths        []nodePath        `json:"paths"`
}

type nodePath struct {
	Kind   string `json:"kind"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	Active bool   `json:"active"`
	OK     bool   `json:"ok"`
	Err    string `json:"err"`
}

type unreachable struct {
	Peer   string `json:"peer"`
	Reason string `json:"reason"`
}

// introducerFresh: registration runs every 20 s, so three missed replies.
const introducerFresh = 90 * time.Second

// maxSkew is how far a peer's clock may drift before leases and expiry
// windows start misbehaving.
const maxSkew = 60 * time.Second

const connectFix = "run `pipedpeer invite` on a machine already in the cluster, then `pipedpeer connect <code>` here"

// errStatus is a non-2xx answer from the daemon.
type errStatus int

func (e errStatus) Error() string { return "HTTP " + strconv.Itoa(int(e)) }

func (c Cluster) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.API+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return errStatus(resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Run performs the cluster checks.
func (c Cluster) Run(ctx context.Context) []Check {
	var out []Check
	if c.Token == "" {
		out = append(out, Check{Fail, "no cluster secret on this machine", connectFix})
	} else {
		out = append(out, Check{Level: OK, What: "cluster secret set"})
	}
	if c.API == "" {
		return append(out, Check{Fail, "daemon not running", "pipedpeer start"})
	}
	if err := c.get(ctx, "/health", nil); err != nil {
		if s, ok := err.(errStatus); ok && s == http.StatusUnauthorized {
			return append(out, Check{Fail, "daemon rejects this machine's secret (it started with another one)",
				"pipedpeer stop && pipedpeer start"})
		}
		return append(out, Check{Fail, "daemon not answering at " + c.API + ": " + err.Error(),
			"pipedpeer stop && pipedpeer start"})
	}
	out = append(out, Check{Level: OK, What: "daemon running at " + c.API})
	peers, reached := c.peers(ctx)
	intros := c.introducers(ctx)
	if reached {
		// The introducer is how machines join and re-find each other; with a
		// peer answering now the cluster works, so its silence is a warning
		// (e6: doctor ran seconds after Wi-Fi came back, before the next
		// registration).
		for i := range intros {
			if intros[i].Level == Fail {
				intros[i].Level = Warn
				intros[i].What += "; peers still answer, but new machines cannot join through it"
			}
		}
	}
	out = append(out, intros...)
	return append(out, peers...)
}

func (c Cluster) introducers(ctx context.Context) []Check {
	var list []introducerState
	err := c.get(ctx, "/v1/introducers", &list)
	if s, ok := err.(errStatus); ok && s == http.StatusNotFound {
		return []Check{{Warn, "this daemon does not report its introducers (older build)",
			"install the cluster's build on every machine"}}
	}
	if err != nil {
		return []Check{{Warn, "could not read introducer status: " + err.Error(), "pipedpeer stop && pipedpeer start"}}
	}
	if len(list) == 0 {
		if len(c.Introducers) == 0 {
			return []Check{{Warn, "no introducer configured: only machines on this network will be found", connectFix}}
		}
		return []Check{{Warn, "introducers configured (" + strings.Join(c.Introducers, ", ") +
			") but the daemon is not using them", "pipedpeer stop && pipedpeer start"}}
	}
	now := c.Now()
	// One answering introducer is enough to find the cluster: a silent one
	// next to it (an invite's LAN address seen from another network, E6) is
	// a warning, not a failure.
	silent := Fail
	for _, in := range list {
		if !in.LastReply.IsZero() && now.Sub(in.LastReply) < introducerFresh {
			silent = Warn
		}
	}
	var out []Check
	for _, in := range list {
		if !in.LastReply.IsZero() && now.Sub(in.LastReply) < introducerFresh {
			out = append(out, Check{Level: OK, What: fmt.Sprintf("introducer %s answering (%d members)", in.Addr, in.Members)})
			continue
		}
		why := "never answered"
		if !in.LastReply.IsZero() {
			why = "last answered " + now.Sub(in.LastReply).Round(time.Second).String() + " ago"
		}
		if in.LastErr != "" {
			why += ": " + in.LastErr
		}
		out = append(out, Check{silent, "introducer " + in.Addr + " not answering (" + why + ")",
			"check that machine is on and its UDP port is reachable from here; if its address changed, " + connectFix})
	}
	return out
}

// peers checks every other machine; reached is whether one answers now.
func (c Cluster) peers(ctx context.Context) (out []Check, reached bool) {
	var nodes []node
	if err := c.get(ctx, "/v1/nodes", &nodes); err != nil {
		return []Check{{Fail, "could not list peers: " + err.Error(), "pipedpeer stop && pipedpeer start"}}, false
	}
	peers := 0
	for _, n := range nodes {
		if n.Source == "self" || n.Path == "self" {
			continue
		}
		peers++
		checks := c.peer(ctx, n)
		reached = reached || (n.State == "healthy" && !failed(checks))
		out = append(out, checks...)
	}
	var unr []unreachable
	if err := c.get(ctx, "/v1/peers/unreachable", &unr); err == nil {
		for _, u := range unr {
			out = append(out, Check{Fail, "peer " + u.Peer + " has no direct path: " + u.Reason,
				"open UDP on both machines (`pipedpeer setup -y`); if both sit behind strict NATs, use a VPN such as Tailscale"})
		}
	} else if s, ok := err.(errStatus); !ok || s != http.StatusNotFound {
		out = append(out, Check{Warn, "could not read unreachable peers: " + err.Error(), ""})
	}
	if peers == 0 {
		out = append(out, Check{Warn, "no other machines found", connectFix})
	}
	return out, reached
}

func failed(checks []Check) bool {
	for _, c := range checks {
		if c.Level == Fail {
			return true
		}
	}
	return false
}

func (n node) name() string {
	id := n.NodeID
	if len(id) > 8 {
		id = id[:8]
	}
	if h := n.Capabilities["hostname"]; h != "" {
		// Cloned lab PCs share a hostname; the id tells them apart.
		return h + " (" + id + ")"
	}
	return id
}

func (n node) host() string {
	if n.Host != "" {
		return n.Host
	}
	ep := strings.TrimPrefix(n.SSHEndpoint, "ssh://")
	if i := strings.LastIndex(ep, "@"); i >= 0 {
		ep = ep[i+1:]
	}
	if h, _, err := net.SplitHostPort(ep); err == nil {
		return h
	}
	return ep
}

// working is the node's path that answers now: the active one when it does,
// else any verified one.
func (n node) working() (nodePath, bool) {
	var best nodePath
	found := false
	for _, p := range n.Paths {
		if p.OK && (!found || p.Active) {
			best, found = p, true
		}
	}
	return best, found
}

// strangers warns about a node's candidate addresses where a machine outside
// the cluster answered, while another path works: a network that reuses the
// node's subnet (E6). Not a fault; the daemon drops them.
func (n node) strangers(w nodePath) []Check {
	var out []Check
	for _, p := range n.Paths {
		if p.OK || !strings.Contains(p.Err, authtoken.UnprovenPhrase) {
			continue
		}
		at := net.JoinHostPort(p.Host, strconv.Itoa(p.Port))
		out = append(out, Check{Warn, n.name() + ": a different machine answers at " + at +
			" on this network; using the " + w.Kind + " path", ""})
	}
	return out
}

func (c Cluster) peer(ctx context.Context, n node) []Check {
	host, port := n.host(), n.DaemonPort
	var out []Check
	if w, ok := n.working(); ok {
		host, port = w.Host, w.Port
		out = n.strangers(w)
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	dctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	conn, err := c.Dial(dctx, "tcp", addr)
	cancel()
	if err != nil {
		lan := n.Source == "discovery" || n.Path == "lan"
		if lan {
			// Its discovery packets reached us, so UDP crosses; TCP does not.
			// The college-router case (experiments E5d).
			return append(out, Check{Fail, n.name() + " at " + addr + ": TCP blocked but UDP passes (a router between the subnets forwards only UDP)",
				"link over UDP instead: " + connectFix + "; or ask the network admin to allow TCP between the subnets"})
		}
		why := err.Error()
		if n.Reason != "" {
			why = n.Reason
		}
		return append(out, Check{Fail, n.name() + " at " + addr + " unreachable: " + why,
			"check that machine is on and running (`pipedpeer start` there), then run `pipedpeer doctor` on it"})
	}
	conn.Close()
	if n.State != "healthy" {
		what := n.name() + " is " + n.State
		if n.Reason != "" {
			what += ": " + n.Reason
		}
		out = append(out, Check{Warn, what, "run `pipedpeer doctor` on that machine"})
	} else {
		out = append(out, Check{Level: OK, What: n.name() + " healthy at " + addr})
	}
	if skew, ok := c.skew(ctx, addr); ok && (skew > maxSkew || skew < -maxSkew) {
		out = append(out, Check{Warn, fmt.Sprintf("%s clock is off by %s", n.name(), skew.Round(time.Second)),
			"turn on time sync on both machines: `sudo timedatectl set-ntp true`"})
	}
	return out
}

// skew is the peer's clock minus ours, from the Date header of any answer
// (a 401 carries one too). Date has one-second resolution, enough for maxSkew.
func (c Cluster) skew(ctx context.Context, addr string) (time.Duration, bool) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health", nil)
	if err != nil {
		return 0, false
	}
	sent := c.Now()
	resp, err := c.PeerHTTP.Do(req)
	if err != nil {
		say.Warnf("doctor: clock check of %s failed: %v\n", addr, err)
		return 0, false
	}
	resp.Body.Close()
	d, err := http.ParseTime(resp.Header.Get("Date"))
	if err != nil {
		say.Warnf("doctor: %s sent no usable Date header: %v\n", addr, err)
		return 0, false
	}
	return d.Sub(sent), true
}
