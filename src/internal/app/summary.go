package app

import (
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
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// receiptPrefix starts the one stderr line the shim writes at exit (C-RCPT).
const receiptPrefix = "[pipedpeer] receipt "

// receipt is the shim's account of where the script's parallel items ran.
type receipt struct {
	RemoteItems int            `json:"remote_items"`
	LocalItems  int            `json:"local_items"`
	ByWhere     map[string]int `json:"by_where"`
}

// summary is what a single run measured, rendered at its end.
type summary struct {
	Host string // executing daemon, as dialled
	Port int

	BuildTime  time.Duration // 0: not measured here
	Closure    string        // rendered closure line, "" to skip
	ScriptRan  bool
	ScriptTime time.Duration // 0 when unknown (attach)
	Peak       int64
	Stderr     string // the job's stderr, receipt line included
	Files      *daemonctl.ResultManifest
	Total      time.Duration
	Err        error
}

// closureLine says what crossed the wire for the environment.
func closureLine(up *daemonctl.UploadResponse, uploadTime time.Duration, cold bool) string {
	if up == nil {
		return ""
	}
	switch up.Closure {
	case "on-node":
		return "already on the node"
	case "partial":
		return fmt.Sprintf("sent %d of %d paths (%s)", up.SentPaths, up.TotalPaths, humanize.Bytes(uint64(up.SentBytes)))
	case "full":
		t := up.SendTime
		if t <= 0 {
			t = uploadTime
		}
		line := fmt.Sprintf("sent %s in %s", humanize.Bytes(uint64(up.SentBytes)), secs(t))
		if cold {
			line += " (cold)"
		}
		return line
	}
	return ""
}

// lastReceipt finds the newest receipt line in the job's stderr.
func lastReceipt(stderr string) (receipt, bool) {
	var rc receipt
	i := strings.LastIndex(stderr, receiptPrefix)
	if i < 0 {
		return rc, false
	}
	line := stderr[i+len(receiptPrefix):]
	if nl := strings.IndexByte(line, '\n'); nl >= 0 {
		line = line[:nl]
	}
	if err := json.Unmarshal([]byte(line), &rc); err != nil {
		fmt.Fprintf(os.Stderr, "warning: unreadable pipedpeer receipt %q: %v\n", line, err)
		return rc, false
	}
	return rc, true
}

// spreadLine says how the script's items were split: "120 items: 40 on
// laptop-a, 80 on 2 others (pc-2 40, pc-3 40)". names maps a peer's
// "host:daemon_port" to its hostname; an unknown peer prints as its address.
func spreadLine(rc receipt, ranOn string, names map[string]string) (line string, unresolved []string) {
	byWhere := rc.ByWhere
	if len(byWhere) == 0 {
		byWhere = map[string]int{"local": rc.LocalItems}
		if rc.RemoteItems > 0 {
			byWhere["peer:other machines"] = rc.RemoteItems
		}
	}
	local, total := 0, 0
	peers := map[string]int{}
	for where, n := range byWhere {
		total += n
		if where == "local" {
			local += n
			continue
		}
		addr := strings.TrimPrefix(where, "peer:")
		name := names[addr]
		if name == "" {
			name = addr
			if addr != "other machines" {
				unresolved = append(unresolved, addr)
			}
		}
		peers[name] += n
	}
	if total == 0 {
		return "", nil
	}
	if len(peers) == 0 {
		return fmt.Sprintf("%d items, all on %s", total, ranOn), unresolved
	}
	type kv struct {
		name string
		n    int
	}
	var list []kv
	remote := 0
	for name, n := range peers {
		list = append(list, kv{name, n})
		remote += n
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].n != list[j].n {
			return list[i].n > list[j].n
		}
		return list[i].name < list[j].name
	})
	parts := make([]string, len(list))
	for i, p := range list {
		parts[i] = fmt.Sprintf("%s %d", p.name, p.n)
	}
	others := fmt.Sprintf("%d on %d other", remote, len(list))
	if len(list) > 1 {
		others += "s"
	}
	others += " (" + strings.Join(parts, ", ") + ")"
	if local == 0 {
		return fmt.Sprintf("%d items: %s", total, others), unresolved
	}
	return fmt.Sprintf("%d items: %d on %s, %s", total, local, ranOn, others), unresolved
}

// summaryLines renders s. ranOn is the executing machine's hostname.
func summaryLines(s summary, ranOn string, names map[string]string) (lines []string, unresolved []string) {
	add := func(label, value string) {
		lines = append(lines, fmt.Sprintf("  %-12s %s", label, value))
	}
	if s.Host != "" && ranOn != s.Host {
		add("ran on", fmt.Sprintf("%s (%s)", ranOn, s.Host))
	} else {
		add("ran on", ranOn)
	}
	if s.BuildTime > 0 {
		add("environment", "built in "+secs(s.BuildTime))
	}
	if s.Closure != "" {
		add("closure", s.Closure)
	}
	if s.ScriptRan {
		var v []string
		if s.ScriptTime > 0 {
			v = append(v, secs(s.ScriptTime))
		}
		if s.Peak > 0 {
			v = append(v, "peak memory "+humanize.Bytes(uint64(s.Peak)))
		}
		if len(v) > 0 {
			add("script", strings.Join(v, " · "))
		}
	}
	if rc, ok := lastReceipt(s.Stderr); ok {
		var line string
		line, unresolved = spreadLine(rc, ranOn, names)
		if line != "" {
			add("spread", line)
		}
	}
	if m := s.Files; m != nil {
		if n := m.Count(); n == 0 && len(m.Deleted) == 0 {
			add("files back", "none")
		} else {
			v := fmt.Sprintf("%d (%d new, %d updated)", n, len(m.New), len(m.Updated))
			if len(m.Deleted) > 0 {
				v = fmt.Sprintf("%d (%d new, %d updated, %d deleted)", n, len(m.New), len(m.Updated), len(m.Deleted))
			}
			add("files back", v)
		}
	}
	switch {
	case errors.Is(s.Err, context.Canceled):
		add("total", "cancelled after "+secs(s.Total))
	case s.Err != nil:
		add("total", "failed after "+secs(s.Total))
	default:
		add("total", secs(s.Total))
	}
	return lines, unresolved
}

// printSummary looks up the names the summary needs from the executing
// daemon and prints it. Lookups that fail leave the raw value and say so.
func printSummary(w io.Writer, s summary) {
	ranOn := daemonHostname(s.Host, s.Port)
	names := map[string]string{}
	if _, ok := lastReceipt(s.Stderr); ok {
		names = peerNames(s.Host, s.Port)
	}
	lines, unresolved := summaryLines(s, ranOn, names)
	if len(unresolved) > 0 {
		fmt.Fprintf(os.Stderr, "warning: no hostname known for %s; showing the address\n", strings.Join(unresolved, ", "))
	}
	fmt.Fprintln(w, "\n=== Summary ===")
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
}

var lookupClient = &http.Client{Timeout: 3 * time.Second}

// daemonHostname asks the daemon at host:port for its hostname; on failure it
// warns and returns host.
func daemonHostname(host string, port int) string {
	var h struct {
		Capabilities map[string]string `json:"capabilities"`
	}
	url := fmt.Sprintf("http://%s/health", net.JoinHostPort(host, strconv.Itoa(port)))
	if err := getJSON(url, &h); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not ask %s for its hostname: %v\n", host, err)
		return host
	}
	if name := h.Capabilities["hostname"]; name != "" {
		return name
	}
	fmt.Fprintf(os.Stderr, "warning: %s reports no hostname; showing its address\n", host)
	return host
}

// peerNames maps "host:daemon_port" to hostname for every node the daemon at
// host:port knows — the same address form its pool receipt uses.
func peerNames(host string, port int) map[string]string {
	names := map[string]string{}
	var nodes []struct {
		SSHEndpoint  string            `json:"ssh_endpoint"`
		DaemonPort   int               `json:"daemon_port"`
		Capabilities map[string]string `json:"capabilities"`
	}
	url := fmt.Sprintf("http://%s/v1/nodes", net.JoinHostPort(host, strconv.Itoa(port)))
	if err := getJSON(url, &nodes); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not list %s's peers for the summary: %v\n", host, err)
		return names
	}
	for _, n := range nodes {
		name := n.Capabilities["hostname"]
		if name == "" {
			continue
		}
		ep := n.SSHEndpoint
		if i := strings.LastIndex(ep, "@"); i >= 0 {
			ep = ep[i+1:]
		}
		if h, _, err := net.SplitHostPort(ep); err == nil {
			ep = h
		}
		names[net.JoinHostPort(ep, strconv.Itoa(n.DaemonPort))] = name
	}
	return names
}

func getJSON(url string, v any) error {
	resp, err := lookupClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func secs(d time.Duration) string {
	return fmt.Sprintf("%.1f s", d.Seconds())
}
