package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// clusterEvent is one thing this machine's daemon did, read back out of
// daemon.log. Kind is one of: start join leave link nopath work cancel env
// trust pool warn net.
type clusterEvent struct {
	At   time.Time
	Kind string
	Text string
	// minor events (net, stray warnings) are shown only with --all.
	minor bool
}

func (e clusterEvent) String() string {
	ts := strings.Repeat(" ", 8)
	if !e.At.IsZero() {
		ts = e.At.Format("15:04:05")
	}
	return fmt.Sprintf("%s  %-6s %s", ts, strings.ToUpper(e.Kind), e.Text)
}

var (
	ansiRe      = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	stdlibRe    = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (.*)$`)
	kvRe        = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*=`)
	reachableRe = regexp.MustCompile(`^internet: peer (\S+) reachable at \S+ \((.*)\)$`)
	callingRe   = regexp.MustCompile(`^internet: peer (\S+) is trying to reach us; punching back$`)
	couldNotRe  = regexp.MustCompile(`^internet: could not meet (\S+?):`)
	idRe        = regexp.MustCompile(`\b[0-9a-f]{8}(?:-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{8})?\b`)
)

// parseEventLine turns one daemon.log line (zerolog console, zerolog JSON or
// stdlib log format) into an event. ok is false for lines that are not events.
func parseEventLine(line string) (clusterEvent, bool) {
	line = strings.TrimSpace(ansiRe.ReplaceAllString(line, ""))
	if line == "" {
		return clusterEvent{}, false
	}
	var (
		at     time.Time
		level  = "info"
		msg    string
		fields = map[string]string{}
	)
	switch {
	case strings.HasPrefix(line, "{"):
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return clusterEvent{}, false
		}
		for k, v := range raw {
			s := fmt.Sprint(v)
			switch k {
			case "level":
				level = s
			case "message":
				msg = s
			case "time":
				if t, err := time.Parse(time.RFC3339, s); err == nil {
					at = t.Local()
				}
			default:
				fields[k] = s
			}
		}
	case stdlibRe.MatchString(line):
		m := stdlibRe.FindStringSubmatch(line)
		at, _ = time.ParseInLocation("2006/01/02 15:04:05", m[1], time.Local)
		msg = m[2]
	default:
		toks := strings.Fields(line)
		if len(toks) < 3 {
			return clusterEvent{}, false
		}
		now := time.Now()
		var t time.Time
		var err error
		if t, err = time.Parse("15:04:05", toks[0]); err != nil {
			t, err = time.Parse("3:04PM", toks[0])
		}
		if err != nil {
			return clusterEvent{}, false
		}
		at = time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.Local)
		switch toks[1] {
		case "INF":
			level = "info"
		case "WRN":
			level = "warn"
		case "ERR", "FTL", "PNC":
			level = "error"
		case "DBG", "TRC":
			level = "debug"
		default:
			return clusterEvent{}, false
		}
		rest := toks[2:]
		i := 0
		for i < len(rest) && !kvRe.MatchString(rest[i]) {
			i++
		}
		msg = strings.Join(rest[:i], " ")
		for _, kv := range rest[i:] {
			if k, v, found := strings.Cut(kv, "="); found && kvRe.MatchString(kv) {
				fields[k] = v
			}
		}
	}
	return classifyEvent(at, level, msg, fields)
}

func classifyEvent(at time.Time, level, msg string, f map[string]string) (clusterEvent, bool) {
	ev := func(kind, text string) (clusterEvent, bool) {
		return clusterEvent{At: at, Kind: kind, Text: text}, true
	}
	who := func() string {
		if f["host"] != "" {
			return f["host"]
		}
		return f["peer"]
	}
	paren := func(s string) string {
		if s == "" {
			return ""
		}
		return " (" + s + ")"
	}
	switch {
	case msg == "peer up":
		return ev("join", who()+" is up"+paren(f["at"]))
	case msg == "peer joined over the internet":
		return ev("join", f["peer"]+" joined over the internet"+paren(f["at"]))
	case msg == "peer down":
		return ev("leave", who()+" is gone")
	case msg == "peer left":
		return ev("leave", f["peer"]+" left")
	case reachableRe.MatchString(msg):
		m := reachableRe.FindStringSubmatch(msg)
		return ev("link", m[1]+" reachable ("+m[2]+")")
	case callingRe.MatchString(msg):
		return ev("link", callingRe.FindStringSubmatch(msg)[1]+" is calling; answering")
	case strings.HasPrefix(msg, "internet: no direct path to"):
		return ev("nopath", strings.TrimPrefix(msg, "internet: "))
	case couldNotRe.MatchString(msg):
		return ev("nopath", "no direct path to "+couldNotRe.FindStringSubmatch(msg)[1])
	case msg == "peer has no direct path; not offering it work":
		return ev("nopath", f["peer"]+" has no direct path"+paren(f["reason"])+"; not offering it work")
	case msg == "lease granted":
		return ev("work", "lease granted: job "+f["job"]+" from "+f["submitter"])
	case msg == "lease released":
		return ev("work", "lease released ("+f["status"]+")")
	case msg == "lease expired":
		e, _ := ev("warn", "lease expired ("+f["state"]+")")
		return e, true
	case msg == "job started":
		return ev("work", "job "+f["job"]+" started: "+f["script"])
	case msg == "job finished":
		return ev("work", fmt.Sprintf("job %s finished: exit %s in %s s", f["job"], f["exit"], f["secs"]))
	case msg == "job cancelled":
		return ev("cancel", fmt.Sprintf("job %s cancelled after %s s", f["job"], f["secs"]))
	case msg == "closure imported":
		return ev("env", "environment imported ("+f["how"]+", "+f["secs"]+" s)")
	case strings.Contains(msg, "imported a closure whose signatures"),
		msg == "broadcasting closure to peers",
		msg == "sending only the store paths this peer lacks",
		msg == "peer already has every path; nothing to send":
		return ev("env", msg)
	case strings.Contains(msg, "system nix now trusts"):
		return ev("trust", msg)
	case strings.HasPrefix(msg, "[pool] "):
		return ev("pool", strings.TrimPrefix(msg, "[pool] "))
	case msg == "daemon starting":
		return ev("start", "daemon starting"+paren("port "+f["port"]))
	case strings.HasPrefix(msg, "internet: "):
		e, _ := ev("net", strings.TrimPrefix(msg, "internet: "))
		e.minor = true
		return e, true
	case level == "warn" || level == "error":
		e, _ := ev("warn", msg)
		e.minor = true
		return e, true
	}
	return clusterEvent{}, false
}

// collapseEvents merges consecutive events with the same kind and text.
func collapseEvents(in []clusterEvent) []clusterEvent {
	var out []clusterEvent
	var base string
	n := 0
	for _, e := range in {
		if k := len(out) - 1; k >= 0 && out[k].Kind == e.Kind && base == e.Text {
			n++
			out[k].Text = fmt.Sprintf("%s (×%d)", e.Text, n)
			continue
		}
		n, base = 1, e.Text
		out = append(out, e)
	}
	return out
}

// readEvents returns the last limit events in the log, oldest first.
func readEvents(logFile string, limit int, all bool) ([]clusterEvent, error) {
	data, err := os.ReadFile(logFile)
	if err != nil {
		return nil, fmt.Errorf("daemon log not found at %s (is the daemon running?): %w", logFile, err)
	}
	var evs []clusterEvent
	for _, line := range strings.Split(string(data), "\n") {
		if e, ok := parseEventLine(line); ok && (all || !e.minor) {
			evs = append(evs, e)
		}
	}
	evs = collapseEvents(evs)
	if limit > 0 && len(evs) > limit {
		evs = evs[len(evs)-limit:]
	}
	return evs, nil
}

// followEvents calls fn for every new event, starting from the end of the
// file, until ctx is done. A plain 500 ms poll, like tailTraffic.
// ponytail: no rotation handling (Start() truncates the log); repeats are
// dropped when identical to the previous event, without a count.
func followEvents(ctx context.Context, logFile string, all bool, fn func(clusterEvent)) error {
	f, err := os.Open(logFile)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	buf := make([]byte, 4096)
	var pending []byte
	var last clusterEvent
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			pending = append(pending, buf[:n]...)
			for {
				idx := bytes.IndexByte(pending, '\n')
				if idx < 0 {
					break
				}
				line := string(pending[:idx])
				pending = pending[idx+1:]
				e, ok := parseEventLine(line)
				if !ok || (!all && e.minor) || (e.Kind == last.Kind && e.Text == last.Text) {
					continue
				}
				last = e
				fn(e)
			}
		}
		if rerr != nil || n == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

// nodeNames maps the first 8 characters of each node id to its hostname.
func nodeNames(daemonPort int) map[string]string {
	names := map[string]string{}
	nodes, err := fetchClusterNodes(daemonPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "events: could not look up node names, showing ids: %v\n", err)
		return names
	}
	for _, n := range nodes {
		if h := n.Capabilities["hostname"]; h != "" && len(n.NodeID) >= 8 {
			names[n.NodeID[:8]] = h
		}
	}
	return names
}

// withNames replaces node ids (8 hex, 16 hex or uuid) by hostnames.
func (e clusterEvent) withNames(names map[string]string) clusterEvent {
	e.Text = idRe.ReplaceAllStringFunc(e.Text, func(id string) string {
		if h, ok := names[id[:8]]; ok {
			return h
		}
		return id
	})
	return e
}

func newEventsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "events",
		Short: "Show what this machine's daemon has been doing (joins, links, jobs, environments, pool work)",
		RunE: func(cmd *cobra.Command, args []string) error {
			follow, _ := cmd.Flags().GetBool("follow")
			limit, _ := cmd.Flags().GetInt("limit")
			all, _ := cmd.Flags().GetBool("all")
			port, _ := cmd.Flags().GetInt("port")
			logFile := daemonctl.DefaultPaths().LogFile

			names := nodeNames(port)
			evs, err := readEvents(logFile, limit, all)
			if err != nil {
				return err
			}
			for _, e := range evs {
				fmt.Println(e.withNames(names))
			}
			if !follow {
				return nil
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			refreshed := time.Now()
			return followEvents(ctx, logFile, all, func(e clusterEvent) {
				if time.Since(refreshed) > 10*time.Second {
					names, refreshed = nodeNames(port), time.Now()
				}
				fmt.Println(e.withNames(names))
			})
		},
	}
	cmd.Flags().BoolP("follow", "f", false, "Keep running and print new events")
	cmd.Flags().Int("limit", 30, "Max events to show")
	cmd.Flags().Bool("all", false, "Include network details and other warnings")
	cmd.Flags().Int("port", 38080, "Local daemon port (for node names)")
	return cmd
}
